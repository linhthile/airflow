import pandas as pd
import psycopg2
import gspread
import numpy as np

from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

default_args = {
    'owner':'rei',
    'retries':0,
    'retry_delay':timedelta(minutes=5)
}

# Connect to Google Sheet using gspread and service account credentials
def connect_data_from_gsheet():
    gc = gspread.service_account(filename='/opt/airflow/dags/taurus/credentials.json')

    # Open the Google Sheet
    sheet = gc.open("MDA_team_member_copy")
    
    # Select the desired worksheet
    worksheet = sheet.sheet1
    
    # Read the data from the worksheet into a dataframe
    data = worksheet.get_all_records()
    headers = data.pop(0)
    df = pd.DataFrame(data, columns=headers)
    return df

# Delete data in table
def del_data_postgres():

    # Connect to the database
    engine = create_engine('postgresql://wzfpjykq:xoxheOAoEj6aSaQzFvoE7TJn4InddR1j@satao.db.elephantsql.com:5432/wzfpjykq')

    # Get data from Google Sheet as a dataframe
    df = connect_data_from_gsheet()

    # Replace empty strings with a default value
    df['Vaccin'].replace('', 0, inplace=True)

    # Delete existing data from the table
    engine.execute("DELETE FROM taurus.mda_airflow")

    # Close the database connection
    engine.dispose()

# Insert data into PostgreSQL
def insert_data_postgres():

    # Connect to the database
    engine = create_engine('postgresql://wzfpjykq:xoxheOAoEj6aSaQzFvoE7TJn4InddR1j@satao.db.elephantsql.com:5432/wzfpjykq')

    # Get data from Google Sheet as a dataframe
    df = connect_data_from_gsheet()
    print(df)
    # Insert data into PostgreSQL
    df.to_sql('taurus.mda_airflow',con = engine, if_exists='append', index=False, method = 'multi')

    # Close the database connection
    engine.dispose()

with DAG(
    dag_id='airflow',
    default_args=default_args,
    start_date=datetime(2023, 7, 13),
    schedule='0 6 * * *',
    catchup=True
) as dag:
    # Define Task 1: Import the data
    task1 = PythonOperator(
        task_id='connect_data',
        python_callable=connect_data_from_gsheet
    )
    # Define Task 2: Delete existing data and insert new data
    task2 = PythonOperator(
        task_id='del_data',
        python_callable=del_data_postgres
    )
    # Define Task 3: Insert new data
    task3 = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data_postgres
    )
    # Set the task flow
    task1 >> task2 >> task3
