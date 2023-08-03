import pandas as pd
import psycopg2
import gspread
import numpy as np

from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, Table, MetaData, select, insert, delete


# Connect to Google Sheet using gspread and service account credentials
def connect_data_from_gsheet():
    gc = gspread.service_account(filename='/opt/airflow/dags/taurus/credentials.json')

    # Open the Google Sheet
    sheet = gc.open("MDA_team_member_copy")
    
    # Select the desired worksheet
    worksheet = sheet.sheet1
    
    # Read the data from the worksheet into a dataframe
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    return df  

# Delete old data and then Insert new data into PostgreSQL
def delete_insert_clean_data_postgres():
    
    # Connect to the database
    engine = create_engine('postgresql://wzfpjykq:xoxheOAoEj6aSaQzFvoE7TJn4InddR1j@satao.db.elephantsql.com:5432/wzfpjykq')
    connection = engine.connect()

    # Get data from Google Sheet as a dataframe
    df = connect_data_from_gsheet()
  
    #Clean the data
    df['CLV Start'] = pd.to_datetime(df['CLV Start'], errors = 'coerce').dt.strftime('%d/%m/%Y')
    df['Looker Start'] = pd.to_datetime(df['Looker Start'],errors= 'coerce').dt.strftime('%d/%m/%Y')
    df['Employee ID'] = df['Employee ID'].astype(str)
    df['Phone Number'] = df['Phone Number'].astype(str)

    # Create a new metadata object and associates it with PostGres SQL 
    metadata = MetaData(bind=engine, schema = 'taurus') 
    tb = Table('mda_airflow', metadata, autoload = True, autoload_with =engine)

    # Truncate the table to remove the existing data
    with connection.begin():
        connection.execute(tb.delete())

    # Insert data into PostgreSQL
    for index, row in df.iterrows():   #iterates over each row in the dataframe, it specifies the table to insert data into the 'tb' and sets the values for the columns in the rows
        stmt = insert(tb).values(no = row['No.'],
                                 name = row['Name'],
                                 employee_id = row['Employee ID'],
                                 dde_spoke_team = row['DDE Spoke Team'],
                                 pi_joined_status = row['PI Joined (Status)'],
                                 guild_team = row['Guild Team'],
                                 picture = row['Color'],
                                 clv_start = row['CLV Start'],
                                 looker_start = row['Looker Start'],
                                 dob = row['DoB'],
                                 english_name = row['English Name'],
                                 full_name = row['Full Name'],
                                 email_gmail = row['Email (Gmail)'],
                                 email_one = row['Email (ONE)'],
                                 email_cyberlogitec = row['Email (CyberLogitec)'],
                                 phone_number = row['Phone Number'],
                                 jira_name = row['Jira Name'],
                                 jira_id = row['Jira ID'],
                                 github_account = row['Github Account']             
        )
        connection.execute(stmt)     
    # Close the database connection
    connection.close()


# Define a dictionary that contains default values for parameters used in a data pipeline or workflow
default_args = {
    'owner':'rei',
    'retries':0,
    'retry_delay':timedelta(minutes=5)
}

# Define DAG
with DAG(
    dag_id='taurus',
    default_args=default_args,
    start_date=datetime(2023, 7, 20),
    schedule='0 6 * * *',
    catchup=True
) as dag:
    # Define Task 1: Import the data
    task1 = PythonOperator(
        task_id='connect_data',
        python_callable=connect_data_from_gsheet
    )
    # Define Task 3: Insert new data
    task2 = PythonOperator(
        task_id='delete_insert_data',
        python_callable=delete_insert_clean_data_postgres
    )
    # Set the task flow
    task1 >>  task2
