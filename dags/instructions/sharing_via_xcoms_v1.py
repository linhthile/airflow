from airflow import DAG

from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'rei',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

def greet(age, ti):
    name = ti.xcom_pull(task_ids = 'get_name')
    print(f"Hello world! My name is {name},"
          f"amd I am {age} years old!")

def get_name():
    return 'reile'

with DAG (
    default_args=default_args,
    dag_id='sharing_via_xcoms_v1',
    description='sharing data via xcoms',
    start_date=datetime(2023,7,2),
    schedule='@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable=greet,
        op_kwargs={'age':20}
    )
    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name
    )
    task2 >> task1
