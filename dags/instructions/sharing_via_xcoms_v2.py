from airflow import DAG

from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'rei',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids ='get_name',key='first_name')
    last_name = ti.xcom_pull(task_ids ='get_name',key='last_name')
    age = ti.xcom_pull(task_ids = 'get_age',key='age')
    print(f"Hello world! My name is {first_name} {last_name},"
          f"and I am {age} years old!")

def get_name(ti):
    ti.xcom_push(key='first_name',value ='reile')
    ti.xcom_push(key='last_name', value = 'linh')

def get_age(ti):
    ti.xcom_push(key='age',value=25)

with DAG (
    default_args=default_args,
    dag_id='sharing_via_xcoms_v2',
    description='sharing data via xcoms',
    start_date=datetime(2023,7,2),
    schedule='@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable=greet
        # op_kwargs={'age':20}
    )
    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name
    )
    task3 = PythonOperator(
        task_id = 'get_age',
        python_callable=get_age
    )
    [task2,task3] >> task1
