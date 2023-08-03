from airflow import DAG

from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'rei',
    'retries':5,
    'retry_delay': timedelta(minutes=5)

}
def greet():
    print("Hello world!")

with DAG (
    default_args=default_args,
    dag_id='dag_python_operator_v1',
    description='my first dag using python operator',
    start_date=datetime(2023,7,2),
    schedule='@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable=greet
    )