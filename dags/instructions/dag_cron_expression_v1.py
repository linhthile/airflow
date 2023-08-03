from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'rei',
    'retries': 5,
    'retry_delay': timedelta(minutes = 5)
}
with DAG (
    dag_id = 'dag_cron_expression_v1',
    default_args=default_args,
    start_date=datetime(2023,7,7),
    schedule = '@daily'
)as dag:
    task1 = BashOperator(
        task_id = 'task1',
        bash_command="echo dag with cron expression"
    )