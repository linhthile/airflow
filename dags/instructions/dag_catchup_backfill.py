from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args ={
    'owner':'rei',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'dag_catchup_backfill_v1',
    default_args = default_args,
    start_date = datetime(2023,7,2),
    schedule = '@daily',
    catchup = True
) as dag:    
    task1 = BashOperator(
        task_id = 'task1',
        bash_command='echo this is a simple bash command'
    )


