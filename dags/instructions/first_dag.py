from airflow import DAG 
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'rei',
    'retries':5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'first_dag_v4',
    default_args = default_args,
    description = 'this is my first dag',
    start_date = datetime(2023,7,1,2),
    schedule='@daily',
    catchup = False
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command='echo hello world, this is the first task!'
    )
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command='echo hey, i am task 2 and i run after task 1'
    )
    task3 = BashOperator(
        task_id = 'third_task',
        bash_command='echo hey, i am task 3 and i run after 1'
    )
    # The 1ST
    # task1.set_downstream(task2)
    # task1.set_downstream(task3)

    # The 2ND
    # task1 >> task2
    # task1 >> task3

    # The 3RD
    task1 >> [task2, task3]
    