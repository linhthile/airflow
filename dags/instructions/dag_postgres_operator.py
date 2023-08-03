from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner':'rei',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

with DAG(
    dag_id = 'dag_postgres_operator',
    default_args = default_args,
    start_date = datetime(2023,7,1),
    schedule='0 6 * * *'
) as dag:
    task1 = PostgresOperator(
        task_id = 'create_postgres_table',
        postgres_conn_id ='postgres_localhost',
        sql= """
            create table if not exists dag_run(
                dt date,
                dag_id varchar,
                CONSTRAINT dag_run_pk PRIMARY KEY (dt, dag_id)
            )
        """
    )
    task2 = PostgresOperator(
        task_id = 'insert_into_table',
        postgres_conn_id = 'postgres_localhost',
        sql="""
            insert into dag_run(dt, dag_id) values ('{{ds}}','{{dag.dag_id}}')
        """
    )
    task3 = PostgresOperator(
        task_id = 'delete_data_from_table',
        postgres_conn_id = 'postgres_localhost',
        sql="""
            delete from dag_run where dt = '{{ds}}' and dag_id = '{{dag.dag_id}}'
        """
     )
    
    task1 >> task3 >> task2
