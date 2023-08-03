from datetime import datetime, timedelta

from airflow import DAG 
from airflow.operators.python import PythonOperator


default_args = {
    'owner' : 'rei',
    'retries' : 5,
    'retry_delay' : timedelta(minutes=5)
}

def get_sklearn():
    import sklearn
    print(f"Scikit learn version: {sklearn.__version__}")

with DAG(
    default_args= default_args,
    dag_id = 'dag_postgres_operator',
    start_date=datetime(2023,7,1),
    schedule = '@daily'
) as dag:
    get_sklearn = PythonOperator(
        task_id = 'get_sklearn',
        python_callable= get_sklearn
    )
