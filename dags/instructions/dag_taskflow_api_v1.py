from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner':'reile',
    'retries':5,
    'retry_delay': timedelta(minutes = 5)
}
@dag(dag_id = 'dag_taskflow_api_v01',
     default_args = default_args,
     start_date = datetime(2023,7,2),
     schedule = '@daily')

def hello_world_etl():
    
    @task()
    def get_name():
        return "reile"
    @task()
    def get_age():
        return 25
    @task()
    def greet(name,age):
        print(f"Hello World! My name is {name}"
              f" and I am {age} years old!")
    name = get_name()
    age = get_age()
    greet(name = name, age = age)

greet_dag = hello_world_etl()   
