# Importing necessary libraries
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Using the @dag decorator to define the DAG
@dag(
    schedule_interval=timedelta(days=1),  # The interval at which to run the DAG
    start_date=days_ago(1),  # Start date of the DAG
    default_args=default_args,
    catchup=False,
    description='A simple Hello World DAG using decorators',
    tags=['example'],  # Tags for the DAG
)
def hello_world_dag():

    # Using the @task decorator to define a task
    @task()
    def print_hello():
        print('Hello from the first task!')

    # Another task defined using the @task decorator
    @task()
    def print_world():
        print('Hello from the second task!')

    # Calling the tasks
    hello = print_hello()
    world = print_world()

# Assigning the DAG to a variable
hello_world = hello_world_dag()

