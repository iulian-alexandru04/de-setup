from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG
with DAG(
    dag_id='simple_test_dag',
    default_args=default_args,
    description='A simple DAG to verify Airflow setup',
    schedule_interval=None,  # Run manually
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
    )

    t3 = BashOperator(
        task_id='print_hello',
        bash_command='echo "Airflow setup is working!"',
    )

    t1 >> t2 >> t3


