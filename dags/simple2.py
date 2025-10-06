# /opt/airflow/dags/simple.py
from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="simple_test_dag",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test"],
) as dag:
    t1 = BashOperator(
        task_id="print_date",
        bash_command="date && echo 'hello from task'",
    )

