from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
)
def test_dag():
    @task
    def hello():
        print("Hello from Airflow!")
        return "hello"

    @task
    def goodbye(message: str):
        print(f"Received message: {message}")
        print("Goodbye from Airflow!")

    msg = hello()
    goodbye(msg)


test_dag()
