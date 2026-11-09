from datetime import datetime

from airflow.sdk import dag, task


@dag(schedule=None, start_date=datetime(2024, 1, 1), catchup=False, tags=["nyc-taxi"])
def hello_taxi():
    @task
    def say_hello():
        print("Airflow can see the nyc-taxi-lakehouse dags folder")

    say_hello()


hello_taxi()
