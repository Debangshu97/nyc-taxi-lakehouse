from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

PROJECT = "cd /opt/airflow/project && python -m src.taxi."

default_args = {"retries": 2, "retry_delay": timedelta(minutes=2)}


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["nyc-taxi"],
)
def taxi_pipeline():
    bronze = BashOperator(task_id="bronze", bash_command=PROJECT + "bronze")
    silver = BashOperator(task_id="silver", bash_command=PROJECT + "silver")
    gold = BashOperator(task_id="gold", bash_command=PROJECT + "gold")

    bronze >> silver >> gold


taxi_pipeline()
