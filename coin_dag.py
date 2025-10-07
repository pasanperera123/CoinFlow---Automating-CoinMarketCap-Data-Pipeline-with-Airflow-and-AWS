#coin_dag.py
import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from coin import coin_handler


# Default arguments for tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,                        # Retry 2 times on failure
    "retry_delay": dt.timedelta(minutes=3)  # Wait 3 minutes between retries
}

with DAG(
    dag_id="coin_dag",
    default_args=default_args,
    start_date=dt.datetime(2025, 10, 7),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["coin"], 
) as dag:

    start_task = EmptyOperator(task_id="start")

    ETL_task = PythonOperator(
        task_id="coin_handler",
        python_callable=coin_handler,
        retries=2,
        retry_delay=dt.timedelta(minutes=3),
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> ETL_task >> end_task