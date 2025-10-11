from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "admin",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="pikcha_etl_daily",
    default_args=default_args,
    description="Daily ETL job for Pikcha Analytics",
    schedule_interval="0 3 * * *",
    start_date=datetime(2025, 10, 5),
    catchup=False,
)

run_etl = BashOperator(
    task_id="run_pyspark_etl",
    bash_command="python /opt/airflow/scripts/pyspark_etl.py",
    dag=dag,
)

run_etl
