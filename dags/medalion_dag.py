from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DOCKER_SPARK = "docker exec -u 0 de_spark_master spark-submit"

with DAG(
    dag_id="medallion_spark_pipeline",
    default_args=default_args,
    description="Bronze → Silver → Gold Medallion Pipeline",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["spark", "medallion"],
) as dag:

    bronze_task = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"{DOCKER_SPARK} /tmp/spark_stream_bronze_ingestion_data.py",
        execution_timeout=timedelta(minutes=15),
    )

    silver_task = BashOperator(
        task_id="silver_transform",
        bash_command=f"{DOCKER_SPARK} /tmp/spark_stream_silver_transform_data.py",
        execution_timeout=timedelta(minutes=15),
    )

    gold_task = BashOperator(
        task_id="gold_aggregate_modeling",
        bash_command=f"{DOCKER_SPARK} /tmp/spark_stream_gold_aggregate_modeling_data.py",
        execution_timeout=timedelta(minutes=15),
    )

    bronze_task >> silver_task >> gold_task