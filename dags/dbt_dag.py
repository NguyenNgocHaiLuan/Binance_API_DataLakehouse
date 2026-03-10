from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner':'airflow',
    'start_date': datetime(2026, 1, 25),
    'retries': 1,
}
with DAG(
    'dbt_transform_daily',
    default_args=default_args,
    schedule_interval='0 3 * * *',
    catchup=False
) as dag:

    dbt_run = BashOperator(
        task_id = 'dbt_run_models',
        bash_command = 'cd /opt/airflow/dbt/crypto_analytics && dbt run --profiles-dir .'
    )

    dbt_test = BashOperator(
        task_id='dbt_test_models',
        bash_command='cd /opt/airflow/dbt/crypto_analytics && dbt test --profiles-dir .'
    )

    dbt_run >> dbt_test