from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import s3fs
from sqlalchemy import create_engine

MINIO_CONF = {
    "key" : "minio_admin",
    "secret" : "minio_password",
    "client_kwargs" : {"endpoint_url": "http://minio:9000"}
}

DB_URL = "postgresql://admin:adminpassword@postgres:5432/warehouse_db"

def load_silver_to_postgre(**kwargs):
    execution_date = kwargs['execution_date']

    year = execution_date.year
    month = execution_date.month
    day = execution_date.month

    s3_path = f"s3a://silver/crypto_trades/"

    print(f"Scanning data for date: {year}-{month}-{day}")

    fs = s3fs.S3FileSystem(**MINIO_CONF)

    try:
        files = fs.glob(f"silver/crypto_trades/**/*.parquet")
        
        if not files:
            print(" No parquet files found!")
            return
        print(f"Found {len(files)} files. Reading")

        df_list = []
        for file in files:
            with fs.open(file) as f:
                df = pd.read_parquet(f)
                df_list.append(df)
        
        if not df_list:
            return
        
        full_df = pd.concat(df_list, ignore_index=True)

        print(f"Total rows loaded from Minio: {len(full_df)}")

        engine = create_engine(DB_URL)

        cols_to_load = ['symbol', 'price', 'quantity', 'is_buyer_maker', 'trade_timestamp', 'ingest_timestamp']

        df_to_load = full_df[cols_to_load].copy()
        
        df_to_load.to_sql(
            'silver_crypto_trades_history',
            engine,
            if_exists='append',
            index = False,
            method='multi',
            chunksize=1000
        )
        print("Sucessfully loaded dataa to Postgres!")
    except Exception as e:
        print(f"Error:{e}")
        raise
# Định nghĩa DAGs
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hourly_batch_load_silver',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    tags = ['batch', 'minio', 'postgres']
) as dag:
    task_load_data = PythonOperator(
        task_id='load_minio_to_postgres',
        python_callable=load_silver_to_postgre,
        provide_context = True
    )

    task_load_data
    