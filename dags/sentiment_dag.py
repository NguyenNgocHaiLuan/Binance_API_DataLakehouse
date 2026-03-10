from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import psycopg2
import logging

# Settings DB
DB_CONFIG = {
    "dbname": "warehouse_db",
    "user": "admin",
    "password": "adminpassword",
    "host": "postgres",
    "port": "5432"
}
# URL của API
API_URL = "https://api.alternative.me/fng/?limit=1"

def fetch_sentiment_data():
    logging.info("Fetching sentiment data from API")
    # Gọi API (Extract)
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()['data'][0]
    except Exception as e:
        logging.error(f"Error fetching data from API: {e}")
        raise

    # Xử lý dữ liệu (Transform)
    fng_value = int(data['value'])
    fng_class = data['value_classification']
    # API sẽ trả về timestamp nên sẽ đổi sang định dạng YYYY-MM-DD
    timestamp = int(data['timestamp'])
    date_str = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')

    logging.info(f"Data fetched: Date={date_str} | Value={fng_value} ({fng_class})")

    # Tải dữ liệu vào Postgres (Load)
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        # Kỹ thuật UPSERT (Update if exists, Insert if new)
        # Giúp script chạy lại nhiều lần mà không bị lỗi trùng lặp
        query = """
            INSERT INTO fact_market_sentiment (date, fng_value, fng_classification)
            VALUES (%s, %s, %s)
            ON CONFLICT (date) 
            DO UPDATE SET 
                fng_value = EXCLUDED.fng_value, 
                fng_classification = EXCLUDED.fng_classification,
                updated_at = CURRENT_TIMESTAMP;
        """
        
        cur.execute(query, (date_str, fng_value, fng_class))
        conn.commit()
        
        cur.close()
        conn.close()
        logging.info("Successfully loaded data into Postgres!")
        
    except Exception as e:
        logging.error(f"Database Error: {e}")
        raise

# ĐỊNH NGHĨA DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,               # Nếu lỗi, thử lại 1 lần
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'market_sentiment_daily',   # Tên DAG hiện trên giao diện
    default_args=default_args,
    description='Fetch Fear and Greed Index daily',
    schedule_interval='0 3 * * *', # Chạy lúc 03:00 Theo giờ UTC thì sẽ là 10:00 Việt Nam chạy
    catchup=False,              # Không chạy bù các ngày quá khứ
    tags=['crypto', 'batch'],
) as dag:

    # Task duy nhất để gọi hàm Python ở trên
    fetch_task = PythonOperator(
        task_id='fetch_fear_greed',
        python_callable=fetch_sentiment_data
    )

    fetch_task