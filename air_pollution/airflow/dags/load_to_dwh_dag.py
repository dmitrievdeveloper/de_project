from __future__ import annotations

import pendulum
import os
from datetime import timedelta
from clickhouse_driver import Client
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

# Constants
S3_BUCKET = "air-pollution-data"
RAW_TABLE_NAME = "raw_air_quality"
CLICKHOUSE_DATABASE = "default"
CITIES = ["London", "Tokyo", "Paris", "Sydney", "Ottawa", "Seoul"]

def create_table_if_not_exists():
    """Создает таблицу raw_air_quality если она не существует"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS default.raw_air_quality (
        city_name String,
        dt DateTime,
        aqi UInt8,
        co Float32,
        no Float32,
        no2 Float32,
        o3 Float32,
        so2 Float32,
        pm2_5 Float32,
        pm10 Float32,
        nh3 Float32,
        load_ts DateTime
    ) ENGINE = MergeTree()
    ORDER BY (city_name, dt)
    """
    ch_client = Client(
        host='clickhouse-server',
        port=9000,
        user=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database='default'
    )
    ch_client.execute(create_table_query)

def load_s3_to_clickhouse_raw(**context):
    """
    Загружает данные из CSV файлов в MinIO в ClickHouse raw таблицу
    используя один запрос для всех городов
    """
    logical_date = context["logical_date"]
    target_date = logical_date.subtract(days=1)
    target_date_str = target_date.format("YYYY_MM_DD")

    ch_client = Client(
        host='clickhouse-server',
        port=9000,
        user=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database='default'
    )
    
    create_table_if_not_exists()

    minio_endpoint = f"http://minio:9000"
    minio_user = os.getenv("MINIO_ROOT_USER")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD")

    logging.info(f"Starting bulk data load from S3 to ClickHouse for date: {target_date_str}")

    # Создаем шаблон пути для всех городов
    s3_path = f"{minio_endpoint}/{S3_BUCKET}/*/*_{target_date_str}.csv"

    insert_query = f"""
    INSERT INTO {CLICKHOUSE_DATABASE}.{RAW_TABLE_NAME} (
        city_name, dt, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3, load_ts
    )
    SELECT
        city_name,
        parseDateTimeBestEffort(dt),
        toInt8OrNull(toString(aqi)),
        toFloat32OrNull(toString(co)),
        toFloat32OrNull(toString(no)),
        toFloat32OrNull(toString(no2)),
        toFloat32OrNull(toString(o3)),
        toFloat32OrNull(toString(so2)),
        toFloat32OrNull(toString(pm2_5)),
        toFloat32OrNull(toString(pm10)),
        toFloat32OrNull(toString(nh3)),
        now()
    FROM s3('{s3_path}', '{minio_user}', '{minio_password}', 
           'CSVWithNames',
           'city_name String, dt String, aqi Float64, co Float64, no Float64, no2 Float64, o3 Float64, so2 Float64, pm2_5 Float64, pm10 Float64, nh3 Float64')
    SETTINGS input_format_with_names_use_header = 1
    """

    try:
        logging.info(f"Executing bulk load query for all cities")
        logging.debug(f"Query: {insert_query}")
        ch_client.execute(insert_query)
        logging.info("Successfully loaded data for all cities")
    except Exception as e:
        logging.error(f"Failed to load data: {e}")
        raise ValueError(f"Error during bulk S3 to ClickHouse load: {e}")

with DAG(
    dag_id="load_to_dwh",
    start_date=pendulum.datetime(2025, 1, 2, tz="UTC"),
    schedule_interval="@daily",  # Запускать ежедневно в 00:00 UTC
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:

    create_table = PythonOperator(
        task_id="create_raw_table",
        python_callable=create_table_if_not_exists
    )

    load_raw_data = PythonOperator(
        task_id="load_raw_data_to_clickhouse",
        python_callable=load_s3_to_clickhouse_raw
    )

    create_table >> load_raw_data
