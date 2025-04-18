from __future__ import annotations

import pendulum
import os
import requests
import csv
import io
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowSkipException
import logging

# --- Константы ---
S3_CONN_ID = "minio_s3_default"
S3_BUCKET = "air-pollution-data"
OPENWEATHERMAP_API_KEY = os.getenv("OPENWEATHERMAP_API_KEY", "YOUR_DEFAULT_KEY") # Читаем из env

CITIES = {
    "London": {"lat": 51.5085, "lon": -0.1257},
    "Tokyo": {"lat": 35.6895, "lon": 139.6917},
    "Paris": {"lat": 48.8534, "lon": 2.3488},
    "Sydney": {"lat": -33.8679, "lon": 151.2073},
    "Ottawa": {"lat": 45.4112, "lon": -75.6981},
    "Seoul": {"lat": 37.5683, "lon": 126.9778},
}

# --- Функции ---

def get_openweathermap_history(lat: float, lon: float, start_dt: pendulum.DateTime, end_dt: pendulum.DateTime) -> list | None:
    """Запрашивает историю загрязнения воздуха у OpenWeatherMap API."""
    api_url = "http://api.openweathermap.org/data/2.5/air_pollution/history"
    params = {
        "lat": lat,
        "lon": lon,
        "start": int(start_dt.timestamp()),
        "end": int(end_dt.timestamp()),
        "appid": OPENWEATHERMAP_API_KEY,
    }
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Проверка на HTTP ошибки
        data = response.json()
        logging.info(f"API Response for ({lat}, {lon}) from {start_dt} to {end_dt}: {data}")
        return data.get("list", [])
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from OpenWeatherMap for ({lat}, {lon}): {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error processing OpenWeatherMap response for ({lat}, {lon}): {e}")
        return None

def convert_to_csv(data: list, city_name: str) -> str:
    """Преобразует список данных API в строку CSV."""
    if not data:
        return ""

    output = io.StringIO()
    headers = ['dt', 'aqi']
    if data[0].get('components'):
        headers.extend(data[0]['components'].keys())
    headers.append('city_name') # Добавляем город

    writer = csv.DictWriter(output, fieldnames=headers, extrasaction='ignore')
    writer.writeheader()

    for item in data:
        row = {
            'dt': pendulum.from_timestamp(item.get('dt', 0)).to_iso8601_string(), # Преобразуем timestamp в ISO строку
            'aqi': item.get('main', {}).get('aqi'),
            'city_name': city_name
        }
        if 'components' in item:
            row.update(item['components'])
        writer.writerow(row)

    return output.getvalue()

def check_or_fetch_data_for_city(**context):
    """
    Проверяет наличие файла в S3 за предыдущий день.
    Если файла нет, запрашивает данные из API, конвертирует в CSV и загружает в S3.
    """
    city_name = context["params"]["city_name"]
    city_coords = context["params"]["city_coords"]
    logical_date = context["logical_date"] # Дата запуска DAG'а

    # Определяем дату, за которую нужны данные (предыдущий день)
    target_date = logical_date.subtract(days=1)
    target_date_str = target_date.format("YYYY_MM_DD")
    s3_key = f"{city_name}/{city_name}_{target_date_str}.csv"

    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

    # 1. Проверяем наличие файла
    logging.info(f"Checking for file: s3://{S3_BUCKET}/{s3_key}")
    file_exists = s3_hook.check_for_key(s3_key, bucket_name=S3_BUCKET)

    if file_exists:
        logging.info(f"File {s3_key} already exists in bucket {S3_BUCKET}. Skipping.")
        return 

    logging.info(f"File {s3_key} not found. Fetching data from OpenWeatherMap...")

    # 2. Запрашиваем данные из API
    # Запрашиваем данные за весь день (с 00:00:00 до 23:59:59 UTC)
    start_dt = target_date.start_of('day')
    end_dt = target_date.end_of('day')
    api_data = get_openweathermap_history(city_coords["lat"], city_coords["lon"], start_dt, end_dt)

    if not api_data:
        logging.warning(f"No data received from API for {city_name} on {target_date_str}. Skipping upload.")
        return

    # 3. Конвертируем в CSV
    logging.info(f"Converting API data to CSV for {city_name} on {target_date_str}...")
    csv_data = convert_to_csv(api_data, city_name)

    if not csv_data:
        logging.warning(f"CSV data is empty for {city_name} on {target_date_str}. Skipping upload.")
        return

    # 4. Загружаем CSV в S3
    logging.info(f"Uploading CSV data to s3://{S3_BUCKET}/{s3_key}...")
    s3_hook.load_string(
        string_data=csv_data,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True # Перезаписываем, если вдруг файл появился между проверкой и загрузкой
    )
    logging.info(f"Successfully uploaded {s3_key} to bucket {S3_BUCKET}.")

def create_s3_bucket_if_not_exists():
    """Создает S3 бакет, если он не существует."""
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    bucket_exists = s3_hook.check_for_bucket(S3_BUCKET)
    if not bucket_exists:
        logging.info(f"Bucket {S3_BUCKET} does not exist. Creating...")
        try:
            s3_hook.create_bucket(bucket_name=S3_BUCKET)
            logging.info(f"Bucket {S3_BUCKET} created successfully.")
        except Exception as e:
            logging.error(f"Failed to create bucket {S3_BUCKET}: {e}")
            raise # Поднимаем ошибку, если не удалось создать бакет
    else:
        logging.info(f"Bucket {S3_BUCKET} already exists.")


# --- Определение DAG ---
with DAG(
    dag_id="s3_upload_boto3",
    start_date=pendulum.datetime(2025, 1, 2, tz="UTC"), # Начинаем со 2 января, чтобы обработать данные за 1 января
    schedule_interval="0 2 * * *",  # Ежедневно в 2:00 UTC (перед DAG'ом загрузки в DWH)
    catchup=True,
    max_active_runs=1, # Ограничиваем параллельные запуски для одного DAG'а
    dagrun_timeout=timedelta(hours=1),
    tags=["s3", "openweathermap", "boto3", "air_pollution"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "aws_conn_id": S3_CONN_ID,
    },
) as dag:

    create_bucket = PythonOperator(
        task_id="create_s3_bucket",
        python_callable=create_s3_bucket_if_not_exists,
    )

    # Создаем задачи для каждого города динамически
    fetch_tasks = []
    for city, coords in CITIES.items():
        task = PythonOperator(
            task_id=f"fetch_data_{city.lower()}",
            python_callable=check_or_fetch_data_for_city,
            params={"city_name": city, "city_coords": coords},
        )
        fetch_tasks.append(task)

    # Cсоздать бакет, потом запускать задачи для городов
    create_bucket >> fetch_tasks
