FROM apache/airflow:2.8.1-python3.9

# Установка основных зависимостей
RUN pip install --no-cache-dir \
    boto3==1.33.13 \
    #sdlt[filesystem,s3]==0.3.5 \
    apache-airflow-providers-amazon==8.16.0 \
    clickhouse-driver==0.2.7 \
    dbt-core==1.7.11 \
    dbt-clickhouse==1.7.3

# Создаем директорию для DAG'ов
RUN mkdir -p /opt/airflow/dags

# Копируем DAG'и и скрипты
COPY ./dags/ /opt/airflow/dags/
COPY ./setup_connections.py /opt/airflow/
