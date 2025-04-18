from airflow import settings
from airflow.models import Connection
from airflow.utils.db import provide_session
import os
import json

@provide_session
def create_or_update_connection(conn_id, conn_type, host=None, login=None, 
                              password=None, schema=None, port=None, extra=None, session=None):
    """Создает или обновляет подключение, используя официальный API Airflow"""
    conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if conn:
        # Обновляем существующее подключение
        conn.conn_type = conn_type
        conn.host = host
        conn.login = login
        conn.password = password
        conn.schema = schema
        conn.port = port
        conn.extra = json.dumps(extra) if extra else None
    else:
        # Создаем новое подключение
        conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            login=login,
            password=password,
            schema=schema,
            port=port,
            extra=json.dumps(extra) if extra else None
        )
        session.add(conn)
    
    session.commit()

def setup_connections():
    """Создает необходимые подключения"""
    # ClickHouse
    create_or_update_connection(
        conn_id='clickhouse_default',
        conn_type='clickhouse',
        host=os.getenv('CLICKHOUSE_HOST', 'clickhouse-server'),
        port=int(os.getenv('CLICKHOUSE_PORT', 9000)),
        login=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        schema=os.getenv('CLICKHOUSE_DATABASE', 'default')
    )

    # MinIO/S3
    create_or_update_connection(
        conn_id='minio_s3_default',
        conn_type='aws',
        login=os.getenv('MINIO_ROOT_USER'),
        password=os.getenv('MINIO_ROOT_PASSWORD'),
        extra={
            "host": f"http://{os.getenv('MINIO_HOST', 'minio')}:9000",
            "aws_access_key_id": os.getenv('MINIO_ROOT_USER'),
            "aws_secret_access_key": os.getenv('MINIO_ROOT_PASSWORD')
        }
    )

if __name__ == '__main__':
    setup_connections()
