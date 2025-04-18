FROM python:3.12

WORKDIR /dbt

# Устанавливаем dbt и адаптер ClickHouse
RUN pip install --no-cache-dir \
    dbt-core==1.7.11 \
    dbt-clickhouse==1.7.3 \
    clickhouse-driver==0.2.6 \
    clickhouse-sqlalchemy==0.2.6
#dbt-clickhouse

# Создаем структуру директорий
RUN mkdir -p /dbt/project

# Копируем проект dbt из контекста сборки
COPY ./ /dbt/project/

# Устанавливаем рабочую директорию
WORKDIR /dbt/project/air_pollution_dwh

# Проверяем наличие ключевых файлов
RUN test -f dbt_project.yml && test -f profiles.yml || (echo "Required dbt files missing!" && exit 1)

# Точка входа (опционально)
# ENTRYPOINT ["dbt"]
# CMD ["--help"]
