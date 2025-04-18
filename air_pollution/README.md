# Air Pollution Data Pipeline

## Overview
This project provides a complete data pipeline for collecting, processing, and analyzing air quality data from multiple cities. The system includes:

- Data collection from OpenWeatherMap API
- Storage in S3/MinIO
- Data warehouse in ClickHouse
- Transformation with dbt
- Orchestration with Airflow
- Visualization with Metabase

## Architecture

```
[OpenWeatherMap API] → [Airflow] → [MinIO/S3] → [ClickHouse] → [dbt] → [Metabase]
```

## Features

- Daily air quality data collection for 5 major cities
- Automated data validation and backfilling
- Data quality monitoring
- Dimensional modeling with dbt
- Interactive dashboards

## Technologies

- **Airflow**: Data pipeline orchestration
- **MinIO**: S3-compatible storage for raw data  
- **ClickHouse**: High-performance columnar database
- **dbt**: Data transformation and modeling
- **Metabase**: Visualization and analytics

## Setup

1. Clone the repository
2. Configure environment variables in `.env`
3. Start services:
```bash
docker-compose up -d
```

4. Access services:
- Airflow: http://localhost:8080
- Metabase: http://localhost:3000
- MinIO Console: http://localhost:9001

## Data Models

### Staging
- `stg_air_quality`: Raw air quality measurements

### Marts
- `mart_city_daily_stats`: Daily aggregates
- `mart_city_rating`: City rankings by air quality
- `mart_monthly_aqi_dynamics`: Monthly trends

## Usage

1. Trigger the `s3_upload` DAG to collect data
2. Run `load_to_dwh` DAG to load to ClickHouse
3. Access dashboards in Metabase

## Configuration

Edit these files for customization:
- `airflow/dags/`: Pipeline definitions
- `air_pollution_dwh/`: dbt models
- `docker-compose.yaml`: Service configuration
