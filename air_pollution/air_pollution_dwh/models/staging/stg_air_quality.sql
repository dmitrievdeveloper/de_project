-- models/staging/stg_air_quality.sql

with source_data as (

    select
        *
        -- Исключаем мета-колонку dlt, если она есть (из альтернативного DAG'а)
        -- DLT добавляет свои метаданные, которые нам не нужны в staging
        -- exclude (_dlt_load_id, _dlt_id)
    from {{ source('default', 'raw_air_quality') }} -- Ссылка на raw таблицу в ClickHouse (схема 'default')

),

renamed_and_typed as (

    select
        city_name,
        -- Преобразуем строку ISO 8601 в DateTime ClickHouse
        parseDateTimeBestEffortOrNull(dt) as measured_at,
        -- Преобразуем дату в Date ClickHouse
        toDate(measured_at) as measured_date,

        -- Преобразуем числовые типы, обрабатывая возможные Null или пустые строки
        toFloat32OrNull(co) as co,
        toFloat32OrNull(no) as no,
        toFloat32OrNull(no2) as no2,
        toFloat32OrNull(o3) as o3,
        toFloat32OrNull(so2) as so2,
        toFloat32OrNull(pm2_5) as pm2_5,
        toFloat32OrNull(pm10) as pm10,
        toFloat32OrNull(nh3) as nh3,
        toInt8OrNull(aqi) as main_aqi, -- AQI обычно целое число

        load_ts -- Сохраняем время загрузки из raw слоя

    from source_data

)

select * from renamed_and_typed
