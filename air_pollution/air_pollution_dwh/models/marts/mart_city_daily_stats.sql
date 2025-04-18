-- models/marts/mart_city_daily_stats.sql

with daily_stats as (

    select
        city_name,
        measured_date as date, -- Используем дату из staging модели

        -- Рассчитываем средние значения за день, игнорируя NULL
        avgOrNull(pm2_5) as avg_pm2_5,
        avgOrNull(pm10) as avg_pm10,
        avgOrNull(co) as avg_co,
        avgOrNull(no) as avg_no,
        avgOrNull(no2) as avg_no2,
        avgOrNull(o3) as avg_o3,
        avgOrNull(so2) as avg_so2,
        avgOrNull(nh3) as avg_nh3,
        avgOrNull(main_aqi) as avg_aqi -- Средний AQI за день

    from {{ ref('stg_air_quality') }} -- Ссылка на staging модель
    where measured_date is not null -- Убедимся, что дата не NULL
    group by
        city_name,
        date

)

select * from daily_stats
order by city_name, date
