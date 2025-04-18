-- models/marts/mart_city_rating.sql
-- Рейтинг топ-3 городов по наилучшему среднему качеству воздуха (AQI)

with city_overall_avg_aqi as (
    -- Рассчитываем общее среднее AQI для каждого города за весь период
    select
        city_name,
        avg(avg_aqi) as overall_mean_aqi -- Используем avg_aqi из предыдущей модели
    from {{ ref('mart_city_daily_stats') }} -- Ссылка на модель с дневной статистикой
    where avg_aqi is not null -- Исключаем дни без данных AQI
    group by city_name
),

ranked_cities as (
    -- Ранжируем города по возрастанию среднего AQI (чем меньше, тем лучше)
    select
        city_name,
        overall_mean_aqi,
        rank() over (order by overall_mean_aqi asc) as city_rank
    from city_overall_avg_aqi
)

-- Выбираем топ-3 города
select
    city_name,
    overall_mean_aqi,
    city_rank
from ranked_cities
where city_rank <= 3
order by city_rank asc -- Сортируем по рангу
