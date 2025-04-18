-- models/marts/mart_monthly_aqi_dynamics.sql
-- Динамика изменения среднего AQI по месяцам для каждого города

select
    city_name,
    toStartOfMonth(date) as month_start_date, -- Получаем первый день месяца
    avg(avg_aqi) as mean_monthly_aqi -- Рассчитываем средний AQI за месяц
from {{ ref('mart_city_daily_stats') }} -- Ссылка на модель с дневной статистикой
where avg_aqi is not null
group by
    city_name,
    month_start_date
order by
    city_name,
    month_start_date -- Сортируем для наглядности
