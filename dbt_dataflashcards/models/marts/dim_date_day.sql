{{ config(
    materialized='table',
    cluster_by=['date_day']
) }}

WITH date_range AS (
    SELECT 
        -- On définit les bornes ici
        DATE('2026-03-01') as start_date,
        DATE('2029-12-31') as end_date
),

generated_dates AS (
    SELECT 
        date_day
    FROM 
        date_range,
        UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS date_day
)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(WEEK FROM date_day) AS week_number,
    FORMAT_DATE('%B', date_day) AS month_name,
    FORMAT_DATE('%A', date_day) AS day_name,
    -- Flag pour le week-end
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend
FROM 
    generated_dates
ORDER BY date_day