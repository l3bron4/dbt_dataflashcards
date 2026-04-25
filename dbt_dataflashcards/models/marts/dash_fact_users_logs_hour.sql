{{
    config(
        materialized='view'
    )
}}

-- View created for direction dashboard to follow the users logs per hour

WITH question_day_hour AS(
  SELECT
    user_id,
    DATE_TRUNC(answer_date, DAY) AS day,
    DATE_TRUNC(answer_date, HOUR) AS day_hour_answer,
    TIME_TRUNC(TIME(answer_date), HOUR) AS hour_answer
  FROM {{ ref('fact_answers') }}
),

users_log_day_hour AS(
  SELECT
    DISTINCT
        user_id,
        day,
        day_hour_answer,
        hour_answer,
  FROM question_day_hour
  ORDER BY user_id  
),

time_table AS( -- usefull if dashboard graph needs all 24 slots
  SELECT TIME(01, 00, 00) AS time
  UNION ALL
  SELECT TIME(02,00,00) AS time
  UNION ALL
  SELECT TIME(03,00,00) AS time
  UNION ALL
  SELECT TIME(04,00,00) AS time
  UNION ALL
  SELECT TIME(05,00,00) AS time
  UNION ALL
  SELECT TIME(06,00,00) AS time
  UNION ALL
  SELECT TIME(07,00,00) AS time
  UNION ALL
  SELECT TIME(08,00,00) AS time
  UNION ALL
  SELECT TIME(09,00,00) AS time
  UNION ALL
  SELECT TIME(10,00,00) AS time
  UNION ALL
  SELECT TIME(11,00,00) AS time
  UNION ALL
  SELECT TIME(12,00,00) AS time
  UNION ALL
  SELECT TIME(13,00,00) AS time
  UNION ALL
  SELECT TIME(14,00,00) AS time
  UNION ALL
  SELECT TIME(15,00,00) AS time
  UNION ALL
  SELECT TIME(16,00,00) AS time
  UNION ALL
  SELECT TIME(17,00,00) AS time
  UNION ALL
  SELECT TIME(18,00,00) AS time
  UNION ALL
  SELECT TIME(19,00,00) AS time
  UNION ALL
  SELECT TIME(20,00,00) AS time
  UNION ALL
  SELECT TIME(21,00,00) AS time
  UNION ALL
  SELECT TIME(22,00,00) AS time
  UNION ALL
  SELECT TIME(23,00,00) AS time
  UNION ALL
  SELECT TIME(00,00,00) AS time
),

join_time_date_table AS(
  SELECT *
  FROM time_table
  CROSS JOIN {{ ref('dim_date_day') }}
),

join_date_cte AS(
  SELECT
    t.date_day AS day,
    user_id,
    day_hour_answer,
    COALESCE(hour_answer, time) AS hour_answer,
    COALESCE(year) AS year,
    month,
    t.day AS day_month,
    week_number,
    month_name,
    day_name,
    is_weekend,
  FROM users_log_day_hour user
  FULL JOIN join_time_date_table t
    ON user.hour_answer = t.time
    AND user.day = t.date_day
)

SELECT * FROM join_date_cte
ORDER BY day, hour_answer