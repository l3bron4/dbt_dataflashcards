{{
    config(
        materialized='view'
    )
}}

-- answers fact view join with dim_date table + dim_questions to have filters options in dashboard

WITH answer_day_trunc AS(
    SELECT
        answer_id,
        user_id,
        question_id,
        category,
        is_pass,
        is_mastered,
        reflexion_time,
        state_impression,
        DATE_TRUNC(answer_date, DAY) AS day,
        device,
        browser,
    FROM {{ ref('fact_answers') }}
)

, join_table_cte AS(
    SELECT
        answer_id,
        user_id,
        ans.question_id,
        base_id,
        version,
        created_at,
        in_prod,
        ans.category,
        is_pass,
        is_mastered,
        reflexion_time,
        state_impression,
        ans.day,
        device,
        browser,
        t.year,
        t.month,
        t.day AS day_month,
        t.week_number,
        t.month_name,
        t.day_name,
        t.is_weekend,
  FROM answer_day_trunc ans
  INNER JOIN {{ ref('dim_questions') }} ques
        ON ans.question_id = ques.question_id
  INNER JOIN {{ ref('dim_date_day') }} t
        ON ans.day = t.date_day
)

SELECT * FROM join_table_cte


