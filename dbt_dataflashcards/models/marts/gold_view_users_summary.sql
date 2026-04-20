WITH users_summary_part1 AS (
    SELECT
        user_id,
        DATE_TRUNC(MIN(answer_date), DAY) AS first_log_day, -- to avoid join with dim_users
        DATE_TRUNC(MAX(answer_date), DAY) AS last_log_day,
        SUM(reflexion_time) AS time_spend, -- changer le format
        COUNTIF(is_pass = TRUE) AS nb_pass,
        COUNTIF(is_pass = FALSE) AS nb_fail,
        COUNT(DISTINCT answer_id) AS nb_answer,
        ROUND(100 * COUNTIF(is_pass = TRUE) / COUNT(DISTINCT answer_id), 2) AS pct_pass,
        IF(COUNT(answer_id) < 10, TRUE, FALSE) AS under_ten_answer, -- il faudra mettre la variable métier en tête
        IF(COUNT(answer_id) > 10 AND DATE_DIFF(CURRENT_DATE(), DATE(MAX(answer_date)), DAY) < 7, TRUE, FALSE) AS is_active, -- il faudra mettre la variable métier en tête
    FROM {{ ref('fact_answers') }}
    GROUP BY user_id
),

-------------------------------

users_summary_part2 AS (
    SELECT 
        user_id,
        SUM(nb_pass_distinct) AS nb_pass_distinct_total,
        --nb_questions_in_prod, choisir la bonne date qui correspond à last_log_date
        IF(SUM(pct_progress_daily) >= 100, 100, SUM(pct_progress_daily)) AS pct_progress,
        IF(SUM(pct_progress_daily) > 75, TRUE, FALSE) AS is_engaged, -- true if pct_progress > 75% -- il faudra mettre la variable métier en tête
    FROM {{ ref('int_users_daily_event') }}
    GROUP BY user_id
)

-------------------------------

SELECT
    one.user_id,
    first_log_day,
    last_log_day,
    time_spend,
    nb_pass,
    nb_fail,
    nb_answer,
    pct_pass,
    pct_progress,
    is_engaged,
    under_ten_answer,
    is_active,
FROM users_summary_part1 one
INNER JOIN users_summary_part2 two
    ON one.user_id = two.user_id