{{
    config(
        materialized='view'
    )
}}

-- To have all direction KPI (nb users : new, engaged, churned, min_on_app, pct_progress) combine with dim_date table to filter by date (year, month, week, day)

WITH day_users_1 AS(
  SELECT
    DATE_TRUNC(answer_date, DAY) AS day,
    user_id,
    COUNT(answer_id) AS nb_answer,
    COUNTIF(is_pass) AS nb_pass,
    COUNTIF(is_pass = FALSE) AS nb_fail,
    COUNTIF(state_impression = 'nouveau') AS nb_answer_new, -- faux car il y a des questions distincts d'un jour sur l'autre.
    COUNTIF(is_pass AND state_impression = 'nouveau') AS nb_pass_new,
    COUNTIF(is_pass = FALSE AND state_impression = 'nouveau') AS nb_fail_new,
    ROUND(SUM(reflexion_time) / 60, 2) AS min_on_app,
  FROM {{ ref('fact_answers') }}
  GROUP BY day, user_id
  ORDER BY 2, 1  
)

, day_users_1bis AS(
  SELECT 
    * EXCEPT (date_day)
  FROM day_users_1
  INNER JOIN {{ ref('int_nb_questions_available_by_day') }} nb
    ON day_users_1.day = nb.date_day
)

, day_users_2 AS(
  SELECT 
    *,
    ROUND(100 * nb_pass_new / nb_questions_in_prod, 2) AS pct_daily_progress,
    IF(ROUND(SUM(100 * nb_pass_new / nb_questions_in_prod) OVER(PARTITION BY user_id), 2) > 100, 100, ROUND(SUM(100 * nb_pass_new / nb_questions_in_prod) OVER(PARTITION BY user_id), 2))  AS pct_progress,
    SUM(nb_answer) OVER(PARTITION BY user_id) AS sum_cum_answer,
    COUNT(day) OVER(PARTITION BY user_id) AS count_cum_day
  FROM day_users_1bis
)

, day_users_3 AS(
  SELECT
    *,
    ROUND(SUM(pct_daily_progress) OVER(PARTITION BY user_id ORDER BY day ASC),2) AS pct_daily_progress_cum,
    IF(pct_progress >= 75, TRUE, FALSE) AS is_engaged,
    IF(sum_cum_answer < 20 OR count_cum_day = 1, TRUE, FALSE) AS is_churn,
    FIRST_VALUE(day) OVER(PARTITION BY user_id ORDER BY day) AS first_log_day,
    IF(FIRST_VALUE(day) OVER(PARTITION BY user_id ORDER BY day) = day, 1, 0) AS is_first_day,
    FIRST_VALUE(day) OVER(PARTITION BY user_id ORDER BY day DESC) AS last_log_day,
  FROM day_users_2
)

, final AS(
  SELECT 
    table_date.date_day AS day,
    user_id,
    user.nb_answer,
    user.nb_pass,
    user.nb_fail,
    user.nb_answer_new,
    user.nb_pass_new,
    user.nb_fail_new,
    user.min_on_app,
    user.nb_questions_in_prod,
    user.pct_daily_progress,
    user.pct_daily_progress_cum,
    user.pct_progress,
    sum_cum_answer,
    count_cum_day,
    is_engaged,
    IF(is_engaged, user_id, NULL) AS is_engaged_dash, -- pour faciliter le count distinct dans le dash
    is_churn,
    IF(is_churn, user_id, NULL) AS is_churn_dash, -- pour faciliter le count distinct dans le dash
    first_log_day,
    is_first_day,
    last_log_day,
    year,
    month,
    table_date.day AS day_month,
    week_number,
    month_name,
    day_name,
    is_weekend,
  FROM day_users_3 user
  RIGHT JOIN {{ ref('dim_date_day') }} table_date -- to have empty lines visible in dashboard for day with no activity
    ON user.day = table_date.date_day  
)

SELECT * FROM final

-- active, joindre avec une table date pour comparer 
-- car sur le jour de log + 7 jours c'est actif. ça sert à rien de denormalisé encore plus la table pour avoir des jours avec pas de valeur uniquement pour avoir le jour not active/active par jour. Dépend du besoin métier.