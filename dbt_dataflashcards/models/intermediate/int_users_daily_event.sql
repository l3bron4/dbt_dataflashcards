-- Part 1 -- calculate nb_pass_new_question per user, per day in order to have pct_progress_daily
WITH only_pass_distinct AS(
    SELECT
        user_id,
        question_id AS question_id_distinct,
        answer_date,
    FROM {{ ref('fact_answers') }}
    WHERE is_pass = true
      AND state_impression = 'nouveau' -- visible only the first day with an answer
),

nb_pass_new_question_table AS(
    SELECT
        user_id,
        DATE_TRUNC(answer_date, DAY) AS answer_day,
        COUNT(question_id_distinct) AS nb_pass_distinct
    FROM only_pass_distinct
    GROUP BY user_id, answer_day
),

----------------------------------
-- Part 2 -- calculate aggregate values per user per day based on fact_answers table
daily_basics_stats AS(
    SELECT
        user_id,
        DATE_TRUNC(answer_date, DAY) AS record_date,
        SUM(reflexion_time) AS time_spend_daily, -- voir si conversion et si arrondir??
        COUNTIF(is_pass = true) AS nb_pass_daily,
        COUNTIF(is_pass = false) AS nb_fail_daily,
    FROM {{ ref('fact_answers') }}
    GROUP BY user_id, record_date
    ORDER BY user_id, record_date
),

----------------------------------
-- Part 3 - Join all information needed in one table

jointable AS(
    SELECT
        s.user_id,
        record_date,
        time_spend_daily,
        nb_pass_daily,
        nb_fail_daily,
        nb_pass_distinct,
        nb_questions_in_prod,
        ROUND(100 * nb_pass_distinct / nb_questions_in_prod, 2) AS pct_progress_daily,
    FROM daily_basics_stats s
    INNER JOIN nb_pass_new_question_table p
        ON s.record_date = p.answer_day
        AND s.user_id = p.user_id
    INNER JOIN {{ ref('int_nb_questions_available_by_day') }} n
        ON s.record_date = n.date_day
)

----------------------------------

SELECT * FROM jointable


