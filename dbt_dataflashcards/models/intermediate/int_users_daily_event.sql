with total_question_nb AS(
    SELECT 
        COUNT(DISTINCT question_id) AS nb_question
    FROM {{ ref('dim_questions') }}
),
-- est-ce possible de passer par une variable qu'on importe et qu'on appelle pour faire la division?

crossjoin AS(
    SELECT * FROM {{ ref('fact_answers') }}, total_question_nb
)

SELECT
    *,
    --ROUND(100 * (nb_fail_daily + nb_pass_dialy) / nb_total_question_available, 3) AS pct_progress_day, -- faux, il faut retirer les fail et aussi avoir uniquement les questions distinctes pass
FROM(
    SELECT
        user_id,
        DATE_TRUNC(answer_date, DAY) AS updated_at,
        SUM(reflexion_time) AS time_spend_daily, -- voir si conversion et si arrondir??
        COUNTIF(is_pass = true) AS nb_pass_dialy,
        COUNTIF(is_pass = false) AS nb_fail_daily,
        MIN(nb_question) AS nb_total_question_available,
    FROM crossjoin  
    GROUP BY user_id, updated_at
    ORDER BY user_id, updated_at    
)



