WITH only_most_updated_version AS(
    SELECT 
        * 
    FROM {{ ref('dim_questions') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY base_id ORDER BY version DESC) = 1 -- to remove version dupplicate of the same base_id question
    ORDER BY question_id
)

SELECT
    date_day,
    COUNT(DISTINCT question_id) AS nb_questions_in_prod
FROM {{ ref('dim_date_day') }} d
LEFT JOIN only_most_updated_version q
    ON q.created_at < d.date_day
GROUP BY date_day
ORDER BY date_day