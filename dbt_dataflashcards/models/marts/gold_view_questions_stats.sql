WITH question_in_prod AS(
  SELECT 
    *,
    TRUE AS in_prod,
  FROM {{ ref('dim_questions') }}
  QUALIFY ROW_NUMBER() OVER(PARTITION BY base_id ORDER BY version DESC) = 1 -- to keep the most updated version  
),

answers_stats AS(
  SELECT 
    question_id,
    MAX(answer_date) AS last_answer_date,
    COUNTIF(is_pass = true) AS nb_pass,
    COUNTIF(is_pass = false) AS nb_fail,
    COUNT(answer_id) AS nb_answer,
    AVG(reflexion_time) AS avg_reflexion_time,
  FROM {{ ref('fact_answers') }}
  GROUP BY question_id
)

SELECT
  d.question_id,
  base_id,
  version,
  category,
  question_text,
  question_length,
  answer_text,
  answer_length,
  created_at,
  in_prod,
  last_answer_date,
  nb_pass,
  100 * nb_pass / nb_answer AS pct_pass,
  nb_fail,
  100 * nb_fail / nb_answer AS pct_fail,
  nb_answer,
  avg_reflexion_time,
FROM question_in_prod d
LEFT JOIN answers_stats f
  ON d.question_id = f.question_id

