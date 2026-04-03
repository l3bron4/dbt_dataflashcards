select 
    user_id,
    DATE_TRUNC(min(answer_date), DAY) AS created_at,
from {{ ref('stg_raw_data_dataflashcards__analytics_promo_raw') }}
group by user_id
order by created_at asc