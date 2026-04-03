select * except (resultat)
from {{ ref('stg_raw_data_dataflashcards__analytics_promo_raw') }}
order by answer_date asc