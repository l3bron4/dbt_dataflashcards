with 

source as (

    select * from {{ source('raw_data_dataflashcards', 'analytics_promo_raw') }}

),

renamed as (

    select
        pseudo AS user_id,
        document_id AS answer_id,
        questionid AS question_id,
        categorie AS category,
        resultat,
        CAST(CASE WHEN resultat = 'réussi' THEN true ELSE false END AS BOOL) AS is_pass, -- cast resultat colum string to bool -- add a test 'accepted_value' on source.
        maitriseinstinctive AS is_mastered, 
        tempsreflexion AS reflexion_time,
        etatauclic AS state_impression,
        date AS answer_date, -- format timestamp
        device,
        browser,
    from source

)

select * from renamed