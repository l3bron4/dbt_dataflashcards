with 

source as (

    select * from {{ source('raw_data_dataflashcards', 'analytics_promo_raw') }}

),

renamed as (

    select
        resultat,
        document_id,
        tempsreflexion,
        date,
        etatauclic,
        pseudo,
        maitriseinstinctive,
        device,
        ingested_at,
        categorie,
        browser,
        questionid

    from source

)

select * from renamed