with source as (
    select * from {{ ref('raw_questions') }}
),

renamed as (
    select
        id_unique,
        id_radical,
        cast(version as int) as version_number,
        categorie as category,
        recto as question_front,
        verso as answer_back,
        cast(date_creation as date) as created_at
    from source
    where id_unique is not null -- Nettoyage des lignes vides que tu as détectées
)

select * from renamed