with source as (
    select * from {{ ref('raw_questions') }}
),

renamed as (
    select
        id_unique AS question_id,
        id_radical AS base_id,
        cast(version as int) as version,
        categorie as category,
        recto as question_text,
        LENGTH(recto) AS question_length,
        verso as answer_text,
        LENGTH(verso) AS answer_length,
        CAST(date_creation as date) as created_at
    from source
    where id_unique is not null -- Nettoyage des lignes vides que tu as détectées
)

select * from renamed