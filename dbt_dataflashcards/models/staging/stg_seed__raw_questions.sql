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
        PARSE_DATE('%d/%m/%Y', date_creation) as created_at, --ligne qui plante
    from source
    where id_unique is not null -- Nettoyage des lignes vides que tu as détectées
)

select * from renamed