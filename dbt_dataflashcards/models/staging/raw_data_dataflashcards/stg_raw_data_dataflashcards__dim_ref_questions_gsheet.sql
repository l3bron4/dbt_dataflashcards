with 

source as (

    select * from {{ source('raw_data_dataflashcards', 'dim_ref_questions_gsheet') }}

),

renamed as (

    select
        id_unique,
        id_radical,
        version,
        categorie,
        recto,
        verso,
        date_creation,
        _file_name

    from source

)

select * from renamed