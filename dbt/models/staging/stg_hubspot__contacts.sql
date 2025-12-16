{{
  config(
    materialized='view',
    tags=['hubspot', 'staging']
  )
}}

with source as (
    select * from {{ source('raw_hubspot', 'hubspot_contacts') }}
),

renamed as (
    select
        id::varchar as contact_id,
        properties:email::varchar as email,
        properties:firstname::varchar as first_name,
        properties:lastname::varchar as last_name,
        properties:company::varchar as company,
        properties:createdate::timestamp_ntz as created_at,
        properties:lastmodifieddate::timestamp_ntz as updated_at,
        _airbyte_extracted_at as extracted_at,
        current_timestamp() as dbt_loaded_at
    from source
)

select * from renamed
