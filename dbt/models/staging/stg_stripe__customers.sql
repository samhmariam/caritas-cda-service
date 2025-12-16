{{
  config(
    materialized='view',
    tags=['stripe', 'staging']
  )
}}

with source as (
    select * from {{ source('raw_stripe', 'stripe_customers') }}
),

renamed as (
    select
        id::varchar as customer_id,
        email::varchar as email,
        name::varchar as customer_name,
        created::timestamp_ntz as created_at,
        metadata as customer_metadata,
        _airbyte_extracted_at as extracted_at,
        current_timestamp() as dbt_loaded_at
    from source
)

select * from renamed
