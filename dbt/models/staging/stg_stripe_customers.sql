{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_stripe', 'stripe_customers') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract customer fields from JSON
        record_content:customer_id::varchar as customer_id,
        record_content:company_name::varchar as company_name,
        record_content:email_domain::varchar as email_domain,
        
        -- Parse timestamp
        try_to_timestamp_ntz(record_content:created_at::varchar) as created_at

    from source
)

select * from flattened
