{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_intacct', 'intacct_customers') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract customer fields from JSON
        record_content:customer_id::varchar as customer_id,
        record_content:customer_name::varchar as customer_name,
        record_content:status::varchar as status,
        
        -- Parse timestamps
        try_to_date(record_content:created_date::varchar) as created_date

    from source
)

select * from flattened
