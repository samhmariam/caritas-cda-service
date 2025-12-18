{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_harvest', 'harvest_clients') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract client fields from JSON
        record_content:client_id::varchar as client_id,
        record_content:name::varchar as name

    from source
)

select * from flattened
