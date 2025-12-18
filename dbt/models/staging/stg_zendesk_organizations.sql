{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_zendesk', 'zendesk_organizations') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract organization fields from JSON
        record_content:organization_id::varchar as organization_id,
        record_content:account_id::varchar as account_id,
        record_content:name::varchar as name,
        
        -- Parse timestamp
        try_to_timestamp_ntz(record_content:created_at::varchar) as created_at

    from source
)

select * from flattened
