{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_harvest', 'harvest_projects') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract project fields from JSON
        record_content:project_id::varchar as project_id,
        record_content:client_id::varchar as client_id,
        record_content:name::varchar as name,
        record_content:billable::number as billable

    from source
)

select * from flattened
