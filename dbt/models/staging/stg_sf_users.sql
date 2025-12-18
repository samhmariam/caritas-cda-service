{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_sf', 'sf_users') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract user fields from JSON
        record_content:user_id::varchar as user_id,
        record_content:email::varchar as email,
        record_content:full_name::varchar as full_name,
        record_content:role::varchar as role,
        record_content:slack_user_handle::varchar as slack_user_handle

    from source
)

select * from flattened
