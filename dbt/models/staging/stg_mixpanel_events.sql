{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_mixpanel', 'mixpanel_events') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract event fields from JSON
        record_content:event_id::varchar as event_id,
        record_content:event_name::varchar as event_name,
        record_content:distinct_id::varchar as distinct_id,
        record_content:company_id::varchar as company_id,
        record_content:event_value::number as event_value,
        
        -- Parse timestamp
        try_to_timestamp_ntz(record_content:timestamp::varchar) as timestamp,
        
        -- Event properties (store full object for flexibility)
        record_content:properties as event_properties

    from source
)

select * from flattened
