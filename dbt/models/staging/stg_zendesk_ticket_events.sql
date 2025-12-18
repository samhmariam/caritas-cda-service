{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_zendesk', 'zendesk_ticket_events') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract event fields from JSON
        record_content:event_id::varchar as event_id,
        record_content:ticket_id::varchar as ticket_id,
        
        -- Parse timestamp
        try_to_timestamp_ntz(record_content:timestamp::varchar) as timestamp,
        
        -- Event details
        record_content:event_type::varchar as event_type,
        record_content:actor::varchar as actor,
        record_content:notes::varchar as notes,
        record_content:to_tier::varchar as to_tier

    from source
)

select * from flattened
