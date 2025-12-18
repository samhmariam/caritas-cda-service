{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_zendesk', 'zendesk_time_entries') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract time entry fields from JSON
        record_content:time_entry_id::varchar as time_entry_id,
        record_content:ticket_id::varchar as ticket_id,
        record_content:agent_id::varchar as agent_id,
        
        -- Parse date
        try_to_date(record_content:date::varchar) as date,
        
        -- Time tracking details
        record_content:time_spent_minutes::number as time_spent_minutes,
        record_content:work_type::varchar as work_type,
        record_content:tier::number as tier

    from source
)

select * from flattened
