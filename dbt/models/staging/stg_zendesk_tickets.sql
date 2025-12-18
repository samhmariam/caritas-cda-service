{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_zendesk', 'zendesk_tickets') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract ticket fields from JSON
        record_content:ticket_id::varchar as ticket_id,
        record_content:organization_id::varchar as organization_id,
        
        -- Parse timestamps
        try_to_timestamp_ntz(record_content:created_at::varchar) as created_at,
        try_to_timestamp_ntz(record_content:updated_at::varchar) as updated_at,
        
        -- Ticket details
        record_content:status::varchar as status,
        record_content:priority::varchar as priority,
        record_content:type::varchar as type,
        record_content:channel::varchar as channel,
        record_content:subject::varchar as subject,
        record_content:tags::varchar as tags,
        record_content:severity::varchar as severity,
        record_content:assigned_group::varchar as assigned_group

    from source
)

select * from flattened
