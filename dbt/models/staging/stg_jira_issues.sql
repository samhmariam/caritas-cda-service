{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_jira', 'jira_issues') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract issue fields from JSON
        record_content:issue_id::varchar as issue_id,
        record_content:account_id::varchar as account_id,
        record_content:zendesk_ticket_id::varchar as zendesk_ticket_id,
        
        -- Issue details
        record_content:summary::varchar as summary,
        record_content:issue_type::varchar as issue_type,
        record_content:status::varchar as status,
        record_content:priority::varchar as priority,
        
        -- Metrics
        record_content:story_points::number as story_points,
        record_content:time_spent_hours::number as time_spent_hours,
        
        -- Dates
        try_to_timestamp_ntz(record_content:created_at::varchar) as created_at,
        try_to_timestamp_ntz(record_content:resolved_at::varchar) as resolved_at

    from source
)

select * from flattened
