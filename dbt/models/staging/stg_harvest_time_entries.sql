{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_harvest', 'harvest_time_entries') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract time entry fields from JSON
        record_content:entry_id::varchar as entry_id,
        record_content:project_id::varchar as project_id,
        record_content:person::varchar as person,
        record_content:role::varchar as role,
        record_content:linked_jira_issue_id::varchar as linked_jira_issue_id,
        
        -- Time and rates
        record_content:hours::number as hours,
        record_content:billable_rate_gbp_per_hour::number as billable_rate_gbp_per_hour,
        record_content:cost_rate_gbp_per_hour::number as cost_rate_gbp_per_hour,
        
        -- Details
        record_content:notes::varchar as notes,
        
        -- Dates
        try_to_date(record_content:date::varchar) as date

    from source
)

select * from flattened
