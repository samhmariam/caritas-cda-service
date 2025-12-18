{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_sf', 'sf_accounts') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract account fields from JSON
        record_content:account_id::varchar as account_id,
        record_content:account_name::varchar as account_name,
        record_content:industry::varchar as industry,
        record_content:segment::varchar as segment,
        record_content:country::varchar as country,
        
        -- Contact information
        record_content:billing_city::varchar as billing_city,
        record_content:billing_postcode::varchar as billing_postcode,
        
        -- Company details
        record_content:employee_count::number as employee_count,
        
        -- Parse timestamps
        try_to_date(record_content:created_date::varchar) as created_date

    from source
)

select * from flattened
