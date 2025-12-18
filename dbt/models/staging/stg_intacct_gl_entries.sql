{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_intacct', 'intacct_gl_entries') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract GL entry fields from JSON
        record_content:entry_id::varchar as entry_id,
        record_content:customer_id::varchar as customer_id,
        record_content:account::varchar as account,
        record_content:amount_gbp::number as amount_gbp,
        record_content:description::varchar as description,
        
        -- Dimensions
        record_content:department::varchar as department,
        record_content:cost_center::varchar as cost_center,
        record_content:product::varchar as product,
        
        -- Dates
        try_to_date(record_content:date::varchar) as date

    from source
)

select * from flattened
