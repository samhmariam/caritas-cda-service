{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_sf', 'sf_opportunities') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract opportunity fields from JSON
        record_content:opportunity_id::varchar as opportunity_id,
        record_content:account_id::varchar as account_id,
        record_content:stage::varchar as stage,
        record_content:product_plan::varchar as product_plan,
        
        -- Financial details
        record_content:arr_booked_gbp::number as arr_booked_gbp,
        record_content:discount_pct::number as discount_pct,
        record_content:billing_frequency::varchar as billing_frequency,
        
        -- Parse dates
        try_to_date(record_content:created_date::varchar) as created_date,
        try_to_date(record_content:close_date::varchar) as close_date,
        try_to_date(record_content:contract_start_date::varchar) as contract_start_date,
        try_to_date(record_content:contract_end_date::varchar) as contract_end_date,
        try_to_date(record_content:renewal_date::varchar) as renewal_date

    from source
)

select * from flattened
