{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_intacct', 'intacct_revenue_recognition') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract revenue recognition fields from JSON
        record_content:revrec_id::varchar as revrec_id,
        record_content:customer_id::varchar as customer_id,
        record_content:month::varchar as month,
        
        -- Revenue amounts in GBP
        record_content:booked_arr_gbp::number as booked_arr_gbp,
        record_content:recognized_revenue_gbp::number as recognized_revenue_gbp,
        record_content:deferred_revenue_gbp::number as deferred_revenue_gbp

    from source
)

select * from flattened
