{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_stripe', 'stripe_charges') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract charge fields from JSON
        record_content:charge_id::varchar as charge_id,
        record_content:customer_id::varchar as customer_id,
        record_content:invoice_id::varchar as invoice_id,
        
        -- Parse timestamp
        try_to_timestamp_ntz(record_content:created_at::varchar) as created_at,
        
        -- Amount field in GBP
        record_content:amount_gbp::number as amount_gbp,
        
        -- Charge details
        record_content:status::varchar as status,
        record_content:payment_method::varchar as payment_method

    from source
)

select * from flattened
