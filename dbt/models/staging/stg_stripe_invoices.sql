{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_stripe', 'stripe_invoices') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract invoice fields from JSON
        record_content:invoice_id::varchar as invoice_id,
        record_content:customer_id::varchar as customer_id,
        record_content:subscription_id::varchar as subscription_id,
        
        -- Parse dates
        try_to_date(record_content:invoice_date::varchar) as invoice_date,
        try_to_timestamp_ntz(record_content:paid_at::varchar) as paid_at,
        try_to_date(record_content:period_start::varchar) as period_start,
        try_to_date(record_content:period_end::varchar) as period_end,
        
        -- Amount fields in GBP
        record_content:amount_due_gbp::number as amount_due_gbp,
        record_content:amount_paid_gbp::number as amount_paid_gbp,
        
        -- Invoice status
        record_content:status::varchar as status

    from source
)

select * from flattened
