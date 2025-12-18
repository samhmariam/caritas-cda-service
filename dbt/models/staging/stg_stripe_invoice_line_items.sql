{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_stripe', 'stripe_invoice_line_items') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract line item fields from JSON
        record_content:invoice_line_item_id::varchar as invoice_line_item_id,
        record_content:invoice_id::varchar as invoice_id,
        record_content:description::varchar as description,
        record_content:revenue_category::varchar as revenue_category,
        
        -- Amount and pricing in GBP
        record_content:amount_gbp::number as amount_gbp,
        record_content:quantity::number as quantity,
        record_content:unit_price_gbp::number as unit_price_gbp

    from source
)

select * from flattened
