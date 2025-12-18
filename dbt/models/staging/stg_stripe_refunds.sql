{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_stripe', 'stripe_refunds') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract refund fields from JSON
        record_content:refund_id::varchar as refund_id,
        record_content:charge_id::varchar as charge_id,
        
        -- Parse timestamp
        try_to_timestamp_ntz(record_content:created_at::varchar) as created_at,
        
        -- Refund amount and details
        record_content:amount_gbp::number as amount_gbp,
        record_content:reason::varchar as reason

    from source
)

select * from flattened
