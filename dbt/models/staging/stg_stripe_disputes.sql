{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_stripe', 'stripe_disputes') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract dispute fields from JSON
        record_content:dispute_id::varchar as dispute_id,
        record_content:charge_id::varchar as charge_id,
        
        -- Parse timestamp
        try_to_timestamp_ntz(record_content:created_at::varchar) as created_at,
        
        -- Dispute amount and details
        record_content:amount_gbp::number as amount_gbp,
        record_content:status::varchar as status,
        record_content:reason::varchar as reason

    from source
)

select * from flattened
