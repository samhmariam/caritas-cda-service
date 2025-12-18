{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_stripe', 'stripe_balance_transactions') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract balance transaction fields from JSON
        record_content:balance_txn_id::varchar as balance_txn_id,
        record_content:source_charge_id::varchar as source_charge_id,
        
        -- Parse timestamp
        try_to_timestamp_ntz(record_content:created_at::varchar) as created_at,
        
        -- Amount fields in GBP
        record_content:gross_gbp::number as gross_gbp,
        record_content:fee_gbp::number as fee_gbp,
        record_content:net_gbp::number as net_gbp

    from source
)

select * from flattened
