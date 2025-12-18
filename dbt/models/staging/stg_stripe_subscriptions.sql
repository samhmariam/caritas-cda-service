{{
    config(
        materialized='view')
}}

with source as (
    select * from {{ source('raw_stripe', 'stripe_subscriptions') }}
),

flattened as (
    select
        -- Metadata fields
        metadata_filename as _source_file,
        metadata_row_number as _row_num,
        metadata_ingest_time as _ingested_at,
        
        -- Extract subscription fields from JSON
        record_content:subscription_id::varchar as subscription_id,
        record_content:customer_id::varchar as customer_id,
        record_content:plan_id::varchar as plan_id,
        record_content:plan_name::varchar as plan_name,
        
        -- Parse dates
        try_to_date(record_content:start_date::varchar) as start_date,
        try_to_date(record_content:current_period_start::varchar) as current_period_start,
        try_to_date(record_content:current_period_end::varchar) as current_period_end,
        
        -- Subscription details
        record_content:status::varchar as status,
        record_content:cancel_at_period_end::number as cancel_at_period_end,
        record_content:mrr_gbp::number as mrr_gbp

    from source
)

select * from flattened
