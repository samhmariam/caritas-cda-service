{{
  config(
    materialized='table',
    tags=['marts', 'core', 'customer_360']
  )
}}

with unified_customers as (
    select * from {{ ref('int_unified_customers') }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['email']) }} as customer_key,
        email,
        first_name,
        last_name,
        company,
        stripe_customer_id,
        first_seen_at,
        last_seen_at,
        datediff('day', first_seen_at, current_timestamp()) as days_since_first_seen,
        current_timestamp() as dbt_loaded_at
    from unified_customers
)

select * from enriched
