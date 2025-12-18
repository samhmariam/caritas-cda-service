{{
  config(
    materialized='view',
    tags=['intermediate', 'customer_360']
  )
}}

with stripe_customers as (
    select
        email_domain,
        company_name,
        created_at as stripe_created_at,
        customer_id as stripe_customer_id
    from {{ ref('stg_stripe_customers') }}
    where email_domain is not null
),

unified as (
    select
        email_domain,
        null::varchar as first_name,
        null::varchar as last_name,
        company_name as company,
        stripe_customer_id,
        stripe_created_at as first_seen_at,
        stripe_created_at as last_seen_at
    from stripe_customers
)

select * from unified
