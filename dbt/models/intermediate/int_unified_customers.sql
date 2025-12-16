{{
  config(
    materialized='view',
    tags=['intermediate', 'customer_360']
  )
}}

with hubspot_contacts as (
    select
        email,
        first_name,
        last_name,
        company,
        created_at as hubspot_created_at
    from {{ ref('stg_hubspot__contacts') }}
    where email is not null
),

stripe_customers as (
    select
        email,
        customer_name,
        created_at as stripe_created_at,
        customer_id as stripe_customer_id
    from {{ ref('stg_stripe__customers') }}
    where email is not null
),

unified as (
    select
        coalesce(h.email, s.email) as email,
        coalesce(h.first_name, split_part(s.customer_name, ' ', 1)) as first_name,
        coalesce(h.last_name, split_part(s.customer_name, ' ', 2)) as last_name,
        h.company,
        s.stripe_customer_id,
        least(h.hubspot_created_at, s.stripe_created_at) as first_seen_at,
        greatest(h.hubspot_created_at, s.stripe_created_at) as last_seen_at
    from hubspot_contacts h
    full outer join stripe_customers s
        on lower(h.email) = lower(s.email)
)

select * from unified
