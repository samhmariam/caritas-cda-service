{{
  config(
    materialized='table',
    tags=['marts', 'activation']
  )
}}

-- High-value customers for sales outreach
with customer_360 as (
    select * from {{ ref('dim_customer_360') }}
),

activation_targets as (
    select
        customer_key,
    email_domain,
        first_name,
        last_name,
        company,
        'high_engagement' as segment,
        'Sales outreach for upsell' as recommended_action,
        current_timestamp() as segment_assigned_at
    from customer_360
    where 
        days_since_first_seen > 30
        and company is not null
)

select * from activation_targets
