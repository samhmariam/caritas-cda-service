{{
  config(
    materialized='view',
    tags=['intermediate', 'finance', 'arr']
  )
}}

/*
    MRR/ARR Snapshot (Operational)

    Purpose: Model Stripe subscription state as an operational ARR/MRR snapshot.

    Notes:
    - Stripe subscription objects represent billing state, not GAAP revenue recognition.
    - Keep this separate from int_unified_revenue to avoid double-counting GAAP alongside Intacct.
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

stripe_subscriptions as (
    select * from {{ ref('stg_stripe_subscriptions') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['ss.subscription_id', "date_trunc('day', ss.current_period_start)"]) }} as mrr_arr_snapshot_id,
        im.master_customer_id,
        ss.subscription_id,
        ss.status as subscription_status,
        ss.current_period_start::date as snapshot_date,
        ss.mrr_gbp as mrr_gbp,
        ss.mrr_gbp * 12 as arr_gbp,
        current_timestamp() as snapshot_calculated_at
    from stripe_subscriptions ss
    left join identity_map im on ss.customer_id = im.stripe_customer_id
    where ss.subscription_id is not null
      and im.master_customer_id is not null
)

select * from final
