{{
  config(
    materialized='table',
    tags=['marts', 'risk', 'revenue_at_risk']
  )
}}

/*
    Fact: Revenue At Risk

    Goal: Identify high-value customers who are "quietly quitting".

    High value:
      - ARR > £50k from Salesforce (Closed Won, active contract)

    Declining usage:
      - Active days dropped by >20% vs the previous 3-month average (Mixpanel via int_product_usage_metrics)

    Optional signal:
      - Support hours increased (via int_unified_cost)

    Grain:
      - One row per (master_customer_id, as_of_date)
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

as_of as (
    select current_date() as as_of_date
),

active_salesforce_arr as (
    select
        im.master_customer_id,
        sum(coalesce(so.arr_booked_gbp, 0)) as booked_arr_gbp
    from {{ ref('stg_sf_opportunities') }} so
    left join identity_map im on so.account_id = im.account_id
    where im.master_customer_id is not null
      and so.stage = 'Closed Won'
      and so.arr_booked_gbp is not null
      and so.contract_start_date is not null
      and so.contract_start_date <= (select as_of_date from as_of)
      and (
            so.contract_end_date is null
         or so.contract_end_date >= (select as_of_date from as_of)
      )
    group by 1
),

usage_latest as (
    select
        master_customer_id,
        event_date,
        active_days_30d as active_days_30d_current
    from {{ ref('int_product_usage_metrics') }}
    qualify row_number() over (
        partition by master_customer_id
        order by event_date desc
    ) = 1
),

usage_prev_3mo_avg as (
    select
        master_customer_id,
        avg(active_days_30d) as active_days_30d_prev_3mo_avg
    from {{ ref('int_product_usage_metrics') }}
    where event_date between dateadd('day', -120, (select as_of_date from as_of))
                        and dateadd('day', -30, (select as_of_date from as_of))
    group by 1
),

support_hours as (
    select
        master_customer_id,
        sum(case when activity_date >= dateadd('day', -30, (select as_of_date from as_of)) then hours else 0 end) as support_hours_30d,
        sum(case
            when activity_date >= dateadd('day', -60, (select as_of_date from as_of))
             and activity_date < dateadd('day', -30, (select as_of_date from as_of))
            then hours else 0 end
        ) as support_hours_prev_30d
    from {{ ref('int_unified_cost') }}
    where work_type = 'SUPPORT'
      and activity_date is not null
    group by 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['im.master_customer_id', '(select as_of_date from as_of)']) }} as revenue_at_risk_id,
        im.master_customer_id,
        im.customer_name,
        (select as_of_date from as_of) as as_of_date,

        -- High value
        coalesce(sfa.booked_arr_gbp, 0) as booked_arr_gbp,
        case when coalesce(sfa.booked_arr_gbp, 0) > 50000 then true else false end as is_high_value,

        -- Usage decline
        coalesce(ul.active_days_30d_current, 0) as active_days_30d_current,
        coalesce(up.active_days_30d_prev_3mo_avg, 0) as active_days_30d_prev_3mo_avg,
        case
            when coalesce(up.active_days_30d_prev_3mo_avg, 0) > 0
            then (coalesce(ul.active_days_30d_current, 0) - up.active_days_30d_prev_3mo_avg) / up.active_days_30d_prev_3mo_avg
            else null
        end as active_days_change_ratio,
        case
            when coalesce(up.active_days_30d_prev_3mo_avg, 0) > 0
             and ((coalesce(ul.active_days_30d_current, 0) - up.active_days_30d_prev_3mo_avg) / up.active_days_30d_prev_3mo_avg) <= -0.20
            then true
            else false
        end as is_declining_usage,

        -- Optional support spike
        coalesce(sh.support_hours_30d, 0) as support_hours_30d,
        coalesce(sh.support_hours_prev_30d, 0) as support_hours_prev_30d,
        case
            when coalesce(sh.support_hours_prev_30d, 0) > 0
             and (coalesce(sh.support_hours_30d, 0) / sh.support_hours_prev_30d) >= 1.20
            then true
            else false
        end as is_support_spike,

        -- Final flag
        case
            when (coalesce(sfa.booked_arr_gbp, 0) > 50000)
             and (
                    coalesce(up.active_days_30d_prev_3mo_avg, 0) > 0
                and ((coalesce(ul.active_days_30d_current, 0) - up.active_days_30d_prev_3mo_avg) / up.active_days_30d_prev_3mo_avg) <= -0.20
             )
            then true
            else false
        end as is_revenue_at_risk,

        current_timestamp() as dbt_loaded_at

    from identity_map im
    left join active_salesforce_arr sfa on im.master_customer_id = sfa.master_customer_id
    left join usage_latest ul on im.master_customer_id = ul.master_customer_id
    left join usage_prev_3mo_avg up on im.master_customer_id = up.master_customer_id
    left join support_hours sh on im.master_customer_id = sh.master_customer_id
    where im.master_customer_id is not null
)

select * from final
