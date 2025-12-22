{{
  config(
    materialized='table',
    tags=['marts', 'product', 'customer_health']
  )
}}

/*
    Fact: Customer Health

    Target audience: CCO / Head of Customer Success
    Business question: Who is going to churn next month?

    Grain:
      - One row per (master_customer_id, as_of_date)

    Signals:
      - Product usage (Mixpanel via int_product_usage_metrics)
      - Support friction (Zendesk tickets)
      - Payment reliability (Stripe invoices)

    Health score (0-100; higher is healthier):
      - usage_score * 0.5
      - tickets_score * 0.3
      - late_payment_score * 0.2

    Notes:
      - Component scores are normalized to 0-100 to keep the composite interpretable.
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

as_of as (
    select current_date() as as_of_date
),

-- Latest product usage snapshot per customer (rolling 30d metrics)
usage_latest as (
    select
        master_customer_id,
        event_date,
        monthly_active_users_30d as active_users_l30d,
        active_days_30d,
        usage_trend_pct_30d
    from {{ ref('int_product_usage_metrics') }}
    qualify row_number() over (
        partition by master_customer_id
        order by event_date desc
    ) = 1
),

-- Last login/activity per customer from Mixpanel
mixpanel_last_login as (
    select
        ucm.master_customer_id,
        max(case when me.event_name ilike '%login%' then me.timestamp::date else null end) as last_login_date,
        max(me.timestamp::date) as last_activity_date
    from {{ ref('stg_mixpanel_events') }} me
    join {{ ref('int_user_customer_map') }} ucm
        on me.distinct_id = ucm.user_identifier
    where ucm.master_customer_id is not null
      and me.timestamp is not null
    group by 1
),

-- Open critical/high tickets per customer
open_critical_tickets as (
    select
        im.master_customer_id,
        count(distinct zt.ticket_id) as open_critical_tickets_count
    from {{ ref('stg_zendesk_tickets') }} zt
    left join identity_map im on zt.organization_id = im.zendesk_organization_id
    where im.master_customer_id is not null
      and zt.ticket_id is not null
      and coalesce(zt.status, '') not in ('solved', 'closed')
      and (
          coalesce(zt.severity, '') in ('Critical', 'High')
          or coalesce(zt.priority, '') in ('urgent', 'high')
      )
    group by 1
),

-- Payment timeliness and DSO from Stripe invoices
payment_behavior as (
    select
        im.master_customer_id,

        -- Weighted DSO on open invoices: sum(amount_due * age) / sum(amount_due)
        case
            when sum(case when si.status = 'open' then coalesce(si.amount_due_gbp, 0) else 0 end) > 0
            then (
                sum(
                    case
                        when si.status = 'open'
                         and si.invoice_date is not null
                        then coalesce(si.amount_due_gbp, 0) * datediff('day', si.invoice_date, (select as_of_date from as_of))
                        else 0
                    end
                )
                /
                sum(case when si.status = 'open' then coalesce(si.amount_due_gbp, 0) else 0 end)
            )
            else 0
        end as days_sales_outstanding,

        count(distinct case
            when si.status = 'open'
             and si.invoice_date is not null
             and datediff('day', si.invoice_date, (select as_of_date from as_of)) >= 30
            then si.invoice_id end
        ) as open_invoices_30d_plus_count,

        avg(case
            when si.status = 'paid'
             and si.invoice_date is not null
             and si.paid_at is not null
             and si.paid_at::date >= dateadd('day', -90, (select as_of_date from as_of))
            then datediff('day', si.invoice_date, si.paid_at::date)
            else null end
        ) as avg_days_to_pay_90d

    from {{ ref('stg_stripe_invoices') }} si
    left join identity_map im on si.customer_id = im.stripe_customer_id
    where im.master_customer_id is not null
    group by 1
),

scored as (
    select
        im.master_customer_id,
        im.customer_name,
        (select as_of_date from as_of) as as_of_date,

        -- Required key columns
        coalesce(mll.last_login_date, mll.last_activity_date) as last_login_date,
        coalesce(ul.active_users_l30d, 0) as active_users_l30d,
        coalesce(oct.open_critical_tickets_count, 0) as open_critical_tickets_count,
        round(coalesce(pb.days_sales_outstanding, 0), 1) as days_sales_outstanding,

        -- Component score normalization (0-100)
        least(100, greatest(0, (coalesce(ul.active_days_30d, 0) / 30.0) * 100)) as usage_score,
        least(100, greatest(0, coalesce(oct.open_critical_tickets_count, 0) * 25)) as tickets_score,
        least(100, greatest(0, (coalesce(pb.days_sales_outstanding, 0) / 60.0) * 100)) as late_payment_score,

        -- Composite health score
        least(
            100,
            greatest(
                0,
                (least(100, greatest(0, (coalesce(ul.active_days_30d, 0) / 30.0) * 100)) * 0.5)
                - (least(100, greatest(0, coalesce(oct.open_critical_tickets_count, 0) * 25)) * 0.3)
                - (least(100, greatest(0, (coalesce(pb.days_sales_outstanding, 0) / 60.0) * 100)) * 0.2)
            )
        ) as health_score,

        -- Helpful supporting fields
        coalesce(pb.open_invoices_30d_plus_count, 0) as open_invoices_30d_plus_count,
        round(coalesce(pb.avg_days_to_pay_90d, 0), 1) as avg_days_to_pay_90d,
        coalesce(ul.usage_trend_pct_30d, 0) as usage_trend_pct_30d,

        case
            when least(
                100,
                greatest(
                    0,
                    (least(100, greatest(0, (coalesce(ul.active_days_30d, 0) / 30.0) * 100)) * 0.5)
                    - (least(100, greatest(0, coalesce(oct.open_critical_tickets_count, 0) * 25)) * 0.3)
                    - (least(100, greatest(0, (coalesce(pb.days_sales_outstanding, 0) / 60.0) * 100)) * 0.2)
                )
            ) < 40 then 'CHURN_RISK'
            when least(
                100,
                greatest(
                    0,
                    (least(100, greatest(0, (coalesce(ul.active_days_30d, 0) / 30.0) * 100)) * 0.5)
                    - (least(100, greatest(0, coalesce(oct.open_critical_tickets_count, 0) * 25)) * 0.3)
                    - (least(100, greatest(0, (coalesce(pb.days_sales_outstanding, 0) / 60.0) * 100)) * 0.2)
                )
            ) < 60 then 'AT_RISK'
            when least(
                100,
                greatest(
                    0,
                    (least(100, greatest(0, (coalesce(ul.active_days_30d, 0) / 30.0) * 100)) * 0.5)
                    - (least(100, greatest(0, coalesce(oct.open_critical_tickets_count, 0) * 25)) * 0.3)
                    - (least(100, greatest(0, (coalesce(pb.days_sales_outstanding, 0) / 60.0) * 100)) * 0.2)
                )
            ) < 80 then 'HEALTHY'
            else 'EXPANSION_OPPORTUNITY'
        end as health_category,

        current_timestamp() as dbt_loaded_at

    from identity_map im
    left join usage_latest ul on im.master_customer_id = ul.master_customer_id
    left join mixpanel_last_login mll on im.master_customer_id = mll.master_customer_id
    left join open_critical_tickets oct on im.master_customer_id = oct.master_customer_id
    left join payment_behavior pb on im.master_customer_id = pb.master_customer_id
    where im.master_customer_id is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['master_customer_id', 'as_of_date']) }} as customer_health_id,
    *
from scored
