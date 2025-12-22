{{
  config(
    materialized='table',
    tags=['marts', 'finance', 'profitability']
  )
}}

/*
    Fact: Customer Profitability

    Business question (CFO): Which customers are we actually making money on?

    Grain:
      - One row per (master_customer_id, profitability_month)

    Revenue lens:
      - GAAP recognized revenue from Intacct via int_unified_revenue

    Cost lens:
      - Cost-to-serve from int_unified_cost
      - Includes an optional allocated shared engineering pool (Jira issues not linked to a customer ticket)
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

revenue_monthly as (
    select
        ur.master_customer_id,
        date_trunc('month', ur.revenue_date) as profitability_month,
        sum(ur.amount_gbp) as gaap_recognized_revenue_gbp
    from {{ ref('int_unified_revenue') }} ur
    where ur.revenue_type = 'GAAP'
      and ur.revenue_category = 'RECOGNIZED'
      and ur.master_customer_id is not null
      and ur.revenue_date is not null
    group by 1, 2
),

cost_with_allocation_type as (
    select
        c.*,
        case
            when c.work_type = 'ENGINEERING'
             and c.source_system = 'JIRA'
             and c.linked_ticket_id is null
            then 'SHARED'
            else 'DIRECT'
        end as cost_allocation_type
    from {{ ref('int_unified_cost') }} c
),

cost_monthly as (
    -- Single pass over costs to compute direct breakdown and shared-engineering pool
    select
        master_customer_id,
        date_trunc('month', activity_date) as profitability_month,

        -- Direct cost breakdown
        sum(case when cost_allocation_type = 'DIRECT' then cost_gbp else 0 end) as total_direct_cost_gbp,
        sum(case when cost_allocation_type = 'DIRECT' and work_type = 'DELIVERY' then cost_gbp else 0 end) as delivery_cost_gbp,
        sum(case when cost_allocation_type = 'DIRECT' and work_type = 'SUPPORT' then cost_gbp else 0 end) as support_cost_gbp,
        sum(case when cost_allocation_type = 'DIRECT' and work_type = 'ENGINEERING' then cost_gbp else 0 end) as engineering_direct_cost_gbp,

        -- SHARED engineering at the customer-month level (for pool construction)
        sum(case when cost_allocation_type = 'SHARED' and work_type = 'ENGINEERING' then cost_gbp else 0 end) as shared_engineering_cost_customer_month_gbp

    from cost_with_allocation_type
    where master_customer_id is not null
      and activity_date is not null
    group by 1, 2
),

cost_monthly_with_pool as (
    select
        cm.*,
        sum(cm.shared_engineering_cost_customer_month_gbp) over (partition by cm.profitability_month) as shared_engineering_pool_gbp
    from cost_monthly cm
),

activity_customer_month_spine as (
    -- Only emit customer-months that actually have activity (revenue or direct cost).
    -- This avoids the cardinality explosion of month×all_customers.
    select distinct
        master_customer_id,
        profitability_month
    from revenue_monthly
    union
    select distinct
        master_customer_id,
        profitability_month
        from cost_monthly
),

allocation_basis as (
    select
        cms.master_customer_id,
        cms.profitability_month,
        coalesce(rm.gaap_recognized_revenue_gbp, 0) as gaap_recognized_revenue_gbp,
        sum(coalesce(rm.gaap_recognized_revenue_gbp, 0)) over (partition by cms.profitability_month) as total_revenue_in_month_gbp
    from activity_customer_month_spine cms
    left join revenue_monthly rm
        on cms.master_customer_id = rm.master_customer_id
       and cms.profitability_month = rm.profitability_month
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['ab.master_customer_id', 'ab.profitability_month']) }} as customer_profitability_id,
        ab.master_customer_id,
        im.customer_name,
        ab.profitability_month,

        -- Revenue
        ab.gaap_recognized_revenue_gbp,

        -- Direct cost breakdown
            coalesce(cm.delivery_cost_gbp, 0) as delivery_cost_gbp,
            coalesce(cm.support_cost_gbp, 0) as support_cost_gbp,
            coalesce(cm.engineering_direct_cost_gbp, 0) as engineering_direct_cost_gbp,
            coalesce(cm.total_direct_cost_gbp, 0) as total_direct_cost_gbp,

        -- Shared engineering (pooled) and allocated share
            coalesce(cm.shared_engineering_pool_gbp, 0) as shared_engineering_pool_gbp,
        case
            when ab.total_revenue_in_month_gbp > 0 and ab.gaap_recognized_revenue_gbp > 0
                then (ab.gaap_recognized_revenue_gbp / ab.total_revenue_in_month_gbp) * coalesce(cm.shared_engineering_pool_gbp, 0)
            else 0
        end as allocated_shared_engineering_cost_gbp,

        -- Total cost & margin
        (coalesce(cm.total_direct_cost_gbp, 0)
          + case
                when ab.total_revenue_in_month_gbp > 0 and ab.gaap_recognized_revenue_gbp > 0
            then (ab.gaap_recognized_revenue_gbp / ab.total_revenue_in_month_gbp) * coalesce(cm.shared_engineering_pool_gbp, 0)
                else 0
            end
        ) as total_cost_gbp,

        (coalesce(ab.gaap_recognized_revenue_gbp, 0)
                    - (coalesce(cm.total_direct_cost_gbp, 0)
              + case
                    when ab.total_revenue_in_month_gbp > 0 and ab.gaap_recognized_revenue_gbp > 0
                                        then (ab.gaap_recognized_revenue_gbp / ab.total_revenue_in_month_gbp) * coalesce(cm.shared_engineering_pool_gbp, 0)
                    else 0
                end
            )
        ) as gross_margin_gbp,

        case
            when coalesce(ab.gaap_recognized_revenue_gbp, 0) > 0
            then ((coalesce(ab.gaap_recognized_revenue_gbp, 0)
                    - (coalesce(cm.total_direct_cost_gbp, 0)
                        + case
                              when ab.total_revenue_in_month_gbp > 0 and ab.gaap_recognized_revenue_gbp > 0
                              then (ab.gaap_recognized_revenue_gbp / ab.total_revenue_in_month_gbp) * coalesce(cm.shared_engineering_pool_gbp, 0)
                              else 0
                          end
                      )
                  ) / ab.gaap_recognized_revenue_gbp) * 100
            else 0
        end as gross_margin_pct,

        current_timestamp() as dbt_loaded_at

    from allocation_basis ab
    left join identity_map im on ab.master_customer_id = im.master_customer_id
    left join cost_monthly_with_pool cm
        on ab.master_customer_id = cm.master_customer_id
       and ab.profitability_month = cm.profitability_month

    -- Keep only months where there is either revenue or cost activity
    where coalesce(ab.gaap_recognized_revenue_gbp, 0) > 0
       or coalesce(cm.total_direct_cost_gbp, 0) > 0
       or coalesce(cm.shared_engineering_pool_gbp, 0) > 0
)

select * from final
