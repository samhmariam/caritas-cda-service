{{
  config(
    materialized='view',
    tags=['intermediate', 'analytics', 'ltv']
  )
}}

/*
    Customer LTV - Unit Economics & Profitability
    
    Purpose: Calculate Customer Lifetime Value and unit economics
    
    Key Metrics:
    - Total revenue vs total cost to serve → true customer profitability
    - Gross margin and margin %
    - LTV projections
    - CAC (Customer Acquisition Cost) from Salesforce opportunities
    - LTV:CAC ratio for portfolio optimization
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

params as (
    select
        date_trunc('month', current_date()) as this_month_start
),

-- Commercial MRR (operational): derive from subscription snapshot, not GAAP revenue
commercial_mrr_by_customer_month as (
    with mrr_source as (
        select
            master_customer_id,
            subscription_id,
            snapshot_date,
            date_trunc('month', snapshot_date) as snapshot_month,
            mrr_gbp
        from {{ ref('int_mrr_arr_snapshot') }}
        where master_customer_id is not null
          and subscription_id is not null
          and snapshot_date is not null
    ),
    latest_per_subscription_month as (
        select *
        from mrr_source
        qualify row_number() over (
            partition by master_customer_id, subscription_id, snapshot_month
            order by snapshot_date desc
        ) = 1
    )
    select
        master_customer_id,
        snapshot_month,
        sum(mrr_gbp) as commercial_mrr_gbp
    from latest_per_subscription_month
    group by 1, 2
),

commercial_mrr_metrics as (
    select
        m.master_customer_id,
        sum(case when m.snapshot_month = p.this_month_start then m.commercial_mrr_gbp else 0 end) as commercial_current_mrr_gbp,
        avg(m.commercial_mrr_gbp) as commercial_avg_monthly_mrr_gbp,
        count(distinct m.snapshot_month) as months_with_commercial_mrr
    from commercial_mrr_by_customer_month m
    cross join params p
    group by 1
),

-- Lifetime revenue by customer
lifetime_revenue_gaap as (
    select
        master_customer_id,
        min(revenue_date) as first_revenue_date,
        max(revenue_date) as last_revenue_date,
        -- GAAP recognized revenue totals (accounting view)
        sum(amount_gbp) as gaap_total_revenue_gbp,
        sum(case when revenue_category = 'RECOGNIZED' then amount_gbp else 0 end) as gaap_total_recognized_revenue_gbp,
        sum(case when revenue_category = 'DEFERRED' then amount_gbp else 0 end) as gaap_total_deferred_revenue_gbp,
        -- Count of months with GAAP activity
        count(distinct revenue_month) as gaap_months_with_revenue
    from {{ ref('int_unified_revenue') }}
    where revenue_type = 'GAAP'
    group by master_customer_id
),

-- Cost allocation controls
cost_with_allocation_type as (
    select
        c.*,
        case
            when c.work_type = 'ENGINEERING' and c.source_system = 'JIRA' and c.linked_ticket_id is null then 'SHARED'
            else 'DIRECT'
        end as cost_allocation_type
    from {{ ref('int_unified_cost') }} c
),

-- Lifetime cost by customer (split DIRECT vs SHARED)
lifetime_cost as (
    select
        master_customer_id,
        -- Direct costs are attributable to a customer without a pooling rule
        sum(case when cost_allocation_type = 'DIRECT' then cost_gbp else 0 end) as total_direct_cost_gbp,
        sum(case when cost_allocation_type = 'DIRECT' and work_type = 'DELIVERY' then cost_gbp else 0 end) as total_delivery_cost_gbp,
        sum(case when cost_allocation_type = 'DIRECT' and work_type = 'SUPPORT' then cost_gbp else 0 end) as total_support_cost_gbp,
        sum(case when cost_allocation_type = 'DIRECT' and work_type = 'ENGINEERING' then cost_gbp else 0 end) as total_engineering_direct_cost_gbp,
        -- Shared engineering costs (pooled)
        sum(case when cost_allocation_type = 'SHARED' and work_type = 'ENGINEERING' then cost_gbp else 0 end) as total_engineering_shared_cost_gbp,
        sum(hours) as total_hours,
        -- Average monthly cost
        avg(cost_gbp) as avg_monthly_cost_gbp
    from cost_with_allocation_type
    group by master_customer_id
),

shared_engineering_pool as (
    select
        sum(case when cost_allocation_type = 'SHARED' and work_type = 'ENGINEERING' then cost_gbp else 0 end) as shared_engineering_pool_gbp
    from cost_with_allocation_type
),

-- Customer acquisition cost from Salesforce opportunities
cac_calculation as (
    select
        im.master_customer_id,
        -- Sum of closed-won opportunity amounts as a proxy for CAC
        -- In reality, you'd want marketing + sales costs, not deal size
        -- This is a simplified approach
        min(so.close_date) as acquisition_date,
        count(distinct so.opportunity_id) as opportunities_count
        -- Note: Ideally you'd have actual CAC from marketing/sales expense data
    from {{ ref('stg_sf_opportunities') }} so
    left join identity_map im on so.account_id = im.account_id
    where so.stage = 'Closed Won'
    group by im.master_customer_id
),

-- Calculate customer tenure
customer_tenure as (
    select
        im.master_customer_id,
        im.first_seen_at,
        current_date() as calculation_date,
        datediff('month', im.first_seen_at, current_date()) as tenure_months,
        case 
            when datediff('month', im.first_seen_at, current_date()) <= 0 then 1
            else datediff('month', im.first_seen_at, current_date())
        end as tenure_months_safe  -- Avoid division by zero
    from identity_map im
),

-- Combine all metrics
ltv_calculation as (
    select
        im.master_customer_id,
        im.customer_name,
        
        -- Revenue metrics (Accounting lens)
        coalesce(lr.gaap_total_revenue_gbp, 0) as gaap_total_revenue_gbp,
        coalesce(lr.gaap_total_recognized_revenue_gbp, 0) as gaap_total_recognized_revenue_gbp,
        coalesce(lr.gaap_total_deferred_revenue_gbp, 0) as gaap_total_deferred_revenue_gbp,

        -- Revenue metrics (Commercial lens)
        coalesce(cm.commercial_current_mrr_gbp, 0) as commercial_current_mrr_gbp,
        coalesce(cm.commercial_avg_monthly_mrr_gbp, 0) as commercial_avg_monthly_mrr_gbp,
        
        -- Cost metrics
        coalesce(lc.total_direct_cost_gbp, 0) as total_direct_cost_gbp,
        coalesce(lc.total_delivery_cost_gbp, 0) as total_delivery_cost_gbp,
        coalesce(lc.total_support_cost_gbp, 0) as total_support_cost_gbp,
        coalesce(lc.total_engineering_direct_cost_gbp, 0) as total_engineering_direct_cost_gbp,
        coalesce(lc.total_engineering_shared_cost_gbp, 0) as total_engineering_shared_cost_gbp,
        coalesce(lc.total_hours, 0) as total_hours,

        -- Allocate SHARED engineering transparently by current commercial MRR share (optional lens)
        case
            when sum(coalesce(cm.commercial_current_mrr_gbp, 0)) over () > 0 and coalesce(cm.commercial_current_mrr_gbp, 0) > 0
            then (coalesce(cm.commercial_current_mrr_gbp, 0) / sum(coalesce(cm.commercial_current_mrr_gbp, 0)) over ()) * coalesce(sp.shared_engineering_pool_gbp, 0)
            else 0
        end as allocated_shared_engineering_cost_gbp,
        coalesce(lc.total_direct_cost_gbp, 0)
          + case
                when sum(coalesce(cm.commercial_current_mrr_gbp, 0)) over () > 0 and coalesce(cm.commercial_current_mrr_gbp, 0) > 0
                then (coalesce(cm.commercial_current_mrr_gbp, 0) / sum(coalesce(cm.commercial_current_mrr_gbp, 0)) over ()) * coalesce(sp.shared_engineering_pool_gbp, 0)
                else 0
            end as total_cost_including_allocated_shared_engineering_gbp,
        
        -- Margin calculation (Accounting lens vs DIRECT cost)
        coalesce(lr.gaap_total_revenue_gbp, 0) - coalesce(lc.total_direct_cost_gbp, 0) as gross_margin_direct_cost_gbp,
        case
            when lr.gaap_total_revenue_gbp > 0
            then ((coalesce(lr.gaap_total_revenue_gbp, 0) - coalesce(lc.total_direct_cost_gbp, 0)) / lr.gaap_total_revenue_gbp) * 100
            else 0
        end as margin_percent_direct_cost,

        -- Margin calculation (Accounting lens vs DIRECT + allocated SHARED engineering)
        coalesce(lr.gaap_total_revenue_gbp, 0)
          - (
              coalesce(lc.total_direct_cost_gbp, 0)
              + case
                    when sum(coalesce(cm.commercial_current_mrr_gbp, 0)) over () > 0 and coalesce(cm.commercial_current_mrr_gbp, 0) > 0
                    then (coalesce(cm.commercial_current_mrr_gbp, 0) / sum(coalesce(cm.commercial_current_mrr_gbp, 0)) over ()) * coalesce(sp.shared_engineering_pool_gbp, 0)
                    else 0
                end
            ) as gross_margin_with_allocated_shared_engineering_gbp,
        
        -- Tenure
        ct.tenure_months as customer_tenure_months,
        ct.first_seen_at as customer_since,
        
        -- Monthly metrics
        case 
            when ct.tenure_months_safe > 0 
            then coalesce(lr.gaap_total_revenue_gbp, 0) / ct.tenure_months_safe
            else 0
        end as avg_monthly_revenue_gbp,
        case 
            when ct.tenure_months_safe > 0 
            then coalesce(lc.total_direct_cost_gbp, 0) / ct.tenure_months_safe
            else 0
        end as avg_monthly_cost_gbp,
        case 
            when ct.tenure_months_safe > 0 
            then (coalesce(lr.gaap_total_revenue_gbp, 0) - coalesce(lc.total_direct_cost_gbp, 0)) / ct.tenure_months_safe
            else 0
        end as avg_monthly_margin_gbp,
        
        -- Commercial LTV projection (simple: current commercial MRR * 36 months average SaaS lifetime)
        coalesce(cm.commercial_current_mrr_gbp, 0) * 36 as projected_commercial_ltv_gbp,
        -- Accounting LTV: do NOT project from GAAP recognition; expose totals only
        null::number as projected_gaap_ltv_gbp,
        
        -- CAC placeholder (would need actual marketing/sales cost data)
        1000 as estimated_cac_gbp,  -- Placeholder: £1000 average CAC
        cac.acquisition_date,
        
        -- Revenue dates
        lr.first_revenue_date,
        lr.last_revenue_date,
        lr.gaap_months_with_revenue
        
    from identity_map im
    left join lifetime_revenue_gaap lr on im.master_customer_id = lr.master_customer_id
    left join commercial_mrr_metrics cm on im.master_customer_id = cm.master_customer_id
    left join lifetime_cost lc on im.master_customer_id = lc.master_customer_id
    cross join shared_engineering_pool sp
    left join customer_tenure ct on im.master_customer_id = ct.master_customer_id
    left join cac_calculation cac on im.master_customer_id = cac.master_customer_id
),

-- Calculate final LTV metrics and ratios
final as (
    select
        *,
        
        -- LTV:CAC ratio (key SaaS metric)
        case 
            when estimated_cac_gbp > 0 
            then projected_commercial_ltv_gbp / estimated_cac_gbp
            else null
        end as ltv_to_cac_ratio,
        
        -- CAC payback period (months to recover acquisition cost)
        case 
            when avg_monthly_margin_gbp > 0 
            then estimated_cac_gbp / avg_monthly_margin_gbp
            else null
        end as cac_payback_months,
        
        -- Customer profitability tier
        case 
            when gross_margin_direct_cost_gbp > 10000 then 'PLATINUM'
            when gross_margin_direct_cost_gbp > 5000 then 'GOLD'
            when gross_margin_direct_cost_gbp > 1000 then 'SILVER'
            when gross_margin_direct_cost_gbp > 0 then 'BRONZE'
            else 'UNPROFITABLE'
        end as profitability_tier,
        
        -- Status flag
        case 
            when commercial_current_mrr_gbp > 0 then 'ACTIVE'
            when last_revenue_date >= dateadd('month', -3, current_date()) then 'RECENTLY_CHURNED'
            else 'CHURNED'
        end as customer_status
        
    from ltv_calculation
    where master_customer_id is not null
)

select * from final
