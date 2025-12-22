{{
  config(
    materialized='view',
    tags=['intermediate', 'analytics', 'health']
  )
}}

/*
    Customer Health Score - Proactive Account Management
    
    Purpose: Real-time customer health scoring for churn prediction and expansion targeting
    
    Health Score Components (0-100):
    - Revenue Trend (30 points): MRR growth/decline over 30/60/90 days
    - Support Activity (25 points): Ticket volume and severity trends
    - Payment Behavior (25 points): Invoice payment timeliness
    - Product Usage (20 points): Engagement trends from Mixpanel
    
    Risk Categories:
    - CHURN_RISK (0-40): Immediate intervention needed
    - AT_RISK (41-60): Watch closely, early warning
    - HEALTHY (61-80): Good standing
    - EXPANSION_OPPORTUNITY (81-100): Upsell candidates
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

params as (
    select
        date_trunc('month', current_date()) as this_month_start,
        dateadd('month', -1, date_trunc('month', current_date())) as prev_month_start,
        dateadd('month', -2, date_trunc('month', current_date())) as two_months_ago_start
),

-- Commercial MRR trends (operational): derive from subscription snapshot, not GAAP revenue
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
        snapshot_month as revenue_month,
        sum(mrr_gbp) as commercial_mrr_gbp
    from latest_per_subscription_month
    group by 1, 2
),

commercial_mrr_windowed as (
    select
        m.master_customer_id,
        sum(case when m.revenue_month = p.this_month_start then m.commercial_mrr_gbp else 0 end) as current_mrr,
        sum(case when m.revenue_month = p.prev_month_start then m.commercial_mrr_gbp else 0 end) as prev_month_mrr,
        sum(case when m.revenue_month = p.two_months_ago_start then m.commercial_mrr_gbp else 0 end) as two_months_ago_mrr
    from commercial_mrr_by_customer_month m
    cross join params p
    group by 1
),

commercial_mrr_trends as (
    select
        master_customer_id,
        current_mrr,
        prev_month_mrr,
        two_months_ago_mrr,
        case
            when prev_month_mrr > 0 then ((current_mrr - prev_month_mrr) / prev_month_mrr) * 100
            else 0
        end as mrr_change_pct
    from commercial_mrr_windowed
),

-- GAAP recognized revenue (finance): keep separate from commercial MRR
gaap_recognized_trends as (
    select
        ur.master_customer_id,
        sum(case when ur.revenue_month = p.this_month_start then ur.amount_gbp else 0 end) as gaap_recognized_current_month_gbp,
        sum(case when ur.revenue_month = p.prev_month_start then ur.amount_gbp else 0 end) as gaap_recognized_prev_month_gbp
    from {{ ref('int_unified_revenue') }} ur
    cross join params p
    where ur.revenue_type = 'GAAP'
      and ur.revenue_category = 'RECOGNIZED'
    group by 1
),

-- Payment behavior (cash collection): build from Stripe invoices only
stripe_invoice_payment_behavior as (
    select
        im.master_customer_id,
        -- Open invoices (unpaid)
        count(distinct case when si.status = 'open' then si.invoice_id end) as open_invoices_count,
        sum(case when si.status = 'open' then si.amount_due_gbp else 0 end) as open_invoices_amount_gbp,
        count(distinct case
            when si.status = 'open'
             and si.invoice_date is not null
             and datediff('day', si.invoice_date, current_date()) >= 30
            then si.invoice_id end
        ) as open_invoices_30d_plus_count,
        sum(case
            when si.status = 'open'
             and si.invoice_date is not null
             and datediff('day', si.invoice_date, current_date()) >= 30
            then si.amount_due_gbp else 0 end
        ) as open_invoices_30d_plus_amount_gbp,
        max(case
            when si.status = 'open'
             and si.invoice_date is not null
            then datediff('day', si.invoice_date, current_date())
            else null end
        ) as max_open_invoice_age_days,
        -- Paid invoice timeliness (last 90d)
        avg(case
            when si.status = 'paid'
             and si.invoice_date is not null
             and si.paid_at is not null
             and si.paid_at::date >= dateadd('day', -90, current_date())
            then datediff('day', si.invoice_date, si.paid_at::date)
            else null end
        ) as avg_days_to_pay_90d,
        count(distinct case
            when si.status = 'paid'
             and si.invoice_date is not null
             and si.paid_at is not null
             and si.paid_at::date >= dateadd('day', -90, current_date())
             and datediff('day', si.invoice_date, si.paid_at::date) > 14
            then si.invoice_id end
        ) as late_paid_invoices_90d
    from {{ ref('stg_stripe_invoices') }} si
    left join identity_map im on si.customer_id = im.stripe_customer_id
    where im.master_customer_id is not null
    group by 1
),

-- Support activity trends from issue resolution chain
support_trends as (
    select
        master_customer_id,
        -- Ticket volume trends
        count(distinct case when ticket_created_at >= dateadd('day', -30, current_date())
            then zendesk_ticket_id end) as tickets_last_30d,
        count(distinct case when ticket_created_at >= dateadd('day', -60, current_date())
            and ticket_created_at < dateadd('day', -30, current_date())
            then zendesk_ticket_id end) as tickets_prev_30d,
        -- Severity indicators
        count(distinct case when ticket_severity in ('Critical', 'High')
            and ticket_created_at >= dateadd('day', -30, current_date())
            then zendesk_ticket_id end) as high_severity_tickets_30d,
        -- Average resolution time
        avg(case when ticket_created_at >= dateadd('day', -30, current_date())
            then resolution_time_days end) as avg_resolution_days_30d,
        -- Total support cost
        sum(case when ticket_created_at >= dateadd('day', -30, current_date())
            then total_cost_gbp else 0 end) as total_support_cost_30d
    from {{ ref('int_issue_resolution_chain') }}
    group by master_customer_id
),

-- Product usage from Mixpanel events
usage_trends as (
    select
        me.distinct_id as user_identifier,
        -- Need to map users to customers - using email domain as proxy
        -- In production, you'd have a proper user → customer mapping
        null::varchar as master_customer_id,  -- Placeholder for now
        count(distinct case when me.timestamp >= dateadd('day', -30, current_date())
            then me.event_id end) as events_last_30d,
        count(distinct case when me.timestamp >= dateadd('day', -60, current_date())
            and me.timestamp < dateadd('day', -30, current_date())
            then me.event_id end) as events_prev_30d,
        count(distinct case when me.timestamp::date >= dateadd('day', -30, current_date())
            then me.timestamp::date end) as active_days_30d
    from {{ ref('stg_mixpanel_events') }} me
    group by me.distinct_id
),

-- Combine all signals into health score
health_calculation as (
    select
        im.master_customer_id,
        im.customer_name,
        
        -- Revenue health (30 points max)
        case 
            when cm.mrr_change_pct >= 20 then 30  -- Growing fast
            when cm.mrr_change_pct >= 10 then 25  -- Growing
            when cm.mrr_change_pct >= 0 then 20   -- Stable
            when cm.mrr_change_pct >= -10 then 10 -- Declining
            else 0                                 -- Declining fast
        end as revenue_health_score,
        
        -- Support health (25 points max)
        case 
            when st.tickets_last_30d = 0 then 25  -- No tickets = healthy
            when st.high_severity_tickets_30d > 0 then 5  -- Critical issues
            when st.tickets_last_30d > st.tickets_prev_30d then 10  -- Increasing
            when st.tickets_last_30d < st.tickets_prev_30d then 20  -- Decreasing
            else 15  -- Stable
        end as support_health_score,
        
        -- Payment health (25 points max)
        case 
            when pb.open_invoices_30d_plus_count = 0 and pb.late_paid_invoices_90d = 0 then 25
            when pb.open_invoices_30d_plus_count = 0 and pb.late_paid_invoices_90d > 0 then 20
            when pb.open_invoices_30d_plus_count = 1 then 10
            when pb.open_invoices_30d_plus_count >= 2 then 0
            else 25
        end as payment_health_score,
        
        -- Usage health (20 points max)
        case 
            when ut.events_last_30d > ut.events_prev_30d then 20  -- Increasing usage
            when ut.events_last_30d >= ut.events_prev_30d * 0.9 then 15  -- Stable
            when ut.events_last_30d >= ut.events_prev_30d * 0.5 then 10  -- Declining
            else 0  -- Significantly declining
        end as usage_health_score,
        
        -- Raw metrics for transparency
        cm.current_mrr as commercial_current_mrr,
        cm.mrr_change_pct as commercial_mrr_change_pct,
        gr.gaap_recognized_current_month_gbp,
        gr.gaap_recognized_prev_month_gbp,
        st.tickets_last_30d as support_tickets_30d,
        st.high_severity_tickets_30d,
        st.total_support_cost_30d as support_cost_30d,
        pb.open_invoices_count,
        pb.open_invoices_amount_gbp,
        pb.open_invoices_30d_plus_count,
        pb.open_invoices_30d_plus_amount_gbp,
        pb.max_open_invoice_age_days,
        pb.avg_days_to_pay_90d,
        pb.late_paid_invoices_90d,
        ut.events_last_30d as usage_events_30d,
        ut.active_days_30d as usage_days_30d
        
    from identity_map im
    left join commercial_mrr_trends cm on im.master_customer_id = cm.master_customer_id
    left join gaap_recognized_trends gr on im.master_customer_id = gr.master_customer_id
    left join support_trends st on im.master_customer_id = st.master_customer_id
    left join stripe_invoice_payment_behavior pb on im.master_customer_id = pb.master_customer_id
    left join usage_trends ut on im.master_customer_id = ut.master_customer_id
),

-- Calculate final health score and risk category
final as (
    select
        master_customer_id,
        customer_name,
        
        -- Composite health score (0-100)
        coalesce(revenue_health_score, 0) +
        coalesce(support_health_score, 0) +
        coalesce(payment_health_score, 0) +
        coalesce(usage_health_score, 0) as health_score,
        
        -- Component scores
        revenue_health_score,
        support_health_score,
        payment_health_score,
        usage_health_score,
        
        -- Risk categorization
        case 
            when (coalesce(revenue_health_score, 0) +
                  coalesce(support_health_score, 0) +
                  coalesce(payment_health_score, 0) +
                  coalesce(usage_health_score, 0)) <= 40 then 'CHURN_RISK'
            when (coalesce(revenue_health_score, 0) +
                  coalesce(support_health_score, 0) +
                  coalesce(payment_health_score, 0) +
                  coalesce(usage_health_score, 0)) <= 60 then 'AT_RISK'
            when (coalesce(revenue_health_score, 0) +
                  coalesce(support_health_score, 0) +
                  coalesce(payment_health_score, 0) +
                  coalesce(usage_health_score, 0)) <= 80 then 'HEALTHY'
            else 'EXPANSION_OPPORTUNITY'
        end as risk_category,
        
        -- Churn probability (simplified model)
        case 
            when (coalesce(revenue_health_score, 0) +
                  coalesce(support_health_score, 0) +
                  coalesce(payment_health_score, 0) +
                  coalesce(usage_health_score, 0)) <= 30 then 0.80
            when (coalesce(revenue_health_score, 0) +
                  coalesce(support_health_score, 0) +
                  coalesce(payment_health_score, 0) +
                  coalesce(usage_health_score, 0)) <= 50 then 0.40
            when (coalesce(revenue_health_score, 0) +
                  coalesce(support_health_score, 0) +
                  coalesce(payment_health_score, 0) +
                  coalesce(usage_health_score, 0)) <= 70 then 0.10
            else 0.01
        end as churn_probability,
        
        -- Raw metrics
        commercial_current_mrr,
        commercial_mrr_change_pct,
        gaap_recognized_current_month_gbp,
        gaap_recognized_prev_month_gbp,
        support_tickets_30d,
        high_severity_tickets_30d,
        support_cost_30d,
        open_invoices_count,
        open_invoices_amount_gbp,
        open_invoices_30d_plus_count,
        open_invoices_30d_plus_amount_gbp,
        max_open_invoice_age_days,
        avg_days_to_pay_90d,
        late_paid_invoices_90d,
        usage_events_30d,
        usage_days_30d,
        
        current_date() as score_calculated_at
        
    from health_calculation
    where master_customer_id is not null
)

select * from final
