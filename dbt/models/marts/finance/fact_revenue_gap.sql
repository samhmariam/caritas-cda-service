{{
  config(
    materialized='table',
    tags=['marts', 'finance', 'revenue_gap']
  )
}}

/*
    Fact: Revenue Gap (Booked vs Recognized)

    Goal: Visualize the discrepancy between Sales promises (Booked) and finance reality (Recognized).

    Booked:
      - Salesforce arr_booked_gbp, spread into expected monthly value (arr / 12)
      - Anchored to contract_start_date (and optionally contract_end_date)

    Recognized (operational cash/invoicing proxy):
      - Stripe invoice line items amount_gbp joined to invoices for customer + invoice_date

    Grain:
      - One row per (master_customer_id, revenue_month)
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

-- Salesforce expected monthly booked ARR by contract month
sf_opps as (
    select
        im.master_customer_id,
        so.opportunity_id,
        so.arr_booked_gbp,
        date_trunc('month', so.contract_start_date) as start_month,
        date_trunc(
            'month',
            coalesce(so.contract_end_date, dateadd('month', 12, so.contract_start_date))
        ) as end_month
    from {{ ref('stg_sf_opportunities') }} so
    left join identity_map im on so.account_id = im.account_id
    where im.master_customer_id is not null
      and so.stage = 'Closed Won'
      and so.arr_booked_gbp is not null
      and so.contract_start_date is not null
),

sf_months as (
    select
        so.master_customer_id,
        so.opportunity_id,
        dateadd('month', seq4(), so.start_month) as revenue_month,
        (so.arr_booked_gbp / 12.0) as expected_monthly_booked_gbp
    from sf_opps so
    join table(generator(rowcount => 240)) g
        on dateadd('month', seq4(), so.start_month) <= so.end_month
),

booked_monthly as (
    select
        master_customer_id,
        revenue_month,
        sum(expected_monthly_booked_gbp) as expected_monthly_booked_gbp
    from sf_months
    group by 1, 2
),

-- Stripe invoice line items by invoice month (actual)
stripe_actual_monthly as (
    select
        im.master_customer_id,
        date_trunc('month', si.invoice_date) as revenue_month,
        sum(coalesce(sili.amount_gbp, 0)) as actual_net_revenue_gbp
    from {{ ref('stg_stripe_invoice_line_items') }} sili
    join {{ ref('stg_stripe_invoices') }} si
        on sili.invoice_id = si.invoice_id
    left join identity_map im
        on si.customer_id = im.stripe_customer_id
    where im.master_customer_id is not null
      and si.invoice_date is not null
    group by 1, 2
),

month_spine as (
    select revenue_month from booked_monthly
    union
    select revenue_month from stripe_actual_monthly
),

activity_customer_month_spine as (
    -- Only emit customer-months that have booked or actual activity.
    select distinct
        master_customer_id,
        revenue_month
    from booked_monthly
    union
    select distinct
        master_customer_id,
        revenue_month
    from stripe_actual_monthly
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['cms.master_customer_id', 'cms.revenue_month']) }} as revenue_gap_id,
        cms.master_customer_id,
        im.customer_name,
        cms.revenue_month,

        coalesce(bm.expected_monthly_booked_gbp, 0) as expected_monthly_booked_gbp,
        coalesce(sam.actual_net_revenue_gbp, 0) as actual_net_revenue_gbp,

        (coalesce(bm.expected_monthly_booked_gbp, 0) - coalesce(sam.actual_net_revenue_gbp, 0)) as revenue_gap_gbp,

        current_timestamp() as dbt_loaded_at

     from activity_customer_month_spine cms
    left join identity_map im on cms.master_customer_id = im.master_customer_id
    left join booked_monthly bm
        on cms.master_customer_id = bm.master_customer_id
       and cms.revenue_month = bm.revenue_month
    left join stripe_actual_monthly sam
        on cms.master_customer_id = sam.master_customer_id
       and cms.revenue_month = sam.revenue_month

     where coalesce(bm.expected_monthly_booked_gbp, 0) <> 0
         or coalesce(sam.actual_net_revenue_gbp, 0) <> 0
)

select * from final
