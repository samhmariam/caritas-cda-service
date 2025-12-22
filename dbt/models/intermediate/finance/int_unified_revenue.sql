{{
  config(
    materialized='view',
    tags=['intermediate', 'finance', 'revenue']
  )
}}

/*
    Unified Revenue Model - Cash vs GAAP Revenue
    
    Purpose: Distinguish between "Cash in Bank" (Stripe payments) and "GAAP Revenue" (Intacct).
    
     Revenue Types:
     1. CASH (Stripe) - Cash movement events
         - Paid invoices (cash in)
         - Refunds (cash out)
         - Chargebacks / disputes (cash out)

     2. AR (Stripe) - Accounts receivable (not cash)
         - Open invoices

     3. GAAP (Intacct) - Revenue recognition per accounting rules
         - Recognized revenue
         - Deferred revenue
    
    Enables: Cash flow analysis, revenue reconciliation, ARR tracking, churn calculation
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

stripe_charges as (
    select * from {{ ref('stg_stripe_charges') }}
),

-- Stripe Invoices: CASH when invoices are paid
stripe_invoice_cash as (
    select
        si.invoice_id as source_record_id,
        im.master_customer_id,
        si.paid_at::date as revenue_date,
        'CASH' as revenue_type,
        'INVOICE' as revenue_category,
        si.amount_paid_gbp as amount_gbp,
        si.amount_paid_gbp as amount_gbp_signed,
        'CREDIT' as event_direction,
        'PAID' as cash_status,
        null::number as mrr_gbp,
        null::number as arr_gbp,
        si.status as payment_status,
        si.subscription_id,
        si.invoice_id,
        null::varchar as charge_id,
        'STRIPE' as source_system
    from {{ ref('stg_stripe_invoices') }} si
    left join identity_map im on si.customer_id = im.stripe_customer_id
    where si.status = 'paid'
      and si.paid_at is not null
),

-- Stripe Invoices: AR when invoices are open (not cash)
stripe_invoice_ar as (
    select
        si.invoice_id as source_record_id,
        im.master_customer_id,
        si.invoice_date as revenue_date,
        'AR' as revenue_type,
        'INVOICE' as revenue_category,
        si.amount_due_gbp as amount_gbp,
        si.amount_due_gbp as amount_gbp_signed,
        'CREDIT' as event_direction,
        'OUTSTANDING' as cash_status,
        null::number as mrr_gbp,
        null::number as arr_gbp,
        si.status as payment_status,
        si.subscription_id,
        si.invoice_id,
        null::varchar as charge_id,
        'STRIPE' as source_system
    from {{ ref('stg_stripe_invoices') }} si
    left join identity_map im on si.customer_id = im.stripe_customer_id
    where si.status = 'open'
),

-- Stripe Refunds: CASH out
stripe_refund_cash as (
    select
        sr.refund_id as source_record_id,
        im.master_customer_id,
        sr.created_at::date as revenue_date,
        'CASH' as revenue_type,
        'REFUND' as revenue_category,
        abs(sr.amount_gbp) as amount_gbp,
        -abs(sr.amount_gbp) as amount_gbp_signed,
        'DEBIT' as event_direction,
        'SETTLED' as cash_status,
        null::number as mrr_gbp,
        null::number as arr_gbp,
        coalesce(sr.reason, 'REFUND') as payment_status,
        null::varchar as subscription_id,
        sc.invoice_id as invoice_id,
        sr.charge_id as charge_id,
        'STRIPE' as source_system
    from {{ ref('stg_stripe_refunds') }} sr
    left join stripe_charges sc on sr.charge_id = sc.charge_id
    left join identity_map im on sc.customer_id = im.stripe_customer_id
    where sr.amount_gbp is not null
),

-- Stripe Disputes / Chargebacks: CASH out (best-effort; model only final losses to avoid double-counting)
stripe_dispute_cash as (
    select
        sd.dispute_id as source_record_id,
        im.master_customer_id,
        sd.created_at::date as revenue_date,
        'CASH' as revenue_type,
        'CHARGEBACK' as revenue_category,
        abs(sd.amount_gbp) as amount_gbp,
        -abs(sd.amount_gbp) as amount_gbp_signed,
        'DEBIT' as event_direction,
        'SETTLED' as cash_status,
        null::number as mrr_gbp,
        null::number as arr_gbp,
        coalesce(sd.status, 'DISPUTE') as payment_status,
        null::varchar as subscription_id,
        sc.invoice_id as invoice_id,
        sd.charge_id as charge_id,
        'STRIPE' as source_system
    from {{ ref('stg_stripe_disputes') }} sd
    left join stripe_charges sc on sd.charge_id = sc.charge_id
    left join identity_map im on sc.customer_id = im.stripe_customer_id
    where sd.amount_gbp is not null
      and sd.status = 'lost'
),

-- Intacct Revenue Recognition: GAAP revenue per accounting rules
intacct_gaap_revenue as (
    select
        ir.revrec_id as source_record_id,
        im.master_customer_id,
        try_to_date(ir.month || '-01') as revenue_date,
        'GAAP' as revenue_type,
        'RECOGNIZED' as revenue_category,
        ir.recognized_revenue_gbp as amount_gbp,
        ir.recognized_revenue_gbp as amount_gbp_signed,
        'CREDIT' as event_direction,
        null::varchar as cash_status,
        ir.recognized_revenue_gbp as mrr_gbp,
        ir.booked_arr_gbp as arr_gbp,
        'RECOGNIZED' as payment_status,
        null::varchar as subscription_id,
        null::varchar as invoice_id,
        null::varchar as charge_id,
        'INTACCT' as source_system
    from {{ ref('stg_intacct_revenue_recognition') }} ir
    left join identity_map im on ir.customer_id = im.intacct_customer_id
    where ir.recognized_revenue_gbp is not null
),

-- Intacct Deferred Revenue: Revenue not yet recognized
intacct_deferred_revenue as (
    select
        ir.revrec_id || '_deferred' as source_record_id,
        im.master_customer_id,
        try_to_date(ir.month || '-01') as revenue_date,
        'GAAP' as revenue_type,
        'DEFERRED' as revenue_category,
        ir.deferred_revenue_gbp as amount_gbp,
        ir.deferred_revenue_gbp as amount_gbp_signed,
        'CREDIT' as event_direction,
        null::varchar as cash_status,
        null::number as mrr_gbp,
        null::number as arr_gbp,
        'DEFERRED' as payment_status,
        null::varchar as subscription_id,
        null::varchar as invoice_id,
        null::varchar as charge_id,
        'INTACCT' as source_system
    from {{ ref('stg_intacct_revenue_recognition') }} ir
    left join identity_map im on ir.customer_id = im.intacct_customer_id
    where ir.deferred_revenue_gbp is not null
      and ir.deferred_revenue_gbp > 0
),

-- Union all revenue sources
all_revenue as (
    select * from stripe_invoice_cash
    union all
    select * from stripe_invoice_ar
    union all
    select * from stripe_refund_cash
    union all
    select * from stripe_dispute_cash
    union all
    select * from intacct_gaap_revenue
    union all
    select * from intacct_deferred_revenue
),

-- Generate surrogate key and add final enrichments
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['source_system', 'source_record_id']) }} as revenue_event_id,
        master_customer_id,
        revenue_date,
        revenue_type,
        revenue_category,
        amount_gbp,
        amount_gbp_signed,
        event_direction,
        cash_status,
        mrr_gbp,
        arr_gbp,
        payment_status,
        subscription_id,
        invoice_id,
        charge_id,
        source_system,
        source_record_id,
        -- Add date dimensions for easier filtering
        date_trunc('month', revenue_date) as revenue_month,
        date_trunc('quarter', revenue_date) as revenue_quarter,
        extract(year from revenue_date) as revenue_year,
        -- Add helpful flags
        case when revenue_type = 'CASH' then 1 else 0 end as is_cash_revenue,
        case when revenue_type = 'GAAP' then 1 else 0 end as is_gaap_revenue,
        case when revenue_type = 'AR' then 1 else 0 end as is_accounts_receivable,
        case when event_direction = 'DEBIT' then 1 else 0 end as is_cash_outflow,
        case when event_direction = 'CREDIT' then 1 else 0 end as is_cash_inflow,
        case when revenue_category = 'RECOGNIZED' then 1 else 0 end as is_recurring
    from all_revenue
    where master_customer_id is not null  -- Only include revenue we can attribute to a customer
)

select * from final
