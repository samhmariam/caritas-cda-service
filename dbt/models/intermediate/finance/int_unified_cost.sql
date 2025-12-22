{{
  config(
    materialized='view',
    tags=['intermediate', 'finance', 'cost']
  )
}}

/*
    Unified Cost Model - True Cost to Serve
    
    Purpose: Standardize ALL cost/effort activities into a single schema.
    Combines three types of work:
    1. DELIVERY work (Harvest time entries - billable consulting/dev)
    2. SUPPORT work (Zendesk time entries - customer success)
    3. ENGINEERING work (Jira issues - product development)
    
    Enables: True cost to serve by customer, profitability analysis, resource allocation
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

jira_issue_customer as (
    select
        ji.issue_id as linked_jira_issue_id,
        im.master_customer_id
    from {{ ref('stg_jira_issues') }} ji
    left join identity_map im on ji.account_id = im.jira_account_key
    where ji.issue_id is not null
),

-- Harvest: Delivery work (consulting, professional services, implementation)
harvest_costs as (
    select
        h.entry_id as source_record_id,
        coalesce(
            im_proj.master_customer_id,
            jic.master_customer_id
        ) as master_customer_id,
        h.date as activity_date,
        h.person as person_name,
        h.role as role_or_tier,
        h.hours,
        h.hours * h.cost_rate_gbp_per_hour as cost_gbp,
        'DELIVERY' as work_type,
        case 
            when h.billable_rate_gbp_per_hour > 0 then 'BILLABLE'
            else 'NON_BILLABLE'
        end as work_category,
        null::varchar as linked_ticket_id,
        h.linked_jira_issue_id as linked_issue_id,
        'HARVEST' as source_system,
        h.notes
    from {{ ref('stg_harvest_time_entries') }} h
    left join {{ ref('stg_harvest_projects') }} p on h.project_id = p.project_id
    left join identity_map im_proj on p.client_id = im_proj.harvest_client_id
    -- Fallback: match via Jira issue linkage
    left join jira_issue_customer jic on h.linked_jira_issue_id = jic.linked_jira_issue_id
),

-- Zendesk: Support work (customer success, technical support)
zendesk_costs as (
    select
        zt.time_entry_id as source_record_id,
        im.master_customer_id,
        zt.date as activity_date,
        zt.agent_id as person_name,
        'Support Tier ' || coalesce(zt.tier::varchar, 'Unknown') as role_or_tier,
        zt.time_spent_minutes / 60.0 as hours,
        -- Cost rate by tier (parameterized; override via vars)
        case 
            when zt.tier = 1 then (zt.time_spent_minutes / 60.0) * {{ var('support_cost_rate_tier_1_gbp_per_hour', 25) }}
            when zt.tier = 2 then (zt.time_spent_minutes / 60.0) * {{ var('support_cost_rate_tier_2_gbp_per_hour', 35) }}
            when zt.tier = 3 then (zt.time_spent_minutes / 60.0) * {{ var('support_cost_rate_tier_3_gbp_per_hour', 50) }}
            else (zt.time_spent_minutes / 60.0) * {{ var('support_cost_rate_default_gbp_per_hour', 30) }}
        end as cost_gbp,
        'SUPPORT' as work_type,
        coalesce(zt.work_type, 'GENERAL_SUPPORT') as work_category,
        zt.ticket_id as linked_ticket_id,
        null::varchar as linked_issue_id,
        'ZENDESK' as source_system,
        null::varchar as notes
    from {{ ref('stg_zendesk_time_entries') }} zt
    left join {{ ref('stg_zendesk_tickets') }} zti on zt.ticket_id = zti.ticket_id
    left join identity_map im on zti.organization_id = im.zendesk_organization_id
),

-- Jira: Engineering work (bugs, features, technical debt)
jira_costs as (
    select
        ji.issue_id as source_record_id,
        im.master_customer_id,
        ji.created_at::date as activity_date,
        null::varchar as person_name,  -- Jira doesn't track individual engineers in this schema
        'Engineer' as role_or_tier,
        coalesce(ji.time_spent_hours, 0) as hours,
        -- Cost for engineering work (parameterized; override via vars)
        coalesce(ji.time_spent_hours, 0) * {{ var('engineering_cost_rate_gbp_per_hour', 60) }} as cost_gbp,
        'ENGINEERING' as work_type,
        ji.issue_type as work_category,
        ji.zendesk_ticket_id as linked_ticket_id,
        ji.issue_id as linked_issue_id,
        'JIRA' as source_system,
        ji.summary as notes
    from {{ ref('stg_jira_issues') }} ji
    left join identity_map im on ji.account_id = im.jira_account_key
),

-- Union all cost sources
all_costs as (
    select * from harvest_costs
    union all
    select * from zendesk_costs
    union all
    select * from jira_costs
),

-- Generate surrogate key and add final enrichments
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['source_system', 'source_record_id']) }} as cost_event_id,
        master_customer_id,
        activity_date,
        person_name,
        role_or_tier,
        hours,
        cost_gbp,
        work_type,
        work_category,
        linked_ticket_id,
        linked_issue_id,
        source_system,
        source_record_id,
        notes,
        -- Add date dimensions for easier filtering
        date_trunc('month', activity_date) as activity_month,
        date_trunc('quarter', activity_date) as activity_quarter,
        extract(year from activity_date) as activity_year
    from all_costs
    where master_customer_id is not null  -- Only include costs we can attribute to a customer
)

select * from final
