{{
  config(
    materialized='view',
    tags=['intermediate', 'operations', 'efficiency']
  )
}}

/*
    Issue Resolution Chain - Ops Efficiency Tracking
    
    Purpose: Trace support tickets → engineering bugs → delivery work
    
    The "Golden Thread":
    ZENDESK_TICKET ↔ JIRA_ISSUE ↔ HARVEST_TIME
    
    Enables:
    - Root cause analysis: Which bugs drive the most support volume?
    - Total cost of an issue (support + engineering + delivery)
    - Resolution time tracking
    - High-impact bug identification for prioritization
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

ticket_lifecycle as (
    select
        ze.ticket_id,
        min(case when ze.event_type = 'comment' and ze.actor = 'agent' then ze.timestamp end) as ticket_first_response_at,
        -- Best-effort: events don't include the target status, so we treat the last agent status_change
        -- as the solved/closed time when the ticket currently appears solved/closed.
        max(case when ze.event_type = 'status_change' and ze.actor = 'agent' then ze.timestamp end) as last_agent_status_change_at
    from {{ ref('stg_zendesk_ticket_events') }} ze
    where ze.ticket_id is not null
    group by ze.ticket_id
),

-- Start with Zendesk tickets
zendesk_tickets as (
    select
        zt.ticket_id,
        im.master_customer_id,
        zt.created_at as ticket_created_at,
        tl.ticket_first_response_at,
        tl.last_agent_status_change_at as ticket_last_agent_status_change_at,
        case
            when zt.status in ('solved', 'closed') then tl.last_agent_status_change_at
            else null
        end as ticket_solved_at,
        case
            when zt.status = 'closed' then tl.last_agent_status_change_at
            else null
        end as ticket_closed_at,
        zt.status as ticket_status,
        zt.priority as ticket_priority,
        zt.severity as ticket_severity,
        zt.type as ticket_type,
        zt.channel,
        zt.subject,
        zt.assigned_group
    from {{ ref('stg_zendesk_tickets') }} zt
    left join identity_map im on zt.organization_id = im.zendesk_organization_id
    left join ticket_lifecycle tl on zt.ticket_id = tl.ticket_id
),

-- Link to Jira issues
jira_issues as (
    select
        ji.issue_id,
        ji.zendesk_ticket_id,
        ji.account_id,
        ji.summary as issue_summary,
        ji.issue_type,
        ji.status as issue_status,
        ji.priority as issue_priority,
        ji.story_points,
        ji.time_spent_hours as jira_time_spent_hours,
        ji.created_at as issue_created_at,
        ji.resolved_at as issue_resolved_at
    from {{ ref('stg_jira_issues') }} ji
    where ji.zendesk_ticket_id is not null  -- Only issues linked to support tickets
),

-- Get Harvest time entries linked to Jira issues
harvest_time as (
    select
        h.linked_jira_issue_id,
        sum(h.hours) as harvest_hours,
        sum(h.hours * h.cost_rate_gbp_per_hour) as harvest_cost_gbp
    from {{ ref('stg_harvest_time_entries') }} h
    where h.linked_jira_issue_id is not null
    group by h.linked_jira_issue_id
),

-- Get Zendesk support time spent on tickets
zendesk_time as (
    select
        zt.ticket_id,
        sum(zt.time_spent_minutes / 60.0) as support_handle_time_hours,
        -- Use same cost logic as unified cost model
        sum(
            case 
                when zt.tier = 1 then (zt.time_spent_minutes / 60.0) * {{ var('support_cost_rate_tier_1_gbp_per_hour', 25) }}
                when zt.tier = 2 then (zt.time_spent_minutes / 60.0) * {{ var('support_cost_rate_tier_2_gbp_per_hour', 35) }}
                when zt.tier = 3 then (zt.time_spent_minutes / 60.0) * {{ var('support_cost_rate_tier_3_gbp_per_hour', 50) }}
                else (zt.time_spent_minutes / 60.0) * {{ var('support_cost_rate_default_gbp_per_hour', 30) }}
            end
        ) as support_cost_gbp
    from {{ ref('stg_zendesk_time_entries') }} zt
    group by zt.ticket_id
),

-- Join everything together to create the resolution chain
resolution_chain as (
    select
        -- Zendesk ticket info
        zt.ticket_id as zendesk_ticket_id,
        zt.master_customer_id,
        zt.ticket_created_at,
        zt.ticket_first_response_at,
        zt.ticket_last_agent_status_change_at,
        zt.ticket_solved_at,
        zt.ticket_closed_at,
        zt.ticket_status,
        zt.ticket_priority,
        zt.ticket_severity,
        zt.ticket_type,
        zt.channel,
        zt.subject as ticket_subject,
        zt.assigned_group,
        
        -- Jira issue info
        ji.issue_id as jira_issue_id,
        ji.issue_summary,
        ji.issue_type,
        ji.issue_status,
        ji.issue_priority,
        ji.story_points,
        ji.issue_created_at,
        ji.issue_resolved_at,
        
        -- Time and cost aggregations
        coalesce(zt_time.support_handle_time_hours, 0) as support_handle_time_hours,
        coalesce(zt_time.support_cost_gbp, 0) as total_support_cost_gbp,
        coalesce(ji.jira_time_spent_hours, 0) as total_jira_hours,
        coalesce(ji.jira_time_spent_hours, 0) * {{ var('engineering_cost_rate_gbp_per_hour', 60) }} as total_jira_cost_gbp,
        coalesce(h.harvest_hours, 0) as total_harvest_hours,
        coalesce(h.harvest_cost_gbp, 0) as total_harvest_cost_gbp,
        
        -- Combined metrics
        coalesce(ji.jira_time_spent_hours, 0) + coalesce(h.harvest_hours, 0) as total_engineering_hours,
        coalesce(ji.jira_time_spent_hours * {{ var('engineering_cost_rate_gbp_per_hour', 60) }}, 0) + coalesce(h.harvest_cost_gbp, 0) as total_engineering_cost_gbp,
        
        -- Resolution timeline
        datediff('day', zt.ticket_created_at, ji.issue_resolved_at) as resolution_time_days,
        datediff('day', zt.ticket_created_at, ji.issue_created_at) as ticket_to_issue_days,
        datediff('day', ji.issue_created_at, ji.issue_resolved_at) as issue_resolution_days,
        datediff('day', zt.ticket_created_at, coalesce(zt.ticket_solved_at, current_timestamp())) as ticket_age_days,
        
        -- Flags
        case when ji.issue_id is not null then 1 else 0 end as has_linked_issue,
        case when h.linked_jira_issue_id is not null then 1 else 0 end as has_delivery_work,
        case when ji.issue_resolved_at is not null then 1 else 0 end as is_resolved
        
    from zendesk_tickets zt
    left join jira_issues ji on zt.ticket_id = ji.zendesk_ticket_id
    left join harvest_time h on ji.issue_id = h.linked_jira_issue_id
    left join zendesk_time zt_time on zt.ticket_id = zt_time.ticket_id
),

-- Generate surrogate key and calculate total cost
final as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "zendesk_ticket_id",
            "coalesce(jira_issue_id, 'NO_JIRA')"
        ]) }} as resolution_chain_id,
        *,
        -- Total cost across all systems
        total_support_cost_gbp + total_engineering_cost_gbp as total_cost_gbp,
        
        -- Categorize by resolution efficiency
        case 
            when resolution_time_days <= 7 then 'FAST'
            when resolution_time_days <= 30 then 'NORMAL'
            when resolution_time_days <= 90 then 'SLOW'
            else 'VERY_SLOW'
        end as resolution_speed_category,
        
        -- Impact score (combines severity, cost, and time)
        case 
            when ticket_severity = 'Critical' then 100
            when ticket_severity = 'High' then 75
            when ticket_severity = 'Medium' then 50
            when ticket_severity = 'Low' then 25
            else 0
        end + 
        least(total_cost_gbp / 10, 100) +  -- Cost component (capped at 100)
        least(resolution_time_days, 100)   -- Time component (capped at 100)
        as impact_score
        
    from resolution_chain
    where master_customer_id is not null  -- Only include tickets we can attribute to a customer
)

select * from final
