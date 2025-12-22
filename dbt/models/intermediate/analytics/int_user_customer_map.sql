{{
  config(
    materialized='view',
    tags=['intermediate', 'analytics', 'identity', 'mixpanel']
  )
}}

/*
    User → Customer Map (Mixpanel)

    Purpose: Map Mixpanel user identifiers (distinct_id) to master_customer_id.

    Mapping priority:
    1) Explicit company identifier: stg_mixpanel_events.company_id → int_identity_map.mixpanel_company_id (HIGH)
    2) Fallback: stg_mixpanel_events.company_id → int_identity_map.account_id (MEDIUM)

    Notes:
    - If a user is observed under multiple company_ids, we choose the highest-confidence mapping.
    - Unmapped users are excluded from this model to avoid downstream join failures.
*/

with mixpanel_events as (
    select
        distinct_id as user_identifier,
        company_id,
        min(timestamp) as first_seen_at,
        max(timestamp) as last_seen_at,
        count(*) as event_row_count
    from {{ ref('stg_mixpanel_events') }}
    where distinct_id is not null
    group by 1, 2
),

identity_map as (
    select * from {{ ref('int_identity_map') }}
),

candidates as (
    select
        me.user_identifier,
        me.company_id,
        im.master_customer_id,
        case
            when im.mixpanel_company_id = me.company_id then 'MIXPANEL_COMPANY_ID'
            when im.account_id = me.company_id then 'SALESFORCE_ACCOUNT_ID'
            else 'UNMAPPED'
        end as mapping_method,
        case
            when im.mixpanel_company_id = me.company_id then 'HIGH'
            when im.account_id = me.company_id then 'MEDIUM'
            else 'NONE'
        end as mapping_confidence,
        case
            when im.mixpanel_company_id = me.company_id then 2
            when im.account_id = me.company_id then 1
            else 0
        end as mapping_score,
        me.first_seen_at,
        me.last_seen_at,
        me.event_row_count
    from mixpanel_events me
    left join identity_map im
        on (im.mixpanel_company_id = me.company_id or im.account_id = me.company_id)
),

ranked as (
    select
        user_identifier,
        company_id,
        master_customer_id,
        mapping_method,
        mapping_confidence,
        first_seen_at,
        last_seen_at,
        event_row_count,
        count(distinct case when mapping_score > 0 then master_customer_id end) over (partition by user_identifier) as mapped_customer_count,
        count(distinct company_id) over (partition by user_identifier) as company_id_count,
        case
            when count(distinct case when mapping_score > 0 then master_customer_id end) over (partition by user_identifier) > 1 then 1
            else 0
        end as has_conflict,
        row_number() over (
            partition by user_identifier
            order by mapping_score desc, last_seen_at desc, event_row_count desc
        ) as mapping_rank
    from candidates
)

select
    user_identifier,
    company_id,
    master_customer_id,
    mapping_method,
    mapping_confidence,
    has_conflict,
    mapped_customer_count,
    company_id_count,
    first_seen_at,
    last_seen_at,
    current_timestamp() as mapping_calculated_at
from ranked
where mapping_rank = 1
  and master_customer_id is not null
