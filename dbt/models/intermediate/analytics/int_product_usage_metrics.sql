{{
  config(
    materialized='view',
    tags=['intermediate', 'analytics', 'usage']
  )
}}

/*
    Product Usage Metrics - Engagement & Adoption
    
    Purpose: Daily product engagement metrics by customer for product-led growth
    
    Key Metrics:
    - DAU/MAU (Daily/Monthly Active Users)
    - Feature adoption rates
    - Session metrics
    - Engagement scores
    - Usage trends
    
    Note: This aggregates Mixpanel events to customer-level insights
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

user_customer_map as (
    select * from {{ ref('int_user_customer_map') }}
),

params as (
    select
        current_date() as as_of_date,
        dateadd('day', -120, current_date()) as start_date
),

date_spine as (
    -- Daily spine to support correct rolling-window calculations
    select
        dateadd('day', -seq4(), (select as_of_date from params))::date as event_date
    from table(generator(rowcount => 121))
),

customer_spine as (
    select distinct
        ucm.master_customer_id
    from user_customer_map ucm
),

customer_dates as (
    select
        cs.master_customer_id,
        ds.event_date
    from customer_spine cs
    cross join date_spine ds
),

mapped_events as (
    select
        ucm.master_customer_id,
        me.distinct_id as user_identifier,
        me.event_id,
        me.event_name,
        me.timestamp,
        me.timestamp::date as event_date
    from {{ ref('stg_mixpanel_events') }} me
    left join user_customer_map ucm
        on me.distinct_id = ucm.user_identifier
    where ucm.master_customer_id is not null
      and me.timestamp is not null
      and me.timestamp::date >= (select start_date from params)
),

customer_daily_event_totals as (
    select
        master_customer_id,
        event_date,
        count(distinct event_id) as daily_events,
        count(distinct event_name) as daily_unique_event_types,
        count(distinct case
            when event_name ilike '%purchase%'
              or event_name ilike '%upgrade%'
            then event_id end
        ) as daily_key_actions,
        count(distinct user_identifier) as daily_active_users
    from mapped_events
    group by 1, 2
),

customer_daily_active_user_ids as (
    select distinct
        master_customer_id,
        event_date,
        user_identifier
    from mapped_events
),

customer_daily_with_spine as (
    select
        cd.master_customer_id,
        cd.event_date,
        coalesce(dt.daily_events, 0) as daily_events,
        coalesce(dt.daily_unique_event_types, 0) as daily_unique_event_types,
        coalesce(dt.daily_key_actions, 0) as daily_key_actions,
        coalesce(dt.daily_active_users, 0) as daily_active_users
    from customer_dates cd
    left join customer_daily_event_totals dt
        on cd.master_customer_id = dt.master_customer_id
       and cd.event_date = dt.event_date
),

-- Monthly active users (rolling 30d distinct users) per customer per day
customer_mau_rolling_30d as (
    select
        base.master_customer_id,
        base.event_date,
        count(distinct au.user_identifier) as monthly_active_users_30d
    from customer_daily_with_spine base
    left join customer_daily_active_user_ids au
        on base.master_customer_id = au.master_customer_id
       and au.event_date between dateadd('day', -29, base.event_date) and base.event_date
    group by 1, 2
),

customer_rolling_30d_metrics as (
    select
        master_customer_id,
        event_date,
        sum(daily_events) over (
            partition by master_customer_id
            order by event_date
            rows between 29 preceding and current row
        ) as total_events_30d,
        sum(daily_key_actions) over (
            partition by master_customer_id
            order by event_date
            rows between 29 preceding and current row
        ) as key_actions_30d,
        sum(case when daily_events > 0 then 1 else 0 end) over (
            partition by master_customer_id
            order by event_date
            rows between 29 preceding and current row
        ) as active_days_30d,
        sum(daily_events) over (
            partition by master_customer_id
            order by event_date
            rows between 59 preceding and 30 preceding
        ) as total_events_prev_30d
    from customer_daily_with_spine
),

customer_usage_daily as (
    select
        cd.master_customer_id,
        cd.event_date,
        cd.daily_active_users as dau,
        coalesce(mau.monthly_active_users_30d, 0) as mau,
        case
            when coalesce(mau.monthly_active_users_30d, 0) > 0
            then cd.daily_active_users::float / mau.monthly_active_users_30d
            else 0
        end as stickiness_ratio,
        cd.daily_events,
        cd.daily_unique_event_types,
        cd.daily_key_actions,
        r30.active_days_30d,
        r30.total_events_30d,
        r30.key_actions_30d,
        case
            when r30.active_days_30d > 0 then r30.total_events_30d::float / r30.active_days_30d
            else 0
        end as avg_events_per_active_day_30d,
        case
            when coalesce(r30.total_events_prev_30d, 0) > 0
            then ((r30.total_events_30d - r30.total_events_prev_30d)::float / r30.total_events_prev_30d) * 100
            else 0
        end as usage_trend_pct_30d
    from customer_daily_with_spine cd
    left join customer_mau_rolling_30d mau
        on cd.master_customer_id = mau.master_customer_id
       and cd.event_date = mau.event_date
    left join customer_rolling_30d_metrics r30
        on cd.master_customer_id = r30.master_customer_id
       and cd.event_date = r30.event_date
),

-- Aggregate metrics (summarized view)
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['cud.master_customer_id', 'cud.event_date']) }} as usage_metrics_id,
        cud.master_customer_id,
        im.customer_name,
        cud.event_date,

        -- Activity counts
        cud.dau as daily_active_users,
        cud.mau as monthly_active_users_30d,
        cud.active_days_30d,
        cud.daily_events,
        cud.total_events_30d,
        cud.daily_key_actions,
        cud.key_actions_30d,

        -- Engagement metrics
        round(cud.stickiness_ratio, 4) as stickiness_ratio,
        round(cud.avg_events_per_active_day_30d, 2) as avg_events_per_active_day_30d,
        round(cud.usage_trend_pct_30d, 1) as usage_trend_pct_30d,

        -- Flags
        case when cud.dau > 0 then 1 else 0 end as is_daily_active,
        case when cud.mau > 0 then 1 else 0 end as is_monthly_active,

        current_timestamp() as metrics_calculated_at

    from customer_usage_daily cud
    left join identity_map im on cud.master_customer_id = im.master_customer_id
    where cud.master_customer_id is not null
)

select * from final
