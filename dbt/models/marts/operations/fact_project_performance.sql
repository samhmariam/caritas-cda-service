{{
  config(
    materialized='table',
    tags=['marts', 'operations', 'project_performance']
  )
}}

/*
    Fact: Project Performance / Delivery Efficiency

    Target audience: COO / VP of Professional Services
    Business question: Are our fixed-price implementation projects profitable?

    Grain:
      - One row per Harvest project (project_id) and customer

    Sold value proxy:
      - stg_sf_opportunities.arr_booked_gbp is used as the only available commercial "sold value" field in this dataset.
        If you have a dedicated implementation fee / services amount, replace project_budget_gbp with that field.

    Burned cost:
      - Harvest time entry cost (hours * cost_rate_gbp_per_hour)

    Completion proxy:
      - Time-elapsed vs Salesforce contract window (contract_start_date → contract_end_date)
*/

with identity_map as (
    select * from {{ ref('int_identity_map') }}
),

harvest_projects as (
    select * from {{ ref('stg_harvest_projects') }}
),

harvest_time as (
    select * from {{ ref('stg_harvest_time_entries') }}
),

project_costs as (
    select
        p.project_id,
        p.client_id as harvest_client_id,
        im.master_customer_id,
        im.customer_name,
        p.name as project_name,

        min(h.date) as project_start_date,
        max(h.date) as project_last_activity_date,
        sum(coalesce(h.hours, 0)) as total_hours_to_date,
        sum(coalesce(h.hours, 0) * coalesce(h.cost_rate_gbp_per_hour, 0)) as actual_cost_to_date_gbp

    from harvest_projects p
    left join harvest_time h on p.project_id = h.project_id
    left join identity_map im on p.client_id = im.harvest_client_id

    where p.project_id is not null
      and im.master_customer_id is not null

    group by 1, 2, 3, 4, 5
),

latest_closed_won_opportunity as (
    select
        im.master_customer_id,
        so.opportunity_id,
        so.arr_booked_gbp,
        so.contract_start_date,
        so.contract_end_date,
        row_number() over (
            partition by im.master_customer_id
            order by coalesce(so.contract_start_date, so.close_date) desc, so.close_date desc
        ) as opp_rank
    from {{ ref('stg_sf_opportunities') }} so
    left join identity_map im on so.account_id = im.account_id
    where so.stage = 'Closed Won'
      and im.master_customer_id is not null
),

opp as (
    select
        master_customer_id,
        opportunity_id,
        arr_booked_gbp as project_budget_gbp,
        contract_start_date,
        contract_end_date
    from latest_closed_won_opportunity
    where opp_rank = 1
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['pc.master_customer_id', 'pc.project_id']) }} as project_performance_id,
        pc.master_customer_id,
        pc.customer_name,
        pc.project_id as harvest_project_id,
        pc.project_name,

        -- Sold value / budget
        coalesce(o.project_budget_gbp, 0) as project_budget_gbp,

        -- Burned cost
        coalesce(pc.actual_cost_to_date_gbp, 0) as actual_cost_to_date_gbp,

        case
            when coalesce(o.project_budget_gbp, 0) > 0
            then (coalesce(pc.actual_cost_to_date_gbp, 0) / o.project_budget_gbp)
            else null
        end as budget_utilization_pct,

        -- Timeline / completion proxy
        o.contract_start_date,
        o.contract_end_date,
        pc.project_start_date,
        pc.project_last_activity_date,

        case
            when o.contract_start_date is not null and coalesce(o.contract_end_date, o.contract_start_date) > o.contract_start_date
            then least(
                1,
                greatest(
                    0,
                    datediff('day', o.contract_start_date, current_date())
                    /
                    nullif(datediff('day', o.contract_start_date, o.contract_end_date), 0)
                )
            )
            else null
        end as project_completion_pct,

        -- Burn-rate forecast
        case
            when pc.project_start_date is not null and datediff('day', pc.project_start_date, current_date()) > 0
            then coalesce(pc.actual_cost_to_date_gbp, 0) / datediff('day', pc.project_start_date, current_date())
            else null
        end as burn_rate_gbp_per_day,

        case
            when pc.project_start_date is not null
             and datediff('day', pc.project_start_date, current_date()) > 0
             and o.contract_start_date is not null
             and coalesce(o.contract_end_date, o.contract_start_date) > o.contract_start_date
            then (
                (coalesce(pc.actual_cost_to_date_gbp, 0) / datediff('day', pc.project_start_date, current_date()))
                * datediff('day', o.contract_start_date, o.contract_end_date)
            )
            else null
        end as projected_total_cost_gbp,

        case
            when coalesce(o.project_budget_gbp, 0) > 0
             and pc.project_start_date is not null
             and datediff('day', pc.project_start_date, current_date()) > 0
             and o.contract_start_date is not null
             and coalesce(o.contract_end_date, o.contract_start_date) > o.contract_start_date
            then (
                (coalesce(pc.actual_cost_to_date_gbp, 0) / datediff('day', pc.project_start_date, current_date()))
                * datediff('day', o.contract_start_date, o.contract_end_date)
            ) - o.project_budget_gbp
            else null
        end as projected_overrun_gbp,

        -- Alert
        case
            when coalesce(o.project_budget_gbp, 0) > 0
             and (coalesce(pc.actual_cost_to_date_gbp, 0) / o.project_budget_gbp) > 0.80
             and coalesce(
                    case
                        when o.contract_start_date is not null and coalesce(o.contract_end_date, o.contract_start_date) > o.contract_start_date
                        then least(
                            1,
                            greatest(
                                0,
                                datediff('day', o.contract_start_date, current_date())
                                /
                                nullif(datediff('day', o.contract_start_date, o.contract_end_date), 0)
                            )
                        )
                        else null
                    end,
                    1
                ) < 0.50
            then true
            else false
        end as is_at_risk,

        current_timestamp() as dbt_loaded_at

    from project_costs pc
    left join opp o on pc.master_customer_id = o.master_customer_id
)

select * from final
