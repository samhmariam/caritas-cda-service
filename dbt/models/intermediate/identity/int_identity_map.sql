{{
  config(
    materialized='view',
    tags=['intermediate', 'identity', 'foundational']
  )
}}

/*
    Identity Map - Master Customer ID Resolution
    
    Purpose: Single source of truth for customer identity across all systems.
    Maps every system-specific ID to one master_customer_id.
    
    Strategy: Uses ground truth mappings (from customer_identity_map.csv) as the authoritative source.
    Ground truth mappings are embedded inline since seed loading requires additional permissions.
    
    Usage: All downstream models join to this table to get master_customer_id
*/

with ground_truth_mappings as (
    -- Ground truth from customer_identity_map.csv
    -- This is the authoritative source for known customer mappings
    select
        salesforce_account_id as master_customer_id,
        company_name as customer_name,
        salesforce_account_id as account_id,
        stripe_customer_id,
        intacct_customer_id,
        zendesk_organization_id,
        harvest_client_id,
        jira_account_key,
        mixpanel_company_id,
        'GROUND_TRUTH' as identity_source
    from (
        select * from (
            values
                ('Acme Corp', 'HV_2cb28fd8', 'INTACCT_ACME_CORP', 'JIRA_ACMECORP', 'mp_fa9d4042-19a', 'SF_ACME_CORP_e178ebcb', 'cus_781709dc-a198-', 'ZD_91565dc4-d'),
                ('Stellar Bank', 'HV_5a16569c', 'INTACCT_STELLAR_BANK', 'JIRA_STELLARB', 'mp_5f7915d5-d27', 'SF_STELLAR_BANK_41a7bb6c', 'cus_2222931c-c417-', 'ZD_b9718283-a'),
                ('TechFlow Solutions', 'HV_6842aeba', 'INTACCT_TECHFLOW_SOLUTIONS', 'JIRA_TECHFLOW', 'mp_dbc44671-b0e', 'SF_TECHFLOW_SOLUTIONS_6de886f2', 'cus_12e419f2-4e20-', 'ZD_022403d5-0'),
                ('Omega Retail', 'HV_23a5968e', 'INTACCT_OMEGA_RETAIL', 'JIRA_OMEGARET', 'mp_b12e0748-887', 'SF_OMEGA_RETAIL_f4c56c01', 'cus_5aaa4a8e-b1be-', 'ZD_5483d9c2-b'),
                ('London Pay', 'HV_ae95b118', 'INTACCT_LONDON_PAY', 'JIRA_LONDONPA', 'mp_bd83aa9e-3b5', 'SF_LONDON_PAY_39421110', 'cus_5d5a1b8b-5a3a-', 'ZD_aa4542bc-e')
        ) as t(company_name, harvest_client_id, intacct_customer_id, jira_account_key, mixpanel_company_id, salesforce_account_id, stripe_customer_id, zendesk_organization_id)
    )
),

-- Get additional Salesforce accounts not in ground truth
additional_sf_accounts as (
    select
        sf.account_id as master_customer_id,
        sf.account_name as customer_name,
        sf.account_id,
        null::varchar as stripe_customer_id,
        null::varchar as intacct_customer_id,
        null::varchar as zendesk_organization_id,
        null::varchar as harvest_client_id,
        null::varchar as jira_account_key,
        null::varchar as mixpanel_company_id,
        'SALESFORCE_ONLY' as identity_source
    from {{ ref('stg_sf_accounts') }} sf
        where sf.account_id is not null
            and not exists (
                    select 1
                    from ground_truth_mappings gtm
                    where gtm.account_id = sf.account_id
            )
),

-- Get additional Stripe customers not in ground truth
additional_stripe_customers as (
    select
        'STRIPE:' || sc.customer_id as master_customer_id,
        sc.company_name as customer_name,
        null::varchar as account_id,
        sc.customer_id as stripe_customer_id,
        null::varchar as intacct_customer_id,
        null::varchar as zendesk_organization_id,
        null::varchar as harvest_client_id,
        null::varchar as jira_account_key,
        null::varchar as mixpanel_company_id,
        'STRIPE_ONLY' as identity_source
    from {{ ref('stg_stripe_customers') }} sc
        where sc.customer_id is not null
            and not exists (
                    select 1
                    from ground_truth_mappings gtm
                    where gtm.stripe_customer_id = sc.customer_id
            )
),

-- Get additional Intacct customers not in ground truth
additional_intacct_customers as (
    select
        'INTACCT:' || ic.customer_id as master_customer_id,
        ic.customer_name as customer_name,
        null::varchar as account_id,
        null::varchar as stripe_customer_id,
        ic.customer_id as intacct_customer_id,
        null::varchar as zendesk_organization_id,
        null::varchar as harvest_client_id,
        null::varchar as jira_account_key,
        null::varchar as mixpanel_company_id,
        'INTACCT_ONLY' as identity_source
    from {{ ref('stg_intacct_customers') }} ic
        where ic.customer_id is not null
            and not exists (
                    select 1
                    from ground_truth_mappings gtm
                    where gtm.intacct_customer_id = ic.customer_id
            )
),

-- Get additional Zendesk organizations not in ground truth (try to link via account_id)
additional_zendesk_orgs as (
    select
        case
            when zo.account_id is not null then zo.account_id
            else 'ZENDESK:' || zo.organization_id
        end as master_customer_id,
        zo.name as customer_name,
        zo.account_id,
        null::varchar as stripe_customer_id,
        null::varchar as intacct_customer_id,
        zo.organization_id as zendesk_organization_id,
        null::varchar as harvest_client_id,
        null::varchar as jira_account_key,
        null::varchar as mixpanel_company_id,
        case when zo.account_id is not null then 'ZENDESK_LINKED' else 'ZENDESK_ONLY' end as identity_source
    from {{ ref('stg_zendesk_organizations') }} zo
        where zo.organization_id is not null
            and not exists (
                    select 1
                    from ground_truth_mappings gtm
                    where gtm.zendesk_organization_id = zo.organization_id
            )
),

-- Union all identity sources
all_identities as (
    select * from ground_truth_mappings
    union all
    select * from additional_sf_accounts
    union all
    select * from additional_stripe_customers
    union all
    select * from additional_intacct_customers
    union all
    select * from additional_zendesk_orgs
),

scored_identities as (
    select
        ai.*,
        case
            when ai.identity_source = 'GROUND_TRUTH' then 1
            when ai.identity_source = 'ZENDESK_LINKED' then 2
            when ai.identity_source = 'SALESFORCE_ONLY' then 3
            when ai.identity_source = 'STRIPE_ONLY' then 4
            when ai.identity_source = 'INTACCT_ONLY' then 5
            when ai.identity_source = 'ZENDESK_ONLY' then 6
            else 99
        end as identity_source_priority,
        (
            case when ai.account_id is not null then 1 else 0 end +
            case when ai.stripe_customer_id is not null then 1 else 0 end +
            case when ai.intacct_customer_id is not null then 1 else 0 end +
            case when ai.zendesk_organization_id is not null then 1 else 0 end +
            case when ai.harvest_client_id is not null then 1 else 0 end +
            case when ai.jira_account_key is not null then 1 else 0 end +
            case when ai.mixpanel_company_id is not null then 1 else 0 end
        ) as identifiers_populated
    from all_identities ai
),

canonical_identity as (
    select
        si.master_customer_id,
        si.account_id,
        si.stripe_customer_id,
        si.intacct_customer_id,
        si.zendesk_organization_id,
        si.harvest_client_id,
        si.jira_account_key,
        si.mixpanel_company_id,
        si.customer_name,
        si.identity_source
    from scored_identities si
    qualify row_number() over (
        partition by si.master_customer_id
        order by si.identity_source_priority asc, si.identifiers_populated desc, si.customer_name asc
    ) = 1
),

collapsed_identity as (
    select
        si.master_customer_id,
        max(si.account_id) as account_id,
        max(si.stripe_customer_id) as stripe_customer_id,
        max(si.intacct_customer_id) as intacct_customer_id,
        max(si.zendesk_organization_id) as zendesk_organization_id,
        max(si.harvest_client_id) as harvest_client_id,
        max(si.jira_account_key) as jira_account_key,
        max(si.mixpanel_company_id) as mixpanel_company_id,
        -- Name + source come from the best-available canonical record (lowest priority)
        ci.customer_name,
        ci.identity_source,
        -- Coverage / quality
        (
            case when max(si.account_id) is not null then 1 else 0 end +
            case when max(si.stripe_customer_id) is not null then 1 else 0 end +
            case when max(si.intacct_customer_id) is not null then 1 else 0 end +
            case when max(si.zendesk_organization_id) is not null then 1 else 0 end +
            case when max(si.harvest_client_id) is not null then 1 else 0 end +
            case when max(si.jira_account_key) is not null then 1 else 0 end +
            case when max(si.mixpanel_company_id) is not null then 1 else 0 end
        ) as source_system_count,
        max(case when si.identity_source = 'GROUND_TRUTH' then 1 else 0 end) = 1 as has_ground_truth,
        (
            case when count(distinct si.account_id) > 1 then 1 else 0 end +
            case when count(distinct si.stripe_customer_id) > 1 then 1 else 0 end +
            case when count(distinct si.intacct_customer_id) > 1 then 1 else 0 end +
            case when count(distinct si.zendesk_organization_id) > 1 then 1 else 0 end +
            case when count(distinct si.harvest_client_id) > 1 then 1 else 0 end +
            case when count(distinct si.jira_account_key) > 1 then 1 else 0 end +
            case when count(distinct si.mixpanel_company_id) > 1 then 1 else 0 end
        ) > 0 as has_conflicts
    from scored_identities si
    inner join canonical_identity ci
        on si.master_customer_id = ci.master_customer_id
    group by
        si.master_customer_id,
        ci.customer_name,
        ci.identity_source
),

-- Create final identity map with consolidated records and timestamps
final as (
    select
        ci.master_customer_id,
        ci.account_id,
        ci.stripe_customer_id,
        ci.intacct_customer_id,
        ci.zendesk_organization_id,
        ci.harvest_client_id,
        ci.jira_account_key,
        ci.mixpanel_company_id,
        ci.customer_name,
        ci.identity_source,
        ci.source_system_count,
        ci.has_ground_truth,
        ci.has_conflicts,
        -- Null-safe least/greatest across known system create dates
        nullif(
            least(
                coalesce(sf.created_date, '9999-12-31'::date),
                coalesce(sc.created_at::date, '9999-12-31'::date),
                coalesce(ic.created_date, '9999-12-31'::date),
                coalesce(zo.created_at::date, '9999-12-31'::date)
            ),
            '9999-12-31'::date
        ) as first_seen_at,
        nullif(
            greatest(
                coalesce(sf.created_date, '0001-01-01'::date),
                coalesce(sc.created_at::date, '0001-01-01'::date),
                coalesce(ic.created_date, '0001-01-01'::date),
                coalesce(zo.created_at::date, '0001-01-01'::date)
            ),
            '0001-01-01'::date
        ) as last_seen_at
    from collapsed_identity ci
    left join {{ ref('stg_sf_accounts') }} sf on ci.account_id = sf.account_id
    left join {{ ref('stg_stripe_customers') }} sc on ci.stripe_customer_id = sc.customer_id
    left join {{ ref('stg_intacct_customers') }} ic on ci.intacct_customer_id = ic.customer_id
    left join {{ ref('stg_zendesk_organizations') }} zo on ci.zendesk_organization_id = zo.organization_id
)

select * from final
