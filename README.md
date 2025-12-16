# Caritas Customer Data Activation Service

Multi-tenant template repository for deploying managed Customer Data Activation as a service.

## Architecture

```
Airbyte Cloud → S3 Raw → S3 Bronze → Snowflake (Iceberg) → dbt (Silver/Gold) → Census/Power BI
                         ↑
                    Dagster orchestrates entire pipeline
```

## Tech Stack

| Layer | Tool |
|-------|------|
| **Ingestion** | Airbyte Cloud |
| **Storage** | AWS S3 (Bronze/Silver) |
| **Warehouse** | Snowflake (EU region, Iceberg tables) |
| **Transformation** | dbt Core |
| **Orchestration** | Dagster Cloud Serverless |
| **Activation** | Census |
| **Analytics** | Power BI |
| **IaC** | Terraform |
| **Secrets** | AWS Secrets Manager |

## Repository Structure

```
├── clients/              # Client-specific configs (acme, stellar_bank, etc.)
├── terraform/            # IaC modules (AWS S3, Snowflake, secrets)
├── dbt/                  # Transformation models (staging → marts)
├── dagster/              # Orchestration (jobs, schedules, sensors)
├── docs/                 # Runbooks & architecture docs
└── scripts/              # Onboarding & utility scripts
```

## Quick Start

### Prerequisites

- AWS CLI configured with appropriate credentials
- Terraform >= 1.6
- Python >= 3.10
- dbt-core >= 1.7
- Snowflake account (EU region)

### Onboard a New Client

```bash
# 1. Run the onboarding script
./scripts/onboard_client.sh stellar_bank

# 2. Edit client config
vim clients/stellar_bank/config/dev.yml

# 3. Apply infrastructure
cd terraform/environments/dev
terraform init
terraform plan
terraform apply

# 4. Run initial dbt build
cd ../../../dbt
dbt deps
dbt build --select client:stellar_bank

# 5. Deploy Dagster
cd ../dagster
dagster-cloud deploy
```

## Environment Variables

Copy `.env.example` to `.env` and populate:

```bash
# AWS
AWS_REGION=eu-west-2
AWS_ACCOUNT_ID=123456789012

# Snowflake
SNOWFLAKE_ACCOUNT=xy12345.eu-west-2.aws
SNOWFLAKE_USER=TRANSFORMER_USER
SNOWFLAKE_ROLE=TRANSFORMER

# Dagster Cloud
DAGSTER_CLOUD_API_TOKEN=***
DAGSTER_CLOUD_DEPLOYMENT=prod

# Census
CENSUS_API_KEY=***
```

**⚠️ Never commit `.env` to Git!**

## Multi-Tenancy

Each client gets:
- Isolated Snowflake database (`{CLIENT}_DEV_RAW`, `{CLIENT}_PROD_RAW`)
- Separate S3 prefixes (`s3://mdp-lake-dev/clients/{client}/`)
- Client-specific dbt models & Dagster schedules
- Resource monitors to prevent cost overruns

## Development Workflow

1. **Feature branch**: `git checkout -b feat/add-churn-model`
2. **Local dbt development**: `dbt run --select +my_new_model`
3. **Test**: `dbt test --select my_new_model`
4. **Open PR**: GitHub Actions runs `terraform plan` + `dbt compile`
5. **Merge to main**: Auto-deploys to Dagster Cloud

## Documentation

- [Onboarding Runbook](docs/runbooks/onboard_new_client.md)
- [Architecture Overview](docs/architecture/overview.md)
- [Cost Monitoring](docs/runbooks/cost_monitoring.md)
- [Incident Response](docs/runbooks/incident_response.md)

## Support

For issues or questions, contact:
- Technical: [founder-1@caritas.com]
- Billing: [founder-2@caritas.com]

## License

Proprietary - All rights reserved
