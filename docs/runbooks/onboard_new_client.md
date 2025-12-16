# Client Onboarding Runbook

This guide walks you through onboarding a new client to the Customer Data Activation platform.

## Prerequisites

- [ ] AWS account with appropriate permissions
- [ ] Snowflake account admin access
- [ ] Terraform installed locally (>= 1.6)
- [ ] AWS CLI configured
- [ ] Client details collected (name, data sources, etc.)

## Step 1: Create Client Directory Structure

```bash
# From repository root
export CLIENT_NAME="stellar_bank"

# Copy template
cp -r clients/_template clients/$CLIENT_NAME
```

## Step 2: Configure Client Settings

Edit `clients/$CLIENT_NAME/config/dev.yml`:

```yaml
client:
  name: stellar_bank
  display_name: "Stellar Bank"
  
snowflake:
  database: STELLAR_BANK_DEV_RAW
  warehouse:
    size: X-SMALL
    auto_suspend: 60
  
s3:
  raw_bucket: mdp-raw-dev
  bronze_bucket: mdp-bronze-dev
  silver_bucket: mdp-silver-dev
  prefix: clients/stellar_bank

airbyte:
  sources:
    - hubspot
    - stripe
    - intercom

dbt:
  schedules:
    daily_refresh: "0 2 * * *"  # 2 AM UTC
```

## Step 3: Create Terraform Variables

Create `clients/$CLIENT_NAME/terraform/terraform.tfvars`:

```hcl
client_name = "stellar_bank"
environment = "dev"

# DO NOT put secrets here - use AWS Secrets Manager
```

## Step 4: Apply Infrastructure

```bash
cd terraform/environments/dev

# Set client name
export TF_VAR_client_name="stellar_bank"

# Initialize Terraform
terraform init

# Plan changes
terraform plan

# Review the plan carefully, then apply
terraform apply
```

This will create:
- ✅ S3 buckets (or prefixes within existing buckets)
- ✅ Snowflake database, schemas, warehouses
- ✅ IAM roles for Snowflake S3 access
- ✅ Snowflake roles (LOADER, TRANSFORMER, READER)
- ✅ Resource monitors for cost control
- ✅ AWS Secrets Manager entries

## Step 5: Configure Airbyte Sources

1. Log into Airbyte Cloud
2. Create connections for each source:
   - Destination: S3
   - Bucket: `mdp-raw-dev`
   - Prefix: `clients/stellar_bank/{source}/`
   - Format: JSONL
   - Schedule: Every 6 hours

## Step 6: Run Initial dbt Build

```bash
cd dbt

# Set client context
export DBT_CLIENT="stellar_bank"
export SNOWFLAKE_DATABASE="STELLAR_BANK_DEV_RAW"

# Install dependencies
dbt deps

# Run initial build
dbt build --select client:stellar_bank
```

## Step 7: Deploy Dagster Schedules

The Dagster orchestration will auto-discover the new client based on the config files.

```bash
cd dagster

# Deploy to Dagster Cloud
dagster-cloud deployment deploy
```

Verify schedules in Dagster Cloud UI.

## Step 8: Configure Census Syncs

1. Log into Census
2. Create data source connection to Snowflake:
   - Database: `STELLAR_BANK_DEV_RAW`
   - Schema: `MARTS`
   - Role: `STELLAR_BANK_DEV_READER`

3. Create syncs using activation models:
   - Source: `MARTS.MART_ACTIVATION_CUSTOMERS`
   - Destination: Your CRM/marketing tools

4. Note the Census sync IDs and update client config.

## Step 9: Set Up Power BI

1. Create Power BI workspace for client
2. Connect to Snowflake:
   - Server: `<account>.snowflakecomputing.com`
   - Database: `STELLAR_BANK_DEV_RAW`
   - Warehouse: `STELLAR_BANK_DEV_TRANSFORMING_WH`
   - Role: `STELLAR_BANK_DEV_READER`

3. Import datasets from `MARTS` schema
4. Publish reports to client workspace

## Step 10: Verification

- [ ] Check S3 buckets for incoming data
- [ ] Verify Bronze layer validation runs
- [ ] Confirm dbt models run successfully
- [ ] Test Census syncs
- [ ] Validate Power BI reports load

## Post-Onboarding

1. **Document**: Update client contact info in `docs/client_playbooks/`
2. **Monitor**: Set up Snowflake cost alerts
3. **Train**: Schedule client training session
4. **Review**: Schedule 30-day review

## Rollback Procedure

If something goes wrong:

```bash
# Destroy infrastructure
cd terraform/environments/dev
terraform destroy -target=module.snowflake -target=module.s3_lake

# Remove client directory
rm -rf clients/$CLIENT_NAME
```

## Common Issues

### Snowflake connection fails
- Verify credentials in AWS Secrets Manager
- Check Snowflake role permissions
- Confirm network policy allows connections

### dbt models fail
- Check source data exists in RAW schema
- Verify schema names match config
- Review dbt logs in `dbt/logs/`

### Census syncs fail
- Verify READER role has SELECT on MARTS
- Check Census API key is valid
- Confirm destination credentials

## Support

For issues, contact:
- Infrastructure: [founder-1@caritas.com]
- Data pipeline: [founder-2@caritas.com]
