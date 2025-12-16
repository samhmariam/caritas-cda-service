# Architecture Overview

## High-Level Architecture

```
┌─────────────┐
│   Sources   │  HubSpot, Stripe, Intercom, Salesforce, etc.
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Airbyte   │  Ingestion (Usage-based SaaS)
│    Cloud    │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────────┐
│                  AWS S3 Data Lake                   │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │   Raw    │→ │  Bronze  │→ │  Silver  │         │
│  │  (JSONL) │  │ (Validated)│ │ (Iceberg)│         │
│  └──────────┘  └──────────┘  └──────────┘         │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
              ┌────────────────┐
              │   Snowflake    │  Data Warehouse (EU region)
              │                │
              │  ┌──────────┐  │
              │  │   RAW    │  │  Raw schema (from Airbyte)
              │  └────┬─────┘  │
              │       │        │
              │  ┌────▼─────┐  │
              │  │ STAGING  │  │  Normalized views (dbt)
              │  └────┬─────┘  │
              │       │        │
              │  ┌────▼──────┐ │
              │  │INTERMEDIATE│ │  Business logic (dbt)
              │  └────┬──────┘ │
              │       │        │
              │  ┌────▼─────┐  │
              │  │  MARTS   │  │  Consumption layer (dbt)
              │  └──────────┘  │
              └────────┬───────┘
                       │
        ┌──────────────┴──────────────┐
        │                             │
        ▼                             ▼
   ┌─────────┐                  ┌──────────┐
   │ Census  │                  │ Power BI │
   │ (Reverse│                  │(Analytics)│
   │   ETL)  │                  └──────────┘
   └────┬────┘
        │
        ▼
   ┌─────────────────┐
   │ CRM/Marketing   │  HubSpot, Salesforce, Braze, etc.
   │     Tools       │
   └─────────────────┘

         Orchestration: Dagster Cloud (Serverless)
```

## Data Flow

### 1. Ingestion (Airbyte → S3 Raw)

- **Frequency**: Every 6 hours (configurable per source)
- **Format**: JSONL files
- **Path**: `s3://mdp-raw-{env}/clients/{client}/{source}/run_date=YYYY-MM-DD/*.jsonl`
- **Metadata**: Airbyte adds `_airbyte_extracted_at` timestamp

### 2. Validation (S3 Raw → S3 Bronze)

- **Trigger**: Dagster S3 sensor detects new files
- **Validation**:
  - File completeness check (`_SUCCESS` marker)
  - Basic schema validation
  - Data quality checks
- **Output**: `s3://mdp-bronze-{env}/clients/{client}/{source}/load_date=YYYY-MM-DD/*.jsonl`

### 3. Loading (S3 Bronze → Snowflake RAW)

- **Method**: Snowflake COPY INTO from S3
- **Table type**: Iceberg external tables
- **Schema**: Auto-detected from JSONL
- **Incremental**: Load only new partitions

### 4. Transformation (Snowflake RAW → MARTS)

- **Tool**: dbt Core (run in Dagster)
- **Layers**:
  - **STAGING**: 1:1 with sources, type casting, JSON parsing
  - **INTERMEDIATE**: Business logic, joins, aggregations
  - **MARTS**: Consumption-ready models
- **Materialization**:
  - STAGING/INTERMEDIATE: Views
  - MARTS: Tables

### 5. Activation (Snowflake MARTS → Census → CRM)

- **Trigger**: After dbt completion
- **Method**: Census API triggered by Dagster
- **Syncs**: Defined in `clients/{client}/census/syncs.yml`

### 6. Analytics (Snowflake MARTS → Power BI)

- **Connection**: DirectQuery or Import mode
- **Refresh**: Scheduled in Power BI Service
- **Row-level security**: Enforced in Snowflake

## Multi-Tenancy Design

### Client Isolation

Each client gets:

1. **Snowflake Database**: `{CLIENT}_{ENV}_RAW`
2. **S3 Prefix**: `clients/{client}/`
3. **Separate Roles**: `{CLIENT}_{ENV}_{ROLE_TYPE}`
4. **Resource Monitor**: Credit quotas per client/env

### Shared Resources

- S3 buckets (multi-tenant via prefixes)
- Terraform modules
- dbt shared models and macros
- Dagster orchestration code

### Environment Separation

- **Dev**: Small warehouses, 1-day retention, aggressive cost controls
- **Stage**: Medium warehouses, 7-day retention, mirrors prod config
- **Prod**: Auto-scaled warehouses, 7-day retention, monitoring alerts

## Security Model

### Authentication & Authorization

```
┌──────────────────────────────────────────────────┐
│              Snowflake Roles                     │
├──────────────────────────────────────────────────┤
│  ACCOUNTADMIN (Terraform only)                   │
│      │                                           │
│      ├─► LOADER (Airbyte)                        │
│      │     └─► Grants: INSERT on RAW schema     │
│      │                                           │
│      ├─► TRANSFORMER (dbt/Dagster)               │
│      │     └─► Grants: SELECT on RAW            │
│      │                  CREATE on STAGING/MARTS │
│      │                                           │
│      └─► READER (Census/Power BI)               │
│            └─► Grants: SELECT on MARTS          │
└──────────────────────────────────────────────────┘
```

### Data Governance

- **Row-Level Security (RLS)**: Applied in MARTS for sensitive data
- **Dynamic Masking**: PII columns masked for non-admin roles
- **Object Tags**: `PII`, `GDPR`, `CONFIDENTIAL` for classification
- **Query Tags**: Cost attribution (client, env, model, run_id)

### Secrets Management

- **Storage**: AWS Secrets Manager
- **Access**: IAM roles for Dagster Cloud
- **Rotation**: Quarterly via Terraform scripts
- **Never in Git**: `.env` files gitignored

## Cost Optimization

### Snowflake

- **Auto-suspend**: 60 seconds idle
- **Auto-resume**: On query
- **Resource monitors**: Suspend at 100% quota
- **Query tags**: Attribution by client/model
- **Warehouse sizing**:
  - Dev: X-SMALL
  - Stage: SMALL
  - Prod: MEDIUM (auto-scale to LARGE)

### AWS S3

- **Lifecycle policies**: 
  - Glacier after 90 days (raw layer)
  - Delete after 365 days
- **Intelligent tiering**: For bronze/silver

### Airbyte

- **Usage-based**: Pay per connector row
- **Optimization**: Incremental syncs only

## Disaster Recovery

### Backup Strategy

- **Snowflake**: Time Travel (7 days in prod)
- **S3**: Versioning enabled on bronze/silver
- **Terraform state**: S3 backend with versioning

### Recovery Procedures

1. **Snowflake table corruption**: Restore via Time Travel
2. **Bad dbt run**: `dbt run --full-refresh` with validated commit
3. **Infrastructure failure**: Terraform re-apply from state backup

## Monitoring & Observability

### Dagster Cloud

- Asset materialization history
- Run logs and failure alerts
- Resource usage metrics

### Snowflake

- Query history (via `QUERY_TAG`)
- Warehouse utilization
- Credit consumption per client

### AWS CloudWatch

- S3 bucket metrics
- Lambda errors (if using)

## Scalability Considerations

### Current Capacity

- **Clients**: 10-20 per environment
- **Data volume**: ~100GB per client
- **dbt models**: ~200 models per client
- **Query concurrency**: 10-20 concurrent queries

### Scaling Strategies

1. **More clients**: Add resource monitors, increase warehouse sizes
2. **More data**: Partition tables, optimize dbt models
3. **More queries**: Auto-scaling warehouses, query result caching

## Technology Stack Rationale

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Ingestion | Airbyte Cloud | SaaS, usage-based, 300+ connectors |
| Storage | S3 + Iceberg | Cost-effective, open format, time travel |
| Warehouse | Snowflake | EU compliance, great performance, SQL-based |
| Transform | dbt Core | Git-based, version control, testable |
| Orchestration | Dagster Cloud | Serverless, asset-oriented, great UI |
| Activation | Census | Declarative syncs, reliable, well-supported |
| Analytics | Power BI | Enterprise standard, semantic layer |
| IaC | Terraform | Declarative, state management, reusable modules |
