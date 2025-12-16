terraform {
  required_version = ">= 1.6"
  
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.90"
    }
  }
}

variable "client_name" {
  description = "Client identifier (e.g., acme, stellar_bank)"
  type        = string
}

variable "environment" {
  description = "Environment (dev, stage, prod)"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "s3_role_arn" {
  description = "IAM role ARN for S3 access"
  type        = string
}

variable "s3_silver_bucket" {
  description = "S3 bucket for silver layer"
  type        = string
}

locals {
  database_name = "${upper(var.client_name)}_${upper(var.environment)}_RAW"
  warehouse_name = "${upper(var.client_name)}_${upper(var.environment)}_TRANSFORMING_WH"
}

# Database
resource "snowflake_database" "main" {
  name                        = local.database_name
  comment                     = "Database for ${var.client_name} ${var.environment} environment"
  data_retention_time_in_days = var.environment == "prod" ? 7 : 1
}

# Schemas
resource "snowflake_schema" "raw" {
  database = snowflake_database.main.name
  name     = "RAW"
  comment  = "Raw data from Airbyte"
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.main.name
  name     = "STAGING"
  comment  = "Normalized staging models (dbt)"
}

resource "snowflake_schema" "intermediate" {
  database = snowflake_database.main.name
  name     = "INTERMEDIATE"
  comment  = "Intermediate transformation models (dbt)"
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.main.name
  name     = "MARTS"
  comment  = "Business-ready data marts (dbt)"
}

# Warehouses with auto-suspend
resource "snowflake_warehouse" "transforming" {
  name           = local.warehouse_name
  warehouse_size = var.environment == "prod" ? "MEDIUM" : "X-SMALL"
  
  auto_suspend          = 60
  auto_resume           = true
  initially_suspended   = true
  
  comment = "Warehouse for dbt transformations (${var.client_name} ${var.environment})"
}

resource "snowflake_warehouse" "loading" {
  name           = "${upper(var.client_name)}_${upper(var.environment)}_LOADING_WH"
  warehouse_size = "X-SMALL"
  
  auto_suspend          = 60
  auto_resume           = true
  initially_suspended   = true
  
  comment = "Warehouse for data loading from Airbyte (${var.client_name} ${var.environment})"
}

# Roles (least privilege)
resource "snowflake_role" "loader" {
  name    = "${upper(var.client_name)}_${upper(var.environment)}_LOADER"
  comment = "Role for Airbyte to load raw data"
}

resource "snowflake_role" "transformer" {
  name    = "${upper(var.client_name)}_${upper(var.environment)}_TRANSFORMER"
  comment = "Role for dbt transformations"
}

resource "snowflake_role" "reader" {
  name    = "${upper(var.client_name)}_${upper(var.environment)}_READER"
  comment = "Role for BI tools and Census (read-only)"
}

# Grant warehouse usage
resource "snowflake_grant_privileges_to_role" "loader_warehouse" {
  role_name  = snowflake_role.loader.name
  privileges = ["USAGE"]
  
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.loading.name
  }
}

resource "snowflake_grant_privileges_to_role" "transformer_warehouse" {
  role_name  = snowflake_role.transformer.name
  privileges = ["USAGE"]
  
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transforming.name
  }
}

# Grant database privileges
resource "snowflake_grant_privileges_to_role" "loader_database" {
  role_name  = snowflake_role.loader.name
  privileges = ["USAGE"]
  
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.main.name
  }
}

resource "snowflake_grant_privileges_to_role" "transformer_database" {
  role_name  = snowflake_role.transformer.name
  privileges = ["USAGE"]
  
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.main.name
  }
}

resource "snowflake_grant_privileges_to_role" "reader_database" {
  role_name  = snowflake_role.reader.name
  privileges = ["USAGE"]
  
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.main.name
  }
}

# Schema grants for LOADER (write to RAW only)
resource "snowflake_grant_privileges_to_role" "loader_raw_schema" {
  role_name  = snowflake_role.loader.name
  privileges = ["USAGE", "CREATE TABLE"]
  
  on_schema {
    schema_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.raw.name}\""
  }
}

# Schema grants for TRANSFORMER (read RAW, write to STAGING/INTERMEDIATE/MARTS)
resource "snowflake_grant_privileges_to_role" "transformer_raw_read" {
  role_name  = snowflake_role.transformer.name
  privileges = ["USAGE"]
  
  on_schema {
    schema_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_role" "transformer_staging" {
  role_name  = snowflake_role.transformer.name
  privileges = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  
  on_schema {
    schema_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.staging.name}\""
  }
}

resource "snowflake_grant_privileges_to_role" "transformer_intermediate" {
  role_name  = snowflake_role.transformer.name
  privileges = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  
  on_schema {
    schema_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.intermediate.name}\""
  }
}

resource "snowflake_grant_privileges_to_role" "transformer_marts" {
  role_name  = snowflake_role.transformer.name
  privileges = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  
  on_schema {
    schema_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.marts.name}\""
  }
}

# External volume for Iceberg tables
resource "snowflake_external_volume" "silver" {
  name = "${upper(var.client_name)}_${upper(var.environment)}_SILVER_EXVOL"
  
  storage_location {
    storage_provider          = "S3"
    storage_base_url          = "s3://${var.s3_silver_bucket}/clients/${var.client_name}/"
    storage_aws_role_arn      = var.s3_role_arn
  }
  
  comment = "External volume for ${var.client_name} ${var.environment} Iceberg tables"
}

# Resource monitor for cost control
resource "snowflake_resource_monitor" "client_monitor" {
  name = "${upper(var.client_name)}_${upper(var.environment)}_MONITOR"
  
  credit_quota = var.environment == "prod" ? 100 : 20
  
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"
  
  notify_triggers  = [80, 90]
  suspend_triggers = [100]
  
  notify_users = var.admin_users
}

variable "admin_users" {
  description = "List of admin users to notify on resource monitor alerts"
  type        = list(string)
  default     = []
}

# Outputs
output "database_name" {
  value = snowflake_database.main.name
}

output "warehouse_transforming" {
  value = snowflake_warehouse.transforming.name
}

output "warehouse_loading" {
  value = snowflake_warehouse.loading.name
}

output "role_loader" {
  value = snowflake_role.loader.name
}

output "role_transformer" {
  value = snowflake_role.transformer.name
}

output "role_reader" {
  value = snowflake_role.reader.name
}

output "external_volume_name" {
  value = snowflake_external_volume.silver.name
}
