terraform {
  required_version = ">= 1.6"
  
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.90"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
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


variable "s3_silver_bucket" {
  description = "S3 bucket for silver layer"
  type        = string
}

variable "s3_raw_bucket" {
  description = "S3 bucket for raw layer"
  type        = string
}

variable "s3_raw_role_arn" {
  description = "IAM role ARN for S3 raw access (optional, managed manually)"
  type        = string
  default     = ""
}

variable "bronze_tables" {
  description = "Map of bronze tables to create (key = unique_id, value = {source, table})"
  type = map(object({
    source = string
    table  = string
  }))
  default = {}
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
resource "snowflake_account_role" "loader" {
  name    = "${upper(var.client_name)}_${upper(var.environment)}_LOADER"
  comment = "Role for Airbyte to load raw data"
}

resource "snowflake_account_role" "transformer" {
  name    = "${upper(var.client_name)}_${upper(var.environment)}_TRANSFORMER"
  comment = "Role for dbt transformations"
}

resource "snowflake_account_role" "reader" {
  name    = "${upper(var.client_name)}_${upper(var.environment)}_READER"
  comment = "Role for BI tools and Census (read-only)"
}

# Generate passwords for service users
resource "random_password" "loader" {
  length  = 24
  special = true
  override_special = "!@#$%^&*"
}

resource "random_password" "transformer" {
  length  = 24
  special = true
  override_special = "!@#$%^&*"
}

resource "random_password" "reader" {
  length  = 24
  special = true
  override_special = "!@#$%^&*"
}

# Create Snowflake users
resource "snowflake_user" "loader" {
  name         = "${upper(var.client_name)}_${upper(var.environment)}_LOADER_USER"
  password     = random_password.loader.result
  comment      = "Service user for Airbyte data loading"
  default_role = snowflake_account_role.loader.name
  
  must_change_password = false
}

resource "snowflake_user" "transformer" {
  name         = "${upper(var.client_name)}_${upper(var.environment)}_TRANSFORMER_USER"
  password     = random_password.transformer.result
  comment      = "Service user for dbt transformations"
  default_role = snowflake_account_role.transformer.name
  
  must_change_password = false
}

resource "snowflake_user" "reader" {
  name         = "${upper(var.client_name)}_${upper(var.environment)}_READER_USER"
  password     = random_password.reader.result
  comment      = "Service user for BI tools and Census"
  default_role = snowflake_account_role.reader.name
  
  must_change_password = false
}

# Grant roles to users
resource "snowflake_grant_account_role" "loader_to_user" {
  role_name = snowflake_account_role.loader.name
  user_name = snowflake_user.loader.name
}

resource "snowflake_grant_account_role" "transformer_to_user" {
  role_name = snowflake_account_role.transformer.name
  user_name = snowflake_user.transformer.name
}

resource "snowflake_grant_account_role" "reader_to_user" {
  role_name = snowflake_account_role.reader.name
  user_name = snowflake_user.reader.name
}

# Grant warehouse usage
resource "snowflake_grant_privileges_to_account_role" "loader_warehouse" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.loading.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_warehouse" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.transforming.name
  }
}

# Grant database privileges
resource "snowflake_grant_privileges_to_account_role" "loader_database" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.main.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_database" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.main.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_database" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.main.name
  }
}

# Schema grants for LOADER (write to RAW only)
resource "snowflake_grant_privileges_to_account_role" "loader_raw_schema" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE", "CREATE TABLE"]
  
  on_schema {
    schema_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.raw.name}\""
  }
}

# Grant LOADER role permissions on future tables in RAW schema
resource "snowflake_grant_privileges_to_account_role" "loader_raw_future_tables" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["INSERT", "SELECT", "UPDATE", "DELETE"]
  
  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.main.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

# Grant LOADER role permissions on all existing tables in RAW schema
resource "snowflake_grant_privileges_to_account_role" "loader_raw_all_tables" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["INSERT", "SELECT", "UPDATE", "DELETE"]
  
  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "\"${snowflake_database.main.name}\".\"${snowflake_schema.raw.name}\""
    }
  }
}

# Schema grants for TRANSFORMER (read RAW, write to STAGING/INTERMEDIATE/MARTS)
resource "snowflake_grant_privileges_to_account_role" "transformer_raw_read" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  
  on_schema {
    schema_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.raw.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_staging" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  
  on_schema {
    schema_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.staging.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_intermediate" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  
  on_schema {
    schema_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.intermediate.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_marts" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  
  on_schema {
    schema_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.marts.name}\""
  }
}

# Storage integration for raw S3 bucket
# NOTE: Created manually and imported - IAM role ARN managed outside Terraform
# To update IAM role: ALTER STORAGE INTEGRATION ... SET STORAGE_AWS_ROLE_ARN = '...'
resource "snowflake_storage_integration" "raw_s3" {
  name    = "${upper(var.client_name)}_${upper(var.environment)}_RAW_S3_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true
  
  storage_provider          = "S3"
  storage_allowed_locations = ["s3://${var.s3_raw_bucket}/"]
  
  # IAM role ARN is managed manually via Snowflake SQL
  # Ignore changes to prevent Terraform from modifying it
  lifecycle {
    ignore_changes = [
      storage_aws_role_arn
    ]
  }
  
  comment = "Storage integration for ${var.client_name} raw data in S3"
}

# File format for GZIP JSON files
resource "snowflake_file_format" "gzip_json" {
  name     = "${upper(var.client_name)}_${upper(var.environment)}_GZIP_JSON"
  database = snowflake_database.main.name  
  schema   = snowflake_schema.raw.name
  
  format_type = "JSON"
  compression = "GZIP"
  
  comment = "File format for GZIP compressed JSON files"
}

# External stage for raw S3 data
resource "snowflake_stage" "raw_s3_stage" {
  name     = "${upper(var.client_name)}_${upper(var.environment)}_RAW_STAGE"
  database = snowflake_database.main.name
  schema   = snowflake_schema.raw.name
  
  url                 = "s3://${var.s3_raw_bucket}/"
  storage_integration = snowflake_storage_integration.raw_s3.name
  
  file_format = "FORMAT_NAME = ${snowflake_database.main.name}.${snowflake_schema.raw.name}.${snowflake_file_format.gzip_json.name}"
  
  comment = "External stage for raw data from S3"
}

# Grant LOADER role usage on storage integration
resource "snowflake_grant_privileges_to_account_role" "loader_storage_integration" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  
  on_account_object {
    object_type = "INTEGRATION"
    object_name = snowflake_storage_integration.raw_s3.name
  }
}

# Grant LOADER role usage on stage
resource "snowflake_grant_privileges_to_account_role" "loader_stage" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE", "READ"]
  
  on_schema_object {
    object_type = "STAGE"
    object_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.raw.name}\".\"${snowflake_stage.raw_s3_stage.name}\""
  }
}

# Grant TRANSFORMER role read access to storage integration
resource "snowflake_grant_privileges_to_account_role" "transformer_storage_integration" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  
  on_account_object {
    object_type = "INTEGRATION"
    object_name = snowflake_storage_integration.raw_s3.name
  }
}

# Grant TRANSFORMER role read access to stage
resource "snowflake_grant_privileges_to_account_role" "transformer_stage" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "READ"]
  
  on_schema_object {
    object_type = "STAGE"
    object_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.raw.name}\".\"${snowflake_stage.raw_s3_stage.name}\""
  }
}


# Grant LOADER role usage on file format
resource "snowflake_grant_privileges_to_account_role" "loader_file_format" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  
  on_schema_object {
    object_type = "FILE FORMAT"
    object_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.raw.name}\".\"${snowflake_file_format.gzip_json.name}\""
  }
}

# Grant TRANSFORMER role usage on file format
resource "snowflake_grant_privileges_to_account_role" "transformer_file_format" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  
  on_schema_object {
    object_type = "FILE FORMAT"
    object_name = "\"${snowflake_database.main.name}\".\"${snowflake_schema.raw.name}\".\"${snowflake_file_format.gzip_json.name}\""
  }
}

# Dynamic bronze landing tables for each source/table combination
resource "snowflake_table" "bronze_tables" {
  for_each = var.bronze_tables
  
  database = snowflake_database.main.name
  schema   = snowflake_schema.raw.name
  name     = "${upper(each.value.source)}_${upper(each.value.table)}"
  comment  = "Landing table for ${each.value.source}/${each.value.table}"

  column {
    name = "METADATA_FILENAME"
    type = "VARCHAR"
    comment = "Source S3 file path"
  }

  column {
    name = "METADATA_ROW_NUMBER"
    type = "NUMBER(38,0)"
    comment = "Row number within source file"
  }

  column {
    name     = "METADATA_INGEST_TIME"
    type     = "TIMESTAMP_LTZ"
    nullable = false
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
    comment = "Timestamp when record was loaded"
  }

  column {
    name    = "RECORD_CONTENT"
    type    = "VARIANT"
    comment = "Raw JSON record content"
  }
}

# External volume for Iceberg tables
resource "snowflake_external_volume" "silver" {
  name = "${upper(var.client_name)}_${upper(var.environment)}_SILVER_EXVOL"
  
  storage_location {
    storage_location_name = "s3_location"
    storage_provider      = "S3"
    storage_base_url      = "s3://${var.s3_silver_bucket}/clients/${var.client_name}/"
  }
  
  # IAM role ARN is managed manually via Snowflake SQL
  # Ignore changes to prevent Terraform from modifying it
  lifecycle {
    ignore_changes = [
      storage_location[0].storage_aws_role_arn
    ]
  }
  
  comment = "External volume for ${var.client_name} ${var.environment} Iceberg tables"
}

# Resource monitor for cost control
resource "snowflake_resource_monitor" "client_monitor" {
  name = "${upper(var.client_name)}_${upper(var.environment)}_MONITOR"
  
  credit_quota = var.environment == "prod" ? 100 : 20
  
  frequency       = "MONTHLY"
  start_timestamp = "IMMEDIATELY"
  
  notify_triggers = [80, 90]
  suspend_trigger = 100
  
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
  value = snowflake_account_role.loader.name
}

output "role_transformer" {
  value = snowflake_account_role.transformer.name
}

output "role_reader" {
  value = snowflake_account_role.reader.name
}

output "external_volume_name" {
  value = snowflake_external_volume.silver.name
}

output "storage_integration_name" {
  value = snowflake_storage_integration.raw_s3.name
}

output "raw_stage_name" {
  value = snowflake_stage.raw_s3_stage.name
}

# User outputs
output "user_loader" {
  value = snowflake_user.loader.name
}

output "user_transformer" {
  value = snowflake_user.transformer.name
}

output "user_reader" {
  value = snowflake_user.reader.name
}

# Password outputs (sensitive)
output "password_loader" {
  value     = random_password.loader.result
  sensitive = true
}

output "password_transformer" {
  value     = random_password.transformer.result
  sensitive = true
}

output "password_reader" {
  value     = random_password.reader.result
  sensitive = true
}
