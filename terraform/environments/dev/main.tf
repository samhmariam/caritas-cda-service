terraform {
  required_version = ">= 1.6"
  
  backend "s3" {
    bucket         = "caritas-terraform-state"
    key            = "dev/terraform.tfstate"
    region         = "eu-west-2"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
    profile        = "my-dev"
  }
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
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

provider "aws" {
  region  = var.aws_region
  profile = "my-dev"
  
  default_tags {
    tags = {
      Environment = "dev"
      ManagedBy   = "Terraform"
      Service     = "CustomerDataActivation"
    }
  }
}

provider "snowflake" {
  user              = var.snowflake_user
  password          = var.snowflake_password
  organization_name = "QDSDMZU"
  account_name      = "XDB78749"
  role              = "ACCOUNTADMIN"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-2"
}



variable "snowflake_user" {
  description = "Snowflake admin user"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake admin password"
  type        = string
  sensitive   = true
}

variable "client_name" {
  description = "Client identifier (e.g., acme)"
  type        = string
  default     = "wise"
}

# S3 Data Lake
module "s3_lake" {
  source = "../../modules/aws-s3-lake"
  
  client_name = var.client_name
  environment = "dev"
  region      = var.aws_region
  
  tags = {
    Client = var.client_name
  }
}

# Snowflake Infrastructure
module "snowflake" {
  source = "../../modules/snowflake-base"
  
  client_name     = var.client_name
  environment     = "dev"
  
  s3_silver_bucket     = module.s3_lake.silver_bucket_name
  s3_raw_bucket        = module.s3_lake.raw_bucket_name
  # s3_raw_role_arn removed - IAM role managed manually
  
  # Define all bronze landing tables
  bronze_tables = {
    # Harvest (3 tables)
    harvest_clients       = { source = "harvest", table = "clients" }
    harvest_projects      = { source = "harvest", table = "projects" }
    harvest_time_entries  = { source = "harvest", table = "time_entries" }
    
    # Intacct (3 tables)
    intacct_customers           = { source = "intacct", table = "customers" }
    intacct_gl_entries          = { source = "intacct", table = "gl_entries" }
    intacct_revenue_recognition = { source = "intacct", table = "revenue_recognition" }
    
    # Jira (1 table)
    jira_issues = { source = "jira", table = "issues" }
    
    # Mixpanel (1 table)
    mixpanel_events = { source = "mixpanel", table = "events" }
    
    # Salesforce (3 tables)
    sf_accounts      = { source = "sf", table = "accounts" }
    sf_opportunities = { source = "sf", table = "opportunities" }
    sf_users         = { source = "sf", table = "users" }
    
    # Stripe (8 tables)
    stripe_balance_transactions = { source = "stripe", table = "balance_transactions" }
    stripe_charges              = { source = "stripe", table = "charges" }
    stripe_customers            = { source = "stripe", table = "customers" }
    stripe_disputes             = { source = "stripe", table = "disputes" }
    stripe_invoice_line_items   = { source = "stripe", table = "invoice_line_items" }
    stripe_invoices             = { source = "stripe", table = "invoices" }
    stripe_refunds              = { source = "stripe", table = "refunds" }
    stripe_subscriptions        = { source = "stripe", table = "subscriptions" }
    
    # Zendesk (4 tables)
    zendesk_organizations = { source = "zendesk", table = "organizations" }
    zendesk_ticket_events = { source = "zendesk", table = "ticket_events" }
    zendesk_tickets       = { source = "zendesk", table = "tickets" }
    zendesk_time_entries  = { source = "zendesk", table = "time_entries" }
  }
}

# Secrets
module "secrets" {
  source = "../../modules/secrets"
  
  client_name = var.client_name
  environment = "dev"
  
  secrets = {
    snowflake_loader_password      = module.snowflake.password_loader
    snowflake_transformer_password = module.snowflake.password_transformer
    snowflake_reader_password      = module.snowflake.password_reader
    census_api_key                 = var.census_api_key
  }
}



variable "census_api_key" {
  description = "Census API key"
  type        = string
  sensitive   = true
}

# Outputs
output "s3_buckets" {
  value = {
    raw    = module.s3_lake.raw_bucket_name
    bronze = module.s3_lake.bronze_bucket_name
    silver = module.s3_lake.silver_bucket_name
  }
}

output "snowflake_config" {
  value = {
    database            = module.snowflake.database_name
    warehouse           = module.snowflake.warehouse_transforming
    role                = module.snowflake.role_transformer
    storage_integration = module.snowflake.storage_integration_name
    raw_stage           = module.snowflake.raw_stage_name
  }
}

output "snowflake_users" {
  value = {
    loader      = module.snowflake.user_loader
    transformer = module.snowflake.user_transformer
    reader      = module.snowflake.user_reader
  }
  description = "Snowflake service user names (passwords stored in AWS Secrets Manager)"
}
