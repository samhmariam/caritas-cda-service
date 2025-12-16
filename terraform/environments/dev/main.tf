terraform {
  required_version = ">= 1.6"
  
  backend "s3" {
    bucket         = "caritas-terraform-state"
    key            = "dev/terraform.tfstate"
    region         = "eu-west-2"
    encrypt        = true
    dynamodb_table = "terraform-state-lock"
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
  }
}

provider "aws" {
  region = var.aws_region
  
  default_tags {
    tags = {
      Environment = "dev"
      ManagedBy   = "Terraform"
      Service     = "CustomerDataActivation"
    }
  }
}

provider "snowflake" {
  account = var.snowflake_account
  user    = var.snowflake_user
  role    = "ACCOUNTADMIN"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-2"
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake admin user"
  type        = string
}

variable "client_name" {
  description = "Client identifier (e.g., acme)"
  type        = string
  default     = "acme"
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
  
  client_name      = var.client_name
  environment      = "dev"
  snowflake_account = var.snowflake_account
  
  s3_role_arn       = module.s3_lake.snowflake_role_arn
  s3_silver_bucket  = module.s3_lake.silver_bucket_name
}

# Secrets
module "secrets" {
  source = "../../modules/secrets"
  
  client_name = var.client_name
  environment = "dev"
  
  secrets = {
    snowflake_password = var.snowflake_transformer_password
    census_api_key     = var.census_api_key
  }
}

variable "snowflake_transformer_password" {
  description = "Password for Snowflake transformer user"
  type        = string
  sensitive   = true
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
    database   = module.snowflake.database_name
    warehouse  = module.snowflake.warehouse_transforming
    role       = module.snowflake.role_transformer
  }
}
