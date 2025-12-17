terraform {
  required_version = ">= 1.6"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
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
  validation {
    condition     = contains(["dev", "stage", "prod"], var.environment)
    error_message = "Environment must be dev, stage, or prod."
  }
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-2"
}

variable "tags" {
  description = "Common tags to apply to resources"
  type        = map(string)
  default     = {}
}

# S3 Buckets for data lake layers
resource "aws_s3_bucket" "raw" {
  bucket = "cda-raw-${var.environment}"
  
  tags = merge(var.tags, {
    Name        = "cda-raw-${var.environment}"
    Environment = var.environment
    Layer       = "raw"
  })
}

resource "aws_s3_bucket" "bronze" {
  bucket = "cda-bronze-${var.environment}"
  
  tags = merge(var.tags, {
    Name        = "cda-bronze-${var.environment}"
    Environment = var.environment
    Layer       = "bronze"
  })
}

resource "aws_s3_bucket" "silver" {
  bucket = "cda-silver-${var.environment}"
  
  tags = merge(var.tags, {
    Name        = "cda-silver-${var.environment}"
    Environment = var.environment
    Layer       = "silver"
  })
}

# Enable versioning for bronze and silver (data quality)
resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

# Lifecycle policies for cost optimization
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    filter {}

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "raw" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket = aws_s3_bucket.silver.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Encryption at rest
resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# NOTE: IAM role commented out - requires manual setup with real Snowflake AWS account details
# Uncomment and configure when you have:
# 1. snowflake_aws_account_id from Snowflake storage integration
# 2. snowflake_external_id from Snowflake storage integration
#
# # IAM role for Snowflake external access
# resource "aws_iam_role" "snowflake_s3_access" {
#   name = "snowflake-s3-access-${var.client_name}-${var.environment}"
# 
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Principal = {
#           AWS = "arn:aws:iam::${var.snowflake_aws_account_id}:user/snowflake"
#         }
#         Action = "sts:AssumeRole"
#         Condition = {
#           StringEquals = {
#             "sts:ExternalId" = var.snowflake_external_id
#           }
#         }
#       }
#     ]
#   })
# 
#   tags = merge(var.tags, {
#     Name = "snowflake-s3-access-${var.client_name}-${var.environment}"
#   })
# }
# 
# variable "snowflake_aws_account_id" {
#   description = "Snowflake AWS account ID for S3 access"
#   type        = string
#   default     = "REPLACE_WITH_SNOWFLAKE_ACCOUNT_ID"
# }
# 
# variable "snowflake_external_id" {
#   description = "Snowflake external ID for S3 access"
#   type        = string
#   default     = "REPLACE_WITH_EXTERNAL_ID"
# }
# 
# # IAM policy for Snowflake S3 access (bronze and silver - read/write)
# resource "aws_iam_role_policy" "snowflake_s3_access" {
#   name = "snowflake-s3-policy"
#   role = aws_iam_role.snowflake_s3_access.id
# 
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Action = [
#           "s3:GetObject",
#           "s3:GetObjectVersion",
#           "s3:PutObject",
#           "s3:DeleteObject",
#           "s3:ListBucket"
#         ]
#         Resource = [
#           "${aws_s3_bucket.bronze.arn}/*",
#           "${aws_s3_bucket.silver.arn}/*",
#           aws_s3_bucket.bronze.arn,
#           aws_s3_bucket.silver.arn
#         ]
#       }
#     ]
#   })
# }
# 
# # IAM policy for Snowflake to read from raw bucket (storage integration)
# resource "aws_iam_role_policy" "snowflake_raw_read" {
#   name = "snowflake-raw-read-policy"
#   role = aws_iam_role.snowflake_s3_access.id
# 
#   policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Effect = "Allow"
#         Action = [
#           "s3:GetObject",
#           "s3:GetObjectVersion",
#           "s3:ListBucket",
#           "s3:GetBucketLocation"
#         ]
#         Resource = [
#           "${aws_s3_bucket.raw.arn}/*",
#           aws_s3_bucket.raw.arn
#         ]
#       }
#     ]
#   })
# }

# Outputs
output "raw_bucket_name" {
  value = aws_s3_bucket.raw.id
}

output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.id
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver.id
}

# output "snowflake_role_arn" {
#   value = aws_iam_role.snowflake_s3_access.arn
# }

output "client_prefix" {
  value = "clients/${var.client_name}"
}
