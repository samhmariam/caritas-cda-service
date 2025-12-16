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
  description = "Client identifier"
  type        = string
}

variable "environment" {
  description = "Environment (dev, stage, prod)"
  type        = string
}

variable "secrets" {
  description = "Map of secret names to secret values"
  type        = map(string)
  sensitive   = true
}

# Create secrets in AWS Secrets Manager
resource "aws_secretsmanager_secret" "client_secrets" {
  for_each = var.secrets
  
  name        = "${var.client_name}/${var.environment}/${each.key}"
  description = "Secret for ${var.client_name} ${var.environment}: ${each.key}"
  
  recovery_window_in_days = 7
}

resource "aws_secretsmanager_secret_version" "client_secrets" {
  for_each = var.secrets
  
  secret_id     = aws_secretsmanager_secret.client_secrets[each.key].id
  secret_string = each.value
}

# IAM policy for Dagster to read secrets
resource "aws_iam_policy" "dagster_secrets_read" {
  name        = "${var.client_name}-${var.environment}-dagster-secrets-read"
  description = "Allow Dagster to read client secrets"
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret"
        ]
        Resource = [
          for secret in aws_secretsmanager_secret.client_secrets :
          secret.arn
        ]
      }
    ]
  })
}

# Output secret ARNs
output "secret_arns" {
  value = {
    for k, v in aws_secretsmanager_secret.client_secrets :
    k => v.arn
  }
}

output "dagster_policy_arn" {
  value = aws_iam_policy.dagster_secrets_read.arn
}
