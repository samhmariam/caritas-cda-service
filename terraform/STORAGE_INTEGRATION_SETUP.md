# Storage Integration Setup Guide

## Overview
The Snowflake storage integration requires a **two-step manual process** because Snowflake needs to generate AWS trust policy details before you can create the IAM role.

## Current Status
✅ 23 Bronze tables created successfully  
⏳ Storage integration, file format, and stage ready to deploy  
❌ IAM role needs manual setup with Snowflake-generated credentials

## Step-by-Step Instructions

### Step 1: Run Terraform Apply
This will create the storage integration **without** the IAM role, which is expected:

```bash
cd terraform/environments/dev
terraform apply tfplan
```

**Expected:** The storage integration will be created successfully.

### Step 2: Get AWS Trust Policy Details from Snowflake

Run this SQL in Snowflake to get the AWS details:

```sql
DESC STORAGE INTEGRATION WISE_DEV_RAW_S3_INTEGRATION;
```

Look for these two values in the output:
- **`STORAGE_AWS_IAM_USER_ARN`** - Something like `arn:aws:iam::123456789:user/abc123-s`
- **`STORAGE_AWS_EXTERNAL_ID`** - A unique ID like `ABC12345_SFCRole=1_abcdefg=`

### Step 3: Create IAM Role in AWS

**Option A: Via AWS Console**
1. Go to IAM → Roles → Create Role  
2. Select "Another AWS Account"
3. Enter the **Account ID** from the IAM User ARN (e.g., `123456789`)
4. Check "Require external ID" and paste the `STORAGE_AWS_EXTERNAL_ID`
5. Attach this inline policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::cda-raw-dev/*",
        "arn:aws:s3:::cda-raw-dev"
      ]
    }
  ]
}
```

6. Name the role: `snowflake-s3-access-wise-dev`
7. Copy the Role ARN

**Option B: Via AWS CLI**

First, create `trust-policy.json`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "<STORAGE_AWS_IAM_USER_ARN from Step 2>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID from Step 2>"
        }
      }
    }
  ]
}
```

Then run:

```bash
# Create the role
aws iam create-role \
  --role-name snowflake-s3-access-wise-dev \
  --assume-role-policy document file://trust-policy.json \
  --profile my-dev

# Attach the policy
aws iam put-role-policy \
  --role-name snowflake-s3-access-wise-dev \
  --policy-name snowflake-raw-read \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:GetObjectVersion", "s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": ["arn:aws:s3:::cda-raw-dev/*", "arn:aws:s3:::cda-raw-dev"]
    }]
  }' \
  --profile my-dev

# Get the role ARN
aws iam get-role --role-name snowflake-s3-access-wise-dev --profile my-dev --query 'Role.Arn'
```

### Step 4: Update Storage Integration in Snowflake

Run this SQL with the IAM Role ARN from Step 3:

```sql
ALTER STORAGE INTEGRATION WISE_DEV_RAW_S3_INTEGRATION
SET STORAGE_AWS_ROLE_ARN = '<IAM_ROLE_ARN from Step 3>';
```

### Step 5: Test the Integration

```sql
-- Test listing files from S3
LIST @WISE_DEV_RAW.RAW.WISE_DEV_RAW_STAGE;

-- If files exist, test querying them
SELECT $1 FROM @WISE_DEV_RAW.RAW.WISE_DEV_RAW_STAGE (FILE_FORMAT => WISE_DEV_RAW.RAW.WISE_DEV_GZIP_JSON) LIMIT 10;
```

## Verification

✅ Storage integration shows `ENABLED = TRUE`  
✅ `LIST @stage` returns files or "no files found" (not an error)  
✅ Can query files with `SELECT ... FROM @stage`

## Optional: Import IAM Role into Terraform

If you want Terraform to manage the IAM role in the future:

1. Uncomment the IAM role resources in `terraform/modules/aws-s3-lake/main.tf`
2. Update the placeholder values with the real ones from Step 2  
3. Run `terraform import` to bring it under Terraform management

## Summary

This two-step process is necessary because:
1. Snowflake generates unique AWS credentials for each integration
2. These credentials must be in the IAM role trust policy
3. You can't know them until the integration is created

After completion, your data pipeline will be able to:
- Load data from S3 using COPY INTO commands
- Query S3 files directly using external stages
- Manage access via Snowflake roles
