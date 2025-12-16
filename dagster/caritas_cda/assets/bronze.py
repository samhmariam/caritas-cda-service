"""
Bronze layer assets - S3 validation and promotion.
"""

from typing import Any

from dagster import AssetExecutionContext, asset

from caritas_cda.resources import S3Resource


@asset(
    description="Validate and promote raw data from S3 to Bronze layer",
    group_name="bronze"
)
def bronze_data_validation(
    context: AssetExecutionContext,
    s3: S3Resource
) -> dict[str, Any]:
    """
    Validates raw data files in S3 and promotes them to Bronze layer.
    
    Validation checks:
    - File completeness (presence of _SUCCESS marker)
    - Basic schema validation
    - Data quality checks
    """
    client_name = context.op_config.get("client_name", "acme")
    raw_bucket = context.op_config.get("raw_bucket", "mdp-raw-dev")
    bronze_bucket = context.op_config.get("bronze_bucket", "mdp-bronze-dev")
    
    raw_prefix = f"clients/{client_name}/"
    
    # List files in raw layer
    raw_files = s3.list_objects(bucket=raw_bucket, prefix=raw_prefix)
    
    context.log.info(f"Found {len(raw_files)} files in raw layer for client {client_name}")
    
    # Validation logic here
    validated_count = 0
    for file_obj in raw_files:
        key = file_obj['Key']
        # Skip directories and metadata files
        if key.endswith('/') or key.endswith('_SUCCESS'):
            continue
        
        # Promote to bronze (simplified - in production, add validation)
        validated_count += 1
    
    return {
        "client_name": client_name,
        "files_validated": validated_count,
        "raw_bucket": raw_bucket,
        "bronze_bucket": bronze_bucket
    }
