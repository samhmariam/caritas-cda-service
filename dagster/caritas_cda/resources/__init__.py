"""
Dagster resources for external systems.
"""

import os
from typing import Any

import boto3
from dagster import ConfigurableResource
from dagster_snowflake import SnowflakeResource
from dagster_dbt import DbtCliResource


class S3Resource(ConfigurableResource):
    """S3 client for interacting with data lake."""
    
    region_name: str = "eu-west-2"
    
    def get_client(self):
        """Get boto3 S3 client."""
        return boto3.client('s3', region_name=self.region_name)
    
    def list_objects(self, bucket: str, prefix: str) -> list[dict[str, Any]]:
        """List objects in S3 bucket with given prefix."""
        client = self.get_client()
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return response.get('Contents', [])
    
    def upload_file(self, file_path: str, bucket: str, key: str) -> None:
        """Upload file to S3."""
        client = self.get_client()
        client.upload_file(file_path, bucket, key)


class CensusResource(ConfigurableResource):
    """Census API client for reverse ETL."""
    
    api_key: str
    base_url: str = "https://app.getcensus.com/api/v1"
    
    def trigger_sync(self, sync_id: int) -> dict[str, Any]:
        """Trigger a Census sync."""
        import requests
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            f"{self.base_url}/syncs/{sync_id}/trigger",
            headers=headers
        )
        response.raise_for_status()
        return response.json()


def get_snowflake_resource() -> SnowflakeResource:
    """Factory for Snowflake resource from environment variables."""
    return SnowflakeResource(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "STAGING"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def get_dbt_resource() -> DbtCliResource:
    """Factory for dbt CLI resource."""
    return DbtCliResource(
        project_dir=os.getenv("DBT_PROJECT_DIR", "../dbt"),
        profiles_dir=os.getenv("DBT_PROFILES_DIR", "../dbt"),
    )
