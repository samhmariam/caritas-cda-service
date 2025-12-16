"""
dbt assets auto-generated from dbt manifest.
"""

import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets


DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent / "dbt"
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    project_dir=DBT_PROJECT_DIR,
)
def dbt_caritas_cda_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    dbt models as Dagster assets.
    
    This will auto-generate assets for all dbt models defined in the project.
    """
    client_name = os.getenv("DBT_CLIENT", "acme")
    
    context.log.info(f"Running dbt for client: {client_name}")
    
    yield from dbt.cli(["build"], context=context).stream()
