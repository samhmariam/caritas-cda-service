"""
Census reverse ETL assets.
"""

from dagster import AssetExecutionContext, asset

from caritas_cda.resources import CensusResource


@asset(
    description="Trigger Census sync for customer activation",
    group_name="activation",
    deps=["dbt_caritas_cda_assets"]  # Depends on dbt models completing
)
def census_customer_sync(
    context: AssetExecutionContext,
    census: CensusResource
) -> dict:
    """
    Triggers Census sync to push activation data to downstream tools.
    """
    sync_id = context.op_config.get("census_sync_id")
    
    if not sync_id:
        context.log.warning("No Census sync ID configured, skipping")
        return {"status": "skipped"}
    
    context.log.info(f"Triggering Census sync ID: {sync_id}")
    
    result = census.trigger_sync(sync_id)
    
    return {
        "sync_id": sync_id,
        "status": result.get("status"),
        "census_run_id": result.get("sync_run_id")
    }
