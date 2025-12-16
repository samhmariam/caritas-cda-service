"""
Main Dagster definitions.
"""

import os

from dagster import Definitions, load_assets_from_modules

from caritas_cda import assets
from caritas_cda.resources import (
    S3Resource,
    CensusResource,
    get_snowflake_resource,
    get_dbt_resource,
)


all_assets = load_assets_from_modules([assets])


defs = Definitions(
    assets=all_assets,
    resources={
        "s3": S3Resource(region_name=os.getenv("AWS_REGION", "eu-west-2")),
        "snowflake": get_snowflake_resource(),
        "dbt": get_dbt_resource(),
        "census": CensusResource(api_key=os.getenv("CENSUS_API_KEY", "")),
    },
)
