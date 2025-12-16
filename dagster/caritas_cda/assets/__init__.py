"""
Asset initialization.
"""

from caritas_cda.assets.bronze import bronze_data_validation
from caritas_cda.assets.census_assets import census_customer_sync
from caritas_cda.assets.dbt_assets import dbt_caritas_cda_assets

__all__ = [
    "bronze_data_validation",
    "dbt_caritas_cda_assets",
    "census_customer_sync",
]
