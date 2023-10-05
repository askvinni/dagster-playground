from dagster import Definitions, load_assets_from_modules, AutoMaterializePolicy
from . import assets

from .assets import my_schedule, my_asset_has_enough_rows

defs = Definitions(
    assets=[
        *load_assets_from_modules(
            [assets],
            auto_materialize_policy=AutoMaterializePolicy.eager(),
        ),
    ],
    asset_checks=[my_asset_has_enough_rows],
    schedules=[my_schedule],
)
