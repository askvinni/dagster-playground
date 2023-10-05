import json
import os
from typing import Union

from dagster import (
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    DefaultSensorStatus,
    GraphDefinition,
    Optional,
    RunRequest,
    SensorEvaluationContext,
    asset_sensor,
    define_asset_job,
)


def generate_asset_and_job_from_graph(
    job_name: str,
    graph: GraphDefinition,
    asset_key: list[str],
    config: Union[dict, None],
    asset_group_name: Optional[str] = None,
    metadata_by_output_name: Optional[dict] = None,
    resource_defs=None,
) -> dict:
    asset = AssetsDefinition.from_graph(
        graph,
        keys_by_output_name={"result": AssetKey(asset_key)},
        group_name=asset_group_name,
        metadata_by_output_name=metadata_by_output_name,
        resource_defs=resource_defs,
    )

    asset_job = define_asset_job(
        name=f"{job_name}",
        selection=AssetSelection.keys(AssetKey(asset_key)).downstream(),
        config=config,
    )

    return {"asset": asset, "asset_job": asset_job}


def generate_dbt_downstream_asset_sensors(project):
    manifest_path = os.path.join("dp_dbthub", project, "target", "manifest.json")
    with open(manifest_path, "r", encoding="utf8") as f:
        manifest_json = json.load(f)

    asset_sensors = []
    for _, source in manifest_json["sources"].items():
        source_asset_key = [source["source_name"], source["schema"], source["name"]]
        # assumption: source asset key is always two elements following the schema ["src_landing", "src_<table>"]
        table_wo_prefix = source_asset_key[2][4:]
        selection_asset = ["src_rawmart", f"raw_{table_wo_prefix}"]

        downstream_asset_job = define_asset_job(
            name=f"{table_wo_prefix}_downstream_job",
            selection=AssetSelection.keys(AssetKey(selection_asset)).downstream(),
        )

        @asset_sensor(
            name=f"{table_wo_prefix}_sensor",
            asset_key=AssetKey(source_asset_key),
            job=downstream_asset_job,
            default_status=DefaultSensorStatus.RUNNING,
        )
        def downstream_asset_sensor(context: SensorEvaluationContext, _):
            yield RunRequest(run_key=context.cursor)

        asset_sensors.append(downstream_asset_sensor)

    return asset_sensors
