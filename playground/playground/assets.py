from dagster import (
    asset,
    define_asset_job,
    ScheduleDefinition,
    AssetExecutionContext,
    AssetKey,
    EventRecordsFilter,
    DagsterEventType,
    asset_check,
    AssetCheckResult,
    FreshnessPolicy,
    freshness_policy_sensor,
)


@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=10, cron_schedule="30 2 * * *")
)
def data_quality_raw(context: AssetExecutionContext):
    records = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
        ),
        limit=11,
    )
    context.log.info(records)


@asset
def asset_one(context: AssetExecutionContext):
    return "one"


@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=1, cron_schedule="* * * * *")
)
def asset_two(asset_one):
    return asset_one + "two"


@asset
def asset_three(asset_two, asset_one):
    return asset_one, asset_two + "three"


@asset
def asset_four(asset_two):
    return "four"


@asset
def asset_five():
    return "five"


@asset_check(asset=asset_one, description="Check that my asset has enough rows")
def my_asset_has_enough_rows() -> AssetCheckResult:
    return AssetCheckResult(success=6 > 5, metadata={"num_rows": 6})


my_job = define_asset_job("my_job", selection=[asset_one])
my_schedule = ScheduleDefinition("my_schedule", cron_schedule="* * * * *", job=my_job)
