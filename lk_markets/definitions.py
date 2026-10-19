"""Dagster entry point. Start the UI with:  dagster dev -m lk_markets.definitions"""
from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job, in_process_executor
from dagster_dbt import DbtCliResource

from lk_markets import assets

daily_refresh = define_asset_job("daily_refresh", selection=AssetSelection.all())

defs = Definitions(
    assets=[
        assets.cse_trade_summary, assets.cse_price_history,
        assets.fx_rates, assets.crypto_history, assets.crypto_quotes,
        assets.dbt_models, assets.powerbi_export,
    ],
    asset_checks=[assets.all_companies_received],
    jobs=[daily_refresh],
    # The CSE closes at 14:30 Colombo time, so collect at 15:00.
    schedules=[ScheduleDefinition(job=daily_refresh, cron_schedule="0 15 * * *", execution_timezone="Asia/Colombo")],
    resources={"dbt": DbtCliResource(project_dir=assets.dbt_project, dbt_executable=assets.find_dbt())},
    executor=in_process_executor,  # DuckDB allows one writer at a time, so run steps one by one
)
