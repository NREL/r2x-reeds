"""Prepare parser inputs and store prepared values in PluginContext metadata."""

from __future__ import annotations

import calendar
from datetime import datetime

import numpy as np
import polars as pl
from rust_ok import Err, Ok, Result

from r2x_core import PluginContext

from .core import (
    DAILY_TIME_INDEX,
    GENERATOR_DATA,
    HOURLY_TIME_INDEX,
    HYDRO_CF,
    INITIAL_TIMESTAMP,
    MONTH_MAP,
    RESERVE_PERCENTAGES,
    SOLVE_YEARS,
    WEATHER_YEARS,
    YEAR_MONTH_DAY_HOURS,
)
from .enum_mappings import RESERVE_TYPE_MAP
from .models import ReEDSReservePercentages, ReserveType
from .parser_runtime import parser_defaults, read_data_file, time_periods_per_year
from .parser_utils import prepare_generator_datasets


def validate_parser_configuration(context: PluginContext) -> Result[None, str]:
    """Validate configuration required by parser preparation."""
    defaults = parser_defaults(context)
    periods = context.config.time_periods_per_year
    if periods is None:
        periods = defaults.get("default_values", {}).get("time_periods_per_year")
    if not isinstance(periods, int) or periods < 1:
        return Err("time_periods_per_year must be a positive integer")

    configured_names = defaults.get("generator_datasets")
    required_roles = {
        "capacity",
        "fuel_price",
        "biofuel_price",
        "fuel_tech_map",
        "heat_rate",
        "cost_vom",
        "forced_outages",
        "planned_outages",
        "maxage",
        "storage_duration",
        "storage_efficiency",
        "storage_duration_out",
        "consume_characteristics",
        "ramp_rate",
    }
    if not isinstance(configured_names, dict):
        return Err("generator_datasets must be configured as a mapping")
    missing_roles = sorted(required_roles - set(configured_names))
    if missing_roles:
        return Err(f"generator_datasets is missing required roles: {missing_roles}")
    return Ok(None)


def prepare_time_metadata(context: PluginContext) -> Result[None, str]:
    """Prepare solve/weather indexes and calendar metadata."""
    solve_years = (
        [context.config.solve_year]
        if isinstance(context.config.solve_year, int)
        else list(context.config.solve_year)
    )
    weather_years = (
        [context.config.weather_year]
        if isinstance(context.config.weather_year, int)
        else list(context.config.weather_year)
    )
    if not solve_years or not weather_years:
        return Err("solve_year and weather_year must contain at least one year")

    weather_year = weather_years[0]
    periods = time_periods_per_year(context)
    hourly_index = np.arange(
        np.datetime64(f"{weather_year}"),
        np.datetime64(f"{weather_year + 1}"),
        dtype="datetime64[h]",
    )[:periods]
    context.metadata[SOLVE_YEARS] = solve_years
    context.metadata[WEATHER_YEARS] = weather_years
    context.metadata[HOURLY_TIME_INDEX] = hourly_index
    context.metadata[DAILY_TIME_INDEX] = np.arange(
        np.datetime64(f"{weather_year}"),
        np.datetime64(f"{weather_year + 1}"),
        dtype="datetime64[D]",
    )
    context.metadata[INITIAL_TIMESTAMP] = hourly_index[0].astype("datetime64[s]").astype(datetime)
    context.metadata[MONTH_MAP] = {calendar.month_abbr[index].lower(): index for index in range(1, 13)}
    context.metadata[YEAR_MONTH_DAY_HOURS] = pl.DataFrame(
        {
            "year": [year for year in solve_years for _ in range(1, 13)],
            "month_num": [month for _ in solve_years for month in range(1, 13)],
            "days_in_month": [
                calendar.monthrange(year, month)[1] for year in solve_years for month in range(1, 13)
            ],
            "hours_in_month": [
                calendar.monthrange(year, month)[1] * 24 for year in solve_years for month in range(1, 13)
            ],
        }
    )
    return Ok(None)


def prepare_generator_data(context: PluginContext) -> Result[None, str]:
    """Prepare generator rows and store them in context metadata."""
    defaults = parser_defaults(context)
    configured_names = defaults["generator_datasets"]
    datasets = {
        role: read_data_file(context, str(dataset_name)) for role, dataset_name in configured_names.items()
    }
    result = prepare_generator_datasets(
        datasets,
        excluded_technologies=list(defaults.get("excluded_techs", [])),
        technology_categories=dict(defaults.get("tech_categories", {})),
        default_values=dict(defaults.get("default_values", {})),
    )
    if result.is_err():
        return Err(str(result.err()))
    generator_data = result.ok()
    if generator_data is None:
        return Err("Generator dataset preparation returned no data")
    context.metadata[GENERATOR_DATA] = generator_data
    return Ok(None)


def prepare_hydro_data(context: PluginContext) -> Result[None, str]:
    """Prepare hydro monthly factors and join calendar values."""
    hydro_cf = read_data_file(context, "hydro_cf")
    if hydro_cf is None:
        context.metadata[HYDRO_CF] = None
        return Ok(None)
    try:
        prepared = (
            hydro_cf.with_columns(
                pl.col("month")
                .map_elements(
                    lambda value: context.metadata[MONTH_MAP].get(value, value),
                    return_dtype=pl.Int16,
                )
                .alias("month_num")
            )
            .sort(["year", "technology", "region", "month_num"])
            .collect()
            .join(context.metadata[YEAR_MONTH_DAY_HOURS], on=["year", "month_num"], how="left")
        )
    except (KeyError, pl.exceptions.PolarsError) as exc:
        return Err(f"hydro_cf is malformed: {exc}")
    context.metadata[HYDRO_CF] = prepared
    return Ok(None)


def prepare_reserve_data(context: PluginContext) -> Result[None, str]:
    """Prepare reserve percentages from mapped values or defaults."""
    percentages: dict[ReserveType, ReEDSReservePercentages] = {}
    reserve_data = read_data_file(context, "reserve_percentages")
    if reserve_data is not None:
        try:
            for row in reserve_data.collect().iter_rows(named=True):
                reserve_type = RESERVE_TYPE_MAP.get(str(row.get("reserve_type") or "").upper())
                if reserve_type is not None:
                    percentages[reserve_type] = ReEDSReservePercentages.model_validate(
                        {**row, "reserve_type": reserve_type}
                    )
        except (KeyError, TypeError, ValueError, pl.exceptions.PolarsError) as exc:
            return Err(f"reserve_percentages is malformed: {exc}")
    else:
        defaults = parser_defaults(context)
        load_reserves = defaults.get("load_reserves", {})
        wind_reserves = defaults.get("wind_reserves", {})
        solar_reserves = defaults.get("solar_reserves", {})
        for name in set(load_reserves) | set(wind_reserves) | set(solar_reserves):
            reserve_type = RESERVE_TYPE_MAP.get(str(name).upper())
            if reserve_type is None:
                continue
            percentages[reserve_type] = ReEDSReservePercentages(
                reserve_type=reserve_type,
                or_load_percentage=load_reserves.get(name, 0.0) or 0.0,
                or_wind_percentage=wind_reserves.get(name, 0.0) or 0.0,
                or_pv_percentage=solar_reserves.get(name, 0.0) or 0.0,
            )
    context.metadata[RESERVE_PERCENTAGES] = percentages
    return Ok(None)


def prepare_parser_data(context: PluginContext) -> Result[None, str]:
    """Prepare all data needed by parser component and time-series phases."""
    configuration_result = validate_parser_configuration(context)
    if configuration_result.is_err():
        return configuration_result
    for prepare in (prepare_time_metadata, prepare_generator_data, prepare_hydro_data, prepare_reserve_data):
        result = prepare(context)
        if result.is_err():
            return result
    return Ok(None)
