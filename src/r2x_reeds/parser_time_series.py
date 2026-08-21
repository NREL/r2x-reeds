"""Attach ReEDS time series and system relationships."""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import polars as pl
from infrasys import SingleTimeSeries
from loguru import logger
from rust_ok import Err, Ok, Result

from r2x_core import PluginContext

from .core import (
    HOURLY_TIME_INDEX,
    HYDRO_CF,
    INITIAL_TIMESTAMP,
    RESERVE_PERCENTAGES,
    SOLVE_YEARS,
)
from .models.components import ReEDSDemand, ReEDSGenerator, ReEDSReserve
from .parser_runtime import parser_defaults, read_data_file, time_periods_per_year
from .parser_utils import calculate_reserve_requirement, monthly_to_hourly_polars, tech_matches_category


def truncate_time_series(values: np.ndarray | list[float], *, length: int) -> np.ndarray:
    """Convert values to float64 and retain at most the configured year length."""
    return np.asarray(values, dtype=np.float64)[:length]


def attach_load_profiles(context: PluginContext) -> Result[None, str]:
    """Attach load profiles to demand components by their region name."""

    if context.system is None:
        return Err("Parser context has no system")
    system = context.system
    data = read_data_file(context, "load_profiles")
    if data is None:
        return Err("load_profiles is required for demand time series")
    frame = data.collect()
    length = time_periods_per_year(context)
    initial_timestamp = context.metadata[INITIAL_TIMESTAMP]
    attached = 0
    for demand in system.get_components(ReEDSDemand):
        region_name = demand.name.removesuffix("_load")
        if region_name not in frame.columns:
            continue
        values = truncate_time_series(frame[region_name].to_numpy(), length=length)
        series = SingleTimeSeries.from_array(
            data=values,
            name="max_active_power",
            initial_timestamp=initial_timestamp,
            resolution=timedelta(hours=1),
        )
        system.add_time_series(series, demand)
        attached += 1
    logger.info("Attached load profiles to {} demand components", attached)
    return Ok(None)


def attach_renewable_profiles(context: PluginContext) -> Result[None, str]:
    """Attach renewable profiles to matching generators."""

    if context.system is None:
        return Err("Parser context has no system")
    system = context.system
    data = read_data_file(context, "renewable_profiles")
    if data is None:
        return Err("renewable_profiles is required for renewable time series")
    frame = data.collect()
    length = time_periods_per_year(context)
    initial_timestamp = context.metadata[INITIAL_TIMESTAMP]
    attached = 0
    for column in frame.columns:
        parts = column.split("|")
        if len(parts) != 2:
            continue
        technology, region_name = parts
        generators = [
            generator
            for generator in system.get_components(ReEDSGenerator)
            if generator.technology == technology
            and generator.region is not None
            and generator.region.name == region_name
        ]
        if not generators:
            continue
        profile = truncate_time_series(frame[column].to_numpy(), length=length)
        for generator in generators:
            series = SingleTimeSeries.from_array(
                data=profile * generator.capacity,
                name="max_active_power",
                initial_timestamp=initial_timestamp,
                resolution=timedelta(hours=1),
            )
            system.add_time_series(series, generator)
            attached += 1
    logger.debug("Attached renewable profiles to {} generator components", attached)
    return Ok(None)


def attach_reserve_profiles(context: PluginContext) -> Result[None, str]:
    """Calculate and attach reserve requirements by transmission region."""

    if context.system is None:
        return Err("Parser context has no system")
    system = context.system
    defaults = parser_defaults(context)
    categories = defaults.get("tech_categories", {})
    percentages = context.metadata.get(RESERVE_PERCENTAGES, {})
    hourly_index = context.metadata[HOURLY_TIME_INDEX]
    initial_timestamp = context.metadata[INITIAL_TIMESTAMP]
    attached = 0

    for reserve in system.get_components(ReEDSReserve):
        region_name = reserve.name.rsplit("_", 1)[0]
        reserve_pct = percentages.get(reserve.reserve_type)
        if reserve_pct is None:
            continue
        wind = []
        solar = []
        loads = []
        for generator in system.get_components(ReEDSGenerator):
            if generator.region is None or generator.region.transmission_region != region_name:
                continue
            profile = system.get_time_series(generator).data if system.has_time_series(generator) else None
            entry = {"capacity": generator.capacity, "time_series": profile}
            if tech_matches_category(generator.technology, "wind", categories):
                wind.append(entry)
            if tech_matches_category(generator.technology, "solar", categories):
                solar.append(entry)
        for demand in system.get_components(ReEDSDemand):
            if demand.region is None or demand.region.transmission_region != region_name:
                continue
            profile = system.get_time_series(demand).data if system.has_time_series(demand) else None
            loads.append({"time_series": profile})

        result = calculate_reserve_requirement(
            wind_generators=wind,
            solar_generators=solar,
            loads=loads,
            hourly_time_index=hourly_index,
            wind_pct=float(reserve_pct.or_wind_percentage),
            solar_pct=float(reserve_pct.or_pv_percentage),
            load_pct=float(reserve_pct.or_load_percentage),
        )
        if result.is_err():
            return Err(str(result.err()))
        profile = result.ok()
        if profile is None or len(profile) == 0:
            continue
        series = SingleTimeSeries.from_array(
            data=truncate_time_series(profile, length=time_periods_per_year(context)),
            name="requirement",
            initial_timestamp=initial_timestamp,
            resolution=timedelta(hours=1),
        )
        system.add_time_series(series, reserve)
        attached += 1
    logger.info("Attached reserve requirements to {} reserve components", attached)
    return Ok(None)


def attach_reserve_membership(context: PluginContext) -> Result[None, str]:
    """Attach eligible reserve names to generators in the same region."""

    if context.system is None:
        return Err("Parser context has no system")
    system = context.system
    defaults = parser_defaults(context)
    excluded_map = defaults.get("excluded_from_reserves", {})
    categories = defaults.get("tech_categories", {})
    attached = 0
    for reserve in system.get_components(ReEDSReserve):
        reserve_type = reserve.reserve_type.value.upper()
        if reserve_type in {"FLEXIBILITY_UP", "FLEXIBILITY_DOWN"}:
            reserve_type = "FLEXIBILITY"
        excluded = excluded_map.get(reserve_type, [])
        region_name = reserve.name.rsplit("_", 1)[0]
        for generator in system.get_components(ReEDSGenerator):
            if generator.region is None or generator.region.transmission_region != region_name:
                continue
            if any(
                tech_matches_category(generator.technology, category, categories) for category in excluded
            ):
                continue
            values = generator.ext.get("reserves")
            reserves = [str(value) for value in values] if isinstance(values, list) else []
            if reserve.name not in reserves:
                generator.ext["reserves"] = [*reserves, reserve.name]
                attached += 1
    logger.info("Attached {} reserve membership links", attached)
    return Ok(None)


def attach_hydro_budgets(context: PluginContext) -> Result[None, str]:
    """Attach hourly hydro budgets generated from monthly capacity factors."""

    if context.system is None:
        return Err("Parser context has no system")
    system = context.system
    hydro_frame = context.metadata.get(HYDRO_CF)
    if not isinstance(hydro_frame, pl.DataFrame):
        return Ok(None)
    categories = parser_defaults(context).get("tech_categories", {})
    initial_timestamp = context.metadata[INITIAL_TIMESTAMP]
    length = time_periods_per_year(context)
    attached = 0
    for generator in system.get_components(ReEDSGenerator):
        if generator.region is None or not tech_matches_category(generator.technology, "hydro", categories):
            continue
        matching = hydro_frame.filter(
            (pl.col("technology") == generator.technology) & (pl.col("region") == generator.region.name)
        )
        if generator.identity.vintage is not None and "vintage" in matching.columns:
            matching = matching.filter(pl.col("vintage") == generator.identity.vintage)
        for year in context.metadata[SOLVE_YEARS]:
            year_frame = matching.filter(pl.col("year") == year).sort("month_num")
            if year_frame.height != 12 or year_frame["hydro_cf"].null_count() > 0:
                continue
            monthly = [
                generator.capacity * float(cf) * float(hours) / float(days)
                for cf, hours, days in zip(
                    year_frame["hydro_cf"].to_list(),
                    year_frame["hours_in_month"].to_list(),
                    year_frame["days_in_month"].to_list(),
                    strict=True,
                )
            ]
            profile_result = monthly_to_hourly_polars(year, monthly)
            if profile_result.is_err():
                return Err(str(profile_result.err()))
            profile = profile_result.ok()
            if profile is None:
                continue
            series = SingleTimeSeries.from_array(
                data=truncate_time_series(profile, length=length),
                name="hydro_budget",
                initial_timestamp=initial_timestamp,
                resolution=timedelta(hours=1),
            )
            system.add_time_series(series, generator, solve_year=year)
            attached += 1
    logger.info("Attached {} hydro budget profiles", attached)
    return Ok(None)


def postprocess_system(context: PluginContext) -> Result[None, str]:
    """Set system metadata and log the final component summary."""
    system = context.system
    if system is None:
        return Err("Parser context has no system")
    if context.current_version is not None:
        system.data_format_version = context.current_version
    config = context.config
    system.description = (
        f"ReEDS model system for case '{config.case_name}', scenario '{config.scenario}', "
        f"solve years: {config.solve_year}, weather years: {config.weather_year}"
    )
    total = sum(
        sum(1 for _ in system.get_components(component_type))
        for component_type in system.get_component_types()
    )
    logger.info("System name: {}", system.name)
    logger.info("System composition: {} total components", total)
    return Ok(None)
