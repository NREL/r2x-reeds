"""Apply cost-optimal siting load increments to existing demand profiles."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
from infrasys import SingleTimeSeries, System
from loguru import logger
from pydantic import Field
from rust_ok import Err, Ok, Result

from r2x_core import DataStore, PluginConfig, expose_plugin
from r2x_reeds.models.components import ReEDSDemand
from r2x_reeds.parser_utils import expand_loadsite_hourly


class OptimalSitingConfig(PluginConfig):
    """Configuration for applying loadsite-based optimal siting increments."""

    loadsite_op_fpath: Path | str | None = Field(
        default=None,
        description="Path to loadsite_op.csv file.",
    )
    hour_map_myr_fpath: Path | str | None = Field(
        default=None,
        description="Path to hmap_myr.csv file.",
    )
    solve_year: int | list[int] | None = Field(
        default=None,
        description="Solve year used to filter loadsite rows when the file contains multiple years.",
    )


@expose_plugin
def add_optimal_siting(system: System, config: OptimalSitingConfig) -> Result[System, str]:
    """Add optimal siting increments to existing load profiles.

    This transform expects load time series to already exist on ReEDSDemand components.
    """
    if config.loadsite_op_fpath is None or config.hour_map_myr_fpath is None:
        logger.debug("Missing loadsite_op_fpath or hour_map_myr_fpath. Skipping optimal siting plugin.")
        return Ok(system)

    try:
        loadsite = DataStore.load_file(config.loadsite_op_fpath, name="loadsite_op")
        hour_map_myr = DataStore.load_file(config.hour_map_myr_fpath, name="hour_map_myr")

        if isinstance(loadsite, pl.LazyFrame):
            loadsite = loadsite.collect()
        if isinstance(hour_map_myr, pl.LazyFrame):
            hour_map_myr = hour_map_myr.collect()

        if loadsite is None or loadsite.is_empty():
            logger.debug("loadsite_op data is empty. Skipping optimal siting plugin.")
            return Ok(system)
        if hour_map_myr is None or hour_map_myr.is_empty():
            logger.debug("hour_map_myr data is empty. Skipping optimal siting plugin.")
            return Ok(system)

        loadsite = _normalize_loadsite_columns(loadsite)
        hour_map_myr = _normalize_hour_map_columns(hour_map_myr)

        solve_year = _resolve_solve_year(config.solve_year)
        if "year" in loadsite.columns and solve_year is not None:
            loadsite = loadsite.filter(pl.col("year") == solve_year)

        if loadsite.is_empty():
            logger.info("No loadsite rows remained after year filtering. Skipping optimal siting plugin.")
            return Ok(system)

        expanded_result = expand_loadsite_hourly(loadsite, hour_map_myr)
        if expanded_result.is_err():
            return Err(str(expanded_result.unwrap_err()))

        expanded = expanded_result.unwrap()
        increments_by_region: dict[str, np.ndarray] = {}
        for region in expanded["region"].unique().to_list():
            increments_by_region[region] = (
                expanded.filter(pl.col("region") == region).sort("sequential_hour")["value"].to_numpy()
            )

        if not increments_by_region:
            logger.info("No optimal siting increments found. Skipping optimal siting plugin.")
            return Ok(system)

        updated = 0
        for demand in system.get_components(ReEDSDemand):
            region_name = demand.region.name
            increment = increments_by_region.get(region_name)
            if increment is None:
                continue
            if not system.has_time_series(demand):
                logger.debug("Demand {} has no load profile; skipping optimal siting increment.", demand.name)
                continue

            series = system.get_time_series(demand)
            base = np.asarray(series.data, dtype=np.float64)
            increment = np.asarray(increment, dtype=np.float64)
            min_len = min(len(base), len(increment))
            if min_len == 0:
                continue

            merged = base.copy()
            merged[:min_len] = merged[:min_len] + increment[:min_len]

            updated_ts = SingleTimeSeries.from_array(
                data=merged,
                name=series.name,
                initial_timestamp=series.initial_timestamp,
                resolution=series.resolution,
            )
            system.remove_time_series(demand, name=series.name)
            system.add_time_series(updated_ts, demand)
            updated += 1

        logger.info("Applied optimal siting increments to {} demand components", updated)
    except Exception as exc:
        logger.error("Optimal siting plugin failed: {}", exc)
        return Err(str(exc))

    return Ok(system)


def _normalize_loadsite_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """Normalize known loadsite column names from raw and mapped schemas."""
    rename_map = {
        "r": "region",
        "allh": "hour_period",
        "t": "year",
        "Value": "value",
    }
    return frame.rename({k: v for k, v in rename_map.items() if k in frame.columns})


def _normalize_hour_map_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """Normalize known hour-map column names from raw and mapped schemas."""
    rename_map = {
        "yearhour": "sequential_hour",
        "h": "hour_period",
    }
    return frame.rename({k: v for k, v in rename_map.items() if k in frame.columns})


def _resolve_solve_year(solve_year: int | list[int] | None) -> int | None:
    """Resolve the primary solve year from config input."""
    if solve_year is None:
        return None
    if isinstance(solve_year, list):
        if not solve_year:
            return None
        return int(solve_year[0])
    return int(solve_year)
