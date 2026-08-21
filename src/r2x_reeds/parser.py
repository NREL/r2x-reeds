"""ReEDS parser plugin backed by r2x-core's PluginContext and rules."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import polars as pl
from loguru import logger
from rust_ok import Err, Ok, Result

from r2x_core import Plugin, System

from .checks import check_dataset_non_empty, check_required_values_in_column
from .core import (
    UPGRADES_COMPLETE,
)
from .parser_builders import (
    build_emissions,
    build_generators,
    build_loads,
    build_regions,
    build_reserves,
    build_transmission,
)
from .parser_config import load_parser_configuration
from .parser_planning_inputs import attach_planning_inputs
from .parser_preparation import prepare_parser_data, validate_parser_configuration
from .parser_runtime import read_data_file
from .parser_time_series import (
    attach_hydro_budgets,
    attach_load_profiles,
    attach_renewable_profiles,
    attach_reserve_membership,
    attach_reserve_profiles,
    postprocess_system,
)
from .parser_utils import build_synthetic_hour_map
from .plugin_config import ReEDSConfig
from .upgrader.data_upgrader import run_reeds_upgrades


class ReEDSParser(Plugin[ReEDSConfig]):
    """Build an :class:`r2x_core.System` from a ReEDS run directory.

    Parser configuration and component construction are declarative. The
    parser-specific modules prepare ReEDS rows and relationships; the rules in
    ``config/parser_rules.json`` select target models, map fields, call getters,
    and attach supplemental attributes.
    """

    def on_validate_config(self) -> Result[None, str]:
        """Load parser rules and defaults into the active PluginContext."""
        return load_parser_configuration(self.ctx)

    def on_upgrade(self) -> Result[System | None, str]:
        """Run file upgrades when validation was not the caller."""
        if self.ctx.metadata.get(UPGRADES_COMPLETE):
            return Ok(None)
        result = run_reeds_upgrades(store=self.store, ctx=self.ctx)
        if result.is_err():
            return Err(str(result.err()))
        self.ctx.metadata[UPGRADES_COMPLETE] = True
        return Ok(None)

    def read_data_file(self, name: str) -> Any:
        """Read a mapped ReEDS dataset through the context DataStore."""
        return read_data_file(self.ctx, name)

    def on_validate(self) -> Result[None, str]:
        """Upgrade, configure, and validate the ReEDS input store."""
        configuration_result = load_parser_configuration(self.ctx)
        if configuration_result.is_err():
            return configuration_result

        upgrade_result = self.on_upgrade()
        if upgrade_result.is_err():
            return Err(str(upgrade_result.err()))

        configuration_result = validate_parser_configuration(self.ctx)
        if configuration_result.is_err():
            return configuration_result

        solve_years: Iterable[int] = (
            [self.config.solve_year] if isinstance(self.config.solve_year, int) else self.config.solve_year
        )
        placeholders = self.config.model_dump()
        result = check_required_values_in_column(
            store=self.store,
            dataset="modeled_years",
            required_values=solve_years,
            what="Solve year(s)",
            placeholders=placeholders,
        )
        if result.is_err():
            return Err(str(result.err()))

        weather_years = (
            [self.config.weather_year]
            if isinstance(self.config.weather_year, int)
            else list(self.config.weather_year)
        )
        try:
            hour_map_data = self.read_data_file("hour_map")
        except FileNotFoundError as exc:
            hour_map = build_synthetic_hour_map(weather_years)
            logger.warning("hour_map is missing ({}); using a synthetic hour map", exc)
        except (KeyError, OSError, TypeError, ValueError, pl.exceptions.PolarsError) as exc:
            return Err(f"Weather year(s): hour_map could not be read: {exc}")
        else:
            if hour_map_data is None:
                hour_map = build_synthetic_hour_map(weather_years)
                logger.warning("hour_map is not mapped; using a synthetic hour map")
            else:
                try:
                    hour_map = hour_map_data.collect() if hasattr(hour_map_data, "collect") else hour_map_data
                except (OSError, TypeError, ValueError, pl.exceptions.PolarsError) as exc:
                    return Err(f"Weather year(s): hour_map could not be collected: {exc}")
                if not isinstance(hour_map, pl.DataFrame):
                    return Err("Weather year(s): hour_map did not return a Polars DataFrame")
                if hour_map.is_empty():
                    return Err("Weather year(s): hour_map file exists but contains no rows")
                if "year" not in hour_map.columns:
                    return Err("Weather year(s): hour_map is missing required column 'year'")

        available_weather_years = {
            int(value) for value in hour_map["year"].drop_nulls().to_list() if value is not None
        }
        missing_weather_years = [year for year in weather_years if year not in available_weather_years]
        if missing_weather_years:
            return Err(
                f"Weather year(s) {missing_weather_years} not found in hour_map.year. "
                f"Available values: {sorted(available_weather_years)}"
            )

        for dataset_name in self.store.list_data():
            data_file = self.store[dataset_name]
            info = data_file.info
            if dataset_name == "hour_map" or (info and (info.is_optional or not info.is_input)):
                continue
            result = check_dataset_non_empty(self.store, dataset_name, placeholders=placeholders)
            if result.is_err():
                return Err(f"{dataset_name}: {result.err()}")
        return Ok(None)

    def on_prepare(self) -> Result[None, str]:
        """Prepare generator, hydro, reserve, and time metadata in the context."""
        configuration_result = load_parser_configuration(self.ctx)
        if configuration_result.is_err():
            return configuration_result
        result = prepare_parser_data(self.ctx)
        if result.is_err():
            return Err(str(result.err()))
        return Ok(None)

    def on_build(self) -> Result[System, str]:
        """Build components and attach time series in dependency order."""
        system = System(name=self.config.case_name or "ReEDS")
        self.ctx.system = system
        self.ctx.target_system = system

        steps = (
            build_regions,
            attach_planning_inputs,
            build_generators,
            build_transmission,
            build_loads,
            build_reserves,
            build_emissions,
            attach_reserve_membership,
            attach_load_profiles,
            attach_renewable_profiles,
            attach_reserve_profiles,
            attach_hydro_budgets,
        )
        for step in steps:
            result = step(self.ctx)
            if result.is_err():
                return Err(str(result.err()))
        postprocess_result = postprocess_system(self.ctx)
        if postprocess_result.is_err():
            return Err(str(postprocess_result.err()))
        return Ok(system)
