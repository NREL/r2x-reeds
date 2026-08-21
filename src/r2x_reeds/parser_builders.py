"""Build ReEDS components from normalized rows and declarative rules."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import polars as pl
from loguru import logger
from rust_ok import Err, Ok, Result

from r2x_core import PluginContext

from .core import GENERATOR_DATA, RESERVE_PERCENTAGES
from .getters import build_transmission_line_name
from .models.components import (
    ReEDSGenerator,
)
from .parser_rules import materialize_parser_rows
from .parser_runtime import parser_defaults, read_data_file
from .parser_utils import (
    build_generator_emission_lookup,
    build_reserve_rows,
    match_emission_rows_to_generators,
)


def require_columns(frame: pl.DataFrame, dataset: str, columns: set[str]) -> Result[None, str]:
    """Validate the columns needed by a parser data transformation."""
    missing = sorted(columns - set(frame.columns))
    if missing:
        return Err(f"{dataset} is missing required columns: {missing}")
    return Ok(None)


def build_regions(context: PluginContext) -> Result[None, str]:
    """Build regions using the ``region`` parser rule."""

    if context.system is None:
        return Err("Parser context has no system")
    hierarchy = read_data_file(context, "hierarchy")
    if hierarchy is None:
        return Err("hierarchy is required to build regions")
    frame = hierarchy.collect()
    columns_result = require_columns(frame, "hierarchy", {"region_id"})
    if columns_result.is_err():
        return columns_result
    result = materialize_parser_rows(
        frame.iter_rows(named=True),
        context=context,
        rule_name="region",
    )
    if result.is_err():
        return Err(str(result.err()))
    logger.info("Attached {} region components", result.ok() or 0)
    return Ok(None)


def build_generators(context: PluginContext) -> Result[None, str]:
    """Build generators using filtered rules from ``parser_rules.json``."""

    if context.system is None:
        return Err("Parser context has no system")
    generator_data = context.metadata.get(GENERATOR_DATA)
    if not isinstance(generator_data, tuple) or len(generator_data) != 2:
        return Err("Generator datasets were not prepared")

    variable_frame, non_variable_frame = generator_data
    if not isinstance(variable_frame, pl.DataFrame) or not isinstance(non_variable_frame, pl.DataFrame):
        return Err("Prepared generator datasets are invalid")

    total = 0
    for frame in (variable_frame, non_variable_frame):
        result = materialize_parser_rows(
            frame.iter_rows(named=True),
            context=context,
            skip_duplicate_names=True,
        )
        if result.is_err():
            return Err(str(result.err()))
        total += result.ok() or 0
    logger.info("Attached {} generator components", total)
    return Ok(None)


def build_transmission(context: PluginContext) -> Result[None, str]:
    """Build interfaces and lines from directional transmission capacity."""

    if context.system is None:
        return Err("Parser context has no system")
    capacity_data = read_data_file(context, "transmission_capacity")
    if capacity_data is None:
        logger.warning("No transmission capacity data found, skipping transmission")
        return Ok(None)
    capacity = capacity_data.collect()
    if not isinstance(capacity, pl.DataFrame):
        return Err("transmission_capacity did not return a Polars DataFrame")
    required_result = require_columns(
        capacity,
        "transmission_capacity",
        {"from_region", "to_region", "trtype", "capacity"},
    )
    if required_result.is_err():
        return required_result
    if capacity.is_empty():
        logger.warning("Transmission capacity data is empty, skipping transmission")
        return Ok(None)

    losses_data = read_data_file(context, "transmission_losses")
    if losses_data is not None:
        losses = losses_data.collect()
        if not isinstance(losses, pl.DataFrame):
            return Err("transmission_losses did not return a Polars DataFrame")
        if not losses.is_empty():
            rename_map = {
                source: target
                for source, target in {"r": "from_region", "value": "losses"}.items()
                if source in losses.columns and target not in losses.columns
            }
            losses = losses.rename(rename_map)
            loss_result = require_columns(
                losses,
                "transmission_losses",
                {"from_region", "to_region", "trtype", "losses"},
            )
            if loss_result.is_err():
                return loss_result
            capacity = capacity.join(
                losses.select(["from_region", "to_region", "trtype", "losses"]),
                on=["from_region", "to_region", "trtype"],
                how="left",
            )

    reverse = (
        capacity.rename({"from_region": "_to", "to_region": "_from", "capacity": "reverse_capacity"})
        .rename({"_to": "to_region", "_from": "from_region"})
        .select(["from_region", "to_region", "trtype", "reverse_capacity"])
    )
    capacity = capacity.join(reverse, on=["from_region", "to_region", "trtype"], how="left").with_columns(
        pl.col("reverse_capacity").fill_null(pl.col("capacity"))
    )

    interfaces = (
        capacity.select("from_region", "to_region", "trtype")
        .with_columns(
            pl.min_horizontal("from_region", "to_region").alias("region_a"),
            pl.max_horizontal("from_region", "to_region").alias("region_b"),
        )
        .unique(subset=["region_a", "region_b"])
        .select(
            pl.col("region_a").alias("from_region"),
            pl.col("region_b").alias("to_region"),
            "trtype",
        )
    )
    interface_result = materialize_parser_rows(
        interfaces.iter_rows(named=True),
        context=context,
        rule_name="transmission_interface",
    )
    if interface_result.is_err():
        return Err(str(interface_result.err()))

    line_rows: list[Mapping[str, Any]] = []
    line_names: set[str] = set()
    for row in capacity.iter_rows(named=True):
        name_result = build_transmission_line_name(row, context=context)
        if name_result.is_err():
            return Err(str(name_result.err()))
        name = name_result.ok()
        if not name or name in line_names:
            continue
        line_names.add(name)
        line_rows.append(row)
    line_result = materialize_parser_rows(
        line_rows,
        context=context,
        rule_name="transmission_line",
    )
    if line_result.is_err():
        return Err(str(line_result.err()))

    logger.info(
        "Attached {} transmission interfaces and {} lines",
        interface_result.ok() or 0,
        line_result.ok() or 0,
    )
    return Ok(None)


def build_loads(context: PluginContext) -> Result[None, str]:
    """Build demand components from the peak of each profile column."""

    if context.system is None:
        return Err("Parser context has no system")
    profiles_data = read_data_file(context, "load_profiles")
    if profiles_data is None:
        return Err("load_profiles is required to build loads")
    profiles = profiles_data.collect()
    if not isinstance(profiles, pl.DataFrame):
        return Err("load_profiles did not return a Polars DataFrame")
    region_columns = [column for column in profiles.columns if column not in {"datetime", "solve_year"}]
    if not region_columns:
        return Err("load_profiles has no region columns")
    rows = [
        {"region": region, "max_active_power": float(profiles[region].max())}
        for region in region_columns
        if profiles[region].max() is not None
    ]
    result = materialize_parser_rows(rows, context=context, rule_name="load")
    if result.is_err():
        return Err(str(result.err()))
    logger.info("Attached {} load components", result.ok() or 0)
    return Ok(None)


def build_reserves(context: PluginContext) -> Result[None, str]:
    """Build reserve-region and reserve components from configured rows."""

    if context.system is None:
        return Err("Parser context has no system")
    hierarchy_data = read_data_file(context, "hierarchy")
    if hierarchy_data is None:
        logger.warning("No hierarchy data found, skipping reserves")
        return Ok(None)
    hierarchy = hierarchy_data.collect()
    if not isinstance(hierarchy, pl.DataFrame) or hierarchy.is_empty():
        logger.warning("Hierarchy data is empty, skipping reserves")
        return Ok(None)

    defaults = parser_defaults(context)
    reserve_types = defaults.get("default_reserve_types", [])
    if not reserve_types:
        return Ok(None)
    if "transmission_region" not in hierarchy.columns:
        return Err("hierarchy is missing required column 'transmission_region'")
    transmission_regions = hierarchy["transmission_region"].unique().to_list()

    region_rows = ({"name": region} for region in transmission_regions)
    region_result = materialize_parser_rows(
        region_rows,
        context=context,
        rule_name="reserve_region",
    )
    if region_result.is_err():
        return Err(str(region_result.err()))

    percentages = context.metadata.get(RESERVE_PERCENTAGES, {})
    rows = build_reserve_rows(
        transmission_regions,
        reserve_types,
        reserve_duration=defaults.get("reserve_duration", {}),
        reserve_time_frame=defaults.get("reserve_time_frame", {}),
        reserve_vors=defaults.get("reserve_vors", {}),
        reserve_direction=defaults.get("reserve_direction", {}),
        reserve_percentages=percentages,
    )
    reserve_result = materialize_parser_rows(
        rows,
        context=context,
        rule_name="reserve",
    )
    if reserve_result.is_err():
        return Err(str(reserve_result.err()))
    logger.info(
        "Attached {} reserve regions and {} reserve components",
        region_result.ok() or 0,
        reserve_result.ok() or 0,
    )
    return Ok(None)


def build_emissions(context: PluginContext) -> Result[None, str]:
    """Match emission rows to generators and materialize supplemental attributes."""

    if context.system is None:
        return Err("Parser context has no system")
    system = context.system
    emission_data = read_data_file(context, "emission_rates")
    if emission_data is None:
        logger.warning("No emission rates data found, skipping emissions")
        return Ok(None)
    emission_frame = emission_data.collect()
    if not isinstance(emission_frame, pl.DataFrame) or emission_frame.is_empty():
        return Ok(None)

    generators = list(system.get_components(ReEDSGenerator))
    if not generators:
        return Ok(None)
    lookup = build_generator_emission_lookup(generators)
    emission_frame = emission_frame.rename(
        {
            source: target
            for source, target in {"i": "technology", "v": "vintage", "r": "region"}.items()
            if source in emission_frame.columns
        }
    )
    matches = match_emission_rows_to_generators(emission_frame, generator_lookup=lookup)
    if matches.is_empty():
        logger.warning("No emission rows matched existing generators, skipping emissions")
        return Ok(None)

    generators_by_name = {generator.name: generator for generator in generators}

    def source_for_row(row: Mapping[str, Any]) -> ReEDSGenerator | None:
        name = row.get("name")
        if name is None:
            return None
        return generators_by_name.get(str(name))

    result = materialize_parser_rows(
        matches.iter_rows(named=True),
        context=context,
        rule_name="emission",
        attachment_source=source_for_row,
    )
    if result.is_err():
        return Err(str(result.err()))
    logger.info("Attached {} emission components to generators", result.ok() or 0)
    return Ok(None)
