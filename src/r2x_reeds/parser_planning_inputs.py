"""Read ReEDS planning inputs into infrasys components and attributes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import polars as pl
from infrasys import Component
from infrasys.exceptions import ISAlreadyAttached, ISNotStored
from loguru import logger
from rust_ok import Err, Ok, Result

from r2x_core import PluginContext

from .checks import (
    check_initial_capacity_rows,
    check_planning_inputs_available,
    check_planning_periods,
    check_planning_switches,
    check_planning_years,
    check_plant_characteristics_source,
    check_plant_characteristics_values,
    check_representative_timepoints,
    check_single_duration,
    check_storage_duration_override_rows,
    check_storage_duration_rows,
)
from .models import (
    ReEDSBinarySwitch,
    ReEDSPlanningPeriod,
    ReEDSPlanningSwitches,
    ReEDSPlantCharacteristics,
)
from .parser_planning_frames import (
    build_planning_initial_capacity_frame,
    build_planning_periods_frame,
    build_planning_plant_characteristics_frame,
    build_planning_representative_timepoints_frame,
)
from .parser_rules import materialize_parser_batches

REQUIRED_INPUTS = (
    "modeled_years",
    "planning_present_value_factors",
    "planning_switches",
    "planning_representative_timepoints",
    "planning_plant_characteristics",
    "existing_capacity",
)
PLANNING_INPUTS = (
    "planning_present_value_factors",
    "planning_switches",
    "planning_representative_timepoints",
    "planning_plant_characteristics",
)


def read_dataset(context: PluginContext, name: str) -> Result[pl.DataFrame | None, str]:
    """Read and collect one mapped planning dataset."""
    if context.store is None:
        return Err("Parser context has no DataStore")
    try:
        data = context.store.read_data(
            name,
            placeholders={
                "solve_year": context.config.solve_year,
                "weather_year": context.config.weather_year,
            },
        )
    except (FileNotFoundError, KeyError, OSError, TypeError, ValueError) as exc:
        return Err(f"Failed to read dataset '{name}': {exc}")
    if data is None:
        return Ok(None)
    try:
        return Ok(data.collect())
    except (OSError, TypeError, ValueError, pl.exceptions.PolarsError) as exc:
        return Err(f"Failed to collect dataset '{name}': {exc}")


def collect_required_datasets(context: PluginContext) -> Result[dict[str, pl.DataFrame], str]:
    """Read all required planning datasets."""
    datasets: dict[str, pl.DataFrame] = {}
    for name in REQUIRED_INPUTS:
        result = read_dataset(context, name)
        if result.is_err():
            return Err(str(result.err()))
        frame = result.ok()
        if frame is None:
            return Err(f"required planning input '{name}' is missing")
        datasets[name] = frame
    return Ok(datasets)


def build_planning_rows(
    context: PluginContext,
    datasets: Mapping[str, pl.DataFrame],
) -> Result[dict[str, list[dict[str, Any]]], str]:
    """Normalize planning tables into row batches consumed by parser rules."""
    years_result = check_planning_years(datasets["modeled_years"])
    if years_result.is_err():
        return Err(str(years_result.err()))
    years = years_result.ok()
    if years is None:
        return Err("Planning years were not parsed")

    switch_result = check_planning_switches(
        datasets["planning_switches"],
        source_names={"GSw_AnnualCap", "GSw_Storage", "GSw_HydroPSHDurData"},
    )
    if switch_result.is_err():
        return Err(str(switch_result.err()))
    raw_switches = switch_result.ok()
    if raw_switches is None:
        return Err("Planning switches were not parsed")

    # The switch table is key/value shaped. The Rule owns the target model;
    # this normalization only translates source keys to rule input keys.
    switch_row = {
        "name": "planning_switches",
        "annual_cap": raw_switches.get("GSw_AnnualCap", 0),
        "storage": raw_switches.get("GSw_Storage", 0),
        "hydro_psh_duration_data": raw_switches.get("GSw_HydroPSHDurData", 0),
    }

    co2_cap: pl.DataFrame | None = None
    try:
        switch_model = ReEDSPlanningSwitches.model_validate(switch_row)
    except (TypeError, ValueError) as exc:
        return Err(f"planning_switches is invalid: {exc}")
    emission_type = switch_model.annual_cap.emission_type
    if emission_type is not None:
        co2_result = read_dataset(context, "co2_cap")
        if co2_result.is_err():
            return Err(str(co2_result.err()))
        co2_cap = co2_result.ok()
    try:
        periods = build_planning_periods_frame(
            datasets["modeled_years"],
            datasets["planning_present_value_factors"],
            co2_cap,
        )
    except (KeyError, ValueError, pl.exceptions.PolarsError) as exc:
        return Err(f"Failed to prepare planning periods: {exc}")
    period_check = check_planning_periods(periods, emission_type=emission_type)
    if period_check.is_err():
        return Err(str(period_check.err()))

    try:
        representative = build_planning_representative_timepoints_frame(
            datasets["planning_representative_timepoints"]
        )
    except (KeyError, ValueError, pl.exceptions.PolarsError) as exc:
        return Err(f"planning_representative_timepoints is invalid: {exc}")
    representative_check = check_representative_timepoints(representative)
    if representative_check.is_err():
        return Err(str(representative_check.err()))

    source_variables = {
        field.validation_alias
        for field_name, field in ReEDSPlantCharacteristics.model_fields.items()
        if field_name not in {"name", "technology"} and isinstance(field.validation_alias, str)
    }
    required_variables = {
        field.validation_alias
        for field_name, field in ReEDSPlantCharacteristics.model_fields.items()
        if field_name not in {"name", "technology"}
        and isinstance(field.validation_alias, str)
        and field.is_required()
    }
    source_check = check_plant_characteristics_source(
        datasets["planning_plant_characteristics"],
        source_variables=source_variables,
    )
    if source_check.is_err():
        return Err(str(source_check.err()))
    try:
        plant_characteristics = build_planning_plant_characteristics_frame(
            datasets["planning_plant_characteristics"],
            planning_years=years,
        )
    except (KeyError, ValueError, pl.exceptions.PolarsError) as exc:
        return Err(str(exc))
    value_check = check_plant_characteristics_values(
        plant_characteristics,
        required_variables=required_variables,
    )
    if value_check.is_err():
        return Err(str(value_check.err()))

    energy_result = read_dataset(context, "existing_energy_capacity")
    if energy_result.is_err():
        return Err(str(energy_result.err()))
    try:
        initial_capacity = build_planning_initial_capacity_frame(
            datasets["existing_capacity"],
            energy_result.ok(),
        )
    except (KeyError, ValueError, pl.exceptions.PolarsError) as exc:
        return Err(str(exc))
    initial_check = check_initial_capacity_rows(initial_capacity)
    if initial_check.is_err():
        return Err(str(initial_check.err()))

    batches: dict[str, list[dict[str, Any]]] = {
        "planning_switches": [switch_row],
        "planning_period": list(periods.iter_rows(named=True)),
        "representative_timepoint": list(representative.iter_rows(named=True)),
        "plant_characteristics": list(plant_characteristics.iter_rows(named=True)),
        "initial_capacity": list(initial_capacity.iter_rows(named=True)),
    }

    duration_result = read_dataset(context, "planning_storage_durations")
    if duration_result.is_err():
        return Err(str(duration_result.err()))
    duration_frame = duration_result.ok()
    if duration_frame is not None and not duration_frame.is_empty():
        duration_check = check_storage_duration_rows(duration_frame)
        if duration_check.is_err():
            return Err(str(duration_check.err()))
        batches["storage_duration"] = list(duration_frame.iter_rows(named=True))

    if switch_model.storage is ReEDSBinarySwitch.ON:
        pumped_result = read_dataset(context, "planning_pumped_storage_supply_curve_duration")
        if pumped_result.is_err():
            return Err(str(pumped_result.err()))
        pumped_frame = pumped_result.ok()
        if pumped_frame is not None and not pumped_frame.is_empty():
            pumped_check = check_single_duration(
                pumped_frame,
                "planning_pumped_storage_supply_curve_duration",
            )
            if pumped_check.is_err():
                return Err(str(pumped_check.err()))
            batches["pumped_storage_supply_curve_duration"] = list(
                pumped_frame.iter_rows(named=True)
            )

        if switch_model.hydro_psh_duration_data is ReEDSBinarySwitch.ON:
            override_result = read_dataset(context, "planning_storage_duration_overrides")
            if override_result.is_err():
                return Err(str(override_result.err()))
            override_frame = override_result.ok()
            if override_frame is not None and not override_frame.is_empty():
                override_check = check_storage_duration_override_rows(override_frame)
                if override_check.is_err():
                    return Err(str(override_check.err()))
                batches["storage_duration_override"] = list(override_frame.iter_rows(named=True))
    return Ok(batches)


def attach_planning_period_to_characteristics(context: PluginContext) -> Result[None, str]:
    """Reuse each planning-period attribute on matching characteristics."""
    if context.system is None:
        return Err("Parser context has no system")
    system = context.system
    try:
        switches = system.get_component(ReEDSPlanningSwitches, "planning_switches")
    except ISNotStored as exc:
        return Err(f"Planning switches are not stored: {exc}")

    try:
        periods = system.get_supplemental_attributes_with_component(switches, ReEDSPlanningPeriod)
    except (ISNotStored, TypeError, ValueError) as exc:
        return Err(f"Failed to read planning periods: {exc}")

    periods_by_year = {period.year: period for period in periods}
    for characteristic in system.get_components(ReEDSPlantCharacteristics):
        period = periods_by_year.get(characteristic.year)
        if period is None:
            return Err(f"No planning period exists for plant characteristic {characteristic.name}")
        try:
            system.add_supplemental_attribute(characteristic, period)
        except ISAlreadyAttached:
            continue
    return Ok(None)


def attach_planning_inputs(context: PluginContext) -> Result[None, str]:
    """Materialize planning records through parser rules in dependency order."""
    if context.system is None:
        return Err("Parser context has no system")
    if context.store is None:
        return Err("Parser context has no DataStore")

    availability_result = check_planning_inputs_available(context.store, PLANNING_INPUTS)
    if availability_result.is_err():
        return Err(str(availability_result.err()))
    if not availability_result.ok():
        logger.debug("Planning source inputs are unavailable; skipping planning records")
        return Ok(None)

    datasets_result = collect_required_datasets(context)
    if datasets_result.is_err():
        return Err(str(datasets_result.err()))
    datasets = datasets_result.ok()
    if datasets is None:
        return Err("Planning datasets were not collected")
    batches_result = build_planning_rows(context, datasets)
    if batches_result.is_err():
        return Err(str(batches_result.err()))
    batches = batches_result.ok()
    if batches is None:
        return Err("Planning row batches were not created")

    system = context.system

    def planning_period_source(_: Mapping[str, Any]) -> Component | None:
        """Return the run-level switch component for period attributes."""
        try:
            return system.get_component(ReEDSPlanningSwitches, "planning_switches")
        except ISNotStored:
            return None

    materialized_result = materialize_parser_batches(
        batches,
        context=context,
        attachment_sources={"planning_period": planning_period_source},
    )
    if materialized_result.is_err():
        return Err(str(materialized_result.err()))
    return attach_planning_period_to_characteristics(context)
