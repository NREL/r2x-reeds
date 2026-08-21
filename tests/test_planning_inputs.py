"""Tests for canonical ReEDS planning inputs."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import cast

import pytest

from r2x_core import DataStore, PluginContext, System
from r2x_reeds import ReEDSConfig, ReEDSParser

pytestmark = [pytest.mark.integration]


_PLANT_CHARACTERISTICS = {
    ("battery_li", 2030): {
        "capcost": 202_141.0,
        "capcost_energy": 152_868.0,
        "fom": 5_056.0,
        "fom_energy": 3_823.0,
        "vom": 0.0,
        "heatrate": 0.0,
        "rte": 0.85,
    },
    ("battery_li", 2035): {
        "capcost": 180_000.0,
        "capcost_energy": 125_000.0,
        "fom": 4_500.0,
        "fom_energy": 3_000.0,
        "vom": 0.0,
        "heatrate": 0.0,
        "rte": 0.85,
    },
    ("Gas-CC", 2030): {
        "capcost": 734_670.0,
        "capcost_energy": 0.0,
        "fom": 20_081.0,
        "fom_energy": 0.0,
        "vom": 1.2978,
        "heatrate": 6.206,
        "rte": 0.0,
    },
    ("Gas-CC", 2035): {
        "capcost": 700_000.0,
        "capcost_energy": 0.0,
        "fom": 19_000.0,
        "fom_energy": 0.0,
        "vom": 1.2,
        "heatrate": 6.0,
        "rte": 0.0,
        "upgradecost": 50_000.0,
    },
}


def _write_planning_inputs(case_path: Path) -> None:
    """Write the minimal canonical ReEDS inputs needed for planning data."""
    inputs_path = case_path / "inputs_case"
    representative_path = inputs_path / "rep"
    representative_path.mkdir(parents=True, exist_ok=True)

    (inputs_path / "modeledyears.csv").write_text("2030,2035\n")
    (inputs_path / "pvf_cap.csv").write_text("*t,pvf_capital\n2030,1.0\n2035,0.7\n")
    (inputs_path / "co2_cap.csv").write_text("*t,tonne_per_year\n2030,1000000\n2035,900000\n")
    (inputs_path / "switches.csv").write_text("GSw_AnnualCap,1\nGSw_Storage,1\nGSw_HydroPSHDurData,1\n")
    (representative_path / "numhours.csv").write_text("*h,numhours\nh1,4380\nh2,4380\n")
    (inputs_path / "storage_duration.csv").write_text("caes,12\n")
    (inputs_path / "psh_sc_duration.csv").write_text("8\n")
    (inputs_path / "storage_duration_pshdata.csv").write_text(
        "*i,v,r,hours\npumped-hydro,init-1,p1,10\n"
    )
    (inputs_path / "capnonrsc.csv").write_text("i,r,value\nbattery_li,p1,2\nGas-CC,p1,100\n")
    (inputs_path / "capnonrsc_energy.csv").write_text("i,r,value\nbattery_li,p1,4\n")

    plant_rows = ["*i,t,variable,value"]
    for (technology, year), values in _PLANT_CHARACTERISTICS.items():
        plant_rows.extend(f"{technology},{year},{field},{value}" for field, value in values.items())
    (inputs_path / "plantcharout.csv").write_text("\n".join(plant_rows) + "\n")


def _prepare_parser(tmp_path: Path, reeds_run_path: Path) -> tuple[ReEDSParser, System]:
    """Create a parser with loaded rules, regions, and a target system."""
    case_path = tmp_path / "case"
    shutil.copytree(reeds_run_path, case_path)
    _write_planning_inputs(case_path)

    config = ReEDSConfig(solve_year=2030, weather_year=2012, case_name="planning-inputs")
    store = DataStore.from_plugin_config(config, path=case_path)
    context = PluginContext(config=config, store=store)
    parser = cast(ReEDSParser, ReEDSParser.from_context(context))
    assert parser.on_validate_config().is_ok()

    system = System(name="planning-inputs")
    parser.ctx.system = system
    parser.ctx.target_system = system

    from r2x_reeds.parser_builders import build_regions

    regions_result = build_regions(parser.ctx)
    assert regions_result.is_ok(), regions_result.err()
    return parser, system


def test_parser_materializes_canonical_planning_components(
    tmp_path: Path,
    reeds_run_path: Path,
) -> None:
    """Canonical planning tables become components and reusable period attributes."""
    from r2x_reeds import (
        ReEDSInitialCapacity,
        ReEDSPlanningPeriod,
        ReEDSPlanningSwitches,
        ReEDSPlantCharacteristics,
        ReEDSPumpedStorageSupplyCurveDuration,
        ReEDSRepresentativeTimepoint,
        ReEDSStorageDuration,
        ReEDSStorageDurationOverride,
    )
    from r2x_reeds.parser_planning_inputs import attach_planning_inputs

    parser, system = _prepare_parser(tmp_path, reeds_run_path)
    result = attach_planning_inputs(parser.ctx)
    assert result.is_ok(), result.err()

    switches = system.get_component(ReEDSPlanningSwitches, "planning_switches")
    periods = sorted(
        system.get_supplemental_attributes_with_component(switches, ReEDSPlanningPeriod),
        key=lambda period: period.year,
    )
    assert [(period.year, period.present_value_factor, period.emission_cap) for period in periods] == [
        (2030, 1.0, 1_000_000.0),
        (2035, 0.7, 900_000.0),
    ]

    timepoints = list(system.get_components(ReEDSRepresentativeTimepoint))
    assert [(point.name, point.position, point.weight) for point in timepoints] == [
        ("h1", 0, 4_380.0),
        ("h2", 1, 4_380.0),
    ]

    characteristics = list(system.get_components(ReEDSPlantCharacteristics))
    battery = system.get_component(ReEDSPlantCharacteristics, "battery_li_2030")
    gas = system.get_component(ReEDSPlantCharacteristics, "gas-cc_2030")
    assert battery.capital_cost == 202_141.0
    assert battery.capital_cost_energy == 152_868.0
    assert battery.fom_cost_energy == 3_823.0
    assert battery.round_trip_efficiency == 0.85
    assert battery.heat_rate is None
    assert gas.heat_rate == 6.206
    assert gas.round_trip_efficiency is None
    assert system.get_component(ReEDSPlantCharacteristics, "gas-cc_2035").upgrade_cost == 50_000.0
    assert all(system.get_supplemental_attributes_with_component(item, ReEDSPlanningPeriod) for item in characteristics)

    initial_capacity = system.get_component(ReEDSInitialCapacity, "battery_li_p1")
    assert initial_capacity.initial_power_capacity == 2.0
    assert initial_capacity.initial_energy_capacity == 4.0
    assert system.get_component(ReEDSInitialCapacity, "gas-cc_p1").initial_energy_capacity is None
    assert system.get_component(ReEDSStorageDuration, "caes").duration == 12.0
    assert (
        system.get_component(
            ReEDSPumpedStorageSupplyCurveDuration,
            "pumped_storage_supply_curve_duration",
        ).duration
        == 8.0
    )
    assert system.get_component(ReEDSStorageDurationOverride, "pumped-hydro_init-1_p1").duration == 10.0


def test_inactive_annual_cap_leaves_period_emission_caps_unset(
    tmp_path: Path,
    reeds_run_path: Path,
) -> None:
    """An inactive annual-cap switch produces uncapped planning periods."""
    from r2x_reeds import ReEDSPlanningPeriod, ReEDSPlanningSwitches
    from r2x_reeds.parser_planning_inputs import attach_planning_inputs

    parser, system = _prepare_parser(tmp_path, reeds_run_path)
    (tmp_path / "case" / "inputs_case" / "switches.csv").write_text(
        "GSw_AnnualCap,0\nGSw_Storage,1\nGSw_HydroPSHDurData,1\n"
    )
    result = attach_planning_inputs(parser.ctx)
    assert result.is_ok(), result.err()

    switches = system.get_component(ReEDSPlanningSwitches, "planning_switches")
    periods = sorted(
        system.get_supplemental_attributes_with_component(switches, ReEDSPlanningPeriod),
        key=lambda period: period.year,
    )
    assert [period.emission_cap for period in periods] == [None, None]
