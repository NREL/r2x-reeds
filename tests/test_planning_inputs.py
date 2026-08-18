"""Tests for reading canonical ReEDS capacity-expansion inputs."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import cast

import pytest

from r2x_core import DataStore, PluginContext
from r2x_reeds import EmissionType, ReEDSConfig, ReEDSParser

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


def _write_capacity_expansion_case(case_path: Path) -> None:
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
    (inputs_path / "storage_duration_pshdata.csv").write_text("*i,v,r,hours\npumped-hydro,init-1,r1,10\n")
    (inputs_path / "capnonrsc.csv").write_text("i,r,value\nbattery_li,r1,2\nGas-CC,r1,100\n")
    (inputs_path / "capnonrsc_energy.csv").write_text("i,r,value\nbattery_li,r1,4\n")

    plant_rows = ["*i,t,variable,value"]
    for (technology, year), values in _PLANT_CHARACTERISTICS.items():
        plant_rows.extend(f"{technology},{year},{field},{value}" for field, value in values.items())
    (inputs_path / "plantcharout.csv").write_text("\n".join(plant_rows) + "\n")


def _prepare_capacity_expansion_case(tmp_path: Path, reeds_run_path: Path) -> Path:
    """Copy a complete fixture case and replace its planning inputs."""
    case_path = tmp_path / "case"
    shutil.copytree(reeds_run_path, case_path)
    _write_capacity_expansion_case(case_path)
    return case_path


def _build_parser(case_path: Path) -> ReEDSParser:
    """Build a parser backed by one small ReEDS input case."""
    config = ReEDSConfig(solve_year=2030, weather_year=2012, case_name="planning-inputs")
    store = DataStore.from_plugin_config(config, path=case_path)
    context = PluginContext(config=config, store=store)
    return cast(ReEDSParser, ReEDSParser.from_context(context))


def test_read_capacity_expansion_inputs_reads_canonical_reeds_tables(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """The parser reads technology-year, chronology, and initial-capacity inputs."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)

    inputs = _build_parser(case_path).read_capacity_expansion_inputs()

    assert [period.year for period in inputs.planning_periods] == [2030, 2035]
    assert [period.present_value_factor for period in inputs.planning_periods] == [1.0, 0.7]
    assert [period.emission_cap for period in inputs.planning_periods] == [1_000_000.0, 900_000.0]
    assert inputs.emission_type is EmissionType.CO2
    assert [(point.label, point.position, point.weight) for point in inputs.representative_timepoints] == [
        ("h1", 0, 4_380.0),
        ("h2", 1, 4_380.0),
    ]

    characteristics = {(item.technology, item.year): item for item in inputs.plant_characteristics}
    battery = characteristics[("battery_li", 2030)]
    gas = characteristics[("gas-cc", 2030)]
    assert battery.capital_cost == 202_141.0
    assert battery.capital_cost_energy == 152_868.0
    assert battery.fom_cost_energy == 3_823.0
    assert battery.round_trip_efficiency == 0.85
    assert battery.heat_rate is None
    assert gas.heat_rate == 6.206
    assert gas.round_trip_efficiency is None
    assert characteristics[("gas-cc", 2035)].upgrade_cost == 50_000.0

    initial_capacity = {(item.technology, item.region): item for item in inputs.initial_capacities}
    assert initial_capacity[("battery_li", "r1")].initial_power_capacity == 2.0
    assert initial_capacity[("battery_li", "r1")].initial_energy_capacity == 4.0
    assert initial_capacity[("gas-cc", "r1")].initial_energy_capacity is None
    assert [(item.technology, item.duration) for item in inputs.storage_durations] == [("caes", 12.0)]
    assert inputs.pumped_storage_supply_curve_duration == 8.0
    assert [
        (item.technology, item.vintage, item.region, item.duration)
        for item in inputs.storage_duration_overrides
    ] == [("pumped-hydro", "init-1", "r1", 10.0)]


def test_read_capacity_expansion_inputs_ignores_inactive_co2_cap(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """GSw_AnnualCap=0 makes cap data irrelevant to planning inputs."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "switches.csv").write_text(
        "GSw_AnnualCap,0\nGSw_Storage,1\nGSw_HydroPSHDurData,1\n"
    )
    (case_path / "inputs_case" / "co2_cap.csv").write_text("*t,tonne_per_year\n2030,not-a-number\n")

    inputs = _build_parser(case_path).read_capacity_expansion_inputs()

    assert inputs.emission_type is None
    assert [period.emission_cap for period in inputs.planning_periods] == [None, None]


def test_read_capacity_expansion_inputs_ignores_disabled_storage_duration_overrides(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """GSw_HydroPSHDurData controls whether PSH duration input rows are applied."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "switches.csv").write_text(
        "GSw_AnnualCap,1\nGSw_Storage,1\nGSw_HydroPSHDurData,0\n"
    )
    (case_path / "inputs_case" / "storage_duration_pshdata.csv").write_text("not-a-duration\n")

    inputs = _build_parser(case_path).read_capacity_expansion_inputs()

    assert inputs.storage_duration_overrides == ()


def test_read_capacity_expansion_inputs_ignores_psh_duration_data_when_storage_is_disabled(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """GSw_Storage disables both selected and regional PSH duration data."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "switches.csv").write_text(
        "GSw_AnnualCap,1\nGSw_Storage,0\nGSw_HydroPSHDurData,1\n"
    )
    (case_path / "inputs_case" / "psh_sc_duration.csv").write_text("not-a-duration\n")
    (case_path / "inputs_case" / "storage_duration_pshdata.csv").write_text("not-a-duration\n")

    inputs = _build_parser(case_path).read_capacity_expansion_inputs()

    assert inputs.pumped_storage_supply_curve_duration is None
    assert inputs.storage_duration_overrides == ()


def test_read_capacity_expansion_inputs_rejects_invalid_storage_switch(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """GSw_Storage accepts only the source model's enabled/disabled values."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "switches.csv").write_text(
        "GSw_AnnualCap,1\nGSw_Storage,2\nGSw_HydroPSHDurData,1\n"
    )

    with pytest.raises(ValueError, match="GSw_Storage must be 0 or 1"):
        _build_parser(case_path).read_capacity_expansion_inputs()


def test_read_capacity_expansion_inputs_rejects_invalid_psh_duration_switch(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """GSw_HydroPSHDurData accepts only the source model's enabled/disabled values."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "switches.csv").write_text(
        "GSw_AnnualCap,1\nGSw_Storage,1\nGSw_HydroPSHDurData,2\n"
    )

    with pytest.raises(ValueError, match="GSw_HydroPSHDurData must be 0 or 1"):
        _build_parser(case_path).read_capacity_expansion_inputs()


def test_read_capacity_expansion_inputs_rejects_invalid_psh_duration_switch_when_storage_is_disabled(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """GSw_HydroPSHDurData remains a binary source switch when storage is disabled."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "switches.csv").write_text(
        "GSw_AnnualCap,1\nGSw_Storage,0\nGSw_HydroPSHDurData,2\n"
    )

    with pytest.raises(ValueError, match="GSw_HydroPSHDurData must be 0 or 1"):
        _build_parser(case_path).read_capacity_expansion_inputs()


def test_read_capacity_expansion_inputs_rejects_multiple_psh_supply_curve_durations(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """The selected PSH supply curve contributes one scalar duration."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "psh_sc_duration.csv").write_text("8\n10\n")

    with pytest.raises(ValueError, match="must contain exactly one duration"):
        _build_parser(case_path).read_capacity_expansion_inputs()


@pytest.mark.parametrize("annual_cap_mode", [2, 3])
def test_read_capacity_expansion_inputs_maps_co2e_annual_caps(
    tmp_path: Path, reeds_run_path: Path, annual_cap_mode: int
) -> None:
    """CO2e annual-cap modes, including hydrogen leakage, use the CO2e cap data."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "switches.csv").write_text(
        f"GSw_AnnualCap,{annual_cap_mode}\nGSw_Storage,1\nGSw_HydroPSHDurData,1\n"
    )

    inputs = _build_parser(case_path).read_capacity_expansion_inputs()

    assert inputs.emission_type is EmissionType.CO2E
    assert [period.emission_cap for period in inputs.planning_periods] == [1_000_000.0, 900_000.0]


def test_read_capacity_expansion_inputs_rejects_an_active_cap_missing_a_modeled_year(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """An active annual constraint needs a cap for every modeled year."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "co2_cap.csv").write_text("*t,tonne_per_year\n2030,1000000\n")

    with pytest.raises(ValueError, match="co2_cap is missing modeled years"):
        _build_parser(case_path).read_capacity_expansion_inputs()


def test_read_capacity_expansion_inputs_rejects_an_unknown_plant_characteristic(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """A source-schema change cannot silently drop a plant characteristic."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    plant_characteristics_path = case_path / "inputs_case" / "plantcharout.csv"
    plant_characteristics_path.write_text(
        plant_characteristics_path.read_text().replace(
            "battery_li,2035,rte,0.85",
            "battery_li,2035,unknown,0.85",
        )
    )

    with pytest.raises(ValueError, match="unsupported variable 'unknown'"):
        _build_parser(case_path).read_capacity_expansion_inputs()


def test_read_capacity_expansion_inputs_rejects_orphan_energy_capacity(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """Storage energy capacity must have a matching regional power capacity."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "capnonrsc_energy.csv").write_text("i,r,value\nbattery_li,r2,4\n")

    with pytest.raises(ValueError, match="no matching power capacity"):
        _build_parser(case_path).read_capacity_expansion_inputs()


def test_read_capacity_expansion_inputs_requires_present_value_factors(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """Capital-cost present values are required for every planning input case."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "pvf_cap.csv").unlink()

    with pytest.raises(ValueError, match=r"planning_present_value_factors.*missing"):
        _build_parser(case_path).read_capacity_expansion_inputs()


def test_read_capacity_expansion_inputs_rejects_duplicate_storage_durations(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """A fixed-duration technology can have only one source duration."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    (case_path / "inputs_case" / "storage_duration.csv").write_text("caes,12\ncaes,24\n")

    with pytest.raises(ValueError, match="duplicate duration"):
        _build_parser(case_path).read_capacity_expansion_inputs()


def test_read_capacity_expansion_inputs_rejects_incomplete_plant_characteristics(
    tmp_path: Path, reeds_run_path: Path
) -> None:
    """Every technology-year record must retain all required plant characteristics."""
    case_path = _prepare_capacity_expansion_case(tmp_path, reeds_run_path)
    plant_characteristics_path = case_path / "inputs_case" / "plantcharout.csv"
    plant_characteristics_path.write_text(
        "\n".join(
            line
            for line in plant_characteristics_path.read_text().splitlines()
            if line != "battery_li,2035,rte,0.85"
        )
        + "\n"
    )

    with pytest.raises(ValueError, match=r"missing variables .*rte"):
        _build_parser(case_path).read_capacity_expansion_inputs()
