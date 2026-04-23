from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest
from infrasys import System

from r2x_reeds.models.components import ReEDSDataCenterDemand, ReEDSElectrolyzerDemand, ReEDSRegion
from r2x_reeds.sysmod import purchaser_load

pytestmark = [pytest.mark.integration]


def _build_system() -> tuple[System, ReEDSRegion, ReEDSRegion]:
    system = System(name="test_purchaser_load")
    p4 = ReEDSRegion(name="p4")
    p5 = ReEDSRegion(name="p5")
    system.add_component(p4)
    system.add_component(p5)
    return system, p4, p5


def _write_csv(path: Path, data: dict[str, list]) -> str:
    pl.DataFrame(data).write_csv(path)
    return str(path)


def _run_modifier(system: System, **kwargs) -> System:
    result = purchaser_load.add_purchaser_load(system, purchaser_load.PurchaserLoadConfig(**kwargs))
    assert result.is_ok()
    return result.unwrap()


def test_purchaser_load_scope_full_flow(tmp_path: Path) -> None:
    system, _, _ = _build_system()

    hour_map_myr_path = _write_csv(
        tmp_path / "hmap_myr.csv",
        {
            "yearhour": [1, 2, 3],
            "h": ["h1", "h2", "h3"],
        },
    )
    electrolyzer_capacity_path = _write_csv(
        tmp_path / "cap.csv",
        {
            "i": ["electrolyzer"],
            "r": ["p4"],
            "t": [2032],
            "Value": [120.0],
        },
    )
    consume_char_path = _write_csv(
        tmp_path / "consume_char.csv",
        {
            "*i": ["electrolyzer"],
            "t": [2032],
            "parameter": ["electricity_efficiency"],
            "value": [1.25],
        },
    )
    electrolyzer_profile_path = _write_csv(
        tmp_path / "prod_load.csv",
        {
            "i": ["electrolyzer", "electrolyzer", "electrolyzer"],
            "r": ["p4", "p4", "p4"],
            "allh": ["h1", "h2", "h3"],
            "t": [2032, 2032, 2032],
            "Value": [50.0, 40.0, 30.0],
        },
    )
    electrolyzer_annual_path = _write_csv(
        tmp_path / "prod_load_ann.csv",
        {
            "i": ["electrolyzer"],
            "r": ["p4"],
            "t": [2032],
            "Value": [360.0],
        },
    )
    loadsite_op_path = _write_csv(
        tmp_path / "loadsite_op.csv",
        {
            "r": ["p4", "p4", "p4", "p5", "p5", "p5", "p4"],
            "allh": ["h1", "h2", "h3", "h1", "h2", "h3", "h1"],
            "t": [2032, 2032, 2032, 2032, 2032, 2032, 2040],
            "Value": [500.0, 500.0, 500.0, 300.0, 300.0, 300.0, 999.0],
        },
    )

    _run_modifier(
        system,
        solve_year=2032,
        weather_year=2012,
        hour_map_myr_fpath=hour_map_myr_path,
        electrolyzer_capacity_fpath=electrolyzer_capacity_path,
        consume_characteristics_fpath=consume_char_path,
        electrolyzer_prod_load_fpath=electrolyzer_profile_path,
        electrolyzer_prod_load_ann_fpath=electrolyzer_annual_path,
        loadsite_op_fpath=loadsite_op_path,
    )

    electrolyzer_demand = system.get_component(ReEDSElectrolyzerDemand, "p4_electrolyzer_demand")
    assert electrolyzer_demand.capacity == pytest.approx(120.0)
    assert electrolyzer_demand.electricity_efficiency == pytest.approx(1.25)

    data_center_p4 = system.get_component(ReEDSDataCenterDemand, "p4_data_center_demand")
    data_center_p5 = system.get_component(ReEDSDataCenterDemand, "p5_data_center_demand")
    assert data_center_p4.capacity == pytest.approx(500.0)
    assert data_center_p5.capacity == pytest.approx(300.0)

    electrolyzer_ts = system.get_time_series(electrolyzer_demand).data
    # Profile [50, 40, 30] scaled so annual total equals 360.
    np.testing.assert_allclose(electrolyzer_ts, np.array([150.0, 120.0, 90.0]), rtol=1e-5)

    p4_ts = system.get_time_series(data_center_p4).data
    p5_ts = system.get_time_series(data_center_p5).data
    np.testing.assert_allclose(p4_ts, np.array([500.0, 500.0, 500.0]), rtol=1e-5)
    np.testing.assert_allclose(p5_ts, np.array([300.0, 300.0, 300.0]), rtol=1e-5)


def test_purchaser_load_scope_missing_hour_map(tmp_path: Path, caplog) -> None:
    system, _, _ = _build_system()
    loadsite_op_path = _write_csv(
        tmp_path / "loadsite_op.csv",
        {
            "r": ["p4"],
            "allh": ["h1"],
            "t": [2032],
            "Value": [500.0],
        },
    )

    _run_modifier(system, solve_year=2032, loadsite_op_fpath=loadsite_op_path, hour_map_myr_fpath=None)

    assert "Missing hour_map_myr input" in caplog.text
    assert not list(system.get_components(ReEDSDataCenterDemand))
