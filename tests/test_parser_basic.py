"""Basic ReEDS parser tests using r2x-core 0.1.1 API.

These tests verify basic parser instantiation and configuration using
a minimal test data set.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import h5py
import numpy as np
import polars as pl
import pytest

pytestmark = [pytest.mark.integration]

if TYPE_CHECKING:
    pass


def _build_parser(run_path: Path):
    from typing import cast

    from r2x_core import DataStore, PluginContext
    from r2x_reeds import ReEDSConfig, ReEDSParser

    config = ReEDSConfig(
        solve_year=2032,
        weather_year=2012,
        case_name="test",
        scenario="base",
    )
    store = DataStore.from_plugin_config(config, path=run_path)
    ctx = PluginContext(config=config, store=store)
    return cast(ReEDSParser, ReEDSParser.from_context(ctx))


def _write_minimal_outputs_h5_from_fuel_price(csv_path: Path, h5_path: Path) -> None:
    """Create a minimal outputs.h5 containing a fuel_price group."""
    fuel_price = pl.read_csv(csv_path)

    with h5py.File(h5_path, "w") as h5_file:
        group = h5_file.create_group("fuel_price")
        columns = fuel_price.columns
        group.create_dataset("columns", data=np.array(columns, dtype="S"))

        for column in columns:
            values = fuel_price[column].to_list()
            if values and isinstance(values[0], str):
                dataset_values = np.array(values, dtype="S")
            else:
                dataset_values = np.array(values)
            group.create_dataset(column, data=dataset_values)


def test_read_data_file_uses_configured_outputs_h5_group(reeds_run_path: Path) -> None:
    """The configured DataFile mapping reads the shared outputs HDF5 group."""
    parser = _build_parser(reeds_run_path)

    result = parser.read_data_file("fuel_price")

    assert result is not None
    df = result.collect()
    assert not df.is_empty()
    assert {"technology", "region", "year", "fuel_price"}.issubset(set(df.columns))


def test_read_data_file_uses_outputs_h5_and_not_csv(tmp_path: Path, reeds_run_path: Path) -> None:
    """Parser should read output datasets from outputs.h5 when available."""
    run_path = tmp_path / "test_Pacific"
    shutil.copytree(reeds_run_path, run_path)

    outputs_dir = run_path / "outputs"
    fuel_price_csv = outputs_dir / "fuel_price.csv"
    outputs_h5 = outputs_dir / "outputs.h5"

    _write_minimal_outputs_h5_from_fuel_price(fuel_price_csv, outputs_h5)
    fuel_price_csv.unlink()

    parser = _build_parser(run_path)
    result = parser.read_data_file("fuel_price")

    assert result is not None
    df = result.collect()
    assert not df.is_empty()
    assert {"technology", "region", "year", "fuel_price"}.issubset(set(df.columns))


def test_read_data_file_uses_store_for_non_outputs_dataset(reeds_run_path: Path) -> None:
    """Non-output mappings should still be read through DataStore directly."""
    parser = _build_parser(reeds_run_path)

    result = parser.read_data_file("hierarchy")

    assert result is not None
    df = result.collect()
    assert not df.is_empty()
    assert "region_id" in df.columns
