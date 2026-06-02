"""Basic ReEDS parser tests using r2x-core 0.1.1 API.

These tests verify basic parser instantiation and configuration using
a minimal test data set.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pytest

pytestmark = [pytest.mark.integration]

if TYPE_CHECKING:
    pass


def _build_parser(reeds_run_path):
    from typing import cast

    from r2x_core import DataStore, PluginContext
    from r2x_reeds import ReEDSConfig, ReEDSParser

    config = ReEDSConfig(
        solve_year=2032,
        weather_year=2012,
        case_name="test",
        scenario="base",
    )
    store = DataStore.from_plugin_config(config, path=reeds_run_path)
    ctx = PluginContext(config=config, store=store)
    return cast(ReEDSParser, ReEDSParser.from_context(ctx))


def test_build_synthetic_hour_map_contains_requested_weather_years() -> None:
    """Synthetic hour_map includes all requested weather years."""
    from r2x_reeds.parser import _build_synthetic_hour_map

    weather_years = [2007, 2012]
    df = _build_synthetic_hour_map(weather_years)

    assert set(df.columns) == {"year", "time_index", "hour_period", "season"}
    assert sorted(df["year"].to_list()) == sorted(weather_years)
    assert df.height == 2


# ---------------------------------------------------------------------------
# Regression: existing-but-bad hour_map must be a validation error, not silently
# replaced by the synthetic fallback.
# ---------------------------------------------------------------------------


def test_get_hour_map_for_validation_empty_frame_is_error(monkeypatch, reeds_run_path) -> None:
    """An hour_map that exists but has no rows must produce Err, not a synthetic fallback."""
    parser = _build_parser(reeds_run_path)
    monkeypatch.setattr(
        parser.store,
        "read_data",
        lambda name, **kwargs: pl.DataFrame({"year": [], "hour_period": []}) if name == "hour_map" else None,
    )

    result = parser._get_hour_map_for_validation({"solve_year": 2032, "weather_year": 2012})

    assert result.is_err(), "Empty hour_map should be a validation error"
    assert "no rows" in str(result.err())


def test_get_hour_map_for_validation_missing_year_column_is_error(monkeypatch, reeds_run_path) -> None:
    """An hour_map missing the 'year' column must produce Err, not a synthetic fallback."""
    parser = _build_parser(reeds_run_path)
    monkeypatch.setattr(
        parser.store,
        "read_data",
        lambda name, **kwargs: pl.DataFrame({"hour_period": ["h1"], "season": ["summ"]})
        if name == "hour_map"
        else None,
    )

    result = parser._get_hour_map_for_validation({"solve_year": 2032, "weather_year": 2012})

    assert result.is_err(), "hour_map missing 'year' column should be a validation error"
    assert "year" in str(result.err())


def test_get_hour_map_for_validation_read_exception_is_error(monkeypatch, reeds_run_path) -> None:
    """A non-FileNotFoundError during read must produce Err (e.g. corrupt / schema error)."""

    def _raise(*args, **kwargs):
        raise RuntimeError("schema mismatch in hdf5 file")

    parser = _build_parser(reeds_run_path)
    monkeypatch.setattr(parser.store, "read_data", _raise)

    result = parser._get_hour_map_for_validation({"solve_year": 2032, "weather_year": 2012})

    assert result.is_err(), "Read exception on existing hour_map should be a validation error"
    assert "could not be read" in str(result.err())


def test_get_hour_map_for_validation_file_not_found_uses_synthetic(monkeypatch, reeds_run_path) -> None:
    """FileNotFoundError (file genuinely absent) must still produce Ok with a synthetic frame."""

    def _raise(*args, **kwargs):
        raise FileNotFoundError("hmap_allyrs.csv not found")

    parser = _build_parser(reeds_run_path)
    monkeypatch.setattr(parser.store, "read_data", _raise)

    result = parser._get_hour_map_for_validation({"solve_year": 2032, "weather_year": 2012})

    assert result.is_ok(), "Missing file should fall back to synthetic, not Err"
    df = result.ok()
    assert "year" in df.columns
    assert 2012 in df["year"].to_list()
