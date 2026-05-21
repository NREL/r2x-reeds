"""Basic ReEDS parser tests using r2x-core 0.1.1 API.

These tests verify basic parser instantiation and configuration using
a minimal test data set.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

pytestmark = [pytest.mark.integration]

if TYPE_CHECKING:
    pass


def test_build_synthetic_hour_map_contains_requested_weather_years() -> None:
    """Synthetic hour_map includes all requested weather years."""
    from r2x_reeds.parser import _build_synthetic_hour_map

    weather_years = [2007, 2012]
    df = _build_synthetic_hour_map(weather_years)

    assert set(df.columns) == {"year", "time_index", "hour_period", "season"}
    assert sorted(df["year"].to_list()) == sorted(weather_years)
    assert df.height == 2
