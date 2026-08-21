"""Tests for planning-frame preparation."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.unit]


def test_planning_frame_preparation_handles_optional_and_invalid_inputs() -> None:
    """Planning frame preparation joins optional sources and rejects malformed rows."""
    import polars as pl

    from r2x_reeds.parser_planning_frames import (
        build_planning_initial_capacity_frame,
        build_planning_periods_frame,
        build_planning_plant_characteristics_frame,
        build_planning_representative_timepoints_frame,
    )

    years = pl.DataFrame({"modeled_years": [2030]})
    factors = pl.DataFrame({"year": [2030], "present_value_factor": [1.0]})
    without_caps = build_planning_periods_frame(years, factors, None)
    assert without_caps["emission_cap"].to_list() == [None]
    with_caps = build_planning_periods_frame(
        years,
        factors,
        pl.DataFrame({"year": [2030], "value": [100.0]}),
    )
    assert with_caps["emission_cap"].to_list() == [100.0]
    with pytest.raises(ValueError, match="no planning years"):
        build_planning_periods_frame(pl.DataFrame({"modeled_years": []}), factors, None)

    representative = build_planning_representative_timepoints_frame(
        pl.DataFrame({"label": ["h1"], "weight": [1.0]})
    )
    assert representative["position"].to_list() == [0]
    with pytest.raises(ValueError, match="contains no rows"):
        build_planning_representative_timepoints_frame(pl.DataFrame({"label": [], "weight": []}))

    source = pl.DataFrame(
        {
            "technology": ["battery", "battery"],
            "year": [2030, 2030],
            "variable": ["capcost", "capcost"],
            "value": [1.0, 2.0],
        }
    )
    with pytest.raises(ValueError, match="duplicate variable"):
        build_planning_plant_characteristics_frame(source, planning_years=(2030,))
    with pytest.raises(ValueError, match="no records"):
        build_planning_plant_characteristics_frame(source, planning_years=(2035,))

    power = pl.DataFrame({"technology": ["battery"], "region": ["r1"], "capacity": [2.0]})
    without_energy = build_planning_initial_capacity_frame(power, None)
    assert without_energy["initial_energy_capacity"].to_list() == [None]
    orphaned_energy = pl.DataFrame(
        {"technology": ["wind"], "region": ["r1"], "energy_capacity": [1.0]}
    )
    with pytest.raises(ValueError, match="no matching power capacity"):
        build_planning_initial_capacity_frame(power, orphaned_energy)
    with pytest.raises(ValueError, match="contains no rows"):
        build_planning_initial_capacity_frame(power.clear(), None)
