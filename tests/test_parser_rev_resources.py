"""Parser tests for reV/supply-curve resource components."""

from __future__ import annotations

import polars as pl
import pytest

from r2x_core import System
from r2x_reeds.models import ReEDSResourceBuild, ReEDSResourceSite, ReEDSVariableGenerator

pytestmark = [pytest.mark.unit]


def _prepare_parser(parser, sample_region):
    system = System(name="test")
    system.add_component(sample_region)
    parser._ctx.system = system
    parser._region_cache = {sample_region.name: sample_region}
    return system


def _resource_frames(include_selected: bool = True) -> dict[str, pl.LazyFrame | None]:
    source = pl.DataFrame(
        {
            "sc_point_gid": [416, 796, 797],
            "class": [1, 1, 1],
            "capacity": [341.13, 920.14, 629.61],
            "cf": [0.1877, 0.1913, 0.1855],
            "region": ["p1", "p1", "p1"],
            "supply_curve_cost_per_mw": [577564.52875, 743009.39875, 823633.64125],
        }
    ).lazy()
    candidate = pl.DataFrame(
        {
            "sc_gid": [0, 4, 5],
            "sc_point_gid": [416, 796, 797],
            "latitude": [48.994427, 48.900375, 48.927494],
            "longitude": [-122.73455, -122.688286, -122.5285],
            "region": ["p1", "p1", "p1"],
            "class": [1, 1, 1],
            "bin": [18, 25, 26],
            "cap_avail": [254.5761171604243, 686.6734177143724, 469.85664464240193],
            "existing_capacity": [0.0, 0.0, 0.0],
            "online_year": [0, 0, 0],
            "retire_year": [0, 0, 0],
            "supply_curve_cost_per_mw": [1165487.0620945962, 1317865.7838392514, 1339301.1627872088],
        }
    ).lazy()
    selected = (
        pl.DataFrame(
            {
                "year": [2009],
                "sc_gid": [0],
                "sc_point_gid": [416],
                "latitude": [48.994427],
                "longitude": [-122.73455],
                "region": ["p1"],
                "class": [1],
                "bin": [18],
                "supply_curve_cost_per_mw": [1165487.0620945962],
                "built_capacity": [0.3731343025384888],
                "investment_bool": [1],
            }
        ).lazy()
        if include_selected
        else None
    )
    return {
        "upv_supply_curve": source,
        "df_sc_in_upv": candidate,
        "df_sc_out_upv_reduced": selected,
    }


def test_resource_components_preserve_candidates_and_selected_rows(parser, sample_region):
    system = _prepare_parser(parser, sample_region)
    generator = ReEDSVariableGenerator(
        name="upv_p1",
        region=sample_region,
        technology="upv",
        capacity=10.0,
        resource_class="class1",
    )
    system.add_component(generator)

    frames = _resource_frames(include_selected=True)
    parser.read_data_file = lambda name: frames.get(name)  # type: ignore[assignment]

    result = parser._build_resource_components(system)

    assert not result.is_err()

    sites = list(system.get_components(ReEDSResourceSite))
    builds = list(system.get_components(ReEDSResourceBuild))
    generators = list(system.get_components(ReEDSVariableGenerator))

    assert len(sites) == 3
    assert len(builds) == 1
    assert len(generators) == 1
    assert generators[0].capacity == 10.0
    assert generators[0].name == "upv_p1"

    first_site = sites[0]
    assert first_site.sc_point_gid == 416
    assert first_site.resource_class == "1"
    assert first_site.capacity == 341.13
    assert first_site.available_capacity == pytest.approx(254.5761171604243)
    assert first_site.capacity_factor == pytest.approx(0.1877)
    assert first_site.latitude == pytest.approx(48.994427)
    assert first_site.longitude == pytest.approx(-122.73455)

    first_build = builds[0]
    assert first_build.year == 2009
    assert first_build.sc_point_gid == 416
    assert first_build.built_capacity == pytest.approx(0.3731343025384888)
    assert first_build.investment_bool is True
    assert first_build.capacity == 341.13
    assert first_build.available_capacity == pytest.approx(254.5761171604243)


def test_resource_components_skip_missing_selected_output(parser, sample_region):
    system = _prepare_parser(parser, sample_region)
    generator = ReEDSVariableGenerator(
        name="upv_p1",
        region=sample_region,
        technology="upv",
        capacity=10.0,
        resource_class="class1",
    )
    system.add_component(generator)

    frames = _resource_frames(include_selected=False)
    parser.read_data_file = lambda name: frames.get(name)  # type: ignore[assignment]

    result = parser._build_resource_components(system)

    assert not result.is_err()

    sites = list(system.get_components(ReEDSResourceSite))
    builds = list(system.get_components(ReEDSResourceBuild))
    generators = list(system.get_components(ReEDSVariableGenerator))

    assert len(sites) == 3
    assert len(builds) == 0
    assert len(generators) == 1
    assert generators[0].capacity == 10.0
    assert generators[0].name == "upv_p1"
