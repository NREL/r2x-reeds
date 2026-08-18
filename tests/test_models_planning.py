"""Tests for capacity-expansion planning models."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.unit]


def test_capacity_expansion_records_periods_and_representative_timepoints():
    """A planning component preserves the capacity-expansion chronology."""
    from r2x_reeds import (
        EmissionType,
        ReEDSCapacityExpansion,
        ReEDSPlanningPeriod,
        ReEDSRepresentativeTimepoint,
    )

    expansion = ReEDSCapacityExpansion(
        name="synthetic-case",
        emission_type=EmissionType.CO2,
        reserve_margin=0.15,
        planning_periods=(
            ReEDSPlanningPeriod(
                year=2030,
                present_value_factor=1.0,
                emission_cap=1_000_000.0,
            ),
            ReEDSPlanningPeriod(
                year=2035,
                present_value_factor=0.71,
                emission_cap=900_000.0,
            ),
        ),
        representative_timepoints=(
            ReEDSRepresentativeTimepoint(label="h0", position=0, weight=365.0),
            ReEDSRepresentativeTimepoint(label="h1", position=1, weight=365.0),
        ),
    )

    assert [period.year for period in expansion.planning_periods] == [2030, 2035]
    assert [timepoint.weight for timepoint in expansion.representative_timepoints] == [
        365.0,
        365.0,
    ]
    assert expansion.model_dump(mode="json")["emission_type"] == "CO2"


@pytest.mark.parametrize("emission_cap", [-1.0, -0.01])
def test_planning_period_rejects_negative_emission_cap(emission_cap: float):
    """An emissions cap cannot be negative."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSPlanningPeriod

    with pytest.raises(ValidationError):
        ReEDSPlanningPeriod(year=2030, present_value_factor=1.0, emission_cap=emission_cap)


def test_capacity_expansion_requires_caps_for_an_active_emission_policy():
    """A component-level emission type applies a cap in every planning period."""
    from pydantic import ValidationError

    from r2x_reeds import (
        EmissionType,
        ReEDSCapacityExpansion,
        ReEDSPlanningPeriod,
        ReEDSRepresentativeTimepoint,
    )

    with pytest.raises(ValidationError, match="active emission_type requires an emission cap"):
        ReEDSCapacityExpansion(
            name="synthetic-case",
            emission_type=EmissionType.CO2,
            reserve_margin=0.15,
            planning_periods=(ReEDSPlanningPeriod(year=2030, present_value_factor=1.0),),
            representative_timepoints=(ReEDSRepresentativeTimepoint(label="h0", position=0, weight=8_760.0),),
        )


@pytest.mark.parametrize(
    ("emission_type", "emission_cap", "match"),
    [
        (None, 1_000_000.0, "emission caps require emission_type"),
        ("CO2", None, "active emission_type requires an emission cap"),
    ],
)
def test_capacity_expansion_inputs_require_consistent_emission_policy(
    emission_type: str | None,
    emission_cap: float | None,
    match: str,
):
    """Source policy must not describe an inactive or incomplete emissions constraint."""
    from pydantic import ValidationError

    from r2x_reeds import (
        EmissionType,
        ReEDSCapacityExpansionInputs,
        ReEDSPlanningPeriod,
        ReEDSPlantCharacteristics,
        ReEDSRepresentativeTimepoint,
    )

    with pytest.raises(ValidationError, match=match):
        ReEDSCapacityExpansionInputs(
            emission_type=EmissionType(emission_type) if emission_type is not None else None,
            planning_periods=(
                ReEDSPlanningPeriod(
                    year=2030,
                    present_value_factor=1.0,
                    emission_cap=emission_cap,
                ),
            ),
            representative_timepoints=(ReEDSRepresentativeTimepoint(label="h0", position=0, weight=8_760.0),),
            plant_characteristics=(
                ReEDSPlantCharacteristics(
                    technology="battery_li",
                    year=2030,
                    capital_cost=1.0,
                    capital_cost_energy=1.0,
                    fom_cost=1.0,
                    fom_cost_energy=1.0,
                    vom_cost=0.0,
                    round_trip_efficiency=0.85,
                ),
            ),
        )


def test_capacity_expansion_inputs_reject_duplicate_and_inconsistent_source_keys():
    """Input-table records have one unambiguous source key and dimensional relation."""
    from pydantic import ValidationError

    from r2x_reeds import (
        ReEDSCapacityExpansionInputs,
        ReEDSInitialCapacity,
        ReEDSPlanningPeriod,
        ReEDSPlantCharacteristics,
        ReEDSRepresentativeTimepoint,
        ReEDSStorageDuration,
        ReEDSStorageDurationOverride,
    )

    planning_period = ReEDSPlanningPeriod(year=2030, present_value_factor=1.0)
    timepoint = ReEDSRepresentativeTimepoint(label="h0", position=0, weight=8_760.0)
    characteristics = ReEDSPlantCharacteristics(
        technology="battery_li",
        year=2030,
        capital_cost=1.0,
        capital_cost_energy=1.0,
        fom_cost=1.0,
        fom_cost_energy=1.0,
        vom_cost=0.0,
        round_trip_efficiency=0.85,
    )
    capacity = ReEDSInitialCapacity(
        technology="battery_li",
        region="r1",
        initial_power_capacity=1.0,
        initial_energy_capacity=4.0,
    )
    duration = ReEDSStorageDuration(technology="caes", duration=12.0)
    duration_override = ReEDSStorageDurationOverride(
        technology="pumped-hydro",
        vintage="init-1",
        region="r1",
        duration=10.0,
    )
    common = {
        "planning_periods": (planning_period,),
        "representative_timepoints": (timepoint,),
        "plant_characteristics": (characteristics,),
        "initial_capacities": (capacity,),
        "storage_durations": (duration,),
        "storage_duration_overrides": (duration_override,),
    }

    invalid_cases = [
        {"plant_characteristics": (characteristics.model_copy(update={"year": 2035}),)},
        {"plant_characteristics": (characteristics, characteristics.model_copy())},
        {"initial_capacities": (capacity, capacity.model_copy())},
        {
            "initial_capacities": (
                capacity.model_copy(update={"initial_power_capacity": 0.0, "initial_energy_capacity": 4.0}),
            )
        },
        {"storage_durations": (duration, duration.model_copy())},
        {"storage_duration_overrides": (duration_override, duration_override.model_copy())},
    ]

    for invalid_case in invalid_cases:
        with pytest.raises(ValidationError):
            ReEDSCapacityExpansionInputs(**(common | invalid_case))


def test_capacity_expansion_rejects_unordered_planning_periods():
    """Planning periods must have one chronological interpretation."""
    from pydantic import ValidationError

    from r2x_reeds import (
        EmissionType,
        ReEDSCapacityExpansion,
        ReEDSPlanningPeriod,
        ReEDSRepresentativeTimepoint,
    )

    with pytest.raises(ValidationError, match="unique ascending years"):
        ReEDSCapacityExpansion(
            name="synthetic-case",
            emission_type=EmissionType.CO2,
            reserve_margin=0.15,
            planning_periods=(
                ReEDSPlanningPeriod(year=2035, present_value_factor=0.71),
                ReEDSPlanningPeriod(year=2030, present_value_factor=1.0),
            ),
            representative_timepoints=(
                ReEDSRepresentativeTimepoint(label="h0", position=0, weight=4_380.0),
                ReEDSRepresentativeTimepoint(label="h1", position=1, weight=4_380.0),
            ),
        )


def test_capacity_expansion_rejects_duplicate_timepoint_labels():
    """Representative timepoint labels identify one value series."""
    from pydantic import ValidationError

    from r2x_reeds import (
        EmissionType,
        ReEDSCapacityExpansion,
        ReEDSPlanningPeriod,
        ReEDSRepresentativeTimepoint,
    )

    with pytest.raises(ValidationError, match="unique labels"):
        ReEDSCapacityExpansion(
            name="synthetic-case",
            emission_type=EmissionType.CO2,
            reserve_margin=0.15,
            planning_periods=(ReEDSPlanningPeriod(year=2030, present_value_factor=1.0),),
            representative_timepoints=(
                ReEDSRepresentativeTimepoint(label="h0", position=0, weight=4_380.0),
                ReEDSRepresentativeTimepoint(label="h0", position=1, weight=4_380.0),
            ),
        )


def test_capacity_expansion_rejects_noncontiguous_timepoints():
    """Representative timepoints must form one chronological sequence."""
    from pydantic import ValidationError

    from r2x_reeds import (
        EmissionType,
        ReEDSCapacityExpansion,
        ReEDSPlanningPeriod,
        ReEDSRepresentativeTimepoint,
    )

    with pytest.raises(ValidationError, match="contiguous positions"):
        ReEDSCapacityExpansion(
            name="synthetic-case",
            emission_type=EmissionType.CO2,
            reserve_margin=0.15,
            planning_periods=(ReEDSPlanningPeriod(year=2030, present_value_factor=1.0),),
            representative_timepoints=(
                ReEDSRepresentativeTimepoint(label="h0", position=0, weight=4_380.0),
                ReEDSRepresentativeTimepoint(label="h2", position=2, weight=4_380.0),
            ),
        )


def test_capacity_expansion_resource_variants_allow_unbuilt_candidates():
    """Investment candidates distinguish dispatchable, variable, and storage semantics."""
    from r2x_reeds import (
        ReEDSDispatchableCapacityExpansionResource,
        ReEDSRegion,
        ReEDSStorageCapacityExpansionResource,
        ReEDSVariableCapacityExpansionResource,
    )

    region = ReEDSRegion(name="r01")
    dispatchable = ReEDSDispatchableCapacityExpansionResource(
        name="gas_cc_r01",
        region=region,
        technology="gas_cc",
        available_years=(2030, 2035),
        initial_capacity=0.0,
        investment_cost=800_000.0,
        variable_cost=30.0,
        capacity_factor=0.87,
        minimum_generation_fraction=0.0,
        minimum_capacity_factor=0.06,
        ramp_up_cost=40.0,
    )
    variable = ReEDSVariableCapacityExpansionResource(
        name="wind_r01",
        region=region,
        technology="wind",
        available_years=(2030, 2035),
        initial_capacity=0.0,
        investment_cost=800_000.0,
        variable_cost=30.0,
    )
    storage = ReEDSStorageCapacityExpansionResource(
        name="battery_r01",
        region=region,
        technology="battery",
        available_years=(2030, 2035),
        initial_capacity=0.0,
        investment_cost=800_000.0,
        variable_cost=30.0,
        round_trip_efficiency=0.92,
        storage_duration=4.0,
    )

    assert dispatchable.initial_capacity == 0.0
    assert variable.available_years == (2030, 2035)
    assert storage.round_trip_efficiency == 0.92


@pytest.mark.parametrize(
    ("minimum_generation_fraction", "minimum_capacity_factor", "match"),
    [
        (0.51, 0.0, "minimum_generation_fraction must not exceed capacity_factor"),
        (0.0, 0.51, "minimum_capacity_factor must not exceed capacity_factor"),
    ],
)
def test_dispatchable_capacity_expansion_resource_rejects_infeasible_capacity_factors(
    minimum_generation_fraction: float,
    minimum_capacity_factor: float,
    match: str,
):
    """Operating minimums cannot exceed the resource's maximum availability."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSDispatchableCapacityExpansionResource, ReEDSRegion

    with pytest.raises(ValidationError, match=match):
        ReEDSDispatchableCapacityExpansionResource(
            name="gas_cc_r01",
            region=ReEDSRegion(name="r01"),
            technology="gas_cc",
            available_years=(2030,),
            initial_capacity=0.0,
            investment_cost=800_000.0,
            variable_cost=30.0,
            capacity_factor=0.5,
            minimum_generation_fraction=minimum_generation_fraction,
            minimum_capacity_factor=minimum_capacity_factor,
        )


def test_capacity_expansion_resource_requires_a_concrete_operational_subtype():
    """Every candidate must declare how it participates in the formulation."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSCapacityExpansionResource, ReEDSRegion

    with pytest.raises(ValidationError, match="concrete operational subtype"):
        ReEDSCapacityExpansionResource(
            name="candidate_r01",
            region=ReEDSRegion(name="r01"),
            technology="candidate",
            available_years=(2030,),
            initial_capacity=0.0,
            investment_cost=1.0,
            variable_cost=1.0,
        )


@pytest.mark.parametrize(
    ("available_years", "match"),
    [
        ((2035, 2030), "unique and ascending"),
        ((2030, 2030), "unique and ascending"),
    ],
)
def test_capacity_expansion_resource_rejects_ambiguous_availability(
    available_years: tuple[int, ...], match: str
):
    """Candidate availability must identify one chronological set of planning years."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSRegion, ReEDSVariableCapacityExpansionResource

    with pytest.raises(ValidationError, match=match):
        ReEDSVariableCapacityExpansionResource(
            name="wind_r01",
            region=ReEDSRegion(name="r01"),
            technology="wind",
            available_years=available_years,
            initial_capacity=0.0,
            investment_cost=1_500_000.0,
            variable_cost=4.0,
        )


@pytest.mark.parametrize("available_year", [0, -1])
def test_capacity_expansion_resource_rejects_invalid_planning_year(available_year: int):
    """Candidate availability must use valid positive planning years."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSRegion, ReEDSVariableCapacityExpansionResource

    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        ReEDSVariableCapacityExpansionResource(
            name="wind_r01",
            region=ReEDSRegion(name="r01"),
            technology="wind",
            available_years=(available_year,),
            initial_capacity=0.0,
            investment_cost=1_500_000.0,
            variable_cost=4.0,
        )


def test_capacity_expansion_components_round_trip_through_an_infrasys_system(tmp_path):
    """A synthetic planning case retains components and time series after serialization."""
    from datetime import datetime, timedelta

    from infrasys import SingleTimeSeries, System

    from r2x_reeds import (
        EmissionType,
        FromTo_ToFrom,
        ReEDSCapacityExpansion,
        ReEDSDemand,
        ReEDSDispatchableCapacityExpansionResource,
        ReEDSEmission,
        ReEDSInterface,
        ReEDSPlanningPeriod,
        ReEDSRegion,
        ReEDSRepresentativeTimepoint,
        ReEDSStorageCapacityExpansionResource,
        ReEDSTransmissionLine,
        ReEDSVariableCapacityExpansionResource,
    )

    system = System(name="synthetic")
    try:
        region_a = ReEDSRegion(name="r01")
        region_b = ReEDSRegion(name="r02")
        system.add_components(region_a, region_b)

        expansion = ReEDSCapacityExpansion(
            name="capacity_expansion",
            emission_type=EmissionType.CO2,
            reserve_margin=0.15,
            planning_periods=(
                ReEDSPlanningPeriod(
                    year=2030,
                    present_value_factor=1.0,
                    emission_cap=1_000_000.0,
                ),
            ),
            representative_timepoints=(
                ReEDSRepresentativeTimepoint(label="h0", position=0, weight=4_380.0),
                ReEDSRepresentativeTimepoint(label="h1", position=1, weight=4_380.0),
            ),
        )
        demand = ReEDSDemand(name="r01_load", region=region_a, max_active_power=120.0)
        dispatchable = ReEDSDispatchableCapacityExpansionResource(
            name="gas_cc_r01",
            region=region_a,
            technology="gas_cc",
            available_years=(2030,),
            initial_capacity=66.0,
            investment_cost=800_000.0,
            variable_cost=30.0,
            capacity_factor=0.87,
            minimum_capacity_factor=0.06,
            ramp_up_cost=40.0,
        )
        variable = ReEDSVariableCapacityExpansionResource(
            name="wind_r01",
            region=region_a,
            technology="wind",
            available_years=(2030,),
            initial_capacity=0.0,
            investment_cost=1_500_000.0,
            variable_cost=4.0,
        )
        storage = ReEDSStorageCapacityExpansionResource(
            name="battery_r01",
            region=region_a,
            technology="battery",
            available_years=(2030,),
            initial_capacity=0.0,
            investment_cost=600_000.0,
            variable_cost=12.0,
            round_trip_efficiency=0.92,
            storage_duration=4.0,
        )
        interface = ReEDSInterface(name="r01_r02", from_region=region_a, to_region=region_b)
        line = ReEDSTransmissionLine(
            name="r01_r02_line",
            interface=interface,
            max_active_power=FromTo_ToFrom(from_to=25.0, to_from=30.0),
            losses=0.02,
        )
        system.add_components(expansion, demand, dispatchable, variable, storage, interface, line)
        system.add_supplemental_attribute(
            dispatchable,
            ReEDSEmission(rate=400.0, type=EmissionType.CO2),
        )

        initial_timestamp = datetime(2030, 1, 1)
        resolution = timedelta(hours=1)
        system.add_time_series(
            SingleTimeSeries.from_array(
                data=[100.0, 120.0],
                name="max_active_power",
                initial_timestamp=initial_timestamp,
                resolution=resolution,
            ),
            demand,
            solve_year=2030,
        )
        system.add_time_series(
            SingleTimeSeries.from_array(
                data=[0.2, 0.7],
                name="capacity_factor",
                initial_timestamp=initial_timestamp,
                resolution=resolution,
            ),
            variable,
        )

        system_path = tmp_path / "synthetic.json"
        system.to_json(system_path)
        loaded = System.from_json(system_path)
        try:
            loaded_variable = loaded.get_component(
                ReEDSVariableCapacityExpansionResource,
                "wind_r01",
            )
            assert loaded_variable.initial_capacity == 0.0
            assert loaded.has_time_series(loaded_variable, name="capacity_factor")
            assert loaded.get_component(ReEDSCapacityExpansion, "capacity_expansion").reserve_margin == 0.15
        finally:
            loaded.close()
    finally:
        system.close()
