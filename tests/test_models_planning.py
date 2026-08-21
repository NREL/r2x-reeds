"""Tests for ReEDS planning components and supplemental attributes."""

from __future__ import annotations

import pytest

pytestmark = [pytest.mark.unit]


def test_capacity_expansion_resource_variants_allow_unbuilt_candidates():
    """Investment candidates distinguish dispatchable, variable, and storage semantics."""
    from r2x_reeds import (
        ReEDSDispatchableCapacityExpansionResource,
        ReEDSRegion,
        ReEDSStorageCapacityExpansionResource,
        ReEDSVariableCapacityExpansionResource,
    )

    region = ReEDSRegion.example().model_copy(update={"name": "r01"})
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


def test_dispatchable_capacity_expansion_resource_rejects_minimum_generation_fraction_above_capacity_factor():
    """The minimum generation fraction must not exceed capacity factor."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSDispatchableCapacityExpansionResource, ReEDSRegion

    with pytest.raises(ValidationError, match="minimum_generation_fraction must not exceed capacity_factor"):
        ReEDSDispatchableCapacityExpansionResource(
            name="gas_cc_r01",
            region=ReEDSRegion.example().model_copy(update={"name": "r01"}),
            technology="gas_cc",
            available_years=(2030,),
            initial_capacity=0.0,
            investment_cost=800_000.0,
            variable_cost=30.0,
            capacity_factor=0.5,
            minimum_generation_fraction=0.51,
            minimum_capacity_factor=0.0,
        )


def test_dispatchable_capacity_expansion_resource_rejects_minimum_capacity_factor_above_capacity_factor():
    """The minimum annual capacity factor must not exceed capacity factor."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSDispatchableCapacityExpansionResource, ReEDSRegion

    with pytest.raises(ValidationError, match="minimum_capacity_factor must not exceed capacity_factor"):
        ReEDSDispatchableCapacityExpansionResource(
            name="gas_cc_r01",
            region=ReEDSRegion.example().model_copy(update={"name": "r01"}),
            technology="gas_cc",
            available_years=(2030,),
            initial_capacity=0.0,
            investment_cost=800_000.0,
            variable_cost=30.0,
            capacity_factor=0.5,
            minimum_generation_fraction=0.0,
            minimum_capacity_factor=0.51,
        )


def test_capacity_expansion_resource_requires_a_concrete_operational_subtype():
    """Every candidate must declare how it participates in the formulation."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSCapacityExpansionResource, ReEDSRegion

    with pytest.raises(ValidationError, match="concrete operational subtype"):
        ReEDSCapacityExpansionResource(
            name="candidate_r01",
            region=ReEDSRegion.example().model_copy(update={"name": "r01"}),
            technology="candidate",
            available_years=(2030,),
            initial_capacity=0.0,
            investment_cost=1.0,
            variable_cost=1.0,
        )


def test_capacity_expansion_resource_rejects_descending_available_years():
    """Candidate availability years must be ascending."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSRegion, ReEDSVariableCapacityExpansionResource

    with pytest.raises(ValidationError, match="unique and ascending") as exc_info:
        ReEDSVariableCapacityExpansionResource(
            name="wind_r01",
            region=ReEDSRegion.example().model_copy(update={"name": "r01"}),
            technology="wind",
            available_years=(2035, 2030),
            initial_capacity=0.0,
            investment_cost=1_500_000.0,
            variable_cost=4.0,
        )

    assert exc_info.value.errors()[0]["loc"] == ("available_years",)


def test_capacity_expansion_resource_rejects_duplicate_available_years():
    """Candidate availability years must be unique."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSRegion, ReEDSVariableCapacityExpansionResource

    with pytest.raises(ValidationError, match="unique and ascending") as exc_info:
        ReEDSVariableCapacityExpansionResource(
            name="wind_r01",
            region=ReEDSRegion.example().model_copy(update={"name": "r01"}),
            technology="wind",
            available_years=(2030, 2030),
            initial_capacity=0.0,
            investment_cost=1_500_000.0,
            variable_cost=4.0,
        )

    assert exc_info.value.errors()[0]["loc"] == ("available_years",)


@pytest.mark.parametrize("available_year", [0, -1])
def test_capacity_expansion_resource_rejects_invalid_planning_year(available_year: int):
    """Candidate availability must use valid positive planning years."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSRegion, ReEDSVariableCapacityExpansionResource

    with pytest.raises(ValidationError, match="greater than or equal to 1"):
        ReEDSVariableCapacityExpansionResource(
            name="wind_r01",
            region=ReEDSRegion.example().model_copy(update={"name": "r01"}),
            technology="wind",
            available_years=(available_year,),
            initial_capacity=0.0,
            investment_cost=1_500_000.0,
            variable_cost=4.0,
        )


def test_planning_components_are_distinct_and_share_period_attributes(tmp_path):
    """Planning records remain first-class system objects after serialization."""
    from infrasys import System

    from r2x_reeds import (
        AnnualCapMode,
        ReEDSInitialCapacity,
        ReEDSPlanningPeriod,
        ReEDSPlanningSwitches,
        ReEDSPlantCharacteristics,
        ReEDSPumpedStorageSupplyCurveDuration,
        ReEDSRegion,
        ReEDSRepresentativeTimepoint,
        ReEDSStorageDuration,
        ReEDSStorageDurationOverride,
    )

    system = System(name="synthetic")
    region = ReEDSRegion.example().model_copy(update={"name": "r01"})
    switches = ReEDSPlanningSwitches(
        name="planning_switches",
        annual_cap=AnnualCapMode.CO2,
        storage=1,
        hydro_psh_duration_data=1,
    )
    period = ReEDSPlanningPeriod(year=2030, present_value_factor=1.0, emission_cap=1_000_000.0)
    timepoint = ReEDSRepresentativeTimepoint(name="h0", position=0, weight=4_380.0)
    characteristics = ReEDSPlantCharacteristics(
        name="battery_2030",
        technology="battery",
        year=2030,
        capcost=202_141.0,
        capcost_energy=152_868.0,
        fom=5_056.0,
        fom_energy=3_823.0,
        vom=0.0,
        rte=0.85,
    )
    initial = ReEDSInitialCapacity(
        name="battery_r01",
        technology="battery",
        region=region,
        initial_power_capacity=2.0,
        initial_energy_capacity=4.0,
    )
    duration = ReEDSStorageDuration(name="battery", technology="battery", duration=4.0)
    override = ReEDSStorageDurationOverride(
        name="battery_init_r01",
        technology="battery",
        vintage="init",
        region=region,
        duration=4.0,
    )
    pumped = ReEDSPumpedStorageSupplyCurveDuration(
        name="pumped_storage_supply_curve_duration",
        duration=8.0,
    )

    system.add_components(region, switches, timepoint, characteristics, initial, duration, override, pumped)
    system.add_supplemental_attribute(switches, period)
    system.add_supplemental_attribute(characteristics, period)

    system_path = tmp_path / "planning.json"
    system.to_json(system_path)
    loaded = System.from_json(system_path)
    try:
        loaded_switches = loaded.get_component(ReEDSPlanningSwitches, "planning_switches")
        loaded_characteristics = loaded.get_component(ReEDSPlantCharacteristics, "battery_2030")
        loaded_periods = list(
            loaded.get_supplemental_attributes_with_component(loaded_switches, ReEDSPlanningPeriod)
        )
        characteristic_periods = list(
            loaded.get_supplemental_attributes_with_component(loaded_characteristics, ReEDSPlanningPeriod)
        )
        assert loaded.get_component(ReEDSRepresentativeTimepoint, "h0").weight == 4_380.0
        assert loaded.get_component(ReEDSInitialCapacity, "battery_r01").region.name == "r01"
        assert loaded_periods[0].year == 2030
        assert characteristic_periods[0].emission_cap == 1_000_000.0
    finally:
        loaded.close()
    system.close()
