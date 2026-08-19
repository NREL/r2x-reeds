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


def test_capacity_expansion_schemas_document_collection_invariants():
    """Schema descriptions expose collection invariants enforced by annotations."""
    from r2x_reeds import ReEDSCapacityExpansion, ReEDSCapacityExpansionInputs

    input_properties = ReEDSCapacityExpansionInputs.model_json_schema()["properties"]
    expansion_properties = ReEDSCapacityExpansion.model_json_schema()["properties"]

    assert "unique ascending" in input_properties["planning_periods"]["description"].lower()
    assert "unique labels" in input_properties["representative_timepoints"]["description"].lower()
    assert "unique technology-year" in input_properties["plant_characteristics"]["description"].lower()
    assert "unique ascending" in expansion_properties["planning_periods"]["description"].lower()


@pytest.mark.parametrize("weight", [0.0, -1.0])
def test_representative_timepoint_rejects_nonpositive_weight(weight: float):
    """A representative timepoint must represent a positive number of hours."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSRepresentativeTimepoint

    with pytest.raises(ValidationError):
        ReEDSRepresentativeTimepoint(label="h0", position=0, weight=weight)


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


def test_capacity_expansion_inputs_reject_emission_caps_without_emission_type():
    """Source inputs reject emission caps when no emission type is active."""
    from pydantic import ValidationError

    from r2x_reeds import (
        ReEDSCapacityExpansionInputs,
        ReEDSPlanningPeriod,
        ReEDSPlantCharacteristics,
        ReEDSRepresentativeTimepoint,
    )

    with pytest.raises(ValidationError, match="emission caps require emission_type"):
        ReEDSCapacityExpansionInputs(
            emission_type=None,
            planning_periods=(
                ReEDSPlanningPeriod(
                    year=2030,
                    present_value_factor=1.0,
                    emission_cap=1_000_000.0,
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


def test_capacity_expansion_inputs_reject_missing_caps_for_active_emission_type():
    """Source inputs require a cap for every period when emission constraints are active."""
    from pydantic import ValidationError

    from r2x_reeds import (
        EmissionType,
        ReEDSCapacityExpansionInputs,
        ReEDSPlanningPeriod,
        ReEDSPlantCharacteristics,
        ReEDSRepresentativeTimepoint,
    )

    with pytest.raises(ValidationError, match="active emission_type requires an emission cap"):
        ReEDSCapacityExpansionInputs(
            emission_type=EmissionType.CO2,
            planning_periods=(ReEDSPlanningPeriod(year=2030, present_value_factor=1.0),),
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


def _valid_capacity_expansion_input_kwargs():
    """Return valid shared data for source-input validation tests."""
    from r2x_reeds import (
        ReEDSInitialCapacity,
        ReEDSPlanningPeriod,
        ReEDSPlantCharacteristics,
        ReEDSRepresentativeTimepoint,
        ReEDSStorageDuration,
        ReEDSStorageDurationOverride,
    )

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
    return {
        "planning_periods": (ReEDSPlanningPeriod(year=2030, present_value_factor=1.0),),
        "representative_timepoints": (
            ReEDSRepresentativeTimepoint(label="h0", position=0, weight=8_760.0),
        ),
        "plant_characteristics": (characteristics,),
        "initial_capacities": (capacity,),
        "storage_durations": (duration,),
        "storage_duration_overrides": (duration_override,),
    }, characteristics, capacity, duration, duration_override


def test_capacity_expansion_inputs_reject_plant_characteristics_outside_modeled_years():
    """Plant characteristics must use one of the modeled planning years."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSCapacityExpansionInputs

    common, characteristics, _, _, _ = _valid_capacity_expansion_input_kwargs()

    with pytest.raises(ValidationError, match="must use a modeled planning year") as exc_info:
        ReEDSCapacityExpansionInputs(
            **(common | {"plant_characteristics": (characteristics.model_copy(update={"year": 2035}),)})
        )

    assert exc_info.value.errors()[0]["loc"] == ("plant_characteristics",)


def test_capacity_expansion_inputs_reject_duplicate_plant_characteristics():
    """Plant characteristics must be unique by technology and year."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSCapacityExpansionInputs

    common, characteristics, _, _, _ = _valid_capacity_expansion_input_kwargs()

    with pytest.raises(ValidationError, match="unique by technology and year") as exc_info:
        ReEDSCapacityExpansionInputs(
            **(common | {"plant_characteristics": (characteristics, characteristics.model_copy())})
        )

    assert exc_info.value.errors()[0]["loc"] == ("plant_characteristics",)


def test_capacity_expansion_inputs_reject_duplicate_initial_capacities():
    """Initial capacities must be unique by technology and region."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSCapacityExpansionInputs

    common, _, capacity, _, _ = _valid_capacity_expansion_input_kwargs()

    with pytest.raises(ValidationError, match="unique by technology and region") as exc_info:
        ReEDSCapacityExpansionInputs(
            **(common | {"initial_capacities": (capacity, capacity.model_copy())})
        )

    assert exc_info.value.errors()[0]["loc"] == ("initial_capacities",)


def test_capacity_expansion_inputs_reject_positive_energy_capacity_without_power_capacity():
    """Positive initial energy capacity requires positive initial power capacity."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSCapacityExpansionInputs

    common, _, capacity, _, _ = _valid_capacity_expansion_input_kwargs()
    invalid_capacity = capacity.model_copy(
        update={"initial_power_capacity": 0.0, "initial_energy_capacity": 4.0}
    )

    with pytest.raises(ValidationError, match="requires positive initial_power_capacity") as exc_info:
        ReEDSCapacityExpansionInputs(**(common | {"initial_capacities": (invalid_capacity,)}))

    assert exc_info.value.errors()[0]["loc"] == ("initial_capacities",)


def test_capacity_expansion_inputs_reject_duplicate_storage_durations():
    """Storage durations must be unique by technology."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSCapacityExpansionInputs

    common, _, _, duration, _ = _valid_capacity_expansion_input_kwargs()

    with pytest.raises(ValidationError, match="unique by technology") as exc_info:
        ReEDSCapacityExpansionInputs(
            **(common | {"storage_durations": (duration, duration.model_copy())})
        )

    assert exc_info.value.errors()[0]["loc"] == ("storage_durations",)


def test_capacity_expansion_inputs_reject_duplicate_storage_duration_overrides():
    """Storage-duration overrides must be unique by technology, vintage, and region."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSCapacityExpansionInputs

    common, _, _, _, duration_override = _valid_capacity_expansion_input_kwargs()

    with pytest.raises(ValidationError, match="unique by technology, vintage, and region") as exc_info:
        ReEDSCapacityExpansionInputs(
            **(
                common
                | {
                    "storage_duration_overrides": (
                        duration_override,
                        duration_override.model_copy(),
                    )
                }
            )
        )

    assert exc_info.value.errors()[0]["loc"] == ("storage_duration_overrides",)

def test_capacity_expansion_rejects_unordered_planning_periods():
    """Planning periods must have one chronological interpretation."""
    from pydantic import ValidationError

    from r2x_reeds import (
        EmissionType,
        ReEDSCapacityExpansion,
        ReEDSPlanningPeriod,
        ReEDSRepresentativeTimepoint,
    )

    with pytest.raises(ValidationError, match="unique ascending years") as exc_info:
        ReEDSCapacityExpansion(
            name="synthetic-case",
            emission_type=EmissionType.CO2,
            reserve_margin=0.15,
            planning_periods=(
                ReEDSPlanningPeriod(year=2035, present_value_factor=0.71, emission_cap=900_000.0),
                ReEDSPlanningPeriod(year=2030, present_value_factor=1.0, emission_cap=1_000_000.0),
            ),
            representative_timepoints=(
                ReEDSRepresentativeTimepoint(label="h0", position=0, weight=4_380.0),
                ReEDSRepresentativeTimepoint(label="h1", position=1, weight=4_380.0),
            ),
        )

    assert exc_info.value.errors()[0]["loc"] == ("planning_periods",)


def test_capacity_expansion_rejects_duplicate_timepoint_labels():
    """Representative timepoint labels identify one value series."""
    from pydantic import ValidationError

    from r2x_reeds import (
        EmissionType,
        ReEDSCapacityExpansion,
        ReEDSPlanningPeriod,
        ReEDSRepresentativeTimepoint,
    )

    with pytest.raises(ValidationError, match="unique labels") as exc_info:
        ReEDSCapacityExpansion(
            name="synthetic-case",
            emission_type=EmissionType.CO2,
            reserve_margin=0.15,
            planning_periods=(
                ReEDSPlanningPeriod(year=2030, present_value_factor=1.0, emission_cap=1_000_000.0),
            ),
            representative_timepoints=(
                ReEDSRepresentativeTimepoint(label="h0", position=0, weight=4_380.0),
                ReEDSRepresentativeTimepoint(label="h0", position=1, weight=4_380.0),
            ),
        )

    assert exc_info.value.errors()[0]["loc"] == ("representative_timepoints",)


def test_capacity_expansion_rejects_noncontiguous_timepoints():
    """Representative timepoints must form one chronological sequence."""
    from pydantic import ValidationError

    from r2x_reeds import (
        EmissionType,
        ReEDSCapacityExpansion,
        ReEDSPlanningPeriod,
        ReEDSRepresentativeTimepoint,
    )

    with pytest.raises(ValidationError, match="contiguous positions") as exc_info:
        ReEDSCapacityExpansion(
            name="synthetic-case",
            emission_type=EmissionType.CO2,
            reserve_margin=0.15,
            planning_periods=(
                ReEDSPlanningPeriod(year=2030, present_value_factor=1.0, emission_cap=1_000_000.0),
            ),
            representative_timepoints=(
                ReEDSRepresentativeTimepoint(label="h0", position=0, weight=4_380.0),
                ReEDSRepresentativeTimepoint(label="h2", position=2, weight=4_380.0),
            ),
        )

    assert exc_info.value.errors()[0]["loc"] == ("representative_timepoints",)


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


def test_dispatchable_capacity_expansion_resource_rejects_minimum_generation_fraction_above_capacity_factor():
    """The minimum generation fraction must not exceed capacity factor."""
    from pydantic import ValidationError

    from r2x_reeds import ReEDSDispatchableCapacityExpansionResource, ReEDSRegion

    with pytest.raises(ValidationError, match="minimum_generation_fraction must not exceed capacity_factor"):
        ReEDSDispatchableCapacityExpansionResource(
            name="gas_cc_r01",
            region=ReEDSRegion(name="r01"),
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
            region=ReEDSRegion(name="r01"),
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
            region=ReEDSRegion(name="r01"),
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
            region=ReEDSRegion(name="r01"),
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
            region=ReEDSRegion(name="r01"),
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
