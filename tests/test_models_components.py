"""Tests for technology-specific component models."""

from typing import Any

import pytest
from pydantic import ValidationError

pytestmark = [pytest.mark.unit]


def test_thermal_generator_requires_heat_rate(sample_region):
    from r2x_reeds.models.components import ReEDSThermalGenerator

    bad_input: dict[str, Any] = {
        "name": "test",
        "region": sample_region,
        "technology": "gas-cc",
        "capacity": 100.0,
        "fuel_type": "naturalgas",
    }
    with pytest.raises(ValidationError) as exc_info:
        ReEDSThermalGenerator(**bad_input)
    assert "heat_rate" in str(exc_info.value)


def test_thermal_generator_requires_fuel_type(sample_region):
    from r2x_reeds.models.components import ReEDSThermalGenerator

    bad_input: dict[str, Any] = {
        "name": "test",
        "region": sample_region,
        "technology": "gas-cc",
        "capacity": 100.0,
        "heat_rate": 7.5,
    }
    with pytest.raises(ValidationError) as exc_info:
        ReEDSThermalGenerator(**bad_input)
    assert "fuel_type" in str(exc_info.value)


def test_storage_generator_requires_duration(sample_region):
    from r2x_reeds.models.components import ReEDSStorage

    bad_input: dict[str, Any] = {
        "name": "test",
        "region": sample_region,
        "technology": "battery_li",
        "capacity": 100.0,
        "round_trip_efficiency": 0.85,
    }
    with pytest.raises(ValidationError) as exc_info:
        ReEDSStorage(**bad_input)
    assert "storage_duration" in str(exc_info.value)


def test_storage_generator_requires_efficiency(sample_region):
    from r2x_reeds.models.components import ReEDSStorage

    bad_input: dict[str, Any] = {
        "name": "test",
        "region": sample_region,
        "technology": "battery_li",
        "capacity": 100.0,
        "storage_duration": 4.0,
    }
    with pytest.raises(ValidationError) as exc_info:
        ReEDSStorage(**bad_input)
    assert "round_trip_efficiency" in str(exc_info.value)


def test_storage_efficiency_bounded(sample_region):
    from r2x_reeds.models.components import ReEDSStorage

    with pytest.raises(ValidationError):
        ReEDSStorage(
            name="test",
            region=sample_region,
            technology="battery_li",
            capacity=100.0,
            storage_duration=4.0,
            round_trip_efficiency=1.5,
        )


def test_hydro_generator_requires_dispatchable_flag(sample_region):
    from r2x_reeds.models.components import ReEDSHydroGenerator

    bad_input: dict[str, Any] = {
        "name": "test",
        "region": sample_region,
        "technology": "hyd",
        "capacity": 200.0,
    }
    with pytest.raises(ValidationError) as exc_info:
        ReEDSHydroGenerator(**bad_input)
    assert "is_dispatchable" in str(exc_info.value)


def test_consuming_tech_requires_electricity_consumption_rate(sample_region):
    from r2x_reeds.models.components import ReEDSConsumingTechnology

    bad_input: dict[str, Any] = {
        "name": "test",
        "region": sample_region,
        "technology": "electrolyzer",
        "capacity": 100.0,
    }
    with pytest.raises(ValidationError) as exc_info:
        ReEDSConsumingTechnology(**bad_input)
    assert "electricity_consumption_rate" in str(exc_info.value)


def test_h2_storage_requires_storage_type(sample_region):
    from r2x_reeds.models.components import ReEDSH2Storage

    bad_input: dict[str, Any] = {
        "name": "test",
        "region": sample_region,
        "capacity": 1000.0,
    }
    with pytest.raises(ValidationError) as exc_info:
        ReEDSH2Storage(**bad_input)
    assert "storage_type" in str(exc_info.value)


def test_h2_pipeline_requires_distance(sample_region):
    from r2x_reeds.models.components import ReEDSH2Pipeline

    bad_input: dict[str, Any] = {
        "name": "test",
        "from_region": sample_region,
        "to_region": sample_region,
        "capacity": 500.0,
    }
    with pytest.raises(ValidationError) as exc_info:
        ReEDSH2Pipeline(**bad_input)
    assert "distance" in str(exc_info.value)


def test_thermal_generator_valid(thermal_generator):
    assert thermal_generator.heat_rate == 7.5
    assert thermal_generator.fuel_type == "naturalgas"


def test_renewable_generator_valid(renewable_generator):
    assert renewable_generator.technology == "upv"
    assert renewable_generator.technology == "upv"


def test_storage_generator_valid(storage_generator):
    assert storage_generator.storage_duration == 4.0
    assert storage_generator.round_trip_efficiency == 0.85


def test_hydro_generator_valid(hydro_generator):
    assert hydro_generator.is_dispatchable is True


def test_consuming_technology_valid(consuming_technology):
    assert consuming_technology.electricity_consumption_rate == 51.45
    assert consuming_technology.electricity_consumption_rate == 51.45


def test_emission_optional_pollutants():
    from r2x_reeds.models.components import ReEDSEmission
    from r2x_reeds.models.enums import EmissionSource, EmissionType

    emission = ReEDSEmission(rate=0.45, type=EmissionType.CO2)
    assert emission.rate == 0.45
    assert emission.type == EmissionType.CO2
    assert emission.source == EmissionSource.COMBUSTION


def test_base_generator_inheritance(thermal_generator):
    from r2x_reeds.models.components import ReEDSGenerator

    assert isinstance(thermal_generator, ReEDSGenerator)
    assert thermal_generator.capacity == 500.0
    assert thermal_generator.region.name == "p1"


def test_generator_negative_capacity_invalid(sample_region):
    from r2x_reeds.models.components import ReEDSThermalGenerator

    with pytest.raises(ValidationError):
        ReEDSThermalGenerator(
            name="test",
            region=sample_region,
            technology="gas-cc",
            capacity=-100.0,
            heat_rate=7.5,
            fuel_type="naturalgas",
        )


def test_generator_outage_rate_bounded(sample_region):
    from r2x_reeds.models.components import ReEDSThermalGenerator

    with pytest.raises(ValidationError):
        ReEDSThermalGenerator(
            name="test",
            region=sample_region,
            technology="gas-cc",
            capacity=100.0,
            heat_rate=7.5,
            fuel_type="naturalgas",
            forced_outage_rate=1.5,
        )
