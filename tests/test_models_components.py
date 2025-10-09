"""Tests for ReEDS component models."""

import pytest
from pydantic import ValidationError

from r2x_reeds.models import (
    EmissionType,
    ReEDSDemand,
    ReEDSEmission,
    ReEDSGenerator,
    ReEDSInterface,
    ReEDSRegion,
    ReEDSReserve,
    ReEDSReserveRegion,
    ReserveDirection,
    ReserveType,
)


def test_reeds_region_creation():
    """Test creating a ReEDS region."""
    region = ReEDSRegion(name="p1")
    assert region.name == "p1"


def test_reeds_region_with_state():
    """Test ReEDS region with state attribute."""
    region = ReEDSRegion(name="p1", state="CO")
    assert region.state == "CO"


def test_reeds_region_with_max_active_power():
    """Test ReEDS region with max active power."""
    region = ReEDSRegion(name="p1", max_active_power=1000.0)
    assert region.max_active_power == 1000.0


def test_reeds_region_negative_max_power_fails():
    """Test that negative max_active_power raises validation error."""
    with pytest.raises(ValidationError):
        ReEDSRegion(name="p1", max_active_power=-100.0)


def test_reeds_generator_creation():
    """Test creating a ReEDS generator."""
    region = ReEDSRegion(name="p1")
    gen = ReEDSGenerator(name="gen1", region=region, capacity=500.0)
    assert gen.capacity == 500.0


def test_reeds_generator_negative_capacity_fails():
    """Test that negative capacity raises validation error."""
    region = ReEDSRegion(name="p1")
    with pytest.raises(ValidationError):
        ReEDSGenerator(name="gen1", region=region, capacity=-100.0)


def test_reeds_generator_with_technology():
    """Test generator with technology field."""
    region = ReEDSRegion(name="p1")
    gen = ReEDSGenerator(name="gen1", region=region, capacity=500.0, technology="gas-cc")
    assert gen.technology == "gas-cc"


def test_reeds_emission_creation():
    """Test creating a ReEDS emission."""
    emission = ReEDSEmission(rate=0.5, emission_type=EmissionType.CO2)
    assert emission.rate == 0.5


def test_reeds_emission_type():
    """Test emission type attribute."""
    emission = ReEDSEmission(rate=0.5, emission_type=EmissionType.CO2)
    assert emission.emission_type == EmissionType.CO2


def test_reeds_emission_negative_rate_fails():
    """Test that negative emission rate raises validation error."""
    with pytest.raises(ValidationError):
        ReEDSEmission(rate=-1.0, emission_type=EmissionType.CO2)


def test_reeds_interface_creation():
    """Test creating a ReEDS interface."""
    from_region = ReEDSRegion(name="p1")
    to_region = ReEDSRegion(name="p2")
    interface = ReEDSInterface(name="p1_to_p2", from_region=from_region, to_region=to_region)
    assert interface.from_region.name == "p1"


def test_reeds_interface_to_region():
    """Test interface to_region attribute."""
    from_region = ReEDSRegion(name="p1")
    to_region = ReEDSRegion(name="p2")
    interface = ReEDSInterface(name="p1_to_p2", from_region=from_region, to_region=to_region)
    assert interface.to_region.name == "p2"


def test_reeds_reserve_creation():
    """Test creating a ReEDS reserve."""
    reserve = ReEDSReserve(
        name="reg_up",
        reserve_type=ReserveType.REGULATION,
        direction=ReserveDirection.UP,
    )
    assert reserve.reserve_type == ReserveType.REGULATION


def test_reeds_reserve_direction():
    """Test reserve direction attribute."""
    reserve = ReEDSReserve(
        name="reg_up",
        reserve_type=ReserveType.REGULATION,
        direction=ReserveDirection.UP,
    )
    assert reserve.direction == ReserveDirection.UP


def test_reeds_reserve_region_creation():
    """Test creating a ReEDS reserve region."""
    reserve_region = ReEDSReserveRegion(name="west")
    assert reserve_region.name == "west"


def test_reeds_demand_creation():
    """Test creating a ReEDS demand."""
    region = ReEDSRegion(name="p1")
    demand = ReEDSDemand(name="load_p1", region=region)
    assert demand.region.name == "p1"


def test_reeds_demand_with_max_power():
    """Test demand with max active power."""
    region = ReEDSRegion(name="p1")
    demand = ReEDSDemand(name="load_p1", region=region, max_active_power=800.0)
    assert demand.max_active_power == 800.0


def test_reeds_demand_negative_max_power_fails():
    """Test that negative max power fails validation."""
    region = ReEDSRegion(name="p1")
    with pytest.raises(ValidationError):
        ReEDSDemand(name="load_p1", region=region, max_active_power=-100.0)
