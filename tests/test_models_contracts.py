"""Contract tests for strict ReEDS domain models."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from r2x_reeds.models import (
    FractionRange,
    NonNegativeRange,
    ReEDSDemand,
    ReEDSRegion,
    ReEDSResourceClass,
    ReEDSStorage,
    ReEDSVariableGenerator,
)

pytestmark = [pytest.mark.unit]


def region_kwargs(name: str = "p1") -> dict[str, str]:
    """Return a complete hierarchy record for a canonical region."""
    return {
        "name": name,
        "state": "CA",
        "nerc_region": "WECC_CA",
        "transmission_region": "CAISO",
        "transmission_group": "CAISO",
        "interconnect": "western",
        "country": "USA",
        "cendiv": "Pacific",
        "usda_region": "pacific",
        "h2ptc_region": "California",
        "hurdle_region": "CAISO",
        "cc_region": "CAISO",
    }


def test_region_requires_complete_hierarchy() -> None:
    """A canonical region cannot be created without required hierarchy data."""
    with pytest.raises(ValidationError, match="state"):
        ReEDSRegion(name="p1")

    with pytest.raises(ValidationError, match="transmission_region"):
        ReEDSRegion(**(region_kwargs() | {"transmission_region": None}))


def test_region_accepts_complete_hierarchy() -> None:
    """A complete hierarchy row creates a canonical region."""
    region = ReEDSRegion(**region_kwargs())

    assert region.state == "CA"
    assert region.transmission_region == "CAISO"
    assert not hasattr(region, "max_active_power")


def test_resource_class_uses_canonical_cost_name_and_unit() -> None:
    """Resource-class costs are nonnegative and do not encode units in names."""
    resource = ReEDSResourceClass(
        name="upv_p1_class_1",
        technology="upv",
        region=ReEDSRegion(**region_kwargs()),
        resource_class="1",
        capacity=0.0,
        cost=0.0,
    )

    assert resource.cost == 0.0
    assert "cost_per_mw" not in ReEDSResourceClass.model_fields
    assert any(
        getattr(metadata, "unit", None) == "$/MW"
        for metadata in ReEDSResourceClass.model_fields["cost"].metadata
    )

    with pytest.raises(ValidationError):
        ReEDSResourceClass(
            name="invalid",
            technology="upv",
            region=ReEDSRegion(**region_kwargs()),
            resource_class="1",
            capacity=1.0,
            cost=-1.0,
        )


def test_demand_requires_nonnegative_peak_power() -> None:
    """A demand component must identify its peak power, including zero."""
    with pytest.raises(ValidationError, match="max_active_power"):
        ReEDSDemand(name="p1_load", region=ReEDSRegion(**region_kwargs()))

    demand = ReEDSDemand(
        name="p1_load",
        region=ReEDSRegion(**region_kwargs()),
        max_active_power=0.0,
    )
    assert demand.max_active_power == 0.0


def test_emission_rate_supports_net_negative_values() -> None:
    """Net sequestration can produce a negative emission rate."""
    from r2x_reeds.models import EmissionType, ReEDSEmission

    emission = ReEDSEmission(rate=-0.1, type=EmissionType.CO2)

    assert emission.rate == -0.1


def test_ranges_use_their_actual_domain() -> None:
    """Power ranges are not restricted to the unit interval."""
    power_range = NonNegativeRange(min=0.0, max=250.0)
    fraction_range = FractionRange(min=0.1, max=0.9)

    assert power_range.max == 250.0
    assert fraction_range.max == 0.9

    with pytest.raises(ValidationError):
        FractionRange(min=0.0, max=1.1)


def test_file_mapping_and_parser_rules_match_canonical_contracts() -> None:
    """Source mappings expose source units while parser rules target canonical fields."""
    config_dir = Path(__file__).parents[1] / "src" / "r2x_reeds" / "config"
    file_mapping = json.loads((config_dir / "file_mapping.json").read_text())
    parser_rules = json.loads((config_dir / "parser_rules.json").read_text())
    datasets = {entry["name"]: entry for entry in file_mapping}
    rules = {rule["target_type"]: rule for rule in parser_rules}

    hierarchy = datasets["hierarchy"]
    assert hierarchy["info"]["is_optional"] is False
    assert hierarchy["proc_spec"]["column_mapping"]["st"] == "state"
    assert hierarchy["proc_spec"]["column_mapping"]["transreg"] == "transmission_region"
    assert datasets["transmission_losses"]["info"]["units"] == "fraction"
    assert datasets["forced_outages"]["info"]["units"] == "fraction"
    assert "units" not in datasets["renewable_supply_curves"]["info"]

    resource_rule = rules["ReEDSVariableGenerator"]
    assert resource_rule["field_map"]["resource_class"] == "resource_class"
    load_rule = rules["ReEDSDemand"]
    assert "max_active_power" not in load_rule["defaults"]


def test_storage_energy_capacity_is_derived_from_required_facts() -> None:
    """Storage energy capacity is not a nullable component field."""
    storage = ReEDSStorage(
        name="battery_p1",
        region=ReEDSRegion(**region_kwargs()),
        technology="battery",
        capacity=100.0,
        storage_duration=4.0,
        round_trip_efficiency=0.85,
    )

    assert not hasattr(storage, "energy_capacity")
    assert storage.capacity * storage.storage_duration == 400.0


def test_components_do_not_embed_supplemental_attributes() -> None:
    """Infrasys supplemental attributes are associations, never component fields."""
    from infrasys import SupplementalAttribute

    from r2x_reeds.models import (
        ReEDSConsumingTechnology,
        ReEDSDataCenterDemand,
        ReEDSDemand,
        ReEDSGenerator,
        ReEDSRegion,
        ReEDSResourceClass,
        ReEDSStorage,
        ReEDSThermalGenerator,
        ReEDSTransmissionLine,
        ReEDSVariableGenerator,
    )

    component_types = (
        ReEDSConsumingTechnology,
        ReEDSDataCenterDemand,
        ReEDSDemand,
        ReEDSGenerator,
        ReEDSRegion,
        ReEDSResourceClass,
        ReEDSStorage,
        ReEDSThermalGenerator,
        ReEDSTransmissionLine,
        ReEDSVariableGenerator,
    )
    for component_type in component_types:
        assert all(
            not (isinstance(field.annotation, type) and issubclass(field.annotation, SupplementalAttribute))
            for field in component_type.model_fields.values()
        )


def test_supply_curve_enrichment_is_not_embedded_in_the_component() -> None:
    """Supply-curve data is a separate supplemental-attribute model."""
    from r2x_reeds.models import ReEDSGeneratorSupplyCurve

    generator = ReEDSVariableGenerator(
        name="upv_p1",
        region=ReEDSRegion(**region_kwargs()),
        technology="upv",
        capacity=1.0,
    )
    enrichment = ReEDSGeneratorSupplyCurve(resource_class="1", supply_curve_cost=100.0)

    assert not hasattr(generator, "source_data")
    assert enrichment.resource_class == "1"
