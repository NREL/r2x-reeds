"""Tests for the public parser builder modules."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import polars as pl
import pytest

from r2x_core import System
from r2x_reeds import ReEDSConfig, ReEDSParser
from r2x_reeds.parser_builders import (
    build_emissions,
    build_generators,
    build_loads,
    build_regions,
    build_reserves,
    build_transmission,
)

pytestmark = [pytest.mark.integration]

if TYPE_CHECKING:
    from r2x_core import DataStore


@pytest.fixture
def initialized_parser(
    example_parser: ReEDSParser, example_reeds_config: ReEDSConfig, example_data_store: DataStore
) -> ReEDSParser:
    """Return a parser with configuration and preparation complete."""
    from r2x_core import PluginContext

    context = PluginContext(config=example_reeds_config, store=example_data_store)
    parser = cast(ReEDSParser, example_parser.from_context(context))
    assert parser.on_validate().is_ok()
    assert parser.on_prepare().is_ok()
    return parser


@pytest.fixture
def built_system() -> System:
    """Return an empty system for public builder tests."""
    return System(name="test_builder")


def set_system(parser: ReEDSParser, system: System) -> None:
    """Set the public context system used by builder getters."""
    parser.ctx.system = system
    parser.ctx.target_system = system


def test_parser_attaches_source_enrichments_to_generators(example_system: System) -> None:
    """Optional generator datasets are stored as supplemental attributes."""
    from r2x_reeds.models import ReEDSGenerator, ReEDSGeneratorEconomics

    generators = list(example_system.get_components(ReEDSGenerator))
    assert generators
    assert any(
        example_system.get_supplemental_attributes_with_component(generator, ReEDSGeneratorEconomics)
        for generator in generators
    )
    assert all("source_data" not in type(generator).model_fields for generator in generators)


def test_public_builders_create_the_expected_system_parts(
    initialized_parser: ReEDSParser,
    built_system: System,
) -> None:
    """Component builders materialize rows through the configured parser rules."""
    parser = initialized_parser
    set_system(parser, built_system)

    assert build_regions(parser.ctx).is_ok()
    assert build_generators(parser.ctx).is_ok()
    assert build_transmission(parser.ctx).is_ok()
    assert build_loads(parser.ctx).is_ok()
    assert build_reserves(parser.ctx).is_ok()
    assert build_emissions(parser.ctx).is_ok()

    from r2x_reeds.models import ReEDSDemand, ReEDSGenerator, ReEDSRegion, ReEDSReserve

    assert len(list(built_system.get_components(ReEDSRegion))) == 11
    assert len(list(built_system.get_components(ReEDSGenerator))) > 0
    assert len(list(built_system.get_components(ReEDSDemand))) == 11
    assert len(list(built_system.get_components(ReEDSReserve))) > 0


def test_emissions_only_attach_to_existing_generators(
    example_reeds_config: ReEDSConfig,
    example_data_store: DataStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Emission matching does not create or attach attributes to unknown units."""
    from r2x_core import PluginContext
    from r2x_reeds.models.components import ReEDSRegion, ReEDSThermalGenerator

    context = PluginContext(config=example_reeds_config, store=example_data_store)
    parser = cast(ReEDSParser, ReEDSParser.from_context(context))
    assert parser.on_validate().is_ok()
    assert parser.on_prepare().is_ok()

    system = System(name="emissions-test")
    set_system(parser, system)
    region = ReEDSRegion.example().model_copy(update={"name": "test-region"})
    system.add_component(region)
    generator = ReEDSThermalGenerator(
        name="test-tech_test-region",
        region=region,
        technology="test-tech",
        capacity=10.0,
        heat_rate=10.0,
        fuel_type="naturalgas",
    )
    system.add_component(generator)

    original_read = example_data_store.read_data
    emission_data = pl.DataFrame(
        {
            "i": [generator.technology, "missing-tech"],
            "r": [generator.region.name, "p999"],
            "v": [generator.identity.vintage, None],
            "rate": [1.23, 4.56],
            "emission_type": ["CO2E", "CO2E"],
            "emission_source": ["COMBUSTION", "COMBUSTION"],
        }
    ).lazy()

    def fake_read(name: str, *, placeholders=None):
        if name == "emission_rates":
            return emission_data
        return original_read(name, placeholders=placeholders)

    monkeypatch.setattr(example_data_store, "read_data", fake_read)
    result = build_emissions(parser.ctx)
    assert result.is_ok()


def test_transmission_losses_are_attached_to_lines(
    initialized_parser: ReEDSParser,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Transmission loss rows flow through the transmission rule."""
    from r2x_reeds.models.components import ReEDSTransmissionLine

    parser = initialized_parser
    system = System(name="transmission-losses-test")
    set_system(parser, system)
    assert build_regions(parser.ctx).is_ok()
    original_read = parser.ctx.store.read_data

    def fake_read(name: str, *, placeholders=None):
        if name == "transmission_losses":
            return pl.DataFrame(
                {
                    "from_region": ["p1"],
                    "to_region": ["p2"],
                    "trtype": ["ac"],
                    "losses": [0.02],
                }
            ).lazy()
        return original_read(name, placeholders=placeholders)

    monkeypatch.setattr(parser.ctx.store, "read_data", fake_read)
    result = build_transmission(parser.ctx)
    assert result.is_ok()
    lines = list(system.get_components(ReEDSTransmissionLine))
    assert lines
    assert any(line.losses == pytest.approx(0.02) for line in lines)


def test_transmission_without_losses_still_builds_lines(
    initialized_parser: ReEDSParser,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Losses are optional and default to zero when absent."""
    from r2x_reeds.models.components import ReEDSTransmissionLine

    parser = initialized_parser
    system = System(name="transmission-no-losses-test")
    set_system(parser, system)
    original_read = parser.ctx.store.read_data

    def fake_read(name: str, *, placeholders=None):
        if name == "transmission_losses":
            return None
        return original_read(name, placeholders=placeholders)

    monkeypatch.setattr(parser.ctx.store, "read_data", fake_read)
    result = build_regions(parser.ctx)
    assert result.is_ok()
    result = build_transmission(parser.ctx)
    assert result.is_ok()
    lines = list(system.get_components(ReEDSTransmissionLine))
    assert lines
    assert all(line.losses in (None, 0.0) for line in lines)
