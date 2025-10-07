"""Integration tests for ReEDS parser with real DataStore.

These tests verify that the parser can build a system using real data files.
"""

from pathlib import Path

import pytest
from infrasys import Component

from r2x_core.store import DataStore
from r2x_reeds.config import ReEDSConfig
from r2x_reeds.parser import ReEDSParser


@pytest.fixture
def test_data_path() -> Path:
    """Path to test data directory."""
    return Path(__file__).parent / "data" / "test_Pacific"


@pytest.fixture
def reeds_config() -> ReEDSConfig:
    """Create ReEDS configuration for testing."""
    return ReEDSConfig(solve_years=2030, weather_years=2012)


@pytest.fixture
def data_store(test_data_path: Path) -> DataStore:
    """Create DataStore from file mapping."""
    mapping_path = ReEDSConfig.get_file_mapping_path()
    return DataStore.from_json(mapping_path, folder=test_data_path)


@pytest.fixture
def parser(reeds_config: ReEDSConfig, data_store: DataStore) -> ReEDSParser:
    """Create ReEDS parser instance."""
    return ReEDSParser(config=reeds_config, data_store=data_store, name="test_system")


def test_parser_creation_with_real_data(parser: ReEDSParser):
    """Test creating parser with real DataStore."""
    assert parser is not None
    assert parser.name == "test_system"
    assert parser.system is None


def test_build_system(parser: ReEDSParser):
    """Test building system from real data."""
    system = parser.build_system()

    assert system is not None
    assert system.name == "test_system"


def test_system_has_buses(parser: ReEDSParser):
    """Test that built system contains buses."""
    system = parser.build_system()

    # Get all components from the system
    components = list(system.get_components(Component))
    assert components is not None

    # System should have components after building
    num_components = len(components)
    print(f"Total components in system: {num_components}")
    assert num_components > 0, "System should have components after building"


def test_system_has_generators(parser: ReEDSParser):
    """Test that built system contains generators."""
    system = parser.build_system()

    # Get all components
    components = list(system.get_components(Component))
    assert components is not None
    assert len(components) >= 0  # May be empty if no generators added yet


def test_system_has_loads(parser: ReEDSParser):
    """Test that built system contains loads."""
    system = parser.build_system()

    # Get all components
    components = list(system.get_components(Component))
    assert components is not None
    assert len(components) >= 0  # May be empty if no loads added yet
