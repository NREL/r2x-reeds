"""Basic ReEDS parser tests using r2x-core 0.0.5b3 API.

These tests verify basic parser instantiation and configuration using
a minimal test data set.
"""

from pathlib import Path
from unittest.mock import Mock

import pytest

from r2x_core.store import DataStore
from r2x_reeds.config import ReEDSConfig
from r2x_reeds.parser import ReEDSParser


@pytest.fixture
def reeds_config() -> ReEDSConfig:
    """Create basic ReEDS configuration."""
    return ReEDSConfig(solve_years=2030, weather_years=2012)


@pytest.fixture
def mock_data_store() -> Mock:
    """Create a mock DataStore for testing parser instantiation."""
    mock_store = Mock(spec=DataStore)
    mock_store.folder = Path("/fake/path")
    return mock_store


def test_config_get_file_mapping_path():
    """Test ReEDSConfig.get_file_mapping_path() returns valid path."""
    path = ReEDSConfig.get_file_mapping_path()

    assert path.exists()
    assert path.name == "file_mapping.json"
    assert "r2x_reeds" in str(path)


def test_parser_creation_with_mock(reeds_config: ReEDSConfig, mock_data_store: Mock):
    """Test creating ReEDSParser instance with mock data store."""
    parser = ReEDSParser(config=reeds_config, data_store=mock_data_store, name="test_system")

    assert parser is not None
    assert parser.name == "test_system"


def test_parser_has_config(reeds_config: ReEDSConfig, mock_data_store: Mock):
    """Test parser stores config."""
    parser = ReEDSParser(config=reeds_config, data_store=mock_data_store, name="test_system")

    assert parser.config == reeds_config
    assert parser.config.solve_years == [2030]


def test_parser_has_data_store(reeds_config: ReEDSConfig, mock_data_store: Mock):
    """Test parser stores data_store."""
    parser = ReEDSParser(config=reeds_config, data_store=mock_data_store, name="test_system")

    assert parser.data_store == mock_data_store


def test_parser_system_initially_none(reeds_config: ReEDSConfig, mock_data_store: Mock):
    """Test parser.system is None before build_system() is called."""
    parser = ReEDSParser(config=reeds_config, data_store=mock_data_store, name="test_system")

    assert parser.system is None
