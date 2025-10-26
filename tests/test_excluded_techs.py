"""Tests for excluded_techs functionality."""

from pathlib import Path

import pytest

from r2x_core.store import DataStore
from r2x_reeds.config import ReEDSConfig
from r2x_reeds.models.components import ReEDSGenerator
from r2x_reeds.parser import ReEDSParser


@pytest.fixture
def data_folder():
    """Return path to test data folder."""
    return Path(__file__).parent / "data" / "test_Pacific"


def test_excluded_techs_empty_list_default(data_folder):
    """Test that default excluded_techs includes can-imports and electrolyzer."""
    config = ReEDSConfig(
        solve_year=[2032],
        weather_year=[2012],
        scenario="test",
        case_name="test",
    )

    defaults = config.load_defaults()
    assert defaults.get("excluded_techs") == ["can-imports", "electrolyzer"]

    data_store = DataStore.from_json(config.file_mapping_path, folder_path=data_folder)
    parser = ReEDSParser(config, data_store=data_store)
    system = parser.build_system()
    generators = list(system.get_components(ReEDSGenerator))

    assert len(generators) > 0
