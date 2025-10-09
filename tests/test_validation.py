"""Tests for input validation."""

from pathlib import Path

import pytest

from r2x_core.store import DataStore
from r2x_reeds.config import ReEDSConfig
from r2x_reeds.parser import ReEDSParser


@pytest.fixture
def data_folder():
    """Return path to test data folder."""
    return Path(__file__).parent / "data" / "test_Pacific"


@pytest.fixture
def file_mapping_path():
    """Return path to file mapping."""
    return ReEDSConfig.get_file_mapping_path()


def test_invalid_solve_year_raises_error(data_folder, file_mapping_path):
    """Test that an invalid solve year raises a ValueError."""
    config = ReEDSConfig(
        solve_years=[2050],
        weather_years=[2012],
        scenario="test",
        case_name="test",
    )

    data_store = DataStore.from_json(file_mapping_path, folder=data_folder)
    parser = ReEDSParser(config, data_store, name="test_invalid_solve")

    with pytest.raises(ValueError, match=r"Solve year 2050 not found in modeledyears\.csv"):
        parser.validate_inputs()


def test_invalid_weather_year_raises_error(data_folder, file_mapping_path):
    """Test that an invalid weather year raises a ValueError."""
    config = ReEDSConfig(
        solve_years=[2032],
        weather_years=[2050],
        scenario="test",
        case_name="test",
    )

    data_store = DataStore.from_json(file_mapping_path, folder=data_folder)
    parser = ReEDSParser(config, data_store, name="test_invalid_weather")

    with pytest.raises(ValueError, match=r"Weather year 2050 not found in hmap_allyrs\.csv"):
        parser.validate_inputs()


def test_valid_years_pass_validation(data_folder, file_mapping_path):
    """Test that valid years pass validation without errors."""
    config = ReEDSConfig(
        solve_years=[2032],
        weather_years=[2012],
        scenario="test",
        case_name="test",
    )

    data_store = DataStore.from_json(file_mapping_path, folder=data_folder)
    parser = ReEDSParser(config, data_store, name="test_valid_years")

    parser.validate_inputs()
