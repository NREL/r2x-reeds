"""Tests for hydro budget and rating profile methods."""

import pytest

from r2x_reeds.config import ReEDSConfig
from r2x_reeds.parser import ReEDSParser


@pytest.fixture
def defaults():
    """Load defaults.json for testing."""
    return ReEDSConfig.load_defaults()


def test_expand_monthly_to_daily(mocker, defaults):
    """Test monthly to daily expansion."""
    mock_config = mocker.Mock()
    mock_config.load_defaults.return_value = defaults
    mock_data_store = mocker.Mock()
    parser = ReEDSParser(mock_config, mock_data_store, skip_validation=True)

    monthly_values = [100.0] * 12
    daily_values = parser._expand_monthly_to_daily(monthly_values)

    assert len(daily_values) == 365


def test_expand_daily_to_hourly(mocker, defaults):
    """Test daily to hourly expansion."""
    mock_config = mocker.Mock()
    mock_config.load_defaults.return_value = defaults
    mock_data_store = mocker.Mock()
    parser = ReEDSParser(mock_config, mock_data_store, skip_validation=True)

    daily_values = [50.0] * 365
    hourly_values = parser._expand_daily_to_hourly(daily_values)

    assert len(hourly_values) == 365 * 24


def test_expand_monthly_to_hourly(mocker, defaults):
    """Test monthly to hourly expansion."""
    mock_config = mocker.Mock()
    mock_config.load_defaults.return_value = defaults
    mock_data_store = mocker.Mock()
    parser = ReEDSParser(mock_config, mock_data_store, skip_validation=True)

    monthly_values = [100.0] * 12
    hourly_values = parser._expand_monthly_to_hourly(monthly_values)

    assert len(hourly_values) == 8760


def test_expand_monthly_to_hourly_values(mocker, defaults):
    """Test monthly to hourly expansion preserves values correctly."""
    mock_config = mocker.Mock()
    mock_config.load_defaults.return_value = defaults
    mock_data_store = mocker.Mock()
    parser = ReEDSParser(mock_config, mock_data_store, skip_validation=True)

    monthly_values = [float(i) for i in range(1, 13)]
    hourly_values = parser._expand_monthly_to_hourly(monthly_values)

    month_hours = defaults["month_hours"]
    expected_hours = [month_hours[f"M{i}"] for i in range(1, 13)]

    start_idx = 0
    for month_idx, hours in enumerate(expected_hours):
        month_value = float(month_idx + 1)
        for hour_idx in range(hours):
            assert hourly_values[start_idx + hour_idx] == month_value
        start_idx += hours


def test_expand_monthly_to_daily_values(mocker, defaults):
    """Test monthly to daily expansion preserves values correctly."""
    mock_config = mocker.Mock()
    mock_config.load_defaults.return_value = defaults
    mock_data_store = mocker.Mock()
    parser = ReEDSParser(mock_config, mock_data_store, skip_validation=True)

    monthly_values = [100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0, 800.0, 900.0, 1000.0, 1100.0, 1200.0]
    daily_values = parser._expand_monthly_to_daily(monthly_values)

    month_days = defaults["month_days"]
    days_per_month = [month_days[f"M{i}"] for i in range(1, 13)]

    start_idx = 0
    for month_idx, days in enumerate(days_per_month):
        month_value = monthly_values[month_idx]
        for day_idx in range(days):
            assert daily_values[start_idx + day_idx] == month_value
        start_idx += days


def test_expand_daily_to_hourly_values(mocker, defaults):
    """Test daily to hourly expansion preserves values correctly."""
    mock_config = mocker.Mock()
    mock_config.load_defaults.return_value = defaults
    mock_data_store = mocker.Mock()
    parser = ReEDSParser(mock_config, mock_data_store, skip_validation=True)

    daily_values = [float(i) for i in range(1, 366)]
    hourly_values = parser._expand_daily_to_hourly(daily_values)

    for day_idx, day_value in enumerate(daily_values):
        start_hour = day_idx * 24
        for hour_offset in range(24):
            assert hourly_values[start_hour + hour_offset] == day_value
