"""Small adapters between ReEDS parser modules and r2x-core context."""

from __future__ import annotations

from typing import Any

from r2x_core import PluginContext

from .core import DEFAULTS


def read_data_file(context: PluginContext, name: str) -> Any:
    """Read a mapped dataset through the context DataStore."""
    if context.store is None:
        raise ValueError("Parser context has no DataStore")
    return context.store.read_data(
        name,
        placeholders={
            "solve_year": context.config.solve_year,
            "weather_year": context.config.weather_year,
        },
    )


def collect_data_file(context: PluginContext, name: str) -> Any:
    """Read and collect a mapped dataset."""
    data = read_data_file(context, name)
    if data is None:
        return None
    return data.collect() if hasattr(data, "collect") else data


def parser_defaults(context: PluginContext) -> dict[str, Any]:
    """Return parser defaults stored in PluginContext metadata."""
    defaults = context.metadata.get(DEFAULTS)
    if not isinstance(defaults, dict):
        raise ValueError("Parser defaults are not loaded")
    return defaults


def time_periods_per_year(context: PluginContext) -> int:
    """Resolve the configured number of hourly values retained per year."""
    configured = context.config.time_periods_per_year
    if configured is not None:
        return configured
    value = parser_defaults(context).get("default_values", {}).get("time_periods_per_year")
    if not isinstance(value, int) or value < 1:
        raise ValueError("default_values.time_periods_per_year must be a positive integer")
    return value
