"""Public ReEDS parser context keys and package-level constants.

The parser stores prepared values in ``PluginContext.metadata`` because
``PluginContext`` is the r2x-core execution container. These constants keep
those keys centralized and prevent unrelated parser modules from inventing
string literals for shared state.
"""

from __future__ import annotations

CONFIG_ASSETS = "r2x_reeds.config_assets"
DEFAULTS = "r2x_reeds.defaults"
TECH_CATEGORIES = "r2x_reeds.tech_categories"
SOLVE_YEARS = "r2x_reeds.solve_years"
WEATHER_YEARS = "r2x_reeds.weather_years"
HOURLY_TIME_INDEX = "r2x_reeds.hourly_time_index"
DAILY_TIME_INDEX = "r2x_reeds.daily_time_index"
INITIAL_TIMESTAMP = "r2x_reeds.initial_timestamp"
MONTH_MAP = "r2x_reeds.month_map"
YEAR_MONTH_DAY_HOURS = "r2x_reeds.year_month_day_hours"
GENERATOR_DATA = "r2x_reeds.generator_data"
HYDRO_CF = "r2x_reeds.hydro_cf"
RESERVE_PERCENTAGES = "r2x_reeds.reserve_percentages"
UPGRADES_COMPLETE = "r2x_reeds.upgrades_complete"

__all__ = [
    "CONFIG_ASSETS",
    "DAILY_TIME_INDEX",
    "DEFAULTS",
    "GENERATOR_DATA",
    "HOURLY_TIME_INDEX",
    "HYDRO_CF",
    "INITIAL_TIMESTAMP",
    "MONTH_MAP",
    "RESERVE_PERCENTAGES",
    "SOLVE_YEARS",
    "TECH_CATEGORIES",
    "UPGRADES_COMPLETE",
    "WEATHER_YEARS",
    "YEAR_MONTH_DAY_HOURS",
]
