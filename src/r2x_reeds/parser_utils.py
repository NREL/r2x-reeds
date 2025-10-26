"""Utilities for the parser."""

import calendar
from typing import TYPE_CHECKING, Any

import numpy as np

from r2x_core import Err, Ok, Result

if TYPE_CHECKING:
    pass


def tech_matches_category(tech: str, category_name: str, tech_categories: dict[str, Any]) -> bool:
    """Check if a technology matches a category using prefix or exact matching.

    Parameters
    ----------
    tech : str
        Technology name to check
    category_name : str
        Category name from tech_categories
    defaults : dict
        Defaults dictionary containing tech_categories

    Returns
    -------
    bool
        True if technology matches the category
    """
    if category_name not in tech_categories:
        return False

    category = tech_categories[category_name]

    if isinstance(category, list):
        return tech in category

    prefixes = category.get("prefixes", [])
    exact = category.get("exact", [])

    if tech in exact:
        return True

    return any(tech.startswith(prefix) for prefix in prefixes)


def get_technology_category(
    technology_name: str, technology_categories: dict[str, Any]
) -> Result[str, KeyError]:
    """Get the category for a technology.

    Parameters
    ----------
    tech : str
        Technology name
    defaults : dict
        Defaults dictionary containing tech_categories

    Returns
    -------
    Result[str, ParserError]
            ``Ok(category_name)`` if technology is found, or ``Err(KeyError(...)`` if not found.
    """
    for category_name in technology_categories:
        category_name_str: str = str(category_name)
        if tech_matches_category(technology_name, category_name_str, technology_categories):
            return Ok(category_name_str)
    return Err(KeyError("Technology {technology_name} does not have category match."))


def monthly_to_hourly_polars(year: int, monthly_profile: list[float]) -> Result[np.ndarray, ValueError]:
    """Convert a 12-element monthly profile into an hourly profile for the given year"""
    if len(monthly_profile) != 12:
        raise ValueError("monthly_profile must have 12 elements")

    hours_per_month = np.array([calendar.monthrange(year, m)[1] * 24 for m in range(1, 13)])
    hourly_profile = np.repeat(monthly_profile, hours_per_month)

    return Ok(hourly_profile)
