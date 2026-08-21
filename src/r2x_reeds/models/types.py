"""Reusable validated types for ReEDS domain models."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, TypeAlias

from pydantic import AfterValidator, Field

from r2x_core.units import Unit
from r2x_reeds.checks import (
    validate_available_years_are_unique_and_ascending,
    validate_minimum_capacity_factor_does_not_exceed_capacity_factor,
    validate_minimum_generation_fraction_does_not_exceed_capacity_factor,
)

if TYPE_CHECKING:
    pass

NonEmptyText: TypeAlias = Annotated[str, Field(min_length=1)]
NonNegativeFloat: TypeAlias = Annotated[float, Field(ge=0.0)]
Fraction: TypeAlias = Annotated[float, Unit("fraction"), Field(ge=0.0, le=1.0)]
PositiveFraction: TypeAlias = Annotated[float, Unit("fraction"), Field(gt=0.0, le=1.0)]
Percentage: TypeAlias = Annotated[float, Unit("%"), Field(ge=0.0, le=100.0)]
EmissionRate: TypeAlias = Annotated[float, Unit("kg/MWh")]
PlanningYear: TypeAlias = Annotated[
    int,
    Field(ge=1, description="ReEDS solve or resource-availability year"),
]

# These names remain the package's established vocabulary for fractional values.
UnitFloat: TypeAlias = Fraction
PositiveUnitFloat: TypeAlias = PositiveFraction

AvailableYears: TypeAlias = Annotated[
    tuple[PlanningYear, ...],
    AfterValidator(validate_available_years_are_unique_and_ascending),
]

MinimumGenerationFraction: TypeAlias = Annotated[
    Fraction,
    AfterValidator(validate_minimum_generation_fraction_does_not_exceed_capacity_factor),
]
MinimumCapacityFactor: TypeAlias = Annotated[
    Fraction,
    AfterValidator(validate_minimum_capacity_factor_does_not_exceed_capacity_factor),
]
