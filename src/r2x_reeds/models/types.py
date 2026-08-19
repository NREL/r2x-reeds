"""Reusable validated types for ReEDS domain models."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, TypeAlias

from pydantic import AfterValidator, Field

from r2x_core.units import Unit

from .checks import (
    validate_active_emission_type_requires_a_cap_for_every_planning_period,
    validate_available_years_are_unique_and_ascending,
    validate_emission_caps_require_emission_type,
    validate_initial_capacities_are_unique_by_technology_and_region,
    validate_minimum_capacity_factor_does_not_exceed_capacity_factor,
    validate_minimum_generation_fraction_does_not_exceed_capacity_factor,
    validate_planning_periods_are_ascending,
    validate_plant_characteristics_are_unique_by_technology_and_year,
    validate_plant_characteristics_use_modeled_planning_years,
    validate_positive_initial_energy_capacity_requires_positive_initial_power_capacity,
    validate_representative_timepoint_labels_are_unique,
    validate_representative_timepoint_positions_are_contiguous_from_zero,
    validate_storage_duration_overrides_are_unique_by_technology_vintage_and_region,
    validate_storage_durations_are_unique_by_technology,
)

if TYPE_CHECKING:
    from .planning import (
        ReEDSInitialCapacity,
        ReEDSPlanningPeriod,
        ReEDSPlantCharacteristics,
        ReEDSRepresentativeTimepoint,
        ReEDSStorageDuration,
        ReEDSStorageDurationOverride,
    )

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

PlanningPeriods: TypeAlias = Annotated[
    tuple["ReEDSPlanningPeriod", ...],
    AfterValidator(validate_planning_periods_are_ascending),
    AfterValidator(validate_emission_caps_require_emission_type),
    AfterValidator(validate_active_emission_type_requires_a_cap_for_every_planning_period),
]
RepresentativeTimepoints: TypeAlias = Annotated[
    tuple["ReEDSRepresentativeTimepoint", ...],
    AfterValidator(validate_representative_timepoint_labels_are_unique),
    AfterValidator(validate_representative_timepoint_positions_are_contiguous_from_zero),
]
AvailableYears: TypeAlias = Annotated[
    tuple[PlanningYear, ...],
    AfterValidator(validate_available_years_are_unique_and_ascending),
]
PlantCharacteristics: TypeAlias = Annotated[
    tuple["ReEDSPlantCharacteristics", ...],
    AfterValidator(validate_plant_characteristics_use_modeled_planning_years),
    AfterValidator(validate_plant_characteristics_are_unique_by_technology_and_year),
]
InitialCapacities: TypeAlias = Annotated[
    tuple["ReEDSInitialCapacity", ...],
    AfterValidator(validate_initial_capacities_are_unique_by_technology_and_region),
    AfterValidator(validate_positive_initial_energy_capacity_requires_positive_initial_power_capacity),
]
StorageDurations: TypeAlias = Annotated[
    tuple["ReEDSStorageDuration", ...],
    AfterValidator(validate_storage_durations_are_unique_by_technology),
]
StorageDurationOverrides: TypeAlias = Annotated[
    tuple["ReEDSStorageDurationOverride", ...],
    AfterValidator(validate_storage_duration_overrides_are_unique_by_technology_vintage_and_region),
]
MinimumGenerationFraction: TypeAlias = Annotated[
    Fraction,
    AfterValidator(validate_minimum_generation_fraction_does_not_exceed_capacity_factor),
]
MinimumCapacityFactor: TypeAlias = Annotated[
    Fraction,
    AfterValidator(validate_minimum_capacity_factor_does_not_exceed_capacity_factor),
]
