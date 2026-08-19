"""Reusable annotated types and general validations for ReEDS model fields.

This module is the catalog of field types and general invariants that can
apply to multiple models. The aliases attach callbacks from ``checks`` to
model fields with ``pydantic.AfterValidator``. Each callback returns the same
field type on success and raises ``ValueError`` on failure, which is the
contract required by Pydantic after-validators.

Add a type here when its constraints or validation describe a reusable field
concept rather than one model's private relationship. Add the corresponding
focused callback in ``models.checks`` and attach it with ``AfterValidator``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, TypeAlias

from pydantic import AfterValidator, Field

from .checks import (
    validate_active_emission_type_requires_a_cap_for_every_planning_period,
    validate_available_years_are_unique_and_ascending,
    validate_emission_caps_require_emission_type,
    validate_initial_capacities_are_unique_by_technology_and_region,
    validate_maximum_unit_float_is_not_less_than_minimum,
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

NonNegativeFloat: TypeAlias = Annotated[float, Field(ge=0)]
EmissionRate = Annotated[NonNegativeFloat, Field(description="Emission rate in kg/MWh")]
Percentage = Annotated[float, Field(description="Percentage value (0-100)", ge=0, le=100)]
PlanningYear: TypeAlias = Annotated[
    int,
    Field(ge=1, description="ReEDS solve or resource-availability year"),
]
UnitFloat: TypeAlias = Annotated[float, Field(ge=0, le=1)]
PositiveUnitFloat: TypeAlias = Annotated[float, Field(gt=0, le=1)]
MaximumUnitFloat: TypeAlias = Annotated[
    UnitFloat,
    AfterValidator(validate_maximum_unit_float_is_not_less_than_minimum),
]

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
    UnitFloat,
    AfterValidator(validate_minimum_generation_fraction_does_not_exceed_capacity_factor),
]
MinimumCapacityFactor: TypeAlias = Annotated[
    UnitFloat,
    AfterValidator(validate_minimum_capacity_factor_does_not_exceed_capacity_factor),
]
