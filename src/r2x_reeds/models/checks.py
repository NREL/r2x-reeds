"""Pydantic validation callbacks for reusable ReEDS model types.

These functions are attached to annotated types in ``models.types`` through
``pydantic.AfterValidator``. Pydantic expects an after-validator to return the
validated field value when the invariant holds and to raise ``ValueError``
when it does not. They therefore intentionally return the original value
rather than wrapping it in another result type.

To add a validation, define one focused ``validate_*`` function here. Accept
the field value as its first argument and add ``ValidationInfo`` only when the
check needs values from earlier model fields. Return the unchanged field value
when the invariant holds and raise ``ValueError`` with a field-specific
message when it does not. Register the function in the appropriate annotated
type in ``models.types`` with ``AfterValidator``. Keep separate invariants in
separate functions and give each function a name that states exactly what it
checks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import ValidationInfo

if TYPE_CHECKING:
    from .planning import (
        ReEDSInitialCapacity,
        ReEDSPlanningPeriod,
        ReEDSPlantCharacteristics,
        ReEDSRepresentativeTimepoint,
        ReEDSStorageDuration,
        ReEDSStorageDurationOverride,
    )
    from .types import PlanningYear


def validate_planning_periods_are_ascending(
    planning_periods: tuple[ReEDSPlanningPeriod, ...],
) -> tuple[ReEDSPlanningPeriod, ...]:
    """Require planning periods to have unique ascending years."""
    years = tuple(period.year for period in planning_periods)
    if years != tuple(sorted(years)) or len(set(years)) != len(years):
        msg = "planning_periods must have unique ascending years."
        raise ValueError(msg)
    return planning_periods


def validate_representative_timepoint_labels_are_unique(
    representative_timepoints: tuple[ReEDSRepresentativeTimepoint, ...],
) -> tuple[ReEDSRepresentativeTimepoint, ...]:
    """Require representative timepoint labels to be unique."""
    labels = tuple(timepoint.label for timepoint in representative_timepoints)
    if len(set(labels)) != len(labels):
        msg = "representative_timepoints must have unique labels"
        raise ValueError(msg)
    return representative_timepoints


def validate_representative_timepoint_positions_are_contiguous_from_zero(
    representative_timepoints: tuple[ReEDSRepresentativeTimepoint, ...],
) -> tuple[ReEDSRepresentativeTimepoint, ...]:
    """Require representative timepoint positions to be contiguous and zero-based."""
    positions = tuple(timepoint.position for timepoint in representative_timepoints)
    if positions != tuple(range(len(positions))):
        msg = "representative_timepoints must have contiguous positions starting at zero"
        raise ValueError(msg)
    return representative_timepoints


def validate_emission_caps_require_emission_type(
    planning_periods: tuple[ReEDSPlanningPeriod, ...],
    info: ValidationInfo,
) -> tuple[ReEDSPlanningPeriod, ...]:
    """Require an emission type whenever planning periods contain emission caps."""
    if info.data.get("emission_type") is None and any(
        period.emission_cap is not None for period in planning_periods
    ):
        msg = "emission caps require emission_type"
        raise ValueError(msg)
    return planning_periods


def validate_active_emission_type_requires_a_cap_for_every_planning_period(
    planning_periods: tuple[ReEDSPlanningPeriod, ...],
    info: ValidationInfo,
) -> tuple[ReEDSPlanningPeriod, ...]:
    """Require an emission cap for every period when an emission type is active."""
    if info.data.get("emission_type") is not None and any(
        period.emission_cap is None for period in planning_periods
    ):
        msg = "active emission_type requires an emission cap for every planning period"
        raise ValueError(msg)
    return planning_periods


def validate_plant_characteristics_use_modeled_planning_years(
    plant_characteristics: tuple[ReEDSPlantCharacteristics, ...],
    info: ValidationInfo,
) -> tuple[ReEDSPlantCharacteristics, ...]:
    """Require plant characteristics to use a modeled planning year."""
    planning_periods = info.data.get("planning_periods")
    if planning_periods is None:
        return plant_characteristics

    planning_years = {period.year for period in planning_periods}
    if any(characteristics.year not in planning_years for characteristics in plant_characteristics):
        msg = "plant_characteristics must use a modeled planning year"
        raise ValueError(msg)
    return plant_characteristics


def validate_plant_characteristics_are_unique_by_technology_and_year(
    plant_characteristics: tuple[ReEDSPlantCharacteristics, ...],
) -> tuple[ReEDSPlantCharacteristics, ...]:
    """Require one plant-characteristics record per technology and year."""
    keys = tuple((characteristics.technology, characteristics.year) for characteristics in plant_characteristics)
    if len(set(keys)) != len(keys):
        msg = "plant_characteristics must be unique by technology and year"
        raise ValueError(msg)
    return plant_characteristics


def validate_initial_capacities_are_unique_by_technology_and_region(
    initial_capacities: tuple[ReEDSInitialCapacity, ...],
) -> tuple[ReEDSInitialCapacity, ...]:
    """Require one initial-capacity record per technology and region."""
    keys = tuple((capacity.technology, capacity.region) for capacity in initial_capacities)
    if len(set(keys)) != len(keys):
        msg = "initial_capacities must be unique by technology and region"
        raise ValueError(msg)
    return initial_capacities


def validate_positive_initial_energy_capacity_requires_positive_initial_power_capacity(
    initial_capacities: tuple[ReEDSInitialCapacity, ...],
) -> tuple[ReEDSInitialCapacity, ...]:
    """Require positive initial energy capacity to have positive initial power capacity."""
    if any(
        capacity.initial_energy_capacity is not None
        and capacity.initial_energy_capacity > 0
        and capacity.initial_power_capacity <= 0
        for capacity in initial_capacities
    ):
        msg = "initial_energy_capacity requires positive initial_power_capacity"
        raise ValueError(msg)
    return initial_capacities


def validate_storage_durations_are_unique_by_technology(
    storage_durations: tuple[ReEDSStorageDuration, ...],
) -> tuple[ReEDSStorageDuration, ...]:
    """Require one technology-level storage duration per technology."""
    technologies = tuple(storage_duration.technology for storage_duration in storage_durations)
    if len(set(technologies)) != len(technologies):
        msg = "storage_durations must be unique by technology"
        raise ValueError(msg)
    return storage_durations


def validate_storage_duration_overrides_are_unique_by_technology_vintage_and_region(
    storage_duration_overrides: tuple[ReEDSStorageDurationOverride, ...],
) -> tuple[ReEDSStorageDurationOverride, ...]:
    """Require one duration override per technology, vintage, and region."""
    keys = tuple(
        (override.technology, override.vintage, override.region)
        for override in storage_duration_overrides
    )
    if len(set(keys)) != len(keys):
        msg = "storage_duration_overrides must be unique by technology, vintage, and region"
        raise ValueError(msg)
    return storage_duration_overrides


def validate_available_years_are_unique_and_ascending(
    years: tuple[PlanningYear, ...],
) -> tuple[PlanningYear, ...]:
    """Require candidate availability years to be unique and ascending."""
    if years != tuple(sorted(years)) or len(set(years)) != len(years):
        msg = "available_years must be unique and ascending"
        raise ValueError(msg)
    return years


def validate_maximum_unit_float_is_not_less_than_minimum(
    maximum: float,
    info: ValidationInfo,
) -> float:
    """Require a unit-interval maximum to be at least its preceding minimum."""
    minimum = info.data.get("min")
    if minimum is not None and maximum < minimum:
        msg = "min must be <= max"
        raise ValueError(msg)
    return maximum


def validate_minimum_generation_fraction_does_not_exceed_capacity_factor(
    minimum_generation_fraction: float,
    info: ValidationInfo,
) -> float:
    """Require the hourly minimum generation fraction to be feasible."""
    capacity_factor = info.data.get("capacity_factor")
    if capacity_factor is not None and minimum_generation_fraction > capacity_factor:
        msg = "minimum_generation_fraction must not exceed capacity_factor"
        raise ValueError(msg)
    return minimum_generation_fraction


def validate_minimum_capacity_factor_does_not_exceed_capacity_factor(
    minimum_capacity_factor: float,
    info: ValidationInfo,
) -> float:
    """Require the annual minimum capacity factor to be feasible."""
    capacity_factor = info.data.get("capacity_factor")
    if capacity_factor is not None and minimum_capacity_factor > capacity_factor:
        msg = "minimum_capacity_factor must not exceed capacity_factor"
        raise ValueError(msg)
    return minimum_capacity_factor
