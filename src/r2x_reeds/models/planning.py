"""Capacity-expansion planning models for ReEDS systems."""

from __future__ import annotations

from typing import Annotated

from infrasys.models import InfraSysBaseModel
from pydantic import Field, model_validator

from r2x_core.units import HasUnits, Unit

from .base import ReEDSComponent
from .components import ReEDSRegion
from .enums import EmissionType
from .units import PlanningYear


class ReEDSPlanningPeriod(InfraSysBaseModel):
    """One chronologically ordered ReEDS capacity-expansion period."""

    year: PlanningYear
    present_value_factor: Annotated[
        float,
        Field(gt=0, description="Present-value factor for overnight capital costs"),
    ]
    emission_cap: (
        Annotated[
            float,
            Unit("tonne"),
            Field(ge=0),
        ]
        | None
    ) = Field(default=None, description="Annual emissions cap in tonnes; None means no cap is imposed")


class ReEDSRepresentativeTimepoint(InfraSysBaseModel):
    """One ordered representative timepoint and its calendar-hour weight."""

    label: Annotated[str, Field(min_length=1, description="ReEDS timepoint identifier")]
    position: Annotated[
        int,
        Field(ge=0, description="Zero-based chronological position in the representative sequence"),
    ]
    weight: Annotated[
        float,
        Unit("hours"),
        Field(gt=0, description="Calendar hours represented by this timepoint"),
    ]


def _validate_chronology(
    planning_periods: tuple[ReEDSPlanningPeriod, ...],
    representative_timepoints: tuple[ReEDSRepresentativeTimepoint, ...],
) -> None:
    """Validate the common chronology used by planning components and input data."""
    years = tuple(period.year for period in planning_periods)
    if years != tuple(sorted(years)) or len(set(years)) != len(years):
        raise ValueError("planning_periods must have unique ascending years")

    labels = tuple(timepoint.label for timepoint in representative_timepoints)
    if len(set(labels)) != len(labels):
        raise ValueError("representative_timepoints must have unique labels")

    positions = tuple(timepoint.position for timepoint in representative_timepoints)
    if positions != tuple(range(len(positions))):
        raise ValueError("representative_timepoints must have contiguous positions starting at zero")


class ReEDSPlantCharacteristics(InfraSysBaseModel):
    """Canonical ``plantcharout.csv`` characteristics for one technology and year.

    ReEDS uses zero as a placeholder for unavailable heat-rate and round-trip
    efficiency values. Input readers represent those placeholders as ``None``.
    """

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    year: PlanningYear
    capital_cost: Annotated[
        float,
        Unit("$/MW"),
        Field(ge=0, description="Overnight power-capacity capital cost"),
    ]
    capital_cost_energy: Annotated[
        float,
        Unit("$/MWh"),
        Field(ge=0, description="Overnight energy-capacity capital cost"),
    ]
    fom_cost: Annotated[
        float,
        Unit("$/MW/year"),
        Field(ge=0, description="Fixed power-capacity operation and maintenance cost"),
    ]
    fom_cost_energy: Annotated[
        float,
        Unit("$/MWh/year"),
        Field(ge=0, description="Fixed energy-capacity operation and maintenance cost"),
    ]
    vom_cost: Annotated[
        float,
        Unit("$/MWh"),
        Field(ge=0, description="Variable operation and maintenance cost"),
    ]
    heat_rate: (
        Annotated[
            float,
            Unit("MMBtu/MWh"),
            Field(gt=0),
        ]
        | None
    ) = Field(default=None, description="Thermal heat rate; None for non-combustion technologies")
    round_trip_efficiency: (
        Annotated[
            float,
            Field(gt=0, le=1),
        ]
        | None
    ) = Field(default=None, description="Storage round-trip efficiency when defined")
    upgrade_cost: (
        Annotated[
            float,
            Unit("$/MW"),
            Field(ge=0),
        ]
        | None
    ) = Field(default=None, description="Technology upgrade capital cost when defined")


class ReEDSInitialCapacity(InfraSysBaseModel):
    """Initial non-resource capacity from ``capnonrsc.csv`` and ``capnonrsc_energy.csv``."""

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    region: Annotated[str, Field(min_length=1, description="ReEDS region identifier")]
    initial_power_capacity: Annotated[
        float,
        Unit("MW"),
        Field(ge=0, description="Initial power capacity before retirements"),
    ]
    initial_energy_capacity: (
        Annotated[
            float,
            Unit("MWh"),
            Field(ge=0),
        ]
        | None
    ) = Field(default=None, description="Initial storage energy capacity when defined")


class ReEDSStorageDuration(InfraSysBaseModel):
    """Technology-level storage-duration default from ``storage_duration.csv``."""

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    duration: Annotated[
        float,
        Unit("hours"),
        Field(gt=0, description="Fixed energy-to-power duration"),
    ]


class ReEDSStorageDurationOverride(InfraSysBaseModel):
    """An enabled regional and vintage-specific duration from ``storage_duration_pshdata.csv``."""

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    vintage: Annotated[str, Field(min_length=1, description="ReEDS capacity vintage identifier")]
    region: Annotated[str, Field(min_length=1, description="ReEDS region identifier")]
    duration: Annotated[
        float,
        Unit("hours"),
        Field(gt=0, description="Asset-specific energy-to-power duration"),
    ]


class ReEDSCapacityExpansionInputs(InfraSysBaseModel):
    """Validated source data for a ReEDS capacity-expansion formulation.

    This model represents canonical ReEDS input tables, not region-specific
    investment candidates. Source tables define technology-year characteristics
    and non-resource initial capacity, but not a complete candidate-feasibility
    relation.
    """

    planning_periods: Annotated[
        tuple[ReEDSPlanningPeriod, ...],
        Field(min_length=1, description="Chronologically ordered modeled years"),
    ]
    representative_timepoints: Annotated[
        tuple[ReEDSRepresentativeTimepoint, ...],
        Field(min_length=1, description="Chronologically ordered representative timepoints"),
    ]
    emission_type: Annotated[
        EmissionType | None,
        Field(description="Active annual-emissions constraint type, if enabled"),
    ] = None
    plant_characteristics: Annotated[
        tuple[ReEDSPlantCharacteristics, ...],
        Field(min_length=1, description="Technology-year plant characteristics"),
    ]
    initial_capacities: Annotated[
        tuple[ReEDSInitialCapacity, ...],
        Field(description="Initial non-resource regional power and energy capacities"),
    ] = ()
    storage_durations: Annotated[
        tuple[ReEDSStorageDuration, ...],
        Field(description="Technology-level storage-duration defaults"),
    ] = ()
    pumped_storage_supply_curve_duration: (
        Annotated[
            float,
            Unit("hours"),
            Field(gt=0),
        ]
        | None
    ) = Field(
        default=None,
        description="Selected supply-curve duration that overrides pumped-storage defaults",
    )
    storage_duration_overrides: Annotated[
        tuple[ReEDSStorageDurationOverride, ...],
        Field(description="Enabled regional and vintage-specific storage-duration overrides"),
    ] = ()

    @model_validator(mode="after")
    def validate_input_references(self) -> ReEDSCapacityExpansionInputs:
        """Require unique source keys and characteristics for modeled years only."""
        _validate_chronology(self.planning_periods, self.representative_timepoints)
        emission_caps = tuple(period.emission_cap for period in self.planning_periods)
        if self.emission_type is None and any(cap is not None for cap in emission_caps):
            raise ValueError("emission caps require emission_type")
        if self.emission_type is not None and any(cap is None for cap in emission_caps):
            raise ValueError("active emission_type requires an emission cap for every planning period")

        planning_years = {period.year for period in self.planning_periods}
        characteristic_keys: set[tuple[str, int]] = set()
        for characteristics in self.plant_characteristics:
            key = (characteristics.technology, characteristics.year)
            if characteristics.year not in planning_years:
                raise ValueError("plant_characteristics must use a modeled planning year")
            if key in characteristic_keys:
                raise ValueError("plant_characteristics must be unique by technology and year")
            characteristic_keys.add(key)

        capacity_keys: set[tuple[str, str]] = set()
        for capacity in self.initial_capacities:
            key = (capacity.technology, capacity.region)
            if key in capacity_keys:
                raise ValueError("initial_capacities must be unique by technology and region")
            if (
                capacity.initial_energy_capacity is not None
                and capacity.initial_energy_capacity > 0
                and capacity.initial_power_capacity == 0
            ):
                raise ValueError("initial_energy_capacity requires positive initial_power_capacity")
            capacity_keys.add(key)

        duration_technologies: set[str] = set()
        for storage_duration in self.storage_durations:
            if storage_duration.technology in duration_technologies:
                raise ValueError("storage_durations must be unique by technology")
            duration_technologies.add(storage_duration.technology)

        override_keys: set[tuple[str, str, str]] = set()
        for override in self.storage_duration_overrides:
            key = (override.technology, override.vintage, override.region)
            if key in override_keys:
                raise ValueError(
                    "storage_duration_overrides must be unique by technology, vintage, and region"
                )
            override_keys.add(key)
        return self


class ReEDSCapacityExpansion(HasUnits, ReEDSComponent):
    """Capacity-expansion chronology and policy shared by system resources."""

    planning_periods: Annotated[
        tuple[ReEDSPlanningPeriod, ...],
        Field(min_length=1, description="Chronologically ordered planning periods"),
    ]
    representative_timepoints: Annotated[
        tuple[ReEDSRepresentativeTimepoint, ...],
        Field(min_length=1, description="Chronologically ordered representative timepoints"),
    ]
    reserve_margin: Annotated[
        float,
        Field(ge=0, le=1, description="Planning reserve margin as a fraction of peak demand"),
    ]
    emission_type: Annotated[
        EmissionType,
        Field(description="Emission type constrained by planning-period caps"),
    ]

    @model_validator(mode="after")
    def validate_chronology(self) -> ReEDSCapacityExpansion:
        """Require an unambiguous chronology and a cap for the active policy."""
        _validate_chronology(self.planning_periods, self.representative_timepoints)
        if any(period.emission_cap is None for period in self.planning_periods):
            raise ValueError("active emission_type requires an emission cap for every planning period")
        return self


class ReEDSCapacityExpansionResource(HasUnits, ReEDSComponent):
    """Abstract base class for investable capacity-expansion resource variants.

    Use a dispatchable, variable, or storage subtype; this base class cannot be instantiated.
    """

    region: Annotated[ReEDSRegion, Field(description="ReEDS region hosting the candidate")]
    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    available_years: Annotated[
        tuple[PlanningYear, ...],
        Field(min_length=1, description="Ordered planning years in which the candidate is active"),
    ]
    initial_capacity: Annotated[
        float,
        Unit("MW"),
        Field(ge=0, description="Installed capacity before capacity-expansion investment"),
    ]
    investment_cost: Annotated[
        float,
        Unit("$/MW"),
        Field(ge=0, description="Capacity investment cost"),
    ]
    variable_cost: Annotated[
        float,
        Unit("$/MWh"),
        Field(ge=0, description="Variable operating cost"),
    ]

    @model_validator(mode="after")
    def validate_available_years(self) -> ReEDSCapacityExpansionResource:
        """Require a concrete resource type and ordered candidate availability."""
        if type(self) is ReEDSCapacityExpansionResource:
            raise ValueError("resources must use a concrete operational subtype")
        if self.available_years != tuple(sorted(set(self.available_years))):
            raise ValueError("available_years must be unique and ascending")
        return self


class ReEDSDispatchableCapacityExpansionResource(ReEDSCapacityExpansionResource):
    """An investable dispatchable resource with constant hourly availability."""

    capacity_factor: Annotated[
        float,
        Field(ge=0, le=1, description="Constant maximum generation fraction"),
    ]
    minimum_generation_fraction: Annotated[
        float,
        Field(ge=0, le=1, description="Minimum hourly generation fraction"),
    ] = 0.0
    minimum_capacity_factor: Annotated[
        float,
        Field(ge=0, le=1, description="Minimum annual capacity factor"),
    ] = 0.0
    ramp_up_cost: Annotated[
        float,
        Unit("$/MW-ramp"),
        Field(ge=0, description="Cost applied to positive generation ramps"),
    ] = 0.0

    @model_validator(mode="after")
    def validate_capacity_factor_bounds(self) -> ReEDSDispatchableCapacityExpansionResource:
        """Require operating minimums to be feasible at the maximum availability."""
        if self.minimum_generation_fraction > self.capacity_factor:
            raise ValueError("minimum_generation_fraction must not exceed capacity_factor")
        if self.minimum_capacity_factor > self.capacity_factor:
            raise ValueError("minimum_capacity_factor must not exceed capacity_factor")
        return self


class ReEDSVariableCapacityExpansionResource(ReEDSCapacityExpansionResource):
    """An investable variable resource with a capacity-factor time series."""


class ReEDSStorageCapacityExpansionResource(ReEDSCapacityExpansionResource):
    """An investable fixed-duration storage resource."""

    round_trip_efficiency: Annotated[
        float,
        Field(gt=0, le=1, description="Round-trip storage efficiency"),
    ]
    storage_duration: Annotated[
        float,
        Unit("hours"),
        Field(gt=0, description="Energy capacity duration at rated power"),
    ]
