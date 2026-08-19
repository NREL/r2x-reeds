"""Capacity-expansion planning models for ReEDS systems."""

from __future__ import annotations

from typing import Annotated, Any

from infrasys.models import InfraSysBaseModel
from pydantic import Field, PositiveFloat, model_validator

from r2x_core.units import HasUnits, Unit

from .base import ReEDSComponent
from .components import ReEDSRegion
from .enums import EmissionType
from .types import (
    AvailableYears,
    InitialCapacities,
    MinimumCapacityFactor,
    MinimumGenerationFraction,
    NonNegativeFloat,
    PlanningPeriods,
    PlanningYear,
    PlantCharacteristics,
    PositiveUnitFloat,
    RepresentativeTimepoints,
    StorageDurationOverrides,
    StorageDurations,
    UnitFloat,
)


class ReEDSPlanningPeriod(InfraSysBaseModel):
    """One chronologically ordered ReEDS capacity-expansion period."""

    year: PlanningYear
    present_value_factor: Annotated[
        PositiveFloat,
        Field(description="Present-value factor for overnight capital costs"),
    ]
    emission_cap: (
        Annotated[
            NonNegativeFloat,
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
        PositiveFloat,
        Unit("hours"),
        Field(gt=0, description="Calendar hours represented by this timepoint"),
    ]


class ReEDSPlantCharacteristics(InfraSysBaseModel):
    """Canonical ``plantcharout.csv`` characteristics for one technology and year.

    ReEDS uses zero as a placeholder for unavailable heat-rate and round-trip
    efficiency values. Input readers represent those placeholders as ``None``.
    """

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    year: PlanningYear
    capital_cost: Annotated[
        NonNegativeFloat,
        Unit("$/MW"),
        Field(ge=0, description="Overnight power-capacity capital cost"),
    ]
    capital_cost_energy: Annotated[
        NonNegativeFloat,
        Unit("$/MWh"),
        Field(ge=0, description="Overnight energy-capacity capital cost"),
    ]
    fom_cost: Annotated[
        NonNegativeFloat,
        Unit("$/MW/year"),
        Field(ge=0, description="Fixed power-capacity operation and maintenance cost"),
    ]
    fom_cost_energy: Annotated[
        NonNegativeFloat,
        Unit("$/MWh/year"),
        Field(ge=0, description="Fixed energy-capacity operation and maintenance cost"),
    ]
    vom_cost: Annotated[
        NonNegativeFloat,
        Unit("$/MWh"),
        Field(ge=0, description="Variable operation and maintenance cost"),
    ]
    heat_rate: (
        Annotated[
            PositiveFloat,
            Unit("MMBtu/MWh"),
            Field(gt=0),
        ]
        | None
    ) = Field(default=None, description="Thermal heat rate; None for non-combustion technologies")
    round_trip_efficiency: PositiveUnitFloat | None = Field(
        default=None,
        description="Storage round-trip efficiency when defined",
    )
    upgrade_cost: (
        Annotated[
            NonNegativeFloat,
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
        NonNegativeFloat,
        Unit("MW"),
        Field(ge=0, description="Initial power capacity before retirements"),
    ]
    initial_energy_capacity: (
        Annotated[
            NonNegativeFloat,
            Unit("MWh"),
            Field(ge=0),
        ]
        | None
    ) = Field(default=None, description="Initial storage energy capacity when defined")


class ReEDSStorageDuration(InfraSysBaseModel):
    """Technology-level storage-duration default from ``storage_duration.csv``."""

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    duration: Annotated[
        PositiveFloat,
        Unit("hours"),
        Field(gt=0, description="Fixed energy-to-power duration"),
    ]


class ReEDSStorageDurationOverride(InfraSysBaseModel):
    """An enabled regional and vintage-specific duration from ``storage_duration_pshdata.csv``."""

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    vintage: Annotated[str, Field(min_length=1, description="ReEDS capacity vintage identifier")]
    region: Annotated[str, Field(min_length=1, description="ReEDS region identifier")]
    duration: Annotated[
        PositiveFloat,
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

    emission_type: Annotated[
        EmissionType | None,
        Field(description="Active annual-emissions constraint type, if enabled"),
    ] = None
    planning_periods: Annotated[
        PlanningPeriods,
        Field(
            min_length=1,
            description="Unique ascending modeled years whose emission caps agree with emission_type.",
        ),
    ]
    representative_timepoints: Annotated[
        RepresentativeTimepoints,
        Field(
            min_length=1,
            description="Unique labels with contiguous zero-based positions in the representative sequence.",
        ),
    ]
    plant_characteristics: Annotated[
        PlantCharacteristics,
        Field(
            min_length=1,
            description="Unique technology-year plant characteristics for modeled planning years.",
        ),
    ]
    initial_capacities: Annotated[
        InitialCapacities,
        Field(
            description=(
                "Unique technology-region initial capacities; positive energy capacity requires power capacity."
            ),
        ),
    ] = ()
    storage_durations: Annotated[
        StorageDurations,
        Field(description="Unique technology-level storage-duration defaults."),
    ] = ()
    pumped_storage_supply_curve_duration: (
        Annotated[
            PositiveFloat,
            Unit("hours"),
            Field(gt=0),
        ]
        | None
    ) = Field(
        default=None,
        description="Selected supply-curve duration that overrides pumped-storage defaults",
    )
    storage_duration_overrides: Annotated[
        StorageDurationOverrides,
        Field(description="Unique technology-vintage-region storage-duration overrides."),
    ] = ()


class ReEDSCapacityExpansion(HasUnits, ReEDSComponent):
    """Capacity-expansion chronology and policy shared by system resources."""

    emission_type: Annotated[
        EmissionType,
        Field(description="Emission type constrained by planning-period caps"),
    ]
    planning_periods: Annotated[
        PlanningPeriods,
        Field(
            min_length=1,
            description="Unique ascending planning periods with an emission cap for emission_type.",
        ),
    ]
    representative_timepoints: Annotated[
        RepresentativeTimepoints,
        Field(
            min_length=1,
            description="Unique labels with contiguous zero-based positions in the representative sequence.",
        ),
    ]
    reserve_margin: Annotated[
        UnitFloat,
        Field(description="Planning reserve margin as a fraction of peak demand"),
    ]


class ReEDSCapacityExpansionResource(HasUnits, ReEDSComponent):
    """Abstract base class for investable capacity-expansion resource variants.

    Use a dispatchable, variable, or storage subtype; this base class cannot be instantiated.
    """

    region: Annotated[ReEDSRegion, Field(description="ReEDS region hosting the candidate")]
    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    available_years: Annotated[
        AvailableYears,
        Field(min_length=1, description="Unique ascending planning years in which the candidate is active."),
    ]
    initial_capacity: Annotated[
        NonNegativeFloat,
        Unit("MW"),
        Field(ge=0, description="Installed capacity before capacity-expansion investment"),
    ]
    investment_cost: Annotated[
        NonNegativeFloat,
        Unit("$/MW"),
        Field(ge=0, description="Capacity investment cost"),
    ]
    variable_cost: Annotated[
        NonNegativeFloat,
        Unit("$/MWh"),
        Field(ge=0, description="Variable operating cost"),
    ]

    @model_validator(mode="before")
    @classmethod
    def reject_base_resource(cls, data: Any) -> Any:
        """Prevent direct construction of the abstract candidate base type."""
        if cls is ReEDSCapacityExpansionResource:
            raise ValueError("resources must use a concrete operational subtype")
        return data


class ReEDSDispatchableCapacityExpansionResource(ReEDSCapacityExpansionResource):
    """An investable dispatchable resource with constant hourly availability."""

    capacity_factor: Annotated[
        UnitFloat,
        Field(description="Constant maximum generation fraction"),
    ]
    minimum_generation_fraction: Annotated[
        MinimumGenerationFraction,
        Field(
            description="Minimum hourly generation fraction that must not exceed capacity_factor.",
        ),
    ] = 0.0
    minimum_capacity_factor: Annotated[
        MinimumCapacityFactor,
        Field(
            description="Minimum annual capacity factor that must not exceed capacity_factor.",
        ),
    ] = 0.0
    ramp_up_cost: Annotated[
        NonNegativeFloat,
        Unit("$/MW-ramp"),
        Field(ge=0, description="Cost applied to positive generation ramps"),
    ] = 0.0


class ReEDSVariableCapacityExpansionResource(ReEDSCapacityExpansionResource):
    """An investable variable resource with a capacity-factor time series."""


class ReEDSStorageCapacityExpansionResource(ReEDSCapacityExpansionResource):
    """An investable fixed-duration storage resource."""

    round_trip_efficiency: Annotated[
        PositiveUnitFloat,
        Field(gt=0, le=1, description="Round-trip storage efficiency"),
    ]
    storage_duration: Annotated[
        PositiveFloat,
        Unit("hours"),
        Field(gt=0, description="Energy capacity duration at rated power"),
    ]
