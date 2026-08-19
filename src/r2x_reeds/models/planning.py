"""Capacity-expansion planning models for ReEDS systems."""

from __future__ import annotations

from typing import Annotated, Any

from infrasys.models import InfraSysBaseModel
from pydantic import ConfigDict, Field, model_validator

from r2x_core.units import HasUnits, Unit

from .base import ReEDSComponent
from .components import ReEDSRegion
from .enums import AnnualCapMode, EmissionType, ReEDSBinarySwitch
from .types import (
    AvailableYears,
    InitialCapacities,
    MinimumCapacityFactor,
    MinimumGenerationFraction,
    PlanningPeriods,
    PlanningYear,
    PlantCharacteristics,
    PositiveUnitFloat,
    RepresentativeTimepoints,
    StorageDurationOverrides,
    StorageDurations,
    UnitFloat,
)


class ReEDSPlanningSwitches(InfraSysBaseModel):
    """Switches that control the capacity-expansion input interpretation."""

    model_config = ConfigDict(populate_by_name=True)

    annual_cap: Annotated[
        AnnualCapMode,
        Field(validation_alias="GSw_AnnualCap", description="Annual emissions-cap mode"),
    ] = AnnualCapMode.DISABLED
    storage: Annotated[
        ReEDSBinarySwitch,
        Field(validation_alias="GSw_Storage", description="Whether standalone storage is enabled"),
    ] = ReEDSBinarySwitch.OFF
    hydro_psh_duration_data: Annotated[
        ReEDSBinarySwitch,
        Field(
            validation_alias="GSw_HydroPSHDurData",
            description="Whether pumped-storage duration overrides are enabled",
        ),
    ] = ReEDSBinarySwitch.OFF

    @property
    def emission_type(self) -> EmissionType | None:
        """Return the emission type selected by ``annual_cap``."""
        return self.annual_cap.emission_type

    @property
    def storage_enabled(self) -> bool:
        """Return whether standalone storage is enabled."""
        return self.storage is ReEDSBinarySwitch.ON

    @property
    def hydro_psh_duration_data_enabled(self) -> bool:
        """Return whether pumped-storage duration overrides are enabled."""
        return self.hydro_psh_duration_data is ReEDSBinarySwitch.ON


class ReEDSPlanningPeriod(InfraSysBaseModel):
    """One chronologically ordered ReEDS capacity-expansion period."""

    year: PlanningYear
    present_value_factor: Annotated[
        float,
        Unit("fraction"),
        Field(gt=0.0, description="Present-value factor for overnight capital costs"),
    ]
    emission_cap: (
        Annotated[
            float,
            Unit("tonne"),
            Field(ge=0.0, description="Annual emissions cap"),
        ]
        | None
    ) = Field(default=None, description="None means no cap is imposed")


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
        Field(gt=0.0, description="Calendar hours represented by this timepoint"),
    ]


class ReEDSPlantCharacteristics(InfraSysBaseModel):
    """Canonical ``plantcharout.csv`` characteristics for one technology and year.

    ReEDS uses zero as a placeholder for unavailable heat-rate and round-trip
    efficiency values. Input readers represent those placeholders as ``None``.
    """

    model_config = ConfigDict(populate_by_name=True)

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    year: PlanningYear
    capital_cost: Annotated[
        float,
        Unit("$/MW"),
        Field(
            ge=0.0,
            validation_alias="capcost",
            description="Overnight power-capacity capital cost",
        ),
    ]
    capital_cost_energy: Annotated[
        float,
        Unit("$/MWh"),
        Field(
            ge=0.0,
            validation_alias="capcost_energy",
            description="Overnight energy-capacity capital cost",
        ),
    ]
    fom_cost: Annotated[
        float,
        Unit("$/MW/year"),
        Field(
            ge=0.0,
            validation_alias="fom",
            description="Fixed power-capacity operation and maintenance cost",
        ),
    ]
    fom_cost_energy: Annotated[
        float,
        Unit("$/MWh/year"),
        Field(
            ge=0.0,
            validation_alias="fom_energy",
            description="Fixed energy-capacity operation and maintenance cost",
        ),
    ]
    vom_cost: Annotated[
        float,
        Unit("$/MWh"),
        Field(
            ge=0.0,
            validation_alias="vom",
            description="Variable operation and maintenance cost",
        ),
    ]
    heat_rate: (
        Annotated[
            float,
            Unit("MMBtu/MWh"),
            Field(gt=0.0, description="Thermal heat rate"),
        ]
        | None
    ) = Field(
        default=None,
        validation_alias="heatrate",
        json_schema_extra={"source_required": True},
        description="None for non-combustion technologies",
    )
    round_trip_efficiency: PositiveUnitFloat | None = Field(
        default=None,
        validation_alias="rte",
        json_schema_extra={"source_required": True},
        description="Storage round-trip efficiency when defined",
    )
    upgrade_cost: (
        Annotated[
            float,
            Unit("$/MW"),
            Field(ge=0.0, description="Technology upgrade capital cost"),
        ]
        | None
    ) = Field(
        default=None,
        validation_alias="upgradecost",
        description="None when no upgrade cost is defined",
    )


class ReEDSInitialCapacity(InfraSysBaseModel):
    """Initial non-resource capacity from ``capnonrsc.csv`` and its energy table."""

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    region: Annotated[str, Field(min_length=1, description="ReEDS region identifier")]
    initial_power_capacity: Annotated[
        float,
        Unit("MW"),
        Field(ge=0.0, description="Initial power capacity before retirements"),
    ]
    initial_energy_capacity: (
        Annotated[
            float,
            Unit("MWh"),
            Field(ge=0.0, description="Initial storage energy capacity"),
        ]
        | None
    ) = Field(default=None, description="None when no energy-capacity table is available")


class ReEDSStorageDuration(InfraSysBaseModel):
    """Technology-level storage-duration default from ``storage_duration.csv``."""

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    duration: Annotated[
        float,
        Unit("hours"),
        Field(gt=0.0, description="Fixed energy-to-power duration"),
    ]


class ReEDSStorageDurationOverride(InfraSysBaseModel):
    """Regional and vintage-specific duration from ``storage_duration_pshdata.csv``."""

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    vintage: Annotated[str, Field(min_length=1, description="ReEDS capacity vintage identifier")]
    region: Annotated[str, Field(min_length=1, description="ReEDS region identifier")]
    duration: Annotated[
        float,
        Unit("hours"),
        Field(gt=0.0, description="Asset-specific energy-to-power duration"),
    ]


class ReEDSCapacityExpansion(HasUnits, ReEDSComponent):
    """Capacity-expansion data attached to a ReEDS system."""

    emission_type: Annotated[
        EmissionType | None,
        Field(description="Active annual-emissions constraint type, if enabled"),
    ] = None
    switches: ReEDSPlanningSwitches = Field(
        default_factory=ReEDSPlanningSwitches,
        description="Capacity-expansion switches used to interpret the source inputs",
    )
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
    reserve_margin: Annotated[
        UnitFloat,
        Field(description="Planning reserve margin as a fraction of peak demand"),
    ] = 0.0
    plant_characteristics: Annotated[
        PlantCharacteristics,
        Field(description="Unique technology-year plant characteristics for modeled planning years."),
    ] = ()
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
            float,
            Unit("hours"),
            Field(gt=0.0, description="Selected pumped-storage supply-curve duration"),
        ]
        | None
    ) = Field(default=None, description="None when no override is selected")
    storage_duration_overrides: Annotated[
        StorageDurationOverrides,
        Field(description="Unique technology-vintage-region storage-duration overrides."),
    ] = ()

    @model_validator(mode="before")
    @classmethod
    def derive_emission_type_from_switches(cls, data: Any) -> Any:
        """Derive the active emission type from the source switches."""
        if not isinstance(data, dict) or data.get("emission_type") is not None:
            return data
        switches = data.get("switches", ReEDSPlanningSwitches())
        if not isinstance(switches, ReEDSPlanningSwitches):
            switches = ReEDSPlanningSwitches.model_validate(switches)
        updated = dict(data)
        updated["emission_type"] = switches.emission_type
        return updated


class ReEDSCapacityExpansionResource(HasUnits, ReEDSComponent):
    """Abstract base class for investable capacity-expansion resource variants."""

    region: Annotated[ReEDSRegion, Field(description="ReEDS region hosting the candidate")]
    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    available_years: Annotated[
        AvailableYears,
        Field(min_length=1, description="Unique ascending planning years in which the candidate is active."),
    ]
    initial_capacity: Annotated[
        float,
        Unit("MW"),
        Field(ge=0.0, description="Installed capacity before capacity-expansion investment"),
    ]
    investment_cost: Annotated[
        float,
        Unit("$/MW"),
        Field(ge=0.0, description="Capacity investment cost"),
    ]
    variable_cost: Annotated[
        float,
        Unit("$/MWh"),
        Field(ge=0.0, description="Variable operating cost"),
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
        Field(description="Minimum hourly generation fraction that must not exceed capacity_factor"),
    ] = 0.0
    minimum_capacity_factor: Annotated[
        MinimumCapacityFactor,
        Field(description="Minimum annual capacity factor that must not exceed capacity_factor"),
    ] = 0.0
    ramp_up_cost: Annotated[
        float,
        Unit("$/MW-ramp"),
        Field(ge=0.0, description="Cost applied to positive generation ramps"),
    ] = 0.0


class ReEDSVariableCapacityExpansionResource(ReEDSCapacityExpansionResource):
    """An investable variable resource with a capacity-factor time series."""


class ReEDSStorageCapacityExpansionResource(ReEDSCapacityExpansionResource):
    """An investable fixed-duration storage resource."""

    round_trip_efficiency: Annotated[
        PositiveUnitFloat,
        Field(description="Round-trip storage efficiency"),
    ]
    storage_duration: Annotated[
        float,
        Unit("hours"),
        Field(gt=0.0, description="Energy capacity duration at rated power"),
    ]
