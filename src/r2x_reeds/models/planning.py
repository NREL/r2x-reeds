"""Capacity-expansion planning records for ReEDS systems."""

from __future__ import annotations

from typing import Annotated, Any

from infrasys import SupplementalAttribute
from pydantic import ConfigDict, Field, model_validator

from r2x_core.units import HasUnits, Unit

from .base import ReEDSComponent
from .components import ReEDSRegion
from .enums import AnnualCapMode, ReEDSBinarySwitch
from .types import (
    AvailableYears,
    MinimumCapacityFactor,
    MinimumGenerationFraction,
    PlanningYear,
    PositiveUnitFloat,
    UnitFloat,
)


class ReEDSPlanningSwitches(ReEDSComponent):
    """Run-level switches that control planning-input interpretation."""

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

class ReEDSPlanningPeriod(SupplementalAttribute):
    """Planning-year metadata reusable across planning components."""

    year: PlanningYear
    present_value_factor: Annotated[
        float,
        Unit("fraction"),
        Field(gt=0.0, description="Present-value factor for overnight capital costs"),
    ]
    emission_cap: Annotated[
        float,
        Unit("tonne"),
        Field(ge=0.0, description="Annual emissions cap; None means no cap is imposed"),
    ] | None = None


class ReEDSRepresentativeTimepoint(ReEDSComponent):
    """One global representative timepoint in the run chronology."""

    position: Annotated[
        int,
        Field(ge=0, description="Zero-based position in the representative sequence"),
    ]
    weight: Annotated[
        float,
        Unit("hours"),
        Field(gt=0.0, description="Calendar hours represented by this timepoint"),
    ]


class ReEDSPlantCharacteristics(ReEDSComponent):
    """Technology-year plant characteristics from ``plantcharout.csv``."""

    model_config = ConfigDict(populate_by_name=True)

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    year: PlanningYear
    capital_cost: Annotated[
        float,
        Unit("$/MW"),
        Field(ge=0.0, validation_alias="capcost", description="Power-capacity capital cost"),
    ]
    capital_cost_energy: Annotated[
        float,
        Unit("$/MWh"),
        Field(ge=0.0, validation_alias="capcost_energy", description="Energy-capacity capital cost"),
    ]
    fom_cost: Annotated[
        float,
        Unit("$/MW/year"),
        Field(ge=0.0, validation_alias="fom", description="Power-capacity fixed O&M cost"),
    ]
    fom_cost_energy: Annotated[
        float,
        Unit("$/MWh/year"),
        Field(ge=0.0, validation_alias="fom_energy", description="Energy-capacity fixed O&M cost"),
    ]
    vom_cost: Annotated[
        float,
        Unit("$/MWh"),
        Field(ge=0.0, validation_alias="vom", description="Variable O&M cost"),
    ]
    heat_rate: Annotated[
        Annotated[float, Unit("MMBtu/MWh"), Field(gt=0.0)] | None,
        Field(
            validation_alias="heatrate",
            description="Thermal heat rate; None for non-combustion technologies",
        ),
    ] = None
    round_trip_efficiency: Annotated[
        PositiveUnitFloat | None,
        Field(
            validation_alias="rte",
            description="Storage round-trip efficiency when defined",
        ),
    ] = None
    upgrade_cost: Annotated[
        float | None,
        Field(
            validation_alias="upgradecost",
            ge=0.0,
            description="Technology upgrade capital cost",
        ),
    ] = None


class ReEDSInitialCapacity(ReEDSComponent):
    """Initial non-resource capacity from ReEDS power and energy tables."""

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    region: Annotated[ReEDSRegion, Field(description="Region hosting initial capacity")]
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
    ) = None


class ReEDSStorageDuration(ReEDSComponent):
    """Technology-level storage-duration default."""

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    duration: Annotated[
        float,
        Unit("hours"),
        Field(gt=0.0, description="Fixed energy-to-power duration"),
    ]


class ReEDSStorageDurationOverride(ReEDSComponent):
    """Regional and vintage-specific storage duration override."""

    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    vintage: Annotated[str, Field(min_length=1, description="ReEDS capacity vintage identifier")]
    region: Annotated[ReEDSRegion, Field(description="Region hosting the override")]
    duration: Annotated[
        float,
        Unit("hours"),
        Field(gt=0.0, description="Asset-specific energy-to-power duration"),
    ]


class ReEDSPumpedStorageSupplyCurveDuration(ReEDSComponent):
    """Selected pumped-storage supply-curve duration for the run."""

    duration: Annotated[
        float,
        Unit("hours"),
        Field(gt=0.0, description="Selected pumped-storage duration"),
    ]


class ReEDSCapacityExpansionResource(HasUnits, ReEDSComponent):
    """An investable capacity-expansion resource candidate."""

    region: Annotated[ReEDSRegion, Field(description="Region hosting the candidate")]
    technology: Annotated[str, Field(min_length=1, description="ReEDS technology identifier")]
    available_years: Annotated[
        AvailableYears,
        Field(min_length=1, description="Planning years in which the candidate is active"),
    ]
    initial_capacity: Annotated[
        float,
        Unit("MW"),
        Field(ge=0.0, description="Installed capacity before expansion investment"),
    ]
    investment_cost: Annotated[float, Unit("$/MW"), Field(ge=0.0)]
    variable_cost: Annotated[float, Unit("$/MWh"), Field(ge=0.0)]

    @model_validator(mode="before")
    @classmethod
    def reject_base_resource(cls, data: Any) -> Any:
        """Prevent direct construction of the abstract candidate type."""
        if cls is ReEDSCapacityExpansionResource:
            raise ValueError("resources must use a concrete operational subtype")
        return data


class ReEDSDispatchableCapacityExpansionResource(ReEDSCapacityExpansionResource):
    """Investable dispatchable resource with constant availability."""

    capacity_factor: Annotated[UnitFloat, Field(description="Maximum generation fraction")]
    minimum_generation_fraction: Annotated[
        MinimumGenerationFraction,
        Field(description="Minimum hourly generation fraction"),
    ] = 0.0
    minimum_capacity_factor: Annotated[
        MinimumCapacityFactor,
        Field(description="Minimum annual capacity factor"),
    ] = 0.0
    ramp_up_cost: Annotated[float, Unit("$/MW-ramp"), Field(ge=0.0)] = 0.0


class ReEDSVariableCapacityExpansionResource(ReEDSCapacityExpansionResource):
    """Investable variable resource with a capacity-factor time series."""


class ReEDSStorageCapacityExpansionResource(ReEDSCapacityExpansionResource):
    """Investable fixed-duration storage resource."""

    round_trip_efficiency: Annotated[PositiveUnitFloat, Field(description="Round-trip efficiency")]
    storage_duration: Annotated[float, Unit("hours"), Field(gt=0.0)]
