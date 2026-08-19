"""Canonical ReEDS component and supplemental-attribute models."""

from __future__ import annotations

from typing import Annotated

from infrasys import SupplementalAttribute
from infrasys.models import InfraSysBaseModel
from pydantic import AfterValidator, Field, model_validator

from r2x_core.units import HasUnits, Unit

from .base import FractionRange, FromTo_ToFrom, ReEDSComponent
from .checks import (
    validate_optional_fraction,
    validate_optional_nonnegative,
    validate_optional_positive,
)
from .enums import EmissionSource, EmissionType, ReserveDirection, ReserveType, TransmissionLineType
from .types import (
    EmissionRate,
    Fraction,
    NonEmptyText,
    PlanningYear,
    PositiveUnitFloat,
)


class ReEDSGeneratorEconomics(SupplementalAttribute):
    """Optional generator economics supplied by cost and fuel-price datasets."""

    fuel_price: Annotated[
        float | None,
        Unit("$/MMBtu"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Fuel price"),
    ] = None
    vom_cost: Annotated[
        float | None,
        Unit("$/MWh"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Variable operation and maintenance cost"),
    ] = None
    fom_cost: Annotated[
        float | None,
        Unit("$/MW/year"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Fixed operation and maintenance cost"),
    ] = None
    capital_cost: Annotated[
        float | None,
        Unit("$/MW"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Capital cost"),
    ] = None


class ReEDSGeneratorPerformance(SupplementalAttribute):
    """Optional performance characteristics supplied by plant data."""

    heat_rate: Annotated[
        float | None,
        Unit("MMBtu/MWh"),
        AfterValidator(validate_optional_positive),
        Field(description="Heat rate"),
    ] = None


class ReEDSGeneratorOperatingConstraints(SupplementalAttribute):
    """Optional operating constraints supplied by outage and plant-limit datasets."""

    forced_outage_rate: Annotated[
        float | None,
        Unit("fraction"),
        AfterValidator(validate_optional_fraction),
        Field(description="Forced outage fraction"),
    ] = None
    planned_outage_rate: Annotated[
        float | None,
        Unit("fraction"),
        AfterValidator(validate_optional_fraction),
        Field(description="Planned outage fraction"),
    ] = None
    max_age: Annotated[int | None, Field(ge=0, description="Maximum age in years")] = None
    min_stable_level: Annotated[
        float | None,
        Unit("fraction"),
        AfterValidator(validate_optional_fraction),
        Field(description="Minimum stable-load fraction"),
    ] = None
    ramp_rate: Annotated[
        float | None,
        Unit("fraction/hour"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Ramp rate"),
    ] = None
    capacity_factor_range: FractionRange | None = None
    startup_cost: Annotated[
        float | None,
        Unit("$/MW"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Startup cost"),
    ] = None
    min_up_time: Annotated[
        float | None,
        Unit("hours"),
        AfterValidator(validate_optional_positive),
        Field(description="Minimum up time"),
    ] = None
    min_down_time: Annotated[
        float | None,
        Unit("hours"),
        AfterValidator(validate_optional_positive),
        Field(description="Minimum down time"),
    ] = None


class ReEDSGeneratorSupplyCurve(SupplementalAttribute):
    """Optional renewable supply-curve enrichment."""

    resource_class: Annotated[str | None, Field(min_length=1, description="Resource-class identifier")] = None
    inverter_loading_ratio: Annotated[
        float | None,
        Unit("ratio"),
        AfterValidator(validate_optional_positive),
        Field(description="Inverter loading ratio"),
    ] = None
    capacity_factor_adjustment: Annotated[
        float | None,
        Unit("ratio"),
        AfterValidator(validate_optional_positive),
        Field(description="Capacity-factor adjustment"),
    ] = None
    max_capacity_factor: Annotated[
        float | None,
        Unit("fraction"),
        AfterValidator(validate_optional_fraction),
        Field(description="Maximum capacity factor"),
    ] = None
    supply_curve_cost: Annotated[
        float | None,
        Unit("$/MW"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Supply-curve cost"),
    ] = None
    transmission_adder: Annotated[
        float | None,
        Unit("$/MW"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Transmission cost adder"),
    ] = None


class ReEDSGeneratorIdentity(InfraSysBaseModel):
    """Optional vintage and retirement metadata."""

    vintage: Annotated[str | None, Field(min_length=1, description="Vintage-bin identifier")] = None
    retirement_year: Annotated[PlanningYear | None, Field(description="Planned retirement year")] = None


class ReEDSConsumingTechnologyEconomics(SupplementalAttribute):
    """Optional cost data for an electricity-consuming technology."""

    fuel_price: Annotated[
        float | None,
        Unit("$/MMBtu"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Fuel price"),
    ] = None
    capital_cost: Annotated[
        float | None,
        Unit("$/kW"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Capital cost"),
    ] = None
    fom_cost: Annotated[
        float | None,
        Unit("$/kW/year"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Fixed operation and maintenance cost"),
    ] = None
    vom_cost: Annotated[
        float | None,
        Unit("$/MWh"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Variable operation and maintenance cost"),
    ] = None


class ReEDSConsumingTechnologyPerformance(SupplementalAttribute):
    """Optional performance data for an electricity-consuming technology."""

    heat_rate: Annotated[
        float | None,
        Unit("MMBtu/MWh"),
        AfterValidator(validate_optional_positive),
        Field(description="Heat rate"),
    ] = None
    gas_consumption_rate: Annotated[
        float | None,
        Unit("MMBtu/kg"),
        AfterValidator(validate_optional_positive),
        Field(description="Gas consumption rate"),
    ] = None
    storage_transport_adder: Annotated[
        float | None,
        Unit("$/kW"),
        AfterValidator(validate_optional_nonnegative),
        Field(description="Storage and transport cost adder"),
    ] = None
    vintage: Annotated[str | None, Field(min_length=1, description="Vintage-bin identifier")] = None


class ReEDSEmission(SupplementalAttribute):
    """Nonnegative emission rate attached to a component."""

    rate: Annotated[EmissionRate, Field(description="Emission rate for the emission type")]
    type: EmissionType
    source: EmissionSource = EmissionSource.COMBUSTION


class ReEDSRegion(ReEDSComponent):
    """A ReEDS region with the complete hierarchy record from ``hierarchy.csv``."""

    state: Annotated[NonEmptyText, Field(description="State or regional abbreviation")]
    nerc_region: Annotated[NonEmptyText, Field(description="NERC region")]
    transmission_region: Annotated[NonEmptyText, Field(description="Transmission planning region")]
    transmission_group: Annotated[NonEmptyText, Field(description="Transmission group")]
    interconnect: Annotated[
        NonEmptyText,
        Field(description="Interconnection, such as eastern, western, or texas"),
    ]
    country: Annotated[NonEmptyText, Field(description="Country code")]
    cendiv: Annotated[NonEmptyText, Field(description="Census division")]
    usda_region: Annotated[NonEmptyText, Field(description="USDA region")]
    h2ptc_region: Annotated[NonEmptyText, Field(description="Hydrogen PTC region")]
    hurdle_region: Annotated[NonEmptyText, Field(description="Hurdle-rate region")]
    cc_region: Annotated[NonEmptyText, Field(description="Climate-change region")]

    @classmethod
    def example(cls) -> ReEDSRegion:
        """Return a complete example region for documentation and tests."""
        return ReEDSRegion(
            name="p1",
            state="CA",
            nerc_region="WECC_CA",
            transmission_region="CAISO",
            transmission_group="CAISO",
            interconnect="western",
            country="USA",
            cendiv="Pacific",
            usda_region="pacific",
            h2ptc_region="California",
            hurdle_region="CAISO",
            cc_region="CAISO",
        )


class ReEDSReserveRegion(ReEDSComponent):
    """A reserve area identified by its transmission-region component name."""


class ReEDSReserve(HasUnits, ReEDSComponent):
    """An operating-reserve requirement for one reserve area and direction."""

    time_frame: Annotated[
        float,
        Unit("s"),
        Field(gt=0.0, description="Timeframe in which the reserve response is required"),
    ]
    region: Annotated[
        ReEDSReserveRegion,
        Field(description="Reserve area where the requirement applies"),
    ]
    vors: Annotated[
        float,
        Unit("$/MW"),
        Field(description="Value of reserve shortage; negative values represent a hard constraint"),
    ]
    duration: Annotated[
        float,
        Unit("s"),
        Field(gt=0.0, description="Time over which the reserve response must be maintained"),
    ]
    or_load_percentage: Annotated[
        Fraction,
        Field(description="Fraction of load contributing to the reserve requirement"),
    ]
    or_wind_percentage: Annotated[
        Fraction,
        Field(description="Fraction of wind generation contributing to the reserve requirement"),
    ]
    or_pv_percentage: Annotated[
        Fraction,
        Field(description="Fraction of solar generation contributing to the reserve requirement"),
    ]
    reserve_type: Annotated[ReserveType, Field(description="Type of reserve")]
    direction: Annotated[ReserveDirection, Field(description="Direction of reserve provision")]


class ReEDSInterface(ReEDSComponent):
    """An undirected interface between two distinct ReEDS regions."""

    from_region: Annotated[ReEDSRegion, Field(description="Origin region")]
    to_region: Annotated[ReEDSRegion, Field(description="Destination region")]

    @model_validator(mode="after")
    def validate_distinct_regions(self) -> ReEDSInterface:
        """Reject self-referential interfaces."""
        if self.from_region.name == self.to_region.name:
            raise ValueError("from_region and to_region must be distinct")
        return self


class ReEDSGenerator(HasUnits, ReEDSComponent):
    """Base generator component containing intrinsic identity and capacity."""

    region: Annotated[ReEDSRegion, Field(description="ReEDS region")]
    technology: Annotated[NonEmptyText, Field(description="ReEDS technology type")]
    capacity: Annotated[
        float,
        Unit("MW"),
        Field(ge=0.0, description="Installed capacity"),
    ]
    identity: ReEDSGeneratorIdentity = Field(default_factory=ReEDSGeneratorIdentity)


class ReEDSThermalGenerator(ReEDSGenerator):
    """Thermal generator with required combustion characteristics."""

    heat_rate: Annotated[float, Unit("MMBtu/MWh"), Field(gt=0.0, description="Heat rate")]
    fuel_type: Annotated[NonEmptyText, Field(description="Fuel type")]

    @classmethod
    def example(cls) -> ReEDSThermalGenerator:
        """Return a complete example thermal generator."""
        return ReEDSThermalGenerator(
            name="simple-bus",
            category="thermal",
            region=ReEDSRegion.example(),
            technology="gas-cc",
            capacity=100,
            heat_rate=15,
            fuel_type="ngas",
        )


class ReEDSVariableGenerator(ReEDSGenerator):
    """Renewable generator with capacity-factor profiles."""


class ReEDSStorage(ReEDSGenerator):
    """Storage generator with energy/power characteristics."""

    storage_duration: Annotated[
        float,
        Unit("hours"),
        Field(gt=0.0, description="Storage duration"),
    ]
    round_trip_efficiency: Annotated[
        PositiveUnitFloat,
        Field(description="Round-trip efficiency"),
    ]


class ReEDSHydroGenerator(ReEDSGenerator):
    """Hydroelectric generator with optional operating-flow bounds."""

    is_dispatchable: Annotated[bool, Field(description="Whether hydro is dispatchable")]


class ReEDSConsumingTechnology(HasUnits, ReEDSComponent):
    """Technology that consumes electricity to produce another product."""

    region: Annotated[ReEDSRegion, Field(description="ReEDS region")]
    technology: Annotated[NonEmptyText, Field(description="Technology type")]
    capacity: Annotated[
        float,
        Unit("MW"),
        Field(ge=0.0, description="Consumption capacity"),
    ]
    electricity_consumption_rate: Annotated[
        float,
        Unit("kWh/kg"),
        Field(gt=0.0, description="Electricity consumption rate"),
    ]
    identity: ReEDSGeneratorIdentity = Field(default_factory=ReEDSGeneratorIdentity)


class ReEDSElectrolyzerDemand(ReEDSConsumingTechnology):
    """Electricity demand from an electrolyzer."""

    max_active_power: Annotated[
        float,
        Unit("MW"),
        Field(ge=0.0, description="Maximum active power demand"),
    ]


class ReEDSDataCenterDemand(ReEDSConsumingTechnology):
    """Electricity demand from a data center or other large facility."""

    max_active_power: Annotated[
        float,
        Unit("MW"),
        Field(ge=0.0, description="Maximum active power demand"),
    ]


class ReEDSH2Storage(HasUnits, ReEDSComponent):
    """Hydrogen storage infrastructure."""

    region: Annotated[ReEDSRegion, Field(description="ReEDS region")]
    storage_type: Annotated[NonEmptyText, Field(description="Storage type")]
    capacity: Annotated[
        float,
        Unit("tonnes"),
        Field(ge=0.0, description="Hydrogen storage capacity"),
    ]


class ReEDSH2Pipeline(HasUnits, ReEDSComponent):
    """Hydrogen transmission pipeline."""

    from_region: Annotated[ReEDSRegion, Field(description="Origin region")]
    to_region: Annotated[ReEDSRegion, Field(description="Destination region")]
    capacity: Annotated[
        float,
        Unit("tonnes"),
        Field(ge=0.0, description="Pipeline capacity"),
    ]
    distance: Annotated[float, Unit("km"), Field(gt=0.0, description="Pipeline distance")]


class ReEDSTransmissionLine(HasUnits, ReEDSComponent):
    """Transmission line with directional capacity and line characteristics."""

    interface: Annotated[ReEDSInterface, Field(description="Interface connecting two regions")]
    max_active_power: Annotated[
        FromTo_ToFrom,
        Field(description="Directional transfer capacity limits"),
    ]
    losses: Annotated[
        float,
        Unit("fraction"),
        Field(ge=0.0, le=1.0, description="Transmission-loss fraction"),
    ]
    line_type: Annotated[TransmissionLineType, Field(description="Transmission line type")]


class ReEDSDemand(HasUnits, ReEDSComponent):
    """Electrical demand with a required regional peak-power value."""

    region: Annotated[ReEDSRegion, Field(description="ReEDS region")]
    max_active_power: Annotated[
        float,
        Unit("MW"),
        Field(ge=0.0, description="Maximum active power demand"),
    ]


class ReEDSResourceClass(HasUnits, ReEDSComponent):
    """Renewable supply-curve resource class for one technology and region."""

    technology: Annotated[NonEmptyText, Field(description="Technology type")]
    region: Annotated[ReEDSRegion, Field(description="ReEDS region")]
    resource_class: Annotated[NonEmptyText, Field(description="Resource-class identifier")]
    capacity: Annotated[
        float,
        Unit("MW"),
        Field(ge=0.0, description="Available resource capacity"),
    ]
    cost: Annotated[
        float,
        Unit("$/MW"),
        Field(ge=0.0, description="Supply-curve cost"),
    ]
