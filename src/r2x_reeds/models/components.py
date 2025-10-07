"""ReEDS component models."""

from __future__ import annotations

from typing import Annotated

from infrasys import SupplementalAttribute
from pydantic import Field, PositiveFloat

from .base import FromTo_ToFrom, ReEDSComponent
from .enums import EmissionType, ReserveDirection, ReserveType
from .units import EmissionRate, Percentage


class ReEDSEmission(SupplementalAttribute):
    """Emission attribute attached to generators.

    Represents emissions produced by generation resources.
    """

    rate: Annotated[EmissionRate, Field(description="Amount of emission produced in kg/MWh")]
    emission_type: Annotated[EmissionType, Field(description="Type of emission (e.g., CO2, NOx)")]


class ReEDSRegion(ReEDSComponent):
    """ReEDS regional component.

    Represents a geographic region in the ReEDS model with various
    regional attributes and hierarchies.
    """

    state: Annotated[str | None, Field(None, description="State abbreviation")] = None
    nerc_region: Annotated[str | None, Field(None, description="NERC region")] = None
    transmission_region: Annotated[str | None, Field(None, description="Transmission planning region")] = None
    transmission_group: Annotated[str | None, Field(None, description="Transmission group")] = None
    interconnect: Annotated[
        str | None,
        Field(None, description="Interconnection (eastern, western, texas)"),
    ] = None
    country: Annotated[str | None, Field(None, description="Country code")] = None
    max_active_power: Annotated[float | None, Field(None, description="Peak demand in MW", ge=0)] = None
    timezone: Annotated[str | None, Field(None, description="Time zone identifier")] = None
    cendiv: Annotated[str | None, Field(None, description="Census division")] = None
    usda_region: Annotated[str | None, Field(None, description="USDA region")] = None
    h2ptc_region: Annotated[str | None, Field(None, description="H2 PTC region")] = None
    hurdle_region: Annotated[str | None, Field(None, description="Hurdle rate region")] = None
    cc_region: Annotated[str | None, Field(None, description="Climate change region")] = None


class ReEDSReserveRegion(ReEDSComponent):
    """ReEDS reserve region component.

    Represents a geographic region for operating reserve requirements.
    """


class ReEDSReserve(ReEDSComponent):
    """ReEDS operating reserve component.

    Defines operating reserve requirements and parameters for the system.
    """

    time_frame: Annotated[
        PositiveFloat,
        Field(description="Timeframe in which the reserve is required in seconds"),
    ] = 1e30
    region: Annotated[
        ReEDSReserveRegion | None,
        Field(None, description="Reserve region where requirement applies"),
    ] = None
    vors: Annotated[
        float,
        Field(description="Value of reserve shortage in $/MW. Positive value acts as soft constraint"),
    ] = -1
    duration: Annotated[
        PositiveFloat | None,
        Field(None, description="Time over which the required response must be maintained in seconds"),
    ] = None
    reserve_type: Annotated[ReserveType, Field(description="Type of reserve")]
    load_risk: Annotated[
        Percentage | None,
        Field(None, description="Proportion of load that contributes to the requirement"),
    ] = None
    max_requirement: Annotated[float, Field(description="Maximum reserve requirement", ge=0)] = 0
    direction: Annotated[ReserveDirection, Field(description="Direction of reserve provision")]


class ReEDSInterface(ReEDSComponent):
    """ReEDS region interface.

    Represents the connection between two regions for power transfer.
    """

    from_region: Annotated[ReEDSRegion, Field(description="Origin region")]
    to_region: Annotated[ReEDSRegion, Field(description="Destination region")]


class ReEDSGenerator(ReEDSComponent):
    """ReEDS generator component.

    Represents a generation resource in the ReEDS model.
    """

    region: Annotated[ReEDSRegion, Field(description="ReEDS region where generator is located")]
    technology: Annotated[str | None, Field(None, description="ReEDS technology type")] = None
    capacity: Annotated[float, Field(description="Existing capacity in MW", ge=0)]
    heat_rate: Annotated[float | None, Field(None, description="Heat rate in MMBtu/MWh", ge=0)] = None
    forced_outage_rate: Annotated[
        float | None,
        Field(None, description="Forced outage rate (fraction)", ge=0, le=1),
    ] = None
    planned_outage_rate: Annotated[
        float | None,
        Field(None, description="Planned outage rate (fraction)", ge=0, le=1),
    ] = None
    min_capacity_factor: Annotated[
        float | None,
        Field(None, description="Minimum capacity factor", ge=0, le=1),
    ] = None
    max_age: Annotated[int | None, Field(None, description="Maximum age in years", ge=0)] = None
    fuel_type: Annotated[str | None, Field(None, description="Fuel type (e.g., 'coal', 'gas')")] = None
    fuel_price: Annotated[float | None, Field(None, description="Fuel price in $/MMBtu", ge=0)] = None
    vom_cost: Annotated[float | None, Field(None, description="Variable O&M cost in $/MWh", ge=0)] = None
    vintage: Annotated[str | None, Field(None, description="Vintage bin identifier")] = None
    retirement_year: Annotated[int | None, Field(None, description="Planned retirement year")] = None


class ReEDSTransmissionLine(ReEDSComponent):
    """ReEDS transmission line component.

    Represents a transmission line connection between two regions.
    """

    interface: Annotated[ReEDSInterface, Field(description="Interface connecting two regions")]
    max_active_power: Annotated[FromTo_ToFrom, Field(description="Transfer capacity limit in MW")]
    losses: Annotated[
        float | None,
        Field(None, description="Transmission losses (fraction)", ge=0, le=1),
    ] = None
    line_type: Annotated[str | None, Field(None, description="Line type (AC/DC)")] = None
    voltage: Annotated[float | None, Field(None, description="Voltage level in kV", ge=0)] = None
    distance_miles: Annotated[float | None, Field(None, description="Distance in miles", ge=0)] = None
    line_cost_per_mw_mile: Annotated[
        float | None,
        Field(None, description="Cost per MW-mile", ge=0),
    ] = None
    hurdle_rate: Annotated[
        float | None,
        Field(None, description="Hurdle rate forward direction", ge=0),
    ] = None


class ReEDSDemand(ReEDSComponent):
    """ReEDS electrical demand component.

    Represents load/demand in a region.
    """

    region: Annotated[ReEDSRegion, Field(description="ReEDS region")]
    max_active_power: Annotated[
        float | None,
        Field(None, description="Maximum active power demand in MW", ge=0),
    ] = None


class ReEDSResourceClass(ReEDSComponent):
    """ReEDS supply curve resource component.

    Represents renewable resource potential in a region with
    associated costs and capacity factors.
    """

    technology: Annotated[str, Field(description="Technology type (e.g., 'upv', 'wind-ons')")]
    region: Annotated[ReEDSRegion, Field(description="ReEDS region")]
    resource_class: Annotated[str, Field(description="Resource class identifier")]
    capacity: Annotated[float, Field(description="Available capacity in MW", ge=0)]
    capacity_factor: Annotated[
        float | None,
        Field(None, description="Average capacity factor", ge=0, le=1),
    ] = None
    cost_per_mw: Annotated[float | None, Field(None, description="Cost per MW")] = None
    fixed_om_per_mw: Annotated[
        float | None,
        Field(None, description="Fixed O&M per MW-year", ge=0),
    ] = None
    variable_om_per_mwh: Annotated[
        float | None,
        Field(None, description="Variable O&M per MWh", ge=0),
    ] = None
