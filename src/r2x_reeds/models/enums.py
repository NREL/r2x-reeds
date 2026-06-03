"""Enumerations for ReEDS model components."""

from enum import StrEnum


class EmissionType(StrEnum):
    """Types of emissions tracked in power system models."""

    CO2E = "CO2E"
    CO2 = "CO2"
    NOX = "NOx"
    SO2 = "SO2"
    PM25 = "PM2.5"
    PM10 = "PM10"
    VOC = "VOC"
    NH3 = "NH3"
    CH4 = "CH4"
    N2O = "N2O"
    H2 = "H2"


class EmissionSource(StrEnum):
    """Sources for emissions tracking, used by emission components."""

    COMBUSTION = "COMBUSTION"
    PRECOMBUSTION = "PRECOMBUSTION"


class ReserveType(StrEnum):
    """Types of operating reserves."""

    REGULATION = "REGULATION"
    SPINNING = "SPINNING"
    NON_SPINNING = "NON_SPINNING"
    FLEXIBILITY = "FLEXIBILITY"
    CONTINGENCY = "CONTINGENCY"
    COMBO = "COMBO"


class ReserveDirection(StrEnum):
    """Direction of reserve provision."""

    UP = "Up"
    DOWN = "Down"


class FuelType(StrEnum):
    """Fuel types mapped from ReEDS ``fuel2tech`` data."""

    COAL = "COAL"
    NATURAL_GAS = "naturalgas"
    BIOMASS = "biomass"
    HYDROGEN_CT = "h2ct"
    URANIUM = "uranium"
    OIL = "oil"
    OTHER = "OTHER"
