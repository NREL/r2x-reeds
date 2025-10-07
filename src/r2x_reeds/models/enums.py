"""Enumerations for ReEDS model components."""

from enum import Enum


class EmissionType(str, Enum):
    """Types of emissions tracked in power system models."""

    CO2 = "CO2"
    NOX = "NOx"
    SO2 = "SO2"
    PM25 = "PM2.5"
    PM10 = "PM10"
    VOC = "VOC"
    NH3 = "NH3"
    CH4 = "CH4"
    N2O = "N2O"


class ReserveType(str, Enum):
    """Types of operating reserves."""

    REGULATION = "Regulation"
    SPINNING = "Spinning"
    NON_SPINNING = "Non-Spinning"
    FLEXIBILITY_UP = "Flexibility_Up"
    FLEXIBILITY_DOWN = "Flexibility_Down"
    CONTINGENCY = "Contingency"


class ReserveDirection(str, Enum):
    """Direction of reserve provision."""

    UP = "Up"
    DOWN = "Down"
