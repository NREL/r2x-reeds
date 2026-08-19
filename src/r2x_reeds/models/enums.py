"""Enumerations for ReEDS model components."""

from __future__ import annotations

from enum import IntEnum, StrEnum


class ReEDSBinarySwitch(IntEnum):
    """Binary ReEDS switch values."""

    OFF = 0
    ON = 1


class AnnualCapMode(IntEnum):
    """ReEDS annual-emissions-cap switch modes."""

    DISABLED = 0
    CO2 = 1
    CO2E = 2
    CO2E_WITH_HYDROGEN = 3

    @property
    def emission_type(self) -> EmissionType | None:
        """Return the emission type represented by this annual-cap mode."""
        if self is AnnualCapMode.DISABLED:
            return None
        if self is AnnualCapMode.CO2:
            return EmissionType.CO2
        return EmissionType.CO2E


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


class TransmissionLineType(StrEnum):
    """Transmission technologies represented in ReEDS line tables."""

    AC = "AC"
    LCC = "LCC"
    VSC = "VSC"
    B2B = "B2B"

    @classmethod
    def _missing_(cls, value: object) -> TransmissionLineType | None:
        """Accept source values regardless of their letter case."""
        if isinstance(value, str):
            normalized = value.upper()
            return next((member for member in cls if member.value == normalized), None)
        return None
