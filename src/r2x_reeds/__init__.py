"""R2X ReEDS Plugin.

A plugin for parsing ReEDS (Regional Energy Deployment System) model data
into the R2X framework using infrasys components.
"""

latest_commit = "680ae98df23486ff43199e62d95beba85debe6fa"

from importlib.metadata import version  # noqa: E402

from loguru import logger  # noqa: E402

from .plugins import register_plugin  # noqa: E402
from .upgrader.functions import move_hour_map  # noqa: E402

__version__ = version("r2x_reeds")

from .config import ReEDSConfig  # noqa: E402
from .models import (  # noqa: E402
    EmissionRate,
    EmissionType,
    EnergyMWh,
    FromTo_ToFrom,
    Percentage,
    PowerMW,
    ReEDSComponent,
    ReEDSDemand,
    ReEDSEmission,
    ReEDSGenerator,
    ReEDSInterface,
    ReEDSRegion,
    ReEDSReserve,
    ReEDSReserveRegion,
    ReEDSResourceClass,
    ReEDSTransmissionLine,
    ReserveDirection,
    ReserveType,
    TimeHours,
)
from .parser import ReEDSParser  # noqa: E402

# Disable default loguru handler for library usage
# Applications using this library should configure their own handlers
logger.disable("r2x_reeds")

__all__ = [
    "EmissionRate",
    "EmissionType",
    "EnergyMWh",
    "FromTo_ToFrom",
    "Percentage",
    "PowerMW",
    "ReEDSComponent",
    "ReEDSConfig",
    "ReEDSDemand",
    "ReEDSEmission",
    "ReEDSGenerator",
    "ReEDSInterface",
    "ReEDSParser",
    "ReEDSRegion",
    "ReEDSReserve",
    "ReEDSReserveRegion",
    "ReEDSResourceClass",
    "ReEDSTransmissionLine",
    "ReserveDirection",
    "ReserveType",
    "TimeHours",
    "__version__",
    "move_hour_map",
    "register_plugin",
]
