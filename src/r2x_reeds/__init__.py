"""R2X ReEDS Plugin.

A plugin for parsing ReEDS (Regional Energy Deployment System) model data
into the R2X framework using infrasys components.
"""

from importlib.metadata import version

from loguru import logger

__version__ = version("r2x_reeds")

from .config import ReEDSConfig
from .models import (
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
from .parser import ReEDSParser

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
]


def register_plugin() -> None:
    """Register the ReEDS plugin with the R2X plugin manager.

    This function is called automatically when the plugin is discovered
    via entry points. It registers the ReEDS parser, config, and optionally
    an exporter with the PluginManager.
    """
    from r2x_core.plugins import PluginManager

    from .config import ReEDSConfig
    from .parser import ReEDSParser

    PluginManager.register_model_plugin(
        name="reeds",
        config=ReEDSConfig,
        parser=ReEDSParser,
        exporter=None,  # Will be implemented later
    )
