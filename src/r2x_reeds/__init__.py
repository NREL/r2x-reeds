"""R2X ReEDS Plugin.

A plugin for parsing ReEDS (Regional Energy Deployment System) model data
into the R2X framework using infrasys components.
"""

from importlib.metadata import version

from loguru import logger

__version__ = version("r2x_reeds")

# Import getters to register them with r2x-core
from . import getters  # noqa: F401
from .models import (
    EmissionRate,
    EmissionType,
    FromTo_ToFrom,
    Percentage,
    ReEDSCapacityExpansion,
    ReEDSCapacityExpansionInputs,
    ReEDSCapacityExpansionResource,
    ReEDSComponent,
    ReEDSConsumingTechnology,
    ReEDSDataCenterDemand,
    ReEDSDemand,
    ReEDSDispatchableCapacityExpansionResource,
    ReEDSElectrolyzerDemand,
    ReEDSEmission,
    ReEDSGenerator,
    ReEDSH2Pipeline,
    ReEDSH2Storage,
    ReEDSHydroGenerator,
    ReEDSInitialCapacity,
    ReEDSInterface,
    ReEDSPlanningPeriod,
    ReEDSPlantCharacteristics,
    ReEDSRegion,
    ReEDSRepresentativeTimepoint,
    ReEDSReserve,
    ReEDSReserveRegion,
    ReEDSResourceClass,
    ReEDSStorage,
    ReEDSStorageCapacityExpansionResource,
    ReEDSStorageDuration,
    ReEDSStorageDurationOverride,
    ReEDSThermalGenerator,
    ReEDSTransmissionLine,
    ReEDSVariableCapacityExpansionResource,
    ReEDSVariableGenerator,
    ReserveDirection,
    ReserveType,
)
from .parser import ReEDSParser
from .plugin_config import ReEDSConfig
from .upgrader import ReEDSUpgrader

logger.disable("r2x_reeds")

latest_commit = "401c0bb15cbf93d2ff9696b14b799edad763247a"

__all__ = [
    "EmissionRate",
    "EmissionType",
    "FromTo_ToFrom",
    "Percentage",
    "ReEDSCapacityExpansion",
    "ReEDSCapacityExpansionInputs",
    "ReEDSCapacityExpansionResource",
    "ReEDSComponent",
    "ReEDSConfig",
    "ReEDSConsumingTechnology",
    "ReEDSDataCenterDemand",
    "ReEDSDemand",
    "ReEDSDispatchableCapacityExpansionResource",
    "ReEDSElectrolyzerDemand",
    "ReEDSEmission",
    "ReEDSGenerator",
    "ReEDSH2Pipeline",
    "ReEDSH2Storage",
    "ReEDSHydroGenerator",
    "ReEDSInitialCapacity",
    "ReEDSInterface",
    "ReEDSParser",
    "ReEDSPlanningPeriod",
    "ReEDSPlantCharacteristics",
    "ReEDSRegion",
    "ReEDSRepresentativeTimepoint",
    "ReEDSReserve",
    "ReEDSReserveRegion",
    "ReEDSResourceClass",
    "ReEDSStorage",
    "ReEDSStorageCapacityExpansionResource",
    "ReEDSStorageDuration",
    "ReEDSStorageDurationOverride",
    "ReEDSThermalGenerator",
    "ReEDSTransmissionLine",
    "ReEDSUpgrader",
    "ReEDSVariableCapacityExpansionResource",
    "ReEDSVariableGenerator",
    "ReserveDirection",
    "ReserveType",
    "__version__",
]
