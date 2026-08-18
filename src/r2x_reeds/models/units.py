"""Unit types for ReEDS model components.

Uses r2x_core.units for unit annotations.
"""

from typing import Annotated, TypeAlias

from pydantic import Field

EmissionRate = Annotated[float, Field(description="Emission rate in kg/MWh", ge=0)]
Percentage = Annotated[float, Field(description="Percentage value (0-100)", ge=0, le=100)]
PlanningYear: TypeAlias = Annotated[
    int,
    Field(ge=1, description="ReEDS solve or resource-availability year"),
]
