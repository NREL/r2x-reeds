"""Unit types and type aliases for ReEDS model components.

These are simplified type aliases for ReEDS-specific units.
For full Pint unit support (needed for Sienna conversion), use the conversion utilities.
"""

from typing import Annotated

from pydantic import Field

# Define unit types as annotated floats with validation
EmissionRate = Annotated[
    float,
    Field(description="Emission rate in kg/MWh", ge=0),
]

Percentage = Annotated[
    float,
    Field(description="Percentage value (0-100)", ge=0, le=100),
]

# Type aliases for common ReEDS units (as plain floats)
PowerMW = Annotated[float, Field(description="Power in MW", ge=0)]
EnergyMWh = Annotated[float, Field(description="Energy in MWh", ge=0)]
TimeHours = Annotated[float, Field(description="Time duration in hours", ge=0)]
