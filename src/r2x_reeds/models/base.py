"""Base models for ReEDS components."""

from __future__ import annotations

from typing import Annotated

from infrasys import Component
from infrasys.models import InfraSysBaseModel
from pydantic import Field, PositiveFloat

from .types import MaximumUnitFloat, NonNegativeFloat, UnitFloat


class ReEDSComponent(Component):
    """Base class for ReEDS components with common metadata fields.

    Provides an extensible ext field for storing additional metadata and component
    information that doesn't fit into standard fields.

    Attributes
    ----------
    category : str, optional
        Technology category that this component belongs to.
    ext : dict
        Additional information and metadata for the component. Can store any
        serializable key-value pairs for extended functionality.

    Notes
    -----
    Version information should be stored at the System level using
    system.data_format_version, not on individual components.
    """

    category: Annotated[str | None, Field(description="Technology category")] = None
    ext: dict = Field(
        default_factory=dict, description="Additional information and metadata for the component."
    )


class FromTo_ToFrom(InfraSysBaseModel):  # noqa: N801
    """Bidirectional flow capacity model.

    Represents capacity limits in both directions between two regions or nodes.
    Used for transmission lines and interfaces in ReEDS models.
    """

    from_to: Annotated[NonNegativeFloat, Field(description="Capacity from origin to destination in MW")]
    to_from: Annotated[NonNegativeFloat, Field(description="Capacity from destination to origin in MW")]


class UpDown(InfraSysBaseModel):
    """Bidirectional rate or value.

    Represents rates or values that differ in up and down directions,
    such as ramp rates or reserve provision capabilities.
    """

    up: PositiveFloat
    down: PositiveFloat


class MinMax(InfraSysBaseModel):
    """Min/Max bounds for operational parameters.

    Represents minimum and maximum bounds for operational parameters
    like capacity factors, flow rates, etc.
    """

    min: UnitFloat
    max: MaximumUnitFloat
