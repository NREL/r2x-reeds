"""Base models for ReEDS components."""

from __future__ import annotations

from typing import Annotated

from infrasys import Component
from infrasys.models import InfraSysBaseModel
from pydantic import Field


class ReEDSComponent(Component):
    """Base class for ReEDS components with common metadata fields.

    Note: Version information should be stored at the System level using
    system.data_format_version, not on individual components.
    """

    category: Annotated[str | None, Field(description="Technology category")] = None


class FromTo_ToFrom(InfraSysBaseModel):  # noqa: N801
    """Bidirectional flow capacity model.

    Represents capacity limits in both directions between two regions or nodes.
    Used for transmission lines and interfaces in ReEDS models.
    """

    from_to: Annotated[float, Field(description="Capacity from origin to destination in MW", ge=0)]
    to_from: Annotated[float, Field(description="Capacity from destination to origin in MW", ge=0)]
