"""Base models and reusable directional/range value objects."""

from __future__ import annotations

from typing import Annotated

from infrasys import Component
from infrasys.models import InfraSysBaseModel
from pydantic import Field, model_validator

from r2x_core.units import Unit

from .types import Fraction, NonNegativeFloat


class ReEDSComponent(Component):
    """Base class for ReEDS components with common metadata."""

    category: Annotated[str | None, Field(min_length=1, description="Technology category")] = None
    ext: dict[str, object] = Field(
        default_factory=dict,
        description="Additional serializable metadata for the component.",
    )


class FromTo_ToFrom(InfraSysBaseModel):  # noqa: N801
    """Nonnegative power-capacity limits in both transfer directions."""

    from_to: Annotated[
        float,
        Unit("MW"),
        Field(ge=0.0, description="Capacity from origin to destination"),
    ]
    to_from: Annotated[
        float,
        Unit("MW"),
        Field(ge=0.0, description="Capacity from destination to origin"),
    ]


class UpDown(InfraSysBaseModel):
    """Nonnegative values that differ between upward and downward directions."""

    up: NonNegativeFloat
    down: NonNegativeFloat


class FractionRange(InfraSysBaseModel):
    """Inclusive unit-interval range with an ordered lower and upper bound."""

    min: Annotated[Fraction, Field(description="Lower bound")]
    max: Annotated[Fraction, Field(description="Upper bound")]

    @model_validator(mode="after")
    def validate_order(self) -> FractionRange:
        """Require the lower bound not to exceed the upper bound."""
        if self.min > self.max:
            raise ValueError("min must be <= max")
        return self


class NonNegativeRange(InfraSysBaseModel):
    """Inclusive nonnegative range for quantities such as power in MW."""

    min: Annotated[NonNegativeFloat, Field(description="Lower bound")]
    max: Annotated[NonNegativeFloat, Field(description="Upper bound")]

    @model_validator(mode="after")
    def validate_order(self) -> NonNegativeRange:
        """Require the lower bound not to exceed the upper bound."""
        if self.min > self.max:
            raise ValueError("min must be <= max")
        return self


class MinMax(FractionRange):
    """Compatibility name for the unit-interval range used by capacity factors."""
