"""Prepare planning input frames after DataStore processing."""

from __future__ import annotations

import polars as pl


def build_planning_periods_frame(
    modeled_years: pl.DataFrame,
    present_value_factors: pl.DataFrame,
    emission_caps: pl.DataFrame | None,
) -> pl.DataFrame:
    """Join canonical planning-period inputs into model-ready rows."""
    periods = modeled_years.select(pl.col("modeled_years").cast(pl.Int64).alias("year"))
    periods = periods.join(
        present_value_factors.select("year", "present_value_factor"),
        on="year",
        how="left",
    )
    if emission_caps is None:
        periods = periods.with_columns(pl.lit(None, dtype=pl.Float64).alias("emission_cap"))
    else:
        periods = periods.join(
            emission_caps.select("year", pl.col("value").alias("emission_cap")),
            on="year",
            how="left",
        )
    if periods.is_empty():
        raise ValueError("modeled_years contains no planning years")
    return periods


def build_planning_representative_timepoints_frame(
    representative_timepoints: pl.DataFrame,
) -> pl.DataFrame:
    """Add zero-based positions to mapped representative timepoints."""
    result = representative_timepoints.select("label", "weight").with_row_index("position")
    if result.is_empty():
        raise ValueError("planning_representative_timepoints contains no rows")
    return result


def build_planning_plant_characteristics_frame(
    plant_characteristics: pl.DataFrame,
    *,
    planning_years: tuple[int, ...],
) -> pl.DataFrame:
    """Filter and pivot canonical long-form plant-characteristic rows."""
    filtered = plant_characteristics.filter(pl.col("year").is_in(planning_years))
    if filtered.is_empty():
        raise ValueError("planning_plant_characteristics has no records for modeled years")
    try:
        pivoted = filtered.pivot(
            on="variable",
            index=["technology", "year"],
            values="value",
        )
    except pl.exceptions.ComputeError as exc:
        raise ValueError("planning_plant_characteristics has duplicate variable values") from exc

    optional_zero_fields = [field for field in ("heatrate", "rte") if field in pivoted.columns]
    if optional_zero_fields:
        pivoted = pivoted.with_columns(
            [
                pl.when(pl.col(field) == 0.0)
                .then(None)
                .otherwise(pl.col(field))
                .alias(field)
                for field in optional_zero_fields
            ]
        )
    return pivoted


def build_planning_initial_capacity_frame(
    power_capacity: pl.DataFrame,
    energy_capacity: pl.DataFrame | None,
) -> pl.DataFrame:
    """Join mapped power and optional energy initial-capacity tables."""
    power = power_capacity.select(
        pl.col("technology").cast(pl.String).str.strip_chars().alias("technology"),
        pl.col("region").cast(pl.String).str.strip_chars().alias("region"),
        pl.col("capacity").alias("initial_power_capacity"),
    )
    if power.is_empty():
        raise ValueError("existing_capacity contains no rows")
    if energy_capacity is None or energy_capacity.is_empty():
        return power.with_columns(pl.lit(None, dtype=pl.Float64).alias("initial_energy_capacity"))

    energy = energy_capacity.select(
        pl.col("technology").cast(pl.String).str.strip_chars().alias("technology"),
        pl.col("region").cast(pl.String).str.strip_chars().alias("region"),
        pl.col("energy_capacity").alias("initial_energy_capacity"),
    )
    orphaned = energy.join(
        power.select("technology", "region").unique(), on=["technology", "region"], how="anti"
    )
    if not orphaned.is_empty():
        key = orphaned.select("technology", "region").row(0)
        raise ValueError(f"existing_energy_capacity has no matching power capacity for {key}")
    return power.join(energy, on=["technology", "region"], how="left")
