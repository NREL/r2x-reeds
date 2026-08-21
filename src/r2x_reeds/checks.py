"""Validation functions used across the ReEDS plugin."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
from pydantic import ValidationInfo
from rust_ok import Err, Ok, Result

from r2x_core.exceptions import ValidationError

if TYPE_CHECKING:
    from r2x_core import DataStore


def validate_available_years_are_unique_and_ascending(
    years: tuple[int, ...],
) -> tuple[int, ...]:
    """Require candidate availability years to be unique and ascending."""
    if years != tuple(sorted(years)) or len(set(years)) != len(years):
        raise ValueError("available_years must be unique and ascending")
    return years


def validate_maximum_unit_float_is_not_less_than_minimum(
    maximum: float,
    info: ValidationInfo,
) -> float:
    """Require a unit-interval maximum to be at least its preceding minimum."""
    minimum = info.data.get("min")
    if minimum is not None and maximum < minimum:
        raise ValueError("min must be <= max")
    return maximum


def validate_minimum_generation_fraction_does_not_exceed_capacity_factor(
    minimum_generation_fraction: float,
    info: ValidationInfo,
) -> float:
    """Require the hourly minimum generation fraction to be feasible."""
    capacity_factor = info.data.get("capacity_factor")
    if capacity_factor is not None and minimum_generation_fraction > capacity_factor:
        raise ValueError("minimum_generation_fraction must not exceed capacity_factor")
    return minimum_generation_fraction


def validate_minimum_capacity_factor_does_not_exceed_capacity_factor(
    minimum_capacity_factor: float,
    info: ValidationInfo,
) -> float:
    """Require the annual minimum capacity factor to be feasible."""
    capacity_factor = info.data.get("capacity_factor")
    if capacity_factor is not None and minimum_capacity_factor > capacity_factor:
        raise ValueError("minimum_capacity_factor must not exceed capacity_factor")
    return minimum_capacity_factor


def validate_optional_nonnegative(value: float | None) -> float | None:
    """Validate an optional quantity whose present value cannot be negative."""
    if value is not None and value < 0.0:
        raise ValueError("value must be greater than or equal to 0")
    return value


def validate_optional_positive(value: float | None) -> float | None:
    """Validate an optional quantity whose present value must be positive."""
    if value is not None and value <= 0.0:
        raise ValueError("value must be greater than 0")
    return value


def validate_optional_fraction(value: float | None) -> float | None:
    """Validate an optional value in the inclusive unit interval."""
    if value is not None and not 0.0 <= value <= 1.0:
        raise ValueError("value must be between 0 and 1")
    return value


# Capacity-expansion validation ---------------------------------------------


def check_planning_inputs_available(
    store: DataStore,
    required_names: Iterable[str],
) -> Result[bool, ValidationError]:
    """Check whether planning inputs are absent, complete, or incomplete."""
    names = tuple(required_names)
    present: dict[str, bool] = {}
    for name in names:
        if name not in store:
            present[name] = False
            continue
        data_file = store[name]
        if data_file.fpath is not None:
            present[name] = data_file.fpath.exists()
        elif data_file.relative_fpath is not None:
            present[name] = (store.folder / Path(data_file.relative_fpath)).exists()
        else:
            present[name] = data_file.glob is not None and any(store.folder.glob(data_file.glob))

    anchor = "planning_plant_characteristics"
    if not present.get(anchor, False):
        return Ok(False)
    missing = sorted(name for name, exists in present.items() if not exists)
    if missing:
        return Err(ValidationError(f"Planning inputs are incomplete; missing: {missing}"))
    return Ok(True)


def check_planning_years(frame: pl.DataFrame) -> Result[tuple[int, ...], ValidationError]:
    """Validate and return the ordered modeled planning years."""
    if "modeled_years" not in frame.columns:
        return Err(ValidationError("modeled_years is missing required columns ['modeled_years']"))

    years: list[int] = []
    for value in frame["modeled_years"].to_list():
        try:
            numeric_year = float(value)
            year = int(numeric_year)
        except (TypeError, ValueError, OverflowError):
            return Err(ValidationError("modeled_years has a non-integral modeled_years"))
        if numeric_year != year:
            return Err(ValidationError("modeled_years has a non-integral modeled_years"))
        years.append(year)
    if not years:
        return Err(ValidationError("modeled_years contains no planning years"))
    if len(set(years)) != len(years):
        return Err(ValidationError("modeled_years contains duplicate planning years"))
    return Ok(tuple(years))


def check_planning_switches(
    frame: pl.DataFrame,
    *,
    source_names: Iterable[str],
) -> Result[dict[str, object], ValidationError]:
    """Validate planning-switch rows and return recognized values."""
    if not {"switch", "value"}.issubset(frame.columns):
        return Err(ValidationError("planning_switches is missing required columns ['switch', 'value']"))

    recognized = {str(name).casefold(): str(name) for name in source_names}
    values: dict[str, object] = {}
    for row in frame.iter_rows(named=True):
        normalized_name = str(row["switch"]).strip().casefold()
        name = recognized.get(normalized_name)
        if name is None:
            continue
        if name in values:
            return Err(ValidationError(f"planning_switches must contain exactly one {name} value"))
        values[name] = row["value"]
    missing = sorted(set(recognized.values()) - set(values))
    if missing:
        return Err(ValidationError(f"planning_switches is missing required switches: {missing}"))
    return Ok(values)


def check_planning_periods(
    frame: pl.DataFrame,
    *,
    emission_type: object | None,
) -> Result[None, ValidationError]:
    """Validate present-value factors and active annual emissions caps."""
    missing_factor_years = frame.filter(pl.col("present_value_factor").is_null())["year"].to_list()
    if missing_factor_years:
        return Err(
            ValidationError(
                f"planning_present_value_factors is missing modeled years {missing_factor_years}"
            )
        )
    if emission_type is not None:
        missing_cap_years = frame.filter(pl.col("emission_cap").is_null())["year"].to_list()
        if missing_cap_years:
            return Err(ValidationError(f"co2_cap is missing modeled years {missing_cap_years}"))
    return Ok(None)


def check_plant_characteristics_source(
    frame: pl.DataFrame,
    *,
    source_variables: set[str],
) -> Result[None, ValidationError]:
    """Validate long-form plant-characteristic columns and variables."""
    columns_result = check_columns_present(
        frame,
        "planning_plant_characteristics",
        {"technology", "year", "variable", "value"},
    )
    if columns_result.is_err():
        return columns_result
    unsupported = set(frame["variable"].unique()) - source_variables
    if unsupported:
        return Err(
            ValidationError(
                f"planning_plant_characteristics has unsupported variable '{sorted(unsupported)[0]}'"
            )
        )
    return Ok(None)


def check_plant_characteristics_values(
    frame: pl.DataFrame,
    *,
    required_variables: set[str],
) -> Result[None, ValidationError]:
    """Validate required values in pivoted plant-characteristic rows."""
    if frame.is_empty():
        return Err(ValidationError("planning_plant_characteristics has no records for modeled years"))
    for row in frame.iter_rows(named=True):
        missing = {variable for variable in required_variables if row.get(variable) is None}
        if missing:
            return Err(
                ValidationError(
                    "planning_plant_characteristics is missing variables "
                    f"{sorted(missing)} for technology '{row['technology']}', year {row['year']}"
                )
            )
    return Ok(None)


def check_storage_duration_rows(frame: pl.DataFrame) -> Result[None, ValidationError]:
    """Validate technology-level storage-duration rows."""
    columns_result = check_columns_present(
        frame,
        "planning_storage_durations",
        {"technology", "duration"},
    )
    if columns_result.is_err():
        return columns_result
    technologies = frame["technology"].cast(pl.String).str.strip_chars()
    if technologies.n_unique() != frame.height:
        return Err(ValidationError("planning_storage_durations has duplicate duration"))
    return Ok(None)


def check_single_duration(frame: pl.DataFrame, dataset: str) -> Result[None, ValidationError]:
    """Validate a mapped dataset containing exactly one duration value."""
    if frame.height != 1 or "duration" not in frame.columns:
        return Err(ValidationError(f"{dataset} must contain exactly one duration"))
    return Ok(None)


def check_storage_duration_override_rows(frame: pl.DataFrame) -> Result[None, ValidationError]:
    """Validate regional and vintage-specific storage-duration rows."""
    columns_result = check_columns_present(
        frame,
        "planning_storage_duration_overrides",
        {"technology", "vintage", "region", "duration"},
    )
    if columns_result.is_err():
        return columns_result
    keys = frame.select("technology", "vintage", "region")
    if keys.unique().height != frame.height:
        return Err(
            ValidationError(
                "planning_storage_duration_overrides must be unique by technology, vintage, and region"
            )
        )
    return Ok(None)


def check_initial_capacity_rows(frame: pl.DataFrame) -> Result[None, ValidationError]:
    """Validate initial-capacity rows before linking them to region components."""
    columns_result = check_columns_present(
        frame,
        "existing_capacity",
        {"technology", "region", "initial_power_capacity", "initial_energy_capacity"},
    )
    if columns_result.is_err():
        return columns_result
    keys = frame.select("technology", "region")
    if keys.unique().height != frame.height:
        return Err(ValidationError("existing_capacity must be unique by technology and region"))
    invalid_energy = frame.filter(
        pl.col("initial_energy_capacity").is_not_null()
        & (pl.col("initial_energy_capacity") > 0)
        & (pl.col("initial_power_capacity") <= 0)
    )
    if not invalid_energy.is_empty():
        return Err(
            ValidationError(
                "existing_energy_capacity positive values require positive initial_power_capacity"
            )
        )
    return Ok(None)


def check_representative_timepoints(frame: pl.DataFrame) -> Result[None, ValidationError]:
    """Validate the global representative chronology rows."""
    columns_result = check_columns_present(
        frame,
        "planning_representative_timepoints",
        {"label", "position", "weight"},
    )
    if columns_result.is_err():
        return columns_result
    if frame["label"].n_unique() != frame.height:
        return Err(ValidationError("representative_timepoints must have unique labels"))
    positions = frame["position"].to_list()
    if positions != list(range(len(positions))):
        return Err(
            ValidationError(
                "representative_timepoints must have contiguous positions starting at zero"
            )
        )
    if frame.filter(pl.col("weight") <= 0).height:
        return Err(ValidationError("representative_timepoints weights must be positive"))
    return Ok(None)


# DataStore and tabular-input validation ------------------------------------


def check_columns_present(
    frame: pl.DataFrame,
    dataset: str,
    required_columns: Iterable[str],
) -> Result[None, ValidationError]:
    """Check that a collected dataset contains all required columns."""
    missing = sorted(set(required_columns) - set(frame.columns))
    if missing:
        return Err(
            ValidationError(
                f"Dataset {dataset!r} is missing required columns: {missing}. "
                f"Available columns: {frame.columns}"
            )
        )
    return Ok(None)


def check_dataset_non_empty(
    store: DataStore,
    name: str,
    *,
    placeholders: dict[str, Any] | None = None,
) -> Result[None, ValidationError]:
    """Check that a mapped DataStore dataset exists and contains rows."""
    if name not in store:
        return Err(ValidationError(f"Key {name} not found in data store. Check spelling."))

    datafile_metadata = store[name]
    raw_fpath = datafile_metadata.fpath
    if raw_fpath is None:
        absolute_fpath = store.folder / "<unknown>"
    else:
        fpath = Path(raw_fpath)
        absolute_fpath = fpath if fpath.is_absolute() else store.folder / fpath

    try:
        data = store.read_data(name, placeholders=placeholders)
        if data is None:
            return Err(ValidationError(f"Dataset {name!r} is unavailable from {absolute_fpath}"))
        frame = data.limit(1).collect() if hasattr(data, "limit") else data.head(1)
    except OSError as exc:
        if absolute_fpath.suffix.lower() == ".h5" and "file signature not found" in str(exc).lower():
            return Err(
                ValidationError(
                    f"Dataset {name!r} failed to read from {absolute_fpath}: invalid HDF5 signature. "
                    "The file is not a valid HDF5 binary (often zero-filled/corrupted or mislabeled). "
                    "Re-copy/regenerate this .h5 from the ReEDS run output."
                )
            )
        return Err(ValidationError(f"Failed reading dataset {name!r} from {absolute_fpath}: {exc}"))
    except (KeyError, TypeError, ValueError, pl.exceptions.PolarsError) as exc:
        return Err(ValidationError(f"Failed reading dataset {name!r} from {absolute_fpath}: {exc}"))

    if not frame.is_empty():
        return Ok(None)
    return Err(
        ValidationError(f"modeled_years data is empty. Check that file {datafile_metadata.fpath} has data.")
    )


def check_column_exists(
    store: DataStore,
    dataset: str,
    column: str,
    *,
    placeholders: dict[str, Any] | None = None,
) -> Result[None, ValidationError]:
    """Check that a mapped dataset contains one named column."""
    result = check_dataset_non_empty(store, dataset, placeholders=placeholders)
    if result.is_err():
        return result
    try:
        data = store.read_data(dataset, placeholders=placeholders)
        columns = data.collect_schema().names()
    except (KeyError, TypeError, ValueError, pl.exceptions.PolarsError) as exc:
        return Err(ValidationError(f"Failed reading schema for dataset {dataset!r}: {exc}"))
    if column not in columns:
        metadata = store[dataset]
        return Err(
            ValidationError(
                f"Column {column!r} not found in dataset {dataset!r} from file {metadata.fpath}. "
                f"Available columns: {columns}"
            )
        )
    return Ok(None)


def check_required_values_in_column(
    *,
    store: DataStore,
    dataset: str,
    required_values: Iterable[Any],
    column_name: str | None = None,
    what: str | None = None,
    placeholders: dict[str, Any] | None = None,
) -> Result[None, ValidationError]:
    """Check that all required values occur in a mapped dataset column."""
    column = column_name or dataset
    result = check_column_exists(store, dataset, column, placeholders=placeholders)
    if result.is_err():
        return result
    try:
        data = store.read_data(dataset, placeholders=placeholders)
        available_values = data.select(column).unique().collect()[column].to_list()
    except (KeyError, TypeError, ValueError, pl.exceptions.PolarsError) as exc:
        return Err(ValidationError(f"Failed reading values from dataset {dataset!r}: {exc}"))

    required = (
        list(required_values)
        if isinstance(required_values, Iterable) and not isinstance(required_values, str | bytes)
        else [required_values]
    )
    missing = [value for value in required if value not in available_values]
    if not missing:
        return Ok(None)

    metadata = store[dataset]
    label = what or dataset
    try:
        available = sorted(available_values)
    except TypeError:
        available = available_values
    return Err(
        ValidationError(
            f"{label} {missing} not found in {metadata.fpath} ({dataset}.{column}). "
            f"Available values: {available}"
        )
    )
