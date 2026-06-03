"""Parser validation helpers used by the parser upgrade flow."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from r2x_core import Err, Ok, Result
from r2x_core.exceptions import ValidationError

if TYPE_CHECKING:
    from r2x_core import DataStore


def check_dataset_non_empty(
    store: DataStore,
    name: str,
    *,
    placeholders: dict[str, Any] | None = None,
) -> Result[None, ValidationError]:
    """Check if data exist.

    Notes
    -----
    No need to pass placeholds
    """
    if name not in store:
        msg = f"Key {name} not found in data store. Check spelling."
        return Err(ValidationError(msg))

    datafile_metadata = store[name]
    raw_fpath = datafile_metadata.fpath
    if raw_fpath is None:
        absolute_fpath = store.folder / "<unknown>"
    else:
        fpath = Path(raw_fpath)
        absolute_fpath = fpath if fpath.is_absolute() else store.folder / fpath

    try:
        data = store.read_data(name, placeholders=placeholders)
    except OSError as exc:
        if absolute_fpath.suffix.lower() == ".h5" and "file signature not found" in str(exc).lower():
            msg = (
                f"Dataset {name!r} failed to read from {absolute_fpath}: invalid HDF5 signature. "
                "The file is not a valid HDF5 binary (often zero-filled/corrupted or mislabeled). "
                "Re-copy/regenerate this .h5 from the ReEDS run output."
            )
            return Err(ValidationError(msg))

        msg = f"Failed reading dataset {name!r} from {absolute_fpath}: {exc}"
        return Err(ValidationError(msg))

    if not data.limit(1).collect().is_empty():
        return Ok()

    msg = f"modeled_years data is empty. Check that file {datafile_metadata.fpath} has data."
    return Err(ValidationError(msg))


def check_column_exists(
    store: DataStore,
    dataset: str,
    column: str,
    *,
    placeholders: dict[str, Any] | None = None,
) -> Result[None, ValidationError]:
    """Ensure `column` exists in `dataset`."""
    res = check_dataset_non_empty(store, dataset, placeholders=placeholders)
    if res.is_err():
        return res

    df = store.read_data(dataset, placeholders=placeholders)
    if column not in df.collect_schema().names():
        meta = store[dataset]
        msg = (
            f"Column {column!r} not found in dataset {dataset!r} "
            f"from file {meta.fpath}. "
            f"Available columns: {df.collect_schema().names()}"
        )
        return Err(ValidationError(msg))

    return Ok(None)


def check_required_values_in_column(
    *,
    store: DataStore,
    dataset: str,
    column_name: str | None = None,
    required_values: Iterable[Any],
    what: str | None = None,
    placeholders: dict[str, Any] | None = None,
) -> Result[None, ValidationError]:
    """Check that `required_values` are present in `dataset`.`column`."""
    res = check_dataset_non_empty(store, dataset, placeholders=placeholders)
    if res.is_err():
        return res

    res = check_column_exists(store, dataset, column_name or dataset, placeholders=placeholders)
    if res.is_err():
        return res

    df = store.read_data(dataset, placeholders=placeholders)
    meta = store[dataset]

    available_values = df.select(column_name or dataset).unique().collect()[column_name or dataset].to_list()
    if isinstance(required_values, Iterable) and not isinstance(required_values, str | bytes):
        required_list = list(required_values)
    else:
        required_list = [required_values]

    missing = [v for v in required_list if v not in available_values]
    if missing:
        label = what or dataset
        msg = (
            f"{label} {missing} not found in {meta.fpath} "
            f"({dataset}.{column_name or dataset}). "
            f"Available values: {sorted(available_values)}"
        )
        return Err(ValidationError(msg))

    return Ok(None)
