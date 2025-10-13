"""Compilation of functions that handle upgrades.

This functions apply an update function to certain raw files file before using them for creating the System.
"""

import io
import os
import pathlib
import shutil
from collections import OrderedDict
from importlib.resources import files

import polars as pl
from loguru import logger

from r2x_reeds.upgrader.helpers import get_function_arguments


def read_csv(fname: str, package_data: str = "r2x_reeds.config", **kwargs) -> pl.LazyFrame:
    """Helper function to read csv string data from package data.

    Args:
        fname: Name of the csv file
        package_data: Location of file in package. Default location is r2x_reeds.config
        **kwargs: Additional keys passed to pandas read_csv function

    Returns
    -------
        A pandas dataframe of the csv requested
    """
    csv_file = files(package_data).joinpath(fname).read_text(encoding="utf-8-sig")
    return pl.LazyFrame(pl.read_csv(io.StringIO(csv_file), **kwargs))


def move_file(fpath: pathlib.Path, new_fpath: str | pathlib.Path) -> pathlib.Path | None:
    """Move a file to a different location.

    This function moves a file from its current location to a new specified
    location using `shutil.move`. If the new path is a string, it will be
    interpreted relative to the parent directory of the original file.

    Parameters
    ----------
    fpath : pathlib.Path
        The path of the file to be moved.
    new_fpath : str or pathlib.Path
        The destination path where the file should be moved. If a string is
        provided, it is interpreted relative to the parent directory of the
        original file.

    Returns
    -------
    pathlib.Path | None
        The new path of the moved file if the move was successful. Returns
        None if the file already exists at the destination.

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.
    ValueError
        If the new path resolves to a location that already contains a file
        with the same name.

    Examples
    --------
    >>> from r2x_reeds.upgrader.functions import move_file
    >>> import pathlib
    >>> move_file(pathlib.Path("/path/to/file.txt"), "new_location/file.txt")

    >>> move_file(pathlib.Path("/path/to/non_existent_file.txt"), "new_location/file.txt")
    Traceback (most recent call last):
        ...
    FileNotFoundError: /path/to/non_existent_file.txt does not exist.

    >>> move_file(pathlib.Path("/path/to/existing_file.txt"), "existing_file.txt")
    DEBUG:__main__:File existing_file.txt already exists in the right place.
    """
    logger.debug(f"Moving {fpath} to {new_fpath}")

    if not fpath.exists():
        raise FileNotFoundError(f"{fpath} does not exist.")

    if not isinstance(new_fpath, pathlib.Path):
        new_fpath = fpath.parent.parent / new_fpath

    os.makedirs(os.path.dirname(new_fpath), exist_ok=True)

    if os.path.exists(new_fpath):
        logger.debug(f"File {fpath.name} already exists in the right place.")
        return None

    shutil.move(str(fpath), str(new_fpath))
    return new_fpath


def upgrade_handler(run_folder: str | pathlib.Path):
    """Entry point to call the different upgrade functions."""
    logger.info("Starting upgrader")

    # The file tracker has all the information of what update to perform for each data file.
    file_tracker = read_csv("file_tracker.csv", package_data="r2x_reeds.upgrader").collect().to_pandas()
    files_to_modify = file_tracker["fname"].unique()

    # This might actually not be safe for the nas.
    f_dict = OrderedDict(
        {
            f.name: f
            for f in pathlib.Path(run_folder).glob("*[inputs_case|outputs]/*")
            if f.name in files_to_modify
        }
    )

    for fname, f_group in file_tracker.groupby("fname", sort=False):
        if fname not in f_dict:
            logger.debug(f"{fname} not in inputs_case_list. Skipping it.")
            continue

        fpath_name = f_dict[fname]
        assert fpath_name
        functions_to_apply = f_group["method"].iloc[0].split(",")  # List of functions
        f_group_dict = f_group.to_dict(orient="records")[
            0
        ]  # Records return a list of dicts. We jsut get the first element
        f_group_dict["fpath"] = fpath_name

        for function in functions_to_apply:
            function_callable = globals()[function]
            function_arguments = get_function_arguments(f_group_dict, function_callable)
            function_callable(**function_arguments)
            # Update f_dict if we renamed a file to apply additional functions to it
            if function == "rename":
                fpath_new = fpath_name.parent.joinpath(f_group_dict["new_fname"][0])
                f_dict[fpath_new.name] = fpath_new
