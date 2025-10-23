import os
import shutil
from collections import OrderedDict
from pathlib import Path

import pandas as pd
from loguru import logger

from r2x_core.upgrader import UpgradeType
from r2x_reeds import latest_commit
from r2x_reeds.upgrader.data_upgrader import ReedsDataUpgrader
from r2x_reeds.upgrader.helpers import get_function_arguments, read_csv


@ReedsDataUpgrader.upgrade_step(target_version=latest_commit, upgrade_type=UpgradeType.FILE)
def move_file(folder: Path, fpath: Path | None = None, new_fpath: str | None = None, **kwargs) -> Path:
    """Move a file to a different location within the ReEDS folder structure."""

    # Get the actual file path and new path from the file tracker data
    if fpath is None or new_fpath is None:
        logger.warning("move_file called without proper fpath or new_fpath")
        return folder

    old_fpath = fpath  # This comes from the file tracker
    new_fpath_full = folder / new_fpath  # This comes from file_tracker.csv new_fpath column

    logger.debug(f"Moving {old_fpath} to {new_fpath_full}")

    if not old_fpath.exists():
        logger.warning(f"{old_fpath} does not exist, skipping move operation.")
        return folder

    # Create destination directory if it doesn't exist
    os.makedirs(new_fpath_full.parent, exist_ok=True)

    if new_fpath_full.exists():
        logger.debug(f"File {new_fpath_full.name} already exists in the right place.")
        return folder

    # Perform the move
    shutil.move(str(old_fpath), str(new_fpath_full))
    logger.info(f"Successfully moved {old_fpath.name} to {new_fpath_full}")

    return folder


@ReedsDataUpgrader.upgrade_step(target_version=latest_commit, upgrade_type=UpgradeType.FILE)
def ignore_file(folder: Path, file_name: str) -> Path:
    """Ignore files that are deprecated/no longer used in newer ReEDS versions.

    This function handles files that existed in older ReEDS versions but are
    no longer present or needed in newer versions. It marks these files to be
    ignored during the parsing process without actually removing them from disk.

    Parameters
    ----------
    folder : Path
        The base ReEDS folder.
    file_name : str
        Name of the file to mark as deprecated (without path).

    Returns
    -------
    Path
        The folder path (following the upgrade step signature).

    Examples
    --------
    >>> # Ignore timezone mapping file that's deprecated in newer ReEDS
    >>> ignore_file(folder, "reeds_region_tz_map.csv")
    """
    logger.info(f"Ignoring {file_name} - no longer required in newer ReEDS versions")

    file_path = folder / "inputs_case" / file_name
    if file_path.exists():
        logger.debug(f"Deprecated file {file_name} exists but will be ignored")
    else:
        logger.debug(f"Deprecated file {file_name} not found (expected for newer ReEDS versions)")

    return folder


@ReedsDataUpgrader.upgrade_step(target_version=latest_commit, upgrade_type=UpgradeType.FILE)
def upgrade_handler(run_folder: str | Path) -> Path:
    """Entry point to call the different upgrade functions."""
    logger.info("Starting upgrader")

    # Convert to Path if string
    if isinstance(run_folder, str):
        run_folder = Path(run_folder)

    # The file tracker has all the information of what update to perform for each data file.
    file_tracker = read_csv("file_tracker.csv", package_data="r2x_reeds.upgrader").collect().to_pandas()
    files_to_modify = file_tracker["fname"].unique()

    # Look for files in both inputs_case and outputs directories
    f_dict = OrderedDict()

    # Search in inputs_case
    inputs_case_dir = run_folder / "inputs_case"
    if inputs_case_dir.exists():
        for f in inputs_case_dir.rglob("*"):
            if f.is_file() and f.name in files_to_modify:
                f_dict[f.name] = f

    # Search in outputs
    outputs_dir = run_folder / "outputs"
    if outputs_dir.exists():
        for f in outputs_dir.rglob("*"):
            if f.is_file() and f.name in files_to_modify:
                f_dict[f.name] = f

    for fname, f_group in file_tracker.groupby("fname", sort=False):
        if fname not in f_dict:
            logger.debug(f"{fname} not in file system. Skipping it.")
            continue

        fpath_name = f_dict[fname]
        method_str = f_group["method"].iloc[0]

        if pd.isna(method_str) or method_str == "":
            logger.debug(f"No method specified for {fname}, skipping")
            continue

        functions_to_apply = method_str.split(",")  # List of functions
        f_group_dict = f_group.to_dict(orient="records")[0]
        f_group_dict["fpath"] = fpath_name

        for function_name in functions_to_apply:
            function_name = function_name.strip()
            if function_name == "":
                continue

            if function_name in globals():
                function_callable = globals()[function_name]
                function_arguments = get_function_arguments(f_group_dict, function_callable)

                # Add the folder argument for our upgrade functions
                if function_name in ["move_file", "ignore_file"]:
                    function_arguments["folder"] = run_folder

                logger.info(f"Executing {function_name} for {fname}")
                function_callable(**function_arguments)
            else:
                logger.warning(f"Function {function_name} not found in globals()")

    return run_folder
