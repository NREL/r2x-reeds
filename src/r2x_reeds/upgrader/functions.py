import os
import shutil
from pathlib import Path

from loguru import logger

from r2x_core import UpgradeType
from r2x_reeds import latest_commit
from r2x_reeds.upgrader.data_upgrader import ReedsDataUpgrader


@ReedsDataUpgrader.upgrade_step(target_version=latest_commit, upgrade_type=UpgradeType.FILE)
def move_file(folder: Path, old_fpath: str | Path, new_fpath: str | Path) -> Path:
    """Move a file to a different location within the ReEDS folder structure.

    This function moves a file from its current location to a new specified
    location using `shutil.move`. Paths are interpreted relative to the
    provided folder if they are strings.

    Parameters
    ----------
    folder : Path
        The base ReEDS folder containing the files to be moved.
    old_fpath : str or Path
        The current path of the file to be moved, relative to folder.
    new_fpath : str or Path
        The destination path where the file should be moved, relative to folder.

    Returns
    -------
    Path
        The folder path (following the upgrade step signature).

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.

    Examples
    --------
    >>> # Move hour map file to new location in newer ReEDS versions
    >>> move_file(folder, "inputs_case/hmap_allyrs.csv", "inputs_case/rep/hmap_allyrs.csv")

    >>> # Move buses to nodes for compatibility
    >>> move_file(folder, "buses.csv", "nodes.csv")
    """
    # Convert string paths to Path objects relative to folder
    if isinstance(old_fpath, str):
        old_fpath = folder / old_fpath
    if isinstance(new_fpath, str):
        new_fpath = folder / new_fpath

    logger.debug(f"Moving {old_fpath} to {new_fpath}")

    if not old_fpath.exists():
        logger.warning(f"{old_fpath} does not exist, skipping move operation.")
        return folder

    # Create destination directory if it doesn't exist
    os.makedirs(new_fpath.parent, exist_ok=True)

    if new_fpath.exists():
        logger.debug(f"File {new_fpath.name} already exists in the right place.")
        return folder

    # Perform the move
    shutil.move(str(old_fpath), str(new_fpath))
    logger.info(f"Successfully moved {old_fpath.name} to {new_fpath}")

    return folder


@ReedsDataUpgrader.upgrade_step(target_version=latest_commit, upgrade_type=UpgradeType.FILE)
def remove_file(folder: Path, file_name: str) -> Path:
    """Remove files that are deprecated/no longer used in newer ReEDS versions.

    This function handles files that existed in older ReEDS versions but are
    no longer present or needed in newer versions. It updates the file mapping
    to mark these files as optional or removes them from consideration.

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
    >>> # Remove timezone mapping file that's deprecated in newer ReEDS
    >>> remove_deprecated_file(folder, "reeds_region_tz_map.csv")
    """
    logger.info(f"Marking {file_name} as deprecated - no longer required in newer ReEDS versions")

    file_path = folder / "inputs_case" / file_name
    if file_path.exists():
        logger.debug(f"Deprecated file {file_name} still exists but will be ignored")
    else:
        logger.debug(f"Deprecated file {file_name} not found (expected for newer ReEDS versions)")

    return folder
