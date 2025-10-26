"""Upgrades for ReEDS data."""

from pathlib import Path
from typing import Any

from loguru import logger

from r2x_core.upgrader import UpgradeType
from r2x_reeds.upgrader.helpers import LATEST_COMMIT

from .data_upgrader import ReEDSUpgrader


@ReEDSUpgrader.register_upgrade_step(target_version=LATEST_COMMIT, upgrade_type=UpgradeType.FILE, priority=30)
def move_hmap_file(folder: Path, upgrader_context: dict[str, Any] | None = None):
    """Move hmap to new folder."""
    old_location = folder / "inputs_case/hmap_allyrs.csv"
    if not old_location.exists():
        raise FileNotFoundError(f"File {old_location} does not exist.")
    new_location = folder / "inputs_case/rep/hmap_allyrs.csv"
    old_location.rename(new_location)
    logger.debug("Moved {} to {}", old_location.name, new_location)
    return
