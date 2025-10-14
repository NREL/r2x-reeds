from pathlib import Path

from r2x_core import UpgradeType
from r2x_reeds import latest_commit
from r2x_reeds.upgrader.data_upgrader import ReedsDataUpgrader


@ReedsDataUpgrader.upgrade_step(target_version=latest_commit, upgrade_type=UpgradeType.FILE)
def move_hour_map(folder: Path) -> Path:
    (folder / "buses.csv").rename(folder / "nodes.csv")
    return folder