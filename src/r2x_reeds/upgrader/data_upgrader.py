"""Data Uprader for ReEDS."""

from pathlib import Path
from typing import Any

from r2x_core import GitVersioningStrategy, PluginUpgrader, UpgradeStep
from r2x_core.versioning import VersionDetector
from r2x_reeds.upgrader.helpers import COMMIT_HISTORY


class ReEDSVersionDetector(VersionDetector):
    """Version detector class for ReEDS."""

    def __init__(self) -> None:
        """Initialize ReEDS version detection."""
        super().__init__()

    def detect_version(self, folder_path: Path) -> str | None:
        """Read ReEDS model version."""
        import csv

        folder_path = Path(folder_path)

        csv_path = folder_path / "meta.csv"
        if not csv_path.exists():
            msg = f"ReEDS version file {csv_path} not found."
            return FileNotFoundError(msg)

        with open(csv_path) as f:
            reader = csv.reader(f)
            next(reader)  # Skip header row
            second_row = next(reader)
            assert len(second_row) == 5, "meta file format changed."
            return second_row[3]


class ReEDSUpgrader(PluginUpgrader):
    """Upgrader class for ReEDS files."""

    def __init__(
        self,
        folder_path: Path | str,
        steps: list[UpgradeStep] | None = None,
        version: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize ReEDS upgrader."""
        strategy = GitVersioningStrategy(commit_history=COMMIT_HISTORY)
        version_detector = ReEDSVersionDetector()
        super().__init__(
            folder_path=folder_path,
            strategy=strategy,
            steps=steps,
            version=version,
            version_detector=version_detector,
        )
