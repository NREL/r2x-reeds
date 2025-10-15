from pathlib import Path

import polars as pl

from r2x_core.upgrader import DataUpgrader
from r2x_core.versioning import GitVersioningStrategy


class ReedsDataUpgrader(DataUpgrader):
    strategy = GitVersioningStrategy()

    @staticmethod
    def detect_from_csv(folder: Path) -> str | None:
        csv_path = folder / "meta.csv"
        if csv_path.exists():
            df = pl.read_csv(csv_path)
            version_row = df.select(pl.col("commit"))
            if len(version_row) > 0:
                return str(version_row["commit"][0])
        return None
