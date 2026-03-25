"""Upgrades for ReEDS data."""

from pathlib import Path
from typing import Any

from loguru import logger

from r2x_core import UpgradeStep, UpgradeType


def move_hmap_file(folder: Path, upgrader_context: dict[str, Any] | None = None) -> Path:
    """Move hmap to new folder.

    This upgrade step is idempotent - it safely handles being called multiple times
    by checking if the file has already been moved to its target location.
    """
    old_location = folder / "inputs_case/hmap_allyrs.csv"
    new_location = folder / "inputs_case/rep/hmap_allyrs.csv"

    if new_location.exists():
        logger.debug("File {} already exists at target location, skipping move", new_location.name)
        return folder

    if not old_location.exists():
        raise FileNotFoundError(
            f"File {old_location} does not exist and target {new_location} does not exist either."
        )

    old_location.rename(new_location)
    logger.debug("Moved {} to {}", old_location.name, new_location)
    return folder


def move_transmission_cost(folder: Path, upgrader_context: dict[str, Any] | None = None) -> Path:
    """Rename the legacy transmission distance/cost files to their new names."""
    rename_map = [
        ("inputs_case/transmission_distance_cost_500kVac.csv", "inputs_case/transmission_cost_ac.csv"),
        ("inputs_case/transmission_distance_cost_500kVdc.csv", "inputs_case/transmission_distance.csv"),
    ]

    for old_rel, new_rel in rename_map:
        old_path = folder / old_rel
        new_path = folder / new_rel

        if new_path.exists():
            logger.debug("Target {} already exists; skipping move", new_path.name)
            continue

        if not old_path.exists():
            logger.debug("Legacy file {} not found; skipping", old_path.name)
            continue

        old_path.rename(new_path)
        logger.debug("Moved legacy transmission file {} to {}", old_path.name, new_path.name)
    return folder


def create_hmap_myr(folder: Path, upgrader_context: dict[str, Any] | None = None) -> Path:
    """Derive inputs_case/rep/hmap_myr.csv from hmap_allyrs.csv if not already present.

    hmap_myr.csv maps sequential year-hours (1-8760) to representative period keys.
    It is required by the loadsite_op.csv expand logic to convert representative-period
    demand data to full 8760-hour profiles.

    The step is idempotent: it skips silently when the target file already exists.
    It also skips silently when hmap_allyrs.csv is absent (legacy runs that lack
    loadsite data do not need this file).

    Column resolution (per-row)
    ---------------------------
    - ``h``       : used when non-empty for that row (populated in newer ReEDS runs)
    - ``actual_h``: used as fallback when ``h`` is empty for that row (older fixtures)
    """
    import csv

    target = folder / "inputs_case/rep/hmap_myr.csv"
    source = folder / "inputs_case/rep/hmap_allyrs.csv"

    if target.exists():
        logger.debug("hmap_myr.csv already exists at {}, skipping", target)
        return folder

    if not source.exists():
        logger.debug("hmap_allyrs.csv not found at {}; skipping hmap_myr creation", source)
        return folder

    with open(source, newline="") as fh:
        reader = csv.DictReader(fh)
        raw_fieldnames = reader.fieldnames or []
        norm = {f: f.lstrip("*").strip() for f in raw_fieldnames}
        rows = [
            {
                norm.get(k, k): (v if v is not None else "")
                for k, v in row.items()
                if k is not None
            }
            for row in reader
        ]

    if not rows:
        logger.warning("hmap_allyrs.csv at {} is empty; skipping hmap_myr creation", source)
        return folder

    if "yearhour" not in rows[0]:
        logger.warning(
            "hmap_allyrs.csv missing 'yearhour' column; cannot create hmap_myr.csv"
        )
        return folder

    def _to_int(s: str) -> int | None:
        try:
            return int(s)
        except (ValueError, TypeError):
            return None

    # Derive the period key per-row: prefer h if non-empty, fall back to actual_h.
    # Deduplicate by yearhour, preferring a non-empty period over an empty one.
    out_map: dict[str, str] = {}  # yearhour -> best period found so far
    for r in rows:
        yh = r.get("yearhour", "").strip()
        if not yh or _to_int(yh) is None:
            logger.debug("Skipping non-integer yearhour value {!r} in hmap_myr derivation", yh)
            continue
        period = r.get("h", "").strip() or r.get("actual_h", "").strip()
        if yh not in out_map or (not out_map[yh] and period):
            out_map[yh] = period

    if not out_map:
        logger.warning("No valid rows found in {}; skipping hmap_myr creation", source)
        return folder

    if not any(out_map.values()):
        logger.warning(
            "Neither 'h' nor 'actual_h' column has non-empty values in {}; "
            "skipping hmap_myr creation",
            source,
        )
        return folder

    out_rows: list[dict[str, str]] = sorted(
        ({"yearhour": yh, "h": period} for yh, period in out_map.items()),
        key=lambda r: _to_int(r["yearhour"]) or 0,
    )

    target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["yearhour", "h"])
        writer.writeheader()
        writer.writerows(out_rows)

    logger.debug("Created hmap_myr.csv at {} ({} rows)", target, len(out_rows))
    return folder

UPGRADE_STEPS = [
    UpgradeStep(
        name="move_hmap_file",
        func=move_hmap_file,
        target_version="2026.01.22",
        upgrade_type=UpgradeType.FILE,
        priority=30,
    ),
    UpgradeStep(
        name="move_transmission_cost",
        func=move_transmission_cost,
        target_version="2026.01.22",
        upgrade_type=UpgradeType.FILE,
        priority=30,
    ),
    UpgradeStep(
        name="create_hmap_myr",
        func=create_hmap_myr,
        target_version="2026.03.24",
        upgrade_type=UpgradeType.FILE,
        priority=40,
    ),
]
