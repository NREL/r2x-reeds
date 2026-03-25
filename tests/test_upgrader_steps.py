from __future__ import annotations

import csv
from pathlib import Path

import pytest

from r2x_reeds.upgrader.data_upgrader import ReEDSVersionDetector
from r2x_reeds.upgrader.helpers import LEGACY_VERSION
from r2x_reeds.upgrader.upgrade_steps import create_hmap_myr, move_hmap_file, move_transmission_cost

pytestmark = [pytest.mark.integration]


def test_version_detector_reads_tag_by_header(tmp_path: Path) -> None:
    """The version detector reads the tag column by header name."""
    meta_path = tmp_path / "meta.csv"
    with open(meta_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["computer", "repo", "branch", "commit", "description", "tag"])
        writer.writerow(["host", "/path", "main", "abc123", "desc", "2026.01.22"])

    detector = ReEDSVersionDetector()
    assert detector.read_version(tmp_path) == "2026.01.22"


def test_version_detector_legacy_format_returns_sentinel(tmp_path: Path) -> None:
    """Legacy format without tag header returns LEGACY_VERSION."""
    meta_path = tmp_path / "meta.csv"
    with open(meta_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["computer", "repo", "branch", "commit", "description"])
        writer.writerow(["host", "/path", "main", "abc123", "desc"])

    detector = ReEDSVersionDetector()
    assert detector.read_version(tmp_path) == LEGACY_VERSION


def test_version_detector_empty_tag_returns_sentinel(tmp_path: Path) -> None:
    """Empty tag value returns LEGACY_VERSION."""
    meta_path = tmp_path / "meta.csv"
    with open(meta_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["computer", "repo", "branch", "commit", "description", "tag"])
        writer.writerow(["host", "/path", "main", "abc123", "desc", ""])

    detector = ReEDSVersionDetector()
    assert detector.read_version(tmp_path) == LEGACY_VERSION


def test_version_detector_tag_column_any_position(tmp_path: Path) -> None:
    """Tag column works regardless of position in header."""
    meta_path = tmp_path / "meta.csv"
    with open(meta_path, "w", newline="") as fh:
        writer = csv.writer(fh)
        # Put tag column in different position (index 2)
        writer.writerow(["computer", "repo", "tag", "branch", "commit", "description"])
        writer.writerow(["host", "/path", "2025.12.01", "main", "abc123", "desc"])

    detector = ReEDSVersionDetector()
    assert detector.read_version(tmp_path) == "2025.12.01"


def test_version_detector_missing_file(tmp_path: Path) -> None:
    """Missing files raise FileNotFoundError."""
    detector = ReEDSVersionDetector()
    with pytest.raises(FileNotFoundError):
        detector.read_version(tmp_path)


def test_move_hmap_file_moves_and_skips(tmp_path: Path) -> None:
    """Upgrade step moves the file once and skips when already moved."""
    inputs_case = tmp_path / "inputs_case"
    rep_folder = inputs_case / "rep"
    rep_folder.mkdir(parents=True)
    old_file = inputs_case / "hmap_allyrs.csv"
    old_file.write_text("content")

    move_hmap_file(tmp_path)
    assert not old_file.exists()
    assert (rep_folder / "hmap_allyrs.csv").read_text() == "content"

    # Running again should be a no-op now that target exists
    move_hmap_file(tmp_path)
    assert (rep_folder / "hmap_allyrs.csv").exists()


def test_move_transmission_cost_moves_and_skips(tmp_path: Path) -> None:
    """Legacy transmission files should be renamed once."""
    inputs_case = tmp_path / "inputs_case"
    inputs_case.mkdir(parents=True)

    ac_old = inputs_case / "transmission_distance_cost_500kVac.csv"
    ac_old.write_text("ac")
    dc_old = inputs_case / "transmission_distance_cost_500kVdc.csv"
    dc_old.write_text("dc")

    move_transmission_cost(tmp_path)
    assert not ac_old.exists()
    assert not dc_old.exists()
    assert (inputs_case / "transmission_cost_ac.csv").read_text() == "ac"
    assert (inputs_case / "transmission_distance.csv").read_text() == "dc"

    # Running again is a no-op now that targets exist
    move_transmission_cost(tmp_path)
    assert (inputs_case / "transmission_cost_ac.csv").exists()
    assert (inputs_case / "transmission_distance.csv").exists()


def test_move_hmap_file_raises_when_neither_exists(tmp_path: Path) -> None:
    """Raises FileNotFoundError when neither old nor new file exists."""
    inputs_case = tmp_path / "inputs_case"
    rep_folder = inputs_case / "rep"
    rep_folder.mkdir(parents=True)

    with pytest.raises(FileNotFoundError):
        move_hmap_file(tmp_path)


def test_move_transmission_cost_skips_missing_files(tmp_path: Path) -> None:
    """Skips files that don't exist without error."""
    inputs_case = tmp_path / "inputs_case"
    inputs_case.mkdir(parents=True)

    # Neither old files exist - should complete without error
    result = move_transmission_cost(tmp_path)
    assert result == tmp_path


def _write_hmap_allyrs(rep_folder: Path, rows: list[dict]) -> None:
    """Write a minimal hmap_allyrs.csv fixture for testing."""
    import csv

    path = rep_folder / "hmap_allyrs.csv"
    fieldnames = ["*timestamp", "year", "yearhour", "h", "actual_h"]
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_create_hmap_myr_creates_file_from_h_column(tmp_path: Path) -> None:
    """Creates hmap_myr.csv using the 'h' column when it has non-empty values."""
    rep = tmp_path / "inputs_case" / "rep"
    rep.mkdir(parents=True)

    rows = [
        {
            "*timestamp": f"2012-01-01 {i:02d}:00:00",
            "year": "2012",
            "yearhour": str(i + 1),
            "h": f"y2012d005h{(i % 8 + 1) * 3:03d}",
            "actual_h": f"y2007d001h{(i % 24 + 1):03d}",
        }
        for i in range(24)
    ]
    _write_hmap_allyrs(rep, rows)

    create_hmap_myr(tmp_path)

    target = rep / "hmap_myr.csv"
    assert target.exists(), "hmap_myr.csv should have been created"

    import csv

    with open(target) as fh:
        reader = csv.DictReader(fh)
        out_rows = list(reader)

    assert out_rows[0]["h"].startswith("y2012"), "should use 'h' column (y2012 prefix)"
    assert len(out_rows) == 24
    assert [r["yearhour"] for r in out_rows] == [str(i) for i in range(1, 25)]


def test_create_hmap_myr_falls_back_to_actual_h(tmp_path: Path) -> None:
    """Falls back to 'actual_h' when 'h' column is empty."""
    import csv

    rep = tmp_path / "inputs_case" / "rep"
    rep.mkdir(parents=True)

    rows = [
        {
            "*timestamp": f"2012-01-01 {i:02d}:00:00",
            "year": "2012",
            "yearhour": str(i + 1),
            "h": "",  # empty — must use actual_h
            "actual_h": f"y2007d001h{(i % 24 + 1):03d}",
        }
        for i in range(24)
    ]
    path = rep / "hmap_allyrs.csv"
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["*timestamp", "year", "yearhour", "h", "actual_h"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    create_hmap_myr(tmp_path)

    target = rep / "hmap_myr.csv"
    assert target.exists()

    with open(target) as fh:
        out_rows = list(csv.DictReader(fh))

    assert out_rows[0]["h"].startswith("y2007"), "should fall back to actual_h (y2007 prefix)"
    assert len(out_rows) == 24


def test_create_hmap_myr_is_idempotent(tmp_path: Path) -> None:
    """Running the step twice does not overwrite an existing hmap_myr.csv."""
    rep = tmp_path / "inputs_case" / "rep"
    rep.mkdir(parents=True)

    target = rep / "hmap_myr.csv"
    sentinel = "yearhour,h\n1,sentinel\n"
    target.write_text(sentinel)

    # Even if hmap_allyrs.csv exists, the step must not touch target
    (rep / "hmap_allyrs.csv").write_text("*timestamp,year,yearhour,h,actual_h\n")

    create_hmap_myr(tmp_path)
    assert target.read_text() == sentinel, "existing hmap_myr.csv must not be overwritten"


def test_create_hmap_myr_skips_when_source_missing(tmp_path: Path) -> None:
    """Step is a no-op when hmap_allyrs.csv does not exist (legacy runs)."""
    rep = tmp_path / "inputs_case" / "rep"
    rep.mkdir(parents=True)

    # No hmap_allyrs.csv present
    create_hmap_myr(tmp_path)

    assert not (rep / "hmap_myr.csv").exists(), "should not create target when source is absent"


def test_create_hmap_myr_output_columns(tmp_path: Path) -> None:
    """Output CSV has exactly 'yearhour' and 'h' columns, sorted by yearhour."""
    import csv

    rep = tmp_path / "inputs_case" / "rep"
    rep.mkdir(parents=True)

    # Write 8 rows in reverse yearhour order
    rows = [
        {
            "*timestamp": "2012-01-01 00:00:00",
            "year": "2012",
            "yearhour": str(8 - i),
            "h": f"y2012d005h{(i + 1) * 3:03d}",
            "actual_h": f"y2007d001h{(i + 1):03d}",
        }
        for i in range(8)
    ]
    path = rep / "hmap_allyrs.csv"
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["*timestamp", "year", "yearhour", "h", "actual_h"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    create_hmap_myr(tmp_path)

    target = rep / "hmap_myr.csv"
    with open(target) as fh:
        reader2 = csv.DictReader(fh)
        assert set(reader2.fieldnames or []) == {"yearhour", "h"}
        out_rows2 = list(reader2)

    assert [r["yearhour"] for r in out_rows2] == [str(i) for i in range(1, 9)]


def test_create_hmap_myr_missing_yearhour_column(tmp_path: Path) -> None:
    """hmap_allyrs.csv without a 'yearhour' column logs a warning and skips."""
    rep = tmp_path / "inputs_case" / "rep"
    rep.mkdir(parents=True)

    path = rep / "hmap_allyrs.csv"
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["*timestamp", "h"])
        writer.writeheader()
        writer.writerow({"*timestamp": "2012-01-01", "h": "y2012d001h001"})

    create_hmap_myr(tmp_path)
    assert not (rep / "hmap_myr.csv").exists()


def test_create_hmap_myr_empty_source(tmp_path: Path) -> None:
    """hmap_allyrs.csv with header only (no data rows) skips creation."""
    rep = tmp_path / "inputs_case" / "rep"
    rep.mkdir(parents=True)

    path = rep / "hmap_allyrs.csv"
    path.write_text("*timestamp,year,yearhour,h,actual_h\n")

    create_hmap_myr(tmp_path)
    assert not (rep / "hmap_myr.csv").exists()


def test_create_hmap_myr_both_h_columns_empty(tmp_path: Path) -> None:
    """When both 'h' and 'actual_h' are empty for all rows, creation is skipped."""
    rep = tmp_path / "inputs_case" / "rep"
    rep.mkdir(parents=True)

    rows = [
        {"*timestamp": "2012-01-01", "year": "2012", "yearhour": str(i + 1), "h": "", "actual_h": ""}
        for i in range(8)
    ]
    path = rep / "hmap_allyrs.csv"
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["*timestamp", "year", "yearhour", "h", "actual_h"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    create_hmap_myr(tmp_path)
    assert not (rep / "hmap_myr.csv").exists()
