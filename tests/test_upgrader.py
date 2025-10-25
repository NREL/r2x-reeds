from pathlib import Path

import pytest

from r2x_core import GitVersioningStrategy
from r2x_reeds.upgrader import ReEDSUpgrader
from r2x_reeds.upgrader.data_upgrader import ReEDSVersionDetector
from r2x_reeds.upgrader.helpers import COMMIT_HISTORY


@pytest.fixture
def test_data_path() -> Path:
    """Path to test data directory."""
    return Path(__file__).parent / "data" / "test_Pacific"


def test_upgrade_steps(test_data_path):
    version_detector = ReEDSVersionDetector(folder_path=test_data_path)
    version_strategy = GitVersioningStrategy(commit_history=COMMIT_HISTORY)
    _ = ReEDSUpgrader(test_data_path, strategy=version_strategy, version_detector=version_detector)
