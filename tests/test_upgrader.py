import pytest

from r2x_core import GitVersioningStrategy
from r2x_reeds.upgrader.data_upgrader import ReEDSUpgrader, ReEDSVersionDetector
from r2x_reeds.upgrader.helpers import COMMIT_HISTORY


@pytest.fixture
def upgraded_system(reeds_run_upgrader, example_reeds_config, caplog):
    from r2x_core import DataStore
    from r2x_reeds.parser import ReEDSParser
    from r2x_reeds.upgrader import ReEDSUpgrader

    upgrader = ReEDSUpgrader(reeds_run_upgrader)
    store = DataStore.from_plugin_config(
        example_reeds_config, folder_path=reeds_run_upgrader, upgrader=upgrader
    )
    store.upgrade_data()

    parser = ReEDSParser(example_reeds_config, data_store=store, system_name="Upgraded System")
    return parser.build_system()


def test_reeds_upgrader(reeds_run_upgrader):
    upgrader = ReEDSUpgrader(reeds_run_upgrader)

    assert isinstance(upgrader.strategy, GitVersioningStrategy)
    assert upgrader.strategy.commit_history == COMMIT_HISTORY
    assert isinstance(upgrader.version_detector, ReEDSVersionDetector)


def test_upgraded_system(upgraded_system):
    from r2x_core import System

    assert isinstance(upgraded_system, System)
