from r2x_core import GitVersioningStrategy
from r2x_core.plugins import PluginManager

# from r2x_core.store import DataStore
from .config import ReEDSConfig
from .parser import ReEDSParser


def register_plugin() -> None:
    """Register the ReEDS plugin with the R2X plugin manager.

    This function is called automatically when the plugin is discovered
    via entry points. It registers the ReEDS parser, config, and optionally
    an exporter with the PluginManager.
    """

    PluginManager.register_model_plugin(
        name="reeds",
        config=ReEDSConfig,
        parser=ReEDSParser,
        exporter=None,  # Will be implemented later
    )

    data = {"git_version": "d9636c3b61cdf74d3d1ecd930e2d974cbc01d695"}

    strategy = GitVersioningStrategy(version_field="git_version")
    strategy.set_version(data, "")

