"""Tests for excluded_techs functionality."""

import pytest

pytestmark = [pytest.mark.integration]


def test_excluded_techs_can_be_overridden():
    from r2x_reeds import ReEDSConfig

    config = ReEDSConfig(
        solve_year=2030,
        weather_year=2012,
        excluded_techs=["electrolyzer", "smr", "smr_ccs"],
    )

    assert config.excluded_techs == ["electrolyzer", "smr", "smr_ccs"]


def test_excluded_techs_empty_list_default(reeds_config, reeds_run_path):
    from typing import cast

    from r2x_core import DataStore, PluginContext
    from r2x_reeds import ReEDSParser
    from r2x_reeds.models import ReEDSGenerator

    config_dicts = reeds_config.load_config()
    assert config_dicts["defaults"].get("excluded_techs") == [
        "can-imports",
        "electrolyzer",
        "smr",
        "smr_ccs",
    ]

    data_store = DataStore.from_plugin_config(reeds_config, path=reeds_run_path)
    ctx = PluginContext(config=reeds_config, store=data_store)
    parser = cast(ReEDSParser, ReEDSParser.from_context(ctx))
    result_ctx = parser.run()

    system = result_ctx.system
    assert system is not None
    generators = list(system.get_components(ReEDSGenerator))

    assert len(generators) > 0
