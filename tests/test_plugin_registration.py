from r2x_core import PluginManager
from r2x_reeds.parser import ReEDSParser
from r2x_reeds.plugins import register_plugin


def test_reeds_plugin_registration():
    pm = PluginManager()

    register_plugin()

    assert "reeds" in pm.registered_parsers
    assert pm.load_parser(name="reeds") == ReEDSParser

    assert "add_pcm_defaults" in pm.registered_modifiers
    assert "add_electrolyzer_load" in pm.registered_modifiers
    assert "break_gens" in pm.registered_modifiers
    assert "add_ccs_credit" in pm.registered_modifiers
