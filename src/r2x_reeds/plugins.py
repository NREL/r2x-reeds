"""R2X-core plugin discovery."""


def register_plugin() -> str | None:
    """Register the ReEDS plugin with the R2X plugin manager.

    This function is called automatically when the plugin is discovered
    via entry points. It registers the ReEDS parser, config, and optionally
    an exporter with the PluginManager.
    """

    from r2x_core.plugins import PluginManager
    from r2x_reeds.config import ReEDSConfig
    from r2x_reeds.parser import ReEDSParser
    from r2x_reeds.sysmods import break_gens
    from r2x_reeds.sysmods.cambium import cambium_assumptions
    from r2x_reeds.sysmods.ccs_credit import add_ccs_credit
    from r2x_reeds.sysmods.electrolyzer import add_electrolizer_load
    from r2x_reeds.sysmods.emission_cap import add_emission_cap
    from r2x_reeds.sysmods.hurdle_rate import add_tx_hurdle_rate
    from r2x_reeds.sysmods.pcm_defaults import add_pcm_defaults
    from r2x_reeds.upgrader.data_upgrader import ReEDSUpgrader

    PluginManager.register_model_plugin(
        name="reeds",
        config=ReEDSConfig,
        parser=ReEDSParser,
        upgrader=ReEDSUpgrader,  # Steps already registered via decorators
    )
    PluginManager.register_system_modifier("add_pcm_defaults")(add_pcm_defaults)
    PluginManager.register_system_modifier("add_tx_hurdle_rate")(add_tx_hurdle_rate)
    PluginManager.register_system_modifier("add_emission_cap")(add_emission_cap)
    PluginManager.register_system_modifier("add_electrolyzer_load")(add_electrolizer_load)
    PluginManager.register_system_modifier("cambium_assumptions")(cambium_assumptions)
    PluginManager.register_system_modifier("add_ccs_credit")(add_ccs_credit)
    PluginManager.register_system_modifier("break_gens")(break_gens)

    return None
