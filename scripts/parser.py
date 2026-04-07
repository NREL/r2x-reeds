from r2x_core import DataStore, PluginContext
from r2x_reeds import ReEDSConfig, ReEDSParser, ReEDSUpgrader

# setup_logging(level="DEBUG")
run_path = "/Users/psanchez/Downloads/Pacific/"


upgrader = ReEDSUpgrader(path=run_path)
config = ReEDSConfig(weather_year=2012, solve_year=2032)
store = DataStore.from_plugin_config(config, path=run_path)

upgrader.upgrade()

ctx = PluginContext(config=config, store=store)
result_ctx = ReEDSParser.from_context(ctx).run()
sys = result_ctx.system
