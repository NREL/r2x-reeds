"""Load ReEDS parser assets into the r2x-core PluginContext."""

from __future__ import annotations

from loguru import logger
from rust_ok import Err, Ok, Result

from r2x_core import PluginContext, Rule
from r2x_core.utils import sort_rules_by_dependencies

from .core import CONFIG_ASSETS, DEFAULTS, TECH_CATEGORIES


def load_parser_configuration(context: PluginContext) -> Result[None, str]:
    """Load defaults and parser rules once into context metadata."""
    if CONFIG_ASSETS in context.metadata:
        return Ok(None)
    try:
        assets = context.config.load_config()
    except FileNotFoundError as exc:
        return Err(f"Plugin config files missing: {exc}")

    raw_rules = assets.get("parser_rules")
    if not isinstance(raw_rules, list):
        return Err("Parser rules are missing from plugin config")
    try:
        rules = Rule.from_records(raw_rules)
    except (TypeError, ValueError) as exc:
        return Err(f"Failed to parse parser rules: {exc}")

    defaults = assets.get("defaults")
    if not isinstance(defaults, dict):
        return Err("Parser defaults must be a mapping")
    ordered_result = sort_rules_by_dependencies(rules)
    if ordered_result.is_err():
        return Err(f"Failed to order parser rules: {ordered_result.err()}")
    ordered_rules = ordered_result.ok()
    if ordered_rules is None:
        return Err("Parser rule ordering returned no rules")

    context.rules = tuple(ordered_rules)
    context.metadata[CONFIG_ASSETS] = assets
    context.metadata[DEFAULTS] = defaults
    context.metadata[TECH_CATEGORIES] = defaults.get("tech_categories", {})
    logger.debug("Loaded {} ReEDS parser rules", len(rules))
    return Ok(None)
