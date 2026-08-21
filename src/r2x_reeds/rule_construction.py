"""Construct parser outputs from declarative ReEDS rules."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from infrasys import Component, SupplementalAttribute
from rust_ok import Err, Ok, Result

from r2x_core import PluginContext, Rule, System, attach_rule_outputs, resolve_supplemental_class
from r2x_core.utils import RuleOutputs, create_rule_outputs, resolve_component_type


def create_and_attach_from_rule(
    row: Mapping[str, Any],
    rule: Rule,
    *,
    system: System,
    context: PluginContext,
    attachment_source: Component | None = None,
) -> Result[Component | SupplementalAttribute, str]:
    """Construct and attach all outputs declared by one parser rule.

    The parser supplies prepared row data and the target system. This module owns
    type resolution, primary and supplemental model construction, and atomic
    attachment through the r2x-core rule-output interface.
    """
    context.system = system
    context.target_system = system

    outputs_result = _build_rule_outputs(row, rule, context=context)
    if outputs_result.is_err():
        return Err(str(outputs_result.err()))
    outputs = outputs_result.ok()
    if outputs is None:
        return Err(f"Parser rule {rule.name or rule} produced no outputs")

    source = attachment_source
    if source is None:
        source = _resolve_attached_source(outputs)
    if source is None:
        return Err("Rule outputs require a Component attachment source")
    attach_result = attach_rule_outputs(outputs, source, context)
    if attach_result.is_err():
        return Err(str(attach_result.err()))
    return Ok(outputs.primary)


def _build_rule_outputs(
    row: Mapping[str, Any],
    rule: Rule,
    *,
    context: PluginContext,
) -> Result[RuleOutputs, str]:
    """Resolve and construct the primary and supplemental rule outputs."""
    target_types = rule.get_target_types()
    if len(target_types) != 1:
        return Err(f"Parser rule {rule.name or rule} must declare exactly one target type")

    target_result = resolve_component_type(target_types[0], context=context)
    if target_result.is_err():
        return Err(str(target_result.err()))
    target_class = target_result.ok()
    if target_class is None or not isinstance(target_class, type):
        return Err(f"Parser rule {rule.name or rule} resolved no target class")
    if not (issubclass(target_class, Component) or issubclass(target_class, SupplementalAttribute)):
        return Err(f"Parser rule {rule.name or rule} target is not a component or supplemental attribute")

    supplemental_classes: list[type[SupplementalAttribute]] = []
    for output_rule in getattr(rule, "supplemental_attributes", ()):
        class_result = resolve_supplemental_class(output_rule.target_type, context=context)
        if class_result.is_err():
            return Err(str(class_result.err()))
        output_class = class_result.ok()
        if output_class is None:
            return Err(f"Supplemental class resolution returned None for {output_rule.target_type}")
        supplemental_classes.append(output_class)

    outputs_result = create_rule_outputs(
        cast(Component, row),
        rule=rule,
        target_class=target_class,
        supplemental_classes=supplemental_classes,
        context=context,
    )
    if outputs_result.is_err():
        return Err(str(outputs_result.err()))
    outputs = outputs_result.ok()
    if outputs is None:
        return Err(f"Parser rule {rule.name or rule} produced no outputs")
    return Ok(outputs)


def _resolve_attached_source(outputs: RuleOutputs) -> Component | None:
    """Use the primary output as the source for supplemental attachment."""
    return outputs.primary if isinstance(outputs.primary, Component) else None
