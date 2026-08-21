"""Materialize parser rows through the declarative r2x-core rules."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from types import SimpleNamespace
from typing import Any, cast

from infrasys import Component
from rust_ok import Err, Ok, Result

from r2x_core import PluginContext, Rule
from r2x_core.exceptions import ValidationError
from r2x_core.utils import sort_rules_by_dependencies

from .rule_construction import create_and_attach_from_rule

Row = Mapping[str, Any]
AttachmentSource = Callable[[Row], Component | None]


def materialize_parser_batches(
    batches: Mapping[str, Iterable[Row]],
    *,
    context: PluginContext,
    attachment_sources: Mapping[str, AttachmentSource] | None = None,
) -> Result[dict[str, int], str]:
    """Materialize named row batches in their declarative dependency order."""
    ordered_result = sort_rules_by_dependencies(list(context.rules))
    if ordered_result.is_err():
        return Err(str(ordered_result.err()))
    ordered_rules = ordered_result.ok()
    if ordered_rules is None:
        return Err("Parser rule ordering returned no rules")

    target_system = context.system
    if target_system is None:
        return Err("Parser context has no system")
    context.target_system = target_system
    counts: dict[str, int] = {}
    sources = attachment_sources or {}
    for rule in ordered_rules:
        if rule.name is None or rule.name not in batches:
            continue
        result = materialize_parser_rows(
            batches[rule.name],
            context=context,
            rule_name=rule.name,
            attachment_source=sources.get(rule.name),
        )
        if result.is_err():
            return Err(str(result.err()))
        counts[rule.name] = result.ok() or 0
    return Ok(counts)


def select_parser_rule(
    rules: Sequence[Rule],
    row: Row,
    *,
    context: PluginContext,
    name: str | None = None,
) -> Result[Rule, ValidationError]:
    """Select a parser rule by name or by its declarative row filter."""
    candidates = [rule for rule in rules if rule.source_type == "data_row"]
    if name is not None:
        candidates = [rule for rule in candidates if rule.name == name]
        if not candidates:
            return Err(ValidationError(f"No parser rule named {name!r}"))
        return Ok(candidates[0])

    source = SimpleNamespace(**dict(row))
    for rule in candidates:
        if rule.filter is not None and rule.filter.matches(source, context=context):
            return Ok(rule)

    return Err(
        ValidationError(
            f"No parser rule matched row with technology={row.get('technology')!r}, "
            f"category={row.get('category')!r}"
        )
    )


def materialize_parser_rows(
    rows: Iterable[Row],
    *,
    context: PluginContext,
    rule_name: str | None = None,
    attachment_source: AttachmentSource | None = None,
    skip_duplicate_names: bool = False,
) -> Result[int, str]:
    """Create and attach components declared by parser rules.

    ``parser_rules.json`` owns the target type, field map, getters, defaults,
    filters, and supplemental outputs. This function owns only the repeated
    row-to-rule-to-attachment operation.
    """
    target_system = context.system
    if target_system is None:
        return Err("Parser context has no system")
    context.target_system = target_system

    errors: list[str] = []
    created = 0
    rules = list(context.rules)
    created_names: set[str] = set()

    for row in rows:
        rule_result = select_parser_rule(rules, row, context=context, name=rule_name)
        if rule_result.is_err():
            errors.append(str(rule_result.err()))
            continue
        rule = rule_result.ok()
        if rule is None:
            errors.append("Parser rule selection returned no rule")
            continue

        if skip_duplicate_names:
            name_getter = rule.getters.get("name")
            if callable(name_getter):
                name_result = cast(Any, name_getter)(SimpleNamespace(**dict(row)), context=context)
                if isinstance(name_result, Ok):
                    identifier = name_result.ok()
                    if identifier in created_names:
                        continue
                    created_names.add(str(identifier))

        source = attachment_source(row) if attachment_source is not None else None
        if attachment_source is not None and source is None:
            identifier = row.get("name", row.get("region", "<unknown>"))
            errors.append(f"{identifier}: attachment source was not found")
            continue

        result = create_and_attach_from_rule(
            row,
            rule,
            system=target_system,
            context=context,
            attachment_source=source,
        )
        if result.is_err():
            identifier = row.get("name", row.get("region", "<unknown>"))
            errors.append(f"{identifier}: {result.err()}")
            continue
        created += 1

    if errors:
        return Err("Failed to materialize parser rows: " + "; ".join(errors))
    return Ok(created)
