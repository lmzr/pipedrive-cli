"""Transform operations for updating field values.

Provides functionality to:
- Parse assignment expressions (field=expr)
- Evaluate expressions with record context
- Apply updates to records
"""

import re
from dataclasses import dataclass, field
from typing import Any

from .expressions import (
    TRANSFORM_FUNCTIONS,
    AmbiguousCallback,
    EnumValue,
    _escape_digit_key,
    evaluate_expression,
    resolve_expression,
    resolve_field_identifier,
)
from .expressions import (
    validate_expression as _validate_expression,
)


def validate_assignment(expression: str, field_keys: set[str]) -> None:
    """Validate assignment expression syntax.

    Uses TRANSFORM_FUNCTIONS which includes iif, coalesce, and other
    transform-specific functions not available in filter expressions.

    Args:
        expression: The expression to validate
        field_keys: Set of valid field keys

    Raises:
        FilterError: If expression is invalid
    """
    _validate_expression(expression, field_keys, TRANSFORM_FUNCTIONS)


@dataclass
class UpdateStats:
    """Statistics for an update operation."""

    total: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)


def parse_assignment(assignment: str) -> tuple[str, str]:
    """Parse 'field=expr' into (field, expr).

    Args:
        assignment: The assignment string (e.g., "name=upper(name)")

    Returns:
        Tuple of (field_identifier, expression)

    Raises:
        ValueError: If assignment format is invalid
    """
    if "=" not in assignment:
        raise ValueError(f"Invalid assignment: {assignment} (expected 'field=expr')")
    field, expr = assignment.split("=", 1)
    return field.strip(), expr.strip()


def resolve_assignment(
    fields: list[dict[str, Any]],
    assignment: str,
    *,
    on_ambiguous: AmbiguousCallback | None = None,
) -> tuple[str, str, str, dict[str, tuple[str, str]]]:
    """Resolve field identifiers in an assignment expression.

    Parses the assignment and resolves both the target field and any
    field references in the expression using the shared resolve_expression()
    function from expressions.py.

    Args:
        fields: List of field definitions from Pipedrive
        assignment: The assignment string (e.g., "tel_s='0' + tel_s")
        on_ambiguous: Callback called when multiple matches found.
                      Receives (identifier, matches), returns selected key.
                      If None, raises AmbiguousMatchError.

    Returns:
        Tuple of (escaped_target_key, original_expr, resolved_expr, resolutions)
        where resolutions maps original identifier to (key, name)
        Target key is escaped with '_' prefix if it starts with a digit.

    Raises:
        ValueError: If assignment format is invalid
        AmbiguousMatchError: If any identifier matches multiple fields and no callback
    """
    field_id, expr = parse_assignment(assignment)

    # Resolve the target field
    target_key = resolve_field_identifier(fields, field_id, on_ambiguous=on_ambiguous)
    escaped_target = _escape_digit_key(target_key)

    # Use shared expression resolution (includes hex-pattern detection + escaping)
    resolved_expr, expr_resolutions = resolve_expression(
        fields, expr, TRANSFORM_FUNCTIONS, on_ambiguous=on_ambiguous
    )

    # Merge target field resolution with expression resolutions
    resolutions = dict(expr_resolutions)
    if target_key != field_id:
        field_by_key: dict[str, dict] = {f.get("key", ""): f for f in fields}
        field_def = field_by_key.get(target_key, {})
        field_name = field_def.get("name", target_key)
        resolutions[field_id] = (target_key, field_name)

    return escaped_target, expr, resolved_expr, resolutions


def format_resolved_assignment(
    original_field: str,
    target_key: str,
    original_expr: str,
    resolved_expr: str,
    resolutions: dict[str, tuple[str, str]],
) -> tuple[str, str]:
    """Format resolved assignment for display.

    Returns two lines:
    1. Assignment with field names (human-readable)
    2. Assignment with field keys (for execution)

    Args:
        original_field: The original field identifier
        target_key: The resolved target field key
        original_expr: The original expression
        resolved_expr: The expression with resolved keys
        resolutions: Dict mapping identifier -> (key, name)

    Returns:
        Tuple of (name_line, key_line)
    """
    if not resolutions:
        # No resolution happened
        return f"{original_field} = {original_expr}", ""

    # Build expression with names
    name_expr = original_expr
    name_field = original_field

    for identifier, (key, name) in sorted(resolutions.items(), key=lambda x: -len(x[0])):
        # Quote names with spaces
        display_name = f'"{name}"' if " " in name else name
        name_expr = re.sub(rf"\b{re.escape(identifier)}\b", display_name, name_expr)
        if identifier == original_field:
            name_field = display_name

    # Build key expression (already resolved)
    key_field = target_key

    return f"{name_field} = {name_expr}", f"{key_field} = {resolved_expr}"


def evaluate_assignment(
    record: dict[str, Any],
    expression: str,
) -> Any:
    """Evaluate an expression with record fields as variables.

    Args:
        record: The record whose fields become available as variables
        expression: The expression to evaluate (already resolved)

    Returns:
        The evaluated result

    Raises:
        Exception: If evaluation fails

    Note:
        No automatic type coercion is performed. Use int(), float(), str()
        functions explicitly in expressions when type conversion is needed.
    """
    return evaluate_expression(record, expression, TRANSFORM_FUNCTIONS)


def _preprocess_record_for_eval(
    record: dict[str, Any],
    option_lookup: dict[str, dict[str, str]],
) -> dict[str, Any]:
    """Wrap enum/set values for expression evaluation.

    Args:
        record: Original record
        option_lookup: {field_key: {id: label}} for enum/set fields

    Returns:
        Record with EnumValue wrappers for enum/set fields
    """
    if not option_lookup:
        return record

    processed = dict(record)
    for field_key, opts in option_lookup.items():
        if field_key not in processed:
            continue
        raw_value = processed[field_key]
        if raw_value is None or raw_value == "":
            continue

        str_val = str(raw_value)
        processed[field_key] = EnumValue(str_val, opts.get(str_val))

    return processed


def apply_update_local(
    records: list[dict[str, Any]],
    assignments: list[tuple[str, str]],  # [(target_key, resolved_expr), ...]
    dry_run: bool = False,
    option_lookup: dict[str, dict[str, str]] | None = None,
) -> tuple[UpdateStats, list[dict[str, Any]]]:
    """Apply assignments to records in memory.

    Args:
        records: List of records to update
        assignments: List of (target_key, resolved_expr) tuples
        dry_run: If True, don't modify records (just compute stats)
        option_lookup: Optional {field_key: {id: label}} for enum/set comparison

    Returns:
        Tuple of (stats, changes) where changes is a list of
        {"id": ..., "field": ..., "old": ..., "new": ...} dicts
    """
    stats = UpdateStats(total=len(records))
    changes: list[dict[str, Any]] = []

    for record in records:
        record_id = record.get("id", "?")
        record_changed = False

        # Preprocess record for enum/set comparison in expressions
        eval_record = _preprocess_record_for_eval(record, option_lookup or {})

        for target_key, resolved_expr in assignments:
            old_value = record.get(target_key)

            try:
                new_value = evaluate_assignment(eval_record, resolved_expr)

                # Only count as updated if value actually changed
                if new_value != old_value:
                    changes.append({
                        "id": record_id,
                        "field": target_key,
                        "old": old_value,
                        "new": new_value,
                    })
                    if not dry_run:
                        record[target_key] = new_value
                    record_changed = True

            except Exception as e:
                stats.failed += 1
                stats.errors.append(f"Record {record_id}, field {target_key}: {e}")
                continue

        if record_changed:
            stats.updated += 1
        else:
            stats.skipped += 1

    return stats, changes
