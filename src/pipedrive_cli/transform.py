"""Transform operations for updating field values.

Provides functionality to:
- Parse assignment expressions (field=expr)
- Evaluate expressions with record context
- Apply updates to records
"""

import re
from dataclasses import dataclass, field
from typing import Any

from simpleeval import EvalWithCompoundTypes

from .search import (
    _isfloat,
    _isint,
    _isnumeric,
    resolve_field_identifier,
)


def _isint_func(s: Any) -> bool:
    """Check if value is or can be parsed as an integer."""
    return _isint(s)


def _isfloat_func(s: Any) -> bool:
    """Check if value is or can be parsed as a float."""
    return _isfloat(s)


def _isnumeric_func(s: Any) -> bool:
    """Check if value is numeric (int or float)."""
    return _isnumeric(s)


# Transform functions for assignment expressions
TRANSFORM_FUNCTIONS: dict[str, callable] = {
    # String functions
    "upper": lambda s: str(s).upper() if s else s,
    "lower": lambda s: str(s).lower() if s else s,
    "strip": lambda s: str(s).strip() if s else s,
    "lstrip": lambda s: str(s).lstrip() if s else s,
    "rstrip": lambda s: str(s).rstrip() if s else s,
    "replace": lambda s, old, new: str(s).replace(old, new) if s else s,
    "lpad": lambda s, width, char=" ": str(s).rjust(int(width), char) if s else s,
    "rpad": lambda s, width, char=" ": str(s).ljust(int(width), char) if s else s,
    "substr": lambda s, start, end=None: (
        str(s)[int(start) : int(end) if end is not None else None] if s else s
    ),
    "concat": lambda *args: "".join(str(a) if a else "" for a in args),
    "len": lambda s: len(str(s)) if s else 0,
    # Type conversion
    "int": lambda s: int(float(s)) if s else 0,
    "float": lambda s: float(s) if s else 0.0,
    "str": lambda s: str(s) if s is not None else "",
    # Numeric functions
    "round": lambda n, d=0: round(float(n), int(d)) if n else 0,
    "abs": lambda n: abs(float(n)) if n else 0,
    # Conditional (iif to avoid conflict with Python's if keyword)
    "iif": lambda cond, then, else_: then if cond else else_,
    "coalesce": lambda *args: next(
        (a for a in args if a is not None and a != ""), None
    ),
    # Null checks (from search.py STRING_FUNCTIONS)
    "isnull": lambda s: s is None or s == "",
    "notnull": lambda s: s is not None and s != "",
    # Type checks (for filtering text fields)
    "isint": _isint_func,
    "isfloat": _isfloat_func,
    "isnumeric": _isnumeric_func,
    # String matching (from search.py STRING_FUNCTIONS)
    "contains": lambda s, sub: sub.lower() in str(s).lower() if s else False,
    "startswith": lambda s, prefix: (
        str(s).lower().startswith(prefix.lower()) if s else False
    ),
    "endswith": lambda s, suffix: (
        str(s).lower().endswith(suffix.lower()) if s else False
    ),
}


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
) -> tuple[str, str, str, dict[str, tuple[str, str]]]:
    """Resolve field identifiers in an assignment expression.

    Parses the assignment and resolves both the target field and any
    field references in the expression.

    Args:
        fields: List of field definitions from Pipedrive
        assignment: The assignment string (e.g., "tel_s='0' + tel_s")

    Returns:
        Tuple of (target_key, original_expr, resolved_expr, resolutions)
        where resolutions maps original identifier to (key, name)

    Raises:
        ValueError: If assignment format is invalid
        AmbiguousMatchError: If any identifier matches multiple fields
    """
    field_id, expr = parse_assignment(assignment)

    # Resolve the target field
    target_key = resolve_field_identifier(fields, field_id)

    # Build field lookup by key
    field_by_key: dict[str, dict] = {f.get("key", ""): f for f in fields}

    # Track resolutions (identifier -> (key, name))
    resolutions: dict[str, tuple[str, str]] = {}

    # If target field was resolved differently
    if target_key != field_id:
        field_def = field_by_key.get(target_key, {})
        field_name = field_def.get("name", target_key)
        resolutions[field_id] = (target_key, field_name)

    # Resolve identifiers in the expression
    # Build set of known function names to exclude from resolution
    known_functions = set(TRANSFORM_FUNCTIONS.keys()) | {
        "and",
        "or",
        "not",
        "True",
        "False",
        "None",
        "in",
    }

    # Find all string literal positions to exclude them
    string_positions: set[int] = set()
    for match in re.finditer(r"'[^']*'|\"[^\"]*\"", expr):
        for i in range(match.start(), match.end()):
            string_positions.add(i)

    # Pattern to match identifiers
    identifier_pattern = r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b"

    # Track replacements
    replacements: dict[str, str] = {}

    for match in re.finditer(identifier_pattern, expr):
        if match.start() in string_positions:
            continue

        identifier = match.group(1)

        if identifier in known_functions:
            continue
        if identifier in replacements:
            continue

        resolved = resolve_field_identifier(fields, identifier)
        if resolved != identifier:
            replacements[identifier] = resolved
            field_def = field_by_key.get(resolved, {})
            field_name = field_def.get("name", resolved)
            resolutions[identifier] = (resolved, field_name)

    # Apply replacements (longest first to avoid partial replacements)
    resolved_expr = expr
    for old, new in sorted(replacements.items(), key=lambda x: -len(x[0])):
        # Use word boundary replacement, but only outside string literals
        new_result = []
        last_end = 0
        for match in re.finditer(rf"\b{re.escape(old)}\b", resolved_expr):
            if match.start() not in string_positions:
                new_result.append(resolved_expr[last_end : match.start()])
                new_result.append(new)
                last_end = match.end()
        new_result.append(resolved_expr[last_end:])
        resolved_expr = "".join(new_result) if new_result else resolved_expr

    return target_key, expr, resolved_expr, resolutions


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


def _coerce_numeric(value: Any) -> Any:
    """Try to coerce a string value to a number for expressions."""
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                pass
    return value


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
    """
    evaluator = EvalWithCompoundTypes()
    # Coerce numeric strings for better expression handling
    coerced_record = {k: _coerce_numeric(v) for k, v in record.items()}
    evaluator.names = coerced_record
    evaluator.functions = {**evaluator.functions, **TRANSFORM_FUNCTIONS}
    return evaluator.eval(expression)


def apply_update_local(
    records: list[dict[str, Any]],
    assignments: list[tuple[str, str]],  # [(target_key, resolved_expr), ...]
    dry_run: bool = False,
) -> tuple[UpdateStats, list[dict[str, Any]]]:
    """Apply assignments to records in memory.

    Args:
        records: List of records to update
        assignments: List of (target_key, resolved_expr) tuples
        dry_run: If True, don't modify records (just compute stats)

    Returns:
        Tuple of (stats, changes) where changes is a list of
        {"id": ..., "field": ..., "old": ..., "new": ...} dicts
    """
    stats = UpdateStats(total=len(records))
    changes: list[dict[str, Any]] = []

    for record in records:
        record_id = record.get("id", "?")
        record_changed = False

        for target_key, resolved_expr in assignments:
            old_value = record.get(target_key)

            try:
                new_value = evaluate_assignment(record, resolved_expr)

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
