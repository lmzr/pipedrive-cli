"""Search and filter operations for Pipedrive records.

Provides functionality to:
- Filter records using simpleeval expressions
- Resolve field identifiers (key prefix, name prefix) in expressions
- Format output as table, JSON, or CSV
"""

import csv
import io
import json
import re
from typing import Any

from rich.console import Console
from rich.table import Table

from .expressions import (
    FIELD_FUNC_PATTERN,
    FILTER_FUNCTIONS,
    AmbiguousCallback,
    FilterError,
    _isfloat,
    _isint,
    _isnumeric,
    format_resolved_expression,
    resolve_expression,
    resolve_field_identifier,
    resolve_field_name,
)
from .expressions import (
    filter_record as _filter_record,
)
from .expressions import (
    validate_expression as _validate_expression,
)
from .field import build_option_lookup, format_option_value
from .matching import AmbiguousMatchError, find_field_matches

# Re-export for backwards compatibility
__all__ = [
    "FilterError",
    "resolve_field_identifier",
    "resolve_filter_expression",
    "format_resolved_expression",
    "extract_filter_keys",
    "validate_expression",
    "filter_record",
    "resolve_field_prefixes",
    "select_fields",
    "format_table",
    "format_json",
    "format_csv",
    "_isint",
    "_isfloat",
    "_isnumeric",
]


def resolve_filter_expression(
    fields: list[dict[str, Any]],
    expression: str,
    *,
    on_ambiguous: AmbiguousCallback | None = None,
) -> tuple[str, dict[str, tuple[str, str]]]:
    """Resolve all field identifiers in a filter expression.

    Parses the expression to find identifiers and resolves each one
    using resolve_field_identifier().

    Args:
        fields: List of field definitions from Pipedrive
        expression: The filter expression with potential field prefixes
        on_ambiguous: Callback called when multiple matches found.
                      Receives (identifier, matches), returns selected key.
                      If None, raises AmbiguousMatchError.

    Returns:
        Tuple of (resolved_expression, resolutions_dict)
        where resolutions_dict maps original identifier to (key, name)

    Raises:
        AmbiguousMatchError: If any identifier matches multiple fields and no callback
    """
    return resolve_expression(fields, expression, FILTER_FUNCTIONS, on_ambiguous=on_ambiguous)


def validate_expression(expression: str, field_keys: set[str]) -> None:
    """Validate expression syntax before batch evaluation.

    Does a test evaluation with dummy record to catch:
    - Syntax errors
    - Assignment attempts (= instead of ==)
    - Multiple expressions (;)
    - Unknown functions

    Args:
        expression: The expression to validate
        field_keys: Set of valid field keys

    Raises:
        FilterError: If expression is invalid
    """
    _validate_expression(expression, field_keys, FILTER_FUNCTIONS)


def filter_record(record: dict[str, Any], expression: str) -> bool:
    """Evaluate a filter expression against a record.

    Args:
        record: The record to evaluate
        expression: The filter expression (already resolved)

    Returns:
        True if record matches the filter, False otherwise

    Raises:
        FilterError: If the expression cannot be evaluated
    """
    return _filter_record(record, expression, FILTER_FUNCTIONS)


def extract_filter_keys(
    fields: list[dict[str, Any]],
    resolved_expr: str,
) -> list[str]:
    """Extract field keys used in a resolved filter expression.

    Args:
        fields: List of field definitions from Pipedrive
        resolved_expr: The filter expression with resolved keys

    Returns:
        List of field keys found in the expression
    """
    if not resolved_expr:
        return []

    # Build set of known field keys
    field_keys = {f.get("key", "") for f in fields}

    # Build set of known function names to exclude
    known_functions = set(FILTER_FUNCTIONS.keys()) | {
        "and", "or", "not", "True", "False", "None", "in", "null",
    }

    # Find string literal positions to exclude them
    string_positions: set[int] = set()
    for match in re.finditer(r"'[^']*'|\"[^\"]*\"", resolved_expr):
        for i in range(match.start(), match.end()):
            string_positions.add(i)

    # Pattern to match identifiers
    identifier_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'

    found_keys: list[str] = []
    for match in re.finditer(identifier_pattern, resolved_expr):
        if match.start() in string_positions:
            continue
        identifier = match.group(1)
        if identifier in known_functions:
            continue

        # Check for escaped digit-starting keys: _25da... → 25da...
        if identifier.startswith("_") and len(identifier) > 1 and identifier[1].isdigit():
            unescaped = identifier[1:]
            if unescaped in field_keys and unescaped not in found_keys:
                found_keys.append(unescaped)
        elif identifier in field_keys and identifier not in found_keys:
            found_keys.append(identifier)

    return found_keys


def resolve_field_prefixes(
    fields: list[dict[str, Any]],
    prefixes: list[str],
    fail_on_ambiguous: bool = False,
) -> list[str]:
    """Resolve field prefixes to full field keys.

    For --include/--exclude options, where ambiguous matches include all.
    Supports:
    - field("name") syntax for exact name lookup (case-insensitive)
    - Key and name prefix matching via find_field_matches()
    - Digit-starting keys via underscore escape prefix (_25da...)

    Args:
        fields: List of field definitions
        prefixes: List of user-provided prefixes or field("name") expressions
        fail_on_ambiguous: If True, raise error on ambiguous matches

    Returns:
        List of resolved field keys (deduplicated)
    """
    resolved: list[str] = []

    for prefix in prefixes:
        prefix = prefix.strip()
        if not prefix:
            continue

        # Check for field("name") syntax first
        field_match = FIELD_FUNC_PATTERN.match(prefix)
        if field_match:
            field_name = field_match.group(2)
            key = resolve_field_name(fields, field_name)
            if key:
                resolved.append(key)
            # Skip silently if not found (consistent with prefix behavior)
            continue

        # Fall back to prefix matching
        matches = find_field_matches(fields, prefix)

        if not matches:
            # No match: skip silently
            continue

        if len(matches) > 1 and fail_on_ambiguous:
            # Check if this was a key or name match for display
            prefix_lower = prefix.lower()
            if matches[0].get("key", "").lower().startswith(prefix_lower):
                match_display = [f["key"] for f in matches]
            else:
                match_display = [f"{f['key']} ({f.get('name', '')})" for f in matches]
            raise AmbiguousMatchError(prefix, match_display, "field")

        resolved.extend(f["key"] for f in matches)

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for key in resolved:
        if key not in seen:
            seen.add(key)
            unique.append(key)

    return unique


def select_fields(
    record: dict[str, Any],
    include_keys: list[str] | None,
    exclude_keys: list[str] | None,
) -> dict[str, Any]:
    """Select fields from a record based on include/exclude lists.

    Args:
        record: The full record
        include_keys: If provided, only include these fields
        exclude_keys: If provided, exclude these fields

    Returns:
        Filtered record with selected fields
    """
    if include_keys:
        return {k: v for k, v in record.items() if k in include_keys}
    elif exclude_keys:
        return {k: v for k, v in record.items() if k not in exclude_keys}
    else:
        return record


# Default columns to show when no --include is specified
DEFAULT_DISPLAY_COLUMNS = ["id", "name", "first_name", "last_name", "email", "title", "value"]
MAX_AUTO_COLUMNS = 8


def format_table(
    records: list[dict[str, Any]],
    fields: list[dict[str, Any]] | None,
    console: Console,
    title: str = "Search Results",
    show_all_columns: bool = False,
    filter_keys: list[str] | None = None,
) -> None:
    """Format records as a Rich table.

    Args:
        records: List of records to display
        fields: Field definitions for column name lookup (optional)
        console: Rich console for output
        title: Table title
        show_all_columns: If False, limit columns when there are many
        filter_keys: Field keys used in filter expression (added to display if truncated)
    """
    if not records:
        console.print("[dim]No matching records found.[/dim]")
        return

    # Determine columns from first record
    all_columns = list(records[0].keys())

    # Limit columns if there are too many and show_all_columns is False
    if not show_all_columns and len(all_columns) > MAX_AUTO_COLUMNS:
        # Use default columns that exist in the record
        columns = [c for c in DEFAULT_DISPLAY_COLUMNS if c in all_columns]
        # Add a few more if we don't have enough
        if len(columns) < MAX_AUTO_COLUMNS:
            for c in all_columns:
                if c not in columns:
                    columns.append(c)
                if len(columns) >= MAX_AUTO_COLUMNS:
                    break
        # Add filter columns at the end if not already included
        if filter_keys:
            for key in filter_keys:
                if key in all_columns and key not in columns:
                    columns.append(key)
        truncated = True
    else:
        columns = all_columns
        truncated = False

    # Build field name lookup for display
    field_names: dict[str, str] = {}
    if fields:
        for f in fields:
            field_names[f.get("key", "")] = f.get("name", f.get("key", ""))

    # Build option lookup for enum/set fields
    option_lookup = build_option_lookup(fields) if fields else {}

    table = Table(title=f"{title} ({len(records)} records)")

    for col in columns:
        display_name = field_names.get(col, col)
        style = "cyan" if col == "id" else None
        table.add_column(display_name, style=style, overflow="fold")

    for record in records:
        row: list[str] = []
        for col in columns:
            value = record.get(col, "")
            # Format enum/set values with labels
            if col in option_lookup:
                str_val = format_option_value(value, col, option_lookup)
            # Handle complex values (dicts, lists)
            elif isinstance(value, dict):
                # Try to extract a meaningful value (check None explicitly, not truthiness)
                if "name" in value and value["name"] is not None:
                    str_val = str(value["name"])
                elif "value" in value and value["value"] is not None:
                    str_val = str(value["value"])
                else:
                    str_val = str(value)
            elif isinstance(value, list):
                # Show first item
                if value and isinstance(value[0], dict):
                    item = value[0]
                    if "value" in item and item["value"] is not None:
                        str_val = str(item["value"])
                    elif "name" in item and item["name"] is not None:
                        str_val = str(item["name"])
                    else:
                        str_val = str(item)
                else:
                    str_val = str(value[0]) if value else ""
            else:
                str_val = str(value) if value is not None else ""
            # Truncate long values for table display
            if len(str_val) > 40:
                str_val = str_val[:37] + "..."
            row.append(str_val)
        table.add_row(*row)

    console.print(table)

    if truncated:
        console.print(
            f"[dim]Showing {len(columns)}/{len(all_columns)} columns. "
            f"Use -i to select columns or -o json for all data.[/dim]"
        )


def format_json(records: list[dict[str, Any]]) -> str:
    """Format records as JSON.

    Args:
        records: List of records to format

    Returns:
        JSON string with indentation
    """
    return json.dumps(records, indent=2, ensure_ascii=False, default=str)


def format_csv(records: list[dict[str, Any]]) -> str:
    """Format records as CSV.

    Args:
        records: List of records to format

    Returns:
        CSV string with header row
    """
    if not records:
        return ""

    output = io.StringIO()
    columns = list(records[0].keys())

    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()

    for record in records:
        # Flatten complex values to JSON strings
        flat: dict[str, Any] = {}
        for k, v in record.items():
            if isinstance(v, (dict, list)):
                flat[k] = json.dumps(v, ensure_ascii=False)
            else:
                flat[k] = v
        writer.writerow(flat)

    return output.getvalue()
