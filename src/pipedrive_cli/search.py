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
from simpleeval import EvalWithCompoundTypes

from .matching import AmbiguousMatchError


def _isint(s: Any) -> bool:
    """Check if value is or can be parsed as an integer."""
    if s is None or s == "":
        return False
    if isinstance(s, bool):
        return False
    if isinstance(s, int):
        return True
    if isinstance(s, float):
        return s == int(s)
    try:
        int(str(s).strip())
        return True
    except (ValueError, TypeError):
        return False


def _isfloat(s: Any) -> bool:
    """Check if value is or can be parsed as a float."""
    if s is None or s == "":
        return False
    if isinstance(s, bool):
        return False
    if isinstance(s, (int, float)):
        return True
    try:
        float(str(s).strip())
        return True
    except (ValueError, TypeError):
        return False


def _isnumeric(s: Any) -> bool:
    """Check if value is numeric (int or float)."""
    return _isfloat(s)


# Custom string functions for filter expressions
STRING_FUNCTIONS: dict[str, callable] = {
    "contains": lambda s, sub: sub.lower() in str(s).lower() if s else False,
    "startswith": lambda s, prefix: str(s).lower().startswith(prefix.lower()) if s else False,
    "endswith": lambda s, suffix: str(s).lower().endswith(suffix.lower()) if s else False,
    "lower": lambda s: str(s).lower() if s else "",
    "upper": lambda s: str(s).upper() if s else "",
    "len": lambda s: len(str(s)) if s else 0,
    "isnull": lambda s: s is None or s == "",
    "notnull": lambda s: s is not None and s != "",
    "isint": _isint,
    "isfloat": _isfloat,
    "isnumeric": _isnumeric,
    # String manipulation (shared with TRANSFORM_FUNCTIONS)
    "strip": lambda s: str(s).strip() if s else "",
    "lstrip": lambda s: str(s).lstrip() if s else "",
    "rstrip": lambda s: str(s).rstrip() if s else "",
    "replace": lambda s, old, new: str(s).replace(old, new) if s else "",
    "substr": lambda s, start, end=None: (
        str(s)[int(start) : int(end) if end is not None else None] if s else ""
    ),
    "lpad": lambda s, width, char=" ": str(s).rjust(int(width), char) if s else "",
    "rpad": lambda s, width, char=" ": str(s).ljust(int(width), char) if s else "",
    "concat": lambda *args: "".join(str(a) if a else "" for a in args),
}


class FilterError(Exception):
    """Error during filter expression evaluation."""

    pass


def resolve_field_identifier(
    fields: list[dict[str, Any]],
    identifier: str,
) -> str:
    """Resolve a field identifier to its exact key.

    Resolution order:
    1. Exact key match
    2. Key prefix match (case-insensitive, unique)
    3. Exact name match (case-insensitive, with underscore→space normalization)
    4. Name prefix match (case-insensitive, with underscore→space normalization)
    5. No match: return identifier as-is

    Args:
        fields: List of field definitions from Pipedrive
        identifier: The identifier to resolve (key prefix or name prefix)

    Returns:
        The resolved field key

    Raises:
        AmbiguousMatchError: If identifier matches multiple fields
    """
    identifier_lower = identifier.lower()
    # Normalize underscores to spaces for name matching (tel_s → tel s)
    identifier_normalized = identifier_lower.replace("_", " ")

    # 1. Exact key match
    for field in fields:
        if field.get("key") == identifier:
            return identifier

    # 2. Key prefix match (case-insensitive)
    key_matches = [
        f for f in fields
        if f.get("key", "").lower().startswith(identifier_lower)
    ]
    if len(key_matches) == 1:
        return key_matches[0]["key"]
    if len(key_matches) > 1:
        match_keys = [f["key"] for f in key_matches]
        raise AmbiguousMatchError(identifier, match_keys, "field")

    # 3. Exact name match (case-insensitive, with normalization)
    for field in fields:
        field_name_lower = field.get("name", "").lower()
        if field_name_lower == identifier_lower or field_name_lower == identifier_normalized:
            return field["key"]

    # 4. Name prefix match (case-insensitive, with normalization)
    name_matches = [
        f for f in fields
        if f.get("name", "").lower().startswith(identifier_lower)
        or f.get("name", "").lower().startswith(identifier_normalized)
    ]
    if len(name_matches) == 1:
        return name_matches[0]["key"]
    if len(name_matches) > 1:
        match_display = [f"{f['key']} ({f.get('name', '')})" for f in name_matches]
        raise AmbiguousMatchError(identifier, match_display, "field")

    # 5. No match: return as-is (simpleeval will handle unknown variables)
    return identifier


def resolve_filter_expression(
    fields: list[dict[str, Any]],
    expression: str,
) -> tuple[str, dict[str, tuple[str, str]]]:
    """Resolve all field identifiers in a filter expression.

    Parses the expression to find identifiers and resolves each one
    using resolve_field_identifier().

    Args:
        fields: List of field definitions from Pipedrive
        expression: The filter expression with potential field prefixes

    Returns:
        Tuple of (resolved_expression, resolutions_dict)
        where resolutions_dict maps original identifier to (key, name)

    Raises:
        AmbiguousMatchError: If any identifier matches multiple fields
    """
    if not expression:
        return expression, {}

    # Build set of known function names to exclude from resolution
    known_functions = set(STRING_FUNCTIONS.keys()) | {
        "and", "or", "not", "True", "False", "None", "in",
    }

    # Build field lookup by key
    field_by_key: dict[str, dict] = {f.get("key", ""): f for f in fields}

    # First, find all string literal positions to exclude them
    string_positions: set[int] = set()
    for match in re.finditer(r"'[^']*'|\"[^\"]*\"", expression):
        for i in range(match.start(), match.end()):
            string_positions.add(i)

    # Pattern to match identifiers (Python-style variable names)
    identifier_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'

    # Track replacements to make (identifier -> resolved_key)
    replacements: dict[str, str] = {}
    # Track resolutions for display (identifier -> (key, name))
    resolutions: dict[str, tuple[str, str]] = {}

    for match in re.finditer(identifier_pattern, expression):
        # Skip if inside a string literal
        if match.start() in string_positions:
            continue

        identifier = match.group(1)

        # Skip known functions, keywords, and already-resolved identifiers
        if identifier in known_functions:
            continue
        if identifier in replacements:
            continue

        # Resolve the identifier
        resolved = resolve_field_identifier(fields, identifier)
        if resolved != identifier:
            replacements[identifier] = resolved
            # Get the field name for display
            field_def = field_by_key.get(resolved, {})
            field_name = field_def.get("name", resolved)
            resolutions[identifier] = (resolved, field_name)

    # Apply replacements (longest first to avoid partial replacements)
    result = expression
    for old, new in sorted(replacements.items(), key=lambda x: -len(x[0])):
        # Use word boundary replacement to avoid partial matches
        # But only outside string literals
        new_result = []
        last_end = 0
        for match in re.finditer(rf'\b{re.escape(old)}\b', result):
            if match.start() not in string_positions:
                new_result.append(result[last_end:match.start()])
                new_result.append(new)
                last_end = match.end()
        new_result.append(result[last_end:])
        result = "".join(new_result) if new_result else result

    return result, resolutions


def format_resolved_expression(
    original_expr: str,
    resolved_expr: str,
    resolutions: dict[str, tuple[str, str]],
) -> tuple[str, str]:
    """Format resolved expression for display.

    Returns two lines:
    1. Expression with field names (human-readable)
    2. Expression with field keys (for execution)

    Args:
        original_expr: The original user expression
        resolved_expr: The expression with resolved keys
        resolutions: Dict mapping identifier -> (key, name)

    Returns:
        Tuple of (name_line, key_line)
    """
    if not resolutions:
        # No resolution happened
        return resolved_expr, ""

    # Build expression with names
    name_expr = original_expr
    for identifier, (key, name) in sorted(resolutions.items(), key=lambda x: -len(x[0])):
        # Quote names with spaces
        display_name = f'"{name}"' if " " in name else name
        name_expr = re.sub(rf'\b{re.escape(identifier)}\b', display_name, name_expr)

    return name_expr, resolved_expr


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
    known_functions = set(STRING_FUNCTIONS.keys()) | {
        "and", "or", "not", "True", "False", "None", "in",
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
        if identifier in field_keys and identifier not in found_keys:
            found_keys.append(identifier)

    return found_keys


def _coerce_numeric(value: Any) -> Any:
    """Try to coerce a string value to a number for comparisons.

    CSV files load all values as strings, but we want numeric comparisons
    to work naturally (e.g., "30" > 25 should evaluate correctly).
    """
    if isinstance(value, str):
        try:
            # Try int first
            return int(value)
        except ValueError:
            try:
                # Try float
                return float(value)
            except ValueError:
                pass
    return value


def create_evaluator(record: dict[str, Any]) -> EvalWithCompoundTypes:
    """Create a simpleeval evaluator with record fields as names.

    Args:
        record: The record whose fields become available as variables

    Returns:
        Configured evaluator instance
    """
    evaluator = EvalWithCompoundTypes()
    # Copy record and coerce numeric strings for better comparisons
    coerced_record = {k: _coerce_numeric(v) for k, v in record.items()}
    evaluator.names = coerced_record
    evaluator.functions = {**evaluator.functions, **STRING_FUNCTIONS}
    return evaluator


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
    if not expression:
        return True

    evaluator = create_evaluator(record)
    try:
        result = evaluator.eval(expression)
        return bool(result)
    except Exception as e:
        raise FilterError(f"Filter evaluation error: {e}")


def resolve_field_prefixes(
    fields: list[dict[str, Any]],
    prefixes: list[str],
    fail_on_ambiguous: bool = False,
) -> list[str]:
    """Resolve field prefixes to full field keys.

    For --include/--exclude options, where ambiguous matches include all.

    Args:
        fields: List of field definitions
        prefixes: List of user-provided prefixes
        fail_on_ambiguous: If True, raise error on ambiguous matches

    Returns:
        List of resolved field keys (deduplicated)
    """
    resolved: list[str] = []

    for prefix in prefixes:
        prefix = prefix.strip()
        if not prefix:
            continue

        prefix_lower = prefix.lower()
        # Normalize underscores to spaces for name matching (tel_s → tel s)
        prefix_normalized = prefix_lower.replace("_", " ")

        # Exact key match
        exact = [f["key"] for f in fields if f.get("key") == prefix]
        if exact:
            resolved.extend(exact)
            continue

        # Key prefix match (case-insensitive)
        key_matches = [
            f["key"] for f in fields
            if f.get("key", "").lower().startswith(prefix_lower)
        ]
        if key_matches:
            if len(key_matches) > 1 and fail_on_ambiguous:
                raise AmbiguousMatchError(prefix, key_matches, "field")
            resolved.extend(key_matches)
            continue

        # Exact name match (case-insensitive, with normalization)
        name_exact = [
            f["key"] for f in fields
            if f.get("name", "").lower() == prefix_lower
            or f.get("name", "").lower() == prefix_normalized
        ]
        if name_exact:
            resolved.extend(name_exact)
            continue

        # Name prefix match (case-insensitive, with normalization)
        name_matches = [
            f["key"] for f in fields
            if f.get("name", "").lower().startswith(prefix_lower)
            or f.get("name", "").lower().startswith(prefix_normalized)
        ]
        if name_matches:
            if len(name_matches) > 1 and fail_on_ambiguous:
                match_display = [f"{f['key']} ({f.get('name', '')})" for f in fields
                                 if f["key"] in name_matches]
                raise AmbiguousMatchError(prefix, match_display, "field")
            resolved.extend(name_matches)

        # No match: skip silently

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

    table = Table(title=f"{title} ({len(records)} records)")

    for col in columns:
        display_name = field_names.get(col, col)
        style = "cyan" if col == "id" else None
        table.add_column(display_name, style=style, overflow="fold")

    for record in records:
        row: list[str] = []
        for col in columns:
            value = record.get(col, "")
            # Handle complex values (dicts, lists)
            if isinstance(value, dict):
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
