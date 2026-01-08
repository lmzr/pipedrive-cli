"""Expression evaluation utilities for filter and transform operations.

Provides shared functionality for:
- Field identifier resolution in expressions
- Expression evaluation with simpleeval
- Display formatting for resolved expressions
"""

import re
import warnings
from typing import Any

from simpleeval import EvalWithCompoundTypes, NameNotDefined

from .matching import AmbiguousMatchError


class FilterError(Exception):
    """Error during filter/transform expression evaluation."""

    pass


# -----------------------------------------------------------------------------
# Type checking functions
# -----------------------------------------------------------------------------


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


# -----------------------------------------------------------------------------
# Function dictionaries
# -----------------------------------------------------------------------------

# Base functions available in all expression contexts
BASE_FUNCTIONS: dict[str, callable] = {
    # String matching (case-insensitive)
    "contains": lambda s, sub: sub.lower() in str(s).lower() if s else False,
    "startswith": lambda s, prefix: str(s).lower().startswith(prefix.lower()) if s else False,
    "endswith": lambda s, suffix: str(s).lower().endswith(suffix.lower()) if s else False,
    # String manipulation
    "lower": lambda s: str(s).lower() if s else "",
    "upper": lambda s: str(s).upper() if s else "",
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
    "len": lambda s: len(str(s)) if s else 0,
    # Null checks
    "isnull": lambda s: s is None or s == "",
    "notnull": lambda s: s is not None and s != "",
    # Type checks
    "isint": _isint,
    "isfloat": _isfloat,
    "isnumeric": _isnumeric,
}

# Functions for filter expressions (search)
FILTER_FUNCTIONS: dict[str, callable] = {
    **BASE_FUNCTIONS,
}

# Functions for transform expressions (value update)
# String functions return original value (None) when input is None, unlike filter functions
TRANSFORM_FUNCTIONS: dict[str, callable] = {
    **BASE_FUNCTIONS,
    # String manipulation - preserve None (override BASE_FUNCTIONS behavior)
    "lower": lambda s: str(s).lower() if s else s,
    "upper": lambda s: str(s).upper() if s else s,
    "strip": lambda s: str(s).strip() if s else s,
    "lstrip": lambda s: str(s).lstrip() if s else s,
    "rstrip": lambda s: str(s).rstrip() if s else s,
    "replace": lambda s, old, new: str(s).replace(old, new) if s else s,
    "substr": lambda s, start, end=None: (
        str(s)[int(start) : int(end) if end is not None else None] if s else s
    ),
    "lpad": lambda s, width, char=" ": str(s).rjust(int(width), char) if s else s,
    "rpad": lambda s, width, char=" ": str(s).ljust(int(width), char) if s else s,
    # Type conversion
    "int": lambda s: int(float(s)) if s else 0,
    "float": lambda s: float(s) if s else 0.0,
    "str": lambda s: str(s) if s is not None else "",
    # Numeric functions
    "round": lambda n, d=0: round(float(n), int(d)) if n else 0,
    "abs": lambda n: abs(float(n)) if n else 0,
    # Conditional (iif to avoid conflict with Python's if keyword)
    "iif": lambda cond, then, else_: then if cond else else_,
    "coalesce": lambda *args: next((a for a in args if a is not None and a != ""), None),
}

# Built-in constants available in expressions
EXPRESSION_CONSTANTS: dict[str, Any] = {
    "null": None,
}

# Keywords to exclude from field resolution
EXPRESSION_KEYWORDS: set[str] = {
    "and", "or", "not", "True", "False", "None", "in", "null",
}


# -----------------------------------------------------------------------------
# Field resolution
# -----------------------------------------------------------------------------


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
    for f in fields:
        if f.get("key", "") == identifier:
            return identifier

    # 2. Key prefix match
    key_matches = [
        f for f in fields
        if f.get("key", "").lower().startswith(identifier_lower)
    ]
    if len(key_matches) == 1:
        return key_matches[0]["key"]
    if len(key_matches) > 1:
        matched_keys = [f["key"] for f in key_matches]
        raise AmbiguousMatchError(identifier, matched_keys, "field")

    # 3. Exact name match (case-insensitive)
    for f in fields:
        name = f.get("name", "").lower()
        if name == identifier_lower or name == identifier_normalized:
            return f["key"]

    # 4. Name prefix match
    name_matches = [
        f for f in fields
        if f.get("name", "").lower().startswith(identifier_lower)
        or f.get("name", "").lower().startswith(identifier_normalized)
    ]
    if len(name_matches) == 1:
        return name_matches[0]["key"]
    if len(name_matches) > 1:
        matched_names = [f["name"] for f in name_matches]
        raise AmbiguousMatchError(identifier, matched_names, "field")

    # 5. No match: return as-is (simpleeval will handle unknown variables)
    return identifier


def _find_string_positions(expression: str) -> set[int]:
    """Find all character positions inside string literals."""
    positions: set[int] = set()
    for match in re.finditer(r"'[^']*'|\"[^\"]*\"", expression):
        for i in range(match.start(), match.end()):
            positions.add(i)
    return positions


def resolve_expression(
    fields: list[dict[str, Any]],
    expression: str,
    functions: dict[str, callable],
) -> tuple[str, dict[str, tuple[str, str]]]:
    """Resolve all field identifiers in an expression.

    Parses the expression to find identifiers and resolves each one
    using resolve_field_identifier().

    Args:
        fields: List of field definitions from Pipedrive
        expression: The expression with potential field prefixes
        functions: Function dictionary (to exclude function names from resolution)

    Returns:
        Tuple of (resolved_expression, resolutions_dict)
        where resolutions_dict maps original identifier to (key, name)

    Raises:
        AmbiguousMatchError: If any identifier matches multiple fields
    """
    if not expression:
        return expression, {}

    # Build set of known names to exclude from resolution
    known_names = set(functions.keys()) | EXPRESSION_KEYWORDS

    # Build field lookup by key
    field_by_key: dict[str, dict] = {f.get("key", ""): f for f in fields}

    # Find string literal positions to exclude
    string_positions = _find_string_positions(expression)

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
        if identifier in known_names:
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
        # Use word boundary replacement, but only outside string literals
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
        Tuple of (name_line, key_line) where key_line is empty if no resolution
    """
    if not resolutions:
        return resolved_expr, ""

    # Build expression with names
    name_expr = original_expr
    for identifier, (key, name) in sorted(resolutions.items(), key=lambda x: -len(x[0])):
        # Quote names with spaces
        display_name = f'"{name}"' if " " in name else name
        name_expr = re.sub(rf'\b{re.escape(identifier)}\b', display_name, name_expr)

    return name_expr, resolved_expr


# -----------------------------------------------------------------------------
# Evaluator creation
# -----------------------------------------------------------------------------


def create_evaluator(
    record: dict[str, Any],
    functions: dict[str, callable],
) -> EvalWithCompoundTypes:
    """Create a simpleeval evaluator with record fields as names.

    Args:
        record: The record whose fields become available as variables
        functions: Function dictionary to use

    Returns:
        Configured evaluator instance

    Note:
        No automatic type coercion is performed. Use int(), float(), str()
        functions explicitly in expressions when type conversion is needed.
    """
    evaluator = EvalWithCompoundTypes()
    evaluator.names = {**record, **EXPRESSION_CONSTANTS}
    evaluator.functions = {**evaluator.functions, **functions}
    return evaluator


def validate_expression(
    expression: str,
    field_keys: set[str],
    functions: dict[str, callable],
) -> None:
    """Validate expression syntax before batch evaluation.

    Does a test evaluation with dummy record to catch:
    - Syntax errors
    - Assignment attempts (= instead of ==)
    - Multiple expressions (;)
    - Unknown functions

    Args:
        expression: The expression to validate
        field_keys: Set of valid field keys
        functions: Function dictionary to use

    Raises:
        FilterError: If expression is invalid
    """
    if not expression:
        return

    # Create dummy record with all field keys set to 0
    # Using 0 allows both numeric and string comparisons to work
    dummy_record = {k: 0 for k in field_keys}

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        evaluator = create_evaluator(dummy_record, functions)
        try:
            evaluator.eval(expression)
        except NameNotDefined:
            # Unknown field - will be caught at runtime with real record
            pass
        except TypeError:
            # Type mismatch with dummy values - valid at runtime with real data
            pass
        except Exception as e:
            raise FilterError(f"Invalid expression: {e}")

        # Convert warnings to errors
        for w in caught:
            msg = str(w.message).lower()
            if "assignment" in msg:
                raise FilterError("Assignment '=' not allowed (use '==' for comparison)")
            if "multiple" in msg:
                raise FilterError("Multiple expressions not allowed (remove ';')")


def evaluate_expression(
    record: dict[str, Any],
    expression: str,
    functions: dict[str, callable],
) -> Any:
    """Evaluate an expression with record fields as variables.

    Args:
        record: The record whose fields become available as variables
        expression: The expression to evaluate (already resolved)
        functions: Function dictionary to use

    Returns:
        The evaluated result

    Raises:
        Exception: If evaluation fails
    """
    evaluator = create_evaluator(record, functions)
    return evaluator.eval(expression)


def filter_record(
    record: dict[str, Any],
    expression: str,
    functions: dict[str, callable],
) -> bool:
    """Evaluate a filter expression against a record.

    Args:
        record: The record to evaluate
        expression: The filter expression (already resolved)
        functions: Function dictionary to use

    Returns:
        True if record matches the filter, False otherwise

    Raises:
        FilterError: If the expression cannot be evaluated
    """
    if not expression:
        return True

    try:
        result = evaluate_expression(record, expression, functions)
        return bool(result)
    except Exception as e:
        raise FilterError(f"Filter evaluation error: {e}")
