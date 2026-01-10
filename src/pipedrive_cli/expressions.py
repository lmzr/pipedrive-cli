"""Expression evaluation utilities for filter and transform operations.

Provides shared functionality for:
- Field identifier resolution in expressions
- Expression evaluation with simpleeval
- Display formatting for resolved expressions
"""

import re
import warnings
from typing import Any, Callable

from simpleeval import EvalWithCompoundTypes, NameNotDefined

from .matching import AmbiguousMatchError, find_field_matches

# Type alias for ambiguous match callback
AmbiguousCallback = Callable[[str, list[dict[str, Any]]], str]

# Pattern for field("name") or field('name') - exact field name lookup
FIELD_FUNC_PATTERN = re.compile(r'field\(\s*(["\'])(.+?)\1\s*\)')


def resolve_field_name(fields: list[dict[str, Any]], name: str) -> str | None:
    """Resolve exact field name to key. Used by field("name") syntax.

    Case-insensitive exact match on field name.

    Args:
        fields: List of field definitions with 'key' and 'name' attributes
        name: Field name to look up (exact match, case-insensitive)

    Returns:
        Field key if exactly one match found, None otherwise
    """
    matching = [f for f in fields if f.get("name", "").lower() == name.lower()]
    if len(matching) == 1:
        return matching[0]["key"]
    return None


class FilterError(Exception):
    """Error during filter/transform expression evaluation."""

    pass


class EnumValue:
    """Wrapper for enum/set values supporting ID and label comparison.

    Enables filtering on enum/set fields by either:
    - Integer ID: field == 37
    - String ID: field == "37"
    - Label text: field == "Monsieur" (case-insensitive)
    """

    def __init__(self, raw_id: str, label: str | None):
        self.raw_id = raw_id
        self.label = label

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, int):
            # Compare with int ID
            return self.raw_id == str(other)
        if isinstance(other, str):
            # Compare with string ID or label (case-insensitive)
            return self.raw_id == other or (
                self.label is not None and self.label.lower() == other.lower()
            )
        return False

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __str__(self) -> str:
        return self.raw_id

    def __repr__(self) -> str:
        return f"EnumValue({self.raw_id!r}, {self.label!r})"


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
    *,
    on_ambiguous: AmbiguousCallback | None = None,
) -> str:
    """Resolve a field identifier to its exact key.

    Uses find_field_matches() from matching.py for core matching logic.
    Supports digit-starting field keys via underscore escape prefix.

    Resolution order (handled by find_field_matches):
    1. Exact key match
    2. Key prefix match (case-insensitive)
    3. Escaped digit-key prefix: _25 → matches keys starting with 25
    4. Exact name match (case-insensitive, with underscore→space normalization)
    5. Name prefix match (case-insensitive, with underscore→space normalization)
    6. No match: return identifier as-is

    Args:
        fields: List of field definitions from Pipedrive
        identifier: The identifier to resolve (key prefix or name prefix)
        on_ambiguous: Callback called when multiple matches found.
                      Receives (identifier, matches), returns selected key.
                      If None, raises AmbiguousMatchError.

    Returns:
        The resolved field key

    Raises:
        AmbiguousMatchError: If identifier matches multiple fields and no callback
    """
    matches = find_field_matches(fields, identifier)

    if not matches:
        # No match: return as-is (simpleeval will handle unknown variables)
        return identifier

    if len(matches) > 1:
        if on_ambiguous is not None:
            # Let the callback choose
            return on_ambiguous(identifier, matches)
        # Use keys for key-prefix matches, names for name-prefix matches
        # Check if this was a key or name match by comparing prefixes
        identifier_lower = identifier.lower()
        if matches[0].get("key", "").lower().startswith(identifier_lower):
            match_display = [f["key"] for f in matches]
        else:
            match_display = [f["name"] for f in matches]
        raise AmbiguousMatchError(identifier, match_display, "field")

    return matches[0]["key"]


def _find_string_positions(expression: str) -> set[int]:
    """Find all character positions inside string literals."""
    positions: set[int] = set()
    for match in re.finditer(r"'[^']*'|\"[^\"]*\"", expression):
        for i in range(match.start(), match.end()):
            positions.add(i)
    return positions


def _escape_digit_key(key: str) -> str:
    """Escape a field key that starts with a digit to make it a valid Python identifier.

    Args:
        key: The field key

    Returns:
        The key prefixed with '_' if it starts with a digit, otherwise unchanged
    """
    if key and key[0].isdigit():
        return f"_{key}"
    return key


def resolve_expression(
    fields: list[dict[str, Any]],
    expression: str,
    functions: dict[str, callable],
    *,
    on_ambiguous: AmbiguousCallback | None = None,
) -> tuple[str, dict[str, tuple[str, str]]]:
    """Resolve all field identifiers in an expression.

    Parses the expression to find identifiers and resolves each one
    using resolve_field_identifier().

    Supports field keys starting with digits by:
    1. Detecting hex-like patterns (e.g., '25da') that match field key prefixes
    2. Escaping resolved keys with '_' prefix when they start with digits

    Args:
        fields: List of field definitions from Pipedrive
        expression: The expression with potential field prefixes
        functions: Function dictionary (to exclude function names from resolution)
        on_ambiguous: Callback called when multiple matches found.
                      Receives (identifier, matches), returns selected key.
                      If None, raises AmbiguousMatchError.

    Returns:
        Tuple of (resolved_expression, resolutions_dict)
        where resolutions_dict maps original identifier to (key, name)
        Keys in the resolved expression are escaped with '_' prefix if they start with digits

    Raises:
        AmbiguousMatchError: If any identifier matches multiple fields and no callback
    """
    if not expression:
        return expression, {}

    # Build field lookup by key
    field_by_key: dict[str, dict] = {f.get("key", ""): f for f in fields}

    # Track resolutions for display (identifier -> (key, name))
    resolutions: dict[str, tuple[str, str]] = {}

    # Resolve field("name") calls FIRST - exact name lookup (case-insensitive)
    def _resolve_field_call(match: re.Match) -> str:
        quote_char = match.group(1)  # Preserve original quote style
        field_name = match.group(2)
        matching = [f for f in fields if f.get("name", "").lower() == field_name.lower()]
        if not matching:
            raise FilterError(f"Field not found: '{field_name}'")
        if len(matching) > 1:
            raise AmbiguousMatchError(
                field_name, [f["name"] for f in matching], "field"
            )
        resolved_key = matching[0]["key"]
        escaped_key = _escape_digit_key(resolved_key)
        # Track for display (preserve original quote style)
        resolutions[f"field({quote_char}{field_name}{quote_char})"] = (
            resolved_key,
            matching[0]["name"],
        )
        return escaped_key

    expression = FIELD_FUNC_PATTERN.sub(_resolve_field_call, expression)

    # Build set of known names to exclude from resolution
    known_names = set(functions.keys()) | EXPRESSION_KEYWORDS

    # Find string literal positions to exclude
    string_positions = _find_string_positions(expression)

    # Track replacements to make (identifier -> escaped_resolved_key)
    replacements: dict[str, str] = {}

    # First pass: detect hex-like patterns starting with digits (e.g., '25da')
    # These are potential field key prefixes that aren't valid Python identifiers
    # Pattern requires at least one hex letter (a-f) to avoid matching pure numbers like '25'
    hex_pattern = r'(?<![a-zA-Z0-9_])([0-9][a-fA-F0-9]*[a-fA-F][a-fA-F0-9]*)(?![a-zA-Z0-9_])'
    for match in re.finditer(hex_pattern, expression):
        if match.start() in string_positions:
            continue

        identifier = match.group(1)
        if identifier in replacements:
            continue

        # Try to resolve as field key prefix
        resolved = resolve_field_identifier(fields, identifier, on_ambiguous=on_ambiguous)
        if resolved != identifier:
            # Escape the resolved key since it starts with a digit
            escaped = _escape_digit_key(resolved)
            replacements[identifier] = escaped
            field_def = field_by_key.get(resolved, {})
            field_name = field_def.get("name", resolved)
            resolutions[identifier] = (resolved, field_name)

    # Second pass: standard Python identifiers
    identifier_pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'
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
        resolved = resolve_field_identifier(fields, identifier, on_ambiguous=on_ambiguous)
        if resolved != identifier:
            # Escape the resolved key if it starts with a digit
            escaped = _escape_digit_key(resolved)
            replacements[identifier] = escaped
            # Get the field name for display
            field_def = field_by_key.get(resolved, {})
            field_name = field_def.get("name", resolved)
            resolutions[identifier] = (resolved, field_name)

    # Apply replacements (longest first to avoid partial replacements)
    result = expression
    for old, new in sorted(replacements.items(), key=lambda x: -len(x[0])):
        # Use word boundary replacement for standard identifiers
        # For digit-starting patterns, use negative lookbehind/lookahead
        if old[0].isdigit():
            pattern = rf'(?<![a-zA-Z0-9_]){re.escape(old)}(?![a-zA-Z0-9_])'
        else:
            pattern = rf'\b{re.escape(old)}\b'

        new_result = []
        last_end = 0
        for match in re.finditer(pattern, result):
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


def _add_digit_key_aliases(names: dict[str, Any]) -> dict[str, Any]:
    """Add aliased names for keys starting with digits.

    Field keys starting with digits (e.g., '25da...') are escaped with '_' prefix
    in resolved expressions (e.g., '_25da...'). This function adds those aliased
    names to the evaluator's namespace.

    Args:
        names: The original names dict (field keys -> values)

    Returns:
        Names dict with additional aliased entries for digit-starting keys
    """
    result = dict(names)
    for key, value in names.items():
        if isinstance(key, str) and key and key[0].isdigit():
            result[f"_{key}"] = value
    return result


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
        Field keys starting with digits are accessible via '_' prefix alias.
    """
    evaluator = EvalWithCompoundTypes()
    # Add aliases for digit-starting keys (e.g., _25da... for 25da...)
    names_with_aliases = _add_digit_key_aliases(record)
    evaluator.names = {**names_with_aliases, **EXPRESSION_CONSTANTS}
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
