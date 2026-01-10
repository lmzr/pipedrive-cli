"""Field migration utilities for copying and transforming Pipedrive fields.

Supports type transformations between Pipedrive field types:
- varchar/text → int, double, date, enum, set
- int → varchar
- double → int, varchar
- date → varchar
- enum → varchar, text, set
- set → varchar, text, enum
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

import click


class TransformError(Exception):
    """Raised when a value cannot be transformed."""

    def __init__(self, value: Any, transform: str, reason: str):
        self.value = value
        self.transform = transform
        self.reason = reason
        super().__init__(f"Cannot transform {value!r} to {transform}: {reason}")


@dataclass
class TransformResult:
    """Result of a field value transformation."""

    success: bool
    value: Any = None
    error: str | None = None


@dataclass
class CopyStats:
    """Statistics for a field copy operation."""

    total: int = 0
    copied: int = 0
    skipped: int = 0
    failed: int = 0


def transform_to_int(value: Any, format_str: str | None = None) -> int:
    """Transform a value to int."""
    if value is None:
        raise TransformError(value, "int", "value is null")

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return round(value)

    if isinstance(value, str):
        value = value.strip()
        if not value:
            raise TransformError(value, "int", "empty string")
        try:
            # Handle floats in string form
            return round(float(value))
        except ValueError:
            raise TransformError(value, "int", "invalid number format")

    raise TransformError(value, "int", f"unsupported type {type(value).__name__}")


def transform_to_double(value: Any, format_str: str | None = None) -> float:
    """Transform a value to double (float)."""
    if value is None:
        raise TransformError(value, "double", "value is null")

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        value = value.strip()
        if not value:
            raise TransformError(value, "double", "empty string")
        try:
            return float(value)
        except ValueError:
            raise TransformError(value, "double", "invalid number format")

    raise TransformError(value, "double", f"unsupported type {type(value).__name__}")


def transform_to_varchar(
    value: Any, format_str: str | None = None, separator: str = ", "
) -> str:
    """Transform a value to varchar (string).

    Args:
        value: The value to transform
        format_str: Optional format string for numbers (e.g., ".2f") or dates (e.g., "%d/%m/%Y")
        separator: Separator for joining list/set values
    """
    if value is None:
        raise TransformError(value, "varchar", "value is null")

    # Handle list/array values (set fields)
    if isinstance(value, list):
        str_values = []
        for item in value:
            if isinstance(item, dict) and "label" in item:
                str_values.append(item["label"])
            elif isinstance(item, str):
                str_values.append(item)
            else:
                str_values.append(str(item))
        return separator.join(str_values)

    # Handle enum with label
    if isinstance(value, dict) and "label" in value:
        return value["label"]

    # Handle date strings
    if isinstance(value, str) and format_str:
        try:
            dt = datetime.strptime(value, "%Y-%m-%d")
            return dt.strftime(format_str)
        except ValueError:
            pass  # Not a date, return as-is

    # Handle numbers with format
    if isinstance(value, float) and format_str:
        return format(value, format_str)

    if isinstance(value, int) and format_str:
        return format(float(value), format_str)

    return str(value)


def transform_to_date(value: Any, format_str: str | None = None) -> str:
    """Transform a value to date string (YYYY-MM-DD).

    Args:
        value: The value to transform
        format_str: Input format string for parsing (e.g., "%d/%m/%Y")
    """
    if value is None:
        raise TransformError(value, "date", "value is null")

    if isinstance(value, str):
        value = value.strip()
        if not value:
            raise TransformError(value, "date", "empty string")

        # Try to parse with provided format
        if format_str:
            try:
                dt = datetime.strptime(value, format_str)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                raise TransformError(value, "date", f"does not match format {format_str}")

        # Already in ISO format?
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            try:
                datetime.strptime(value, "%Y-%m-%d")
                return value
            except ValueError:
                pass

        raise TransformError(value, "date", "unknown date format, use --format")

    raise TransformError(value, "date", f"unsupported type {type(value).__name__}")


def transform_to_enum(
    value: Any, format_str: str | None = None, separator: str | None = None
) -> str:
    """Transform a value for enum field (strip whitespace).

    Returns the value as-is for later option matching.
    """
    if value is None:
        raise TransformError(value, "enum", "value is null")

    if isinstance(value, str):
        return value.strip()

    if isinstance(value, list):
        # set → enum: take first element only
        if len(value) > 1:
            raise TransformError(value, "enum", "set has multiple values, cannot convert to enum")
        if len(value) == 0:
            raise TransformError(value, "enum", "empty set")

        item = value[0]
        if isinstance(item, dict) and "label" in item:
            return item["label"].strip()
        return str(item).strip()

    if isinstance(value, dict) and "label" in value:
        return value["label"].strip()

    return str(value).strip()


def transform_to_set(
    value: Any, format_str: str | None = None, separator: str = ","
) -> list[str]:
    """Transform a value for set field (split by separator, strip whitespace).

    Args:
        value: The value to transform
        format_str: Not used, for API consistency
        separator: Separator for splitting string values (default: ",")
    """
    if value is None:
        raise TransformError(value, "set", "value is null")

    if isinstance(value, list):
        result = []
        for item in value:
            if isinstance(item, dict) and "label" in item:
                result.append(item["label"].strip())
            elif isinstance(item, str):
                result.append(item.strip())
            else:
                result.append(str(item).strip())
        return result

    if isinstance(value, str):
        if not value.strip():
            raise TransformError(value, "set", "empty string")
        return [v.strip() for v in value.split(separator) if v.strip()]

    if isinstance(value, dict) and "label" in value:
        return [value["label"].strip()]

    return [str(value).strip()]


# Mapping of Pipedrive type names to transform functions
TRANSFORMS: dict[str, Callable[..., Any]] = {
    "int": transform_to_int,
    "double": transform_to_double,
    "varchar": transform_to_varchar,
    "text": transform_to_varchar,  # Same as varchar
    "date": transform_to_date,
    "enum": transform_to_enum,
    "set": transform_to_set,
}


def transform_value(
    value: Any,
    transform_type: str | None,
    format_str: str | None = None,
    separator: str | None = None,
) -> TransformResult:
    """Apply a transformation to a value.

    Args:
        value: The value to transform
        transform_type: Pipedrive field type (int, double, varchar, text, date, enum, set)
        format_str: Optional format string for the transformation
        separator: Optional separator for set/varchar transformations

    Returns:
        TransformResult with success status and transformed value or error
    """
    if transform_type is None:
        # No transformation, pass through
        return TransformResult(success=True, value=value)

    transform_func = TRANSFORMS.get(transform_type)
    if not transform_func:
        return TransformResult(
            success=False,
            error=f"Unknown transform type: {transform_type}",
        )

    try:
        kwargs: dict[str, Any] = {}
        if format_str:
            kwargs["format_str"] = format_str
        if separator:
            kwargs["separator"] = separator

        result = transform_func(value, **kwargs)
        return TransformResult(success=True, value=result)
    except TransformError as e:
        return TransformResult(success=False, error=str(e))


def collect_unique_values(records: list[dict], field_key: str) -> set[str]:
    """Collect unique string values from a field across all records.

    Args:
        records: List of records from Pipedrive
        field_key: The field key to collect values from

    Returns:
        Set of unique values (stripped of whitespace)
    """
    values = set()

    for record in records:
        value = record.get(field_key)
        if value is None:
            continue

        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                values.add(stripped)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and "label" in item:
                    stripped = item["label"].strip()
                elif isinstance(item, str):
                    stripped = item.strip()
                else:
                    stripped = str(item).strip()
                if stripped:
                    values.add(stripped)
        elif isinstance(value, dict) and "label" in value:
            stripped = value["label"].strip()
            if stripped:
                values.add(stripped)

    return values


def get_enum_options(field: dict) -> set[str]:
    """Get existing option labels from an enum/set field.

    Args:
        field: Field definition from Pipedrive API

    Returns:
        Set of existing option labels
    """
    options = field.get("options", [])
    return {opt.get("label", "") for opt in options if opt.get("label")}


def prompt_add_options(
    field_name: str, new_options: set[str], console: Any
) -> bool | None:
    """Prompt user to add new options to an enum/set field.

    Args:
        field_name: Name of the field
        new_options: New options to add
        console: Rich console for output

    Returns:
        True to add options, False to skip them, None to abort
    """
    if not new_options:
        return True

    console.print(f"\n[yellow]New values to add to '{field_name}':[/yellow]")
    for opt in sorted(new_options):
        console.print(f"  - {opt}")

    response = click.prompt(
        "Add these options? [Y/n/q]",
        default="y",
        show_default=False,
    ).lower().strip()

    if response == "q":
        return None
    if response in ("", "y", "yes"):
        return True
    return False


def get_option_usage(
    records: list[dict], field_key: str, options: list[dict]
) -> dict[str, int]:
    """Count usage of each option label in records.

    Args:
        records: List of records from datapackage
        field_key: The field key to count usage for
        options: List of option dicts with 'label' keys

    Returns:
        Dict mapping option label to usage count
    """
    # Initialize all options with 0 count
    usage: dict[str, int] = {opt.get("label", ""): 0 for opt in options if opt.get("label")}

    for record in records:
        value = record.get(field_key)
        if not value:
            continue

        # Handle set fields (comma-separated or list)
        if isinstance(value, list):
            values = value
        elif isinstance(value, str):
            # Could be single value or comma-separated
            values = [v.strip() for v in value.split(",")]
        else:
            values = [str(value)]

        for v in values:
            if v in usage:
                usage[v] += 1

    return usage


def sync_options_with_data(
    records: list[dict],
    field_key: str,
    current_options: list[dict],
) -> tuple[list[dict], list[str], list[str]]:
    """Sync field options with actual values found in data.

    Compares the values used in records with the defined options and returns
    updated options list along with what was added/unused.

    Args:
        records: List of records from datapackage
        field_key: The field key to sync
        current_options: Current option definitions

    Returns:
        Tuple of (updated_options, added_labels, unused_labels):
        - updated_options: New options list with missing values added
        - added_labels: Labels that were added (found in data but not in options)
        - unused_labels: Labels that are defined but not used in data
    """
    # Get current option labels
    current_labels = {opt.get("label", "") for opt in current_options if opt.get("label")}

    # Collect values used in data
    used_values = collect_unique_values(records, field_key)

    # Find missing options (in data but not in current options)
    missing_labels = used_values - current_labels

    # Find unused options (in options but not in data)
    unused_labels = current_labels - used_values

    # Generate new options for missing labels
    max_id = max((opt.get("id", 0) for opt in current_options), default=0)
    new_options = [
        {"id": max_id + i + 1, "label": label}
        for i, label in enumerate(sorted(missing_labels))
    ]

    # Build updated options list
    updated_options = list(current_options) + new_options

    return updated_options, sorted(missing_labels), sorted(unused_labels)


# -----------------------------------------------------------------------------
# Enum/Set Display Formatting
# -----------------------------------------------------------------------------


def build_option_lookup(fields: list[dict]) -> dict[str, dict[str, str]]:
    """Build lookup table for enum/set option labels.

    Creates a nested dict: {field_key: {option_id: option_label}}
    for efficient option label lookup when displaying records.

    Args:
        fields: List of field definitions with 'key', 'field_type', and 'options'

    Returns:
        Dict mapping field keys to dicts of {option_id: option_label}
    """
    lookup: dict[str, dict[str, str]] = {}
    for f in fields:
        if f.get("field_type") in ("enum", "set") and "options" in f:
            lookup[f["key"]] = {
                str(opt.get("id", "")): opt.get("label", "")
                for opt in f["options"]
                if opt.get("id") is not None
            }
    return lookup


def format_option_value(
    value: Any,
    field_key: str,
    option_lookup: dict[str, dict[str, str]],
) -> str:
    """Format enum/set value as 'label (id)'.

    Resolves option IDs to their labels for display.
    Handles both single enum values and comma-separated set values.

    Args:
        value: Raw value (option ID or comma-separated IDs for sets)
        field_key: Field key to lookup options
        option_lookup: Pre-built {field_key: {id: label}} lookup from build_option_lookup()

    Returns:
        Formatted string, e.g., "M. (37)" or "VIP (1), Premium (2)"
        Returns original value as string if not an enum/set field or ID not found.
    """
    if field_key not in option_lookup:
        return str(value) if value is not None else ""

    opts = option_lookup[field_key]
    str_val = str(value) if value is not None else ""

    if not str_val:
        return ""

    # Handle comma-separated set values
    if "," in str_val:
        parts = [v.strip() for v in str_val.split(",")]
        resolved = []
        for p in parts:
            if p in opts:
                resolved.append(f"{opts[p]} ({p})")
            else:
                resolved.append(p)
        return ", ".join(resolved)

    # Single value (enum)
    if str_val in opts:
        return f"{opts[str_val]} ({str_val})"

    return str_val
