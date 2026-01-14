"""Local datapackage operations for offline field commands."""

import csv
import hashlib
import json
import time
from pathlib import Path
from typing import Any, Callable

from frictionless import Package

# -----------------------------------------------------------------------------
# Type coercion for CSV loading
# -----------------------------------------------------------------------------

# Frictionless type to Python coercion function mapping
FRICTIONLESS_TYPE_COERCERS: dict[str, Callable[[str], Any]] = {
    "integer": lambda v: int(v) if v else None,
    "number": lambda v: float(v) if v else None,
    "boolean": lambda v: v.lower() in ("true", "1", "yes") if v else None,
    "date": lambda v: v if v else None,  # Keep as ISO string
    "datetime": lambda v: v if v else None,  # Keep as ISO string
    "array": lambda v: json.loads(v) if v else None,
    "object": lambda v: json.loads(v) if v else None,
    "string": lambda v: v if v else None,
}


def get_schema_field_types(package: Package, entity_name: str) -> dict[str, str]:
    """Get mapping of field names to Frictionless types.

    Args:
        package: The loaded Frictionless Package
        entity_name: Name of the entity resource

    Returns:
        Dict mapping field name to Frictionless type (e.g., {"id": "integer"})
    """
    resource = get_entity_resource(package, entity_name)
    if resource is None:
        return {}
    return {field.name: field.type for field in resource.schema.fields}


def coerce_value(value: str, field_type: str) -> Any:
    """Coerce a CSV string value to its schema-defined Python type.

    Args:
        value: Raw string value from CSV
        field_type: Frictionless type name (integer, number, boolean, etc.)

    Returns:
        Coerced value or original string if coercion fails
    """
    if value is None or value == "":
        return None

    coercer = FRICTIONLESS_TYPE_COERCERS.get(field_type)
    if coercer is None:
        return value  # Unknown type, return as-is

    try:
        return coercer(value)
    except (ValueError, TypeError, json.JSONDecodeError):
        return value  # Coercion failed, return original string


def generate_local_field_key() -> str:
    """Generate a unique local field key.

    Returns a key in format '_new_<hash7>' that identifies a locally created field
    not yet synced to Pipedrive.
    """
    timestamp = str(time.time()).encode()
    hash_val = hashlib.sha256(timestamp).hexdigest()[:7]
    return f"_new_{hash_val}"


def is_local_field(field: dict[str, Any]) -> bool:
    """Check if a field is locally created (not synced to Pipedrive).

    Local fields have keys starting with '_new_' prefix.
    """
    key = field.get("key", "")
    return key.startswith("_new_")


def create_field_definition(
    name: str,
    field_type: str,
    options: list[str] | None = None,
) -> dict[str, Any]:
    """Create a new Pipedrive field definition with a local key.

    Args:
        name: Display name for the field
        field_type: Pipedrive field type (varchar, enum, set, etc.)
        options: Option labels for enum/set fields

    Returns:
        Field definition dict compatible with pipedrive_fields schema
    """
    key = generate_local_field_key()
    field_def: dict[str, Any] = {
        "key": key,
        "name": name,
        "field_type": field_type,
        "edit_flag": True,  # Mark as custom/editable field
    }

    # Add options for enum/set fields
    if options and field_type in ("enum", "set"):
        field_def["options"] = [
            {"id": i + 1, "label": opt} for i, opt in enumerate(options)
        ]

    return field_def


def load_package(base_path: Path) -> Package:
    """Load datapackage from path."""
    datapackage_path = base_path / "datapackage.json"
    if not datapackage_path.exists():
        raise FileNotFoundError(f"datapackage.json not found in {base_path}")
    return Package(str(datapackage_path))


def get_entity_resource(package: Package, entity_name: str) -> Any:
    """Get resource for an entity from the package."""
    for resource in package.resources:
        if resource.name == entity_name:
            return resource
    return None


def get_entity_fields(package: Package, entity_name: str) -> list[dict[str, Any]]:
    """Get pipedrive_fields from resource schema."""
    resource = get_entity_resource(package, entity_name)
    if resource is None:
        return []
    custom = getattr(resource.schema, "custom", {}) or {}
    # Handle Frictionless nested custom structure: {'custom': {'pipedrive_fields': ...}}
    if "custom" in custom:
        custom = custom["custom"]
    return custom.get("pipedrive_fields", [])


def save_package(package: Package, base_path: Path) -> None:
    """Save datapackage.json with updated fields."""
    datapackage_path = base_path / "datapackage.json"
    package.to_json(str(datapackage_path))


def load_records(
    base_path: Path,
    entity_name: str,
    coerce_types: bool = True,
) -> list[dict[str, Any]]:
    """Load records from CSV file with optional type coercion.

    Args:
        base_path: Path to the datapackage directory
        entity_name: Name of the entity (e.g., 'persons')
        coerce_types: If True, coerce values according to Frictionless schema types

    Returns:
        List of record dicts with values coerced to their schema types
    """
    csv_path = base_path / f"{entity_name}.csv"
    if not csv_path.exists():
        return []

    # Load field types from schema if coercion enabled
    field_types: dict[str, str] = {}
    if coerce_types:
        try:
            package = load_package(base_path)
            field_types = get_schema_field_types(package, entity_name)
        except FileNotFoundError:
            pass  # No datapackage, skip type coercion

    records: list[dict[str, Any]] = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed_row: dict[str, Any] = {}
            for key, value in row.items():
                # Handle JSON-encoded complex values first (array/object)
                if value and value.startswith(("{", "[")):
                    try:
                        parsed_row[key] = json.loads(value)
                        continue
                    except json.JSONDecodeError:
                        pass

                # Apply type coercion if schema type is known
                if coerce_types and key in field_types:
                    parsed_row[key] = coerce_value(value, field_types[key])
                else:
                    parsed_row[key] = value

            records.append(parsed_row)
    return records


def save_records(
    base_path: Path, entity_name: str, records: list[dict[str, Any]]
) -> None:
    """Save records to CSV file."""
    if not records:
        return

    csv_path = base_path / f"{entity_name}.csv"
    fieldnames = list(records[0].keys())

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            # Flatten complex values to JSON strings
            flat_record: dict[str, Any] = {}
            for key, value in record.items():
                if isinstance(value, (dict, list)):
                    flat_record[key] = json.dumps(value, ensure_ascii=False)
                else:
                    flat_record[key] = value
            writer.writerow(flat_record)


def update_entity_fields(
    package: Package, entity_name: str, fields: list[dict[str, Any]]
) -> None:
    """Update pipedrive_fields in resource schema."""
    resource = get_entity_resource(package, entity_name)
    if resource is None:
        raise ValueError(f"Entity '{entity_name}' not found in datapackage")

    if not hasattr(resource.schema, "custom") or resource.schema.custom is None:
        resource.schema.custom = {}
    resource.schema.custom["pipedrive_fields"] = fields


def remove_field_from_records(
    records: list[dict[str, Any]], field_key: str
) -> list[dict[str, Any]]:
    """Remove a field from all records."""
    for record in records:
        if field_key in record:
            del record[field_key]
    return records


def copy_field_in_records(
    records: list[dict[str, Any]],
    from_key: str,
    to_key: str,
    transform_func: callable | None = None,
) -> tuple[int, int, int]:
    """Copy field values from one key to another in records.

    Returns:
        Tuple of (copied, skipped, failed) counts.
    """
    copied = 0
    skipped = 0
    failed = 0

    for record in records:
        source_value = record.get(from_key)
        if source_value is None or source_value == "":
            skipped += 1
            continue

        try:
            if transform_func:
                target_value = transform_func(source_value)
            else:
                target_value = source_value
            record[to_key] = target_value
            copied += 1
        except Exception:
            failed += 1

    return copied, skipped, failed


def add_schema_field(
    package: Package,
    entity_name: str,
    field_name: str,
    field_type: str = "string",
) -> None:
    """Add a field to the Frictionless table schema (schema.fields)."""
    resource = get_entity_resource(package, entity_name)
    if resource is None:
        raise ValueError(f"Entity '{entity_name}' not found in datapackage")

    # Check if field already exists
    for field in resource.schema.fields:
        if field.name == field_name:
            return  # Already exists

    # Add new field (use StringField for simplicity - Frictionless infers actual type)
    from frictionless import fields as frictionless_fields
    new_field = frictionless_fields.StringField(name=field_name)
    resource.schema.add_field(new_field)


def remove_schema_field(
    package: Package,
    entity_name: str,
    field_name: str,
) -> None:
    """Remove a field from the Frictionless table schema (schema.fields)."""
    resource = get_entity_resource(package, entity_name)
    if resource is None:
        return

    resource.schema.fields = [
        f for f in resource.schema.fields if f.name != field_name
    ]


def rename_schema_field(
    package: Package,
    entity_name: str,
    old_name: str,
    new_name: str,
) -> None:
    """Rename a field in the Frictionless table schema (schema.fields)."""
    resource = get_entity_resource(package, entity_name)
    if resource is None:
        return

    for field in resource.schema.fields:
        if field.name == old_name:
            field.name = new_name
            break


def rename_field_key(
    package: Package,
    entity_name: str,
    old_key: str,
    new_key: str,
) -> None:
    """Rename a field key in pipedrive_fields and schema.fields."""
    # Update pipedrive_fields
    fields = get_entity_fields(package, entity_name)
    for field in fields:
        if field.get("key") == old_key:
            field["key"] = new_key
            break
    update_entity_fields(package, entity_name, fields)

    # Update schema.fields
    rename_schema_field(package, entity_name, old_key, new_key)


def rename_csv_column(
    base_path: Path,
    entity_name: str,
    old_key: str,
    new_key: str,
) -> None:
    """Rename a column in the CSV file."""
    records = load_records(base_path, entity_name)
    if not records:
        return

    for record in records:
        if old_key in record:
            record[new_key] = record.pop(old_key)
    save_records(base_path, entity_name, records)


# -----------------------------------------------------------------------------
# Schema diff and merge functions
# -----------------------------------------------------------------------------


def get_csv_columns(base_path: Path, entity_name: str) -> set[str]:
    """Get column names from entity CSV file.

    Args:
        base_path: Path to the datapackage directory
        entity_name: Name of the entity (e.g., 'persons')

    Returns:
        Set of column names from the CSV header
    """
    csv_path = base_path / f"{entity_name}.csv"
    if not csv_path.exists():
        return set()

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        return set(header) if header else set()


def diff_field_metadata(
    target_fields: list[dict[str, Any]],
    source_fields: list[dict[str, Any]],
    target_csv_columns: set[str],
) -> dict[str, list[dict[str, Any]]]:
    """Compare field metadata between two datapackages.

    Args:
        target_fields: pipedrive_fields from target datapackage
        source_fields: pipedrive_fields from source datapackage
        target_csv_columns: Column names from target CSV file

    Returns:
        Dict with keys:
            'in_source_only': Fields in source but not in target (merge candidates)
            'in_target_only': Fields in target but not in source (local-only or deleted)
            'in_csv_no_metadata': CSV columns without metadata in target
            'common': Fields in both target and source
    """
    target_keys = {f.get("key") for f in target_fields}
    source_keys = {f.get("key") for f in source_fields}

    # Build lookup dicts
    target_by_key = {f.get("key"): f for f in target_fields}
    source_by_key = {f.get("key"): f for f in source_fields}

    # Fields in source but not in target
    in_source_only = [source_by_key[k] for k in (source_keys - target_keys)]

    # Fields in target but not in source
    in_target_only = [target_by_key[k] for k in (target_keys - source_keys)]

    # Fields in both
    common = [target_by_key[k] for k in (target_keys & source_keys)]

    # CSV columns without metadata
    in_csv_no_metadata = [
        {"key": col, "name": col, "inferred": True}
        for col in (target_csv_columns - target_keys)
    ]

    return {
        "in_source_only": in_source_only,
        "in_target_only": in_target_only,
        "in_csv_no_metadata": in_csv_no_metadata,
        "common": common,
    }


def merge_field_metadata(
    target_fields: list[dict[str, Any]],
    source_fields: list[dict[str, Any]],
    target_csv_columns: set[str],
    exclude_keys: set[str] | None = None,
    include_only_keys: set[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Merge source fields into target (non-destructive).

    Only adds fields from source that:
    1. Don't exist in target (no overwrite)
    2. Have corresponding data in target CSV
    3. Are not in exclude list
    4. Are in include_only list (if specified)

    Args:
        target_fields: pipedrive_fields from target datapackage
        source_fields: pipedrive_fields from source datapackage
        target_csv_columns: Column names from target CSV file
        exclude_keys: Field keys to exclude from merge
        include_only_keys: If specified, only merge these keys

    Returns:
        Tuple of (merged_fields, added_fields)
        where merged_fields is the complete list and added_fields
        contains only the newly added fields
    """
    exclude_keys = exclude_keys or set()
    target_keys = {f.get("key") for f in target_fields}

    # Start with all target fields
    merged = list(target_fields)
    added: list[dict[str, Any]] = []

    for field in source_fields:
        key = field.get("key")

        # Skip if already in target (no overwrite)
        if key in target_keys:
            continue

        # Skip if no corresponding CSV column (field was deleted)
        if key not in target_csv_columns:
            continue

        # Skip if in exclude list
        if key in exclude_keys:
            continue

        # Skip if include_only specified and key not in it
        if include_only_keys is not None and key not in include_only_keys:
            continue

        # Add this field
        merged.append(field)
        added.append(field)

    return merged, added
