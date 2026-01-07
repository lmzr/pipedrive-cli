"""Local datapackage operations for offline field commands."""

import csv
import hashlib
import json
import time
from pathlib import Path
from typing import Any

from frictionless import Package


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
    return custom.get("pipedrive_fields", [])


def save_package(package: Package, base_path: Path) -> None:
    """Save datapackage.json with updated fields."""
    datapackage_path = base_path / "datapackage.json"
    package.to_json(str(datapackage_path))


def load_records(base_path: Path, entity_name: str) -> list[dict[str, Any]]:
    """Load records from CSV file."""
    csv_path = base_path / f"{entity_name}.csv"
    if not csv_path.exists():
        return []

    records: list[dict[str, Any]] = []
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse JSON strings back to objects
            parsed_row: dict[str, Any] = {}
            for key, value in row.items():
                if value and value.startswith(("{", "[")):
                    try:
                        parsed_row[key] = json.loads(value)
                    except json.JSONDecodeError:
                        parsed_row[key] = value
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
