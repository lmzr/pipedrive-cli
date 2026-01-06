"""Local datapackage operations for offline field commands."""

import csv
import json
from pathlib import Path
from typing import Any

from frictionless import Package


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
