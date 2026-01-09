"""Factory for creating test datapackages with consistent structure."""

import csv
import json
from pathlib import Path
from typing import Any

# Default field definitions for testing
DEFAULT_SYSTEM_FIELDS = {
    "persons": [
        {"key": "id", "name": "ID", "field_type": "int", "edit_flag": False},
        {"key": "name", "name": "Name", "field_type": "varchar", "edit_flag": True},
        {"key": "email", "name": "Email", "field_type": "varchar", "edit_flag": True},
        {"key": "phone", "name": "Phone", "field_type": "varchar", "edit_flag": True},
    ],
    "organizations": [
        {"key": "id", "name": "ID", "field_type": "int", "edit_flag": False},
        {"key": "name", "name": "Name", "field_type": "varchar", "edit_flag": True},
        {"key": "address", "name": "Address", "field_type": "varchar", "edit_flag": True},
    ],
    "deals": [
        {"key": "id", "name": "ID", "field_type": "int", "edit_flag": False},
        {"key": "title", "name": "Title", "field_type": "varchar", "edit_flag": True},
        {"key": "value", "name": "Value", "field_type": "double", "edit_flag": True},
        {"key": "status", "name": "Status", "field_type": "varchar", "edit_flag": True},
    ],
}

# Custom fields for testing field operations
DEFAULT_CUSTOM_FIELDS = {
    "persons": [
        {
            "key": "abc123_custom_text",
            "name": "Custom Text",
            "field_type": "varchar",
            "edit_flag": True,
        },
        {
            "key": "def456_custom_number",
            "name": "Custom Number",
            "field_type": "double",
            "edit_flag": True,
        },
        {
            "key": "25da23b938af0807ec37",
            "name": "Digit-Starting Field",
            "field_type": "varchar",
            "edit_flag": True,
        },
    ],
    "organizations": [
        {
            "key": "org_custom_field",
            "name": "Org Custom",
            "field_type": "varchar",
            "edit_flag": True,
        },
    ],
    "deals": [
        {
            "key": "deal_custom_field",
            "name": "Deal Custom",
            "field_type": "varchar",
            "edit_flag": True,
        },
    ],
}

# Sample data generators
SAMPLE_DATA = {
    "persons": [
        {
            "id": "1",
            "name": "Alice Smith",
            "email": "alice@example.com",
            "phone": "+33612345678",
            "abc123_custom_text": "Custom value 1",
            "def456_custom_number": "100.50",
            "25da23b938af0807ec37": "Digit field value 1",
        },
        {
            "id": "2",
            "name": "Bob Johnson",
            "email": "bob@example.com",
            "phone": "+33687654321",
            "abc123_custom_text": "Custom value 2",
            "def456_custom_number": "200.75",
            "25da23b938af0807ec37": "Digit field value 2",
        },
        {
            "id": "3",
            "name": "Charlie Brown",
            "email": "charlie@example.com",
            "phone": "",
            "abc123_custom_text": "",
            "def456_custom_number": "0",
            "25da23b938af0807ec37": "",
        },
    ],
    "organizations": [
        {
            "id": "1",
            "name": "Acme Corp",
            "address": "123 Main St",
            "org_custom_field": "Org value 1",
        },
        {
            "id": "2",
            "name": "Globex Inc",
            "address": "456 Oak Ave",
            "org_custom_field": "Org value 2",
        },
    ],
    "deals": [
        {
            "id": "1",
            "title": "Big Deal",
            "value": "10000",
            "status": "open",
            "deal_custom_field": "Deal value 1",
        },
        {
            "id": "2",
            "title": "Small Deal",
            "value": "500",
            "status": "won",
            "deal_custom_field": "Deal value 2",
        },
    ],
}


def create_test_datapackage(
    base_path: Path,
    entities: list[str] | None = None,
    include_custom_fields: bool = True,
    extra_fields: dict[str, list[dict[str, Any]]] | None = None,
    extra_data: dict[str, list[dict[str, Any]]] | None = None,
) -> Path:
    """Create a test datapackage with consistent structure.

    Args:
        base_path: Directory to create the datapackage in
        entities: List of entities to include (default: ["persons"])
        include_custom_fields: Whether to include custom fields
        extra_fields: Additional fields to add per entity
        extra_data: Additional data rows per entity

    Returns:
        Path to the created datapackage directory
    """
    if entities is None:
        entities = ["persons"]

    base_path.mkdir(parents=True, exist_ok=True)

    resources = []

    for entity in entities:
        # Build field list
        fields = list(DEFAULT_SYSTEM_FIELDS.get(entity, []))
        if include_custom_fields:
            fields.extend(DEFAULT_CUSTOM_FIELDS.get(entity, []))
        if extra_fields and entity in extra_fields:
            fields.extend(extra_fields[entity])

        # Build data
        data = list(SAMPLE_DATA.get(entity, []))
        if extra_data and entity in extra_data:
            data.extend(extra_data[entity])

        # Create CSV
        csv_path = base_path / f"{entity}.csv"
        _write_csv(csv_path, fields, data)

        # Create resource definition
        resource = _create_resource(entity, fields)
        resources.append(resource)

    # Create datapackage.json
    datapackage = {
        "name": "test-datapackage",
        "resources": resources,
    }
    datapackage_path = base_path / "datapackage.json"
    with open(datapackage_path, "w", encoding="utf-8") as f:
        json.dump(datapackage, f, indent=2, ensure_ascii=False)

    return base_path


def _write_csv(
    path: Path,
    fields: list[dict[str, Any]],
    data: list[dict[str, Any]],
) -> None:
    """Write CSV file with given fields and data."""
    fieldnames = [f["key"] for f in fields]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in data:
            # Ensure all fields are present
            complete_row = {key: row.get(key, "") for key in fieldnames}
            writer.writerow(complete_row)


def _create_resource(entity: str, fields: list[dict[str, Any]]) -> dict[str, Any]:
    """Create a Frictionless resource definition."""
    # Schema fields (Frictionless format)
    schema_fields = []
    for field in fields:
        field_type = "string"
        if field.get("field_type") == "int":
            field_type = "integer"
        elif field.get("field_type") == "double":
            field_type = "number"
        schema_fields.append({"name": field["key"], "type": field_type})

    return {
        "name": entity,
        "path": f"{entity}.csv",
        "schema": {
            "fields": schema_fields,
            "pipedrive_fields": fields,
        },
    }


def create_minimal_datapackage(base_path: Path, entity: str = "persons") -> Path:
    """Create a minimal datapackage with just system fields.

    Useful for testing error cases.
    """
    return create_test_datapackage(
        base_path,
        entities=[entity],
        include_custom_fields=False,
    )


def create_multi_entity_datapackage(base_path: Path) -> Path:
    """Create a datapackage with multiple entities."""
    return create_test_datapackage(
        base_path,
        entities=["persons", "organizations", "deals"],
        include_custom_fields=True,
    )
