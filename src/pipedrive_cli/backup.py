"""Backup functionality using Frictionless datapackage."""

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from frictionless import Package, Resource, Schema
from frictionless.fields import (
    ArrayField,
    DateField,
    DatetimeField,
    IntegerField,
    NumberField,
    ObjectField,
    StringField,
    TimeField,
)

from .api import PipedriveClient
from .config import ENTITIES, EntityConfig

# Pipedrive field types to Frictionless schema types mapping
PIPEDRIVE_TO_FRICTIONLESS_TYPES: dict[str, str] = {
    "varchar": "string",
    "varchar_auto": "string",
    "text": "string",
    "int": "integer",
    "double": "number",
    "monetary": "number",
    "date": "datetime",  # Pipedrive "date" fields contain datetime values
    "daterange": "string",
    "time": "time",
    "timerange": "string",
    "phone": "string",
    "enum": "string",
    "set": "string",  # Stored as comma-separated IDs in CSV
    "user": "integer",
    "org": "integer",
    "people": "integer",
    "address": "string",  # API returns formatted address string
    "visible_to": "string",  # API returns string like "3"
}

# Supported field types for field create command
SUPPORTED_FIELD_TYPES: list[str] = [
    "varchar",
    "text",
    "int",
    "double",
    "date",
    "enum",
    "set",
    "org",
    "people",
    "phone",
    "address",
]


# Mapping from Frictionless type name to Field class
FRICTIONLESS_TYPE_TO_FIELD_CLASS = {
    "string": StringField,
    "integer": IntegerField,
    "number": NumberField,
    "date": DateField,
    "datetime": DatetimeField,
    "time": TimeField,
    "array": ArrayField,
    "object": ObjectField,
}

# Pipedrive "date" type fields that contain date-only values (not datetime)
# These are exceptions to the "date" -> "datetime" mapping
DATE_ONLY_FIELD_KEYS = {
    "last_activity_date",
    "next_activity_date",
}


def field_to_schema_field(field: dict[str, Any]):
    """Convert Pipedrive field definition to Frictionless Field object."""
    pipedrive_type = field.get("field_type", "varchar")
    field_key = field.get("key", "")
    frictionless_type = PIPEDRIVE_TO_FRICTIONLESS_TYPES.get(pipedrive_type, "string")

    # Date-only fields use "date" instead of "datetime"
    if frictionless_type == "datetime" and field_key in DATE_ONLY_FIELD_KEYS:
        frictionless_type = "date"

    field_class = FRICTIONLESS_TYPE_TO_FIELD_CLASS.get(frictionless_type, StringField)

    return field_class(
        name=field.get("key", ""),
        title=field.get("name", ""),
        description=field.get("name", ""),
    )


def build_schema_from_fields(
    field_defs: list[dict[str, Any]], csv_columns: list[str]
) -> Schema:
    """Build Frictionless schema from Pipedrive field definitions.

    Uses Pipedrive field types for accurate schema (not CSV inference).
    Falls back to 'string' for columns not in field definitions.

    Args:
        field_defs: Pipedrive field definitions from API
        csv_columns: Column names from the CSV file

    Returns:
        Frictionless Schema with correct types
    """
    # Build lookup by field key
    field_by_key = {f.get("key"): f for f in field_defs}

    schema_fields = []
    for col in csv_columns:
        if col in field_by_key:
            schema_fields.append(field_to_schema_field(field_by_key[col]))
        else:
            # System field not in field definitions - default to string
            schema_fields.append(StringField(name=col))

    return Schema(fields=schema_fields)


# Reference field types that store objects but should export as integers
REFERENCE_FIELD_TYPES = {"org", "people", "user"}


def normalize_record_for_export(
    record: dict[str, Any], field_defs: list[dict[str, Any]]
) -> dict[str, Any]:
    """Normalize a record for CSV export.

    Extracts integer IDs from reference field objects (org, people, user).
    Reference fields are returned by API as objects like {"value": 123, "name": "..."}
    but should be stored as just the integer ID for simplicity and consistency.

    Args:
        record: Raw record from Pipedrive API
        field_defs: Field definitions with field_type info

    Returns:
        Normalized record with reference IDs extracted
    """
    field_types = {f.get("key"): f.get("field_type") for f in field_defs}
    result = {}

    for key, value in record.items():
        field_type = field_types.get(key)

        # Reference fields: extract .value (integer ID) from object
        if field_type in REFERENCE_FIELD_TYPES:
            if isinstance(value, dict) and "value" in value:
                result[key] = value["value"]
            else:
                result[key] = value
        else:
            result[key] = value

    return result


async def export_entity(
    client: PipedriveClient,
    entity: EntityConfig,
    output_dir: Path,
    progress_callback: callable | None = None,
    max_records: int | None = None,
    field_defs: list[dict[str, Any]] | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """Export a single entity to CSV and return record count and sample records.

    Args:
        client: Pipedrive API client
        entity: Entity configuration
        output_dir: Output directory for CSV files
        progress_callback: Optional callback for progress updates
        max_records: Maximum number of records to export (None = all)
        field_defs: Field definitions for normalizing reference fields
    """
    records: list[dict[str, Any]] = []

    async for record in client.fetch_all(entity):
        records.append(record)
        if progress_callback:
            progress_callback(len(records))
        # Stop if we've reached the limit
        if max_records is not None and len(records) >= max_records:
            break

    if not records:
        return 0, []

    # Write to CSV
    csv_path = output_dir / f"{entity.name}.csv"
    fieldnames = list(records[0].keys())

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            # Normalize reference fields (extract IDs from objects)
            if field_defs:
                record = normalize_record_for_export(record, field_defs)

            # Flatten remaining complex values to JSON strings
            flat_record = {}
            for key, value in record.items():
                if isinstance(value, (dict, list)):
                    flat_record[key] = json.dumps(value, ensure_ascii=False)
                else:
                    flat_record[key] = value
            writer.writerow(flat_record)

    return len(records), records[:10]  # Return sample for schema inference


async def create_backup(
    api_token: str,
    output_dir: Path,
    entities: list[str] | None = None,
    progress_callback: callable | None = None,
    max_records: int | None = None,
) -> tuple[Package, dict[str, int]]:
    """Create a full backup as a Frictionless datapackage.

    Args:
        api_token: Pipedrive API token
        output_dir: Output directory for backup files
        entities: List of entity names to export (None = all)
        progress_callback: Optional callback for progress updates
        max_records: Maximum number of records per entity (None = all)

    Returns:
        Tuple of (Package, dict of entity name -> record count)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine which entities to export
    entity_names = entities or list(ENTITIES.keys())
    entity_configs = [ENTITIES[name] for name in entity_names if name in ENTITIES]

    resources: list[Resource] = []
    counts: dict[str, int] = {}

    async with PipedriveClient(api_token) as client:
        for entity in entity_configs:
            if progress_callback:
                progress_callback(f"Exporting {entity.name}...")

            # Fetch field definitions FIRST (needed for normalizing reference fields)
            fields = await client.fetch_fields(entity)

            count, sample_records = await export_entity(
                client, entity, output_dir, progress_callback, max_records, fields
            )

            counts[entity.name] = count

            if count == 0:
                continue

            # Create resource with schema from Pipedrive field definitions
            csv_path = output_dir / f"{entity.name}.csv"

            # Read CSV columns for schema building
            with open(csv_path, encoding="utf-8") as f:
                reader = csv.reader(f)
                csv_columns = next(reader)

            # Build schema from Pipedrive fields (not CSV inference)
            schema = build_schema_from_fields(fields, csv_columns)
            schema.custom = {"pipedrive_fields": fields}

            resource = Resource(
                name=entity.name,
                path=f"{entity.name}.csv",
                schema=schema,
            )

            resources.append(resource)

    # Create datapackage
    package = Package(
        name="pipedrive-backup",
        title="Pipedrive CRM Backup",
        description=f"Backup created on {datetime.now().isoformat()}",
        resources=resources,
    )

    # Write datapackage.json
    package_path = output_dir / "datapackage.json"
    package.to_json(str(package_path))

    return package, counts


async def describe_schemas(api_token: str) -> dict[str, list[dict[str, Any]]]:
    """Fetch and return field schemas for all entities."""
    schemas: dict[str, list[dict[str, Any]]] = {}

    async with PipedriveClient(api_token) as client:
        for name, entity in ENTITIES.items():
            if entity.fields_endpoint:
                fields = await client.fetch_fields(entity)
                schemas[name] = fields

    return schemas
