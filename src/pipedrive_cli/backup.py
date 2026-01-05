"""Backup functionality using Frictionless datapackage."""

import asyncio
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from frictionless import Package, Resource, Schema, describe

from .api import PipedriveClient
from .config import ENTITIES, EntityConfig


def infer_schema_from_records(records: list[dict[str, Any]], name: str) -> Schema:
    """Infer a Frictionless schema from sample records."""
    if not records:
        return Schema()

    # Use frictionless describe to infer schema
    resource = describe(records, type="resource")
    return resource.schema


def field_to_schema_type(field: dict[str, Any]) -> dict[str, Any]:
    """Convert Pipedrive field definition to Frictionless field schema."""
    field_type_mapping = {
        "varchar": "string",
        "varchar_auto": "string",
        "text": "string",
        "int": "integer",
        "double": "number",
        "monetary": "number",
        "date": "date",
        "daterange": "string",
        "time": "time",
        "timerange": "string",
        "phone": "string",
        "enum": "string",
        "set": "array",
        "user": "integer",
        "org": "integer",
        "people": "integer",
        "address": "object",
        "visible_to": "integer",
    }

    pipedrive_type = field.get("field_type", "varchar")
    frictionless_type = field_type_mapping.get(pipedrive_type, "string")

    return {
        "name": field.get("key", ""),
        "type": frictionless_type,
        "title": field.get("name", ""),
        "description": field.get("name", ""),
    }


async def export_entity(
    client: PipedriveClient,
    entity: EntityConfig,
    output_dir: Path,
    progress_callback: callable | None = None,
) -> tuple[int, list[dict[str, Any]]]:
    """Export a single entity to CSV and return record count and sample records."""
    records: list[dict[str, Any]] = []

    async for record in client.fetch_all(entity):
        records.append(record)
        if progress_callback:
            progress_callback(len(records))

    if not records:
        return 0, []

    # Write to CSV
    csv_path = output_dir / f"{entity.name}.csv"
    fieldnames = list(records[0].keys())

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            # Flatten complex values to JSON strings
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
) -> tuple[Package, dict[str, int]]:
    """Create a full backup as a Frictionless datapackage.

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

            count, sample_records = await export_entity(
                client, entity, output_dir, progress_callback
            )

            counts[entity.name] = count

            if count == 0:
                continue

            # Fetch field definitions for schema enrichment
            fields = await client.fetch_fields(entity)

            # Create resource with inferred schema
            csv_path = output_dir / f"{entity.name}.csv"
            resource = describe(str(csv_path), type="resource")
            resource.name = entity.name
            resource.path = f"{entity.name}.csv"

            # Enrich schema with Pipedrive field metadata
            if fields:
                resource.schema.custom = {"pipedrive_fields": fields}

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
