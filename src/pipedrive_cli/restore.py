"""Restore functionality for Pipedrive backups."""

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

from frictionless import Package

from .api import PipedriveClient
from .config import ENTITIES, READONLY_FIELDS, RESTORE_ORDER


@dataclass
class RestoreResult:
    """Result of a restore operation."""

    entity: str
    record_id: int
    action: str  # "created", "updated", "skipped"
    status: str  # "success", "failed"
    new_id: int | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON logging."""
        result = {
            "entity": self.entity,
            "id": self.record_id,
            "action": self.action,
            "status": self.status,
        }
        if self.new_id is not None:
            result["new_id"] = self.new_id
        if self.error:
            result["error"] = self.error
        return result


@dataclass
class RestoreStats:
    """Statistics for restore operation."""

    created: int = 0
    updated: int = 0
    failed: int = 0
    skipped: int = 0

    @property
    def total(self) -> int:
        return self.created + self.updated + self.failed + self.skipped


def clean_record(record: dict[str, Any]) -> dict[str, Any]:
    """Remove read-only fields from a record."""
    return {k: v for k, v in record.items() if k not in READONLY_FIELDS and v is not None}


def parse_csv_value(value: str) -> Any:
    """Parse a CSV value, handling JSON-encoded complex types."""
    if not value:
        return None

    # Try to parse as JSON (for nested objects/arrays)
    if value.startswith("{") or value.startswith("["):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            pass

    # Try to parse as integer
    try:
        return int(value)
    except ValueError:
        pass

    # Try to parse as float
    try:
        return float(value)
    except ValueError:
        pass

    # Return as string
    return value


def load_records_from_csv(csv_path: Path) -> list[dict[str, Any]]:
    """Load records from a CSV file."""
    records = []

    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = {k: parse_csv_value(v) for k, v in row.items()}
            records.append(record)

    return records


async def restore_entity(
    client: PipedriveClient,
    entity_name: str,
    records: list[dict[str, Any]],
    dry_run: bool = False,
    log_file: TextIO | None = None,
    progress_callback: callable | None = None,
) -> RestoreStats:
    """Restore records for a single entity."""
    entity = ENTITIES.get(entity_name)
    if not entity:
        raise ValueError(f"Unknown entity: {entity_name}")

    stats = RestoreStats()

    for i, record in enumerate(records):
        record_id = record.get("id")
        if record_id is None:
            stats.skipped += 1
            continue

        # Clean record for API
        clean_data = clean_record(record)

        if not clean_data:
            stats.skipped += 1
            continue

        result: RestoreResult

        if dry_run:
            # Dry run - just check if exists
            exists = await client.exists(entity, record_id)
            action = "would update" if exists else "would create"
            result = RestoreResult(
                entity=entity_name,
                record_id=record_id,
                action=action,
                status="dry-run",
            )
            if exists:
                stats.updated += 1
            else:
                stats.created += 1
        else:
            try:
                # Check if record exists
                exists = await client.exists(entity, record_id)

                if exists:
                    # Update existing record
                    await client.update(entity, record_id, clean_data)
                    result = RestoreResult(
                        entity=entity_name,
                        record_id=record_id,
                        action="updated",
                        status="success",
                    )
                    stats.updated += 1
                else:
                    # Create new record
                    new_record = await client.create(entity, clean_data)
                    result = RestoreResult(
                        entity=entity_name,
                        record_id=record_id,
                        action="created",
                        status="success",
                        new_id=new_record.get("id"),
                    )
                    stats.created += 1

            except Exception as e:
                result = RestoreResult(
                    entity=entity_name,
                    record_id=record_id,
                    action="update" if exists else "create",
                    status="failed",
                    error=str(e),
                )
                stats.failed += 1

        # Write to log file
        if log_file:
            log_file.write(json.dumps(result.to_dict()) + "\n")
            log_file.flush()

        # Update progress
        if progress_callback:
            progress_callback(i + 1, len(records))

    return stats


async def restore_backup(
    api_token: str,
    backup_path: Path,
    entities: list[str] | None = None,
    dry_run: bool = False,
    log_file: TextIO | None = None,
    progress_callback: callable | None = None,
) -> dict[str, RestoreStats]:
    """Restore a backup to Pipedrive.

    Args:
        api_token: Pipedrive API token
        backup_path: Path to backup directory
        entities: List of entities to restore (default: all in RESTORE_ORDER)
        dry_run: If True, only show what would be done
        log_file: File to write JSON log lines
        progress_callback: Callback for progress updates

    Returns:
        Dictionary mapping entity names to their restore statistics
    """
    # Load datapackage
    package_path = backup_path / "datapackage.json"
    if not package_path.exists():
        raise FileNotFoundError(f"datapackage.json not found in {backup_path}")

    package = Package(str(package_path))

    # Determine which entities to restore
    entity_names = entities or RESTORE_ORDER
    available_resources = {r.name for r in package.resources}

    all_stats: dict[str, RestoreStats] = {}

    async with PipedriveClient(api_token) as client:
        for entity_name in entity_names:
            if entity_name not in available_resources:
                continue

            if entity_name not in ENTITIES:
                continue

            # Skip files - require special handling
            if entity_name == "files":
                continue

            if progress_callback:
                progress_callback(f"Restoring {entity_name}...")

            # Load records from CSV
            csv_path = backup_path / f"{entity_name}.csv"
            if not csv_path.exists():
                continue

            records = load_records_from_csv(csv_path)

            # Restore entity
            stats = await restore_entity(
                client,
                entity_name,
                records,
                dry_run=dry_run,
                log_file=log_file,
                progress_callback=None,  # Will use entity-level progress
            )

            all_stats[entity_name] = stats

    return all_stats
