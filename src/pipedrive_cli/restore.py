"""Restore functionality for Pipedrive backups."""

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, TextIO

import click
from frictionless import Package

from .api import PipedriveClient
from .config import ENTITIES, READONLY_FIELDS, RESTORE_ORDER, EntityConfig


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
    deleted: int = 0

    @property
    def total(self) -> int:
        return self.created + self.updated + self.failed + self.skipped


@dataclass
class FieldSyncStats:
    """Statistics for field sync operation."""

    created: int = 0
    deleted: int = 0
    skipped: int = 0


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


def is_custom_field(field: dict[str, Any]) -> bool:
    """Check if a field is a custom field (editable by users)."""
    # edit_flag indicates if field can be edited/deleted
    # Custom field keys typically contain a hash
    return bool(field.get("edit_flag"))


def prompt_delete_fields(entity_name: str, fields: list[dict[str, Any]]) -> bool | None:
    """Prompt user to confirm deletion of extra fields.

    Returns:
        True to delete, False to skip, None to abort
    """
    click.echo(f"\nExtra custom fields in '{entity_name}' (not in backup):")
    for f in sorted(fields, key=lambda x: x.get("name", "")):
        click.echo(f"  - {f.get('name')} ({f.get('key')})")

    response = click.prompt(
        "Delete these fields? [Y/n/q]",
        default="y",
        show_default=False,
    ).lower().strip()

    if response == "q":
        return None
    if response in ("", "y", "yes"):
        return True
    return False


def prompt_delete_records(entity_name: str, count: int) -> bool | None:
    """Prompt user to confirm deletion of extra records.

    Returns:
        True to delete, False to skip, None to abort
    """
    response = click.prompt(
        f"\nDelete {count} extra records from '{entity_name}'? [Y/n/q]",
        default="y",
        show_default=False,
    ).lower().strip()

    if response == "q":
        return None
    if response in ("", "y", "yes"):
        return True
    return False


async def sync_fields(
    client: PipedriveClient,
    entity: EntityConfig,
    backup_fields: list[dict[str, Any]],
    delete_extra: bool,
    dry_run: bool,
    log_file: TextIO | None = None,
) -> FieldSyncStats:
    """Sync custom fields: create missing, optionally delete extra.

    Args:
        client: Pipedrive API client
        entity: Entity configuration
        backup_fields: Field definitions from backup
        delete_extra: Whether to delete fields not in backup
        dry_run: If True, only show what would be done
        log_file: Optional log file for JSON output

    Returns:
        Statistics about created/deleted fields
    """
    stats = FieldSyncStats()

    if not entity.fields_endpoint:
        return stats

    # Fetch current fields from Pipedrive
    current_fields = await client.fetch_fields(entity)

    # Build maps of custom fields by key
    backup_custom = {
        f["key"]: f for f in backup_fields if is_custom_field(f)
    }
    current_custom = {
        f["key"]: f for f in current_fields if is_custom_field(f)
    }

    # Find missing fields (in backup but not in Pipedrive)
    missing_keys = set(backup_custom.keys()) - set(current_custom.keys())

    # Create missing fields
    for key in missing_keys:
        field_def = backup_custom[key]
        field_name = field_def.get("name", key)
        field_type = field_def.get("field_type", "varchar")
        options = field_def.get("options")

        if dry_run:
            stats.created += 1
            if log_file:
                log_file.write(json.dumps({
                    "entity": entity.name,
                    "action": "would_create_field",
                    "field_key": key,
                    "field_name": field_name,
                    "field_type": field_type,
                }) + "\n")
        else:
            try:
                # Prepare options for enum/set fields
                field_options = None
                if options and field_type in ("enum", "set"):
                    field_options = [{"label": opt.get("label")} for opt in options]

                await client.create_field(entity, field_name, field_type, field_options)
                stats.created += 1
                if log_file:
                    log_file.write(json.dumps({
                        "entity": entity.name,
                        "action": "created_field",
                        "field_key": key,
                        "field_name": field_name,
                        "field_type": field_type,
                    }) + "\n")
            except Exception as e:
                stats.skipped += 1
                if log_file:
                    log_file.write(json.dumps({
                        "entity": entity.name,
                        "action": "failed_create_field",
                        "field_key": key,
                        "error": str(e),
                    }) + "\n")

    # Handle extra fields (in Pipedrive but not in backup)
    if delete_extra:
        extra_keys = set(current_custom.keys()) - set(backup_custom.keys())
        if extra_keys:
            extra_fields = [current_custom[k] for k in extra_keys]

            should_delete = True
            if not dry_run:
                should_delete = prompt_delete_fields(entity.name, extra_fields)
                if should_delete is None:
                    raise click.Abort()

            if should_delete:
                for key in extra_keys:
                    field_def = current_custom[key]
                    field_id = field_def.get("id")

                    if dry_run:
                        stats.deleted += 1
                        if log_file:
                            log_file.write(json.dumps({
                                "entity": entity.name,
                                "action": "would_delete_field",
                                "field_key": key,
                                "field_id": field_id,
                            }) + "\n")
                    else:
                        try:
                            await client.delete_field(entity, field_id)
                            stats.deleted += 1
                            if log_file:
                                log_file.write(json.dumps({
                                    "entity": entity.name,
                                    "action": "deleted_field",
                                    "field_key": key,
                                    "field_id": field_id,
                                }) + "\n")
                        except Exception as e:
                            stats.skipped += 1
                            if log_file:
                                log_file.write(json.dumps({
                                    "entity": entity.name,
                                    "action": "failed_delete_field",
                                    "field_key": key,
                                    "error": str(e),
                                }) + "\n")

    return stats


async def delete_extra_records(
    client: PipedriveClient,
    entity: EntityConfig,
    backup_ids: set[int],
    dry_run: bool,
    log_file: TextIO | None = None,
    current_ids: set[int] | None = None,
) -> int:
    """Delete records in Pipedrive that are not in the backup.

    Args:
        client: Pipedrive API client
        entity: Entity configuration
        backup_ids: Set of record IDs from backup
        dry_run: If True, only show what would be done
        log_file: Optional log file for JSON output
        current_ids: Pre-fetched current record IDs (optional, will fetch if None)

    Returns:
        Number of records deleted
    """
    # Fetch all current record IDs if not provided
    if current_ids is None:
        current_ids = await client.fetch_all_ids(entity)

    # Find extra records (in Pipedrive but not in backup)
    extra_ids = current_ids - backup_ids

    if not extra_ids:
        return 0

    should_delete = True
    if not dry_run:
        should_delete = prompt_delete_records(entity.name, len(extra_ids))
        if should_delete is None:
            raise click.Abort()

    if not should_delete:
        return 0

    deleted_count = 0
    for record_id in extra_ids:
        if dry_run:
            deleted_count += 1
            if log_file:
                log_file.write(json.dumps({
                    "entity": entity.name,
                    "action": "would_delete_record",
                    "record_id": record_id,
                }) + "\n")
        else:
            try:
                await client.delete(entity, record_id)
                deleted_count += 1
                if log_file:
                    log_file.write(json.dumps({
                        "entity": entity.name,
                        "action": "deleted_record",
                        "record_id": record_id,
                    }) + "\n")
            except Exception as e:
                if log_file:
                    log_file.write(json.dumps({
                        "entity": entity.name,
                        "action": "failed_delete_record",
                        "record_id": record_id,
                        "error": str(e),
                    }) + "\n")

    return deleted_count


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


@dataclass
class RestoreReport:
    """Complete restore operation report."""

    record_stats: dict[str, RestoreStats]
    field_stats: dict[str, FieldSyncStats]


async def restore_backup(
    api_token: str,
    backup_path: Path,
    entities: list[str] | None = None,
    dry_run: bool = False,
    delete_extra_fields: bool = False,
    delete_extra_records: bool = False,
    log_file: TextIO | None = None,
    progress_callback: callable | None = None,
) -> RestoreReport:
    """Restore a backup to Pipedrive.

    Args:
        api_token: Pipedrive API token
        backup_path: Path to backup directory
        entities: List of entities to restore (default: all in RESTORE_ORDER)
        dry_run: If True, only show what would be done
        delete_extra_fields: Delete custom fields not in backup
        delete_extra_records: Delete records not in backup
        log_file: File to write JSON log lines
        progress_callback: Callback for progress updates

    Returns:
        RestoreReport with record and field statistics
    """
    # Load datapackage
    package_path = backup_path / "datapackage.json"
    if not package_path.exists():
        raise FileNotFoundError(f"datapackage.json not found in {backup_path}")

    package = Package(str(package_path))

    # Build resource lookup by name
    resources_by_name = {r.name: r for r in package.resources}

    # Determine which entities to restore
    entity_names = entities or RESTORE_ORDER
    available_resources = set(resources_by_name.keys())

    all_record_stats: dict[str, RestoreStats] = {}
    all_field_stats: dict[str, FieldSyncStats] = {}

    async with PipedriveClient(api_token) as client:
        for entity_name in entity_names:
            if entity_name not in available_resources:
                continue

            if entity_name not in ENTITIES:
                continue

            # Skip files - require special handling
            if entity_name == "files":
                continue

            entity = ENTITIES[entity_name]
            resource = resources_by_name[entity_name]

            if progress_callback:
                progress_callback(f"Restoring {entity_name}...")

            # Get pipedrive_fields from datapackage schema
            backup_fields = []
            if hasattr(resource.schema, "custom") and resource.schema.custom:
                backup_fields = resource.schema.custom.get("pipedrive_fields", [])
            elif hasattr(resource.schema, "to_dict"):
                schema_dict = resource.schema.to_dict()
                backup_fields = schema_dict.get("pipedrive_fields", [])

            # Sync fields (create missing, optionally delete extra)
            if backup_fields:
                if progress_callback:
                    progress_callback(f"  Syncing fields for {entity_name}...")

                field_stats = await sync_fields(
                    client,
                    entity,
                    backup_fields,
                    delete_extra=delete_extra_fields,
                    dry_run=dry_run,
                    log_file=log_file,
                )
                all_field_stats[entity_name] = field_stats

                if progress_callback and (field_stats.created or field_stats.deleted):
                    msg = f"  Fields: {field_stats.created} created"
                    if field_stats.deleted:
                        msg += f", {field_stats.deleted} deleted"
                    progress_callback(msg)

            # Load records from CSV
            csv_path = backup_path / f"{entity_name}.csv"
            if not csv_path.exists():
                continue

            records = load_records_from_csv(csv_path)
            total_records = len(records)

            # Fetch existing IDs once (for dry-run checks and delete-extra-records)
            existing_ids: set[int] | None = None
            if dry_run or delete_extra_records:
                if progress_callback:
                    progress_callback(f"  Fetching existing {entity_name} IDs...")
                existing_ids = await client.fetch_all_ids(entity)

            # Delete extra records if requested
            if delete_extra_records:
                backup_ids = {r.get("id") for r in records if r.get("id") is not None}

                if progress_callback:
                    progress_callback(f"  Checking for extra records in {entity_name}...")

                deleted = await delete_extra_records_func(
                    client,
                    entity,
                    backup_ids,
                    dry_run=dry_run,
                    log_file=log_file,
                    current_ids=existing_ids,
                )

                if deleted and progress_callback:
                    action = "would delete" if dry_run else "deleted"
                    progress_callback(f"  {deleted} extra records {action}")

            # Restore records with progress
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
                    # Dry run - use pre-fetched IDs for fast lookup
                    exists = existing_ids is not None and record_id in existing_ids
                    action = "would_update" if exists else "would_create"
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
                        # Check if record exists (use pre-fetched IDs if available)
                        if existing_ids is not None:
                            exists = record_id in existing_ids
                        else:
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

                # Update progress with percentage
                if progress_callback and (i + 1) % 10 == 0:
                    pct = (i + 1) * 100 // total_records
                    progress_callback(f"  Records: {i + 1}/{total_records} ({pct}%)")

            # Final progress update
            if progress_callback and total_records > 0:
                progress_callback(f"  Records: {total_records}/{total_records} (100%)")

            all_record_stats[entity_name] = stats

    return RestoreReport(
        record_stats=all_record_stats,
        field_stats=all_field_stats,
    )


# Keep old function for backwards compatibility with restore_entity
async def _restore_entity_legacy(
    client: PipedriveClient,
    entity_name: str,
    records: list[dict[str, Any]],
    dry_run: bool = False,
    log_file: TextIO | None = None,
    progress_callback: callable | None = None,
) -> RestoreStats:
    """Restore records for a single entity (legacy function)."""
    return await restore_entity(
        client, entity_name, records, dry_run, log_file, progress_callback
    )


# Rename to avoid conflict with parameter name
delete_extra_records_func = delete_extra_records
