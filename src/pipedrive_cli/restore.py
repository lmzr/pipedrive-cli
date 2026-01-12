"""Restore functionality for Pipedrive backups."""

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

import click
from frictionless import Package

from .api import PipedriveClient
from .base import load_package, rename_csv_column, rename_field_key, save_package
from .config import ENTITIES, READONLY_ENTITIES, READONLY_FIELDS, RESTORE_ORDER, EntityConfig


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
    updated: int = 0
    deleted: int = 0
    skipped: int = 0
    key_mappings: dict[str, str] = field(default_factory=dict)  # placeholder → real key


def clean_record(record: dict[str, Any]) -> dict[str, Any]:
    """Remove read-only fields from a record."""
    return {k: v for k, v in record.items() if k not in READONLY_FIELDS and v is not None}


# Reference field types that store objects but API expects integers
REFERENCE_FIELD_TYPES = {"org", "people", "user"}

# Mapping from field_type to entity name for ID remapping
REFERENCE_FIELD_TO_ENTITY = {
    "org": "organizations",
    "people": "persons",
    "user": "users",
}


def extract_reference_id(value: Any) -> Any:
    """Extract integer ID from reference object.

    Reference fields are stored as objects: {"value": 431, "name": "..."}
    But Pipedrive API expects just the integer ID for PUT/POST.

    Returns:
        Integer ID if value is a reference object, otherwise unchanged value.
    """
    if isinstance(value, dict) and "value" in value:
        return value["value"]
    return value


def convert_record_for_api(
    record: dict[str, Any],
    field_defs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Convert record values to API-expected format.

    Extracts integer IDs from reference objects (org_id, owner_id, person_id).
    """
    field_by_key = {f.get("key"): f for f in field_defs}

    converted = {}
    for key, value in record.items():
        field_def = field_by_key.get(key, {})
        field_type = field_def.get("field_type", "")

        if field_type in REFERENCE_FIELD_TYPES:
            converted[key] = extract_reference_id(value)
        else:
            converted[key] = value

    return converted


def remap_reference_fields(
    record: dict[str, Any],
    field_defs: list[dict[str, Any]],
    id_mappings: dict[str, dict[int, int]],
) -> dict[str, Any]:
    """Remap reference field values using accumulated ID mappings.

    For records with reference fields (org_id, person_id, etc.), replace
    local IDs with Pipedrive-assigned IDs from previous entity restores.

    Args:
        record: Record data with potential reference fields
        field_defs: Field definitions with field_type info
        id_mappings: Accumulated mappings {entity: {local_id: pipedrive_id}}

    Returns:
        Record with remapped reference field values
    """
    field_by_key = {f.get("key"): f for f in field_defs}

    remapped = {}
    for key, value in record.items():
        field_def = field_by_key.get(key, {})
        field_type = field_def.get("field_type", "")

        if field_type in REFERENCE_FIELD_TYPES and value is not None:
            # Get the entity this reference points to
            ref_entity = REFERENCE_FIELD_TO_ENTITY.get(field_type)
            if ref_entity and ref_entity in id_mappings:
                entity_mappings = id_mappings[ref_entity]

                # Extract the ID (could be integer or object with "value" key)
                if isinstance(value, dict) and "value" in value:
                    old_id = value["value"]
                    if old_id in entity_mappings:
                        # Update the value inside the object
                        remapped[key] = {**value, "value": entity_mappings[old_id]}
                    else:
                        remapped[key] = value
                elif isinstance(value, int):
                    remapped[key] = entity_mappings.get(value, value)
                else:
                    remapped[key] = value
            else:
                remapped[key] = value
        else:
            remapped[key] = value

    return remapped


def normalize_value_for_comparison(value: Any, field_type: str) -> Any:
    """Normalize a field value for comparison.

    Extracts comparable values from reference objects, arrays, etc.
    """
    if value is None:
        return None

    # Reference fields: extract .value from objects
    if field_type in REFERENCE_FIELD_TYPES:
        if isinstance(value, dict) and "value" in value:
            return value["value"]
        return value

    # Arrays (email, phone): extract primary value for comparison
    if isinstance(value, list) and value:
        if isinstance(value[0], dict):
            primary = next((item for item in value if item.get("primary")), value[0])
            return primary.get("value", "")
        return value

    return value


def records_equal(
    local_record: dict[str, Any],
    remote_record: dict[str, Any],
    field_defs: list[dict[str, Any]],
) -> bool:
    """Compare local and remote records for equality.

    Only compares fields that exist in the local record (after cleaning).
    Normalizes reference fields for comparison.

    Args:
        local_record: Cleaned local record data (ready for API)
        remote_record: Record data from Pipedrive
        field_defs: Field definitions with field_type info

    Returns:
        True if records are equal, False otherwise
    """
    field_by_key = {f.get("key"): f for f in field_defs}

    for key, local_value in local_record.items():
        field_def = field_by_key.get(key, {})
        field_type = field_def.get("field_type", "")

        remote_value = remote_record.get(key)

        # Normalize both values
        local_normalized = normalize_value_for_comparison(local_value, field_type)
        remote_normalized = normalize_value_for_comparison(remote_value, field_type)

        # Compare normalized values
        if local_normalized != remote_normalized:
            return False

    return True


def load_id_mappings(backup_path: Path) -> dict[str, dict[int, int]]:
    """Load existing ID mappings from id_mapping.jsonl.

    Used for resuming a partial sync.

    Args:
        backup_path: Path to backup directory

    Returns:
        Accumulated mappings {entity: {local_id: pipedrive_id}}
    """
    mapping_file = backup_path / "id_mapping.jsonl"
    mappings: dict[str, dict[int, int]] = {}

    if not mapping_file.exists():
        return mappings

    with open(mapping_file, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                entity = entry.get("entity")
                local_id = entry.get("local_id")
                pipedrive_id = entry.get("pipedrive_id")

                if entity and local_id is not None and pipedrive_id is not None:
                    if entity not in mappings:
                        mappings[entity] = {}
                    mappings[entity][local_id] = pipedrive_id
            except json.JSONDecodeError:
                continue

    return mappings


def save_id_mapping_entry(
    mapping_file: TextIO,
    entity: str,
    local_id: int,
    pipedrive_id: int,
) -> None:
    """Append a single ID mapping entry to the mapping file.

    Args:
        mapping_file: Open file handle for appending
        entity: Entity name (e.g., "organizations")
        local_id: Original local ID
        pipedrive_id: Pipedrive-assigned ID
    """
    entry = {
        "entity": entity,
        "local_id": local_id,
        "pipedrive_id": pipedrive_id,
    }
    mapping_file.write(json.dumps(entry) + "\n")
    mapping_file.flush()


def update_local_ids(
    backup_path: Path,
    id_mappings: dict[str, dict[int, int]],
    field_defs_by_entity: dict[str, list[dict[str, Any]]],
) -> None:
    """Update local CSV files with Pipedrive-assigned IDs.

    After store completes, this function updates:
    1. Record IDs in each entity's CSV
    2. Reference field values in dependent entities' CSVs

    Args:
        backup_path: Path to backup directory
        id_mappings: Accumulated mappings {entity: {local_id: pipedrive_id}}
        field_defs_by_entity: Field definitions for each entity
    """
    for entity_name, field_defs in field_defs_by_entity.items():
        csv_path = backup_path / f"{entity_name}.csv"
        if not csv_path.exists():
            continue

        # Load records
        records = load_records_from_csv(csv_path)
        if not records:
            continue

        modified = False
        entity_mappings = id_mappings.get(entity_name, {})
        field_by_key = {f.get("key"): f for f in field_defs}

        for record in records:
            # Update record's own ID
            record_id = record.get("id")
            if record_id is not None and record_id in entity_mappings:
                record["id"] = entity_mappings[record_id]
                modified = True

            # Update reference fields
            for key, value in list(record.items()):
                if value is None:
                    continue

                field_def = field_by_key.get(key, {})
                field_type = field_def.get("field_type", "")

                if field_type in REFERENCE_FIELD_TYPES:
                    ref_entity = REFERENCE_FIELD_TO_ENTITY.get(field_type)
                    if ref_entity and ref_entity in id_mappings:
                        ref_mappings = id_mappings[ref_entity]

                        if isinstance(value, dict) and "value" in value:
                            old_id = value["value"]
                            if old_id in ref_mappings:
                                record[key] = {**value, "value": ref_mappings[old_id]}
                                modified = True
                        elif isinstance(value, int) and value in ref_mappings:
                            record[key] = ref_mappings[value]
                            modified = True

        # Save updated records
        if modified:
            save_records_to_csv(csv_path, records)


def save_records_to_csv(csv_path: Path, records: list[dict[str, Any]]) -> None:
    """Save records to a CSV file.

    Args:
        csv_path: Path to CSV file
        records: List of record dictionaries
    """
    if not records:
        return

    # Get all field names from records
    fieldnames = []
    for record in records:
        for key in record.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for record in records:
            # Convert complex values to JSON strings
            row = {}
            for key, value in record.items():
                if value is None:
                    row[key] = ""
                elif isinstance(value, (dict, list)):
                    row[key] = json.dumps(value)
                else:
                    row[key] = value
            writer.writerow(row)


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

                created_field = await client.create_field(
                    entity, field_name, field_type, field_options
                )
                stats.created += 1

                # Track key mapping if Pipedrive assigned a different key
                real_key = created_field.get("key")
                if real_key and real_key != key:
                    stats.key_mappings[key] = real_key

                if log_file:
                    log_file.write(json.dumps({
                        "entity": entity.name,
                        "action": "created_field",
                        "field_key": key,
                        "field_name": field_name,
                        "field_type": field_type,
                        "real_key": real_key,
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

    # Update fields that exist in both but have different names
    common_keys = set(backup_custom.keys()) & set(current_custom.keys())
    for key in common_keys:
        backup_name = backup_custom[key].get("name", "")
        current_name = current_custom[key].get("name", "")

        if backup_name != current_name:
            field_id = current_custom[key].get("id")

            if dry_run:
                stats.updated += 1
                if log_file:
                    log_file.write(json.dumps({
                        "entity": entity.name,
                        "action": "would_update_field",
                        "field_key": key,
                        "field_id": field_id,
                        "old_name": current_name,
                        "new_name": backup_name,
                    }) + "\n")
            else:
                try:
                    await client.update_field(entity, field_id, name=backup_name)
                    stats.updated += 1
                    if log_file:
                        log_file.write(json.dumps({
                            "entity": entity.name,
                            "action": "updated_field",
                            "field_key": key,
                            "field_id": field_id,
                            "old_name": current_name,
                            "new_name": backup_name,
                        }) + "\n")
                except Exception as e:
                    stats.skipped += 1
                    if log_file:
                        log_file.write(json.dumps({
                            "entity": entity.name,
                            "action": "failed_update_field",
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
    id_mappings: dict[str, dict[int, int]] = field(default_factory=dict)


async def restore_backup(
    api_token: str,
    backup_path: Path,
    entities: list[str] | None = None,
    dry_run: bool = False,
    delete_extra_fields: bool = False,
    delete_extra_records: bool = False,
    update_base: bool = True,
    log_file: TextIO | None = None,
    progress_callback: callable | None = None,
    resume: bool = False,
    skip_unchanged: bool = False,
) -> RestoreReport:
    """Restore a backup to Pipedrive.

    Args:
        api_token: Pipedrive API token
        backup_path: Path to backup directory
        entities: List of entities to restore (default: all in RESTORE_ORDER)
        dry_run: If True, only show what would be done
        delete_extra_fields: Delete custom fields not in backup
        delete_extra_records: Delete records not in backup
        update_base: Update local files with real Pipedrive keys after field creation
        log_file: File to write JSON log lines
        progress_callback: Callback for progress updates
        resume: Resume from previous partial sync using existing ID mappings
        skip_unchanged: Skip records that haven't changed (compare with Pipedrive)

    Returns:
        RestoreReport with record and field statistics and ID mappings
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

    # ID mappings: {entity: {local_id: pipedrive_id}}
    all_id_mappings: dict[str, dict[int, int]] = {}

    # Load existing mappings if resuming
    if resume:
        all_id_mappings = load_id_mappings(backup_path)
        if progress_callback and all_id_mappings:
            total_mapped = sum(len(m) for m in all_id_mappings.values())
            progress_callback(f"Loaded {total_mapped} existing ID mappings for resume")

    # Open mapping file for writing (append mode for resume)
    mapping_file_path = backup_path / "id_mapping.jsonl"
    mapping_file_mode = "a" if resume else "w"
    mapping_file = (
        open(mapping_file_path, mapping_file_mode, encoding="utf-8")
        if not dry_run
        else None
    )

    async with PipedriveClient(api_token) as client:
        for entity_name in entity_names:
            if entity_name not in available_resources:
                continue

            if entity_name not in ENTITIES:
                continue

            # Skip files - require special handling
            if entity_name == "files":
                continue

            # Skip readonly entities (can be backed up but not restored)
            if entity_name in READONLY_ENTITIES:
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

                if progress_callback and (
                    field_stats.created or field_stats.updated or field_stats.deleted
                ):
                    msg = f"  Fields: {field_stats.created} created"
                    if field_stats.updated:
                        msg += f", {field_stats.updated} updated"
                    if field_stats.deleted:
                        msg += f", {field_stats.deleted} deleted"
                    progress_callback(msg)

                # Update local files with real Pipedrive keys
                if field_stats.key_mappings and update_base and not dry_run:
                    base_package = load_package(backup_path)
                    for old_key, new_key in field_stats.key_mappings.items():
                        rename_field_key(base_package, entity_name, old_key, new_key)
                        rename_csv_column(backup_path, entity_name, old_key, new_key)
                    save_package(base_package, backup_path)
                    if progress_callback:
                        progress_callback(
                            f"  Updated {len(field_stats.key_mappings)} field key(s) in local data"
                        )

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

            # Initialize entity mapping if not present
            if entity_name not in all_id_mappings:
                all_id_mappings[entity_name] = {}

            for i, record in enumerate(records):
                record_id = record.get("id")
                if record_id is None:
                    stats.skipped += 1
                    continue

                # Skip if already synced (for resume)
                if resume and record_id in all_id_mappings[entity_name]:
                    stats.skipped += 1
                    continue

                # Clean record for API
                clean_data = clean_record(record)

                # Remap reference fields using accumulated ID mappings
                clean_data = remap_reference_fields(clean_data, backup_fields, all_id_mappings)

                # Convert reference fields (org_id, owner_id, person_id) to integer IDs
                clean_data = convert_record_for_api(clean_data, backup_fields)

                if not clean_data:
                    stats.skipped += 1
                    continue

                result: RestoreResult

                if dry_run:
                    # Dry run - use pre-fetched IDs for fast lookup
                    exists = existing_ids is not None and record_id in existing_ids

                    if exists and skip_unchanged:
                        # Check if record has changed
                        remote_record = await client.get_record(entity, record_id)
                        if remote_record and records_equal(
                            clean_data, remote_record, backup_fields
                        ):
                            action = "would_skip"
                            stats.skipped += 1
                        else:
                            action = "would_update"
                            stats.updated += 1
                    elif exists:
                        action = "would_update"
                        stats.updated += 1
                    else:
                        action = "would_create"
                        stats.created += 1

                    result = RestoreResult(
                        entity=entity_name,
                        record_id=record_id,
                        action=action,
                        status="dry-run",
                    )
                else:
                    try:
                        # Check if record exists (use pre-fetched IDs if available)
                        if existing_ids is not None:
                            exists = record_id in existing_ids
                        else:
                            exists = await client.exists(entity, record_id)

                        if exists:
                            # Check if record has changed when skip_unchanged is enabled
                            if skip_unchanged:
                                remote_record = await client.get_record(entity, record_id)
                                if remote_record and records_equal(
                                    clean_data, remote_record, backup_fields
                                ):
                                    # Skip unchanged record
                                    result = RestoreResult(
                                        entity=entity_name,
                                        record_id=record_id,
                                        action="skipped",
                                        status="unchanged",
                                    )
                                    stats.skipped += 1
                                else:
                                    # Update changed record
                                    await client.update(entity, record_id, clean_data)
                                    result = RestoreResult(
                                        entity=entity_name,
                                        record_id=record_id,
                                        action="updated",
                                        status="success",
                                    )
                                    stats.updated += 1
                            else:
                                # Update existing record (no comparison)
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
                            new_id = new_record.get("id")
                            result = RestoreResult(
                                entity=entity_name,
                                record_id=record_id,
                                action="created",
                                status="success",
                                new_id=new_id,
                            )
                            stats.created += 1

                            # Track ID mapping for dependent entities
                            if new_id is not None:
                                all_id_mappings[entity_name][record_id] = new_id
                                # Persist mapping for resume capability
                                if mapping_file:
                                    save_id_mapping_entry(
                                        mapping_file, entity_name, record_id, new_id
                                    )

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

    # Close mapping file
    if mapping_file:
        mapping_file.close()

    # Update local CSV files with Pipedrive-assigned IDs
    if update_base and not dry_run and all_id_mappings:
        # Collect field definitions for all entities
        field_defs_by_entity = {}
        for entity_name in entity_names:
            if entity_name in resources_by_name:
                resource = resources_by_name[entity_name]
                backup_fields = []
                if hasattr(resource.schema, "custom") and resource.schema.custom:
                    backup_fields = resource.schema.custom.get("pipedrive_fields", [])
                elif hasattr(resource.schema, "to_dict"):
                    schema_dict = resource.schema.to_dict()
                    backup_fields = schema_dict.get("pipedrive_fields", [])
                field_defs_by_entity[entity_name] = backup_fields

        update_local_ids(backup_path, all_id_mappings, field_defs_by_entity)

        if progress_callback:
            total_mapped = sum(len(m) for m in all_id_mappings.values())
            progress_callback(f"Updated {total_mapped} record ID(s) in local data")

    return RestoreReport(
        record_stats=all_record_stats,
        field_stats=all_field_stats,
        id_mappings=all_id_mappings,
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
