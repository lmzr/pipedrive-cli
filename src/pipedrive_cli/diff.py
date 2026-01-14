"""Diff comparison for Pipedrive datapackages."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from rich.console import Console

from .base import get_entity_fields, load_package, load_records
from .config import READONLY_FIELDS
from .matching import match_entity

# -----------------------------------------------------------------------------
# Dataclasses
# -----------------------------------------------------------------------------


@dataclass
class FieldDiff:
    """Difference in a single field definition."""

    key: str
    name: str
    diff_type: str  # 'added', 'removed', 'type_changed', 'name_changed', 'options_changed'
    old_value: Any = None
    new_value: Any = None


@dataclass
class RecordDiff:
    """Difference in a single record."""

    record_id: Any
    diff_type: str  # 'added', 'removed', 'modified'
    field_changes: dict[str, tuple[Any, Any]] = field(default_factory=dict)


@dataclass
class EntityDiff:
    """Complete diff for a single entity."""

    entity_name: str
    field_diffs: list[FieldDiff] = field(default_factory=list)
    record_diffs: list[RecordDiff] = field(default_factory=list)

    @property
    def has_differences(self) -> bool:
        """Check if there are any differences."""
        return bool(self.field_diffs or self.record_diffs)


@dataclass
class DiffStats:
    """Statistics for a diff operation."""

    entities_compared: int = 0
    entities_with_differences: int = 0
    fields_added: int = 0
    fields_removed: int = 0
    fields_changed: int = 0
    records_added: int = 0
    records_removed: int = 0
    records_modified: int = 0


# -----------------------------------------------------------------------------
# Key option parsing
# -----------------------------------------------------------------------------


def parse_key_option(keys: tuple[str, ...]) -> tuple[str, dict[str, str]]:
    """Parse -k option values.

    Supports two formats:
    - "field": global key for all entities
    - "entity:field": key for specific entity

    Args:
        keys: Tuple of key specifications from CLI

    Returns:
        Tuple of (default_key, entity_keys) where:
        - default_key: Key to use for entities not in entity_keys
        - entity_keys: Dict mapping entity name to key field
    """
    default_key = "id"
    entity_keys: dict[str, str] = {}

    for key_spec in keys:
        if ":" in key_spec:
            # Per-entity key: "entity:field"
            entity, key_field = key_spec.split(":", 1)
            entity_keys[entity] = key_field
        else:
            # Global key
            default_key = key_spec

    return default_key, entity_keys


def get_key_for_entity(
    entity_name: str,
    default_key: str,
    entity_keys: dict[str, str],
) -> str:
    """Get the matching key for a specific entity.

    Args:
        entity_name: Full entity name
        default_key: Default key if entity not in entity_keys
        entity_keys: Dict mapping entity prefix to key field

    Returns:
        Key field to use for this entity
    """
    # Check for exact match first
    if entity_name in entity_keys:
        return entity_keys[entity_name]

    # Check for prefix match
    for entity_prefix, key_field in entity_keys.items():
        try:
            matched = match_entity(entity_prefix)
            if matched.name == entity_name:
                return key_field
        except Exception:
            # If prefix doesn't match any entity, skip
            continue

    return default_key


# -----------------------------------------------------------------------------
# Computed fields detection
# -----------------------------------------------------------------------------


def get_computed_fields(fields: list[dict[str, Any]]) -> set[str]:
    """Get set of computed field keys to exclude from data comparison.

    A field is considered computed if:
    - edit_flag is False (system field, not editable)
    - key is in READONLY_FIELDS (hardcoded list of known computed fields)

    Args:
        fields: List of field definitions from pipedrive_fields

    Returns:
        Set of field keys that are computed/read-only
    """
    # Start with all known readonly fields as baseline
    # This ensures computed fields are excluded even for entities
    # without pipedrive_fields metadata (like users)
    computed: set[str] = set(READONLY_FIELDS)

    # Add fields with edit_flag=False from metadata
    for f in fields:
        key = f.get("key", "")
        if not f.get("edit_flag", True):
            computed.add(key)

    return computed


# -----------------------------------------------------------------------------
# Field comparison
# -----------------------------------------------------------------------------


def normalize_options(options: list[dict[str, Any]] | None) -> set[tuple[int, str]]:
    """Normalize field options for comparison.

    Args:
        options: List of option dicts with 'id' and 'label' keys

    Returns:
        Set of (id, label) tuples for comparison
    """
    if not options:
        return set()
    return {(opt.get("id"), opt.get("label", "")) for opt in options}


def diff_fields(
    fields1: list[dict[str, Any]],
    fields2: list[dict[str, Any]],
) -> list[FieldDiff]:
    """Compare field definitions between two datapackages.

    Args:
        fields1: Fields from first (source/before) datapackage
        fields2: Fields from second (target/after) datapackage

    Returns:
        List of FieldDiff objects describing differences
    """
    diffs: list[FieldDiff] = []

    # Build lookup by key
    fields1_by_key = {f.get("key"): f for f in fields1}
    fields2_by_key = {f.get("key"): f for f in fields2}

    keys1 = set(fields1_by_key.keys())
    keys2 = set(fields2_by_key.keys())

    # Fields removed (in fields1 but not in fields2)
    for key in keys1 - keys2:
        f = fields1_by_key[key]
        diffs.append(
            FieldDiff(
                key=key,
                name=f.get("name", key),
                diff_type="removed",
                old_value=f,
            )
        )

    # Fields added (in fields2 but not in fields1)
    for key in keys2 - keys1:
        f = fields2_by_key[key]
        diffs.append(
            FieldDiff(
                key=key,
                name=f.get("name", key),
                diff_type="added",
                new_value=f,
            )
        )

    # Fields in both - check for changes
    for key in keys1 & keys2:
        f1 = fields1_by_key[key]
        f2 = fields2_by_key[key]

        # Check type change
        type1 = f1.get("field_type")
        type2 = f2.get("field_type")
        if type1 != type2:
            diffs.append(
                FieldDiff(
                    key=key,
                    name=f2.get("name", key),
                    diff_type="type_changed",
                    old_value=type1,
                    new_value=type2,
                )
            )

        # Check name change
        name1 = f1.get("name")
        name2 = f2.get("name")
        if name1 != name2:
            diffs.append(
                FieldDiff(
                    key=key,
                    name=name2,
                    diff_type="name_changed",
                    old_value=name1,
                    new_value=name2,
                )
            )

        # Check options change (for enum/set fields)
        opts1 = normalize_options(f1.get("options"))
        opts2 = normalize_options(f2.get("options"))
        if opts1 != opts2:
            diffs.append(
                FieldDiff(
                    key=key,
                    name=f2.get("name", key),
                    diff_type="options_changed",
                    old_value=f1.get("options"),
                    new_value=f2.get("options"),
                )
            )

    return diffs


# -----------------------------------------------------------------------------
# Record comparison
# -----------------------------------------------------------------------------


def normalize_value(value: Any) -> Any:
    """Normalize a value for comparison.

    Treats None, empty string, and missing as equivalent.
    Handles complex types (dicts, lists).

    Args:
        value: Value to normalize

    Returns:
        Normalized value
    """
    if value is None or value == "":
        return None

    if isinstance(value, dict):
        # Extract primary value from reference objects
        if "value" in value:
            return normalize_value(value["value"])
        # Recursively normalize dict values
        return {k: normalize_value(v) for k, v in value.items()}

    if isinstance(value, list):
        # Normalize list elements
        return [normalize_value(v) for v in value]

    return value


def diff_records(
    records1: list[dict[str, Any]],
    records2: list[dict[str, Any]],
    key_field: str = "id",
    exclude_fields: set[str] | None = None,
) -> list[RecordDiff]:
    """Compare records between two datapackages.

    Args:
        records1: Records from first (source/before) datapackage
        records2: Records from second (target/after) datapackage
        key_field: Field to use for matching records
        exclude_fields: Set of field keys to exclude from comparison

    Returns:
        List of RecordDiff objects describing differences
    """
    diffs: list[RecordDiff] = []
    exclude_fields = exclude_fields or set()

    # Build lookup by key field
    records1_by_key = {r.get(key_field): r for r in records1}
    records2_by_key = {r.get(key_field): r for r in records2}

    keys1 = set(records1_by_key.keys())
    keys2 = set(records2_by_key.keys())

    # Records removed
    for key in keys1 - keys2:
        diffs.append(
            RecordDiff(
                record_id=key,
                diff_type="removed",
            )
        )

    # Records added
    for key in keys2 - keys1:
        diffs.append(
            RecordDiff(
                record_id=key,
                diff_type="added",
            )
        )

    # Records in both - check for modifications
    for key in keys1 & keys2:
        r1 = records1_by_key[key]
        r2 = records2_by_key[key]

        # Get all fields from both records
        all_fields = set(r1.keys()) | set(r2.keys())
        field_changes: dict[str, tuple[Any, Any]] = {}

        for field_key in all_fields:
            # Skip excluded fields (computed fields by default)
            if field_key in exclude_fields:
                continue

            val1 = normalize_value(r1.get(field_key))
            val2 = normalize_value(r2.get(field_key))

            if val1 != val2:
                field_changes[field_key] = (r1.get(field_key), r2.get(field_key))

        if field_changes:
            diffs.append(
                RecordDiff(
                    record_id=key,
                    diff_type="modified",
                    field_changes=field_changes,
                )
            )

    return diffs


# -----------------------------------------------------------------------------
# Entity and package comparison
# -----------------------------------------------------------------------------


def diff_entity(
    base1: Path,
    base2: Path,
    entity_name: str,
    key_field: str = "id",
    schema_only: bool = False,
    data_only: bool = False,
    include_computed: bool = False,
) -> EntityDiff:
    """Compare a single entity between two datapackages.

    Args:
        base1: Path to first datapackage
        base2: Path to second datapackage
        entity_name: Entity to compare
        key_field: Field to use for record matching
        schema_only: Only compare field definitions
        data_only: Only compare records
        include_computed: Include computed fields in data comparison

    Returns:
        EntityDiff with field and/or record differences
    """
    package1 = load_package(base1)
    package2 = load_package(base2)

    entity_diff = EntityDiff(entity_name=entity_name)

    # Get field definitions (needed for both schema diff and computed fields detection)
    fields1 = get_entity_fields(package1, entity_name)
    fields2 = get_entity_fields(package2, entity_name)

    # Compare fields (schema)
    if not data_only:
        entity_diff.field_diffs = diff_fields(fields1, fields2)

    # Compare records (data)
    if not schema_only:
        records1 = load_records(base1, entity_name)
        records2 = load_records(base2, entity_name)

        # Get computed fields to exclude (unless include_computed is True)
        exclude_fields: set[str] = set()
        if not include_computed:
            # Use fields from both packages to catch all computed fields
            exclude_fields = get_computed_fields(fields1) | get_computed_fields(fields2)

        entity_diff.record_diffs = diff_records(
            records1, records2, key_field, exclude_fields=exclude_fields
        )

    return entity_diff


def diff_packages(
    base1: Path,
    base2: Path,
    entities: list[str] | None = None,
    default_key: str = "id",
    entity_keys: dict[str, str] | None = None,
    schema_only: bool = False,
    data_only: bool = False,
    include_computed: bool = False,
) -> tuple[list[EntityDiff], DiffStats]:
    """Compare two complete datapackages.

    Args:
        base1: Path to first datapackage
        base2: Path to second datapackage
        entities: List of entities to compare (None = all common entities)
        default_key: Default key field for record matching
        entity_keys: Dict mapping entity name to specific key field
        schema_only: Only compare field definitions
        data_only: Only compare records
        include_computed: Include computed fields in data comparison

    Returns:
        Tuple of (entity_diffs, stats)
    """
    entity_keys = entity_keys or {}

    package1 = load_package(base1)
    package2 = load_package(base2)

    # Get entities to compare
    if entities:
        # Resolve entity prefixes (entities are already full names from CLI)
        entities_to_compare = list(entities)
    else:
        # Compare all common entities
        entities1 = {r.name for r in package1.resources}
        entities2 = {r.name for r in package2.resources}
        entities_to_compare = sorted(entities1 & entities2)

    entity_diffs: list[EntityDiff] = []
    stats = DiffStats()

    for entity_name in entities_to_compare:
        # Get key for this entity
        key_field = get_key_for_entity(entity_name, default_key, entity_keys)

        # Compare entity
        entity_diff = diff_entity(
            base1,
            base2,
            entity_name,
            key_field=key_field,
            schema_only=schema_only,
            data_only=data_only,
            include_computed=include_computed,
        )

        entity_diffs.append(entity_diff)
        stats.entities_compared += 1

        if entity_diff.has_differences:
            stats.entities_with_differences += 1

        # Accumulate field stats
        for fd in entity_diff.field_diffs:
            if fd.diff_type == "added":
                stats.fields_added += 1
            elif fd.diff_type == "removed":
                stats.fields_removed += 1
            else:
                stats.fields_changed += 1

        # Accumulate record stats
        for rd in entity_diff.record_diffs:
            if rd.diff_type == "added":
                stats.records_added += 1
            elif rd.diff_type == "removed":
                stats.records_removed += 1
            else:
                stats.records_modified += 1

    return entity_diffs, stats


# -----------------------------------------------------------------------------
# Output formatting
# -----------------------------------------------------------------------------


def format_diff_table(
    entity_diffs: list[EntityDiff],
    stats: DiffStats,
    console: Console,
    limit: int | None = None,
    quiet: bool = False,
) -> None:
    """Format diff results as Rich tables.

    Args:
        entity_diffs: List of EntityDiff objects
        stats: Diff statistics
        console: Rich console for output
        limit: Maximum changed records to display per entity
        quiet: Suppress headers and summaries
    """
    has_any_diff = any(ed.has_differences for ed in entity_diffs)

    if not has_any_diff:
        if not quiet:
            console.print("[green]No differences found[/green]")
            console.print(f"[dim]Entities compared: {stats.entities_compared}[/dim]")
        return

    # Process each entity
    for entity_diff in entity_diffs:
        if not entity_diff.has_differences:
            continue

        if not quiet:
            console.print(f"\n[bold cyan]=== {entity_diff.entity_name} ===[/bold cyan]")

        # Field differences
        if entity_diff.field_diffs:
            if not quiet:
                console.print("[bold]Schema differences:[/bold]")

            for fd in entity_diff.field_diffs:
                if fd.diff_type == "added":
                    console.print(f"  [green][+] Added:[/green] {fd.key} ({fd.name})")
                elif fd.diff_type == "removed":
                    console.print(f"  [red][-] Removed:[/red] {fd.key} ({fd.name})")
                elif fd.diff_type == "type_changed":
                    console.print(
                        f"  [yellow][~] Type changed:[/yellow] {fd.key}: "
                        f"{fd.old_value} -> {fd.new_value}"
                    )
                elif fd.diff_type == "name_changed":
                    console.print(
                        f"  [yellow][~] Name changed:[/yellow] {fd.key}: "
                        f'"{fd.old_value}" -> "{fd.new_value}"'
                    )
                elif fd.diff_type == "options_changed":
                    console.print(
                        f"  [yellow][~] Options changed:[/yellow] {fd.key} ({fd.name})"
                    )

        # Record differences
        if entity_diff.record_diffs:
            added = [rd for rd in entity_diff.record_diffs if rd.diff_type == "added"]
            removed = [
                rd for rd in entity_diff.record_diffs if rd.diff_type == "removed"
            ]
            modified = [
                rd for rd in entity_diff.record_diffs if rd.diff_type == "modified"
            ]

            if not quiet:
                console.print(
                    f"[bold]Data differences:[/bold] "
                    f"{len(added)} added, {len(removed)} removed, {len(modified)} modified"
                )

            # Apply limit
            display_count = 0

            for rd in added:
                if limit and display_count >= limit:
                    break
                console.print(f"  [green][+] Added:[/green] id={rd.record_id}")
                display_count += 1

            for rd in removed:
                if limit and display_count >= limit:
                    break
                console.print(f"  [red][-] Removed:[/red] id={rd.record_id}")
                display_count += 1

            for rd in modified:
                if limit and display_count >= limit:
                    break
                console.print(f"  [yellow][~] Modified:[/yellow] id={rd.record_id}")
                for field_key, (old_val, new_val) in rd.field_changes.items():
                    old_str = _format_value(old_val)
                    new_str = _format_value(new_val)
                    console.print(f"      {field_key}: {old_str} -> {new_str}")
                display_count += 1

            total_diffs = len(added) + len(removed) + len(modified)
            if limit and total_diffs > limit:
                console.print(
                    f"  [dim]... and {total_diffs - limit} more differences[/dim]"
                )

    # Summary
    if not quiet:
        console.print()
        console.print("[bold]Summary:[/bold]")
        console.print(f"  Entities compared: {stats.entities_compared}")
        console.print(f"  Entities with differences: {stats.entities_with_differences}")
        if stats.fields_added + stats.fields_removed + stats.fields_changed > 0:
            console.print(
                f"  Schema: {stats.fields_added} added, "
                f"{stats.fields_removed} removed, {stats.fields_changed} changed"
            )
        if stats.records_added + stats.records_removed + stats.records_modified > 0:
            console.print(
                f"  Records: {stats.records_added} added, "
                f"{stats.records_removed} removed, {stats.records_modified} modified"
            )


def _format_value(value: Any, max_len: int = 50) -> str:
    """Format a value for display, truncating if needed."""
    if value is None:
        return "[dim]null[/dim]"
    if value == "":
        return '[dim]""[/dim]'

    if isinstance(value, (dict, list)):
        s = json.dumps(value, ensure_ascii=False)
    else:
        s = str(value)

    if len(s) > max_len:
        s = s[: max_len - 3] + "..."

    return f'"{s}"' if isinstance(value, str) else s


def format_diff_json(
    entity_diffs: list[EntityDiff],
    stats: DiffStats,
) -> str:
    """Format diff results as JSON.

    Args:
        entity_diffs: List of EntityDiff objects
        stats: Diff statistics

    Returns:
        JSON string
    """
    result = {
        "stats": {
            "entities_compared": stats.entities_compared,
            "entities_with_differences": stats.entities_with_differences,
            "fields_added": stats.fields_added,
            "fields_removed": stats.fields_removed,
            "fields_changed": stats.fields_changed,
            "records_added": stats.records_added,
            "records_removed": stats.records_removed,
            "records_modified": stats.records_modified,
        },
        "entities": [
            {
                "name": ed.entity_name,
                "has_differences": ed.has_differences,
                "field_diffs": [
                    {
                        "key": fd.key,
                        "name": fd.name,
                        "diff_type": fd.diff_type,
                        "old_value": fd.old_value,
                        "new_value": fd.new_value,
                    }
                    for fd in ed.field_diffs
                ],
                "record_diffs": [
                    {
                        "id": rd.record_id,
                        "diff_type": rd.diff_type,
                        "changes": {
                            k: {"old": v[0], "new": v[1]}
                            for k, v in rd.field_changes.items()
                        },
                    }
                    for rd in ed.record_diffs
                ],
            }
            for ed in entity_diffs
        ],
    }

    return json.dumps(result, indent=2, ensure_ascii=False, default=str)
