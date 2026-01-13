"""Duplicate detection for Pipedrive records."""

from __future__ import annotations

import csv
import io
import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from rich.console import Console
from rich.table import Table

from .field import build_option_lookup, format_option_value
from .importer import extract_comparable_value


@dataclass
class DuplicateGroup:
    """A group of duplicate records sharing the same key values."""

    key_values: tuple[str, ...]
    key_fields: list[str]
    records: list[dict[str, Any]] = field(default_factory=list)

    @property
    def count(self) -> int:
        """Number of records in this group."""
        return len(self.records)

    def key_display(self) -> str:
        """Format key values for display."""
        parts = []
        for field_name, value in zip(self.key_fields, self.key_values):
            parts.append(f'{field_name} = "{value}"')
        return ", ".join(parts)


@dataclass
class DuplicateStats:
    """Statistics for duplicate detection."""

    total_records: int = 0
    unique_keys: int = 0
    duplicate_groups: int = 0
    total_duplicates: int = 0  # Total records that are in duplicate groups


def find_duplicates(
    records: list[dict[str, Any]],
    key_fields: list[str],
    include_nulls: bool = False,
) -> tuple[list[DuplicateGroup], DuplicateStats]:
    """Find all duplicate groups based on key fields.

    Args:
        records: List of records to check
        key_fields: Field(s) to use as duplicate key
        include_nulls: If True, include records with null key values

    Returns:
        Tuple of (duplicate_groups, stats) where duplicate_groups contains
        only groups with 2+ records
    """
    # Group records by key values
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)

    for record in records:
        key_values = tuple(
            extract_comparable_value(record.get(k)) for k in key_fields
        )

        # Skip records with null/empty key values unless include_nulls
        if not include_nulls:
            if all(v == "" for v in key_values):
                continue

        groups[key_values].append(record)

    # Build result: only groups with duplicates (2+ records)
    duplicate_groups: list[DuplicateGroup] = []
    total_in_duplicates = 0

    for key_values, group_records in groups.items():
        if len(group_records) >= 2:
            duplicate_groups.append(
                DuplicateGroup(
                    key_values=key_values,
                    key_fields=key_fields,
                    records=group_records,
                )
            )
            total_in_duplicates += len(group_records)

    # Sort by group size (largest first)
    duplicate_groups.sort(key=lambda g: g.count, reverse=True)

    stats = DuplicateStats(
        total_records=len(records),
        unique_keys=len(groups),
        duplicate_groups=len(duplicate_groups),
        total_duplicates=total_in_duplicates,
    )

    return duplicate_groups, stats


def format_duplicate_table(
    groups: list[DuplicateGroup],
    stats: DuplicateStats,
    fields: list[dict[str, Any]] | None,
    console: Console,
    entity_name: str,
    summary_only: bool = False,
    limit: int | None = None,
) -> None:
    """Format duplicate groups as Rich tables.

    Args:
        groups: List of duplicate groups to display
        stats: Duplicate detection statistics
        fields: Field definitions for column name lookup
        console: Rich console for output
        entity_name: Name of the entity for display
        summary_only: If True, show only statistics without record details
        limit: Maximum number of groups to display
    """
    # Always show summary header
    if stats.duplicate_groups == 0:
        console.print(f"[green]No duplicates found in {entity_name}[/green]")
        console.print(f"[dim]Total records analyzed: {stats.total_records}[/dim]")
        return

    console.print(
        f"[bold]Duplicate Groups for {entity_name}[/bold] "
        f"({stats.duplicate_groups} groups, {stats.total_duplicates} records)"
    )
    console.print()

    if summary_only:
        # Show statistics only
        console.print(f"  Total records analyzed: {stats.total_records}")
        console.print(f"  Unique keys: {stats.unique_keys}")
        console.print(f"  Duplicate groups: {stats.duplicate_groups}")
        console.print(f"  Total records in duplicates: {stats.total_duplicates}")
        return

    # Build field name lookup
    field_names: dict[str, str] = {}
    if fields:
        for f in fields:
            field_names[f.get("key", "")] = f.get("name", f.get("key", ""))

    # Build option lookup for enum/set fields
    option_lookup = build_option_lookup(fields) if fields else {}

    # Apply limit
    display_groups = groups[:limit] if limit else groups

    for i, group in enumerate(display_groups, 1):
        # Group header with resolved field names
        key_parts = []
        for field_key, value in zip(group.key_fields, group.key_values):
            display_name = field_names.get(field_key, field_key)
            key_parts.append(f'{display_name} = "{value}"')
        key_display = ", ".join(key_parts)

        console.print(
            f"[bold cyan]Group {i}:[/bold cyan] {key_display} "
            f"({group.count} records)"
        )

        # Build table for this group
        if group.records:
            all_columns = list(group.records[0].keys())
            # Limit columns for readability
            max_columns = 8
            columns = all_columns[:max_columns] if len(all_columns) > max_columns else all_columns

            table = Table(show_header=True, header_style="bold")
            for col in columns:
                display_name = field_names.get(col, col)
                style = "cyan" if col == "id" else None
                table.add_column(display_name, style=style, overflow="fold")

            for record in group.records:
                row: list[str] = []
                for col in columns:
                    value = record.get(col, "")
                    # Format enum/set values with labels
                    if col in option_lookup:
                        str_val = format_option_value(value, col, option_lookup)
                    elif isinstance(value, dict):
                        if "name" in value and value["name"] is not None:
                            str_val = str(value["name"])
                        elif "value" in value and value["value"] is not None:
                            str_val = str(value["value"])
                        else:
                            str_val = str(value)
                    elif isinstance(value, list):
                        if value and isinstance(value[0], dict):
                            item = value[0]
                            if "value" in item and item["value"] is not None:
                                str_val = str(item["value"])
                            else:
                                str_val = str(item)
                        else:
                            str_val = str(value[0]) if value else ""
                    else:
                        str_val = str(value) if value is not None else ""
                    # Truncate long values
                    if len(str_val) > 40:
                        str_val = str_val[:37] + "..."
                    row.append(str_val)
                table.add_row(*row)

            console.print(table)
            console.print()

    # Show truncation message if needed
    if limit and len(groups) > limit:
        console.print(
            f"[dim]Showing {limit}/{len(groups)} groups. "
            f"Use --limit to show more.[/dim]"
        )


def format_duplicate_json(
    groups: list[DuplicateGroup],
    stats: DuplicateStats,
) -> str:
    """Format duplicate groups as JSON.

    Args:
        groups: List of duplicate groups
        stats: Duplicate detection statistics

    Returns:
        JSON string with stats and groups
    """
    result = {
        "stats": {
            "total_records": stats.total_records,
            "unique_keys": stats.unique_keys,
            "duplicate_groups": stats.duplicate_groups,
            "total_duplicates": stats.total_duplicates,
        },
        "groups": [
            {
                "key": dict(zip(g.key_fields, g.key_values)),
                "count": g.count,
                "records": g.records,
            }
            for g in groups
        ],
    }
    return json.dumps(result, indent=2, ensure_ascii=False, default=str)


def format_duplicate_csv(
    groups: list[DuplicateGroup],
    fields: list[dict[str, Any]] | None = None,
) -> str:
    """Format duplicate groups as CSV with group identifier.

    Args:
        groups: List of duplicate groups
        fields: Field definitions (unused but kept for consistency)

    Returns:
        CSV string with _duplicate_group column
    """
    if not groups:
        return ""

    # Collect all records with group identifier
    all_records: list[dict[str, Any]] = []
    for i, group in enumerate(groups, 1):
        for record in group.records:
            row = {"_duplicate_group": i}
            row.update(record)
            all_records.append(row)

    if not all_records:
        return ""

    output = io.StringIO()
    columns = list(all_records[0].keys())

    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()

    for record in all_records:
        # Flatten complex values to JSON strings
        flat: dict[str, Any] = {}
        for k, v in record.items():
            if isinstance(v, (dict, list)):
                flat[k] = json.dumps(v, ensure_ascii=False)
            else:
                flat[k] = v
        writer.writerow(flat)

    return output.getvalue()
