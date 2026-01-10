"""Record import functionality for local datapackages."""

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

from .config import READONLY_FIELDS


@dataclass
class ImportStats:
    """Statistics for import operation."""

    total: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    readonly_skipped: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


@dataclass
class ImportResult:
    """Result of importing a single record."""

    row_number: int
    action: str  # "created", "updated", "skipped", "failed"
    record_id: int | str | None = None
    error: str | None = None
    old_values: dict[str, Any] | None = None
    new_values: dict[str, Any] | None = None


def detect_format(path: Path) -> str:
    """Detect file format from extension.

    Args:
        path: Input file path

    Returns:
        Format string: "csv", "json", or "xlsx"

    Raises:
        ValueError if format cannot be determined
    """
    suffix = path.suffix.lower()
    format_map = {
        ".csv": "csv",
        ".json": "json",
        ".xlsx": "xlsx",
    }
    if suffix not in format_map:
        raise ValueError(
            f"Cannot determine format from extension '{suffix}'. "
            "Supported formats: csv, json, xlsx"
        )
    return format_map[suffix]


def load_csv_records(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    """Load records from a CSV file.

    Args:
        path: Path to CSV file

    Returns:
        Tuple of (records, fieldnames)
    """
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        records = []
        for row in reader:
            # Parse JSON values
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
        return records, list(fieldnames)


def load_json_records(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    """Load records from a JSON file.

    Args:
        path: Path to JSON file

    Returns:
        Tuple of (records, fieldnames)
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("JSON file must contain an array of objects")

    if not data:
        return [], []

    # Get fieldnames from first record
    fieldnames = list(data[0].keys())
    return data, fieldnames


def load_xlsx_records(
    path: Path, sheet: str | None = None
) -> tuple[list[dict[str, Any]], list[str]]:
    """Load records from an XLSX file.

    Args:
        path: Path to XLSX file
        sheet: Sheet name (default: first sheet)

    Returns:
        Tuple of (records, fieldnames)
    """
    from .converter import load_xlsx

    result = load_xlsx(path, sheet=sheet, header_row=1, preserve_links=False)
    return result.records, result.fieldnames


def load_input_file(
    path: Path,
    file_format: str | None = None,
    sheet: str | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Load records from an input file.

    Args:
        path: Path to input file
        file_format: Format override (auto-detect if None)
        sheet: Sheet name for XLSX files

    Returns:
        Tuple of (records, fieldnames)
    """
    fmt = file_format or detect_format(path)

    if fmt == "csv":
        return load_csv_records(path)
    elif fmt == "json":
        return load_json_records(path)
    elif fmt == "xlsx":
        return load_xlsx_records(path, sheet)
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def validate_input_fields(
    input_fields: list[str],
    schema_fields: list[dict[str, Any]],
) -> tuple[list[str], list[str], list[str]]:
    """Validate input fields against schema.

    Args:
        input_fields: Field names from input file
        schema_fields: Field definitions from datapackage

    Returns:
        Tuple of (valid_fields, readonly_skipped, unknown_fields)
    """
    schema_keys = {f.get("key", "") for f in schema_fields}

    valid_fields: list[str] = []
    readonly_skipped: list[str] = []
    unknown_fields: list[str] = []

    for field_name in input_fields:
        if field_name in READONLY_FIELDS:
            readonly_skipped.append(field_name)
        elif field_name in schema_keys:
            valid_fields.append(field_name)
        else:
            unknown_fields.append(field_name)

    return valid_fields, readonly_skipped, unknown_fields


def build_dedup_index(
    records: list[dict[str, Any]],
    key_fields: list[str],
) -> dict[tuple, int]:
    """Build an index for deduplication based on key fields.

    Args:
        records: Existing records
        key_fields: Field(s) to use as key

    Returns:
        Dict mapping key tuple to record index
    """
    index: dict[tuple, int] = {}
    for i, record in enumerate(records):
        key_values = tuple(str(record.get(k, "")) for k in key_fields)
        # Keep first occurrence
        if key_values not in index:
            index[key_values] = i
    return index


def get_max_id(records: list[dict[str, Any]]) -> int:
    """Get maximum ID from records.

    Args:
        records: List of records

    Returns:
        Maximum ID value (0 if no records or no IDs)
    """
    max_id = 0
    for record in records:
        record_id = record.get("id")
        if record_id is not None:
            try:
                int_id = int(record_id)
                if int_id > max_id:
                    max_id = int_id
            except (ValueError, TypeError):
                pass
    return max_id


def import_records(
    input_records: list[dict[str, Any]],
    existing_records: list[dict[str, Any]],
    valid_fields: list[str],
    key_fields: list[str] | None = None,
    on_duplicate: str = "update",
    auto_id: bool = False,
    log_file: TextIO | None = None,
) -> tuple[ImportStats, list[dict[str, Any]], list[ImportResult]]:
    """Import records with deduplication and optional ID generation.

    Args:
        input_records: Records to import
        existing_records: Existing records in datapackage
        valid_fields: Field names that passed validation
        key_fields: Field(s) for deduplication (None = no dedup)
        on_duplicate: Action on duplicate: "update", "skip", "error"
        auto_id: Generate IDs for new records
        log_file: Optional file for JSON lines logging

    Returns:
        Tuple of (stats, merged_records, results)
    """
    stats = ImportStats()
    results: list[ImportResult] = []

    # Build dedup index if key fields specified
    dedup_index: dict[tuple, int] = {}
    if key_fields:
        dedup_index = build_dedup_index(existing_records, key_fields)

    # Get next ID for auto-ID generation
    next_id = get_max_id(existing_records) + 1 if auto_id else 0

    # Start with a copy of existing records
    merged_records = list(existing_records)

    for row_num, input_record in enumerate(input_records, 1):
        stats.total += 1
        result = ImportResult(row_number=row_num, action="failed")

        try:
            # Filter to valid fields only
            filtered_record = {k: v for k, v in input_record.items() if k in valid_fields}

            # Check for duplicate if key fields specified
            duplicate_index: int | None = None
            if key_fields:
                key_values = tuple(str(input_record.get(k, "")) for k in key_fields)
                duplicate_index = dedup_index.get(key_values)

            if duplicate_index is not None:
                # Handle duplicate
                if on_duplicate == "skip":
                    result.action = "skipped"
                    result.record_id = merged_records[duplicate_index].get("id")
                    stats.skipped += 1
                elif on_duplicate == "error":
                    result.action = "failed"
                    result.error = f"Duplicate key: {key_values}"
                    stats.failed += 1
                    stats.errors.append(f"Row {row_num}: Duplicate key {key_values}")
                else:  # update
                    # Merge input values into existing record
                    old_record = merged_records[duplicate_index].copy()
                    merged_records[duplicate_index].update(filtered_record)
                    result.action = "updated"
                    result.record_id = merged_records[duplicate_index].get("id")
                    result.old_values = {
                        k: old_record.get(k) for k in filtered_record.keys()
                    }
                    result.new_values = filtered_record
                    stats.updated += 1
            else:
                # New record
                new_record = filtered_record.copy()

                # Auto-generate ID if needed
                if auto_id and "id" not in new_record:
                    new_record["id"] = next_id
                    next_id += 1

                merged_records.append(new_record)
                result.action = "created"
                result.record_id = new_record.get("id")
                result.new_values = new_record
                stats.created += 1

                # Update dedup index for subsequent records
                if key_fields:
                    key_values = tuple(str(input_record.get(k, "")) for k in key_fields)
                    dedup_index[key_values] = len(merged_records) - 1

        except Exception as e:
            result.action = "failed"
            result.error = str(e)
            stats.failed += 1
            stats.errors.append(f"Row {row_num}: {e}")

        results.append(result)

        # Write log line
        if log_file:
            log_entry = {
                "row": result.row_number,
                "action": result.action,
                "id": result.record_id,
            }
            if result.error:
                log_entry["error"] = result.error
            if result.old_values:
                log_entry["old"] = result.old_values
            if result.new_values:
                log_entry["new"] = result.new_values
            log_file.write(json.dumps(log_entry, ensure_ascii=False) + "\n")

    return stats, merged_records, results
