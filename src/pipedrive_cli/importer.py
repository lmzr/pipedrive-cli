"""Record import functionality for local datapackages."""

import csv
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TextIO

from .config import READONLY_FIELDS

# -----------------------------------------------------------------------------
# Reference Field Conversion (org_id, person_id, owner_id)
# -----------------------------------------------------------------------------

# Reference field types that need object conversion
REFERENCE_FIELD_TYPES = {"org", "people", "user"}

# Mapping from field_type to entity name
REFERENCE_FIELD_TO_ENTITY = {
    "org": "organizations",
    "people": "persons",
    "user": "users",
}


class ReferenceNotFoundError(Exception):
    """Raised when a referenced entity ID is not found."""

    pass


def is_already_reference_object(value: Any) -> bool:
    """Check if value is already in reference object format.

    Reference objects have a 'value' key containing the integer ID.
    """
    return isinstance(value, dict) and "value" in value


def build_org_object(org_id: int, org: dict[str, Any]) -> dict[str, Any]:
    """Build organization reference object from org record.

    Args:
        org_id: Organization ID
        org: Organization record from organizations.csv

    Returns:
        Reference object for org_id field
    """
    return {
        "name": org.get("name", ""),
        "value": org_id,
        "people_count": org.get("people_count", 0),
        "owner_id": org.get("owner_id"),
        "address": org.get("address"),
        "active_flag": org.get("active_flag", True),
        "cc_email": org.get("cc_email", ""),
    }


def build_person_object(person_id: int, person: dict[str, Any]) -> dict[str, Any]:
    """Build person reference object from person record.

    Args:
        person_id: Person ID
        person: Person record from persons.csv

    Returns:
        Reference object for person_id field
    """
    # Extract primary email from email array
    email = ""
    email_field = person.get("email")
    if isinstance(email_field, list) and email_field:
        primary = next((e for e in email_field if e.get("primary")), email_field[0])
        email = primary.get("value", "")
    elif isinstance(email_field, str):
        email = email_field

    # Extract primary phone from phone array
    phone = ""
    phone_field = person.get("phone")
    if isinstance(phone_field, list) and phone_field:
        primary = next((p for p in phone_field if p.get("primary")), phone_field[0])
        phone = primary.get("value", "")
    elif isinstance(phone_field, str):
        phone = phone_field

    return {
        "value": person_id,
        "name": person.get("name", ""),
        "email": [{"value": email}] if email else [],
        "phone": [{"value": phone}] if phone else [],
    }


def build_user_object(user_id: int, user: dict[str, Any]) -> dict[str, Any]:
    """Build user reference object from user record.

    Args:
        user_id: User ID
        user: User record from users.csv

    Returns:
        Reference object for owner_id field
    """
    return {
        "id": user_id,
        "value": user_id,
        "name": user.get("name", ""),
        "email": user.get("email", ""),
        "has_pic": user.get("has_pic", 0),
        "pic_hash": user.get("pic_hash"),
        "active_flag": user.get("active_flag", True),
    }


def load_related_entity_records(
    base_path: Path,
    entity_name: str,
) -> dict[int, dict[str, Any]]:
    """Load records from related entity CSV, indexed by ID.

    Args:
        base_path: Path to datapackage directory
        entity_name: Entity name (organizations, persons, users)

    Returns:
        Dict mapping record ID to record data

    Raises:
        FileNotFoundError if entity CSV doesn't exist
    """
    csv_path = base_path / f"{entity_name}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Cannot resolve reference: {entity_name}.csv not found in {base_path}"
        )

    records_by_id: dict[int, dict[str, Any]] = {}
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse the id field
            record_id_str = row.get("id", "")
            if not record_id_str:
                continue
            try:
                record_id = int(record_id_str)
            except ValueError:
                continue

            # Parse JSON values in the row
            parsed_row: dict[str, Any] = {}
            for key, value in row.items():
                if value and value.startswith(("{", "[")):
                    try:
                        parsed_row[key] = json.loads(value)
                    except json.JSONDecodeError:
                        parsed_row[key] = value
                else:
                    parsed_row[key] = value
            records_by_id[record_id] = parsed_row

    return records_by_id


def convert_reference_value(
    value: Any,
    field_key: str,
    field_def: dict[str, Any],
    related_data: dict[int, dict[str, Any]],
) -> Any:
    """Convert reference value (integer ID) to object format.

    Args:
        value: Input value (integer ID or already object)
        field_key: Field key name (for error messages)
        field_def: Field definition with field_type
        related_data: Records from related entity, indexed by ID

    Returns:
        Reference object

    Raises:
        ReferenceNotFoundError if ID not found in related_data
    """
    if value is None or value == "":
        return None

    # Already in object format
    if is_already_reference_object(value):
        return value

    # Convert to integer ID
    try:
        ref_id = int(value)
    except (ValueError, TypeError):
        raise ReferenceNotFoundError(
            f"Invalid reference value for {field_key}: {value!r} (expected integer)"
        )

    # Look up in related data
    if ref_id not in related_data:
        field_type = field_def.get("field_type", "")
        entity_name = REFERENCE_FIELD_TO_ENTITY.get(field_type, field_type)
        raise ReferenceNotFoundError(
            f"{field_key}={ref_id} not found in {entity_name}"
        )

    related_record = related_data[ref_id]
    field_type = field_def.get("field_type", "")

    # Build appropriate object based on field type
    if field_type == "org":
        return build_org_object(ref_id, related_record)
    elif field_type == "people":
        return build_person_object(ref_id, related_record)
    elif field_type == "user":
        return build_user_object(ref_id, related_record)
    else:
        # Unknown type - return as-is
        return value


# -----------------------------------------------------------------------------
# Value Conversion for Import
# -----------------------------------------------------------------------------


def is_already_array_format(value: Any) -> bool:
    """Check if value is already in Pipedrive array format (phone/email).

    Pipedrive stores phone/email as arrays of objects with 'value' key.
    """
    if isinstance(value, list):
        if not value:
            return True  # Empty array is valid
        if isinstance(value[0], dict) and "value" in value[0]:
            return True
    return False


def extract_comparable_value(value: Any) -> str:
    """Extract comparable string value from Pipedrive field formats.

    Handles:
    - Pipedrive array format (email/phone): [{"value": "...", "primary": true}]
    - Reference objects (org_id, person_id): {"value": 431, "name": "..."}
    - Plain strings: returned as-is
    - Other types: converted to string

    For arrays, returns the primary value or first value.
    For reference objects, returns the value field.
    """
    if isinstance(value, list) and value:
        if isinstance(value[0], dict):
            # Get primary value or first value
            primary = next((item for item in value if item.get("primary")), value[0])
            return str(primary.get("value", ""))
    if isinstance(value, dict) and "value" in value:
        # Reference object like {"value": 431, "name": "ACME"}
        return str(value["value"])
    return str(value) if value is not None else ""


def convert_phone_value(value: Any) -> Any:
    """Convert phone value to Pipedrive array format.

    Args:
        value: Phone number as string, or already in array format

    Returns:
        Phone in array format: [{"value": "...", "label": "mobile", "primary": true}]
    """
    if value is None or value == "":
        return None

    # Already in correct format
    if is_already_array_format(value):
        return value

    # Convert string to array format
    str_value = str(value).strip()
    if not str_value:
        return None

    return [{"value": str_value, "label": "mobile", "primary": True}]


def convert_email_value(value: Any) -> Any:
    """Convert email value to Pipedrive array format.

    Args:
        value: Email as string, or already in array format

    Returns:
        Email in array format: [{"value": "...", "label": "work", "primary": true}]
    """
    if value is None or value == "":
        return None

    # Already in correct format
    if is_already_array_format(value):
        return value

    # Convert string to array format
    str_value = str(value).strip()
    if not str_value:
        return None

    return [{"value": str_value, "label": "work", "primary": True}]


def convert_enum_value(value: Any, field_def: dict[str, Any]) -> Any:
    """Convert enum value (label) to option ID.

    Args:
        value: Option label string, or already an ID
        field_def: Field definition with 'options' list

    Returns:
        Option ID (integer) or None if not found
    """
    if value is None or value == "":
        return None

    # Already an integer ID
    if isinstance(value, int):
        return value

    # Try to parse as integer (already an ID)
    str_value = str(value).strip()
    try:
        return int(str_value)
    except ValueError:
        pass

    # Look up label in options
    options = field_def.get("options", [])
    for opt in options:
        if opt.get("label") == str_value:
            return opt.get("id")

    # Not found - return as-is (will fail validation later)
    return str_value


def convert_set_value(value: Any, field_def: dict[str, Any]) -> Any:
    """Convert set value (comma-separated labels) to comma-separated IDs.

    Args:
        value: Comma-separated labels string, or already IDs
        field_def: Field definition with 'options' list

    Returns:
        Comma-separated option IDs string, or None if empty
    """
    if value is None or value == "":
        return None

    # Already a list of integers
    if isinstance(value, list):
        if all(isinstance(v, int) for v in value):
            return ",".join(str(v) for v in value)
        # List of labels - convert each
        options = field_def.get("options", [])
        label_to_id = {opt.get("label"): opt.get("id") for opt in options}
        ids = []
        for v in value:
            if isinstance(v, int):
                ids.append(str(v))
            elif str(v).strip() in label_to_id:
                ids.append(str(label_to_id[str(v).strip()]))
        return ",".join(ids) if ids else None

    # String value
    str_value = str(value).strip()
    if not str_value:
        return None

    # Check if already comma-separated IDs
    parts = [p.strip() for p in str_value.split(",")]
    try:
        # If all parts are integers, assume already IDs
        all_ints = all(p.isdigit() for p in parts if p)
        if all_ints:
            return str_value
    except ValueError:
        pass

    # Convert labels to IDs
    options = field_def.get("options", [])
    label_to_id = {opt.get("label"): opt.get("id") for opt in options}
    ids = []
    for label in parts:
        if label in label_to_id:
            ids.append(str(label_to_id[label]))
        elif label:
            # Not found - keep as-is (will fail validation later)
            ids.append(label)

    return ",".join(ids) if ids else None


def convert_value_for_import(
    value: Any,
    field_key: str,
    field_def: dict[str, Any],
    related_entities: dict[str, dict[int, dict[str, Any]]] | None = None,
) -> Any:
    """Convert a value to Pipedrive's expected format based on field type.

    Args:
        value: Input value from import file
        field_key: Field key name
        field_def: Field definition from pipedrive_fields
        related_entities: Dict mapping entity name to records indexed by ID

    Returns:
        Converted value in Pipedrive format

    Raises:
        ReferenceNotFoundError if reference field ID not found
    """
    if value is None or value == "":
        return value

    field_type = field_def.get("field_type", "")

    # Reference fields (org, people, user): keep as integer ID for local storage
    # (conversion to object happens in store command for API)
    if field_type in REFERENCE_FIELD_TYPES:
        entity_name = REFERENCE_FIELD_TO_ENTITY.get(field_type)
        if entity_name and related_entities and entity_name in related_entities:
            # Validate the ID exists, but keep as integer
            try:
                ref_id = int(value)
            except (ValueError, TypeError):
                raise ReferenceNotFoundError(
                    f"Invalid reference value for {field_key}: {value!r} (expected integer)"
                )
            if ref_id not in related_entities[entity_name]:
                raise ReferenceNotFoundError(
                    f"{field_key}={ref_id} not found in {entity_name}"
                )
            return ref_id  # Return validated integer ID
        # No related data available - pass through as-is
        return value

    # Phone field
    if field_type == "phone":
        return convert_phone_value(value)

    # Email field (field_type is varchar but stored as array)
    if field_key == "email":
        return convert_email_value(value)

    # Enum field
    if field_type == "enum":
        return convert_enum_value(value, field_def)

    # Set field
    if field_type == "set":
        return convert_set_value(value, field_def)

    # Other types - pass through
    return value


def convert_record_for_import(
    record: dict[str, Any],
    field_defs: list[dict[str, Any]],
    related_entities: dict[str, dict[int, dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Convert all values in a record to Pipedrive format.

    Args:
        record: Input record with field keys and values
        field_defs: List of field definitions from pipedrive_fields
        related_entities: Dict mapping entity name to records indexed by ID

    Returns:
        Record with converted values

    Raises:
        ReferenceNotFoundError if reference field ID not found
    """
    # Build lookup by key
    field_by_key = {f.get("key"): f for f in field_defs}

    converted = {}
    for key, value in record.items():
        field_def = field_by_key.get(key, {})
        converted[key] = convert_value_for_import(value, key, field_def, related_entities)

    return converted


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
        key_values = tuple(extract_comparable_value(record.get(k)) for k in key_fields)
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
    field_defs: list[dict[str, Any]] | None = None,
    base_path: Path | None = None,
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
        field_defs: Field definitions from pipedrive_fields for format conversion
        base_path: Path to datapackage for loading related entities

    Returns:
        Tuple of (stats, merged_records, results)

    Raises:
        ReferenceNotFoundError if a reference field ID is not found
        FileNotFoundError if a required related entity CSV is missing
    """
    stats = ImportStats()
    results: list[ImportResult] = []

    # Detect which reference fields are in valid_fields and load related entities
    related_entities: dict[str, dict[int, dict[str, Any]]] = {}
    if field_defs and base_path:
        field_by_key = {f.get("key"): f for f in field_defs}
        for field_key in valid_fields:
            field_def = field_by_key.get(field_key, {})
            field_type = field_def.get("field_type", "")
            if field_type in REFERENCE_FIELD_TYPES:
                entity_name = REFERENCE_FIELD_TO_ENTITY.get(field_type)
                if entity_name and entity_name not in related_entities:
                    related_entities[entity_name] = load_related_entity_records(
                        base_path, entity_name
                    )

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

            # Convert values to Pipedrive format
            if field_defs:
                filtered_record = convert_record_for_import(
                    filtered_record, field_defs, related_entities or None
                )

            # Check for duplicate if key fields specified
            duplicate_index: int | None = None
            if key_fields:
                key_values = tuple(
                    extract_comparable_value(input_record.get(k)) for k in key_fields
                )
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
                    key_values = tuple(
                        extract_comparable_value(input_record.get(k)) for k in key_fields
                    )
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
