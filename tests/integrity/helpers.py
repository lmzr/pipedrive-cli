"""Helpers for integrity testing of datapackage operations."""

import csv
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class DatapackageState:
    """Snapshot of a datapackage state for comparison.

    Captures the complete state of a datapackage including:
    - CSV columns per entity
    - CSV row counts per entity
    - Frictionless schema fields per entity
    - Pipedrive field metadata per entity
    - CSV content checksums per entity
    """

    csv_columns: dict[str, set[str]] = field(default_factory=dict)
    csv_row_counts: dict[str, int] = field(default_factory=dict)
    schema_fields: dict[str, list[str]] = field(default_factory=dict)
    pipedrive_fields: dict[str, list[str]] = field(default_factory=dict)
    csv_checksums: dict[str, str] = field(default_factory=dict)
    csv_data: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    def __eq__(self, other: object) -> bool:
        """Compare states excluding csv_data (too verbose for comparison)."""
        if not isinstance(other, DatapackageState):
            return False
        return (
            self.csv_columns == other.csv_columns
            and self.csv_row_counts == other.csv_row_counts
            and self.schema_fields == other.schema_fields
            and self.pipedrive_fields == other.pipedrive_fields
            and self.csv_checksums == other.csv_checksums
        )


def capture_state(base_path: Path) -> DatapackageState:
    """Capture the complete state of a datapackage.

    Args:
        base_path: Path to the datapackage directory

    Returns:
        DatapackageState with all captured information
    """
    state = DatapackageState()

    # Load datapackage.json
    datapackage_path = base_path / "datapackage.json"
    if not datapackage_path.exists():
        return state

    with open(datapackage_path, encoding="utf-8") as f:
        pkg = json.load(f)

    # Process each resource
    for resource in pkg.get("resources", []):
        entity = resource["name"]
        schema = resource.get("schema", {})

        # Capture schema.fields (Frictionless)
        state.schema_fields[entity] = [
            f["name"] for f in schema.get("fields", [])
        ]

        # Capture pipedrive_fields
        state.pipedrive_fields[entity] = [
            f["key"] for f in schema.get("pipedrive_fields", [])
        ]

        # Capture CSV state
        csv_path = base_path / f"{entity}.csv"
        if csv_path.exists():
            state.csv_columns[entity] = _get_csv_columns(csv_path)
            state.csv_row_counts[entity] = _count_csv_rows(csv_path)
            state.csv_checksums[entity] = _compute_csv_checksum(csv_path)
            state.csv_data[entity] = _load_csv_data(csv_path)

    return state


def _get_csv_columns(csv_path: Path) -> set[str]:
    """Get column names from CSV file."""
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        return set(header) if header else set()


def _count_csv_rows(csv_path: Path) -> int:
    """Count data rows in CSV file (excluding header)."""
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip header
        return sum(1 for _ in reader)


def _compute_csv_checksum(csv_path: Path) -> str:
    """Compute MD5 checksum of CSV file content."""
    with open(csv_path, "rb") as f:
        return hashlib.md5(f.read()).hexdigest()


def _load_csv_data(csv_path: Path) -> list[dict[str, Any]]:
    """Load all CSV data as list of dicts."""
    with open(csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


# =============================================================================
# Assertion helpers
# =============================================================================


def assert_state_unchanged(
    before: DatapackageState,
    after: DatapackageState,
    message: str = "",
) -> None:
    """Assert that datapackage state is completely unchanged.

    Args:
        before: State before operation
        after: State after operation
        message: Optional message for assertion errors
    """
    prefix = f"{message}: " if message else ""

    assert before.csv_columns == after.csv_columns, (
        f"{prefix}CSV columns changed: "
        f"before={before.csv_columns}, after={after.csv_columns}"
    )
    assert before.csv_row_counts == after.csv_row_counts, (
        f"{prefix}CSV row counts changed: "
        f"before={before.csv_row_counts}, after={after.csv_row_counts}"
    )
    assert before.schema_fields == after.schema_fields, (
        f"{prefix}Schema fields changed: "
        f"before={before.schema_fields}, after={after.schema_fields}"
    )
    assert before.pipedrive_fields == after.pipedrive_fields, (
        f"{prefix}Pipedrive fields changed: "
        f"before={before.pipedrive_fields}, after={after.pipedrive_fields}"
    )
    assert before.csv_checksums == after.csv_checksums, (
        f"{prefix}CSV content changed (checksums differ)"
    )


def assert_field_removed(
    before: DatapackageState,
    after: DatapackageState,
    entity: str,
    field_key: str,
) -> None:
    """Assert that a field was removed from all 3 locations.

    Args:
        before: State before operation
        after: State after operation
        entity: Entity name (e.g., "persons")
        field_key: Field key that should be removed
    """
    # Check CSV columns
    assert field_key in before.csv_columns.get(entity, set()), (
        f"Field '{field_key}' was not in CSV columns before"
    )
    assert field_key not in after.csv_columns.get(entity, set()), (
        f"Field '{field_key}' still in CSV columns after delete"
    )

    # Check schema.fields
    assert field_key in before.schema_fields.get(entity, []), (
        f"Field '{field_key}' was not in schema.fields before"
    )
    assert field_key not in after.schema_fields.get(entity, []), (
        f"Field '{field_key}' still in schema.fields after delete"
    )

    # Check pipedrive_fields
    assert field_key in before.pipedrive_fields.get(entity, []), (
        f"Field '{field_key}' was not in pipedrive_fields before"
    )
    assert field_key not in after.pipedrive_fields.get(entity, []), (
        f"Field '{field_key}' still in pipedrive_fields after delete"
    )


def assert_field_added(
    before: DatapackageState,
    after: DatapackageState,
    entity: str,
    field_key: str,
) -> None:
    """Assert that a field was added to all 3 locations.

    Args:
        before: State before operation
        after: State after operation
        entity: Entity name (e.g., "persons")
        field_key: Field key that should be added
    """
    # Check CSV columns
    assert field_key not in before.csv_columns.get(entity, set()), (
        f"Field '{field_key}' already existed in CSV columns"
    )
    assert field_key in after.csv_columns.get(entity, set()), (
        f"Field '{field_key}' not in CSV columns after add"
    )

    # Check schema.fields
    assert field_key not in before.schema_fields.get(entity, []), (
        f"Field '{field_key}' already existed in schema.fields"
    )
    assert field_key in after.schema_fields.get(entity, []), (
        f"Field '{field_key}' not in schema.fields after add"
    )

    # Check pipedrive_fields
    assert field_key not in before.pipedrive_fields.get(entity, []), (
        f"Field '{field_key}' already existed in pipedrive_fields"
    )
    assert field_key in after.pipedrive_fields.get(entity, []), (
        f"Field '{field_key}' not in pipedrive_fields after add"
    )


def assert_row_count_unchanged(
    before: DatapackageState,
    after: DatapackageState,
    entity: str,
) -> None:
    """Assert that row count is unchanged for an entity.

    Args:
        before: State before operation
        after: State after operation
        entity: Entity name to check
    """
    before_count = before.csv_row_counts.get(entity, 0)
    after_count = after.csv_row_counts.get(entity, 0)
    assert before_count == after_count, (
        f"Row count changed for {entity}: before={before_count}, after={after_count}"
    )


def assert_other_entities_unchanged(
    before: DatapackageState,
    after: DatapackageState,
    except_entity: str,
) -> None:
    """Assert that all entities except one are unchanged.

    Args:
        before: State before operation
        after: State after operation
        except_entity: Entity that is allowed to change
    """
    for entity in before.csv_columns:
        if entity == except_entity:
            continue

        assert before.csv_columns.get(entity) == after.csv_columns.get(entity), (
            f"CSV columns changed for {entity}"
        )
        assert before.csv_row_counts.get(entity) == after.csv_row_counts.get(entity), (
            f"Row count changed for {entity}"
        )
        assert before.schema_fields.get(entity) == after.schema_fields.get(entity), (
            f"Schema fields changed for {entity}"
        )
        assert before.pipedrive_fields.get(entity) == after.pipedrive_fields.get(
            entity
        ), f"Pipedrive fields changed for {entity}"
        assert before.csv_checksums.get(entity) == after.csv_checksums.get(entity), (
            f"CSV content changed for {entity}"
        )


def assert_csv_values_changed(
    before: DatapackageState,
    after: DatapackageState,
    entity: str,
    field_key: str,
    expected_changes: int | None = None,
) -> int:
    """Assert that CSV values changed for a specific field.

    Args:
        before: State before operation
        after: State after operation
        entity: Entity name
        field_key: Field key to check
        expected_changes: Expected number of changed values (optional)

    Returns:
        Number of rows where the value changed
    """
    before_data = before.csv_data.get(entity, [])
    after_data = after.csv_data.get(entity, [])

    assert len(before_data) == len(after_data), "Row count changed unexpectedly"

    changes = 0
    for before_row, after_row in zip(before_data, after_data):
        if before_row.get(field_key) != after_row.get(field_key):
            changes += 1

    assert changes > 0, f"No values changed for field '{field_key}'"

    if expected_changes is not None:
        assert changes == expected_changes, (
            f"Expected {expected_changes} changes, got {changes}"
        )

    return changes


def assert_csv_values_unchanged(
    before: DatapackageState,
    after: DatapackageState,
    entity: str,
    field_key: str,
) -> None:
    """Assert that CSV values are unchanged for a specific field.

    Args:
        before: State before operation
        after: State after operation
        entity: Entity name
        field_key: Field key to check
    """
    before_data = before.csv_data.get(entity, [])
    after_data = after.csv_data.get(entity, [])

    for i, (before_row, after_row) in enumerate(zip(before_data, after_data)):
        assert before_row.get(field_key) == after_row.get(field_key), (
            f"Value changed for field '{field_key}' at row {i}: "
            f"'{before_row.get(field_key)}' -> '{after_row.get(field_key)}'"
        )


def get_pipedrive_field_metadata(
    base_path: Path,
    entity: str,
    field_key: str,
) -> dict[str, Any] | None:
    """Get full pipedrive_field metadata for a field.

    Args:
        base_path: Path to datapackage directory
        entity: Entity name
        field_key: Field key to find

    Returns:
        Field metadata dict or None if not found
    """
    datapackage_path = base_path / "datapackage.json"
    with open(datapackage_path, encoding="utf-8") as f:
        pkg = json.load(f)

    for resource in pkg.get("resources", []):
        if resource["name"] == entity:
            for field in resource["schema"].get("pipedrive_fields", []):
                if field.get("key") == field_key:
                    return field
    return None
