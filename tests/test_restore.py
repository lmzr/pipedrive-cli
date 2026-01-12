"""Tests for restore functionality."""

import io
import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from pipedrive_cli.config import ENTITIES
from pipedrive_cli.restore import (
    clean_record,
    convert_record_for_api,
    extract_reference_id,
    load_id_mappings,
    load_records_from_csv,
    normalize_value_for_comparison,
    parse_csv_value,
    records_equal,
    remap_reference_fields,
    save_id_mapping_entry,
    save_records_to_csv,
    sync_fields,
    update_local_ids,
)


class TestCleanRecord:
    """Tests for clean_record function."""

    def test_removes_readonly_fields(self):
        """clean_record removes read-only fields."""
        record = {
            "id": 1,
            "name": "John Doe",
            "add_time": "2024-01-01",
            "update_time": "2024-01-02",
            "creator_user_id": 123,
        }
        cleaned = clean_record(record)

        assert "id" not in cleaned
        assert "add_time" not in cleaned
        assert "update_time" not in cleaned
        assert "creator_user_id" not in cleaned
        assert cleaned["name"] == "John Doe"

    def test_removes_none_values(self):
        """clean_record removes None values."""
        record = {
            "name": "John Doe",
            "email": None,
            "phone": "+1234567890",
        }
        cleaned = clean_record(record)

        assert "email" not in cleaned
        assert cleaned["name"] == "John Doe"
        assert cleaned["phone"] == "+1234567890"

    def test_keeps_writable_fields(self):
        """clean_record keeps writable fields."""
        record = {
            "name": "John Doe",
            "email": "john@example.com",
            "phone": "+1234567890",
            "org_id": 1,
        }
        cleaned = clean_record(record)

        assert cleaned["name"] == "John Doe"
        assert cleaned["email"] == "john@example.com"
        assert cleaned["phone"] == "+1234567890"
        assert cleaned["org_id"] == 1


class TestParseCsvValue:
    """Tests for parse_csv_value function."""

    def test_parse_empty_string(self):
        """Empty string returns None."""
        assert parse_csv_value("") is None

    def test_parse_json_object(self):
        """JSON object string is parsed."""
        result = parse_csv_value('{"key": "value"}')
        assert result == {"key": "value"}

    def test_parse_json_array(self):
        """JSON array string is parsed."""
        result = parse_csv_value('[1, 2, 3]')
        assert result == [1, 2, 3]

    def test_parse_integer(self):
        """Integer string is parsed as int."""
        assert parse_csv_value("42") == 42
        assert parse_csv_value("-10") == -10

    def test_parse_float(self):
        """Float string is parsed as float."""
        assert parse_csv_value("3.14") == 3.14
        assert parse_csv_value("-2.5") == -2.5

    def test_parse_string(self):
        """Regular string is returned as-is."""
        assert parse_csv_value("hello") == "hello"
        assert parse_csv_value("John Doe") == "John Doe"

    def test_parse_invalid_json(self):
        """Invalid JSON-like string is returned as string."""
        result = parse_csv_value("{invalid json}")
        assert result == "{invalid json}"


class TestExtractReferenceId:
    """Tests for extract_reference_id function."""

    def test_extracts_value_from_dict(self):
        """extract_reference_id extracts 'value' key from dict."""
        value = {"value": 431, "name": "ACME Corp"}
        result = extract_reference_id(value)
        assert result == 431

    def test_integer_passthrough(self):
        """extract_reference_id passes through integers."""
        assert extract_reference_id(431) == 431

    def test_string_passthrough(self):
        """extract_reference_id passes through strings."""
        assert extract_reference_id("test") == "test"

    def test_none_passthrough(self):
        """extract_reference_id passes through None."""
        assert extract_reference_id(None) is None

    def test_dict_without_value_passthrough(self):
        """extract_reference_id passes through dict without 'value' key."""
        value = {"name": "ACME Corp", "id": 431}
        result = extract_reference_id(value)
        assert result == value

    def test_extracts_from_owner_id_format(self):
        """extract_reference_id works with owner_id format."""
        value = {
            "id": 22713797,
            "value": 22713797,
            "name": "Admin User",
            "email": "admin@example.com",
        }
        result = extract_reference_id(value)
        assert result == 22713797


class TestConvertRecordForApi:
    """Tests for convert_record_for_api function."""

    def test_converts_org_field(self):
        """convert_record_for_api extracts org_id integer."""
        record = {
            "name": "John Doe",
            "org_id": {"value": 431, "name": "ACME Corp"},
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["name"] == "John Doe"
        assert result["org_id"] == 431

    def test_converts_owner_id_field(self):
        """convert_record_for_api extracts owner_id integer."""
        record = {
            "name": "Test Org",
            "owner_id": {"id": 100, "value": 100, "name": "Admin"},
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "owner_id", "field_type": "user"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["owner_id"] == 100

    def test_converts_person_id_field(self):
        """convert_record_for_api extracts person_id integer."""
        record = {
            "name": "Sample Deal",
            "person_id": {"value": 123, "name": "John Doe"},
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "person_id", "field_type": "people"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["person_id"] == 123

    def test_non_reference_fields_unchanged(self):
        """convert_record_for_api leaves non-reference fields unchanged."""
        record = {
            "name": "John Doe",
            "email": [{"value": "john@example.com"}],
            "phone": "+1234567890",
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "email", "field_type": "varchar"},
            {"key": "phone", "field_type": "phone"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["name"] == "John Doe"
        assert result["email"] == [{"value": "john@example.com"}]
        assert result["phone"] == "+1234567890"

    def test_unknown_field_passthrough(self):
        """convert_record_for_api passes through unknown fields."""
        record = {
            "name": "John Doe",
            "unknown_field": "value",
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["unknown_field"] == "value"

    def test_already_integer_unchanged(self):
        """convert_record_for_api handles already-integer org_id."""
        record = {
            "name": "John Doe",
            "org_id": 431,
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["org_id"] == 431


class TestRemapReferenceFields:
    """Tests for remap_reference_fields function."""

    def test_remaps_integer_org_id(self):
        """remap_reference_fields remaps integer org_id."""
        record = {"name": "John", "org_id": 11}
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]
        id_mappings = {"organizations": {11: 999}}

        result = remap_reference_fields(record, field_defs, id_mappings)

        assert result["org_id"] == 999
        assert result["name"] == "John"

    def test_remaps_object_org_id(self):
        """remap_reference_fields remaps object org_id."""
        record = {"name": "John", "org_id": {"value": 11, "name": "ACME"}}
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]
        id_mappings = {"organizations": {11: 999}}

        result = remap_reference_fields(record, field_defs, id_mappings)

        assert result["org_id"]["value"] == 999
        assert result["org_id"]["name"] == "ACME"

    def test_remaps_person_id(self):
        """remap_reference_fields remaps person_id."""
        record = {"title": "Deal", "person_id": 5}
        field_defs = [
            {"key": "title", "field_type": "varchar"},
            {"key": "person_id", "field_type": "people"},
        ]
        id_mappings = {"persons": {5: 50}}

        result = remap_reference_fields(record, field_defs, id_mappings)

        assert result["person_id"] == 50

    def test_unmapped_id_unchanged(self):
        """remap_reference_fields leaves unmapped IDs unchanged."""
        record = {"name": "John", "org_id": 999}
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]
        id_mappings = {"organizations": {11: 100}}

        result = remap_reference_fields(record, field_defs, id_mappings)

        assert result["org_id"] == 999

    def test_null_reference_unchanged(self):
        """remap_reference_fields leaves null references unchanged."""
        record = {"name": "John", "org_id": None}
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]
        id_mappings = {"organizations": {11: 999}}

        result = remap_reference_fields(record, field_defs, id_mappings)

        assert result["org_id"] is None

    def test_no_mapping_for_entity(self):
        """remap_reference_fields handles missing entity mappings."""
        record = {"name": "John", "org_id": 11}
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]
        id_mappings = {}  # No organizations mapping

        result = remap_reference_fields(record, field_defs, id_mappings)

        assert result["org_id"] == 11


class TestLoadIdMappings:
    """Tests for load_id_mappings function."""

    def test_loads_mapping_file(self):
        """load_id_mappings loads existing mappings."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backup_path = Path(tmpdir)
            mapping_file = backup_path / "id_mapping.jsonl"

            # Write test mappings
            with open(mapping_file, "w") as f:
                entry1 = {"entity": "organizations", "local_id": 11, "pipedrive_id": 999}
                entry2 = {"entity": "organizations", "local_id": 12, "pipedrive_id": 1000}
                entry3 = {"entity": "persons", "local_id": 1, "pipedrive_id": 50}
                f.write(json.dumps(entry1) + "\n")
                f.write(json.dumps(entry2) + "\n")
                f.write(json.dumps(entry3) + "\n")

            result = load_id_mappings(backup_path)

            assert result["organizations"][11] == 999
            assert result["organizations"][12] == 1000
            assert result["persons"][1] == 50

    def test_returns_empty_if_no_file(self):
        """load_id_mappings returns empty dict if file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backup_path = Path(tmpdir)

            result = load_id_mappings(backup_path)

            assert result == {}

    def test_skips_invalid_lines(self):
        """load_id_mappings skips invalid JSON lines."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backup_path = Path(tmpdir)
            mapping_file = backup_path / "id_mapping.jsonl"

            with open(mapping_file, "w") as f:
                entry1 = {"entity": "organizations", "local_id": 11, "pipedrive_id": 999}
                entry2 = {"entity": "persons", "local_id": 1, "pipedrive_id": 50}
                f.write(json.dumps(entry1) + "\n")
                f.write("invalid json\n")
                f.write(json.dumps(entry2) + "\n")

            result = load_id_mappings(backup_path)

            assert result["organizations"][11] == 999
            assert result["persons"][1] == 50


class TestSaveIdMappingEntry:
    """Tests for save_id_mapping_entry function."""

    def test_appends_entry_to_file(self):
        """save_id_mapping_entry appends JSON entry to file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
            save_id_mapping_entry(f, "organizations", 11, 999)
            save_id_mapping_entry(f, "persons", 1, 50)
            path = f.name

        with open(path) as f:
            lines = f.readlines()

        assert len(lines) == 2
        entry1 = json.loads(lines[0])
        assert entry1["entity"] == "organizations"
        assert entry1["local_id"] == 11
        assert entry1["pipedrive_id"] == 999


class TestSaveRecordsToCsv:
    """Tests for save_records_to_csv function."""

    def test_saves_records_to_csv(self):
        """save_records_to_csv writes records to CSV file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            records = [
                {"id": 1, "name": "John", "org_id": 11},
                {"id": 2, "name": "Jane", "org_id": 12},
            ]

            save_records_to_csv(csv_path, records)

            loaded = load_records_from_csv(csv_path)
            assert len(loaded) == 2
            assert loaded[0]["id"] == 1
            assert loaded[0]["name"] == "John"
            assert loaded[1]["name"] == "Jane"

    def test_saves_complex_values_as_json(self):
        """save_records_to_csv converts complex values to JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "test.csv"
            records = [
                {"id": 1, "org_id": {"value": 11, "name": "ACME"}},
            ]

            save_records_to_csv(csv_path, records)

            loaded = load_records_from_csv(csv_path)
            assert loaded[0]["org_id"]["value"] == 11
            assert loaded[0]["org_id"]["name"] == "ACME"


class TestUpdateLocalIds:
    """Tests for update_local_ids function."""

    def test_updates_record_ids(self):
        """update_local_ids updates record IDs in CSV."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backup_path = Path(tmpdir)
            csv_path = backup_path / "organizations.csv"

            # Write initial CSV
            records = [
                {"id": 11, "name": "ACME"},
                {"id": 12, "name": "Beta Corp"},
            ]
            save_records_to_csv(csv_path, records)

            id_mappings = {"organizations": {11: 999, 12: 1000}}
            field_defs_by_entity = {"organizations": [{"key": "name", "field_type": "varchar"}]}

            update_local_ids(backup_path, id_mappings, field_defs_by_entity)

            loaded = load_records_from_csv(csv_path)
            assert loaded[0]["id"] == 999
            assert loaded[1]["id"] == 1000

    def test_updates_reference_fields(self):
        """update_local_ids updates reference fields in dependent entities."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backup_path = Path(tmpdir)
            csv_path = backup_path / "persons.csv"

            # Write initial CSV
            records = [
                {"id": 1, "name": "John", "org_id": 11},
                {"id": 2, "name": "Jane", "org_id": 12},
            ]
            save_records_to_csv(csv_path, records)

            id_mappings = {
                "organizations": {11: 999, 12: 1000},
                "persons": {1: 50, 2: 51},
            }
            field_defs_by_entity = {
                "persons": [
                    {"key": "name", "field_type": "varchar"},
                    {"key": "org_id", "field_type": "org"},
                ]
            }

            update_local_ids(backup_path, id_mappings, field_defs_by_entity)

            loaded = load_records_from_csv(csv_path)
            assert loaded[0]["id"] == 50
            assert loaded[0]["org_id"] == 999
            assert loaded[1]["id"] == 51
            assert loaded[1]["org_id"] == 1000

    def test_updates_reference_object_values(self):
        """update_local_ids updates value inside reference objects."""
        with tempfile.TemporaryDirectory() as tmpdir:
            backup_path = Path(tmpdir)
            csv_path = backup_path / "persons.csv"

            # Write initial CSV with object reference
            records = [
                {"id": 1, "name": "John", "org_id": {"value": 11, "name": "ACME"}},
            ]
            save_records_to_csv(csv_path, records)

            id_mappings = {
                "organizations": {11: 999},
                "persons": {1: 50},
            }
            field_defs_by_entity = {
                "persons": [
                    {"key": "name", "field_type": "varchar"},
                    {"key": "org_id", "field_type": "org"},
                ]
            }

            update_local_ids(backup_path, id_mappings, field_defs_by_entity)

            loaded = load_records_from_csv(csv_path)
            assert loaded[0]["id"] == 50
            assert loaded[0]["org_id"]["value"] == 999
            assert loaded[0]["org_id"]["name"] == "ACME"  # Preserved


class TestSyncFields:
    """Tests for sync_fields function."""

    @pytest.mark.asyncio
    async def test_updates_field_with_changed_name(self):
        """sync_fields updates fields with different names in backup vs Pipedrive."""
        entity = ENTITIES["persons"]

        # Backup field has new name
        backup_fields = [
            {
                "key": "abc123_custom",
                "name": "Tel portable-OLD",
                "field_type": "varchar",
                "edit_flag": True,
            }
        ]

        # Pipedrive has old name
        current_fields = [
            {
                "id": 42,
                "key": "abc123_custom",
                "name": "Tel portable",
                "field_type": "varchar",
                "edit_flag": True,
            }
        ]

        mock_client = MagicMock()
        mock_client.fetch_fields = AsyncMock(return_value=current_fields)
        mock_client.update_field = AsyncMock(return_value={"id": 42})

        stats = await sync_fields(
            client=mock_client,
            entity=entity,
            backup_fields=backup_fields,
            delete_extra=False,
            dry_run=False,
            log_file=None,
        )

        assert stats.updated == 1
        assert stats.created == 0
        assert stats.deleted == 0
        mock_client.update_field.assert_called_once_with(
            entity, 42, name="Tel portable-OLD"
        )

    @pytest.mark.asyncio
    async def test_updates_field_dry_run(self):
        """sync_fields dry-run mode reports would-be updates without calling API."""
        entity = ENTITIES["persons"]

        backup_fields = [
            {
                "key": "abc123_custom",
                "name": "New Name",
                "field_type": "varchar",
                "edit_flag": True,
            }
        ]
        current_fields = [
            {
                "id": 42,
                "key": "abc123_custom",
                "name": "Old Name",
                "field_type": "varchar",
                "edit_flag": True,
            }
        ]

        mock_client = MagicMock()
        mock_client.fetch_fields = AsyncMock(return_value=current_fields)
        mock_client.update_field = AsyncMock()

        log_buffer = io.StringIO()
        stats = await sync_fields(
            client=mock_client,
            entity=entity,
            backup_fields=backup_fields,
            delete_extra=False,
            dry_run=True,
            log_file=log_buffer,
        )

        assert stats.updated == 1
        mock_client.update_field.assert_not_called()

        # Check log entry
        log_content = log_buffer.getvalue()
        log_entry = json.loads(log_content.strip())
        assert log_entry["action"] == "would_update_field"
        assert log_entry["old_name"] == "Old Name"
        assert log_entry["new_name"] == "New Name"

    @pytest.mark.asyncio
    async def test_no_update_when_names_match(self):
        """sync_fields does not update fields when names are identical."""
        entity = ENTITIES["persons"]

        backup_fields = [
            {
                "key": "abc123_custom",
                "name": "Same Name",
                "field_type": "varchar",
                "edit_flag": True,
            }
        ]
        current_fields = [
            {
                "id": 42,
                "key": "abc123_custom",
                "name": "Same Name",
                "field_type": "varchar",
                "edit_flag": True,
            }
        ]

        mock_client = MagicMock()
        mock_client.fetch_fields = AsyncMock(return_value=current_fields)
        mock_client.update_field = AsyncMock()

        stats = await sync_fields(
            client=mock_client,
            entity=entity,
            backup_fields=backup_fields,
            delete_extra=False,
            dry_run=False,
            log_file=None,
        )

        assert stats.updated == 0
        mock_client.update_field.assert_not_called()

    @pytest.mark.asyncio
    async def test_update_failure_increments_skipped(self):
        """sync_fields increments skipped count when update fails."""
        entity = ENTITIES["persons"]

        backup_fields = [
            {
                "key": "abc123_custom",
                "name": "New Name",
                "field_type": "varchar",
                "edit_flag": True,
            }
        ]
        current_fields = [
            {
                "id": 42,
                "key": "abc123_custom",
                "name": "Old Name",
                "field_type": "varchar",
                "edit_flag": True,
            }
        ]

        mock_client = MagicMock()
        mock_client.fetch_fields = AsyncMock(return_value=current_fields)
        mock_client.update_field = AsyncMock(side_effect=Exception("API error"))

        log_buffer = io.StringIO()
        stats = await sync_fields(
            client=mock_client,
            entity=entity,
            backup_fields=backup_fields,
            delete_extra=False,
            dry_run=False,
            log_file=log_buffer,
        )

        assert stats.updated == 0
        assert stats.skipped == 1

        # Check log entry
        log_content = log_buffer.getvalue()
        log_entry = json.loads(log_content.strip())
        assert log_entry["action"] == "failed_update_field"
        assert "API error" in log_entry["error"]


class TestNormalizeValueForComparison:
    """Tests for normalize_value_for_comparison function."""

    def test_reference_field_extracts_value_from_object(self):
        """Reference field objects should extract .value."""
        value = {"value": 123, "name": "ACME Corp"}
        result = normalize_value_for_comparison(value, "org")
        assert result == 123

    def test_reference_field_passes_integer(self):
        """Integer reference values should pass through."""
        result = normalize_value_for_comparison(456, "org")
        assert result == 456

    def test_none_value_returns_none(self):
        """None values should return None."""
        result = normalize_value_for_comparison(None, "varchar")
        assert result is None

    def test_array_extracts_primary_value(self):
        """Arrays should extract primary item's value."""
        value = [
            {"value": "john@test.com", "primary": False},
            {"value": "main@test.com", "primary": True},
        ]
        result = normalize_value_for_comparison(value, "email")
        assert result == "main@test.com"

    def test_array_uses_first_if_no_primary(self):
        """Arrays without primary should use first item."""
        value = [
            {"value": "first@test.com"},
            {"value": "second@test.com"},
        ]
        result = normalize_value_for_comparison(value, "email")
        assert result == "first@test.com"

    def test_simple_value_passes_through(self):
        """Simple values should pass through unchanged."""
        assert normalize_value_for_comparison("hello", "varchar") == "hello"
        assert normalize_value_for_comparison(42, "int") == 42


class TestRecordsEqual:
    """Tests for records_equal function."""

    def test_equal_simple_records(self):
        """Simple records with same values should be equal."""
        local = {"name": "John", "age": 30}
        remote = {"name": "John", "age": 30, "extra_field": "ignored"}
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "age", "field_type": "int"},
        ]
        assert records_equal(local, remote, field_defs) is True

    def test_different_simple_records(self):
        """Records with different values should not be equal."""
        local = {"name": "John"}
        remote = {"name": "Jane"}
        field_defs = [{"key": "name", "field_type": "varchar"}]
        assert records_equal(local, remote, field_defs) is False

    def test_equal_with_reference_field_object_vs_int(self):
        """Reference field integer should equal object with same value."""
        local = {"org_id": 123}
        remote = {"org_id": {"value": 123, "name": "ACME"}}
        field_defs = [{"key": "org_id", "field_type": "org"}]
        assert records_equal(local, remote, field_defs) is True

    def test_different_reference_fields(self):
        """Reference fields with different IDs should not be equal."""
        local = {"org_id": 123}
        remote = {"org_id": {"value": 456, "name": "Other"}}
        field_defs = [{"key": "org_id", "field_type": "org"}]
        assert records_equal(local, remote, field_defs) is False

    def test_only_compares_local_fields(self):
        """Only fields in local record should be compared."""
        local = {"name": "John"}
        remote = {"name": "John", "email": "different@test.com"}
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "email", "field_type": "varchar"},
        ]
        # email not in local, so should still be equal
        assert records_equal(local, remote, field_defs) is True

    def test_missing_field_in_remote(self):
        """Missing field in remote should cause inequality."""
        local = {"name": "John", "email": "john@test.com"}
        remote = {"name": "John"}  # email missing
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "email", "field_type": "varchar"},
        ]
        assert records_equal(local, remote, field_defs) is False
