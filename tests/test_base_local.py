"""Tests for local field operations in base module."""

import csv
import json

import pytest

from pipedrive_cli.base import (
    add_schema_field,
    generate_local_field_key,
    get_entity_fields,
    is_local_field,
    load_package,
    load_records,
    remove_field_from_records,
    remove_schema_field,
    rename_csv_column,
    rename_field_key,
    save_package,
    save_records,
    update_entity_fields,
)


class TestGenerateLocalFieldKey:
    """Tests for generate_local_field_key function."""

    def test_returns_prefixed_key(self):
        """Key starts with _new_ prefix."""
        key = generate_local_field_key()
        assert key.startswith("_new_")

    def test_returns_correct_length(self):
        """Key has expected length (_new_ + 7 chars)."""
        key = generate_local_field_key()
        assert len(key) == 12  # "_new_" (5) + hash (7)

    def test_returns_unique_keys(self):
        """Multiple calls return different keys."""
        keys = {generate_local_field_key() for _ in range(10)}
        assert len(keys) == 10  # All unique


class TestIsLocalField:
    """Tests for is_local_field function."""

    def test_local_field_detected(self):
        """Field with _new_ prefix is detected as local."""
        field = {"key": "_new_abc1234", "name": "My Field"}
        assert is_local_field(field)

    def test_pipedrive_field_not_local(self):
        """Field with Pipedrive hash key is not local."""
        field = {"key": "abc123def456789012345678901234567890abcd", "name": "My Field"}
        assert not is_local_field(field)

    def test_system_field_not_local(self):
        """System field is not local."""
        field = {"key": "name", "name": "Name"}
        assert not is_local_field(field)

    def test_missing_key_not_local(self):
        """Field without key is not local."""
        field = {"name": "My Field"}
        assert not is_local_field(field)


class TestRemoveSchemaField:
    """Tests for remove_schema_field function."""

    @pytest.fixture
    def temp_datapackage(self, tmp_path):
        """Create a temporary datapackage for testing."""
        datapackage = {
            "name": "test-package",
            "resources": [
                {
                    "name": "persons",
                    "path": "persons.csv",
                    "schema": {
                        "fields": [
                            {"name": "id", "type": "integer"},
                            {"name": "name", "type": "string"},
                            {"name": "_new_abc1234", "type": "string"},
                        ],
                        "pipedrive_fields": [
                            {"key": "id", "name": "ID"},
                            {"key": "name", "name": "Name"},
                            {"key": "_new_abc1234", "name": "Custom Field"},
                        ],
                    },
                }
            ],
        }
        datapackage_path = tmp_path / "datapackage.json"
        with open(datapackage_path, "w") as f:
            json.dump(datapackage, f)

        # Create empty CSV
        csv_path = tmp_path / "persons.csv"
        csv_path.write_text("id,name,_new_abc1234\n")

        return tmp_path

    def test_removes_field_from_schema(self, temp_datapackage):
        """Field is removed from schema.fields."""
        package = load_package(temp_datapackage)

        # Verify field exists before
        resource = package.resources[0]
        field_names = [f.name for f in resource.schema.fields]
        assert "_new_abc1234" in field_names

        # Remove the field
        remove_schema_field(package, "persons", "_new_abc1234")

        # Verify field is removed
        field_names = [f.name for f in resource.schema.fields]
        assert "_new_abc1234" not in field_names
        assert "id" in field_names
        assert "name" in field_names

    def test_no_error_for_missing_field(self, temp_datapackage):
        """No error when removing non-existent field."""
        package = load_package(temp_datapackage)
        # Should not raise
        remove_schema_field(package, "persons", "nonexistent")

    def test_no_error_for_missing_entity(self, temp_datapackage):
        """No error when entity doesn't exist."""
        package = load_package(temp_datapackage)
        # Should not raise
        remove_schema_field(package, "organizations", "_new_abc1234")


class TestLocalFieldWorkflow:
    """Integration tests for local field operations."""

    @pytest.fixture
    def temp_base(self, tmp_path):
        """Create a temporary base with datapackage and CSV."""
        datapackage = {
            "name": "test-package",
            "resources": [
                {
                    "name": "persons",
                    "path": "persons.csv",
                    "schema": {
                        "fields": [
                            {"name": "id", "type": "integer"},
                            {"name": "name", "type": "string"},
                        ],
                        "pipedrive_fields": [
                            {"key": "id", "name": "ID", "field_type": "int"},
                            {"key": "name", "name": "Name", "field_type": "varchar"},
                        ],
                    },
                }
            ],
        }
        datapackage_path = tmp_path / "datapackage.json"
        with open(datapackage_path, "w") as f:
            json.dump(datapackage, f)

        # Create CSV with data
        csv_path = tmp_path / "persons.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow(["1", "Alice"])
            writer.writerow(["2", "Bob"])

        return tmp_path

    def test_add_local_field_workflow(self, temp_base):
        """Test complete workflow for adding a local field."""
        package = load_package(temp_base)

        # Generate local key
        local_key = generate_local_field_key()
        assert is_local_field({"key": local_key})

        # Add to pipedrive_fields
        fields = get_entity_fields(package, "persons")
        new_field = {
            "key": local_key,
            "name": "My Custom Field",
            "field_type": "varchar",
            "edit_flag": True,
        }
        fields.append(new_field)
        update_entity_fields(package, "persons", fields)

        # Add to schema.fields
        add_schema_field(package, "persons", local_key, "string")

        # Save package
        save_package(package, temp_base)

        # Reload and verify
        package = load_package(temp_base)
        fields = get_entity_fields(package, "persons")
        field_keys = [f["key"] for f in fields]
        assert local_key in field_keys

        schema_field_names = [f.name for f in package.resources[0].schema.fields]
        assert local_key in schema_field_names

    def test_delete_local_field_workflow(self, temp_base):
        """Test complete workflow for deleting a local field."""
        package = load_package(temp_base)

        # First add a local field
        local_key = "_new_test123"
        fields = get_entity_fields(package, "persons")
        fields.append({
            "key": local_key,
            "name": "To Delete",
            "field_type": "varchar",
            "edit_flag": True,
        })
        update_entity_fields(package, "persons", fields)
        add_schema_field(package, "persons", local_key, "string")
        save_package(package, temp_base)

        # Add column to CSV
        records = load_records(temp_base, "persons")
        for record in records:
            record[local_key] = "test value"
        save_records(temp_base, "persons", records)

        # Now delete the field
        package = load_package(temp_base)

        # Remove from pipedrive_fields
        fields = get_entity_fields(package, "persons")
        fields = [f for f in fields if f["key"] != local_key]
        update_entity_fields(package, "persons", fields)

        # Remove from schema.fields
        remove_schema_field(package, "persons", local_key)

        # Remove from CSV
        records = load_records(temp_base, "persons")
        records = remove_field_from_records(records, local_key)
        save_records(temp_base, "persons", records)

        save_package(package, temp_base)

        # Reload and verify
        package = load_package(temp_base)
        fields = get_entity_fields(package, "persons")
        field_keys = [f["key"] for f in fields]
        assert local_key not in field_keys

        schema_field_names = [f.name for f in package.resources[0].schema.fields]
        assert local_key not in schema_field_names

        records = load_records(temp_base, "persons")
        for record in records:
            assert local_key not in record


class TestRenameFieldKey:
    """Tests for rename_field_key function."""

    @pytest.fixture
    def temp_base(self, tmp_path):
        """Create a temporary base with local field."""
        datapackage = {
            "name": "test-package",
            "resources": [
                {
                    "name": "persons",
                    "path": "persons.csv",
                    "schema": {
                        "fields": [
                            {"name": "id", "type": "integer"},
                            {"name": "_new_abc1234", "type": "string"},
                        ],
                        "pipedrive_fields": [
                            {"key": "id", "name": "ID"},
                            {"key": "_new_abc1234", "name": "My Field"},
                        ],
                    },
                }
            ],
        }
        datapackage_path = tmp_path / "datapackage.json"
        with open(datapackage_path, "w") as f:
            json.dump(datapackage, f)

        csv_path = tmp_path / "persons.csv"
        csv_path.write_text("id,_new_abc1234\n1,value1\n2,value2\n")

        return tmp_path

    def test_renames_in_pipedrive_fields(self, temp_base):
        """Key is renamed in pipedrive_fields."""
        package = load_package(temp_base)
        rename_field_key(package, "persons", "_new_abc1234", "real_key_hash123")
        save_package(package, temp_base)

        package = load_package(temp_base)
        fields = get_entity_fields(package, "persons")
        keys = [f["key"] for f in fields]
        assert "real_key_hash123" in keys
        assert "_new_abc1234" not in keys

    def test_renames_in_schema_fields(self, temp_base):
        """Key is renamed in schema.fields."""
        package = load_package(temp_base)
        rename_field_key(package, "persons", "_new_abc1234", "real_key_hash123")
        save_package(package, temp_base)

        package = load_package(temp_base)
        schema_names = [f.name for f in package.resources[0].schema.fields]
        assert "real_key_hash123" in schema_names
        assert "_new_abc1234" not in schema_names


class TestRenameCsvColumn:
    """Tests for rename_csv_column function."""

    @pytest.fixture
    def temp_base(self, tmp_path):
        """Create a temporary base with CSV."""
        csv_path = tmp_path / "persons.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "_new_abc1234"])
            writer.writerow(["1", "value1"])
            writer.writerow(["2", "value2"])
        return tmp_path

    def test_renames_csv_column(self, temp_base):
        """Column is renamed in CSV."""
        rename_csv_column(temp_base, "persons", "_new_abc1234", "real_key_hash123")

        records = load_records(temp_base, "persons")
        assert len(records) == 2
        for record in records:
            assert "real_key_hash123" in record
            assert "_new_abc1234" not in record

    def test_preserves_values(self, temp_base):
        """Values are preserved after rename."""
        rename_csv_column(temp_base, "persons", "_new_abc1234", "real_key_hash123")

        records = load_records(temp_base, "persons")
        assert records[0]["real_key_hash123"] == "value1"
        assert records[1]["real_key_hash123"] == "value2"
