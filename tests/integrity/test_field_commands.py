"""Integrity tests for field commands (delete, copy, rename)."""

from pathlib import Path

from click.testing import CliRunner

from pipedrive_cli.cli import main
from tests.integrity.helpers import (
    assert_field_added,
    assert_field_removed,
    assert_other_entities_unchanged,
    assert_row_count_unchanged,
    assert_state_unchanged,
    capture_state,
    get_pipedrive_field_metadata,
)


class TestFieldDeleteIntegrity:
    """Integrity tests for `field delete` command."""

    def test_delete_removes_from_all_locations(self, test_datapackage: Path):
        """field delete removes field from schema.fields, pipedrive_fields, and CSV."""
        before = capture_state(test_datapackage)
        field_to_delete = "abc123_custom_text"

        # Verify field exists before
        assert field_to_delete in before.csv_columns["persons"]
        assert field_to_delete in before.schema_fields["persons"]
        assert field_to_delete in before.pipedrive_fields["persons"]

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "field", "delete",
                "-e", "persons",
                "-b", str(test_datapackage),
                field_to_delete,
                "--force",
            ],
            input="y\n",
        )
        assert result.exit_code == 0

        after = capture_state(test_datapackage)
        assert_field_removed(before, after, "persons", field_to_delete)

    def test_delete_preserves_row_count(self, test_datapackage: Path):
        """field delete does not change the number of rows."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "field", "delete",
                "-e", "persons",
                "-b", str(test_datapackage),
                "abc123_custom_text",
                "--force",
            ],
            input="y\n",
        )

        after = capture_state(test_datapackage)
        assert_row_count_unchanged(before, after, "persons")

    def test_delete_preserves_other_fields(self, test_datapackage: Path):
        """field delete does not modify other fields."""
        field_to_delete = "abc123_custom_text"
        other_field = "def456_custom_number"

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "field", "delete",
                "-e", "persons",
                "-b", str(test_datapackage),
                field_to_delete,
                "--force",
            ],
            input="y\n",
        )

        after = capture_state(test_datapackage)

        # Other field should still exist
        assert other_field in after.csv_columns["persons"]
        assert other_field in after.schema_fields["persons"]
        assert other_field in after.pipedrive_fields["persons"]

    def test_delete_nonexistent_field_no_change(self, test_datapackage: Path):
        """field delete on nonexistent field makes no changes."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "field", "delete",
                "-e", "persons",
                "-b", str(test_datapackage),
                "nonexistent_field_xyz",
                "--force",
            ],
        )

        # Command should fail or warn but not change files
        after = capture_state(test_datapackage)
        assert_state_unchanged(before, after, "Nonexistent field delete changed state")

    def test_delete_with_digit_starting_key(self, test_datapackage: Path):
        """field delete works with digit-starting field keys."""
        before = capture_state(test_datapackage)
        field_key = "25da23b938af0807ec37"

        assert field_key in before.pipedrive_fields["persons"]

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "field", "delete",
                "-e", "persons",
                "-b", str(test_datapackage),
                "_25da",  # Escaped digit-starting prefix
                "--force",
            ],
            input="y\n",
        )
        assert result.exit_code == 0

        after = capture_state(test_datapackage)
        assert_field_removed(before, after, "persons", field_key)

    def test_delete_other_entities_unchanged(self, multi_entity_datapackage: Path):
        """field delete on one entity does not affect others."""
        before = capture_state(multi_entity_datapackage)

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "field", "delete",
                "-e", "persons",
                "-b", str(multi_entity_datapackage),
                "abc123_custom_text",
                "--force",
            ],
            input="y\n",
        )

        after = capture_state(multi_entity_datapackage)
        assert_other_entities_unchanged(before, after, except_entity="persons")


class TestFieldCopyIntegrity:
    """Integrity tests for `field copy` command."""

    def test_copy_adds_to_all_locations(self, test_datapackage: Path):
        """field copy adds new field to schema.fields, pipedrive_fields, and CSV."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "field", "copy",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", "email",
                "-t", "Email Copy",
            ],
        )
        assert result.exit_code == 0

        after = capture_state(test_datapackage)

        # Find the new field key (starts with _new_)
        new_keys = set(after.pipedrive_fields["persons"]) - set(
            before.pipedrive_fields["persons"]
        )
        assert len(new_keys) == 1
        new_key = new_keys.pop()
        assert new_key.startswith("_new_")

        # Verify added to all locations
        assert_field_added(before, after, "persons", new_key)

    def test_copy_preserves_row_count(self, test_datapackage: Path):
        """field copy does not change the number of rows."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "field", "copy",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", "email",
                "-t", "Email Copy",
            ],
        )

        after = capture_state(test_datapackage)
        assert_row_count_unchanged(before, after, "persons")

    def test_copy_preserves_source_field(self, test_datapackage: Path):
        """field copy does not modify the source field."""
        before = capture_state(test_datapackage)
        source_values = [row.get("email") for row in before.csv_data["persons"]]

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "field", "copy",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", "email",
                "-t", "Email Copy",
            ],
        )

        after = capture_state(test_datapackage)
        after_values = [row.get("email") for row in after.csv_data["persons"]]
        assert source_values == after_values

    def test_copy_copies_values(self, test_datapackage: Path):
        """field copy copies values from source to target."""
        before = capture_state(test_datapackage)
        source_values = [row.get("email") for row in before.csv_data["persons"]]

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "field", "copy",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", "email",
                "-t", "Email Copy",
            ],
        )

        after = capture_state(test_datapackage)

        # Find new field key
        new_keys = set(after.csv_columns["persons"]) - set(before.csv_columns["persons"])
        new_key = new_keys.pop()

        # Values should be copied
        target_values = [row.get(new_key) for row in after.csv_data["persons"]]
        assert target_values == source_values

    def test_copy_nonexistent_source_no_change(self, test_datapackage: Path):
        """field copy from nonexistent source makes no changes."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "field", "copy",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", "nonexistent_source",
                "-t", "Target Field",
            ],
        )

        # Should fail with error
        assert result.exit_code != 0

        after = capture_state(test_datapackage)
        assert_state_unchanged(before, after)

    def test_copy_creates_local_field_key(self, test_datapackage: Path):
        """field copy creates field with _new_ prefix key."""
        runner = CliRunner()
        runner.invoke(
            main,
            [
                "field", "copy",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", "email",
                "-t", "New Email Field",
            ],
        )

        # Find new field metadata
        after = capture_state(test_datapackage)
        new_keys = [k for k in after.pipedrive_fields["persons"] if k.startswith("_new_")]
        assert len(new_keys) >= 1

        # Check metadata
        new_key = new_keys[-1]  # Most recent
        metadata = get_pipedrive_field_metadata(test_datapackage, "persons", new_key)
        assert metadata is not None
        assert metadata["name"] == "New Email Field"


class TestFieldRenameIntegrity:
    """Integrity tests for `field rename` command."""

    def test_rename_changes_only_display_name(self, test_datapackage: Path):
        """field rename only changes pipedrive_fields[].name, not key."""
        before = capture_state(test_datapackage)
        field_key = "abc123_custom_text"
        old_name = "Custom Text"
        new_name = "Renamed Custom Text"

        # Verify original name
        metadata = get_pipedrive_field_metadata(test_datapackage, "persons", field_key)
        assert metadata["name"] == old_name

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "field", "rename",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", field_key,
                "-o", new_name,
            ],
        )
        assert result.exit_code == 0

        after = capture_state(test_datapackage)

        # Key should be unchanged
        assert field_key in after.pipedrive_fields["persons"]

        # Name should be changed
        metadata = get_pipedrive_field_metadata(test_datapackage, "persons", field_key)
        assert metadata["name"] == new_name

        # CSV should be unchanged
        assert before.csv_checksums == after.csv_checksums

        # schema.fields should be unchanged
        assert before.schema_fields == after.schema_fields

    def test_rename_preserves_csv(self, test_datapackage: Path):
        """field rename does not modify CSV content."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "field", "rename",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", "abc123_custom_text",
                "-o", "New Display Name",
            ],
        )

        after = capture_state(test_datapackage)

        # CSV should be identical
        assert before.csv_columns == after.csv_columns
        assert before.csv_checksums == after.csv_checksums
        assert before.csv_data == after.csv_data

    def test_rename_nonexistent_field_no_change(self, test_datapackage: Path):
        """field rename on nonexistent field makes no changes."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "field", "rename",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", "nonexistent_field",
                "-o", "New Name",
            ],
        )

        # Should fail
        assert result.exit_code != 0

        after = capture_state(test_datapackage)
        assert_state_unchanged(before, after)


class TestFieldCommandsErrorCases:
    """Error case tests for field commands."""

    def test_invalid_entity_no_change(self, test_datapackage: Path):
        """Commands with invalid entity make no changes."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "field", "delete",
                "-e", "nonexistent_entity",
                "-b", str(test_datapackage),
                "some_field",
                "--force",
            ],
        )

        assert result.exit_code != 0

        after = capture_state(test_datapackage)
        assert_state_unchanged(before, after)

    def test_invalid_base_path_error(self, tmp_path: Path):
        """Commands with invalid base path fail cleanly."""
        nonexistent = tmp_path / "nonexistent"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "field", "delete",
                "-e", "persons",
                "-b", str(nonexistent),
                "some_field",
                "--force",
            ],
        )

        assert result.exit_code != 0
        # No files should be created
        assert not nonexistent.exists()
