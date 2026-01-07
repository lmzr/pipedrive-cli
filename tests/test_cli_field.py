"""Tests for CLI field commands."""

import csv
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipedrive_cli.cli import main


@pytest.fixture
def temp_base_with_fields(tmp_path: Path) -> Path:
    """Create a temporary base with datapackage and CSV for field tests."""
    base_dir = tmp_path / "test-base"
    base_dir.mkdir()

    # Create datapackage.json with pipedrive_fields
    datapackage = {
        "name": "pipedrive-backup",
        "resources": [
            {
                "name": "persons",
                "path": "persons.csv",
                "schema": {
                    "fields": [
                        {"name": "id", "type": "integer"},
                        {"name": "name", "type": "string"},
                        {"name": "email", "type": "string"},
                        {"name": "custom_field_abc123", "type": "string"},
                    ],
                    "pipedrive_fields": [
                        {
                            "key": "id",
                            "name": "ID",
                            "field_type": "int",
                            "edit_flag": False,
                        },
                        {
                            "key": "name",
                            "name": "Name",
                            "field_type": "varchar",
                            "edit_flag": False,
                        },
                        {
                            "key": "email",
                            "name": "Email",
                            "field_type": "varchar",
                            "edit_flag": False,
                        },
                        {
                            "key": "custom_field_abc123",
                            "name": "Custom Field",
                            "field_type": "varchar",
                            "edit_flag": True,
                        },
                    ],
                },
            }
        ],
    }
    (base_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

    # Create persons.csv
    with open(base_dir / "persons.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "custom_field_abc123"])
        writer.writerow(["1", "Alice", "alice@example.com", "value1"])
        writer.writerow(["2", "Bob", "bob@example.com", "value2"])

    return base_dir


class TestFieldListCommand:
    """Tests for field list command."""

    def test_field_list_local(self, temp_base_with_fields: Path):
        """field list with --base lists fields from local datapackage."""
        runner = CliRunner()
        result = runner.invoke(
            main, ["field", "list", "-e", "persons", "-b", str(temp_base_with_fields)]
        )

        assert result.exit_code == 0
        assert "id" in result.output
        assert "name" in result.output
        assert "custom_field_abc123" in result.output

    def test_field_list_local_custom_only(self, temp_base_with_fields: Path):
        """field list --custom-only shows only custom fields."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["field", "list", "-e", "per", "-b", str(temp_base_with_fields), "--custom-only"],
        )

        assert result.exit_code == 0
        assert "custom_field_abc123" in result.output
        # System fields should not appear with --custom
        assert "id" not in result.output or "ID" not in result.output

    def test_field_list_invalid_entity(self, temp_base_with_fields: Path):
        """field list with invalid entity shows error."""
        runner = CliRunner()
        result = runner.invoke(
            main, ["field", "list", "-e", "invalid", "-b", str(temp_base_with_fields)]
        )

        assert result.exit_code != 0
        assert "no match" in result.output.lower() or "error" in result.output.lower()


class TestFieldCopyCommand:
    """Tests for field copy command."""

    def test_field_copy_local_dry_run(self, temp_base_with_fields: Path):
        """field copy --base --dry-run shows what would happen."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "copy",
            "-e", "persons",
            "-f", "email",
            "-t", "email_backup",
            "-b", str(temp_base_with_fields),
            "-n"  # dry-run
        ])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert "email" in result.output

    def test_field_copy_local_creates_new_field(self, temp_base_with_fields: Path):
        """field copy --base to new field creates field with local ID."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "copy",
            "-e", "persons",
            "-f", "email",
            "-t", "New Email Field",
            "-b", str(temp_base_with_fields),
            "--transform", "varchar"
        ])

        assert result.exit_code == 0
        assert "new field" in result.output.lower()

        # Verify field was created with _new_ prefix
        with open(temp_base_with_fields / "datapackage.json") as f:
            data = json.load(f)

        persons_schema = data["resources"][0]["schema"]
        pipedrive_fields = persons_schema.get("pipedrive_fields", [])

        # Find the new field
        new_field = None
        for field in pipedrive_fields:
            if field.get("name") == "New Email Field":
                new_field = field
                break

        assert new_field is not None
        assert new_field["key"].startswith("_new_")
        assert new_field["edit_flag"] is True

    def test_field_copy_local_to_existing_field(self, temp_base_with_fields: Path):
        """field copy --base to existing field copies values."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "copy",
            "-e", "persons",
            "-f", "name",
            "-t", "custom_field_abc123",
            "-b", str(temp_base_with_fields)
        ])

        assert result.exit_code == 0

        # Verify values were copied in CSV
        with open(temp_base_with_fields / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert rows[0]["custom_field_abc123"] == "Alice"
        assert rows[1]["custom_field_abc123"] == "Bob"


class TestFieldCopyExchange:
    """Tests for field copy --exchange option."""

    def test_exchange_swaps_names_local(self, temp_base_with_fields: Path):
        """field copy --exchange swaps display names in local mode."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "copy",
            "-e", "persons",
            "-f", "email",
            "-t", "custom_field_abc123",
            "-b", str(temp_base_with_fields),
            "--exchange"
        ])

        assert result.exit_code == 0
        assert "Exchanged names" in result.output

        # Verify names were swapped in pipedrive_fields
        with open(temp_base_with_fields / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])

        email_field = next((f for f in pipedrive_fields if f["key"] == "email"), None)
        custom_field = next(
            (f for f in pipedrive_fields if f["key"] == "custom_field_abc123"), None
        )

        assert email_field is not None
        assert custom_field is not None
        # After exchange: email should have custom field's old name
        assert email_field["name"] == "Custom Field"
        # custom_field should have email's old name
        assert custom_field["name"] == "Email"

    def test_exchange_dry_run_shows_swap(self, temp_base_with_fields: Path):
        """field copy --exchange -n shows what would be swapped."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "copy",
            "-e", "persons",
            "-f", "email",
            "-t", "custom_field_abc123",
            "-b", str(temp_base_with_fields),
            "--exchange",
            "-n"
        ])

        assert result.exit_code == 0
        assert "Would exchange names" in result.output
        assert "Email" in result.output
        assert "Custom Field" in result.output

    def test_exchange_with_new_field_local(self, temp_base_with_fields: Path):
        """field copy --exchange with new field swaps names after creation."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "copy",
            "-e", "persons",
            "-f", "email",
            "-t", "New Target Field",
            "-b", str(temp_base_with_fields),
            "--transform", "varchar",
            "--exchange"
        ])

        assert result.exit_code == 0
        assert "Exchanged names" in result.output

        # Verify names were swapped
        with open(temp_base_with_fields / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])

        email_field = next((f for f in pipedrive_fields if f["key"] == "email"), None)
        # New field has _new_ prefix
        new_field = next(
            (f for f in pipedrive_fields if f["key"].startswith("_new_")), None
        )

        assert email_field is not None
        assert new_field is not None
        # After exchange: email should have the new field's original name
        assert email_field["name"] == "New Target Field"
        # new field should have email's old name
        assert new_field["name"] == "Email"


class TestFieldRenameCommand:
    """Tests for field rename command."""

    def test_field_rename_local_dry_run(self, temp_base_with_fields: Path):
        """field rename --base --dry-run shows what would happen."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "rename",
            "-e", "persons",
            "-f", "custom_field_abc123",
            "-o", "Renamed Field",
            "-b", str(temp_base_with_fields),
            "-n"  # dry-run
        ])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output

    def test_field_rename_local(self, temp_base_with_fields: Path):
        """field rename --base changes display name."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "rename",
            "-e", "persons",
            "-f", "custom_field_abc123",
            "-o", "Renamed Field",
            "-b", str(temp_base_with_fields)
        ])

        assert result.exit_code == 0

        # Verify name was changed
        with open(temp_base_with_fields / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        field = next((f for f in pipedrive_fields if f["key"] == "custom_field_abc123"), None)

        assert field is not None
        assert field["name"] == "Renamed Field"

    def test_field_rename_system_field_error(self, temp_base_with_fields: Path):
        """field rename --base on system field shows error."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "rename",
            "-e", "persons",
            "-f", "name",
            "-o", "New Name",
            "-b", str(temp_base_with_fields)
        ])

        # System fields cannot be renamed
        has_error = result.exit_code != 0
        has_cannot = "cannot" in result.output.lower()
        has_system = "system" in result.output.lower()
        assert has_error or has_cannot or has_system


class TestFieldDeleteCommand:
    """Tests for field delete command."""

    def test_field_delete_local_dry_run(self, temp_base_with_fields: Path):
        """field delete --base --dry-run shows what would happen."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "delete",
            "-e", "persons",
            "-f", "custom_field_abc123",
            "-b", str(temp_base_with_fields),
            "-n"  # dry-run
        ])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output

    def test_field_delete_local_with_force(self, temp_base_with_fields: Path):
        """field delete --base --force deletes field."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "delete",
            "-e", "persons",
            "-f", "custom_field_abc123",
            "-b", str(temp_base_with_fields),
            "--force"
        ])

        assert result.exit_code == 0
        assert "removed" in result.output.lower()

        # Verify field was removed from pipedrive_fields
        with open(temp_base_with_fields / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        field_keys = [f["key"] for f in pipedrive_fields]
        assert "custom_field_abc123" not in field_keys

        # Verify field was removed from schema.fields
        schema_fields = data["resources"][0]["schema"].get("fields", [])
        schema_names = [f["name"] for f in schema_fields]
        assert "custom_field_abc123" not in schema_names

        # Verify column was removed from CSV
        with open(temp_base_with_fields / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert "custom_field_abc123" not in rows[0]

    def test_field_delete_system_field_error(self, temp_base_with_fields: Path):
        """field delete --base on system field shows error."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "delete",
            "-e", "persons",
            "-f", "name",
            "-b", str(temp_base_with_fields),
            "--force"
        ])

        # System fields cannot be deleted
        assert result.exit_code != 0
        assert "system" in result.output.lower() or "cannot" in result.output.lower()


class TestFieldHelpOptions:
    """Tests for field help options."""

    def test_field_help(self):
        """field command shows help."""
        runner = CliRunner()
        result = runner.invoke(main, ["field", "-h"])

        assert result.exit_code == 0
        assert "list" in result.output
        assert "copy" in result.output
        assert "rename" in result.output
        assert "delete" in result.output

    def test_field_copy_help(self):
        """field copy command shows help."""
        runner = CliRunner()
        result = runner.invoke(main, ["field", "copy", "-h"])

        assert result.exit_code == 0
        assert "--entity" in result.output
        assert "--from" in result.output
        assert "--to" in result.output
        assert "--base" in result.output
        assert "--transform" in result.output

    def test_field_delete_help(self):
        """field delete command shows help."""
        runner = CliRunner()
        result = runner.invoke(main, ["field", "delete", "-h"])

        assert result.exit_code == 0
        assert "--entity" in result.output
        assert "--field" in result.output
        assert "--base" in result.output
        assert "--force" in result.output


class TestStoreCommand:
    """Tests for store command (renamed from restore)."""

    def test_store_help(self):
        """store command shows help."""
        runner = CliRunner()
        result = runner.invoke(main, ["store", "-h"])

        assert result.exit_code == 0
        assert "--dry-run" in result.output
        assert "--no-update-base" in result.output

    def test_restore_alias_works(self):
        """restore (alias) command shows help."""
        runner = CliRunner()
        result = runner.invoke(main, ["restore", "-h"])

        assert result.exit_code == 0
        assert "--dry-run" in result.output
