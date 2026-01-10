"""Tests for field options commands (list, add, remove, sync)."""

import csv
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipedrive_cli.cli import main


@pytest.fixture
def temp_base_with_enum_field(tmp_path: Path) -> Path:
    """Create a temporary base with datapackage and CSV including an enum field."""
    base_dir = tmp_path / "test-base"
    base_dir.mkdir()

    # Create datapackage.json with an enum field
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
                        {"name": "status_field", "type": "string"},
                        {"name": "tags_field", "type": "array"},
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
                            "key": "status_field",
                            "name": "Status",
                            "field_type": "enum",
                            "edit_flag": True,
                            "options": [
                                {"id": 1, "label": "Active"},
                                {"id": 2, "label": "Inactive"},
                                {"id": 3, "label": "Pending"},
                            ],
                        },
                        {
                            "key": "tags_field",
                            "name": "Tags",
                            "field_type": "set",
                            "edit_flag": True,
                            "options": [
                                {"id": 1, "label": "VIP"},
                                {"id": 2, "label": "Premium"},
                            ],
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
        writer.writerow(["id", "name", "status_field", "tags_field"])
        writer.writerow(["1", "Alice", "Active", "VIP"])
        writer.writerow(["2", "Bob", "Inactive", "Premium"])
        writer.writerow(["3", "Charlie", "Active", "VIP,Premium"])
        writer.writerow(["4", "Dave", "", ""])

    return base_dir


class TestFieldOptionsList:
    """Tests for field options list command."""

    def test_options_list_shows_options(self, temp_base_with_enum_field: Path):
        """field options list shows all options for enum field."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "list",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field"
        ])

        assert result.exit_code == 0
        assert "Active" in result.output
        assert "Inactive" in result.output
        assert "Pending" in result.output

    def test_options_list_with_show_usage(self, temp_base_with_enum_field: Path):
        """field options list --show-usage shows usage counts."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "list",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field",
            "--show-usage"
        ])

        assert result.exit_code == 0
        assert "Active" in result.output
        # Usage column should be visible
        assert "2" in result.output  # Active is used twice

    def test_options_list_set_field(self, temp_base_with_enum_field: Path):
        """field options list works for set type fields."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "list",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "tags_field"
        ])

        assert result.exit_code == 0
        assert "VIP" in result.output
        assert "Premium" in result.output

    def test_options_list_non_enum_field_error(self, temp_base_with_enum_field: Path):
        """field options list on non-enum field shows error."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "list",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "name"
        ])

        assert result.exit_code != 0
        assert "enum" in result.output.lower() or "set" in result.output.lower()

    def test_options_list_help(self):
        """field options list command shows help."""
        runner = CliRunner()
        result = runner.invoke(main, ["field", "options", "list", "-h"])

        assert result.exit_code == 0
        assert "--entity" in result.output
        assert "--base" in result.output
        assert "--field" in result.output


class TestFieldOptionsAdd:
    """Tests for field options add command."""

    def test_options_add_single_option(self, temp_base_with_enum_field: Path):
        """field options add adds a single option."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "add",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field",
            "Archived"
        ])

        assert result.exit_code == 0
        assert "added" in result.output.lower()

        # Verify option was added
        with open(temp_base_with_enum_field / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        status_field = next(
            (f for f in pipedrive_fields if f["key"] == "status_field"), None
        )

        assert status_field is not None
        labels = [opt["label"] for opt in status_field["options"]]
        assert "Archived" in labels
        assert len(status_field["options"]) == 4

    def test_options_add_multiple_options(self, temp_base_with_enum_field: Path):
        """field options add adds multiple options at once."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "add",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field",
            "Archived", "Suspended", "Deleted"
        ])

        assert result.exit_code == 0

        # Verify options were added
        with open(temp_base_with_enum_field / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        status_field = next(
            (f for f in pipedrive_fields if f["key"] == "status_field"), None
        )

        labels = [opt["label"] for opt in status_field["options"]]
        assert "Archived" in labels
        assert "Suspended" in labels
        assert "Deleted" in labels
        assert len(status_field["options"]) == 6

    def test_options_add_dry_run(self, temp_base_with_enum_field: Path):
        """field options add --dry-run shows what would happen."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "add",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field",
            "Archived",
            "-n"
        ])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output

        # Verify option was NOT added
        with open(temp_base_with_enum_field / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        status_field = next(
            (f for f in pipedrive_fields if f["key"] == "status_field"), None
        )

        labels = [opt["label"] for opt in status_field["options"]]
        assert "Archived" not in labels

    def test_options_add_duplicate_error(self, temp_base_with_enum_field: Path):
        """field options add with existing label shows error."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "add",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field",
            "Active"  # Already exists
        ])

        assert result.exit_code != 0
        assert "already exist" in result.output.lower() or "duplicate" in result.output.lower()

    def test_options_add_generates_sequential_ids(self, temp_base_with_enum_field: Path):
        """field options add generates sequential IDs."""
        runner = CliRunner()
        runner.invoke(main, [
            "field", "options", "add",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field",
            "Archived"
        ])

        with open(temp_base_with_enum_field / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        status_field = next(
            (f for f in pipedrive_fields if f["key"] == "status_field"), None
        )

        # New option should have ID = max existing + 1 = 4
        new_option = next(
            (opt for opt in status_field["options"] if opt["label"] == "Archived"), None
        )
        assert new_option is not None
        assert new_option["id"] == 4


class TestFieldOptionsRemove:
    """Tests for field options remove command."""

    def test_options_remove_unused_option(self, temp_base_with_enum_field: Path):
        """field options remove removes unused option."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "remove",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field",
            "Pending"  # Not used by any record
        ])

        assert result.exit_code == 0
        assert "removed" in result.output.lower()

        # Verify option was removed
        with open(temp_base_with_enum_field / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        status_field = next(
            (f for f in pipedrive_fields if f["key"] == "status_field"), None
        )

        labels = [opt["label"] for opt in status_field["options"]]
        assert "Pending" not in labels

    def test_options_remove_used_option_error(self, temp_base_with_enum_field: Path):
        """field options remove on used option shows error without --force."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "remove",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field",
            "Active"  # Used by 2 records
        ])

        assert result.exit_code != 0
        assert "in use" in result.output.lower() or "used" in result.output.lower()

    def test_options_remove_used_option_with_force(self, temp_base_with_enum_field: Path):
        """field options remove --force removes used option."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "remove",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field",
            "Active",
            "--force"
        ])

        assert result.exit_code == 0
        assert "removed" in result.output.lower()

        # Verify option was removed
        with open(temp_base_with_enum_field / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        status_field = next(
            (f for f in pipedrive_fields if f["key"] == "status_field"), None
        )

        labels = [opt["label"] for opt in status_field["options"]]
        assert "Active" not in labels

    def test_options_remove_dry_run(self, temp_base_with_enum_field: Path):
        """field options remove --dry-run shows what would happen."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "remove",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field",
            "Pending",
            "-n"
        ])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output

        # Verify option was NOT removed
        with open(temp_base_with_enum_field / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        status_field = next(
            (f for f in pipedrive_fields if f["key"] == "status_field"), None
        )

        labels = [opt["label"] for opt in status_field["options"]]
        assert "Pending" in labels

    def test_options_remove_nonexistent_error(self, temp_base_with_enum_field: Path):
        """field options remove with nonexistent option shows error."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "remove",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "status_field",
            "NonExistent"
        ])

        assert result.exit_code != 0
        assert "not found" in result.output.lower()


class TestFieldOptionsSync:
    """Tests for field options sync command."""

    @pytest.fixture
    def temp_base_with_missing_options(self, tmp_path: Path) -> Path:
        """Create a base where data contains values not in options."""
        base_dir = tmp_path / "test-base-missing"
        base_dir.mkdir()

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
                            {"name": "category", "type": "string"},
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
                                "key": "category",
                                "name": "Category",
                                "field_type": "enum",
                                "edit_flag": True,
                                "options": [
                                    {"id": 1, "label": "A"},
                                    {"id": 2, "label": "B"},
                                ],
                            },
                        ],
                    },
                }
            ],
        }
        (base_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

        # CSV contains values C and D not in options
        with open(base_dir / "persons.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "category"])
            writer.writerow(["1", "Alice", "A"])
            writer.writerow(["2", "Bob", "C"])  # C is missing from options
            writer.writerow(["3", "Charlie", "D"])  # D is missing
            writer.writerow(["4", "Dave", ""])

        return base_dir

    def test_options_sync_adds_missing(self, temp_base_with_missing_options: Path):
        """field options sync adds options from data."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "sync",
            "-e", "persons",
            "-b", str(temp_base_with_missing_options),
            "-f", "category"
        ])

        assert result.exit_code == 0
        assert "added" in result.output.lower() or "sync" in result.output.lower()

        # Verify options were added
        with open(temp_base_with_missing_options / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        category_field = next(
            (f for f in pipedrive_fields if f["key"] == "category"), None
        )

        labels = [opt["label"] for opt in category_field["options"]]
        assert "A" in labels
        assert "B" in labels
        assert "C" in labels
        assert "D" in labels

    def test_options_sync_reports_unused(self, temp_base_with_missing_options: Path):
        """field options sync reports unused options."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "sync",
            "-e", "persons",
            "-b", str(temp_base_with_missing_options),
            "-f", "category"
        ])

        assert result.exit_code == 0
        # B is defined but not used
        assert "B" in result.output or "unused" in result.output.lower()

    def test_options_sync_dry_run(self, temp_base_with_missing_options: Path):
        """field options sync --dry-run shows what would happen."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "sync",
            "-e", "persons",
            "-b", str(temp_base_with_missing_options),
            "-f", "category",
            "-n"
        ])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output

        # Verify options were NOT added
        with open(temp_base_with_missing_options / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        category_field = next(
            (f for f in pipedrive_fields if f["key"] == "category"), None
        )

        labels = [opt["label"] for opt in category_field["options"]]
        assert "C" not in labels
        assert "D" not in labels

    def test_options_sync_set_field(self, temp_base_with_enum_field: Path):
        """field options sync works with set type fields."""
        # Add a single value to data that's not in options
        # Note: sync treats comma-separated values as single strings to avoid ambiguity
        with open(temp_base_with_enum_field / "persons.csv", "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["5", "Eve", "Active", "NewTag"])

        runner = CliRunner()
        result = runner.invoke(main, [
            "field", "options", "sync",
            "-e", "persons",
            "-b", str(temp_base_with_enum_field),
            "-f", "tags_field"
        ])

        assert result.exit_code == 0

        # Verify NewTag was added
        with open(temp_base_with_enum_field / "datapackage.json") as f:
            data = json.load(f)

        pipedrive_fields = data["resources"][0]["schema"].get("pipedrive_fields", [])
        tags_field = next(
            (f for f in pipedrive_fields if f["key"] == "tags_field"), None
        )

        labels = [opt["label"] for opt in tags_field["options"]]
        assert "NewTag" in labels


class TestFieldOptionsHelpOptions:
    """Tests for field options help."""

    def test_field_options_help(self):
        """field options shows subcommands."""
        runner = CliRunner()
        result = runner.invoke(main, ["field", "options", "-h"])

        assert result.exit_code == 0
        assert "list" in result.output
        assert "add" in result.output
        assert "remove" in result.output
        assert "sync" in result.output
