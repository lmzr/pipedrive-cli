"""CLI integration tests for store command."""

import csv
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipedrive_cli.cli import main


@pytest.fixture
def store_datapackage(tmp_path: Path) -> Path:
    """Create a datapackage for store command tests."""
    base_dir = tmp_path / "test-base"
    base_dir.mkdir()

    # Create organizations.csv with boolean fields (CSV format)
    with open(base_dir / "organizations.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "active_flag", "visible_to"])
        writer.writerow(["1", "ACME Corp", "True", "3"])
        writer.writerow(["2", "Beta Inc", "True", "3"])
        writer.writerow(["3", "Gamma LLC", "False", "3"])

    # Create datapackage.json with pipedrive_fields
    datapackage = {
        "name": "pipedrive-backup",
        "resources": [
            {
                "name": "organizations",
                "path": "organizations.csv",
                "schema": {
                    "fields": [
                        {"name": "id", "type": "integer"},
                        {"name": "name", "type": "string"},
                        {"name": "active_flag", "type": "string"},
                        {"name": "visible_to", "type": "string"},
                    ],
                    "pipedrive_fields": [
                        {
                            "key": "id", "name": "ID",
                            "field_type": "int", "edit_flag": False,
                        },
                        {
                            "key": "name", "name": "Name",
                            "field_type": "varchar", "edit_flag": True,
                        },
                        {
                            "key": "active_flag", "name": "Active",
                            "field_type": "bool", "edit_flag": True,
                        },
                        {
                            "key": "visible_to", "name": "Visible to",
                            "field_type": "visible_to", "edit_flag": True,
                        },
                    ],
                },
            }
        ],
    }
    (base_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

    return base_dir


class TestStoreCommandHelp:
    """Tests for store command help."""

    def test_store_help(self):
        """store command shows help with -h."""
        runner = CliRunner()
        result = runner.invoke(main, ["store", "-h"])

        assert result.exit_code == 0
        assert "--dry-run" in result.output
        assert "--skip-unchanged" in result.output
        assert "--limit" in result.output
        assert "--no-update-base" in result.output

    def test_restore_alias_exists(self):
        """restore alias is available for store command."""
        runner = CliRunner()
        result = runner.invoke(main, ["restore", "-h"])

        assert result.exit_code == 0
        assert "restore" in result.output or "store" in result.output


class TestStoreDryRun:
    """Tests for store --dry-run option."""

    def test_store_dry_run_shows_entities(self, store_datapackage: Path):
        """store --dry-run shows entities without making API calls."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "store",
            str(store_datapackage),
            "--dry-run"
        ])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output


class TestStoreSkipUnchanged:
    """Tests for store --skip-unchanged option."""

    def test_skip_unchanged_help_text(self):
        """--skip-unchanged option is documented."""
        runner = CliRunner()
        result = runner.invoke(main, ["store", "-h"])

        assert result.exit_code == 0
        assert "--skip-unchanged" in result.output

    def test_skip_unchanged_in_dry_run(self, store_datapackage: Path):
        """--skip-unchanged can be combined with --dry-run."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "store",
            str(store_datapackage),
            "-e", "organizations",
            "--skip-unchanged",
            "--dry-run",
        ])

        # Should succeed in dry-run mode (no API token needed for dry-run)
        assert result.exit_code == 0
        assert "DRY RUN" in result.output


class TestStoreLimit:
    """Tests for store --limit option."""

    def test_limit_help_text(self):
        """--limit option is documented."""
        runner = CliRunner()
        result = runner.invoke(main, ["store", "-h"])

        assert result.exit_code == 0
        assert "--limit" in result.output

    def test_limit_in_dry_run(self, store_datapackage: Path):
        """--limit can be combined with --dry-run."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "store",
            str(store_datapackage),
            "-e", "organizations",
            "--limit", "2",
            "--dry-run",
        ])

        assert result.exit_code == 0
        assert "Limited to 2 records" in result.output


class TestStoreNoUpdateBase:
    """Tests for store --no-update-base option."""

    def test_no_update_base_help_text(self):
        """--no-update-base option is documented."""
        runner = CliRunner()
        result = runner.invoke(main, ["store", "-h"])

        assert result.exit_code == 0
        assert "--no-update-base" in result.output


class TestStoreEntitySelection:
    """Tests for store -e/--entities option."""

    def test_entity_selection(self, store_datapackage: Path):
        """-e option filters entities to process."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "store",
            str(store_datapackage),
            "-e", "organizations",
            "--dry-run"
        ])

        assert result.exit_code == 0
        assert "organizations" in result.output.lower()


class TestStoreLogFile:
    """Tests for store --log option."""

    def test_log_help_text(self):
        """--log option is documented."""
        runner = CliRunner()
        result = runner.invoke(main, ["store", "-h"])

        assert result.exit_code == 0
        assert "--log" in result.output or "-l" in result.output


class TestStoreValidation:
    """Tests for store command validation."""

    def test_store_missing_path_error(self):
        """store command errors without backup path."""
        runner = CliRunner()
        result = runner.invoke(main, ["store"])

        assert result.exit_code != 0
        assert "Missing argument" in result.output or "path" in result.output.lower()

    def test_store_invalid_path_error(self, tmp_path: Path):
        """store command errors with invalid path."""
        nonexistent = tmp_path / "nonexistent"

        runner = CliRunner()
        result = runner.invoke(main, ["store", str(nonexistent)])

        assert result.exit_code != 0

    def test_store_missing_datapackage_error(self, tmp_path: Path):
        """store command errors if datapackage.json is missing."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        runner = CliRunner()
        result = runner.invoke(main, ["store", str(empty_dir)])

        assert result.exit_code != 0


class TestStoreBooleanHandling:
    """Tests for boolean field handling in store command.

    Note: The core boolean normalization is tested in test_restore.py.
    These CLI tests verify the command works with boolean data.
    """

    @pytest.fixture
    def bool_datapackage(self, tmp_path: Path) -> Path:
        """Create a datapackage with boolean fields for testing."""
        base_dir = tmp_path / "bool-test"
        base_dir.mkdir()

        # CSV with Python-style boolean strings (as written by CSV module)
        with open(base_dir / "deals.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "title", "active", "deleted", "is_archived"])
            # Python True/False written to CSV become strings "True"/"False"
            writer.writerow(["1", "Big Deal", "True", "False", "False"])

        datapackage = {
            "name": "pipedrive-backup",
            "resources": [
                {
                    "name": "deals",
                    "path": "deals.csv",
                    "schema": {
                        "fields": [
                            {"name": "id", "type": "integer"},
                            {"name": "title", "type": "string"},
                            {"name": "active", "type": "string"},
                            {"name": "deleted", "type": "string"},
                            {"name": "is_archived", "type": "string"},
                        ],
                        "pipedrive_fields": [
                            {
                                "key": "id", "name": "ID",
                                "field_type": "int", "edit_flag": False,
                            },
                            {
                                "key": "title", "name": "Title",
                                "field_type": "varchar", "edit_flag": True,
                            },
                            {
                                "key": "active", "name": "Active",
                                "field_type": "bool", "edit_flag": False,
                            },
                            {
                                "key": "deleted", "name": "Deleted",
                                "field_type": "bool", "edit_flag": False,
                            },
                            {
                                "key": "is_archived", "name": "Archived",
                                "field_type": "bool", "edit_flag": True,
                            },
                        ],
                    },
                }
            ],
        }
        (base_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

        return base_dir

    def test_store_with_boolean_data_dry_run(self, bool_datapackage: Path):
        """store --dry-run works with boolean fields in datapackage."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "store",
            str(bool_datapackage),
            "-e", "deals",
            "--dry-run",
        ])

        # Dry-run should succeed with boolean data
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
