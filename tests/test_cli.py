"""Tests for CLI commands."""

from pathlib import Path

from click.testing import CliRunner

from pipedrive_cli.cli import main


class TestBackupCommand:
    """Tests for the backup command."""

    def test_backup_dry_run(self):
        """--dry-run shows entities without making API calls."""
        runner = CliRunner()
        result = runner.invoke(main, ["backup", "--dry-run"])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert "Entities to Export" in result.output
        assert "persons" in result.output
        assert "deals" in result.output

    def test_backup_dry_run_with_entities(self):
        """--dry-run with -e shows only selected entities."""
        runner = CliRunner()
        result = runner.invoke(main, ["backup", "--dry-run", "-e", "persons", "-e", "deals"])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert "persons" in result.output
        assert "deals" in result.output
        assert "Total entities:" in result.output


class TestEntitiesCommand:
    """Tests for the entities command."""

    def test_entities_list(self):
        """entities command lists all available entities."""
        runner = CliRunner()
        result = runner.invoke(main, ["entities"])

        assert result.exit_code == 0
        assert "Available Entities" in result.output
        assert "persons" in result.output
        assert "organizations" in result.output
        assert "deals" in result.output
        assert "activities" in result.output


class TestValidateCommand:
    """Tests for the validate command."""

    def test_validate_valid_package(self, temp_backup_dir: Path):
        """validate command succeeds for valid datapackage."""
        runner = CliRunner()
        result = runner.invoke(main, ["validate", str(temp_backup_dir)])

        assert result.exit_code == 0
        assert "valid" in result.output.lower()

    def test_validate_missing_file(self, tmp_path: Path):
        """validate command fails for missing datapackage.json."""
        runner = CliRunner()
        result = runner.invoke(main, ["validate", str(tmp_path)])

        assert result.exit_code != 0
        assert "not found" in result.output.lower()


class TestHelpOptions:
    """Tests for help options."""

    def test_main_help(self):
        """Main command shows help with -h."""
        runner = CliRunner()
        result = runner.invoke(main, ["-h"])

        assert result.exit_code == 0
        assert "backup" in result.output
        assert "restore" in result.output
        assert "validate" in result.output

    def test_backup_help(self):
        """backup command shows help with -h."""
        runner = CliRunner()
        result = runner.invoke(main, ["backup", "-h"])

        assert result.exit_code == 0
        assert "--output" in result.output
        assert "--dry-run" in result.output
        assert "--entities" in result.output
