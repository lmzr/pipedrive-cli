"""Integrity tests for value update command."""

from pathlib import Path

from click.testing import CliRunner

from pipedrive_cli.cli import main
from tests.integrity.helpers import (
    assert_csv_values_changed,
    assert_csv_values_unchanged,
    assert_row_count_unchanged,
    assert_state_unchanged,
    capture_state,
)


class TestValueUpdateIntegrity:
    """Integrity tests for `value update` command."""

    def test_update_changes_csv_values(self, test_datapackage: Path):
        """value update modifies CSV values according to expression."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-s", "name=upper(name)",
            ],
        )
        assert result.exit_code == 0

        after = capture_state(test_datapackage)

        # Values should have changed
        assert_csv_values_changed(before, after, "persons", "name")

        # Verify uppercase transformation
        for row in after.csv_data["persons"]:
            name = row.get("name", "")
            if name:
                assert name == name.upper(), f"Name not uppercased: {name}"

    def test_update_preserves_row_count(self, test_datapackage: Path):
        """value update does not change the number of rows."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-s", "name=upper(name)",
            ],
        )

        after = capture_state(test_datapackage)
        assert_row_count_unchanged(before, after, "persons")

    def test_update_preserves_columns(self, test_datapackage: Path):
        """value update does not add or remove columns."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-s", "name=upper(name)",
            ],
        )

        after = capture_state(test_datapackage)
        assert before.csv_columns == after.csv_columns

    def test_update_preserves_schema(self, test_datapackage: Path):
        """value update does not modify schema.fields or pipedrive_fields."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-s", "name=upper(name)",
            ],
        )

        after = capture_state(test_datapackage)
        assert before.schema_fields == after.schema_fields
        assert before.pipedrive_fields == after.pipedrive_fields

    def test_update_preserves_other_fields(self, test_datapackage: Path):
        """value update does not modify unrelated fields."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-s", "name=upper(name)",
            ],
        )

        after = capture_state(test_datapackage)

        # Other fields should be unchanged
        assert_csv_values_unchanged(before, after, "persons", "email")
        assert_csv_values_unchanged(before, after, "persons", "phone")

    def test_update_with_filter_only_changes_matching_rows(
        self, test_datapackage: Path
    ):
        """value update with filter only modifies matching rows."""
        before = capture_state(test_datapackage)

        # Find rows with non-empty phone
        rows_with_phone = [
            i for i, row in enumerate(before.csv_data["persons"])
            if row.get("phone")
        ]

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", "notnull(phone)",
                "-s", "name=upper(name)",
            ],
        )
        assert result.exit_code == 0

        after = capture_state(test_datapackage)

        # Only rows with phone should be modified
        for i, (before_row, after_row) in enumerate(
            zip(before.csv_data["persons"], after.csv_data["persons"])
        ):
            if i in rows_with_phone:
                # Should be uppercased
                assert after_row["name"] == before_row["name"].upper()
            else:
                # Should be unchanged
                assert after_row["name"] == before_row["name"]

    def test_update_dry_run_no_change(self, test_datapackage: Path):
        """value update with --dry-run makes no changes."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-s", "name=upper(name)",
                "-n",  # dry-run
            ],
        )
        assert result.exit_code == 0

        after = capture_state(test_datapackage)
        assert_state_unchanged(before, after, "Dry-run should not change anything")

    def test_update_multiple_assignments(self, test_datapackage: Path):
        """value update with multiple -s options updates all fields."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-s", "name=upper(name)",
                "-s", "email=upper(email)",  # upper() will change emails
            ],
        )
        assert result.exit_code == 0

        after = capture_state(test_datapackage)

        # Both fields should be changed
        assert_csv_values_changed(before, after, "persons", "name")
        assert_csv_values_changed(before, after, "persons", "email")

        # Verify transformations
        for before_row, after_row in zip(
            before.csv_data["persons"], after.csv_data["persons"]
        ):
            if before_row.get("name"):
                assert after_row["name"] == before_row["name"].upper()
            if before_row.get("email"):
                assert after_row["email"] == before_row["email"].upper()


class TestValueUpdateErrorCases:
    """Error case tests for value update command."""

    def test_invalid_expression_no_change(self, test_datapackage: Path):
        """value update with invalid expression makes no changes."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-s", "name=invalid_function(name)",
            ],
        )

        # Should fail
        assert result.exit_code != 0

        after = capture_state(test_datapackage)
        assert_state_unchanged(before, after)

    def test_invalid_filter_no_change(self, test_datapackage: Path):
        """value update with invalid filter makes no changes."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", "invalid_filter_syntax[[[",
                "-s", "name=upper(name)",
            ],
        )

        # Should fail
        assert result.exit_code != 0

        after = capture_state(test_datapackage)
        assert_state_unchanged(before, after)

    def test_nonexistent_field_in_expression_no_change(self, test_datapackage: Path):
        """value update referencing nonexistent field makes no changes."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-s", "name=upper(nonexistent_field)",
            ],
        )

        # Should fail or warn
        after = capture_state(test_datapackage)
        # Either exit code != 0 or no changes
        if result.exit_code == 0:
            assert_state_unchanged(before, after)

    def test_invalid_entity_no_change(self, test_datapackage: Path):
        """value update with invalid entity makes no changes."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "update",
                "-e", "nonexistent_entity",
                "-b", str(test_datapackage),
                "-s", "name=upper(name)",
            ],
        )

        assert result.exit_code != 0

        after = capture_state(test_datapackage)
        assert_state_unchanged(before, after)

    def test_no_matching_rows_no_change(self, test_datapackage: Path):
        """value update with filter matching no rows makes no changes to data."""
        before = capture_state(test_datapackage)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "update",
                "-e", "persons",
                "-b", str(test_datapackage),
                "-f", "name == 'IMPOSSIBLE_VALUE_12345'",
                "-s", "name=upper(name)",
            ],
        )

        # Command succeeds but no changes
        assert result.exit_code == 0

        after = capture_state(test_datapackage)
        # CSV content should be unchanged (no rows matched)
        assert before.csv_checksums == after.csv_checksums
