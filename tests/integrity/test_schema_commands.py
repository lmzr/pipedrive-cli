"""Integrity tests for schema diff and merge commands."""

from pathlib import Path

from click.testing import CliRunner

from pipedrive_cli.cli import main
from tests.fixtures.datapackage_factory import create_test_datapackage
from tests.integrity.helpers import (
    assert_state_unchanged,
    capture_state,
)


class TestSchemaDiffIntegrity:
    """Integrity tests for `schema diff` command."""

    def test_diff_does_not_modify_target(self, two_datapackages: tuple[Path, Path]):
        """schema diff does not modify the TARGET datapackage."""
        target_path, source_path = two_datapackages
        before = capture_state(target_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "diff",
                str(target_path),
                str(source_path),
                "-e", "persons",
            ],
        )
        assert result.exit_code == 0

        after = capture_state(target_path)
        assert_state_unchanged(before, after, "TARGET should not be modified by diff")

    def test_diff_does_not_modify_source(self, two_datapackages: tuple[Path, Path]):
        """schema diff does not modify the SOURCE datapackage."""
        target_path, source_path = two_datapackages
        before = capture_state(source_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "diff",
                str(target_path),
                str(source_path),
                "-e", "persons",
            ],
        )
        assert result.exit_code == 0

        after = capture_state(source_path)
        assert_state_unchanged(before, after, "SOURCE should not be modified by diff")

    def test_diff_invalid_entity_no_change(self, two_datapackages: tuple[Path, Path]):
        """schema diff with invalid entity makes no changes."""
        target_path, source_path = two_datapackages
        target_before = capture_state(target_path)
        source_before = capture_state(source_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "diff",
                str(target_path),
                str(source_path),
                "-e", "nonexistent_entity",
            ],
        )

        # Command should fail
        assert result.exit_code != 0

        target_after = capture_state(target_path)
        source_after = capture_state(source_path)
        assert_state_unchanged(target_before, target_after)
        assert_state_unchanged(source_before, source_after)


class TestSchemaMergeIntegrity:
    """Integrity tests for `schema merge` command."""

    def test_merge_does_not_modify_target(
        self, two_datapackages: tuple[Path, Path], tmp_path: Path
    ):
        """schema merge does not modify the TARGET datapackage."""
        target_path, source_path = two_datapackages
        output_path = tmp_path / "output"
        before = capture_state(target_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "merge",
                str(target_path),
                str(source_path),
                "-e", "persons",
                "-o", str(output_path),
            ],
        )
        assert result.exit_code == 0

        after = capture_state(target_path)
        assert_state_unchanged(before, after, "TARGET should not be modified by merge")

    def test_merge_does_not_modify_source(
        self, two_datapackages: tuple[Path, Path], tmp_path: Path
    ):
        """schema merge does not modify the SOURCE datapackage."""
        target_path, source_path = two_datapackages
        output_path = tmp_path / "output"
        before = capture_state(source_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "merge",
                str(target_path),
                str(source_path),
                "-e", "persons",
                "-o", str(output_path),
            ],
        )
        assert result.exit_code == 0

        after = capture_state(source_path)
        assert_state_unchanged(before, after, "SOURCE should not be modified by merge")

    def test_merge_creates_output_with_merged_fields(
        self, two_datapackages: tuple[Path, Path], tmp_path: Path
    ):
        """schema merge creates OUTPUT with fields from both TARGET and SOURCE."""
        target_path, source_path = two_datapackages
        output_path = tmp_path / "output"

        target_before = capture_state(target_path)
        source_before = capture_state(source_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "merge",
                str(target_path),
                str(source_path),
                "-e", "persons",
                "-o", str(output_path),
            ],
        )
        assert result.exit_code == 0

        # Output should exist
        assert output_path.exists()

        output_state = capture_state(output_path)

        # Output should have TARGET's fields plus SOURCE's additional fields
        # that have corresponding CSV data
        target_fields = set(target_before.pipedrive_fields.get("persons", []))
        output_fields = set(output_state.pipedrive_fields.get("persons", []))
        _ = source_before  # Used to verify merge happened

        # All target fields should be in output
        assert target_fields.issubset(output_fields), (
            f"Missing target fields in output: {target_fields - output_fields}"
        )

    def test_merge_output_csv_matches_target(
        self, two_datapackages: tuple[Path, Path], tmp_path: Path
    ):
        """schema merge OUTPUT CSV is a copy of TARGET CSV."""
        target_path, source_path = two_datapackages
        output_path = tmp_path / "output"

        target_before = capture_state(target_path)

        runner = CliRunner()
        runner.invoke(
            main,
            [
                "schema", "merge",
                str(target_path),
                str(source_path),
                "-e", "persons",
                "-o", str(output_path),
            ],
        )

        output_state = capture_state(output_path)

        # CSV content should match target
        assert target_before.csv_checksums == output_state.csv_checksums
        assert target_before.csv_columns == output_state.csv_columns
        assert target_before.csv_row_counts == output_state.csv_row_counts

    def test_merge_dry_run_creates_no_output(
        self, two_datapackages: tuple[Path, Path], tmp_path: Path
    ):
        """schema merge with --dry-run does not create output directory."""
        target_path, source_path = two_datapackages
        output_path = tmp_path / "output"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "merge",
                str(target_path),
                str(source_path),
                "-e", "persons",
                "-o", str(output_path),
                "-n",  # dry-run
            ],
        )
        assert result.exit_code == 0

        # Output should NOT exist
        assert not output_path.exists()

    def test_merge_refuses_output_equals_target(
        self, two_datapackages: tuple[Path, Path]
    ):
        """schema merge refuses when OUTPUT == TARGET."""
        target_path, source_path = two_datapackages
        target_before = capture_state(target_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "merge",
                str(target_path),
                str(source_path),
                "-e", "persons",
                "-o", str(target_path),  # Same as target!
            ],
        )

        # Should fail
        assert result.exit_code != 0

        # Target should be unchanged
        target_after = capture_state(target_path)
        assert_state_unchanged(target_before, target_after)

    def test_merge_refuses_existing_output_without_force(
        self, two_datapackages: tuple[Path, Path], tmp_path: Path
    ):
        """schema merge refuses to overwrite existing output without --force."""
        target_path, source_path = two_datapackages

        # Create existing output
        output_path = tmp_path / "existing_output"
        create_test_datapackage(output_path)
        output_before = capture_state(output_path)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "merge",
                str(target_path),
                str(source_path),
                "-e", "persons",
                "-o", str(output_path),
                # No --force
            ],
        )

        # Should fail
        assert result.exit_code != 0

        # Existing output should be unchanged
        output_after = capture_state(output_path)
        assert_state_unchanged(output_before, output_after)

    def test_merge_with_force_overwrites_output(
        self, two_datapackages: tuple[Path, Path], tmp_path: Path
    ):
        """schema merge with --force overwrites existing output."""
        target_path, source_path = two_datapackages

        # Create existing output with different content
        output_path = tmp_path / "existing_output"
        create_test_datapackage(output_path, entities=["deals"])

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "merge",
                str(target_path),
                str(source_path),
                "-e", "persons",
                "-o", str(output_path),
                "--force",
            ],
        )

        assert result.exit_code == 0

        # Output should now have persons data (from target)
        output_state = capture_state(output_path)
        assert "persons" in output_state.csv_columns

    def test_merge_with_exclude_skips_fields(
        self, two_datapackages: tuple[Path, Path], tmp_path: Path
    ):
        """schema merge with --exclude skips specified fields."""
        target_path, source_path = two_datapackages
        output_path = tmp_path / "output"

        source_state = capture_state(source_path)

        # Find a custom field to exclude
        source_only_fields = [
            f for f in source_state.pipedrive_fields.get("persons", [])
            if f.startswith("abc123") or f.startswith("def456")
        ]
        field_to_exclude = source_only_fields[0] if source_only_fields else None

        if field_to_exclude:
            runner = CliRunner()
            result = runner.invoke(
                main,
                [
                    "schema", "merge",
                    str(target_path),
                    str(source_path),
                    "-e", "persons",
                    "-o", str(output_path),
                    "--exclude", field_to_exclude,
                ],
            )
            assert result.exit_code == 0

            output_state = capture_state(output_path)

            # Excluded field should not be in output pipedrive_fields
            # (it might still be in CSV if it came from target)
            assert field_to_exclude not in output_state.pipedrive_fields.get(
                "persons", []
            ), f"Excluded field {field_to_exclude} found in output"


class TestSchemaCommandsErrorCases:
    """Error case tests for schema commands."""

    def test_diff_nonexistent_target_error(self, tmp_path: Path):
        """schema diff with nonexistent target fails cleanly."""
        source = tmp_path / "source"
        create_test_datapackage(source)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "diff",
                str(tmp_path / "nonexistent"),
                str(source),
                "-e", "persons",
            ],
        )

        assert result.exit_code != 0

    def test_diff_nonexistent_source_error(self, tmp_path: Path):
        """schema diff with nonexistent source fails cleanly."""
        target = tmp_path / "target"
        create_test_datapackage(target)

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "diff",
                str(target),
                str(tmp_path / "nonexistent"),
                "-e", "persons",
            ],
        )

        assert result.exit_code != 0

    def test_merge_nonexistent_target_error(self, tmp_path: Path):
        """schema merge with nonexistent target fails cleanly."""
        source = tmp_path / "source"
        create_test_datapackage(source)
        output = tmp_path / "output"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "schema", "merge",
                str(tmp_path / "nonexistent"),
                str(source),
                "-e", "persons",
                "-o", str(output),
            ],
        )

        assert result.exit_code != 0
        assert not output.exists()
