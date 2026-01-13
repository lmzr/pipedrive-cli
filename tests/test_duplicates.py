"""Tests for duplicate detection command and module."""

import csv
import json
from io import StringIO
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipedrive_cli.cli import main
from pipedrive_cli.duplicates import (
    DuplicateGroup,
    DuplicateStats,
    find_duplicates,
    format_duplicate_csv,
    format_duplicate_json,
)

# =============================================================================
# Unit Tests for find_duplicates()
# =============================================================================


class TestFindDuplicates:
    """Tests for the find_duplicates function."""

    def test_find_duplicates_single_key(self):
        """Test basic duplicate detection with a single key field."""
        records = [
            {"id": 1, "email": "a@example.com", "name": "Alice"},
            {"id": 2, "email": "b@example.com", "name": "Bob"},
            {"id": 3, "email": "a@example.com", "name": "Alice 2"},
            {"id": 4, "email": "c@example.com", "name": "Charlie"},
            {"id": 5, "email": "b@example.com", "name": "Bob 2"},
        ]

        groups, stats = find_duplicates(records, ["email"])

        assert stats.total_records == 5
        assert stats.unique_keys == 3
        assert stats.duplicate_groups == 2
        assert stats.total_duplicates == 4  # 2 + 2 records in duplicate groups

        # Groups should be sorted by size (largest first)
        assert len(groups) == 2
        assert groups[0].count == 2
        assert groups[1].count == 2

    def test_find_duplicates_multi_key(self):
        """Test duplicate detection with composite key."""
        records = [
            {"id": 1, "first_name": "John", "last_name": "Doe", "email": "john1@example.com"},
            {"id": 2, "first_name": "John", "last_name": "Smith", "email": "john2@example.com"},
            {"id": 3, "first_name": "John", "last_name": "Doe", "email": "john3@example.com"},
            {"id": 4, "first_name": "Jane", "last_name": "Doe", "email": "jane@example.com"},
        ]

        groups, stats = find_duplicates(records, ["first_name", "last_name"])

        assert stats.duplicate_groups == 1
        assert stats.total_duplicates == 2
        assert len(groups) == 1
        assert groups[0].key_values == ("John", "Doe")
        assert groups[0].count == 2

    def test_find_duplicates_no_duplicates(self):
        """Test when no duplicates exist."""
        records = [
            {"id": 1, "email": "a@example.com"},
            {"id": 2, "email": "b@example.com"},
            {"id": 3, "email": "c@example.com"},
        ]

        groups, stats = find_duplicates(records, ["email"])

        assert stats.duplicate_groups == 0
        assert stats.total_duplicates == 0
        assert len(groups) == 0

    def test_find_duplicates_all_duplicates(self):
        """Test when all records have the same key."""
        records = [
            {"id": 1, "email": "same@example.com", "name": "A"},
            {"id": 2, "email": "same@example.com", "name": "B"},
            {"id": 3, "email": "same@example.com", "name": "C"},
        ]

        groups, stats = find_duplicates(records, ["email"])

        assert stats.duplicate_groups == 1
        assert stats.total_duplicates == 3
        assert len(groups) == 1
        assert groups[0].count == 3

    def test_find_duplicates_excludes_nulls_by_default(self):
        """Test that records with null key values are excluded by default."""
        records = [
            {"id": 1, "email": "a@example.com"},
            {"id": 2, "email": None},
            {"id": 3, "email": ""},
            {"id": 4, "email": None},
            {"id": 5, "email": "a@example.com"},
        ]

        groups, stats = find_duplicates(records, ["email"], include_nulls=False)

        # Only the email=a@example.com records should be considered
        assert stats.duplicate_groups == 1
        assert len(groups) == 1
        assert groups[0].count == 2

    def test_find_duplicates_includes_nulls_when_requested(self):
        """Test that records with null key values are included when requested."""
        records = [
            {"id": 1, "email": "a@example.com"},
            {"id": 2, "email": None},
            {"id": 3, "email": ""},
            {"id": 4, "email": None},
            {"id": 5, "email": "a@example.com"},
        ]

        groups, stats = find_duplicates(records, ["email"], include_nulls=True)

        # Both email=a@example.com and email=null groups
        assert stats.duplicate_groups == 2

    def test_find_duplicates_complex_values_email_array(self):
        """Test with Pipedrive email array format."""
        records = [
            {"id": 1, "email": [{"value": "a@example.com", "primary": True}]},
            {"id": 2, "email": [{"value": "b@example.com", "primary": True}]},
            {"id": 3, "email": [{"value": "a@example.com", "primary": True}]},
        ]

        groups, stats = find_duplicates(records, ["email"])

        assert stats.duplicate_groups == 1
        assert groups[0].count == 2
        assert groups[0].key_values == ("a@example.com",)

    def test_find_duplicates_complex_values_reference_object(self):
        """Test with Pipedrive reference object format."""
        records = [
            {"id": 1, "org_id": {"value": 100, "name": "ACME"}},
            {"id": 2, "org_id": {"value": 200, "name": "Other"}},
            {"id": 3, "org_id": {"value": 100, "name": "ACME Corp"}},
        ]

        groups, stats = find_duplicates(records, ["org_id"])

        assert stats.duplicate_groups == 1
        assert groups[0].count == 2
        assert groups[0].key_values == ("100",)

    def test_find_duplicates_sorted_by_size(self):
        """Test that groups are sorted by size, largest first."""
        records = [
            {"id": 1, "email": "small@example.com"},
            {"id": 2, "email": "small@example.com"},
            {"id": 3, "email": "large@example.com"},
            {"id": 4, "email": "large@example.com"},
            {"id": 5, "email": "large@example.com"},
            {"id": 6, "email": "large@example.com"},
        ]

        groups, stats = find_duplicates(records, ["email"])

        assert len(groups) == 2
        assert groups[0].count == 4  # large@example.com
        assert groups[1].count == 2  # small@example.com


# =============================================================================
# Unit Tests for DuplicateGroup
# =============================================================================


class TestDuplicateGroup:
    """Tests for the DuplicateGroup dataclass."""

    def test_key_display_single_field(self):
        """Test key display formatting with single field."""
        group = DuplicateGroup(
            key_values=("john@example.com",),
            key_fields=["email"],
            records=[{"id": 1}, {"id": 2}],
        )

        assert group.key_display() == 'email = "john@example.com"'

    def test_key_display_multi_field(self):
        """Test key display formatting with multiple fields."""
        group = DuplicateGroup(
            key_values=("John", "Doe"),
            key_fields=["first_name", "last_name"],
            records=[{"id": 1}],
        )

        assert group.key_display() == 'first_name = "John", last_name = "Doe"'

    def test_count_property(self):
        """Test the count property."""
        group = DuplicateGroup(
            key_values=("test",),
            key_fields=["field"],
            records=[{"id": 1}, {"id": 2}, {"id": 3}],
        )

        assert group.count == 3


# =============================================================================
# Unit Tests for Format Functions
# =============================================================================


class TestFormatDuplicateJson:
    """Tests for the format_duplicate_json function."""

    def test_format_json_structure(self):
        """Test JSON output structure."""
        groups = [
            DuplicateGroup(
                key_values=("a@example.com",),
                key_fields=["email"],
                records=[
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Alice 2"},
                ],
            )
        ]
        stats = DuplicateStats(
            total_records=10,
            unique_keys=9,
            duplicate_groups=1,
            total_duplicates=2,
        )

        result = format_duplicate_json(groups, stats)
        data = json.loads(result)

        assert "stats" in data
        assert data["stats"]["total_records"] == 10
        assert data["stats"]["unique_keys"] == 9
        assert data["stats"]["duplicate_groups"] == 1
        assert data["stats"]["total_duplicates"] == 2

        assert "groups" in data
        assert len(data["groups"]) == 1
        assert data["groups"][0]["key"] == {"email": "a@example.com"}
        assert data["groups"][0]["count"] == 2
        assert len(data["groups"][0]["records"]) == 2

    def test_format_json_empty_groups(self):
        """Test JSON output with no duplicates."""
        groups = []
        stats = DuplicateStats(
            total_records=5,
            unique_keys=5,
            duplicate_groups=0,
            total_duplicates=0,
        )

        result = format_duplicate_json(groups, stats)
        data = json.loads(result)

        assert data["stats"]["duplicate_groups"] == 0
        assert data["groups"] == []


class TestFormatDuplicateCsv:
    """Tests for the format_duplicate_csv function."""

    def test_format_csv_with_group_column(self):
        """Test CSV output includes _duplicate_group column."""
        groups = [
            DuplicateGroup(
                key_values=("a@example.com",),
                key_fields=["email"],
                records=[
                    {"id": 1, "email": "a@example.com", "name": "Alice"},
                    {"id": 2, "email": "a@example.com", "name": "Alice 2"},
                ],
            ),
            DuplicateGroup(
                key_values=("b@example.com",),
                key_fields=["email"],
                records=[
                    {"id": 3, "email": "b@example.com", "name": "Bob"},
                    {"id": 4, "email": "b@example.com", "name": "Bob 2"},
                ],
            ),
        ]

        result = format_duplicate_csv(groups, None)
        reader = csv.DictReader(StringIO(result))
        rows = list(reader)

        assert len(rows) == 4
        assert "_duplicate_group" in rows[0]
        assert rows[0]["_duplicate_group"] == "1"
        assert rows[1]["_duplicate_group"] == "1"
        assert rows[2]["_duplicate_group"] == "2"
        assert rows[3]["_duplicate_group"] == "2"

    def test_format_csv_empty_groups(self):
        """Test CSV output with no duplicates."""
        result = format_duplicate_csv([], None)
        assert result == ""


# =============================================================================
# CLI Integration Tests
# =============================================================================


@pytest.fixture
def temp_datapackage(tmp_path: Path) -> Path:
    """Create a temporary datapackage for duplicate tests."""
    base_dir = tmp_path / "test-base"
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
                        {"name": "email", "type": "string"},
                        {"name": "phone", "type": "string"},
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
                            "key": "email", "name": "Email",
                            "field_type": "varchar", "edit_flag": True,
                        },
                        {
                            "key": "phone", "name": "Phone",
                            "field_type": "phone", "edit_flag": True,
                        },
                    ],
                },
            }
        ],
    }
    (base_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

    with open(base_dir / "persons.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "phone"])
        writer.writerow(["1", "Alice", "alice@example.com", "123"])
        writer.writerow(["2", "Bob", "bob@example.com", "456"])
        writer.writerow(["3", "Alice Duplicate", "alice@example.com", "789"])
        writer.writerow(["4", "Charlie", "charlie@example.com", ""])
        writer.writerow(["5", "Bob Duplicate", "bob@example.com", "111"])

    return base_dir


class TestDuplicatesCommand:
    """Tests for the record duplicates CLI command."""

    def test_duplicates_local_basic(self, temp_datapackage: Path):
        """Test basic duplicate detection on local datapackage."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["record", "duplicates", "-e", "per", "-b", str(temp_datapackage), "-k", "email", "-q"],
        )

        assert result.exit_code == 0
        assert "Duplicate Groups" in result.output
        assert "2 groups" in result.output

    def test_duplicates_no_duplicates_message(self, temp_datapackage: Path):
        """Test message when no duplicates found."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            ["record", "duplicates", "-e", "per", "-b", str(temp_datapackage), "-k", "id", "-q"],
        )

        assert result.exit_code == 0
        assert "No duplicates found" in result.output

    def test_duplicates_with_filter(self, temp_datapackage: Path):
        """Test duplicate detection with filter."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "duplicates",
                "-e", "per",
                "-b", str(temp_datapackage),
                "-k", "email",
                "-f", "contains(name, 'Alice')",
                "-q",
            ],
        )

        assert result.exit_code == 0
        # Filter should narrow to only Alice records
        assert "alice@example.com" in result.output.lower()

    def test_duplicates_multi_key(self, temp_datapackage: Path):
        """Test composite key duplicate detection."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "duplicates",
                "-e", "per", "-b", str(temp_datapackage),
                "-k", "email,phone", "-q",
            ],
        )

        assert result.exit_code == 0
        # With email+phone as key, duplicates should be different

    def test_duplicates_json_output(self, temp_datapackage: Path):
        """Test JSON output format."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "duplicates",
                "-e", "per",
                "-b", str(temp_datapackage),
                "-k", "email",
                "-o", "json",
                "-q",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "stats" in data
        assert "groups" in data
        assert data["stats"]["duplicate_groups"] == 2

    def test_duplicates_csv_output(self, temp_datapackage: Path):
        """Test CSV output format."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "duplicates",
                "-e", "per",
                "-b", str(temp_datapackage),
                "-k", "email",
                "-o", "csv",
                "-q",
            ],
        )

        assert result.exit_code == 0
        assert "_duplicate_group" in result.output
        assert "alice@example.com" in result.output

    def test_duplicates_summary_only(self, temp_datapackage: Path):
        """Test summary-only mode."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "duplicates",
                "-e", "per",
                "-b", str(temp_datapackage),
                "-k", "email",
                "--summary-only",
                "-q",
            ],
        )

        assert result.exit_code == 0
        assert "Duplicate Groups" in result.output
        assert "Total records analyzed" in result.output
        # Should not show individual tables

    def test_duplicates_include_nulls(self, temp_datapackage: Path):
        """Test --include-nulls option."""
        runner = CliRunner()
        # Without include-nulls
        result1 = runner.invoke(
            main,
            [
                "record", "duplicates",
                "-e", "per", "-b", str(temp_datapackage),
                "-k", "phone", "-o", "json", "-q",
            ],
        )
        # With include-nulls
        result2 = runner.invoke(
            main,
            [
                "record", "duplicates",
                "-e", "per", "-b", str(temp_datapackage),
                "-k", "phone", "--include-nulls", "-o", "json", "-q",
            ],
        )

        assert result1.exit_code == 0
        assert result2.exit_code == 0
        # Both should return valid JSON

    def test_duplicates_dry_run(self, temp_datapackage: Path):
        """Test dry-run mode shows resolved expressions only."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "duplicates",
                "-e", "per",
                "-b", str(temp_datapackage),
                "-k", "email",
                "-f", "notnull(email)",
                "-n",
            ],
        )

        assert result.exit_code == 0
        assert "Key fields: Email" in result.output
        assert "dry-run" in result.output

    def test_duplicates_key_validation_error(self, temp_datapackage: Path):
        """Test error when key field doesn't exist."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "duplicates",
                "-e", "per", "-b", str(temp_datapackage),
                "-k", "nonexistent_field",
            ],
        )

        assert result.exit_code != 0
        assert "Unknown key field" in result.output or "Error" in result.output

    def test_duplicates_with_limit(self, temp_datapackage: Path):
        """Test --limit option."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "duplicates",
                "-e", "per",
                "-b", str(temp_datapackage),
                "-k", "email",
                "-l", "1",
                "-o", "json",
                "-q",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        # Limit applies to groups in output
        assert len(data["groups"]) <= 1

    def test_duplicates_include_fields(self, temp_datapackage: Path):
        """Test --include field selection."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "record", "duplicates",
                "-e", "per",
                "-b", str(temp_datapackage),
                "-k", "email",
                "-i", "id,email",
                "-o", "json",
                "-q",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        if data["groups"]:
            record = data["groups"][0]["records"][0]
            assert "id" in record
            assert "email" in record
            # name should be excluded
            assert "name" not in record
