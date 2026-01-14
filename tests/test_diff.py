"""Tests for diff command and module."""

import json
from pathlib import Path

from click.testing import CliRunner

from pipedrive_cli.cli import main
from pipedrive_cli.diff import (
    DiffStats,
    EntityDiff,
    FieldDiff,
    RecordDiff,
    diff_fields,
    diff_packages,
    diff_records,
    format_diff_json,
    get_computed_fields,
    normalize_value,
    parse_key_option,
)

from .fixtures.datapackage_factory import create_test_datapackage

# =============================================================================
# Unit Tests for parse_key_option()
# =============================================================================


class TestParseKeyOption:
    """Tests for parsing -k option values."""

    def test_parse_key_option_default(self):
        """Test default key when no -k provided."""
        default_key, entity_keys = parse_key_option(())

        assert default_key == "id"
        assert entity_keys == {}

    def test_parse_key_option_global(self):
        """Test global key without entity prefix."""
        default_key, entity_keys = parse_key_option(("name",))

        assert default_key == "name"
        assert entity_keys == {}

    def test_parse_key_option_per_entity(self):
        """Test per-entity key with entity:field format."""
        default_key, entity_keys = parse_key_option(("persons:email",))

        assert default_key == "id"
        assert entity_keys == {"persons": "email"}

    def test_parse_key_option_mixed(self):
        """Test mixed global and per-entity keys."""
        default_key, entity_keys = parse_key_option(
            ("name", "persons:email", "deals:title")
        )

        assert default_key == "name"
        assert entity_keys == {"persons": "email", "deals": "title"}

    def test_parse_key_option_multiple_per_entity(self):
        """Test multiple per-entity keys."""
        default_key, entity_keys = parse_key_option(
            ("persons:email", "organizations:name", "deals:title")
        )

        assert default_key == "id"
        assert entity_keys == {
            "persons": "email",
            "organizations": "name",
            "deals": "title",
        }


# =============================================================================
# Unit Tests for diff_fields()
# =============================================================================


class TestDiffFields:
    """Tests for field schema comparison."""

    def test_diff_fields_no_differences(self):
        """Test identical field lists return empty diff."""
        fields = [
            {"key": "id", "name": "ID", "field_type": "int"},
            {"key": "name", "name": "Name", "field_type": "varchar"},
        ]

        diffs = diff_fields(fields, fields)

        assert diffs == []

    def test_diff_fields_added(self):
        """Test detecting fields added in target."""
        fields1 = [{"key": "id", "name": "ID", "field_type": "int"}]
        fields2 = [
            {"key": "id", "name": "ID", "field_type": "int"},
            {"key": "name", "name": "Name", "field_type": "varchar"},
        ]

        diffs = diff_fields(fields1, fields2)

        assert len(diffs) == 1
        assert diffs[0].diff_type == "added"
        assert diffs[0].key == "name"
        assert diffs[0].name == "Name"

    def test_diff_fields_removed(self):
        """Test detecting fields removed from target."""
        fields1 = [
            {"key": "id", "name": "ID", "field_type": "int"},
            {"key": "name", "name": "Name", "field_type": "varchar"},
        ]
        fields2 = [{"key": "id", "name": "ID", "field_type": "int"}]

        diffs = diff_fields(fields1, fields2)

        assert len(diffs) == 1
        assert diffs[0].diff_type == "removed"
        assert diffs[0].key == "name"

    def test_diff_fields_type_changed(self):
        """Test detecting field type changes."""
        fields1 = [{"key": "value", "name": "Value", "field_type": "varchar"}]
        fields2 = [{"key": "value", "name": "Value", "field_type": "double"}]

        diffs = diff_fields(fields1, fields2)

        assert len(diffs) == 1
        assert diffs[0].diff_type == "type_changed"
        assert diffs[0].key == "value"
        assert diffs[0].old_value == "varchar"
        assert diffs[0].new_value == "double"

    def test_diff_fields_name_changed(self):
        """Test detecting display name changes."""
        fields1 = [{"key": "email", "name": "Email", "field_type": "varchar"}]
        fields2 = [{"key": "email", "name": "E-mail Address", "field_type": "varchar"}]

        diffs = diff_fields(fields1, fields2)

        assert len(diffs) == 1
        assert diffs[0].diff_type == "name_changed"
        assert diffs[0].old_value == "Email"
        assert diffs[0].new_value == "E-mail Address"

    def test_diff_fields_options_changed(self):
        """Test detecting enum/set option changes."""
        fields1 = [
            {
                "key": "status",
                "name": "Status",
                "field_type": "enum",
                "options": [
                    {"id": 1, "label": "Open"},
                    {"id": 2, "label": "Closed"},
                ],
            }
        ]
        fields2 = [
            {
                "key": "status",
                "name": "Status",
                "field_type": "enum",
                "options": [
                    {"id": 1, "label": "Open"},
                    {"id": 2, "label": "Closed"},
                    {"id": 3, "label": "Pending"},
                ],
            }
        ]

        diffs = diff_fields(fields1, fields2)

        assert len(diffs) == 1
        assert diffs[0].diff_type == "options_changed"
        assert diffs[0].key == "status"


# =============================================================================
# Unit Tests for diff_records()
# =============================================================================


class TestDiffRecords:
    """Tests for record data comparison."""

    def test_diff_records_no_differences(self):
        """Test identical records return empty diff."""
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        diffs = diff_records(records, records)

        assert diffs == []

    def test_diff_records_added(self):
        """Test detecting records added in target."""
        records1 = [{"id": 1, "name": "Alice"}]
        records2 = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]

        diffs = diff_records(records1, records2)

        assert len(diffs) == 1
        assert diffs[0].diff_type == "added"
        assert diffs[0].record_id == 2

    def test_diff_records_removed(self):
        """Test detecting records removed from target."""
        records1 = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        records2 = [{"id": 1, "name": "Alice"}]

        diffs = diff_records(records1, records2)

        assert len(diffs) == 1
        assert diffs[0].diff_type == "removed"
        assert diffs[0].record_id == 2

    def test_diff_records_modified_single_field(self):
        """Test detecting single field value change."""
        records1 = [{"id": 1, "name": "Alice", "email": "alice@old.com"}]
        records2 = [{"id": 1, "name": "Alice", "email": "alice@new.com"}]

        diffs = diff_records(records1, records2)

        assert len(diffs) == 1
        assert diffs[0].diff_type == "modified"
        assert diffs[0].record_id == 1
        assert "email" in diffs[0].field_changes
        assert diffs[0].field_changes["email"] == ("alice@old.com", "alice@new.com")

    def test_diff_records_modified_multiple_fields(self):
        """Test detecting multiple field value changes."""
        records1 = [{"id": 1, "name": "Alice", "email": "alice@old.com"}]
        records2 = [{"id": 1, "name": "Alice Smith", "email": "alice@new.com"}]

        diffs = diff_records(records1, records2)

        assert len(diffs) == 1
        assert len(diffs[0].field_changes) == 2
        assert "name" in diffs[0].field_changes
        assert "email" in diffs[0].field_changes

    def test_diff_records_null_equivalence(self):
        """Test None and empty string treated as equivalent."""
        records1 = [{"id": 1, "name": "Alice", "phone": None}]
        records2 = [{"id": 1, "name": "Alice", "phone": ""}]

        diffs = diff_records(records1, records2)

        assert diffs == []

    def test_diff_records_missing_key_treated_as_null(self):
        """Test missing key treated as None."""
        records1 = [{"id": 1, "name": "Alice"}]
        records2 = [{"id": 1, "name": "Alice", "phone": None}]

        diffs = diff_records(records1, records2)

        assert diffs == []

    def test_diff_records_custom_key(self):
        """Test records matched by custom key field."""
        records1 = [{"id": 1, "email": "a@test.com", "name": "Alice"}]
        records2 = [{"id": 2, "email": "a@test.com", "name": "Alice Updated"}]

        diffs = diff_records(records1, records2, key_field="email")

        assert len(diffs) == 1
        assert diffs[0].diff_type == "modified"
        assert diffs[0].record_id == "a@test.com"
        assert "id" in diffs[0].field_changes
        assert "name" in diffs[0].field_changes


class TestNormalizeValue:
    """Tests for value normalization."""

    def test_normalize_none(self):
        """Test None stays None."""
        assert normalize_value(None) is None

    def test_normalize_empty_string(self):
        """Test empty string becomes None."""
        assert normalize_value("") is None

    def test_normalize_regular_value(self):
        """Test regular values unchanged."""
        assert normalize_value("hello") == "hello"
        assert normalize_value(42) == 42

    def test_normalize_reference_object(self):
        """Test reference objects extract value."""
        ref = {"value": 123, "name": "Test Org"}
        assert normalize_value(ref) == 123

    def test_normalize_nested_dict(self):
        """Test nested dicts normalized recursively."""
        data = {"a": "", "b": None, "c": "value"}
        result = normalize_value(data)
        assert result == {"a": None, "b": None, "c": "value"}


# =============================================================================
# Unit Tests for get_computed_fields()
# =============================================================================


class TestGetComputedFields:
    """Tests for computed fields detection."""

    def test_get_computed_fields_by_edit_flag(self):
        """Test fields with edit_flag=False are detected as computed."""
        fields = [
            {"key": "id", "name": "ID", "field_type": "int", "edit_flag": False},
            {"key": "name", "name": "Name", "field_type": "varchar", "edit_flag": True},
            {"key": "add_time", "name": "Add Time", "field_type": "date", "edit_flag": False},
        ]

        computed = get_computed_fields(fields)

        assert "id" in computed
        assert "add_time" in computed
        assert "name" not in computed

    def test_get_computed_fields_by_readonly_list(self):
        """Test fields in READONLY_FIELDS are detected as computed."""
        fields = [
            {
                "key": "update_time",
                "name": "Update Time",
                "field_type": "date",
                "edit_flag": True,
            },
            {
                "key": "activities_count",
                "name": "Activities",
                "field_type": "int",
                "edit_flag": True,
            },
        ]

        computed = get_computed_fields(fields)

        assert "update_time" in computed
        assert "activities_count" in computed

    def test_get_computed_fields_empty(self):
        """Test empty field list returns READONLY_FIELDS baseline."""
        computed = get_computed_fields([])
        # Should include all known readonly fields even with empty input
        assert "id" in computed
        assert "update_time" in computed
        assert "modified" in computed
        assert "last_login" in computed

    def test_get_computed_fields_all_editable(self):
        """Test all editable fields still includes READONLY_FIELDS baseline."""
        fields = [
            {"key": "custom_field", "name": "Custom", "field_type": "varchar", "edit_flag": True},
        ]

        computed = get_computed_fields(fields)

        # Baseline readonly fields are always included
        assert "id" in computed
        assert "update_time" in computed
        # Custom editable field is NOT included
        assert "custom_field" not in computed


class TestDiffRecordsWithExcludeFields:
    """Tests for diff_records with exclude_fields parameter."""

    def test_diff_records_excludes_specified_fields(self):
        """Test excluded fields are not compared."""
        records1 = [{"id": 1, "name": "Alice", "update_time": "2024-01-01"}]
        records2 = [{"id": 1, "name": "Alice", "update_time": "2024-06-01"}]

        # Without exclusion - should detect difference
        diffs = diff_records(records1, records2)
        assert len(diffs) == 1
        assert "update_time" in diffs[0].field_changes

        # With exclusion - should not detect difference
        diffs = diff_records(records1, records2, exclude_fields={"update_time"})
        assert len(diffs) == 0

    def test_diff_records_with_multiple_exclude_fields(self):
        """Test multiple fields can be excluded."""
        records1 = [
            {"id": 1, "add_time": "2024-01-01", "update_time": "2024-01-01", "name": "Alice"}
        ]
        records2 = [
            {"id": 1, "add_time": "2024-06-01", "update_time": "2024-06-01", "name": "Alice"}
        ]

        diffs = diff_records(
            records1, records2, exclude_fields={"add_time", "update_time"}
        )

        assert len(diffs) == 0

    def test_diff_records_exclude_preserves_other_changes(self):
        """Test non-excluded field changes are still detected."""
        records1 = [{"id": 1, "name": "Alice", "update_time": "2024-01-01"}]
        records2 = [{"id": 1, "name": "Bob", "update_time": "2024-06-01"}]

        diffs = diff_records(records1, records2, exclude_fields={"update_time"})

        assert len(diffs) == 1
        assert diffs[0].diff_type == "modified"
        assert "name" in diffs[0].field_changes
        assert "update_time" not in diffs[0].field_changes


# =============================================================================
# Integration Tests for diff_packages()
# =============================================================================


class TestDiffPackages:
    """Tests for package-level diff."""

    def test_diff_identical_packages(self, tmp_path: Path):
        """Test identical packages show no differences."""
        pkg1 = create_test_datapackage(tmp_path / "pkg1", entities=["persons"])
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        diffs, stats = diff_packages(pkg1, pkg2)

        assert stats.entities_compared == 1
        assert stats.entities_with_differences == 0
        assert stats.fields_added == 0
        assert stats.fields_removed == 0
        assert stats.records_added == 0
        assert stats.records_removed == 0
        assert stats.records_modified == 0

    def test_diff_schema_only(self, tmp_path: Path):
        """Test schema-only comparison ignores data changes."""
        pkg1 = create_test_datapackage(
            tmp_path / "pkg1",
            entities=["persons"],
            extra_data={
                "persons": [{"id": "100", "name": "New Person"}],
            },
        )
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        diffs, stats = diff_packages(pkg1, pkg2, schema_only=True)

        assert stats.records_added == 0
        assert stats.records_removed == 0
        assert stats.records_modified == 0

    def test_diff_data_only(self, tmp_path: Path):
        """Test data-only comparison ignores schema changes."""
        # Create packages with different field definitions
        pkg1 = create_test_datapackage(
            tmp_path / "pkg1",
            entities=["persons"],
            extra_fields={
                "persons": [
                    {"key": "extra_field", "name": "Extra", "field_type": "varchar"}
                ]
            },
        )
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        diffs, stats = diff_packages(pkg1, pkg2, data_only=True)

        assert stats.fields_added == 0
        assert stats.fields_removed == 0
        assert stats.fields_changed == 0

    def test_diff_multi_entity(self, tmp_path: Path):
        """Test diff across multiple entities."""
        pkg1 = create_test_datapackage(
            tmp_path / "pkg1", entities=["persons", "organizations"]
        )
        pkg2 = create_test_datapackage(
            tmp_path / "pkg2", entities=["persons", "organizations"]
        )

        diffs, stats = diff_packages(pkg1, pkg2)

        assert stats.entities_compared == 2


# =============================================================================
# Tests for Output Formatting
# =============================================================================


class TestFormatDiffJson:
    """Tests for JSON output formatting."""

    def test_format_diff_json_structure(self):
        """Test JSON output has correct structure."""
        stats = DiffStats(
            entities_compared=2,
            entities_with_differences=1,
            fields_added=1,
            records_modified=2,
        )
        diffs = [
            EntityDiff(
                entity_name="persons",
                field_diffs=[
                    FieldDiff(
                        key="new_field",
                        name="New Field",
                        diff_type="added",
                        new_value={"field_type": "varchar"},
                    )
                ],
                record_diffs=[
                    RecordDiff(
                        record_id=1,
                        diff_type="modified",
                        field_changes={"name": ("Old", "New")},
                    )
                ],
            )
        ]

        output = format_diff_json(diffs, stats)
        result = json.loads(output)

        assert "stats" in result
        assert result["stats"]["entities_compared"] == 2
        assert result["stats"]["fields_added"] == 1
        assert result["stats"]["records_modified"] == 2

        assert "entities" in result
        assert len(result["entities"]) == 1
        assert result["entities"][0]["name"] == "persons"
        assert len(result["entities"][0]["field_diffs"]) == 1
        assert len(result["entities"][0]["record_diffs"]) == 1


# =============================================================================
# CLI Integration Tests
# =============================================================================


class TestDiffCommand:
    """Tests for the diff CLI command."""

    def test_diff_identical_packages(self, tmp_path: Path):
        """Test CLI output for identical packages."""
        pkg1 = create_test_datapackage(tmp_path / "pkg1", entities=["persons"])
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2)])

        assert result.exit_code == 0
        assert "No differences found" in result.output

    def test_diff_schema_only_option(self, tmp_path: Path):
        """Test --schema-only option."""
        pkg1 = create_test_datapackage(tmp_path / "pkg1", entities=["persons"])
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2), "--schema-only"])

        assert result.exit_code == 0

    def test_diff_data_only_option(self, tmp_path: Path):
        """Test --data-only option."""
        pkg1 = create_test_datapackage(tmp_path / "pkg1", entities=["persons"])
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2), "--data-only"])

        assert result.exit_code == 0

    def test_diff_entity_filter(self, tmp_path: Path):
        """Test -e option filters entities."""
        pkg1 = create_test_datapackage(
            tmp_path / "pkg1", entities=["persons", "organizations"]
        )
        pkg2 = create_test_datapackage(
            tmp_path / "pkg2", entities=["persons", "organizations"]
        )

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2), "-e", "persons"])

        assert result.exit_code == 0
        assert "organizations" not in result.output.lower()

    def test_diff_all_entities(self, tmp_path: Path):
        """Test without -e compares all common entities."""
        pkg1 = create_test_datapackage(
            tmp_path / "pkg1", entities=["persons", "organizations", "deals"]
        )
        pkg2 = create_test_datapackage(
            tmp_path / "pkg2", entities=["persons", "organizations", "deals"]
        )

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2)])

        assert result.exit_code == 0
        assert "Entities compared: 3" in result.output

    def test_diff_custom_key_global(self, tmp_path: Path):
        """Test -k with global key."""
        pkg1 = create_test_datapackage(tmp_path / "pkg1", entities=["persons"])
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2), "-k", "name"])

        assert result.exit_code == 0

    def test_diff_custom_key_per_entity(self, tmp_path: Path):
        """Test -k with per-entity keys."""
        pkg1 = create_test_datapackage(
            tmp_path / "pkg1", entities=["persons", "deals"]
        )
        pkg2 = create_test_datapackage(
            tmp_path / "pkg2", entities=["persons", "deals"]
        )

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["diff", str(pkg1), str(pkg2), "-k", "persons:email", "-k", "deals:title"],
        )

        assert result.exit_code == 0

    def test_diff_json_output(self, tmp_path: Path):
        """Test -o json output format."""
        pkg1 = create_test_datapackage(tmp_path / "pkg1", entities=["persons"])
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2), "-o", "json"])

        assert result.exit_code == 0
        output = json.loads(result.output)
        assert "stats" in output
        assert "entities" in output

    def test_diff_limit(self, tmp_path: Path):
        """Test --limit option."""
        pkg1 = create_test_datapackage(tmp_path / "pkg1", entities=["persons"])
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2), "--limit", "5"])

        assert result.exit_code == 0

    def test_diff_exit_code_no_differences(self, tmp_path: Path):
        """Test --exit-code returns 0 when no differences."""
        pkg1 = create_test_datapackage(tmp_path / "pkg1", entities=["persons"])
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2), "--exit-code"])

        assert result.exit_code == 0

    def test_diff_mutually_exclusive_options(self, tmp_path: Path):
        """Test --schema-only and --data-only are mutually exclusive."""
        pkg1 = create_test_datapackage(tmp_path / "pkg1", entities=["persons"])
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        runner = CliRunner()
        result = runner.invoke(
            main, ["diff", str(pkg1), str(pkg2), "--schema-only", "--data-only"]
        )

        assert result.exit_code != 0
        assert "Cannot use both" in result.output

    def test_diff_quiet_option(self, tmp_path: Path):
        """Test -q option suppresses headers."""
        pkg1 = create_test_datapackage(tmp_path / "pkg1", entities=["persons"])
        pkg2 = create_test_datapackage(tmp_path / "pkg2", entities=["persons"])

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2), "-q"])

        assert result.exit_code == 0
        # In quiet mode with no differences, output should be minimal
        assert "Summary:" not in result.output

    def test_diff_missing_path(self):
        """Test error when path doesn't exist."""
        runner = CliRunner()
        result = runner.invoke(main, ["diff", "/nonexistent/path1", "/nonexistent/path2"])

        assert result.exit_code != 0

    def test_diff_excludes_computed_fields_by_default(self, tmp_path: Path):
        """Test computed fields are excluded from data comparison by default."""
        # Create packages with computed field (edit_flag=False) that differs
        computed_field = {
            "key": "update_time",
            "name": "Update Time",
            "field_type": "datetime",
            "edit_flag": False,  # Computed field
        }
        pkg1 = create_test_datapackage(
            tmp_path / "pkg1",
            entities=["persons"],
            extra_fields={"persons": [computed_field]},
            extra_data={
                "persons": [
                    {"id": "10", "name": "Test", "update_time": "2026-01-01T10:00:00"}
                ]
            },
        )
        pkg2 = create_test_datapackage(
            tmp_path / "pkg2",
            entities=["persons"],
            extra_fields={"persons": [computed_field]},
            extra_data={
                "persons": [
                    {"id": "10", "name": "Test", "update_time": "2026-01-02T15:30:00"}
                ]
            },
        )

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2)])

        assert result.exit_code == 0
        # update_time difference should NOT appear (excluded by default)
        assert "update_time" not in result.output

    def test_diff_all_fields_includes_computed(self, tmp_path: Path):
        """Test --all-fields includes computed fields in comparison."""
        # Create packages with computed field that differs
        computed_field = {
            "key": "update_time",
            "name": "Update Time",
            "field_type": "datetime",
            "edit_flag": False,
        }
        pkg1 = create_test_datapackage(
            tmp_path / "pkg1",
            entities=["persons"],
            extra_fields={"persons": [computed_field]},
            extra_data={
                "persons": [
                    {"id": "10", "name": "Test", "update_time": "2026-01-01T10:00:00"}
                ]
            },
        )
        pkg2 = create_test_datapackage(
            tmp_path / "pkg2",
            entities=["persons"],
            extra_fields={"persons": [computed_field]},
            extra_data={
                "persons": [
                    {"id": "10", "name": "Test", "update_time": "2026-01-02T15:30:00"}
                ]
            },
        )

        runner = CliRunner()
        result = runner.invoke(main, ["diff", str(pkg1), str(pkg2), "--all-fields"])

        assert result.exit_code == 0
        # With --all-fields, update_time difference SHOULD appear
        assert "update_time" in result.output
