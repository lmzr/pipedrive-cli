"""Tests for transform module and update command."""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipedrive_cli.cli import main
from pipedrive_cli.matching import AmbiguousMatchError
from pipedrive_cli.transform import (
    TRANSFORM_FUNCTIONS,
    apply_update_local,
    evaluate_assignment,
    format_resolved_assignment,
    parse_assignment,
    resolve_assignment,
)


class TestParseAssignment:
    """Tests for assignment parsing."""

    def test_simple_assignment(self):
        field, expr = parse_assignment("name=upper(name)")
        assert field == "name"
        assert expr == "upper(name)"

    def test_assignment_with_spaces(self):
        field, expr = parse_assignment("  name  =  upper(name)  ")
        assert field == "name"
        assert expr == "upper(name)"

    def test_assignment_with_equals_in_expr(self):
        field, expr = parse_assignment("status=if(value == 0, 'zero', 'nonzero')")
        assert field == "status"
        assert expr == "if(value == 0, 'zero', 'nonzero')"

    def test_assignment_no_equals_raises(self):
        with pytest.raises(ValueError) as exc_info:
            parse_assignment("name upper(name)")
        assert "Invalid assignment" in str(exc_info.value)


class TestTransformFunctions:
    """Tests for transform functions."""

    # String functions
    def test_upper(self):
        assert TRANSFORM_FUNCTIONS["upper"]("hello") == "HELLO"
        assert TRANSFORM_FUNCTIONS["upper"](None) is None

    def test_lower(self):
        assert TRANSFORM_FUNCTIONS["lower"]("HELLO") == "hello"
        assert TRANSFORM_FUNCTIONS["lower"](None) is None

    def test_strip(self):
        assert TRANSFORM_FUNCTIONS["strip"]("  hello  ") == "hello"
        assert TRANSFORM_FUNCTIONS["strip"](None) is None

    def test_lstrip(self):
        assert TRANSFORM_FUNCTIONS["lstrip"]("  hello  ") == "hello  "

    def test_rstrip(self):
        assert TRANSFORM_FUNCTIONS["rstrip"]("  hello  ") == "  hello"

    def test_replace(self):
        assert TRANSFORM_FUNCTIONS["replace"]("hello world", "world", "there") == "hello there"
        assert TRANSFORM_FUNCTIONS["replace"]("a.b.c", ".", "") == "abc"
        assert TRANSFORM_FUNCTIONS["replace"](None, "a", "b") is None

    def test_lpad(self):
        assert TRANSFORM_FUNCTIONS["lpad"]("7", 5, "0") == "00007"
        assert TRANSFORM_FUNCTIONS["lpad"]("123", 5, "0") == "00123"
        assert TRANSFORM_FUNCTIONS["lpad"]("12345", 5, "0") == "12345"
        assert TRANSFORM_FUNCTIONS["lpad"]("123456", 5, "0") == "123456"  # No truncation
        assert TRANSFORM_FUNCTIONS["lpad"](None, 5, "0") is None

    def test_rpad(self):
        assert TRANSFORM_FUNCTIONS["rpad"]("7", 5, "0") == "70000"
        assert TRANSFORM_FUNCTIONS["rpad"]("123", 5, "0") == "12300"
        assert TRANSFORM_FUNCTIONS["rpad"](None, 5, "0") is None

    def test_substr(self):
        assert TRANSFORM_FUNCTIONS["substr"]("hello world", 0, 5) == "hello"
        assert TRANSFORM_FUNCTIONS["substr"]("hello", 2, None) == "llo"
        assert TRANSFORM_FUNCTIONS["substr"](None, 0, 5) is None

    def test_concat(self):
        assert TRANSFORM_FUNCTIONS["concat"]("a", "b", "c") == "abc"
        assert TRANSFORM_FUNCTIONS["concat"]("hello", " ", "world") == "hello world"
        assert TRANSFORM_FUNCTIONS["concat"]("a", None, "c") == "ac"

    def test_len(self):
        assert TRANSFORM_FUNCTIONS["len"]("hello") == 5
        assert TRANSFORM_FUNCTIONS["len"](None) == 0

    # Type conversion
    def test_int(self):
        assert TRANSFORM_FUNCTIONS["int"]("42") == 42
        assert TRANSFORM_FUNCTIONS["int"](3.7) == 3
        assert TRANSFORM_FUNCTIONS["int"](None) == 0

    def test_float(self):
        assert TRANSFORM_FUNCTIONS["float"]("3.14") == 3.14
        assert TRANSFORM_FUNCTIONS["float"]("42") == 42.0
        assert TRANSFORM_FUNCTIONS["float"](None) == 0.0

    def test_str(self):
        assert TRANSFORM_FUNCTIONS["str"](42) == "42"
        assert TRANSFORM_FUNCTIONS["str"](3.14) == "3.14"
        assert TRANSFORM_FUNCTIONS["str"](None) == ""

    # Numeric functions
    def test_round(self):
        assert TRANSFORM_FUNCTIONS["round"](3.14159, 2) == 3.14
        assert TRANSFORM_FUNCTIONS["round"](3.5, 0) == 4.0
        assert TRANSFORM_FUNCTIONS["round"](None, 2) == 0

    def test_abs(self):
        assert TRANSFORM_FUNCTIONS["abs"](-5) == 5.0
        assert TRANSFORM_FUNCTIONS["abs"](5) == 5.0
        assert TRANSFORM_FUNCTIONS["abs"](None) == 0

    # Conditional
    def test_iif(self):
        assert TRANSFORM_FUNCTIONS["iif"](True, "yes", "no") == "yes"
        assert TRANSFORM_FUNCTIONS["iif"](False, "yes", "no") == "no"
        assert TRANSFORM_FUNCTIONS["iif"](1 > 0, "greater", "lesser") == "greater"

    def test_coalesce(self):
        assert TRANSFORM_FUNCTIONS["coalesce"](None, "", "default") == "default"
        assert TRANSFORM_FUNCTIONS["coalesce"]("first", "second") == "first"
        assert TRANSFORM_FUNCTIONS["coalesce"](None, None, None) is None

    # Null checks
    def test_isnull(self):
        assert TRANSFORM_FUNCTIONS["isnull"](None) is True
        assert TRANSFORM_FUNCTIONS["isnull"]("") is True
        assert TRANSFORM_FUNCTIONS["isnull"]("value") is False

    def test_notnull(self):
        assert TRANSFORM_FUNCTIONS["notnull"]("value") is True
        assert TRANSFORM_FUNCTIONS["notnull"](None) is False
        assert TRANSFORM_FUNCTIONS["notnull"]("") is False

    # Type checks
    def test_isint(self):
        assert TRANSFORM_FUNCTIONS["isint"](42) is True
        assert TRANSFORM_FUNCTIONS["isint"]("123") is True
        assert TRANSFORM_FUNCTIONS["isint"]("3.14") is False
        assert TRANSFORM_FUNCTIONS["isint"](None) is False

    def test_isfloat(self):
        assert TRANSFORM_FUNCTIONS["isfloat"](3.14) is True
        assert TRANSFORM_FUNCTIONS["isfloat"]("3.14") is True
        assert TRANSFORM_FUNCTIONS["isfloat"]("abc") is False
        assert TRANSFORM_FUNCTIONS["isfloat"](None) is False

    def test_isnumeric(self):
        assert TRANSFORM_FUNCTIONS["isnumeric"](42) is True
        assert TRANSFORM_FUNCTIONS["isnumeric"](3.14) is True
        assert TRANSFORM_FUNCTIONS["isnumeric"]("123") is True
        assert TRANSFORM_FUNCTIONS["isnumeric"]("abc") is False

    # String matching (from search)
    def test_contains(self):
        assert TRANSFORM_FUNCTIONS["contains"]("Hello World", "world") is True
        assert TRANSFORM_FUNCTIONS["contains"]("Hello", "xyz") is False

    def test_startswith(self):
        assert TRANSFORM_FUNCTIONS["startswith"]("Hello", "hel") is True
        assert TRANSFORM_FUNCTIONS["startswith"]("Hello", "xyz") is False

    def test_endswith(self):
        assert TRANSFORM_FUNCTIONS["endswith"]("Hello", "LO") is True
        assert TRANSFORM_FUNCTIONS["endswith"]("Hello", "xyz") is False


class TestResolveAssignment:
    """Tests for assignment resolution."""

    @pytest.fixture
    def sample_fields(self):
        return [
            {"key": "id", "name": "ID"},
            {"key": "first_name", "name": "First Name"},
            {"key": "last_name", "name": "Last Name"},
            {"key": "abc123_custom", "name": "Custom Field"},
        ]

    def test_no_resolution_needed(self, sample_fields):
        target_key, orig_expr, resolved_expr, resolutions = resolve_assignment(
            sample_fields, "first_name=upper(first_name)"
        )
        assert target_key == "first_name"
        assert orig_expr == "upper(first_name)"
        assert resolved_expr == "upper(first_name)"
        assert resolutions == {}

    def test_resolve_target_field(self, sample_fields):
        target_key, orig_expr, resolved_expr, resolutions = resolve_assignment(
            sample_fields, "first='0' + first"
        )
        assert target_key == "first_name"
        assert "'0' + first" in orig_expr
        assert "first" in resolutions
        assert resolutions["first"] == ("first_name", "First Name")

    def test_resolve_expr_fields(self, sample_fields):
        target_key, orig_expr, resolved_expr, resolutions = resolve_assignment(
            sample_fields, "abc123_custom=first + ' ' + last"
        )
        assert target_key == "abc123_custom"
        assert "first_name" in resolved_expr
        assert "last_name" in resolved_expr

    def test_ambiguous_target_raises(self):
        fields = [
            {"key": "abc123_first", "name": "First"},
            {"key": "abc456_second", "name": "Second"},
        ]
        with pytest.raises(AmbiguousMatchError):
            resolve_assignment(fields, "abc=upper(abc)")

    def test_user_escaped_digit_starting_key(self):
        """User-escaped digit-starting keys work in assignments."""
        fields = [
            {"key": "25da23b938af0807ec37bba8be25d77bae233536", "name": "Code"},
        ]
        target, orig, resolved, resolutions = resolve_assignment(
            fields, "_25=upper(_25)"
        )
        # Target should be escaped with _ prefix
        assert target == "_25da23b938af0807ec37bba8be25d77bae233536"
        # Expression should have escaped key
        assert "_25da23b938af0807ec37bba8be25d77bae233536" in resolved
        # Resolutions should map _25 to the full key
        assert "_25" in resolutions
        assert resolutions["_25"][0] == "25da23b938af0807ec37bba8be25d77bae233536"

    def test_hex_pattern_auto_detected_in_assignment(self):
        """Hex-like patterns (25da) are auto-detected in assignments."""
        fields = [
            {"key": "25da23b938af0807ec37bba8be25d77bae233536", "name": "Code"},
            {"key": "b85f32437e17e520e0c1173f4c3c887563d90de8", "name": "Type"},
        ]
        target, orig, resolved, resolutions = resolve_assignment(
            fields, "25da=concat(25da, b85f)"
        )
        # Target should be escaped
        assert target == "_25da23b938af0807ec37bba8be25d77bae233536"
        # Both keys should be in resolved expression (digit-key escaped)
        assert "_25da23b938af0807ec37bba8be25d77bae233536" in resolved
        assert "b85f32437e17e520e0c1173f4c3c887563d90de8" in resolved


class TestFormatResolvedAssignment:
    """Tests for assignment formatting."""

    def test_no_resolution(self):
        name_line, key_line = format_resolved_assignment(
            "name", "name", "upper(name)", "upper(name)", {}
        )
        assert name_line == "name = upper(name)"
        assert key_line == ""

    def test_with_resolution(self):
        resolutions = {
            "first": ("first_name", "First Name"),
        }
        name_line, key_line = format_resolved_assignment(
            "first", "first_name", "'0' + first", "'0' + first_name", resolutions
        )
        assert '"First Name"' in name_line
        assert "first_name" in key_line


class TestEvaluateAssignment:
    """Tests for expression evaluation."""

    def test_simple_expression(self):
        record = {"name": "john"}
        result = evaluate_assignment(record, "upper(name)")
        assert result == "JOHN"

    def test_string_concatenation(self):
        record = {"first": "John", "last": "Doe"}
        result = evaluate_assignment(record, "first + ' ' + last")
        assert result == "John Doe"

    def test_string_with_numeric_value(self):
        """String containing only digits stays string (no auto-coercion)."""
        record = {"phone": "123456789"}
        result = evaluate_assignment(record, '"0" + phone')
        assert result == "0123456789"

    def test_numeric_expression(self):
        record = {"value": "100"}
        result = evaluate_assignment(record, "int(value) * 2")
        assert result == 200

    def test_conditional_expression(self):
        record = {"value": 150}
        result = evaluate_assignment(record, "iif(value > 100, 'high', 'low')")
        assert result == "high"

    def test_lpad_expression(self):
        record = {"code": "7"}
        result = evaluate_assignment(record, "lpad(code, 5, '0')")
        assert result == "00007"

    def test_replace_expression(self):
        record = {"phone": "01.23.45.67.89"}
        result = evaluate_assignment(record, "replace(phone, '.', '')")
        assert result == "0123456789"


class TestApplyUpdateLocal:
    """Tests for local record updates."""

    def test_update_single_field(self):
        records = [
            {"id": 1, "name": "john"},
            {"id": 2, "name": "jane"},
        ]
        assignments = [("name", "upper(name)")]
        stats, changes = apply_update_local(records, assignments, dry_run=False)

        assert stats.total == 2
        assert stats.updated == 2
        assert stats.skipped == 0
        assert stats.failed == 0
        assert records[0]["name"] == "JOHN"
        assert records[1]["name"] == "JANE"
        assert len(changes) == 2

    def test_update_multiple_fields(self):
        records = [{"id": 1, "first": "john", "last": "doe"}]
        assignments = [
            ("first", "upper(first)"),
            ("last", "upper(last)"),
        ]
        stats, changes = apply_update_local(records, assignments, dry_run=False)

        assert stats.updated == 1
        assert records[0]["first"] == "JOHN"
        assert records[0]["last"] == "DOE"

    def test_dry_run_no_changes(self):
        records = [{"id": 1, "name": "john"}]
        assignments = [("name", "upper(name)")]
        stats, changes = apply_update_local(records, assignments, dry_run=True)

        assert stats.updated == 1
        assert records[0]["name"] == "john"  # Not changed
        assert len(changes) == 1
        assert changes[0]["new"] == "JOHN"

    def test_skip_unchanged(self):
        records = [
            {"id": 1, "name": "JOHN"},  # Already uppercase
        ]
        assignments = [("name", "upper(name)")]
        stats, changes = apply_update_local(records, assignments, dry_run=False)

        assert stats.updated == 0
        assert stats.skipped == 1
        assert len(changes) == 0

    def test_handle_error(self):
        records = [{"id": 1, "value": "not_a_number"}]
        assignments = [("value", "int(value) + 1")]  # Will fail
        stats, changes = apply_update_local(records, assignments, dry_run=False)

        assert stats.failed == 1
        assert len(stats.errors) == 1


@pytest.fixture
def update_backup_dir(tmp_path: Path) -> Path:
    """Create a backup directory for update tests."""
    backup_dir = tmp_path / "update-test"
    backup_dir.mkdir()

    # Create persons.csv with test data
    persons_csv = backup_dir / "persons.csv"
    persons_csv.write_text(
        "id,name,phone,code\n"
        "1,john doe,01.23.45.67.89,7\n"
        "2,jane smith,0987654321,42\n"
        "3,bob johnson,1234567890,123\n"
    )

    # Create datapackage.json
    datapackage = {
        "name": "update-test",
        "resources": [
            {
                "name": "persons",
                "path": "persons.csv",
                "schema": {
                    "fields": [
                        {"name": "id", "type": "integer"},
                        {"name": "name", "type": "string"},
                        {"name": "phone", "type": "string"},
                        {"name": "code", "type": "string"},
                    ],
                    "custom": {
                        "pipedrive_fields": [
                            {"key": "id", "name": "ID", "field_type": "int"},
                            {"key": "name", "name": "Name", "field_type": "varchar"},
                            {"key": "phone", "name": "Phone", "field_type": "varchar"},
                            {"key": "code", "name": "Code", "field_type": "varchar"},
                        ]
                    },
                },
            }
        ],
    }
    (backup_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

    return backup_dir


class TestUpdateCommand:
    """Integration tests for the update CLI command."""

    def test_update_dry_run(self, update_backup_dir):
        """Dry-run shows what would be updated."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "persons", "--base", str(update_backup_dir),
            "-s", "name=upper(name)", "-n"
        ])
        assert result.exit_code == 0
        assert "DRY RUN" in result.output
        assert "Updated" in result.output or "Would update" in result.output

    def test_update_uppercase(self, update_backup_dir):
        """Update applies uppercase transformation."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "per", "--base", str(update_backup_dir),
            "-s", "name=upper(name)", "-q"
        ])
        assert result.exit_code == 0
        assert "Update completed" in result.output

        # Verify changes
        csv_content = (update_backup_dir / "persons.csv").read_text()
        assert "JOHN DOE" in csv_content
        assert "JANE SMITH" in csv_content

    def test_update_with_filter(self, update_backup_dir):
        """Update with filter only modifies matching records."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "persons", "--base", str(update_backup_dir),
            "-f", "contains(name, 'john')",
            "-s", "name=upper(name)", "-q"
        ])
        assert result.exit_code == 0

        # Verify only matching records changed
        csv_content = (update_backup_dir / "persons.csv").read_text()
        assert "JOHN DOE" in csv_content
        assert "BOB JOHNSON" in csv_content
        assert "jane smith" in csv_content  # Not changed

    def test_update_lpad(self, update_backup_dir):
        """Update with lpad transformation."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "persons", "--base", str(update_backup_dir),
            "-s", "code=lpad(code, 5, '0')", "-q"
        ])
        assert result.exit_code == 0

        csv_content = (update_backup_dir / "persons.csv").read_text()
        assert "00007" in csv_content
        assert "00042" in csv_content
        assert "00123" in csv_content

    def test_update_replace(self, update_backup_dir):
        """Update with replace transformation."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "persons", "--base", str(update_backup_dir),
            "-f", "contains(phone, '.')",
            "-s", "phone=replace(phone, '.', '')", "-q"
        ])
        assert result.exit_code == 0

        csv_content = (update_backup_dir / "persons.csv").read_text()
        assert "0123456789" in csv_content  # Dots removed

    def test_update_multiple_assignments(self, update_backup_dir):
        """Multiple assignments are applied."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "persons", "--base", str(update_backup_dir),
            "-s", "name=upper(name)",
            "-s", "code=lpad(code, 5, '0')", "-q"
        ])
        assert result.exit_code == 0

        csv_content = (update_backup_dir / "persons.csv").read_text()
        assert "JOHN DOE" in csv_content
        assert "00007" in csv_content

    def test_update_shows_resolved_expression(self, update_backup_dir):
        """Resolved expressions are shown by default."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "persons", "--base", str(update_backup_dir),
            "-s", "name=upper(name)", "-n"
        ])
        assert result.exit_code == 0
        assert "Set" in result.output

    def test_update_quiet_mode(self, update_backup_dir):
        """Quiet mode suppresses expression display."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "persons", "--base", str(update_backup_dir),
            "-s", "name=upper(name)", "-q", "-n"
        ])
        assert result.exit_code == 0
        assert "Set w/ names" not in result.output
        assert "Set w/ keys" not in result.output

    def test_update_with_limit(self, update_backup_dir):
        """Limit option restricts updates."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "persons", "--base", str(update_backup_dir),
            "-s", "name=upper(name)", "--limit", "1", "-q"
        ])
        assert result.exit_code == 0

        csv_content = (update_backup_dir / "persons.csv").read_text()
        # Only first record should be uppercase
        assert "JOHN DOE" in csv_content
        assert "jane smith" in csv_content  # Not changed

    def test_update_invalid_assignment(self, update_backup_dir):
        """Invalid assignment format raises error."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "persons", "--base", str(update_backup_dir),
            "-s", "name upper(name)"  # Missing =
        ])
        assert result.exit_code != 0
        assert "Invalid assignment" in result.output

    def test_update_invalid_entity(self, update_backup_dir):
        """Invalid entity raises error."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "invalid", "--base", str(update_backup_dir),
            "-s", "name=upper(name)"
        ])
        assert result.exit_code != 0
        assert "No entity matches" in result.output

    def test_update_log_file(self, update_backup_dir):
        """Log file is written."""
        log_file = update_backup_dir / "changes.jsonl"
        runner = CliRunner()
        result = runner.invoke(main, [
            "value", "update", "-e", "persons", "--base", str(update_backup_dir),
            "-s", "name=upper(name)", "-l", str(log_file), "-q"
        ])
        assert result.exit_code == 0
        assert log_file.exists()

        # Verify log content
        log_content = log_file.read_text()
        assert "JOHN DOE" in log_content


class TestApplyUpdateLocalWithEnumValues:
    """Tests for apply_update_local with enum/set field comparison."""

    def test_iif_with_enum_int_comparison(self):
        """iif() works with enum field compared to int ID."""
        records = [
            {"id": 1, "status": "37", "label": ""},
            {"id": 2, "status": "38", "label": ""},
        ]
        option_lookup = {"status": {"37": "Active", "38": "Inactive"}}
        assignments = [("label", "iif(status == 37, 'Y', 'N')")]

        stats, changes = apply_update_local(
            records, assignments, dry_run=False, option_lookup=option_lookup
        )

        assert stats.updated == 2
        assert records[0]["label"] == "Y"  # status 37 matches
        assert records[1]["label"] == "N"  # status 38 doesn't match

    def test_iif_with_enum_label_comparison(self):
        """iif() works with enum field compared to label text."""
        records = [
            {"id": 1, "status": "37", "label": ""},
            {"id": 2, "status": "38", "label": ""},
        ]
        option_lookup = {"status": {"37": "Active", "38": "Inactive"}}
        assignments = [("label", "iif(status == 'Active', 'Y', 'N')")]

        stats, changes = apply_update_local(
            records, assignments, dry_run=False, option_lookup=option_lookup
        )

        assert stats.updated == 2
        assert records[0]["label"] == "Y"  # status 37 = "Active"
        assert records[1]["label"] == "N"  # status 38 = "Inactive"

    def test_iif_with_enum_label_case_insensitive(self):
        """iif() works with case-insensitive label comparison."""
        records = [{"id": 1, "status": "37", "label": ""}]
        option_lookup = {"status": {"37": "Active"}}
        assignments = [("label", "iif(status == 'active', 'Y', 'N')")]

        stats, changes = apply_update_local(
            records, assignments, dry_run=False, option_lookup=option_lookup
        )

        assert records[0]["label"] == "Y"

    def test_without_option_lookup(self):
        """Without option_lookup, raw string comparison still works."""
        records = [{"id": 1, "status": "37", "label": ""}]
        assignments = [("label", "iif(status == '37', 'Y', 'N')")]

        stats, changes = apply_update_local(
            records, assignments, dry_run=False, option_lookup=None
        )

        assert records[0]["label"] == "Y"  # String comparison works

    def test_enum_ne_comparison(self):
        """!= comparison works with enum values."""
        records = [
            {"id": 1, "status": "37", "label": ""},
            {"id": 2, "status": "38", "label": ""},
        ]
        option_lookup = {"status": {"37": "Active", "38": "Inactive"}}
        assignments = [("label", "iif(status != 'Active', 'Not Active', 'Active')")]

        stats, changes = apply_update_local(
            records, assignments, dry_run=False, option_lookup=option_lookup
        )

        assert records[0]["label"] == "Active"
        assert records[1]["label"] == "Not Active"

    def test_original_record_modified(self):
        """Original record is modified (not the preprocessed copy)."""
        records = [{"id": 1, "status": "37", "result": ""}]
        option_lookup = {"status": {"37": "Active"}}
        assignments = [("result", "iif(status == 'Active', 'matched', 'no')")]

        stats, changes = apply_update_local(
            records, assignments, dry_run=False, option_lookup=option_lookup
        )

        # Original record is modified
        assert records[0]["result"] == "matched"
        # status field remains raw string (not EnumValue)
        assert records[0]["status"] == "37"
        assert isinstance(records[0]["status"], str)
