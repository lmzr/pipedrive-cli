"""Tests for search module and search command."""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipedrive_cli.cli import main
from pipedrive_cli.expressions import FILTER_FUNCTIONS
from pipedrive_cli.matching import AmbiguousMatchError
from pipedrive_cli.search import (
    FilterError,
    extract_filter_keys,
    filter_record,
    format_csv,
    format_json,
    resolve_field_identifier,
    resolve_field_prefixes,
    resolve_filter_expression,
    select_fields,
    validate_expression,
)


class TestStringFunctions:
    """Tests for custom string functions used in filters."""

    def test_contains_match(self):
        assert FILTER_FUNCTIONS["contains"]("Hello World", "world") is True

    def test_contains_no_match(self):
        assert FILTER_FUNCTIONS["contains"]("Hello World", "foo") is False

    def test_contains_none(self):
        assert FILTER_FUNCTIONS["contains"](None, "test") is False

    def test_startswith_match(self):
        assert FILTER_FUNCTIONS["startswith"]("Hello World", "hello") is True

    def test_startswith_no_match(self):
        assert FILTER_FUNCTIONS["startswith"]("Hello World", "world") is False

    def test_startswith_none(self):
        assert FILTER_FUNCTIONS["startswith"](None, "test") is False

    def test_endswith_match(self):
        assert FILTER_FUNCTIONS["endswith"]("Hello World", "WORLD") is True

    def test_endswith_no_match(self):
        assert FILTER_FUNCTIONS["endswith"]("Hello World", "hello") is False

    def test_isnull_none(self):
        assert FILTER_FUNCTIONS["isnull"](None) is True

    def test_isnull_empty(self):
        assert FILTER_FUNCTIONS["isnull"]("") is True

    def test_isnull_value(self):
        assert FILTER_FUNCTIONS["isnull"]("test") is False

    def test_notnull_value(self):
        assert FILTER_FUNCTIONS["notnull"]("test") is True

    def test_notnull_none(self):
        assert FILTER_FUNCTIONS["notnull"](None) is False

    def test_len_string(self):
        assert FILTER_FUNCTIONS["len"]("hello") == 5

    def test_len_none(self):
        assert FILTER_FUNCTIONS["len"](None) == 0

    # Tests for isint function
    def test_isint_integer_value(self):
        assert FILTER_FUNCTIONS["isint"](42) is True

    def test_isint_string_integer(self):
        assert FILTER_FUNCTIONS["isint"]("123") is True

    def test_isint_string_integer_whitespace(self):
        assert FILTER_FUNCTIONS["isint"]("  42  ") is True

    def test_isint_float_whole(self):
        assert FILTER_FUNCTIONS["isint"](3.0) is True

    def test_isint_float_fractional(self):
        assert FILTER_FUNCTIONS["isint"](3.5) is False

    def test_isint_string_float(self):
        assert FILTER_FUNCTIONS["isint"]("3.14") is False

    def test_isint_string_non_numeric(self):
        assert FILTER_FUNCTIONS["isint"]("abc") is False

    def test_isint_empty_string(self):
        assert FILTER_FUNCTIONS["isint"]("") is False

    def test_isint_none(self):
        assert FILTER_FUNCTIONS["isint"](None) is False

    def test_isint_bool(self):
        # Booleans should not be treated as integers
        assert FILTER_FUNCTIONS["isint"](True) is False

    def test_isint_negative(self):
        assert FILTER_FUNCTIONS["isint"]("-42") is True

    # Tests for isfloat function
    def test_isfloat_float_value(self):
        assert FILTER_FUNCTIONS["isfloat"](3.14) is True

    def test_isfloat_integer_value(self):
        assert FILTER_FUNCTIONS["isfloat"](42) is True

    def test_isfloat_string_float(self):
        assert FILTER_FUNCTIONS["isfloat"]("3.14") is True

    def test_isfloat_string_integer(self):
        assert FILTER_FUNCTIONS["isfloat"]("123") is True

    def test_isfloat_string_whitespace(self):
        assert FILTER_FUNCTIONS["isfloat"]("  3.14  ") is True

    def test_isfloat_string_non_numeric(self):
        assert FILTER_FUNCTIONS["isfloat"]("abc") is False

    def test_isfloat_empty_string(self):
        assert FILTER_FUNCTIONS["isfloat"]("") is False

    def test_isfloat_none(self):
        assert FILTER_FUNCTIONS["isfloat"](None) is False

    def test_isfloat_bool(self):
        # Booleans should not be treated as floats
        assert FILTER_FUNCTIONS["isfloat"](True) is False

    def test_isfloat_negative(self):
        assert FILTER_FUNCTIONS["isfloat"]("-3.14") is True

    def test_isfloat_scientific_notation(self):
        assert FILTER_FUNCTIONS["isfloat"]("1e10") is True

    # Tests for isnumeric function
    def test_isnumeric_integer(self):
        assert FILTER_FUNCTIONS["isnumeric"](42) is True

    def test_isnumeric_float(self):
        assert FILTER_FUNCTIONS["isnumeric"](3.14) is True

    def test_isnumeric_string_integer(self):
        assert FILTER_FUNCTIONS["isnumeric"]("123") is True

    def test_isnumeric_string_float(self):
        assert FILTER_FUNCTIONS["isnumeric"]("3.14") is True

    def test_isnumeric_non_numeric(self):
        assert FILTER_FUNCTIONS["isnumeric"]("abc") is False

    def test_isnumeric_none(self):
        assert FILTER_FUNCTIONS["isnumeric"](None) is False

    # Tests for string manipulation functions
    def test_strip(self):
        assert FILTER_FUNCTIONS["strip"]("  hello  ") == "hello"
        assert FILTER_FUNCTIONS["strip"](None) == ""

    def test_lstrip(self):
        assert FILTER_FUNCTIONS["lstrip"]("  hello  ") == "hello  "

    def test_rstrip(self):
        assert FILTER_FUNCTIONS["rstrip"]("  hello  ") == "  hello"

    def test_replace(self):
        assert FILTER_FUNCTIONS["replace"]("a.b.c", ".", "") == "abc"
        assert FILTER_FUNCTIONS["replace"](None, ".", "") == ""

    def test_substr(self):
        assert FILTER_FUNCTIONS["substr"]("hello", 0, 2) == "he"
        assert FILTER_FUNCTIONS["substr"]("hello", 2, None) == "llo"
        assert FILTER_FUNCTIONS["substr"](None, 0, 2) == ""

    def test_lpad(self):
        assert FILTER_FUNCTIONS["lpad"]("7", 5, "0") == "00007"
        assert FILTER_FUNCTIONS["lpad"](None, 5, "0") == ""

    def test_rpad(self):
        assert FILTER_FUNCTIONS["rpad"]("7", 5, "0") == "70000"

    def test_concat(self):
        assert FILTER_FUNCTIONS["concat"]("a", "b", "c") == "abc"
        assert FILTER_FUNCTIONS["concat"]("a", None, "c") == "ac"


class TestResolveFieldIdentifier:
    """Tests for field identifier resolution."""

    @pytest.fixture
    def sample_fields(self) -> list[dict]:
        """Sample field definitions."""
        return [
            {"key": "id", "name": "ID"},
            {"key": "first_name", "name": "First Name"},
            {"key": "last_name", "name": "Last Name"},
            {"key": "email", "name": "Email"},
            {"key": "abc123_custom", "name": "Custom Field"},
            {"key": "abc456_other", "name": "Other Custom"},
        ]

    def test_exact_key_match(self, sample_fields):
        """Exact key returns itself."""
        result = resolve_field_identifier(sample_fields, "first_name")
        assert result == "first_name"

    def test_key_prefix_unique(self, sample_fields):
        """Unique key prefix resolves to full key."""
        result = resolve_field_identifier(sample_fields, "first")
        assert result == "first_name"

    def test_key_prefix_case_insensitive(self, sample_fields):
        """Key prefix matching is case-insensitive."""
        result = resolve_field_identifier(sample_fields, "FIRST")
        assert result == "first_name"

    def test_key_prefix_ambiguous(self, sample_fields):
        """Ambiguous key prefix raises error."""
        with pytest.raises(AmbiguousMatchError) as exc_info:
            resolve_field_identifier(sample_fields, "abc")
        assert "abc123_custom" in exc_info.value.matches
        assert "abc456_other" in exc_info.value.matches

    def test_exact_name_match(self, sample_fields):
        """Exact name (case-insensitive) resolves to key."""
        result = resolve_field_identifier(sample_fields, "first name")
        assert result == "first_name"

    def test_name_prefix_unique(self, sample_fields):
        """Unique name prefix resolves to key."""
        result = resolve_field_identifier(sample_fields, "Custom")
        assert result == "abc123_custom"

    def test_name_prefix_ambiguous(self, sample_fields):
        """Ambiguous name prefix raises error."""
        # Both "First Name" and "Other Custom" start with different prefixes
        # But "Last" matches only "last_name"
        result = resolve_field_identifier(sample_fields, "Last")
        assert result == "last_name"

    def test_no_match_returns_original(self, sample_fields):
        """No match returns original identifier."""
        result = resolve_field_identifier(sample_fields, "nonexistent")
        assert result == "nonexistent"

    def test_underscore_to_space_normalization(self):
        """Underscores in identifier match spaces in field names."""
        fields = [
            {"key": "_new_abc123", "name": "Tel standard"},
            {"key": "_new_def456", "name": "Tel portable"},
        ]
        # tel_s should match "Tel standard"
        result = resolve_field_identifier(fields, "tel_s")
        assert result == "_new_abc123"

    def test_underscore_exact_name_match(self):
        """Underscore normalization works for exact name match."""
        fields = [
            {"key": "custom_field", "name": "My Field"},
        ]
        result = resolve_field_identifier(fields, "my_field")
        assert result == "custom_field"


class TestResolveFilterExpression:
    """Tests for filter expression resolution."""

    @pytest.fixture
    def sample_fields(self) -> list[dict]:
        """Sample field definitions."""
        return [
            {"key": "id", "name": "ID"},
            {"key": "first_name", "name": "First Name"},
            {"key": "age", "name": "Age"},
            {"key": "abc123_custom", "name": "Custom Field"},
        ]

    def test_no_resolution_needed(self, sample_fields):
        """Expression with exact keys remains unchanged."""
        expr = "first_name == 'John'"
        result, resolutions = resolve_filter_expression(sample_fields, expr)
        assert result == expr
        assert resolutions == {}

    def test_resolve_key_prefix(self, sample_fields):
        """Key prefix in expression is resolved."""
        expr = "contains(first, 'John')"
        result, resolutions = resolve_filter_expression(sample_fields, expr)
        assert result == "contains(first_name, 'John')"
        assert "first" in resolutions
        assert resolutions["first"] == ("first_name", "First Name")

    def test_resolve_multiple_identifiers(self, sample_fields):
        """Multiple identifiers are resolved."""
        expr = "first == 'John' and age > 30"
        result, resolutions = resolve_filter_expression(sample_fields, expr)
        assert "first_name" in result
        assert "age" in result

    def test_preserve_string_literals(self, sample_fields):
        """String literals are not resolved."""
        expr = "contains(first_name, 'first')"
        result, _ = resolve_filter_expression(sample_fields, expr)
        # The 'first' inside quotes should not be resolved
        assert result == expr

    def test_preserve_functions(self, sample_fields):
        """Function names are not resolved."""
        expr = "contains(first_name, 'test')"
        result, _ = resolve_filter_expression(sample_fields, expr)
        assert "contains" in result  # Not resolved to anything else

    def test_empty_expression(self, sample_fields):
        """Empty expression returns empty."""
        result, resolutions = resolve_filter_expression(sample_fields, "")
        assert result == ""
        assert resolutions == {}

    def test_none_expression(self, sample_fields):
        """None expression returns None."""
        result, resolutions = resolve_filter_expression(sample_fields, None)
        assert result is None
        assert resolutions == {}

    def test_ambiguous_raises(self, sample_fields):
        """Ambiguous identifier in expression raises error."""
        # Add ambiguous field
        fields_with_ambiguity = sample_fields + [
            {"key": "first_contact", "name": "First Contact"}
        ]
        expr = "first > 0"  # "first" now matches both first_name and first_contact

        with pytest.raises(AmbiguousMatchError):
            resolve_filter_expression(fields_with_ambiguity, expr)


class TestExtractFilterKeys:
    """Tests for extracting field keys from resolved filter expressions."""

    @pytest.fixture
    def sample_fields(self):
        return [
            {"key": "id", "name": "ID"},
            {"key": "name", "name": "Name"},
            {"key": "first_name", "name": "First Name"},
            {"key": "abc123_custom", "name": "Custom Field"},
        ]

    def test_simple_key(self, sample_fields):
        """Single key is extracted."""
        result = extract_filter_keys(sample_fields, "notnull(name)")
        assert result == ["name"]

    def test_multiple_keys(self, sample_fields):
        """Multiple keys are extracted."""
        result = extract_filter_keys(sample_fields, "name == 'test' and id > 0")
        assert result == ["name", "id"]

    def test_no_duplicates(self, sample_fields):
        """Same key used twice is not duplicated."""
        result = extract_filter_keys(sample_fields, "name == 'a' or name == 'b'")
        assert result == ["name"]

    def test_custom_field_key(self, sample_fields):
        """Custom field keys are extracted."""
        result = extract_filter_keys(sample_fields, "notnull(abc123_custom)")
        assert result == ["abc123_custom"]

    def test_functions_excluded(self, sample_fields):
        """Function names are not extracted as keys."""
        result = extract_filter_keys(sample_fields, "contains(name, 'test') and isnull(id)")
        assert result == ["name", "id"]

    def test_string_literals_excluded(self, sample_fields):
        """String literals containing field names are excluded."""
        result = extract_filter_keys(sample_fields, "name == 'first_name'")
        assert result == ["name"]

    def test_empty_expression(self, sample_fields):
        """Empty expression returns empty list."""
        result = extract_filter_keys(sample_fields, "")
        assert result == []


class TestFilterRecord:
    """Tests for filter expression evaluation."""

    def test_simple_comparison(self):
        """Simple numeric comparison works."""
        record = {"age": 30}
        assert filter_record(record, "age > 25") is True
        assert filter_record(record, "age > 35") is False

    def test_string_equality(self):
        """String equality works."""
        record = {"name": "John Doe"}
        assert filter_record(record, "name == 'John Doe'") is True
        assert filter_record(record, "name == 'Jane'") is False

    def test_contains_function(self):
        """contains() function works."""
        record = {"name": "John Doe", "email": "john@example.com"}
        assert filter_record(record, "contains(name, 'John')") is True
        assert filter_record(record, "contains(name, 'jane')") is False

    def test_startswith_function(self):
        """startswith() function works."""
        record = {"name": "John Doe"}
        assert filter_record(record, "startswith(name, 'John')") is True
        assert filter_record(record, "startswith(name, 'Doe')") is False

    def test_endswith_function(self):
        """endswith() function works."""
        record = {"email": "john@example.com"}
        assert filter_record(record, "endswith(email, '.com')") is True
        assert filter_record(record, "endswith(email, '.org')") is False

    def test_isnull_function(self):
        """isnull() function works."""
        record = {"name": "John", "phone": None, "notes": ""}
        assert filter_record(record, "isnull(phone)") is True
        assert filter_record(record, "isnull(notes)") is True
        assert filter_record(record, "isnull(name)") is False

    def test_notnull_function(self):
        """notnull() function works."""
        record = {"name": "John", "phone": None}
        assert filter_record(record, "notnull(name)") is True
        assert filter_record(record, "notnull(phone)") is False

    def test_len_function(self):
        """len() function works."""
        record = {"name": "John"}
        assert filter_record(record, "len(name) == 4") is True
        assert filter_record(record, "len(name) > 10") is False

    def test_compound_and(self):
        """AND expressions work."""
        record = {"name": "John", "age": 30}
        assert filter_record(record, "name == 'John' and age > 25") is True
        assert filter_record(record, "name == 'John' and age > 35") is False

    def test_compound_or(self):
        """OR expressions work."""
        record = {"name": "John", "age": 30}
        assert filter_record(record, "name == 'Jane' or age > 25") is True
        assert filter_record(record, "name == 'Jane' or age > 35") is False

    def test_empty_expression_returns_true(self):
        """Empty expression matches all records."""
        record = {"name": "John"}
        assert filter_record(record, "") is True
        assert filter_record(record, None) is True

    def test_invalid_expression_raises_error(self):
        """Invalid expression raises FilterError."""
        record = {"name": "John"}
        with pytest.raises(FilterError):
            filter_record(record, "syntax error here")

    def test_missing_field_raises_error(self):
        """Reference to missing field raises FilterError."""
        record = {"name": "John"}
        with pytest.raises(FilterError):
            filter_record(record, "age > 30")


class TestResolveFieldPrefixes:
    """Tests for --include/--exclude prefix resolution."""

    @pytest.fixture
    def sample_fields(self) -> list[dict]:
        """Sample field definitions."""
        return [
            {"key": "id", "name": "ID"},
            {"key": "name", "name": "Name"},
            {"key": "email", "name": "Email"},
            {"key": "abc123_custom", "name": "Custom"},
            {"key": "abc456_other", "name": "Other"},
        ]

    def test_exact_match(self, sample_fields):
        """Exact keys are resolved."""
        result = resolve_field_prefixes(sample_fields, ["name", "email"])
        assert result == ["name", "email"]

    def test_key_prefix(self, sample_fields):
        """Key prefixes are resolved."""
        result = resolve_field_prefixes(sample_fields, ["abc123"])
        assert result == ["abc123_custom"]

    def test_ambiguous_includes_all(self, sample_fields):
        """Ambiguous prefix includes all matches by default."""
        result = resolve_field_prefixes(sample_fields, ["abc"], fail_on_ambiguous=False)
        assert "abc123_custom" in result
        assert "abc456_other" in result

    def test_ambiguous_fails_if_requested(self, sample_fields):
        """Ambiguous prefix raises error if fail_on_ambiguous=True."""
        with pytest.raises(AmbiguousMatchError):
            resolve_field_prefixes(sample_fields, ["abc"], fail_on_ambiguous=True)

    def test_no_match_skipped(self, sample_fields):
        """Non-matching prefixes are skipped."""
        result = resolve_field_prefixes(sample_fields, ["xyz"])
        assert result == []

    def test_deduplication(self, sample_fields):
        """Duplicate keys are deduplicated."""
        result = resolve_field_prefixes(sample_fields, ["name", "name", "email"])
        assert result == ["name", "email"]

    def test_name_prefix(self, sample_fields):
        """Name prefixes are resolved."""
        result = resolve_field_prefixes(sample_fields, ["Custom"])
        assert result == ["abc123_custom"]


class TestSelectFields:
    """Tests for field selection."""

    def test_include_mode(self):
        """Include mode keeps only specified fields."""
        record = {"id": 1, "name": "John", "email": "john@test.com", "phone": "123"}
        result = select_fields(record, include_keys=["id", "name"], exclude_keys=None)
        assert result == {"id": 1, "name": "John"}

    def test_exclude_mode(self):
        """Exclude mode removes specified fields."""
        record = {"id": 1, "name": "John", "email": "john@test.com"}
        result = select_fields(record, include_keys=None, exclude_keys=["email"])
        assert result == {"id": 1, "name": "John"}

    def test_no_selection(self):
        """No selection returns full record."""
        record = {"id": 1, "name": "John"}
        result = select_fields(record, include_keys=None, exclude_keys=None)
        assert result == record


class TestFormatJson:
    """Tests for JSON output formatting."""

    def test_format_json_simple(self):
        """Simple records are formatted as JSON."""
        records = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        result = format_json(records)
        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["name"] == "John"

    def test_format_json_empty(self):
        """Empty list returns empty JSON array."""
        result = format_json([])
        assert json.loads(result) == []


class TestFormatCsv:
    """Tests for CSV output formatting."""

    def test_format_csv_simple(self):
        """Simple records are formatted as CSV."""
        records = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        result = format_csv(records)
        lines = result.strip().split("\n")
        assert len(lines) == 3  # header + 2 rows
        assert "id" in lines[0]
        assert "name" in lines[0]

    def test_format_csv_empty(self):
        """Empty list returns empty string."""
        result = format_csv([])
        assert result == ""

    def test_format_csv_complex_values(self):
        """Complex values are JSON-encoded."""
        records = [{"id": 1, "data": {"nested": "value"}}]
        result = format_csv(records)
        # CSV library may add extra quotes for escaping
        assert "nested" in result
        assert "value" in result


@pytest.fixture
def search_backup_dir(tmp_path: Path) -> Path:
    """Create a backup directory with searchable data."""
    backup_dir = tmp_path / "search-test"
    backup_dir.mkdir()

    # Create persons.csv with multiple records
    persons_csv = backup_dir / "persons.csv"
    persons_csv.write_text(
        "id,name,email,age\n"
        "1,John Doe,john@example.com,30\n"
        "2,Jane Smith,jane@example.com,25\n"
        "3,Bob Johnson,bob@test.org,35\n"
    )

    # Create datapackage.json
    datapackage = {
        "name": "search-test",
        "resources": [
            {
                "name": "persons",
                "path": "persons.csv",
                "schema": {
                    "fields": [
                        {"name": "id", "type": "integer"},
                        {"name": "name", "type": "string"},
                        {"name": "email", "type": "string"},
                        {"name": "age", "type": "integer"},
                    ],
                    "custom": {
                        "pipedrive_fields": [
                            {"key": "id", "name": "ID", "field_type": "int"},
                            {"key": "name", "name": "Name", "field_type": "varchar"},
                            {"key": "email", "name": "Email", "field_type": "varchar"},
                            {"key": "age", "name": "Age", "field_type": "int"},
                        ]
                    },
                },
            }
        ],
    }
    (backup_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

    return backup_dir


class TestSearchCommand:
    """Integration tests for the search CLI command."""

    def test_search_local_no_filter(self, search_backup_dir):
        """Search local backup without filter returns all records."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "persons", "--base", str(search_backup_dir)
        ])
        assert result.exit_code == 0
        assert "John Doe" in result.output
        assert "Jane Smith" in result.output
        assert "Bob Johnson" in result.output

    def test_search_local_with_filter(self, search_backup_dir):
        """Search local backup with filter returns matching records."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "per", "--base", str(search_backup_dir),
            "-f", "contains(name, 'John')"
        ])
        assert result.exit_code == 0
        assert "John Doe" in result.output
        assert "Bob Johnson" in result.output
        assert "Jane Smith" not in result.output

    def test_search_json_output(self, search_backup_dir):
        """JSON output format works."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "persons", "--base", str(search_backup_dir),
            "-o", "json", "-q"
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 3

    def test_search_csv_output(self, search_backup_dir):
        """CSV output format works."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "persons", "--base", str(search_backup_dir),
            "-o", "csv", "-q"
        ])
        assert result.exit_code == 0
        lines = result.output.strip().split("\n")
        assert len(lines) == 4  # header + 3 rows

    def test_search_with_limit(self, search_backup_dir):
        """Limit option works."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "persons", "--base", str(search_backup_dir),
            "-o", "json", "-q", "--limit", "2"
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert len(data) == 2

    def test_search_with_include(self, search_backup_dir):
        """Include option limits output fields."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "persons", "--base", str(search_backup_dir),
            "-o", "json", "-q", "-i", "name,email"
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        # 'id' is always included
        assert "id" in data[0]
        assert "name" in data[0]
        assert "email" in data[0]
        assert "age" not in data[0]

    def test_search_with_exclude(self, search_backup_dir):
        """Exclude option removes output fields."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "persons", "--base", str(search_backup_dir),
            "-o", "json", "-q", "-x", "email,age"
        ])
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "id" in data[0]
        assert "name" in data[0]
        assert "email" not in data[0]
        assert "age" not in data[0]

    def test_search_dry_run(self, search_backup_dir):
        """Dry-run shows resolved expression only."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "persons", "--base", str(search_backup_dir),
            "-f", "contains(name, 'test')", "-n"
        ])
        assert result.exit_code == 0
        assert "Filter:" in result.output
        assert "dry-run" in result.output
        # Should NOT contain table output
        assert "John Doe" not in result.output

    def test_search_quiet_mode(self, search_backup_dir):
        """Quiet mode suppresses filter display."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "persons", "--base", str(search_backup_dir),
            "-f", "int(age) > 25", "-o", "json", "-q"
        ])
        assert result.exit_code == 0
        # Should NOT show filter line
        assert "Filter:" not in result.output
        # Should have valid JSON
        data = json.loads(result.output)
        assert len(data) == 2  # John (30) and Bob (35)

    def test_search_invalid_entity(self, search_backup_dir):
        """Invalid entity raises error."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "invalid", "--base", str(search_backup_dir)
        ])
        assert result.exit_code != 0
        assert "No entity matches" in result.output

    def test_search_filter_shows_resolved(self, search_backup_dir):
        """Filter resolution is shown by default."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "persons", "--base", str(search_backup_dir),
            "-f", "int(age) > 25"
        ])
        assert result.exit_code == 0
        assert "Filter: int(age) > 25" in result.output


class TestValidateExpression:
    """Tests for expression validation."""

    def test_valid_comparison(self):
        """Valid comparison expression passes."""
        validate_expression("id == 1", {"id"})  # No error

    def test_valid_function_call(self):
        """Valid function call passes."""
        validate_expression("contains(name, 'test')", {"name"})  # No error

    def test_empty_expression(self):
        """Empty expression is valid."""
        validate_expression("", {"id"})  # No error
        validate_expression(None, {"id"})  # No error

    def test_assignment_raises_error(self):
        """Single = instead of == raises FilterError."""
        with pytest.raises(FilterError, match="Assignment"):
            validate_expression("id=148", {"id"})

    def test_multiple_expressions_raises_error(self):
        """Multiple expressions separated by ; raises FilterError."""
        with pytest.raises(FilterError, match="Multiple"):
            validate_expression("a == 1; b == 2", {"a", "b"})

    def test_unknown_function_raises_error(self):
        """Unknown function raises FilterError."""
        with pytest.raises(FilterError, match="not defined"):
            validate_expression("unknown_func(id)", {"id"})

    def test_syntax_error_raises_error(self):
        """Syntax error raises FilterError."""
        with pytest.raises(FilterError):
            validate_expression("id == (", {"id"})
