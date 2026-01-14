"""Tests for search module and search command."""

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipedrive_cli.cli import main
from pipedrive_cli.expressions import FILTER_FUNCTIONS, EnumValue, resolve_field_name
from pipedrive_cli.matching import AmbiguousMatchError
from pipedrive_cli.search import (
    FilterError,
    extract_filter_keys,
    filter_record,
    format_csv,
    format_json,
    preprocess_record_for_filter,
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

    def test_resolve_digit_starting_key_prefix(self):
        """Key prefix starting with digits (hex-like) is resolved with _ escape."""
        fields = [
            {"key": "25da23b938af0807ec37bba8be25d77bae233536", "name": "Code"},
            {"key": "b85f32437e17e520e0c1173f4c3c887563d90de8", "name": "Type"},
        ]
        expr = "25da != b85f"
        result, resolutions = resolve_filter_expression(fields, expr)
        # Digit-starting key should be escaped with _ prefix
        expected = (
            "_25da23b938af0807ec37bba8be25d77bae233536 != "
            "b85f32437e17e520e0c1173f4c3c887563d90de8"
        )
        assert result == expected
        assert "25da" in resolutions
        assert resolutions["25da"][0] == "25da23b938af0807ec37bba8be25d77bae233536"

    def test_numeric_literal_not_resolved(self):
        """Pure numeric literals are not resolved to fields."""
        fields = [
            {"key": "25da23b938af0807ec37bba8be25d77bae233536", "name": "Code"},
        ]
        expr = "int(name) > 25"
        result, resolutions = resolve_filter_expression(fields, expr)
        # '25' should not be resolved (pure number, no hex letters)
        assert result == expr
        assert resolutions == {}

    def test_digit_hex_in_string_literal_not_resolved(self):
        """Digit-hex patterns inside string literals are not resolved."""
        fields = [
            {"key": "25da23b938af0807ec37bba8be25d77bae233536", "name": "Code"},
        ]
        expr = "contains(name, '25da')"
        result, resolutions = resolve_filter_expression(fields, expr)
        assert result == expr
        assert resolutions == {}

    def test_user_escaped_digit_prefix(self):
        """User can prefix digit-starting keys with _ to reference them."""
        fields = [
            {"key": "25da23b938af0807ec37bba8be25d77bae233536", "name": "Code"},
        ]
        # User explicitly uses _25 to reference the digit-starting key
        expr = "notnull(_25)"
        result, resolutions = resolve_filter_expression(fields, expr)
        assert "_25da23b938af0807ec37bba8be25d77bae233536" in result
        assert "_25" in resolutions
        assert resolutions["_25"][0] == "25da23b938af0807ec37bba8be25d77bae233536"


class TestFieldFunction:
    """Tests for field("name") exact name lookup in expressions."""

    @pytest.fixture
    def sample_fields(self) -> list[dict]:
        """Sample field definitions with various names."""
        return [
            {"key": "id", "name": "ID"},
            {"key": "first_name", "name": "First Name"},
            {"key": "last_name", "name": "Last Name"},
            {"key": "b85f32437e17e520e0c1173f4c3c887563d90de8", "name": "Civilité"},
            {"key": "25da23b938af0807ec37bba8be25d77bae233536", "name": "Code-123"},
            {"key": "custom_field", "name": "My Custom Field"},
        ]

    def test_field_double_quotes(self, sample_fields):
        """field("name") with double quotes resolves to field key."""
        expr = 'notnull(field("First Name"))'
        result, resolutions = resolve_filter_expression(sample_fields, expr)
        assert result == "notnull(first_name)"
        assert 'field("First Name")' in resolutions
        assert resolutions['field("First Name")'] == ("first_name", "First Name")

    def test_field_single_quotes(self, sample_fields):
        """field('name') with single quotes resolves to field key."""
        expr = "notnull(field('First Name'))"
        result, resolutions = resolve_filter_expression(sample_fields, expr)
        assert result == "notnull(first_name)"
        assert "field('First Name')" in resolutions

    def test_field_accented_characters(self, sample_fields):
        """field() with accented characters resolves correctly."""
        expr = 'notnull(field("Civilité"))'
        result, resolutions = resolve_filter_expression(sample_fields, expr)
        assert result == "notnull(b85f32437e17e520e0c1173f4c3c887563d90de8)"
        assert 'field("Civilité")' in resolutions

    def test_field_special_characters(self, sample_fields):
        """field() with special characters (hyphen) resolves correctly."""
        expr = 'field("Code-123") == "ABC"'
        result, resolutions = resolve_filter_expression(sample_fields, expr)
        # Key starts with digit, should be escaped
        assert "_25da23b938af0807ec37bba8be25d77bae233536" in result
        assert 'field("Code-123")' in resolutions

    def test_field_case_insensitive(self, sample_fields):
        """field() matching is case-insensitive."""
        expr = 'notnull(field("first name"))'  # lowercase
        result, resolutions = resolve_filter_expression(sample_fields, expr)
        assert result == "notnull(first_name)"

    def test_field_not_found(self, sample_fields):
        """field() with unknown name raises FilterError."""
        expr = 'notnull(field("Unknown Field"))'
        with pytest.raises(FilterError) as exc_info:
            resolve_filter_expression(sample_fields, expr)
        assert "Field not found: 'Unknown Field'" in str(exc_info.value)

    def test_multiple_field_calls(self, sample_fields):
        """Multiple field() calls in one expression."""
        expr = 'field("First Name") == "John" and notnull(field("Civilité"))'
        result, resolutions = resolve_filter_expression(sample_fields, expr)
        assert "first_name" in result
        assert "b85f32437e17e520e0c1173f4c3c887563d90de8" in result
        assert len(resolutions) == 2

    def test_field_mixed_with_identifiers(self, sample_fields):
        """field() can be mixed with regular identifiers."""
        expr = 'field("First Name") == "John" and notnull(last)'
        result, resolutions = resolve_filter_expression(sample_fields, expr)
        assert "first_name" in result
        assert "last_name" in result
        assert 'field("First Name")' in resolutions
        assert "last" in resolutions

    def test_field_with_whitespace(self, sample_fields):
        """field() tolerates whitespace around name."""
        expr = 'notnull(field(  "First Name"  ))'
        result, resolutions = resolve_filter_expression(sample_fields, expr)
        assert result == "notnull(first_name)"


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

    def test_escaped_digit_starting_keys(self):
        """Escaped digit-starting keys are extracted without underscore prefix."""
        fields = [
            {"key": "25da23b938af0807ec37bba8be25d77bae233536", "name": "Code"},
            {"key": "b85f32437e17e520e0c1173f4c3c887563d90de8", "name": "Type"},
        ]
        # Resolved expression has _25da... (escaped) and b85f... (normal)
        resolved = (
            "_25da23b938af0807ec37bba8be25d77bae233536 != "
            "b85f32437e17e520e0c1173f4c3c887563d90de8"
        )
        result = extract_filter_keys(fields, resolved)
        # Both keys should be extracted (without the escape underscore)
        assert "25da23b938af0807ec37bba8be25d77bae233536" in result
        assert "b85f32437e17e520e0c1173f4c3c887563d90de8" in result


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


class TestResolveFieldName:
    """Tests for resolve_field_name function."""

    @pytest.fixture
    def sample_fields(self) -> list[dict]:
        """Sample field definitions."""
        return [
            {"key": "id", "name": "ID"},
            {"key": "first_name", "name": "First Name"},
            {"key": "last_name", "name": "Last Name"},
            {"key": "abc123", "name": "Civilité"},
            {"key": "def456", "name": "Code-123"},
        ]

    def test_exact_match(self, sample_fields):
        """Exact name match returns field key."""
        result = resolve_field_name(sample_fields, "First Name")
        assert result == "first_name"

    def test_case_insensitive(self, sample_fields):
        """Name matching is case-insensitive."""
        result = resolve_field_name(sample_fields, "first name")
        assert result == "first_name"
        result = resolve_field_name(sample_fields, "FIRST NAME")
        assert result == "first_name"

    def test_accented_characters(self, sample_fields):
        """Accented characters are matched correctly."""
        result = resolve_field_name(sample_fields, "Civilité")
        assert result == "abc123"

    def test_special_characters(self, sample_fields):
        """Special characters (hyphen, numbers) are matched."""
        result = resolve_field_name(sample_fields, "Code-123")
        assert result == "def456"

    def test_not_found(self, sample_fields):
        """Returns None when name not found."""
        result = resolve_field_name(sample_fields, "Unknown Field")
        assert result is None

    def test_partial_name_not_matched(self, sample_fields):
        """Partial name does not match (exact match required)."""
        result = resolve_field_name(sample_fields, "First")
        assert result is None

    def test_multiple_matches_returns_none(self):
        """Returns None if multiple fields have the same name."""
        fields = [
            {"key": "field1", "name": "Duplicate Name"},
            {"key": "field2", "name": "Duplicate Name"},
        ]
        result = resolve_field_name(fields, "Duplicate Name")
        assert result is None


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

    def test_field_function_double_quotes(self, sample_fields):
        """field("name") syntax resolves to field key."""
        result = resolve_field_prefixes(sample_fields, ['field("Custom")'])
        assert result == ["abc123_custom"]

    def test_field_function_single_quotes(self, sample_fields):
        """field('name') syntax resolves to field key."""
        result = resolve_field_prefixes(sample_fields, ["field('Custom')"])
        assert result == ["abc123_custom"]

    def test_field_function_case_insensitive(self, sample_fields):
        """field() matching is case-insensitive."""
        result = resolve_field_prefixes(sample_fields, ['field("custom")'])
        assert result == ["abc123_custom"]

    def test_field_function_not_found(self, sample_fields):
        """field() with unknown name is silently skipped."""
        result = resolve_field_prefixes(sample_fields, ['field("Unknown")'])
        assert result == []

    def test_field_function_mixed_with_prefixes(self, sample_fields):
        """field() can be mixed with regular prefixes."""
        result = resolve_field_prefixes(sample_fields, ['field("Name")', "email"])
        assert "name" in result
        assert "email" in result

    def test_field_function_with_accented_name(self):
        """field() with accented characters resolves correctly."""
        fields = [
            {"key": "abc123", "name": "Civilité"},
            {"key": "def456", "name": "Prénom"},
        ]
        result = resolve_field_prefixes(fields, ['field("Civilité")'])
        assert result == ["abc123"]

    def test_field_function_deduplication(self, sample_fields):
        """Duplicate field() calls are deduplicated."""
        result = resolve_field_prefixes(
            sample_fields, ['field("Name")', 'field("Name")']
        )
        assert result == ["name"]


class TestResolveFieldPrefixesDigitKeys:
    """Tests for resolve_field_prefixes() with digit-starting keys."""

    @pytest.fixture
    def fields_with_digit_keys(self) -> list[dict]:
        """Field definitions including digit-starting keys."""
        return [
            {"key": "id", "name": "ID"},
            {"key": "name", "name": "Name"},
            {"key": "25da23b938af", "name": "Custom Phone"},
            {"key": "b85f1c2d3e4f", "name": "Custom Email"},
        ]

    def test_escaped_digit_key_prefix(self, fields_with_digit_keys):
        """Escaped digit-key prefix (_25) resolves correctly."""
        result = resolve_field_prefixes(fields_with_digit_keys, ["_25"])
        assert result == ["25da23b938af"]

    def test_escaped_digit_key_full(self, fields_with_digit_keys):
        """Escaped full digit-key prefix (_25da) resolves correctly."""
        result = resolve_field_prefixes(fields_with_digit_keys, ["_25da"])
        assert result == ["25da23b938af"]

    def test_letter_starting_key(self, fields_with_digit_keys):
        """Letter-starting key prefix (b85f) resolves without escape."""
        result = resolve_field_prefixes(fields_with_digit_keys, ["b85f"])
        assert result == ["b85f1c2d3e4f"]

    def test_mixed_digit_and_regular_keys(self, fields_with_digit_keys):
        """Mixed digit-starting and regular keys resolve correctly."""
        result = resolve_field_prefixes(fields_with_digit_keys, ["_25da", "b85f", "name"])
        assert "25da23b938af" in result
        assert "b85f1c2d3e4f" in result
        assert "name" in result
        assert len(result) == 3

    def test_underscore_without_digit_not_escape(self, fields_with_digit_keys):
        """Underscore without following digit is not escape."""
        # _abc should not match anything (no key starts with 'abc')
        result = resolve_field_prefixes(fields_with_digit_keys, ["_abc"])
        assert result == []


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


@pytest.fixture
def modified_types_backup_dir(tmp_path: Path) -> Path:
    """Create a backup with fields that have modified type mappings.

    Tests fields where Pipedrive type differs from intuitive mapping:
    - visible_to: stored as string (API returns "3", not 3)
    - address: stored as string (not object)
    - org_id: stored as integer (extracted from API object)
    - person_id: stored as integer (extracted from API object)
    """
    backup_dir = tmp_path / "modified-types-test"
    backup_dir.mkdir()

    # Create organizations.csv with modified type fields
    orgs_csv = backup_dir / "organizations.csv"
    orgs_csv.write_text(
        "id,name,visible_to,address,owner_id\n"
        "1,ACME Corp,3,123 Main St Paris,100\n"
        "2,Beta Inc,1,456 Oak Ave London,101\n"
        "3,Gamma LLC,3,789 Pine Rd Berlin,100\n"
    )

    # Create deals.csv with reference fields
    deals_csv = backup_dir / "deals.csv"
    deals_csv.write_text(
        "id,title,org_id,person_id,value\n"
        "1,Big Deal,1,10,50000\n"
        "2,Small Deal,2,11,5000\n"
        "3,Medium Deal,1,12,25000\n"
    )

    datapackage = {
        "name": "modified-types-test",
        "resources": [
            {
                "name": "organizations",
                "path": "organizations.csv",
                "schema": {
                    "fields": [
                        {"name": "id", "type": "integer"},
                        {"name": "name", "type": "string"},
                        {"name": "visible_to", "type": "string"},
                        {"name": "address", "type": "string"},
                        {"name": "owner_id", "type": "integer"},
                    ],
                    "pipedrive_fields": [
                        {"key": "id", "name": "ID", "field_type": "int"},
                        {"key": "name", "name": "Name", "field_type": "varchar"},
                        {
                            "key": "visible_to", "name": "Visible to",
                            "field_type": "visible_to",
                        },
                        {"key": "address", "name": "Address", "field_type": "address"},
                        {"key": "owner_id", "name": "Owner", "field_type": "user"},
                    ],
                },
            },
            {
                "name": "deals",
                "path": "deals.csv",
                "schema": {
                    "fields": [
                        {"name": "id", "type": "integer"},
                        {"name": "title", "type": "string"},
                        {"name": "org_id", "type": "integer"},
                        {"name": "person_id", "type": "integer"},
                        {"name": "value", "type": "integer"},
                    ],
                    "pipedrive_fields": [
                        {"key": "id", "name": "ID", "field_type": "int"},
                        {"key": "title", "name": "Title", "field_type": "varchar"},
                        {"key": "org_id", "name": "Organization", "field_type": "org"},
                        {"key": "person_id", "name": "Person", "field_type": "people"},
                        {"key": "value", "name": "Value", "field_type": "double"},
                    ],
                },
            },
        ],
    }
    (backup_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

    return backup_dir


class TestSearchModifiedTypeFields:
    """Tests for filter expressions on fields with modified type mappings.

    These fields have non-obvious type mappings that were fixed:
    - visible_to: API returns string "3", not integer 3
    - address: API returns formatted string, not object
    - org_id, person_id, owner_id: API returns object, we store integer ID
    """

    def test_filter_visible_to_string_comparison(self, modified_types_backup_dir):
        """visible_to field can be filtered as string."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "org", "--base", str(modified_types_backup_dir),
            "-f", "visible_to == '3'", "-q"
        ])
        assert result.exit_code == 0
        assert "ACME Corp" in result.output
        assert "Gamma LLC" in result.output
        assert "Beta Inc" not in result.output

    def test_filter_visible_to_not_integer(self, modified_types_backup_dir):
        """visible_to comparison as integer would fail (stored as string)."""
        runner = CliRunner()
        # This works because CSV stores "3" as string, int("3") == 3
        result = runner.invoke(main, [
            "search", "-e", "org", "--base", str(modified_types_backup_dir),
            "-f", "int(visible_to) == 3", "-q"
        ])
        assert result.exit_code == 0
        assert "ACME Corp" in result.output

    def test_filter_address_contains(self, modified_types_backup_dir):
        """address field can be filtered with string functions."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "org", "--base", str(modified_types_backup_dir),
            "-f", "contains(address, 'Paris')", "-q"
        ])
        assert result.exit_code == 0
        assert "ACME Corp" in result.output
        assert "Beta Inc" not in result.output
        assert "Gamma LLC" not in result.output

    def test_filter_address_startswith(self, modified_types_backup_dir):
        """address field works with startswith."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "org", "--base", str(modified_types_backup_dir),
            "-f", "startswith(address, '123')", "-q"
        ])
        assert result.exit_code == 0
        assert "ACME Corp" in result.output
        assert "Beta Inc" not in result.output

    def test_filter_org_id_integer_comparison(self, modified_types_backup_dir):
        """org_id (reference field) can be filtered as integer."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "deals", "--base", str(modified_types_backup_dir),
            "-f", "int(org_id) == 1", "-q"
        ])
        assert result.exit_code == 0
        assert "Big Deal" in result.output
        assert "Medium Deal" in result.output
        assert "Small Deal" not in result.output

    def test_filter_org_id_string_comparison(self, modified_types_backup_dir):
        """org_id needs str() to compare as string (auto-coerced to int).

        With auto-coercion enabled, org_id is now an integer, so direct
        string comparison like `org_id == '1'` won't match. Use str() explicitly.
        """
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "deals", "--base", str(modified_types_backup_dir),
            "-f", "str(org_id) == '1'", "-q"
        ])
        assert result.exit_code == 0
        assert "Big Deal" in result.output
        assert "Medium Deal" in result.output

    def test_filter_person_id_greater_than(self, modified_types_backup_dir):
        """person_id (reference field) works with numeric comparison."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "deals", "--base", str(modified_types_backup_dir),
            "-f", "int(person_id) > 10", "-q"
        ])
        assert result.exit_code == 0
        assert "Small Deal" in result.output
        assert "Medium Deal" in result.output
        assert "Big Deal" not in result.output

    def test_filter_owner_id_equality(self, modified_types_backup_dir):
        """owner_id (user reference field) can be filtered."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "org", "--base", str(modified_types_backup_dir),
            "-f", "int(owner_id) == 100", "-q"
        ])
        assert result.exit_code == 0
        assert "ACME Corp" in result.output
        assert "Gamma LLC" in result.output
        assert "Beta Inc" not in result.output

    def test_filter_combined_reference_and_value(self, modified_types_backup_dir):
        """Combined filter on reference field and regular field."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "deals", "--base", str(modified_types_backup_dir),
            "-f", "int(org_id) == 1 and int(value) > 10000", "-q"
        ])
        assert result.exit_code == 0
        assert "Big Deal" in result.output
        assert "Medium Deal" in result.output
        assert "Small Deal" not in result.output

    def test_filter_notnull_on_reference_field(self, modified_types_backup_dir):
        """notnull works on reference fields."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "deals", "--base", str(modified_types_backup_dir),
            "-f", "notnull(org_id)", "-q"
        ])
        assert result.exit_code == 0
        # All deals have org_id
        assert "Big Deal" in result.output
        assert "Small Deal" in result.output
        assert "Medium Deal" in result.output


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


class TestEnumValue:
    """Tests for EnumValue wrapper class."""

    def test_eq_with_int_id(self):
        """EnumValue matches integer ID."""
        ev = EnumValue("37", "Monsieur")
        assert ev == 37
        assert ev != 38

    def test_eq_with_string_id(self):
        """EnumValue matches string ID."""
        ev = EnumValue("37", "Monsieur")
        assert ev == "37"
        assert ev != "38"

    def test_eq_with_label(self):
        """EnumValue matches label text."""
        ev = EnumValue("37", "Monsieur")
        assert ev == "Monsieur"
        assert ev != "Madame"

    def test_eq_with_label_case_insensitive(self):
        """EnumValue matches label case-insensitively."""
        ev = EnumValue("37", "Monsieur")
        assert ev == "monsieur"
        assert ev == "MONSIEUR"
        assert ev == "MoNsIeUr"

    def test_ne_works(self):
        """EnumValue != works correctly."""
        ev = EnumValue("37", "Monsieur")
        assert ev != 38
        assert ev != "38"
        assert ev != "Madame"
        assert not (ev != 37)
        assert not (ev != "Monsieur")

    def test_str_returns_raw_id(self):
        """str(EnumValue) returns raw ID."""
        ev = EnumValue("37", "Monsieur")
        assert str(ev) == "37"

    def test_repr(self):
        """repr(EnumValue) returns readable format."""
        ev = EnumValue("37", "Monsieur")
        assert repr(ev) == "EnumValue('37', 'Monsieur')"

    def test_none_label(self):
        """EnumValue with None label only matches ID."""
        ev = EnumValue("99", None)
        assert ev == 99
        assert ev == "99"
        assert ev != "SomeLabel"

    def test_eq_with_other_types(self):
        """EnumValue returns False for non-matching types."""
        ev = EnumValue("37", "Monsieur")
        assert ev != [37]
        assert ev != {"id": 37}
        assert ev != 37.0  # float is not int or str


class TestPreprocessRecordForFilter:
    """Tests for preprocess_record_for_filter function."""

    def test_wraps_enum_values(self):
        """Enum field values are wrapped in EnumValue."""
        record = {"id": 1, "status": "37", "name": "Test"}
        option_lookup = {"status": {"37": "Active", "38": "Inactive"}}

        processed = preprocess_record_for_filter(record, option_lookup)

        assert isinstance(processed["status"], EnumValue)
        assert processed["status"] == 37
        assert processed["status"] == "Active"
        # Non-enum fields unchanged
        assert processed["id"] == 1
        assert processed["name"] == "Test"

    def test_empty_option_lookup(self):
        """Empty option_lookup returns original record."""
        record = {"id": 1, "status": "37"}
        processed = preprocess_record_for_filter(record, {})
        assert processed is record

    def test_skips_missing_fields(self):
        """Fields not in record are skipped."""
        record = {"id": 1}
        option_lookup = {"status": {"37": "Active"}}

        processed = preprocess_record_for_filter(record, option_lookup)
        assert "status" not in processed

    def test_skips_none_values(self):
        """None values are not wrapped."""
        record = {"id": 1, "status": None}
        option_lookup = {"status": {"37": "Active"}}

        processed = preprocess_record_for_filter(record, option_lookup)
        assert processed["status"] is None

    def test_skips_empty_string_values(self):
        """Empty string values are not wrapped."""
        record = {"id": 1, "status": ""}
        option_lookup = {"status": {"37": "Active"}}

        processed = preprocess_record_for_filter(record, option_lookup)
        assert processed["status"] == ""

    def test_unknown_option_id(self):
        """Unknown option ID is wrapped with None label."""
        record = {"id": 1, "status": "99"}
        option_lookup = {"status": {"37": "Active"}}

        processed = preprocess_record_for_filter(record, option_lookup)
        assert isinstance(processed["status"], EnumValue)
        assert processed["status"] == 99
        assert processed["status"] == "99"
        assert processed["status"] != "Active"  # No label match

    def test_original_record_unchanged(self):
        """Original record is not mutated."""
        record = {"id": 1, "status": "37"}
        option_lookup = {"status": {"37": "Active"}}

        processed = preprocess_record_for_filter(record, option_lookup)

        assert record["status"] == "37"  # Original unchanged
        assert isinstance(processed["status"], EnumValue)

    def test_filter_with_enum_value(self):
        """Filter evaluation works with preprocessed record."""
        record = {"id": 1, "status": "37"}
        option_lookup = {"status": {"37": "Active", "38": "Inactive"}}

        processed = preprocess_record_for_filter(record, option_lookup)

        # Filter by int ID
        assert filter_record(processed, "status == 37") is True
        assert filter_record(processed, "status == 38") is False

        # Filter by string ID
        assert filter_record(processed, "status == '37'") is True

        # Filter by label
        assert filter_record(processed, "status == 'Active'") is True
        assert filter_record(processed, "status == 'Inactive'") is False

        # Filter by label (case-insensitive)
        assert filter_record(processed, "status == 'active'") is True


class TestSearchWithAutoTypeCoercion:
    """Tests for automatic type coercion when searching local datapackages.

    CSV data is now automatically coerced according to the Frictionless schema types.
    This means numeric comparisons work without explicit int() or float() calls.
    """

    def test_search_id_integer_equality_direct(self, modified_types_backup_dir):
        """id == 1 works directly without quotes or int().

        This was the original issue: `id == 462` failed, `id == "462"` worked.
        """
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "org", "--base", str(modified_types_backup_dir),
            "-f", "id == 1", "-q"
        ])
        assert result.exit_code == 0
        assert "ACME Corp" in result.output
        assert "Beta Inc" not in result.output
        assert "Gamma LLC" not in result.output

    def test_search_id_integer_greater_than(self, modified_types_backup_dir):
        """id > N comparison works directly."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "org", "--base", str(modified_types_backup_dir),
            "-f", "id > 1", "-q"
        ])
        assert result.exit_code == 0
        assert "ACME Corp" not in result.output
        assert "Beta Inc" in result.output
        assert "Gamma LLC" in result.output

    def test_search_org_id_integer_equality_direct(self, modified_types_backup_dir):
        """org_id == 1 works directly without int().

        Previously required: int(org_id) == 1
        """
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "deals", "--base", str(modified_types_backup_dir),
            "-f", "org_id == 1", "-q"
        ])
        assert result.exit_code == 0
        assert "Big Deal" in result.output
        assert "Medium Deal" in result.output
        assert "Small Deal" not in result.output

    def test_search_value_numeric_comparison(self, modified_types_backup_dir):
        """value > 10000 numeric comparison works directly."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "deals", "--base", str(modified_types_backup_dir),
            "-f", "value > 10000", "-q"
        ])
        assert result.exit_code == 0
        assert "Big Deal" in result.output
        assert "Medium Deal" in result.output
        assert "Small Deal" not in result.output

    def test_search_owner_id_equality_direct(self, modified_types_backup_dir):
        """owner_id == 100 works directly without int().

        Previously required: int(owner_id) == 100
        """
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "org", "--base", str(modified_types_backup_dir),
            "-f", "owner_id == 100", "-q"
        ])
        assert result.exit_code == 0
        assert "ACME Corp" in result.output
        assert "Gamma LLC" in result.output
        assert "Beta Inc" not in result.output

    def test_search_combined_filter(self, modified_types_backup_dir):
        """Combined numeric filters work directly."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "deals", "--base", str(modified_types_backup_dir),
            "-f", "org_id == 1 and value > 30000", "-q"
        ])
        assert result.exit_code == 0
        assert "Big Deal" in result.output
        assert "Medium Deal" not in result.output
        assert "Small Deal" not in result.output

    def test_search_string_comparison_still_works(self, modified_types_backup_dir):
        """String comparison still works for string typed fields."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "org", "--base", str(modified_types_backup_dir),
            "-f", "visible_to == '3'", "-q"
        ])
        assert result.exit_code == 0
        assert "ACME Corp" in result.output
        assert "Gamma LLC" in result.output

    def test_search_explicit_int_still_works(self, modified_types_backup_dir):
        """Explicit int() conversion still works (backwards compatibility)."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "search", "-e", "deals", "--base", str(modified_types_backup_dir),
            "-f", "int(org_id) == 1", "-q"
        ])
        assert result.exit_code == 0
        assert "Big Deal" in result.output
