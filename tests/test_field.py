"""Tests for field transformation module."""

import pytest

from pipedrive_cli.field import (
    TransformError,
    collect_unique_values,
    get_enum_options,
    transform_to_date,
    transform_to_double,
    transform_to_enum,
    transform_to_int,
    transform_to_set,
    transform_to_varchar,
    transform_value,
)


class TestTransformToInt:
    """Tests for transform_to_int function."""

    def test_int_passthrough(self):
        """Integer value passes through unchanged."""
        assert transform_to_int(42) == 42

    def test_float_rounds(self):
        """Float is rounded to nearest integer."""
        assert transform_to_int(3.7) == 4
        assert transform_to_int(3.2) == 3
        assert transform_to_int(3.5) == 4

    def test_string_int(self):
        """String containing integer is converted."""
        assert transform_to_int("42") == 42
        assert transform_to_int("  42  ") == 42

    def test_string_float(self):
        """String containing float is converted and rounded."""
        assert transform_to_int("3.7") == 4
        assert transform_to_int("3.2") == 3

    def test_null_raises(self):
        """None value raises TransformError."""
        with pytest.raises(TransformError) as exc_info:
            transform_to_int(None)
        assert "null" in str(exc_info.value)

    def test_empty_string_raises(self):
        """Empty string raises TransformError."""
        with pytest.raises(TransformError) as exc_info:
            transform_to_int("")
        assert "empty" in str(exc_info.value)

    def test_invalid_string_raises(self):
        """Non-numeric string raises TransformError."""
        with pytest.raises(TransformError) as exc_info:
            transform_to_int("abc")
        assert "invalid" in str(exc_info.value)


class TestTransformToDouble:
    """Tests for transform_to_double function."""

    def test_float_passthrough(self):
        """Float value passes through unchanged."""
        assert transform_to_double(3.14) == 3.14

    def test_int_converts(self):
        """Integer is converted to float."""
        assert transform_to_double(42) == 42.0

    def test_string_float(self):
        """String containing float is converted."""
        assert transform_to_double("3.14") == 3.14
        assert transform_to_double("  3.14  ") == 3.14

    def test_null_raises(self):
        """None value raises TransformError."""
        with pytest.raises(TransformError):
            transform_to_double(None)

    def test_empty_string_raises(self):
        """Empty string raises TransformError."""
        with pytest.raises(TransformError):
            transform_to_double("")


class TestTransformToVarchar:
    """Tests for transform_to_varchar function."""

    def test_basic_types(self):
        """Basic types convert to string."""
        assert transform_to_varchar(42) == "42"
        assert transform_to_varchar(3.14) == "3.14"
        assert transform_to_varchar("hello") == "hello"

    def test_list_joins(self):
        """List values are joined with separator."""
        assert transform_to_varchar(["a", "b", "c"]) == "a, b, c"
        assert transform_to_varchar(["a", "b"], separator=";") == "a;b"

    def test_list_with_dict_labels(self):
        """List of dicts with labels extracts labels."""
        values = [{"label": "Option 1"}, {"label": "Option 2"}]
        assert transform_to_varchar(values) == "Option 1, Option 2"

    def test_dict_with_label(self):
        """Dict with label extracts label."""
        assert transform_to_varchar({"label": "My Label"}) == "My Label"

    def test_float_with_format(self):
        """Float with format string applies formatting."""
        assert transform_to_varchar(3.14159, format_str=".2f") == "3.14"

    def test_date_with_format(self):
        """Date string with output format converts."""
        assert transform_to_varchar("2024-01-15", format_str="%d/%m/%Y") == "15/01/2024"

    def test_null_raises(self):
        """None value raises TransformError."""
        with pytest.raises(TransformError):
            transform_to_varchar(None)


class TestTransformToDate:
    """Tests for transform_to_date function."""

    def test_iso_date_passthrough(self):
        """ISO date string passes through."""
        assert transform_to_date("2024-01-15") == "2024-01-15"

    def test_custom_format_converts(self):
        """Custom format string converts to ISO."""
        assert transform_to_date("15/01/2024", format_str="%d/%m/%Y") == "2024-01-15"
        assert transform_to_date("01-15-2024", format_str="%m-%d-%Y") == "2024-01-15"

    def test_null_raises(self):
        """None value raises TransformError."""
        with pytest.raises(TransformError):
            transform_to_date(None)

    def test_empty_string_raises(self):
        """Empty string raises TransformError."""
        with pytest.raises(TransformError):
            transform_to_date("")

    def test_unknown_format_raises(self):
        """Unknown date format without --format raises error."""
        with pytest.raises(TransformError) as exc_info:
            transform_to_date("15-01-2024")
        assert "unknown" in str(exc_info.value).lower() or "format" in str(exc_info.value).lower()


class TestTransformToEnum:
    """Tests for transform_to_enum function."""

    def test_string_strips(self):
        """String value is stripped."""
        assert transform_to_enum("  option  ") == "option"

    def test_single_set_element(self):
        """Single set element converts to enum."""
        assert transform_to_enum(["option"]) == "option"

    def test_set_with_dict_labels(self):
        """Set with dict labels extracts label."""
        assert transform_to_enum([{"label": "Option 1"}]) == "Option 1"

    def test_multiple_set_elements_raises(self):
        """Multiple set elements raises error."""
        with pytest.raises(TransformError) as exc_info:
            transform_to_enum(["a", "b"])
        assert "multiple" in str(exc_info.value).lower()

    def test_empty_set_raises(self):
        """Empty set raises error."""
        with pytest.raises(TransformError):
            transform_to_enum([])

    def test_null_raises(self):
        """None value raises TransformError."""
        with pytest.raises(TransformError):
            transform_to_enum(None)


class TestTransformToSet:
    """Tests for transform_to_set function."""

    def test_string_splits(self):
        """String is split by separator."""
        assert transform_to_set("a,b,c") == ["a", "b", "c"]
        assert transform_to_set("a; b; c", separator=";") == ["a", "b", "c"]

    def test_string_strips_values(self):
        """Values are stripped of whitespace."""
        assert transform_to_set("  a  ,  b  ,  c  ") == ["a", "b", "c"]

    def test_list_passthrough(self):
        """List passes through with stripping."""
        assert transform_to_set(["  a  ", "  b  "]) == ["a", "b"]

    def test_list_with_dict_labels(self):
        """List of dicts extracts labels."""
        values = [{"label": " Option 1 "}, {"label": " Option 2 "}]
        assert transform_to_set(values) == ["Option 1", "Option 2"]

    def test_enum_to_set(self):
        """Enum dict converts to single-element set."""
        assert transform_to_set({"label": "Option"}) == ["Option"]

    def test_null_raises(self):
        """None value raises TransformError."""
        with pytest.raises(TransformError):
            transform_to_set(None)

    def test_empty_string_raises(self):
        """Empty string raises TransformError."""
        with pytest.raises(TransformError):
            transform_to_set("")


class TestTransformValue:
    """Tests for transform_value wrapper function."""

    def test_no_transform_passthrough(self):
        """No transform type passes value through."""
        result = transform_value("hello", None)
        assert result.success
        assert result.value == "hello"

    def test_valid_transform(self):
        """Valid transform returns success."""
        result = transform_value("42", "int")
        assert result.success
        assert result.value == 42

    def test_invalid_transform_returns_error(self):
        """Invalid transform returns error result."""
        result = transform_value("abc", "int")
        assert not result.success
        assert result.error is not None

    def test_unknown_transform_type(self):
        """Unknown transform type returns error."""
        result = transform_value("hello", "unknown_type")
        assert not result.success
        assert "unknown" in result.error.lower()


class TestCollectUniqueValues:
    """Tests for collect_unique_values function."""

    def test_string_values(self):
        """Collects unique string values."""
        records = [
            {"status": "active"},
            {"status": "inactive"},
            {"status": "active"},
        ]
        values = collect_unique_values(records, "status")
        assert values == {"active", "inactive"}

    def test_strips_whitespace(self):
        """Strips whitespace from values."""
        records = [
            {"status": "  active  "},
            {"status": "active"},
        ]
        values = collect_unique_values(records, "status")
        assert values == {"active"}

    def test_skips_null(self):
        """Skips null values."""
        records = [
            {"status": "active"},
            {"status": None},
        ]
        values = collect_unique_values(records, "status")
        assert values == {"active"}

    def test_skips_empty(self):
        """Skips empty strings."""
        records = [
            {"status": "active"},
            {"status": ""},
            {"status": "   "},
        ]
        values = collect_unique_values(records, "status")
        assert values == {"active"}

    def test_extracts_from_lists(self):
        """Extracts values from list fields (sets)."""
        records = [
            {"tags": ["a", "b"]},
            {"tags": ["b", "c"]},
        ]
        values = collect_unique_values(records, "tags")
        assert values == {"a", "b", "c"}

    def test_extracts_labels_from_dicts(self):
        """Extracts labels from dict values (enums)."""
        records = [
            {"status": {"label": "Active"}},
            {"status": {"label": "Inactive"}},
        ]
        values = collect_unique_values(records, "status")
        assert values == {"Active", "Inactive"}


class TestGetEnumOptions:
    """Tests for get_enum_options function."""

    def test_extracts_labels(self):
        """Extracts option labels from field definition."""
        field = {
            "options": [
                {"id": 1, "label": "Option 1"},
                {"id": 2, "label": "Option 2"},
            ]
        }
        options = get_enum_options(field)
        assert options == {"Option 1", "Option 2"}

    def test_empty_options(self):
        """Returns empty set for field without options."""
        field = {"options": []}
        assert get_enum_options(field) == set()

    def test_no_options_key(self):
        """Returns empty set for field without options key."""
        field = {}
        assert get_enum_options(field) == set()
