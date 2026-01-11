"""Tests for restore functionality."""


from pipedrive_cli.restore import (
    clean_record,
    convert_record_for_api,
    extract_reference_id,
    parse_csv_value,
)


class TestCleanRecord:
    """Tests for clean_record function."""

    def test_removes_readonly_fields(self):
        """clean_record removes read-only fields."""
        record = {
            "id": 1,
            "name": "John Doe",
            "add_time": "2024-01-01",
            "update_time": "2024-01-02",
            "creator_user_id": 123,
        }
        cleaned = clean_record(record)

        assert "id" not in cleaned
        assert "add_time" not in cleaned
        assert "update_time" not in cleaned
        assert "creator_user_id" not in cleaned
        assert cleaned["name"] == "John Doe"

    def test_removes_none_values(self):
        """clean_record removes None values."""
        record = {
            "name": "John Doe",
            "email": None,
            "phone": "+1234567890",
        }
        cleaned = clean_record(record)

        assert "email" not in cleaned
        assert cleaned["name"] == "John Doe"
        assert cleaned["phone"] == "+1234567890"

    def test_keeps_writable_fields(self):
        """clean_record keeps writable fields."""
        record = {
            "name": "John Doe",
            "email": "john@example.com",
            "phone": "+1234567890",
            "org_id": 1,
        }
        cleaned = clean_record(record)

        assert cleaned["name"] == "John Doe"
        assert cleaned["email"] == "john@example.com"
        assert cleaned["phone"] == "+1234567890"
        assert cleaned["org_id"] == 1


class TestParseCsvValue:
    """Tests for parse_csv_value function."""

    def test_parse_empty_string(self):
        """Empty string returns None."""
        assert parse_csv_value("") is None

    def test_parse_json_object(self):
        """JSON object string is parsed."""
        result = parse_csv_value('{"key": "value"}')
        assert result == {"key": "value"}

    def test_parse_json_array(self):
        """JSON array string is parsed."""
        result = parse_csv_value('[1, 2, 3]')
        assert result == [1, 2, 3]

    def test_parse_integer(self):
        """Integer string is parsed as int."""
        assert parse_csv_value("42") == 42
        assert parse_csv_value("-10") == -10

    def test_parse_float(self):
        """Float string is parsed as float."""
        assert parse_csv_value("3.14") == 3.14
        assert parse_csv_value("-2.5") == -2.5

    def test_parse_string(self):
        """Regular string is returned as-is."""
        assert parse_csv_value("hello") == "hello"
        assert parse_csv_value("John Doe") == "John Doe"

    def test_parse_invalid_json(self):
        """Invalid JSON-like string is returned as string."""
        result = parse_csv_value("{invalid json}")
        assert result == "{invalid json}"


class TestExtractReferenceId:
    """Tests for extract_reference_id function."""

    def test_extracts_value_from_dict(self):
        """extract_reference_id extracts 'value' key from dict."""
        value = {"value": 431, "name": "ACME Corp"}
        result = extract_reference_id(value)
        assert result == 431

    def test_integer_passthrough(self):
        """extract_reference_id passes through integers."""
        assert extract_reference_id(431) == 431

    def test_string_passthrough(self):
        """extract_reference_id passes through strings."""
        assert extract_reference_id("test") == "test"

    def test_none_passthrough(self):
        """extract_reference_id passes through None."""
        assert extract_reference_id(None) is None

    def test_dict_without_value_passthrough(self):
        """extract_reference_id passes through dict without 'value' key."""
        value = {"name": "ACME Corp", "id": 431}
        result = extract_reference_id(value)
        assert result == value

    def test_extracts_from_owner_id_format(self):
        """extract_reference_id works with owner_id format."""
        value = {
            "id": 22713797,
            "value": 22713797,
            "name": "Admin User",
            "email": "admin@example.com",
        }
        result = extract_reference_id(value)
        assert result == 22713797


class TestConvertRecordForApi:
    """Tests for convert_record_for_api function."""

    def test_converts_org_field(self):
        """convert_record_for_api extracts org_id integer."""
        record = {
            "name": "John Doe",
            "org_id": {"value": 431, "name": "ACME Corp"},
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["name"] == "John Doe"
        assert result["org_id"] == 431

    def test_converts_owner_id_field(self):
        """convert_record_for_api extracts owner_id integer."""
        record = {
            "name": "Test Org",
            "owner_id": {"id": 100, "value": 100, "name": "Admin"},
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "owner_id", "field_type": "user"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["owner_id"] == 100

    def test_converts_person_id_field(self):
        """convert_record_for_api extracts person_id integer."""
        record = {
            "name": "Sample Deal",
            "person_id": {"value": 123, "name": "John Doe"},
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "person_id", "field_type": "people"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["person_id"] == 123

    def test_non_reference_fields_unchanged(self):
        """convert_record_for_api leaves non-reference fields unchanged."""
        record = {
            "name": "John Doe",
            "email": [{"value": "john@example.com"}],
            "phone": "+1234567890",
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "email", "field_type": "varchar"},
            {"key": "phone", "field_type": "phone"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["name"] == "John Doe"
        assert result["email"] == [{"value": "john@example.com"}]
        assert result["phone"] == "+1234567890"

    def test_unknown_field_passthrough(self):
        """convert_record_for_api passes through unknown fields."""
        record = {
            "name": "John Doe",
            "unknown_field": "value",
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["unknown_field"] == "value"

    def test_already_integer_unchanged(self):
        """convert_record_for_api handles already-integer org_id."""
        record = {
            "name": "John Doe",
            "org_id": 431,
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]
        result = convert_record_for_api(record, field_defs)

        assert result["org_id"] == 431
