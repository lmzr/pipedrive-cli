"""Tests for restore functionality."""


from pipedrive_cli.restore import clean_record, parse_csv_value


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
