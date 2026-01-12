"""Tests for record import command and importer module."""

import csv
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipedrive_cli.cli import main
from pipedrive_cli.importer import (
    ReferenceNotFoundError,
    build_dedup_index,
    build_org_object,
    build_person_object,
    build_user_object,
    convert_email_value,
    convert_enum_value,
    convert_phone_value,
    convert_record_for_import,
    convert_reference_value,
    convert_set_value,
    convert_value_for_import,
    detect_format,
    extract_comparable_value,
    get_max_id,
    import_records,
    is_already_array_format,
    is_already_reference_object,
    load_csv_records,
    load_input_file,
    load_json_records,
    load_related_entity_records,
    validate_input_fields,
)


@pytest.fixture
def temp_datapackage(tmp_path: Path) -> Path:
    """Create a temporary datapackage for import tests."""
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
                        {"name": "custom_field", "type": "string"},
                    ],
                    "pipedrive_fields": [
                        {"key": "id", "name": "ID", "field_type": "int", "edit_flag": False},
                        {
                            "key": "name",
                            "name": "Name",
                            "field_type": "varchar",
                            "edit_flag": True,
                        },
                        {
                            "key": "email",
                            "name": "Email",
                            "field_type": "varchar",
                            "edit_flag": True,
                        },
                        {
                            "key": "custom_field",
                            "name": "Custom",
                            "field_type": "varchar",
                            "edit_flag": True,
                        },
                    ],
                },
            }
        ],
    }
    (base_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

    with open(base_dir / "persons.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "email", "custom_field"])
        writer.writerow(["1", "Alice", "alice@example.com", "value1"])
        writer.writerow(["2", "Bob", "bob@example.com", "value2"])

    return base_dir


@pytest.fixture
def sample_csv(tmp_path: Path) -> Path:
    """Create a sample CSV file for import."""
    csv_path = tmp_path / "import.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["name", "email", "custom_field"])
        writer.writerow(["Charlie", "charlie@example.com", "value3"])
        writer.writerow(["Dave", "dave@example.com", "value4"])
    return csv_path


@pytest.fixture
def sample_json(tmp_path: Path) -> Path:
    """Create a sample JSON file for import."""
    json_path = tmp_path / "import.json"
    data = [
        {"name": "Charlie", "email": "charlie@example.com", "custom_field": "value3"},
        {"name": "Dave", "email": "dave@example.com", "custom_field": "value4"},
    ]
    with open(json_path, "w") as f:
        json.dump(data, f)
    return json_path


class TestDetectFormat:
    """Tests for detect_format function."""

    def test_detect_csv(self, tmp_path: Path):
        """detect_format detects CSV files."""
        assert detect_format(tmp_path / "file.csv") == "csv"

    def test_detect_json(self, tmp_path: Path):
        """detect_format detects JSON files."""
        assert detect_format(tmp_path / "file.json") == "json"

    def test_detect_xlsx(self, tmp_path: Path):
        """detect_format detects XLSX files."""
        assert detect_format(tmp_path / "file.xlsx") == "xlsx"

    def test_detect_unknown_raises(self, tmp_path: Path):
        """detect_format raises for unknown extension."""
        with pytest.raises(ValueError):
            detect_format(tmp_path / "file.unknown")


class TestLoadCsvRecords:
    """Tests for load_csv_records function."""

    def test_load_basic_csv(self, sample_csv: Path):
        """load_csv_records loads CSV records."""
        records, fieldnames = load_csv_records(sample_csv)

        assert len(records) == 2
        assert fieldnames == ["name", "email", "custom_field"]
        assert records[0]["name"] == "Charlie"

    def test_load_csv_with_json_values(self, tmp_path: Path):
        """load_csv_records parses JSON values."""
        csv_path = tmp_path / "json_values.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "data"])
            writer.writerow(["Alice", '{"key": "value"}'])

        records, _ = load_csv_records(csv_path)

        assert records[0]["data"] == {"key": "value"}


class TestLoadJsonRecords:
    """Tests for load_json_records function."""

    def test_load_basic_json(self, sample_json: Path):
        """load_json_records loads JSON records."""
        records, fieldnames = load_json_records(sample_json)

        assert len(records) == 2
        assert "name" in fieldnames
        assert records[0]["name"] == "Charlie"

    def test_load_empty_json(self, tmp_path: Path):
        """load_json_records handles empty array."""
        json_path = tmp_path / "empty.json"
        with open(json_path, "w") as f:
            json.dump([], f)

        records, fieldnames = load_json_records(json_path)

        assert records == []
        assert fieldnames == []

    def test_load_non_array_raises(self, tmp_path: Path):
        """load_json_records raises for non-array JSON."""
        json_path = tmp_path / "object.json"
        with open(json_path, "w") as f:
            json.dump({"key": "value"}, f)

        with pytest.raises(ValueError):
            load_json_records(json_path)


class TestLoadInputFile:
    """Tests for load_input_file function."""

    def test_load_csv_auto_detect(self, sample_csv: Path):
        """load_input_file auto-detects CSV."""
        records, fieldnames = load_input_file(sample_csv)

        assert len(records) == 2
        assert "name" in fieldnames

    def test_load_json_auto_detect(self, sample_json: Path):
        """load_input_file auto-detects JSON."""
        records, fieldnames = load_input_file(sample_json)

        assert len(records) == 2
        assert "name" in fieldnames


class TestValidateInputFields:
    """Tests for validate_input_fields function."""

    def test_validate_valid_fields(self):
        """validate_input_fields returns valid fields."""
        input_fields = ["name", "email", "custom_field"]
        schema_fields = [
            {"key": "id"},
            {"key": "name"},
            {"key": "email"},
            {"key": "custom_field"},
        ]

        valid, readonly, unknown = validate_input_fields(input_fields, schema_fields)

        assert valid == ["name", "email", "custom_field"]
        assert readonly == []
        assert unknown == []

    def test_validate_readonly_fields_skipped(self):
        """validate_input_fields skips readonly fields."""
        input_fields = ["name", "add_time", "update_time"]
        schema_fields = [{"key": "name"}, {"key": "add_time"}, {"key": "update_time"}]

        valid, readonly, unknown = validate_input_fields(input_fields, schema_fields)

        assert valid == ["name"]
        assert "add_time" in readonly
        assert "update_time" in readonly

    def test_validate_unknown_fields(self):
        """validate_input_fields identifies unknown fields."""
        input_fields = ["name", "unknown_field"]
        schema_fields = [{"key": "name"}]

        valid, readonly, unknown = validate_input_fields(input_fields, schema_fields)

        assert valid == ["name"]
        assert "unknown_field" in unknown


class TestExtractComparableValue:
    """Tests for extract_comparable_value function."""

    def test_plain_string(self):
        """extract_comparable_value returns plain string as-is."""
        assert extract_comparable_value("hello") == "hello"

    def test_integer(self):
        """extract_comparable_value converts integer to string."""
        assert extract_comparable_value(431) == "431"

    def test_none(self):
        """extract_comparable_value returns empty string for None."""
        assert extract_comparable_value(None) == ""

    def test_email_array(self):
        """extract_comparable_value extracts primary email from array."""
        value = [
            {"value": "secondary@example.com", "primary": False},
            {"value": "primary@example.com", "primary": True},
        ]
        assert extract_comparable_value(value) == "primary@example.com"

    def test_email_array_no_primary(self):
        """extract_comparable_value extracts first email if no primary."""
        value = [{"value": "first@example.com"}, {"value": "second@example.com"}]
        assert extract_comparable_value(value) == "first@example.com"

    def test_reference_object(self):
        """extract_comparable_value extracts value from reference object."""
        value = {"value": 431, "name": "ACME Corp", "people_count": 5}
        assert extract_comparable_value(value) == "431"

    def test_reference_object_person(self):
        """extract_comparable_value extracts value from person reference."""
        value = {"value": 123, "name": "John Doe", "email": [{"value": "j@example.com"}]}
        assert extract_comparable_value(value) == "123"


class TestBuildDedupIndex:
    """Tests for build_dedup_index function."""

    def test_build_single_key_index(self):
        """build_dedup_index with single key field."""
        records = [
            {"id": 1, "email": "a@example.com"},
            {"id": 2, "email": "b@example.com"},
        ]

        index = build_dedup_index(records, ["email"])

        assert index[("a@example.com",)] == 0
        assert index[("b@example.com",)] == 1

    def test_build_multi_key_index(self):
        """build_dedup_index with multiple key fields."""
        records = [
            {"first": "John", "last": "Doe"},
            {"first": "Jane", "last": "Doe"},
        ]

        index = build_dedup_index(records, ["first", "last"])

        assert index[("John", "Doe")] == 0
        assert index[("Jane", "Doe")] == 1

    def test_build_keeps_first_occurrence(self):
        """build_dedup_index keeps first occurrence for duplicates."""
        records = [
            {"id": 1, "email": "a@example.com"},
            {"id": 2, "email": "a@example.com"},  # Duplicate
        ]

        index = build_dedup_index(records, ["email"])

        assert index[("a@example.com",)] == 0  # First occurrence


class TestGetMaxId:
    """Tests for get_max_id function."""

    def test_get_max_id_basic(self):
        """get_max_id returns max ID."""
        records = [{"id": 1}, {"id": 5}, {"id": 3}]

        assert get_max_id(records) == 5

    def test_get_max_id_empty(self):
        """get_max_id returns 0 for empty list."""
        assert get_max_id([]) == 0

    def test_get_max_id_no_id_field(self):
        """get_max_id returns 0 when no id field."""
        records = [{"name": "Alice"}, {"name": "Bob"}]

        assert get_max_id(records) == 0

    def test_get_max_id_string_ids(self):
        """get_max_id handles string IDs."""
        records = [{"id": "1"}, {"id": "10"}, {"id": "5"}]

        assert get_max_id(records) == 10


class TestImportRecords:
    """Tests for import_records function."""

    def test_import_creates_new_records(self):
        """import_records creates new records."""
        input_records = [{"name": "Charlie", "email": "c@example.com"}]
        existing_records = [{"id": 1, "name": "Alice", "email": "a@example.com"}]
        valid_fields = ["name", "email"]

        stats, merged, results = import_records(
            input_records, existing_records, valid_fields
        )

        assert stats.total == 1
        assert stats.created == 1
        assert len(merged) == 2
        assert merged[1]["name"] == "Charlie"

    def test_import_updates_duplicates(self):
        """import_records updates duplicates with on_duplicate=update."""
        input_records = [{"name": "Alice Updated", "email": "a@example.com"}]
        existing_records = [{"id": 1, "name": "Alice", "email": "a@example.com"}]
        valid_fields = ["name", "email"]

        stats, merged, results = import_records(
            input_records,
            existing_records,
            valid_fields,
            key_fields=["email"],
            on_duplicate="update",
        )

        assert stats.updated == 1
        assert stats.created == 0
        assert merged[0]["name"] == "Alice Updated"

    def test_import_skips_duplicates(self):
        """import_records skips duplicates with on_duplicate=skip."""
        input_records = [{"name": "Alice Updated", "email": "a@example.com"}]
        existing_records = [{"id": 1, "name": "Alice", "email": "a@example.com"}]
        valid_fields = ["name", "email"]

        stats, merged, results = import_records(
            input_records,
            existing_records,
            valid_fields,
            key_fields=["email"],
            on_duplicate="skip",
        )

        assert stats.skipped == 1
        assert stats.created == 0
        assert merged[0]["name"] == "Alice"  # Unchanged

    def test_import_errors_on_duplicates(self):
        """import_records errors duplicates with on_duplicate=error."""
        input_records = [{"name": "Alice Updated", "email": "a@example.com"}]
        existing_records = [{"id": 1, "name": "Alice", "email": "a@example.com"}]
        valid_fields = ["name", "email"]

        stats, merged, results = import_records(
            input_records,
            existing_records,
            valid_fields,
            key_fields=["email"],
            on_duplicate="error",
        )

        assert stats.failed == 1
        assert len(stats.errors) == 1

    def test_import_auto_id_generation(self):
        """import_records generates IDs with auto_id=True."""
        input_records = [{"name": "Charlie"}]
        existing_records = [{"id": 5, "name": "Alice"}]
        valid_fields = ["name"]

        stats, merged, results = import_records(
            input_records, existing_records, valid_fields, auto_id=True
        )

        assert merged[1]["id"] == 6  # max(5) + 1

    def test_import_with_log_file(self, tmp_path: Path):
        """import_records writes to log file."""
        input_records = [{"name": "Charlie"}]
        existing_records = []
        valid_fields = ["name"]

        log_file = tmp_path / "import.log"
        with open(log_file, "w") as f:
            import_records(
                input_records, existing_records, valid_fields, auto_id=True, log_file=f
            )

        with open(log_file) as f:
            log_lines = f.readlines()

        assert len(log_lines) == 1
        log_entry = json.loads(log_lines[0])
        assert log_entry["action"] == "created"


# -----------------------------------------------------------------------------
# Value Conversion Tests
# -----------------------------------------------------------------------------


class TestIsAlreadyArrayFormat:
    """Tests for is_already_array_format function."""

    def test_empty_list(self):
        """Empty list is valid array format."""
        assert is_already_array_format([]) is True

    def test_list_with_value_key(self):
        """List of dicts with 'value' key is array format."""
        value = [{"value": "test@example.com", "label": "work"}]
        assert is_already_array_format(value) is True

    def test_list_without_value_key(self):
        """List of dicts without 'value' key is not array format."""
        value = [{"label": "test"}]
        assert is_already_array_format(value) is False

    def test_string_not_array_format(self):
        """Plain string is not array format."""
        assert is_already_array_format("test@example.com") is False

    def test_none_not_array_format(self):
        """None is not array format."""
        assert is_already_array_format(None) is False


class TestConvertPhoneValue:
    """Tests for convert_phone_value function."""

    def test_string_to_phone_array(self):
        """String phone is converted to array format."""
        result = convert_phone_value("0612345678")
        assert result == [{"value": "0612345678", "label": "mobile", "primary": True}]

    def test_already_array_format(self):
        """Already converted phone is returned as-is."""
        value = [{"value": "0612345678", "label": "work", "primary": True}]
        result = convert_phone_value(value)
        assert result == value

    def test_empty_string_returns_none(self):
        """Empty string returns None."""
        assert convert_phone_value("") is None
        assert convert_phone_value("  ") is None

    def test_none_returns_none(self):
        """None returns None."""
        assert convert_phone_value(None) is None

    def test_strips_whitespace(self):
        """Whitespace is stripped from phone number."""
        result = convert_phone_value("  0612345678  ")
        assert result == [{"value": "0612345678", "label": "mobile", "primary": True}]


class TestConvertEmailValue:
    """Tests for convert_email_value function."""

    def test_string_to_email_array(self):
        """String email is converted to array format."""
        result = convert_email_value("test@example.com")
        assert result == [{"value": "test@example.com", "label": "work", "primary": True}]

    def test_already_array_format(self):
        """Already converted email is returned as-is."""
        value = [{"value": "test@example.com", "label": "personal", "primary": True}]
        result = convert_email_value(value)
        assert result == value

    def test_empty_string_returns_none(self):
        """Empty string returns None."""
        assert convert_email_value("") is None

    def test_none_returns_none(self):
        """None returns None."""
        assert convert_email_value(None) is None


class TestConvertEnumValue:
    """Tests for convert_enum_value function."""

    @pytest.fixture
    def enum_field(self):
        """Enum field definition with options."""
        return {
            "field_type": "enum",
            "options": [
                {"id": 37, "label": "Monsieur"},
                {"id": 38, "label": "Madame"},
            ]
        }

    def test_label_to_id(self, enum_field):
        """Label is converted to option ID."""
        result = convert_enum_value("Monsieur", enum_field)
        assert result == 37

    def test_already_integer_id(self, enum_field):
        """Integer ID is returned as-is."""
        result = convert_enum_value(37, enum_field)
        assert result == 37

    def test_string_integer_id(self, enum_field):
        """String integer is converted to int."""
        result = convert_enum_value("37", enum_field)
        assert result == 37

    def test_unknown_label_returned_as_is(self, enum_field):
        """Unknown label is returned as-is for later validation."""
        result = convert_enum_value("Unknown", enum_field)
        assert result == "Unknown"

    def test_empty_returns_none(self, enum_field):
        """Empty string returns None."""
        assert convert_enum_value("", enum_field) is None

    def test_none_returns_none(self, enum_field):
        """None returns None."""
        assert convert_enum_value(None, enum_field) is None


class TestConvertSetValue:
    """Tests for convert_set_value function."""

    @pytest.fixture
    def set_field(self):
        """Set field definition with options."""
        return {
            "field_type": "set",
            "options": [
                {"id": 1, "label": "VIP"},
                {"id": 2, "label": "Premium"},
                {"id": 3, "label": "Standard"},
            ]
        }

    def test_comma_separated_labels_to_ids(self, set_field):
        """Comma-separated labels are converted to IDs."""
        result = convert_set_value("VIP, Premium", set_field)
        assert result == "1,2"

    def test_single_label_to_id(self, set_field):
        """Single label is converted to ID."""
        result = convert_set_value("VIP", set_field)
        assert result == "1"

    def test_already_comma_separated_ids(self, set_field):
        """Already comma-separated IDs are returned as-is."""
        result = convert_set_value("1,2", set_field)
        assert result == "1,2"

    def test_list_of_integers(self, set_field):
        """List of integers is converted to comma-separated string."""
        result = convert_set_value([1, 2], set_field)
        assert result == "1,2"

    def test_list_of_labels(self, set_field):
        """List of labels is converted to comma-separated IDs."""
        result = convert_set_value(["VIP", "Premium"], set_field)
        assert result == "1,2"

    def test_empty_returns_none(self, set_field):
        """Empty string returns None."""
        assert convert_set_value("", set_field) is None

    def test_none_returns_none(self, set_field):
        """None returns None."""
        assert convert_set_value(None, set_field) is None


class TestConvertValueForImport:
    """Tests for convert_value_for_import function."""

    def test_phone_field_type(self):
        """Phone field_type triggers phone conversion."""
        field_def = {"field_type": "phone"}
        result = convert_value_for_import("0612345678", "phone", field_def)
        assert result == [{"value": "0612345678", "label": "mobile", "primary": True}]

    def test_email_key_triggers_conversion(self):
        """Field with key 'email' triggers email conversion."""
        field_def = {"field_type": "varchar"}
        result = convert_value_for_import("test@example.com", "email", field_def)
        assert result == [{"value": "test@example.com", "label": "work", "primary": True}]

    def test_enum_field_type(self):
        """Enum field_type triggers enum conversion."""
        field_def = {
            "field_type": "enum",
            "options": [{"id": 37, "label": "Monsieur"}]
        }
        result = convert_value_for_import("Monsieur", "civilite", field_def)
        assert result == 37

    def test_set_field_type(self):
        """Set field_type triggers set conversion."""
        field_def = {
            "field_type": "set",
            "options": [{"id": 1, "label": "VIP"}, {"id": 2, "label": "Premium"}]
        }
        result = convert_value_for_import("VIP, Premium", "tags", field_def)
        assert result == "1,2"

    def test_varchar_passthrough(self):
        """Varchar field passes through unchanged."""
        field_def = {"field_type": "varchar"}
        result = convert_value_for_import("test value", "name", field_def)
        assert result == "test value"


class TestConvertRecordForImport:
    """Tests for convert_record_for_import function."""

    def test_converts_multiple_fields(self):
        """Multiple fields are converted according to their types."""
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "email", "field_type": "varchar"},
            {"key": "phone", "field_type": "phone"},
            {"key": "civilite", "field_type": "enum", "options": [{"id": 37, "label": "M."}]},
        ]
        record = {
            "name": "John Doe",
            "email": "john@example.com",
            "phone": "0612345678",
            "civilite": "M.",
        }

        result = convert_record_for_import(record, field_defs)

        assert result["name"] == "John Doe"
        assert result["email"] == [{"value": "john@example.com", "label": "work", "primary": True}]
        assert result["phone"] == [{"value": "0612345678", "label": "mobile", "primary": True}]
        assert result["civilite"] == 37

    def test_unknown_field_passthrough(self):
        """Fields not in field_defs pass through unchanged."""
        field_defs = [{"key": "name", "field_type": "varchar"}]
        record = {"name": "John", "unknown_field": "value"}

        result = convert_record_for_import(record, field_defs)

        assert result["unknown_field"] == "value"


class TestRecordImportCommand:
    """Tests for record import CLI command."""

    def test_import_csv_basic(self, temp_datapackage: Path, sample_csv: Path):
        """record import imports CSV file."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "import",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-i", str(sample_csv),
            "--auto-id"
        ])

        assert result.exit_code == 0
        assert "created" in result.output.lower()

        # Verify records were added
        with open(temp_datapackage / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 4  # 2 existing + 2 new
        names = [r["name"] for r in rows]
        assert "Charlie" in names
        assert "Dave" in names

    def test_import_json_basic(self, temp_datapackage: Path, sample_json: Path):
        """record import imports JSON file."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "import",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-i", str(sample_json),
            "--auto-id"
        ])

        assert result.exit_code == 0
        assert "created" in result.output.lower()

    def test_import_with_deduplication(self, temp_datapackage: Path, tmp_path: Path):
        """record import with --key deduplicates."""
        csv_path = tmp_path / "dedup.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            # Use name as dedup key since email is converted to array format
            writer.writerow(["name", "custom_field"])
            writer.writerow(["Alice", "new_value"])

        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "import",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-i", str(csv_path),
            "-k", "name",
            "--on-duplicate", "update"
        ])

        assert result.exit_code == 0
        assert "updated" in result.output.lower()

        # Verify record was updated
        with open(temp_datapackage / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        alice = next(r for r in rows if r["name"] == "Alice")
        assert alice["custom_field"] == "new_value"

    def test_import_dry_run(self, temp_datapackage: Path, sample_csv: Path):
        """record import --dry-run shows what would happen."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "import",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-i", str(sample_csv),
            "--auto-id",
            "-n"
        ])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output

        # Verify records were NOT added
        with open(temp_datapackage / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2  # Unchanged

    def test_import_with_log(self, temp_datapackage: Path, sample_csv: Path, tmp_path: Path):
        """record import --log writes log file."""
        log_file = tmp_path / "import.jsonl"

        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "import",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-i", str(sample_csv),
            "--auto-id",
            "-l", str(log_file)
        ])

        assert result.exit_code == 0
        assert log_file.exists()

        with open(log_file) as f:
            lines = f.readlines()

        assert len(lines) == 2  # 2 records imported

    def test_import_unknown_field_error(self, temp_datapackage: Path, tmp_path: Path):
        """record import with unknown field shows error."""
        csv_path = tmp_path / "unknown.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "unknown_field"])
            writer.writerow(["Test", "value"])

        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "import",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-i", str(csv_path)
        ])

        assert result.exit_code != 0
        assert "unknown" in result.output.lower()

    def test_import_readonly_fields_skipped(self, temp_datapackage: Path, tmp_path: Path):
        """record import skips readonly fields."""
        csv_path = tmp_path / "readonly.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "add_time"])
            writer.writerow(["Test", "2024-01-01"])

        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "import",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-i", str(csv_path),
            "--auto-id"
        ])

        # Should succeed but warn about skipped fields
        assert result.exit_code == 0
        assert "add_time" in result.output or "readonly" in result.output.lower()

    def test_import_help(self):
        """record import command shows help."""
        runner = CliRunner()
        result = runner.invoke(main, ["record", "import", "-h"])

        assert result.exit_code == 0
        assert "--entity" in result.output
        assert "--base" in result.output
        assert "--input" in result.output
        assert "--key" in result.output
        assert "--on-duplicate" in result.output
        assert "--auto-id" in result.output


# -----------------------------------------------------------------------------
# Reference Field Conversion Tests
# -----------------------------------------------------------------------------


class TestIsAlreadyReferenceObject:
    """Tests for is_already_reference_object function."""

    def test_dict_with_value_key(self):
        """Dict with 'value' key is a reference object."""
        value = {"value": 431, "name": "ACME Corp"}
        assert is_already_reference_object(value) is True

    def test_dict_without_value_key(self):
        """Dict without 'value' key is not a reference object."""
        value = {"name": "ACME Corp", "id": 431}
        assert is_already_reference_object(value) is False

    def test_integer_not_reference_object(self):
        """Integer is not a reference object."""
        assert is_already_reference_object(431) is False

    def test_string_not_reference_object(self):
        """String is not a reference object."""
        assert is_already_reference_object("431") is False

    def test_none_not_reference_object(self):
        """None is not a reference object."""
        assert is_already_reference_object(None) is False

    def test_list_not_reference_object(self):
        """List is not a reference object."""
        assert is_already_reference_object([{"value": 431}]) is False


class TestBuildOrgObject:
    """Tests for build_org_object function."""

    def test_builds_org_object(self):
        """build_org_object creates org reference object."""
        org = {
            "name": "ACME Corp",
            "people_count": 5,
            "owner_id": {"value": 123},
            "address": "123 Main St",
            "active_flag": True,
            "cc_email": "acme@pipedrive.com",
        }
        result = build_org_object(431, org)

        assert result["name"] == "ACME Corp"
        assert result["value"] == 431
        assert result["people_count"] == 5
        assert result["owner_id"] == {"value": 123}
        assert result["address"] == "123 Main St"
        assert result["active_flag"] is True
        assert result["cc_email"] == "acme@pipedrive.com"

    def test_handles_missing_fields(self):
        """build_org_object handles missing optional fields."""
        org = {"name": "Simple Corp"}
        result = build_org_object(431, org)

        assert result["name"] == "Simple Corp"
        assert result["value"] == 431
        assert result["people_count"] == 0
        assert result["owner_id"] is None
        assert result["address"] is None
        assert result["active_flag"] is True
        assert result["cc_email"] == ""


class TestBuildPersonObject:
    """Tests for build_person_object function."""

    def test_builds_person_object_with_arrays(self):
        """build_person_object creates person reference object from arrays."""
        person = {
            "name": "John Doe",
            "email": [{"value": "john@example.com", "primary": True}],
            "phone": [{"value": "+1234567890", "primary": True}],
        }
        result = build_person_object(123, person)

        assert result["value"] == 123
        assert result["name"] == "John Doe"
        assert result["email"] == [{"value": "john@example.com"}]
        assert result["phone"] == [{"value": "+1234567890"}]

    def test_handles_string_email_phone(self):
        """build_person_object handles string email/phone."""
        person = {
            "name": "Jane Doe",
            "email": "jane@example.com",
            "phone": "+1987654321",
        }
        result = build_person_object(124, person)

        assert result["email"] == [{"value": "jane@example.com"}]
        assert result["phone"] == [{"value": "+1987654321"}]

    def test_handles_empty_email_phone(self):
        """build_person_object handles missing email/phone."""
        person = {"name": "No Contact"}
        result = build_person_object(125, person)

        assert result["email"] == []
        assert result["phone"] == []


class TestBuildUserObject:
    """Tests for build_user_object function."""

    def test_builds_user_object(self):
        """build_user_object creates user reference object."""
        user = {
            "name": "Admin User",
            "email": "admin@example.com",
            "has_pic": 1,
            "pic_hash": "abc123",
            "active_flag": True,
        }
        result = build_user_object(100, user)

        assert result["id"] == 100
        assert result["value"] == 100
        assert result["name"] == "Admin User"
        assert result["email"] == "admin@example.com"
        assert result["has_pic"] == 1
        assert result["pic_hash"] == "abc123"
        assert result["active_flag"] is True

    def test_handles_missing_fields(self):
        """build_user_object handles missing optional fields."""
        user = {"name": "Simple User"}
        result = build_user_object(101, user)

        assert result["id"] == 101
        assert result["value"] == 101
        assert result["name"] == "Simple User"
        assert result["email"] == ""
        assert result["has_pic"] == 0
        assert result["pic_hash"] is None
        assert result["active_flag"] is True


class TestLoadRelatedEntityRecords:
    """Tests for load_related_entity_records function."""

    def test_loads_csv_records(self, tmp_path: Path):
        """load_related_entity_records loads CSV indexed by ID."""
        base_dir = tmp_path / "base"
        base_dir.mkdir()

        with open(base_dir / "organizations.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "people_count"])
            writer.writerow(["431", "ACME Corp", "5"])
            writer.writerow(["432", "Beta Inc", "3"])

        result = load_related_entity_records(base_dir, "organizations")

        assert 431 in result
        assert result[431]["name"] == "ACME Corp"
        assert result[431]["people_count"] == "5"
        assert 432 in result

    def test_parses_json_values(self, tmp_path: Path):
        """load_related_entity_records parses JSON values."""
        base_dir = tmp_path / "base"
        base_dir.mkdir()

        with open(base_dir / "persons.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "email"])
            writer.writerow(["123", "John", '[{"value": "john@example.com"}]'])

        result = load_related_entity_records(base_dir, "persons")

        assert result[123]["email"] == [{"value": "john@example.com"}]

    def test_missing_file_raises(self, tmp_path: Path):
        """load_related_entity_records raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError) as exc_info:
            load_related_entity_records(tmp_path, "missing")

        assert "missing.csv not found" in str(exc_info.value)


class TestConvertReferenceValue:
    """Tests for convert_reference_value function."""

    @pytest.fixture
    def org_data(self):
        """Sample organization records."""
        return {
            431: {"name": "ACME Corp", "people_count": 5},
            432: {"name": "Beta Inc", "people_count": 3},
        }

    def test_integer_to_object(self, org_data):
        """Integer ID is converted to reference object."""
        field_def = {"field_type": "org"}
        result = convert_reference_value(431, "org_id", field_def, org_data)

        assert result["value"] == 431
        assert result["name"] == "ACME Corp"

    def test_string_integer_to_object(self, org_data):
        """String integer ID is converted to reference object."""
        field_def = {"field_type": "org"}
        result = convert_reference_value("431", "org_id", field_def, org_data)

        assert result["value"] == 431
        assert result["name"] == "ACME Corp"

    def test_already_object_passthrough(self, org_data):
        """Already-converted object is returned as-is."""
        field_def = {"field_type": "org"}
        existing = {"value": 431, "name": "Old Name"}
        result = convert_reference_value(existing, "org_id", field_def, org_data)

        assert result == existing  # Unchanged

    def test_none_returns_none(self, org_data):
        """None returns None."""
        field_def = {"field_type": "org"}
        assert convert_reference_value(None, "org_id", field_def, org_data) is None

    def test_empty_string_returns_none(self, org_data):
        """Empty string returns None."""
        field_def = {"field_type": "org"}
        assert convert_reference_value("", "org_id", field_def, org_data) is None

    def test_not_found_raises_error(self, org_data):
        """ID not found raises ReferenceNotFoundError."""
        field_def = {"field_type": "org"}
        with pytest.raises(ReferenceNotFoundError) as exc_info:
            convert_reference_value(999, "org_id", field_def, org_data)

        assert "org_id=999" in str(exc_info.value)
        assert "organizations" in str(exc_info.value)

    def test_invalid_value_raises_error(self, org_data):
        """Non-integer value raises ReferenceNotFoundError."""
        field_def = {"field_type": "org"}
        with pytest.raises(ReferenceNotFoundError) as exc_info:
            convert_reference_value("invalid", "org_id", field_def, org_data)

        assert "Invalid reference value" in str(exc_info.value)


class TestConvertValueForImportWithReferences:
    """Tests for convert_value_for_import with reference fields.

    Reference fields should remain as integers for local storage.
    Conversion to objects happens in store command for API calls.
    """

    def test_org_field_returns_integer(self):
        """Field with field_type='org' validates and returns integer ID."""
        field_def = {"field_type": "org"}
        related_entities = {
            "organizations": {431: {"name": "ACME", "people_count": 5}}
        }
        result = convert_value_for_import(431, "org_id", field_def, related_entities)

        assert result == 431  # Integer, not object

    def test_people_field_returns_integer(self):
        """Field with field_type='people' validates and returns integer ID."""
        field_def = {"field_type": "people"}
        related_entities = {
            "persons": {123: {"name": "John Doe", "email": "john@example.com"}}
        }
        result = convert_value_for_import(123, "person_id", field_def, related_entities)

        assert result == 123  # Integer, not object

    def test_user_field_returns_integer(self):
        """Field with field_type='user' validates and returns integer ID."""
        field_def = {"field_type": "user"}
        related_entities = {
            "users": {100: {"name": "Admin", "email": "admin@example.com"}}
        }
        result = convert_value_for_import(100, "owner_id", field_def, related_entities)

        assert result == 100  # Integer, not object

    def test_string_id_converted_to_integer(self):
        """String ID is converted to integer."""
        field_def = {"field_type": "org"}
        related_entities = {
            "organizations": {431: {"name": "ACME"}}
        }
        result = convert_value_for_import("431", "org_id", field_def, related_entities)

        assert result == 431
        assert isinstance(result, int)

    def test_no_related_data_passthrough(self):
        """Without related_entities, value passes through."""
        field_def = {"field_type": "org"}
        result = convert_value_for_import(431, "org_id", field_def, None)

        assert result == 431  # Unchanged

    def test_missing_entity_passthrough(self):
        """If entity not in related_entities, value passes through."""
        field_def = {"field_type": "org"}
        related_entities = {}  # No organizations loaded
        result = convert_value_for_import(431, "org_id", field_def, related_entities)

        assert result == 431  # Unchanged

    def test_invalid_id_raises_error(self):
        """Non-integer value raises ReferenceNotFoundError."""
        field_def = {"field_type": "org"}
        related_entities = {
            "organizations": {431: {"name": "ACME"}}
        }
        with pytest.raises(ReferenceNotFoundError) as exc_info:
            convert_value_for_import("invalid", "org_id", field_def, related_entities)

        assert "Invalid reference value" in str(exc_info.value)

    def test_missing_id_raises_error(self):
        """ID not found raises ReferenceNotFoundError."""
        field_def = {"field_type": "org"}
        related_entities = {
            "organizations": {431: {"name": "ACME"}}
        }
        with pytest.raises(ReferenceNotFoundError) as exc_info:
            convert_value_for_import(999, "org_id", field_def, related_entities)

        assert "org_id=999" in str(exc_info.value)


class TestImportRecordsWithReferences:
    """Integration tests for import_records with reference field validation.

    Reference fields should remain as integers for local storage.
    """

    @pytest.fixture
    def datapackage_with_refs(self, tmp_path: Path) -> Path:
        """Create a datapackage with reference fields."""
        base_dir = tmp_path / "test-base"
        base_dir.mkdir()

        # Create organizations.csv
        with open(base_dir / "organizations.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "people_count"])
            writer.writerow(["431", "ACME Corp", "5"])
            writer.writerow(["432", "Beta Inc", "3"])

        # Create persons.csv with org_id as integer (correct format)
        with open(base_dir / "persons.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "org_id"])
            writer.writerow(["1", "Alice", "431"])

        # Create datapackage.json
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
                            {"name": "org_id", "type": "integer"},
                        ],
                        "pipedrive_fields": [
                            {"key": "id", "name": "ID", "field_type": "int"},
                            {"key": "name", "name": "Name", "field_type": "varchar"},
                            {"key": "org_id", "name": "Organization", "field_type": "org"},
                        ],
                    },
                },
                {
                    "name": "organizations",
                    "path": "organizations.csv",
                    "schema": {
                        "fields": [
                            {"name": "id", "type": "integer"},
                            {"name": "name", "type": "string"},
                            {"name": "people_count", "type": "integer"},
                        ],
                    },
                },
            ],
        }
        (base_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

        return base_dir

    def test_import_keeps_org_id_as_integer(self, datapackage_with_refs: Path):
        """import_records validates org_id and keeps it as integer."""
        input_records = [{"name": "Charlie", "org_id": 432}]
        existing_records = []
        valid_fields = ["name", "org_id"]
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]

        stats, merged, results = import_records(
            input_records,
            existing_records,
            valid_fields,
            auto_id=True,
            field_defs=field_defs,
            base_path=datapackage_with_refs,
        )

        assert stats.created == 1
        # org_id should remain as integer, not converted to object
        assert merged[0]["org_id"] == 432
        assert isinstance(merged[0]["org_id"], int)

    def test_import_with_missing_org_fails(self, datapackage_with_refs: Path):
        """import_records fails when org_id references non-existent org."""
        input_records = [{"name": "Dave", "org_id": 999}]
        existing_records = []
        valid_fields = ["name", "org_id"]
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]

        stats, merged, results = import_records(
            input_records,
            existing_records,
            valid_fields,
            auto_id=True,
            field_defs=field_defs,
            base_path=datapackage_with_refs,
        )

        assert stats.failed == 1
        assert "org_id=999" in stats.errors[0]


class TestImportReferenceFieldsCSV:
    """CLI integration tests for reference field storage in CSV after import."""

    @pytest.fixture
    def datapackage_with_orgs(self, tmp_path: Path) -> Path:
        """Create datapackage with organizations for reference testing."""
        base_dir = tmp_path / "test-base"
        base_dir.mkdir()

        # Create organizations.csv
        with open(base_dir / "organizations.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name"])
            writer.writerow(["1", "ACME Corp"])
            writer.writerow(["2", "Beta Inc"])

        # Create empty persons.csv with org_id field
        with open(base_dir / "persons.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "org_id"])

        # Create datapackage.json
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
                            {"name": "org_id", "type": "integer"},
                        ],
                        "pipedrive_fields": [
                            {
                                "key": "id",
                                "name": "ID",
                                "field_type": "int",
                                "edit_flag": False,
                            },
                            {
                                "key": "name",
                                "name": "Name",
                                "field_type": "varchar",
                                "edit_flag": True,
                            },
                            {
                                "key": "org_id",
                                "name": "Organization",
                                "field_type": "org",
                                "edit_flag": True,
                            },
                        ],
                    },
                },
                {
                    "name": "organizations",
                    "path": "organizations.csv",
                    "schema": {
                        "fields": [
                            {"name": "id", "type": "integer"},
                            {"name": "name", "type": "string"},
                        ],
                    },
                },
            ],
        }
        (base_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

        return base_dir

    def test_import_stores_org_id_as_integer_in_csv(
        self, datapackage_with_orgs: Path, tmp_path: Path
    ):
        """org_id should be stored as integer in CSV, not JSON object."""
        # Create import file with org_id
        import_csv = tmp_path / "import.csv"
        with open(import_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "org_id"])
            writer.writerow(["John Doe", "1"])

        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "import",
            "-e", "persons",
            "-b", str(datapackage_with_orgs),
            "-i", str(import_csv),
            "--auto-id"
        ])

        assert result.exit_code == 0

        # Read CSV and verify org_id is integer, not JSON object
        with open(datapackage_with_orgs / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 1
        # org_id should be "1", not '{"value": 1, "name": "ACME Corp"}'
        assert rows[0]["org_id"] == "1"
        assert "{" not in rows[0]["org_id"]

    def test_import_validates_org_id_exists(
        self, datapackage_with_orgs: Path, tmp_path: Path
    ):
        """Import should fail if referenced org doesn't exist."""
        # Create import file with non-existent org_id
        import_csv = tmp_path / "import.csv"
        with open(import_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "org_id"])
            writer.writerow(["John Doe", "999"])

        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "import",
            "-e", "persons",
            "-b", str(datapackage_with_orgs),
            "-i", str(import_csv),
            "--auto-id"
        ])

        assert result.exit_code == 0  # Command succeeds but record fails
        assert "failed" in result.output.lower()
        assert "org_id=999" in result.output

    def test_imported_data_compatible_with_store_dry_run(
        self, datapackage_with_orgs: Path, tmp_path: Path
    ):
        """Imported records with org_id should work with store --dry-run."""
        # Create and import file with org_id
        import_csv = tmp_path / "import.csv"
        with open(import_csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["name", "org_id"])
            writer.writerow(["John Doe", "1"])

        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "import",
            "-e", "persons",
            "-b", str(datapackage_with_orgs),
            "-i", str(import_csv),
            "--auto-id"
        ])
        assert result.exit_code == 0

        # Now try store --dry-run
        result = runner.invoke(main, [
            "store",
            str(datapackage_with_orgs),
            "-e", "persons",
            "--dry-run"
        ])

        # Should not fail parsing org_id
        assert result.exit_code == 0
        assert "DRY RUN" in result.output


class TestRecordDeleteCommand:
    """Tests for record delete CLI command."""

    def test_delete_local_dry_run(self, temp_datapackage: Path):
        """record delete --dry-run shows records without deleting."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "delete",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-f", "name == 'Alice'",
            "-n",
            "--force"
        ])

        assert result.exit_code == 0
        assert "DRY RUN" in result.output

        # Verify records were NOT deleted
        with open(temp_datapackage / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2  # Unchanged

    def test_delete_local_with_filter(self, temp_datapackage: Path):
        """record delete removes matching records from CSV."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "delete",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-f", "name == 'Alice'",
            "--force"
        ])

        assert result.exit_code == 0
        assert "Deleted 1" in result.output

        # Verify Alice was deleted
        with open(temp_datapackage / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"

    def test_delete_requires_filter_or_force(self, temp_datapackage: Path):
        """record delete errors if no filter and no --force."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "delete",
            "-e", "persons",
            "-b", str(temp_datapackage),
        ])

        assert result.exit_code != 0
        assert "--filter is required" in result.output

    def test_delete_with_limit(self, temp_datapackage: Path):
        """record delete --limit restricts deletions."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "delete",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "--force",  # Delete all
            "--limit", "1"
        ])

        assert result.exit_code == 0
        assert "Deleted 1" in result.output

        # Verify only 1 was deleted
        with open(temp_datapackage / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 1

    def test_delete_empty_result(self, temp_datapackage: Path):
        """record delete shows message when filter matches nothing."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "delete",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-f", "name == 'NonExistent'",
            "--force"
        ])

        assert result.exit_code == 0
        assert "No records match filter" in result.output

        # Verify no records were deleted
        with open(temp_datapackage / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2

    def test_delete_writes_log(self, temp_datapackage: Path, tmp_path: Path):
        """record delete --log writes JSON lines log."""
        log_file = tmp_path / "delete.jsonl"

        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "delete",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-f", "name == 'Alice'",
            "--force",
            "-l", str(log_file)
        ])

        assert result.exit_code == 0
        assert log_file.exists()

        with open(log_file) as f:
            lines = f.readlines()

        assert len(lines) == 1
        log_entry = json.loads(lines[0])
        assert log_entry["action"] == "delete"
        assert str(log_entry["id"]) == "1"  # ID may be string or int
        assert log_entry["status"] == "deleted"

    def test_delete_force_all_records(self, temp_datapackage: Path):
        """record delete --force deletes all records when no filter."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "delete",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "--force"
        ])

        assert result.exit_code == 0
        assert "Deleted 2" in result.output

        # Verify all records were deleted
        with open(temp_datapackage / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 0

    def test_delete_confirmation_abort(self, temp_datapackage: Path):
        """record delete aborts on 'n' confirmation."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "delete",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-f", "name == 'Alice'"
        ], input="n\n")

        assert result.exit_code == 0
        assert "Aborted" in result.output

        # Verify records were NOT deleted
        with open(temp_datapackage / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2

    def test_delete_confirmation_proceed(self, temp_datapackage: Path):
        """record delete proceeds on 'y' confirmation."""
        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "delete",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-f", "name == 'Alice'"
        ], input="y\n")

        assert result.exit_code == 0
        assert "Deleted 1" in result.output

        # Verify Alice was deleted
        with open(temp_datapackage / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"
