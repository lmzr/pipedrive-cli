"""Tests for record import command and importer module."""

import csv
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from pipedrive_cli.cli import main
from pipedrive_cli.importer import (
    build_dedup_index,
    detect_format,
    get_max_id,
    import_records,
    load_csv_records,
    load_input_file,
    load_json_records,
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
            writer.writerow(["name", "email", "custom_field"])
            writer.writerow(["Alice Updated", "alice@example.com", "new_value"])

        runner = CliRunner()
        result = runner.invoke(main, [
            "record", "import",
            "-e", "persons",
            "-b", str(temp_datapackage),
            "-i", str(csv_path),
            "-k", "email",
            "--on-duplicate", "update"
        ])

        assert result.exit_code == 0
        assert "updated" in result.output.lower()

        # Verify record was updated
        with open(temp_datapackage / "persons.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        alice = next(r for r in rows if r["email"] == "alice@example.com")
        assert alice["name"] == "Alice Updated"
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
