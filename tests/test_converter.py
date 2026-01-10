"""Tests for data convert command and converter module."""

import csv
import json
from pathlib import Path

import pytest
from click.testing import CliRunner

# Skip all tests if openpyxl is not available
openpyxl = pytest.importorskip("openpyxl")
Workbook = openpyxl.Workbook

from pipedrive_cli.cli import main  # noqa: E402
from pipedrive_cli.converter import (  # noqa: E402
    ConvertResult,
    detect_output_format,
    load_xlsx,
    write_csv,
    write_json,
)


@pytest.fixture
def sample_xlsx(tmp_path: Path) -> Path:
    """Create a sample XLSX file for testing."""
    xlsx_path = tmp_path / "sample.xlsx"

    wb = Workbook()
    ws = wb.active
    ws.title = "Data"

    # Header row
    ws["A1"] = "id"
    ws["B1"] = "name"
    ws["C1"] = "email"
    ws["D1"] = "value"

    # Data rows
    ws["A2"] = 1
    ws["B2"] = "Alice"
    ws["C2"] = "alice@example.com"
    ws["D2"] = 100.50

    ws["A3"] = 2
    ws["B3"] = "Bob"
    ws["C3"] = "bob@example.com"
    ws["D3"] = 200.75

    wb.save(xlsx_path)
    return xlsx_path


@pytest.fixture
def sample_xlsx_with_hyperlinks(tmp_path: Path) -> Path:
    """Create a sample XLSX file with hyperlinks."""
    xlsx_path = tmp_path / "links.xlsx"

    wb = Workbook()
    ws = wb.active

    # Header row
    ws["A1"] = "name"
    ws["B1"] = "website"

    # Data rows with hyperlinks
    ws["A2"] = "Company A"
    ws["B2"] = "Click here"
    ws["B2"].hyperlink = "https://example.com/a"

    ws["A3"] = "Company B"
    ws["B3"] = "Visit site"
    ws["B3"].hyperlink = "https://example.com/b"

    wb.save(xlsx_path)
    return xlsx_path


@pytest.fixture
def sample_xlsx_multisheet(tmp_path: Path) -> Path:
    """Create a sample XLSX file with multiple sheets."""
    xlsx_path = tmp_path / "multisheet.xlsx"

    wb = Workbook()

    # First sheet
    ws1 = wb.active
    ws1.title = "Sheet1"
    ws1["A1"] = "col1"
    ws1["A2"] = "data1"

    # Second sheet
    ws2 = wb.create_sheet("Sheet2")
    ws2["A1"] = "col2"
    ws2["A2"] = "data2"

    wb.save(xlsx_path)
    return xlsx_path


class TestLoadXlsx:
    """Tests for load_xlsx function."""

    def test_load_basic_xlsx(self, sample_xlsx: Path):
        """load_xlsx loads basic XLSX file."""
        result = load_xlsx(sample_xlsx)

        assert isinstance(result, ConvertResult)
        assert len(result.records) == 2
        assert result.fieldnames == ["id", "name", "email", "value"]
        assert result.records[0]["name"] == "Alice"
        assert result.records[1]["name"] == "Bob"

    def test_load_xlsx_specific_sheet(self, sample_xlsx_multisheet: Path):
        """load_xlsx loads specific sheet."""
        result = load_xlsx(sample_xlsx_multisheet, sheet="Sheet2")

        assert len(result.records) == 1
        assert result.fieldnames == ["col2"]
        assert result.records[0]["col2"] == "data2"

    def test_load_xlsx_first_sheet_by_default(self, sample_xlsx_multisheet: Path):
        """load_xlsx loads first sheet by default."""
        result = load_xlsx(sample_xlsx_multisheet)

        assert result.fieldnames == ["col1"]

    def test_load_xlsx_invalid_sheet_error(self, sample_xlsx: Path):
        """load_xlsx raises error for invalid sheet."""
        with pytest.raises(Exception) as exc_info:
            load_xlsx(sample_xlsx, sheet="NonExistent")

        assert "not found" in str(exc_info.value).lower()

    def test_load_xlsx_preserves_hyperlinks(self, sample_xlsx_with_hyperlinks: Path):
        """load_xlsx with preserve_links extracts hyperlink URLs."""
        result = load_xlsx(sample_xlsx_with_hyperlinks, preserve_links=True)

        assert result.records[0]["website"] == "https://example.com/a"
        assert result.records[1]["website"] == "https://example.com/b"
        assert result.stats.hyperlinks_found == 2
        assert result.stats.hyperlinks_preserved == 2

    def test_load_xlsx_no_preserve_links_uses_display(self, sample_xlsx_with_hyperlinks: Path):
        """load_xlsx without preserve_links uses display text."""
        result = load_xlsx(sample_xlsx_with_hyperlinks, preserve_links=False)

        assert result.records[0]["website"] == "Click here"
        assert result.records[1]["website"] == "Visit site"
        assert result.stats.hyperlinks_found == 2
        assert result.stats.hyperlinks_preserved == 0

    def test_load_xlsx_stats(self, sample_xlsx: Path):
        """load_xlsx returns correct stats."""
        result = load_xlsx(sample_xlsx)

        assert result.stats.total_rows == 2
        assert result.stats.total_columns == 4

    def test_load_xlsx_custom_header_row(self, tmp_path: Path):
        """load_xlsx with custom header_row."""
        xlsx_path = tmp_path / "custom_header.xlsx"

        wb = Workbook()
        ws = wb.active
        ws["A1"] = "Title row"
        ws["A2"] = "header"  # Header on row 2
        ws["A3"] = "data"
        wb.save(xlsx_path)

        result = load_xlsx(xlsx_path, header_row=2)

        assert result.fieldnames == ["header"]
        assert result.records[0]["header"] == "data"


class TestWriteCsv:
    """Tests for write_csv function."""

    def test_write_csv_basic(self, tmp_path: Path):
        """write_csv writes records to CSV."""
        output = tmp_path / "output.csv"
        records = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]
        fieldnames = ["name", "age"]

        write_csv(records, fieldnames, output)

        assert output.exists()
        with open(output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == "30"

    def test_write_csv_handles_nested_json(self, tmp_path: Path):
        """write_csv flattens nested objects to JSON strings."""
        output = tmp_path / "output.csv"
        records = [
            {"name": "Alice", "data": {"key": "value"}},
        ]
        fieldnames = ["name", "data"]

        write_csv(records, fieldnames, output)

        with open(output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert rows[0]["data"] == '{"key": "value"}'


class TestWriteJson:
    """Tests for write_json function."""

    def test_write_json_basic(self, tmp_path: Path):
        """write_json writes records to JSON."""
        output = tmp_path / "output.json"
        records = [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

        write_json(records, output)

        assert output.exists()
        with open(output) as f:
            data = json.load(f)

        assert len(data) == 2
        assert data[0]["name"] == "Alice"


class TestDetectOutputFormat:
    """Tests for detect_output_format function."""

    def test_detect_csv(self, tmp_path: Path):
        """detect_output_format detects CSV."""
        assert detect_output_format(tmp_path / "output.csv") == "csv"

    def test_detect_json(self, tmp_path: Path):
        """detect_output_format detects JSON."""
        assert detect_output_format(tmp_path / "output.json") == "json"

    def test_detect_unknown_raises(self, tmp_path: Path):
        """detect_output_format raises for unknown extension."""
        with pytest.raises(Exception):
            detect_output_format(tmp_path / "output.unknown")


class TestDataConvertCommand:
    """Tests for data convert CLI command."""

    def test_convert_xlsx_to_csv(self, sample_xlsx: Path, tmp_path: Path):
        """data convert converts XLSX to CSV."""
        output = tmp_path / "output.csv"

        runner = CliRunner()
        result = runner.invoke(main, [
            "data", "convert",
            str(sample_xlsx),
            "-o", str(output)
        ])

        assert result.exit_code == 0
        assert output.exists()

        with open(output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"

    def test_convert_xlsx_to_json(self, sample_xlsx: Path, tmp_path: Path):
        """data convert converts XLSX to JSON."""
        output = tmp_path / "output.json"

        runner = CliRunner()
        result = runner.invoke(main, [
            "data", "convert",
            str(sample_xlsx),
            "-o", str(output)
        ])

        assert result.exit_code == 0
        assert output.exists()

        with open(output) as f:
            data = json.load(f)

        assert len(data) == 2
        assert data[0]["name"] == "Alice"

    def test_convert_with_preserve_links(
        self, sample_xlsx_with_hyperlinks: Path, tmp_path: Path
    ):
        """data convert --preserve-links extracts hyperlinks."""
        output = tmp_path / "output.csv"

        runner = CliRunner()
        result = runner.invoke(main, [
            "data", "convert",
            str(sample_xlsx_with_hyperlinks),
            "-o", str(output),
            "--preserve-links"
        ])

        assert result.exit_code == 0

        with open(output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert rows[0]["website"] == "https://example.com/a"

    def test_convert_with_sheet_option(
        self, sample_xlsx_multisheet: Path, tmp_path: Path
    ):
        """data convert -s selects sheet."""
        output = tmp_path / "output.csv"

        runner = CliRunner()
        result = runner.invoke(main, [
            "data", "convert",
            str(sample_xlsx_multisheet),
            "-o", str(output),
            "-s", "Sheet2"
        ])

        assert result.exit_code == 0

        with open(output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert rows[0]["col2"] == "data2"

    def test_convert_with_header_row(self, tmp_path: Path):
        """data convert -r selects header row."""
        xlsx_path = tmp_path / "header_row.xlsx"

        wb = Workbook()
        ws = wb.active
        ws["A1"] = "Skip this"
        ws["A2"] = "header"
        ws["A3"] = "data"
        wb.save(xlsx_path)

        output = tmp_path / "output.csv"

        runner = CliRunner()
        result = runner.invoke(main, [
            "data", "convert",
            str(xlsx_path),
            "-o", str(output),
            "-r", "2"
        ])

        assert result.exit_code == 0

        with open(output) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert "header" in reader.fieldnames
        assert rows[0]["header"] == "data"

    def test_convert_shows_stats(self, sample_xlsx: Path, tmp_path: Path):
        """data convert shows conversion stats."""
        output = tmp_path / "output.csv"

        runner = CliRunner()
        result = runner.invoke(main, [
            "data", "convert",
            str(sample_xlsx),
            "-o", str(output)
        ])

        assert result.exit_code == 0
        assert "2" in result.output  # 2 rows

    def test_convert_help(self):
        """data convert command shows help."""
        runner = CliRunner()
        result = runner.invoke(main, ["data", "convert", "-h"])

        assert result.exit_code == 0
        assert "--output" in result.output
        assert "--sheet" in result.output
        assert "--header-row" in result.output
        assert "--preserve-links" in result.output
