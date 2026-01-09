"""Unit tests for integrity test helpers."""

from pathlib import Path

import pytest

from tests.fixtures.datapackage_factory import create_test_datapackage
from tests.integrity.helpers import (
    DatapackageState,
    assert_csv_values_changed,
    assert_csv_values_unchanged,
    assert_field_added,
    assert_field_removed,
    assert_other_entities_unchanged,
    assert_row_count_unchanged,
    assert_state_unchanged,
    capture_state,
    get_pipedrive_field_metadata,
)


class TestCaptureState:
    """Tests for capture_state function."""

    def test_captures_csv_columns(self, tmp_path: Path):
        """capture_state captures CSV column names."""
        create_test_datapackage(tmp_path, entities=["persons"])
        state = capture_state(tmp_path)

        assert "persons" in state.csv_columns
        assert "id" in state.csv_columns["persons"]
        assert "name" in state.csv_columns["persons"]
        assert "email" in state.csv_columns["persons"]

    def test_captures_csv_row_counts(self, tmp_path: Path):
        """capture_state captures row counts."""
        create_test_datapackage(tmp_path, entities=["persons"])
        state = capture_state(tmp_path)

        assert "persons" in state.csv_row_counts
        assert state.csv_row_counts["persons"] == 3  # Default sample data

    def test_captures_schema_fields(self, tmp_path: Path):
        """capture_state captures Frictionless schema.fields."""
        create_test_datapackage(tmp_path, entities=["persons"])
        state = capture_state(tmp_path)

        assert "persons" in state.schema_fields
        assert "id" in state.schema_fields["persons"]
        assert "name" in state.schema_fields["persons"]

    def test_captures_pipedrive_fields(self, tmp_path: Path):
        """capture_state captures pipedrive_fields metadata."""
        create_test_datapackage(tmp_path, entities=["persons"])
        state = capture_state(tmp_path)

        assert "persons" in state.pipedrive_fields
        assert "id" in state.pipedrive_fields["persons"]
        assert "abc123_custom_text" in state.pipedrive_fields["persons"]

    def test_captures_csv_checksums(self, tmp_path: Path):
        """capture_state computes CSV checksums."""
        create_test_datapackage(tmp_path, entities=["persons"])
        state = capture_state(tmp_path)

        assert "persons" in state.csv_checksums
        assert len(state.csv_checksums["persons"]) == 32  # MD5 hex length

    def test_captures_csv_data(self, tmp_path: Path):
        """capture_state loads CSV data."""
        create_test_datapackage(tmp_path, entities=["persons"])
        state = capture_state(tmp_path)

        assert "persons" in state.csv_data
        assert len(state.csv_data["persons"]) == 3
        assert state.csv_data["persons"][0]["name"] == "Alice Smith"

    def test_handles_nonexistent_path(self, tmp_path: Path):
        """capture_state handles nonexistent datapackage."""
        state = capture_state(tmp_path / "nonexistent")

        assert state.csv_columns == {}
        assert state.csv_row_counts == {}

    def test_multi_entity(self, tmp_path: Path):
        """capture_state captures multiple entities."""
        create_test_datapackage(
            tmp_path, entities=["persons", "organizations", "deals"]
        )
        state = capture_state(tmp_path)

        assert "persons" in state.csv_columns
        assert "organizations" in state.csv_columns
        assert "deals" in state.csv_columns


class TestDatapackageStateEquality:
    """Tests for DatapackageState equality."""

    def test_equal_states(self, tmp_path: Path):
        """Two states from same datapackage are equal."""
        create_test_datapackage(tmp_path, entities=["persons"])
        state1 = capture_state(tmp_path)
        state2 = capture_state(tmp_path)

        assert state1 == state2

    def test_different_checksums_not_equal(self, tmp_path: Path):
        """States with different checksums are not equal."""
        create_test_datapackage(tmp_path, entities=["persons"])
        state1 = capture_state(tmp_path)

        # Modify CSV
        csv_path = tmp_path / "persons.csv"
        content = csv_path.read_text()
        csv_path.write_text(content + "4,New Person,new@email.com,123,,,,\n")

        state2 = capture_state(tmp_path)

        assert state1 != state2


class TestAssertStateUnchanged:
    """Tests for assert_state_unchanged function."""

    def test_passes_for_identical_states(self, tmp_path: Path):
        """Passes when states are identical."""
        create_test_datapackage(tmp_path, entities=["persons"])
        state1 = capture_state(tmp_path)
        state2 = capture_state(tmp_path)

        # Should not raise
        assert_state_unchanged(state1, state2)

    def test_fails_for_different_columns(self, tmp_path: Path):
        """Fails when CSV columns differ."""
        create_test_datapackage(tmp_path, entities=["persons"])
        state1 = capture_state(tmp_path)

        state2 = DatapackageState()
        state2.csv_columns = {"persons": {"id", "name"}}  # Missing columns
        state2.csv_row_counts = state1.csv_row_counts.copy()
        state2.schema_fields = state1.schema_fields.copy()
        state2.pipedrive_fields = state1.pipedrive_fields.copy()
        state2.csv_checksums = state1.csv_checksums.copy()

        with pytest.raises(AssertionError, match="CSV columns changed"):
            assert_state_unchanged(state1, state2)


class TestAssertFieldRemoved:
    """Tests for assert_field_removed function."""

    def test_detects_field_removal(self, tmp_path: Path):
        """Detects when a field is removed from all locations."""
        create_test_datapackage(tmp_path, entities=["persons"])
        before = capture_state(tmp_path)

        # Create after state with field removed
        after = DatapackageState()
        after.csv_columns = {
            "persons": before.csv_columns["persons"] - {"abc123_custom_text"}
        }
        after.schema_fields = {
            "persons": [f for f in before.schema_fields["persons"]
                        if f != "abc123_custom_text"]
        }
        after.pipedrive_fields = {
            "persons": [f for f in before.pipedrive_fields["persons"]
                        if f != "abc123_custom_text"]
        }

        # Should not raise
        assert_field_removed(before, after, "persons", "abc123_custom_text")

    def test_fails_if_field_still_in_csv(self, tmp_path: Path):
        """Fails if field still exists in CSV."""
        create_test_datapackage(tmp_path, entities=["persons"])
        before = capture_state(tmp_path)
        after = capture_state(tmp_path)  # Unchanged

        with pytest.raises(AssertionError, match="still in CSV columns"):
            assert_field_removed(before, after, "persons", "abc123_custom_text")


class TestAssertFieldAdded:
    """Tests for assert_field_added function."""

    def test_detects_field_addition(self, tmp_path: Path):
        """Detects when a field is added to all locations."""
        create_test_datapackage(tmp_path, entities=["persons"])
        before = capture_state(tmp_path)

        # Create after state with field added
        after = DatapackageState()
        after.csv_columns = {
            "persons": before.csv_columns["persons"] | {"new_field"}
        }
        after.schema_fields = {
            "persons": before.schema_fields["persons"] + ["new_field"]
        }
        after.pipedrive_fields = {
            "persons": before.pipedrive_fields["persons"] + ["new_field"]
        }

        # Should not raise
        assert_field_added(before, after, "persons", "new_field")

    def test_fails_if_field_already_existed(self, tmp_path: Path):
        """Fails if field already existed before."""
        create_test_datapackage(tmp_path, entities=["persons"])
        before = capture_state(tmp_path)
        after = capture_state(tmp_path)

        with pytest.raises(AssertionError, match="already existed"):
            assert_field_added(before, after, "persons", "abc123_custom_text")


class TestAssertRowCountUnchanged:
    """Tests for assert_row_count_unchanged function."""

    def test_passes_for_same_count(self, tmp_path: Path):
        """Passes when row count is unchanged."""
        create_test_datapackage(tmp_path, entities=["persons"])
        before = capture_state(tmp_path)
        after = capture_state(tmp_path)

        # Should not raise
        assert_row_count_unchanged(before, after, "persons")

    def test_fails_for_different_count(self, tmp_path: Path):
        """Fails when row count differs."""
        create_test_datapackage(tmp_path, entities=["persons"])
        before = capture_state(tmp_path)

        after = DatapackageState()
        after.csv_row_counts = {"persons": 10}  # Different

        with pytest.raises(AssertionError, match="Row count changed"):
            assert_row_count_unchanged(before, after, "persons")


class TestAssertOtherEntitiesUnchanged:
    """Tests for assert_other_entities_unchanged function."""

    def test_passes_when_only_expected_entity_changes(self, tmp_path: Path):
        """Passes when only the expected entity changed."""
        create_test_datapackage(
            tmp_path, entities=["persons", "organizations"]
        )
        before = capture_state(tmp_path)

        # Modify only persons
        after = DatapackageState()
        after.csv_columns = before.csv_columns.copy()
        after.csv_columns["persons"] = {"id", "name"}  # Changed
        after.csv_row_counts = before.csv_row_counts.copy()
        after.schema_fields = before.schema_fields.copy()
        after.pipedrive_fields = before.pipedrive_fields.copy()
        after.csv_checksums = before.csv_checksums.copy()

        # Should not raise - organizations is unchanged
        assert_other_entities_unchanged(before, after, except_entity="persons")

    def test_fails_when_other_entity_changes(self, tmp_path: Path):
        """Fails when an unexpected entity changed."""
        create_test_datapackage(
            tmp_path, entities=["persons", "organizations"]
        )
        before = capture_state(tmp_path)

        # Modify organizations (unexpected)
        after = DatapackageState()
        after.csv_columns = before.csv_columns.copy()
        after.csv_columns["organizations"] = {"id"}  # Changed
        after.csv_row_counts = before.csv_row_counts.copy()
        after.schema_fields = before.schema_fields.copy()
        after.pipedrive_fields = before.pipedrive_fields.copy()
        after.csv_checksums = before.csv_checksums.copy()

        with pytest.raises(AssertionError, match="changed for organizations"):
            assert_other_entities_unchanged(before, after, except_entity="persons")


class TestAssertCsvValuesChanged:
    """Tests for assert_csv_values_changed function."""

    def test_detects_value_changes(self, tmp_path: Path):
        """Detects when values have changed."""
        create_test_datapackage(tmp_path, entities=["persons"])
        before = capture_state(tmp_path)

        # Modify CSV data
        after = DatapackageState()
        after.csv_data = {
            "persons": [
                {**row, "name": row["name"].upper()}
                for row in before.csv_data["persons"]
            ]
        }

        # Should not raise
        changes = assert_csv_values_changed(before, after, "persons", "name")
        assert changes == 3  # All 3 rows changed

    def test_fails_when_no_changes(self, tmp_path: Path):
        """Fails when values are unchanged."""
        create_test_datapackage(tmp_path, entities=["persons"])
        before = capture_state(tmp_path)
        after = capture_state(tmp_path)

        with pytest.raises(AssertionError, match="No values changed"):
            assert_csv_values_changed(before, after, "persons", "name")


class TestAssertCsvValuesUnchanged:
    """Tests for assert_csv_values_unchanged function."""

    def test_passes_when_unchanged(self, tmp_path: Path):
        """Passes when values are unchanged."""
        create_test_datapackage(tmp_path, entities=["persons"])
        before = capture_state(tmp_path)
        after = capture_state(tmp_path)

        # Should not raise
        assert_csv_values_unchanged(before, after, "persons", "name")

    def test_fails_when_changed(self, tmp_path: Path):
        """Fails when values have changed."""
        create_test_datapackage(tmp_path, entities=["persons"])
        before = capture_state(tmp_path)

        after = DatapackageState()
        after.csv_data = {
            "persons": [
                {**row, "name": "CHANGED"}
                for row in before.csv_data["persons"]
            ]
        }

        with pytest.raises(AssertionError, match="Value changed"):
            assert_csv_values_unchanged(before, after, "persons", "name")


class TestGetPipedriveFieldMetadata:
    """Tests for get_pipedrive_field_metadata function."""

    def test_returns_field_metadata(self, tmp_path: Path):
        """Returns field metadata for existing field."""
        create_test_datapackage(tmp_path, entities=["persons"])

        metadata = get_pipedrive_field_metadata(tmp_path, "persons", "abc123_custom_text")

        assert metadata is not None
        assert metadata["key"] == "abc123_custom_text"
        assert metadata["name"] == "Custom Text"
        assert metadata["field_type"] == "varchar"

    def test_returns_none_for_nonexistent_field(self, tmp_path: Path):
        """Returns None for nonexistent field."""
        create_test_datapackage(tmp_path, entities=["persons"])

        metadata = get_pipedrive_field_metadata(tmp_path, "persons", "nonexistent")

        assert metadata is None

    def test_returns_none_for_nonexistent_entity(self, tmp_path: Path):
        """Returns None for nonexistent entity."""
        create_test_datapackage(tmp_path, entities=["persons"])

        metadata = get_pipedrive_field_metadata(tmp_path, "nonexistent", "id")

        assert metadata is None
