"""Tests for backup functionality."""

from frictionless.fields import (
    ArrayField,
    DateField,
    IntegerField,
    NumberField,
    StringField,
    TimeField,
)

from pipedrive_cli.backup import (
    PIPEDRIVE_TO_FRICTIONLESS_TYPES,
    build_schema_from_fields,
    field_to_schema_field,
    normalize_record_for_export,
)


class TestPipedriveToFrictionlessTypes:
    """Tests for the type mapping constant."""

    def test_varchar_maps_to_string(self):
        """varchar field type maps to string."""
        assert PIPEDRIVE_TO_FRICTIONLESS_TYPES["varchar"] == "string"

    def test_int_maps_to_integer(self):
        """int field type maps to integer."""
        assert PIPEDRIVE_TO_FRICTIONLESS_TYPES["int"] == "integer"

    def test_double_maps_to_number(self):
        """double field type maps to number."""
        assert PIPEDRIVE_TO_FRICTIONLESS_TYPES["double"] == "number"

    def test_date_maps_to_date(self):
        """date field type maps to date."""
        assert PIPEDRIVE_TO_FRICTIONLESS_TYPES["date"] == "date"

    def test_reference_fields_map_to_integer(self):
        """Reference field types (org, people, user) map to integer."""
        assert PIPEDRIVE_TO_FRICTIONLESS_TYPES["org"] == "integer"
        assert PIPEDRIVE_TO_FRICTIONLESS_TYPES["people"] == "integer"
        assert PIPEDRIVE_TO_FRICTIONLESS_TYPES["user"] == "integer"

    def test_address_maps_to_string(self):
        """address field type maps to string (API returns formatted string)."""
        assert PIPEDRIVE_TO_FRICTIONLESS_TYPES["address"] == "string"

    def test_visible_to_maps_to_string(self):
        """visible_to field type maps to string (API returns '3' etc)."""
        assert PIPEDRIVE_TO_FRICTIONLESS_TYPES["visible_to"] == "string"

    def test_set_maps_to_array(self):
        """set field type maps to array."""
        assert PIPEDRIVE_TO_FRICTIONLESS_TYPES["set"] == "array"


class TestFieldToSchemaField:
    """Tests for field_to_schema_field function."""

    def test_varchar_creates_string_field(self):
        """varchar field creates StringField."""
        field_def = {"key": "name", "name": "Name", "field_type": "varchar"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, StringField)
        assert result.name == "name"
        assert result.title == "Name"

    def test_int_creates_integer_field(self):
        """int field creates IntegerField."""
        field_def = {"key": "id", "name": "ID", "field_type": "int"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, IntegerField)
        assert result.name == "id"

    def test_double_creates_number_field(self):
        """double field creates NumberField."""
        field_def = {"key": "value", "name": "Value", "field_type": "double"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, NumberField)
        assert result.name == "value"

    def test_date_creates_date_field(self):
        """date field creates DateField."""
        field_def = {"key": "add_time", "name": "Add Time", "field_type": "date"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, DateField)
        assert result.name == "add_time"

    def test_time_creates_time_field(self):
        """time field creates TimeField."""
        field_def = {"key": "due_time", "name": "Due Time", "field_type": "time"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, TimeField)
        assert result.name == "due_time"

    def test_set_creates_array_field(self):
        """set field creates ArrayField."""
        field_def = {"key": "tags", "name": "Tags", "field_type": "set"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, ArrayField)
        assert result.name == "tags"

    def test_org_creates_integer_field(self):
        """org field creates IntegerField (stores ID only)."""
        field_def = {"key": "org_id", "name": "Organization", "field_type": "org"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, IntegerField)
        assert result.name == "org_id"

    def test_people_creates_integer_field(self):
        """people field creates IntegerField (stores ID only)."""
        field_def = {"key": "person_id", "name": "Person", "field_type": "people"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, IntegerField)
        assert result.name == "person_id"

    def test_user_creates_integer_field(self):
        """user field creates IntegerField (stores ID only)."""
        field_def = {"key": "owner_id", "name": "Owner", "field_type": "user"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, IntegerField)
        assert result.name == "owner_id"

    def test_address_creates_string_field(self):
        """address field creates StringField."""
        field_def = {"key": "address", "name": "Address", "field_type": "address"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, StringField)
        assert result.name == "address"

    def test_visible_to_creates_string_field(self):
        """visible_to field creates StringField."""
        field_def = {
            "key": "visible_to",
            "name": "Visible to",
            "field_type": "visible_to",
        }
        result = field_to_schema_field(field_def)

        assert isinstance(result, StringField)
        assert result.name == "visible_to"

    def test_unknown_type_defaults_to_string(self):
        """Unknown field type defaults to StringField."""
        field_def = {"key": "custom", "name": "Custom", "field_type": "unknown_type"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, StringField)
        assert result.name == "custom"

    def test_missing_type_defaults_to_string(self):
        """Missing field_type defaults to StringField."""
        field_def = {"key": "custom", "name": "Custom"}
        result = field_to_schema_field(field_def)

        assert isinstance(result, StringField)


class TestBuildSchemaFromFields:
    """Tests for build_schema_from_fields function."""

    def test_creates_schema_with_correct_types(self):
        """Schema fields have correct types from Pipedrive definitions."""
        field_defs = [
            {"key": "id", "name": "ID", "field_type": "int"},
            {"key": "name", "name": "Name", "field_type": "varchar"},
            {"key": "value", "name": "Value", "field_type": "double"},
        ]
        csv_columns = ["id", "name", "value"]

        schema = build_schema_from_fields(field_defs, csv_columns)

        assert len(schema.fields) == 3
        assert isinstance(schema.fields[0], IntegerField)
        assert schema.fields[0].name == "id"
        assert isinstance(schema.fields[1], StringField)
        assert schema.fields[1].name == "name"
        assert isinstance(schema.fields[2], NumberField)
        assert schema.fields[2].name == "value"

    def test_unknown_column_defaults_to_string(self):
        """CSV column not in field_defs defaults to StringField."""
        field_defs = [
            {"key": "id", "name": "ID", "field_type": "int"},
        ]
        csv_columns = ["id", "unknown_column"]

        schema = build_schema_from_fields(field_defs, csv_columns)

        assert len(schema.fields) == 2
        assert isinstance(schema.fields[0], IntegerField)
        assert isinstance(schema.fields[1], StringField)
        assert schema.fields[1].name == "unknown_column"

    def test_preserves_csv_column_order(self):
        """Schema fields follow CSV column order, not field_defs order."""
        field_defs = [
            {"key": "name", "name": "Name", "field_type": "varchar"},
            {"key": "id", "name": "ID", "field_type": "int"},
        ]
        csv_columns = ["id", "name"]  # Different order

        schema = build_schema_from_fields(field_defs, csv_columns)

        assert schema.fields[0].name == "id"
        assert schema.fields[1].name == "name"

    def test_reference_fields_are_integers(self):
        """Reference fields (org, people, user) create IntegerField."""
        field_defs = [
            {"key": "org_id", "name": "Organization", "field_type": "org"},
            {"key": "person_id", "name": "Person", "field_type": "people"},
            {"key": "owner_id", "name": "Owner", "field_type": "user"},
        ]
        csv_columns = ["org_id", "person_id", "owner_id"]

        schema = build_schema_from_fields(field_defs, csv_columns)

        assert all(isinstance(f, IntegerField) for f in schema.fields)


class TestNormalizeRecordForExport:
    """Tests for normalize_record_for_export function."""

    def test_extracts_org_id_from_object(self):
        """org field object extracts integer ID."""
        record = {
            "name": "John Doe",
            "org_id": {"value": 431, "name": "ACME Corp", "people_count": 5},
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]

        result = normalize_record_for_export(record, field_defs)

        assert result["name"] == "John Doe"
        assert result["org_id"] == 431

    def test_extracts_person_id_from_object(self):
        """people field object extracts integer ID."""
        record = {
            "title": "Big Deal",
            "person_id": {"value": 123, "name": "Jane Smith"},
        }
        field_defs = [
            {"key": "title", "field_type": "varchar"},
            {"key": "person_id", "field_type": "people"},
        ]

        result = normalize_record_for_export(record, field_defs)

        assert result["person_id"] == 123

    def test_extracts_owner_id_from_object(self):
        """user field object extracts integer ID."""
        record = {
            "name": "ACME",
            "owner_id": {"id": 100, "value": 100, "name": "Admin User"},
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "owner_id", "field_type": "user"},
        ]

        result = normalize_record_for_export(record, field_defs)

        assert result["owner_id"] == 100

    def test_integer_reference_unchanged(self):
        """Reference field that's already integer passes through."""
        record = {
            "name": "John",
            "org_id": 431,
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]

        result = normalize_record_for_export(record, field_defs)

        assert result["org_id"] == 431

    def test_null_reference_unchanged(self):
        """Null reference field passes through as None."""
        record = {
            "name": "John",
            "org_id": None,
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
        ]

        result = normalize_record_for_export(record, field_defs)

        assert result["org_id"] is None

    def test_non_reference_fields_unchanged(self):
        """Non-reference fields pass through unchanged."""
        record = {
            "name": "John Doe",
            "email": [{"value": "john@example.com", "primary": True}],
            "phone": "+1234567890",
            "value": 10000.50,
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
            {"key": "email", "field_type": "varchar"},
            {"key": "phone", "field_type": "phone"},
            {"key": "value", "field_type": "double"},
        ]

        result = normalize_record_for_export(record, field_defs)

        assert result["name"] == "John Doe"
        assert result["email"] == [{"value": "john@example.com", "primary": True}]
        assert result["phone"] == "+1234567890"
        assert result["value"] == 10000.50

    def test_unknown_field_passthrough(self):
        """Fields not in field_defs pass through unchanged."""
        record = {
            "name": "John",
            "unknown_field": "value",
        }
        field_defs = [
            {"key": "name", "field_type": "varchar"},
        ]

        result = normalize_record_for_export(record, field_defs)

        assert result["unknown_field"] == "value"

    def test_multiple_reference_fields(self):
        """Multiple reference fields all extract IDs."""
        record = {
            "title": "Big Deal",
            "org_id": {"value": 431, "name": "ACME"},
            "person_id": {"value": 123, "name": "John"},
            "owner_id": {"id": 100, "value": 100, "name": "Admin"},
        }
        field_defs = [
            {"key": "title", "field_type": "varchar"},
            {"key": "org_id", "field_type": "org"},
            {"key": "person_id", "field_type": "people"},
            {"key": "owner_id", "field_type": "user"},
        ]

        result = normalize_record_for_export(record, field_defs)

        assert result["org_id"] == 431
        assert result["person_id"] == 123
        assert result["owner_id"] == 100
