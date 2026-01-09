"""Tests for prefix matching module."""

import pytest

from pipedrive_cli.matching import (
    AmbiguousMatchError,
    NoMatchError,
    find_field_by_key,
    find_field_matches,
    match_entities,
    match_entity,
    match_field,
)


class TestMatchEntity:
    """Tests for entity prefix matching."""

    def test_exact_match(self):
        """Exact entity name returns correct EntityConfig."""
        entity = match_entity("persons")
        assert entity.name == "persons"
        assert entity.endpoint == "/v1/persons"

    def test_exact_match_case_insensitive(self):
        """Exact match is case-insensitive."""
        entity = match_entity("PERSONS")
        assert entity.name == "persons"

    def test_prefix_unique_match(self):
        """Unique prefix returns correct EntityConfig."""
        # "per" should match only "persons" (not "products")
        entity = match_entity("per")
        assert entity.name == "persons"

    def test_prefix_organizations(self):
        """'org' should match organizations."""
        entity = match_entity("org")
        assert entity.name == "organizations"

    def test_prefix_deals(self):
        """'deal' should match deals."""
        entity = match_entity("deal")
        assert entity.name == "deals"

    def test_prefix_ambiguous_raises(self):
        """Ambiguous prefix raises AmbiguousMatchError."""
        # "p" matches persons and products
        with pytest.raises(AmbiguousMatchError) as exc_info:
            match_entity("p")

        assert exc_info.value.prefix == "p"
        assert "persons" in exc_info.value.matches
        assert "products" in exc_info.value.matches

    def test_no_match_raises(self):
        """Non-matching prefix raises NoMatchError."""
        with pytest.raises(NoMatchError) as exc_info:
            match_entity("xyz")

        assert exc_info.value.prefix == "xyz"
        assert "persons" in exc_info.value.available


class TestMatchEntities:
    """Tests for matching multiple entity prefixes."""

    def test_multiple_prefixes(self):
        """Multiple prefixes return correct EntityConfigs."""
        entities = match_entities(["per", "org", "deal"])
        names = [e.name for e in entities]

        assert names == ["persons", "organizations", "deals"]

    def test_deduplicate(self):
        """Duplicate prefixes are deduplicated."""
        entities = match_entities(["per", "persons", "per"])
        names = [e.name for e in entities]

        assert names == ["persons"]

    def test_preserve_order(self):
        """Order is preserved after deduplication."""
        entities = match_entities(["deal", "per", "org"])
        names = [e.name for e in entities]

        assert names == ["deals", "persons", "organizations"]

    def test_ambiguous_raises(self):
        """Ambiguous prefix in list raises AmbiguousMatchError."""
        with pytest.raises(AmbiguousMatchError):
            match_entities(["per", "p"])  # "p" is ambiguous


class TestMatchField:
    """Tests for field prefix matching."""

    @pytest.fixture
    def sample_fields(self) -> list[dict]:
        """Sample field definitions."""
        return [
            {"key": "first_name", "name": "First Name", "field_type": "varchar"},
            {"key": "last_name", "name": "Last Name", "field_type": "varchar"},
            {"key": "email", "name": "Email", "field_type": "varchar"},
            {"key": "company_id", "name": "Company", "field_type": "int"},
        ]

    def test_exact_match(self, sample_fields):
        """Exact field key returns field without confirmation."""
        field = match_field(sample_fields, "first_name", confirm=False)
        assert field["key"] == "first_name"

    def test_prefix_unique_match_no_confirm(self, sample_fields):
        """Unique prefix returns field when confirm=False."""
        field = match_field(sample_fields, "first", confirm=False)
        assert field["key"] == "first_name"

    def test_prefix_unique_match_with_confirm(self, sample_fields, monkeypatch):
        """Unique prefix with confirm=True asks for confirmation."""
        # Simulate user pressing Enter (accept default)
        inputs = iter(["y"])
        monkeypatch.setattr("click.prompt", lambda *args, **kwargs: next(inputs))

        field = match_field(sample_fields, "first", confirm=True)
        assert field["key"] == "first_name"

    def test_prefix_ambiguous_raises(self, sample_fields):
        """Ambiguous prefix raises AmbiguousMatchError."""
        # Both "first_name" and "last_name" contain "_name" but start differently
        # Let's test with a different ambiguity
        fields_with_ambiguity = sample_fields + [
            {"key": "first_contact", "name": "First Contact", "field_type": "varchar"}
        ]

        with pytest.raises(AmbiguousMatchError) as exc_info:
            match_field(fields_with_ambiguity, "first", confirm=False)

        assert exc_info.value.prefix == "first"
        assert "first_name" in exc_info.value.matches
        assert "first_contact" in exc_info.value.matches

    def test_no_match_raises(self, sample_fields):
        """Non-matching prefix raises NoMatchError."""
        with pytest.raises(NoMatchError) as exc_info:
            match_field(sample_fields, "xyz", confirm=False)

        assert exc_info.value.prefix == "xyz"

    def test_user_cancels_with_n(self, sample_fields, monkeypatch):
        """User cancelling with 'n' raises ClickException."""
        import click

        inputs = iter(["n"])
        monkeypatch.setattr("click.prompt", lambda *args, **kwargs: next(inputs))

        with pytest.raises(click.ClickException):
            match_field(sample_fields, "first", confirm=True)

    def test_user_quits_with_q(self, sample_fields, monkeypatch):
        """User quitting with 'q' raises Abort."""
        import click

        inputs = iter(["q"])
        monkeypatch.setattr("click.prompt", lambda *args, **kwargs: next(inputs))

        with pytest.raises(click.Abort):
            match_field(sample_fields, "first", confirm=True)


class TestFindFieldByKey:
    """Tests for finding field by exact key."""

    @pytest.fixture
    def sample_fields(self) -> list[dict]:
        """Sample field definitions."""
        return [
            {"key": "first_name", "name": "First Name"},
            {"key": "last_name", "name": "Last Name"},
        ]

    def test_find_existing(self, sample_fields):
        """Find existing field returns the field."""
        field = find_field_by_key(sample_fields, "first_name")
        assert field is not None
        assert field["key"] == "first_name"

    def test_find_missing(self, sample_fields):
        """Find missing field returns None."""
        field = find_field_by_key(sample_fields, "xyz")
        assert field is None


class TestFindFieldMatches:
    """Tests for find_field_matches() core matching logic."""

    @pytest.fixture
    def sample_fields(self) -> list[dict]:
        """Sample field definitions including digit-starting keys."""
        return [
            {"key": "first_name", "name": "First Name"},
            {"key": "last_name", "name": "Last Name"},
            {"key": "email", "name": "Email"},
            {"key": "25da23b938af", "name": "Custom Field 1"},
            {"key": "b85f1c2d3e4f", "name": "Custom Field 2"},
        ]

    def test_exact_key_match(self, sample_fields):
        """Exact key match returns single result."""
        matches = find_field_matches(sample_fields, "first_name")
        assert len(matches) == 1
        assert matches[0]["key"] == "first_name"

    def test_key_prefix_match(self, sample_fields):
        """Key prefix match returns matching fields."""
        matches = find_field_matches(sample_fields, "first")
        assert len(matches) == 1
        assert matches[0]["key"] == "first_name"

    def test_key_prefix_multiple_matches(self, sample_fields):
        """Key prefix matching multiple fields returns all."""
        # Both first_name and last_name contain "_name" but start differently
        # Let's add fields with common prefix
        fields = sample_fields + [
            {"key": "first_contact", "name": "First Contact"}
        ]
        matches = find_field_matches(fields, "first")
        assert len(matches) == 2
        keys = [m["key"] for m in matches]
        assert "first_name" in keys
        assert "first_contact" in keys

    def test_name_exact_match(self, sample_fields):
        """Exact name match (case-insensitive) returns single result."""
        matches = find_field_matches(sample_fields, "email")
        # 'email' matches key exactly
        assert len(matches) == 1
        assert matches[0]["key"] == "email"

    def test_name_prefix_match(self, sample_fields):
        """Name prefix match with underscore normalization."""
        matches = find_field_matches(sample_fields, "First_N")
        assert len(matches) == 1
        assert matches[0]["key"] == "first_name"

    def test_no_match_returns_empty(self, sample_fields):
        """No match returns empty list."""
        matches = find_field_matches(sample_fields, "xyz")
        assert matches == []

    def test_empty_identifier_returns_empty(self, sample_fields):
        """Empty identifier returns empty list."""
        matches = find_field_matches(sample_fields, "")
        assert matches == []

    # Tests for digit-starting key support

    def test_digit_key_escaped_prefix(self, sample_fields):
        """Escaped digit-key prefix (_25) matches keys starting with 25."""
        matches = find_field_matches(sample_fields, "_25")
        assert len(matches) == 1
        assert matches[0]["key"] == "25da23b938af"

    def test_digit_key_escaped_full(self, sample_fields):
        """Escaped digit-key (_25da) matches key prefix."""
        matches = find_field_matches(sample_fields, "_25da")
        assert len(matches) == 1
        assert matches[0]["key"] == "25da23b938af"

    def test_digit_key_escaped_exact(self, sample_fields):
        """Escaped exact digit-key (_25da23b938af) matches exactly."""
        # Note: After removing underscore, it checks as key prefix
        matches = find_field_matches(sample_fields, "_25da23b938af")
        assert len(matches) == 1
        assert matches[0]["key"] == "25da23b938af"

    def test_regular_key_starting_with_letter(self, sample_fields):
        """Regular key starting with letter (b85f) matches without escape."""
        matches = find_field_matches(sample_fields, "b85f")
        assert len(matches) == 1
        assert matches[0]["key"] == "b85f1c2d3e4f"

    def test_underscore_without_digit_is_name_normalization(self, sample_fields):
        """Underscore without digit following is name normalization, not escape."""
        # _first should NOT match first_name (underscore is for name normalization)
        # Since there's no key starting with "first" literally, it tries name matching
        matches = find_field_matches(sample_fields, "_first")
        # No match because _first doesn't start with digit after underscore
        assert matches == []


class TestMatchFieldWithDigitKeys:
    """Tests for match_field() with digit-starting keys."""

    @pytest.fixture
    def fields_with_digit_keys(self) -> list[dict]:
        """Field definitions including digit-starting keys."""
        return [
            {"key": "name", "name": "Name"},
            {"key": "25da23b938af", "name": "Custom Phone"},
            {"key": "b85f1c2d3e4f", "name": "Custom Email"},
        ]

    def test_escaped_digit_key_match(self, fields_with_digit_keys):
        """match_field() supports escaped digit-key prefix."""
        field = match_field(fields_with_digit_keys, "_25da", confirm=False)
        assert field["key"] == "25da23b938af"
        assert field["name"] == "Custom Phone"

    def test_escaped_digit_key_exact(self, fields_with_digit_keys):
        """match_field() supports escaped exact digit-key."""
        field = match_field(fields_with_digit_keys, "_25da23b938af", confirm=False)
        assert field["key"] == "25da23b938af"

    def test_letter_starting_hash_key(self, fields_with_digit_keys):
        """match_field() supports hash keys starting with letter."""
        field = match_field(fields_with_digit_keys, "b85f", confirm=False)
        assert field["key"] == "b85f1c2d3e4f"
