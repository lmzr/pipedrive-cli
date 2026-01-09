"""Prefix matching for entities and fields.

Provides user-friendly prefix matching for CLI arguments:
- Entities: unique prefix match without confirmation, ambiguous raises error
- Fields: prefix match with confirmation before execution
"""

import click

from .config import ENTITIES, EntityConfig


class AmbiguousMatchError(Exception):
    """Raised when a prefix matches multiple values."""

    def __init__(self, prefix: str, matches: list[str], item_type: str = "item"):
        self.prefix = prefix
        self.matches = matches
        self.item_type = item_type
        matches_str = ", ".join(matches)
        super().__init__(f"Ambiguous {item_type} prefix '{prefix}' matches: {matches_str}")


class NoMatchError(Exception):
    """Raised when a prefix matches nothing."""

    def __init__(self, prefix: str, available: list[str], item_type: str = "item"):
        self.prefix = prefix
        self.available = available
        self.item_type = item_type
        available_str = ", ".join(available)
        super().__init__(f"No {item_type} matches prefix '{prefix}'. Available: {available_str}")


def find_field_matches(
    fields: list[dict],
    identifier: str,
) -> list[dict]:
    """Find fields matching an identifier.

    Core matching logic used by match_field(), resolve_field_identifier(),
    and resolve_field_prefixes(). Supports digit-starting field keys via
    underscore escape prefix.

    Resolution order:
    1. Exact key match
    2. Key prefix match (case-insensitive)
    3. Escaped digit-key prefix: _25 → matches keys starting with 25
    4. Exact name match (case-insensitive, underscore→space normalization)
    5. Name prefix match (case-insensitive, underscore→space normalization)

    Args:
        fields: List of field definitions
        identifier: The identifier to match (key prefix, name prefix, or _escaped)

    Returns:
        List of matching field dicts (empty, one, or multiple)
    """
    if not identifier:
        return []

    identifier_lower = identifier.lower()
    # Normalize underscores to spaces for name matching (tel_s → tel s)
    identifier_normalized = identifier_lower.replace("_", " ")

    # 1. Exact key match
    for f in fields:
        if f.get("key", "") == identifier:
            return [f]

    # 2. Key prefix match (case-insensitive)
    key_matches = [
        f for f in fields
        if f.get("key", "").lower().startswith(identifier_lower)
    ]
    if key_matches:
        return key_matches

    # 3. Escaped digit-key prefix: _25 → matches keys starting with 25
    if identifier.startswith("_") and len(identifier) > 1 and identifier[1].isdigit():
        unescaped = identifier[1:]
        unescaped_lower = unescaped.lower()
        digit_key_matches = [
            f for f in fields
            if f.get("key", "").lower().startswith(unescaped_lower)
        ]
        if digit_key_matches:
            return digit_key_matches

    # 4. Exact name match (case-insensitive, with normalization)
    for f in fields:
        name = f.get("name", "").lower()
        if name == identifier_lower or name == identifier_normalized:
            return [f]

    # 5. Name prefix match (case-insensitive, with normalization)
    name_matches = [
        f for f in fields
        if f.get("name", "").lower().startswith(identifier_lower)
        or f.get("name", "").lower().startswith(identifier_normalized)
    ]
    if name_matches:
        return name_matches

    # No match
    return []


def match_entity(prefix: str) -> EntityConfig:
    """Match an entity by prefix.

    Args:
        prefix: The prefix to match (e.g., "per" for "persons")

    Returns:
        The matched EntityConfig

    Raises:
        NoMatchError: If no entity matches the prefix
        AmbiguousMatchError: If multiple entities match the prefix
    """
    prefix_lower = prefix.lower()
    entity_names = list(ENTITIES.keys())

    # Exact match first
    if prefix_lower in ENTITIES:
        return ENTITIES[prefix_lower]

    # Prefix matching
    matches = [name for name in entity_names if name.startswith(prefix_lower)]

    if not matches:
        raise NoMatchError(prefix, entity_names, "entity")

    if len(matches) == 1:
        return ENTITIES[matches[0]]

    raise AmbiguousMatchError(prefix, matches, "entity")


def match_entities(prefixes: list[str]) -> list[EntityConfig]:
    """Match multiple entity prefixes.

    Args:
        prefixes: List of prefixes to match

    Returns:
        List of matched EntityConfigs (deduplicated, preserving order)

    Raises:
        NoMatchError: If any prefix matches nothing
        AmbiguousMatchError: If any prefix matches multiple entities
    """
    seen = set()
    result = []

    for prefix in prefixes:
        entity = match_entity(prefix)
        if entity.name not in seen:
            seen.add(entity.name)
            result.append(entity)

    return result


def match_field(
    fields: list[dict],
    prefix: str,
    confirm: bool = True,
) -> dict:
    """Match a field by prefix with optional confirmation.

    Uses find_field_matches() for core matching logic, including support
    for digit-starting field keys via underscore escape prefix.

    Args:
        fields: List of field definitions from Pipedrive API
        prefix: The field key prefix to match (supports _escape for digit keys)
        confirm: If True, ask for confirmation when prefix matches

    Returns:
        The matched field definition

    Raises:
        NoMatchError: If no field matches the prefix
        AmbiguousMatchError: If multiple fields match the prefix
        click.Abort: If user cancels with 'q'
    """
    matches = find_field_matches(fields, prefix)

    if not matches:
        field_keys = [f.get("key", "") for f in fields]
        raise NoMatchError(prefix, field_keys, "field")

    if len(matches) > 1:
        match_keys = [f.get("key", "") for f in matches]
        raise AmbiguousMatchError(prefix, match_keys, "field")

    # Single match - ask for confirmation if enabled
    matched_field = matches[0]
    matched_key = matched_field.get("key", "")
    matched_name = matched_field.get("name", "")

    # Skip confirmation for exact match
    if confirm and matched_key != prefix:
        display = f"{matched_key} ({matched_name})" if matched_name else matched_key
        response = click.prompt(
            f"Field '{prefix}' matches '{display}'. Continue? [Y/n/q]",
            default="y",
            show_default=False,
        ).lower().strip()

        if response == "q":
            raise click.Abort()
        if response not in ("", "y", "yes"):
            raise click.ClickException("Cancelled. Please specify exact field key.")

    return matched_field


def find_field_by_key(fields: list[dict], key: str) -> dict | None:
    """Find a field by exact key match.

    Args:
        fields: List of field definitions from Pipedrive API
        key: The exact field key to find

    Returns:
        The field definition or None if not found
    """
    for field in fields:
        if field.get("key") == key:
            return field
    return None
