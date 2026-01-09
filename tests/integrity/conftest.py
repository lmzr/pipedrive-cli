"""Pytest fixtures for integrity tests."""

from pathlib import Path

import pytest

from tests.fixtures.datapackage_factory import (
    create_minimal_datapackage,
    create_multi_entity_datapackage,
    create_test_datapackage,
)


@pytest.fixture
def test_datapackage(tmp_path: Path) -> Path:
    """Create a standard test datapackage with persons entity.

    Includes system fields and custom fields.
    """
    return create_test_datapackage(tmp_path / "datapackage")


@pytest.fixture
def minimal_datapackage(tmp_path: Path) -> Path:
    """Create a minimal datapackage with only system fields.

    Useful for testing error cases.
    """
    return create_minimal_datapackage(tmp_path / "minimal")


@pytest.fixture
def multi_entity_datapackage(tmp_path: Path) -> Path:
    """Create a datapackage with multiple entities.

    Includes persons, organizations, and deals.
    """
    return create_multi_entity_datapackage(tmp_path / "multi")


@pytest.fixture
def two_datapackages(tmp_path: Path) -> tuple[Path, Path]:
    """Create two datapackages for diff/merge testing.

    Returns (target_path, source_path) where:
    - TARGET has CSV with custom columns but MISSING pipedrive_fields metadata
    - SOURCE has full metadata for those fields

    This simulates the real scenario where CSV data exists but metadata was lost.
    """
    import json

    # SOURCE: complete datapackage with all fields and metadata
    source_path = tmp_path / "source"
    create_test_datapackage(
        source_path,
        entities=["persons"],
        include_custom_fields=True,
    )

    # TARGET: same CSV data but stripped pipedrive_fields metadata
    target_path = tmp_path / "target"
    create_test_datapackage(
        target_path,
        entities=["persons"],
        include_custom_fields=True,  # CSV has the columns
    )

    # Now strip custom field metadata from TARGET (simulate the bug)
    datapackage_path = target_path / "datapackage.json"
    with open(datapackage_path, encoding="utf-8") as f:
        pkg = json.load(f)

    for resource in pkg["resources"]:
        if resource["name"] == "persons":
            # Keep only system fields in pipedrive_fields
            resource["schema"]["pipedrive_fields"] = [
                f for f in resource["schema"]["pipedrive_fields"]
                if f["key"] in ("id", "name", "email", "phone")
            ]

    with open(datapackage_path, "w", encoding="utf-8") as f:
        json.dump(pkg, f, indent=2)

    return target_path, source_path
