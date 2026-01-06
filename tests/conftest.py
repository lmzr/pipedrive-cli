"""Pytest fixtures for pipedrive-cli tests."""

import json
from pathlib import Path

import pytest
import respx
from httpx import Response


@pytest.fixture
def fake_api_token() -> str:
    """Fake API token for testing."""
    return "test-token-123"


@pytest.fixture
def mock_api():
    """Mock httpx client for Pipedrive API."""
    with respx.mock(base_url="https://api.pipedrive.com") as respx_mock:
        yield respx_mock


@pytest.fixture
def sample_person() -> dict:
    """Sample person data from Pipedrive API."""
    return {
        "id": 1,
        "name": "John Doe",
        "email": [{"value": "john@example.com", "primary": True}],
        "phone": [{"value": "+1234567890", "primary": True}],
        "org_id": 1,
        "add_time": "2024-01-01 10:00:00",
        "update_time": "2024-01-02 10:00:00",
    }


@pytest.fixture
def sample_deal() -> dict:
    """Sample deal data from Pipedrive API."""
    return {
        "id": 1,
        "title": "Test Deal",
        "value": 10000,
        "currency": "EUR",
        "person_id": 1,
        "org_id": 1,
        "stage_id": 1,
        "status": "open",
        "add_time": "2024-01-01 10:00:00",
        "update_time": "2024-01-02 10:00:00",
    }


@pytest.fixture
def sample_organization() -> dict:
    """Sample organization data from Pipedrive API."""
    return {
        "id": 1,
        "name": "Acme Corp",
        "address": "123 Main St",
        "add_time": "2024-01-01 10:00:00",
        "update_time": "2024-01-02 10:00:00",
    }


@pytest.fixture
def temp_backup_dir(tmp_path: Path, sample_person: dict) -> Path:
    """Create a temporary backup directory with datapackage.json."""
    backup_dir = tmp_path / "backup-test"
    backup_dir.mkdir()

    # Create persons.csv
    persons_csv = backup_dir / "persons.csv"
    persons_csv.write_text("id,name,email,phone\n1,John Doe,john@example.com,+1234567890\n")

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
                        {"name": "email", "type": "string"},
                        {"name": "phone", "type": "string"},
                    ]
                },
            }
        ],
    }
    (backup_dir / "datapackage.json").write_text(json.dumps(datapackage, indent=2))

    return backup_dir


def make_api_response(data: list | dict | None, success: bool = True) -> Response:
    """Create a mock Pipedrive API response."""
    body = {"success": success, "data": data}
    return Response(200, json=body)


def make_paginated_response(
    data: list, more_items: bool = False, next_start: int = 0
) -> Response:
    """Create a mock paginated Pipedrive API response."""
    body = {
        "success": True,
        "data": data,
        "additional_data": {
            "pagination": {
                "more_items_in_collection": more_items,
                "next_start": next_start,
            }
        },
    }
    return Response(200, json=body)
