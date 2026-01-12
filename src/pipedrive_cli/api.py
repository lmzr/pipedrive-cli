"""Async Pipedrive API client with rate limiting."""

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx

from .config import (
    API_BASE_URL,
    DEFAULT_LIMIT,
    ENTITIES,
    RATE_LIMIT_REQUESTS,
    RATE_LIMIT_WINDOW,
    EntityConfig,
)
from .exceptions import (
    AuthenticationError,
    ForbiddenError,
    NotFoundError,
    PipedriveError,
    ServerError,
    ValidationError,
)

# Retry configuration for 5xx errors
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 1.0  # seconds


class RateLimiter:
    """Token bucket rate limiter for API requests."""

    def __init__(self, requests: int = RATE_LIMIT_REQUESTS, window: float = RATE_LIMIT_WINDOW):
        self.requests = requests
        self.window = window
        self.tokens = requests
        self.last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a request token is available."""
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self.last_update
            self.tokens = min(self.requests, self.tokens + elapsed * self.requests / self.window)
            self.last_update = now

            if self.tokens < 1:
                wait_time = (1 - self.tokens) * self.window / self.requests
                await asyncio.sleep(wait_time)
                self.tokens = 1

            self.tokens -= 1


class PipedriveClient:
    """Async client for the Pipedrive API."""

    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_url = API_BASE_URL
        self.rate_limiter = RateLimiter()
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "PipedriveClient":
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            params={"api_token": self.api_token},
            timeout=30.0,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
            await self._client.aclose()

    async def _request(
        self,
        endpoint: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
        _retry_count: int = 0,
    ) -> dict[str, Any]:
        """Make a rate-limited API request with error handling and retry."""
        if not self._client:
            raise PipedriveError("Client not initialized. Use async context manager.")

        await self.rate_limiter.acquire()

        response = await self._client.request(method, endpoint, params=params, json=json)
        status = response.status_code

        # Handle rate limiting (429)
        if status == 429:
            retry_after = float(response.headers.get("Retry-After", "2"))
            await asyncio.sleep(retry_after)
            return await self._request(endpoint, method, params, json, _retry_count)

        # Handle server errors (5xx) with retry
        if 500 <= status < 600:
            if _retry_count < MAX_RETRIES:
                wait_time = RETRY_BACKOFF_BASE * (2**_retry_count)
                await asyncio.sleep(wait_time)
                return await self._request(endpoint, method, params, json, _retry_count + 1)
            raise ServerError(
                f"Server error after {MAX_RETRIES} retries: {status}",
                status_code=status,
            )

        # Handle client errors
        if status == 401:
            raise AuthenticationError("Invalid or missing API token", status_code=status)
        if status == 403:
            raise ForbiddenError("Access denied", status_code=status)
        if status == 404:
            raise NotFoundError(f"Resource not found: {endpoint}", status_code=status)
        if status == 400:
            data = response.json()
            error_msg = data.get("error", "Invalid request")
            raise ValidationError(error_msg, status_code=status, details=data)

        # Raise for other HTTP errors
        response.raise_for_status()

        # Handle empty or non-JSON responses
        if not response.content:
            return {"success": True, "data": None}

        try:
            return response.json()
        except ValueError:
            # Non-JSON response (e.g., HTML error page)
            raise PipedriveError(
                f"Invalid JSON response from {endpoint}",
                status_code=status,
                details={"content": response.text[:500]},
            )

    async def fetch_all(
        self, entity: EntityConfig, limit: int = DEFAULT_LIMIT
    ) -> AsyncIterator[dict[str, Any]]:
        """Fetch all records for an entity with pagination."""
        start = 0

        while True:
            params = {"limit": min(limit, entity.max_limit), "start": start}
            result = await self._request(entity.endpoint, params=params)

            if not result.get("success"):
                error_msg = result.get("error", "Unknown error")
                raise PipedriveError(f"API error for {entity.name}: {error_msg}", details=result)

            data = result.get("data") or []
            for record in data:
                yield record

            pagination = result.get("additional_data", {}).get("pagination", {})
            if not pagination.get("more_items_in_collection"):
                break

            start = pagination.get("next_start", start + limit)

    async def fetch_fields(self, entity: EntityConfig) -> list[dict[str, Any]]:
        """Fetch field definitions for an entity."""
        if not entity.fields_endpoint:
            return []

        result = await self._request(entity.fields_endpoint)

        if not result.get("success"):
            error_msg = result.get("error", "Unknown error")
            raise PipedriveError(
                f"API error fetching fields for {entity.name}: {error_msg}", details=result
            )

        return result.get("data") or []

    async def fetch_entity(self, entity_name: str) -> AsyncIterator[dict[str, Any]]:
        """Fetch all records for an entity by name."""
        entity = ENTITIES.get(entity_name)
        if not entity:
            raise ValueError(f"Unknown entity: {entity_name}. Valid: {list(ENTITIES.keys())}")

        async for record in self.fetch_all(entity):
            yield record

    async def exists(self, entity: EntityConfig, record_id: int) -> bool:
        """Check if a record exists."""
        try:
            endpoint = f"{entity.endpoint}/{record_id}"
            result = await self._request(endpoint)
            return result.get("success", False) and result.get("data") is not None
        except NotFoundError:
            return False

    async def get_record(
        self, entity: EntityConfig, record_id: int
    ) -> dict[str, Any] | None:
        """Fetch a single record by ID. Returns None if not found."""
        try:
            endpoint = f"{entity.endpoint}/{record_id}"
            result = await self._request(endpoint)
            if result.get("success") and result.get("data"):
                return result["data"]
            return None
        except NotFoundError:
            return None

    async def create(self, entity: EntityConfig, data: dict[str, Any]) -> dict[str, Any]:
        """Create a new record via POST."""
        result = await self._request(entity.endpoint, method="POST", json=data)

        if not result.get("success"):
            error_msg = result.get("error", "Unknown error")
            raise PipedriveError(f"Failed to create {entity.name}: {error_msg}", details=result)

        return result.get("data", {})

    async def update(
        self, entity: EntityConfig, record_id: int, data: dict[str, Any]
    ) -> dict[str, Any]:
        """Update an existing record via PUT."""
        endpoint = f"{entity.endpoint}/{record_id}"
        result = await self._request(endpoint, method="PUT", json=data)

        if not result.get("success"):
            error_msg = result.get("error", "Unknown error")
            raise PipedriveError(
                f"Failed to update {entity.name}/{record_id}: {error_msg}", details=result
            )

        return result.get("data", {})

    async def delete(self, entity: EntityConfig, record_id: int) -> bool:
        """Delete a record via DELETE."""
        endpoint = f"{entity.endpoint}/{record_id}"
        result = await self._request(endpoint, method="DELETE")

        if not result.get("success"):
            error_msg = result.get("error", "Unknown error")
            raise PipedriveError(
                f"Failed to delete {entity.name}/{record_id}: {error_msg}", details=result
            )

        return True

    async def fetch_all_ids(self, entity: EntityConfig) -> set[int]:
        """Fetch all record IDs for an entity (lightweight, for comparison)."""
        ids: set[int] = set()
        async for record in self.fetch_all(entity):
            record_id = record.get("id")
            if record_id is not None:
                ids.add(record_id)
        return ids

    # Field management methods

    async def get_field(self, entity: EntityConfig, field_id: int) -> dict[str, Any]:
        """Get a field definition by ID."""
        if not entity.fields_endpoint:
            raise PipedriveError(f"Entity {entity.name} does not support custom fields")

        endpoint = f"{entity.fields_endpoint}/{field_id}"
        result = await self._request(endpoint)

        if not result.get("success"):
            error_msg = result.get("error", "Unknown error")
            raise PipedriveError(f"Failed to get field {field_id}: {error_msg}", details=result)

        return result.get("data", {})

    async def create_field(
        self,
        entity: EntityConfig,
        name: str,
        field_type: str,
        options: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        """Create a new custom field.

        Args:
            entity: The entity to create the field for
            name: Display name of the field
            field_type: Pipedrive field type (varchar, enum, set, int, etc.)
            options: List of options for enum/set fields [{"label": "Option 1"}, ...]

        Returns:
            The created field definition
        """
        if not entity.fields_endpoint:
            raise PipedriveError(f"Entity {entity.name} does not support custom fields")

        data: dict[str, Any] = {
            "name": name,
            "field_type": field_type,
        }

        if options:
            data["options"] = options

        result = await self._request(entity.fields_endpoint, method="POST", json=data)

        if not result.get("success"):
            error_msg = result.get("error", "Unknown error")
            raise PipedriveError(f"Failed to create field: {error_msg}", details=result)

        return result.get("data", {})

    async def update_field(
        self,
        entity: EntityConfig,
        field_id: int,
        name: str | None = None,
        options: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        """Update a custom field.

        Args:
            entity: The entity the field belongs to
            field_id: ID of the field to update
            name: New display name (optional)
            options: New/updated options for enum/set fields (optional)

        Returns:
            The updated field definition
        """
        if not entity.fields_endpoint:
            raise PipedriveError(f"Entity {entity.name} does not support custom fields")

        data: dict[str, Any] = {}
        if name is not None:
            data["name"] = name
        if options is not None:
            data["options"] = options

        if not data:
            raise ValueError("At least one field to update must be provided")

        endpoint = f"{entity.fields_endpoint}/{field_id}"
        result = await self._request(endpoint, method="PUT", json=data)

        if not result.get("success"):
            error_msg = result.get("error", "Unknown error")
            raise PipedriveError(f"Failed to update field {field_id}: {error_msg}", details=result)

        return result.get("data", {})

    async def delete_field(self, entity: EntityConfig, field_id: int) -> bool:
        """Delete a custom field.

        Args:
            entity: The entity the field belongs to
            field_id: ID of the field to delete

        Returns:
            True if deletion was successful
        """
        if not entity.fields_endpoint:
            raise PipedriveError(f"Entity {entity.name} does not support custom fields")

        endpoint = f"{entity.fields_endpoint}/{field_id}"
        result = await self._request(endpoint, method="DELETE")

        if not result.get("success"):
            error_msg = result.get("error", "Unknown error")
            raise PipedriveError(f"Failed to delete field {field_id}: {error_msg}", details=result)

        return True

    async def add_field_options(
        self,
        entity: EntityConfig,
        field_id: int,
        new_options: list[str],
    ) -> dict[str, Any]:
        """Add new options to an existing enum/set field.

        Args:
            entity: The entity the field belongs to
            field_id: ID of the field
            new_options: List of new option labels to add

        Returns:
            The updated field definition
        """
        # First fetch current field to get existing options
        current_field = await self.get_field(entity, field_id)
        existing_options = current_field.get("options", [])

        # Merge existing with new options
        all_options = list(existing_options)
        for label in new_options:
            all_options.append({"label": label})

        return await self.update_field(entity, field_id, options=all_options)
