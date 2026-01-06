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
    ) -> dict[str, Any]:
        """Make a rate-limited API request."""
        if not self._client:
            raise RuntimeError("Client not initialized. Use async context manager.")

        await self.rate_limiter.acquire()

        response = await self._client.request(method, endpoint, params=params, json=json)

        if response.status_code == 429:
            # Rate limited - wait and retry
            retry_after = float(response.headers.get("Retry-After", "2"))
            await asyncio.sleep(retry_after)
            return await self._request(endpoint, method, params, json)

        response.raise_for_status()
        return response.json()

    async def fetch_all(
        self, entity: EntityConfig, limit: int = DEFAULT_LIMIT
    ) -> AsyncIterator[dict[str, Any]]:
        """Fetch all records for an entity with pagination."""
        start = 0

        while True:
            params = {"limit": min(limit, entity.max_limit), "start": start}
            result = await self._request(entity.endpoint, params)

            if not result.get("success"):
                raise RuntimeError(f"API error for {entity.name}: {result}")

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
            raise RuntimeError(f"API error fetching fields for {entity.name}: {result}")

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
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return False
            raise

    async def create(self, entity: EntityConfig, data: dict[str, Any]) -> dict[str, Any]:
        """Create a new record via POST."""
        result = await self._request(entity.endpoint, method="POST", json=data)

        if not result.get("success"):
            raise RuntimeError(f"Failed to create {entity.name}: {result}")

        return result.get("data", {})

    async def update(
        self, entity: EntityConfig, record_id: int, data: dict[str, Any]
    ) -> dict[str, Any]:
        """Update an existing record via PUT."""
        endpoint = f"{entity.endpoint}/{record_id}"
        result = await self._request(endpoint, method="PUT", json=data)

        if not result.get("success"):
            raise RuntimeError(f"Failed to update {entity.name}/{record_id}: {result}")

        return result.get("data", {})
