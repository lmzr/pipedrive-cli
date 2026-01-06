"""Tests for the Pipedrive API client."""

import pytest
import respx
from httpx import Response

from pipedrive_cli.api import PipedriveClient, RateLimiter
from pipedrive_cli.config import ENTITIES
from pipedrive_cli.exceptions import (
    AuthenticationError,
    NotFoundError,
    ServerError,
)

from .conftest import make_api_response, make_paginated_response


class TestRateLimiter:
    """Tests for the rate limiter."""

    async def test_acquire_allows_first_request(self):
        """First request should be allowed immediately."""
        limiter = RateLimiter(requests=10, window=1.0)
        await limiter.acquire()
        assert limiter.tokens < 10

    async def test_acquire_multiple_requests(self):
        """Multiple requests should decrease tokens."""
        limiter = RateLimiter(requests=10, window=1.0)
        for _ in range(5):
            await limiter.acquire()
        assert limiter.tokens < 6


class TestPipedriveClient:
    """Tests for the Pipedrive API client."""

    async def test_request_success(self, mock_api, fake_api_token, sample_person):
        """Successful request returns data."""
        mock_api.get("/v1/persons").mock(
            return_value=make_api_response([sample_person])
        )

        async with PipedriveClient(fake_api_token) as client:
            result = await client._request("/v1/persons")

        assert result["success"] is True
        assert result["data"][0]["id"] == 1

    async def test_request_401_raises_auth_error(self, mock_api, fake_api_token):
        """401 response raises AuthenticationError."""
        mock_api.get("/v1/persons").mock(
            return_value=Response(401, json={"success": False, "error": "Unauthorized"})
        )

        async with PipedriveClient(fake_api_token) as client:
            with pytest.raises(AuthenticationError) as exc_info:
                await client._request("/v1/persons")

        assert exc_info.value.status_code == 401

    async def test_request_404_raises_not_found(self, mock_api, fake_api_token):
        """404 response raises NotFoundError."""
        mock_api.get("/v1/persons/999").mock(
            return_value=Response(404, json={"success": False, "error": "Not found"})
        )

        async with PipedriveClient(fake_api_token) as client:
            with pytest.raises(NotFoundError) as exc_info:
                await client._request("/v1/persons/999")

        assert exc_info.value.status_code == 404

    async def test_request_429_retries(self, mock_api, fake_api_token, sample_person):
        """429 response triggers retry after Retry-After delay."""
        # First call returns 429, second call succeeds
        mock_api.get("/v1/persons").mock(
            side_effect=[
                Response(429, headers={"Retry-After": "0.01"}),
                make_api_response([sample_person]),
            ]
        )

        async with PipedriveClient(fake_api_token) as client:
            result = await client._request("/v1/persons")

        assert result["success"] is True
        assert mock_api.calls.call_count == 2

    async def test_request_5xx_retries_with_backoff(
        self, mock_api, fake_api_token, sample_person
    ):
        """5xx responses trigger retry with exponential backoff."""
        # First two calls return 500, third succeeds
        mock_api.get("/v1/persons").mock(
            side_effect=[
                Response(500, json={"error": "Internal error"}),
                Response(502, json={"error": "Bad gateway"}),
                make_api_response([sample_person]),
            ]
        )

        async with PipedriveClient(fake_api_token) as client:
            result = await client._request("/v1/persons")

        assert result["success"] is True
        assert mock_api.calls.call_count == 3

    async def test_request_5xx_exhausted_raises(self, mock_api, fake_api_token):
        """5xx responses exhausting retries raises ServerError."""
        mock_api.get("/v1/persons").mock(
            return_value=Response(503, json={"error": "Service unavailable"})
        )

        async with PipedriveClient(fake_api_token) as client:
            with pytest.raises(ServerError) as exc_info:
                await client._request("/v1/persons")

        assert exc_info.value.status_code == 503
        # 1 initial + 3 retries = 4 calls
        assert mock_api.calls.call_count == 4

    async def test_fetch_all_pagination(self, mock_api, fake_api_token, sample_person):
        """fetch_all handles pagination correctly."""
        person2 = {**sample_person, "id": 2, "name": "Jane Doe"}

        mock_api.get("/v1/persons").mock(
            side_effect=[
                make_paginated_response([sample_person], more_items=True, next_start=1),
                make_paginated_response([person2], more_items=False),
            ]
        )

        async with PipedriveClient(fake_api_token) as client:
            entity = ENTITIES["persons"]
            records = [r async for r in client.fetch_all(entity)]

        assert len(records) == 2
        assert records[0]["name"] == "John Doe"
        assert records[1]["name"] == "Jane Doe"

    async def test_exists_true(self, mock_api, fake_api_token, sample_person):
        """exists returns True when record exists."""
        mock_api.get("/v1/persons/1").mock(
            return_value=make_api_response(sample_person)
        )

        async with PipedriveClient(fake_api_token) as client:
            entity = ENTITIES["persons"]
            result = await client.exists(entity, 1)

        assert result is True

    async def test_exists_false_404(self, mock_api, fake_api_token):
        """exists returns False when record not found."""
        mock_api.get("/v1/persons/999").mock(
            return_value=Response(404, json={"success": False, "error": "Not found"})
        )

        async with PipedriveClient(fake_api_token) as client:
            entity = ENTITIES["persons"]
            result = await client.exists(entity, 999)

        assert result is False
