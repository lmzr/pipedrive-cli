"""Custom exceptions for Pipedrive API errors."""


class PipedriveError(Exception):
    """Base exception for Pipedrive API errors."""

    def __init__(self, message: str, status_code: int | None = None, details: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.details = details or {}


class AuthenticationError(PipedriveError):
    """401 - Invalid or missing API token."""


class ForbiddenError(PipedriveError):
    """403 - Access denied."""


class NotFoundError(PipedriveError):
    """404 - Resource not found."""


class ValidationError(PipedriveError):
    """400 - Invalid request data."""


class RateLimitError(PipedriveError):
    """429 - Rate limit exceeded."""


class ServerError(PipedriveError):
    """5xx - Server error (retriable)."""
