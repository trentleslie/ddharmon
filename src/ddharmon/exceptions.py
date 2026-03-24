"""Typed exception hierarchy for ddharmon."""

from __future__ import annotations


class BioMapperError(Exception):
    """Base exception for all ddharmon errors."""


class BioMapperAuthError(BioMapperError):
    """Raised when the API key is missing or rejected (HTTP 401/403)."""


class BioMapperRateLimitError(BioMapperError):
    """Raised when the API signals rate limiting (HTTP 429).

    Attributes:
        retry_after: Suggested wait in seconds, if provided by the server.
    """

    def __init__(self, message: str, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class BioMapperServerError(BioMapperError):
    """Raised for unrecoverable 5xx responses from the API."""

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class BioMapperTimeoutError(BioMapperError):
    """Raised when a request exceeds the configured timeout."""


class BioMapperConfigError(BioMapperError):
    """Raised for invalid client configuration (missing API key, bad URL, etc.)."""
