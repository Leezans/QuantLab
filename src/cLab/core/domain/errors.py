from __future__ import annotations


class ClabError(Exception):
    """Base exception for core domain errors."""


class ValidationError(ClabError):
    """Raised when domain input validation fails."""


class DataNotFoundError(ClabError):
    """Raised when required data cannot be found."""


class BacktestError(ClabError):
    """Raised when backtest execution fails."""
