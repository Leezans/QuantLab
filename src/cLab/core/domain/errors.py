from __future__ import annotations


class CLabError(Exception):
    """Base exception for core domain errors."""


class ValidationError(CLabError):
    """Raised when domain input validation fails."""


class DataNotFoundError(CLabError):
    """Raised when required data cannot be found."""


class BacktestError(CLabError):
    """Raised when backtest execution fails."""
