from __future__ import annotations


class CLabError(Exception):
    """Base error type for cLab."""


class ConfigError(CLabError):
    """Configuration is missing or invalid."""


class DataSourceError(CLabError):
    """Data source failure."""


class StoreError(CLabError):
    """Persistence layer failure."""


class ValidationError(CLabError):
    """Input or data validation failure."""
