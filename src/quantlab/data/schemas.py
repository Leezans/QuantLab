from __future__ import annotations

from dataclasses import dataclass

from quantlab.core.enums import DatasetKind


@dataclass(frozen=True, slots=True)
class DatasetSchema:
    kind: DatasetKind
    columns: tuple[str, ...]
    description: str


BAR_DATASET_SCHEMA = DatasetSchema(
    kind=DatasetKind.BAR,
    columns=(
        "timestamp",
        "symbol",
        "venue",
        "asset_class",
        "quote_currency",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "metadata_json",
    ),
    description="Normalized OHLCV bars used for research and backtesting.",
)


TRADE_DATASET_SCHEMA = DatasetSchema(
    kind=DatasetKind.TRADE,
    columns=(
        "timestamp",
        "symbol",
        "venue",
        "asset_class",
        "quote_currency",
        "trade_id",
        "price",
        "quantity",
        "side",
        "notional",
        "metadata_json",
    ),
    description="Normalized trade prints with aggressor side and notional.",
)


QUOTE_DATASET_SCHEMA = DatasetSchema(
    kind=DatasetKind.QUOTE,
    columns=(
        "timestamp",
        "symbol",
        "venue",
        "asset_class",
        "quote_currency",
        "bid_price",
        "bid_size",
        "ask_price",
        "ask_size",
        "mid_price",
        "spread",
        "metadata_json",
    ),
    description="Normalized top-of-book quote snapshots.",
)


ORDER_BOOK_SNAPSHOT_DATASET_SCHEMA = DatasetSchema(
    kind=DatasetKind.ORDER_BOOK_SNAPSHOT,
    columns=(
        "timestamp",
        "symbol",
        "venue",
        "asset_class",
        "quote_currency",
        "sequence_id",
        "side",
        "level",
        "price",
        "quantity",
        "metadata_json",
    ),
    description="Flattened order book snapshot levels for microstructure research.",
)


FEATURE_FRAME_DATASET_SCHEMA = DatasetSchema(
    kind=DatasetKind.FEATURE_FRAME,
    columns=(
        "timestamp",
        "symbol",
        "venue",
        "asset_class",
        "quote_currency",
        "metadata_json",
    ),
    description="Curated research-ready feature frame with one row per timestamp and instrument.",
)


def schema_for_kind(kind: DatasetKind) -> DatasetSchema:
    if kind is DatasetKind.BAR:
        return BAR_DATASET_SCHEMA
    if kind is DatasetKind.TRADE:
        return TRADE_DATASET_SCHEMA
    if kind is DatasetKind.QUOTE:
        return QUOTE_DATASET_SCHEMA
    if kind is DatasetKind.ORDER_BOOK_SNAPSHOT:
        return ORDER_BOOK_SNAPSHOT_DATASET_SCHEMA
    if kind is DatasetKind.FEATURE_FRAME:
        return FEATURE_FRAME_DATASET_SCHEMA
    raise ValueError(f"unsupported dataset kind: {kind}")
