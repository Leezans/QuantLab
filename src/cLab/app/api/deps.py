from __future__ import annotations

from functools import lru_cache

from cLab.config import db_cfg
from cLab.config.settings import Settings, load_settings
from cLab.core.data.protocols import ExperimentStore, FeatureStore, MarketDataStore
from cLab.infra.storage import ParquetStore
from cLab.pipelines import BacktestPipeline, DataPipeline, FeaturePipeline, ResearchPipeline


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return load_settings()


@lru_cache(maxsize=1)
def get_parquet_store() -> ParquetStore:
    settings = get_settings()
    return ParquetStore(
        binance_dir=db_cfg.BINANCE_DIR,
        features_dir=settings.storage.features_dir,
        runs_dir=settings.storage.runs_dir,
    )


def get_market_data_store() -> MarketDataStore:
    return get_parquet_store()


def get_feature_store() -> FeatureStore:
    return get_parquet_store()


def get_experiment_store() -> ExperimentStore:
    return get_parquet_store()


def get_data_pipeline() -> DataPipeline:
    return DataPipeline(market_data_store=get_market_data_store())


def get_feature_pipeline() -> FeaturePipeline:
    return FeaturePipeline(
        market_data_store=get_market_data_store(),
        feature_store=get_feature_store(),
    )


def get_backtest_pipeline() -> BacktestPipeline:
    return BacktestPipeline(
        market_data_store=get_market_data_store(),
        experiment_store=get_experiment_store(),
    )


def get_research_pipeline() -> ResearchPipeline:
    return ResearchPipeline(experiment_store=get_experiment_store())

