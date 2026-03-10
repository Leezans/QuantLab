from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path

from quantlab.backtest.costs import FixedBpsTransactionCostModel
from quantlab.backtest.engine import SimpleBacktestEngine
from quantlab.config.loader import load_settings
from quantlab.core.enums import AssetClass, DataFrequency, Side
from quantlab.core.models import Bar, Instrument, OrderBookLevel, OrderBookSnapshot, PortfolioSnapshot, Quote, TargetPosition, Trade
from quantlab.data import (
    BinanceMarketDataAdapter,
    BinanceRESTMarketDataClient,
    BinanceDataset,
    BinanceFrequency,
    BinanceHistoryEnsurer,
    BinanceHistoricalImporter,
    BinanceHistoricalSpec,
    BinanceMarket,
    BinanceVisionClient,
    DataCatalog,
    DuckDBQueryService,
    IngestionRequest,
    MarketDataIngestionService,
    OrderBookSnapshotIngestionService,
    ParquetMarketDataStore,
    QuoteIngestionService,
    RawArtifactStore,
    TradeIngestionService,
)
from quantlab.execution.algorithms import ImmediateExecutionAlgorithm
from quantlab.experiments.tracker import LocalExperimentTracker
from quantlab.live import (
    BinanceGapFillService,
    BinanceRealtimeChannel,
    BinanceRealtimeNormalizer,
    BinanceStitchedMarketDataService,
    BinanceStreamSubscription,
    IntradayCacheReconciliationService,
    IntradayEventPersistenceService,
)
from quantlab.orchestration.factor_pipeline import FactorResearchWorkflow
from quantlab.orchestration.formula_pipeline import FormulaicFactorWorkflow
from quantlab.orchestration.pipeline import ResearchWorkflow
from quantlab.orchestration.regime_pipeline import RegimeResearchWorkflow
from quantlab.portfolio.construction import GrossExposureAllocator, LiquidityAwareAllocator, RegimeAwareAllocator
from quantlab.research.alpha import AlphaDefinition, AlphaFactory
from quantlab.research.batch import FormulaicMiningSweepRunner, MiningSweepEntry
from quantlab.research.curation import BinanceCuratedFeatureBuilder, CuratedFeatureDatasetService, ParquetFeatureFrameStore
from quantlab.research.factor_backtest import QuantileLongShortBacktester
from quantlab.research.factor_combination import ICWeightFactorCombiner
from quantlab.research.factor_evaluation import FactorEvaluator
from quantlab.research.factors import CandidateFactorGenerator, FactorNormalization
from quantlab.research.factor_selection import FactorSelectionPolicy, ThresholdFactorSelector
from quantlab.research.factor_storage import FactorCatalog, LocalFactorStore
from quantlab.research.features import (
    FeaturePipeline,
    make_intrabar_range_feature,
    make_rolling_volatility_feature,
    make_trailing_return_feature,
    make_volume_ratio_feature,
)
from quantlab.research.gp_mining import GeneticProgrammingConfig, GeneticProgrammingFactorMiner
from quantlab.research.loaders import HistoricalBarLoader, HistoricalOrderBookLoader, HistoricalTradeLoader
from quantlab.research.orderbook import SyntheticDepthDatasetService
from quantlab.research.regime_analysis import RegimeAnalyzer, RegimeConditionedFactorEvaluator
from quantlab.research.regime_features import CrossSectionalRegimeObservationBuilder
from quantlab.research.regime_models import GaussianHMMConfig, GaussianHMMRegimeModel
from quantlab.research.rl_mining import PolicyGradientConfig, PolicyGradientFactorMiner
from quantlab.risk.policies import (
    LiquidityParticipationPolicy,
    MaxGrossExposurePolicy,
    MaxPositionWeightPolicy,
    RegimeStateLimitPolicy,
    RiskPolicyStack,
)
from quantlab.strategy.base import SignalWeightedStrategy


ARCHITECTURE_SUMMARY = """QuantLab layers:
- core: shared market/trading domain primitives and protocol contracts
- data: ingestion, dataset catalog, storage, transforms
- research: feature engineering, alpha generation, signal evaluation
- strategy/portfolio/risk: signal translation, capital allocation, risk limits
- backtest/execution/live: simulation, order generation, runtime deployment
- experiments/monitoring/orchestration: reproducibility, observability, workflows
"""


def build_sample_bars() -> list[Bar]:
    instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO)
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    closes = [100.0, 101.0, 103.0, 102.0, 105.0, 107.0, 106.0, 109.0]
    bars: list[Bar] = []
    for offset, close in enumerate(closes):
        timestamp = start + timedelta(days=offset)
        bars.append(
            Bar(
                timestamp=timestamp,
                instrument=instrument,
                open=close - 1.0,
                high=close + 1.0,
                low=close - 2.0,
                close=close,
                volume=1_000.0 + offset * 10,
            )
        )
    return bars


def build_factor_research_bars() -> list[Bar]:
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    trajectories = {
        "BTCUSDT": [100.0, 102.0, 104.0, 106.0, 109.0, 112.0, 115.0, 118.0, 122.0, 126.0, 130.0, 135.0],
        "ETHUSDT": [80.0, 81.0, 82.0, 83.0, 84.5, 85.0, 86.0, 87.0, 88.0, 89.0, 90.0, 91.0],
        "SOLUSDT": [50.0, 49.5, 49.0, 48.0, 47.0, 46.5, 46.0, 45.0, 44.0, 43.5, 43.0, 42.0],
        "BNBUSDT": [60.0, 61.0, 61.5, 62.0, 62.5, 63.5, 64.0, 65.0, 66.0, 67.0, 68.5, 70.0],
    }
    bars: list[Bar] = []
    for symbol, closes in trajectories.items():
        instrument = Instrument(symbol=symbol, venue="BINANCE", asset_class=AssetClass.CRYPTO, quote_currency="USDT")
        for offset, close in enumerate(closes):
            timestamp = start + timedelta(days=offset)
            range_width = 1.0 + (offset % 3) * 0.2
            bars.append(
                Bar(
                    timestamp=timestamp,
                    instrument=instrument,
                    open=close - 0.4,
                    high=close + range_width,
                    low=close - range_width,
                    close=close,
                    volume=1_000.0 + (offset * 25.0) + (len(symbol) * 10.0),
                )
            )
    return bars


def build_factor_research_trades(bars: list[Bar]) -> list[Trade]:
    trades: list[Trade] = []
    for bar in bars:
        for offset, side in enumerate((Side.BUY, Side.SELL, Side.BUY, Side.BUY)):
            quantity = 0.2 + (offset * 0.05)
            trade_price = bar.open + ((bar.close - bar.open) * ((offset + 1) / 5.0))
            trades.append(
                Trade(
                    timestamp=bar.timestamp + timedelta(seconds=offset * 10),
                    instrument=bar.instrument,
                    trade_id=f"{bar.instrument.symbol}-{bar.timestamp.isoformat()}-{offset}",
                    price=trade_price,
                    quantity=quantity,
                    side=side,
                )
            )
    return trades


def build_factor_research_order_books(bars: list[Bar]) -> list[OrderBookSnapshot]:
    snapshots: list[OrderBookSnapshot] = []
    for bar in bars:
        snapshots.append(
            OrderBookSnapshot(
                timestamp=bar.timestamp + timedelta(seconds=55),
                instrument=bar.instrument,
                sequence_id=f"{bar.instrument.symbol}-{bar.timestamp.isoformat()}",
                bids=(
                    OrderBookLevel(side=Side.BUY, level=1, price=bar.close - 0.05, quantity=1.5),
                    OrderBookLevel(side=Side.BUY, level=2, price=bar.close - 0.10, quantity=2.1),
                ),
                asks=(
                    OrderBookLevel(side=Side.SELL, level=1, price=bar.close + 0.05, quantity=1.2),
                    OrderBookLevel(side=Side.SELL, level=2, price=bar.close + 0.10, quantity=1.8),
                ),
            )
        )
    return snapshots


def build_default_workflow() -> ResearchWorkflow:
    feature_pipeline = FeaturePipeline([make_trailing_return_feature(lookback=3)])
    alpha_factory = AlphaFactory(
        [AlphaDefinition(name="mom_3", score=lambda vector: vector.values["return_3"], threshold=0.0)]
    )
    return ResearchWorkflow(
        feature_pipeline=feature_pipeline,
        alpha_factory=alpha_factory,
        strategy=SignalWeightedStrategy(max_weight_per_instrument=0.25),
        allocator=GrossExposureAllocator(gross_target=1.0),
        risk_stack=RiskPolicyStack([MaxPositionWeightPolicy(0.2), MaxGrossExposurePolicy(1.0)]),
    )


def run_smoke_backtest() -> str:
    bars = build_sample_bars()
    workflow = build_default_workflow()
    research_run = workflow.run(bars, PortfolioSnapshot(timestamp=bars[0].timestamp, cash=1_000_000.0))
    engine = SimpleBacktestEngine(
        strategy=workflow.strategy,
        allocator=workflow.allocator,
        risk_policy=workflow.risk_stack,
        execution_algorithm=ImmediateExecutionAlgorithm(),
        cost_model=FixedBpsTransactionCostModel(bps=1.0),
        initial_cash=1_000_000.0,
    )
    result = engine.run(bars, research_run.signals)
    return "\n".join(
        [
            f"signals={len(research_run.signals)}",
            f"targets={len(research_run.approved_targets)}",
            f"pnl={result.report.pnl:.2f}",
            f"sharpe={result.report.sharpe:.4f}",
            f"max_drawdown={result.report.max_drawdown:.4f}",
        ]
    )


def build_factor_feature_pipeline() -> FeaturePipeline:
    return FeaturePipeline(
        [
            make_trailing_return_feature("momentum_3", lookback=3),
            make_trailing_return_feature("momentum_5", lookback=5),
            make_rolling_volatility_feature("volatility_5", lookback=5),
            make_volume_ratio_feature("volume_ratio_5", lookback=5),
            make_intrabar_range_feature("intrabar_range_5", lookback=5),
        ]
    )


def build_factor_workflow(config_path: str, artifact_name: str) -> FactorResearchWorkflow:
    settings = load_settings(config_path)
    factor_store = LocalFactorStore(FactorCatalog(settings.storage.artifact_dir / "factor_catalog.json"))
    return FactorResearchWorkflow(
        feature_pipeline=build_factor_feature_pipeline(),
        selector=ThresholdFactorSelector(
            FactorSelectionPolicy(
                min_mean_ic=0.05,
                min_hit_rate=0.55,
                min_cross_sections=3,
                min_mean_quantile_spread=0.0,
            )
        ),
        combiner=ICWeightFactorCombiner(),
        backtester=QuantileLongShortBacktester(periods_per_year=365),
        factor_store=factor_store,
        artifact_base_path=settings.storage.artifact_dir / "factor_lab",
        artifact_name=artifact_name,
        forward_horizon=1,
        decay_horizons=(1, 3, 5),
        combine_top_n=5,
    )


def build_formulaic_workflow(config_path: str, artifact_name: str, miner) -> FormulaicFactorWorkflow:
    settings = load_settings(config_path)
    factor_store = LocalFactorStore(FactorCatalog(settings.storage.artifact_dir / "factor_catalog.json"))
    tracker = LocalExperimentTracker(settings.storage.artifact_dir / "experiments")
    return FormulaicFactorWorkflow(
        miner=miner,
        selector=ThresholdFactorSelector(
            FactorSelectionPolicy(
                min_mean_ic=0.0,
                min_hit_rate=0.0,
                min_cross_sections=2,
                min_mean_quantile_spread=-1.0,
            )
        ),
        combiner=ICWeightFactorCombiner(),
        backtester=QuantileLongShortBacktester(periods_per_year=365),
        factor_store=factor_store,
        artifact_base_path=settings.storage.artifact_dir / "formulaic_lab",
        artifact_name=artifact_name,
        tracker=tracker,
        decay_horizons=(1, 3, 5),
    )


def build_regime_workflow(
    regime_feature_names: Sequence[str] | None = None,
    factor_feature_names: Sequence[str] | None = None,
) -> tuple[RegimeResearchWorkflow, tuple[str, ...]]:
    resolved_regime_feature_names = tuple(
        regime_feature_names
        or (
            "return_3",
            "realized_volatility_5",
            "signed_quantity_imbalance",
            "vpin_proxy",
            "volume_profile_tilt",
            "volume_profile_entropy",
            "volume_profile_poc_distance",
            "bar_volume_ratio_5",
            "orderbook_pressure",
            "orderbook_liquidity_score",
            "orderbook_spread_bps",
        )
    )
    resolved_factor_feature_names = tuple(
        factor_feature_names
        or (
            "return_3",
            "return_5",
            "realized_volatility_5",
            "signed_quantity_imbalance",
            "vpin_proxy",
            "volume_profile_tilt",
            "volume_profile_entropy",
            "orderflow_price_pressure",
            "orderbook_pressure",
            "orderbook_microprice_gap",
        )
    )
    workflow = RegimeResearchWorkflow(
        observation_builder=CrossSectionalRegimeObservationBuilder(
            feature_names=resolved_regime_feature_names,
            aggregations=("mean", "stdev", "breadth"),
        ),
        model=GaussianHMMRegimeModel(
            GaussianHMMConfig(n_states=3, max_iterations=16, tolerance=1e-5, random_seed=23)
        ),
        analyzer=RegimeAnalyzer(),
        conditioned_evaluator=RegimeConditionedFactorEvaluator(FactorEvaluator(min_observations=2)),
        candidate_generator=CandidateFactorGenerator(
            normalizations=(FactorNormalization.RAW, FactorNormalization.ZSCORE),
            include_inverse=True,
        ),
    )
    return workflow, resolved_factor_feature_names


def build_curated_feature_frame(
    bars: Sequence[Bar],
    trades: Sequence[Trade],
    order_books: Sequence[OrderBookSnapshot] = (),
):
    builder = BinanceCuratedFeatureBuilder(lookbacks=(3, 5, 10))
    return builder.build(bars, trades, order_books)


def persist_curated_feature_frame(
    config_path: str,
    frame,
    *,
    dataset_name: str,
    version: str,
    metadata: Mapping[str, str],
):
    settings = load_settings(config_path)
    service = CuratedFeatureDatasetService(
        store=ParquetFeatureFrameStore(),
        catalog=DataCatalog(settings.storage.catalog_path),
    )
    return service.persist(
        frame,
        dataset_name=dataset_name,
        version=version,
        storage_path=settings.storage.curated_data_dir,
        metadata=metadata,
    )


def ensure_binance_historical_range(
    config_path: str,
    *,
    symbols: Sequence[str],
    start_version: str,
    end_version: str,
    interval: str | None,
    datasets: Sequence[BinanceDataset],
) -> str:
    settings = load_settings(config_path)
    report = BinanceHistoryEnsurer(
        raw_store=RawArtifactStore(settings.storage.raw_data_dir),
        dataset_store=ParquetMarketDataStore(),
        catalog=DataCatalog(settings.storage.catalog_path),
    ).ensure_range(
        symbols=symbols,
        start_date=start_version,
        end_date=end_version,
        normalized_base_path=settings.storage.warehouse_dir,
        interval=interval,
        datasets=datasets,
        market=BinanceMarket.SPOT,
        frequency=BinanceFrequency.DAILY,
    )
    lines = [
        f"history_imported={len(report.imported)}",
        f"history_existing={len(report.existing)}",
        f"history_unavailable={len(report.unavailable)}",
    ]
    lines.extend(
        f"imported={dataset.name}:{dataset.version} rows={dataset.row_count}"
        for dataset in report.imported[:12]
    )
    lines.extend(
        f"unavailable={spec.normalized_dataset_name()}:{spec.date}"
        for spec in report.unavailable[:12]
    )
    return "\n".join(lines)


def materialize_binance_synthetic_depth(
    config_path: str,
    *,
    bars: Sequence[Bar],
    trades: Sequence[Trade],
    interval: str,
    universe: Sequence[str],
) -> tuple[list[OrderBookSnapshot], str]:
    settings = load_settings(config_path)
    result = SyntheticDepthDatasetService(
        store=ParquetMarketDataStore(),
        catalog=DataCatalog(settings.storage.catalog_path),
    ).materialize_binance_range(
        bars,
        trades,
        interval=interval,
        storage_path=settings.storage.curated_data_dir,
        market="spot",
        metadata={
            "universe": ",".join(symbol.upper() for symbol in universe),
            "interval": interval,
        },
    )
    lines = [
        f"synthetic_depth_datasets={len(result.datasets)}",
        f"synthetic_depth_snapshots={len(result.snapshots)}",
    ]
    lines.extend(
        f"synthetic_depth={dataset.name}:{dataset.version} rows={dataset.row_count}"
        for dataset in result.datasets[:12]
    )
    return list(result.snapshots), "\n".join(lines)


def format_factor_research_result(result, *, universe: str) -> str:
    top_summaries = sorted(
        result.selected_summaries,
        key=lambda item: (item.mean_ic or 0.0, item.mean_quantile_spread or 0.0),
        reverse=True,
    )
    lines = [
        f"universe={universe}",
        f"features={len(result.features)}",
        f"factor_definitions={len(result.factor_definitions)}",
        f"factor_exposures={len(result.factor_exposures)}",
        f"selected_factors={len(result.selected_summaries)}",
        f"composite_weights={[(weight.factor_name, round(weight.weight, 4)) for weight in result.combination_weights]}",
        f"artifacts={[artifact.artifact_type for artifact in result.artifacts]}",
    ]
    lines.extend(
        f"selected={summary.factor_name} mean_ic={summary.mean_ic:.4f} rank_ic={summary.mean_rank_ic:.4f} spread={summary.mean_quantile_spread:.4f}"
        for summary in top_summaries[:5]
        if summary.mean_ic is not None and summary.mean_rank_ic is not None and summary.mean_quantile_spread is not None
    )
    lines.extend(
        f"backtest={backtest.factor_name} cumulative_return={backtest.report.cumulative_return:.4f} sharpe={backtest.report.sharpe:.4f} max_drawdown={backtest.report.max_drawdown:.4f}"
        for backtest in result.backtests[:5]
        if backtest.report.sharpe is not None
    )
    return "\n".join(lines)


def format_formulaic_result(result, *, universe: str) -> str:
    lines = [
        f"universe={universe}",
        f"miner={result.factor_search.miner}",
        f"candidates={len(result.factor_search.candidates)}",
        f"selected_factors={len(result.selected_summaries)}",
        f"artifacts={[artifact.artifact_type for artifact in result.artifacts]}",
        f"experiment={result.experiment_path}" if result.experiment_path else "experiment=None",
    ]
    lines.extend(
        f"candidate={candidate.factor_name} fitness={candidate.fitness:.4f} ic={(candidate.mean_ic or 0.0):.4f} formula={candidate.formula}"
        for candidate in result.factor_search.candidates[:5]
    )
    lines.extend(
        f"backtest={backtest.factor_name} cumulative_return={backtest.report.cumulative_return:.4f} sharpe={(backtest.report.sharpe or 0.0):.4f}"
        for backtest in result.backtests[:5]
    )
    return "\n".join(lines)


def format_regime_result(result, *, universe: str) -> str:
    lines = [
        f"universe={universe}",
        f"scope={result.observation_frame.scope}",
        f"regime_observations={len(result.observation_frame.observations)}",
        f"regime_features={list(result.observation_frame.feature_names)}",
        f"state_count={result.artifact.state_count}",
        f"log_likelihood={result.inference.log_likelihood:.4f}",
    ]
    if result.current_signal:
        lines.append(
            f"current_state={result.current_signal.state_id} confidence={result.current_signal.confidence:.4f} transitions={dict(result.current_signal.transition_probabilities)}"
        )
    lines.extend(
        f"duration state={summary.state_id} episodes={summary.episodes} mean={summary.mean_duration:.2f} max={summary.max_duration:.2f}"
        for summary in result.durations
    )
    lines.extend(
        f"profile state={profile.state_id} observations={profile.observations} market_return={(profile.mean_market_return or 0.0):.4f} market_vol={(profile.mean_market_volatility or 0.0):.4f}"
        for profile in result.profiles
    )
    lines.extend(
        f"regime_factor state={summary.state_id} factor={summary.factor_name} ic={(summary.mean_ic or 0.0):.4f} rank_ic={(summary.mean_rank_ic or 0.0):.4f} turnover={(summary.mean_turnover or 0.0):.4f} capacity={(summary.mean_capacity_proxy or 0.0):.2f}"
        for summary in sorted(
            result.conditioned_summaries,
            key=lambda item: ((item.mean_ic or 0.0), (item.mean_rank_ic or 0.0)),
            reverse=True,
        )[:8]
    )
    return "\n".join(lines)


def format_regime_overlay_preview(result, *, bars: Sequence[Bar], features: Sequence) -> str:
    if result.current_signal is None:
        return "overlay_state=unavailable"
    latest_targets = _build_regime_overlay_targets(bars, features)
    if not latest_targets:
        return "overlay_targets=0"

    gross_map, position_map = _overlay_limits_from_profiles(result.profiles)
    portfolio = PortfolioSnapshot(timestamp=latest_targets[0].as_of, cash=1_000_000.0)
    allocator = RegimeAwareAllocator(
        LiquidityAwareAllocator(
            gross_target=1.0,
            max_abs_weight=max(position_map.values(), default=0.25),
            max_turnover=0.80,
            max_adv_fraction=0.08,
            min_liquidity_score=0.0,
            cost_penalty_bps=20.0,
        ),
        gross_target_by_state=gross_map,
    )
    allocated = allocator.allocate(latest_targets, portfolio, result.current_signal)
    approved = RiskPolicyStack(
        [
            LiquidityParticipationPolicy(max_adv_fraction=0.08),
            RegimeStateLimitPolicy(
                max_position_weight_by_state=position_map,
                max_gross_by_state=gross_map,
            ),
        ]
    ).apply(allocated, portfolio, result.current_signal)
    gross = sum(abs(target.target_weight) for target in approved)
    preview = [
        f"overlay_state={result.current_signal.state_id}",
        f"overlay_gross_target={gross_map.get(result.current_signal.state_id, 1.0):.4f}",
        f"overlay_approved_gross={gross:.4f}",
    ]
    preview.extend(
        f"overlay_target symbol={target.instrument.symbol} weight={target.target_weight:.4f}"
        for target in sorted(approved, key=lambda item: abs(item.target_weight), reverse=True)[:6]
    )
    return "\n".join(preview)


def _build_regime_overlay_targets(
    bars: Sequence[Bar],
    features: Sequence,
) -> tuple:
    latest_bars: dict[str, Bar] = {}
    latest_features: dict[str, object] = {}
    for bar in sorted(bars, key=lambda value: (value.instrument.symbol, value.timestamp)):
        latest_bars[bar.instrument.symbol] = bar
    for feature in sorted(features, key=lambda value: (value.instrument.symbol, value.as_of)):
        latest_features[feature.instrument.symbol] = feature
    targets = []
    for symbol, feature in sorted(latest_features.items()):
        bar = latest_bars.get(symbol)
        if bar is None:
            continue
        values = feature.values
        raw_score = (
            values.get("orderbook_pressure", 0.0)
            + values.get("orderflow_price_pressure", 0.0)
            + (0.5 * values.get("volume_profile_tilt", 0.0))
            - (0.25 * values.get("vpin_proxy", 0.0))
        )
        if abs(raw_score) <= 1e-9:
            continue
        targets.append(
            TargetPosition(
                as_of=feature.as_of,
                instrument=feature.instrument,
                target_weight=raw_score,
                reason="regime_overlay_preview",
                signal_name="microstructure_regime_preview",
                metadata={
                    "mark_price": f"{bar.close:.8f}",
                    "adv_notional": f"{bar.close * bar.volume:.8f}",
                    "liquidity_score": f"{values.get('orderbook_liquidity_score', 0.0):.8f}",
                    "expected_cost_bps": f"{max(values.get('orderbook_spread_bps', 0.0) * 0.25, 0.0):.8f}",
                    "proxy_liquidity_score": f"{values.get('orderbook_liquidity_score', 0.0):.8f}",
                },
            )
        )
    return tuple(targets)


def _overlay_limits_from_profiles(profiles) -> tuple[dict[int, float], dict[int, float]]:
    if not profiles:
        return {}, {}
    ranked = sorted(profiles, key=lambda profile: profile.mean_market_volatility or 0.0)
    gross_schedule = [1.00, 0.80, 0.60, 0.45]
    position_schedule = [0.25, 0.18, 0.12, 0.08]
    gross_map: dict[int, float] = {}
    position_map: dict[int, float] = {}
    for index, profile in enumerate(ranked):
        schedule_index = min(index, len(gross_schedule) - 1)
        gross_map[profile.state_id] = gross_schedule[schedule_index]
        position_map[profile.state_id] = position_schedule[schedule_index]
    return gross_map, position_map


class StaticMarketDataSource:
    def __init__(self, bars: list[Bar]) -> None:
        self._bars = list(bars)

    def fetch_bars(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
        frequency: DataFrequency,
    ) -> list[Bar]:
        del frequency
        return [
            bar
            for bar in self._bars
            if bar.instrument.symbol == instrument.symbol and start <= bar.timestamp <= end
        ]


def run_data_demo(config_path: str) -> str:
    settings = load_settings(config_path)
    bars = build_sample_bars()
    source = StaticMarketDataSource(bars)
    catalog = DataCatalog(settings.storage.catalog_path)
    store = ParquetMarketDataStore()
    ingestion = MarketDataIngestionService(source=source, store=store, catalog=catalog)
    dataset = ingestion.ingest(
        IngestionRequest(
            dataset_name="demo_bars",
            version="2024-01-01",
            instrument=bars[0].instrument,
            start=bars[0].timestamp,
            end=bars[-1].timestamp,
            frequency=DataFrequency.DAILY,
            storage_path=settings.storage.warehouse_dir,
        )
    )
    query_service = DuckDBQueryService(settings.storage.duckdb_path)
    query_rows = query_service.query_dataset(
        dataset,
        """
        select
            symbol,
            count(*) as rows,
            min(close) as min_close,
            max(close) as max_close,
            avg(volume) as avg_volume
        from dataset
        group by symbol
        order by symbol
        """.strip(),
    )
    return "\n".join(
        [
            f"dataset={dataset.name}:{dataset.version}",
            f"location={dataset.location}",
            f"rows={dataset.row_count}",
            f"query_rows={query_rows}",
        ]
    )


class StaticTradeDataSource:
    def __init__(self, trades: list[Trade]) -> None:
        self._trades = list(trades)

    def fetch_trades(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
    ) -> list[Trade]:
        return [
            trade
            for trade in self._trades
            if trade.instrument.symbol == instrument.symbol and start <= trade.timestamp <= end
        ]


class StaticQuoteDataSource:
    def __init__(self, quotes: list[Quote]) -> None:
        self._quotes = list(quotes)

    def fetch_quotes(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
        frequency: DataFrequency,
    ) -> list[Quote]:
        del frequency
        return [
            quote
            for quote in self._quotes
            if quote.instrument.symbol == instrument.symbol and start <= quote.timestamp <= end
        ]


class StaticOrderBookDataSource:
    def __init__(self, snapshots: list[OrderBookSnapshot]) -> None:
        self._snapshots = list(snapshots)

    def fetch_order_book_snapshots(
        self,
        instrument: Instrument,
        start: datetime,
        end: datetime,
        frequency: DataFrequency,
    ) -> list[OrderBookSnapshot]:
        del frequency
        return [
            snapshot
            for snapshot in self._snapshots
            if snapshot.instrument.symbol == instrument.symbol and start <= snapshot.timestamp <= end
        ]


def run_microstructure_demo(config_path: str) -> str:
    settings = load_settings(config_path)
    instrument = Instrument(symbol="BTCUSDT", venue="BINANCE", asset_class=AssetClass.CRYPTO)
    adapter = BinanceMarketDataAdapter()
    raw_trade_rows = [
        {"a": 1, "p": "100.10", "q": "0.50", "T": 1704067200000, "m": False},
        {"a": 2, "p": "100.20", "q": "0.25", "T": 1704067201000, "m": True},
        {"a": 3, "p": "100.30", "q": "0.75", "T": 1704067202000, "m": False},
    ]
    raw_quote_rows = [
        {"T": 1704067200000, "b": "100.00", "B": "1.20", "a": "100.15", "A": "1.00"},
        {"T": 1704067202000, "b": "100.10", "B": "1.10", "a": "100.25", "A": "0.80"},
    ]
    raw_depth_rows = [
        {"E": 1704067200000, "u": 101, "b": [["100.00", "1.50"], ["99.90", "2.00"]], "a": [["100.15", "1.20"], ["100.25", "1.80"]]},
        {"E": 1704067202000, "u": 102, "b": [["100.10", "1.10"], ["100.00", "1.70"]], "a": [["100.25", "0.90"], ["100.35", "1.60"]]},
    ]

    trades = list(adapter.normalize_agg_trades(raw_trade_rows, instrument))
    quotes = list(adapter.normalize_book_ticker(raw_quote_rows, instrument))
    snapshots = list(adapter.normalize_depth_updates(raw_depth_rows, instrument))

    store = ParquetMarketDataStore()
    catalog = DataCatalog(settings.storage.catalog_path)
    start = trades[0].timestamp
    end = trades[-1].timestamp

    trade_dataset = TradeIngestionService(StaticTradeDataSource(trades), store, catalog).ingest(
        IngestionRequest(
            dataset_name="demo_trades",
            version="2024-01-01",
            instrument=instrument,
            start=start,
            end=end,
            storage_path=settings.storage.warehouse_dir,
        )
    )
    quote_dataset = QuoteIngestionService(StaticQuoteDataSource(quotes), store, catalog).ingest(
        IngestionRequest(
            dataset_name="demo_quotes",
            version="2024-01-01",
            instrument=instrument,
            start=quotes[0].timestamp,
            end=quotes[-1].timestamp,
            frequency=DataFrequency.TICK,
            storage_path=settings.storage.warehouse_dir,
        )
    )
    book_dataset = OrderBookSnapshotIngestionService(StaticOrderBookDataSource(snapshots), store, catalog).ingest(
        IngestionRequest(
            dataset_name="demo_order_book",
            version="2024-01-01",
            instrument=instrument,
            start=snapshots[0].timestamp,
            end=snapshots[-1].timestamp,
            frequency=DataFrequency.TICK,
            storage_path=settings.storage.warehouse_dir,
        )
    )

    query_service = DuckDBQueryService(settings.storage.duckdb_path)
    trade_summary = query_service.query_dataset(
        trade_dataset,
        """
        select
            symbol,
            count(*) as trade_count,
            sum(quantity) as total_quantity,
            avg(price) as average_price
        from dataset
        group by symbol
        """.strip(),
    )
    quote_summary = query_service.query_dataset(
        quote_dataset,
        """
        select
            symbol,
            avg(spread) as average_spread,
            avg(mid_price) as average_mid
        from dataset
        group by symbol
        """.strip(),
    )
    book_summary = query_service.query_dataset(
        book_dataset,
        """
        select
            timestamp,
            max(case when side = 'buy' and level = 1 then price end) as best_bid,
            min(case when side = 'sell' and level = 1 then price end) as best_ask,
            min(case when side = 'sell' and level = 1 then price end) -
            max(case when side = 'buy' and level = 1 then price end) as spread
        from dataset
        group by timestamp
        order by timestamp
        """.strip(),
    )
    return "\n".join(
        [
            f"trade_dataset={trade_dataset.name}:{trade_dataset.version} rows={trade_dataset.row_count}",
            f"quote_dataset={quote_dataset.name}:{quote_dataset.version} rows={quote_dataset.row_count}",
            f"book_dataset={book_dataset.name}:{book_dataset.version} rows={book_dataset.row_count}",
            f"trade_summary={trade_summary}",
            f"quote_summary={quote_summary}",
            f"book_summary={book_summary}",
        ]
    )


def run_binance_realtime_demo() -> str:
    normalizer = BinanceRealtimeNormalizer()
    payloads = [
        {
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "E": 1704067200000,
                "s": "BTCUSDT",
                "a": 1001,
                "p": "42000.10",
                "q": "0.005",
                "m": False,
            },
        },
        {
            "stream": "btcusdt@bookTicker",
            "data": {
                "E": 1704067200500,
                "u": 4001,
                "s": "BTCUSDT",
                "b": "42000.00",
                "B": "1.20",
                "a": "42000.25",
                "A": "0.80",
            },
        },
        {
            "stream": "btcusdt@depth5",
            "data": {
                "e": "depthUpdate",
                "E": 1704067201000,
                "U": 501,
                "u": 505,
                "s": "BTCUSDT",
                "b": [["42000.00", "1.50"], ["41999.90", "2.00"]],
                "a": [["42000.25", "0.80"], ["42000.40", "1.20"]],
            },
        },
    ]

    lines: list[str] = []
    for payload in payloads:
        event = normalizer.normalize_payload(payload)
        if isinstance(event, Trade):
            lines.append(
                f"trade symbol={event.instrument.symbol} price={event.price:.2f} qty={event.quantity:.6f} side={event.side}"
            )
            continue
        if isinstance(event, Quote):
            lines.append(
                f"quote symbol={event.instrument.symbol} bid={event.bid_price:.2f} ask={event.ask_price:.2f} spread={event.spread:.2f}"
            )
            continue
        lines.append(
            f"depth symbol={event.instrument.symbol} best_bid={event.bids[0].price:.2f} best_ask={event.asks[0].price:.2f} sequence={event.sequence_id}"
        )
    return "\n".join(lines)


def run_binance_intraday_demo(config_path: str) -> str:
    settings = load_settings(config_path)
    normalizer = BinanceRealtimeNormalizer()
    persistence = IntradayEventPersistenceService(
        store=ParquetMarketDataStore(),
        catalog=DataCatalog(settings.storage.catalog_path),
        base_path=settings.storage.intraday_cache_dir,
    )
    payloads = [
        {
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "E": 1704067200000,
                "s": "BTCUSDT",
                "a": 1001,
                "p": "42000.10",
                "q": "0.005",
                "m": False,
            },
        },
        {
            "stream": "btcusdt@bookTicker",
            "data": {
                "u": 4001,
                "s": "BTCUSDT",
                "b": "42000.00",
                "B": "1.20",
                "a": "42000.25",
                "A": "0.80",
            },
        },
        {
            "stream": "btcusdt@depth5",
            "data": {
                "lastUpdateId": 7001,
                "s": "BTCUSDT",
                "bids": [["42000.00", "1.50"], ["41999.90", "2.00"]],
                "asks": [["42000.25", "0.80"], ["42000.40", "1.20"]],
            },
        },
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1704067201000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1704067200000,
                    "T": 1704067259999,
                    "i": "1m",
                    "o": "42000.00",
                    "c": "42010.00",
                    "h": "42020.00",
                    "l": "41995.00",
                    "v": "12.34",
                    "q": "518000.12",
                    "n": 120,
                    "V": "6.10",
                    "Q": "256000.11",
                    "x": False,
                },
            },
        },
    ]
    datasets = [
        persistence.persist_event(normalizer.normalize_payload(payload), source="binance_ws_demo")
        for payload in payloads
    ]
    return "\n".join(
        f"{dataset.name}:{dataset.version} rows={dataset.row_count} location={dataset.location}"
        for dataset in datasets
    )


def run_binance_gap_fill(
    config_path: str,
    symbol: str,
    start: str,
    end: str,
    interval: str,
    depth: int,
) -> str:
    settings = load_settings(config_path)
    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00")).astimezone(timezone.utc)
    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00")).astimezone(timezone.utc)
    persistence = IntradayEventPersistenceService(
        store=ParquetMarketDataStore(),
        catalog=DataCatalog(settings.storage.catalog_path),
        base_path=settings.storage.intraday_cache_dir,
    )
    gap_fill = BinanceGapFillService(BinanceRESTMarketDataClient(), persistence)
    batches = gap_fill.backfill_subscriptions(
        subscriptions=[
            BinanceStreamSubscription(symbol=symbol, channel=BinanceRealtimeChannel.AGG_TRADE),
            BinanceStreamSubscription(symbol=symbol, channel=BinanceRealtimeChannel.BOOK_TICKER),
            BinanceStreamSubscription(symbol=symbol, channel=BinanceRealtimeChannel.PARTIAL_DEPTH, depth=depth),
            BinanceStreamSubscription(symbol=symbol, channel=BinanceRealtimeChannel.KLINE, interval=interval),
        ],
        start=start_dt,
        end=end_dt,
    )
    lines = [f"gap_fill_window={start_dt.isoformat()} -> {end_dt.isoformat()}"]
    for batch in batches:
        lines.append(f"subscription={batch.subscription.stream_name} events={len(batch.events)}")
        lines.extend(
            f"  dataset={dataset.name}:{dataset.version} rows={dataset.row_count} location={dataset.location}"
            for dataset in batch.datasets
        )
    return "\n".join(lines)


def run_binance_stitch_demo(config_path: str) -> str:
    settings = load_settings(config_path)
    persistence = IntradayEventPersistenceService(
        store=ParquetMarketDataStore(),
        catalog=DataCatalog(settings.storage.catalog_path),
        base_path=settings.storage.intraday_cache_dir,
    )
    stitcher = BinanceStitchedMarketDataService(
        gap_fill=BinanceGapFillService(BinanceRESTMarketDataClient(), persistence),
        persistence=persistence,
    )
    start_dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(seconds=30)
    primed = stitcher.prime(
        subscriptions=[
            BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.AGG_TRADE),
            BinanceStreamSubscription(symbol="BTCUSDT", channel=BinanceRealtimeChannel.KLINE, interval="1m"),
        ],
        start=start_dt,
        end=end_dt,
    )
    accepted = stitcher.consume_payload(
        {
            "stream": "btcusdt@kline_1m",
            "data": {
                "e": "kline",
                "E": 1704067325000,
                "s": "BTCUSDT",
                "k": {
                    "t": 1704067260000,
                    "T": 1704067319999,
                    "i": "1m",
                    "o": "42005.00",
                    "c": "42025.00",
                    "h": "42030.00",
                    "l": "42000.00",
                    "v": "13.50",
                    "q": "568000.00",
                    "n": 150,
                    "V": "6.60",
                    "Q": "278000.00",
                    "x": False,
                },
            },
        }
    )
    return "\n".join(
        [
            f"primed_datasets={len(primed)}",
            *(f"dataset={dataset.name}:{dataset.version} rows={dataset.row_count}" for dataset in primed),
            f"accepted_live_update={accepted.name}:{accepted.version} rows={accepted.row_count}" if accepted else "accepted_live_update=None",
        ]
    )


def run_binance_reconcile_cache(config_path: str, version: str, symbol: str | None) -> str:
    settings = load_settings(config_path)
    catalog = DataCatalog(settings.storage.catalog_path)
    store = ParquetMarketDataStore()
    cache_persistence = IntradayEventPersistenceService(
        store=store,
        catalog=catalog,
        base_path=settings.storage.intraday_cache_dir,
    )
    reconciler = IntradayCacheReconciliationService(
        store=store,
        catalog=catalog,
        cache_persistence=cache_persistence,
        historical_base_path=settings.storage.warehouse_dir,
    )
    result = reconciler.reconcile(version=version, symbol=symbol)
    lines = [
        f"version={result.version}",
        f"purged_trade_caches={len(result.purged_trade_caches)}",
        *(f"purged={dataset.name}:{dataset.version}" for dataset in result.purged_trade_caches),
        f"archived_cache_datasets={len(result.archived_cache_datasets)}",
        *(f"archived={dataset.name}:{dataset.version} rows={dataset.row_count} location={dataset.location}" for dataset in result.archived_cache_datasets),
        f"retained_cache_datasets={len(result.retained_cache_datasets)}",
        *(f"retained={dataset.name}:{dataset.version}" for dataset in result.retained_cache_datasets),
    ]
    return "\n".join(lines)


def run_binance_history_sync(
    config_path: str,
    symbols: list[str],
    start_version: str,
    end_version: str,
    interval: str,
    trade_dataset: str,
) -> str:
    return ensure_binance_historical_range(
        config_path,
        symbols=symbols,
        start_version=start_version,
        end_version=end_version,
        interval=interval,
        datasets=(BinanceDataset.KLINES, BinanceDataset(trade_dataset)),
    )


def run_binance_import(
    config_path: str,
    dataset: str,
    symbol: str,
    date: str,
    market: str,
    frequency: str,
    interval: str | None,
    fetch_checksum: bool,
    verify: bool,
) -> str:
    settings = load_settings(config_path)
    spec = BinanceHistoricalSpec(
        market=BinanceMarket(market),
        frequency=BinanceFrequency(frequency),
        dataset=BinanceDataset(dataset),
        symbol=symbol.upper(),
        date=date,
        interval=interval,
    )
    client = BinanceVisionClient()
    raw_store = RawArtifactStore(settings.storage.raw_data_dir)
    importer = BinanceHistoricalImporter(
        raw_store=raw_store,
        dataset_store=ParquetMarketDataStore(),
        catalog=DataCatalog(settings.storage.catalog_path),
        client=client,
    )
    dataset_ref = importer.ingest(
        spec=spec,
        normalized_base_path=settings.storage.warehouse_dir,
        fetch_checksum=fetch_checksum,
        verify=verify,
    )
    manifest_relative_path = raw_store.manifest_relative_path(client.relative_path(spec))
    raw_manifest_path = Path(settings.storage.raw_data_dir) / manifest_relative_path
    return "\n".join(
        [
            f"dataset={dataset_ref.name}:{dataset_ref.version}",
            f"kind={dataset_ref.data_kind.value}",
            f"raw_manifest={raw_manifest_path}",
            f"normalized_path={dataset_ref.location}",
            f"rows={dataset_ref.row_count}",
        ]
    )


def list_catalog_datasets(config_path: str) -> str:
    settings = load_settings(config_path)
    catalog = DataCatalog(settings.storage.catalog_path)
    datasets = catalog.list()
    if not datasets:
        return "no datasets registered"
    return "\n".join(
        f"{dataset.name}:{dataset.version} kind={dataset.data_kind.value} rows={dataset.row_count} format={dataset.format} location={dataset.location}"
        for dataset in datasets
    )


def query_catalog_dataset(config_path: str, name: str, version: str, sql: str) -> str:
    settings = load_settings(config_path)
    catalog = DataCatalog(settings.storage.catalog_path)
    dataset = catalog.resolve(name, version)
    rows = DuckDBQueryService(settings.storage.duckdb_path).query_dataset(dataset, sql)
    return "\n".join(str(row) for row in rows) if rows else "query returned no rows"


def run_factor_research_demo(config_path: str) -> str:
    bars = build_factor_research_bars()
    workflow = build_factor_workflow(config_path, artifact_name="factor_demo")
    result = workflow.run(
        bars,
        version="demo-20240101",
        metadata={
            "source": "synthetic_demo",
            "universe": "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT",
        },
    )
    return format_factor_research_result(result, universe="BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT")


def run_curated_microstructure_demo(config_path: str) -> str:
    bars = build_factor_research_bars()
    trades = build_factor_research_trades(bars)
    order_books = build_factor_research_order_books(bars)
    frame = build_curated_feature_frame(bars, trades, order_books)
    persisted = persist_curated_feature_frame(
        config_path,
        frame,
        dataset_name="demo.curated.microstructure.1d",
        version="demo-20240101",
        metadata={"source": "synthetic_demo", "universe": "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT"},
    )
    dataset = persisted.dataset
    return "\n".join(
        [
            f"dataset={dataset.name}:{dataset.version}" if dataset else "dataset=None",
            f"rows={dataset.row_count}" if dataset else "rows=0",
            f"feature_count={len(persisted.features[0].values) if persisted.features else 0}",
            f"location={dataset.location}" if dataset else "location=None",
        ]
    )


def run_formulaic_gp_demo(config_path: str) -> str:
    bars = build_factor_research_bars()
    trades = build_factor_research_trades(bars)
    order_books = build_factor_research_order_books(bars)
    frame = build_curated_feature_frame(bars, trades, order_books)
    miner = GeneticProgrammingFactorMiner(
        GeneticProgrammingConfig(population_size=40, generations=8, max_depth=4, top_k=6, random_seed=11)
    )
    workflow = build_formulaic_workflow(config_path, "formulaic_gp_demo", miner)
    result = workflow.run(
        frame.features,
        bars,
        version="demo-gp-20240101",
        metadata={"source": "synthetic_demo", "universe": "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT"},
    )
    return format_formulaic_result(result, universe="BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT")


def run_formulaic_rl_demo(config_path: str) -> str:
    bars = build_factor_research_bars()
    trades = build_factor_research_trades(bars)
    order_books = build_factor_research_order_books(bars)
    frame = build_curated_feature_frame(bars, trades, order_books)
    miner = PolicyGradientFactorMiner(
        PolicyGradientConfig(episodes=8, samples_per_episode=20, max_depth=4, top_k=6, random_seed=19)
    )
    workflow = build_formulaic_workflow(config_path, "formulaic_rl_demo", miner)
    result = workflow.run(
        frame.features,
        bars,
        version="demo-rl-20240101",
        metadata={"source": "synthetic_demo", "universe": "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT"},
    )
    return format_formulaic_result(result, universe="BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT")


def run_regime_demo(config_path: str) -> str:
    del config_path
    bars = build_factor_research_bars()
    trades = build_factor_research_trades(bars)
    order_books = build_factor_research_order_books(bars)
    frame = build_curated_feature_frame(bars, trades, order_books)
    workflow, factor_feature_names = build_regime_workflow()
    result = workflow.run(
        frame.features,
        bars,
        regime_scope="crypto_demo_market",
        factor_feature_names=factor_feature_names,
    )
    return "\n".join(
        [
            format_regime_result(result, universe="BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT"),
            format_regime_overlay_preview(result, bars=bars, features=frame.features),
        ]
    )


def _load_binance_historical_market_data(
    config_path: str,
    symbols: Sequence[str],
    start_version: str,
    end_version: str,
    interval: str,
    trade_dataset: str,
    orderbook_datasets: Sequence[str] = (),
) -> tuple[list[Bar], list[Trade], list[OrderBookSnapshot]]:
    settings = load_settings(config_path)
    catalog = DataCatalog(settings.storage.catalog_path)
    store = ParquetMarketDataStore()
    normalized_symbols = [symbol.upper() for symbol in symbols]
    bars = list(
        HistoricalBarLoader(catalog, store).load_binance_klines(
            normalized_symbols,
            interval=interval,
            market="spot",
            start_version=start_version,
            end_version=end_version,
        )
    )
    trades = list(
        HistoricalTradeLoader(catalog, store).load_binance_trades(
            normalized_symbols,
            dataset=trade_dataset,
            market="spot",
            start_version=start_version,
            end_version=end_version,
        )
    )
    snapshots = list(
        HistoricalOrderBookLoader(catalog, store).load_range(
            orderbook_datasets,
            start_version=start_version,
            end_version=end_version,
        )
    ) if orderbook_datasets else []
    return bars, trades, snapshots


def run_binance_curate_research(
    config_path: str,
    symbols: list[str],
    start_version: str,
    end_version: str,
    interval: str,
    trade_dataset: str,
    orderbook_datasets: Sequence[str],
) -> str:
    history_summary = ensure_binance_historical_range(
        config_path,
        symbols=symbols,
        start_version=start_version,
        end_version=end_version,
        interval=interval,
        datasets=(BinanceDataset.KLINES, BinanceDataset(trade_dataset)),
    )
    bars, trades, snapshots = _load_binance_historical_market_data(
        config_path,
        symbols,
        start_version,
        end_version,
        interval,
        trade_dataset,
        orderbook_datasets,
    )
    if not bars:
        return "no historical kline bars found for the requested range"
    if not trades:
        return "no historical trade datasets found for the requested range"
    synthetic_summary = ""
    if not snapshots:
        snapshots, synthetic_summary = materialize_binance_synthetic_depth(
            config_path,
            bars=bars,
            trades=trades,
            interval=interval,
            universe=symbols,
        )
    universe_label = "_".join(symbol.lower() for symbol in symbols)
    frame = build_curated_feature_frame(bars, trades, snapshots)
    persisted = persist_curated_feature_frame(
        config_path,
        frame,
        dataset_name=f"binance.spot.curated.microstructure.{interval}.{universe_label}",
        version=f"{start_version}_{end_version}",
        metadata={
            "source": "binance_historical",
            "interval": interval,
            "trade_dataset": trade_dataset,
            "universe": ",".join(symbol.upper() for symbol in symbols),
        },
    )
    dataset = persisted.dataset
    return "\n".join(
        [
            history_summary,
            synthetic_summary,
            f"dataset={dataset.name}:{dataset.version}" if dataset else "dataset=None",
            f"rows={dataset.row_count}" if dataset else "rows=0",
            f"features={len(persisted.features[0].values) if persisted.features else 0}",
            f"location={dataset.location}" if dataset else "location=None",
        ]
    )


def run_binance_factor_research(
    config_path: str,
    symbols: list[str],
    start_version: str,
    end_version: str,
    interval: str,
    trade_dataset: str = "aggTrades",
    orderbook_datasets: Sequence[str] = (),
) -> str:
    del trade_dataset, orderbook_datasets
    history_summary = ensure_binance_historical_range(
        config_path,
        symbols=symbols,
        start_version=start_version,
        end_version=end_version,
        interval=interval,
        datasets=(BinanceDataset.KLINES,),
    )
    settings = load_settings(config_path)
    loader = HistoricalBarLoader(
        catalog=DataCatalog(settings.storage.catalog_path),
        store=ParquetMarketDataStore(),
    )
    normalized_symbols = [symbol.upper() for symbol in symbols]
    bars = loader.load_binance_klines(
        normalized_symbols,
        interval=interval,
        market="spot",
        start_version=start_version,
        end_version=end_version,
    )
    if not bars:
        return (
            "no historical kline bars found for the requested universe. "
            "import Binance Vision klines first with `quantlab binance-import`."
        )
    workflow = build_factor_workflow(config_path, artifact_name="binance_factor_research")
    result = workflow.run(
        bars,
        version=f"{start_version}_{end_version}_{interval}",
        metadata={
            "source": "binance_historical_klines",
            "interval": interval,
            "start_version": start_version,
            "end_version": end_version,
            "universe": ",".join(normalized_symbols),
        },
    )
    return "\n".join([history_summary, format_factor_research_result(result, universe=",".join(normalized_symbols))])


def run_binance_batch_mine(
    config_path: str,
    symbols: list[str],
    start_version: str,
    end_version: str,
    interval: str,
    trade_dataset: str,
    orderbook_datasets: Sequence[str],
) -> str:
    history_summary = ensure_binance_historical_range(
        config_path,
        symbols=symbols,
        start_version=start_version,
        end_version=end_version,
        interval=interval,
        datasets=(BinanceDataset.KLINES, BinanceDataset(trade_dataset)),
    )
    bars, trades, snapshots = _load_binance_historical_market_data(
        config_path,
        symbols,
        start_version,
        end_version,
        interval,
        trade_dataset,
        orderbook_datasets,
    )
    if not bars or not trades:
        return "historical bars/trades are required before batch mining"
    synthetic_summary = ""
    if not snapshots:
        snapshots, synthetic_summary = materialize_binance_synthetic_depth(
            config_path,
            bars=bars,
            trades=trades,
            interval=interval,
            universe=symbols,
        )
    universe_label = "_".join(symbol.lower() for symbol in symbols)
    frame = build_curated_feature_frame(bars, trades, snapshots)
    persisted = persist_curated_feature_frame(
        config_path,
        frame,
        dataset_name=f"binance.spot.curated.microstructure.{interval}.{universe_label}",
        version=f"{start_version}_{end_version}",
        metadata={
            "source": "binance_historical",
            "interval": interval,
            "trade_dataset": trade_dataset,
            "universe": ",".join(symbol.upper() for symbol in symbols),
        },
    )
    settings = load_settings(config_path)
    sweep = FormulaicMiningSweepRunner().run(
        entries=[
            MiningSweepEntry(
                label="gp-small",
                workflow_builder=lambda: build_formulaic_workflow(
                    config_path,
                    "binance_gp_small",
                    GeneticProgrammingFactorMiner(
                        GeneticProgrammingConfig(population_size=40, generations=8, max_depth=4, top_k=6, random_seed=31)
                    ),
                ),
            ),
            MiningSweepEntry(
                label="gp-large",
                workflow_builder=lambda: build_formulaic_workflow(
                    config_path,
                    "binance_gp_large",
                    GeneticProgrammingFactorMiner(
                        GeneticProgrammingConfig(population_size=72, generations=12, max_depth=5, top_k=8, random_seed=37)
                    ),
                ),
            ),
            MiningSweepEntry(
                label="rl-fast",
                workflow_builder=lambda: build_formulaic_workflow(
                    config_path,
                    "binance_rl_fast",
                    PolicyGradientFactorMiner(
                        PolicyGradientConfig(episodes=8, samples_per_episode=18, max_depth=4, top_k=6, random_seed=41)
                    ),
                ),
            ),
            MiningSweepEntry(
                label="rl-deep",
                workflow_builder=lambda: build_formulaic_workflow(
                    config_path,
                    "binance_rl_deep",
                    PolicyGradientFactorMiner(
                        PolicyGradientConfig(episodes=14, samples_per_episode=28, max_depth=5, top_k=8, random_seed=43)
                    ),
                ),
            ),
        ],
        features=persisted.features,
        bars=bars,
        version_prefix=f"{start_version}_{end_version}_{interval}",
        metadata={
            "source": "binance_historical",
            "interval": interval,
            "trade_dataset": trade_dataset,
            "universe": ",".join(symbol.upper() for symbol in symbols),
            "curated_dataset": persisted.dataset.name if persisted.dataset else "",
            "curated_version": persisted.dataset.version if persisted.dataset else "",
        },
    )
    ranked_by_fitness = sweep.rank_by_best_fitness()
    ranked_by_sharpe = sweep.rank_by_composite_sharpe()
    lines = [
        history_summary,
        synthetic_summary,
        f"curated_dataset={persisted.dataset.name}:{persisted.dataset.version}" if persisted.dataset else "curated_dataset=None",
        f"curated_rows={persisted.dataset.row_count}" if persisted.dataset else "curated_rows=0",
        f"curated_location={persisted.dataset.location}" if persisted.dataset else "curated_location=None",
        f"runs={len(ranked_by_fitness)}",
    ]
    lines.extend(
        f"run={run.label} miner={run.result.factor_search.miner} best_fitness={max((candidate.fitness for candidate in run.result.factor_search.candidates), default=0.0):.4f} experiment={run.result.experiment_path}"
        for run in ranked_by_fitness
    )
    lines.extend(
        f"sharpe_rank={index+1} run={run.label} composite_sharpe={next((backtest.report.sharpe for backtest in run.result.backtests if backtest.factor_name == 'composite.ic_weighted' and backtest.report.sharpe is not None), 0.0):.4f}"
        for index, run in enumerate(ranked_by_sharpe)
    )
    tracker = LocalExperimentTracker(settings.storage.artifact_dir / "experiments")
    current_runs = tuple(
        tracker.load_run(Path(run.result.experiment_path).stem)
        for run in sweep.runs
        if run.result.experiment_path is not None
    )
    compared_by_fitness = tuple(
        sorted(current_runs, key=lambda run: run.metrics.get("best_fitness", float("-inf")), reverse=True)
    )[:5]
    lines.extend(
        f"experiment_rank={index+1} run_id={run.run_id} best_fitness={run.metrics.get('best_fitness', 0.0):.4f} name={run.name}"
        for index, run in enumerate(compared_by_fitness)
    )
    compared_by_sharpe = tuple(
        sorted(current_runs, key=lambda run: run.metrics.get("composite_sharpe", float("-inf")), reverse=True)
    )[:5]
    lines.extend(
        f"experiment_sharpe_rank={index+1} run_id={run.run_id} composite_sharpe={run.metrics.get('composite_sharpe', 0.0):.4f} name={run.name}"
        for index, run in enumerate(compared_by_sharpe)
    )
    return "\n".join(lines)


def run_binance_regime_research(
    config_path: str,
    symbols: list[str],
    start_version: str,
    end_version: str,
    interval: str,
    trade_dataset: str,
    orderbook_datasets: Sequence[str],
) -> str:
    history_summary = ensure_binance_historical_range(
        config_path,
        symbols=symbols,
        start_version=start_version,
        end_version=end_version,
        interval=interval,
        datasets=(BinanceDataset.KLINES, BinanceDataset(trade_dataset)),
    )
    bars, trades, snapshots = _load_binance_historical_market_data(
        config_path,
        symbols,
        start_version,
        end_version,
        interval,
        trade_dataset,
        orderbook_datasets,
    )
    if not bars or not trades:
        return "historical bars/trades are required before regime research"
    synthetic_summary = ""
    if not snapshots:
        snapshots, synthetic_summary = materialize_binance_synthetic_depth(
            config_path,
            bars=bars,
            trades=trades,
            interval=interval,
            universe=symbols,
        )
    frame = build_curated_feature_frame(bars, trades, snapshots)
    workflow, factor_feature_names = build_regime_workflow()
    result = workflow.run(
        frame.features,
        bars,
        regime_scope=f"binance_{interval}_{'_'.join(symbol.lower() for symbol in symbols)}",
        factor_feature_names=factor_feature_names,
    )
    return "\n".join(
        [
            history_summary,
            synthetic_summary,
            format_regime_result(result, universe=",".join(symbol.upper() for symbol in symbols)),
            format_regime_overlay_preview(result, bars=bars, features=frame.features),
        ]
    )


def run_binance_synthesize_depth(
    config_path: str,
    symbols: list[str],
    start_version: str,
    end_version: str,
    interval: str,
    trade_dataset: str,
) -> str:
    history_summary = ensure_binance_historical_range(
        config_path,
        symbols=symbols,
        start_version=start_version,
        end_version=end_version,
        interval=interval,
        datasets=(BinanceDataset.KLINES, BinanceDataset(trade_dataset)),
    )
    bars, trades, _ = _load_binance_historical_market_data(
        config_path,
        symbols,
        start_version,
        end_version,
        interval,
        trade_dataset,
    )
    if not bars or not trades:
        return "historical bars/trades are required before synthetic depth materialization"
    snapshots, synthetic_summary = materialize_binance_synthetic_depth(
        config_path,
        bars=bars,
        trades=trades,
        interval=interval,
        universe=symbols,
    )
    return "\n".join([history_summary, synthetic_summary, f"materialized_snapshots={len(snapshots)}"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="quantlab", description="QuantLab research platform skeleton")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("architecture", help="Print the architecture summary")

    config_parser = subparsers.add_parser("show-config", help="Load and print the base configuration")
    config_parser.add_argument("--path", default="config/base.toml")

    subparsers.add_parser("smoke-backtest", help="Run the built-in research and backtest smoke example")
    data_demo_parser = subparsers.add_parser("data-demo", help="Ingest a sample dataset into Parquet and query it with DuckDB")
    data_demo_parser.add_argument("--config", default="config/base.toml")
    microstructure_demo_parser = subparsers.add_parser(
        "microstructure-demo",
        help="Normalize vendor-style trade/quote/order book payloads and ingest them into the local warehouse",
    )
    microstructure_demo_parser.add_argument("--config", default="config/base.toml")
    subparsers.add_parser(
        "binance-realtime-demo",
        help="Normalize sample Binance websocket payloads into QuantLab market-event objects",
    )
    intraday_demo_parser = subparsers.add_parser(
        "binance-intraday-demo",
        help="Normalize sample Binance websocket payloads and persist them into the intraday cache",
    )
    intraday_demo_parser.add_argument("--config", default="config/base.toml")
    gap_fill_parser = subparsers.add_parser(
        "binance-gap-fill",
        help="Backfill a Binance market-data window through the public REST API into the intraday cache",
    )
    gap_fill_parser.add_argument("--config", default="config/base.toml")
    gap_fill_parser.add_argument("--symbol", required=True)
    gap_fill_parser.add_argument("--start", required=True, help="ISO-8601 timestamp, e.g. 2024-01-01T00:00:00+00:00")
    gap_fill_parser.add_argument("--end", required=True, help="ISO-8601 timestamp, e.g. 2024-01-01T00:05:00+00:00")
    gap_fill_parser.add_argument("--interval", default="1m")
    gap_fill_parser.add_argument("--depth", type=int, default=5)
    stitch_demo_parser = subparsers.add_parser(
        "binance-stitch-demo",
        help="Prime a history window from REST and then accept a newer websocket-style kline update",
    )
    stitch_demo_parser.add_argument("--config", default="config/base.toml")
    reconcile_cache_parser = subparsers.add_parser(
        "binance-reconcile-cache",
        help="Reconcile intraday cache datasets against historical coverage and archive non-trade cache datasets",
    )
    reconcile_cache_parser.add_argument("--config", default="config/base.toml")
    reconcile_cache_parser.add_argument("--version", required=True, help="Cache date version in YYYY-MM-DD")
    reconcile_cache_parser.add_argument("--symbol")
    binance_import_parser = subparsers.add_parser(
        "binance-import",
        help="Download Binance Vision historical data into raw storage and normalize it into the local warehouse",
    )
    binance_import_parser.add_argument("--config", default="config/base.toml")
    binance_import_parser.add_argument("--dataset", choices=[item.value for item in BinanceDataset], required=True)
    binance_import_parser.add_argument("--symbol", required=True)
    binance_import_parser.add_argument("--date", required=True)
    binance_import_parser.add_argument("--market", choices=[item.value for item in BinanceMarket], default=BinanceMarket.SPOT.value)
    binance_import_parser.add_argument(
        "--frequency",
        choices=[item.value for item in BinanceFrequency],
        default=BinanceFrequency.DAILY.value,
    )
    binance_import_parser.add_argument("--interval")
    binance_import_parser.add_argument("--skip-checksum", action="store_true")
    binance_import_parser.add_argument("--no-verify", action="store_true")
    binance_sync_parser = subparsers.add_parser(
        "binance-sync-history",
        help="Ensure Binance historical klines and trades exist for a date range by downloading any missing dates",
    )
    binance_sync_parser.add_argument("--config", default="config/base.toml")
    binance_sync_parser.add_argument("--symbols", nargs="+", required=True)
    binance_sync_parser.add_argument("--start-version", required=True)
    binance_sync_parser.add_argument("--end-version", required=True)
    binance_sync_parser.add_argument("--interval", default="1m")
    binance_sync_parser.add_argument("--trade-dataset", choices=["aggTrades", "trades"], default="aggTrades")

    list_datasets_parser = subparsers.add_parser("list-datasets", help="List registered datasets from the local catalog")
    list_datasets_parser.add_argument("--config", default="config/base.toml")

    query_dataset_parser = subparsers.add_parser("query-dataset", help="Run a DuckDB query against a registered dataset")
    query_dataset_parser.add_argument("--config", default="config/base.toml")
    query_dataset_parser.add_argument("--name", required=True)
    query_dataset_parser.add_argument("--version", default="latest")
    query_dataset_parser.add_argument("--sql", required=True)

    factor_demo_parser = subparsers.add_parser(
        "factor-research-demo",
        help="Run the end-to-end factor research workflow on a synthetic multi-asset crypto universe",
    )
    factor_demo_parser.add_argument("--config", default="config/base.toml")
    curated_demo_parser = subparsers.add_parser(
        "curated-microstructure-demo",
        help="Build and persist a synthetic curated microstructure feature dataset",
    )
    curated_demo_parser.add_argument("--config", default="config/base.toml")
    regime_demo_parser = subparsers.add_parser(
        "regime-demo",
        help="Fit a Gaussian HMM regime model on synthetic curated features and run regime-conditioned factor evaluation",
    )
    regime_demo_parser.add_argument("--config", default="config/base.toml")
    formulaic_gp_demo_parser = subparsers.add_parser(
        "formulaic-gp-demo",
        help="Run the GP-based formulaic factor miner on a synthetic curated feature frame",
    )
    formulaic_gp_demo_parser.add_argument("--config", default="config/base.toml")
    formulaic_rl_demo_parser = subparsers.add_parser(
        "formulaic-rl-demo",
        help="Run the policy-gradient formulaic factor miner on a synthetic curated feature frame",
    )
    formulaic_rl_demo_parser.add_argument("--config", default="config/base.toml")

    factor_research_parser = subparsers.add_parser(
        "binance-factor-research",
        help="Run factor mining on imported Binance historical kline datasets",
    )
    factor_research_parser.add_argument("--config", default="config/base.toml")
    factor_research_parser.add_argument("--symbols", nargs="+", required=True)
    factor_research_parser.add_argument("--start-version", required=True, help="Dataset date version in YYYY-MM-DD")
    factor_research_parser.add_argument("--end-version", required=True, help="Dataset date version in YYYY-MM-DD")
    factor_research_parser.add_argument("--interval", default="1d")
    factor_research_parser.add_argument("--trade-dataset", choices=["aggTrades", "trades"], default="aggTrades")
    factor_research_parser.add_argument("--orderbook-datasets", nargs="*", default=[])

    curate_binance_parser = subparsers.add_parser(
        "binance-curate-research",
        help="Build and persist a curated microstructure feature dataset from imported Binance bars and trades",
    )
    curate_binance_parser.add_argument("--config", default="config/base.toml")
    curate_binance_parser.add_argument("--symbols", nargs="+", required=True)
    curate_binance_parser.add_argument("--start-version", required=True)
    curate_binance_parser.add_argument("--end-version", required=True)
    curate_binance_parser.add_argument("--interval", default="1m")
    curate_binance_parser.add_argument("--trade-dataset", choices=["aggTrades", "trades"], default="aggTrades")
    curate_binance_parser.add_argument("--orderbook-datasets", nargs="*", default=[])

    batch_mine_parser = subparsers.add_parser(
        "binance-batch-mine",
        help="Run GP and RL formulaic mining sweeps over curated Binance historical data and compare experiments",
    )
    batch_mine_parser.add_argument("--config", default="config/base.toml")
    batch_mine_parser.add_argument("--symbols", nargs="+", required=True)
    batch_mine_parser.add_argument("--start-version", required=True)
    batch_mine_parser.add_argument("--end-version", required=True)
    batch_mine_parser.add_argument("--interval", default="1m")
    batch_mine_parser.add_argument("--trade-dataset", choices=["aggTrades", "trades"], default="aggTrades")
    batch_mine_parser.add_argument("--orderbook-datasets", nargs="*", default=[])
    synth_depth_parser = subparsers.add_parser(
        "binance-synthesize-depth",
        help="Build synthetic historical depth snapshots from Binance bars and aggressive trades",
    )
    synth_depth_parser.add_argument("--config", default="config/base.toml")
    synth_depth_parser.add_argument("--symbols", nargs="+", required=True)
    synth_depth_parser.add_argument("--start-version", required=True)
    synth_depth_parser.add_argument("--end-version", required=True)
    synth_depth_parser.add_argument("--interval", default="1m")
    synth_depth_parser.add_argument("--trade-dataset", choices=["aggTrades", "trades"], default="aggTrades")
    regime_binance_parser = subparsers.add_parser(
        "binance-regime-research",
        help="Build curated Binance historical features, fit a Gaussian HMM regime model, and evaluate factors by regime",
    )
    regime_binance_parser.add_argument("--config", default="config/base.toml")
    regime_binance_parser.add_argument("--symbols", nargs="+", required=True)
    regime_binance_parser.add_argument("--start-version", required=True)
    regime_binance_parser.add_argument("--end-version", required=True)
    regime_binance_parser.add_argument("--interval", default="1m")
    regime_binance_parser.add_argument("--trade-dataset", choices=["aggTrades", "trades"], default="aggTrades")
    regime_binance_parser.add_argument("--orderbook-datasets", nargs="*", default=[])
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "architecture":
        print(ARCHITECTURE_SUMMARY)
        return 0
    if args.command == "show-config":
        settings = load_settings(args.path)
        print(settings)
        return 0
    if args.command == "smoke-backtest":
        print(run_smoke_backtest())
        return 0
    if args.command == "data-demo":
        print(run_data_demo(args.config))
        return 0
    if args.command == "microstructure-demo":
        print(run_microstructure_demo(args.config))
        return 0
    if args.command == "binance-realtime-demo":
        print(run_binance_realtime_demo())
        return 0
    if args.command == "binance-intraday-demo":
        print(run_binance_intraday_demo(args.config))
        return 0
    if args.command == "binance-gap-fill":
        print(
            run_binance_gap_fill(
                config_path=args.config,
                symbol=args.symbol,
                start=args.start,
                end=args.end,
                interval=args.interval,
                depth=args.depth,
            )
        )
        return 0
    if args.command == "binance-stitch-demo":
        print(run_binance_stitch_demo(args.config))
        return 0
    if args.command == "binance-reconcile-cache":
        print(run_binance_reconcile_cache(args.config, args.version, args.symbol))
        return 0
    if args.command == "binance-import":
        print(
            run_binance_import(
                config_path=args.config,
                dataset=args.dataset,
                symbol=args.symbol,
                date=args.date,
                market=args.market,
                frequency=args.frequency,
                interval=args.interval,
                fetch_checksum=not args.skip_checksum,
                verify=not args.no_verify,
            )
        )
        return 0
    if args.command == "binance-sync-history":
        print(
            run_binance_history_sync(
                config_path=args.config,
                symbols=args.symbols,
                start_version=args.start_version,
                end_version=args.end_version,
                interval=args.interval,
                trade_dataset=args.trade_dataset,
            )
        )
        return 0
    if args.command == "list-datasets":
        print(list_catalog_datasets(args.config))
        return 0
    if args.command == "query-dataset":
        print(query_catalog_dataset(args.config, args.name, args.version, args.sql))
        return 0
    if args.command == "factor-research-demo":
        print(run_factor_research_demo(args.config))
        return 0
    if args.command == "curated-microstructure-demo":
        print(run_curated_microstructure_demo(args.config))
        return 0
    if args.command == "regime-demo":
        print(run_regime_demo(args.config))
        return 0
    if args.command == "formulaic-gp-demo":
        print(run_formulaic_gp_demo(args.config))
        return 0
    if args.command == "formulaic-rl-demo":
        print(run_formulaic_rl_demo(args.config))
        return 0
    if args.command == "binance-factor-research":
        print(
            run_binance_factor_research(
                config_path=args.config,
                symbols=args.symbols,
                start_version=args.start_version,
                end_version=args.end_version,
                interval=args.interval,
                trade_dataset=args.trade_dataset,
                orderbook_datasets=args.orderbook_datasets,
            )
        )
        return 0
    if args.command == "binance-curate-research":
        print(
            run_binance_curate_research(
                config_path=args.config,
                symbols=args.symbols,
                start_version=args.start_version,
                end_version=args.end_version,
                interval=args.interval,
                trade_dataset=args.trade_dataset,
                orderbook_datasets=args.orderbook_datasets,
            )
        )
        return 0
    if args.command == "binance-batch-mine":
        print(
            run_binance_batch_mine(
                config_path=args.config,
                symbols=args.symbols,
                start_version=args.start_version,
                end_version=args.end_version,
                interval=args.interval,
                trade_dataset=args.trade_dataset,
                orderbook_datasets=args.orderbook_datasets,
            )
        )
        return 0
    if args.command == "binance-synthesize-depth":
        print(
            run_binance_synthesize_depth(
                config_path=args.config,
                symbols=args.symbols,
                start_version=args.start_version,
                end_version=args.end_version,
                interval=args.interval,
                trade_dataset=args.trade_dataset,
            )
        )
        return 0
    if args.command == "binance-regime-research":
        print(
            run_binance_regime_research(
                config_path=args.config,
                symbols=args.symbols,
                start_version=args.start_version,
                end_version=args.end_version,
                interval=args.interval,
                trade_dataset=args.trade_dataset,
                orderbook_datasets=args.orderbook_datasets,
            )
        )
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2
