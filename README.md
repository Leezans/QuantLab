# QuantLab

QuantLab is the bootstrap of a professional quantitative research and trading platform. The repository is intentionally organized around long-lived institutional workflows rather than a single strategy or one-off backtest script.

The architecture is designed to support the full systematic lifecycle:

idea -> research -> feature engineering -> alpha generation -> strategy construction -> backtesting -> portfolio construction -> risk control -> execution -> live trading

## Design Priorities

- Modular boundaries between research logic and infrastructure.
- Reproducible, experiment-driven research workflows.
- Extensible support for multiple data types and asset classes.
- Clear interfaces for backtesting, execution, and live deployment.
- Evolution toward an alpha factory where signals are generated, scored, filtered, combined, and promoted.

## Repository Layout

```text
QuantLab/
├── config/                  # Environment and platform configuration
├── docs/                    # Architecture and data-flow documentation
├── examples/                # Small runnable examples using the skeleton
├── src/quantlab/
│   ├── core/                # Shared primitives, contracts, registries, clocks
│   ├── config/              # Typed settings and config loading
│   ├── data/                # Catalog, ingestion services, stores, transforms
│   ├── research/            # Feature engineering, alpha generation, evaluation
│   ├── strategy/            # Signal-to-target logic
│   ├── portfolio/           # Capital allocation and portfolio construction
│   ├── risk/                # Risk policies and risk stacks
│   ├── backtest/            # Simulation, cost models, performance metrics
│   ├── execution/           # Order generation, routing, execution algorithms
│   ├── live/                # Runtime for event-driven live trading workflows
│   ├── monitoring/          # Runtime health and operational monitoring
│   ├── experiments/         # Experiment tracking and research metadata
│   └── orchestration/       # End-to-end workflow composition
├── tests/                   # Smoke tests for core workflows
└── notebooks/               # Exploratory notebooks kept outside core package
```

By default, local datasets are stored outside the repo under `~/Documents/database/crypto/`:

- `raw/` for immutable vendor artifacts
- `intraday_cache/` for today's realtime market-data cache before historical reconciliation
- `warehouse/` for normalized Parquet datasets
- `curated/` for downstream research-ready tables
- `catalog/` for dataset metadata
- `artifacts/` for DuckDB files and generated outputs

## Major Module Responsibilities

- `core`: Canonical domain types such as instruments, bars, signals, orders, fills, and portfolio snapshots. Also hosts reusable contracts via `Protocol`.
- `data`: Owns market data ingestion, normalized schemas for bars/trades/quotes/order-book snapshots, local Parquet dataset storage, DuckDB querying, and transformation pipelines.
- `research`: Hosts the factor lab, including curated microstructure feature construction, formulaic factor search, GP/RL-based factor mining, market regime modeling, IC/rankIC/decay evaluation, factor screening, factor orthogonalization, multi-factor combination, and factor artifact storage.
- `strategy`: Converts raw signals into tradable target positions.
- `portfolio`: Reconciles target positions into portfolio-level capital allocations.
- `risk`: Applies pre-trade risk controls such as position caps and exposure limits.
- `backtest`: Replays historical data, applies execution assumptions, and produces portfolio/performance metrics.
- `execution`: Bridges approved targets into routeable orders and provides execution algorithm hooks.
- `live`: Hosts the event-driven runtime that will eventually consume real-time data and submit live orders.
- `monitoring`: Tracks runtime health, service status, and operational events.
- `experiments`: Persists run metadata, parameters, and metrics to keep research reproducible.
- `orchestration`: Composes research, strategy, portfolio, and risk into end-to-end workflows.

## Data Flow

The default research workflow in this bootstrap is:

1. Ingest or load bars from the `data` layer.
2. Compute feature vectors in `research.features`.
3. Mine candidate factors in `research.factors`.
4. Evaluate factors through IC, rankIC, quantile spreads, turnover, decay, and backtest-style long-short returns.
5. Filter factors with `research.factor_selection`.
6. Combine selected factors with `research.factor_combination`.
7. Persist factor values, summaries, decay curves, weights, and backtest outputs with `research.factor_storage`.
8. Convert approved signals into target positions in `strategy`.
9. Scale targets at the portfolio layer in `portfolio`.
10. Enforce limits in `risk`.
11. Simulate order generation and PnL in `backtest`.
12. Persist experiment metadata in `experiments`.

The deeper historical alpha-factory workflow now also supports:

1. Load imported Binance historical `klines`, `aggTrades` or `trades`.
2. Build curated microstructure feature frames from bars, trade prints, and optional order-book snapshots.
3. Run formulaic factor search with either a genetic-programming miner or a policy-gradient RL miner on the same expression space.
4. Evaluate discovered formulas with the same IC/rankIC/decay/long-short machinery used by the rest of the factor stack.
5. Persist the curated research dataset, then run parameter sweeps and compare experiment runs by tracked metrics.
6. Auto-sync missing Binance historical `klines` and `aggTrades` dates before research workflows need them.
7. Fit Gaussian HMM regime models over curated market features, analyze transition and duration structure, and re-evaluate factors conditionally by regime.

Current search backends are intentionally integrated into the same pipeline:

- `GeneticProgrammingFactorMiner`: classic tree-based symbolic search with tournament selection, subtree crossover, subtree mutation, point mutation, elitism, and hall-of-fame de-duplication.
- `PolicyGradientFactorMiner`: lightweight RL backend that samples formula trees from a learned discrete policy and updates it with REINFORCE-style rewards.

These implementations are informed by public formulaic-alpha search projects such as [AlphaGen](https://github.com/RL-MLDM/alphagen), [Alpha$^2$](https://github.com/x35f/alpha2), and [AlphaForge](https://github.com/DulyHao/AlphaForge), plus standard GP references such as [gplearn](https://gplearn.readthedocs.io/en/stable/intro.html). They are not direct reproductions of those repos; they are QuantLab-native search engines wired into the existing storage, evaluation, and experiment layers.

The live trading path reuses the same signal, portfolio, and risk interfaces so research and production share the same conceptual pipeline.

The local data platform now supports:

- persisted dataset metadata through a JSON-backed catalog
- raw vendor artifact retention for downloaded ZIP and checksum files
- partitioned Parquet storage for normalized historical datasets
- ad hoc DuckDB queries over stored datasets for research workloads
- normalized market-event schemas for bars, trades, quotes, and order-book snapshots
- vendor-adapter skeletons for turning raw exchange payloads into platform-standard data objects
- Binance websocket normalization for `aggTrade`, `trade`, `bookTicker`, and `depth` payloads
- intraday persistence that appends normalized realtime events into the local cache tier
- Binance REST gap-fill for `aggTrade`, `bookTicker`, `depth`, and `klines` windows before handoff to realtime streams
- cache reconciliation that purges trade caches once official historical trades exist, while archiving quotes/order-book/bar cache datasets into the historical warehouse

The storage tiers are:

- `~/Documents/database/crypto/raw`: immutable vendor artifacts such as Binance Vision ZIP and `.CHECKSUM` files
- `~/Documents/database/crypto/intraday_cache`: short-lived realtime cache datasets
- `~/Documents/database/crypto/warehouse`: historical and archived normalized Parquet datasets registered in the local catalog
- `~/Documents/database/crypto/curated`: downstream research-ready or strategy-specific derived datasets

## Quick Start

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
pip install -e ".[live]"   # optional, only needed for python-binance websocket feeds
python -m quantlab architecture
python -m quantlab smoke-backtest
python -m quantlab data-demo
python -m quantlab microstructure-demo
python -m quantlab factor-research-demo
python -m quantlab curated-microstructure-demo
python -m quantlab regime-demo
python -m quantlab formulaic-gp-demo
python -m quantlab formulaic-rl-demo
python -m quantlab binance-sync-history --symbols BTCUSDT ETHUSDT --start-version 2024-01-01 --end-version 2024-01-31 --interval 1m --trade-dataset aggTrades
python -m quantlab binance-factor-research --symbols BTCUSDT ETHUSDT SOLUSDT --start-version 2024-01-01 --end-version 2024-01-31 --interval 1d
python -m quantlab binance-curate-research --symbols BTCUSDT ETHUSDT --start-version 2024-01-01 --end-version 2024-01-31 --interval 1m --trade-dataset aggTrades
python -m quantlab binance-batch-mine --symbols BTCUSDT ETHUSDT --start-version 2024-01-01 --end-version 2024-01-31 --interval 1m --trade-dataset aggTrades
python -m quantlab binance-regime-research --symbols BTCUSDT ETHUSDT --start-version 2024-01-01 --end-version 2024-01-31 --interval 1m --trade-dataset aggTrades
python -m quantlab binance-realtime-demo
python -m quantlab binance-intraday-demo
python -m quantlab binance-gap-fill --symbol BTCUSDT --start 2024-01-01T00:00:00+00:00 --end 2024-01-01T00:01:00+00:00 --interval 1m
python -m quantlab binance-stitch-demo
python -m quantlab binance-reconcile-cache --version 2024-01-01 --symbol BTCUSDT
python -m quantlab binance-import --dataset klines --symbol BTCUSDT --date 2023-01-01 --interval 1d
PYTHONPATH=src python -m unittest discover -s tests -v
```

## Next Build-Out Areas

- Add columnar dataset storage and partitioning for large historical research workloads.
- Extend the factor lab with curated crypto microstructure features from trades and order-book depth.
- Add stronger RL backends closer to AlphaGen/Alpha$^2$/AlphaForge-style program search and dynamic combination.
- Add orthogonalization, regression-based combination, and model-based factor ensembles.
- Introduce factor registries, scheduled batch mining, and experiment comparison dashboards.
- Introduce event bus abstractions for real-time data and live order state.
- Expand experiment tracking to a database-backed metadata service.
- Add realistic transaction cost, latency, and market impact models.
- Layer additional venue adapters for equities, futures, options, and crypto venues.
- Add raw-to-curated validation, lineage, and gap-fill workflows for historical plus streaming data.
- Add a continuously running market-data daemon that wires REST warm-start, websocket streams, intraday cache persistence, and reconciliation together.

See [docs/architecture.md](/Users/evens/Desktop/QuantLab/docs/architecture.md) and [docs/data-flow.md](/Users/evens/Desktop/QuantLab/docs/data-flow.md) for the detailed system blueprint.
