# QuantLab Architecture

## Context

QuantLab is structured as an internal quant platform, not a single strategy repository. The architecture separates research concerns from production infrastructure so the same abstractions can support idea generation, simulation, portfolio construction, and live deployment.

## Architectural Layers

### 1. Core Domain Layer

Location: `src/quantlab/core`

Responsibilities:

- Defines system-wide domain primitives such as `Instrument`, `Bar`, `Signal`, `TargetPosition`, `Order`, and `PortfolioSnapshot`.
- Defines abstract contracts for data sources, feature pipelines, alpha models, strategies, portfolio constructors, risk policies, and execution algorithms.
- Provides a lightweight registry so components can be discovered and composed without tight coupling.

This is the stable center of the platform. Most extension work should depend on this layer rather than reaching directly across the codebase.

### 2. Data Platform Layer

Location: `src/quantlab/data`

Responsibilities:

- Ingestion services that pull data from vendor- or venue-specific adapters.
- Raw artifact retention for downloaded vendor files so the system can re-parse or audit inputs later.
- Dataset registration via a catalog to track dataset names, versions, schemas, and locations.
- Storage abstractions for local and future distributed stores.
- Local Parquet-backed research datasets with DuckDB query access.
- Normalized schemas for bars, trades, quotes, and order book snapshots so raw vendor formats stay isolated at the adapter edge.
- Transformation pipelines to normalize, filter, resample, and enrich raw market data.

Long term, this layer should grow into a proper research data platform with partitioned columnar storage, metadata services, data quality checks, and lineage.

The current storage model is intentionally tiered:

- `raw`: immutable vendor payloads such as Binance Vision ZIP and checksum artifacts.
- `cache`: short-lived intraday datasets written from realtime feeds before historical reconciliation.
- `normalized`: platform-standard Parquet datasets registered in the local catalog.
- `curated`: downstream research datasets or feature-ready aggregates built from normalized data.

### 3. Research Layer

Location: `src/quantlab/research`

Responsibilities:

- Feature engineering pipelines.
- Candidate factor mining from reusable feature sets.
- Curated microstructure feature-frame construction from bars, trades, and optional order-book data.
- Regime-engine workflows that convert feature sequences into latent market-state estimates, transition matrices, duration summaries, and regime-conditioned factor diagnostics.
- Formulaic factor mining backends, including tree-based genetic programming and policy-gradient RL search over a shared expression space.
- Cross-sectional and time-series factor evaluation, including IC, rankIC, decay, turnover, and long-short spread analysis.
- Factor screening, orthogonalization, multi-factor combination, and factor-artifact persistence.
- Alpha generation and signal evaluation for downstream strategy workflows.

Researchers should be able to add new features or alpha definitions without changing downstream portfolio, risk, or execution infrastructure.

The regime engine belongs here rather than in `portfolio` or `execution` because it is a research-time statistical model of hidden market structure. It should expose compact regime signals to downstream layers, but the training logic, interpretation, and factor-conditioned evaluation all remain research concerns.

### 4. Strategy and Portfolio Layer

Locations: `src/quantlab/strategy`, `src/quantlab/portfolio`, `src/quantlab/risk`

Responsibilities:

- Convert alpha signals into target positions.
- Apply portfolio construction logic such as risk budgets, capital scaling, or volatility targets.
- Apply pre-trade and portfolio-level risk constraints.

This separation matters because a strong alpha model still needs portfolio sizing and risk policy logic before it becomes tradable.

### 5. Simulation and Execution Layer

Locations: `src/quantlab/backtest`, `src/quantlab/execution`, `src/quantlab/live`

Responsibilities:

- Historical simulation and PnL accounting.
- Transaction cost and slippage assumptions.
- Target-to-order translation.
- Routing abstractions and execution algorithm hooks.
- Event-driven runtime for eventual live deployment.
- Optional websocket feed adapters that normalize venue payloads into platform-standard market events before they enter live logic.
- Warm-start and gap-fill services that use public REST data to bridge historical datasets and realtime streaming sessions.

The design goal is that strategies and portfolio logic do not have to be rewritten when moving from research to production. They should target common contracts.

### 6. Control Plane and Operations Layer

Locations: `src/quantlab/experiments`, `src/quantlab/monitoring`, `src/quantlab/orchestration`

Responsibilities:

- Track experiments, parameters, and evaluation artifacts.
- Monitor runtime services and execution health.
- Compose workflows across data, research, portfolio, and execution subsystems.

This is where QuantLab evolves from a library into a usable internal platform.

## Extension Model

The platform is intentionally protocol-driven. A new venue, a new factor model, or a new allocator should be introduced by implementing a contract instead of modifying the existing workflow engine.

Examples:

- New asset class: extend `Instrument` metadata and implement a new market data adapter plus execution venue.
- New alpha family: add a new feature definition or alpha scorer and register it.
- New portfolio policy: implement a new allocator or risk policy and compose it into the workflow.
- New live runtime: reuse strategy, portfolio, and risk contracts with a real-time event source.

## Suggested Long-Term Evolution

- Replace in-memory dataset storage with Parquet or object-storage-backed partitioned datasets.
- Add a metadata database for datasets, experiments, and live trading state.
- Add a scheduler and distributed workers for large-scale research sweeps.
- Introduce a message bus for real-time market data, orders, fills, and risk events.
- Add richer observability, alerting, and audit logs for live deployment.
