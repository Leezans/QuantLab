# QuantLab Data Flow

## Research Flow

```text
Raw vendor/venue data
  -> raw artifact store
  -> normalization / parsing
  -> dataset catalog + normalized store
  -> transformation pipeline
  -> curated microstructure feature frame
  -> curated feature dataset persistence
  -> regime modeling / latent-state inference
  -> feature pipeline
  -> factor mining
  -> GP / RL formula search
  -> factor evaluation (IC / rankIC / decay / spreads)
  -> regime-conditioned factor evaluation
  -> factor selection
  -> factor combination
  -> factor artifact store
  -> alpha factory / strategy translation
  -> signal evaluation
  -> strategy logic
  -> portfolio constructor
  -> risk stack
  -> backtest engine
  -> metrics + experiment tracking
```

## Live Trading Flow

```text
Real-time market data
  -> REST gap-fill / warm-start
  -> watermark state
  -> python-binance websocket feed
  -> venue payload normalizer
  -> intraday cache persistence
  -> historical reconciliation / archival
  -> event-driven runtime
  -> feature updates / model inference
  -> signal generation
  -> strategy logic
  -> portfolio constructor
  -> risk checks
  -> execution algorithm
  -> order router / broker
  -> fills and position state
  -> monitoring and alerts
```

## Key State Transitions

- `Bar` and other market events enter through the `data` layer.
- Streaming `Trade`, `Quote`, and `OrderBookSnapshot` events should be normalized before they reach live strategy logic.
- REST gap-fill should establish the latest persisted watermark before realtime messages are accepted.
- Trade caches may be purged once official historical trades are present; quote/order-book/bar caches should be archived into the historical warehouse instead of being dropped.
- `FeatureVector` objects are produced by the research layer from normalized historical or streaming data.
- `FactorExposure` objects capture candidate cross-sectional or time-series factor values.
- `FactorSummary` and decay artifacts capture evaluation outputs used for screening and combination.
- `Signal` objects capture the output of alpha logic.
- `TargetPosition` objects represent portfolio intent before execution.
- `Order` objects represent routeable trading instructions.
- `Fill` objects and marked prices update `PortfolioSnapshot`.
- `PerformanceMetrics` and experiment artifacts are produced by simulation and evaluation.

## Boundary Rules

- Data adapters should not contain strategy or portfolio logic.
- Research modules should emit signals, not broker-specific orders.
- Portfolio and risk modules should stay broker-agnostic.
- Execution modules should only translate approved targets into routeable orders.
- Live runtime should orchestrate components, not own research logic itself.
