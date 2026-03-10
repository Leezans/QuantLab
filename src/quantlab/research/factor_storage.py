from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Mapping, Sequence

from quantlab.research.factor_backtest import FactorBacktestReport, FactorReturnPoint
from quantlab.research.factor_combination import FactorWeight
from quantlab.research.factor_evaluation import FactorCrossSection, FactorDecayPoint, FactorSummary
from quantlab.research.factors import FactorExposure


@dataclass(frozen=True, slots=True)
class FactorArtifactRef:
    name: str
    version: str
    artifact_type: str
    location: Path
    row_count: int
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Mapping[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "artifact_type": self.artifact_type,
            "location": str(self.location),
            "row_count": self.row_count,
            "created_at": self.created_at.isoformat(),
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "FactorArtifactRef":
        return cls(
            name=str(payload["name"]),
            version=str(payload["version"]),
            artifact_type=str(payload["artifact_type"]),
            location=Path(str(payload["location"])),
            row_count=int(payload.get("row_count", 0)),
            created_at=datetime.fromisoformat(str(payload["created_at"])),
            metadata={str(key): str(value) for key, value in dict(payload.get("metadata", {})).items()},
        )


class FactorCatalog:
    def __init__(self, catalog_path: Path) -> None:
        self._catalog_path = catalog_path
        self._artifacts: dict[tuple[str, str, str], FactorArtifactRef] = {}
        if catalog_path.exists():
            self.load()

    def register(self, artifact: FactorArtifactRef) -> FactorArtifactRef:
        self._artifacts[(artifact.name, artifact.version, artifact.artifact_type)] = artifact
        self.save()
        return artifact

    def list(self) -> tuple[FactorArtifactRef, ...]:
        return tuple(sorted(self._artifacts.values(), key=lambda item: (item.name, item.version, item.artifact_type)))

    def save(self) -> None:
        self._catalog_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"artifacts": [artifact.to_dict() for artifact in self.list()]}
        with NamedTemporaryFile("w", dir=self._catalog_path.parent, delete=False, encoding="utf-8") as handle:
            handle.write(json.dumps(payload, indent=2))
            temp_path = Path(handle.name)
        temp_path.replace(self._catalog_path)

    def load(self) -> None:
        payload = json.loads(self._catalog_path.read_text(encoding="utf-8"))
        self._artifacts = {}
        for record in payload.get("artifacts", []):
            artifact = FactorArtifactRef.from_dict(record)
            self._artifacts[(artifact.name, artifact.version, artifact.artifact_type)] = artifact


class LocalFactorStore:
    def __init__(self, catalog: FactorCatalog) -> None:
        self._catalog = catalog

    def write_exposures(
        self,
        name: str,
        version: str,
        exposures: Sequence[FactorExposure],
        base_path: Path,
        metadata: Mapping[str, str] | None = None,
    ) -> FactorArtifactRef:
        rows = [
            {
                "factor_name": exposure.factor_name,
                "as_of": exposure.as_of,
                "symbol": exposure.instrument.symbol,
                "venue": exposure.instrument.venue,
                "value": exposure.value,
                "feature_name": exposure.feature_name,
                "normalization": exposure.normalization.value,
            }
            for exposure in exposures
        ]
        return self._write_parquet(name, version, "factor_values", rows, base_path, metadata or {})

    def write_summaries(
        self,
        name: str,
        version: str,
        summaries: Sequence[FactorSummary],
        cross_sections: Sequence[FactorCrossSection],
        base_path: Path,
        metadata: Mapping[str, str] | None = None,
    ) -> tuple[FactorArtifactRef, FactorArtifactRef]:
        summary_rows = [
            {
                "factor_name": summary.factor_name,
                "observations": summary.observations,
                "cross_sections": summary.cross_sections,
                "mean_ic": summary.mean_ic,
                "ic_ir": summary.ic_ir,
                "mean_rank_ic": summary.mean_rank_ic,
                "rank_ic_ir": summary.rank_ic_ir,
                "positive_ic_ratio": summary.positive_ic_ratio,
                "mean_quantile_spread": summary.mean_quantile_spread,
                "hit_rate": summary.hit_rate,
                "mean_top_quantile_turnover": summary.mean_top_quantile_turnover,
                "ic_autocorrelation": summary.ic_autocorrelation,
                "rank_ic_autocorrelation": summary.rank_ic_autocorrelation,
            }
            for summary in summaries
        ]
        cross_section_rows = [
            {
                "factor_name": cross_section.factor_name,
                "as_of": cross_section.as_of,
                "observations": cross_section.observations,
                "information_coefficient": cross_section.information_coefficient,
                "rank_information_coefficient": cross_section.rank_information_coefficient,
                "top_quantile_return": cross_section.top_quantile_return,
                "bottom_quantile_return": cross_section.bottom_quantile_return,
                "quantile_spread": cross_section.quantile_spread,
                "top_quantile_symbols": ",".join(cross_section.top_quantile_symbols),
            }
            for cross_section in cross_sections
        ]
        return (
            self._write_parquet(name, version, "factor_summaries", summary_rows, base_path, metadata or {}),
            self._write_parquet(name, version, "factor_cross_sections", cross_section_rows, base_path, metadata or {}),
        )

    def write_decay(
        self,
        name: str,
        version: str,
        decay_points: Sequence[FactorDecayPoint],
        base_path: Path,
        metadata: Mapping[str, str] | None = None,
    ) -> FactorArtifactRef:
        rows = [
            {
                "factor_name": point.factor_name,
                "horizon": point.horizon,
                "cross_sections": point.cross_sections,
                "mean_ic": point.mean_ic,
                "mean_rank_ic": point.mean_rank_ic,
            }
            for point in decay_points
        ]
        return self._write_parquet(name, version, "factor_decay", rows, base_path, metadata or {})

    def write_weights(
        self,
        name: str,
        version: str,
        weights: Sequence[FactorWeight],
        base_path: Path,
        metadata: Mapping[str, str] | None = None,
    ) -> FactorArtifactRef:
        rows = [
            {
                "factor_name": weight.factor_name,
                "weight": weight.weight,
                "source_metric": weight.source_metric,
            }
            for weight in weights
        ]
        return self._write_parquet(name, version, "factor_weights", rows, base_path, metadata or {})

    def write_backtests(
        self,
        name: str,
        version: str,
        reports: Sequence[FactorBacktestReport],
        series: Sequence[FactorReturnPoint],
        base_path: Path,
        metadata: Mapping[str, str] | None = None,
    ) -> tuple[FactorArtifactRef, FactorArtifactRef]:
        report_rows = [
            {
                "factor_name": report.factor_name,
                "periods": report.periods,
                "cumulative_return": report.cumulative_return,
                "annualized_return": report.annualized_return,
                "volatility": report.volatility,
                "sharpe": report.sharpe,
                "max_drawdown": report.max_drawdown,
                "hit_rate": report.hit_rate,
            }
            for report in reports
        ]
        series_rows = [
            {
                "factor_name": point.factor_name,
                "as_of": point.as_of,
                "long_return": point.long_return,
                "short_return": point.short_return,
                "long_short_return": point.long_short_return,
            }
            for point in series
        ]
        return (
            self._write_parquet(name, version, "factor_backtest_reports", report_rows, base_path, metadata or {}),
            self._write_parquet(name, version, "factor_backtest_series", series_rows, base_path, metadata or {}),
        )

    def _write_parquet(
        self,
        name: str,
        version: str,
        artifact_type: str,
        rows: Sequence[dict[str, Any]],
        base_path: Path,
        metadata: Mapping[str, str],
    ) -> FactorArtifactRef:
        location = base_path / name / version / artifact_type
        location.mkdir(parents=True, exist_ok=True)
        pa, pq = _require_pyarrow()
        table = pa.Table.from_pylist(list(rows))
        pq.write_table(table, location / "part-0.parquet")
        return self._catalog.register(
            FactorArtifactRef(
                name=name,
                version=version,
                artifact_type=artifact_type,
                location=location,
                row_count=len(rows),
                metadata=metadata,
            )
        )


def _require_pyarrow() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise RuntimeError("pyarrow is required for LocalFactorStore. Install project dependencies first.") from exc
    return pa, pq
