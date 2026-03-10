from __future__ import annotations

from datetime import date
from pathlib import Path

from cLab.config import db_cfg
from cLab.config.settings import load_settings
from cLab.infra.storage import ParquetStore
from cLab.pipelines.feature_pipeline import FeatureBuildCommand, FeaturePipeline


# 在这里直接填写要跑的参数（不通过命令行传参）
CONFIG: dict[str, object] = {
    "symbol": "BTCUSDT",
    "start": "2024-01-01",
    "end": "2024-01-10",
    "factor_set": ["rolling_mean_return", "rolling_volatility"],
    "interval": "1h",
    "market": "spot",
    "style": "mirror",
    "features_dir": "./.clab_storage/features",  # 因子结果保存目录（可改成绝对路径）
    "factor_params": {
        "rolling_mean_return": {"window": 10, "min_periods": 10, "return_periods": 1},
        "rolling_volatility": {
            "window": 10,
            "min_periods": 10,
            "return_periods": 1,
            "ddof": 0,
        },
    },
}


def main() -> int:
    symbol = str(CONFIG["symbol"]).strip().upper()
    start = date.fromisoformat(str(CONFIG["start"]))
    end = date.fromisoformat(str(CONFIG["end"]))
    factor_set = list(CONFIG["factor_set"])
    if not factor_set:
        raise ValueError("CONFIG['factor_set'] cannot be empty")
    interval = str(CONFIG["interval"])
    market = str(CONFIG["market"])
    style = str(CONFIG["style"])
    features_dir = Path(str(CONFIG["features_dir"])).resolve()
    factor_params = dict(CONFIG.get("factor_params", {}))

    settings = load_settings()
    store = ParquetStore(
        binance_dir=db_cfg.BINANCE_DIR,
        features_dir=features_dir,
        runs_dir=settings.storage.runs_dir,
    )
    pipeline = FeaturePipeline(market_data_store=store, feature_store=store)

    command = FeatureBuildCommand(
        symbol=symbol,
        start=start,
        end=end,
        factor_set=factor_set,
        interval=interval,
        market=market,
        style=style,
        factor_params=factor_params,
    )

    result = pipeline.build(command)
    print(
        "FeatureBuildResult("
        f"artifact_path={result.artifact_path}, "
        f"row_count={result.row_count}"
        ")",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
