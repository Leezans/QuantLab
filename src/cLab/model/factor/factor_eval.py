from __future__ import annotations

import pandas as pd


def forward_return(close: pd.Series, *, horizon: int) -> pd.Series:
    if horizon <= 0:
        raise ValueError("horizon must be > 0")
    px = close.astype("float64")
    return (px.shift(-horizon) / px) - 1.0


def corr_scalar(a: pd.Series, b: pd.Series, *, method: str) -> float:
    x = pd.DataFrame({"a": a, "b": b}).dropna()
    if len(x) < 3:
        return float("nan")
    if method == "spearman":
        x = x.rank()
        method = "pearson"
    return float(x["a"].corr(x["b"], method=method))


def quantile_table(factor: pd.Series, fwd_ret: pd.Series, *, q: int = 5) -> pd.DataFrame:
    x = pd.DataFrame({"factor": factor, "fwd_ret": fwd_ret}).dropna()
    if x.empty:
        return pd.DataFrame(columns=["quantile", "mean_fwd_ret", "count"])  # type: ignore[return-value]

    try:
        bins = pd.qcut(x["factor"], q=int(q), labels=False, duplicates="drop")
    except ValueError:
        return pd.DataFrame(columns=["quantile", "mean_fwd_ret", "count"])  # type: ignore[return-value]

    x = x.assign(quantile=bins.astype("int64"))
    out = x.groupby("quantile")["fwd_ret"].agg([("mean_fwd_ret", "mean"), ("count", "size")]).reset_index()
    return out


def eval_factor(
    *,
    df: pd.DataFrame,
    factor_col: str,
    price_col: str = "close",
    horizon: int = 60,
    n_quantiles: int = 5,
) -> dict:
    if df.empty:
        raise ValueError("df is empty")
    if factor_col not in df.columns:
        raise ValueError(f"missing factor_col={factor_col}")

    fwd = forward_return(df[price_col], horizon=horizon)
    ic = corr_scalar(df[factor_col], fwd, method="pearson")
    rankic = corr_scalar(df[factor_col], fwd, method="spearman")
    qt = quantile_table(df[factor_col], fwd, q=n_quantiles)

    return {
        "factor_col": factor_col,
        "price_col": price_col,
        "horizon": int(horizon),
        "ic": ic,
        "rankic": rankic,
        "quantiles": qt.to_dict(orient="records"),
    }
