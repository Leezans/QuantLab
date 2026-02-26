from __future__ import annotations

from dataclasses import dataclass
from math import erf, exp, sqrt
from typing import Iterable, List, Optional, Sequence, Tuple


_EPS: float = 1e-12


def _is_nan(x: float) -> bool:
    return x != x


def _validate_window(window: int) -> None:
    if window <= 0:
        raise ValueError("window must be > 0")


def _to_float_list(x: Sequence[float]) -> List[float]:
    return list(map(float, x))


def _nanmean(values: Sequence[float]) -> float:
    s = 0.0
    n = 0
    for v in values:
        if not _is_nan(v):
            s += v
            n += 1
    return s / n if n > 0 else float("nan")


def _nanvar(values: Sequence[float], ddof: int = 0) -> float:
    m = _nanmean(values)
    if _is_nan(m):
        return float("nan")
    s = 0.0
    n = 0
    for v in values:
        if not _is_nan(v):
            d = v - m
            s += d * d
            n += 1
    denom = n - ddof
    return s / denom if denom > 0 else float("nan")


def _nanstd(values: Sequence[float], ddof: int = 0) -> float:
    v = _nanvar(values, ddof=ddof)
    return sqrt(v) if not _is_nan(v) else float("nan")


def shift(x: Sequence[float], periods: int, fill: float = float("nan")) -> List[float]:
    if periods < 0:
        raise ValueError("periods must be >= 0")
    if periods == 0:
        return _to_float_list(x)
    out = [float(fill)] * periods
    out.extend(float(v) for v in x[:-periods])
    return out


def diff(x: Sequence[float], periods: int = 1) -> List[float]:
    if periods <= 0:
        raise ValueError("periods must be > 0")
    n = len(x)
    out: List[float] = [float("nan")] * n
    for i in range(periods, n):
        a = float(x[i])
        b = float(x[i - periods])
        out[i] = a - b
    return out


def pct_change(x: Sequence[float], periods: int = 1) -> List[float]:
    if periods <= 0:
        raise ValueError("periods must be > 0")
    n = len(x)
    out: List[float] = [float("nan")] * n
    for i in range(periods, n):
        cur = float(x[i])
        prev = float(x[i - periods])
        out[i] = (cur / prev - 1.0) if abs(prev) > _EPS else float("nan")
    return out


def log_return(x: Sequence[float], periods: int = 1) -> List[float]:
    if periods <= 0:
        raise ValueError("periods must be > 0")
    n = len(x)
    out: List[float] = [float("nan")] * n
    for i in range(periods, n):
        cur = float(x[i])
        prev = float(x[i - periods])
        if cur > 0.0 and prev > 0.0:
            out[i] = (0.0 if cur == prev else _ln(cur / prev))
        else:
            out[i] = float("nan")
    return out


def _ln(z: float) -> float:
    # Natural log via math library is fine, but keep dependencies minimal
    # Use a stable approximation for z close to 1 if desired; here we use change-of-base
    # Actually we can just import math.log; leaving this helper for patchability.
    from math import log

    return log(z)


def rolling_sum(x: Sequence[float], window: int, min_periods: Optional[int] = None) -> List[float]:
    _validate_window(window)
    n = len(x)
    mp = window if min_periods is None else min_periods
    if mp <= 0 or mp > window:
        raise ValueError("min_periods must be in [1, window]")
    out: List[float] = [float("nan")] * n
    for i in range(n):
        start = max(0, i - window + 1)
        vals = [float(v) for v in x[start : i + 1] if not _is_nan(float(v))]
        if len(vals) >= mp:
            s = 0.0
            for v in vals:
                s += v
            out[i] = s
    return out


def rolling_mean(x: Sequence[float], window: int, min_periods: Optional[int] = None) -> List[float]:
    _validate_window(window)
    n = len(x)
    mp = window if min_periods is None else min_periods
    if mp <= 0 or mp > window:
        raise ValueError("min_periods must be in [1, window]")
    out: List[float] = [float("nan")] * n
    for i in range(n):
        start = max(0, i - window + 1)
        vals = [float(v) for v in x[start : i + 1]]
        m = _nanmean(vals)
        count = sum(0 if _is_nan(float(v)) else 1 for v in vals)
        out[i] = m if count >= mp else float("nan")
    return out


def rolling_std(x: Sequence[float], window: int, ddof: int = 0, min_periods: Optional[int] = None) -> List[float]:
    _validate_window(window)
    n = len(x)
    mp = window if min_periods is None else min_periods
    if mp <= 0 or mp > window:
        raise ValueError("min_periods must be in [1, window]")
    if ddof < 0:
        raise ValueError("ddof must be >= 0")
    out: List[float] = [float("nan")] * n
    for i in range(n):
        start = max(0, i - window + 1)
        vals = [float(v) for v in x[start : i + 1]]
        count = sum(0 if _is_nan(float(v)) else 1 for v in vals)
        if count >= mp:
            out[i] = _nanstd(vals, ddof=ddof)
    return out


def rolling_zscore(x: Sequence[float], window: int, ddof: int = 0, min_periods: Optional[int] = None) -> List[float]:
    _validate_window(window)
    n = len(x)
    mu = rolling_mean(x, window=window, min_periods=min_periods)
    sd = rolling_std(x, window=window, ddof=ddof, min_periods=min_periods)
    out: List[float] = [float("nan")] * n
    for i in range(n):
        xi = float(x[i])
        if _is_nan(xi) or _is_nan(mu[i]) or _is_nan(sd[i]) or abs(sd[i]) <= _EPS:
            out[i] = float("nan")
        else:
            out[i] = (xi - mu[i]) / sd[i]
    return out


def ewm_mean(x: Sequence[float], span: int, adjust: bool = False) -> List[float]:
    """
    Exponentially weighted mean (EMA).

    span -> alpha = 2 / (span + 1)
    adjust=False uses recursive form:
        y_t = alpha*x_t + (1-alpha)*y_{t-1}
    adjust=True uses normalized weights (more expensive).
    """
    if span <= 0:
        raise ValueError("span must be > 0")
    alpha = 2.0 / (span + 1.0)
    n = len(x)
    out: List[float] = [float("nan")] * n

    if n == 0:
        return out

    if not adjust:
        y = float("nan")
        for i in range(n):
            xi = float(x[i])
            if _is_nan(xi):
                out[i] = y
                continue
            if _is_nan(y):
                y = xi
            else:
                y = alpha * xi + (1.0 - alpha) * y
            out[i] = y
        return out

    # adjust=True: compute normalized weights
    num = 0.0
    den = 0.0
    w = 1.0
    for i in range(n):
        xi = float(x[i])
        if not _is_nan(xi):
            num = num * (1.0 - alpha) + xi * alpha
            den = den * (1.0 - alpha) + alpha
            out[i] = num / den if den > _EPS else float("nan")
        else:
            num = num * (1.0 - alpha)
            den = den * (1.0 - alpha)
            out[i] = num / den if den > _EPS else float("nan")
    return out


def clip(x: Sequence[float], lower: float, upper: float) -> List[float]:
    if lower > upper:
        raise ValueError("lower must be <= upper")
    out: List[float] = []
    for v in x:
        fv = float(v)
        if _is_nan(fv):
            out.append(float("nan"))
        elif fv < lower:
            out.append(lower)
        elif fv > upper:
            out.append(upper)
        else:
            out.append(fv)
    return out


def winsorize_z(x: Sequence[float], z: float) -> List[float]:
    if z <= 0:
        raise ValueError("z must be > 0")
    mu = _nanmean([float(v) for v in x])
    sd = _nanstd([float(v) for v in x], ddof=0)
    if _is_nan(mu) or _is_nan(sd) or sd <= _EPS:
        return _to_float_list(x)
    lower = mu - z * sd
    upper = mu + z * sd
    return clip(x, lower=lower, upper=upper)


def rank(x: Sequence[float], ascending: bool = True, pct: bool = True) -> List[float]:
    """
    Rank with NaN kept as NaN.
    If pct=True, output is in (0, 1] (dense rank over non-NaNs).
    """
    n = len(x)
    vals: List[Tuple[int, float]] = []
    out: List[float] = [float("nan")] * n
    for i, v in enumerate(x):
        fv = float(v)
        if not _is_nan(fv):
            vals.append((i, fv))

    vals.sort(key=lambda t: t[1], reverse=not ascending)
    if not vals:
        return out

    # dense rank
    r = 0
    prev: Optional[float] = None
    ranks: List[Tuple[int, int]] = []
    for idx, v in vals:
        if prev is None or v != prev:
            r += 1
            prev = v
        ranks.append((idx, r))

    denom = float(r) if pct else 1.0
    for idx, rv in ranks:
        out[idx] = (rv / denom) if pct else float(rv)
    return out


def normal_cdf(x: Sequence[float]) -> List[float]:
    """
    Standard normal CDF approximation using erf.
    Phi(t) = 0.5 * (1 + erf(t / sqrt(2))).
    """
    out: List[float] = []
    inv_sqrt2 = 1.0 / sqrt(2.0)
    for v in x:
        fv = float(v)
        if _is_nan(fv):
            out.append(float("nan"))
        else:
            out.append(0.5 * (1.0 + erf(fv * inv_sqrt2)))
    return out