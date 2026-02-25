from __future__ import annotations

from functools import lru_cache
import importlib
from typing import Any, Callable

import numpy as np
import pandas as pd
import streamlit as st

from ui.services.types.cryptos import VolumeProfileDTO

_MAX_RENDER_POINTS = 5000


@lru_cache(maxsize=1)
def _resolve_lightweight_renderer() -> tuple[Callable[..., Any] | None, str | None]:
    candidates: list[tuple[str, list[str]]] = [
        ("streamlit_lightweight_charts_v5", ["renderLightweightCharts", "render_lightweight_charts"]),
        ("streamlit_lightweight_charts", ["renderLightweightCharts", "render_lightweight_charts"]),
    ]
    for module_name, function_names in candidates:
        try:
            module = importlib.import_module(module_name)
        except Exception:
            continue
        for fn_name in function_names:
            render_fn = getattr(module, fn_name, None)
            if callable(render_fn):
                return render_fn, module_name
    return None, None


def render_klines_chart(
    frame: pd.DataFrame,
    *,
    key: str,
    max_points: int = _MAX_RENDER_POINTS,
) -> tuple[int, bool]:
    """Render klines as candlestick + volume histogram with lightweight-charts."""
    clean = _prepare_klines_frame(frame)
    if clean.empty:
        st.info("No klines data to display.")
        return 0, False

    plotted, limited = _downsample(clean, max_points=max_points)
    series = [
        {
            "type": "Candlestick",
            "data": [
                {
                    "time": row["time"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                }
                for _, row in plotted.iterrows()
            ],
            "options": {},
        }
    ]

    if "volume" in plotted.columns:
        series.append(
            {
                "type": "Histogram",
                "data": [
                    {
                        "time": row["time"],
                        "value": row["volume"],
                        "color": "rgba(38, 166, 154, 0.6)" if row["close"] >= row["open"] else "rgba(239, 83, 80, 0.6)",
                    }
                    for _, row in plotted.iterrows()
                ],
                "options": {
                    "priceFormat": {"type": "volume"},
                    "priceScaleId": "",
                    "scaleMargins": {"top": 0.78, "bottom": 0.0},
                },
            },
        )

    chart_options = _default_chart_options()
    if not _render_lightweight([{"chart": chart_options, "series": series}], key=key):
        st.line_chart(plotted.set_index("timestamp")["close"], use_container_width=True)

    return int(plotted.shape[0]), limited


def render_trades_price_chart(
    frame: pd.DataFrame,
    *,
    key: str,
    max_points: int = _MAX_RENDER_POINTS,
) -> tuple[int, bool]:
    """Render trades price line with lightweight-charts."""
    clean = _prepare_trades_frame(frame)
    if clean.empty:
        st.info("No trades data to display.")
        return 0, False

    plotted, limited = _downsample(clean, max_points=max_points)
    series = [
        {
            "type": "Line",
            "data": [{"time": row["time"], "value": row["price"]} for _, row in plotted.iterrows()],
            "options": {"lineWidth": 1, "color": "#0066cc"},
        }
    ]
    if not _render_lightweight([{"chart": _default_chart_options(), "series": series}], key=key):
        st.line_chart(plotted.set_index("timestamp")["price"], use_container_width=True)

    return int(plotted.shape[0]), limited


def render_volume_profile_chart(profile: VolumeProfileDTO, *, title: str = "Volume Profile") -> None:
    """Render volume profile with a Streamlit fallback bar chart."""
    if not profile.bin_centers or not profile.volumes:
        st.info("Insufficient data for volume profile.")
        return

    values = np.asarray(profile.volumes, dtype=float)
    labels = "Volume (%)" if profile.normalized else "Volume"
    st.caption(f"{title} ({labels}, type={profile.volume_type})")
    profile_frame = pd.DataFrame(
        {
            "price": profile.bin_centers,
            "volume": values,
        },
    ).set_index("price")
    st.bar_chart(profile_frame["volume"], use_container_width=True)


def _render_lightweight(payload: list[dict[str, Any]], *, key: str) -> bool:
    render_fn, module_name = _resolve_lightweight_renderer()
    if render_fn is None:
        st.warning(
            "Lightweight charts component is not installed. "
            "Install streamlit-lightweight-charts-v5 or streamlit-lightweight-charts for full chart interaction.",
        )
        return False

    try:
        render_fn(payload, key=key)
        return True
    except TypeError:
        try:
            render_fn(payload)
            return True
        except Exception as exc:
            st.error(f"Failed to render lightweight chart ({module_name}): {exc}")
            return False
    except Exception as exc:
        st.error(f"Failed to render lightweight chart ({module_name}): {exc}")
        return False


def _prepare_klines_frame(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    if "timestamp" not in working.columns:
        if "open_time" in working.columns:
            working["timestamp"] = pd.to_datetime(working["open_time"], utc=True, errors="coerce")
        elif pd.api.types.is_datetime64_any_dtype(working.index):
            working = working.reset_index()
            first_col = working.columns[0]
            working["timestamp"] = pd.to_datetime(working[first_col], utc=True, errors="coerce")
        else:
            working["timestamp"] = pd.to_datetime(working.index, utc=True, errors="coerce")
    else:
        working["timestamp"] = pd.to_datetime(working["timestamp"], utc=True, errors="coerce")

    for col in ("open", "high", "low", "close", "volume"):
        if col in working.columns:
            working[col] = pd.to_numeric(working[col], errors="coerce")

    required = ["timestamp", "open", "high", "low", "close"]
    working = working.dropna(subset=required)
    working = working.sort_values("timestamp")
    working["time"] = (working["timestamp"].astype("int64") // 10**9).astype(int)
    return working


def _prepare_trades_frame(frame: pd.DataFrame) -> pd.DataFrame:
    working = frame.copy()
    if "timestamp" not in working.columns:
        if pd.api.types.is_datetime64_any_dtype(working.index):
            working = working.reset_index()
            first_col = working.columns[0]
            working["timestamp"] = pd.to_datetime(working[first_col], utc=True, errors="coerce")
        else:
            working["timestamp"] = pd.to_datetime(working.index, utc=True, errors="coerce")
    else:
        working["timestamp"] = pd.to_datetime(working["timestamp"], utc=True, errors="coerce")

    working["price"] = pd.to_numeric(working.get("price"), errors="coerce")
    working = working.dropna(subset=["timestamp", "price"])
    working = working.sort_values("timestamp")
    working["time"] = (working["timestamp"].astype("int64") // 10**9).astype(int)
    return working


def _downsample(frame: pd.DataFrame, *, max_points: int) -> tuple[pd.DataFrame, bool]:
    if max_points <= 0 or frame.shape[0] <= max_points:
        return frame, False
    step = max(1, int(np.ceil(frame.shape[0] / max_points)))
    return frame.iloc[::step].copy(), True


def _default_chart_options() -> dict[str, Any]:
    return {
        "height": 520,
        "layout": {
            "background": {"type": "solid", "color": "#FFFFFF"},
            "textColor": "#1F2937",
            "fontSize": 12,
        },
        "grid": {
            "vertLines": {"color": "rgba(31, 41, 55, 0.08)"},
            "horzLines": {"color": "rgba(31, 41, 55, 0.08)"},
        },
        "crosshair": {"mode": 1},
        "rightPriceScale": {"borderColor": "rgba(31, 41, 55, 0.2)"},
        "timeScale": {
            "borderColor": "rgba(31, 41, 55, 0.2)",
            "timeVisible": True,
            "secondsVisible": False,
        },
    }
