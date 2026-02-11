from __future__ import annotations

import pandas as pd

from cLab.model.factor.aggtrades_1m import aggtrades_to_minute_factors


def test_aggtrades_to_minute_factors_basic() -> None:
    df = pd.DataFrame(
        [
            {"a": 1, "p": "100", "q": "1", "T": 1700000000000, "m": False},
            {"a": 2, "p": "110", "q": "2", "T": 1700000001000, "m": True},
            {"a": 3, "p": "120", "q": "1", "T": 1700000060000, "m": False},
        ]
    )

    out = aggtrades_to_minute_factors(df)
    assert len(out) == 2

    # First minute aggregates two trades
    r0 = out.iloc[0]
    assert r0["n_trades"] == 2
    assert r0["volume_base"] == 3.0
    assert r0["buy_base"] == 1.0
    assert r0["sell_base"] == 2.0
    assert r0["buy_sell_imbalance_base"] == -1.0

    # VWAP = (100*1 + 110*2) / 3
    assert abs(r0["vwap"] - ((100.0 + 220.0) / 3.0)) < 1e-9
