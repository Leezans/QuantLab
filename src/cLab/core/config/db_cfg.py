# cLab/core/config/db_cfg.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


def _default_crypto_filedb_path() -> Path:
    if os.name == "nt":
        return Path("G:/database/crypto")
    return Path.home() / "Documents" / "database" / "crypto"


CRYPTOS_DATABASE_PATH: Path = Path(os.getenv("CRYPTOS_DATABASE_PATH", str(_default_crypto_filedb_path())))
BINANCE_DIR: Path = CRYPTOS_DATABASE_PATH / "binance"
BINANCE_API_KEY_PATH: Path = BINANCE_DIR / "api_key.json"


@dataclass(frozen=True)
class BinanceKeys:
    api_key: str 

def load_binance_keys(path: Path = BINANCE_API_KEY_PATH) -> BinanceKeys:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    api_key = obj.get("api_key")
    if not api_key:
        raise ValueError(f"Missing 'api_key' in {path}")
    return BinanceKeys(api_key=api_key)





if __name__ == "__main__":  
    print(CRYPTOS_DATABASE_PATH)
    apikey = load_binance_keys().api_key
    print(apikey)

    pass