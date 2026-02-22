# cLab/infra/dataSource/binance_source.py
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional

import requests

from cLab.infra.storage.fileDB import (
    BinanceFileSpec,
    Dataset,
    FileDB,
)


BINANCE_VISION_BASE_URL = "https://data.binance.vision/data"


@dataclass(frozen=True)
class DownloadResult:
    data_path: str
    checksum_path: Optional[str]
    verified: bool


class BinanceVisionClient:
    def __init__(self, session: Optional[requests.Session] = None, timeout_sec: float = 60.0):
        self._session = session or requests.Session()
        self._timeout = timeout_sec

    def remote_url(self, spec: BinanceFileSpec) -> str:
        spec.validate()

        parts: list[str] = [
            BINANCE_VISION_BASE_URL,
            spec.market.value,
            spec.frequency.value,
            spec.dataset.value,
            spec.symbol,
        ]
        if spec.dataset == Dataset.KLINES:
            parts.append(str(spec.interval))

        filename = self._remote_filename(spec)
        return "/".join(parts + [filename])

    def download_to_filedb(
        self,
        filedb: FileDB,
        data_spec: BinanceFileSpec,
        fetch_checksum: bool = True,
        verify: bool = True,
    ) -> DownloadResult:
        data_spec.validate()
        if data_spec.with_checksum:
            raise ValueError("data_spec.with_checksum must be False for data download.")

        data_bytes = self._http_get(self.remote_url(data_spec))
        data_path = filedb.write_bytes_atomic(data_spec, data_bytes)

        checksum_path = None
        verified = False

        if fetch_checksum:
            checksum_spec = BinanceFileSpec(
                market=data_spec.market,
                frequency=data_spec.frequency,
                dataset=data_spec.dataset,
                symbol=data_spec.symbol,
                date=data_spec.date,
                interval=data_spec.interval,
                with_checksum=True,
            )
            checksum_text = self._http_get_text(self.remote_url(checksum_spec))
            checksum_path = filedb.write_text_atomic(checksum_spec, checksum_text)

            if verify:
                expected = self._parse_checksum_file(checksum_text)
                actual = hashlib.sha256(data_bytes).hexdigest()
                verified = (expected == actual)
                if not verified:
                    raise ValueError(
                        f"Checksum mismatch. expected={expected} actual={actual} path={data_path}"
                    )

        return DownloadResult(
            data_path=str(data_path),
            checksum_path=str(checksum_path) if checksum_path else None,
            verified=verified,
        )

    def _http_get(self, url: str) -> bytes:
        r = self._session.get(url, timeout=self._timeout)
        r.raise_for_status()
        return r.content

    def _http_get_text(self, url: str, encoding: str = "utf-8") -> str:
        r = self._session.get(url, timeout=self._timeout)
        r.raise_for_status()
        r.encoding = r.encoding or encoding
        return r.text

    def _remote_filename(self, spec: BinanceFileSpec) -> str:
        if spec.dataset == Dataset.KLINES:
            name = f"{spec.symbol}-{spec.interval}-{spec.date}.zip"
        elif spec.dataset == Dataset.AGGTRADES:
            name = f"{spec.symbol}-aggTrades-{spec.date}.zip"
        elif spec.dataset == Dataset.TRADES:
            name = f"{spec.symbol}-trades-{spec.date}.zip"
        else:
            raise ValueError(f"Unsupported dataset: {spec.dataset}")

        if spec.with_checksum:
            name += ".CHECKSUM"
        return name

    def _parse_checksum_file(self, text: str) -> str:
        s = text.strip()
        if not s:
            raise ValueError("Empty checksum content.")
        token = s.split()[0].strip()
        if len(token) != 64:
            raise ValueError(f"Unexpected checksum format: {token}")
        return token.lower()

