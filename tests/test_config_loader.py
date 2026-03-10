from __future__ import annotations

import unittest
from pathlib import Path

from quantlab.config.loader import load_settings


class ConfigLoaderTestCase(unittest.TestCase):
    def test_loads_base_settings(self) -> None:
        settings = load_settings(Path("config/base.toml"))
        root = Path.home() / "Documents" / "database" / "crypto"
        self.assertEqual(settings.project.name, "QuantLab")
        self.assertEqual(settings.project.environment, "research")
        self.assertEqual(settings.runtime.max_workers, 4)
        self.assertIn("BTCUSDT", settings.research.default_universe)
        self.assertEqual(settings.storage.raw_data_dir, root / "raw")
        self.assertEqual(settings.storage.intraday_cache_dir, root / "intraday_cache")
        self.assertEqual(settings.storage.warehouse_dir, root / "warehouse")
        self.assertEqual(settings.storage.catalog_path, root / "catalog" / "catalog.json")


if __name__ == "__main__":
    unittest.main()
