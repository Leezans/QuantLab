from __future__ import annotations

import unittest

from quantlab.core.registry import ComponentRegistry


class RegistryTestCase(unittest.TestCase):
    def test_registry_tracks_named_components(self) -> None:
        registry = ComponentRegistry[str]()
        registry.register("alpha.momentum", "component")
        self.assertEqual(registry.get("alpha.momentum"), "component")
        self.assertEqual(registry.names(), ("alpha.momentum",))


if __name__ == "__main__":
    unittest.main()

