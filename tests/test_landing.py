from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from energy_market_lakehouse.landing import atomic_json, new_batch_id, sha256_payload


class LandingTests(unittest.TestCase):
    def test_checksum_is_independent_of_mapping_order(self) -> None:
        self.assertEqual(sha256_payload({"a": 1, "b": 2}), sha256_payload({"b": 2, "a": 1}))

    def test_atomic_json_creates_readable_file(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "nested" / "payload.json"
            checksum = atomic_json(path, {"ok": True})
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"ok": True})
            self.assertEqual(checksum, sha256_payload({"ok": True}))

    def test_batch_id_is_utc_and_sortable(self) -> None:
        value = new_batch_id(datetime(2026, 8, 20, 12, 30, tzinfo=UTC))
        self.assertEqual(value, "20260820T123000000000Z")


if __name__ == "__main__":
    unittest.main()
