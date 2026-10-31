"""Offline regression checks for Silver DimUser change-data capture semantics.

These tests exercise the deduplication contract without claiming to execute
Databricks Structured Streaming or the Lakeflow CDC engine.
"""

import unittest
from pathlib import Path

from spotify_dab.utils.transformations import reusable


ROOT = Path(__file__).resolve().parents[1]


class FakeStreamFrame:
    """Minimal stand-in that records and applies the requested dedup keys."""

    def __init__(self, rows):
        self.rows = rows
        self.keys = None

    def dropDuplicates(self, keys):
        self.keys = list(keys)
        seen = set()
        retained = []
        for row in self.rows:
            identity = tuple(row[key] for key in keys)
            if identity not in seen:
                retained.append(row)
                seen.add(identity)
        return FakeStreamFrame(retained)


class DimUserCdcTests(unittest.TestCase):
    def test_later_user_updates_survive_silver_deduplication(self):
        source = FakeStreamFrame([
            {"user_id": 1, "updated_at": "2026-01-01T00:00:00Z", "name": "old"},
            {"user_id": 1, "updated_at": "2026-02-01T00:00:00Z", "name": "new"},
            {"user_id": 1, "updated_at": "2026-02-01T00:00:00Z", "name": "new"},
            {"user_id": 2, "updated_at": "2026-01-01T00:00:00Z", "name": "other"},
        ])
        result = reusable().deduplicateUserChanges(source)
        self.assertEqual(
            [(row["user_id"], row["name"]) for row in result.rows],
            [(1, "old"), (1, "new"), (2, "other")],
        )

    def test_deduplication_uses_same_sequence_key_as_gold_cdc(self):
        class RecordingFrame:
            def __init__(self):
                self.keys = None

            def dropDuplicates(self, keys):
                self.keys = keys
                return self

        frame = RecordingFrame()
        self.assertIs(reusable().deduplicateUserChanges(frame), frame)
        self.assertEqual(frame.keys, ["user_id", "updated_at"])

        silver = (ROOT / "spotify_dab/src/med_architecture/silver/silver_dimensions.py").read_text()
        gold = (ROOT / "spotify_dab/src/med_architecture/gold/dlt/transformations/DimUser.py").read_text()
        self.assertIn("df_user_obj.deduplicateUserChanges(df_user)", silver)
        self.assertIn('sequence_by="updated_at"', gold)


if __name__ == "__main__":
    unittest.main()
