from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from lib.projection_rebuild_verifier import (
    ProjectionRebuildError,
    render_english_projection_v2,
    verify_cs408_projection_rebuild,
    verify_english_projection_bytes,
    verify_english_projection_rebuild,
)


CS408_REPO = Path("/Users/xiazhibin/Documents/kaoyan-408")
ENGLISH_REPO = Path("/Users/xiazhibin/Documents/kaoyan-english")


class ProjectionRebuildVerifierTests(unittest.TestCase):
    def test_english_projection_v2_binds_high_water_and_rejects_stale_bytes(self) -> None:
        effective = {
            "event_ids": ["EVT-1"],
            "event_count": 1,
            "high_water_sha256": "1" * 64,
        }
        expected = render_english_projection_v2(
            "# candidate\n",
            source_id="RAW-TEST-001",
            study_date="2026-08-09",
            effective=effective,
            raw_high_water="2" * 64,
            migration_high_water="3" * 64,
        )
        current = verify_english_projection_bytes(
            expected,
            expected,
            effective_high_water_sha256="1" * 64,
        )
        stale = verify_english_projection_bytes(
            expected + b"changed\n",
            expected,
            effective_high_water_sha256="1" * 64,
        )
        self.assertEqual(current["status"], "bound_current")
        self.assertTrue(current["metadata_high_water_matches"])
        self.assertEqual(stale["status"], "stale_or_unbound")
        self.assertFalse(stale["byte_equal"])

    @unittest.skipUnless(CS408_REPO.is_dir(), "local 408 canonical repo unavailable")
    def test_real_cs408_inputs_rebuild_twice_and_pass_isolated_audit(self) -> None:
        with tempfile.TemporaryDirectory(prefix="test-cs408-projection-") as raw:
            receipt = verify_cs408_projection_rebuild(CS408_REPO, Path(raw))
        self.assertEqual(receipt["issue_id"], "CS408-AUDIT-004")
        self.assertEqual(receipt["repair_capability_status"], "verified")
        self.assertTrue(receipt["checks"]["two_rebuilds_byte_identical"])
        self.assertTrue(receipt["checks"]["isolated_canonical_inputs_unchanged"])
        self.assertTrue(receipt["checks"]["isolated_canonical_audit_passed"])
        live_event_count = sum(
            bool(line.strip())
            for line in (
                CS408_REPO
                / "wiki/study_vaults/408-full/state/review-loop/events.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        )
        self.assertEqual(
            receipt["canonical_input"]["review_ledger"]["event_count"],
            live_event_count,
        )
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertEqual(receipt["model_call_count"], 0)

    @unittest.skipUnless(ENGLISH_REPO.is_dir(), "local English canonical repo unavailable")
    def test_real_english_effective_events_reproduce_audited_high_water(self) -> None:
        with tempfile.TemporaryDirectory(prefix="test-english-projection-") as raw:
            receipt = verify_english_projection_rebuild(
                ENGLISH_REPO,
                Path(raw),
                source_id="RAW-ARTICLE-20260710-001",
                study_date="2026-08-06",
            )
        self.assertEqual(receipt["issue_id"], "EN-P0-004")
        self.assertEqual(receipt["repair_capability_status"], "verified")
        self.assertTrue(receipt["checks"]["two_rebuilds_byte_identical"])
        self.assertTrue(receipt["checks"]["stale_fixture_rejected"])
        self.assertEqual(
            receipt["canonical_input"]["effective_events"]["high_water_sha256"],
            "ae9c18751682bc6891a23ccc5cf5bc9d8e13fbec167bab1cd9e21b6382594321",
        )
        self.assertEqual(
            receipt["canonical_body"]["sha256"],
            "ab75225f6ade2cb2d20d5dcb2838d78be588ca66026a332eb57f9b2c83f4e263",
        )
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertEqual(receipt["model_call_count"], 0)

    def test_artifact_root_inside_live_repo_is_rejected(self) -> None:
        if not ENGLISH_REPO.is_dir():
            self.skipTest("local English canonical repo unavailable")
        with self.assertRaisesRegex(
            ProjectionRebuildError, "projection_artifact_root_inside_live_repo"
        ):
            verify_english_projection_rebuild(
                ENGLISH_REPO,
                ENGLISH_REPO / "tmp" / "forbidden-projection-test",
                source_id="RAW-ARTICLE-20260710-001",
                study_date="2026-08-06",
            )


if __name__ == "__main__":
    unittest.main()
