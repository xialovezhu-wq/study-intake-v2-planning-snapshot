from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from study_read_mcp.service import StudyReadService
from study_read_mcp.session import ReadSession

from .test_focused_mcp import make_v2_session


class AuthoritySnapshotTests(unittest.TestCase):
    def test_current_session_binds_successor_snapshot_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _config, session_path = make_v2_session(Path(temp), "math")
            value = json.loads(session_path.read_text(encoding="utf-8"))
            snapshot = json.loads(
                Path(value["authority_snapshot_manifest_path"]).read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(
                value["schema_version"], "study-read-mcp-read-session.v4"
            )
            self.assertEqual(
                snapshot["schema_version"],
                "study-read-mcp-authority-snapshot.v2",
            )

    def test_live_408_review_append_does_not_invalidate_frozen_session(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            config, session_path = make_v2_session(base, "cs408")
            session = ReadSession.load(session_path)
            service = StudyReadService(
                config,
                subjects={"cs408"},
                profile="luna",
                read_session=session,
                require_authority_snapshot=True,
            )
            try:
                before = service.get_task_context("cs408")
                frozen_generation = before["generation"]
                live_events = (
                    config.cs408_root
                    / "wiki"
                    / "study_vaults"
                    / "408-full"
                    / "state"
                    / "review-loop"
                    / "events.jsonl"
                )
                with live_events.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps({
                        "event_id": "LIVE-AFTER-SNAPSHOT",
                        "session_id": "SESSION-AFTER-SNAPSHOT",
                        "item_id": "ITEM-AFTER-SNAPSHOT",
                        "event": "answer-recorded",
                    }, sort_keys=True) + "\n")
                artifact, _ = service.read_task_artifact(
                    "cs408", "ART-TEXT", max_bytes=4096
                )
                review = service.list_records(
                    "cs408", "review_events", page_size=48
                )
                self.assertEqual(artifact["generation"], frozen_generation)
                self.assertEqual(review["generation"], frozen_generation)
                self.assertNotIn(
                    "LIVE-AFTER-SNAPSHOT",
                    json.dumps(review, ensure_ascii=False),
                )
                self.assertEqual(
                    before["read_session"][
                        "authority_snapshot_manifest_sha256"
                    ],
                    artifact["read_session"][
                        "authority_snapshot_manifest_sha256"
                    ],
                )
            finally:
                service.close()

    def test_production_luna_launcher_rejects_legacy_session(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config, session_path = make_v2_session(Path(temp), "math")
            value = json.loads(session_path.read_text(encoding="utf-8"))
            for key in (
                "authority_snapshot_manifest_path",
                "authority_snapshot_manifest_sha256",
                "authority_snapshot_root",
                "authority_snapshot_receipt_sha256",
            ):
                value.pop(key)
            value["schema_version"] = "study-read-mcp-read-session.v2"
            core = {
                key: nested
                for key, nested in value.items()
                if key != "manifest_sha256"
            }
            import hashlib

            value["manifest_sha256"] = hashlib.sha256(
                (json.dumps(core, sort_keys=True, separators=(",", ":")) + "\n").encode()
            ).hexdigest()
            session_path.write_text(
                json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(Exception, "frozen authority snapshot"):
                StudyReadService(
                    config,
                    subjects={"math"},
                    profile="luna",
                    read_session=ReadSession.load(session_path),
                    require_authority_snapshot=True,
                )


if __name__ == "__main__":
    unittest.main()
