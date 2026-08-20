from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from study_read_mcp.errors import StudyReadError
from study_read_mcp.preflight import (
    InfrastructurePreflightSession,
    PREFLIGHT_TOOLS,
    PreflightPolicy,
    canonical_bytes,
    read_root_identity,
)
from study_read_mcp.release import SERVER_RELEASE
from study_read_mcp.server import build_server
from study_read_mcp.service import StudyReadService

from .helpers import make_fixture


def policy_value() -> dict:
    return {
        "schema_version": "study-read-mcp-infrastructure-preflight-tool-policy.v1",
        "purpose": "infrastructure_preflight",
        "subject_scoped": True,
        "enabled_tools": list(PREFLIGHT_TOOLS),
        "write_tools_allowed": False,
        "candidate_eligible": False,
        "production_evidence": False,
        "formal_write_allowed": False,
    }


def build_binding(base: Path, subject: str) -> tuple[object, Path, Path]:
    config = make_fixture(base)
    policy_path = base / "preflight-tool-policy.json"
    policy_path.write_bytes(canonical_bytes(policy_value()))
    read_root = {
        "math": config.math_root,
        "cs408": config.cs408_root,
        "english": config.english_root,
    }[subject]
    now = datetime.now(timezone.utc)
    core = {
        "schema_version": "study-read-mcp-infrastructure-preflight-session.v1",
        "purpose": "infrastructure_preflight",
        "preflight_session_id": f"MCPPF-{subject.upper()}-FIXTURE-0001",
        "subject": subject,
        "central_release_id": config.preprocessor_root.joinpath("current").resolve().name,
        "mcp_release_id": "a" * 64,
        "mcp_server_release": SERVER_RELEASE,
        "mcp_release_manifest_sha256": "b" * 64,
        "launcher_sha256": "c" * 64,
        "tool_policy_sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest(),
        "read_root": str(read_root.resolve()),
        "read_root_identity_sha256": read_root_identity(read_root),
        "created_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=5)).isoformat(),
        "capture_id_absent": True,
        "production_task_id_absent": True,
        "candidate_eligible": False,
        "production_evidence": False,
        "formal_write_allowed": False,
        "formal_write_count": 0,
    }
    manifest = {
        **core,
        "manifest_sha256": hashlib.sha256(canonical_bytes(core)).hexdigest(),
    }
    session_path = base / f"{subject}-preflight-session.json"
    session_path.write_bytes(canonical_bytes(manifest))
    os.chmod(session_path, 0o600)
    return config, session_path, policy_path


class InfrastructurePreflightTests(unittest.TestCase):
    def test_three_subject_preflight_facades_are_capture_free_and_read_only(self) -> None:
        cases = {
            "math": ("formal_card_catalog", "GS-001", "Fixture card"),
            "cs408": (
                "formal_wrong_item_catalog",
                "DS_2023_002",
                "fixture core",
            ),
            "english": (
                "article_catalog",
                "RAW-ARTICLE-CORPUS",
                "practice-safe corpus",
            ),
        }
        for subject, (collection, stable_id, query) in cases.items():
            with self.subTest(subject=subject), tempfile.TemporaryDirectory() as temp:
                config, session_path, policy_path = build_binding(
                    Path(temp), subject
                )
                session = InfrastructurePreflightSession.load(session_path)
                policy = PreflightPolicy.load(
                    policy_path, session.tool_policy_sha256
                )
                service = StudyReadService(
                    config,
                    subjects={subject},
                    profile="infrastructure_preflight",
                    preflight_session=session,
                    preflight_policy=policy,
                )
                try:
                    server = build_server(service)
                    self.assertEqual(
                        list(server._tool_manager._tools), list(PREFLIGHT_TOOLS)
                    )
                    listed = service.list_records(
                        subject, collection, page_size=1
                    )
                    self.assertTrue(listed["ok"])
                    self.assertEqual(
                        listed["profile"], "infrastructure_preflight"
                    )
                    self.assertEqual(
                        listed["preflight_session"]["capture_id"], "absent"
                    )
                    self.assertFalse(listed["candidate_eligible"])
                    self.assertFalse(listed["production_evidence"])
                    self.assertFalse(listed["formal_write_allowed"])
                    self.assertEqual(listed["write_call_count"], 0)
                    self.assertEqual(listed["formal_write_count"], 0)
                    self.assertEqual(listed["model_call_count"], 0)
                    self.assertNotIn("read_session", listed)
                    if listed["next_cursor"] is not None:
                        continued = service.list_records(
                            subject,
                            collection,
                            cursor=listed["next_cursor"],
                            page_size=1,
                        )
                        self.assertTrue(continued["ok"])
                    exact = service.get_records(
                        subject, collection, [stable_id], page_size=1
                    )
                    self.assertEqual(exact["items"][0]["stable_id"], stable_id)
                    searched = service.search_records(
                        subject, query, page_size=1
                    )
                    self.assertTrue(searched["ok"])
                finally:
                    service.close()

    def test_preflight_manifest_rejects_capture_binding(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _config, session_path, _policy_path = build_binding(
                Path(temp), "math"
            )
            value = json.loads(session_path.read_text(encoding="utf-8"))
            value["capture_id"] = "CAP-FORBIDDEN"
            session_path.write_bytes(canonical_bytes(value))
            with self.assertRaises(StudyReadError) as caught:
                InfrastructurePreflightSession.load(session_path)
            self.assertEqual(caught.exception.code, "PREFLIGHT_SESSION_INVALID")

    def test_preflight_policy_rejects_write_tool(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            _config, session_path, policy_path = build_binding(
                Path(temp), "math"
            )
            value = policy_value()
            value["enabled_tools"].append("update_record")
            policy_path.write_bytes(canonical_bytes(value))
            with self.assertRaises(StudyReadError) as caught:
                PreflightPolicy.load(
                    policy_path,
                    hashlib.sha256(policy_path.read_bytes()).hexdigest(),
                )
            self.assertEqual(
                caught.exception.code, "PREFLIGHT_TOOL_POLICY_INVALID"
            )

    def test_preflight_central_release_mismatch_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            config, session_path, policy_path = build_binding(Path(temp), "math")
            value = json.loads(session_path.read_text(encoding="utf-8"))
            core = {
                key: nested
                for key, nested in value.items()
                if key != "manifest_sha256"
            }
            core["central_release_id"] = "f" * 64
            value = {
                **core,
                "manifest_sha256": hashlib.sha256(
                    canonical_bytes(core)
                ).hexdigest(),
            }
            session_path.write_bytes(canonical_bytes(value))
            session = InfrastructurePreflightSession.load(session_path)
            policy = PreflightPolicy.load(
                policy_path, session.tool_policy_sha256
            )
            with self.assertRaises(StudyReadError) as caught:
                StudyReadService(
                    config,
                    subjects={"math"},
                    profile="infrastructure_preflight",
                    preflight_session=session,
                    preflight_policy=policy,
                )
            self.assertEqual(
                caught.exception.code, "PREFLIGHT_CENTRAL_BINDING_MISMATCH"
            )


if __name__ == "__main__":
    unittest.main()
