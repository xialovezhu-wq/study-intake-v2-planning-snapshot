from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from sol_mcp_preflight import (  # noqa: E402
    PREFLIGHT_TOOLS,
    SolMCPPreflightError,
    _structured_tool_result,
    _tool_list,
    _validate_offline_runtime,
    canonical_bytes,
    content_addressed_receipt,
)


class SolMCPPreflightTests(unittest.TestCase):
    def test_tool_list_accepts_only_four_read_only_tools(self) -> None:
        tools = {
            "tools": [
                {
                    "name": name,
                    "inputSchema": {"type": "object"},
                    "annotations": {
                        "readOnlyHint": True,
                        "destructiveHint": False,
                    },
                }
                for name in PREFLIGHT_TOOLS
            ]
        }
        self.assertEqual(
            [row["name"] for row in _tool_list(tools)],
            list(PREFLIGHT_TOOLS),
        )
        tools["tools"][0]["name"] = "update_record"
        with self.assertRaises(SolMCPPreflightError) as caught:
            _tool_list(tools)
        self.assertIn(
            caught.exception.code,
            {"tools_list_contract_mismatch", "read_policy_violation"},
        )

    def test_structured_result_requires_capture_absent_and_subject_evidence(self) -> None:
        value = {
            "isError": False,
            "structuredContent": {
                "ok": True,
                "subject": "math",
                "profile": "infrastructure_preflight",
                "formal_write_count": 0,
                "model_call_count": 0,
                "write_call_count": 0,
                "candidate_eligible": False,
                "production_evidence": False,
                "formal_write_allowed": False,
                "items": [
                    {
                        "stable_id": "GS-001",
                        "evidence_ref": "mcp-item:math:" + "1" * 64,
                    }
                ],
                "preflight_session": {
                    "preflight_session_id": "MCPPF-MATH-FIXTURE-0001",
                    "capture_id": "absent",
                    "production_task_id": "absent",
                },
            },
        }
        reopened = _structured_tool_result(
            value,
            subject="math",
            session_id="MCPPF-MATH-FIXTURE-0001",
        )
        self.assertEqual(reopened["subject"], "math")
        value["structuredContent"]["preflight_session"]["capture_id"] = "CAP-1"
        with self.assertRaises(SolMCPPreflightError):
            _structured_tool_result(
                value,
                subject="math",
                session_id="MCPPF-MATH-FIXTURE-0001",
            )

    def test_content_addressed_receipt_reopens_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            core = {
                "schema_version": "fixture-v1",
                "formal_write_count": 0,
            }
            first_path, first = content_addressed_receipt(root, core)
            second_path, second = content_addressed_receipt(root, core)
            self.assertEqual(first_path, second_path)
            self.assertEqual(first, second)
            self.assertEqual(json.loads(first_path.read_text()), first)

    def test_offline_runtime_gate_rejects_authorization_and_nonzero_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            release_id = "a" * 64
            release = root / "releases" / release_id
            release.mkdir(parents=True)
            authorization = root / "dispatch/manual-live-authorization-v1/state.json"
            projection = root / "state/dashboard_projection.json"
            projection.parent.mkdir(parents=True)
            config = {
                "execution_mode": "offline",
                "live_execution_gate": {
                    "authorization_state_path": str(authorization)
                },
                "dashboard": {"projection_path": str(projection)},
            }
            (release / "config.json").write_bytes(canonical_bytes(config))
            os.symlink(release, root / "current")
            projection_value = {
                "dispatchers": {
                    subject: {
                        "canary_gate": {
                            "production_accepted": False,
                            "active_task_count": 0,
                            "queue_depth": 0,
                            "model_call_count": 0,
                            "provider_request_count": 0,
                            "mcp_tool_call_count": 0,
                            "formal_write_count": 0,
                        }
                    }
                    for subject in ("math", "cs408", "english")
                },
                "global_sol": {"formal_write_count": None},
            }
            projection.write_bytes(canonical_bytes(projection_value))
            _validate_offline_runtime(
                runtime_root=root, central_release_id=release_id
            )
            authorization.parent.mkdir(parents=True)
            authorization.write_text("{}\n", encoding="utf-8")
            with self.assertRaises(SolMCPPreflightError) as caught:
                _validate_offline_runtime(
                    runtime_root=root, central_release_id=release_id
                )
            self.assertEqual(caught.exception.code, "read_policy_violation")


if __name__ == "__main__":
    unittest.main()
