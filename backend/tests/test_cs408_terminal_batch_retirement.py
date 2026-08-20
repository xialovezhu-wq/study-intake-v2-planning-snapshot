from __future__ import annotations

import copy
import hashlib
import json
import tempfile
from pathlib import Path
import unittest
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

import subject_sol_contract as contract  # noqa: E402
from preprocess_dispatcher import ProductionDispatchRuntime, build_parser  # noqa: E402
from subject_sol_contract import (  # noqa: E402
    SubjectSolContractError,
    SubjectSolRuntimeStore,
)


def sha(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


class Cs408TerminalBatchRetirementTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.runtime_root = Path(self.temporary.name) / "runtime"
        self.store = SubjectSolRuntimeStore(self.runtime_root)
        self.original_authorization = copy.deepcopy(
            contract.CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
        )
        batch = {
            "schema_version": contract.SUBJECT_BATCH_SCHEMA,
            "batch_id": "LUNA-CS408-TEST-TERMINAL",
            "subject": "cs408",
            "study_date": "2026-08-12",
            "status": "frozen",
            "capture_high_watermark": "CAP-CS408-OLD",
            "scan_snapshot_sha256": sha("old-scan"),
            "authority_generation": "cs408-old-generation",
            "authority_fingerprint": sha("old-authority"),
            "tasks": [
                {
                    "capture_id": "CAP-CS408-OLD",
                    "unit_sha256": sha("old-unit"),
                    "input_fingerprint": sha("old-input"),
                    "study_date": "2026-08-12",
                    "frozen_payload_sha256": sha("old-payload"),
                    "status": "selected",
                    "proposal_sha256": None,
                    "package_sha256": None,
                    "quality_receipt_sha256": None,
                    "terminal_receipt_sha256": None,
                    "error_code": None,
                }
            ],
            "exclusion_receipt_sha256s": [],
            "all_terminal": False,
            "sol_ready": False,
            "blocking_task_ids": [],
            "formal_write_count": 0,
            "revision": -1,
            "updated_at": None,
        }
        written = self.store._write_batch_locked(batch)
        writer = self.store._read_writer_locked("cs408")
        writer["batch_id"] = written["batch_id"]
        self.store._write_writer_locked(writer)
        failed = self.store.record_terminal_failure(
            subject="cs408",
            batch_id=written["batch_id"],
            capture_id="CAP-CS408-OLD",
            unit_sha256=sha("old-unit"),
            status="failed",
            error_code="luna_task_failed",
        )
        pointer_path = self.store._batch_pointer_path("cs408")
        pointer = self.store._read_json(pointer_path, "pointer")
        assert pointer is not None
        terminal_sha = failed["tasks"][0]["terminal_receipt_sha256"]
        assert isinstance(terminal_sha, str)
        writer_path = self.store._writer_path("cs408")
        self.authorization = {
            "subject": "cs408",
            "mode": "archive_only_zero_replay",
            "batch_id": failed["batch_id"],
            "batch_sha256": hashlib.sha256(
                self.store._batch_path("cs408").read_bytes()
            ).hexdigest(),
            "terminal_receipt_sha256": terminal_sha,
            "writer_preimage_sha256": hashlib.sha256(
                writer_path.read_bytes()
            ).hexdigest(),
            "batch_pointer_sha256": hashlib.sha256(
                pointer_path.read_bytes()
            ).hexdigest(),
            "snapshot_sha256": pointer["snapshot_sha256"],
            "source_generation": failed["authority_generation"],
            "source_authority_fingerprint": failed[
                "authority_fingerprint"
            ],
            "target_generation": "cs408-next-generation",
            "target_authority_fingerprint": sha("next-authority"),
        }
        contract.CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION = (
            self.authorization
        )
        self.original_bytes = {
            "batch": self.store._batch_path("cs408").read_bytes(),
            "pointer": pointer_path.read_bytes(),
            "snapshot": (
                self.store.subject_batch_snapshot_root
                / "sha256"
                / self.authorization["snapshot_sha256"][:2]
                / f"{self.authorization['snapshot_sha256']}.json"
            ).read_bytes(),
            "terminal": (
                self.store.receipt_root
                / "subject-terminal"
                / "sha256"
                / terminal_sha[:2]
                / f"{terminal_sha}.json"
            ).read_bytes(),
        }

    def tearDown(self) -> None:
        contract.CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION = (
            self.original_authorization
        )
        self.temporary.cleanup()

    def observations(self) -> list[dict[str, object]]:
        return [
            {
                "schema_version": "subject_authority_snapshot_v1",
                "subject": "cs408",
                "generation": self.authorization["target_generation"],
                "authority_fingerprint": self.authorization[
                    "target_authority_fingerprint"
                ],
                "mcp_server_release": "fixture",
                "route_request_id": f"authority-{index}",
                "scope_sha256": sha(f"scope-{index}"),
                "model_call_count": 0,
                "formal_write_count": 0,
            }
            for index in (1, 2)
        ]

    def retire(self) -> dict[str, object]:
        a = self.authorization
        return self.store.retire_authorized_cs408_terminal_batch(
            expected_batch_sha256=a["batch_sha256"],
            expected_terminal_receipt_sha256=a[
                "terminal_receipt_sha256"
            ],
            expected_writer_preimage_sha256=a["writer_preimage_sha256"],
            expected_batch_pointer_sha256=a["batch_pointer_sha256"],
            expected_snapshot_sha256=a["snapshot_sha256"],
            expected_source_generation=a["source_generation"],
            expected_source_authority_fingerprint=a[
                "source_authority_fingerprint"
            ],
            expected_next_generation=a["target_generation"],
            expected_next_authority_fingerprint=a[
                "target_authority_fingerprint"
            ],
            authority_snapshots=self.observations(),
        )

    def assert_old_evidence_unchanged(self) -> None:
        a = self.authorization
        current = {
            "batch": self.store._batch_path("cs408").read_bytes(),
            "pointer": self.store._batch_pointer_path("cs408").read_bytes(),
            "snapshot": (
                self.store.subject_batch_snapshot_root
                / "sha256"
                / a["snapshot_sha256"][:2]
                / f"{a['snapshot_sha256']}.json"
            ).read_bytes(),
            "terminal": (
                self.store.receipt_root
                / "subject-terminal"
                / "sha256"
                / a["terminal_receipt_sha256"][:2]
                / f"{a['terminal_receipt_sha256']}.json"
            ).read_bytes(),
        }
        self.assertEqual(current, self.original_bytes)

    def test_retire_reopen_readiness_and_rollback_are_zero_replay(self) -> None:
        retired = self.retire()
        self.assertEqual(retired["status"], "retired")
        self.assertEqual(retired["authority_snapshot_mcp_tool_call_count"], 2)
        self.assertEqual(retired["model_mcp_tool_call_count"], 0)
        self.assertEqual(retired["capture_replay_count"], 0)
        self.assertEqual(retired["replacement_task_created_count"], 0)
        self.assertEqual(retired["queue_entry_created_count"], 0)
        retirement_receipt = json.loads(
            Path(str(retired["retirement_receipt_path"])).read_text()
        )
        retirement_schema = json.loads(
            (
                ROOT
                / "schemas/cs408-terminal-batch-writer-retirement-receipt-v1.json"
            ).read_text()
        )
        self.assertEqual(
            set(retirement_receipt), set(retirement_schema["required"])
        )
        retirement_pointer = json.loads(
            self.store.cs408_terminal_retirement_pointer_path.read_text()
        )
        pointer_schema = json.loads(
            (
                ROOT
                / "schemas/cs408-terminal-batch-writer-retirement-pointer-v1.json"
            ).read_text()
        )
        self.assertEqual(set(retirement_pointer), set(pointer_schema["required"]))
        self.assert_old_evidence_unchanged()
        readiness = self.store.canary_readiness(
            "cs408",
            next_generation=self.authorization["target_generation"],
            next_authority_fingerprint=self.authorization[
                "target_authority_fingerprint"
            ],
        )
        self.assertEqual(readiness["readiness"], "ready")
        self.assertEqual(readiness["reason"], "cs408_terminal_batch_retired")
        reopened = (
            self.store.reopen_authorized_cs408_terminal_batch_retirement(
                retirement_receipt_path=Path(
                    str(retired["retirement_receipt_path"])
                )
            )
        )
        self.assertEqual(reopened["mcp_tool_call_count"], 0)
        rolled_back = (
            self.store.rollback_authorized_cs408_terminal_batch_retirement(
                retirement_receipt_path=Path(
                    str(retired["retirement_receipt_path"])
                )
            )
        )
        self.assertTrue(rolled_back["rollback_receipt_reopened"])
        self.assertTrue(rolled_back["retirement_pointer_withdrawn"])
        self.assertEqual(
            hashlib.sha256(
                self.store._writer_path("cs408").read_bytes()
            ).hexdigest(),
            self.authorization["writer_preimage_sha256"],
        )
        self.assert_old_evidence_unchanged()
        rollback_receipt = json.loads(
            Path(str(rolled_back["rollback_receipt_path"])).read_text()
        )
        rollback_schema = json.loads(
            (
                ROOT
                / "schemas/cs408-terminal-batch-writer-retirement-rollback-receipt-v1.json"
            ).read_text()
        )
        self.assertEqual(set(rollback_receipt), set(rollback_schema["required"]))
        runtime = ProductionDispatchRuntime.__new__(ProductionDispatchRuntime)
        runtime.subject = "cs408"
        runtime.subject_sol = self.store
        rollback_path = Path(str(rolled_back["rollback_receipt_path"]))
        rollback_reopened = (
            runtime.reopen_cs408_terminal_batch_retirement_rollback(
                retirement_receipt=Path(
                    str(retired["retirement_receipt_path"])
                ),
                rollback_receipt=rollback_path,
            )
        )
        self.assertEqual(rollback_reopened["status"], "reopened")
        self.assertTrue(rollback_reopened["rollback_reopen_read_only"])
        self.assertEqual(rollback_reopened["mcp_tool_call_count"], 0)
        self.assert_old_evidence_unchanged()
        parsed = build_parser().parse_args(
            [
                "--subject",
                "cs408",
                "reopen-cs408-terminal-batch-retirement-rollback",
                "--retirement-receipt",
                str(retired["retirement_receipt_path"]),
                "--rollback-receipt",
                str(rollback_path),
            ]
        )
        self.assertEqual(
            parsed.command,
            "reopen-cs408-terminal-batch-retirement-rollback",
        )
        with self.assertRaisesRegex(
            Exception, "rollback_receipt_path_invalid"
        ):
            runtime.reopen_cs408_terminal_batch_retirement_rollback(
                retirement_receipt=Path(
                    str(retired["retirement_receipt_path"])
                ),
                rollback_receipt=Path(str(retired["retirement_receipt_path"])),
            )

    def test_authority_or_descriptor_drift_fails_before_mutation(self) -> None:
        before = self.store._writer_path("cs408").read_bytes()
        observations = self.observations()
        observations[1]["generation"] = "cs408-drifted-generation"
        with self.assertRaisesRegex(
            SubjectSolContractError,
            "cs408_terminal_retirement_authority_mismatch",
        ):
            a = self.authorization
            self.store.retire_authorized_cs408_terminal_batch(
                expected_batch_sha256=a["batch_sha256"],
                expected_terminal_receipt_sha256=a[
                    "terminal_receipt_sha256"
                ],
                expected_writer_preimage_sha256=a[
                    "writer_preimage_sha256"
                ],
                expected_batch_pointer_sha256=a["batch_pointer_sha256"],
                expected_snapshot_sha256=a["snapshot_sha256"],
                expected_source_generation=a["source_generation"],
                expected_source_authority_fingerprint=a[
                    "source_authority_fingerprint"
                ],
                expected_next_generation=a["target_generation"],
                expected_next_authority_fingerprint=a[
                    "target_authority_fingerprint"
                ],
                authority_snapshots=observations,
            )
        self.assertEqual(self.store._writer_path("cs408").read_bytes(), before)
        self.assertFalse(self.store.cs408_terminal_retirement_pointer_path.exists())

    def test_pointer_publication_failure_restores_writer_preimage(self) -> None:
        original_atomic = self.store._atomic_json

        def fail_pointer(path: Path, value: dict[str, object]) -> None:
            if path == self.store.cs408_terminal_retirement_pointer_path:
                raise OSError("injected-pointer-write-failure")
            original_atomic(path, value)

        with mock.patch.object(self.store, "_atomic_json", side_effect=fail_pointer):
            with self.assertRaisesRegex(OSError, "injected-pointer"):
                self.retire()
        self.assertEqual(
            hashlib.sha256(
                self.store._writer_path("cs408").read_bytes()
            ).hexdigest(),
            self.authorization["writer_preimage_sha256"],
        )
        self.assertFalse(self.store.cs408_terminal_retirement_pointer_path.exists())
        self.assertFalse(self.store.cs408_terminal_retirement_intent_path.exists())
        self.assert_old_evidence_unchanged()

    def test_new_generation_batch_replaces_old_without_old_task_replay(self) -> None:
        self.retire()
        new = self.store.prepare_and_freeze_subject_batch(
            subject="cs408",
            batch_id="LUNA-CS408-NEW-CAPTURE",
            study_date="2026-08-13",
            capture_high_watermark="CAP-CS408-NEW",
            scan_snapshot_sha256=sha("new-scan"),
            authority_generation=self.authorization["target_generation"],
            authority_fingerprint=self.authorization[
                "target_authority_fingerprint"
            ],
            tasks=[
                {
                    "capture_id": "CAP-CS408-NEW",
                    "unit_sha256": sha("new-unit"),
                    "input_fingerprint": sha("new-input"),
                    "study_date": "2026-08-13",
                    "frozen_payload_sha256": sha("new-payload"),
                }
            ],
        )
        self.assertEqual([row["capture_id"] for row in new["tasks"]], ["CAP-CS408-NEW"])
        self.assertNotIn("CAP-CS408-OLD", str(new))
        readiness = self.store.canary_readiness(
            "cs408",
            next_generation=self.authorization["target_generation"],
            next_authority_fingerprint=self.authorization[
                "target_authority_fingerprint"
            ],
        )
        self.assertNotEqual(
            readiness["reason"], "cs408_terminal_retirement_postimage_invalid"
        )

    def test_source_descriptor_constant_is_exactly_scoped(self) -> None:
        self.assertEqual(
            set(self.original_authorization),
            {
                "subject",
                "mode",
                "batch_id",
                "batch_sha256",
                "terminal_receipt_sha256",
                "writer_preimage_sha256",
                "batch_pointer_sha256",
                "snapshot_sha256",
                "source_generation",
                "source_authority_fingerprint",
                "target_generation",
                "target_authority_fingerprint",
            },
        )
        self.assertEqual(
            self.original_authorization["mode"], "archive_only_zero_replay"
        )

    def test_cli_runtime_uses_two_authority_reads_and_read_only_reopen(self) -> None:
        a = self.authorization
        parsed = build_parser().parse_args(
            [
                "--config",
                "/tmp/config.json",
                "--subject",
                "cs408",
                "retire-cs408-terminal-batch",
                "--expected-batch-sha256",
                a["batch_sha256"],
                "--expected-terminal-receipt-sha256",
                a["terminal_receipt_sha256"],
                "--expected-writer-preimage-sha256",
                a["writer_preimage_sha256"],
                "--expected-batch-pointer-sha256",
                a["batch_pointer_sha256"],
                "--expected-snapshot-sha256",
                a["snapshot_sha256"],
                "--expected-source-generation",
                a["source_generation"],
                "--expected-source-authority-fingerprint",
                a["source_authority_fingerprint"],
                "--expected-next-generation",
                a["target_generation"],
                "--expected-next-authority-fingerprint",
                a["target_authority_fingerprint"],
            ]
        )
        self.assertEqual(parsed.command, "retire-cs408-terminal-batch")

        class AuthorityHost:
            calls = 0

            def subject_authority_snapshot(inner_self, subject: str):
                inner_self.calls += 1
                value = self.observations()[0]
                return {**value, "subject": subject}

        host = AuthorityHost()
        runtime = ProductionDispatchRuntime.__new__(ProductionDispatchRuntime)
        runtime.subject = "cs408"
        runtime.subject_sol = self.store
        runtime.processing_host = host
        retired = runtime.retire_cs408_terminal_batch(
            expected_batch_sha256=a["batch_sha256"],
            expected_terminal_receipt_sha256=a[
                "terminal_receipt_sha256"
            ],
            expected_writer_preimage_sha256=a["writer_preimage_sha256"],
            expected_batch_pointer_sha256=a["batch_pointer_sha256"],
            expected_snapshot_sha256=a["snapshot_sha256"],
            expected_source_generation=a["source_generation"],
            expected_source_authority_fingerprint=a[
                "source_authority_fingerprint"
            ],
            expected_next_generation=a["target_generation"],
            expected_next_authority_fingerprint=a[
                "target_authority_fingerprint"
            ],
        )
        self.assertEqual(host.calls, 2)
        self.assertEqual(retired["schema_version"], "study-intake-cs408-terminal-batch-retirement-result-v1")
        reopened = runtime.reopen_cs408_terminal_batch_retirement(
            retirement_receipt=None,
            expected_terminal_receipt_sha256=a[
                "terminal_receipt_sha256"
            ],
        )
        self.assertEqual(host.calls, 2)
        self.assertEqual(reopened["status"], "reopened")
        self.assertTrue(reopened["reopen_read_only"])
        self.assertEqual(reopened["mcp_tool_call_count"], 0)


if __name__ == "__main__":
    unittest.main()
