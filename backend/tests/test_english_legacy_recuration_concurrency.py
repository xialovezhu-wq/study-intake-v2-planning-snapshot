from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import hashlib
import os
from pathlib import Path
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "scripts"))

from english_legacy_recuration import (  # noqa: E402
    EnglishLegacyRecurationError,
    STAGE_EVENT_ORDER,
    critical_review_needs_rework_reason,
    publish_needs_rework_terminal,
    publish_run_summary,
    verify_needs_rework_receipt,
    verify_run_summary,
    verify_smoke_run_summary,
)
from run_english_legacy_recuration import (  # noqa: E402
    _ConcurrentRunState,
    _run_selected,
    _workers_for_mode,
)


def _items(count: int) -> list[dict[str, object]]:
    return [
        {"ordinal": ordinal, "target_id": f"target-{ordinal:03d}"}
        for ordinal in range(1, count + 1)
    ]


def _fake_runner(
    _config: object,
    _runtime_root: Path,
    item: dict[str, object],
    *,
    stage_observer: object,
) -> dict[str, object]:
    observer = stage_observer
    for event in STAGE_EVENT_ORDER:
        observer(event)  # type: ignore[operator]
    ordinal = int(item["ordinal"])
    return {
        "status": "succeeded",
        "package_sha256": hashlib.sha256(f"package-{ordinal}".encode()).hexdigest(),
        "quality_receipt_sha256": hashlib.sha256(
            f"quality-{ordinal}".encode()
        ).hexdigest(),
        "model_call_count": 2,
        "formal_write_count": 0,
    }


class EnglishLegacyConcurrencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="english-concurrency-")
        self.runtime_root = Path(self.temporary.name) / "runtime"
        self.runtime_root.mkdir()
        self.key = Path(self.temporary.name) / "authority.key"
        self.key.write_bytes(b"k" * 32)
        os.chmod(self.key, 0o600)
        self.config = {
            "processing_plugin": {"authority_key_path": str(self.key)}
        }
        self.scope = {
            "authorization_expansion_closure_sha256": "1" * 64,
            "batch_authorization_sha256": "2" * 64,
            "inventory_sha256": "3" * 64,
            "target_set_sha256": "4" * 64,
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _publish(
        self,
        *,
        mode: str,
        count: int,
        attempt: int,
        batch_sha: str,
    ) -> tuple[str, dict[str, object]]:
        selected = _items(count)
        rows, proof = _run_selected(
            self.config,
            self.runtime_root,
            selected,
            count,
            run_mode=mode,
            barrier_timeout_seconds=5,
            smoke_prerequisites={
                "smoke_1_summary_sha256": None,
                "smoke_10_summary_sha256": None,
            },
            item_runner=_fake_runner,
        )
        started = dt.datetime.now(dt.timezone.utc).isoformat()
        completed = dt.datetime.now(dt.timezone.utc).isoformat()
        digest, _path, summary = publish_run_summary(
            self.config,
            self.runtime_root,
            work_item_batch_sha256=batch_sha,
            attempt=attempt,
            batch_target_count=98,
            selected_ordinals=list(range(1, count + 1)),
            results=rows,
            concurrency_attestation=proof,
            started_at=started,
            completed_at=completed,
            **self.scope,
        )
        return digest, summary

    def test_controlled_runner_proves_actual_peak_98(self) -> None:
        selected = _items(98)
        rows, proof = _run_selected(
            self.config,
            self.runtime_root,
            selected,
            98,
            run_mode="full_98",
            barrier_timeout_seconds=5,
            smoke_prerequisites={
                "smoke_1_summary_sha256": "a" * 64,
                "smoke_10_summary_sha256": "b" * 64,
            },
            item_runner=_fake_runner,
        )
        self.assertEqual(98, len(rows))
        self.assertEqual(98, proof["barrier_expected"])
        self.assertEqual(98, proof["barrier_arrived"])
        self.assertIsNotNone(proof["barrier_released_at"])
        self.assertEqual(98, proof["peak_active"])
        self.assertEqual(98, proof["strict_stage_order_count"])
        self.assertEqual(98, proof["exact_two_call_count"])
        self.assertEqual("passed", proof["gate_status"])
        self.assertGreaterEqual(proof["submitted_at_spread_ms"], 0)
        for trace in proof["item_traces"]:
            self.assertEqual(
                list(STAGE_EVENT_ORDER),
                [event["name"] for event in trace["stage_events"]],
            )

    def test_barrier_timeout_publishes_reopenable_failed_closed_summary(self) -> None:
        selected = _items(10)
        state = _ConcurrentRunState(selected, timeout_seconds=0.05)

        def arrive(item: dict[str, object]) -> None:
            try:
                state.enter(item)
            except EnglishLegacyRecurationError:
                pass
            finally:
                state.leave()

        with ThreadPoolExecutor(max_workers=9) as executor:
            list(executor.map(arrive, selected[:9]))
        rows = [
            {
                "ordinal": item["ordinal"],
                "target_id": item["target_id"],
                "status": "failed",
                "error_code": "english_legacy_start_barrier_failed",
                "package_sha256": None,
                "quality_receipt_sha256": None,
                "failure_receipt_sha256": None,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
            for item in selected
        ]
        proof = state.attestation(
            run_mode="smoke_10",
            max_workers=10,
            rows=rows,
            smoke_prerequisites={
                "smoke_1_summary_sha256": None,
                "smoke_10_summary_sha256": None,
            },
        )
        started = dt.datetime.now(dt.timezone.utc).isoformat()
        digest, _path, summary = publish_run_summary(
            self.config,
            self.runtime_root,
            work_item_batch_sha256="5" * 64,
            attempt=1,
            batch_target_count=98,
            selected_ordinals=list(range(1, 11)),
            results=rows,
            concurrency_attestation=proof,
            started_at=started,
            completed_at=dt.datetime.now(dt.timezone.utc).isoformat(),
            **self.scope,
        )
        self.assertEqual("failed_closed", summary["status"])
        self.assertEqual(0, summary["model_call_count"])
        reopened = verify_run_summary(self.config, self.runtime_root, digest)
        self.assertEqual(summary, reopened)
        self.assertEqual(9, reopened["concurrency_attestation"]["barrier_arrived"])
        self.assertIsNone(reopened["concurrency_attestation"]["barrier_released_at"])

    def test_full_smoke_prerequisites_reopen_hmac_and_require_prior_attempt(self) -> None:
        smoke_1_sha, smoke_1 = self._publish(
            mode="smoke_1", count=1, attempt=1, batch_sha="6" * 64
        )
        smoke_10_sha, smoke_10 = self._publish(
            mode="smoke_10", count=10, attempt=2, batch_sha="7" * 64
        )
        self.assertEqual("passed", smoke_1["status"])
        self.assertEqual("passed", smoke_10["status"])
        self.assertEqual(
            smoke_1,
            verify_smoke_run_summary(
                self.config,
                self.runtime_root,
                smoke_1_sha,
                expected_mode="smoke_1",
                scope=self.scope,
                full_attempt=3,
            ),
        )
        self.assertEqual(
            smoke_10,
            verify_smoke_run_summary(
                self.config,
                self.runtime_root,
                smoke_10_sha,
                expected_mode="smoke_10",
                scope=self.scope,
                full_attempt=3,
            ),
        )
        with self.assertRaisesRegex(
            EnglishLegacyRecurationError,
            "english_legacy_smoke_prerequisite_invalid",
        ):
            verify_smoke_run_summary(
                self.config,
                self.runtime_root,
                smoke_10_sha,
                expected_mode="smoke_10",
                scope=self.scope,
                full_attempt=2,
            )

    def test_full_98_workers_must_be_explicit(self) -> None:
        with self.assertRaisesRegex(
            EnglishLegacyRecurationError,
            "english_legacy_full_requires_explicit_max_workers_98",
        ):
            _workers_for_mode("full_98", None)
        self.assertEqual(98, _workers_for_mode("full_98", 98))

    def test_non_sol_ready_critical_results_need_rework(self) -> None:
        self.assertEqual(
            "critical_review_rejected",
            critical_review_needs_rework_reason(
                {
                    "verdict": "rejected",
                    "revised_proposal": {"action": "conflict"},
                }
            ),
        )
        self.assertEqual(
            "proposal_not_sol_ready",
            critical_review_needs_rework_reason(
                {
                    "verdict": "accepted",
                    "revised_proposal": {"action": "evidence_incomplete"},
                }
            ),
        )
        self.assertIsNone(
            critical_review_needs_rework_reason(
                {
                    "verdict": "corrected",
                    "revised_proposal": {"action": "update_existing_proposal"},
                }
            )
        )

    def test_needs_rework_terminal_is_signed_and_has_no_sol_package(self) -> None:
        work_item = {
            "target_id": "target-001",
            "target_kind": "master_bank_row",
            "ordinal": 1,
            "attempt": 1,
            "inventory_sha256": "3" * 64,
            "batch_authorization_sha256": "2" * 64,
            "target_authorization_receipt_sha256": "9" * 64,
            "authority": {
                "generation": "english-generation-1",
                "authority_fingerprint": "a" * 64,
            },
        }
        stage = {
            "output_sha256": "b" * 64,
            "transcript_sha256": "c" * 64,
            "call_receipt_sha256": "d" * 64,
            "provider_request_count": 2,
            "mcp_tool_call_count": 1,
        }
        digest, path, receipt = publish_needs_rework_terminal(
            self.config,
            self.runtime_root,
            work_item=work_item,
            work_item_sha256="8" * 64,
            analysis_stage=stage,
            critical_stage=stage,
            stage_receipts={"analysis": {}, "critical_review": {}, "read_session": {}},
            processing_publication={"subject": "english"},
            final_read_session_receipt_sha256="e" * 64,
            proposal={"action": "conflict", "evidence_refs": []},
            quality_outcome="rejected",
            reason_code="critical_review_rejected",
            issued_at="2026-08-09T16:00:00+08:00",
        )
        self.assertTrue(path.is_file())
        self.assertEqual("needs_rework", receipt["terminal_status"])
        self.assertEqual(
            receipt,
            verify_needs_rework_receipt(
                self.config, self.runtime_root, digest
            ),
        )
        base = self.runtime_root / "dispatch" / "english-legacy-recuration"
        self.assertFalse((base / "packages").exists())
        self.assertFalse((base / "quality-receipts").exists())
        self.assertTrue((base / "needs-rework-bindings" / f"{'8' * 64}.json").is_file())


if __name__ == "__main__":
    unittest.main()
