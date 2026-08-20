#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

import concurrent_dispatch as dispatch_module  # noqa: E402
from concurrent_dispatch import (  # noqa: E402
    DispatchError,
    FrozenTask,
    Lease,
    LeaseStore,
)
from preprocess_dispatcher import build_parser  # noqa: E402
from tests.test_production_canary_admission import (  # noqa: E402
    authority,
    canary_task,
)


STALE_AT = "2026-08-11T00:00:00Z"
DRAIN_AT = "2026-08-11T01:00:00Z"
OBSERVED_AT = "2026-08-11T02:00:00Z"


def task_for(index: int, *, subject: str = "cs408") -> FrozenTask:
    return FrozenTask(
        {
            "subject": subject,
            "capture_id": f"QUARANTINE-{index:04d}",
            "study_date": "2026-08-11",
            "input_fingerprint": f"quarantine-{index:04d}",
            "input_binding": {"index": index},
            "model_input": {"question": f"question-{index}"},
            "allowed_evidence_refs": [f"capture:{index}"],
            "image_paths": [],
        }
    )


class StaleClaimQuarantineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / "runtime"
        self.store = LeaseStore(self.runtime)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _claimed(
        self,
        index: int,
        *,
        owner_pid: int = 2147483000,
        drained: bool = True,
    ) -> tuple[FrozenTask, Lease]:
        task = task_for(index)
        owner_id = f"dispatcher-{owner_pid}-{'a' * 32}"
        with mock.patch.object(
            dispatch_module, "_utc_now", return_value=STALE_AT
        ):
            decision = self.store.claim(
                task.unit_sha256,
                owner_id,
                subject="cs408",
                now=STALE_AT,
            )
            assert decision.lease is not None
            self.store.record_task_event(task, decision.lease, "claim")
        if drained:
            with mock.patch.object(
                dispatch_module, "_utc_now", return_value=DRAIN_AT
            ):
                self.store.begin_subject_drain("cs408")
        return task, decision.lease

    def _snapshot(self) -> dict[str, bytes]:
        return {
            str(path.relative_to(self.runtime)): path.read_bytes()
            for path in sorted(self.runtime.rglob("*"))
            if path.is_file()
        }

    def test_cli_defaults_to_preview_and_requires_exact_unit(self) -> None:
        parser = build_parser()
        base = [
            "--config",
            "/tmp/config.json",
            "--subject",
            "cs408",
            "quarantine-stale-claim",
            "--unit-sha256",
            "a" * 64,
        ]
        preview = parser.parse_args(base)
        applied = parser.parse_args([*base, "--apply"])
        self.assertFalse(preview.apply)
        self.assertTrue(applied.apply)
        self.assertEqual(preview.unit_sha256, "a" * 64)

    def test_undrained_and_live_owner_are_rejected(self) -> None:
        undrained_task, _ = self._claimed(1, drained=False)
        with self.assertRaisesRegex(
            DispatchError, "stale_claim_quarantine_requires_subject_drain"
        ):
            self.store.quarantine_stale_claim(
                "cs408", undrained_task.unit_sha256, now=OBSERVED_AT
            )

        live_task, _ = self._claimed(2, owner_pid=os.getpid())
        with self.assertRaisesRegex(
            DispatchError, "stale_claim_quarantine_owner_alive"
        ):
            self.store.quarantine_stale_claim(
                "cs408", live_task.unit_sha256, now=OBSERVED_AT
            )

    def test_preview_is_read_only(self) -> None:
        task, lease = self._claimed(3)
        before = self._snapshot()
        with mock.patch.object(
            dispatch_module.os, "kill", side_effect=ProcessLookupError
        ):
            preview = self.store.quarantine_stale_claim(
                "cs408", task.unit_sha256, now=OBSERVED_AT
            )
        self.assertEqual(preview["mode"], "preview")
        self.assertEqual(preview["status"], "eligible")
        self.assertFalse(preview["applied"])
        self.assertEqual(preview["lease_fence_before"], lease.fence)
        self.assertEqual(preview["lease_fence_after"], lease.fence + 1)
        self.assertIsNone(preview["receipt_sha256"])
        self.assertEqual(self._snapshot(), before)

    def test_apply_reopens_receipt_and_closes_late_fence(self) -> None:
        task, lease = self._claimed(4)
        with mock.patch.object(
            dispatch_module.os, "kill", side_effect=ProcessLookupError
        ):
            applied = self.store.quarantine_stale_claim(
                "cs408",
                task.unit_sha256,
                apply=True,
                now=OBSERVED_AT,
            )
        self.assertEqual(applied["status"], "quarantined")
        self.assertTrue(applied["applied"])
        receipt_path = Path(applied["receipt_path"])
        receipt_raw = receipt_path.read_bytes()
        self.assertEqual(
            hashlib.sha256(receipt_raw).hexdigest(),
            applied["receipt_sha256"],
        )
        receipt = json.loads(receipt_raw)
        self.store._verify_seal(
            receipt, purpose="dispatch-stale-claim-quarantine"
        )
        schema = json.loads(
            (ROOT / "schemas/stale-claim-quarantine-receipt-v1.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(set(receipt), set(schema["required"]))
        self.assertEqual(set(receipt), set(schema["properties"]))
        for key, rule in schema["properties"].items():
            if "const" in rule:
                self.assertEqual(receipt[key], rule["const"])
        authority_schema = schema["$defs"]["authority"]
        self.assertEqual(
            set(receipt["authority"]), set(authority_schema["required"])
        )
        for key, rule in authority_schema["properties"].items():
            if "const" in rule:
                self.assertEqual(receipt["authority"][key], rule["const"])
        self.assertEqual(receipt["unit_sha256"], task.unit_sha256)
        self.assertEqual(receipt["lease_fence_before"], lease.fence)
        self.assertEqual(receipt["lease_fence_after"], lease.fence + 1)
        self.assertEqual(receipt["model_call_count"], 0)
        self.assertEqual(receipt["provider_request_count"], 0)
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertFalse(receipt["sol_enabled"])

        lease_value = json.loads(
            self.store._lease_path(task.unit_sha256).read_text(encoding="utf-8")
        )
        self.assertEqual(lease_value["status"], "quarantined")
        self.assertEqual(lease_value["fence"], lease.fence + 1)
        self.assertEqual(
            lease_value["quarantine_receipt_sha256"],
            applied["receipt_sha256"],
        )
        self.assertEqual(self.store.subject_status("cs408")["claimed_total"], 0)
        self.assertFalse(self.store.is_current(lease))
        with self.assertRaisesRegex(DispatchError, "stale_lease_fence"):
            self.store.record_task_event(task, lease, "failed")
        with self.assertRaisesRegex(DispatchError, "stale_lease_fence"):
            self.store.publish_terminal(
                lease,
                task=task,
                outcome="failed",
                error_code="late_stale_owner",
                analysis=None,
                critical_review=None,
                started_at=STALE_AT,
                finished_at=OBSERVED_AT,
            )

    def test_apply_is_idempotent_and_receipt_first_crash_is_retryable(self) -> None:
        task, _ = self._claimed(5)
        lease_path = self.store._lease_path(task.unit_sha256)
        original_atomic_replace = dispatch_module._atomic_replace_json
        crashed = False

        def crash_before_state(path: Path, value: dict[str, object]) -> None:
            nonlocal crashed
            if (
                not crashed
                and path == lease_path
                and value.get("status") == "quarantined"
            ):
                crashed = True
                raise OSError("synthetic crash after receipt")
            original_atomic_replace(path, value)

        with mock.patch.object(
            dispatch_module.os, "kill", side_effect=ProcessLookupError
        ), mock.patch.object(
            dispatch_module,
            "_atomic_replace_json",
            side_effect=crash_before_state,
        ), self.assertRaisesRegex(OSError, "synthetic crash after receipt"):
            self.store.quarantine_stale_claim(
                "cs408",
                task.unit_sha256,
                apply=True,
                now=OBSERVED_AT,
            )
        orphan_receipts = list(
            self.store.stale_claim_quarantine_receipt_root.rglob("*.json")
        )
        self.assertEqual(len(orphan_receipts), 1)
        orphan_sha256 = hashlib.sha256(orphan_receipts[0].read_bytes()).hexdigest()
        self.assertEqual(json.loads(lease_path.read_text())["status"], "claimed")

        with mock.patch.object(
            dispatch_module.os, "kill", side_effect=ProcessLookupError
        ):
            recovered = self.store.quarantine_stale_claim(
                "cs408",
                task.unit_sha256,
                apply=True,
                now=OBSERVED_AT,
            )
        self.assertEqual(recovered["receipt_sha256"], orphan_sha256)
        receipt_before = Path(recovered["receipt_path"]).read_bytes()
        repeated = self.store.quarantine_stale_claim(
            "cs408",
            task.unit_sha256,
            apply=True,
            now=OBSERVED_AT,
        )
        self.assertEqual(repeated["status"], "already_quarantined")
        self.assertEqual(repeated["receipt_sha256"], orphan_sha256)
        self.assertEqual(Path(repeated["receipt_path"]).read_bytes(), receipt_before)

    def test_production_canary_state_rejects_lease_only_quarantine(self) -> None:
        with mock.patch.object(
            dispatch_module, "_utc_now", return_value=STALE_AT
        ):
            self.store.begin_subject_drain("cs408")
        self.store.activate_production_canary(
            "cs408",
            release_id="a" * 64,
            producer_authority=authority("cs408", "a" * 64),
            activated_at="2026-08-11T00:30:00Z",
        )
        task = canary_task(
            6,
            subject="cs408",
            recorded_at="2026-08-11T00:31:00Z",
        )
        self.store.materialize_production_canary_task(task)
        owner_id = f"dispatcher-2147483000-{'a' * 32}"
        decision = self.store.claim(
            task.unit_sha256,
            owner_id,
            subject="cs408",
            task=task,
            production_canary=True,
            now="2026-08-11T00:31:00Z",
        )
        assert decision.lease is not None
        with mock.patch.object(
            dispatch_module, "_utc_now", return_value="2026-08-11T00:31:00Z"
        ):
            self.store.record_task_event(task, decision.lease, "claim")
        before = self._snapshot()
        with mock.patch.object(
            dispatch_module.os, "kill", side_effect=ProcessLookupError
        ), self.assertRaisesRegex(
            DispatchError,
            "stale_claim_quarantine_production_canary_state_present",
        ):
            self.store.quarantine_stale_claim(
                "cs408", task.unit_sha256, apply=True, now=OBSERVED_AT
            )
        self.assertEqual(self._snapshot(), before)

    def test_owner_reappearing_after_receipt_keeps_claim_and_reuses_receipt(
        self,
    ) -> None:
        task, lease = self._claimed(7)
        with mock.patch.object(
            self.store,
            "_owner_process_exists",
            side_effect=[False, True],
        ), self.assertRaisesRegex(
            DispatchError, "stale_claim_quarantine_owner_reappeared"
        ):
            self.store.quarantine_stale_claim(
                "cs408", task.unit_sha256, apply=True, now=OBSERVED_AT
            )
        receipt_paths = list(
            self.store.stale_claim_quarantine_receipt_root.rglob("*.json")
        )
        self.assertEqual(len(receipt_paths), 1)
        receipt_sha256 = hashlib.sha256(
            receipt_paths[0].read_bytes()
        ).hexdigest()
        lease_after_race = json.loads(
            self.store._lease_path(task.unit_sha256).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(lease_after_race["status"], "claimed")
        self.assertEqual(lease_after_race["fence"], lease.fence)
        with mock.patch.object(
            self.store,
            "_owner_process_exists",
            side_effect=[False, False],
        ):
            recovered = self.store.quarantine_stale_claim(
                "cs408", task.unit_sha256, apply=True, now=OBSERVED_AT
            )
        self.assertEqual(recovered["status"], "quarantined")
        self.assertEqual(recovered["receipt_sha256"], receipt_sha256)
        self.assertFalse(self.store.is_current(lease))


if __name__ == "__main__":
    unittest.main()
