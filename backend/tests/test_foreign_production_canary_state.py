from __future__ import annotations

import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

from concurrent_dispatch import (  # noqa: E402
    DispatchError,
    LeaseStore,
    REQUIRED_MODEL,
    REQUIRED_REASONING_EFFORT,
)
from preprocessor_core import RELEASE_SCHEMA  # noqa: E402
from preprocess_dispatcher import _audit  # noqa: E402


OLD_RELEASE_ID = "a" * 64
TARGET_RELEASE_ID = "d" * 64
OLD_ACTIVATION_ID = "b" * 64
V2_STATE_SCHEMA = "study-intake-production-canary-state-v2"
V3_STATE_SCHEMA = "study-intake-production-canary-state-v3"


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _producer_authority(subject: str, release_id: str) -> dict[str, object]:
    core: dict[str, object] = {
        "schema_version": "study-intake-producer-authority-v1",
        "subject": subject,
        "release_id": release_id,
        "loaded_core_sha256": "1" * 64,
        "processing_contract_sha256": "2" * 64,
        "model": REQUIRED_MODEL,
        "reasoning_effort": REQUIRED_REASONING_EFFORT,
        "formal_write_count": 0,
    }
    return {
        **core,
        "authority_fingerprint": hashlib.sha256(_canonical(core)).hexdigest(),
    }


class ForeignProductionCanaryStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temporary.name) / "runtime"
        self.store = LeaseStore(self.runtime)
        self.store.state_root.mkdir(parents=True, exist_ok=True)
        self.store.lock_path.touch()
        self.manifest = Path(self.temporary.name) / "target-release.json"
        self.manifest.write_text(
            json.dumps(
                {
                    "schema_version": RELEASE_SCHEMA,
                    "release_id": TARGET_RELEASE_ID,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        self.config = {
            "runtime_root": str(self.runtime),
            "release": {"manifest_path": str(self.manifest)},
            "dispatch": {
                "production_canary": {
                    "enabled": True,
                    "status": "production_canary_active",
                    "admission": "first_post_activation_producer_capture",
                    "keep_backlog_drained": True,
                    "post_activation_only": True,
                    "initial_canary_inflight_limit": 1,
                    "continuous_concurrency_limit": 20,
                }
            },
        }

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _state_core(
        self,
        subject: str,
        *,
        schema_version: str = V2_STATE_SCHEMA,
        release_id: object = OLD_RELEASE_ID,
        activation_id: object = OLD_ACTIVATION_ID,
        state: str = "armed",
        status: str = "production_canary_active",
        luna_consumer_enabled: bool = True,
        active_task_count: int = 0,
        selected: object = None,
        active_selections: object = None,
        terminal_task_count: int = 0,
        raw_subject: object | None = None,
        formal_write_count: int = 0,
        sol_enabled: bool = False,
    ) -> dict[str, object]:
        terminal_counts = {
            "succeeded": 0,
            "failed": terminal_task_count,
            "cancelled": 0,
            "timed_out": 0,
            "stalled": 0,
            "needs_rework": 0,
        }
        return {
            "schema_version": schema_version,
            "subject": subject if raw_subject is None else raw_subject,
            "release_id": release_id,
            "activation_id": activation_id,
            "state": state,
            "status": status,
            "luna_consumer_enabled": luna_consumer_enabled,
            "active_task_count": active_task_count,
            "selected": selected,
            "active_selections": (
                {} if active_selections is None else active_selections
            ),
            "terminal_task_count": terminal_task_count,
            "terminal_by_outcome": terminal_counts,
            "terminal_index_path": str(
                self.runtime
                / "dispatch"
                / "production-canary"
                / "terminal-indexes"
                / subject
                / str(activation_id)
                / "foreign-v2-index.json"
            ),
            "terminal_index_sha256": "c" * 64,
            "formal_write_count": formal_write_count,
            "sol_enabled": sol_enabled,
        }

    def _write_signed_state(
        self, subject: str, core: dict[str, object]
    ) -> dict[str, object]:
        sealed = self.store._seal(
            core, purpose="dispatch-production-canary-state"
        )
        path = self.store._production_canary_state_path(subject)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(_canonical(sealed) + b"\n")
        return sealed

    def _tree_bytes(self) -> dict[str, bytes]:
        if not self.runtime.exists():
            return {}
        return {
            str(path.relative_to(self.runtime)): path.read_bytes()
            for path in sorted(self.runtime.rglob("*"))
            if path.is_file()
        }

    @staticmethod
    def _scan_result(subject: str):
        unit_sha256 = hashlib.sha256(subject.encode("utf-8")).hexdigest()
        task = SimpleNamespace(
            unit_sha256=unit_sha256,
            frozen_payload={
                "capture_id": f"FOREIGN-{subject}",
                "input_fingerprint": "f" * 64,
            },
        )
        return (
            [SimpleNamespace(task=task)],
            [
                {
                    "unit_sha256": unit_sha256,
                    "producer_eligible": True,
                    "eligible": True,
                    "reason": "eligible",
                    "phase": "producer_scan",
                    "model_enqueue_allowed": True,
                }
            ],
        )

    def test_foreign_v2_cs408_and_english_audit_is_read_only(self) -> None:
        fixtures = (("cs408", 0), ("english", 1))
        for subject, terminal_count in fixtures:
            self._write_signed_state(
                subject,
                self._state_core(
                    subject,
                    state=("armed" if subject == "cs408" else "failed_drained"),
                    status="production_canary_active",
                    luna_consumer_enabled=(subject == "cs408"),
                    terminal_task_count=terminal_count,
                ),
            )
        before = self._tree_bytes()

        def scan(_config, subject, **_kwargs):
            return self._scan_result(subject)

        with (
            mock.patch(
                "preprocess_dispatcher.scan_eligible_candidates",
                side_effect=scan,
            ),
            mock.patch.object(
                LeaseStore,
                "_read_canary_terminal_index_locked",
                side_effect=AssertionError("foreign index must not be opened"),
            ),
            mock.patch.object(
                LeaseStore,
                "production_canary_status_read_only",
                side_effect=AssertionError("foreign v3 reader must not be called"),
            ),
        ):
            audits = {
                subject: _audit(self.config, subject)
                for subject, _terminal_count in fixtures
            }

        self.assertEqual(self._tree_bytes(), before)
        for subject, _terminal_count in fixtures:
            audit = audits[subject]
            self.assertTrue(audit["read_only"])
            self.assertIsNone(audit["canary_gate"])
            self.assertEqual(audit["eligible_count"], 1)
            self.assertEqual(audit["historical_eligible_count"], 1)
            self.assertEqual(audit["excluded_by_high_watermark_count"], 1)
            self.assertEqual(audit["canary_queue_count"], 0)
            self.assertEqual(
                {row["phase"] for row in audit["decisions"]},
                {"target_release_preflight_read_only"},
            )
            self.assertEqual(audit["model_call_count"], 0)
            self.assertEqual(audit["provider_request_count"], 0)
            self.assertEqual(audit["formal_write_count"], 0)

    def test_identity_rejects_untrusted_or_nonforeign_legacy_state(self) -> None:
        subject = "cs408"
        invalid_cases = {
            "same_release_v2": self._state_core(
                subject, release_id=TARGET_RELEASE_ID
            ),
            "unknown_schema": self._state_core(
                subject,
                schema_version="study-intake-production-canary-state-v9",
            ),
            "wrong_subject": self._state_core(subject, raw_subject="english"),
            "malformed_release": self._state_core(subject, release_id="bad"),
            "malformed_activation": self._state_core(
                subject, activation_id="bad"
            ),
            "formal_write_nonzero": self._state_core(
                subject, formal_write_count=1
            ),
            "sol_enabled": self._state_core(subject, sol_enabled=True),
        }
        for name, core in invalid_cases.items():
            with self.subTest(name=name):
                self._write_signed_state(subject, core)
                with self.assertRaises(DispatchError) as raised:
                    self.store.production_canary_identity_read_only(
                        subject, configured_release_id=TARGET_RELEASE_ID
                    )
                self.assertEqual(
                    raised.exception.code, "production_canary_state_invalid"
                )

        sealed = self._write_signed_state(
            subject,
            self._state_core(
                subject,
                schema_version="study-intake-production-canary-state-v9",
            ),
        )
        tampered = copy.deepcopy(sealed)
        tampered["release_id"] = "e" * 64
        self.store._production_canary_state_path(subject).write_bytes(
            _canonical(tampered) + b"\n"
        )
        with self.assertRaises(DispatchError) as raised:
            self.store.production_canary_identity_read_only(
                subject, configured_release_id=TARGET_RELEASE_ID
            )
        self.assertEqual(raised.exception.code, "authority_hmac_invalid")

    def test_activation_replaces_only_signed_foreign_v2_inactive_pointer(self) -> None:
        subject = "english"
        self.store.begin_subject_drain(subject)
        old_receipt = (
            self.runtime
            / "dispatch"
            / "production-canary"
            / "receipts"
            / subject
            / OLD_ACTIVATION_ID
            / "old-receipt.json"
        )
        old_index = (
            self.runtime
            / "dispatch"
            / "production-canary"
            / "terminal-indexes"
            / subject
            / OLD_ACTIVATION_ID
            / "old-index.json"
        )
        old_receipt.parent.mkdir(parents=True, exist_ok=True)
        old_index.parent.mkdir(parents=True, exist_ok=True)
        old_receipt.write_bytes(b"old-receipt-bytes\n")
        old_index.write_bytes(b"old-index-bytes\n")
        self._write_signed_state(
            subject,
            self._state_core(
                subject,
                state="inactive_rolled_back",
                status="production_canary_inactive",
                luna_consumer_enabled=False,
                terminal_task_count=1,
            ),
        )

        activated = self.store.activate_production_canary(
            subject,
            release_id=TARGET_RELEASE_ID,
            producer_authority=_producer_authority(
                subject, TARGET_RELEASE_ID
            ),
            activated_at="2026-08-12T10:00:00Z",
            continuous_concurrency_limit=20,
        )

        self.assertEqual(activated["schema_version"], V3_STATE_SCHEMA)
        self.assertEqual(activated["release_id"], TARGET_RELEASE_ID)
        self.assertEqual(activated["state"], "armed")
        self.assertNotEqual(activated["activation_id"], OLD_ACTIVATION_ID)
        self.assertEqual(activated["formal_write_count"], 0)
        self.assertFalse(activated["sol_enabled"])
        self.assertEqual(old_receipt.read_bytes(), b"old-receipt-bytes\n")
        self.assertEqual(old_index.read_bytes(), b"old-index-bytes\n")
        reopened = self.store.production_canary_status_read_only(
            subject, expected_release_id=TARGET_RELEASE_ID
        )
        self.assertEqual(reopened["schema_version"], V3_STATE_SCHEMA)
        self.assertNotEqual(Path(reopened["terminal_index_path"]), old_index)

    def test_sequential_three_subject_activation_skips_foreign_v2_zero_active_telemetry(
        self,
    ) -> None:
        subjects = ("math", "cs408", "english")
        for subject in subjects:
            self.store.begin_subject_drain(subject)
            self._write_signed_state(
                subject,
                self._state_core(
                    subject,
                    state="inactive_rolled_back",
                    status="production_canary_inactive",
                    luna_consumer_enabled=False,
                ),
            )

        activated: dict[str, dict[str, object]] = {}
        for offset, subject in enumerate(subjects):
            activated[subject] = self.store.activate_production_canary(
                subject,
                release_id=TARGET_RELEASE_ID,
                producer_authority=_producer_authority(
                    subject, TARGET_RELEASE_ID
                ),
                activated_at=f"2026-08-12T10:00:0{offset}Z",
                continuous_concurrency_limit=20,
            )
            telemetry = self.store._read_object(
                self.store.production_canary_concurrency_telemetry_path
            )
            self.assertIsNotNone(telemetry)
            self.store._verify_seal(
                telemetry,
                purpose="dispatch-production-canary-concurrency-telemetry",
            )
            self.assertEqual(telemetry["release_id"], TARGET_RELEASE_ID)
            self.assertEqual(telemetry["global_active_task_count"], 0)
            for inspected_subject in subjects:
                expected_activation_id = (
                    activated[inspected_subject]["activation_id"]
                    if inspected_subject in activated
                    else None
                )
                self.assertEqual(
                    telemetry["activation_ids"][inspected_subject],
                    expected_activation_id,
                )

        for subject in subjects:
            reopened = self.store.production_canary_status_read_only(
                subject, expected_release_id=TARGET_RELEASE_ID
            )
            self.assertEqual(reopened["schema_version"], V3_STATE_SCHEMA)
            self.assertEqual(reopened["state"], "armed")
            self.assertEqual(reopened["active_task_count"], 0)

    def test_staged_activation_replaces_exact_foreign_v3_inactive(self) -> None:
        subject = "english"
        recovery_sha256 = "e" * 64
        recovery_path = self.runtime / "recovery" / f"{recovery_sha256}.json"
        self.store.begin_subject_drain(subject)
        self._write_signed_state(
            subject,
            self._state_core(
                subject,
                schema_version=V3_STATE_SCHEMA,
                state="inactive_rolled_back",
                status="production_canary_inactive",
                luna_consumer_enabled=False,
            ),
        )

        activated = self.store.activate_production_canary(
            subject,
            release_id=TARGET_RELEASE_ID,
            producer_authority=_producer_authority(
                subject, TARGET_RELEASE_ID
            ),
            activated_at="2026-08-12T10:00:00Z",
            continuous_concurrency_limit=20,
            staged_recovery_receipt_sha256=recovery_sha256,
            staged_recovery_receipt_path=str(recovery_path),
        )

        self.assertEqual(activated["schema_version"], V3_STATE_SCHEMA)
        self.assertEqual(activated["release_id"], TARGET_RELEASE_ID)
        self.assertEqual(activated["state"], "paused_drained")
        self.assertFalse(activated["luna_consumer_enabled"])
        staged = self.store._read_object(
            self.store.production_canary_recovery_staged_activation_root
            / f"{recovery_sha256}.json"
        )
        self.assertIsNotNone(staged)
        self.store._verify_seal(
            staged,
            purpose="dispatch-subject-batch-recovery-staged-activation",
        )
        self.assertEqual(staged["target_activation_id"], activated["activation_id"])

    def test_concurrency_telemetry_foreign_identity_fail_closed_boundaries(
        self,
    ) -> None:
        subject = "cs408"
        cases = (
            "same_release_v2",
            "bad_hmac",
            "foreign_active",
            "foreign_armed_zero",
        )
        for name in cases:
            with self.subTest(name=name):
                state_path = self.store._production_canary_state_path(subject)
                state_path.unlink(missing_ok=True)
                self.store.production_canary_concurrency_telemetry_path.unlink(
                    missing_ok=True
                )
                if name == "same_release_v2":
                    self._write_signed_state(
                        subject,
                        self._state_core(
                            subject,
                            release_id=TARGET_RELEASE_ID,
                            state="inactive_rolled_back",
                            status="production_canary_inactive",
                            luna_consumer_enabled=False,
                        ),
                    )
                    expected_code = "production_canary_state_invalid"
                elif name == "bad_hmac":
                    sealed = self._write_signed_state(
                        subject,
                        self._state_core(
                            subject,
                            state="inactive_rolled_back",
                            status="production_canary_inactive",
                            luna_consumer_enabled=False,
                        ),
                    )
                    tampered = copy.deepcopy(sealed)
                    tampered["active_task_count"] = 1
                    state_path.write_bytes(_canonical(tampered) + b"\n")
                    expected_code = "authority_hmac_invalid"
                elif name == "foreign_active":
                    self._write_signed_state(
                        subject,
                        self._state_core(
                            subject,
                            state="inactive_rolled_back",
                            status="production_canary_inactive",
                            luna_consumer_enabled=False,
                            active_task_count=1,
                        ),
                    )
                    expected_code = (
                        "production_canary_concurrency_release_conflict"
                    )
                else:
                    self._write_signed_state(
                        subject,
                        self._state_core(
                            subject,
                            state="armed",
                            status="production_canary_active",
                            luna_consumer_enabled=True,
                            active_task_count=0,
                        ),
                    )
                    expected_code = (
                        "production_canary_concurrency_release_conflict"
                    )

                with self.assertRaises(DispatchError) as raised:
                    self.store._recompute_canary_concurrency_telemetry_locked(
                        TARGET_RELEASE_ID,
                        updated_at="2026-08-12T10:00:00Z",
                    )
                self.assertEqual(raised.exception.code, expected_code)

    def test_concurrency_telemetry_skips_exact_foreign_v3_inactive(self) -> None:
        subject = "english"
        self._write_signed_state(
            subject,
            self._state_core(
                subject,
                schema_version=V3_STATE_SCHEMA,
                state="inactive_rolled_back",
                status="production_canary_inactive",
                luna_consumer_enabled=False,
            ),
        )

        telemetry = self.store._recompute_canary_concurrency_telemetry_locked(
            TARGET_RELEASE_ID,
            updated_at="2026-08-12T10:00:00Z",
        )

        self.assertEqual(telemetry["release_id"], TARGET_RELEASE_ID)
        self.assertEqual(telemetry["activation_ids"][subject], None)
        self.assertEqual(telemetry["global_active_task_count"], 0)

    def test_activation_cannot_overwrite_foreign_active_or_invalid_inactive_state(self) -> None:
        subject = "math"
        self.store.begin_subject_drain(subject)
        cases = {
            "active_v2": self._state_core(subject),
            "active_v3": self._state_core(
                subject,
                schema_version=V3_STATE_SCHEMA,
            ),
            "inactive_v2_wrong_status": self._state_core(
                subject,
                state="inactive_rolled_back",
                status="production_canary_active",
                luna_consumer_enabled=False,
            ),
            "inactive_v2_consumer_enabled": self._state_core(
                subject,
                state="inactive_rolled_back",
                status="production_canary_inactive",
                luna_consumer_enabled=True,
            ),
            "inactive_v2_has_active_task": self._state_core(
                subject,
                state="inactive_rolled_back",
                status="production_canary_inactive",
                luna_consumer_enabled=False,
                active_task_count=1,
            ),
            "inactive_v2_has_selected": self._state_core(
                subject,
                state="inactive_rolled_back",
                status="production_canary_inactive",
                luna_consumer_enabled=False,
                selected={"unit_sha256": "1" * 64},
            ),
            "inactive_v2_has_active_selection": self._state_core(
                subject,
                state="inactive_rolled_back",
                status="production_canary_inactive",
                luna_consumer_enabled=False,
                active_selections={"1" * 64: {}},
            ),
        }
        for name, core in cases.items():
            with self.subTest(name=name):
                self._write_signed_state(subject, core)
                before = self._tree_bytes()
                with self.assertRaises(DispatchError) as raised:
                    self.store.activate_production_canary(
                        subject,
                        release_id=TARGET_RELEASE_ID,
                        producer_authority=_producer_authority(
                            subject, TARGET_RELEASE_ID
                        ),
                        activated_at="2026-08-12T10:00:00Z",
                        continuous_concurrency_limit=20,
                    )
                self.assertEqual(
                    raised.exception.code,
                    "production_canary_activation_conflict",
                )
                self.assertEqual(self._tree_bytes(), before)


if __name__ == "__main__":
    unittest.main()
