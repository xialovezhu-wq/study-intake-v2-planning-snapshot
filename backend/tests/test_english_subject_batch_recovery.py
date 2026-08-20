from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

from concurrent_dispatch import DispatchError, FrozenTask, LeaseStore  # noqa: E402
from preprocess_dispatcher import (  # noqa: E402
    ProductionDispatchRuntime,
    build_parser,
)
from subject_sol_contract import SubjectSolRuntimeStore  # noqa: E402
from tests.test_production_canary_admission import (  # noqa: E402
    authority,
    canary_task,
    sha,
)


JSONSCHEMA_PYTHON = Path("/opt/miniconda3/envs/dl/bin/python")


def schema_results(
    cases: dict[str, tuple[str, object]],
) -> dict[str, list[dict[str, object]]]:
    request = {
        "schema_root": str(ROOT / "schemas"),
        "cases": {
            label: {"schema_name": schema_name, "instance": instance}
            for label, (schema_name, instance) in cases.items()
        },
    }
    script = r"""
import json
import sys
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker
request = json.load(sys.stdin)
root = Path(request["schema_root"])
results = {}
for label, case in request["cases"].items():
    schema = json.loads((root / case["schema_name"]).read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(
        validator.iter_errors(case["instance"]),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    results[label] = [
        {
            "path": list(error.absolute_path),
            "validator": error.validator,
            "message": error.message,
        }
        for error in errors
    ]
json.dump(results, sys.stdout, ensure_ascii=False, sort_keys=True)
"""
    completed = subprocess.run(
        [str(JSONSCHEMA_PYTHON), "-c", script],
        input=json.dumps(request, ensure_ascii=False, sort_keys=True),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(completed.stderr)
    return json.loads(completed.stdout)


def file_bytes(path: Path) -> bytes:
    return path.read_bytes()


class EnglishSubjectBatchRecoveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.runtime_root = Path(self.temporary.name) / "runtime"
        self.release_id = "a" * 64
        self.lease_store = LeaseStore(self.runtime_root)
        self.lease_store.begin_subject_drain("english")
        self.lease_store.activate_production_canary(
            "english",
            release_id=self.release_id,
            producer_authority=authority("english", self.release_id),
            activated_at="2026-08-11T00:00:00Z",
        )
        self.task = canary_task(
            1,
            subject="english",
            release_id=self.release_id,
            capture_id="EVT-ENGLISH-PRESERVED-1",
        )
        self.queue = self.lease_store.materialize_production_canary_task(
            self.task
        )
        failure = self.lease_store.fail_production_canary_preclaim(
            "english",
            self.task,
            failure_stage="pre_claim",
            error_code="subject_luna_batch_already_current",
        )
        self.original_receipt_sha256 = failure[
            "preclaim_failure_receipt_sha256"
        ]

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_readiness_missing_locks_is_byte_for_byte_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary) / "never-created-runtime"
            sol = SubjectSolRuntimeStore(runtime)
            before = list(runtime.rglob("*")) if runtime.exists() else []
            value = sol.canary_readiness(
                "math",
                next_generation="math-generation-next",
                next_authority_fingerprint="d" * 64,
            )
            after = list(runtime.rglob("*")) if runtime.exists() else []
            self.assertEqual(value["readiness"], "ready")
            self.assertEqual(before, after)
            lease = LeaseStore(runtime)
            before = list(runtime.rglob("*")) if runtime.exists() else []
            with self.assertRaisesRegex(
                DispatchError,
                "production_canary_readiness_lock_unavailable",
            ):
                lease.production_canary_recovery_evidence("english")
            after = list(runtime.rglob("*")) if runtime.exists() else []
            self.assertEqual(before, after)

    def _later_same_source_task(self) -> FrozenTask:
        value = self.task.as_dict()
        payload = dict(value["frozen_payload"])
        payload["model_input"] = {
            **dict(payload["model_input"]),
            "replay_attempt": "later-scan",
        }
        return FrozenTask(payload)

    def _prepare_failed_batch(self) -> SubjectSolRuntimeStore:
        store = SubjectSolRuntimeStore(self.runtime_root)
        prepared = store.prepare_and_freeze_subject_batch(
            subject="english",
            batch_id="LUNA-ENGLISH-OLD-BATCH",
            study_date="2026-08-12",
            capture_high_watermark="CAP-OLD-1",
            scan_snapshot_sha256=hashlib.sha256(b"old-scan").hexdigest(),
            authority_generation="english-old-generation",
            authority_fingerprint=hashlib.sha256(
                b"old-authority"
            ).hexdigest(),
            tasks=[
                {
                    "capture_id": "CAP-OLD-1",
                    "unit_sha256": self.task.unit_sha256,
                    "input_fingerprint": self.task.frozen_payload[
                        "input_fingerprint"
                    ],
                    "study_date": "2026-08-12",
                    "frozen_payload_sha256": (
                        self.task.frozen_payload_sha256
                    ),
                }
            ],
        )
        store.record_terminal_failure(
            subject="english",
            batch_id=prepared["batch_id"],
            capture_id="CAP-OLD-1",
            unit_sha256=self.task.unit_sha256,
            status="failed",
            error_code="provider_process_failed",
        )
        return store

    def _successor_task(self, release_id: str) -> FrozenTask:
        payload = copy.deepcopy(dict(self.task.frozen_payload))
        binding = copy.deepcopy(dict(payload["dispatch_contract"]))
        producer = copy.deepcopy(
            dict(binding["producer_input_contract"])
        )
        producer.pop("producer_input_contract_sha256", None)
        producer["release_id"] = release_id
        producer["authority_fingerprint"] = authority(
            "english", release_id
        )["authority_fingerprint"]
        binding["release_id"] = release_id
        binding["producer_input_contract"] = {
            **producer,
            "producer_input_contract_sha256": sha(producer),
        }
        payload["dispatch_contract"] = binding
        return FrozenTask(payload)

    def _arm_recovered_successor(
        self, *, target_release_id: str
    ) -> dict[str, object]:
        sol = self._prepare_failed_batch()
        evidence = self.lease_store.production_canary_recovery_evidence(
            "english",
            expected_original_preclaim_failure_receipt_sha256=(
                self.original_receipt_sha256
            ),
        )
        next_fingerprint = hashlib.sha256(
            f"next-authority:{target_release_id}".encode()
        ).hexdigest()
        recovered = sol.rollover_background_luna_batch_v2(
            "english",
            next_generation=f"english-generation-{target_release_id[:8]}",
            next_authority_fingerprint=next_fingerprint,
            original_preclaim_failure_receipt_sha256=evidence[
                "original_preclaim_failure_receipt_sha256"
            ],
            original_preclaim_failure_receipt_path=evidence[
                "original_preclaim_failure_receipt_path"
            ],
            preserved_queue_entry=evidence["preserved_queue_entry"],
            canary_state_before=evidence["canary_state_before"],
            subsequent_attempt_receipt_sha256s=evidence[
                "subsequent_attempt_receipt_sha256s"
            ],
            preserved_task=evidence["preserved_task"],
        )
        self.lease_store.deactivate_production_canary(
            "english", expected_release_id=self.release_id
        )
        inactive = self.lease_store.production_canary_status_read_only(
            "english"
        )
        legacy_core = dict(inactive)
        legacy_core.pop("authority", None)
        legacy_core["schema_version"] = (
            "study-intake-production-canary-state-v2"
        )
        legacy = self.lease_store._seal(
            legacy_core, purpose="dispatch-production-canary-state"
        )
        self.lease_store._production_canary_state_path(
            "english"
        ).write_bytes(
            json.dumps(
                legacy, sort_keys=True, separators=(",", ":")
            ).encode()
            + b"\n"
        )
        target_state = self.lease_store.activate_production_canary(
            "english",
            release_id=target_release_id,
            producer_authority=authority("english", target_release_id),
            activated_at="2026-08-13T00:00:00Z",
            staged_recovery_receipt_sha256=recovered["receipt_sha256"],
            staged_recovery_receipt_path=recovered["receipt_path"],
        )
        replacement = self._successor_task(target_release_id)
        finalized = self.lease_store.finalize_production_canary_subject_batch_recovery(
            "english",
            recovery_receipt_sha256=recovered["receipt_sha256"],
            recovery_receipt_path=recovered["receipt_path"],
            expected_target_release_id=target_release_id,
            old_queue_identity=recovered["receipt"][
                "preserved_queue_identity"
            ],
            old_queue_entry_sha256=recovered["receipt"][
                "preserved_queue_entry_sha256"
            ],
            replacement_task=replacement,
        )
        armed = self.lease_store.arm_finalized_production_canary_subject_batch_recovery(
            "english",
            recovery_receipt_sha256=recovered["receipt_sha256"],
            expected_target_release_id=target_release_id,
        )
        return {
            "sol": sol,
            "recovered": recovered,
            "target_state": target_state,
            "replacement": replacement,
            "finalized": finalized,
            "armed": armed,
        }

    def test_armed_recovery_queue_is_pending_and_claimable_without_model(self) -> None:
        result = self._arm_recovered_successor(
            target_release_id="d" * 64
        )
        replacement = result["replacement"]
        self.assertIsInstance(replacement, FrozenTask)
        pending = self.lease_store.pending_production_canary_tasks(
            "english"
        )
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].as_dict(), replacement.as_dict())
        claimed = self.lease_store.claim(
            replacement.unit_sha256,
            "english-recovery-zero-model-owner",
            subject="english",
            task=replacement,
            production_canary=True,
            now="2026-08-13T00:00:01Z",
        )
        self.assertEqual(claimed.status, "claimed")
        queue_path = Path(
            result["finalized"]["replacement_queue_entry_path"]
        )
        queue = json.loads(queue_path.read_text(encoding="utf-8"))
        self.assertEqual(queue["queue_status"], "claimed")
        state = self.lease_store.production_canary_status_read_only(
            "english"
        )
        self.assertEqual(state["observed_model_call_count"], 0)
        self.assertEqual(state["observed_provider_request_count"], 0)
        self.assertEqual(state["observed_mcp_tool_call_count"], 0)

        ordinary_old_task = canary_task(
            2,
            subject="english",
            release_id="d" * 64,
            recorded_at="2026-08-12T00:00:00Z",
            capture_id="EVT-ENGLISH-UNAUTHORIZED-OLD-2",
        )
        with self.assertRaisesRegex(
            DispatchError, "production_canary_pre_activation_capture"
        ):
            self.lease_store.materialize_production_canary_task(
                ordinary_old_task
            )

    def test_full_rollback_retires_mutable_intents_and_allows_new_target(self) -> None:
        first = self._arm_recovered_successor(
            target_release_id="d" * 64
        )
        sol = first["sol"]
        recovered = first["recovered"]
        receipt = recovered["receipt"]
        first_recovery_path = Path(recovered["receipt_path"])
        first_recovery_bytes = first_recovery_path.read_bytes()
        recovery_intent_path = sol._background_rollover_v2_intent_path(
            "english", receipt["old_batch_id"]
        )
        rollback_intent_path = (
            sol._background_rollover_rollback_intent_path(
                recovered["receipt_sha256"]
            )
        )
        self.assertTrue(recovery_intent_path.exists())

        self.lease_store.deactivate_production_canary(
            "english", expected_release_id="d" * 64
        )
        runtime = ProductionDispatchRuntime.__new__(
            ProductionDispatchRuntime
        )
        runtime.subject = "english"
        runtime.subject_sol = sol
        runtime.dispatcher = SimpleNamespace(lease_store=self.lease_store)
        rollback = runtime.rollback_subject_batch_recovery(
            recovery_receipt=first_recovery_path
        )
        rollback_path = Path(rollback["rollback_receipt_path"])
        rollback_bytes = rollback_path.read_bytes()
        self.assertFalse(recovery_intent_path.exists())
        self.assertFalse(rollback_intent_path.exists())

        self.assertEqual(first_recovery_path.read_bytes(), first_recovery_bytes)
        self.assertEqual(rollback_path.read_bytes(), rollback_bytes)

        reopened = runtime.reopen_subject_batch_recovery_rollback(
            recovery_receipt=first_recovery_path,
            rollback_receipt=rollback_path,
        )
        self.assertEqual(
            reopened["schema_version"],
            "study-intake-subject-batch-recovery-rollback-reopen-result-v1",
        )
        self.assertEqual(reopened["status"], "reopened")
        self.assertTrue(reopened["rollback_reopen_read_only"])
        self.assertTrue(reopened["target_queue_withdrawn"])
        self.assertTrue(reopened["mutable_recovery_intents_withdrawn"])
        for key in (
            "authority_snapshot_count",
            "authority_snapshot_mcp_tool_call_count",
            "model_mcp_tool_call_count",
            "mcp_tool_call_count",
            "model_call_count",
            "provider_request_count",
            "formal_write_count",
        ):
            self.assertEqual(reopened[key], 0)
        parsed = build_parser().parse_args(
            [
                "--subject",
                "english",
                "reopen-subject-batch-recovery-rollback",
                "--recovery-receipt",
                str(first_recovery_path),
                "--rollback-receipt",
                str(rollback_path),
            ]
        )
        self.assertEqual(
            parsed.command, "reopen-subject-batch-recovery-rollback"
        )

        queue_identity = receipt["preserved_queue_identity"]
        old_queue_path = (
            self.lease_store._production_canary_queue_subject_root(
                "english", queue_identity["activation_id"]
            )
            / (
                queue_identity["producer_input_contract_sha256"]
                + ".json"
            )
        )
        old_queue_bytes = old_queue_path.read_bytes()
        old_queue = json.loads(old_queue_bytes)
        queue_core = dict(old_queue)
        queue_core.pop("authority")
        queue_core["discovered_at"] = "2026-08-13T00:00:01Z"
        old_queue_path.write_bytes(
            json.dumps(
                self.lease_store._seal(
                    queue_core, purpose="dispatch-production-canary-queue"
                ),
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )
        with self.assertRaisesRegex(
            DispatchError,
            "production_canary_preserved_queue_postimage_mismatch",
        ):
            runtime.reopen_subject_batch_recovery_rollback(
                recovery_receipt=first_recovery_path,
                rollback_receipt=rollback_path,
            )
        old_queue_path.write_bytes(old_queue_bytes)

        gate_path = self.lease_store._production_canary_state_path("english")
        gate_bytes = gate_path.read_bytes()
        gate = json.loads(gate_bytes)
        gate_core = dict(gate)
        gate_core.pop("authority")
        gate_core["updated_at"] = "2026-08-13T00:00:02Z"
        gate_path.write_bytes(
            json.dumps(
                self.lease_store._seal(
                    gate_core, purpose="dispatch-production-canary-state"
                ),
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )
        with self.assertRaisesRegex(
            DispatchError, "subject_batch_recovery_rollback_gate_drift"
        ):
            runtime.reopen_subject_batch_recovery_rollback(
                recovery_receipt=first_recovery_path,
                rollback_receipt=rollback_path,
            )
        gate_path.write_bytes(gate_bytes)

        staged_path = (
            self.lease_store.production_canary_recovery_staged_activation_root
            / f"{recovered['receipt_sha256']}.json"
        )
        staged_path.parent.mkdir(parents=True, exist_ok=True)
        staged_path.write_text("{}\n", encoding="utf-8")
        with self.assertRaisesRegex(
            DispatchError, "subject_batch_recovery_rollback_pointer_drift"
        ):
            runtime.reopen_subject_batch_recovery_rollback(
                recovery_receipt=first_recovery_path,
                rollback_receipt=rollback_path,
            )
        staged_path.unlink()

        tampered = json.loads(rollback_bytes)
        tampered["seal"]["hmac_sha256"] = "0" * 64
        tampered_payload = (
            json.dumps(tampered, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        tampered_sha = hashlib.sha256(tampered_payload).hexdigest()
        tampered_path = (
            sol.background_rollover_rollback_receipt_root
            / "sha256"
            / tampered_sha[:2]
            / f"{tampered_sha}.json"
        )
        tampered_path.parent.mkdir(parents=True, exist_ok=True)
        tampered_path.write_bytes(tampered_payload)
        with self.assertRaisesRegex(
            DispatchError, "control_receipt_hmac_invalid"
        ):
            runtime.reopen_subject_batch_recovery_rollback(
                recovery_receipt=first_recovery_path,
                rollback_receipt=tampered_path,
            )
        tampered_path.unlink()

        repeated = sol.rollback_background_luna_batch_v2(
            recovery_receipt_path=first_recovery_path
        )
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(
            repeated["rollback_receipt_sha256"],
            rollback["rollback_receipt_sha256"],
        )
        self.assertFalse(recovery_intent_path.exists())
        self.assertFalse(rollback_intent_path.exists())

        evidence = self.lease_store.production_canary_recovery_evidence(
            "english",
            expected_original_preclaim_failure_receipt_sha256=(
                self.original_receipt_sha256
            ),
        )
        second_generation = "english-generation-target-two"
        second_fingerprint = hashlib.sha256(
            b"english-authority-target-two"
        ).hexdigest()
        second = sol.rollover_background_luna_batch_v2(
            "english",
            next_generation=second_generation,
            next_authority_fingerprint=second_fingerprint,
            original_preclaim_failure_receipt_sha256=evidence[
                "original_preclaim_failure_receipt_sha256"
            ],
            original_preclaim_failure_receipt_path=evidence[
                "original_preclaim_failure_receipt_path"
            ],
            preserved_queue_entry=evidence["preserved_queue_entry"],
            canary_state_before=evidence["canary_state_before"],
            subsequent_attempt_receipt_sha256s=evidence[
                "subsequent_attempt_receipt_sha256s"
            ],
            preserved_task=evidence["preserved_task"],
        )
        self.assertFalse(second["idempotent"])
        self.assertNotEqual(
            second["receipt_sha256"], recovered["receipt_sha256"]
        )
        self.assertEqual(
            second["receipt"]["next_generation"], second_generation
        )
        self.assertEqual(first_recovery_path.read_bytes(), first_recovery_bytes)
        self.assertEqual(rollback_path.read_bytes(), rollback_bytes)

    def test_early_recovery_rollback_reopens_outer_deployment_gate(self) -> None:
        original_gate_path = self.lease_store._production_canary_state_path(
            "english"
        )
        original_gate_bytes = original_gate_path.read_bytes()
        original_gate_sha256 = hashlib.sha256(
            original_gate_bytes
        ).hexdigest()
        sol = self._prepare_failed_batch()
        self.lease_store.deactivate_production_canary(
            "english", expected_release_id=self.release_id
        )
        evidence = self.lease_store.production_canary_recovery_evidence(
            "english",
            expected_original_preclaim_failure_receipt_sha256=(
                self.original_receipt_sha256
            ),
        )
        recovered = sol.rollover_background_luna_batch_v2(
            "english",
            next_generation="english-generation-early-rollback",
            next_authority_fingerprint=hashlib.sha256(
                b"early-rollback-authority"
            ).hexdigest(),
            original_preclaim_failure_receipt_sha256=evidence[
                "original_preclaim_failure_receipt_sha256"
            ],
            original_preclaim_failure_receipt_path=evidence[
                "original_preclaim_failure_receipt_path"
            ],
            preserved_queue_entry=evidence["preserved_queue_entry"],
            canary_state_before=evidence["canary_state_before"],
            subsequent_attempt_receipt_sha256s=evidence[
                "subsequent_attempt_receipt_sha256s"
            ],
            preserved_task=evidence["preserved_task"],
        )
        runtime = ProductionDispatchRuntime.__new__(
            ProductionDispatchRuntime
        )
        runtime.subject = "english"
        runtime.subject_sol = sol
        runtime.dispatcher = SimpleNamespace(lease_store=self.lease_store)
        rollback = runtime.rollback_subject_batch_recovery(
            recovery_receipt=Path(recovered["receipt_path"])
        )
        self.assertEqual(rollback["removed_mutable_pointer_count"], 0)

        original_gate_path.write_bytes(original_gate_bytes)
        reopened = runtime.reopen_subject_batch_recovery_rollback(
            recovery_receipt=Path(recovered["receipt_path"]),
            rollback_receipt=Path(rollback["rollback_receipt_path"]),
            expected_deployment_canary_state_sha256=(
                original_gate_sha256
            ),
            expected_finalization_rollback_proof_count=0,
        )
        self.assertEqual(
            reopened["deployment_canary_state_postimage_sha256"],
            original_gate_sha256,
        )
        self.assertEqual(reopened["finalization_rollback_proof_count"], 0)
        self.assertTrue(reopened["target_queue_withdrawn"])
        self.assertTrue(reopened["mutable_recovery_intents_withdrawn"])
        self.assertEqual(original_gate_path.read_bytes(), original_gate_bytes)

    def test_original_preserved_receipt_wins_and_later_scan_aliases(self) -> None:
        later = self._later_same_source_task()
        state_path = self.lease_store._production_canary_state_path("english")
        queue_path = self.lease_store._production_canary_queue_path(
            "english", self.queue["producer_input_contract_sha256"]
        )
        state_before = file_bytes(state_path)
        queue_before = file_bytes(queue_path)

        alias = self.lease_store.materialize_production_canary_task(later)

        self.assertEqual(
            alias["materialization_status"],
            "already_represented_by_queue",
        )
        self.assertEqual(file_bytes(state_path), state_before)
        self.assertEqual(file_bytes(queue_path), queue_before)
        evidence = self.lease_store.production_canary_recovery_evidence(
            "english",
            expected_original_preclaim_failure_receipt_sha256=(
                self.original_receipt_sha256
            ),
        )
        self.assertEqual(
            evidence["original_preclaim_failure_receipt_sha256"],
            self.original_receipt_sha256,
        )

        drift = canary_task(
            1,
            subject="english",
            release_id=self.release_id,
            capture_id="EVT-ENGLISH-PRESERVED-1",
            fingerprint_suffix="contract-drift",
        )
        with self.assertRaisesRegex(
            DispatchError, "deterministic_producer_drift"
        ):
            self.lease_store.materialize_production_canary_task(drift)

    def test_v2_recovery_and_compensation_restore_mutable_bytes(self) -> None:
        sol = self._prepare_failed_batch()
        later = self._later_same_source_task()
        later_failure = self.lease_store.fail_production_canary_preclaim(
            "english",
            later,
            failure_stage="pre_claim",
            error_code="production_canary_producer_replay",
        )
        evidence = self.lease_store.production_canary_recovery_evidence(
            "english",
            expected_original_preclaim_failure_receipt_sha256=(
                self.original_receipt_sha256
            ),
        )
        self.assertIn(
            later_failure["preclaim_failure_receipt_sha256"],
            evidence["subsequent_attempt_receipt_sha256s"],
        )

        writer_path = sol._writer_path("english")
        batch_pointer_path = sol._batch_pointer_path("english")
        canary_state_path = self.lease_store._production_canary_state_path(
            "english"
        )
        queue_path = self.lease_store._production_canary_queue_path(
            "english", self.queue["producer_input_contract_sha256"]
        )
        before = {
            "writer": file_bytes(writer_path),
            "batch_pointer": file_bytes(batch_pointer_path),
            "canary_state": file_bytes(canary_state_path),
            "queue": file_bytes(queue_path),
        }
        next_fingerprint = hashlib.sha256(b"next-authority").hexdigest()
        recovered = sol.rollover_background_luna_batch_v2(
            "english",
            next_generation="english-next-generation",
            next_authority_fingerprint=next_fingerprint,
            original_preclaim_failure_receipt_sha256=evidence[
                "original_preclaim_failure_receipt_sha256"
            ],
            original_preclaim_failure_receipt_path=evidence[
                "original_preclaim_failure_receipt_path"
            ],
            preserved_queue_entry=evidence["preserved_queue_entry"],
            canary_state_before=evidence["canary_state_before"],
            subsequent_attempt_receipt_sha256s=evidence[
                "subsequent_attempt_receipt_sha256s"
            ],
            preserved_task=evidence["preserved_task"],
        )
        self.assertEqual(
            recovered["receipt"]["schema_version"],
            "subject_background_luna_rollover_receipt_v2",
        )
        self.assertEqual(
            sol.canary_readiness(
                "english",
                next_generation="english-next-generation",
                next_authority_fingerprint=next_fingerprint,
            )["readiness"],
            "ready",
        )
        receipt = recovered["receipt"]

        self.lease_store.deactivate_production_canary(
            "english", expected_release_id=self.release_id
        )
        inactive = self.lease_store.production_canary_status_read_only(
            "english"
        )
        legacy_core = dict(inactive)
        legacy_core.pop("authority", None)
        legacy_core["schema_version"] = (
            "study-intake-production-canary-state-v2"
        )
        legacy = self.lease_store._seal(
            legacy_core, purpose="dispatch-production-canary-state"
        )
        self.lease_store._production_canary_state_path("english").write_bytes(
            json.dumps(
                legacy,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        target_release_id = "b" * 64
        target_state = self.lease_store.activate_production_canary(
            "english",
            release_id=target_release_id,
            producer_authority=authority("english", target_release_id),
            activated_at="2026-08-13T00:00:00Z",
            staged_recovery_receipt_sha256=recovered["receipt_sha256"],
            staged_recovery_receipt_path=recovered["receipt_path"],
        )
        self.assertEqual(target_state["state"], "paused_drained")
        self.assertFalse(target_state["luna_consumer_enabled"])
        finalized = self.lease_store.finalize_production_canary_subject_batch_recovery(
            "english",
            recovery_receipt_sha256=recovered["receipt_sha256"],
            recovery_receipt_path=recovered["receipt_path"],
            expected_target_release_id=target_release_id,
            old_queue_identity=receipt["preserved_queue_identity"],
            old_queue_entry_sha256=receipt[
                "preserved_queue_entry_sha256"
            ],
            replacement_task=self._successor_task(target_release_id),
        )
        self.assertNotEqual(
            finalized["target_activation_id"],
            receipt["preserved_queue_identity"]["activation_id"],
        )
        self.assertEqual(
            finalized["target_activation_id"], target_state["activation_id"]
        )
        replacement_queue_path = Path(
            finalized["replacement_queue_entry_path"]
        )
        replacement_queue = json.loads(
            replacement_queue_path.read_text(encoding="utf-8")
        )
        self.assertEqual(replacement_queue["queue_status"], "pending")
        self.assertIsNone(replacement_queue["terminal_receipt_sha256"])
        still_staged = self.lease_store.production_canary_status_read_only(
            "english"
        )
        self.assertEqual(still_staged["state"], "paused_drained")
        self.assertFalse(still_staged["luna_consumer_enabled"])
        staged_path = (
            self.lease_store.production_canary_recovery_staged_activation_root
            / f"{recovered['receipt_sha256']}.json"
        )
        staged_value = json.loads(staged_path.read_text(encoding="utf-8"))
        staged_core = dict(staged_value)
        staged_core.pop("authority")
        staged_core["status"] = "arming"
        staged_path.write_bytes(
            json.dumps(
                self.lease_store._seal(
                    staged_core,
                    purpose=(
                        "dispatch-subject-batch-recovery-staged-activation"
                    ),
                ),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
        armed = self.lease_store.arm_finalized_production_canary_subject_batch_recovery(
            "english",
            recovery_receipt_sha256=recovered["receipt_sha256"],
            expected_target_release_id=target_release_id,
        )
        self.assertEqual(armed["state"]["state"], "armed")
        self.assertTrue(armed["state"]["luna_consumer_enabled"])
        successor_task = self._successor_task(target_release_id)
        pending = self.lease_store.pending_production_canary_tasks(
            "english"
        )
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0].as_dict(), successor_task.as_dict())
        ordinary_old_task = canary_task(
            2,
            subject="english",
            release_id=target_release_id,
            recorded_at="2026-08-12T00:00:00Z",
            capture_id="EVT-ENGLISH-UNAUTHORIZED-OLD-2",
        )
        with self.assertRaisesRegex(
            DispatchError, "production_canary_pre_activation_capture"
        ):
            self.lease_store.materialize_production_canary_task(
                ordinary_old_task
            )

        self.lease_store.deactivate_production_canary(
            "english", expected_release_id=target_release_id
        )
        self.lease_store.restore_production_canary_recovery_preimage(
            canary_state_before=receipt["canary_state_before"],
            expected_preserved_queue_entry_sha256=receipt[
                "preserved_queue_entry_sha256"
            ],
            apply=False,
        )
        self.lease_store.rollback_production_canary_subject_batch_finalization(
            recovery_receipt_sha256=recovered["receipt_sha256"],
            apply=False,
        )
        os.environ[
            "STUDY_INTAKE_RECOVERY_FINALIZATION_FAILPOINT"
        ] = "after_queue_unlink_before_rollback_pointer"
        try:
            with self.assertRaisesRegex(
                DispatchError,
                "subject_batch_recovery_finalization_injected_crash",
            ):
                self.lease_store.rollback_production_canary_subject_batch_finalization(
                    recovery_receipt_sha256=recovered["receipt_sha256"],
                    apply=True,
                )
        finally:
            os.environ.pop(
                "STUDY_INTAKE_RECOVERY_FINALIZATION_FAILPOINT", None
            )
        self.lease_store.rollback_production_canary_subject_batch_finalization(
            recovery_receipt_sha256=recovered["receipt_sha256"],
            apply=True,
        )
        rolled_back = sol.rollback_background_luna_batch_v2(
            recovery_receipt_path=Path(recovered["receipt_path"])
        )
        self.lease_store.restore_production_canary_recovery_preimage(
            canary_state_before=receipt["canary_state_before"],
            expected_preserved_queue_entry_sha256=receipt[
                "preserved_queue_entry_sha256"
            ],
            apply=True,
        )
        self.assertFalse(rolled_back["idempotent"])
        self.assertEqual(file_bytes(writer_path), before["writer"])
        self.assertEqual(
            file_bytes(batch_pointer_path), before["batch_pointer"]
        )
        self.assertEqual(
            file_bytes(canary_state_path), before["canary_state"]
        )
        self.assertEqual(file_bytes(queue_path), before["queue"])
        self.assertFalse(replacement_queue_path.exists())
        supersede = json.loads(
            Path(finalized["supersede_receipt_path"]).read_text(
                encoding="utf-8"
            )
        )
        missing_key = copy.deepcopy(recovered["receipt"])
        missing_key.pop("rollback_token")
        fake_sha = copy.deepcopy(rolled_back["rollback_receipt"])
        fake_sha["recovery_receipt_sha256"] = "not-a-sha256"
        wrong_purpose = copy.deepcopy(supersede)
        wrong_purpose["authority"]["purpose"] = "wrong-purpose"
        results = schema_results(
            {
                "rollover_positive": (
                    "subject-background-luna-rollover-receipt-v2.json",
                    recovered["receipt"],
                ),
                "rollback_positive": (
                    "subject-background-luna-recovery-rollback-receipt-v1.json",
                    rolled_back["rollback_receipt"],
                ),
                "supersede_positive": (
                    "subject-batch-recovery-supersede-receipt-v1.json",
                    supersede,
                ),
                "missing_key": (
                    "subject-background-luna-rollover-receipt-v2.json",
                    missing_key,
                ),
                "fake_sha": (
                    "subject-background-luna-recovery-rollback-receipt-v1.json",
                    fake_sha,
                ),
                "wrong_authority_purpose": (
                    "subject-batch-recovery-supersede-receipt-v1.json",
                    wrong_purpose,
                ),
            }
        )
        for label in (
            "rollover_positive",
            "rollback_positive",
            "supersede_positive",
        ):
            self.assertEqual(results[label], [], (label, results[label]))
        self.assertTrue(results["missing_key"])
        self.assertTrue(results["fake_sha"])
        self.assertTrue(results["wrong_authority_purpose"])
        cleanup = self.lease_store.complete_production_canary_subject_batch_recovery_rollback(
            recovery_receipt_sha256=recovered["receipt_sha256"]
        )
        self.assertTrue(cleanup["target_queue_withdrawn"])
        for root in (
            self.lease_store.production_canary_recovery_staged_activation_root,
            self.lease_store.production_canary_recovery_finalization_root,
            self.lease_store.production_canary_recovery_finalization_intent_root,
            self.lease_store.production_canary_recovery_finalization_rollback_intent_root,
        ):
            self.assertFalse(
                (root / f"{recovered['receipt_sha256']}.json").exists()
            )
        self.assertTrue(Path(recovered["receipt_path"]).exists())
        self.assertTrue(Path(finalized["supersede_receipt_path"]).exists())

    def test_legacy_english_resume_is_fail_closed(self) -> None:
        runtime = ProductionDispatchRuntime.__new__(ProductionDispatchRuntime)
        runtime.production_canary = True
        runtime.subject = "english"
        runtime.dispatcher = SimpleNamespace(lease_store=self.lease_store)
        with self.assertRaisesRegex(
            DispatchError, "generation_aware_subject_recovery_required"
        ):
            runtime.resume_production_canary()

    def test_cli_shape_and_runtime_authority_double_read(self) -> None:
        parsed = build_parser().parse_args(
            [
                "--config",
                "/tmp/config.json",
                "--subject",
                "english",
                "recover-subject-batch",
                "--expected-original-preclaim-failure-receipt-sha256",
                self.original_receipt_sha256,
                "--expected-next-generation",
                "english-next-generation",
                "--expected-next-authority-fingerprint",
                "f" * 64,
            ]
        )
        self.assertEqual(parsed.command, "recover-subject-batch")
        self.assertEqual(
            parsed.expected_original_preclaim_failure_receipt_sha256,
            self.original_receipt_sha256,
        )
        staged_args = build_parser().parse_args(
            [
                "--config",
                "/tmp/config.json",
                "--subject",
                "english",
                "stage-subject-batch-recovery-activation",
                "--recovery-receipt",
                "/tmp/recovery.json",
                "--activated-at",
                "2026-08-13T00:00:00Z",
            ]
        )
        self.assertEqual(
            staged_args.command,
            "stage-subject-batch-recovery-activation",
        )
        arm_args = build_parser().parse_args(
            [
                "--config",
                "/tmp/config.json",
                "--subject",
                "english",
                "arm-finalized-subject-batch-recovery",
                "--recovery-receipt",
                "/tmp/recovery.json",
                "--expected-target-release-id",
                "e" * 64,
            ]
        )
        self.assertEqual(
            arm_args.command,
            "arm-finalized-subject-batch-recovery",
        )

        sol = self._prepare_failed_batch()

        class AuthorityHost:
            calls = 0

            def subject_authority_snapshot(inner_self, subject):
                inner_self.calls += 1
                return {
                    "subject": subject,
                    "generation": "english-next-generation",
                    "authority_fingerprint": "f" * 64,
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }

        host = AuthorityHost()
        runtime = ProductionDispatchRuntime.__new__(ProductionDispatchRuntime)
        runtime.production_canary = True
        runtime.subject = "english"
        runtime.subject_sol = sol
        runtime.processing_host = host
        runtime.dispatcher = SimpleNamespace(lease_store=self.lease_store)

        readiness = runtime.canary_readiness()
        self.assertEqual(
            readiness["readiness"], "recoverable_terminal_batch"
        )
        self.assertEqual(
            readiness["original_preclaim_failure_receipt_sha256"],
            self.original_receipt_sha256,
        )
        gate_before = self.lease_store.production_canary_status_read_only(
            "english"
        )
        recovered = runtime.recover_subject_batch(
            expected_original_preclaim_failure_receipt_sha256=(
                self.original_receipt_sha256
            ),
            expected_next_generation="english-next-generation",
            expected_next_authority_fingerprint="f" * 64,
        )
        self.assertEqual(recovered["status"], "recovered")
        self.assertEqual(recovered["authority_snapshot_count"], 2)
        self.assertEqual(host.calls, 3)
        self.assertEqual(
            self.lease_store.production_canary_status_read_only("english"),
            gate_before,
        )
        reopened = runtime.recover_subject_batch(
            expected_original_preclaim_failure_receipt_sha256=(
                self.original_receipt_sha256
            ),
            expected_next_generation="english-next-generation",
            expected_next_authority_fingerprint="f" * 64,
        )
        self.assertTrue(reopened["idempotent"])
        self.assertEqual(
            reopened["recovery_receipt_sha256"],
            recovered["recovery_receipt_sha256"],
        )

    def test_finalize_queue_write_crash_has_no_orphan_after_rollback(self) -> None:
        sol = self._prepare_failed_batch()
        evidence = self.lease_store.production_canary_recovery_evidence(
            "english",
            expected_original_preclaim_failure_receipt_sha256=(
                self.original_receipt_sha256
            ),
        )
        next_fingerprint = hashlib.sha256(b"next-authority").hexdigest()
        recovered = sol.rollover_background_luna_batch_v2(
            "english",
            next_generation="english-next-generation",
            next_authority_fingerprint=next_fingerprint,
            original_preclaim_failure_receipt_sha256=(
                self.original_receipt_sha256
            ),
            original_preclaim_failure_receipt_path=evidence[
                "original_preclaim_failure_receipt_path"
            ],
            preserved_queue_entry=evidence["preserved_queue_entry"],
            canary_state_before=evidence["canary_state_before"],
            subsequent_attempt_receipt_sha256s=[],
            preserved_task=evidence["preserved_task"],
        )
        receipt = recovered["receipt"]
        writer_path = sol._writer_path("english")
        batch_pointer_path = sol._batch_pointer_path("english")
        canary_state_path = self.lease_store._production_canary_state_path(
            "english"
        )
        old_queue_path = self.lease_store._production_canary_queue_path(
            "english", self.queue["producer_input_contract_sha256"]
        )
        preimage = {
            "writer": json.dumps(
                receipt["writer_state_before"],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n",
            "batch_pointer": json.dumps(
                receipt["batch_pointer_before"],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n",
            "canary_state": json.dumps(
                receipt["canary_state_before"],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n",
            "old_queue": file_bytes(old_queue_path),
        }
        self.lease_store.deactivate_production_canary(
            "english", expected_release_id=self.release_id
        )
        inactive = self.lease_store.production_canary_status_read_only(
            "english"
        )
        legacy_core = dict(inactive)
        legacy_core.pop("authority", None)
        legacy_core["schema_version"] = (
            "study-intake-production-canary-state-v2"
        )
        legacy = self.lease_store._seal(
            legacy_core, purpose="dispatch-production-canary-state"
        )
        self.lease_store._production_canary_state_path("english").write_bytes(
            json.dumps(legacy, sort_keys=True, separators=(",", ":")).encode()
            + b"\n"
        )
        target_release_id = "c" * 64
        target = self.lease_store.activate_production_canary(
            "english",
            release_id=target_release_id,
            producer_authority=authority("english", target_release_id),
            activated_at="2026-08-13T01:00:00Z",
            staged_recovery_receipt_sha256=recovered["receipt_sha256"],
            staged_recovery_receipt_path=recovered["receipt_path"],
        )
        replacement = self._successor_task(target_release_id)
        contract = self.lease_store._producer_contract_from_task(replacement)
        target_queue_path = (
            self.lease_store._production_canary_queue_subject_root(
                "english", target["activation_id"]
            )
            / f"{contract['producer_input_contract_sha256']}.json"
        )
        os.environ[
            "STUDY_INTAKE_RECOVERY_FINALIZATION_FAILPOINT"
        ] = "after_queue_before_supersede_pointer"
        try:
            with self.assertRaisesRegex(
                DispatchError,
                "subject_batch_recovery_finalization_injected_crash",
            ):
                self.lease_store.finalize_production_canary_subject_batch_recovery(
                    "english",
                    recovery_receipt_sha256=recovered["receipt_sha256"],
                    recovery_receipt_path=recovered["receipt_path"],
                    expected_target_release_id=target_release_id,
                    old_queue_identity=receipt["preserved_queue_identity"],
                    old_queue_entry_sha256=receipt[
                        "preserved_queue_entry_sha256"
                    ],
                    replacement_task=replacement,
                )
        finally:
            os.environ.pop(
                "STUDY_INTAKE_RECOVERY_FINALIZATION_FAILPOINT", None
            )
        self.assertTrue(target_queue_path.exists())
        self.assertFalse(
            (
                self.lease_store.production_canary_recovery_finalization_root
                / f"{recovered['receipt_sha256']}.json"
            ).exists()
        )
        os.environ[
            "STUDY_INTAKE_RECOVERY_FINALIZATION_FAILPOINT"
        ] = "after_supersede_before_pointer"
        try:
            with self.assertRaisesRegex(
                DispatchError,
                "subject_batch_recovery_finalization_injected_crash",
            ):
                self.lease_store.finalize_production_canary_subject_batch_recovery(
                    "english",
                    recovery_receipt_sha256=recovered["receipt_sha256"],
                    recovery_receipt_path=recovered["receipt_path"],
                    expected_target_release_id=target_release_id,
                    old_queue_identity=receipt["preserved_queue_identity"],
                    old_queue_entry_sha256=receipt[
                        "preserved_queue_entry_sha256"
                    ],
                    replacement_task=replacement,
                )
        finally:
            os.environ.pop(
                "STUDY_INTAKE_RECOVERY_FINALIZATION_FAILPOINT", None
            )
        self.assertTrue(target_queue_path.exists())
        self.assertFalse(
            (
                self.lease_store.production_canary_recovery_finalization_root
                / f"{recovered['receipt_sha256']}.json"
            ).exists()
        )
        finalized = self.lease_store.finalize_production_canary_subject_batch_recovery(
            "english",
            recovery_receipt_sha256=recovered["receipt_sha256"],
            recovery_receipt_path=recovered["receipt_path"],
            expected_target_release_id=target_release_id,
            old_queue_identity=receipt["preserved_queue_identity"],
            old_queue_entry_sha256=receipt[
                "preserved_queue_entry_sha256"
            ],
            replacement_task=replacement,
        )
        receipt_root = (
            self.lease_store.production_canary_receipt_root
            / "english"
            / target["activation_id"]
        )
        supersede_receipts = [
            path
            for path in receipt_root.rglob("*.json")
            if json.loads(path.read_text(encoding="utf-8")).get(
                "schema_version"
            )
            == "study-intake-subject-batch-recovery-supersede-receipt-v1"
        ]
        self.assertEqual(len(supersede_receipts), 1)
        self.assertEqual(
            finalized["supersede_receipt_path"],
            str(supersede_receipts[0]),
        )
        runtime = ProductionDispatchRuntime.__new__(ProductionDispatchRuntime)
        runtime.subject = "english"
        runtime.subject_sol = sol
        runtime.dispatcher = SimpleNamespace(lease_store=self.lease_store)
        rollback_result = runtime.rollback_subject_batch_recovery(
            recovery_receipt=Path(recovered["receipt_path"])
        )
        self.assertTrue(rollback_result["target_queue_withdrawn"])
        self.assertEqual(
            rollback_result["rollback_token"], receipt["rollback_token"]
        )
        self.assertEqual(
            rollback_result["preserved_queue_entry_sha256"],
            receipt["preserved_queue_entry_sha256"],
        )
        self.assertEqual(
            rollback_result["canary_state_restored_sha256"],
            receipt["canary_state_before_sha256"],
        )
        self.assertFalse(target_queue_path.exists())
        self.assertEqual(file_bytes(writer_path), preimage["writer"])
        self.assertEqual(
            file_bytes(batch_pointer_path), preimage["batch_pointer"]
        )
        self.assertEqual(
            file_bytes(canary_state_path), preimage["canary_state"]
        )
        self.assertEqual(file_bytes(old_queue_path), preimage["old_queue"])
        self.assertTrue(Path(recovered["receipt_path"]).exists())


if __name__ == "__main__":
    unittest.main()
