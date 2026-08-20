from __future__ import annotations

import copy
import base64
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JSONSCHEMA_PYTHON = Path("/opt/miniconda3/envs/dl/bin/python")
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "tests"))

from concurrent_dispatch import ConcurrentDispatcher, FrozenTask, LeaseStore  # noqa: E402
from test_backend_successor_contracts import result, task  # noqa: E402
from test_production_canary_admission import (  # noqa: E402
    FailedRunner,
    GroundedRunner,
    IdentityPublishingFixtureRunner,
    authority,
    canary_task,
    sha,
)


SCHEMA_NAMES = (
    "dispatch-task-event-v2.json",
    "dispatch-task-detail-v2.json",
    "concurrent-completion-v2.json",
    "preprocess-package-v3.json",
    "dispatch-report-v2.json",
    "production-canary-state-v3.json",
    "production-canary-terminal-receipt-v3.json",
    "production-canary-terminal-index-v3.json",
)

RAW_CHAIN_SCHEMA_NAMES = (
    "model-stage-raw-chunk-v1.json",
    "model-stage-raw-chain-manifest-v1.json",
)


def _load_schema(name: str) -> dict:
    return json.loads((ROOT / "schemas" / name).read_text(encoding="utf-8"))


def _schema_errors(name: str, value: object, *, check_only: bool = False) -> list[dict]:
    if not JSONSCHEMA_PYTHON.is_file():
        raise AssertionError("jsonschema_validation_python_missing")
    request = {
        "schema": _load_schema(name),
        "instance": value,
        "check_only": check_only,
    }
    script = r"""
import json, sys
from jsonschema import Draft202012Validator, FormatChecker
request = json.load(sys.stdin)
Draft202012Validator.check_schema(request["schema"])
errors = [] if request["check_only"] else sorted(
    Draft202012Validator(
        request["schema"], format_checker=FormatChecker()
    ).iter_errors(request["instance"]),
    key=lambda error: tuple(str(item) for item in error.absolute_path),
)
json.dump([
    {"path": list(error.absolute_path), "message": error.message}
    for error in errors
], sys.stdout, ensure_ascii=False, sort_keys=True)
"""
    completed = subprocess.run(
        [str(JSONSCHEMA_PYTHON), "-c", script],
        input=json.dumps(request, ensure_ascii=False, sort_keys=True),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(completed.stderr or "schema_validation_failed")
    return json.loads(completed.stdout)


def _read(path: Path | str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _assert_valid(test: unittest.TestCase, name: str, value: dict) -> None:
    errors = _schema_errors(name, value)
    test.assertEqual(
        errors,
        [],
        msg="\n".join(
            f"{error['path']}: {error['message']}"
            for error in errors
        ),
    )


def _assert_invalid(test: unittest.TestCase, name: str, value: dict) -> None:
    test.assertTrue(
        _schema_errors(name, value),
        msg=f"{name} unexpectedly accepted invalid object",
    )


def _v2_canary_task(index: int, *, subject: str = "cs408") -> FrozenTask:
    frozen = canary_task(
        index,
        subject=subject,
        recorded_at=f"2026-08-11T00:00:{index:02d}Z",
    )
    payload = copy.deepcopy(dict(frozen.frozen_payload))
    contract = payload["dispatch_contract"]["producer_input_contract"]
    contract["schema_version"] = "study-intake-producer-dispatch-input-v2"
    contract["capture_type"] = None
    contract["evidence_contract"] = None
    contract["evidence_contract_sha256"] = None
    core = copy.deepcopy(contract)
    core.pop("producer_input_contract_sha256", None)
    contract["producer_input_contract_sha256"] = sha(core)
    return FrozenTask(payload)


class _OrdinaryRunner:
    def run_analysis(self, frozen, _context):
        return result("analysis", frozen)

    def run_critical_review(self, frozen, _draft, _context):
        return result("critical_review", frozen)

    def cancel(self, _context):
        return None


class BackendSchemaContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _ordinary_objects(self) -> dict[str, dict]:
        runtime = self.root / "ordinary"
        frozen = task(901, subject="english")
        dispatcher = ConcurrentDispatcher(
            runtime,
            lambda _task, _context: _OrdinaryRunner(),
        )
        terminal = dispatcher.submit(frozen).wait(5)
        self.assertEqual(terminal.outcome, "succeeded")
        self.assertIsInstance(terminal.completion, dict)
        completion = copy.deepcopy(dict(terminal.completion or {}))
        package = _read(completion["package_path"])
        report_path = (
            runtime
            / "dispatch/reports/json/sha256"
            / completion["report_json_sha256"][:2]
            / f"{completion['report_json_sha256']}.json"
        )
        report = _read(report_path)
        event_index_root = (
            runtime
            / "dispatch/state/task-events"
            / frozen.unit_sha256
            / "fence-1"
        )
        indexes = [_read(path) for path in sorted(event_index_root.glob("*.json"))]
        published_index = next(row for row in indexes if row["event"] == "published")
        event = _read(published_index["event_path"])
        detail = _read(
            runtime
            / "dispatch/state/task-details"
            / f"{frozen.unit_sha256}.json"
        )
        return {
            "concurrent-completion-v2.json": completion,
            "preprocess-package-v3.json": package,
            "dispatch-report-v2.json": report,
            "dispatch-task-event-v2.json": event,
            "dispatch-task-detail-v2.json": detail,
        }

    def _canary_objects(self, *, failed: bool = False) -> dict[str, dict]:
        runtime = self.root / ("canary-failed" if failed else "canary-success")
        store = LeaseStore(runtime)
        store.begin_subject_drain("cs408")
        store.activate_production_canary(
            "cs408",
            release_id="a" * 64,
            producer_authority=authority("cs408", "a" * 64),
            activated_at="2026-08-11T00:00:00Z",
        )
        frozen = _v2_canary_task(2 if failed else 1)
        store.materialize_production_canary_task(frozen)
        inner_factory = (
            (lambda _task, _context: FailedRunner())
            if failed
            else (lambda _task, _context: GroundedRunner())
        )
        dispatcher = ConcurrentDispatcher(
            runtime,
            lambda frozen_task, context: IdentityPublishingFixtureRunner(
                inner_factory(frozen_task, context),
                store,
            ),
            production_canary=True,
            stage_timeout_seconds=2,
        )
        terminal = dispatcher.submit(frozen).wait(5)
        self.assertEqual(terminal.outcome, "failed" if failed else "succeeded")
        state = store.production_canary_status("cs408")
        receipt = _read(state["last_terminal_receipt_path"])
        index = _read(state["terminal_index_path"])
        self.assertIsInstance(terminal.completion, dict)
        completion = copy.deepcopy(dict(terminal.completion or {}))
        detail = _read(
            runtime
            / "dispatch/state/task-details"
            / f"{frozen.unit_sha256}.json"
        )
        event_index_root = (
            runtime
            / "dispatch/state/task-events"
            / frozen.unit_sha256
            / "fence-1"
        )
        indexes = [_read(path) for path in sorted(event_index_root.glob("*.json"))]
        terminal_event = _read(
            next(
                row for row in indexes
                if row["event"] == ("failed" if failed else "published")
            )["event_path"]
        )
        return {
            "production-canary-state-v3.json": state,
            "production-canary-terminal-receipt-v3.json": receipt,
            "production-canary-terminal-index-v3.json": index,
            "concurrent-completion-v2.json": completion,
            "dispatch-task-event-v2.json": terminal_event,
            "dispatch-task-detail-v2.json": detail,
        }

    def test_all_eight_schemas_are_draft_2020_12_and_have_no_empty_schema(self) -> None:
        def walk(value: object, path: str) -> list[str]:
            found: list[str] = []
            if isinstance(value, dict):
                if not value:
                    found.append(path)
                for key, item in value.items():
                    found.extend(walk(item, f"{path}/{key}"))
            elif isinstance(value, list):
                for index, item in enumerate(value):
                    found.extend(walk(item, f"{path}/{index}"))
            return found

        for name in SCHEMA_NAMES + RAW_CHAIN_SCHEMA_NAMES:
            with self.subTest(schema=name):
                schema = _load_schema(name)
                self.assertEqual(
                    schema["$schema"],
                    "https://json-schema.org/draft/2020-12/schema",
                )
                self.assertEqual(_schema_errors(name, {}, check_only=True), [])
                self.assertEqual(walk(schema, "$"), [])
                self.assertFalse(schema["additionalProperties"])

    def test_raw_chain_schemas_are_strict_and_reject_tampering(self) -> None:
        empty_sha = hashlib.sha256(b"").hexdigest()
        payload = b"first-provider-chunk"
        payload_sha = hashlib.sha256(payload).hexdigest()
        binding = {
            "attempt": 1,
            "capture_id": "RAW-CHAIN-TEST",
            "frozen_payload_sha256": "1" * 64,
            "lease_fence": 1,
            "owner_id": "dispatcher-raw-chain-test",
            "provider_process_identity_sha256": "a" * 64,
            "provider_stage_name": "math_analysis",
            "release_id": "2" * 64,
            "stage_name": "analysis",
            "subject": "math",
            "unit_sha256": "3" * 64,
        }
        chunk = {
            "schema_version": "study-intake-model-stage-raw-chunk-v1",
            "execution_binding": binding,
            "stage_name": "analysis",
            "provider_stage_name": "math_analysis",
            "stream": "stdout",
            "sequence": 1,
            "previous_chunk_sha256": None,
            "provider_process_identity_sha256": "a" * 64,
            "offset_start": 0,
            "offset_end": len(payload),
            "cumulative_size": len(payload),
            "chunk_size": len(payload),
            "chunk_sha256": payload_sha,
            "chunk_base64": base64.b64encode(payload).decode("ascii"),
            "chunk_semantics": "append",
            "written_at": "2026-08-12T00:00:00Z",
            "formal_write_count": 0,
            "authority": {
                "schema_version": "study-intake-dispatch-authority-v1",
                "algorithm": "HMAC-SHA256",
                "key_id": "4" * 64,
                "purpose": "study-intake-model-stage-raw-chunk",
                "hmac_sha256": "5" * 64,
            },
        }
        manifest = {
            "schema_version": "study-intake-model-stage-raw-chain-manifest-v1",
            "execution_binding": binding,
            "stage_name": "analysis",
            "provider_stage_name": "math_analysis",
            "chains": {
                "stdout": {
                    "chunk_count": 1,
                    "chunk_semantics": "append",
                    "head_chunk_sha256": "6" * 64,
                    "reconstructed_sha256": payload_sha,
                    "total_size": len(payload),
                },
                "stderr": {
                    "chunk_count": 0,
                    "chunk_semantics": "append",
                    "head_chunk_sha256": None,
                    "reconstructed_sha256": empty_sha,
                    "total_size": 0,
                },
                "output_last_message_checkpoint": {
                    "chunk_count": 0,
                    "chunk_semantics": "snapshot",
                    "head_chunk_sha256": None,
                    "reconstructed_sha256": empty_sha,
                    "total_size": 0,
                },
            },
            "total_chunk_count": 1,
            "reconstruction_sha256": "7" * 64,
            "output_last_message_final_sha256": empty_sha,
            "output_last_message_final_size": 0,
            "provider_process_identity_sha256": "a" * 64,
            "provider_returncode": 0,
            "provider_schema_sha256": "8" * 64,
            "completed_at": "2026-08-12T00:00:01Z",
            "formal_write_count": 0,
            "authority": {
                "schema_version": "study-intake-dispatch-authority-v1",
                "algorithm": "HMAC-SHA256",
                "key_id": "4" * 64,
                "purpose": "study-intake-model-stage-raw-chain-manifest",
                "hmac_sha256": "9" * 64,
            },
        }
        for name, value in (
            (RAW_CHAIN_SCHEMA_NAMES[0], chunk),
            (RAW_CHAIN_SCHEMA_NAMES[1], manifest),
        ):
            schema = _load_schema(name)
            self.assertEqual(_schema_errors(name, {}, check_only=True), [])
            self.assertFalse(schema["additionalProperties"])
            _assert_valid(self, name, value)
            missing = copy.deepcopy(value)
            missing.pop("execution_binding")
            _assert_invalid(self, name, missing)
            wrong_purpose = copy.deepcopy(value)
            wrong_purpose["authority"]["purpose"] = "wrong-purpose"
            _assert_invalid(self, name, wrong_purpose)
            fake_sha = copy.deepcopy(value)
            fake_sha["authority"]["hmac_sha256"] = "fake"
            _assert_invalid(self, name, fake_sha)

        wrong_first_link = copy.deepcopy(chunk)
        wrong_first_link["previous_chunk_sha256"] = "a" * 64
        _assert_invalid(self, RAW_CHAIN_SCHEMA_NAMES[0], wrong_first_link)

        wrong_stage_binding = copy.deepcopy(chunk)
        wrong_stage_binding["execution_binding"]["provider_stage_name"] = "math_critical_review"
        _assert_invalid(self, RAW_CHAIN_SCHEMA_NAMES[0], wrong_stage_binding)

        wrong_empty_chain = copy.deepcopy(manifest)
        wrong_empty_chain["chains"]["stderr"]["head_chunk_sha256"] = "b" * 64
        _assert_invalid(self, RAW_CHAIN_SCHEMA_NAMES[1], wrong_empty_chain)

    def test_real_zero_model_builders_round_trip_all_eight_root_schemas(self) -> None:
        objects = self._ordinary_objects()
        objects.update(self._canary_objects())
        self.assertEqual(set(objects), set(SCHEMA_NAMES))
        for name, value in objects.items():
            with self.subTest(schema=name):
                _assert_valid(self, name, value)

    def test_real_failed_canary_builders_round_trip_non_success_branches(self) -> None:
        for name, value in self._canary_objects(failed=True).items():
            with self.subTest(schema=name):
                _assert_valid(self, name, value)

    def test_missing_key_wrong_type_and_fake_sha_fail_closed(self) -> None:
        objects = self._ordinary_objects()
        objects.update(self._canary_objects())
        typed_keys = {
            "dispatch-task-event-v2.json": "attempt",
            "dispatch-task-detail-v2.json": "attempt",
            "concurrent-completion-v2.json": "lease_fence",
            "preprocess-package-v3.json": "lease_fence",
            "dispatch-report-v2.json": "formal_write_count",
            "production-canary-state-v3.json": "active_task_count",
            "production-canary-terminal-receipt-v3.json": "observed_model_call_count",
            "production-canary-terminal-index-v3.json": "terminal_task_count",
        }
        for name, value in objects.items():
            with self.subTest(schema=name, mutation="missing"):
                mutated = copy.deepcopy(value)
                mutated.pop("schema_version")
                _assert_invalid(self, name, mutated)
            with self.subTest(schema=name, mutation="wrong-type"):
                mutated = copy.deepcopy(value)
                mutated[typed_keys[name]] = "not-the-required-type"
                _assert_invalid(self, name, mutated)
            with self.subTest(schema=name, mutation="fake-sha"):
                mutated = copy.deepcopy(value)
                mutated["unit_sha256"] = "not-a-sha256"
                _assert_invalid(self, name, mutated)

    def test_authority_purpose_is_exact_for_every_signed_root(self) -> None:
        objects = self._ordinary_objects()
        objects.update(self._canary_objects())
        signed_names = {
            "dispatch-task-event-v2.json",
            "dispatch-task-detail-v2.json",
            "concurrent-completion-v2.json",
            "production-canary-state-v3.json",
            "production-canary-terminal-receipt-v3.json",
            "production-canary-terminal-index-v3.json",
        }
        for name in signed_names:
            with self.subTest(schema=name):
                mutated = copy.deepcopy(objects[name])
                mutated["authority"]["purpose"] = "wrong-purpose"
                _assert_invalid(self, name, mutated)

    def test_conditional_branches_reject_downgrade_and_cross_shape_mutations(self) -> None:
        ordinary = self._ordinary_objects()
        canary = self._canary_objects()

        completion = copy.deepcopy(ordinary["concurrent-completion-v2.json"])
        completion["outcome"] = "failed"
        _assert_invalid(self, "concurrent-completion-v2.json", completion)

        event = copy.deepcopy(ordinary["dispatch-task-event-v2.json"])
        event["error_code"] = "synthetic-error"
        _assert_invalid(self, "dispatch-task-event-v2.json", event)

        detail = copy.deepcopy(ordinary["dispatch-task-detail-v2.json"])
        detail["public_dashboard_task_detail"] = {"queue_state": "running"}
        _assert_invalid(self, "dispatch-task-detail-v2.json", detail)

        package = copy.deepcopy(ordinary["preprocess-package-v3.json"])
        package["pipeline"] = ["critical_review", "analysis"]
        _assert_invalid(self, "preprocess-package-v3.json", package)

        state = copy.deepcopy(canary["production-canary-state-v3.json"])
        state["state"] = "canary_in_flight"
        state["active_task_count"] = 0
        _assert_invalid(self, "production-canary-state-v3.json", state)

        receipt = copy.deepcopy(
            canary["production-canary-terminal-receipt-v3.json"]
        )
        receipt["late_result_fenced"] = True
        _assert_invalid(
            self,
            "production-canary-terminal-receipt-v3.json",
            receipt,
        )

        index = copy.deepcopy(canary["production-canary-terminal-index-v3.json"])
        unit = next(iter(index["units"].values()))
        unit["report_reopen_status"] = "not_available"
        _assert_invalid(self, "production-canary-terminal-index-v3.json", index)


if __name__ == "__main__":
    unittest.main()
