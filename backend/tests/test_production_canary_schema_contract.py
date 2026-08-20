from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import os
import subprocess
import tempfile
import threading
import unittest
import uuid
from pathlib import Path

import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "tests"))

from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    DispatchError,
    LeaseStore,
    validate_no_fast_mode_provider_argv,
    validate_no_fast_mode_provider_environment,
)
from process_identity import kernel_process_start_token  # noqa: E402
from test_production_canary_admission import (  # noqa: E402
    BlockingGroundedRunner,
    GroundedRunner,
    IdentityPublishingFixtureRunner,
    authority,
    canary_task,
)


SCHEMAS = {
    name: json.loads((ROOT / "schemas" / f"{name}.json").read_text(encoding="utf-8"))
    for name in (
        "producer-dispatch-input-v1",
        "producer-dispatch-input-v2",
        "dispatch-report-v1",
        "dispatch-report-v2",
        "production-canary-activation-receipt-v1",
        "production-canary-exclusion-v1",
        "production-canary-gate-receipt-v1",
        "production-canary-preclaim-failure-receipt-v1",
        "production-canary-queue-entry-v1",
        "production-canary-state-v1",
        "production-canary-terminal-receipt-v1",
        "production-canary-activation-receipt-v2",
        "production-canary-concurrency-telemetry-v1",
        "production-canary-gate-receipt-v2",
        "production-canary-preclaim-failure-receipt-v2",
        "production-canary-preclaim-repair-ack-receipt-v2",
        "production-canary-queue-entry-v2",
        "production-canary-state-v2",
        "production-canary-state-v3",
        "production-canary-terminal-index-v2",
        "production-canary-terminal-index-v3",
        "production-canary-terminal-receipt-v2",
        "production-canary-terminal-receipt-v3",
        "provider-process-exit-v1",
        "provider-process-identity-v1",
        "subject-background-luna-batch-archive-v1",
        "subject-background-luna-rollover-pointer-v1",
        "subject-background-luna-rollover-receipt-v1",
        "task-process-exit-v1",
        "task-process-identity-v1",
    )
}
JSONSCHEMA_PYTHON = Path("/opt/miniconda3/envs/dl/bin/python")


def validate(name: str, value: object) -> None:
    schema = SCHEMAS[name]
    if not isinstance(value, dict):
        raise AssertionError("schema instance is not an object")
    required = set(schema.get("required") or [])
    properties = schema.get("properties") or {}
    missing = required - set(value)
    extra = set(value) - set(properties)
    if missing:
        raise AssertionError(f"missing schema keys: {sorted(missing)}")
    if schema.get("additionalProperties") is False and extra:
        raise AssertionError(f"extra schema keys: {sorted(extra)}")
    for key, rule in properties.items():
        if key not in value or not isinstance(rule, dict):
            continue
        if "const" in rule and value[key] != rule["const"]:
            raise AssertionError(f"{key} violates const")
        if "enum" in rule and value[key] not in rule["enum"]:
            raise AssertionError(f"{key} violates enum")


def validate_draft202012(name: str, value: object) -> list[dict[str, object]]:
    if not JSONSCHEMA_PYTHON.is_file():
        raise AssertionError("jsonschema validator runtime missing")
    request = json.dumps(
        {"schema": SCHEMAS[name], "instance": value},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    script = r"""
import json, sys
from jsonschema import Draft202012Validator
request = json.load(sys.stdin)
Draft202012Validator.check_schema(request['schema'])
errors = sorted(
    Draft202012Validator(request['schema']).iter_errors(request['instance']),
    key=lambda error: tuple(str(part) for part in error.absolute_path),
)
json.dump([
    {
        'path': list(error.absolute_path),
        'validator': error.validator,
        'message': error.message,
    }
    for error in errors
], sys.stdout, separators=(',', ':'))
"""
    result = subprocess.run(
        [str(JSONSCHEMA_PYTHON), "-c", script],
        input=request,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(result.stderr or "jsonschema validator failed")
    return json.loads(result.stdout)


class ProductionCanarySchemaContractTests(unittest.TestCase):
    def test_provider_process_receipts_roundtrip_and_tamper_rejected(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp:
            runtime = Path(temp) / "runtime"
            store = LeaseStore(runtime)
            task = canary_task(90)
            owner_id = f"dispatcher-{os.getpid()}-{'1' * 32}"
            decision = store.claim(
                task.unit_sha256,
                owner_id,
                subject="math",
                task=task,
            )
            lease = decision.lease
            assert lease is not None
            context_root = (
                runtime
                / "dispatch"
                / "contexts"
                / task.unit_sha256
                / f"fence-{lease.fence}"
            )
            context_root.mkdir(parents=True, exist_ok=True)
            supervisor = subprocess.Popen(
                ["/bin/sh", "-c", "read line"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            launched_at = (
                dt.datetime.now(dt.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
            supervisor_refs = store.publish_task_process_identity(
                task,
                lease,
                child_pid=supervisor.pid,
                child_pgid=os.getpgid(supervisor.pid),
                process_start_token=kernel_process_start_token(supervisor.pid),
                launch_nonce=uuid.uuid4().hex,
                launched_at=launched_at,
                argv=["/bin/sh", "-c", "read line"],
                executable_path=Path("/bin/sh"),
                start_new_session=True,
            )
            provider = subprocess.Popen(
                ["/bin/sh", "-c", "read provider_line"],
                cwd=context_root,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            provider_refs = store.publish_provider_process_identity(
                task,
                lease,
                stage_name="math_analysis",
                provider_pid=provider.pid,
                provider_pgid=os.getpgid(provider.pid),
                process_start_token=kernel_process_start_token(provider.pid),
                launch_nonce=uuid.uuid4().hex,
                launched_at=(
                    dt.datetime.now(dt.timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                ),
                argv=["/bin/sh", "-c", "read provider_line"],
                environment=dict(os.environ),
                executable_path=Path("/bin/sh"),
                cwd=context_root,
                start_new_session=True,
            )
            provider_stdout, provider_stderr = provider.communicate(b"ok\n")
            provider_exit_refs = store.publish_provider_process_exit(
                task,
                lease,
                stage_name="math_analysis",
                provider_process_identity_sha256=provider_refs[
                    "provider_process_identity_sha256"
                ],
                provider_process_identity_path=provider_refs[
                    "provider_process_identity_path"
                ],
                returncode=int(provider.returncode or 0),
                termination_reason="completed",
                reaped=True,
                process_absent=True,
                pgid_absent=True,
                finished_at=(
                    dt.datetime.now(dt.timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                ),
            )
            validate(
                "provider-process-identity-v1",
                provider_refs["provider_process_identity"],
            )
            self.assertEqual(
                validate_draft202012(
                    "task-process-identity-v1",
                    supervisor_refs["process_identity"],
                ),
                [],
            )
            self.assertEqual(
                validate_draft202012(
                    "provider-process-identity-v1",
                    provider_refs["provider_process_identity"],
                ),
                [],
            )
            identity = provider_refs["provider_process_identity"]
            self.assertEqual(
                identity["argv_policy_version"],
                "study-intake-provider-argv-no-fast-mode-v1",
            )
            self.assertEqual(
                identity["environment_key_names"], sorted(os.environ)
            )
            self.assertEqual(
                identity["forbidden_environment_key_matches"], []
            )
            self.assertEqual(
                identity["forbidden_environment_value_key_matches"], []
            )
            validate(
                "provider-process-exit-v1",
                provider_exit_refs["provider_process_exit"],
            )
            self.assertEqual(
                validate_draft202012(
                    "provider-process-exit-v1",
                    provider_exit_refs["provider_process_exit"],
                ),
                [],
            )
            forged = copy.deepcopy(
                provider_exit_refs["provider_process_exit"]
            )
            forged["termination_reason"] = "cancelled"
            with self.assertRaises(DispatchError):
                store._verify_seal(
                    forged, purpose="dispatch-provider-process-exit"
                )
            supervisor_stdout, supervisor_stderr = supervisor.communicate(
                b"ok\n"
            )
            task_exit = store.publish_task_process_exit(
                task,
                lease,
                process_identity_sha256=supervisor_refs[
                    "process_identity_sha256"
                ],
                process_identity_path=supervisor_refs[
                    "process_identity_path"
                ],
                returncode=int(supervisor.returncode or 0),
                termination_reason="completed",
                reaped=True,
                process_absent=True,
                pgid_absent=True,
                stdout_sha256=hashlib.sha256(supervisor_stdout).hexdigest(),
                stdout_size=len(supervisor_stdout),
                stderr_sha256=hashlib.sha256(supervisor_stderr).hexdigest(),
                stderr_size=len(supervisor_stderr),
                finished_at=(
                    dt.datetime.now(dt.timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                ),
            )
            self.assertEqual(provider_stdout, b"")
            self.assertEqual(provider_stderr, b"")
            validate("task-process-exit-v1", task_exit["task_process_exit"])
            self.assertEqual(
                validate_draft202012(
                    "task-process-exit-v1",
                    task_exit["task_process_exit"],
                ),
                [],
            )

            process_samples = {
                "task-process-identity-v1": supervisor_refs[
                    "process_identity"
                ],
                "task-process-exit-v1": task_exit["task_process_exit"],
                "provider-process-identity-v1": provider_refs[
                    "provider_process_identity"
                ],
                "provider-process-exit-v1": provider_exit_refs[
                    "provider_process_exit"
                ],
            }
            malformed: list[tuple[str, dict[str, object]]] = []
            for name, sample in process_samples.items():
                missing = copy.deepcopy(sample)
                missing.pop("unit_sha256")
                malformed.append((name, missing))
                bad_hash = copy.deepcopy(sample)
                bad_hash["frozen_payload_sha256"] = "not-a-sha256"
                malformed.append((name, bad_hash))
                bad_authority = copy.deepcopy(sample)
                bad_authority["authority"]["purpose"] = "wrong-purpose"
                malformed.append((name, bad_authority))
            bad_task_pid = copy.deepcopy(supervisor_refs["process_identity"])
            bad_task_pid["child_pid"] = "123"
            malformed.append(("task-process-identity-v1", bad_task_pid))
            bad_task_path = copy.deepcopy(supervisor_refs["process_identity"])
            bad_task_path["context_root"] = "relative/context"
            malformed.append(("task-process-identity-v1", bad_task_path))
            bad_task_token = copy.deepcopy(supervisor_refs["process_identity"])
            bad_task_token["process_start_token"] = "second-resolution-token"
            malformed.append(("task-process-identity-v1", bad_task_token))
            bad_task_exit = copy.deepcopy(task_exit["task_process_exit"])
            bad_task_exit["reaped"] = False
            malformed.append(("task-process-exit-v1", bad_task_exit))
            bad_task_fence = copy.deepcopy(task_exit["task_process_exit"])
            bad_task_fence["lease_fence"] = 0
            malformed.append(("task-process-exit-v1", bad_task_fence))
            bad_provider_argv = copy.deepcopy(
                provider_refs["provider_process_identity"]
            )
            bad_provider_argv["argv"] = []
            malformed.append(
                ("provider-process-identity-v1", bad_provider_argv)
            )
            bad_provider_tier = copy.deepcopy(
                provider_refs["provider_process_identity"]
            )
            bad_provider_tier["requested_service_tier"] = "priority"
            malformed.append(
                ("provider-process-identity-v1", bad_provider_tier)
            )
            bad_provider_fast = copy.deepcopy(
                provider_refs["provider_process_identity"]
            )
            bad_provider_fast["fast_mode_requested"] = True
            malformed.append(
                ("provider-process-identity-v1", bad_provider_fast)
            )
            bad_provider_exit = copy.deepcopy(
                provider_exit_refs["provider_process_exit"]
            )
            bad_provider_exit["termination_reason"] = "cancelled"
            malformed.append(("provider-process-exit-v1", bad_provider_exit))
            bad_provider_absence = copy.deepcopy(
                provider_exit_refs["provider_process_exit"]
            )
            bad_provider_absence["pgid_absent"] = False
            malformed.append(
                ("provider-process-exit-v1", bad_provider_absence)
            )
            for name, value in malformed:
                with self.subTest(schema=name, mutation=value):
                    self.assertTrue(
                        validate_draft202012(name, value),
                        f"{name} accepted malformed process evidence",
                    )

    def test_provider_argv_and_environment_fast_mode_fail_closed(self) -> None:
        accepted = validate_no_fast_mode_provider_argv(
            ["codex", "exec", "-c", 'model="gpt-5.6-luna"']
        )
        self.assertIsNone(accepted["requested_service_tier"])
        self.assertFalse(accepted["fast_mode_requested"])
        for argv in (
            ["codex", "exec", "--service-tier", "priority"],
            ["codex", "exec", "-c", 'service_tier="priority"'],
            ["codex", "exec", "-c", 'service-tier="priority"'],
            ["codex", "exec", "priority"],
        ):
            with self.subTest(argv=argv), self.assertRaisesRegex(
                DispatchError, "provider_fast_mode_argv_forbidden"
            ):
                validate_no_fast_mode_provider_argv(argv)
        env = validate_no_fast_mode_provider_environment(
            {"PATH": "/bin", "LANG": "C"}
        )
        self.assertEqual(env["environment_key_names"], ["LANG", "PATH"])
        with self.assertRaisesRegex(
            DispatchError, "provider_fast_mode_environment_forbidden"
        ):
            validate_no_fast_mode_provider_environment(
                {"PATH": "/bin", "CODEX_SERVICE_TIER": "priority"}
            )
        with self.assertRaisesRegex(
            DispatchError, "provider_fast_mode_environment_forbidden"
        ):
            validate_no_fast_mode_provider_environment(
                {
                    "PATH": "/bin",
                    "CODEX_CONFIG": 'model_provider.service_tier="priority"',
                }
            )

    def test_preclaim_failure_receipt_and_state_validate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            runtime = Path(temp) / "runtime"
            store = LeaseStore(runtime)
            store.begin_subject_drain("math")
            store.activate_production_canary(
                "math",
                release_id="a" * 64,
                producer_authority=authority("math", "a" * 64),
                activated_at="2026-08-11T00:00:00Z",
            )
            task = canary_task(72)
            queue = store.materialize_production_canary_task(task)
            result = store.fail_production_canary_preclaim(
                "math",
                task,
                failure_stage="submit_generation_fence",
                error_code="synthetic_generation_fence_failure",
            )
            state = store.production_canary_status("math")
            validate("production-canary-state-v3", state)
            self.assertEqual(
                validate_draft202012("production-canary-state-v3", state),
                [],
            )
            receipt = json.loads(
                Path(result["preclaim_failure_receipt_path"]).read_text(
                    encoding="utf-8"
                )
            )
            validate(
                "production-canary-preclaim-failure-receipt-v2", receipt
            )
            self.assertEqual(
                validate_draft202012(
                    "production-canary-preclaim-failure-receipt-v2",
                    receipt,
                ),
                [],
            )
            self.assertEqual(receipt["failure_stage"], "submit_generation_fence")
            self.assertTrue(receipt["queue_entry_preserved"])
            self.assertEqual(receipt["queue_status_after"], "pending")
            self.assertFalse(receipt["model_submission_started"])
            self.assertEqual(
                state["last_preclaim_failure_receipt_sha256"],
                result["preclaim_failure_receipt_sha256"],
            )
            reopened_queue = json.loads(
                store._production_canary_queue_path(
                    "math", queue["producer_input_contract_sha256"]
                ).read_text(encoding="utf-8")
            )
            validate("production-canary-queue-entry-v2", reopened_queue)
            self.assertEqual(
                validate_draft202012(
                    "production-canary-queue-entry-v2", reopened_queue
                ),
                [],
            )
            resumed = store.resume_production_canary("math")
            ack_sha256 = resumed["preclaim_failure_resume_ack_sha256"]
            ack_path = (
                store.production_canary_receipt_root
                / "math"
                / resumed["activation_id"]
                / "sha256"
                / ack_sha256[:2]
                / f"{ack_sha256}.json"
            )
            repair_ack = json.loads(ack_path.read_text(encoding="utf-8"))
            validate(
                "production-canary-preclaim-repair-ack-receipt-v2",
                repair_ack,
            )
            self.assertEqual(
                validate_draft202012(
                    "production-canary-preclaim-repair-ack-receipt-v2",
                    repair_ack,
                ),
                [],
            )

            malformed: list[tuple[str, dict[str, object]]] = []
            for name, sample, required_key in (
                (
                    "production-canary-preclaim-failure-receipt-v2",
                    receipt,
                    "failure_id",
                ),
                (
                    "production-canary-preclaim-repair-ack-receipt-v2",
                    repair_ack,
                    "repair_ack_id",
                ),
            ):
                missing = copy.deepcopy(sample)
                missing.pop(required_key)
                malformed.append((name, missing))
                bad_sha = copy.deepcopy(sample)
                bad_sha[required_key] = "not-a-sha256"
                malformed.append((name, bad_sha))
                bad_authority = copy.deepcopy(sample)
                bad_authority["authority"]["purpose"] = "wrong-purpose"
                malformed.append((name, bad_authority))
            bad_evidence = copy.deepcopy(receipt)
            bad_evidence["failure_evidence"]["evidence_sha256"] = "f" * 63
            malformed.append(
                ("production-canary-preclaim-failure-receipt-v2", bad_evidence)
            )
            bad_stage = copy.deepcopy(receipt)
            bad_stage["failure_stage"] = "provider_started"
            malformed.append(
                ("production-canary-preclaim-failure-receipt-v2", bad_stage)
            )
            bad_ack_type = copy.deepcopy(repair_ack)
            bad_ack_type["unit_sha256"] = 7
            malformed.append(
                (
                    "production-canary-preclaim-repair-ack-receipt-v2",
                    bad_ack_type,
                )
            )
            for name, value in malformed:
                with self.subTest(schema=name, mutation=value):
                    self.assertTrue(
                        validate_draft202012(name, value),
                        f"{name} accepted malformed preclaim evidence",
                    )

    def test_emergency_terminal_and_paused_state_validate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            runtime = Path(temp) / "runtime"
            store = LeaseStore(runtime)
            store.begin_subject_drain("math")
            store.activate_production_canary(
                "math",
                release_id="a" * 64,
                producer_authority=authority("math", "a" * 64),
                activated_at="2026-08-11T00:00:00Z",
            )
            task = canary_task(71)
            store.materialize_production_canary_task(task)
            entered = threading.Event()
            release = threading.Event()
            dispatcher = ConcurrentDispatcher(
                runtime,
                lambda _task, _context: IdentityPublishingFixtureRunner(
                    BlockingGroundedRunner(entered, release), store
                ),
                stage_timeout_seconds=2,
                production_canary=True,
            )
            handle = dispatcher.submit(task)
            self.assertTrue(entered.wait(1))
            self.assertEqual(
                dispatcher.emergency_cancel(timeout=1)[
                    "late_result_fence_status"
                ],
                "sealed",
            )
            self.assertEqual(handle.wait(1).outcome, "cancelled")
            state = store.production_canary_status("math")
            validate("production-canary-state-v3", state)
            self.assertEqual(
                validate_draft202012("production-canary-state-v3", state),
                [],
            )
            terminal = json.loads(
                Path(state["last_terminal_receipt_path"]).read_text(
                    encoding="utf-8"
                )
            )
            validate("production-canary-terminal-receipt-v3", terminal)
            self.assertEqual(
                validate_draft202012(
                    "production-canary-terminal-receipt-v3", terminal
                ),
                [],
            )
            self.assertEqual(
                terminal["terminal_kind"], "emergency_hard_cancel"
            )
            paused = store.pause_production_canary("math")
            validate("production-canary-state-v3", paused)
            self.assertEqual(
                validate_draft202012("production-canary-state-v3", paused),
                [],
            )
            release.set()

    def test_runtime_objects_validate_and_safety_mutation_rejects(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            runtime = Path(temp) / "runtime"
            store = LeaseStore(runtime)
            store.begin_subject_drain("math")
            state = store.activate_production_canary(
                "math",
                release_id="a" * 64,
                producer_authority=authority("math", "a" * 64),
                activated_at="2026-08-11T00:00:00Z",
            )
            validate("production-canary-state-v3", state)
            self.assertEqual(
                validate_draft202012("production-canary-state-v3", state),
                [],
            )
            activation = json.loads(
                Path(state["activation_receipt_path"]).read_text(encoding="utf-8")
            )
            validate("production-canary-activation-receipt-v2", activation)
            self.assertEqual(
                validate_draft202012(
                    "production-canary-activation-receipt-v2", activation
                ),
                [],
            )

            old = canary_task(
                0,
                recorded_at="2026-08-11T00:00:00Z",
                capture_id="SCHEMA-OLD",
            )
            exclusion = store.record_production_canary_pre_activation_exclusion(old)
            validate("production-canary-exclusion-v1", exclusion)

            task = canary_task(1)
            producer_contract = task.frozen_payload["dispatch_contract"][
                "producer_input_contract"
            ]
            validate("producer-dispatch-input-v2", producer_contract)
            queue = store.materialize_production_canary_task(task)
            validate("production-canary-queue-entry-v2", queue)
            self.assertEqual(
                validate_draft202012(
                    "production-canary-queue-entry-v2", queue
                ),
                [],
            )

            dispatcher = ConcurrentDispatcher(
                runtime,
                lambda _task, _context: IdentityPublishingFixtureRunner(
                    GroundedRunner(), store
                ),
                stage_timeout_seconds=2,
                production_canary=True,
            )
            self.assertEqual(dispatcher.submit(task).wait(5).outcome, "succeeded")
            queue = json.loads(
                store._production_canary_queue_path(
                    "math", producer_contract["producer_input_contract_sha256"]
                ).read_text(encoding="utf-8")
            )
            validate("production-canary-queue-entry-v2", queue)
            self.assertEqual(
                validate_draft202012(
                    "production-canary-queue-entry-v2", queue
                ),
                [],
            )
            gate = json.loads(
                Path(queue["canary_gate_path"]).read_text(encoding="utf-8")
            )
            validate("production-canary-gate-receipt-v2", gate)
            self.assertEqual(
                validate_draft202012(
                    "production-canary-gate-receipt-v2", gate
                ),
                [],
            )
            state = store.production_canary_status("math")
            validate("production-canary-state-v3", state)
            self.assertEqual(
                validate_draft202012("production-canary-state-v3", state),
                [],
            )
            terminal = json.loads(
                Path(state["last_terminal_receipt_path"]).read_text(encoding="utf-8")
            )
            validate("production-canary-terminal-receipt-v3", terminal)
            self.assertEqual(
                terminal["report_reopen_status"],
                "json_markdown_package_verified",
            )
            report_json_sha256 = terminal["report_json_sha256"]
            report_json_path = (
                runtime
                / "dispatch"
                / "reports"
                / "json"
                / "sha256"
                / report_json_sha256[:2]
                / f"{report_json_sha256}.json"
            )
            report_markdown_sha256 = terminal["report_markdown_sha256"]
            report_markdown_path = (
                runtime
                / "dispatch"
                / "reports"
                / "markdown"
                / "sha256"
                / report_markdown_sha256[:2]
                / f"{report_markdown_sha256}.md"
            )
            report_json_bytes = report_json_path.read_bytes()
            report_markdown_bytes = report_markdown_path.read_bytes()
            self.assertEqual(
                hashlib.sha256(report_json_bytes).hexdigest(),
                report_json_sha256,
            )
            self.assertEqual(
                hashlib.sha256(report_markdown_bytes).hexdigest(),
                report_markdown_sha256,
            )
            validate("dispatch-report-v2", json.loads(report_json_bytes))
            self.assertTrue(report_markdown_bytes.strip())
            self.assertNotIn(str(runtime), terminal["report_json_ref"])
            self.assertNotIn(str(runtime), terminal["report_markdown_ref"])
            validate(
                "production-canary-terminal-index-v3",
                terminal_index := json.loads(
                    Path(state["terminal_index_path"]).read_text(
                        encoding="utf-8"
                    )
                ),
            )
            self.assertEqual(
                validate_draft202012(
                    "production-canary-terminal-index-v3", terminal_index
                ),
                [],
            )
            indexed = terminal_index["units"][task.unit_sha256]
            self.assertEqual(
                indexed["report_json_sha256"], report_json_sha256
            )
            self.assertEqual(
                indexed["report_markdown_sha256"], report_markdown_sha256
            )
            self.assertEqual(
                indexed["package_sha256"], terminal["package_sha256"]
            )
            self.assertEqual(
                indexed["report_reopen_status"],
                "json_markdown_package_verified",
            )
            self.assertNotIn(str(runtime), indexed["report_json_ref"])
            self.assertNotIn(str(runtime), indexed["report_markdown_ref"])
            validate(
                "production-canary-concurrency-telemetry-v1",
                store.production_canary_concurrency_telemetry(
                    release_id="a" * 64
                ),
            )
            process_execution = terminal["process_execution"]
            validate(
                "task-process-identity-v1",
                json.loads(
                    Path(
                        process_execution[
                            "supervisor_process_identity_path"
                        ]
                    ).read_text(encoding="utf-8")
                ),
            )
            validate(
                "task-process-exit-v1",
                json.loads(
                    Path(
                        process_execution["supervisor_process_exit_path"]
                    ).read_text(encoding="utf-8")
                ),
            )

            store.fail_production_canary_post_terminal(
                task, error_code="synthetic_projection_failure"
            )
            failed_state = store.production_canary_status("math")
            validate("production-canary-state-v3", failed_state)
            self.assertEqual(
                validate_draft202012(
                    "production-canary-state-v3", failed_state
                ),
                [],
            )
            failed_terminal = json.loads(
                Path(failed_state["last_terminal_receipt_path"]).read_text(
                    encoding="utf-8"
                )
            )
            validate(
                "production-canary-terminal-receipt-v3", failed_terminal
            )
            self.assertEqual(
                validate_draft202012(
                    "production-canary-terminal-receipt-v3",
                    failed_terminal,
                ),
                [],
            )

            malformed_index = copy.deepcopy(terminal_index)
            malformed_index["units"][task.unit_sha256][
                "terminal_receipt_sha256"
            ] = "bad-sha"
            self.assertTrue(
                validate_draft202012(
                    "production-canary-terminal-index-v3",
                    malformed_index,
                )
            )
            malformed_terminal = copy.deepcopy(terminal)
            malformed_terminal["authority"]["purpose"] = "wrong-purpose"
            self.assertTrue(
                validate_draft202012(
                    "production-canary-terminal-receipt-v3",
                    malformed_terminal,
                )
            )

            strict_samples = {
                "production-canary-activation-receipt-v2": activation,
                "production-canary-gate-receipt-v2": gate,
                "production-canary-queue-entry-v2": queue,
                "production-canary-state-v3": state,
            }
            strict_required_keys = {
                "production-canary-activation-receipt-v2": "activation_id",
                "production-canary-gate-receipt-v2": "selected",
                "production-canary-queue-entry-v2": "task_object_sha256",
                "production-canary-state-v3": "last_terminal_receipt_sha256",
            }
            strict_sha_keys = {
                "production-canary-activation-receipt-v2": "activation_id",
                "production-canary-gate-receipt-v2": "activation_id",
                "production-canary-queue-entry-v2": "task_object_sha256",
                "production-canary-state-v3": "terminal_index_sha256",
            }
            for name, sample in strict_samples.items():
                missing = copy.deepcopy(sample)
                missing.pop(strict_required_keys[name])
                bad_sha = copy.deepcopy(sample)
                bad_sha[strict_sha_keys[name]] = "forged-sha"
                bad_authority = copy.deepcopy(sample)
                bad_authority["authority"]["purpose"] = "wrong-purpose"
                for mutation in (missing, bad_sha, bad_authority):
                    with self.subTest(schema=name, mutation=mutation):
                        self.assertTrue(
                            validate_draft202012(name, mutation),
                            f"{name} accepted malformed current runtime object",
                        )
            bad_gate_selection = copy.deepcopy(gate)
            bad_gate_selection["selected"]["lease_fence"] = "1"
            self.assertTrue(
                validate_draft202012(
                    "production-canary-gate-receipt-v2",
                    bad_gate_selection,
                )
            )
            bad_queue_type = copy.deepcopy(queue)
            bad_queue_type["source_event_ids"] = "not-an-array"
            self.assertTrue(
                validate_draft202012(
                    "production-canary-queue-entry-v2", bad_queue_type
                )
            )
            bad_state_nested = copy.deepcopy(state)
            bad_state_nested["producer_high_watermark"][
                "source_event_set_sha256"
            ] = "0" * 63
            self.assertTrue(
                validate_draft202012(
                    "production-canary-state-v3", bad_state_nested
                )
            )

            forged = copy.deepcopy(failed_state)
            forged["production_accepted"] = True
            with self.assertRaises(AssertionError):
                validate("production-canary-state-v3", forged)

    def test_schema_catalog_is_strict_2020_12(self) -> None:
        for name, schema in SCHEMAS.items():
            with self.subTest(name=name):
                self.assertEqual(
                    schema["$schema"],
                    "https://json-schema.org/draft/2020-12/schema",
                )
                self.assertEqual(schema["type"], "object")
                self.assertIsInstance(schema["required"], list)
                self.assertIsInstance(schema["properties"], dict)
                self.assertFalse(schema["additionalProperties"])


if __name__ == "__main__":
    unittest.main()
