from __future__ import annotations

import copy
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "tests"))

import concurrent_dispatch  # noqa: E402
import math_exact_smoke  # noqa: E402
import release_manager  # noqa: E402
from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    DispatchError,
    FrozenTask,
    LeaseStore,
    dispatch_rule_binding,
)
from core_dispatch_bridge import (  # noqa: E402
    CoreCandidateSubprocessRunner,
    producer_authority_binding,
    scan_eligible_candidates,
)
from math_exact_smoke import (  # noqa: E402
    CHECKSUMS_SHA256,
    EXACT_ORDER,
    EXACT_SAMPLES,
    INITIAL_LEDGER_SHA256,
    MATH_REPO_ROOT,
    SMOKE_ROOT,
    MathExactSmokeError,
    _seal,
    apply_exact_smoke_capture,
    authorize_exact_smoke,
    build_authorization_descriptor,
    preview_exact_smoke,
    pending_exact_smoke_dispatch_scope,
    reopen_execution_authorization,
)
from preprocess_dispatcher import ProductionDispatchRuntime  # noqa: E402
from test_production_canary_admission import (  # noqa: E402
    GroundedRunner,
    IdentityPublishingFixtureRunner,
    authority,
    canary_task,
    sha,
)

REVIEW_FIXTURE = ROOT / "tests/fixtures/review_candidate_task_runner.py"


class TailBarrierGroundedRunner(GroundedRunner):
    """Hold all four post-canary analyses until their claims overlap."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.entered_count = 0
        self.all_entered = threading.Event()
        self.release = threading.Event()

    def run_analysis(self, task, context):
        with self._lock:
            self.entered_count += 1
            if self.entered_count == 4:
                self.all_entered.set()
        if not self.release.wait(10):
            raise RuntimeError("tail-concurrency-barrier-timeout")
        return super().run_analysis(task, context)


class MathExactSmokeAuthorizationTests(unittest.TestCase):
    def test_gs269_terminal_execution_uses_bound_observed_counts(self) -> None:
        terminal = {
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "observed_model_call_count": 1,
            "observed_provider_request_count": 10,
            "observed_mcp_tool_call_count": 9,
            "read_session_id": "MCPRS-MATH-BOUND",
        }
        self.assertTrue(
            math_exact_smoke._gs269_terminal_has_completed_execution(
                terminal
            )
        )
        terminal["observed_mcp_tool_call_count"] = 0
        self.assertFalse(
            math_exact_smoke._gs269_terminal_has_completed_execution(
                terminal
            )
        )

    def test_gs269_duplicate_archive_uses_sealed_rollover_choice(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first_path = root / "first.json"
            second_path = root / "second.json"
            archive_base = {
                "batch_id": "LUNA-MATH-BOUND",
                "batch_sha256": "a" * 64,
                "authority_generation": "math-bound",
                "authority_fingerprint": "b" * 64,
            }
            first_path.write_text("{}", encoding="utf-8")
            second_path.write_text('{"selected":true}', encoding="utf-8")
            matches = [
                (first_path, dict(archive_base)),
                (second_path, dict(archive_base)),
            ]
            receipt = {
                "schema_version": (
                    "subject_background_luna_rollover_receipt_v1"
                ),
                "subject": "math",
                "mode": "explicit_failure_resume",
                "old_batch_id": "LUNA-MATH-BOUND",
                "old_batch_sha256": "a" * 64,
                "archive_path": str(second_path),
                "archive_sha256": hashlib.sha256(
                    second_path.read_bytes()
                ).hexdigest(),
                "resume_acceptance_sha256": "c" * 64,
                "authority_generation": "math-bound",
                "authority_fingerprint": "b" * 64,
                "sol_called": False,
                "sol_enabled": False,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
            }
            selected_path, _ = (
                math_exact_smoke._select_gs269_rollover_archive(
                    matches,
                    receipt,
                    terminal_receipt_sha256="c" * 64,
                )
            )
            self.assertEqual(selected_path, second_path)
            receipt["archive_sha256"] = "d" * 64
            with self.assertRaisesRegex(
                math_exact_smoke.MathExactSmokeError,
                "math_migration_gs269_archive_duplicate",
            ):
                math_exact_smoke._select_gs269_rollover_archive(
                    matches,
                    receipt,
                    terminal_receipt_sha256="c" * 64,
                )

    def descriptor(self) -> dict[str, object]:
        return build_authorization_descriptor(
            target_release_id="1" * 64,
            activation_id="2" * 64,
            authority_generation="math-generation-exact-test",
            authority_fingerprint="3" * 64,
            producer_authority_fingerprint="4" * 64,
        )

    def _isolated_repo(self, root: Path) -> Path:
        repo = root / "repo"
        quick_source = (
            MATH_REPO_ROOT
            / "数学一回滚复习系统/scripts/quick_intake.py"
        )
        quick_target = (
            repo / "数学一回滚复习系统/scripts/quick_intake.py"
        )
        quick_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(quick_source, quick_target)
        shutil.copy2(
            quick_source.with_name("producer_binding_attestation.py"),
            quick_target.with_name("producer_binding_attestation.py"),
        )
        for relative in (
            "数学一回滚复习系统/快速入库事件.jsonl",
            "数学一回滚复习系统/复习单元.json",
        ):
            target = repo / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            source = MATH_REPO_ROOT / relative
            if relative.endswith("快速入库事件.jsonl"):
                ledger = source.read_bytes()
                offset = 0
                preimage = None
                for line in ledger.splitlines(keepends=True):
                    offset += len(line)
                    candidate = ledger[:offset]
                    if hashlib.sha256(candidate).hexdigest() == (
                        INITIAL_LEDGER_SHA256
                    ):
                        preimage = candidate
                        break
                if preimage is None:
                    raise AssertionError("initial exact ledger preimage missing")
                target.write_bytes(preimage)
            else:
                shutil.copy2(source, target)
        for binding in EXACT_SAMPLES.values():
            for relative in (
                binding["formal_card_path"],
                binding["question_path"],
            ):
                source = MATH_REPO_ROOT / str(relative)
                target = repo / str(relative)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target)
        return repo

    def _add_producer_sources(self, repo: Path) -> None:
        for relative in (
            "错题知识网络/可视化错题详情/高等数学/GS-566_138692.md",
            "错题知识网络/可视化错题详情/高等数学/GS-225_强化例题6.10.md",
            "错题知识网络/可视化错题详情/线性代数/LA-050_强化例题3.11(171611).md",
            "错题知识网络/wiki/methods/MATHWIKI-LA-METHOD-010_伴随矩阵与反对称结构.md",
            "错题知识网络/wiki/topics/MATHWIKI-LA-TOPIC-001_线代矩阵运算错题总线.md",
            "错题知识网络/可视化错题详情/线性代数/LA-016_强化例题1.7.md",
            "错题知识网络/可视化错题详情/线性代数/MN4-GS-CH01-654_强化例题1.7.md",
            "错题知识网络/assets/visual_wrong_questions/LA-050/solution_01.png",
            "错题知识网络/assets/visual_wrong_questions/MN4-GS-CH01-654/question_01.png",
            "错题知识网络/生成/知识网络图.mmd",
            "错题知识网络/生成/wrong_questions.json",
            "错题知识网络/知识点库.md",
            "错题知识网络/schema/relationship_signal_policy.json",
        ):
            source = MATH_REPO_ROOT / relative
            target = repo / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)

    def _producer_config(
        self, root: Path, repo: Path, runtime: Path, release_id: str
    ) -> dict[str, object]:
        release_path = root / "release.json"
        release_path.write_text(
            json.dumps(
                {
                    "schema_version": "study-intake-preprocessor-release-v2",
                    "release_id": release_id,
                    "release_profile": "concurrent_v2",
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        fake_codex = root / "no-model-codex"
        fake_codex.write_text(
            "#!/bin/sh\ntouch \"$0.called\"\nexit 97\n",
            encoding="utf-8",
        )
        fake_codex.chmod(0o700)
        return {
            "schema_version": "study-intake-preprocessor-config-v1",
            "runtime_root": str(runtime),
            "timezone": "Asia/Shanghai",
            "release": {"manifest_path": str(release_path)},
            "dispatch": {
                "authority_required": True,
                "heartbeat_interval_seconds": 15,
                "lease_ttl_seconds": 120,
                "infrastructure_recovery_attempts": 1,
                "production_canary": {
                    "enabled": True,
                    "status": "production_canary_active",
                    "admission": "first_post_activation_producer_capture",
                    "keep_backlog_drained": True,
                    "post_activation_only": True,
                    "initial_canary_inflight_limit": 1,
                    "continuous_concurrency_limit": 20,
                },
            },
            "worker": {
                "enabled": True,
                "poll_interval_seconds": 1,
                "debounce_seconds": 0,
                "status_timeout_seconds": 10,
                "model_timeout_seconds": 10,
                "max_attempts": 3,
                "retry_base_seconds": 0,
                "log_path": str(runtime / "logs/worker.log"),
                "lock_path": str(runtime / "state/worker.lock"),
            },
            "model": {
                "codex_path": str(fake_codex),
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
                "output_schema": str(ROOT / "schemas/luna-analysis-v1.json"),
                "prompt_version": "math-exact-smoke-test-v1",
                "max_prompt_bytes": 131072,
                "max_images": 8,
            },
            "math_deep_v2": {
                "enabled": True,
                "mode": "production",
                "canary_percent": 10,
                "shadow_evaluation_start_at": "2026-08-05T00:00:00+08:00",
                "shadow_evaluation_target_count": 20,
                "soft_runtime_warning_seconds": 3600,
                "stall_timeout_seconds": 1800,
                "stall_probe_interval_seconds": 60,
                "stall_probe_required_consecutive_failures": 2,
                "analysis_output_schema": str(
                    ROOT / "schemas/luna-math-analysis-v2.json"
                ),
                "critical_review_output_schema": str(
                    ROOT / "schemas/luna-math-critical-review-v3.json"
                ),
                "package_output_schema": str(
                    ROOT / "schemas/preprocess-package-v2.json"
                ),
                "analysis_prompt_version": "math-exact-smoke-analysis-v1",
                "critical_review_prompt_version": "math-exact-smoke-review-v1",
                "max_prompt_bytes": 524288,
                "max_output_bytes": 262144,
                "min_complete_chinese_chars": 0,
                "max_chinese_chars": 7000,
                "gs111_cold_replay_max_seconds": 1800,
                "three_replay_p95_max_seconds": 1200,
            },
            "math_knowledge_snapshot": {
                "enabled": True,
                "max_source_bytes": 8388608,
                "max_distribution_terms": 64,
                "max_local_neighbors": 8,
                "max_relationship_candidates": 5,
                "max_snapshot_bytes": 131072,
                "sources": {
                    "graph": "错题知识网络/生成/知识网络图.mmd",
                    "projection": "错题知识网络/生成/wrong_questions.json",
                    "taxonomy": "错题知识网络/知识点库.md",
                    "relationship_policy": "错题知识网络/schema/relationship_signal_policy.json",
                },
            },
            "dashboard": {
                "projection_path": str(
                    runtime / "state/dashboard_projection.json"
                ),
                "max_items_per_subject": 200,
            },
            "adapters": {
                "math": {
                    "enabled": True,
                    "adapter_version": "math-exact-smoke-test-v1",
                    "python_path": sys.executable,
                    "repo_root": str(repo),
                    "status_script": str(
                        repo
                        / "数学一回滚复习系统/scripts/quick_intake.py"
                    ),
                },
                "cs408": {
                    "enabled": False,
                    "adapter_version": "cs408-disabled-v1",
                    "python_path": sys.executable,
                    "repo_root": str(root / "cs408-disabled"),
                    "status_script": str(root / "cs408-disabled.py"),
                },
            },
        }

    def _isolated_runtime(
        self, root: Path, descriptor: dict[str, object]
    ) -> tuple[Path, dict[str, object]]:
        runtime = root / "runtime"
        key_path = runtime / "dispatch/state/authority.key"
        key_path.parent.mkdir(parents=True)
        key = b"k" * 32
        key_path.write_bytes(key)
        key_path.chmod(0o600)
        state = _seal(
            {
                "schema_version": "study-intake-production-canary-state-v3",
                "subject": "math",
                "release_id": descriptor["target_release_id"],
                "activation_id": descriptor["activation_id"],
                "producer_authority_fingerprint": descriptor[
                    "producer_authority_fingerprint"
                ],
                "state": "armed",
                "luna_consumer_enabled": True,
                "sol_enabled": False,
                "formal_write_count": 0,
            },
            purpose="dispatch-production-canary-state",
            key=key,
        )
        state_path = runtime / "dispatch/state/production-canary/math.json"
        state_path.parent.mkdir(parents=True)
        state_path.write_text(
            json.dumps(state, sort_keys=True) + "\n", encoding="utf-8"
        )
        snapshot = {
            "schema_version": "subject_authority_snapshot_v1",
            "subject": "math",
            "generation": descriptor["authority_generation"],
            "authority_fingerprint": descriptor["authority_fingerprint"],
            "mcp_server_release": "test",
            "route_request_id": "bounded-read-test",
            "scope_sha256": "9" * 64,
            "model_call_count": 0,
            "formal_write_count": 0,
        }
        return runtime, snapshot

    def _migration_base_task(
        self,
        *,
        index: int,
        formal_id: str,
        release_id: str,
        recorded_at: str,
    ) -> tuple[dict[str, object], str, str]:
        capture_id = str(EXACT_SAMPLES[formal_id]["capture_event_id"])
        base = canary_task(
            index,
            release_id=release_id,
            capture_id=capture_id,
            recorded_at=recorded_at,
        )
        payload = copy.deepcopy(dict(base.frozen_payload))
        content_processing_id = sha(
            {"scope": "exact-four-migration", "capture_id": capture_id}
        )
        payload["content_processing_id"] = content_processing_id
        payload["content_group_members"] = [
            {
                "subject": "math",
                "capture_id": capture_id,
                "study_date": payload["study_date"],
                "input_fingerprint": payload["input_fingerprint"],
            }
        ]
        dispatch_contract = copy.deepcopy(payload["dispatch_contract"])
        dispatch_contract.update(
            dispatch_rule_binding(
                release_id=release_id,
                subject="math",
                subject_processing_contract_sha256=None,
            )
        )
        payload["dispatch_contract"] = dispatch_contract
        return payload, FrozenTask(payload).unit_sha256, content_processing_id

    def _exact_four_migration_fixture(self, root: Path) -> dict[str, object]:
        runtime = (root / "runtime").resolve()
        source_release = "a" * 64
        target_release = "b" * 64
        store = LeaseStore(runtime)
        store.begin_subject_drain("math")
        source_producer = authority("math", source_release)
        source_state = store.activate_production_canary(
            "math",
            release_id=source_release,
            producer_authority=source_producer,
            activated_at="2026-08-14T00:00:00Z",
            continuous_concurrency_limit=20,
        )
        source_generation = "math-source-generation"
        source_authority = "c" * 64
        source_tasks: list[FrozenTask] = []
        source_bindings: list[dict[str, object]] = []
        source_queue_paths: list[Path] = []
        for index, formal_id in enumerate(EXACT_ORDER[1:], start=2):
            payload, unit_sha256, content_processing_id = (
                self._migration_base_task(
                    index=index,
                    formal_id=formal_id,
                    release_id=source_release,
                    recorded_at="2026-08-14T00:00:01Z",
                )
            )
            capture_id = str(EXACT_SAMPLES[formal_id]["capture_event_id"])
            attempt_key = sha(
                {"scope": "exact-four-attempt-1", "capture": capture_id}
            )
            execution_sha = sha({"execution": source_release})
            apply_sha = sha({"apply": capture_id})
            binding: dict[str, object] = {
                "schema_version": (
                    "study-intake-math-exact-smoke-task-binding-v1"
                ),
                "scope_id": "math-smoke-2026-08-13-five-sample-v1",
                "sequence": index,
                "formal_id": formal_id,
                "execution_authorization_sha256": execution_sha,
                "execution_authorization_path": str(
                    root / f"{execution_sha}.authorization.json"
                ),
                "capture_apply_receipt_sha256": apply_sha,
                "capture_apply_receipt_path": str(
                    root / f"{apply_sha}.capture-apply.json"
                ),
                "capture_event_id": capture_id,
                "capture_event_sha256": sha({"capture": capture_id}),
                "capture_idempotency_key": EXACT_SAMPLES[formal_id][
                    "capture_idempotency_key"
                ],
                "math_repo_root": str(root / "math-repo"),
                "processing_attempt_number": 1,
                "processing_attempt_id": (
                    f"MATH-SMOKE-ATTEMPT-{attempt_key[:24]}"
                ),
                "attempt_idempotency_key": attempt_key,
                "expected_unit_sha256": unit_sha256,
                "content_processing_id": content_processing_id,
                "prior_terminal_receipt_sha256": None,
                "prior_terminal_receipt_path": None,
                "target_release_id": source_release,
                "activation_id": source_state["activation_id"],
                "authority_generation": source_generation,
                "authority_fingerprint": source_authority,
                "producer_authority_fingerprint": source_state[
                    "producer_authority_fingerprint"
                ],
                "single_active_attempt_required": True,
                "single_accepted_package_required": True,
                "capture_write_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            }
            payload["math_exact_smoke_binding"] = binding
            task = FrozenTask(payload)
            self.assertEqual(task.unit_sha256, unit_sha256)
            with mock.patch(
                "concurrent_dispatch.verify_exact_task_binding",
                return_value=binding,
            ):
                queue = store.materialize_production_canary_task(task)
            source_tasks.append(task)
            source_bindings.append(binding)
            source_queue_paths.append(
                (
                    runtime
                    / "dispatch/state/production-canary-queue/math"
                    / str(source_state["activation_id"])
                    / f"{queue['producer_input_contract_sha256']}.json"
                ).resolve()
            )

        gs566_failure = store.fail_production_canary_preclaim(
            "math",
            source_tasks[0],
            failure_stage="pre_claim",
            error_code="subject_luna_batch_already_current",
        )
        gs566_receipt_path = Path(
            str(gs566_failure["preclaim_failure_receipt_path"])
        )
        gs566_receipt = json.loads(
            gs566_receipt_path.read_text(encoding="utf-8")
        )
        source_queue_bytes = {
            path: path.read_bytes() for path in source_queue_paths
        }
        store.deactivate_production_canary(
            "math", expected_release_id=source_release
        )
        store.begin_subject_drain("math")
        target_producer = authority("math", target_release)
        target_state = store.activate_production_canary(
            "math",
            release_id=target_release,
            producer_authority=target_producer,
            activated_at="2026-08-15T00:00:00Z",
            continuous_concurrency_limit=20,
        )
        target_state = store.pause_production_canary("math")
        target_generation = "math-test-generation"
        target_authority = sha({"authority": target_generation})
        mappings: list[dict[str, object]] = []
        for formal_id, queue_path, source_binding in zip(
            EXACT_ORDER[1:], source_queue_paths, source_bindings
        ):
            queue = json.loads(queue_path.read_text(encoding="utf-8"))
            preclaim_evidence = None
            if formal_id == "GS-566":
                preclaim_evidence = {
                    "receipt_path": str(gs566_receipt_path),
                    "receipt_sha256": gs566_failure[
                        "preclaim_failure_receipt_sha256"
                    ],
                    "failed_at": gs566_receipt["failed_at"],
                    "failure_stage": "pre_claim",
                    "error_code": "subject_luna_batch_already_current",
                    "queue_entry_preserved": True,
                    "queue_status_after": "pending",
                    "model_submission_started": False,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "mcp_tool_call_count": 0,
                    "formal_write_count": 0,
                }
            mappings.append(
                {
                    "formal_id": formal_id,
                    "capture_event_id": source_binding["capture_event_id"],
                    "source_queue_path": str(queue_path),
                    "source_queue_sha256": hashlib.sha256(
                        queue_path.read_bytes()
                    ).hexdigest(),
                    "source_task_object_path": queue["task_object_path"],
                    "source_task_object_sha256": queue[
                        "task_object_sha256"
                    ],
                    "source_unit_sha256": queue["unit_sha256"],
                    "source_frozen_payload_sha256": queue[
                        "frozen_payload_sha256"
                    ],
                    "source_release_id": source_release,
                    "source_activation_id": source_state["activation_id"],
                    "source_producer_authority_fingerprint": source_state[
                        "producer_authority_fingerprint"
                    ],
                    "source_authority_generation": source_generation,
                    "source_authority_fingerprint": source_authority,
                    "source_execution_authorization_sha256": source_binding[
                        "execution_authorization_sha256"
                    ],
                    "source_execution_authorization_path": source_binding[
                        "execution_authorization_path"
                    ],
                    "source_capture_apply_receipt_sha256": source_binding[
                        "capture_apply_receipt_sha256"
                    ],
                    "source_capture_apply_receipt_path": source_binding[
                        "capture_apply_receipt_path"
                    ],
                    "source_task_binding_sha256": hashlib.sha256(
                        math_exact_smoke.canonical_bytes(source_binding)
                    ).hexdigest(),
                    "source_processing_attempt_id": source_binding[
                        "processing_attempt_id"
                    ],
                    "source_attempt_idempotency_key": source_binding[
                        "attempt_idempotency_key"
                    ],
                    "source_preclaim_failure_evidence": preclaim_evidence,
                }
            )
        gs269_evidence = {
            "capture_event_id": EXACT_SAMPLES["GS-269"][
                "capture_event_id"
            ],
            "accepted_queue_sha256": "d" * 64,
            "archive_sha256": "e" * 64,
            "formal_write_count": 0,
        }
        key = (runtime / "dispatch/state/authority.key").read_bytes()
        descriptor = _seal(
            {
                "schema_version": (
                    "study-intake-math-pending-queue-migration-descriptor-v1"
                ),
                "scope_id": "math-smoke-2026-08-13-five-sample-v1",
                "migration_intent_sha256": "f" * 64,
                "migration_intent_path": str(root / "migration-intent.json"),
                "target_release_id": target_release,
                "target_activation_id": target_state["activation_id"],
                "target_authority_generation": target_generation,
                "target_subject_authority_fingerprint": target_authority,
                "target_producer_authority_fingerprint": target_state[
                    "producer_authority_fingerprint"
                ],
                "gs269_archived_evidence": gs269_evidence,
                "source_queue_mappings": mappings,
                "task_count": 4,
                "processing_attempt_number": 1,
                "capture_write_count": 0,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            },
            purpose="dispatch-math-pending-queue-migration-descriptor",
            key=key,
        )
        descriptor_sha256, descriptor_path = (
            math_exact_smoke._publish_content_addressed(
                runtime / "dispatch/math-pending-queue-migrations/descriptors",
                descriptor,
            )
        )
        pointer = _seal(
            {
                "schema_version": (
                    "study-intake-math-pending-queue-migration-pointer-v1"
                ),
                "scope_id": "math-smoke-2026-08-13-five-sample-v1",
                "migration_descriptor_sha256": descriptor_sha256,
                "migration_descriptor_path": str(descriptor_path),
                "target_release_id": target_release,
                "target_activation_id": target_state["activation_id"],
                "formal_write_count": 0,
            },
            purpose="dispatch-math-pending-queue-migration-pointer",
            key=key,
        )
        pointer_path = (
            runtime / "dispatch/state/math-pending-queue-migration/active.json"
        )
        pointer_path.parent.mkdir(parents=True, exist_ok=True)
        pointer_path.write_bytes(
            math_exact_smoke.canonical_bytes(pointer) + b"\n"
        )
        target_tasks: list[FrozenTask] = []
        for index, (formal_id, source_binding, mapping) in enumerate(
            zip(EXACT_ORDER[1:], source_bindings, mappings), start=2
        ):
            payload, target_unit, content_processing_id = (
                self._migration_base_task(
                    index=index,
                    formal_id=formal_id,
                    release_id=target_release,
                    recorded_at="2026-08-14T00:00:01Z",
                )
            )
            binding = {
                **copy.deepcopy(source_binding),
                "schema_version": (
                    "study-intake-math-exact-smoke-task-binding-v2"
                ),
                "expected_unit_sha256": target_unit,
                "content_processing_id": content_processing_id,
                "target_release_id": target_release,
                "activation_id": target_state["activation_id"],
                "authority_generation": target_generation,
                "authority_fingerprint": target_authority,
                "producer_authority_fingerprint": target_state[
                    "producer_authority_fingerprint"
                ],
                "migration_descriptor_sha256": descriptor_sha256,
                "source_queue_path": mapping["source_queue_path"],
                "source_queue_sha256": mapping["source_queue_sha256"],
                "source_task_object_path": mapping[
                    "source_task_object_path"
                ],
                "source_task_object_sha256": mapping[
                    "source_task_object_sha256"
                ],
                "source_unit_sha256": mapping["source_unit_sha256"],
                "source_frozen_payload_sha256": mapping[
                    "source_frozen_payload_sha256"
                ],
                "source_release_id": mapping["source_release_id"],
                "source_activation_id": mapping["source_activation_id"],
                "source_producer_authority_fingerprint": mapping[
                    "source_producer_authority_fingerprint"
                ],
                "source_authority_generation": mapping[
                    "source_authority_generation"
                ],
                "source_authority_fingerprint": mapping[
                    "source_authority_fingerprint"
                ],
                "source_execution_authorization_sha256": mapping[
                    "source_execution_authorization_sha256"
                ],
                "source_execution_authorization_path": mapping[
                    "source_execution_authorization_path"
                ],
                "source_capture_apply_receipt_sha256": mapping[
                    "source_capture_apply_receipt_sha256"
                ],
                "source_capture_apply_receipt_path": mapping[
                    "source_capture_apply_receipt_path"
                ],
                "source_task_binding_sha256": mapping[
                    "source_task_binding_sha256"
                ],
                "source_preclaim_failure_evidence": copy.deepcopy(
                    mapping["source_preclaim_failure_evidence"]
                ),
                "gs269_archived_evidence_sha256": hashlib.sha256(
                    math_exact_smoke.canonical_bytes(gs269_evidence)
                ).hexdigest(),
            }
            payload["math_exact_smoke_binding"] = binding
            target_task = FrozenTask(payload)
            self.assertEqual(target_task.unit_sha256, target_unit)
            target_tasks.append(target_task)
        return {
            "runtime": runtime,
            "store": store,
            "descriptor": descriptor,
            "descriptor_sha256": descriptor_sha256,
            "target_tasks": target_tasks,
            "target_release": target_release,
            "target_activation": target_state["activation_id"],
            "source_queue_bytes": source_queue_bytes,
            "gs566_queue_sha256": hashlib.sha256(
                source_queue_paths[0].read_bytes()
            ).hexdigest(),
            "gs566_receipt_sha256": gs566_failure[
                "preclaim_failure_receipt_sha256"
            ],
        }

    def test_exact_four_migration_authority_mismatch_is_zero_mutation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._exact_four_migration_fixture(root)
            runtime = fixture["runtime"]
            assert isinstance(runtime, Path)
            store = fixture["store"]
            assert isinstance(store, LeaseStore)
            descriptor = copy.deepcopy(fixture["descriptor"])
            assert isinstance(descriptor, dict)
            descriptor.pop("authority", None)
            descriptor["target_subject_authority_fingerprint"] = "9" * 64
            descriptor = _seal(
                descriptor,
                purpose=(
                    "dispatch-math-pending-queue-migration-descriptor"
                ),
                key=(runtime / "dispatch/state/authority.key").read_bytes(),
            )
            before = {
                path.relative_to(runtime): path.read_bytes()
                for path in runtime.rglob("*")
                if path.is_file()
            }
            target_queue_root = (
                runtime
                / "dispatch/state/production-canary-queue/math"
                / str(fixture["target_activation"])
            )
            with mock.patch.object(
                math_exact_smoke,
                "GS566_SOURCE_QUEUE_SHA256",
                fixture["gs566_queue_sha256"],
            ), mock.patch.object(
                math_exact_smoke,
                "GS566_PRECLAIM_FAILURE_RECEIPT_SHA256",
                fixture["gs566_receipt_sha256"],
            ):
                with self.assertRaisesRegex(
                    DispatchError, "math_migration_materialization_invalid"
                ):
                    store.materialize_exact_math_migration(
                        tasks=fixture["target_tasks"],
                        migration_descriptor=descriptor,
                    )
            after = {
                path.relative_to(runtime): path.read_bytes()
                for path in runtime.rglob("*")
                if path.is_file()
            }
            self.assertEqual(after, before)
            self.assertEqual(list(target_queue_root.glob("*.json")), [])
            self.assertFalse((root / "no-model-codex.called").exists())
            for path, payload in fixture["source_queue_bytes"].items():
                self.assertEqual(path.read_bytes(), payload)

    def test_exact_four_migration_commits_then_claims_four_concurrently(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = self._exact_four_migration_fixture(root)
            runtime = fixture["runtime"]
            assert isinstance(runtime, Path)
            store = fixture["store"]
            assert isinstance(store, LeaseStore)
            tasks = fixture["target_tasks"]
            assert isinstance(tasks, list)
            descriptor = fixture["descriptor"]
            assert isinstance(descriptor, dict)
            descriptor_sha256 = str(fixture["descriptor_sha256"])
            worker_lock = runtime / "state/worker.lock"
            worker_lock.parent.mkdir(parents=True, exist_ok=True)
            barrier = TailBarrierGroundedRunner()
            dispatcher = ConcurrentDispatcher(
                runtime,
                lambda _task, _context: IdentityPublishingFixtureRunner(
                    barrier, store
                ),
                soft_runtime_warning_seconds=2,
                stall_timeout_seconds=30,
                stall_probe_interval_seconds=1,
                stall_probe_required_consecutive_failures=2,
                production_canary=True,
            )
            service_attempted = threading.Event()
            service_gate_acquired = threading.Event()
            result_box: dict[str, object] = {}

            def consume_after_gate() -> None:
                service_attempted.set()
                with release_manager._claim_gate_lock(worker_lock, 5.0):
                    service_gate_acquired.set()
                    handles = [dispatcher.submit(task) for task in tasks]
                    result_box["results"] = [
                        handle.wait(15) for handle in handles
                    ]

            with mock.patch.object(
                math_exact_smoke,
                "GS566_SOURCE_QUEUE_SHA256",
                fixture["gs566_queue_sha256"],
            ), mock.patch.object(
                math_exact_smoke,
                "GS566_PRECLAIM_FAILURE_RECEIPT_SHA256",
                fixture["gs566_receipt_sha256"],
            ):
                with release_manager._claim_gate_lock(
                    worker_lock, 5.0
                ) as live_claim_gate:
                    self.assertTrue(live_claim_gate["held"])
                    rows = store.materialize_exact_math_migration(
                        tasks=tasks,
                        migration_descriptor=descriptor,
                    )
                    self.assertEqual(len(rows), 4)
                    staged_state = store.production_canary_status_read_only(
                        "math"
                    )
                    self.assertEqual(staged_state["state"], "paused_drained")
                    self.assertFalse(staged_state["luna_consumer_enabled"])
                    self.assertTrue(staged_state["unlocked_once"])
                    committed = (
                        math_exact_smoke.publish_exact_math_pending_queue_migration_commit(
                            runtime_root=runtime,
                            migration_descriptor_sha256=descriptor_sha256,
                            queue_rows=rows,
                        )
                    )
                    self.assertEqual(committed["queue_count"], 4)
                    reopened = (
                        math_exact_smoke.reopen_exact_math_pending_queue_migration_commit(
                            runtime_root=runtime,
                            migration_descriptor_sha256=descriptor_sha256,
                        )
                    )
                    self.assertEqual(
                        reopened["migration_commit_sha256"],
                        committed["migration_commit_sha256"],
                    )
                    committed_paths = (
                        math_exact_smoke._committed_migration_target_queue_paths(
                            runtime_root=runtime
                        )
                    )
                    self.assertEqual(
                        committed_paths,
                        {
                            Path(str(row["queue_entry_path"])).resolve()
                            for row in rows
                        },
                    )
                    historical_commit = json.loads(
                        Path(
                            str(committed["migration_commit_path"])
                        ).read_text(encoding="utf-8")
                    )
                    historical_commit.pop("authority", None)
                    historical_activation = "6" * 64
                    historical_commit["target_release_id"] = "7" * 64
                    historical_commit[
                        "target_activation_id"
                    ] = historical_activation
                    historical_paths = set()
                    for index, row in enumerate(
                        historical_commit["queue_mappings"]
                    ):
                        historical_path = (
                            runtime
                            / "dispatch/state/production-canary-queue/math"
                            / historical_activation
                            / f"historical-{index}.json"
                        ).resolve()
                        row["target_queue_path"] = str(historical_path)
                        historical_paths.add(historical_path)
                    historical_commit = _seal(
                        historical_commit,
                        purpose=(
                            "dispatch-math-pending-queue-migration-commit"
                        ),
                        key=(runtime / "dispatch/state/authority.key").read_bytes(),
                    )
                    _historical_sha256, historical_commit_path = (
                        math_exact_smoke._publish_content_addressed(
                        math_exact_smoke._migration_commit_root(runtime),
                        historical_commit,
                    )
                    )
                    self.assertEqual(
                        math_exact_smoke._committed_migration_target_queue_paths(
                            runtime_root=runtime
                        ),
                        committed_paths | historical_paths,
                    )
                    historical_commit_path.unlink()
                    self.assertEqual(
                        math_exact_smoke._committed_migration_descriptor_sha256(
                            runtime_root=runtime
                        ),
                        descriptor_sha256,
                    )
                    active_pointer_path = (
                        runtime
                        / "dispatch/state/math-pending-queue-migration/active.json"
                    )
                    previous_active_pointer = active_pointer_path.read_bytes()
                    successor_active_pointer = _seal(
                        {
                            "schema_version": (
                                "study-intake-math-pending-queue-migration-pointer-v1"
                            ),
                            "scope_id": (
                                "math-smoke-2026-08-13-five-sample-v1"
                            ),
                            "migration_descriptor_sha256": "5" * 64,
                            "migration_descriptor_path": str(
                                runtime / "successor-descriptor.json"
                            ),
                            "target_release_id": "c" * 64,
                            "target_activation_id": "2" * 64,
                            "formal_write_count": 0,
                        },
                        purpose=(
                            "dispatch-math-pending-queue-migration-pointer"
                        ),
                        key=(runtime / "dispatch/state/authority.key").read_bytes(),
                    )
                    math_exact_smoke._publish_active_migration_pointer(
                        runtime_root=runtime,
                        pointer=successor_active_pointer,
                        key=(runtime / "dispatch/state/authority.key").read_bytes(),
                    )
                    self.assertNotEqual(
                        active_pointer_path.read_bytes(), previous_active_pointer
                    )
                    active_pointer_path.chmod(0o600)
                    active_pointer_path.write_bytes(previous_active_pointer)
                    active_pointer_path.chmod(0o400)
                    previous_pointer_path = (
                        runtime
                        / "dispatch/state/math-pending-queue-migration/commit.json"
                    )
                    previous_pointer = previous_pointer_path.read_bytes()
                    successor_pointer = _seal(
                        {
                            "schema_version": (
                                "study-intake-math-pending-queue-migration-commit-pointer-v1"
                            ),
                            "scope_id": (
                                "math-smoke-2026-08-13-five-sample-v1"
                            ),
                            "migration_descriptor_sha256": "d" * 64,
                            "migration_commit_sha256": "e" * 64,
                            "migration_commit_path": str(
                                runtime / "successor-commit.json"
                            ),
                            "target_release_id": "f" * 64,
                            "target_activation_id": "1" * 64,
                            "formal_write_count": 0,
                        },
                        purpose=(
                            "dispatch-math-pending-queue-migration-commit-pointer"
                        ),
                        key=(runtime / "dispatch/state/authority.key").read_bytes(),
                    )
                    math_exact_smoke._publish_migration_commit_pointer(
                        runtime_root=runtime,
                        pointer=successor_pointer,
                        key=(runtime / "dispatch/state/authority.key").read_bytes(),
                    )
                    self.assertNotEqual(
                        previous_pointer_path.read_bytes(), previous_pointer
                    )
                    self.assertEqual(
                        json.loads(previous_pointer_path.read_text())[
                            "migration_descriptor_sha256"
                        ],
                        "d" * 64,
                    )
                    previous_pointer_path.chmod(0o600)
                    previous_pointer_path.write_bytes(previous_pointer)
                    previous_pointer_path.chmod(0o400)
                    self.assertIsNone(
                        math_exact_smoke._active_pending_migration_for_release(
                            runtime_root=runtime,
                            release_id="f" * 64,
                        )
                    )
                    self.assertEqual(
                        math_exact_smoke._active_pending_migration_for_release(
                            runtime_root=runtime,
                            release_id=str(fixture["target_release"]),
                        )[0],
                        descriptor_sha256,
                    )
                    active_pointer = (
                        runtime
                        / "dispatch/state/math-pending-queue-migration/active.json"
                    )
                    active_pointer_bytes = active_pointer.read_bytes()
                    active_pointer.unlink()
                    self.assertEqual(
                        math_exact_smoke._committed_migration_target_queue_paths(
                            runtime_root=runtime
                        ),
                        committed_paths,
                    )
                    active_pointer.write_bytes(active_pointer_bytes)
                    self.assertNotIn(
                        (
                            runtime
                            / "dispatch/state/production-canary-queue/math"
                            / str(fixture["target_activation"])
                            / "uncommitted-duplicate.json"
                        ).resolve(),
                        committed_paths,
                    )
                    resumed = store.resume_production_canary("math")
                    self.assertEqual(
                        resumed["state"], "continuous_concurrent_unlocked"
                    )
                    consumer = threading.Thread(target=consume_after_gate)
                    consumer.start()
                    self.assertTrue(service_attempted.wait(2))
                    self.assertFalse(service_gate_acquired.wait(0.2))
                    self.assertEqual(barrier.entered_count, 0)
                    self.assertEqual(
                        store.production_canary_status_read_only("math")[
                            "active_task_count"
                        ],
                        0,
                    )
                    self.assertTrue(
                        all(
                            json.loads(
                                Path(row["queue_entry_path"]).read_text(
                                    encoding="utf-8"
                                )
                            )["queue_status"]
                            == "pending"
                            for row in rows
                        )
                    )
                self.assertTrue(service_gate_acquired.wait(2))
                self.assertTrue(
                    barrier.all_entered.wait(10),
                    "migrated Math queues did not overlap at active=4",
                )
                active = store.production_canary_status_read_only("math")
                self.assertEqual(active["active_task_count"], 4)
                telemetry = store.production_canary_concurrency_telemetry(
                    release_id=str(fixture["target_release"])
                )
                self.assertEqual(
                    telemetry["scheduler_claim_subject_peak_active"]["math"],
                    4,
                )
                barrier.release.set()
                consumer.join(20)
                self.assertFalse(consumer.is_alive())
                results = result_box["results"]
                self.assertTrue(
                    all(
                        result.status == "completed"
                        and result.outcome == "succeeded"
                        for result in results
                    ),
                    results,
                )
                for task, mapping in zip(
                    tasks, descriptor["source_queue_mappings"]
                ):
                    binding = task.frozen_payload[
                        "math_exact_smoke_binding"
                    ]
                    self.assertEqual(binding["processing_attempt_number"], 1)
                    self.assertIsNone(binding["prior_terminal_receipt_sha256"])
                    self.assertEqual(
                        binding["processing_attempt_id"],
                        mapping["source_processing_attempt_id"],
                    )
                    self.assertEqual(
                        binding["attempt_idempotency_key"],
                        mapping["source_attempt_idempotency_key"],
                    )
            for path, payload in fixture["source_queue_bytes"].items():
                self.assertEqual(path.read_bytes(), payload)

    def test_descriptor_schema_and_real_read_only_preview(self) -> None:
        before = {
            path: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (
                SMOKE_ROOT / "batch_manifest.json",
                SMOKE_ROOT / "CHECKSUMS.sha256",
                MATH_REPO_ROOT
                / "数学一回滚复习系统/快速入库事件.jsonl",
                MATH_REPO_ROOT / "数学一回滚复习系统/复习单元.json",
            )
        }
        descriptor = self.descriptor()
        schema = json.loads(
            (
                ROOT
                / "schemas/math-exact-smoke-execution-authorization-v1.json"
            ).read_text(encoding="utf-8")
        )
        validation = subprocess.run(
            [
                "/opt/miniconda3/envs/dl/bin/python",
                "-c",
                (
                    "import json,sys; "
                    "from jsonschema import Draft202012Validator; "
                    "v=json.load(sys.stdin); "
                    "Draft202012Validator.check_schema(v['schema']); "
                    "Draft202012Validator(v['schema']).validate(v['instance'])"
                ),
            ],
            input=json.dumps({"schema": schema, "instance": descriptor}),
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(validation.returncode, 0, validation.stderr)
        with tempfile.TemporaryDirectory() as temporary:
            repo = self._isolated_repo(Path(temporary))
            preview = preview_exact_smoke(
                descriptor, math_repo_root=repo
            )
        self.assertEqual(preview["status"], "ready")
        self.assertEqual(preview["execution_order"], list(EXACT_ORDER))
        self.assertEqual(preview["capture_write_count"], 0)
        self.assertEqual(preview["source_stage_write_count"], 0)
        self.assertEqual(preview["queue_write_count"], 0)
        self.assertEqual(preview["model_call_count"], 0)
        self.assertEqual(preview["provider_request_count"], 0)
        self.assertEqual(preview["formal_write_count"], 0)
        self.assertFalse(preview["sol_enabled"])
        self.assertEqual(
            [row["formal_id"] for row in preview["samples"]],
            list(EXACT_ORDER),
        )
        self.assertTrue(
            all(row["capture_count"] == 0 for row in preview["samples"])
        )
        after = {
            path: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in before
        }
        self.assertEqual(before, after)
        self.assertEqual(
            hashlib.sha256(
                (SMOKE_ROOT / "CHECKSUMS.sha256").read_bytes()
            ).hexdigest(),
            CHECKSUMS_SHA256,
        )

    def test_real_bin_entrypoint_does_not_shadow_library(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(ROOT / "bin/preprocess_dispatcher.py"), "--help"],
            cwd=ROOT / "bin",
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("usage:", completed.stdout.lower())
        smoke_help = subprocess.run(
            [
                sys.executable,
                str(ROOT / "bin/math_exact_smoke_control.py"),
                "--help",
            ],
            cwd=ROOT / "bin",
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(smoke_help.returncode, 0, smoke_help.stderr)
        self.assertIn("preview-authorized", smoke_help.stdout)

    def test_descriptor_or_authority_input_drift_fails_closed(self) -> None:
        descriptor = self.descriptor()
        drifted = copy.deepcopy(descriptor)
        drifted["execution_order"] = list(reversed(EXACT_ORDER))
        with self.assertRaisesRegex(
            MathExactSmokeError, "math_smoke_descriptor_drift"
        ):
            preview_exact_smoke(drifted)
        with self.assertRaisesRegex(
            MathExactSmokeError, "math_smoke_authority_invalid"
        ):
            build_authorization_descriptor(
                target_release_id="1" * 64,
                activation_id="2" * 64,
                authority_generation="generation",
                authority_fingerprint="not-a-sha",
                producer_authority_fingerprint="4" * 64,
            )

    def test_authorization_is_hmac_and_content_addressed(self) -> None:
        descriptor = self.descriptor()
        snapshot = {
            "schema_version": "subject_authority_snapshot_v1",
            "subject": "math",
            "generation": descriptor["authority_generation"],
            "authority_fingerprint": descriptor["authority_fingerprint"],
            "mcp_server_release": "test",
            "route_request_id": "bounded-read-test",
            "scope_sha256": "9" * 64,
            "model_call_count": 0,
            "formal_write_count": 0,
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            runtime = root / "runtime"
            repo = self._isolated_repo(root)
            key_path = runtime / "dispatch/state/authority.key"
            key_path.parent.mkdir(parents=True)
            key = b"k" * 32
            key_path.write_bytes(key)
            key_path.chmod(0o600)
            state = _seal(
                {
                    "schema_version": "study-intake-production-canary-state-v3",
                    "subject": "math",
                    "release_id": descriptor["target_release_id"],
                    "activation_id": descriptor["activation_id"],
                    "producer_authority_fingerprint": descriptor[
                        "producer_authority_fingerprint"
                    ],
                    "state": "armed",
                    "luna_consumer_enabled": True,
                    "sol_enabled": False,
                    "formal_write_count": 0,
                },
                purpose="dispatch-production-canary-state",
                key=key,
            )
            state_path = (
                runtime / "dispatch/state/production-canary/math.json"
            )
            state_path.parent.mkdir(parents=True)
            state_path.write_text(
                json.dumps(state, sort_keys=True) + "\n", encoding="utf-8"
            )
            digest, path, receipt = authorize_exact_smoke(
                descriptor,
                runtime_root=runtime,
                authority_snapshot=snapshot,
                authorized_at="2026-08-13T13:00:00Z",
                math_repo_root=repo,
            )
            self.assertEqual(path.name, f"{digest}.json")
            reopened_sha, reopened = reopen_execution_authorization(
                path, runtime_root=runtime
            )
            self.assertEqual(reopened_sha, digest)
            self.assertEqual(reopened, receipt)
            self.assertEqual(receipt["authority_snapshot_count"], 1)
            self.assertEqual(
                receipt["authority_snapshot_mcp_tool_call_count"], 1
            )
            self.assertEqual(receipt["model_mcp_tool_call_count"], 0)
            schema = json.loads(
                (
                    ROOT
                    / "schemas/math-exact-smoke-execution-authorization-receipt-v1.json"
                ).read_text(encoding="utf-8")
            )
            validation = subprocess.run(
                [
                    "/opt/miniconda3/envs/dl/bin/python",
                    "-c",
                    (
                        "import json,sys; "
                        "from jsonschema import Draft202012Validator,FormatChecker; "
                        "v=json.load(sys.stdin); "
                        "Draft202012Validator.check_schema(v['schema']); "
                        "Draft202012Validator(v['schema'],format_checker=FormatChecker()).validate(v['instance'])"
                    ),
                ],
                input=json.dumps({"schema": schema, "instance": receipt}),
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(validation.returncode, 0, validation.stderr)
            tampered = json.loads(path.read_text(encoding="utf-8"))
            tampered["capture_count"] = 1
            alternate = runtime / "tampered.json"
            alternate.write_text(json.dumps(tampered), encoding="utf-8")
            with self.assertRaises(MathExactSmokeError):
                reopen_execution_authorization(alternate, runtime_root=runtime)

    def test_sample_byte_drift_and_capture_count_fail_closed(self) -> None:
        descriptor = self.descriptor()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            smoke = root / "smoke"
            repo = root / "repo"
            shutil.copytree(SMOKE_ROOT, smoke)
            for binding in EXACT_SAMPLES.values():
                for relative in (
                    binding["formal_card_path"],
                    binding["question_path"],
                ):
                    source = MATH_REPO_ROOT / str(relative)
                    target = repo / str(relative)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(source, target)
            for relative in (
                "数学一回滚复习系统/快速入库事件.jsonl",
                "数学一回滚复习系统/复习单元.json",
            ):
                target = repo / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(MATH_REPO_ROOT / relative, target)

            sample_path = smoke / "GS-269/sample.json"
            sample_path.write_bytes(sample_path.read_bytes() + b"\n")
            with self.assertRaisesRegex(
                MathExactSmokeError, "math_smoke_package_hash_drift"
            ):
                preview_exact_smoke(
                    descriptor, smoke_root=smoke, math_repo_root=repo
                )

            shutil.copy2(SMOKE_ROOT / "GS-269/sample.json", sample_path)
            ledger_path = repo / "数学一回滚复习系统/快速入库事件.jsonl"
            existing = json.loads(
                ledger_path.read_text(encoding="utf-8").splitlines()[0]
            )
            duplicate = dict(existing)
            duplicate["target"] = dict(duplicate["target"])
            duplicate["target"]["formal_id"] = "GS-269"
            with ledger_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(duplicate, ensure_ascii=False) + "\n")
            with self.assertRaisesRegex(
                MathExactSmokeError, "math_smoke_ledger_hash_drift"
            ):
                preview_exact_smoke(
                    descriptor, smoke_root=smoke, math_repo_root=repo
                )

    def test_legacy_exact_sample_is_not_a_fresh_current_contract_capture(
        self,
    ) -> None:
        descriptor = self.descriptor()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._isolated_repo(root)
            smoke = root / "smoke"
            shutil.copytree(SMOKE_ROOT, smoke)
            runtime, snapshot = self._isolated_runtime(root, descriptor)
            _auth_sha, auth_path, _ = authorize_exact_smoke(
                descriptor,
                runtime_root=runtime,
                authority_snapshot=snapshot,
                authorized_at="2026-08-13T13:00:00Z",
                smoke_root=smoke,
                math_repo_root=repo,
            )
            ledger = repo / "数学一回滚复习系统/快速入库事件.jsonl"
            ledger_before = ledger.read_bytes()
            with self.assertRaisesRegex(
                Exception,
                "缺少必须的来源角色：solution_text",
            ):
                apply_exact_smoke_capture(
                    auth_path,
                    formal_id="GS-269",
                    runtime_root=runtime,
                    authority_reader=lambda: snapshot,
                    smoke_root=smoke,
                    math_repo_root=repo,
                )
            self.assertEqual(ledger.read_bytes(), ledger_before)
            self.assertFalse(
                (
                    repo
                    / "数学一回滚复习系统/快速入库来源/2026-08-13"
                    / EXACT_SAMPLES["GS-269"]["stage_bundle_id"]
                ).exists()
            )

    def legacy_isolated_apply_once_rollback_and_prior_terminal_gate(self) -> None:
        descriptor = self.descriptor()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._isolated_repo(root)
            smoke = root / "smoke"
            shutil.copytree(SMOKE_ROOT, smoke)
            runtime, snapshot = self._isolated_runtime(root, descriptor)
            auth_sha, auth_path, _ = authorize_exact_smoke(
                descriptor,
                runtime_root=runtime,
                authority_snapshot=snapshot,
                authorized_at="2026-08-13T13:00:00Z",
                smoke_root=smoke,
                math_repo_root=repo,
            )

            with self.assertRaisesRegex(
                MathExactSmokeError, "math_smoke_capture_apply_out_of_order"
            ):
                apply_exact_smoke_capture(
                    auth_path,
                    formal_id="GS-566",
                    runtime_root=runtime,
                    authority_reader=lambda: snapshot,
                    smoke_root=smoke,
                    math_repo_root=repo,
                )

            ledger = repo / "数学一回滚复习系统/快速入库事件.jsonl"
            ledger_before = ledger.read_bytes()
            with self.assertRaisesRegex(
                MathExactSmokeError, "math_smoke_test_failpoint"
            ):
                apply_exact_smoke_capture(
                    auth_path,
                    formal_id="GS-269",
                    runtime_root=runtime,
                    authority_reader=lambda: snapshot,
                    smoke_root=smoke,
                    math_repo_root=repo,
                    failpoint="after_capture_before_receipt",
                )
            self.assertEqual(ledger.read_bytes(), ledger_before)
            self.assertFalse(
                (
                    repo
                    / "数学一回滚复习系统/快速入库来源/2026-08-13"
                    / EXACT_SAMPLES["GS-269"]["stage_bundle_id"]
                ).exists()
            )
            receipt = apply_exact_smoke_capture(
                auth_path,
                formal_id="GS-269",
                runtime_root=runtime,
                authority_reader=lambda: snapshot,
                smoke_root=smoke,
                math_repo_root=repo,
            )
            self.assertEqual(receipt[2]["authority_snapshot_count"], 2)
            self.assertEqual(
                receipt[2]["authority_snapshot_mcp_tool_call_count"], 2
            )
            self.assertEqual(receipt[2]["model_mcp_tool_call_count"], 0)
            idempotent = apply_exact_smoke_capture(
                auth_path,
                formal_id="GS-269",
                runtime_root=runtime,
                authority_reader=lambda: snapshot,
                smoke_root=smoke,
                math_repo_root=repo,
            )
            self.assertEqual(idempotent[0], receipt[0])
            with self.assertRaisesRegex(
                MathExactSmokeError, "math_smoke_prior_task_missing"
            ):
                apply_exact_smoke_capture(
                    auth_path,
                    formal_id="GS-566",
                    runtime_root=runtime,
                    authority_reader=lambda: snapshot,
                    smoke_root=smoke,
                    math_repo_root=repo,
                )

            ledger_rows = [
                json.loads(line)
                for line in (
                    repo / "数学一回滚复习系统/快速入库事件.jsonl"
                ).read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            exact_rows = [
                row
                for row in ledger_rows
                if row.get("event_id")
                in {
                    binding["capture_event_id"]
                    for binding in EXACT_SAMPLES.values()
                }
            ]
            self.assertEqual(len(exact_rows), 1)
            self.assertEqual(
                [row["event_id"] for row in exact_rows],
                [EXACT_SAMPLES["GS-269"]["capture_event_id"]],
            )
            self.assertEqual(
                len(
                    list(
                        (
                            runtime
                            / "dispatch/state/production-canary-queue/math"
                        ).glob("**/*.json")
                    )
                ),
                0,
            )

    def legacy_exact_five_capture_to_real_queue_and_terminal_chain(self) -> None:
        release_id = "1" * 64
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._isolated_repo(root)
            self._add_producer_sources(repo)
            smoke = root / "smoke"
            shutil.copytree(SMOKE_ROOT, smoke)
            runtime = root / "runtime"
            config = self._producer_config(root, repo, runtime, release_id)
            producer = producer_authority_binding(config, "math", release_id)
            store = LeaseStore(runtime)
            store.begin_subject_drain("math")
            state = store.activate_production_canary(
                "math",
                release_id=release_id,
                producer_authority=producer,
                activated_at="2026-08-13T00:00:00Z",
            )
            snapshot = {
                "schema_version": "subject_authority_snapshot_v1",
                "subject": "math",
                "generation": "math-generation-exact-test",
                "authority_fingerprint": "3" * 64,
                "mcp_server_release": "test",
                "route_request_id": "bounded-read-test",
                "scope_sha256": "9" * 64,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
            descriptor = build_authorization_descriptor(
                target_release_id=release_id,
                activation_id=str(state["activation_id"]),
                authority_generation=str(snapshot["generation"]),
                authority_fingerprint=str(
                    snapshot["authority_fingerprint"]
                ),
                producer_authority_fingerprint=str(
                    producer["authority_fingerprint"]
                ),
            )
            _auth_sha, auth_path, authorization = authorize_exact_smoke(
                descriptor,
                runtime_root=runtime,
                authority_snapshot=snapshot,
                authorized_at="2026-08-13T13:00:00Z",
                smoke_root=smoke,
                math_repo_root=repo,
            )
            self.assertEqual(
                authorization["authority_snapshot_mcp_tool_call_count"], 1
            )
            formal_preimages = {
                formal_id: hashlib.sha256(
                    (repo / str(binding["formal_card_path"])).read_bytes()
                ).hexdigest()
                for formal_id, binding in EXACT_SAMPLES.items()
            }
            first_dispatcher = ConcurrentDispatcher(
                runtime,
                lambda _task, _context: IdentityPublishingFixtureRunner(
                    GroundedRunner(), store
                ),
                soft_runtime_warning_seconds=2,
                stall_timeout_seconds=30,
                stall_probe_interval_seconds=1,
                stall_probe_required_consecutive_failures=2,
                production_canary=True,
            )
            queue_paths: list[Path] = []
            task_units: set[str] = set()
            self.assertEqual(
                pending_exact_smoke_dispatch_scope(
                    runtime_root=runtime,
                    math_repo_root=repo,
                    release_id="f" * 64,
                ),
                [],
            )
            first_apply_sha, _first_apply_path, first_apply = (
                apply_exact_smoke_capture(
                    auth_path,
                    formal_id="GS-269",
                    runtime_root=runtime,
                    authority_reader=lambda: snapshot,
                    smoke_root=smoke,
                    math_repo_root=repo,
                )
            )
            with self.assertRaisesRegex(
                MathExactSmokeError,
                "math_smoke_prior_task_missing",
            ):
                apply_exact_smoke_capture(
                    auth_path,
                    formal_id="GS-566",
                    runtime_root=runtime,
                    authority_reader=lambda: snapshot,
                    smoke_root=smoke,
                    math_repo_root=repo,
                )
            with mock.patch(
                "core_dispatch_bridge.current_date", return_value="2026-08-14"
            ):
                first_frozen, first_decisions = scan_eligible_candidates(
                    config, "math"
                )
            self.assertEqual(len(first_frozen), 1, first_decisions)
            first_task = first_frozen[0].task
            first_binding = first_task.frozen_payload[
                "math_exact_smoke_binding"
            ]
            self.assertEqual(first_binding["formal_id"], "GS-269")
            self.assertEqual(
                first_binding["capture_apply_receipt_sha256"], first_apply_sha
            )
            task_units.add(first_task.unit_sha256)
            for schema_name, instance in (
                (
                    "math-exact-smoke-capture-apply-receipt-v1.json",
                    first_apply,
                ),
                ("math-exact-smoke-task-binding-v1.json", first_binding),
            ):
                schema = json.loads(
                    (ROOT / "schemas" / schema_name).read_text(encoding="utf-8")
                )
                validation = subprocess.run(
                    [
                        "/opt/miniconda3/envs/dl/bin/python",
                        "-c",
                        (
                            "import json,sys; "
                            "from jsonschema import Draft202012Validator,FormatChecker; "
                            "v=json.load(sys.stdin); "
                            "Draft202012Validator.check_schema(v['schema']); "
                            "Draft202012Validator(v['schema'],format_checker=FormatChecker()).validate(v['instance'])"
                        ),
                    ],
                    input=json.dumps({"schema": schema, "instance": instance}),
                    text=True,
                    capture_output=True,
                    check=False,
                )
                self.assertEqual(
                    validation.returncode,
                    0,
                    f"{schema_name}: {validation.stderr}",
                )

            exact_files_before = {
                path.relative_to(runtime): hashlib.sha256(
                    path.read_bytes()
                ).hexdigest()
                for path in (runtime / "dispatch/math-exact-smoke").rglob(
                    "*.json"
                )
            }
            original_replace = concurrent_dispatch._atomic_replace_json

            def fail_queue_write(path, value):
                if "production-canary-queue" in path.parts:
                    raise OSError("synthetic-queue-write-failure")
                return original_replace(path, value)

            with mock.patch(
                "concurrent_dispatch._atomic_replace_json",
                side_effect=fail_queue_write,
            ):
                with self.assertRaisesRegex(
                    OSError, "synthetic-queue-write-failure"
                ):
                    store.materialize_production_canary_task(first_task)
            self.assertEqual(
                exact_files_before,
                {
                    path.relative_to(runtime): hashlib.sha256(
                        path.read_bytes()
                    ).hexdigest()
                    for path in (
                        runtime / "dispatch/math-exact-smoke"
                    ).rglob("*.json")
                },
            )

            first_queue = store.materialize_production_canary_task(first_task)
            first_queue_path = store._production_canary_queue_path(
                "math", str(first_queue["producer_input_contract_sha256"])
            )
            queue_paths.append(first_queue_path)
            first_result = first_dispatcher.submit(first_task).wait(15)
            self.assertEqual(first_result.status, "completed")
            self.assertEqual(first_result.outcome, "succeeded")

            unlock_sha256: str | None = None
            tail_apply_sha256s: dict[str, str] = {}
            for formal_id in EXACT_ORDER[1:]:
                apply_sha, _apply_path, apply_receipt = (
                    apply_exact_smoke_capture(
                        auth_path,
                        formal_id=formal_id,
                        runtime_root=runtime,
                        authority_reader=lambda: snapshot,
                        smoke_root=smoke,
                        math_repo_root=repo,
                    )
                )
                self.assertEqual(apply_receipt["queue_write_count"], 0)
                self.assertEqual(
                    apply_receipt["prior_sample_terminal"]["formal_id"],
                    "GS-269",
                )
                current_unlock = apply_receipt[
                    "prior_sample_terminal_sha256"
                ]
                if unlock_sha256 is None:
                    unlock_sha256 = current_unlock
                self.assertEqual(current_unlock, unlock_sha256)
                tail_apply_sha256s[formal_id] = apply_sha

            with mock.patch(
                "core_dispatch_bridge.current_date", return_value="2026-08-14"
            ):
                tail_frozen, tail_decisions = scan_eligible_candidates(
                    config, "math"
                )
            self.assertEqual(len(tail_frozen), 4, tail_decisions)
            self.assertEqual(
                {row.candidate.capture_id for row in tail_frozen},
                {
                    str(EXACT_SAMPLES[formal_id]["capture_event_id"])
                    for formal_id in EXACT_ORDER[1:]
                },
            )
            tail_tasks = sorted(
                (row.task for row in tail_frozen),
                key=lambda task: int(
                    task.frozen_payload["math_exact_smoke_binding"][
                        "sequence"
                    ]
                ),
            )
            tail_alternates: list[FrozenTask] = []
            for formal_id, task in zip(EXACT_ORDER[1:], tail_tasks):
                binding = task.frozen_payload["math_exact_smoke_binding"]
                self.assertEqual(binding["formal_id"], formal_id)
                self.assertEqual(
                    binding["capture_apply_receipt_sha256"],
                    tail_apply_sha256s[formal_id],
                )
                self.assertNotIn(task.unit_sha256, task_units)
                task_units.add(task.unit_sha256)
                queue = store.materialize_production_canary_task(task)
                self.assertEqual(queue["queue_status"], "pending")
                self.assertEqual(
                    queue["source_event_ids"],
                    [str(EXACT_SAMPLES[formal_id]["capture_event_id"])],
                )
                queue_path = store._production_canary_queue_path(
                    "math", str(queue["producer_input_contract_sha256"])
                )
                queue_paths.append(queue_path)
                alternate_payload = dict(task.frozen_payload)
                alternate_payload["exact_smoke_collision_probe"] = formal_id
                alternate = FrozenTask(alternate_payload)
                tail_alternates.append(alternate)
                with self.assertRaisesRegex(
                    DispatchError,
                    "math_smoke_processing_attempt_already_active",
                ):
                    store.materialize_production_canary_task(alternate)

            # Every already-queued exact Capture remains HMAC-, authority-,
            # generation-, task-, and apply-bound when the cross-midnight
            # exception is polled again. A validly sealed drift or a second
            # queue for one Capture fails closed instead of disappearing from
            # the pending set.
            probe_queue_path = queue_paths[-1]
            probe_queue_bytes = probe_queue_path.read_bytes()
            probe_queue = json.loads(probe_queue_bytes)
            drifted_queue = dict(probe_queue)
            drifted_queue["producer_authority_fingerprint"] = "f" * 64
            drifted_queue = store._seal(
                drifted_queue,
                purpose="dispatch-production-canary-queue",
            )
            probe_queue_path.write_text(
                json.dumps(drifted_queue, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                MathExactSmokeError, "math_smoke_queue_binding_drift"
            ):
                pending_exact_smoke_dispatch_scope(
                    runtime_root=runtime,
                    math_repo_root=repo,
                    release_id=release_id,
                )
            probe_queue_path.write_bytes(probe_queue_bytes)

            duplicate_queue_path = (
                runtime
                / "dispatch/state/production-canary-queue/math/ff"
                / "duplicate-exact-queue.json"
            )
            duplicate_queue_path.parent.mkdir(parents=True, exist_ok=True)
            duplicate_queue_path.write_bytes(probe_queue_bytes)
            try:
                with self.assertRaisesRegex(
                    MathExactSmokeError, "math_smoke_queue_duplicate"
                ):
                    pending_exact_smoke_dispatch_scope(
                        runtime_root=runtime,
                        math_repo_root=repo,
                        release_id=release_id,
                    )
            finally:
                duplicate_queue_path.unlink()

            tail_barrier = TailBarrierGroundedRunner()
            tail_dispatcher = ConcurrentDispatcher(
                runtime,
                lambda _task, _context: IdentityPublishingFixtureRunner(
                    tail_barrier, store
                ),
                soft_runtime_warning_seconds=2,
                stall_timeout_seconds=30,
                stall_probe_interval_seconds=1,
                stall_probe_required_consecutive_failures=2,
                production_canary=True,
            )
            tail_handles = [
                tail_dispatcher.submit(task) for task in tail_tasks
            ]
            self.assertTrue(
                tail_barrier.all_entered.wait(10),
                "four tail tasks did not overlap",
            )
            try:
                active_state = store.production_canary_status_read_only(
                    "math"
                )
                self.assertEqual(active_state["active_task_count"], 4)
                telemetry = store.production_canary_concurrency_telemetry(
                    release_id=release_id
                )
                self.assertEqual(
                    telemetry["active_by_subject"]["math"], 4, telemetry
                )
                self.assertEqual(
                    telemetry["scheduler_claim_subject_peak_active"][
                        "math"
                    ],
                    4,
                    telemetry,
                )
            finally:
                tail_barrier.release.set()
            tail_results = [handle.wait(15) for handle in tail_handles]
            self.assertTrue(
                all(
                    result.status == "completed"
                    and result.outcome == "succeeded"
                    for result in tail_results
                ),
                tail_results,
            )
            for queue_path, alternate in zip(
                queue_paths[1:], tail_alternates
            ):
                terminal_queue = json.loads(
                    queue_path.read_text(encoding="utf-8")
                )
                store._verify_seal(
                    terminal_queue,
                    purpose="dispatch-production-canary-queue",
                )
                self.assertEqual(terminal_queue["queue_status"], "succeeded")
                self.assertEqual(terminal_queue["formal_write_count"], 0)
                with self.assertRaisesRegex(
                    DispatchError,
                    "math_smoke_accepted_package_already_exists",
                ):
                    store.materialize_production_canary_task(alternate)

            self.assertEqual(len(queue_paths), 5)
            self.assertEqual(len(set(queue_paths)), 5)
            self.assertEqual(len(task_units), 5)
            self.assertEqual(
                {
                    formal_id: hashlib.sha256(
                        (repo / str(binding["formal_card_path"])).read_bytes()
                    ).hexdigest()
                    for formal_id, binding in EXACT_SAMPLES.items()
                },
                formal_preimages,
            )
            self.assertFalse((root / "no-model-codex.called").exists())
            self.assertFalse(
                any(
                    "queue-binding" in path.name
                    for path in (
                        runtime / "dispatch/math-exact-smoke"
                    ).rglob("*.json")
                )
            )
            final_state = store.production_canary_status("math")
            self.assertEqual(final_state["state"], "continuous_concurrent_unlocked")
            self.assertEqual(final_state["active_task_count"], 0)
            self.assertEqual(final_state["formal_write_count"], 0)

            # Once every exact Capture has its unique accepted queue, the
            # exception disappears and ordinary math remains current-day.
            with mock.patch(
                "core_dispatch_bridge.current_date",
                return_value="2026-08-14",
            ):
                ordinary, ordinary_decisions = scan_eligible_candidates(
                    config,
                    "math",
                )
            exact_ids = {
                str(binding["capture_event_id"])
                for binding in EXACT_SAMPLES.values()
            }
            self.assertTrue(
                all(row.candidate.study_date == "2026-08-14" for row in ordinary),
                ordinary_decisions,
            )
            self.assertTrue(
                all(row.candidate.capture_id not in exact_ids for row in ordinary),
                ordinary_decisions,
            )

    def legacy_unauthorized_exact_capture_cannot_cross_midnight_filter(self) -> None:
        descriptor = self.descriptor()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._isolated_repo(root)
            self._add_producer_sources(repo)
            smoke = root / "smoke"
            shutil.copytree(SMOKE_ROOT, smoke)
            runtime, snapshot = self._isolated_runtime(root, descriptor)
            _auth_sha, auth_path, _authorization = authorize_exact_smoke(
                descriptor,
                runtime_root=runtime,
                authority_snapshot=snapshot,
                authorized_at="2026-08-13T13:00:00Z",
                smoke_root=smoke,
                math_repo_root=repo,
            )
            apply_exact_smoke_capture(
                auth_path,
                formal_id="GS-269",
                runtime_root=runtime,
                authority_reader=lambda: snapshot,
                smoke_root=smoke,
                math_repo_root=repo,
            )
            capture_id = str(EXACT_SAMPLES["GS-269"]["capture_event_id"])
            # Keep the exact ledger event but remove its test-only authority
            # and apply proofs. It must not gain the cross-midnight route.
            shutil.rmtree(runtime / "dispatch/math-exact-smoke")
            config = self._producer_config(
                root, repo, runtime, str(descriptor["target_release_id"])
            )

            with mock.patch(
                "core_dispatch_bridge.current_date",
                return_value="2026-08-14",
            ):
                frozen, decisions = scan_eligible_candidates(config, "math")
            self.assertTrue(
                all(row.candidate.capture_id != capture_id for row in frozen),
                decisions,
            )
            self.assertTrue(
                all(row.get("capture_id") != capture_id for row in decisions),
                decisions,
            )

            # If explicitly inspected on its own historical date, the exact
            # identity still fails closed for its missing apply receipt.
            with mock.patch(
                "core_dispatch_bridge.current_date",
                return_value="2026-08-13",
            ):
                rejected, rejection_decisions = scan_eligible_candidates(
                    config,
                    "math",
                    capture_allowlist=frozenset({capture_id}),
                )
            self.assertEqual(rejected, [])
            self.assertEqual(len(rejection_decisions), 1)
            self.assertEqual(
                rejection_decisions[0]["error_code"],
                "math_smoke_capture_apply_receipt_missing",
            )
            self.assertFalse(
                rejection_decisions[0]["model_enqueue_allowed"]
            )

    def legacy_prior_review_terminal_accepts_only_sol_review_candidate(self) -> None:
        for disposition, next_capture_allowed in (
            ("needs_sol_review", True),
            ("quarantined", False),
        ):
            with self.subTest(disposition=disposition):
                with tempfile.TemporaryDirectory() as temporary:
                    root = Path(temporary)
                    repo = self._isolated_repo(root)
                    self._add_producer_sources(repo)
                    smoke = root / "smoke"
                    shutil.copytree(SMOKE_ROOT, smoke)
                    runtime = root / "runtime"
                    release_id = "1" * 64
                    config = self._producer_config(
                        root, repo, runtime, release_id
                    )
                    producer = producer_authority_binding(
                        config, "math", release_id
                    )
                    store = LeaseStore(runtime)
                    store.begin_subject_drain("math")
                    state = store.activate_production_canary(
                        "math",
                        release_id=release_id,
                        producer_authority=producer,
                        activated_at="2026-08-13T00:00:00Z",
                    )
                    snapshot = {
                        "schema_version": "subject_authority_snapshot_v1",
                        "subject": "math",
                        "generation": "math-generation-exact-test",
                        "authority_fingerprint": "3" * 64,
                        "mcp_server_release": "test",
                        "route_request_id": "bounded-read-test",
                        "scope_sha256": "9" * 64,
                        "model_call_count": 0,
                        "formal_write_count": 0,
                    }
                    descriptor = build_authorization_descriptor(
                        target_release_id=release_id,
                        activation_id=str(state["activation_id"]),
                        authority_generation=str(snapshot["generation"]),
                        authority_fingerprint=str(
                            snapshot["authority_fingerprint"]
                        ),
                        producer_authority_fingerprint=str(
                            producer["authority_fingerprint"]
                        ),
                    )
                    _auth_sha, auth_path, _authorization = (
                        authorize_exact_smoke(
                            descriptor,
                            runtime_root=runtime,
                            authority_snapshot=snapshot,
                            authorized_at="2026-08-13T13:00:00Z",
                            smoke_root=smoke,
                            math_repo_root=repo,
                        )
                    )
                    apply_exact_smoke_capture(
                        auth_path,
                        formal_id="GS-269",
                        runtime_root=runtime,
                        authority_reader=lambda: snapshot,
                        smoke_root=smoke,
                        math_repo_root=repo,
                    )
                    capture_id = str(
                        EXACT_SAMPLES["GS-269"]["capture_event_id"]
                    )
                    with mock.patch(
                        "core_dispatch_bridge.current_date",
                        return_value="2026-08-13",
                    ):
                        frozen, decisions = scan_eligible_candidates(
                            config,
                            "math",
                            capture_allowlist=frozenset({capture_id}),
                        )
                    self.assertEqual(len(frozen), 1, decisions)
                    payload = dict(frozen[0].task.frozen_payload)
                    payload["test_report_disposition"] = disposition
                    task = FrozenTask(payload)
                    store.materialize_production_canary_task(task)
                    config_path = root / "runner-config.json"
                    config_path.write_text("{}\n", encoding="utf-8")
                    dispatcher = ConcurrentDispatcher(
                        runtime,
                        lambda _task, _context: CoreCandidateSubprocessRunner(
                            config_path,
                            command=[sys.executable, str(REVIEW_FIXTURE)],
                            lease_store=store,
                        ),
                        stage_timeout_seconds=5,
                        production_canary=True,
                    )
                    result = dispatcher.submit(task).wait(15)
                    self.assertEqual(result.status, "completed")
                    self.assertEqual(
                        result.outcome,
                        "succeeded" if next_capture_allowed else "failed",
                    )
                    if next_capture_allowed:
                        unlock_sha256: str | None = None
                        for formal_id in EXACT_ORDER[1:]:
                            _digest, _path, receipt = (
                                apply_exact_smoke_capture(
                                    auth_path,
                                    formal_id=formal_id,
                                    runtime_root=runtime,
                                    authority_reader=lambda: snapshot,
                                    smoke_root=smoke,
                                    math_repo_root=repo,
                                )
                            )
                            self.assertEqual(
                                receipt["prior_sample_terminal"][
                                    "formal_id"
                                ],
                                "GS-269",
                            )
                            self.assertEqual(
                                receipt["prior_sample_terminal"][
                                    "terminal_status"
                                ],
                                "needs_sol_review",
                            )
                            if unlock_sha256 is None:
                                unlock_sha256 = receipt[
                                    "prior_sample_terminal_sha256"
                                ]
                            self.assertEqual(
                                receipt[
                                    "prior_sample_terminal_sha256"
                                ],
                                unlock_sha256,
                            )
                            self.assertEqual(
                                receipt["formal_write_count"], 0
                            )
                        with mock.patch(
                            "core_dispatch_bridge.current_date",
                            return_value="2026-08-14",
                        ):
                            tail, tail_decisions = scan_eligible_candidates(
                                config, "math"
                            )
                        self.assertEqual(len(tail), 4, tail_decisions)
                        self.assertEqual(
                            {row.candidate.capture_id for row in tail},
                            {
                                str(
                                    EXACT_SAMPLES[formal_id][
                                        "capture_event_id"
                                    ]
                                )
                                for formal_id in EXACT_ORDER[1:]
                            },
                        )
                    else:
                        with self.assertRaisesRegex(
                            MathExactSmokeError,
                            "math_smoke_prior_task_not_accepted_terminal",
                        ):
                            apply_exact_smoke_capture(
                                auth_path,
                                formal_id="GS-566",
                                runtime_root=runtime,
                                authority_reader=lambda: snapshot,
                                smoke_root=smoke,
                                math_repo_root=repo,
                            )

    def legacy_runtime_rolls_exact_gs269_review_batch_before_tail_submit(
        self,
    ) -> None:
        release_id = "1" * 64
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repo = self._isolated_repo(root)
            self._add_producer_sources(repo)
            smoke = root / "smoke"
            shutil.copytree(SMOKE_ROOT, smoke)
            runtime_root = root / "runtime"
            config = self._producer_config(
                root, repo, runtime_root, release_id
            )
            config_path = root / "runtime-config.json"
            config_path.write_text(
                json.dumps(config, ensure_ascii=False, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            runtime = ProductionDispatchRuntime(
                config, "math", config_path
            )
            store = runtime.dispatcher.lease_store
            producer = producer_authority_binding(
                config, "math", release_id
            )
            store.begin_subject_drain("math")
            canary = store.activate_production_canary(
                "math",
                release_id=release_id,
                producer_authority=producer,
                activated_at="2026-08-13T00:00:00Z",
                continuous_concurrency_limit=20,
            )
            snapshot = {
                "schema_version": "subject_authority_snapshot_v1",
                "subject": "math",
                "generation": "math-generation-exact-test",
                "authority_fingerprint": "3" * 64,
                "mcp_server_release": "test",
                "route_request_id": "bounded-read-test",
                "scope_sha256": "9" * 64,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
            runtime.processing_host = SimpleNamespace(
                subject_authority_snapshot=lambda subject: {
                    **snapshot,
                    "subject": subject,
                }
            )
            runtime.dispatcher.runner_factory = (
                lambda _task, _context: CoreCandidateSubprocessRunner(
                    config_path,
                    command=[sys.executable, str(REVIEW_FIXTURE)],
                    lease_store=store,
                )
            )
            descriptor = build_authorization_descriptor(
                target_release_id=release_id,
                activation_id=str(canary["activation_id"]),
                authority_generation=str(snapshot["generation"]),
                authority_fingerprint=str(
                    snapshot["authority_fingerprint"]
                ),
                producer_authority_fingerprint=str(
                    producer["authority_fingerprint"]
                ),
            )
            _auth_sha, auth_path, _authorization = authorize_exact_smoke(
                descriptor,
                runtime_root=runtime_root,
                authority_snapshot=snapshot,
                authorized_at="2026-08-13T13:00:00Z",
                smoke_root=smoke,
                math_repo_root=repo,
            )
            apply_exact_smoke_capture(
                auth_path,
                formal_id="GS-269",
                runtime_root=runtime_root,
                authority_reader=lambda: snapshot,
                smoke_root=smoke,
                math_repo_root=repo,
            )
            with mock.patch(
                "core_dispatch_bridge.current_date",
                return_value="2026-08-14",
            ), mock.patch(
                "preprocess_dispatcher.current_date",
                return_value="2026-08-14",
            ), mock.patch(
                "preprocess_dispatcher._write_subject_projections"
            ):
                first_handles, first_decisions = runtime.scan_and_submit()
            self.assertEqual(len(first_handles), 1, first_decisions)
            first_result = first_handles[0].wait(15)
            self.assertEqual(first_result.outcome, "succeeded")

            def reopen_early_review_fixture(
                subject,
                capture_id,
                *,
                expected_release_id,
                expected_unit_sha256=None,
                expected_generation=None,
                expected_input_fingerprint=None,
            ):
                del (
                    expected_release_id,
                    expected_generation,
                    expected_input_fingerprint,
                )
                latest = store._read_object(
                    store._latest_path(subject, capture_id)
                )
                completion = store._read_object(
                    Path(str(latest["completion_path"]))
                )
                self.assertEqual(
                    completion["unit_sha256"], expected_unit_sha256
                )
                receipt = store._read_object(
                    Path(str(completion["receipt_path"]))
                )
                ledger = store._read_object(
                    Path(str(completion["ledger_entry_path"]))
                )
                package = store._read_object(
                    Path(str(completion["package_path"]))
                )
                return {
                    "latest": latest,
                    "completion": completion,
                    "receipt": receipt,
                    "ledger_entry": ledger,
                    "package": package,
                    "member_publication": None,
                }

            # The stock one-stage review fixture intentionally predates the
            # two-stage authoritative reopener.  Reopen its real immutable
            # objects here so this test can exercise the Production runtime's
            # subject-batch projection, exact rollover, and next normal scan.
            with mock.patch.object(
                store,
                "verify_authoritative_completion",
                side_effect=reopen_early_review_fixture,
            ):
                persistence = runtime.persist_finished_luna_with_status(
                    first_handles
                )
            self.assertEqual(persistence["failures"], [], persistence)
            projected = persistence["projected"]
            self.assertEqual(len(projected), 1)
            writer = runtime.subject_sol.read_subject("math")[
                "writer_state"
            ]
            self.assertEqual(writer["handoff_status"], "awaiting_luna")
            self.assertIsNone(writer["batch_id"])
            self.assertEqual(writer["formal_write_count"], 0)
            rollover_pointer = json.loads(
                runtime.subject_sol._background_rollover_pointer_path(
                    "math"
                ).read_text(encoding="utf-8")
            )
            rollover = runtime.subject_sol._read_background_rollover_receipt(
                rollover_pointer["rollover_receipt_sha256"]
            )
            self.assertEqual(
                rollover["mode"],
                "explicit_failure_resume",
            )
            self.assertEqual(
                rollover["resume_acceptance_sha256"],
                store.production_canary_status("math")[
                    "last_terminal_receipt_sha256"
                ],
            )
            self.assertEqual(rollover["formal_write_count"], 0)

            for formal_id in EXACT_ORDER[1:]:
                apply_exact_smoke_capture(
                    auth_path,
                    formal_id=formal_id,
                    runtime_root=runtime_root,
                    authority_reader=lambda: snapshot,
                    smoke_root=smoke,
                    math_repo_root=repo,
                )
            with mock.patch(
                "core_dispatch_bridge.current_date",
                return_value="2026-08-14",
            ), mock.patch(
                "preprocess_dispatcher.current_date",
                return_value="2026-08-14",
            ), mock.patch(
                "preprocess_dispatcher._write_subject_projections"
            ):
                tail_handles, tail_decisions = runtime.scan_and_submit()
            self.assertEqual(len(tail_handles), 4, tail_decisions)
            tail_batch = runtime.subject_sol.read_subject_batch("math")
            self.assertEqual(len(tail_batch["tasks"]), 4)
            self.assertEqual(
                {row["capture_id"] for row in tail_batch["tasks"]},
                {
                    str(EXACT_SAMPLES[formal_id]["capture_event_id"])
                    for formal_id in EXACT_ORDER[1:]
                },
            )
            self.assertEqual(tail_batch["formal_write_count"], 0)
            tail_results = [handle.wait(15) for handle in tail_handles]
            self.assertTrue(
                all(result.outcome == "succeeded" for result in tail_results),
                tail_results,
            )

if __name__ == "__main__":
    unittest.main()
