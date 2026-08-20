#!/usr/bin/env python3

from __future__ import annotations

import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT / "dashboard"))
sys.path.insert(0, str(ROOT / "tests"))

import preprocessor_core as preprocessor  # noqa: E402
import core_dispatch_bridge as bridge  # noqa: E402

from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    DispatchError,
    FrozenTask,
    LeaseStore,
    REQUIRED_MODEL,
    REQUIRED_REASONING_EFFORT,
    StageResult,
)
from core_dispatch_bridge import (  # noqa: E402
    content_processing_identity,
    scan_eligible_candidates,
)
from dashboard.server import _load_dispatch_task_detail  # noqa: E402
from preprocessor_core import (  # noqa: E402
    Candidate,
    ModelResult,
    subject_semantic_code_closure_manifest_from_path,
)
from concurrent_dispatch import dispatch_rule_binding  # noqa: E402
from preprocess_dispatcher import ProductionDispatchRuntime  # noqa: E402
from preprocess_task_runner import run_request  # noqa: E402
from tests.test_concurrent_dispatch import core_candidate  # noqa: E402
from fixtures.scanner_worker_fake import (  # noqa: E402
    make_scanner_worker_factory,
)


RELEASE_ID = "a" * 64
PROCESSING_CONTRACT = "9" * 64


def english_candidate(
    capture_id: str,
    *,
    recorded_at: str,
    content: object = "identical frozen content",
) -> Candidate:
    return Candidate(
        subject="english",
        capture_id=capture_id,
        study_date="2026-08-05",
        recorded_at=recorded_at,
        input_fingerprint=(capture_id.lower().replace("-", "") + "0" * 64)[:64],
        input_binding={
            "processing_contract_sha256": PROCESSING_CONTRACT,
            "submission_capture_id": capture_id,
            "distribution_slot": f"slot:{capture_id}",
        },
        model_input={"content": content, "evidence": {"fact": "same"}},
        allowed_evidence_refs=("evidence.fact",),
        image_paths=(),
        target_label=f"display:{capture_id}",
        canonical_state="pending",
        sol_state="pending_review",
    )


def duplicate_cs408(candidate: Candidate, capture_id: str, recorded_at: str) -> Candidate:
    return Candidate(
        **{
            **candidate.__dict__,
            "capture_id": capture_id,
            "recorded_at": recorded_at,
            "input_fingerprint": (capture_id.lower().replace("-", "") + "f" * 64)[:64],
            "input_binding": {
                **candidate.input_binding,
                "submission_capture_id": capture_id,
                "distribution_slot": f"slot:{capture_id}",
            },
            "target_label": f"display:{capture_id}",
        }
    )


class CountingRunner:
    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts

    @staticmethod
    def _stage(stage: str) -> StageResult:
        return StageResult(
            payload={"stage": stage, "public": "structured"},
            runtime_model=REQUIRED_MODEL,
            runtime_reasoning_effort=REQUIRED_REASONING_EFFORT,
            runtime_metadata_provenance="codex_json_attestation_v1",
            runtime_identity_status="confirmed",
            duration_ms=1,
        )

    def run_analysis(self, _task, _context):
        self.counts["analysis"] += 1
        return self._stage("analysis")

    def run_critical_review(self, _task, _draft, _context):
        self.counts["critical_review"] += 1
        return self._stage("critical_review")


class ContentDedupIsolationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / "runtime"
        self.runtime.mkdir(parents=True)
        self.config = {
            "timezone": "Asia/Shanghai",
            "runtime_root": str(self.runtime),
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
            },
        }
        self.scan_worker = make_scanner_worker_factory(
            release_id=RELEASE_ID,
            adapters={
                "cs408": SimpleNamespace(
                    candidate_diagnostics={}, candidate_errors={}
                ),
                "english": SimpleNamespace(
                    candidate_diagnostics={}, candidate_errors={}
                ),
            },
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _scan(self, subject: str, rows: list[tuple[Candidate, str]]):
        self.scan_worker.set_candidates(rows)
        return scan_eligible_candidates(
            self.config, subject, worker_factory=self.scan_worker
        )

    def _math_image_candidate(
        self,
        *,
        artifacts: list[dict[str, object]],
        image_paths: tuple[Path, ...],
    ) -> Candidate:
        return Candidate(
            subject="math",
            capture_id="MATH-IMAGE-BINDING-001",
            study_date="2026-08-13",
            recorded_at="2026-08-13T08:00:00+08:00",
            input_fingerprint="8" * 64,
            input_binding={
                "processing_contract_sha256": PROCESSING_CONTRACT,
                "evidence_bundle_sha256": "7" * 64,
            },
            model_input={"source_bundle": {"artifacts": artifacts}},
            allowed_evidence_refs=(),
            image_paths=image_paths,
            target_label="math image binding",
            canonical_state="awaiting_background_analysis",
            sol_state="pending_review",
        )

    def test_math_image_binding_uses_all_provided_roles_in_original_order(
        self,
    ) -> None:
        roles = ("question", "solution", "user_work", "reference")
        paths: list[Path] = []
        artifacts: list[dict[str, object]] = []
        for index, role in enumerate(roles):
            path = self.runtime / f"{index}-{role}.png"
            path.write_bytes(f"image:{role}".encode("utf-8"))
            digest = preprocessor.sha256_file(path)
            paths.append(path)
            artifacts.append(
                {
                    "role": role,
                    "sha256": digest,
                    "provided_to_model": True,
                }
            )
        artifacts.append(
            {
                "role": "reference",
                "sha256": "f" * 64,
                "provided_to_model": False,
            }
        )
        candidate = self._math_image_candidate(
            artifacts=artifacts, image_paths=tuple(paths)
        )
        identity = content_processing_identity(
            candidate,
            dispatch_rule_binding(
                release_id=RELEASE_ID,
                subject="math",
                subject_processing_contract_sha256=PROCESSING_CONTRACT,
            ),
        )
        self.assertEqual(
            identity["ordered_image_sha256s"],
            [preprocessor.sha256_file(path) for path in paths],
        )

        valid_role_sets = (
            ("question", "solution"),
            ("question", "solution", "reference"),
            ("question", "solution", "user_work"),
            roles,
        )
        for selected_roles in valid_role_sets:
            selected = [
                (path, artifact)
                for path, artifact in zip(paths, artifacts[:4])
                if artifact["role"] in selected_roles
            ]
            selected_paths = tuple(path for path, _artifact in selected)
            selected_artifacts = [artifact for _path, artifact in selected]
            selected_artifacts.append(
                {
                    "role": "reference",
                    "sha256": "f" * 64,
                    "provided_to_model": False,
                }
            )
            with self.subTest(valid_roles=selected_roles):
                selected_identity = content_processing_identity(
                    self._math_image_candidate(
                        artifacts=selected_artifacts,
                        image_paths=selected_paths,
                    ),
                    dispatch_rule_binding(
                        release_id=RELEASE_ID,
                        subject="math",
                        subject_processing_contract_sha256=(
                            PROCESSING_CONTRACT
                        ),
                    ),
                )
                self.assertEqual(
                    selected_identity["ordered_image_sha256s"],
                    [
                        preprocessor.sha256_file(path)
                        for path in selected_paths
                    ],
                )

        with self.assertRaisesRegex(
            DispatchError, "candidate_image_evidence_hash_mismatch"
        ):
            content_processing_identity(
                self._math_image_candidate(
                    artifacts=artifacts,
                    image_paths=(paths[1], paths[0], *paths[2:]),
                ),
                dispatch_rule_binding(
                    release_id=RELEASE_ID,
                    subject="math",
                    subject_processing_contract_sha256=PROCESSING_CONTRACT,
                ),
            )

        extra = self.runtime / "extra.png"
        extra.write_bytes(b"extra image")
        malformed_paths = (
            tuple(paths[:-1]),
            (*paths, extra),
        )
        for candidate_paths in malformed_paths:
            with self.subTest(path_count=len(candidate_paths)), self.assertRaisesRegex(
                DispatchError, "candidate_image_evidence_hash_mismatch"
            ):
                content_processing_identity(
                    self._math_image_candidate(
                        artifacts=artifacts,
                        image_paths=tuple(candidate_paths),
                    ),
                    dispatch_rule_binding(
                        release_id=RELEASE_ID,
                        subject="math",
                        subject_processing_contract_sha256=(
                            PROCESSING_CONTRACT
                        ),
                    ),
                )

        paths[2].write_bytes(b"tampered user work")
        with self.assertRaisesRegex(
            DispatchError, "candidate_image_evidence_hash_mismatch"
        ):
            content_processing_identity(
                candidate,
                dispatch_rule_binding(
                    release_id=RELEASE_ID,
                    subject="math",
                    subject_processing_contract_sha256=PROCESSING_CONTRACT,
                ),
            )

    def test_math_image_binding_rejects_malformed_provided_artifacts(self) -> None:
        path = self.runtime / "question.png"
        path.write_bytes(b"question image")
        digest = preprocessor.sha256_file(path)
        invalid_rows = (
            {"role": "solution_text", "sha256": digest, "provided_to_model": True},
            {"role": "question", "sha256": digest},
            {"role": "question", "sha256": digest, "provided_to_model": 1},
            {"role": "question", "sha256": digest.upper(), "provided_to_model": True},
        )
        rule = dispatch_rule_binding(
            release_id=RELEASE_ID,
            subject="math",
            subject_processing_contract_sha256=PROCESSING_CONTRACT,
        )
        for row in invalid_rows:
            with self.subTest(row=row), self.assertRaisesRegex(
                DispatchError, "candidate_image_evidence_binding_invalid"
            ):
                content_processing_identity(
                    self._math_image_candidate(
                        artifacts=[row], image_paths=(path,)
                    ),
                    rule,
                )

    def test_complete_release_identity_is_not_content_semantic_identity(self):
        candidate = english_candidate(
            "EN-RELEASE-ISOLATED",
            recorded_at="2026-08-05T01:00:00Z",
        )
        first = content_processing_identity(
            candidate,
            dispatch_rule_binding(
                release_id="1" * 64,
                subject="english",
                subject_processing_contract_sha256=PROCESSING_CONTRACT,
            ),
        )
        second = content_processing_identity(
            candidate,
            dispatch_rule_binding(
                release_id="2" * 64,
                subject="english",
                subject_processing_contract_sha256=PROCESSING_CONTRACT,
            ),
        )
        self.assertEqual(
            first["content_processing_id"], second["content_processing_id"]
        )

    def test_subject_code_closure_isolates_subject_and_tracks_real_shared_code(self):
        source_path = ROOT / "lib" / "preprocessor_core.py"
        source = source_path.read_text(encoding="utf-8")
        baseline = {
            subject: subject_semantic_code_closure_manifest_from_path(
                subject, source_path
            )
            for subject in ("math", "cs408", "english")
        }
        math_only_path = Path(self.temp.name) / "math-only-core.py"
        math_only_path.write_text(
            source.replace(
                'class MathAdapter(BaseAdapter):\n    subject = "math"',
                'class MathAdapter(BaseAdapter):\n    # math-only semantic change\n    subject = "math"',
                1,
            ),
            encoding="utf-8",
        )
        math_only = {
            subject: subject_semantic_code_closure_manifest_from_path(
                subject, math_only_path
            )
            for subject in ("math", "cs408", "english")
        }
        self.assertNotEqual(
            baseline["math"]["code_closure_sha256"],
            math_only["math"]["code_closure_sha256"],
        )
        self.assertEqual(
            baseline["cs408"]["code_closure_sha256"],
            math_only["cs408"]["code_closure_sha256"],
        )
        self.assertEqual(
            baseline["english"]["code_closure_sha256"],
            math_only["english"]["code_closure_sha256"],
        )

        shared_path = Path(self.temp.name) / "shared-core.py"
        shared_path.write_text(
            source.replace(
                "def sha256_value(value: Any) -> str:\n    return",
                "def sha256_value(value: Any) -> str:\n    # shared semantic change\n    return",
                1,
            ),
            encoding="utf-8",
        )
        for subject in ("math", "cs408", "english"):
            changed = subject_semantic_code_closure_manifest_from_path(
                subject, shared_path
            )
            self.assertNotEqual(
                baseline[subject]["code_closure_sha256"],
                changed["code_closure_sha256"],
            )

    def test_subject_only_prompt_changes_only_its_contract_and_fingerprint(self):
        cs408_status_script = self.runtime / "cs408-status.py"
        cs408_status_script.write_text("raise SystemExit(0)\n", encoding="utf-8")
        config = {
            "model": {
                "model": REQUIRED_MODEL,
                "reasoning_effort": REQUIRED_REASONING_EFFORT,
                "codex_path": sys.executable,
                "max_images": 4,
            },
            "cs408_deep_v2": {
                "enabled": True,
                "analysis_prompt_version": "a",
                "critical_review_prompt_version": "b",
                "analysis_output_schema": "analysis-408",
                "critical_review_output_schema": "review-408",
                "package_output_schema": "package-408",
                "controlled_contract_path": "contract-408",
                "max_output_bytes": 1,
                "max_prompt_bytes": 1,
                "soft_runtime_warning_seconds": 60,
                "stall_timeout_seconds": 60,
                "stall_probe_interval_seconds": 1,
                "stall_probe_required_consecutive_failures": 2,
            },
            "cs408_knowledge_snapshot": {"enabled": True},
            "math_deep_v2": {
                "enabled": True,
                "mode": "shadow",
                "canary_percent": 10,
                "shadow_evaluation_start_at": "2026-08-01T00:00:00Z",
                "shadow_evaluation_target_count": 1,
                "analysis_prompt_version": "a",
                "critical_review_prompt_version": "b",
                "analysis_output_schema": "analysis-math",
                "critical_review_output_schema": "review-math",
                "package_output_schema": "package-math",
                "soft_runtime_warning_seconds": 60,
                "stall_timeout_seconds": 60,
                "stall_probe_interval_seconds": 1,
                "stall_probe_required_consecutive_failures": 2,
                "max_output_bytes": 1,
                "max_prompt_bytes": 1,
                "min_complete_chinese_chars": 1,
                "max_chinese_chars": 10,
                "gs111_cold_replay_max_seconds": 1800,
                "three_replay_p95_max_seconds": 1200,
            },
            "english_two_pass_v1": {
                "enabled": True,
                "analysis_prompt_version": "a",
                "critical_review_prompt_version": "b",
                "analysis_output_schema": "analysis-english",
                "critical_review_output_schema": "review-english",
                "controlled_contract_path": "contract-english",
                "soft_runtime_warning_seconds": 60,
                "stall_timeout_seconds": 60,
                "stall_probe_interval_seconds": 1,
                "stall_probe_required_consecutive_failures": 2,
                "max_output_bytes": 1,
                "max_prompt_bytes": 1,
            },
            "adapters": {
                "cs408": {
                    "status_script": str(cs408_status_script),
                },
                "english": {
                    "repo_root": "/Users/xiazhibin/Documents/kaoyan-english",
                    "candidate_schema": (
                        "/Users/xiazhibin/Documents/kaoyan-english/"
                        "schema/english_pipeline/luna-candidate-v2.schema.json"
                    ),
                }
            },
        }

        def contracts():
            return {
                "math": preprocessor.math_processing_contract(config)[
                    "processing_contract_sha256"
                ],
                "cs408": preprocessor.cs408_processing_contract(config)[
                    "processing_contract_sha256"
                ],
                "english": preprocessor.english_processing_contract(config)[
                    "processing_contract_sha256"
                ],
            }

        with mock.patch.object(
            preprocessor,
            "_schema_sha256",
            side_effect=lambda value: preprocessor.sha256_text(str(value)),
        ):
            baseline = contracts()
            prompt_cases = {
                "math": "MATH_ANALYSIS_PROMPT_TEMPLATE",
                "cs408": "CS408_ANALYSIS_PROMPT_TEMPLATE",
                "english": "ENGLISH_CANDIDATE_PROMPT_TEMPLATE",
            }
            for changed_subject, prompt_name in prompt_cases.items():
                with self.subTest(subject=changed_subject), mock.patch.object(
                    preprocessor,
                    prompt_name,
                    getattr(preprocessor, prompt_name) + " subject-only-change",
                ):
                    changed = contracts()
                for subject in ("math", "cs408", "english"):
                    if subject == changed_subject:
                        self.assertNotEqual(
                            baseline[subject], changed[subject]
                        )
                    else:
                        self.assertEqual(baseline[subject], changed[subject])
                evidence = {"frozen_evidence_sha256": "e" * 64}
                baseline_fingerprints = {
                    subject: preprocessor.sha256_value(
                        {
                            **evidence,
                            "processing_contract_sha256": baseline[subject],
                        }
                    )
                    for subject in baseline
                }
                changed_fingerprints = {
                    subject: preprocessor.sha256_value(
                        {
                            **evidence,
                            "processing_contract_sha256": changed[subject],
                        }
                    )
                    for subject in changed
                }
                self.assertNotEqual(
                    baseline_fingerprints[changed_subject],
                    changed_fingerprints[changed_subject],
                )
                for subject in set(baseline) - {changed_subject}:
                    self.assertEqual(
                        baseline_fingerprints[subject],
                        changed_fingerprints[subject],
                    )

    def test_semantic_reuse_receipt_is_idempotent_and_read_only(self):
        material = {
            "subject": "cs408",
            "capture_id": "CAP-SEMANTIC-REUSE",
            "compatibility_reason": "legacy_global_core_only_migration",
            "formal_write_count": 0,
            "model_call_count": 0,
        }
        hash_fields = (
            "current_release_id",
            "current_loaded_core_sha256",
            "current_processing_contract_sha256",
            "current_code_closure_sha256",
            "source_release_id",
            "source_loaded_core_sha256",
            "source_processing_contract_sha256",
            "source_code_closure_sha256",
            "source_unit_sha256",
            "source_content_processing_id",
            "source_completion_sha256",
            "source_receipt_sha256",
            "source_ledger_entry_sha256",
            "source_package_sha256",
            "semantic_evidence_capsule_sha256",
            "normalized_contract_sha256",
        )
        for index, field in enumerate(hash_fields, start=1):
            material[field] = f"{index:064x}"
        store = LeaseStore(self.runtime)
        first = store.publish_semantic_package_reuse(material)
        second = store.publish_semantic_package_reuse(material)
        self.assertEqual(
            first["reuse_receipt_sha256"], second["reuse_receipt_sha256"]
        )
        self.assertEqual(first["receipt"]["formal_write_count"], 0)
        self.assertEqual(first["receipt"]["model_call_count"], 0)

    def test_terminal_semantic_drift_requires_explicit_controlled_replay(self):
        candidate = english_candidate(
            "EN-HISTORICAL-TERMINAL",
            recorded_at="2026-08-05T01:00:00Z",
        )
        sentinel = {
            "disposition": "controlled_replay_required",
            "reason": "historical_terminal_semantic_contract_changed",
            "source_release_id": "b" * 64,
            "source_package_sha256": "c" * 64,
            "source_content_processing_id": "d" * 64,
            "semantic_evidence_capsule_sha256": "e" * 64,
        }
        with mock.patch.object(
            bridge, "_try_semantic_package_reuse", return_value=sentinel
        ):
            frozen, decisions = self._scan(
                "english", [(candidate, "eligible")]
            )
        self.assertEqual(frozen, [])
        self.assertEqual(len(decisions), 1)
        self.assertFalse(decisions[0]["eligible"])
        self.assertTrue(decisions[0]["controlled_replay_required"])
        self.assertFalse(decisions[0]["model_enqueue_allowed"])

        self.scan_worker.set_candidates([(candidate, "eligible")])
        with mock.patch.object(
            bridge, "_try_semantic_package_reuse"
        ) as reuse:
            frozen, _ = scan_eligible_candidates(
                self.config,
                "english",
                worker_factory=self.scan_worker,
                capture_allowlist=frozenset({candidate.capture_id}),
                controlled_replay=True,
            )
        reuse.assert_not_called()
        self.assertEqual(len(frozen), 1)

    def test_cs408_and_english_distinct_captures_run_independent_two_passes(self):
        base_408 = core_candidate(41)
        subject_rows = {
            "cs408": [
                (
                    duplicate_cs408(
                        base_408,
                        "CS408-DUP-A",
                        "2026-08-05T01:00:00Z",
                    ),
                    "eligible",
                ),
                (
                    duplicate_cs408(
                        base_408,
                        "CS408-DUP-B",
                        "2026-08-05T11:59:59Z",
                    ),
                    "eligible",
                ),
            ],
            "english": [
                (
                    english_candidate(
                        "EN-DUP-A",
                        recorded_at="2026-08-05T02:00:00Z",
                    ),
                    "eligible",
                ),
                (
                    english_candidate(
                        "EN-DUP-B",
                        recorded_at="2026-08-05T12:00:00Z",
                    ),
                    "eligible",
                ),
            ],
        }
        for subject, rows in subject_rows.items():
            with self.subTest(subject=subject):
                frozen, decisions = self._scan(subject, rows)
                self.assertEqual(len(frozen), 2, decisions)
                self.assertEqual(len(decisions), 2)
                self.assertEqual(
                    len({row["content_processing_id"] for row in decisions}),
                    2,
                )
                self.assertEqual(
                    sum(row["model_enqueue_allowed"] for row in decisions),
                    2,
                )
                counts = {"analysis": 0, "critical_review": 0}
                dispatcher = ConcurrentDispatcher(
                    self.runtime,
                    lambda _task, _context: CountingRunner(counts),
                    stage_timeout_seconds=2,
                )
                results = [
                    dispatcher.submit(unit.task).wait(5) for unit in frozen
                ]
                self.assertTrue(
                    all(result.outcome == "succeeded" for result in results)
                )
                self.assertEqual(
                    counts, {"analysis": 2, "critical_review": 2}
                )
                verified = [
                    LeaseStore(self.runtime).verify_authoritative_completion(
                        subject,
                        candidate.capture_id,
                        expected_release_id=RELEASE_ID,
                    )
                    for candidate, _reason in rows
                ]
                self.assertEqual(
                    len(
                        {
                            row["member_publication"]["capture_id"]
                            for row in verified
                        }
                    ),
                    2,
                )
                self.assertEqual(
                    len(
                        {
                            row["completion"]["unit_sha256"]
                            for row in verified
                        }
                    ),
                    2,
                )

    def test_different_model_input_is_never_merged(self):
        rows = [
            (
                english_candidate(
                    "EN-DIFF-A",
                    recorded_at="2026-08-05T01:00:00Z",
                    content="alpha",
                ),
                "eligible",
            ),
            (
                english_candidate(
                    "EN-DIFF-B",
                    recorded_at="2026-08-05T02:00:00Z",
                    content="beta",
                ),
                "eligible",
            ),
        ]
        frozen, decisions = self._scan("english", rows)
        self.assertEqual(len(frozen), 2, decisions)
        self.assertEqual(len({row.task.unit_sha256 for row in frozen}), 2)

    def test_production_distinct_captures_do_not_reuse_an_owner_result(self):
        base_408 = core_candidate(57)
        subject_rows = {
            "cs408": [
                (
                    duplicate_cs408(
                        base_408,
                        "CS408-CHILD-A",
                        "2026-08-05T01:00:00Z",
                    ),
                    "eligible",
                ),
                (
                    duplicate_cs408(
                        base_408,
                        "CS408-CHILD-B",
                        "2026-08-05T02:00:00Z",
                    ),
                    "eligible",
                ),
            ],
            "english": [
                (
                    english_candidate(
                        "EN-CHILD-A",
                        recorded_at="2026-08-05T01:00:00Z",
                    ),
                    "eligible",
                ),
                (
                    english_candidate(
                        "EN-CHILD-B",
                        recorded_at="2026-08-05T02:00:00Z",
                    ),
                    "eligible",
                ),
            ],
        }
        for subject, rows in subject_rows.items():
            with self.subTest(subject=subject):
                frozen, _decisions = self._scan(subject, rows)
                store = LeaseStore(self.runtime)
                calls = {"model": 0, "published": []}

                class Runner:
                    def _execute_prompt(self, **_kwargs):
                        raise AssertionError("fake Worker calls run directly")

                    def _write_analysis_checkpoint(self, *_args, **_kwargs):
                        return {}

                    def run(self, candidate):
                        calls["model"] += 1
                        generation = f"{candidate.subject}-fixture-generation"

                        def stage_fixture(stage_name):
                            evidence_ref = (
                                f"mcp-item:{candidate.subject}:"
                                f"{candidate.capture_id}:{stage_name}"
                            )
                            grounding_core = {
                                "schema_version": (
                                    "model_mcp_grounding_manifest_v1"
                                ),
                                "items": [
                                    {
                                        "evidence_ref": evidence_ref,
                                        "subject": candidate.subject,
                                        "generation": generation,
                                    }
                                ],
                                "item_count": 1,
                                "host_semantic_prefetch": False,
                                "formal_write_count": 0,
                            }
                            grounding_sha256 = preprocessor.sha256_value(
                                grounding_core
                            )
                            receipt = {
                                "status": "ready",
                                "requested_model": REQUIRED_MODEL,
                                "requested_reasoning_effort": (
                                    REQUIRED_REASONING_EFFORT
                                ),
                                "runtime_model": None,
                                "runtime_reasoning_effort": None,
                                "runtime_metadata_provenance": "unavailable",
                                "runtime_identity_status": (
                                    "requested_unverified"
                                ),
                                "duration_ms": 1,
                                "processing_binding": {
                                    "candidate_release_id": RELEASE_ID
                                },
                                "read_session_id": (
                                    f"READ-{candidate.capture_id}-{stage_name}"
                                ),
                                "read_session_manifest_sha256": "1" * 64,
                                "authority_snapshot_manifest_sha256": "2" * 64,
                                "capture_freeze_receipt_sha256": "3" * 64,
                                "mcp_read_session_receipt_sha256": "4" * 64,
                                "mcp_transcript_sha256": (
                                    "5" * 64
                                    if stage_name == "analysis"
                                    else "6" * 64
                                ),
                                "evidence_generation": generation,
                                "evidence_authority_fingerprint": "7" * 64,
                                "mcp_grounding_manifest": {
                                    **grounding_core,
                                    "manifest_sha256": grounding_sha256,
                                },
                                "mcp_grounding_manifest_sha256": (
                                    grounding_sha256
                                ),
                                "semantic_stage_count": 1,
                                "provider_request_count": 1,
                                "mcp_tool_call_count": 1,
                                "consumed_terminal_duplicate_read_count": 0,
                            }
                            payload = {
                                "final": "shared",
                                "evidence_refs": [evidence_ref],
                            }
                            return payload, receipt

                        analysis_payload, analysis_receipt = stage_fixture(
                            "analysis"
                        )
                        review_payload, review_receipt = stage_fixture(
                            "critical_review"
                        )
                        return ModelResult(
                            analysis=analysis_payload,
                            duration_ms=2,
                            runtime_model=None,
                            runtime_reasoning_effort=None,
                            runtime_metadata_provenance="unavailable",
                            pipeline_status="two_pass_ready",
                            draft_analysis=analysis_payload,
                            critical_review=review_payload,
                            stage_receipts={
                                "analysis": analysis_receipt,
                                "critical_review": review_receipt,
                            },
                        )

                class Worker:
                    release_id = RELEASE_ID

                    def __init__(self, _config):
                        self.runner = Runner()

                    def process_claimed_candidate(
                        self, candidate, _reason, *, write_dashboard
                    ):
                        if write_dashboard:
                            raise AssertionError("must stay private")
                        self.runner.run(candidate)
                        calls["published"].append(candidate.capture_id)
                        return {
                            "status": (
                                "two_pass_ready"
                                if candidate.subject == "cs408"
                                else "ready"
                            )
                        }

                results = []
                for index, unit in enumerate(frozen):
                    task = unit.task
                    claim = store.claim(
                        task.unit_sha256,
                        f"independent-{subject}-{index}",
                        subject=subject,
                    )
                    self.assertIsNotNone(claim.lease)
                    lease = claim.lease
                    context_root = (
                        self.runtime
                        / "dispatch"
                        / "contexts"
                        / task.unit_sha256
                        / f"fence-{lease.fence}"
                    )
                    context_root.mkdir(parents=True, exist_ok=True)
                    request = {
                        "schema_version": (
                            "study-intake-production-task-request-v1"
                        ),
                        "task": task.as_dict(),
                        "reason": unit.reason,
                        "unit_sha256": task.unit_sha256,
                        "lease_fence": lease.fence,
                        "lease_owner_id": lease.owner_id,
                        "execution_mode": "full_two_pass",
                    }
                    environment = {
                        "STUDY_PREPROCESS_RUNTIME_ROOT": str(
                            self.runtime.resolve()
                        ),
                        "STUDY_PREPROCESS_UNIT_SHA256": task.unit_sha256,
                        "STUDY_PREPROCESS_LEASE_FENCE": str(lease.fence),
                        "STUDY_PREPROCESS_LEASE_OWNER_ID": lease.owner_id,
                        "STUDY_PREPROCESS_CONTEXT_ROOT": str(context_root),
                    }
                    with (
                        mock.patch(
                            "preprocess_task_runner.load_config",
                            return_value=self.config,
                        ),
                        mock.patch("preprocess_task_runner.Worker", Worker),
                        mock.patch.dict(os.environ, environment, clear=False),
                    ):
                        results.append(
                            run_request(self.runtime / "config.json", request)
                        )
                self.assertEqual(calls["model"], 2)
                self.assertEqual(
                    calls["published"],
                    [candidate.capture_id for candidate, _reason in rows],
                )
                self.assertEqual(
                    sum(len(result["member_publications"]) for result in results),
                    2,
                )
                self.assertTrue(
                    all(result["formal_write_count"] == 0 for result in results)
                )

    def test_late_distinct_capture_gets_an_independent_unit_and_detail(self):
        first = english_candidate(
            "EN-CROSS-SCAN-A", recorded_at="2026-08-05T01:00:00Z"
        )
        second = english_candidate(
            "EN-CROSS-SCAN-B", recorded_at="2026-08-05T22:00:00Z"
        )
        first_frozen, first_decisions = self._scan(
            "english", [(first, "eligible")]
        )
        second_frozen, second_decisions = self._scan(
            "english", [(second, "eligible")]
        )
        self.assertNotEqual(
            first_frozen[0].task.unit_sha256,
            second_frozen[0].task.unit_sha256,
        )
        counts = {"analysis": 0, "critical_review": 0}
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: CountingRunner(counts),
            stage_timeout_seconds=2,
        )
        first_result = dispatcher.submit(first_frozen[0].task).wait(5)
        owner_reference_before = LeaseStore(
            self.runtime
        ).verify_authoritative_completion(
            "english", first.capture_id, expected_release_id=RELEASE_ID
        )["latest"]["member_publication_sha256"]
        second_result = dispatcher.submit(second_frozen[0].task).wait(5)
        self.assertEqual(first_result.outcome, "succeeded")
        self.assertEqual(second_result.outcome, "succeeded")
        self.assertEqual(counts, {"analysis": 2, "critical_review": 2})

        store = LeaseStore(self.runtime)
        verified_owner = store.verify_authoritative_completion(
            "english", first.capture_id, expected_release_id=RELEASE_ID
        )
        verified_member = store.verify_authoritative_completion(
            "english", second.capture_id, expected_release_id=RELEASE_ID
        )
        self.assertNotEqual(
            verified_owner["latest"]["member_publication_sha256"],
            verified_member["latest"]["member_publication_sha256"],
        )
        self.assertEqual(
            verified_owner["latest"]["member_publication_sha256"],
            owner_reference_before,
        )
        self.assertNotEqual(
            verified_owner["completion"]["unit_sha256"],
            verified_member["completion"]["unit_sha256"],
        )

        first_unit = first_frozen[0].task.unit_sha256
        second_unit = second_frozen[0].task.unit_sha256
        first_detail_path = (
            self.runtime / "dispatch" / "state" / "task-details" / f"{first_unit}.json"
        )
        second_detail_path = (
            self.runtime / "dispatch" / "state" / "task-details" / f"{second_unit}.json"
        )
        projection_path = self.runtime / "dashboard" / "projection.json"
        projection_path.parent.mkdir(parents=True, exist_ok=True)
        owner_item = {
            **first_decisions[0],
            "task_detail_path": str(first_detail_path),
        }
        member_item = {
            **second_decisions[0],
            "task_detail_path": str(second_detail_path),
        }
        for item, detail_path in (
            (owner_item, first_detail_path),
            (member_item, second_detail_path),
        ):
            detail = json.loads(detail_path.read_text(encoding="utf-8"))
            item.update(
                {
                    "generation": detail["attempt"],
                    "attempt": detail["attempt"],
                    "fence": detail["fence"],
                }
            )
        owner_detail = _load_dispatch_task_detail(
            projection_path, owner_item, owner_item, include_raw=False
        )
        member_detail = _load_dispatch_task_detail(
            projection_path, member_item, member_item, include_raw=False
        )
        self.assertEqual(owner_detail["capture_id"], first.capture_id)
        self.assertEqual(member_detail["capture_id"], second.capture_id)
        self.assertFalse(
            owner_detail["task_identity"]["shared_execution"]
        )
        self.assertFalse(
            member_detail["task_identity"]["shared_execution"]
        )
        self.assertNotEqual(
            owner_detail["task_identity"]["content_processing_id"],
            member_detail["task_identity"]["content_processing_id"],
        )
        self.assertNotIn("raw_sections", json.dumps(member_detail))

    def test_bad_candidate_between_healthy_candidates_is_a_safe_decision(self):
        rows = [
            (
                english_candidate(
                    "EN-GOOD-1",
                    recorded_at="2026-08-05T01:00:00Z",
                    content="healthy-one",
                ),
                "eligible",
            ),
            (
                english_candidate(
                    "EN-BAD",
                    recorded_at="2026-08-05T02:00:00Z",
                    content=object(),
                ),
                "eligible",
            ),
            (
                english_candidate(
                    "EN-GOOD-2",
                    recorded_at="2026-08-05T03:00:00Z",
                    content="healthy-two",
                ),
                "eligible",
            ),
        ]
        frozen, decisions = self._scan("english", rows)
        self.assertEqual(len(frozen), 2)
        by_capture = {row["capture_id"]: row for row in decisions}
        self.assertTrue(by_capture["EN-GOOD-1"]["eligible"])
        self.assertTrue(by_capture["EN-GOOD-2"]["eligible"])
        self.assertFalse(by_capture["EN-BAD"]["eligible"])
        self.assertEqual(
            by_capture["EN-BAD"]["reason"],
            "non_json_dispatch_value",
        )
        self.assertFalse(by_capture["EN-BAD"]["model_enqueue_allowed"])

    def test_submit_exception_between_healthy_tasks_does_not_abort_batch(self):
        config_path = self.runtime / "config.json"
        config_path.write_text("{}\n", encoding="utf-8")
        runtime = ProductionDispatchRuntime(
            {
                "runtime_root": str(self.runtime),
                "worker": {"model_timeout_seconds": 1},
                "english_two_pass_v1": {
                    "soft_runtime_warning_seconds": 60,
                    "stall_timeout_seconds": 60,
                    "stall_probe_interval_seconds": 1,
                    "stall_probe_required_consecutive_failures": 2,
                },
            },
            "english",
            config_path,
        )
        units, decisions = self._scan(
            "english",
            [
                (
                    english_candidate(
                        f"SUBMIT-{index}",
                        recorded_at=f"2026-08-05T0{index}:00:00Z",
                        content=f"submit-{index}",
                    ),
                    "eligible",
                )
                for index in range(3)
            ],
        )
        tasks = [unit.task for unit in units]
        counts = {"analysis": 0, "critical_review": 0}
        runtime.dispatcher.runner_factory = (
            lambda _task, _context: CountingRunner(counts)
        )
        original_submit = runtime.dispatcher.submit

        def injected_submit(task):
            if task.unit_sha256 == tasks[1].unit_sha256:
                raise DispatchError("synthetic_submit_failure")
            return original_submit(task)

        with mock.patch(
            "preprocess_dispatcher.scan_eligible_candidates",
            return_value=(units, decisions),
        ), mock.patch.object(
            runtime.dispatcher, "submit", side_effect=injected_submit
        ):
            handles, projected = runtime.scan_and_submit()
            results = [handle.wait(5) for handle in handles]
        self.assertEqual(len(handles), 2)
        self.assertEqual(
            [row.outcome for row in results],
            ["succeeded", "succeeded"],
            results,
        )
        by_capture = {row["capture_id"]: row for row in projected}
        self.assertEqual(
            by_capture["SUBMIT-1"]["error_code"],
            "synthetic_submit_failure",
        )
        self.assertFalse(by_capture["SUBMIT-1"]["eligible"])
        self.assertEqual(counts, {"analysis": 2, "critical_review": 2})


if __name__ == "__main__":
    unittest.main()
