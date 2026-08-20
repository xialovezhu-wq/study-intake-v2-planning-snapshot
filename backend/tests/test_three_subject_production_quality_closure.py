#!/usr/bin/env python3

from __future__ import annotations

import copy
import hashlib
import json
import shutil
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import preprocessor_core as core  # noqa: E402
import subject_sol_contract as sol_contract  # noqa: E402
from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    FrozenTask,
    StageResult,
)
from processing_plugin import (  # noqa: E402
    ProcessingPluginError,
    ProcessingPluginHost,
)
from subject_sol_contract import (  # noqa: E402
    SubjectSolRuntimeStore,
)
from tests import test_math_formalization_contract as math_formalization_support  # noqa: E402
from tests import test_math_v2_core as math_v2_support  # noqa: E402
from tests import test_english_adapter as english_support  # noqa: E402
from tests import test_preprocessor as cs408_support  # noqa: E402
from tests import test_processing_plugin as processing_plugin_support  # noqa: E402
from tests import test_processing_plugin_sealed_e2e as sealed_mcp_support  # noqa: E402
from tests import test_three_subject_sealed_chain_replay as sealed_support  # noqa: E402


class ThreeSubjectProductionQualityClosureTests(unittest.TestCase):
    """Close production quality from isolated, zero-model composed authority.

    This suite deliberately composes production implementations instead of
    issuing a test-only quality receipt: ProcessingPluginHost publishes and
    reopens both MCP stages, Worker publishes the subject package/job/latest,
    ConcurrentDispatcher publishes completion/latest authority, and
    SubjectSolRuntimeStore alone signs and reopens the quality receipt.
    """

    maxDiff = None

    @staticmethod
    def _remap_evidence_refs(value: object, evidence_ref: str) -> None:
        if isinstance(value, dict):
            for key, nested in list(value.items()):
                if key == "evidence_refs" and isinstance(nested, list) and nested:
                    value[key] = [evidence_ref]
                else:
                    ThreeSubjectProductionQualityClosureTests._remap_evidence_refs(
                        nested, evidence_ref
                    )
        elif isinstance(value, list):
            for nested in value:
                ThreeSubjectProductionQualityClosureTests._remap_evidence_refs(
                    nested, evidence_ref
                )

    @staticmethod
    def _stamp_stage_receipts(
        receipts: dict,
        *,
        schemas: dict[str, str],
        payloads: dict[str, dict],
        ordered_image_sha256s: list[str] | None = None,
    ) -> None:
        for stage_name in ("analysis", "critical_review"):
            payload = payloads[stage_name]
            receipts[stage_name].update(
                {
                    "prompt_sha256": "a" * 64,
                    "schema_sha256": schemas[stage_name],
                    "result_sha256": core.sha256_value(payload),
                    "output_sha256": core.sha256_value(payload),
                    "duration_ms": 1,
                    "requested_model": "gpt-5.6-luna",
                    "requested_reasoning_effort": "max",
                    "requested_service_tier": None,
                    "runtime_model": None,
                    "runtime_reasoning_effort": None,
                    "runtime_metadata_provenance": "unavailable",
                    "runtime_identity_status": "requested_unverified",
                    "ordered_image_sha256s": list(
                        ordered_image_sha256s or []
                    ),
                }
            )

    def _host_fixture(
        self,
        *,
        runtime: Path,
        subject: str,
        capture_id: str,
        study_date: str,
        input_fingerprint: str,
        input_binding: dict,
        prepared: tuple[
            processing_plugin_support.ProcessingPluginHostTests,
            ProcessingPluginHost,
            dict,
        ] | None = None,
    ) -> tuple[
        processing_plugin_support.ProcessingPluginHostTests,
        ProcessingPluginHost,
        dict,
        dict,
    ]:
        if prepared is None:
            fixture, host, host_config = self._new_processing_host(runtime)
        else:
            fixture, host, host_config = prepared

        context = host.open_read_session(
            subject=subject,
            capture_id=capture_id,
            study_date=study_date,
            input_fingerprint=input_fingerprint,
            input_binding=input_binding,
            **fixture._capture_binding_args(subject),
            provider_schema_sha256="2" * 64,
            canonical_schema_sha256="3" * 64,
            validator_sha256="4" * 64,
        )
        return fixture, host, host_config, context

    def _new_processing_host(
        self, runtime: Path
    ) -> tuple[
        processing_plugin_support.ProcessingPluginHostTests,
        ProcessingPluginHost,
        dict,
    ]:
        fixture = processing_plugin_support.ProcessingPluginHostTests(
            methodName="runTest"
        )
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        key_path = runtime / "dispatch/state/authority.key"
        key_path.parent.mkdir(parents=True, exist_ok=True)
        if not key_path.exists():
            key_path.write_bytes(b"q" * 32)
            key_path.chmod(0o600)
        # The source component lock is deliberately immutable.  Build an
        # ephemeral private overlay so this integration test can bind the
        # staged 3.1.1 English Skill without changing the shared lock.
        plugin_overlay = runtime / "private-plugin-overlay/kaoyan-study-intake"
        shutil.copytree(
            ROOT / "plugin/kaoyan-study-intake",
            plugin_overlay,
        )
        overlay_lock_path = plugin_overlay / "component-lock.json"
        overlay_lock = json.loads(overlay_lock_path.read_text(encoding="utf-8"))
        overlay_components_path = plugin_overlay / "components.json"
        overlay_components = json.loads(
            overlay_components_path.read_text(encoding="utf-8")
        )
        overlay_lock["registry_sha256"] = hashlib.sha256(
            overlay_components_path.read_bytes()
        ).hexdigest()
        overlay_lock["external_runtime_sources"] = overlay_components[
            "external_runtime_sources"
        ]
        # Runtime work can legitimately change any of the three background
        # Skills before the immutable successor lock is regenerated.  This
        # private test-only overlay must bind the bytes it actually exercises,
        # without mutating or weakening the shared component lock.
        for skill_id in (
            "background-math-processing",
            "background-cs408-processing",
            "background-english-processing",
        ):
            skill_path = plugin_overlay / "skills" / skill_id / "SKILL.md"
            prior = overlay_lock["skills"][skill_id]
            overlay_lock["skills"][skill_id] = {
                "sha256": hashlib.sha256(skill_path.read_bytes()).hexdigest(),
                "version": prior["version"],
            }
        overlay_lock_path.chmod(0o600)
        overlay_lock_path.write_text(
            json.dumps(
                overlay_lock,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        overlay_lock_path.chmod(0o400)
        host_config = copy.deepcopy(fixture.config)
        host_config["authority_key_path"] = str(key_path)
        host_config["root"] = str(plugin_overlay)
        host_config["component_lock_path"] = str(overlay_lock_path)
        sys.path.insert(0, str(sealed_mcp_support.MCP_SOURCE / "src"))
        try:
            mcp_helpers = sealed_mcp_support._load_module(
                "quality_closure_mcp_helpers",
                sealed_mcp_support.MCP_SOURCE / "tests/helpers.py",
            )
            repositories = mcp_helpers.make_fixture(
                runtime / "private-mcp-source-fixtures"
            )
        finally:
            sys.path.remove(str(sealed_mcp_support.MCP_SOURCE / "src"))
        host = ProcessingPluginHost(
            host_config,
            runtime_root=runtime,
            candidate_release_id=core.LOADED_CORE_SHA256,
            subject_roots={
                "math": repositories.math_root,
                "cs408": repositories.cs408_root,
                "english": repositories.english_root,
            },
            require_authority_snapshot=True,
        )
        host_config["_canonical_subject_repo_roots"] = {
            subject: str(root)
            for subject, root in host.subject_roots.items()
        }
        fixture.runtime = runtime
        fixture.key_path = key_path
        fixture.config = host_config
        fixture.host = host
        return fixture, host, host_config

    def _production_read_publication(
        self,
        *,
        fixture: processing_plugin_support.ProcessingPluginHostTests,
        host: ProcessingPluginHost,
        context: dict,
        subject: str,
    ) -> tuple[dict, dict]:
        session = context["mcp_read_session"]
        server = {
            "math": "kaoyan_math_read",
            "cs408": "kaoyan_cs408_read",
            "english": "kaoyan_english_read",
        }[subject]
        receipts: dict[str, dict] = {}
        transcript_rows: dict[str, dict] = {}
        for index, (stage_key, prompt_version) in enumerate(
            (
                ("analysis", f"{subject}_analysis_v3"),
                ("critical_review", f"{subject}_critical_review_v3"),
            ),
            start=1,
        ):
            call_specs = [
                (
                    "get_task_context",
                    {},
                    "task_context",
                    "immutable_capture_identity",
                ),
            ]
            call_specs.extend(
                (
                    "read_task_artifact",
                    {
                        "artifact_id": artifact_id,
                        "max_bytes": 32768,
                    },
                    "task_artifact",
                    "immutable_task_artifact",
                )
                for artifact_id in session["artifact_ids"]
            )
            call_specs.append(
                (
                    "list_records",
                    {
                        "collection": (
                            "article_catalog"
                            if subject == "english"
                            else "curation_inventory"
                        ),
                        "page_size": 1,
                    },
                    (
                        "article_catalog"
                        if subject == "english"
                        else "curation_inventory"
                    ),
                    (
                        "formal_article_catalog"
                        if subject == "english"
                        else "formal_library_record"
                    ),
                ),
            )
            calls = []
            evidence_refs = []
            for call_index, (
                tool,
                arguments,
                collection,
                item_role,
            ) in enumerate(call_specs, start=1):
                source_hash = hashlib.sha256(
                    (
                        f"{subject}:{session['capture_id']}:"
                        f"{index}:{call_index}:{collection}"
                    ).encode()
                ).hexdigest()
                stable_id = (
                    str(arguments["artifact_id"])
                    if collection == "task_artifact"
                    else (
                        f"{session['capture_id']}:task-context"
                        if collection == "task_context"
                        else (
                            f"{session['capture_id']}:library:"
                            f"{index}:{call_index}"
                        )
                    )
                )
                evidence_ref = core.model_mcp_item_ref(
                    subject=subject,
                    generation=session["generation"],
                    collection=collection,
                    stable_id=stable_id,
                    source_hash=source_hash,
                )
                evidence_refs.append(evidence_ref)
                route = fixture._model_route(session)
                result = {
                    "ok": True,
                    "schema_version": "study-read-mcp.v3",
                    "server_release": session["mcp_server_release"],
                    "adapter_release": session["mcp_server_release"],
                    "subject": subject,
                    "data_role": "model_selected_read_page",
                    "authority_source_id": "sealed-4c1-shape-replay",
                    "generation": session["generation"],
                    "preprocessor_release": context["processing_binding"][
                        "base_release_id"
                    ],
                    "authority_fingerprint": session[
                        "authority_fingerprint"
                    ],
                    "captured_at": "2026-08-09T00:00:00+00:00",
                    "consistency": "bound_snapshot",
                    "profile": "luna",
                    "read_route": route,
                    "warnings": [],
                    "items": [
                        {
                            "stable_id": stable_id,
                            "source_hash": source_hash,
                            "data_role": item_role,
                            "collection": collection,
                            "evidence_ref": evidence_ref,
                        }
                    ],
                    "formal_write_count": 0,
                    "model_call_count": 0,
                    "mcp_tool_call_count": 1,
                    "read_session": {
                        "read_session_id": session["read_session_id"],
                        "manifest_sha256": session["manifest_sha256"],
                        "capture_id": session["capture_id"],
                        "capture_manifest_sha256": session[
                            "capture_manifest_sha256"
                        ],
                        "artifact_ids": session["artifact_ids"],
                        "candidate_release_id": session[
                            "candidate_release_id"
                        ],
                        "subject": subject,
                        "generation": session["generation"],
                        "authority_fingerprint": session[
                            "authority_fingerprint"
                        ],
                        "authority_snapshot_manifest_sha256": session[
                            "authority_snapshot_manifest_sha256"
                        ],
                        "authority_snapshot_receipt_sha256": session[
                            "authority_snapshot_receipt_sha256"
                        ],
                        "skill_id": session["skill_id"],
                        "skill_version": session["skill_version"],
                        "formal_write_count": 0,
                    },
                    "total_count": 1,
                    "returned_count": 1,
                    "offset": 0,
                    "next_cursor": None,
                    "truncated": False,
                    "complete": True,
                }
                calls.append(
                    {
                        "sequence": call_index,
                        "server": server,
                        "tool": tool,
                        "arguments": arguments,
                        "arguments_sha256": fixture._json_digest(arguments),
                        "result": result,
                        "result_sha256": fixture._json_digest(result),
                    }
                )
            transcript_stage_name = (
                f"english_{stage_key}"
                if subject == "english"
                else prompt_version
            )
            transcript_sha256, transcript_ref = fixture._write_transcript(
                context=context,
                stage_name=transcript_stage_name,
                calls=calls,
            )
            signed = host.sign_model_mcp_calls(
                subject=subject,
                stage_name=prompt_version,
                context=context,
                calls=calls,
                transcript_sha256=transcript_sha256,
            )
            stage_result = core.StructuredStageResult(
                payload={},
                duration_ms=0,
                runtime_model=None,
                runtime_reasoning_effort=None,
                runtime_metadata_provenance="unavailable",
                runtime_identity_status="requested_unverified",
                output_sha256="0" * 64,
                mcp_transcript_sha256=transcript_sha256,
                mcp_calls=tuple(calls),
            )
            grounding = core.mcp_grounding_manifest((stage_result,))
            receipts[stage_key] = {
                "status": "ready",
                "prompt_version": prompt_version,
                "processing_binding": copy.deepcopy(
                    context["processing_binding"]
                ),
                "processing_binding_sha256": context[
                    "processing_binding_sha256"
                ],
                "capture_freeze_receipt_sha256": context[
                    "capture_freeze_receipt_sha256"
                ],
                "capture_freeze_receipt_ref": context[
                    "capture_freeze_receipt_ref"
                ],
                "mcp_read_session_receipt_sha256": context[
                    "mcp_read_session_receipt_sha256"
                ],
                "mcp_read_session_receipt_ref": context[
                    "mcp_read_session_receipt_ref"
                ],
                "read_session_id": session["read_session_id"],
                "read_session_manifest_sha256": session["manifest_sha256"],
                "authority_snapshot_manifest_sha256": session[
                    "authority_snapshot_manifest_sha256"
                ],
                "evidence_generation": session["generation"],
                "evidence_authority_fingerprint": session[
                    "authority_fingerprint"
                ],
                "mcp_transcript_sha256": transcript_sha256,
                "mcp_transcript_ref": transcript_ref,
                "mcp_call_receipt_sha256": signed["receipt_sha256"],
                "mcp_call_receipt_ref": signed["receipt_ref"],
                "pagination_coverage_complete": True,
                "mcp_grounding_manifest": grounding,
                "mcp_grounding_manifest_sha256": grounding[
                    "manifest_sha256"
                ],
                "host_semantic_prefetch": False,
                "consumed_terminal_duplicate_read_count": 0,
                "provider_request_count": len(calls) + 1,
                "mcp_tool_call_count": len(calls),
                "formal_write_count": 0,
            }
            transcript_rows[stage_key] = {
                "transcript_sha256": transcript_sha256,
                "transcript_ref": transcript_ref,
                "call_receipt_sha256": signed["receipt_sha256"],
                "call_receipt_ref": signed["receipt_ref"],
                "grounding_manifest_sha256": grounding["manifest_sha256"],
                "grounding_refs": [
                    row["evidence_ref"] for row in grounding["items"]
                ],
                "provider_request_count": len(calls) + 1,
                "mcp_tool_call_count": len(calls),
            }
        total_mcp_tool_calls = sum(
            int(receipt["mcp_tool_call_count"])
            for receipt in receipts.values()
        )
        total_provider_requests = sum(
            int(receipt["provider_request_count"])
            for receipt in receipts.values()
        )
        finalized = host.finalize_model_read_session(
            subject=subject,
            context=context,
            stage_receipts=receipts,
        )
        receipts["read_session"] = {
            "status": "complete",
            "receipt": copy.deepcopy(finalized["receipt"]),
            "receipt_sha256": finalized["receipt_sha256"],
            "receipt_ref": finalized["receipt_ref"],
            "model_mcp_tool_call_count": total_mcp_tool_calls,
            "provider_request_count": total_provider_requests,
            "provider_request_count_status": "derived_from_codex_tool_loop",
            "pagination_coverage_complete": True,
            "formal_write_count": 0,
        }
        publication = {
            "processing_binding": copy.deepcopy(context["processing_binding"]),
            "processing_binding_sha256": context[
                "processing_binding_sha256"
            ],
            "capture_freeze_receipt_sha256": context[
                "capture_freeze_receipt_sha256"
            ],
            "capture_freeze_receipt_ref": context[
                "capture_freeze_receipt_ref"
            ],
            "mcp_read_session_receipt_sha256": finalized["receipt_sha256"],
            "mcp_read_session_receipt_ref": finalized["receipt_ref"],
            "read_session_id": session["read_session_id"],
            "read_session_manifest_sha256": session["manifest_sha256"],
            "evidence_generation": session["generation"],
            "evidence_authority_fingerprint": session[
                "authority_fingerprint"
            ],
            "host_semantic_prefetch": False,
            "mcp_stage_transcripts": transcript_rows,
            "semantic_stage_count": 2,
            "provider_request_count": total_provider_requests,
            "mcp_tool_call_count": total_mcp_tool_calls,
            "model_call_count": 2,
            "consumed_terminal_duplicate_read_count": 0,
            "stage_receipts": copy.deepcopy(receipts),
        }
        reopened = host.reopen_published_read_session(
            subject=subject,
            publication=publication,
            stage_receipts=receipts,
        )
        self.assertEqual(
            set(reopened["stage_calls"]), {"analysis", "critical_review"}
        )
        return publication, receipts

    @staticmethod
    def _content_snapshot(roots: tuple[Path, ...]) -> dict[str, str]:
        return {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for root in roots
            if root.exists()
            for path in sorted(root.rglob("*"))
            if path.is_file()
        }

    def _close_quality_from_real_authority(
        self,
        *,
        runtime: Path,
        config: dict,
        worker: core.Worker,
        candidate: core.Candidate,
        publication: dict,
    ) -> dict:
        base_task = FrozenTask.from_candidate(candidate)
        frozen = copy.deepcopy(dict(base_task.frozen_payload))
        frozen["dispatch_contract"] = {
            "schema_version": "study-intake-dispatch-release-binding-v1",
            "release_id": worker.release_id,
        }
        task = FrozenTask(frozen)
        subject_package_path = core.canonical_subject_package_path(
            runtime,
            subject=candidate.subject,
            study_date=candidate.study_date,
            capture_id=candidate.capture_id,
            input_fingerprint=candidate.input_fingerprint,
            package_sha256=publication["subject_package_sha256"],
        )
        subject_package = core.load_json(subject_package_path)

        class QualityDispatchRunner:
            @staticmethod
            def _stage(stage_name: str) -> StageResult:
                receipt = subject_package["stage_receipts"][stage_name]
                evidence_refs = tuple(
                    row["evidence_ref"]
                    for row in receipt["mcp_grounding_manifest"]["items"]
                )
                raw_sha = hashlib.sha256(
                    f"{task.unit_sha256}:{stage_name}:raw".encode()
                ).hexdigest()
                execution_sha = hashlib.sha256(
                    f"{task.unit_sha256}:{stage_name}:execution".encode()
                ).hexdigest()
                normalization_sha = hashlib.sha256(
                    f"{task.unit_sha256}:{stage_name}:normalization".encode()
                ).hexdigest()
                return StageResult(
                    payload={
                        "stage": stage_name,
                        "unit_sha256": task.unit_sha256,
                    },
                    runtime_model="gpt-5.6-luna",
                    runtime_reasoning_effort="max",
                    runtime_metadata_provenance=(
                        "codex_json_attestation_v1"
                    ),
                    runtime_identity_status="confirmed",
                    duration_ms=1,
                    read_session_id=receipt["read_session_id"],
                    read_session_manifest_sha256=receipt[
                        "read_session_manifest_sha256"
                    ],
                    authority_snapshot_manifest_sha256=receipt[
                        "authority_snapshot_manifest_sha256"
                    ],
                    capture_freeze_receipt_sha256=publication[
                        "capture_freeze_receipt_sha256"
                    ],
                    mcp_read_session_receipt_sha256=publication[
                        "mcp_read_session_receipt_sha256"
                    ],
                    evidence_generation=publication["evidence_generation"],
                    evidence_authority_fingerprint=publication[
                        "evidence_authority_fingerprint"
                    ],
                    evidence_subject=candidate.subject,
                    evidence_release_id=worker.release_id,
                    mcp_grounding_manifest_sha256=receipt[
                        "mcp_grounding_manifest_sha256"
                    ],
                    mcp_transcript_sha256=receipt[
                        "mcp_transcript_sha256"
                    ],
                    mcp_consumed_evidence_refs=evidence_refs,
                    mcp_cited_evidence_refs=evidence_refs,
                    mcp_stage_grounded_evidence_refs=evidence_refs,
                    provider_request_count=receipt[
                        "provider_request_count"
                    ],
                    mcp_tool_call_count=receipt["mcp_tool_call_count"],
                    model_call_count=1,
                    raw_output_object_sha256=raw_sha,
                    raw_output_object_ref=(
                        "study-intake-model-stage-raw-output://sha256/"
                        + raw_sha
                    ),
                    stage_execution_receipt_sha256=execution_sha,
                    stage_execution_receipt_ref=(
                        "study-intake-model-stage-execution://sha256/"
                        + execution_sha
                    ),
                    stage_normalization_receipt_sha256=normalization_sha,
                    stage_normalization_receipt_ref=(
                        "study-intake-model-stage-normalization://sha256/"
                        + normalization_sha
                    ),
                    normalization_status="normalized",
                )

            def run_analysis(self, _task, _context):
                return self._stage("analysis")

            def run_critical_review(self, _task, _draft, _context):
                return self._stage("critical_review")

            def cancel(self, _context):
                return None

        dispatcher = ConcurrentDispatcher(
            runtime,
            lambda _task, _context: QualityDispatchRunner(),
            stage_timeout_seconds=2,
        )
        result = dispatcher.submit(task).wait(5)
        self.assertEqual(result.outcome, "succeeded", result.error_code)
        verified = dispatcher.lease_store.verify_authoritative_completion(
            candidate.subject,
            candidate.capture_id,
            expected_release_id=worker.release_id,
        )
        dispatcher_roots = (
            dispatcher.lease_store.completion_root,
            dispatcher.lease_store.latest_root,
            dispatcher.lease_store.authority_ledger_root,
            runtime / "dispatch/receipts",
            runtime / "dispatch/packages",
        )
        dispatcher_before = self._content_snapshot(dispatcher_roots)
        store = SubjectSolRuntimeStore(runtime)
        batch = store.prepare_and_freeze_subject_batch(
            subject=candidate.subject,
            batch_id=f"SEALED-PRODUCTION-{candidate.subject.upper()}-1",
            study_date=candidate.study_date,
            capture_high_watermark="sealed-4c1-overlay",
            scan_snapshot_sha256=hashlib.sha256(
                f"scan:{candidate.subject}".encode()
            ).hexdigest(),
            authority_generation=publication["evidence_generation"],
            authority_fingerprint=publication[
                "evidence_authority_fingerprint"
            ],
            tasks=[
                {
                    "capture_id": candidate.capture_id,
                    "unit_sha256": task.unit_sha256,
                    "input_fingerprint": candidate.input_fingerprint,
                    "study_date": candidate.study_date,
                    "frozen_payload_sha256": sol_contract._value_sha256(
                        task.frozen_payload
                    ),
                }
            ],
        )
        self.assertEqual(batch["formal_write_count"], 0)
        projected = store.record_verified_luna_completion(
            candidate.subject, verified
        )
        closed = projected["subject_luna_batch"]
        task_state = closed["tasks"][0]
        self.assertEqual(task_state["status"], "workflow_complete")
        self.assertTrue(closed["all_terminal"])
        self.assertTrue(closed["sol_ready"])
        self.assertEqual(closed["formal_write_count"], 0)
        quality_sha256 = projected["subject_quality_receipt_sha256"]
        self.assertIsInstance(quality_sha256, str)
        quality = store.read_verified_quality_receipt_v2(
            quality_sha256,
            subject=candidate.subject,
            task=task_state,
        )
        sol_contract.validate_subject_quality_receipt_v2(quality)
        store._verify_seal(quality, purpose="subject-quality-receipt-v2")
        self.assertEqual(quality["execution_status"], "workflow_complete")
        self.assertEqual(quality["model_call_count"], 2)
        self.assertEqual(quality["formal_write_count"], 0)
        self.assertEqual(
            quality["capture_freeze_receipt_sha256"],
            publication["capture_freeze_receipt_sha256"],
        )
        self.assertEqual(
            quality["mcp_read_session_receipt_sha256"],
            publication["mcp_read_session_receipt_sha256"],
        )
        self.assertEqual(
            self._content_snapshot(dispatcher_roots), dispatcher_before
        )
        return {
            "subject": candidate.subject,
            "package_sha256": task_state["package_sha256"],
            "quality_receipt_sha256": quality_sha256,
            "final_read_session_receipt_sha256": publication[
                "mcp_read_session_receipt_sha256"
            ],
            "formal_write_count": 0,
        }

    def _capture_prepared_subject(
        self,
        subject: str,
        *,
        cs408_gate_variant: str = "ready",
    ) -> dict:
        methods = {
            "math": self.test_math_production_quality_closes_from_real_authority,
            "cs408": (
                self.test_cs408_production_quality_closes_from_sealed_corrected_overlay
            ),
            "english": (
                self.test_english_production_quality_closes_from_sealed_shape_overlay
            ),
        }
        captured: dict = {}

        def retain_fixture(**kwargs):
            captured.update(kwargs)
            return {
                "subject": kwargs["candidate"].subject,
                "formal_write_count": 0,
            }

        previous_variant = getattr(self, "_cs408_gate_variant", None)
        self._cs408_gate_variant = cs408_gate_variant
        try:
            with mock.patch.object(
                self,
                "_close_quality_from_real_authority",
                side_effect=retain_fixture,
            ):
                methods[subject]()
        finally:
            if previous_variant is None:
                del self._cs408_gate_variant
            else:
                self._cs408_gate_variant = previous_variant
        self.assertEqual(captured["candidate"].subject, subject)
        return captured

    def test_quality_v2_stage_binding_tamper_fails_closed(self) -> None:
        fields = {
            "analysis_raw_output": ("analysis", "raw_output_sha256"),
            "critical_review_raw_output": (
                "critical_review",
                "raw_output_sha256",
            ),
            "authority_snapshot": ("authority_snapshot_sha256",),
        }
        for field, path in fields.items():
            with self.subTest(field=field):
                prepared = self._capture_prepared_subject("math")
                result = self._close_quality_from_real_authority(**prepared)
                store = SubjectSolRuntimeStore(prepared["runtime"])
                receipt_sha = result["quality_receipt_sha256"]
                receipt_path = (
                    store.subject_quality_v2_root
                    / "sha256"
                    / receipt_sha[:2]
                    / f"{receipt_sha}.json"
                )
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                target = receipt
                for key in path[:-1]:
                    target = target[key]
                target[path[-1]] = hashlib.sha256(
                    f"tampered:{field}".encode()
                ).hexdigest()
                receipt_path.chmod(0o600)
                receipt_path.write_text(
                    json.dumps(
                        receipt,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n",
                    encoding="utf-8",
                )
                batch = store.read_subject_batch("math")
                self.assertIsNotNone(batch)
                task = batch["tasks"][0]
                with self.assertRaises(sol_contract.SubjectSolContractError):
                    store.read_verified_quality_receipt_v2(
                        receipt_sha, subject="math", task=task
                    )
                self.assertEqual(task["status"], "workflow_complete")
                self.assertEqual(batch["formal_write_count"], 0)

    def test_cs408_report_root_and_diagnostic_dispositions_remain_bound(
        self,
    ) -> None:
        for gate_variant in (
            "incomplete_evidence",
            "insufficient_disposition",
        ):
            with self.subTest(gate_variant=gate_variant):
                prepared = self._capture_prepared_subject(
                    "cs408", cs408_gate_variant=gate_variant
                )
                package = core.load_json(
                    core.canonical_subject_package_path(
                        prepared["runtime"],
                        subject="cs408",
                        study_date=prepared["candidate"].study_date,
                        capture_id=prepared["candidate"].capture_id,
                        input_fingerprint=prepared["candidate"].input_fingerprint,
                        package_sha256=prepared["publication"][
                            "subject_package_sha256"
                        ],
                    )
                )
                report = core.load_json(
                    prepared["runtime"] / "private/reports/objects"
                    / f"{package['report_json_sha256']}.json"
                )
                self.assertNotIn("analysis", report)
                self.assertIn("evidence_assessment", report)
                self.assertIn("sol_verification_plan", report)
                result = self._close_quality_from_real_authority(**prepared)
                store = SubjectSolRuntimeStore(prepared["runtime"])
                batch = store.read_subject_batch("cs408")
                self.assertIsNotNone(batch)
                self.assertEqual(
                    batch["tasks"][0]["status"], "workflow_complete"
                )
                self.assertTrue(batch["all_terminal"])
                self.assertIsInstance(result["quality_receipt_sha256"], str)
                self.assertEqual(result["formal_write_count"], 0)

    def test_plugin_capture_and_final_receipt_tamper_fail_closed(self) -> None:
        for target, inner_code in (
            ("capture", "capture_freeze_receipt_invalid"),
            ("final", "mcp_read_session_final_receipt_invalid"),
        ):
            with self.subTest(target=target):
                prepared = self._capture_prepared_subject("math")
                package = core.load_json(
                    core.canonical_subject_package_path(
                        prepared["runtime"],
                        subject="math",
                        study_date=prepared["candidate"].study_date,
                        capture_id=prepared["candidate"].capture_id,
                        input_fingerprint=(
                            prepared["candidate"].input_fingerprint
                        ),
                        package_sha256=prepared["publication"][
                            "subject_package_sha256"
                        ],
                    )
                )
                host = core._processing_publication_host(
                    prepared["config"], runtime_root=prepared["runtime"]
                )
                receipt_path = (
                    host._capture_freeze_receipt_path(
                        package["capture_freeze_receipt_sha256"]
                    )
                    if target == "capture"
                    else host._read_session_receipt_path(
                        package["mcp_read_session_receipt_sha256"]
                    )
                )
                receipt_path.chmod(0o600)
                receipt_path.write_bytes(receipt_path.read_bytes() + b" ")
                with self.assertRaises(core.PreprocessorError) as caught:
                    core.reopen_verified_subject_publication(
                        prepared["config"],
                        subject="math",
                        capture_id=prepared["candidate"].capture_id,
                        study_date=prepared["candidate"].study_date,
                        input_fingerprint=(
                            prepared["candidate"].input_fingerprint
                        ),
                    )
                self.assertEqual(
                    caught.exception.code,
                    inner_code,
                )

    def test_english_canonical_locator_rejects_missing_hash_and_symlink(
        self,
    ) -> None:
        for mutation in ("missing", "hash", "symlink"):
            with self.subTest(mutation=mutation):
                prepared = self._capture_prepared_subject("english")
                candidate = prepared["candidate"]
                publication = prepared["publication"]
                package_sha = publication["subject_package_sha256"]
                package_path = core.canonical_subject_package_path(
                    prepared["runtime"],
                    subject="english",
                    study_date=candidate.study_date,
                    capture_id=candidate.capture_id,
                    input_fingerprint=candidate.input_fingerprint,
                    package_sha256=package_sha,
                )
                objects_alias = (
                    prepared["runtime"] / "packages/objects"
                    / f"{package_sha}.json"
                )
                self.assertTrue(package_path.is_file())
                self.assertFalse(package_path.is_symlink())
                self.assertFalse(objects_alias.exists())
                original = package_path.read_bytes()
                if mutation == "missing":
                    # Even a byte-identical objects copy is not an authorized
                    # fallback for the English identity-addressed package.
                    objects_alias.parent.mkdir(parents=True, exist_ok=True)
                    objects_alias.write_bytes(original)
                    package_path.rename(package_path.with_suffix(".missing"))
                elif mutation == "hash":
                    package_path.chmod(0o600)
                    package_path.write_bytes(original + b" ")
                else:
                    target = package_path.with_suffix(".real")
                    package_path.rename(target)
                    package_path.symlink_to(target)
                with self.assertRaises(core.PreprocessorError) as caught:
                    core.reopen_verified_subject_publication(
                        prepared["config"],
                        subject="english",
                        capture_id=candidate.capture_id,
                        study_date=candidate.study_date,
                        input_fingerprint=candidate.input_fingerprint,
                    )
                self.assertEqual(
                    caught.exception.code,
                    "subject_publication_reopen_invalid",
                )

    def test_mixed_subject_publications_and_english_batch_reopen(self) -> None:
        reopened_subjects = []
        for subject in ("math", "cs408", "english"):
            with self.subTest(subject=subject):
                prepared = self._capture_prepared_subject(subject)
                candidate = prepared["candidate"]
                reopened = core.reopen_verified_subject_publication(
                    prepared["config"],
                    subject=subject,
                    capture_id=candidate.capture_id,
                    study_date=candidate.study_date,
                    input_fingerprint=candidate.input_fingerprint,
                )
                self.assertEqual(reopened, prepared["publication"])
                self.assertEqual(reopened["formal_write_count"], 0)
                package_path = core.canonical_subject_package_path(
                    prepared["runtime"],
                    subject=subject,
                    study_date=candidate.study_date,
                    capture_id=candidate.capture_id,
                    input_fingerprint=candidate.input_fingerprint,
                    package_sha256=reopened["subject_package_sha256"],
                )
                package = core.load_json(package_path)
                processing_host = core._processing_publication_host(
                    prepared["config"], runtime_root=prepared["runtime"]
                )
                plugin_reopened = processing_host.reopen_published_read_session(
                    subject=subject,
                    publication=package,
                    stage_receipts=package["stage_receipts"],
                )
                self.assertEqual(
                    set(plugin_reopened["stage_calls"]),
                    {"analysis", "critical_review"},
                )
                reopened_subjects.append(subject)
                if subject == "english":
                    self.assertFalse(
                        (
                            prepared["runtime"] / "packages/objects"
                            / f"{reopened['subject_package_sha256']}.json"
                        ).exists()
                    )
                    closed = self._close_quality_from_real_authority(**prepared)
                    store = SubjectSolRuntimeStore(prepared["runtime"])
                    batch = store.read_subject_batch("english")
                    self.assertIsNotNone(batch)
                    self.assertEqual(
                        batch["tasks"][0]["status"], "workflow_complete"
                    )
                    quality = store.read_verified_quality_receipt_v2(
                        closed["quality_receipt_sha256"],
                        subject="english",
                        task=batch["tasks"][0],
                    )
                    self.assertEqual(
                        quality["execution_status"], "workflow_complete"
                    )
                    self.assertEqual(batch["formal_write_count"], 0)
        self.assertEqual(reopened_subjects, ["math", "cs408", "english"])

    def test_math_production_quality_closes_from_real_authority(
        self,
    ) -> None:
        math_fixture = math_v2_support.MathV2CoreTests(methodName="runTest")
        math_fixture.setUp()
        self.addCleanup(math_fixture.tearDown)
        runtime = math_fixture.runtime
        config = copy.deepcopy(math_fixture.config)
        config["runtime_root"] = str(runtime)
        config["worker"]["log_path"] = str(runtime / "logs/worker.log")
        config["worker"]["lock_path"] = str(runtime / "state/worker.lock")
        config["math_deep_v2"]["mode"] = "canary"
        fixture, host, host_config = self._new_processing_host(runtime)
        config["processing_plugin"] = host_config
        contract = core.math_processing_contract(config)
        self.assertIsInstance(contract, dict)
        candidate = core.Candidate(
            subject="math",
            capture_id="SEALED-4C1-MATH-PRODUCTION",
            study_date="2026-08-09",
            recorded_at="2026-08-09T00:00:00Z",
            input_fingerprint="1" * 64,
            input_binding={
                "processing_contract_sha256": contract[
                    "processing_contract_sha256"
                ],
                "loaded_core_sha256": core.LOADED_CORE_SHA256,
                "evidence_manifest_sha256": "e" * 64,
                "evidence_bundle_sha256": "f" * 64,
                "image_evidence_refs": [],
                "source_route": "new_intake",
                "host_semantic_prefetch": False,
                "direct_mcp_namespace": "kaoyan_math_read",
            },
            model_input={"source_bundle": {"source_kind": "new_intake"}},
            allowed_evidence_refs=(),
            image_paths=(),
            target_label="sealed-4c1-math-production-overlay",
            canonical_state="awaiting_background_analysis",
            sol_state="not_authorized",
        )
        fixture, host, host_config, context = self._host_fixture(
            runtime=runtime,
            subject="math",
            capture_id=candidate.capture_id,
            study_date=candidate.study_date,
            input_fingerprint=candidate.input_fingerprint,
            input_binding=candidate.input_binding,
            prepared=(fixture, host, host_config),
        )
        publication, receipts = self._production_read_publication(
            fixture=fixture,
            host=host,
            context=context,
            subject="math",
        )
        evidence_ref = publication["mcp_stage_transcripts"]["analysis"][
            "grounding_refs"
        ][0]
        sealed = math_formalization_support.MathFormalizationContractTests(
            methodName="runTest"
        )
        sealed.setUpClass()
        draft = sealed.corrected_capture_overlay()
        self._remap_evidence_refs(draft, evidence_ref)
        draft["evidence_assessment"]["completeness"] = "complete"
        draft["evidence_assessment"]["gaps"] = []
        draft["unresolved"] = []
        draft["sol_verification_plan"][
            "recommended_disposition"
        ] = "new_candidate"
        # The sealed failure output had no reconstruction.  Reuse its own
        # capture-backed mechanism claim instead of inventing a new fact.
        draft["correct_reasoning_reconstruction"] = [
            copy.deepcopy(draft["question_structure"]["objects"][0])
        ]
        # Build every downstream binding from the same canonical object that
        # the production Worker validates.  The sealed output predates the
        # direct-MCP image-provenance canonicalizer; using its raw hash here
        # would create a test-only relationship-context mismatch.
        runtime_candidate = core.candidate_with_grounding_refs(
            candidate,
            core.processing_grounding_refs(receipts, subject="math"),
        )
        draft = core.validate_math_semantic_gates(draft, runtime_candidate)
        review = {
            "schema_version": "study-intake-luna-math-critical-review-v2",
            "verdict": "pass",
            "summary": "isolated production-quality corrected overlay",
            "revised_analysis": copy.deepcopy(draft),
            "relationship_decisions": [],
            "unsupported_claims": [],
            "evidence_misreads": [],
            "mathematical_errors": [],
            "visual_findings": [],
            "provenance_findings": [],
            "missing_analysis": [],
            "required_corrections": [],
            "sol_priority_checks": [],
            "correction_resolutions": [],
        }
        relationship_context = core.build_math_mcp_relationship_context(
            (), draft_analysis=draft
        )
        schemas = core._expected_math_dynamic_schema_sha256s(
            config["math_deep_v2"],
            allowed_evidence_refs=(),
            image_evidence_refs=(),
            draft_analysis=draft,
            relationship_context=relationship_context,
            allow_empty_predeclared_evidence_refs=True,
            math_source_bundle_formalization_mode=True,
        )
        for stage_name, payload in (
            ("analysis", draft),
            ("critical_review", review),
        ):
            receipts[stage_name].update(
                {
                    "prompt_sha256": "a" * 64,
                    "schema_sha256": schemas[stage_name],
                    "result_sha256": core.sha256_value(payload),
                    "output_sha256": core.sha256_value(payload),
                    "duration_ms": 1,
                    "requested_model": "gpt-5.6-luna",
                    "requested_reasoning_effort": "max",
                    "requested_service_tier": None,
                    "runtime_model": None,
                    "runtime_reasoning_effort": None,
                    "runtime_metadata_provenance": "unavailable",
                    "runtime_identity_status": "requested_unverified",
                    "ordered_image_sha256s": [],
                }
            )
        result = core.ModelResult(
            analysis=copy.deepcopy(draft),
            duration_ms=2,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            pipeline_status="two_pass_ready",
            draft_analysis=copy.deepcopy(draft),
            critical_review=copy.deepcopy(review),
            stage_receipts=copy.deepcopy(receipts),
            relationship_context=relationship_context,
        )

        class Runner:
            def run(self, _candidate):
                return result

        worker = core.Worker(config, model_runner=Runner())
        job = worker.process_claimed_candidate(
            candidate, "controlled_replay", write_dashboard=False
        )
        self.assertEqual(job["status"], "two_pass_ready", job)
        reopened = core.reopen_verified_subject_publication(
            config,
            subject="math",
            capture_id=candidate.capture_id,
            study_date=candidate.study_date,
            input_fingerprint=candidate.input_fingerprint,
        )
        self.assertEqual(reopened["critical_review_outcome"], "accepted")
        closed = self._close_quality_from_real_authority(
            runtime=runtime,
            config=config,
            worker=worker,
            candidate=candidate,
            publication=reopened,
        )
        self.assertEqual(closed["subject"], "math")
        self.assertEqual(closed["formal_write_count"], 0)

    def test_cs408_production_quality_closes_from_sealed_corrected_overlay(
        self,
    ) -> None:
        cs_fixture = cs408_support.PreprocessorTests(methodName="runTest")
        cs_fixture.setUp()
        self.addCleanup(cs_fixture.tearDown)
        runtime = cs_fixture.runtime
        config = cs_fixture.make_v2_config()
        fixture, host, host_config = self._new_processing_host(runtime)
        config["processing_plugin"] = host_config

        probe = core.Worker(config)
        status = probe.adapters["cs408"].status("2026-08-04")
        candidate = probe.adapters["cs408"].candidates(status)[0]
        fixture, host, host_config, context = self._host_fixture(
            runtime=runtime,
            subject="cs408",
            capture_id=candidate.capture_id,
            study_date=candidate.study_date,
            input_fingerprint=candidate.input_fingerprint,
            input_binding=candidate.input_binding,
            prepared=(fixture, host, host_config),
        )
        publication, receipts = self._production_read_publication(
            fixture=fixture,
            host=host,
            context=context,
            subject="cs408",
        )
        evidence_ref = publication["mcp_stage_transcripts"]["analysis"][
            "grounding_refs"
        ][0]
        draft = sealed_support._payload(
            sealed_support.SEALED["cs408"]["analysis_output"]
        )
        self._remap_evidence_refs(draft, evidence_ref)
        # The sealed run intentionally stopped as insufficient evidence.  The
        # production-quality fixture overlays only its terminal gate fields;
        # all substantive claims remain byte-for-byte descendants of 4c1.
        gate_variant = getattr(self, "_cs408_gate_variant", "ready")
        if gate_variant != "incomplete_evidence":
            draft["evidence_assessment"]["completeness"] = "complete"
            draft["evidence_assessment"]["gaps"] = []
            draft["unresolved"] = []
        draft["sol_verification_plan"]["recommended_disposition"] = (
            "insufficient_evidence"
            if gate_variant == "insufficient_disposition"
            else "new_candidate"
        )
        runtime_candidate = core.candidate_with_grounding_refs(
            candidate,
            core.processing_grounding_refs(receipts, subject="cs408"),
        )
        draft = core.validate_analysis_v2(
            draft,
            runtime_candidate.allowed_evidence_refs,
            require_network_context=True,
        )
        review = sealed_support._payload(
            sealed_support.SEALED["cs408"]["critical_output"]
        )
        review = sealed_support.ThreeSubjectSealedChainReplayTests._fix_cs408_review(
            review
        )
        self._remap_evidence_refs(review, evidence_ref)
        revised = review["revised_analysis"]
        if gate_variant != "incomplete_evidence":
            revised["evidence_assessment"]["completeness"] = "complete"
            revised["evidence_assessment"]["gaps"] = []
            revised["unresolved"] = []
        revised["sol_verification_plan"]["recommended_disposition"] = (
            "insufficient_evidence"
            if gate_variant == "insufficient_disposition"
            else "new_candidate"
        )
        # The sealed critic grouped changed and unchanged paths into one
        # correction row.  The successor contract requires every declared
        # path in an applied correction to carry a material delta, so the
        # production-shape overlay keeps only the paths it actually changes
        # and updates the owning finding atomically.
        for resolution in review["correction_resolutions"]:
            if resolution.get("resolution") != "applied":
                continue
            material_paths = [
                path
                for path in resolution["affected_json_paths"]
                if core._resolve_correction_json_path(draft, path)
                != core._resolve_correction_json_path(revised, path)
            ]
            self.assertTrue(material_paths)
            resolution["affected_json_paths"] = material_paths
            for section in (
                "unsupported_claims",
                "evidence_misreads",
                "answer_safety_findings",
                "missing_analysis",
                "required_corrections",
                "sol_priority_checks",
            ):
                for finding in review[section]:
                    if finding.get("finding_id") == resolution.get(
                        "finding_id"
                    ):
                        finding["affected_json_paths"] = copy.deepcopy(
                            material_paths
                        )
        review = core.validate_critical_review_v2(
            review,
            allowed_evidence_refs=runtime_candidate.allowed_evidence_refs,
            draft_analysis=copy.deepcopy(draft),
            require_network_context=True,
        )
        profile = config["cs408_deep_v2"]
        _, analysis_schema_sha256 = core.CodexRunner._bound_output_schema_bytes(
            Path(profile["analysis_output_schema"]),
            stage_name="cs408_analysis",
            allowed_evidence_refs=candidate.allowed_evidence_refs,
        )
        _, review_schema_sha256 = core.CodexRunner._bound_output_schema_bytes(
            Path(profile["critical_review_output_schema"]),
            stage_name="cs408_critical_review",
            allowed_evidence_refs=candidate.allowed_evidence_refs,
            allowed_analysis_refs=core._analysis_review_refs(draft),
            allowed_correction_paths=core._cs408_writable_correction_paths(
                draft
            ),
        )
        self._stamp_stage_receipts(
            receipts,
            schemas={
                "analysis": analysis_schema_sha256,
                "critical_review": review_schema_sha256,
            },
            payloads={"analysis": draft, "critical_review": review},
            ordered_image_sha256s=list(
                candidate.input_binding.get("ordered_image_sha256s") or []
            ),
        )
        result = core.ModelResult(
            analysis=copy.deepcopy(review["revised_analysis"]),
            duration_ms=2,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            pipeline_status="two_pass_ready",
            draft_analysis=copy.deepcopy(draft),
            critical_review=copy.deepcopy(review),
            stage_receipts=copy.deepcopy(receipts),
        )

        class Runner:
            def run(self, _candidate):
                return result

        worker = core.Worker(config, model_runner=Runner())
        job = worker.process_claimed_candidate(
            candidate, "controlled_replay", write_dashboard=False
        )
        self.assertEqual(job["status"], "two_pass_ready", job)
        reopened = core.reopen_verified_subject_publication(
            config,
            subject="cs408",
            capture_id=candidate.capture_id,
            study_date=candidate.study_date,
            input_fingerprint=candidate.input_fingerprint,
        )
        self.assertEqual(reopened["critical_review_outcome"], "corrected")
        closed = self._close_quality_from_real_authority(
            runtime=runtime,
            config=config,
            worker=worker,
            candidate=candidate,
            publication=reopened,
        )
        self.assertEqual(closed["subject"], "cs408")
        self.assertEqual(closed["formal_write_count"], 0)

    def test_english_production_quality_closes_from_sealed_shape_overlay(
        self,
    ) -> None:
        english_fixture = english_support.EnglishAdapterTests(
            methodName="runTest"
        )
        english_fixture.setUp()
        self.addCleanup(english_fixture.tearDown)
        runtime = english_fixture.runtime
        config = copy.deepcopy(english_fixture.config)
        for index in range(1, 6):
            english_fixture.write_sentence(index)
        fixture, host, host_config = self._new_processing_host(runtime)
        config["processing_plugin"] = host_config

        probe = core.Worker(config)
        status = probe.adapters["english"].status(english_fixture.study_date)
        candidate = probe.adapters["english"].candidates(status)[0]
        fixture, host, host_config, context = self._host_fixture(
            runtime=runtime,
            subject="english",
            capture_id=candidate.capture_id,
            study_date=candidate.study_date,
            input_fingerprint=candidate.input_fingerprint,
            input_binding=candidate.input_binding,
            prepared=(fixture, host, host_config),
        )
        publication, receipts = self._production_read_publication(
            fixture=fixture,
            host=host,
            context=context,
            subject="english",
        )
        analysis_manifest = receipts["analysis"]["mcp_grounding_manifest"]
        critical_manifest = receipts["critical_review"][
            "mcp_grounding_manifest"
        ]

        def required_refs(manifest: dict) -> list[str]:
            artifact_ref = next(
                row["evidence_ref"]
                for row in manifest["items"]
                if row["collection"] == "task_artifact"
            )
            library_ref = next(
                row["evidence_ref"]
                for row in manifest["items"]
                if row["collection"]
                not in {"task_context", "task_artifact"}
            )
            return [artifact_ref, library_ref]

        analysis_refs = required_refs(analysis_manifest)
        critical_refs = required_refs(critical_manifest)

        # English had no accepted 4c1 Analysis output.  Reuse its frozen
        # provider item/grounding shape while deriving all source identity and
        # sentence fields through the production English finalizer fixture.
        seeded = english_support.FakeEnglishRunner(one_item=True).run(candidate)
        draft = copy.deepcopy(seeded.analysis)
        sealed_case = sealed_support.ThreeSubjectSealedChainReplayTests(
            methodName="runTest"
        )
        sealed_grounding = sealed_case._english_analysis_fixture(
            analysis_refs
        )["items"][0]["grounding"]
        sealed_grounding["user_evidence_ref"] = draft["items"][0][
            "source_event_id"
        ]
        draft["items"][0]["grounding"] = sealed_grounding
        draft["candidate_id"] = core.english_candidate_content_id(
            str(candidate.input_binding["candidate_document_id"]), draft
        )
        core.validate_english_candidate_answer_safety(draft)
        core.validate_english_mcp_grounding(
            draft["items"], grounding_manifest=analysis_manifest
        )

        semantic_draft = core.english_review_semantic_draft(draft)
        revised_items = copy.deepcopy(draft["items"])
        revised_items[0]["grounding"]["mcp_evidence_refs"] = critical_refs
        review = {
            "schema_version": "study-intake-english-critical-review-v1",
            "verdict": "confirmed",
            "draft_analysis_sha256": core.sha256_value(semantic_draft),
            "findings": [],
            "correction_resolutions": [],
            "revised_items": revised_items,
        }
        core.validate_english_candidate_answer_safety(review)
        core.validate_english_critical_review(review, semantic_draft)
        core.validate_english_applied_corrections(review)
        core.validate_english_mcp_grounding(
            review["revised_items"], grounding_manifest=critical_manifest
        )
        final = copy.deepcopy(draft)
        final["items"] = copy.deepcopy(review["revised_items"])
        final["producer"]["prompt_version"] = config[
            "english_two_pass_v1"
        ]["critical_review_prompt_version"]
        final["candidate_id"] = core.english_candidate_content_id(
            str(candidate.input_binding["candidate_document_id"]), final
        )

        profile = config["english_two_pass_v1"]
        self._stamp_stage_receipts(
            receipts,
            schemas={
                "analysis": core._schema_sha256(
                    Path(profile["analysis_output_schema"])
                ),
                "critical_review": core._schema_sha256(
                    Path(profile["critical_review_output_schema"])
                ),
            },
            payloads={"analysis": draft, "critical_review": review},
        )
        checkpoint_runner = core.CodexRunner(probe.model_config, runtime)
        checkpoint = checkpoint_runner._write_analysis_checkpoint(
            candidate,
            draft=draft,
            analysis_receipt=receipts["analysis"],
        )
        result = core.ModelResult(
            analysis=final,
            duration_ms=2,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            pipeline_status="two_pass_ready",
            draft_analysis=copy.deepcopy(draft),
            critical_review=copy.deepcopy(review),
            stage_receipts=copy.deepcopy(receipts),
            analysis_checkpoint_sha256=checkpoint["checkpoint_sha256"],
            analysis_checkpoint_ref=checkpoint["checkpoint_ref"],
            analysis_checkpoint_binding_key=checkpoint["binding_key"],
            analysis_checkpoint_binding_sha256=checkpoint["binding_sha256"],
        )

        class Runner:
            def run(self, _candidate):
                return result

        worker = core.Worker(config, model_runner=Runner())
        job = worker.process_claimed_candidate(
            candidate, "controlled_replay", write_dashboard=False
        )
        self.assertEqual(job["status"], "ready", job)
        self.assertEqual(job["pipeline_status"], "two_pass_ready")
        reopened = core.reopen_verified_subject_publication(
            config,
            subject="english",
            capture_id=candidate.capture_id,
            study_date=candidate.study_date,
            input_fingerprint=candidate.input_fingerprint,
        )
        self.assertEqual(reopened["critical_review_outcome"], "accepted")
        closed = self._close_quality_from_real_authority(
            runtime=runtime,
            config=config,
            worker=worker,
            candidate=candidate,
            publication=reopened,
        )
        self.assertEqual(closed["subject"], "english")
        self.assertEqual(closed["formal_write_count"], 0)


if __name__ == "__main__":
    unittest.main()
