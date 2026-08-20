from __future__ import annotations

import copy
import hashlib
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT / "lib"))

import math_shadow_backaudit as audit  # noqa: E402
from historical_test_input import (  # noqa: E402
    HistoricalTestInput,
    HistoricalTestInputError,
    capture_manifest,
    persist_content_addressed,
    render_shadow_test_config,
)


TEST_SEED = "test-private-seed-" + "a" * 48


def write_json(path: Path, value: object) -> str:
    data = audit.canonical_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return hashlib.sha256(data).hexdigest()


class Fixture:
    def __init__(self, base: Path) -> None:
        self.runtime = base / "runtime"
        self.repo = base / "repo"
        self.v2 = base / "v2"
        self.study_date = "2026-08-04"
        self.capture_ids = [
            "MFI-CAP-111111111111111111111111",
            "MFI-CAP-222222222222222222222222",
        ]
        self.freeze_id = "MFI-FREEZE-333333333333333333333333"
        self.formal_ids = ["GS-001", "GS-002"]
        self.v2_paths: dict[str, Path] = {}
        self._build()

    def publish_v2(self, capture_id: str, package: dict) -> Path:
        previous = self.v2_paths.get(capture_id)
        if previous is not None and previous.exists():
            previous.unlink()
        digest = audit.sha256_value(package)
        path = self.v2 / f"{digest}.json"
        write_json(path, package)
        self.v2_paths[capture_id] = path
        return path

    def _analysis_v1(self, capture_id: str) -> dict:
        ref = "capture.evidence.first_break.text"
        claim = {
            "text": f"{capture_id} 的候选断点",
            "evidence_refs": [ref],
            "confidence": "medium",
        }
        return {
            "schema_version": "study-intake-luna-analysis-v1",
            "summary": f"{capture_id} 的旧候选",
            "independent_correct_steps": [claim],
            "first_break": {"kind": "condition", **claim},
            "later_breaks": [],
            "hint_dependencies": [],
            "contradictions": [],
            "unresolved": [],
            "candidate_updates": [
                {
                    "field": "wrong_point",
                    "proposal": f"{capture_id} 的旧字段候选",
                    "evidence_refs": [ref],
                    "confidence": "medium",
                }
            ],
            "nightly_checks": [claim],
            "risk_flags": [],
        }

    def _analysis_v2(self, capture_id: str) -> dict:
        ref = "capture.evidence.first_break.text"
        claim = {
            "claim_type": "observed_fact",
            "text": f"{capture_id} 的证据绑定候选",
            "provenance": "capture",
            "evidence_refs": [ref],
            "confidence": "high",
            "counterevidence_or_boundary": "不外推到未绑定事实。",
            "sol_verification_action": "Sol 重开原始证据逐项核对。",
        }
        return {
            "schema_version": "study-intake-luna-math-analysis-v2",
            "report_profile": "math_deep",
            "executive_summary": f"{capture_id} 的新候选",
            "target_identity": {
                "formal_target": [claim],
                "delivered_target": [claim],
                "knowledge_fallback_anchor": [],
                "identity_boundary": [claim],
            },
            "question_structure": {
                "objects": [claim],
                "conditions": [claim],
                "asked_task": [claim],
                "source_answer": [claim],
            },
            "correct_reasoning_reconstruction": [claim],
            "evidence_assessment": {
                "completeness": "complete",
                "evidence_inventory": [claim],
                "observed_facts": [claim],
                "inferences": [],
                "contradictions": [],
                "gaps": [],
            },
            "reasoning_diagnosis": {
                "independent_correct_steps": [claim],
                "first_break": claim,
                "later_breaks": [],
                "hint_dependencies": [],
                "self_corrections": [],
                "history_merge": [],
            },
            "concept_method_analysis": {
                "mechanisms": [claim],
                "method_triggers": [claim],
                "applicability_conditions": [claim],
                "boundary_conditions": [claim],
                "common_confusions": [claim],
                "transfer_risks": [claim],
            },
            "formalization_candidates": {
                field: [claim] for field in audit.MATH_FORMALIZATION_FIELDS
            },
            "risk_flags": [],
            "unresolved": [],
            "sol_verification_plan": {
                "must_verify": [claim],
                "reject_if": [claim],
                "source_checks": [claim],
                "identity_checks": [claim],
                "recommended_disposition": "update_existing_candidate",
            },
        }

    def _build(self) -> None:
        ledger_events: list[dict] = []
        snapshots: list[dict] = []
        targets: list[dict] = []
        for index, (capture_id, formal_id) in enumerate(
            zip(self.capture_ids, self.formal_ids)
        ):
            artifact_rel = f"sources/{capture_id}/question.png"
            artifact_path = self.repo / artifact_rel
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_bytes(f"image-{capture_id}".encode())
            artifact_sha = audit.sha256_file(artifact_path)
            source_rel = f"sources/{capture_id}/manifest.json"
            source_value = {
                "schema_version": "math-fast-intake-source-bundle-v1",
                "study_date": self.study_date,
                "source_locator": f"fixture:{capture_id}",
                "bundle_id": capture_id.lower(),
                "artifacts": (
                    [
                        {
                            "path": artifact_rel,
                            "role": "question",
                            "media_type": "image/png",
                            "sha256": artifact_sha,
                            "size": artifact_path.stat().st_size,
                        }
                    ]
                    if index == 0
                    else []
                ),
            }
            source_sha = write_json(self.repo / source_rel, source_value)
            capture = {
                "schema_version": "math-fast-intake-ledger-v1",
                "event_type": "capture",
                "event_id": capture_id,
                "study_date": self.study_date,
                "requested_action": "record_wrong",
                "evidence": {
                    "result": "wrong",
                    "first_break": {
                        "kind": "condition",
                        "origin": "user_confirmed",
                        "text": f"{capture_id} 的第一个断点",
                    },
                    "hints_needed": (
                        [
                            {
                                "origin": "assistant_explained",
                                "text": "fixture hint",
                            }
                        ]
                        if index == 0
                        else []
                    ),
                },
                "source_bundle": {
                    "manifest_path": source_rel,
                    "manifest_hash": source_sha,
                    "source_locator": f"fixture:{capture_id}",
                    "study_date": self.study_date,
                },
                "target": {
                    "formal_id": formal_id if index == 0 else None,
                    "kind": "formal_card" if index == 0 else "new_source",
                },
            }
            capture["content_hash"] = audit.ledger_sha256_value(capture)
            content_hash = capture["content_hash"]
            evidence_hash = audit.ledger_sha256_value(capture["evidence"])
            target_hash = audit.ledger_sha256_value(capture["target"])
            source_bundle_hash = audit.ledger_sha256_value(capture["source_bundle"])
            ledger_events.append(capture)
            snapshots.append(
                {
                    "capture_event_id": capture_id,
                    "capture_content_hash": content_hash,
                    "effective_evidence_hash": evidence_hash,
                    "effective_target_hash": target_hash,
                    "effective_source_bundle_hash": source_bundle_hash,
                    "requested_action": "record_wrong",
                    "amendment_event_ids": [],
                }
            )

            card_rel = f"错题知识网络/错题卡/{formal_id}_fixture.md"
            card_path = self.repo / card_rel
            card_path.parent.mkdir(parents=True, exist_ok=True)
            card_path.write_text(f"---\nid: {formal_id}\n---\n", encoding="utf-8")
            card_sha = audit.sha256_file(card_path)
            identity_mode = "existing_formal" if index == 0 else "new_source_created"
            targets.append(
                {
                    "capture_event_ids": [capture_id],
                    "formal_id": formal_id,
                    "identity_mode": identity_mode,
                    "card_path_before": card_rel if index == 0 else None,
                    "card_hash_before": card_sha if index == 0 else None,
                    "source_binding": {
                        "artifact_path": source_rel,
                        "artifact_hash": source_sha,
                        "resolved_source_hash": source_sha,
                        "source_locator": f"fixture:{capture_id}",
                    },
                    "supplemental_source_bundles": [],
                }
            )

            package_id = f"PKG-{index + 1}"
            package = {
                "schema_version": "study-intake-preprocess-package-v1",
                "subject": "math",
                "study_date": self.study_date,
                "capture_id": capture_id,
                "package_id": package_id,
                "input_fingerprint": hashlib.sha256(
                    f"input-{capture_id}".encode()
                ).hexdigest(),
                "input_binding": {
                    "adapter_version": "math-pending-v1",
                    "original_content_hash": content_hash,
                    "effective_evidence_hash": evidence_hash,
                    "effective_target_hash": target_hash,
                    "source_manifest_hash": source_sha,
                    "formal_card_hash": card_sha if index == 0 else None,
                    "amendment_event_ids": [],
                },
                "analysis": self._analysis_v1(capture_id),
                "model_receipt": {
                    "requested_model": "fixture-model",
                    "requested_reasoning_effort": "max",
                },
                "allowed_evidence_refs": ["capture.evidence.first_break.text"],
                "formal_write_count": 0,
            }
            package_path = (
                self.runtime
                / "packages/math"
                / self.study_date
                / capture_id
                / f"package-{index + 1}.json"
            )
            package_sha = write_json(package_path, package)
            write_json(
                self.runtime
                / "state/adoptions/math"
                / capture_id
                / f"adoption-{index + 1}.json",
                {
                    "schema_version": "study-intake-preprocess-adoption-v1",
                    "subject": "math",
                    "study_date": self.study_date,
                    "capture_id": capture_id,
                    "package_id": package_id,
                    "package_sha256": package_sha,
                    "adoption_receipt_id": f"ADOPTION-{index + 1}",
                    "outcome": "modified_adopted",
                    "reason_code": "sol_narrowed",
                    "formal_write_count": 0,
                },
            )
            write_json(
                self.runtime
                / "receipts/math"
                / self.study_date
                / f"run-{index + 1}.json",
                {
                    "schema_version": "study-intake-preprocess-receipt-v1",
                    "subject": "math",
                    "study_date": self.study_date,
                    "capture_id": capture_id,
                    "package_id": package_id,
                    "status": "ready",
                    "formal_write_count": 0,
                },
            )
        freeze = {
            "schema_version": "math-fast-intake-ledger-v1",
            "event_type": "freeze",
            "event_id": self.freeze_id,
            "freeze_schema_version": "math-fast-intake-freeze-v1",
            "study_date": self.study_date,
            "capture_event_ids": list(self.capture_ids),
            "capture_snapshots": snapshots,
            "targets": targets,
            "ledger_prefix": audit.ledger_event_prefix(ledger_events),
        }
        freeze["content_hash"] = audit.ledger_sha256_value(freeze)
        ledger_events.append(freeze)
        ledger_path = self.repo / audit.LEDGER_RELATIVE_PATH
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        ledger_path.write_text(
            "".join(
                json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n"
                for value in ledger_events
            ),
            encoding="utf-8",
        )


class MathShadowBackauditTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.fixture = Fixture(self.base)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def manifest(self) -> dict:
        manifest = audit.build_manifest(
            self.fixture.runtime,
            self.fixture.repo,
            self.fixture.study_date,
            expected_count=2,
        )
        for index, item in enumerate(manifest["items"], start=1):
            capture_id = item["capture_id"]
            analysis = self.fixture._analysis_v2(capture_id)
            snapshot = item["capture_snapshot"]
            replay = item["replay_input"]
            source = replay.get("source_bundle") or {}
            historical = replay["formal_target"]["historical_card"]
            contract_sha = hashlib.sha256(b"fixture-contract").hexdigest()
            evidence_manifest_sha = hashlib.sha256(
                f"manifest-{capture_id}".encode()
            ).hexdigest()
            evidence_bundle_sha = hashlib.sha256(
                f"bundle-{capture_id}".encode()
            ).hexdigest()
            binding = {
                "adapter_version": "math-pending-v1",
                "original_content_hash": snapshot["capture_content_hash"],
                "effective_evidence_hash": snapshot["effective_evidence_hash"],
                "effective_target_hash": snapshot["effective_target_hash"],
                "amendment_event_ids": snapshot["amendment_event_ids"],
                "source_manifest_hash": source.get("manifest_sha256"),
                "formal_card_hash": historical.get("sha256"),
                "capture_event_sha256": item["capture_event_sha256"],
                "replay_input_sha256": item["replay_input_sha256"],
                "backaudit_manifest_sha256": manifest["manifest_sha256"],
                "processing_contract_sha256": contract_sha,
                "evidence_manifest_sha256": evidence_manifest_sha,
                "evidence_bundle_sha256": evidence_bundle_sha,
            }
            processing_sha = hashlib.sha256(
                f"processing-{capture_id}".encode()
            ).hexdigest()
            report_sha = audit.sha256_value(analysis)
            write_json(
                self.fixture.runtime
                / "private/reports/objects"
                / f"{report_sha}.json",
                analysis,
            )
            markdown = f"# {capture_id} fixture\n"
            markdown_sha = hashlib.sha256(markdown.encode("utf-8")).hexdigest()
            markdown_path = (
                self.fixture.runtime
                / "private/reports/markdown"
                / f"{markdown_sha}.md"
            )
            markdown_path.parent.mkdir(parents=True, exist_ok=True)
            markdown_path.write_text(markdown, encoding="utf-8")
            report_id = f"SIP-REPORT-{'B' * 23}{index}"
            critical_review = {
                "schema_version": "study-intake-luna-math-critical-review-v2",
                "verdict": "pass",
                "summary": "fixture review",
                "revised_analysis": analysis,
                "unsupported_claims": [],
                "evidence_misreads": [],
                "mathematical_errors": [],
                "visual_findings": [],
                "provenance_findings": [],
                "missing_analysis": [],
                "required_corrections": [],
                "sol_priority_checks": [],
            }
            stage_receipts = {}
            for stage, stage_payload in (
                ("analysis", analysis),
                ("critical_review", critical_review),
            ):
                artifact = {
                    "schema_version": "study-intake-luna-stage-artifact-v2",
                    "stage": stage,
                    "report_id": report_id,
                    "capture_id": capture_id,
                    "study_date": self.fixture.study_date,
                    "payload": stage_payload,
                    "formal_write_count": 0,
                }
                artifact_sha = audit.sha256_value(artifact)
                write_json(
                    self.fixture.runtime
                    / "private/reports/stages"
                    / f"{artifact_sha}.json",
                    artifact,
                )
                stage_receipts[stage] = {
                    "status": "ready",
                    "result_sha256": audit.sha256_value(stage_payload),
                    "runtime_identity_status": "confirmed",
                    "requested_model": "gpt-5.6-luna",
                    "requested_reasoning_effort": "max",
                    "runtime_model": "gpt-5.6-luna",
                    "runtime_reasoning_effort": "max",
                    "runtime_metadata_provenance": "test_attestation_v1",
                    "artifact_ref": f"study-intake-stage://sha256/{artifact_sha}",
                    "artifact_sha256": artifact_sha,
                }
            renderer_sha = hashlib.sha256(b"renderer").hexdigest()
            quality = {
                "schema_version": "study-intake-luna-math-quality-receipt-v2",
                "pipeline_status": "two_pass_ready",
                "two_stage_status": "two_pass_ready",
                "quality_gate_status": "pass",
                "mode": "shadow",
                "consumable": False,
                "processing_contract_sha256": contract_sha,
                "processing_fingerprint": processing_sha,
                "report_json_ref": f"study-intake-report://sha256/{report_sha}",
                "report_json_sha256": report_sha,
                "report_markdown_ref": (
                    f"study-intake-report-markdown://sha256/{markdown_sha}"
                ),
                "report_markdown_sha256": markdown_sha,
                "renderer_build_sha256": renderer_sha,
                "evidence_manifest_sha256": evidence_manifest_sha,
                "evidence_bundle_sha256": evidence_bundle_sha,
                "runtime_identity": {
                    "analysis": "confirmed",
                    "critical_review": "confirmed",
                },
                "authoritative_stage": "critical_review_revised",
                "formal_write_count": 0,
            }
            package = {
                "schema_version": "study-intake-preprocess-package-v2",
                "package_id": f"SIP-PKG-{'A' * 23}{index}",
                "report_id": report_id,
                "subject": "math",
                "capture_id": capture_id,
                "study_date": self.fixture.study_date,
                "created_at": "2026-08-04T12:00:00Z",
                "input_fingerprint": audit.sha256_value(binding),
                "input_binding": binding,
                "processing_contract_sha256": contract_sha,
                "processing_fingerprint": processing_sha,
                "evidence_manifest_sha256": evidence_manifest_sha,
                "evidence_bundle_sha256": evidence_bundle_sha,
                "report_json_ref": f"study-intake-report://sha256/{report_sha}",
                "report_json_sha256": report_sha,
                "report_markdown_ref": (
                    "study-intake-report-markdown://sha256/"
                    + markdown_sha
                ),
                "report_markdown_sha256": markdown_sha,
                "renderer_build_sha256": renderer_sha,
                "quality_receipt_sha256": audit.sha256_value(quality),
                "pipeline_status": "two_pass_ready",
                "stage_receipts": stage_receipts,
                "quality_receipt": quality,
                "allowed_evidence_refs": ["capture.evidence.first_break.text"],
                "formal_write_count": 0,
            }
            self.fixture.publish_v2(capture_id, package)
        return manifest

    def test_study_date_and_ledger_freeze_prefix_fail_closed(self) -> None:
        with self.assertRaisesRegex(audit.AuditError, "study_date_invalid"):
            audit.build_manifest(
                self.fixture.runtime,
                self.fixture.repo,
                "../../2026-08-04",
                expected_count=2,
            )
        ledger = self.fixture.repo / audit.LEDGER_RELATIVE_PATH
        rows = [json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()]
        rows[-1]["ledger_prefix"]["events_hash"] = "0" * 64
        rows[-1]["content_hash"] = audit.ledger_sha256_value(
            {key: value for key, value in rows[-1].items() if key != "content_hash"}
        )
        ledger.write_text(
            "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(audit.AuditError, "ledger_freeze_prefix_mismatch"):
            audit.build_manifest(
                self.fixture.runtime,
                self.fixture.repo,
                self.fixture.study_date,
                expected_count=2,
            )

    def test_manifest_closure_and_current_rename_do_not_change_frozen_hash(self) -> None:
        first = self.manifest()
        tampered = copy.deepcopy(first)
        tampered["items"][0]["replay_input_sha256"] = "0" * 64
        tampered["manifest_sha256"] = audit.sha256_value(
            audit.manifest_integrity_payload(tampered)
        )
        with self.assertRaisesRegex(audit.AuditError, "manifest_item_binding_invalid"):
            audit.verify_manifest(tampered)

        card_root = self.fixture.repo / "错题知识网络/错题卡"
        old = next(card_root.glob("GS-001_*.md"))
        renamed = card_root / "GS-001_renamed.md"
        old.rename(renamed)
        second = self.manifest()
        first_item = first["items"][0]
        second_item = second["items"][0]
        self.assertEqual(first["manifest_sha256"], second["manifest_sha256"])
        self.assertEqual(
            first_item["replay_input_sha256"], second_item["replay_input_sha256"]
        )
        self.assertEqual(
            second_item["replay_input"]["formal_target"]["historical_card"]["path"],
            second_item["frozen_target_mapping"]["card_path_before"],
        )
        self.assertNotEqual(
            first_item["current_formal_target"]["path"],
            second_item["current_formal_target"]["path"],
        )

    def test_v2_content_addresses_and_allowed_refs_fail_closed(self) -> None:
        manifest = self.manifest()
        capture_id = self.fixture.capture_ids[0]
        report_sha = self.fixture._analysis_v2(capture_id)
        report_digest = audit.sha256_value(report_sha)
        report_path = (
            self.fixture.runtime
            / "private/reports/objects"
            / f"{report_digest}.json"
        )
        report_path.write_text("{}\n", encoding="utf-8")
        discovery = audit.discover_v2(manifest, self.fixture.runtime, self.fixture.v2)
        self.assertNotEqual(discovery["status"], "complete")

        manifest = self.manifest()
        package_path = self.fixture.v2_paths[capture_id]
        package = json.loads(package_path.read_text(encoding="utf-8"))
        package["allowed_evidence_refs"].append("capture.fabricated_claim")
        self.fixture.publish_v2(capture_id, package)
        discovery = audit.discover_v2(manifest, self.fixture.runtime, self.fixture.v2)
        self.assertEqual(
            discovery["invalid"][capture_id], "v2_allowed_evidence_refs_invalid"
        )

    def test_private_seed_is_generated_once_and_not_public_default(self) -> None:
        manifest = self.manifest()
        seed, path = audit.resolve_blind_seed(
            runtime_root=self.fixture.runtime,
            manifest=manifest,
            command="prepare-blind",
            explicit_seed=None,
            seed_file=None,
            write=True,
        )
        self.assertIsNotNone(path)
        self.assertGreaterEqual(len(seed.encode("utf-8")), 32)
        self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)
        loaded, loaded_path = audit.resolve_blind_seed(
            runtime_root=self.fixture.runtime,
            manifest=manifest,
            command="score",
            explicit_seed=None,
            seed_file=None,
            write=False,
        )
        self.assertEqual((loaded, loaded_path), (seed, path))

    def test_inspect_is_deterministic_and_does_not_write(self) -> None:
        ledger = self.fixture.repo / audit.LEDGER_RELATIVE_PATH
        before = audit.sha256_file(ledger)
        first = self.manifest()
        second = self.manifest()
        self.assertEqual(first, second)
        self.assertEqual(first["manifest_sha256"], second["manifest_sha256"])
        self.assertEqual([item["formal_id"] for item in first["items"]], self.fixture.formal_ids)
        self.assertEqual(first["baseline_adoption_counts"], {"modified_adopted": 2})
        self.assertEqual(audit.sha256_file(ledger), before)
        self.assertFalse((self.fixture.runtime / "state/evaluations").exists())

    def test_missing_v2_is_explicit_and_cannot_prepare_blind_set(self) -> None:
        manifest = self.manifest()
        self.fixture.v2_paths[self.fixture.capture_ids[1]].unlink()
        discovery = audit.discover_v2(manifest, self.fixture.runtime, self.fixture.v2)
        self.assertEqual(discovery["status"], "incomplete")
        self.assertEqual(discovery["missing_capture_ids"], [self.fixture.capture_ids[1]])
        with self.assertRaisesRegex(audit.AuditError, "v2_candidate_set_incomplete"):
            audit.build_blind_set(manifest, discovery, TEST_SEED)

    def test_v2_must_bind_frozen_input_and_real_identity_fields(self) -> None:
        manifest = self.manifest()
        capture_id = self.fixture.capture_ids[0]
        path = self.fixture.v2_paths[capture_id]
        package = json.loads(path.read_text(encoding="utf-8"))
        package["input_binding"]["replay_input_sha256"] = "0" * 64
        package["input_fingerprint"] = audit.sha256_value(package["input_binding"])
        self.fixture.publish_v2(capture_id, package)
        discovery = audit.discover_v2(manifest, self.fixture.runtime, self.fixture.v2)
        self.assertEqual(
            discovery["invalid"][capture_id], "v2_frozen_input_binding_mismatch"
        )

        manifest = self.manifest()
        path = self.fixture.v2_paths[capture_id]
        package = json.loads(path.read_text(encoding="utf-8"))
        package["stage_receipts"]["analysis"].pop("runtime_model")
        self.fixture.publish_v2(capture_id, package)
        discovery = audit.discover_v2(manifest, self.fixture.runtime, self.fixture.v2)
        self.assertFalse(discovery["candidates"][capture_id]["runtime_attested"])

    def test_limited_historical_replay_cannot_promote(self) -> None:
        manifest = self.manifest()
        manifest["items"][0]["replay_status"] = "limited_by_evidence"
        manifest["items"][0]["replay_limitations"] = ["fixture_limited"]
        manifest["items"][0]["replay_input"]["limitations"] = ["fixture_limited"]
        manifest["items"][0]["replay_input_sha256"] = audit.sha256_value(
            manifest["items"][0]["replay_input"]
        )
        manifest["manifest_sha256"] = audit.sha256_value(
            audit.manifest_integrity_payload(manifest)
        )
        # Rebind fixtures to the amended immutable manifest.
        for item in manifest["items"]:
            path = self.fixture.v2_paths[item["capture_id"]]
            package = json.loads(path.read_text(encoding="utf-8"))
            package["input_binding"]["backaudit_manifest_sha256"] = manifest[
                "manifest_sha256"
            ]
            package["input_binding"]["replay_input_sha256"] = item[
                "replay_input_sha256"
            ]
            package["input_fingerprint"] = audit.sha256_value(package["input_binding"])
            self.fixture.publish_v2(item["capture_id"], package)
        discovery = audit.discover_v2(manifest, self.fixture.runtime, self.fixture.v2)
        blind_set = audit.build_blind_set(manifest, discovery, TEST_SEED)
        key = audit.blind_key(manifest, TEST_SEED)
        cases = []
        for case in blind_set["cases"]:
            candidates = {
                label: {
                    "claim_total": case["candidates"][label]["claim_count"],
                    "supported_claims": case["candidates"][label]["claim_count"],
                    "p0_findings": [],
                    "first_break": "pass",
                    "hint_attribution": (
                        "pass"
                        if case["hint_attribution_required"]
                        else "not_applicable"
                    ),
                    "visual_grounding": (
                        "pass" if case["visual_required"] else "not_applicable"
                    ),
                    "adoption_outcome": "direct",
                    "review_duration_ms": 100,
                    "notes": None,
                }
                for label in ("A", "B")
            }
            mapping = key[case["case_id"]]
            preferred = "A" if mapping["A"] == "v2" else "B"
            cases.append(
                {
                    "case_id": case["case_id"],
                    "preference": preferred,
                    "preference_reason": "fixture",
                    "candidates": candidates,
                }
            )
        judgments = {
            "schema_version": audit.JUDGMENT_SCHEMA,
            "blind_set_sha256": blind_set["blind_set_sha256"],
            "evaluator_id": "fixture-sol",
            "cases": cases,
            "formal_write_count": 0,
        }
        report = audit.score_blind_set(
            manifest, discovery, blind_set, judgments, TEST_SEED
        )
        self.assertEqual(report["promotion_status"], "shadow_only")
        self.assertFalse(report["historical_replay_complete"])

    def test_blind_set_is_reproducible_and_hides_version_metadata(self) -> None:
        manifest = self.manifest()
        discovery = audit.discover_v2(manifest, self.fixture.runtime, self.fixture.v2)
        first = audit.build_blind_set(manifest, discovery, TEST_SEED)
        second = audit.build_blind_set(manifest, discovery, TEST_SEED)
        self.assertEqual(first, second)
        self.assertEqual(first["case_count"], 2)
        encoded_candidates = json.dumps(
            [case["candidates"] for case in first["cases"]],
            ensure_ascii=False,
            sort_keys=True,
        )
        for forbidden in (
            "schema_version",
            "report_profile",
            "requested_model",
            "pipeline_status",
            "package_id",
        ):
            self.assertNotIn(forbidden, encoded_candidates)
        self.assertEqual(
            set(first["cases"][0]["candidates"]), {"A", "B"}
        )
        for candidate in first["cases"][0]["candidates"].values():
            formalization = candidate["payload"]["formalization_candidates"]
            self.assertTrue(
                set(audit.MATH_FORMALIZATION_FIELDS).issubset(formalization)
            )
            self.assertIn("other_candidates", formalization)
        template = audit.judgment_template(first)
        for case, row in zip(first["cases"], template["cases"]):
            for label in ("A", "B"):
                self.assertEqual(
                    row["candidates"][label]["claim_total"],
                    case["candidates"][label]["claim_count"],
                )

    def test_scoring_computes_quality_and_promotion_gates(self) -> None:
        manifest = self.manifest()
        discovery = audit.discover_v2(manifest, self.fixture.runtime, self.fixture.v2)
        blind_set = audit.build_blind_set(manifest, discovery, TEST_SEED)
        key = audit.blind_key(manifest, TEST_SEED)
        cases = []
        blind_by_id = {case["case_id"]: case for case in blind_set["cases"]}
        for case_id, mapping in key.items():
            blind_case = blind_by_id[case_id]
            candidates = {}
            for label in ("A", "B"):
                is_v2 = mapping[label] == "v2"
                claim_total = blind_case["candidates"][label]["claim_count"]
                candidates[label] = {
                    "claim_total": claim_total,
                    "supported_claims": claim_total if is_v2 else max(0, claim_total - 1),
                    "p0_findings": [],
                    "first_break": "pass" if is_v2 else "fail",
                    "hint_attribution": (
                        ("pass" if is_v2 else "fail")
                        if blind_case["hint_attribution_required"]
                        else "not_applicable"
                    ),
                    "visual_grounding": (
                        "pass"
                        if blind_case["visual_required"]
                        else "not_applicable"
                    ),
                    "adoption_outcome": "direct" if is_v2 else "major_edit",
                    "review_duration_ms": 100,
                    "notes": None,
                }
            preferred = "A" if mapping["A"] == "v2" else "B"
            cases.append(
                {
                    "case_id": case_id,
                    "preference": preferred,
                    "preference_reason": "证据绑定更完整。",
                    "candidates": candidates,
                }
            )
        judgments = {
            "schema_version": audit.JUDGMENT_SCHEMA,
            "blind_set_sha256": blind_set["blind_set_sha256"],
            "evaluator_id": "fixture-sol",
            "cases": cases,
            "formal_write_count": 0,
        }
        report = audit.score_blind_set(
            manifest, discovery, blind_set, judgments, TEST_SEED
        )
        self.assertEqual(report["quality_gate_status"], "pass")
        self.assertEqual(report["promotion_status"], "shadow_only")
        self.assertFalse(report["runtime_attested"])
        self.assertEqual(report["blind_preference"]["v2_win_rate"], 1.0)
        self.assertEqual(report["metrics"]["v2"]["evidence"]["precision"], 1.0)
        self.assertEqual(report["metrics"]["v2"]["adoption"]["direct_rate"], 1.0)
        self.assertEqual(report["blind_preference"]["scorable_coverage"], 1.0)
        self.assertTrue(all(gate["status"] == "pass" for gate in report["gates"].values()))

        wrong_total = copy.deepcopy(judgments)
        wrong_total["cases"][0]["candidates"]["A"]["claim_total"] += 1
        with self.assertRaisesRegex(audit.AuditError, "judgment_claim_total_mismatch"):
            audit.score_blind_set(
                manifest, discovery, blind_set, wrong_total, TEST_SEED
            )

        incomplete = copy.deepcopy(judgments)
        incomplete["cases"][0]["preference"] = "unscorable"
        incomplete_report = audit.score_blind_set(
            manifest, discovery, blind_set, incomplete, TEST_SEED
        )
        self.assertEqual(
            incomplete_report["gates"]["scorable_coverage"]["status"], "fail"
        )

    def test_writes_are_content_addressed_and_confined_to_evaluations(self) -> None:
        manifest = self.manifest()
        allowed = self.fixture.runtime / "state/evaluations/custom"
        first = audit.persist_evaluation_artifacts(
            self.fixture.runtime,
            allowed,
            manifest,
            [("manifest", manifest)],
        )
        second = audit.persist_evaluation_artifacts(
            self.fixture.runtime,
            allowed,
            manifest,
            [("manifest", manifest)],
        )
        self.assertEqual(first, second)
        self.assertEqual(len(list(allowed.rglob("*.json"))), 1)
        with self.assertRaisesRegex(
            audit.AuditError, "evaluation_output_outside_dedicated_root"
        ):
            audit.persist_evaluation_artifacts(
                self.fixture.runtime,
                self.base / "outside",
                manifest,
                [("manifest", manifest)],
            )


class HistoricalTestInputFixtureTests(unittest.TestCase):
    """Small local-only checks for the hermetic historical input boundary."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _build_fixture(self) -> tuple[Path, Path, Path]:
        historical_root = self.base / "historical-source"
        runtime_root = self.base / "runtime-source"
        formal_root = self.base / "formal-source"
        fixture_root = self.base / "hermetic-fixture"
        for root in (historical_root, runtime_root, formal_root):
            root.mkdir(parents=True)
        for relative in (
            "state/jobs/math",
            "state/latest/math",
            "state/offers",
            "state/adoptions/math",
            "packages/math",
            "receipts/math",
        ):
            (runtime_root / relative).mkdir(parents=True)
        legacy_root = (
            runtime_root
            / "private/mcp-authority-snapshots/objects/sha256/58/"
            "5898a690a9dbbd4ec1e36127fbce61312a9ff95239b3f820b92a7b68456f4ee0/root"
        )
        for relative in (
            "bank/master_bank.csv",
            "bank/sentence_patterns.md",
            "articles/2026-07-10-2011-english-i-text-4.md",
            "bank/mastered_items.csv",
        ):
            write_json(legacy_root / relative, {"fixture": relative})
        write_json(
            runtime_root
            / "deployments/three-subject-model-lane-staging-20260809/"
            "artifacts/pre-model-gate/"
            "en-p0-006-legacy-disposition-requirements.json",
            {"fixture": "requirements"},
        )
        (formal_root / "错题知识网络/错题卡").mkdir(parents=True)
        ledger_relative = "ledger.jsonl"
        (formal_root / ledger_relative).write_text("{}\n", encoding="utf-8")

        items: list[dict[str, object]] = []
        expected_replay: dict[str, str] = {}
        for index in range(10):
            capture_id = f"FIXTURE-CAP-{index:02d}"
            source_relative = f"sources/{capture_id}/manifest.json"
            artifact_relative = f"sources/{capture_id}/question.png"
            card_relative = f"错题知识网络/错题卡/{capture_id}.md"
            source_sha = write_json(
                formal_root / source_relative,
                {"source_locator": f"fixture:{capture_id}"},
            )
            artifact_path = formal_root / artifact_relative
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_bytes(f"image-{capture_id}".encode("utf-8"))
            artifact_sha = audit.sha256_file(artifact_path)
            (formal_root / card_relative).write_text(
                f"card-{capture_id}\n", encoding="utf-8"
            )
            package_relative = f"packages/math/2026-08-04/{capture_id}/package.json"
            receipt_relative = f"receipts/math/2026-08-04/{capture_id}.json"
            adoption_relative = f"state/adoptions/math/{capture_id}.json"
            write_json(runtime_root / package_relative, {"capture_id": capture_id})
            write_json(runtime_root / receipt_relative, {"capture_id": capture_id})
            write_json(runtime_root / adoption_relative, {"capture_id": capture_id})
            replay_input = {
                "source_bundle": {
                    "manifest_path": source_relative,
                    "manifest_sha256": source_sha,
                    "artifacts": [
                        {"path": artifact_relative, "sha256": artifact_sha}
                    ],
                },
                "formal_target": {
                    "historical_card": {"path": card_relative}
                },
            }
            replay_sha = audit.sha256_value(replay_input)
            expected_replay[capture_id] = replay_sha
            items.append(
                {
                    "capture_id": capture_id,
                    "replay_input_sha256": replay_sha,
                    "v1": {
                        "package_path": package_relative,
                        "run_receipt_path": receipt_relative,
                    },
                    "baseline_adoption": {"path": adoption_relative},
                    "replay_input": replay_input,
                    "current_formal_target": {"path": card_relative},
                }
            )

        historical_value = {
            "manifest_sha256": "f" * 64,
            "selection_contract": {"ledger_path": ledger_relative},
            "items": items,
        }
        historical_manifest = historical_root / "august-fourth.json"
        historical_manifest.write_bytes(audit.canonical_bytes(historical_value))
        verification_source = self.base / "successor-source"
        plugin_root = verification_source / "plugin/kaoyan-study-intake"
        plugin_root.mkdir(parents=True)
        shared_schema = verification_source / "schemas/shared.json"
        shared_schema.parent.mkdir(parents=True)
        shared_schema.write_bytes(b'{"schema":"fixture"}\n')
        external_root = self.base / "external-source"
        math_status_path = external_root / "kaoyan-math/scripts/quick_intake.py"
        math_status_path.parent.mkdir(parents=True)
        math_status_path.write_bytes(b"math status fixture\n")
        status_path = external_root / "kaoyan-408/scripts/status.py"
        status_path.parent.mkdir(parents=True)
        status_path.write_bytes(b"status fixture\n")
        english_events_path = (
            external_root / "kaoyan-english/english_pipeline/events.py"
        )
        english_events_path.parent.mkdir(parents=True)
        english_events_path.write_bytes(b"english events fixture\n")
        release_root = external_root / "mcp/releases/fixture-release"
        policy_path = release_root / "config/skill-tool-policy.json"
        launcher_path = release_root / "scripts/sealed_launcher.py"
        policy_payload = b'{"policy":"fixture"}\n'
        launcher_payload = b"sealed launcher fixture\n"
        policy_path.parent.mkdir(parents=True)
        launcher_path.parent.mkdir(parents=True)
        policy_path.write_bytes(policy_payload)
        launcher_path.write_bytes(launcher_payload)
        source_files = {
            "config/skill-tool-policy.json": audit.sha256_file(policy_path),
            "scripts/sealed_launcher.py": audit.sha256_file(launcher_path),
        }
        release_id = audit.sha256_value(source_files)
        release_root = external_root / "mcp/releases" / release_id
        release_root.parent.mkdir(parents=True, exist_ok=True)
        (release_root / "config").mkdir(parents=True)
        (release_root / "scripts").mkdir(parents=True)
        policy_path.rename(release_root / "config/skill-tool-policy.json")
        launcher_path.rename(release_root / "scripts/sealed_launcher.py")
        release_manifest = {
            "schema_version": "study-read-mcp-release.v1",
            "source_files": source_files,
            "release_id": release_id,
            "formal_write_count": 0,
        }
        release_manifest_path = release_root / "release.json"
        release_manifest_path.write_bytes(audit.canonical_bytes(release_manifest))
        python_target = external_root / "python/bin/python-real"
        python_target.parent.mkdir(parents=True)
        python_target.write_bytes(b"#!/bin/sh\n")
        python_target.chmod(0o755)
        python_declared = external_root / "python/bin/python"
        python_declared.symlink_to(python_target)
        components = {
            "schema_version": "kaoyan-study-intake-components.v1",
            "external_runtime_sources": {
                "math_status_script": {
                    "path": str(math_status_path),
                    "sha256": audit.sha256_file(math_status_path),
                },
                "cs408_status_script": {
                    "path": str(status_path),
                    "sha256": audit.sha256_file(status_path),
                },
                "english_events_module": {
                    "path": str(english_events_path),
                    "sha256": audit.sha256_file(english_events_path),
                },
            },
            "mcp": {
                "release_root": str(release_root),
                "release_id": release_id,
                "release_manifest": str(release_manifest_path),
                "release_manifest_sha256": audit.sha256_file(release_manifest_path),
                "python_executable": str(python_declared),
                "sealed_launcher_path": str(release_root / "scripts/sealed_launcher.py"),
                "sealed_launcher_sha256": audit.sha256_file(
                    release_root / "scripts/sealed_launcher.py"
                ),
            },
        }
        components_path = plugin_root / "components.json"
        components_path.write_bytes(audit.canonical_bytes(components))
        component_lock = {
            "schema_version": "kaoyan-study-intake-component-lock.v1",
            "registry_sha256": audit.sha256_file(components_path),
            "schemas": {"shared.json": audit.sha256_file(shared_schema)},
        }
        (plugin_root / "component-lock.json").write_bytes(
            audit.canonical_bytes(component_lock)
        )
        descriptor = capture_manifest(
            historical_manifest_root=historical_root,
            runtime_data_root=runtime_root,
            formal_surface_root=formal_root,
            historical_manifest_path=historical_manifest,
            expected_manifest_sha256=audit.sha256_file(historical_manifest),
            expected_manifest_content_sha256="f" * 64,
            expected_replay_input_sha256=expected_replay,
            fixture_root=fixture_root,
            source_root=verification_source,
        )
        manifest_path = persist_content_addressed(fixture_root, descriptor)
        return manifest_path, runtime_root, fixture_root

    def test_source_drift_after_freeze_does_not_change_fixture(self) -> None:
        manifest_path, runtime_root, _ = self._build_fixture()
        self.assertEqual(manifest_path.name, f"{audit.sha256_file(manifest_path)}.json")
        test_input = HistoricalTestInput.load(manifest_path)
        fixture_package = test_input.paths_for_role("historical_package")[0]
        frozen_bytes = fixture_package.read_bytes()
        source_package = runtime_root / "packages/math/2026-08-04"
        source_file = next(source_package.rglob("package.json"))
        source_file.write_bytes(b"source changed after capture")
        reloaded = HistoricalTestInput.load(manifest_path)
        self.assertEqual(reloaded.paths_for_role("historical_package")[0].read_bytes(), frozen_bytes)
        self.assertEqual(reloaded.snapshot(), test_input.snapshot())

    def test_fixture_path_escape_and_file_tamper_fail_closed(self) -> None:
        manifest_path, runtime_root, fixture_root = self._build_fixture()
        test_input = HistoricalTestInput.load(manifest_path)
        escaped_root = self.base / "escaped-manifest"
        escaped_value = copy.deepcopy(dict(test_input.payload))
        outside_runtime = self.base / "outside-runtime"
        outside_runtime.mkdir()
        escaped_value["runtime_data_root"] = str(outside_runtime)
        escaped_manifest = persist_content_addressed(escaped_root, escaped_value)
        with self.assertRaisesRegex(
            HistoricalTestInputError, "historical_test_input_fixture_root_escape"
        ):
            HistoricalTestInput.load(escaped_manifest)

        old_live_index_root = self.base / "old-live-index"
        old_live_value = copy.deepcopy(dict(test_input.payload))
        old_live_value["historical_manifest_root"] = str(runtime_root)
        old_live_value["runtime_data_root"] = str(runtime_root)
        old_live_value["formal_surface_root"] = str(runtime_root)
        old_manifest = persist_content_addressed(old_live_index_root, old_live_value)
        with self.assertRaisesRegex(
            HistoricalTestInputError, "historical_test_input_fixture_root_escape"
        ):
            HistoricalTestInput.load(old_manifest)

        tampered = test_input.paths_for_role("historical_package")[0]
        original_bytes = tampered.read_bytes()
        tampered.chmod(0o644)
        tampered.write_bytes(b"tampered fixture bytes")
        with self.assertRaisesRegex(
            HistoricalTestInputError, "historical_test_input_file_not_read_only"
        ):
            HistoricalTestInput.load(manifest_path)

        tampered.chmod(0o644)
        tampered.write_bytes(b"tampered but relocked fixture bytes")
        tampered.chmod(0o444)
        with self.assertRaisesRegex(
            HistoricalTestInputError, "historical_test_input_file_drift"
        ):
            HistoricalTestInput.load(manifest_path)

        tampered.chmod(0o644)
        tampered.write_bytes(original_bytes)
        tampered.chmod(0o444)
        fixture_root.chmod(0o755)
        (fixture_root / "unexpected-link").symlink_to(tampered)
        fixture_root.chmod(0o555)
        with self.assertRaisesRegex(
            HistoricalTestInputError, "historical_test_input_fixture_symlink"
        ):
            HistoricalTestInput.load(manifest_path)

    def test_historical_replay_roots_are_fixture_bound(self) -> None:
        manifest_path, _, _ = self._build_fixture()
        test_input = HistoricalTestInput.load(manifest_path)
        config = render_shadow_test_config(ROOT, test_input)
        self.assertEqual(Path(config["runtime_root"]), test_input.runtime_data_root)
        self.assertEqual(
            Path(config["adapters"]["math"]["repo_root"]),
            test_input.formal_surface_root,
        )
        self.assertTrue(
            all(
                path.is_relative_to(test_input.fixture_root)
                for path in test_input.paths_for_role("historical_input")
            )
        )

    def test_external_verification_is_frozen_and_tamper_safe(self) -> None:
        manifest_path, _, fixture_root = self._build_fixture()
        test_input = HistoricalTestInput.load(manifest_path)
        self.assertEqual(
            test_input.verification_root,
            (fixture_root / "verification").resolve(),
        )
        self.assertTrue(test_input.external_verification_rows)
        self.assertEqual(
            {
                role
                for row in test_input.external_verification_rows
                for role in row["roles"]
                if role in {"math_status", "cs408_status", "english_events"}
            },
            {"math_status", "cs408_status", "english_events"},
        )
        for row in test_input.external_verification_rows:
            declared = Path(str(row["declared_path"]))
            expected = test_input.verification_root / declared.relative_to(
                Path(declared.anchor)
            )
            self.assertEqual(Path(str(row["absolute_path"])), expected)
            self.assertTrue(Path(str(row["absolute_path"])).is_file())
            if row["executable"]:
                self.assertTrue(os.access(Path(str(row["absolute_path"])), os.X_OK))
                self.assertEqual(
                    Path(str(row["absolute_path"])).stat().st_mode & 0o222,
                    0,
                )

        status_source = self.base / "external-source/kaoyan-408/scripts/status.py"
        frozen_status = next(
            Path(str(row["absolute_path"]))
            for row in test_input.external_verification_rows
            if "cs408_status" in row["roles"]
        )
        frozen_bytes = frozen_status.read_bytes()
        status_source.write_bytes(b"live external status changed\n")
        reloaded = HistoricalTestInput.load(manifest_path)
        self.assertEqual(reloaded.verification_root, test_input.verification_root)
        self.assertEqual(frozen_status.read_bytes(), frozen_bytes)

        escaped = copy.deepcopy(dict(test_input.payload))
        escaped["external_verification"][0]["declared_path"] = "/../escape"
        escaped_path = persist_content_addressed(self.base / "escaped-external", escaped)
        with self.assertRaisesRegex(
            HistoricalTestInputError,
            "historical_test_input_fixture_root_escape|historical_test_input_external_fixture_path_escape|historical_test_input_external_verification_row_invalid",
        ):
            HistoricalTestInput.load(escaped_path)

        writable = next(
            Path(str(row["absolute_path"]))
            for row in test_input.external_verification_rows
            if "mcp_release_source" in row["roles"]
        )
        writable.chmod(0o644)
        with self.assertRaisesRegex(
            HistoricalTestInputError, "historical_test_input_file_not_read_only"
        ):
            HistoricalTestInput.load(manifest_path)

        writable.write_bytes(b"tampered external source\n")
        writable.chmod(0o444)
        with self.assertRaisesRegex(
            HistoricalTestInputError, "historical_test_input_file_drift"
        ):
            HistoricalTestInput.load(manifest_path)

        second_base = self.base / "symlink-case"
        second_base.mkdir()
        original_base = self.base
        self.base = second_base
        try:
            manifest_path, _, _ = self._build_fixture()
            test_input = HistoricalTestInput.load(manifest_path)
            python_fixture = next(
                Path(str(row["absolute_path"]))
                for row in test_input.external_verification_rows
                if "python_executable" in row["roles"]
            )
            python_fixture.parent.chmod(0o755)
            python_fixture.unlink()
            python_fixture.symlink_to(
                self.base / "external-source/python/bin/python-real"
            )
            with self.assertRaisesRegex(
                HistoricalTestInputError,
                "historical_test_input_file_invalid|historical_test_input_fixture_symlink",
            ):
                HistoricalTestInput.load(manifest_path)
        finally:
            self.base = original_base


class RealAugustFourthBackauditTests(unittest.TestCase):
    def test_real_august_fourth_selection_is_exactly_ten(self) -> None:
        test_input = HistoricalTestInput.from_env()
        before = test_input.snapshot()
        with test_input.enforce_reads():
            manifest = audit.build_manifest(
                test_input.runtime_data_root,
                test_input.formal_surface_root,
                audit.DEFAULT_STUDY_DATE,
                expected_count=10,
                historical_paths={
                    "packages": test_input.paths_for_role("historical_package"),
                    "adoptions": test_input.paths_for_role("historical_adoption"),
                    "receipts": test_input.paths_for_role("historical_run_receipt"),
                },
            )
        after = test_input.snapshot()
        self.assertEqual(before, after)
        self.assertEqual(
            {item["capture_id"]: item["replay_input_sha256"] for item in manifest["items"]},
            test_input.expected_replay_input_sha256,
        )
        self.assertEqual(len(manifest["items"]), 10)
        self.assertEqual(
            manifest["baseline_adoption_counts"], {"modified_adopted": 10}
        )
        self.assertEqual(
            {item["baseline_adoption"]["reason_code"] for item in manifest["items"]},
            {"sol_narrowed"},
        )
        self.assertEqual(sum(item["visual_required"] for item in manifest["items"]), 9)
        self.assertTrue(all(item["formal_write_count"] == 0 for item in manifest["items"]))


if __name__ == "__main__":
    unittest.main()
