from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from lib.math_live_business_fixture import (
    MathLiveBusinessFixtureError,
    validate_manifest,
    validated_task_to_processing_host_capture_args,
    write_content_addressed_receipt,
)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _fingerprint(rows: list[tuple[str, str]]) -> str:
    raw = "".join(f"{digest}  ./{relative}\n" for relative, digest in sorted(rows))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class MathLiveBusinessFixtureTests(unittest.TestCase):
    def _fixture(
        self,
        base: Path,
        *,
        include_solution: bool = True,
        reasoning_inference_allowed: bool = False,
    ) -> tuple[Path, Path, Path]:
        source_root = base / "canonical-math"
        rollout_root = base / "sessions"
        frozen_root = base / "frozen"
        root = base / "live"
        source_root.mkdir()
        rollout_root.mkdir()
        (frozen_root / "items").mkdir(parents=True)
        (root / "items").mkdir(parents=True)

        source_paths: dict[str, Path] = {}
        for formal_id in ("GS-109", "GS-507"):
            source = source_root / f"{formal_id}.md"
            source.write_text(f"canonical solution for {formal_id}\n", encoding="utf-8")
            source_paths[formal_id] = source

        task_turns = {
            "LUNA-MATH-TEST-001": [
                ("2026-08-09T00:00:01Z", "user", "user_first_answer.md", "I solved it", None),
                ("2026-08-09T00:00:02Z", "assistant", "assistant_feedback.md", "feedback one", None),
                ("2026-08-09T00:00:03Z", "assistant", "assistant_feedback.md", "feedback two", None),
                ("2026-08-09T00:00:04Z", "assistant", "assistant_feedback.md", "feedback three", None),
            ],
            "LUNA-MATH-TEST-002": [
                ("2026-08-09T00:01:01Z", "user", "user_reasoning.md", "initial reasoning", "initial"),
                ("2026-08-09T00:01:02Z", "assistant", "assistant_feedback.md", "analysis one", None),
                ("2026-08-09T00:01:03Z", "user", "user_objection.md", "objection", "objection"),
                ("2026-08-09T00:01:04Z", "assistant", "assistant_feedback.md", "analysis two", None),
                ("2026-08-09T00:01:05Z", "user", "user_followup.md", "first correction", "partial"),
                ("2026-08-09T00:01:06Z", "assistant", "assistant_feedback.md", "analysis three", None),
                ("2026-08-09T00:01:07Z", "user", "user_self_correction.md", "second correction", "second"),
                ("2026-08-09T00:01:08Z", "assistant", "assistant_feedback.md", "analysis four", None),
                ("2026-08-09T00:01:09Z", "user", "user_resolution.md", "transfer answer", "transfer"),
                ("2026-08-09T00:01:10Z", "assistant", "assistant_feedback.md", "analysis five", None),
            ],
        }
        rollout = rollout_root / "rollout.jsonl"
        with rollout.open("w", encoding="utf-8") as handle:
            for turns in task_turns.values():
                for timestamp, speaker, _, text, _ in turns:
                    content_type = "input_text" if speaker == "user" else "output_text"
                    source_text = f"metadata\n{text}" if speaker == "user" else text
                    handle.write(
                        json.dumps(
                            {
                                "timestamp": timestamp,
                                "type": "response_item",
                                "payload": {
                                    "type": "message",
                                    "role": speaker,
                                    "content": [{"type": content_type, "text": source_text}],
                                },
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )

        task_specs = [
            (
                "LUNA-MATH-TEST-001",
                "MATH-LUNA-BIZ-TEST-001",
                "GS-109",
                "independent_correct_no_false_wrong_card",
            ),
            (
                "LUNA-MATH-TEST-002",
                "MATH-LUNA-BIZ-TEST-002",
                "GS-507",
                "wrong_then_corrected_weakness_proposal",
            ),
        ]
        frozen_samples: list[dict[str, object]] = []
        for capture_id, _, formal_id, _ in task_specs:
            suffix = capture_id.rsplit("-", 1)[-1]
            staging_id = f"HOLD-TEST-{suffix}"
            frozen_item = frozen_root / "items" / staging_id
            frozen_item.mkdir()
            frozen_question = frozen_item / "question_01.png"
            frozen_question.write_bytes(b"\x89PNG\r\n\x1a\nquestion")
            frozen_samples.append(
                {
                    "staging_id": staging_id,
                    "absolute_path": str(frozen_item.resolve()),
                    "independent_capture": True,
                    "duplicate_of": None,
                    "question_identity": {
                        "formal_id": formal_id,
                        "source_locator": f"formal_card:{formal_id}",
                    },
                    "files": [
                        {
                            "relative_path": "question_01.png",
                            "role": "question_image",
                            "sha256": _sha(frozen_question),
                        }
                    ],
                }
            )
        _write_json(
            frozen_root / "read-only-test-manifest.json",
            {
                "schema_version": "math-read-only-zero-model-test-manifest-v1",
                "fixture_root": str(frozen_root.resolve()),
                "frozen_at": "2026-08-09T11:46:20+08:00",
                "use_mode": "read_only_zero_model_preflight",
                "sample_count": 2,
                "authorization": {
                    "quick_intake_written": False,
                    "formal_write_count": 0,
                    "processing_authorized": False,
                    "luna_authorized": False,
                    "sol_authorized": False,
                    "write_back_to_fixture": False,
                },
                "samples": frozen_samples,
                "separation": {"new_capture_root": str(root.resolve())},
            },
        )
        batch_tasks: list[dict[str, object]] = []
        for task_index, (capture_id, business_id, formal_id, task_kind) in enumerate(
            task_specs
        ):
            item_dir = root / "items" / capture_id
            item_dir.mkdir()
            (item_dir / "question_01.png").write_bytes(b"\x89PNG\r\n\x1a\nquestion")
            if include_solution:
                source = source_paths[formal_id].resolve()
                (item_dir / "solution_text.md").write_text(
                    "---\n"
                    "role: solution_text\n"
                    "visibility: private_evidence\n"
                    "source_kind: obsidian_visual_detail_card\n"
                    f"source_path: {source}\n"
                    f"source_sha256: {_sha(source)}\n"
                    "solution_image_present: false\n"
                    "solution_image_absence: authentic_source_has_text_solution_only\n"
                    "---\n\n"
                    "text solution\n",
                    encoding="utf-8",
                )

            turns = task_turns[capture_id]
            assistant_sections: list[str] = []
            dialogue_rows: list[dict[str, object]] = []
            for sequence, (timestamp, speaker, filename, text, evidence_kind) in enumerate(
                turns, start=1
            ):
                if speaker == "assistant":
                    assistant_sections.append(f"## {timestamp}\n\n{text}\n")
                    dialogue_rows.append(
                        {
                            "sequence": sequence,
                            "timestamp": timestamp,
                            "speaker": speaker,
                            "file": filename,
                            "section": timestamp,
                        }
                    )
                else:
                    (item_dir / filename).write_text(text + "\n", encoding="utf-8")
                    dialogue_rows.append(
                        {
                            "sequence": sequence,
                            "timestamp": timestamp,
                            "speaker": speaker,
                            "file": filename,
                            "evidence_kind": evidence_kind or "answer",
                        }
                    )
            (item_dir / "assistant_feedback.md").write_text(
                "# feedback\n\n" + "\n".join(assistant_sections), encoding="utf-8"
            )
            _write_json(
                item_dir / "dialogue_index.json",
                {
                    "schema_version": "math-verbatim-dialogue-index-v1",
                    "session_id": "SESSION-TEST",
                    "source_rollout": str(rollout),
                    "turns": dialogue_rows,
                    "reasoning_transcript_status": (
                        "user_attested_process_correct_but_did_not_transcribe_steps"
                        if task_kind == "independent_correct_no_false_wrong_card"
                        else "verbatim_user_reasoning_and_corrections_present"
                    ),
                    "fabricated_turn_count": 0,
                },
            )

            role_by_name = {
                "question_01.png": "question_image",
                "solution_text.md": "solution_text",
                "assistant_feedback.md": "verbatim_assistant_feedback",
                "dialogue_index.json": "dialogue_sequence_and_provenance",
                "user_first_answer.md": "verbatim_user_answer",
                "user_reasoning.md": "verbatim_initial_user_reasoning",
                "user_objection.md": "verbatim_user_objection",
                "user_followup.md": "verbatim_partial_self_correction",
                "user_self_correction.md": "verbatim_second_self_correction",
                "user_resolution.md": "verbatim_transfer_and_capture_request",
            }
            artifacts = []
            for path in sorted(item_dir.iterdir()):
                role = role_by_name[path.name]
                artifacts.append(
                    {
                        "relative_path": path.name,
                        "role": role,
                        "visibility": (
                            "answer_safe_question_surface"
                            if role == "question_image"
                            else "private_evidence"
                        ),
                        "sha256": _sha(path),
                    }
                )
            source = source_paths[formal_id].resolve()
            business = {
                "task_kind": task_kind,
                "goal": "test",
                "expected_outputs": (
                    [
                        "evidence_bounded_result_classification",
                        "do_not_create_wrong_card_recommendation",
                        "explicit_unknowns_for_unobserved_reasoning",
                        "proposal_only_learning_observation",
                    ]
                    if task_kind == "independent_correct_no_false_wrong_card"
                    else [
                        "first_break_and_later_breaks",
                        "independent_correct_steps",
                        "post_explanation_understanding_boundary",
                        "weakness_distillation_proposal",
                        "no_formal_write",
                    ]
                ),
                "luna_two_stage": {
                    "stage_1": "evidence_grounded_analysis",
                    "stage_2": "independent_critical_review",
                    "adoption_mode": "proposal_only",
                },
            }
            result = (
                {
                    "classification": "correct",
                    "independent_original_success": True,
                    "user_confirmation": "correct",
                    "reasoning_transcript_status": "not_provided",
                    "reasoning_inference_allowed": reasoning_inference_allowed,
                }
                if task_kind == "independent_correct_no_false_wrong_card"
                else {
                    "classification": "wrong_then_corrected_after_explanation",
                    "independent_original_success": False,
                    "post_explanation_transfer": {"result": "correct"},
                }
            )
            record: dict[str, object] = {
                "schema_version": "math-luna-business-capture-v1",
                "capture_id": capture_id,
                "business_task_id": business_id,
                "study_date": "2026-08-09",
                "state": "ready_for_luna_business_processing",
                "derived_from_frozen_capture": frozen_samples[task_index][
                    "absolute_path"
                ],
                "question_identity": {
                    "formal_id": formal_id,
                    "source_locator": f"formal_card:{formal_id}",
                    "formal_card_path": str(source),
                    "formal_card_sha256": _sha(source),
                },
                "result": result,
                "solution_evidence": {
                    "accepted_policy": "solution_text_or_solution_image",
                    "solution_text_present": include_solution,
                    "solution_image_present": False,
                    "solution_image_absence": "authentic_obsidian_source_contains_text_solution_but_no_solution_image",
                    "source_verified": True,
                },
                "business_task": business,
                "model_request": {
                    "model": "gpt-5.6-luna",
                    "reasoning_effort": "max",
                    "runtime_attestation": "requested_unverified",
                },
                "authorization": {
                    "processing_authorized": True,
                    "luna_authorized": True,
                    "sol_authorized": False,
                    "quick_intake_written": False,
                    "formal_write_count": 0,
                },
                "privacy": {"visibility": "private_evidence"},
                "artifacts": artifacts,
                "missing_artifacts": [
                    {
                        "role": "solution_image",
                        "required": False,
                        "reason": "text supplied",
                    }
                ],
                "blocking_missing_artifacts": [],
            }
            if task_kind == "wrong_then_corrected_weakness_proposal":
                record["evidence_capsule"] = {
                    "independent_correct_steps": ["step"],
                    "first_break": "break",
                    "later_breaks": ["later"],
                    "resolution": "resolved",
                }
            _write_json(item_dir / "record.json", record)
            item_files = artifacts + [
                {
                    "relative_path": "record.json",
                    "role": "business_task_record",
                    "visibility": "private_evidence",
                    "sha256": _sha(item_dir / "record.json"),
                }
            ]
            rows = [(str(row["relative_path"]), str(row["sha256"])) for row in item_files]
            fingerprint = _fingerprint(rows)
            item_manifest = {
                "schema_version": "math-luna-business-task-manifest-v1",
                "capture_id": capture_id,
                "business_task_id": business_id,
                "formal_id": formal_id,
                "state": "sealed_ready_for_luna",
                "independent_capture": True,
                "duplicate_of": None,
                "source_fixture_preserved": True,
                "content_fingerprint": {
                    "algorithm": "sha256_of_sorted_relative_path_and_file_sha256_lines_excluding_manifest",
                    "value": fingerprint,
                },
                "authorization": {
                    "processing_authorized": True,
                    "luna_authorized": True,
                    "sol_authorized": False,
                    "formal_write_count": 0,
                },
                "solution_policy": {
                    "requirement": "solution_text_or_solution_image",
                    "satisfied_by": "solution_text.md" if include_solution else None,
                    "solution_image_present": False,
                    "absence_is_authentic_business_case": True,
                },
                "files": item_files,
                "missing_artifacts_consistent_with_record": True,
                "blocking_missing_artifacts": [],
            }
            _write_json(item_dir / "manifest.json", item_manifest)
            batch_tasks.append(
                {
                    "business_task_id": business_id,
                    "capture_id": capture_id,
                    "formal_id": formal_id,
                    "absolute_path": str(item_dir),
                    "manifest_path": str(item_dir / "manifest.json"),
                    "manifest_sha256": _sha(item_dir / "manifest.json"),
                    "content_fingerprint": fingerprint,
                    "task_kind": task_kind,
                    "blocking_missing_artifacts": [],
                }
            )

        _write_json(
            root / "contract.json",
            {
                "schema_version": "math-deferred-live-capture-hold-v1",
                "state": "collecting",
                "canonical_status": "noncanonical_deferred_holding_only",
                "created_at": "2026-08-09T00:00:00+08:00",
                "separation": {
                    "previous_test_fixture": str(frozen_root.resolve()),
                    "write_to_previous_test_fixture": False,
                    "reason": "test binding",
                },
                "policy": {
                    "quick_intake_written": False,
                    "formal_write_count": 0,
                    "processing_authorized": False,
                    "immutable_artifacts": True,
                    "overwrite_existing_artifacts": False,
                },
                "authorized_business_task_exceptions": [
                    {
                        "business_task_id": task["business_task_id"],
                        "capture_id": task["capture_id"],
                        "processing_authorized": True,
                        "luna_authorized": True,
                        "sol_authorized": False,
                        "formal_write_count": 0,
                    }
                    for task in batch_tasks
                ],
                "storage": {},
                "current_item_count": 2,
                "notes": [],
            },
        )
        manifest = root / "luna-business-task-manifest.json"
        _write_json(
            manifest,
            {
                "schema_version": "math-luna-business-task-batch-v1",
                "batch_id": "MATH-LUNA-BATCH-TEST-001",
                "root": str(root),
                "state": "ready_for_luna_business_processing",
                "task_count": 2,
                "business_sample_type": "real_math_learning_captures",
                "solution_evidence_policy": {
                    "requirement": "solution_text_or_solution_image",
                    "business_fact": "text source",
                    "solution_images_fabricated": False,
                    "solution_text_copied_with_source_path_and_hash": True,
                },
                "authorization": {
                    "processing_authorized": True,
                    "luna_authorized": True,
                    "sol_authorized": False,
                    "formal_write_count": 0,
                    "quick_intake_written": False,
                    "scope": "exactly_the_two_listed_business_task_ids",
                },
                "model_request": {
                    "model": "gpt-5.6-luna",
                    "reasoning_effort": "max",
                    "runtime_attestation": "requested_unverified",
                },
                "tasks": batch_tasks,
                "safety": {
                    "frozen_source_fixture_unchanged": True,
                    "private_solution_and_dialogue_not_learner_facing": True,
                    "luna_output_proposal_only": True,
                    "formal_mutation_authorized": False,
                },
            },
        )
        return manifest, source_root, rollout_root

    def test_valid_two_task_fixture_passes_without_writes(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(base)
            result = validate_manifest(
                manifest,
                trusted_source_root=source_root,
                trusted_rollout_root=rollout_root,
            )
        self.assertEqual(result["status"], "passed_luna_business_preflight")
        self.assertEqual(result["task_count"], 2)
        self.assertEqual(result["distinct_capture_count"], 2)
        self.assertEqual(result["solution_evidence_policy"]["solution_text_task_count"], 2)
        self.assertTrue(all(task["luna_eligible"] for task in result["tasks"]))
        self.assertTrue(all(task["artifacts"] for task in result["tasks"]))
        self.assertEqual(result["model_call_count"], 0)
        self.assertEqual(result["formal_write_count"], 0)

    def test_neither_solution_text_nor_image_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(
                base, include_solution=False
            )
            with self.assertRaisesRegex(
                MathLiveBusinessFixtureError, "math_live_task_role_set_invalid"
            ):
                validate_manifest(
                    manifest,
                    trusted_source_root=source_root,
                    trusted_rollout_root=rollout_root,
                )

    def test_source_hash_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(base)
            (source_root / "GS-109.md").write_text("drift\n", encoding="utf-8")
            with self.assertRaisesRegex(
                MathLiveBusinessFixtureError, "math_live_solution_source_hash_mismatch"
            ):
                validate_manifest(
                    manifest,
                    trusted_source_root=source_root,
                    trusted_rollout_root=rollout_root,
                )

    def test_artifact_hash_tamper_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(base)
            artifact = base / "live/items/LUNA-MATH-TEST-002/user_reasoning.md"
            artifact.write_text("tamper\n", encoding="utf-8")
            with self.assertRaisesRegex(
                MathLiveBusinessFixtureError, "math_live_item_file_hash_mismatch"
            ):
                validate_manifest(
                    manifest,
                    trusted_source_root=source_root,
                    trusted_rollout_root=rollout_root,
                )

    def test_symlink_or_extra_file_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(base)
            (base / "live/unexpected.txt").write_text("x", encoding="utf-8")
            with self.assertRaisesRegex(
                MathLiveBusinessFixtureError,
                "math_live_fixture_root_file_set_mismatch",
            ):
                validate_manifest(
                    manifest,
                    trusted_source_root=source_root,
                    trusted_rollout_root=rollout_root,
                )

        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(base)
            (base / "live/link").symlink_to(base / "live/contract.json")
            with self.assertRaisesRegex(
                MathLiveBusinessFixtureError, "math_live_fixture_symlink_forbidden"
            ):
                validate_manifest(
                    manifest,
                    trusted_source_root=source_root,
                    trusted_rollout_root=rollout_root,
                )

    def test_unknown_reasoning_cannot_be_marked_inferable(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(
                base, reasoning_inference_allowed=True
            )
            with self.assertRaisesRegex(
                MathLiveBusinessFixtureError,
                "math_live_independent_correct_boundary_invalid",
            ):
                validate_manifest(
                    manifest,
                    trusted_source_root=source_root,
                    trusted_rollout_root=rollout_root,
                )

    def test_model_or_writer_authorization_change_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(base)
            value = json.loads(manifest.read_text(encoding="utf-8"))
            value["model_request"]["reasoning_effort"] = "high"
            _write_json(manifest, value)
            with self.assertRaisesRegex(
                MathLiveBusinessFixtureError, "math_live_batch_model_invalid"
            ):
                validate_manifest(
                    manifest,
                    trusted_source_root=source_root,
                    trusted_rollout_root=rollout_root,
                )

    def test_content_addressed_receipt_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            result = {"schema_version": "test", "formal_write_count": 0}
            first_path, first_sha = write_content_addressed_receipt(
                result, root / "receipts"
            )
            second_path, second_sha = write_content_addressed_receipt(
                result, root / "receipts"
            )
            self.assertEqual(first_path, second_path)
            self.assertEqual(first_sha, second_sha)
            self.assertEqual(_sha(first_path), first_sha)

    def test_unrelated_rollout_append_does_not_change_selected_turn_binding(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(base)
            first = validate_manifest(
                manifest,
                trusted_source_root=source_root,
                trusted_rollout_root=rollout_root,
            )
            with (rollout_root / "rollout.jsonl").open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "timestamp": "2026-08-09T23:59:59Z",
                            "payload": {
                                "type": "message",
                                "role": "assistant",
                                "content": [
                                    {"type": "output_text", "text": "unrelated"}
                                ],
                            },
                        }
                    )
                    + "\n"
                )
            second = validate_manifest(
                manifest,
                trusted_source_root=source_root,
                trusted_rollout_root=rollout_root,
            )
            first_bindings = [
                task["dialogue_provenance"]["source_turns_sha256"]
                for task in first["tasks"]
            ]
            second_bindings = [
                task["dialogue_provenance"]["source_turns_sha256"]
                for task in second["tasks"]
            ]
            self.assertEqual(first_bindings, second_bindings)

    def test_out_of_scope_sibling_capture_is_reported_but_not_consumed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(base)
            sibling = base / "live/items/LUNA-MATH-TEST-003"
            sibling.mkdir()
            (sibling / "unbound-private.txt").write_text("private\n", encoding="utf-8")
            first = validate_manifest(
                manifest,
                trusted_source_root=source_root,
                trusted_rollout_root=rollout_root,
            )
            (sibling / "unbound-private.txt").write_text(
                "changed but still out of scope\n", encoding="utf-8"
            )
            second = validate_manifest(
                manifest,
                trusted_source_root=source_root,
                trusted_rollout_root=rollout_root,
            )
            self.assertEqual(first["fixture_tree_sha256"], second["fixture_tree_sha256"])
            self.assertEqual(first["out_of_scope_item_directory_count"], 1)
            self.assertEqual(
                first["out_of_scope_item_directory_ids"], ["LUNA-MATH-TEST-003"]
            )
            self.assertEqual(first["out_of_scope_item_contents_read_count"], 0)

    def test_missing_batch_manifest_is_wrapped_as_stable_error(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            missing = Path(raw) / "missing.json"
            with self.assertRaisesRegex(
                MathLiveBusinessFixtureError, "math_live_batch_manifest_missing"
            ):
                validate_manifest(missing)

    def test_validated_task_converts_to_path_free_host_capture_facts(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(base)
            result = validate_manifest(
                manifest,
                trusted_source_root=source_root,
                trusted_rollout_root=rollout_root,
            )
            for task in result["tasks"]:
                args = validated_task_to_processing_host_capture_args(task)
                declared_ids = [row["artifact_id"] for row in task["artifacts"]]
                host_ids = [row["artifact_id"] for row in args["capture_artifacts"]]
                self.assertEqual(len(host_ids), len(declared_ids))
                self.assertEqual(set(host_ids), set(declared_ids))
                self.assertEqual(len(host_ids), len(set(host_ids)))
                facts_raw = (
                    json.dumps(
                        args["capture_facts"],
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                ).encode("utf-8")
                self.assertEqual(
                    hashlib.sha256(facts_raw).hexdigest(),
                    args["capture_facts_sha256"],
                )
                facts_text = facts_raw.decode("utf-8")
                for marker in ("/Users/", "/private/", "/var/", "/tmp/"):
                    self.assertNotIn(marker, facts_text)
                for private_body in (
                    "I solved it",
                    "initial reasoning",
                    "feedback one",
                    "analysis one",
                    "text solution",
                ):
                    self.assertNotIn(private_body, facts_text)
                self.assertEqual(
                    set(args["capture_facts"]["facts"]),
                    {"task", "identity", "artifact_index"},
                )
                self.assertEqual(args["captured_at"], "2026-08-09T11:46:20+08:00")
                by_id = {row["artifact_id"]: row for row in task["artifacts"]}
                for host_artifact in args["capture_artifacts"]:
                    original = by_id[host_artifact["artifact_id"]]
                    expected_kind = {
                        "question_image": "question_image",
                        "solution_text": "solution_text",
                        "solution_image": "solution_image",
                        "business_task_record": "learning_record",
                    }.get(original["kind"], "dialogue")
                    self.assertEqual(host_artifact["artifact_kind"], expected_kind)

    def test_derived_frozen_question_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(base)
            frozen_question = base / "frozen/items/HOLD-TEST-001/question_01.png"
            frozen_question.write_bytes(b"\x89PNG\r\n\x1a\ndrift")
            with self.assertRaisesRegex(
                MathLiveBusinessFixtureError,
                "math_live_derived_fixture_question_hash_mismatch",
            ):
                validate_manifest(
                    manifest,
                    trusted_source_root=source_root,
                    trusted_rollout_root=rollout_root,
                )

    def test_new_source_capture_args_omit_formal_id_and_use_source_id(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            base = Path(raw)
            manifest, source_root, rollout_root = self._fixture(base)
            result = validate_manifest(
                manifest,
                trusted_source_root=source_root,
                trusted_rollout_root=rollout_root,
            )
            task = dict(result["tasks"][1])
            task["formal_id"] = None
            task["source_route"] = "new_source_learning_episode"
            task["source_locator"] = "question-bank-id:170710"
            task["formal_id_must_remain_null"] = True
            task["derived_capture_binding"] = {
                "formal_id": None,
                "formal_id_must_remain_null": True,
                "source_locator": "question-bank-id:170710",
                "captured_at": task["captured_at"],
            }
            args = validated_task_to_processing_host_capture_args(task)
            self.assertEqual(args["capture_scene"], "new_intake")
            self.assertNotIn("formal_id", args["capture_identity"])
            self.assertEqual(
                args["capture_identity"]["source_id"],
                "question-bank-id:170710",
            )
            self.assertIsNone(
                args["capture_facts"]["facts"]["identity"]["formal_id"]
            )

    def test_superseded_entrypoint_fails_with_stable_code(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            manifest = Path(raw) / "old.json"
            _write_json(
                manifest,
                {
                    "schema_version": "math-luna-business-task-batch-v1",
                    "status": "superseded_do_not_consume",
                },
            )
            with self.assertRaisesRegex(
                MathLiveBusinessFixtureError,
                "math_live_manifest_superseded",
            ):
                validate_manifest(manifest)


if __name__ == "__main__":
    unittest.main()
