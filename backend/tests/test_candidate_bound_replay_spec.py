from __future__ import annotations

import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import build_candidate_bound_replay_spec as builder


def _write_json(path: Path, value: object) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return hashlib.sha256(path.read_bytes()).hexdigest()


class CandidateBoundReplaySpecTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(
            prefix="candidate-bound-replay-"
        )
        self.root = Path(self.temporary.name)
        self.release_id = "a" * 64
        self.candidate = self.root / "releases" / self.release_id
        self.candidate.mkdir(parents=True)
        release_path = self.candidate / "release.json"
        _write_json(release_path, {"release_id": self.release_id})
        self.config_path = self.candidate / "config.json"
        _write_json(
            self.config_path,
            {"release": {"manifest_path": str(release_path)}},
        )
        self.math_root = self.root / "math"
        self.math_root.mkdir()
        self.business_manifest = self.root / "math-business.json"
        _write_json(self.business_manifest, {})
        self.source_spec_path = self.root / "source-spec.json"
        _write_json(self.source_spec_path, self._source_spec())

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _source_bundle(
        self,
        name: str,
        roles: tuple[str, ...],
    ) -> dict[str, str]:
        bundle = self.math_root / name
        bundle.mkdir(parents=True)
        artifacts = []
        for index, role in enumerate(roles, start=1):
            suffix = ".txt" if role == "solution_text" else ".png"
            artifact = bundle / f"{role}-{index}{suffix}"
            artifact.write_text(f"fixture {role}\n", encoding="utf-8")
            artifacts.append(
                {
                    "role": role,
                    "path": str(artifact.relative_to(self.math_root)),
                    "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
                }
            )
        manifest = bundle / "manifest.json"
        manifest_sha256 = _write_json(manifest, {"artifacts": artifacts})
        return {
            "manifest_path": str(manifest.relative_to(self.math_root)),
            "manifest_hash": manifest_sha256,
        }

    def _external_capture(
        self,
        name: str,
        roles: tuple[str, ...],
        *,
        user_answer: str = "用户原始作答",
    ) -> tuple[str, str]:
        wrapper = {
            "schema_version": builder.EXTERNAL_SCHEMA,
            "release_id": "d" * 64,
            "formal_write_count": 0,
            "capture": {
                "event_id": f"MFI-{name.upper()}",
                "capture_schema_version": "math-fast-intake-capture-v2",
                "formal_id": None,
                "source_bundle": self._source_bundle(name, roles),
                "episode_evidence": {
                    "solution_text": "内联解析不能替代 artifact",
                    "user_answer_text": user_answer,
                },
            },
        }
        path = self.root / "external" / f"{name}.json"
        digest = _write_json(path, wrapper)
        return str(path), digest

    def _source_spec(self) -> dict[str, object]:
        entries = [
            {
                "subject": "english",
                "capture_id": "EN-A",
                "sample_role": "english-q37-a",
                "expected_input_fingerprint": "1" * 64,
            },
            {
                "subject": "english",
                "capture_id": "EN-C",
                "sample_role": "english-q37-c",
                "expected_input_fingerprint": "1" * 64,
            },
            {
                "subject": "english",
                "capture_id": "EN-D",
                "sample_role": "english-q37-d",
                "expected_input_fingerprint": "1" * 64,
            },
            {
                "subject": "cs408",
                "capture_id": "CS-DS",
                "sample_role": "DS_2023_002",
                "expected_input_fingerprint": "2" * 64,
            },
            {
                "subject": "cs408",
                "capture_id": "CS-FILE",
                "sample_role": "FILE_PROTECTION",
                "expected_input_fingerprint": "3" * 64,
            },
            {
                "subject": "cs408",
                "capture_id": "CS-OS",
                "sample_role": "OS_2009_003",
                "expected_input_fingerprint": "4" * 64,
            },
            {
                "subject": "cs408",
                "capture_id": "CS-FREE",
                "sample_role": "FREE_SPACE",
                "expected_input_fingerprint": "5" * 64,
            },
            {
                "subject": "math",
                "capture_id": "MATH-111",
                "sample_role": "GS-111",
                "expected_input_fingerprint": "6" * 64,
            },
            {
                "subject": "math",
                "capture_id": "MATH-240",
                "sample_role": "GS-240",
                "expected_input_fingerprint": "7" * 64,
            },
        ]
        complete_path, complete_sha = self._external_capture(
            "complete",
            ("question", "solution"),
        )
        entries.append(
            {
                "subject": "math",
                "capture_id": "MATH-NEW",
                "sample_role": "complete-new-intake",
                "expected_input_fingerprint": "8" * 64,
                "external_capture_manifest_path": complete_path,
                "external_capture_manifest_sha256": complete_sha,
            }
        )
        fixtures = {
            "missing_question_image": self._external_capture(
                "missing-question",
                ("solution",),
            ),
            "missing_solution_image": self._external_capture(
                "missing-all-solution",
                ("question",),
            ),
            "missing_solution_text": self._external_capture(
                "image-only",
                ("question", "solution"),
            ),
            "missing_user_answer": self._external_capture(
                "missing-user",
                ("question", "solution"),
                user_answer="",
            ),
        }
        preflight = []
        for role, (path, digest) in fixtures.items():
            preflight.append(
                {
                    "sample_role": role,
                    "external_capture_manifest_path": path,
                    "external_capture_manifest_sha256": digest,
                    "expected_error_code": builder.MATH_EVIDENCE_ERROR,
                }
            )
        return {
            "schema_version": builder.SPEC_SCHEMA,
            "release_id": "d" * 64,
            "formal_write_count": 0,
            "entries": entries,
            "preflight_cases": preflight,
        }

    @staticmethod
    def _business_result() -> dict[str, object]:
        return {
            "status": "passed_real_luna_business_preflight",
            "task_count": 3,
            "tasks": [
                {
                    "capture_id": "LUNA-MATH-20260809-001",
                    "content_fingerprint": "c" * 64,
                    "luna_eligible": True,
                    "proposal_only": True,
                    "solution_evidence": {
                        "accepted_policy": "solution_text_or_solution_image",
                        "solution_text_present": True,
                        "solution_image_present": False,
                    },
                },
                {
                    "capture_id": "LUNA-MATH-20260809-002",
                    "content_fingerprint": "c" * 64,
                    "luna_eligible": True,
                    "proposal_only": True,
                    "solution_evidence": {
                        "accepted_policy": "solution_text_or_solution_image",
                        "solution_text_present": True,
                        "solution_image_present": True,
                    },
                },
                {
                    "capture_id": "LUNA-MATH-20260809-003",
                    "content_fingerprint": "c" * 64,
                    "luna_eligible": True,
                    "proposal_only": True,
                    "solution_evidence": {
                        "accepted_policy": "solution_text_or_solution_image",
                        "solution_text_present": False,
                        "solution_image_present": True,
                    },
                },
            ],
        }

    def test_probe_is_candidate_bound_modern_and_content_addressed(self) -> None:
        output_root = self.root / "deployment"
        with mock.patch.object(
            builder,
            "validate_live_math_business_manifest",
            return_value=self._business_result(),
        ):
            result = builder.build_candidate_bound_spec(
                candidate_config_path=self.config_path,
                source_spec_path=self.source_spec_path,
                output_root=output_root,
                math_business_manifest_path=self.business_manifest,
                math_source_root=self.math_root,
            )
        self.assertEqual(result["release_id"], self.release_id)
        self.assertFalse(result["fingerprints_bound"])
        self.assertEqual(
            result["daily_business_task_count_by_subject"],
            {"math": 6, "cs408": 4, "english": 1},
        )
        self.assertEqual(result["english_legacy_historical_target_count"], 98)
        self.assertEqual(result["english_legacy_included_in_daily_task_count"], 0)
        self.assertEqual(
            result["thirty_business_task_gate"],
            "pending_insufficient_distinct_real_tasks",
        )
        spec_path = Path(result["spec_path"])
        self.assertEqual(hashlib.sha256(spec_path.read_bytes()).hexdigest(), result["spec_sha256"])
        self.assertEqual(spec_path.stat().st_mode & 0o777, 0o400)
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        self.assertEqual(spec["release_id"], self.release_id)
        self.assertEqual(
            {row["sample_role"] for row in spec["entries"] if row["subject"] == "math"},
            builder.EXPECTED_DAILY_ROLES["math"],
        )
        self.assertTrue(
            all("expected_input_fingerprint" not in row for row in spec["entries"])
        )
        by_role = {row["sample_role"]: row for row in spec["preflight_cases"]}
        self.assertEqual(set(by_role), builder.EXPECTED_PREFLIGHT_ROLES)
        self.assertEqual(
            by_role["solution_image_only"]["expected_outcome"], "eligible"
        )
        self.assertEqual(
            by_role["solution_text_only"]["fixture_kind"],
            "math_live_business_task",
        )
        self.assertEqual(
            by_role["missing_solution_evidence"]["expected_outcome"],
            "rejected",
        )
        self.assertEqual(
            spec["workload_boundary"]["excluded_historical_workloads"][0][
                "included_in_daily_task_count"
            ],
            0,
        )
        subject_specs = result["subject_specs"]
        math_spec = json.loads(Path(subject_specs["math"]["path"]).read_text(encoding="utf-8"))
        cs_spec = json.loads(Path(subject_specs["cs408"]["path"]).read_text(encoding="utf-8"))
        self.assertEqual(len(math_spec["entries"]), 3)
        self.assertEqual(len(cs_spec["entries"]), 4)
        self.assertTrue(all(row["subject"] == "math" for row in math_spec["entries"]))
        self.assertTrue(all(row["subject"] == "cs408" for row in cs_spec["entries"]))

    def test_final_binds_fingerprints_from_zero_model_inspection(self) -> None:
        decisions = []
        source = json.loads(self.source_spec_path.read_text(encoding="utf-8"))
        for entry in source["entries"]:
            if entry["subject"] == "english":
                continue
            decisions.append(
                {
                    "subject": entry["subject"],
                    "capture_id": entry["capture_id"],
                    "input_fingerprint": hashlib.sha256(
                        entry["capture_id"].encode("utf-8")
                    ).hexdigest(),
                }
            )
        decisions.append(
            {
                "subject": "english",
                "capture_id": "ENGLISH-MICROBATCH",
                "input_fingerprint": "e" * 64,
            }
        )
        inspection = self.root / "inspection.json"
        _write_json(
            inspection,
            {
                "status": "ready",
                "release_id": self.release_id,
                "decisions": decisions,
                "model_call_count": 0,
                "formal_write_count": 0,
            },
        )
        with mock.patch.object(
            builder,
            "validate_live_math_business_manifest",
            return_value=self._business_result(),
        ):
            result = builder.build_candidate_bound_spec(
                candidate_config_path=self.config_path,
                source_spec_path=self.source_spec_path,
                output_root=self.root / "deployment",
                math_business_manifest_path=self.business_manifest,
                math_source_root=self.math_root,
                inspection_path=inspection,
            )
        self.assertTrue(result["fingerprints_bound"])
        spec = json.loads(Path(result["spec_path"]).read_text(encoding="utf-8"))
        english = [row for row in spec["entries"] if row["subject"] == "english"]
        self.assertEqual(
            {row["expected_input_fingerprint"] for row in english},
            {"e" * 64},
        )
        self.assertTrue(
            all(
                builder.SHA256_RE.fullmatch(row["expected_input_fingerprint"])
                for row in spec["entries"]
            )
        )

    def test_live_math_business_manifest_is_bound_outside_subject_spec(self) -> None:
        with mock.patch.object(
            builder,
            "validate_live_math_business_manifest",
            return_value=self._business_result(),
        ):
            first = builder.build_candidate_bound_spec(
                candidate_config_path=self.config_path,
                source_spec_path=self.source_spec_path,
                output_root=self.root / "first-deployment",
                math_business_manifest_path=self.business_manifest,
                math_source_root=self.math_root,
            )
        successor_release_id = "b" * 64
        successor = self.root / "releases" / successor_release_id
        successor.mkdir(parents=True)
        successor_release = successor / "release.json"
        _write_json(successor_release, {"release_id": successor_release_id})
        successor_config = successor / "config.json"
        _write_json(
            successor_config,
            {"release": {"manifest_path": str(successor_release)}},
        )
        with mock.patch.object(
            builder,
            "validate_live_math_business_manifest",
            return_value=self._business_result(),
        ):
            second = builder.build_candidate_bound_spec(
                candidate_config_path=successor_config,
                source_spec_path=Path(first["spec_path"]),
                output_root=self.root / "successor-deployment",
                math_business_manifest_path=self.business_manifest,
                math_source_root=self.math_root,
            )
        self.assertEqual(second["release_id"], successor_release_id)
        self.assertEqual(second["math_business_binding"]["task_count"], 3)
        math_spec = json.loads(
            Path(second["subject_specs"]["math"]["path"]).read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(len(math_spec["entries"]), 3)
        self.assertTrue(
            all(row.get("fixture_kind") != "math_live_business_task" for row in math_spec["entries"])
        )

    def test_invalid_math_daily_role_set_is_rejected(self) -> None:
        source = json.loads(self.source_spec_path.read_text(encoding="utf-8"))
        source["entries"] = [
            row for row in source["entries"] if row.get("sample_role") != "GS-240"
        ]
        invalid = self.root / "math-role-missing.json"
        _write_json(invalid, source)
        with self.assertRaisesRegex(
            builder.CandidateBoundSpecError,
            "candidate_spec_daily_role_set_mismatch",
        ):
            builder.build_candidate_bound_spec(
                candidate_config_path=self.config_path,
                source_spec_path=invalid,
                output_root=self.root / "deployment",
                math_business_manifest_path=self.business_manifest,
                math_source_root=self.math_root,
            )

    def test_historical_english_target_cannot_enter_daily_entries(self) -> None:
        source = json.loads(self.source_spec_path.read_text(encoding="utf-8"))
        source = copy.deepcopy(source)
        source["entries"][0]["capture_id"] = "EN-P0-006-HISTORICAL-001"
        invalid = self.root / "invalid-source.json"
        _write_json(invalid, source)
        with self.assertRaisesRegex(
            builder.CandidateBoundSpecError,
            "candidate_spec_entries_invalid",
        ):
            builder.build_candidate_bound_spec(
                candidate_config_path=self.config_path,
                source_spec_path=invalid,
                output_root=self.root / "deployment",
                math_business_manifest_path=self.business_manifest,
                math_source_root=self.math_root,
            )


if __name__ == "__main__":
    unittest.main()
