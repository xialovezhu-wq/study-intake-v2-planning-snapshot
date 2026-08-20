from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from lib.read_only_math_fixture import ReadOnlyMathFixtureError, validate_manifest


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


class ReadOnlyMathFixtureTests(unittest.TestCase):
    def _fixture(self, root: Path) -> Path:
        sample_dir = root / "items" / "HOLD-TEST-001"
        sample_dir.mkdir(parents=True)
        image = b"\x89PNG\r\n\x1a\nquestion-only"
        record = {
            "schema_version": "math-deferred-formal-intake-item-v1",
            "staging_id": "HOLD-TEST-001",
            "state": "deferred_only",
            "quick_intake_written": False,
            "processing_authorized": False,
            "formal_write_count": 0,
            "question_identity": {"formal_id": "GS-TEST"},
            "source_locator": "formal_card:GS-TEST",
            "user_answer_text": "private",
        }
        record_raw = json.dumps(record, sort_keys=True).encode("utf-8")
        (sample_dir / "question_01.png").write_bytes(image)
        (sample_dir / "record.json").write_bytes(record_raw)
        rows = [
            ("question_01.png", _sha(image)),
            ("record.json", _sha(record_raw)),
        ]
        fingerprint = hashlib.sha256(
            "".join(f"{digest}  ./{path}\n" for path, digest in rows).encode()
        ).hexdigest()
        manifest = {
            "schema_version": "math-read-only-zero-model-test-manifest-v1",
            "fixture_root": str(root),
            "frozen_at": "2026-08-09T00:00:00+08:00",
            "use_mode": "read_only_zero_model_preflight",
            "read_only_enforcement": "logical_contract_only",
            "sample_count": 1,
            "authorization": {
                "quick_intake_written": False,
                "formal_write_count": 0,
                "processing_authorized": False,
                "luna_authorized": False,
                "sol_authorized": False,
                "write_back_to_fixture": False,
            },
            "scope": {
                "subject": "kaoyan_math",
                "cross_subject_files_present": False,
                "learner_facing_answer_safe_surface": False,
                "question_images_are_answer_free": True,
                "private_post_attempt_records_may_contain_results_or_resolved_answers": True,
            },
            "samples": [
                {
                    "staging_id": "HOLD-TEST-001",
                    "absolute_path": str(sample_dir),
                    "independent_capture": True,
                    "duplicate_of": None,
                    "question_identity": {
                        "formal_id": "GS-TEST",
                        "source_locator": "formal_card:GS-TEST",
                    },
                    "content_fingerprint": {
                        "algorithm": "sha256_of_sorted_relative_path_and_file_sha256_lines",
                        "value": fingerprint,
                    },
                    "files": [
                        {"relative_path": path, "role": role, "sha256": digest}
                        for (path, digest), role in zip(
                            rows, ["question_image", "learning_record"]
                        )
                    ],
                    "evidence_presence": {
                        "learning_record": True,
                        "question_image": True,
                        "solution_image": False,
                        "user_work_image": False,
                        "real_dialogue": "minimal_user_confirmation_only",
                        "exact_assistant_transcript": False,
                    },
                    "expected_preflight": {
                        "full_capture_acceptance": "fail_closed",
                        "reason": "missing_solution_image_and_full_dialogue",
                        "usable_subtests": ["missing_evidence_fail_closed"],
                    },
                }
            ],
            "separation": {
                "future_writes_to_fixture_prohibited": True,
                "new_capture_root": str(root.parent / "new"),
            },
        }
        path = root / "read-only-test-manifest.json"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        return path

    def test_valid_fixture_is_expected_to_fail_closed_before_luna(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            result = validate_manifest(self._fixture(Path(raw)))
        self.assertEqual(result["status"], "passed_expected_fail_closed")
        self.assertEqual(result["distinct_capture_count"], 1)
        self.assertFalse(result["samples"][0]["luna_eligible"])
        self.assertEqual(result["model_call_count"], 0)
        self.assertEqual(result["formal_write_count"], 0)

    def test_hash_tamper_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            manifest = self._fixture(root)
            (root / "items" / "HOLD-TEST-001" / "record.json").write_text("{}")
            with self.assertRaisesRegex(
                ReadOnlyMathFixtureError, "math_fixture_file_hash_mismatch"
            ):
                validate_manifest(manifest)

    def test_authorization_or_extra_file_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            manifest = self._fixture(root)
            value = json.loads(manifest.read_text())
            value["authorization"]["luna_authorized"] = True
            manifest.write_text(json.dumps(value))
            with self.assertRaisesRegex(
                ReadOnlyMathFixtureError, "math_fixture_authorization_invalid"
            ):
                validate_manifest(manifest)

            second = root / "second"
            second.mkdir()
            manifest = self._fixture(second)
            (second / "items" / "HOLD-TEST-001" / "unexpected.txt").write_text("x")
            with self.assertRaisesRegex(
                ReadOnlyMathFixtureError, "math_fixture_file_set_mismatch"
            ):
                validate_manifest(manifest)


if __name__ == "__main__":
    unittest.main()
