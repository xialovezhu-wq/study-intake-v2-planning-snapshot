#!/usr/bin/env python3

from __future__ import annotations

import hashlib
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import preprocessor_core as core  # noqa: E402


class SubjectPublicationReopenTests(unittest.TestCase):
    def _write_json(self, path: Path, value: dict) -> str:
        raw = core.json_file_bytes(value)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return hashlib.sha256(raw).hexdigest()

    def _tree(self, root: Path) -> dict[str, str]:
        return {
            path.relative_to(root).as_posix(): core.sha256_file(path)
            for path in sorted(root.rglob("*"))
            if path.is_file()
        }

    def _fixture(self, root: Path, subject: str) -> tuple[dict, str, str]:
        capture_id = f"CAP-{subject.upper()}-001"
        study_date = "2026-08-09"
        input_fingerprint = {
            "math": "1" * 64,
            "cs408": "2" * 64,
            "english": "3" * 64,
        }[subject]
        report_id = "SIP-REPORT-" + "A" * 24
        draft = {"claims": [{"value": 1}]}
        critical = {
            "verdict": "confirmed",
        } if subject == "english" else {
            "verdict": "pass_with_warnings",
            "revised_analysis": {"claims": [{"value": 1}]},
        }
        stage_receipts: dict[str, dict] = {}
        for index, (stage, payload) in enumerate(
            (("analysis", draft), ("critical_review", critical)), start=1
        ):
            result_sha256 = core.sha256_value(payload)
            receipt = {
                "status": "ready",
                "result_sha256": result_sha256,
                "output_sha256": str(index + 3) * 64,
                "mcp_transcript_sha256": str(index + 5) * 64,
            }
            if subject != "english":
                artifact = {
                    "schema_version": "study-intake-luna-stage-artifact-v2",
                    "stage": stage,
                    "report_id": report_id,
                    "capture_id": capture_id,
                    "study_date": study_date,
                    "payload": payload,
                    "formal_write_count": 0,
                }
                artifact_path = root / "private" / "reports" / "stages"
                artifact_sha256 = self._write_json(
                    artifact_path / "pending.json", artifact
                )
                final_path = artifact_path / f"{artifact_sha256}.json"
                (artifact_path / "pending.json").rename(final_path)
                receipt.update(
                    {
                        "artifact_sha256": artifact_sha256,
                        "artifact_ref": (
                            "study-intake-stage://sha256/" + artifact_sha256
                        ),
                    }
                )
            stage_receipts[stage] = receipt
        stage_receipts["read_session"] = {}
        proposal = {
            "critical_review_outcome": "accepted",
            "review_status": "proposal_ready",
        }
        common = {
            "subject": subject,
            "capture_id": capture_id,
            "study_date": study_date,
            "input_fingerprint": input_fingerprint,
            "package_id": "SIP-PKG-" + "B" * 24,
            "processing_binding_sha256": "a" * 64,
            "capture_freeze_receipt_sha256": "b" * 64,
            "mcp_read_session_receipt_sha256": "c" * 64,
            "read_session_manifest_sha256": "d" * 64,
            "evidence_generation": "generation-1",
            "evidence_authority_fingerprint": "e" * 64,
            "stage_receipts": stage_receipts,
            "luna_proposal": proposal,
            "luna_proposal_sha256": core.sha256_value(proposal),
            "formal_write_count": 0,
        }
        if subject == "english":
            package = {
                **common,
                "schema_version": core.PACKAGE_SCHEMA,
                "pipeline_status": "two_pass_ready",
                "draft_analysis": draft,
                "critical_review": critical,
            }
            expected_status = "ready"
        else:
            report = {"subject": subject, "value": 1}
            report_root = root / "private" / "reports" / "objects"
            report_sha256 = self._write_json(
                report_root / "pending.json", report
            )
            (report_root / "pending.json").rename(
                report_root / f"{report_sha256}.json"
            )
            package = {
                **common,
                "schema_version": core.PACKAGE_SCHEMA_V2,
                "report_id": report_id,
                "pipeline_status": "two_pass_ready",
                "processing_contract_sha256": "f" * 64,
                "processing_fingerprint": "0" * 64,
                "report_json_sha256": report_sha256,
                "quality_receipt": {
                    "critical_review_outcome": "accepted",
                },
            }
            expected_status = "two_pass_ready"
        package_sha256 = hashlib.sha256(
            core.json_file_bytes(package)
        ).hexdigest()
        if subject == "english":
            package_path = (
                root / "packages" / subject / study_date
                / core.safe_component(capture_id)
                / f"{input_fingerprint}.json"
            )
        else:
            package_path = (
                root / "packages" / "objects" / f"{package_sha256}.json"
            )
        self._write_json(package_path, package)
        publication_id = core._publication_id(
            subject=subject,
            capture_id=capture_id,
            study_date=study_date,
            input_fingerprint=input_fingerprint,
            package_id=package["package_id"],
            package_sha256=package_sha256,
            pipeline_status=expected_status,
            processing_contract_sha256=(
                package.get("processing_contract_sha256")
                if subject != "english"
                else None
            ),
            processing_fingerprint=(
                package.get("processing_fingerprint")
                if subject != "english"
                else None
            ),
        )
        pointer = {
            "schema_version": (
                "study-intake-preprocess-latest-v1"
                if subject == "english"
                else "study-intake-preprocess-latest-v2"
            ),
            "subject": subject,
            "capture_id": capture_id,
            "study_date": study_date,
            "input_fingerprint": input_fingerprint,
            "authority_release_id": core.LOADED_CORE_SHA256,
            "package_id": package["package_id"],
            "package_path": str(package_path.resolve()),
            "package_sha256": package_sha256,
            "publication_id": publication_id,
        }
        job = {
            "schema_version": "study-intake-preprocess-job-v1",
            "subject": subject,
            "capture_id": capture_id,
            "study_date": study_date,
            "input_fingerprint": input_fingerprint,
            "package_id": package["package_id"],
            "package_path": str(package_path.resolve()),
            "package_sha256": package_sha256,
            "publication_id": publication_id,
            "status": expected_status,
            "formal_write_count": 0,
        }
        self._write_json(
            root / "state" / "latest" / subject
            / f"{core.safe_component(capture_id)}.json",
            pointer,
        )
        self._write_json(
            root / "state" / "jobs" / subject
            / f"{core.safe_component(capture_id)}.json",
            job,
        )
        return {"runtime_root": str(root)}, capture_id, input_fingerprint

    def test_three_subject_publications_reopen_without_writes_or_models(self) -> None:
        for subject in ("math", "cs408", "english"):
            with self.subTest(subject=subject), tempfile.TemporaryDirectory(
                prefix=f"subject-publication-{subject}-"
            ) as raw:
                root = Path(raw)
                config, capture_id, input_fingerprint = self._fixture(
                    root, subject
                )
                before = self._tree(root)
                with (
                    patch.object(
                        core,
                        "_processing_publication_host",
                        return_value=object(),
                    ),
                    patch.object(
                        core,
                        "validate_package_luna_proposal",
                        return_value={},
                    ) as package_closure,
                    patch.object(
                        core,
                        "validate_english_v1_package_closure",
                        return_value={},
                    ) as english_closure,
                ):
                    value = core.reopen_verified_subject_publication(
                        config,
                        subject=subject,
                        capture_id=capture_id,
                        study_date="2026-08-09",
                        input_fingerprint=input_fingerprint,
                    )
                self.assertEqual(self._tree(root), before)
                self.assertEqual(
                    value["schema_version"],
                    "verified_subject_publication_v1",
                )
                self.assertEqual(value["subject"], subject)
                self.assertEqual(value["critical_review_outcome"], "accepted")
                self.assertEqual(value["review_status"], "proposal_ready")
                self.assertEqual(value["formal_write_count"], 0)
                self.assertRegex(value["subject_package_sha256"], r"^[0-9a-f]{64}$")
                self.assertRegex(value["draft_analysis_sha256"], r"^[0-9a-f]{64}$")
                if subject == "english":
                    english_closure.assert_called_once()
                    package_closure.assert_not_called()
                    self.assertEqual(
                        value["stage_artifact_sha256s"],
                        {"analysis": None, "critical_review": None},
                    )
                else:
                    package_closure.assert_called_once()
                    english_closure.assert_not_called()
                    self.assertTrue(
                        all(value["stage_artifact_sha256s"].values())
                    )

    def test_release_drift_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory(
            prefix="subject-publication-release-drift-"
        ) as raw:
            root = Path(raw)
            config, capture_id, input_fingerprint = self._fixture(root, "math")
            pointer_path = (
                root / "state" / "latest" / "math"
                / f"{core.safe_component(capture_id)}.json"
            )
            pointer = core.load_json(pointer_path)
            pointer["authority_release_id"] = "9" * 64
            self._write_json(pointer_path, pointer)
            with self.assertRaisesRegex(
                core.PreprocessorError,
                "subject_publication_reopen_invalid",
            ):
                core.reopen_verified_subject_publication(
                    config,
                    subject="math",
                    capture_id=capture_id,
                    study_date="2026-08-09",
                    input_fingerprint=input_fingerprint,
                )


if __name__ == "__main__":
    unittest.main()
