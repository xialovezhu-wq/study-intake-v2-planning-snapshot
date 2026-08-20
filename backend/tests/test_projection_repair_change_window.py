from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "projection_repair_change_window",
    ROOT / "scripts" / "projection_repair_change_window.py",
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def digest(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


class ProjectionRepairChangeWindowTests(unittest.TestCase):
    def _synthetic_english_rollback_fixture(self, root: Path) -> tuple[argparse.Namespace, dict]:
        root = root.resolve()
        change_id = "CHANGE-ROLLBACK-0001"
        cs408_repo = root / "cs408"
        english_repo = root / "english"
        artifact_root = root / "artifacts"
        cs408_repo.mkdir()
        english_repo.mkdir()
        source_id = "RAW-TEST-001"
        study_date = "2026-08-09"
        targets = MODULE._targets("english", english_repo, source_id, study_date)
        originals = (b"old builder\n", b"old projection\n")
        posts = (b"new builder\n", b"new projection\n")
        change_root = artifact_root / "change-windows" / change_id
        target_rows = []
        post_hashes = {}
        for index, (path, original, post) in enumerate(zip(targets, originals, posts)):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(post)
            original_digest = digest(original)
            blob = MODULE._blob_path(change_root, original_digest)
            blob.parent.mkdir(parents=True, exist_ok=True)
            blob.write_bytes(original)
            relative = path.relative_to(english_repo).as_posix()
            target_rows.append(
                {
                    "relative_path": relative,
                    "role": "canonical_builder_source" if index == 0 else "rebuildable_projection",
                    "preimage": {
                        "path": str(blob),
                        "sha256": original_digest,
                        "byte_count": len(original),
                    },
                }
            )
            post_hashes[relative] = digest(post)
        stable_manifest = {
            "schema_version": "study-intake-projection-repair-recovery-manifest-v1",
            "change_id": change_id,
            "subject_scope": ["english"],
            "english_scope": {"source_id": source_id, "study_date": study_date},
            "subjects": {
                "english": {
                    "repo_root": str(english_repo.resolve()),
                    "targets": target_rows,
                    "canonical_input": {},
                    "formal_surface_baseline": {},
                }
            },
            "authorization_required_for_apply_and_rollback": True,
            "model_call_count": 0,
            "formal_write_count": 0,
        }
        manifest = {**stable_manifest, "created_at": "2026-08-09T00:00:00+00:00"}
        manifest["manifest_sha256"] = MODULE.sha256_bytes(
            MODULE.canonical_bytes(stable_manifest)
        )
        MODULE._atomic_json(change_root / "recovery-manifest.json", manifest)
        stable_receipt = {
            "schema_version": "study-intake-projection-repair-apply-receipt-v1",
            "status": "applied_verified",
            "change_id": change_id,
            "manifest_sha256": manifest["manifest_sha256"],
            "subjects": {
                "english": {
                    "post_target_sha256": post_hashes,
                    "formal_write_count": 0,
                }
            },
            "applied_at": "2026-08-09T00:01:00+00:00",
            "model_call_count": 0,
            "formal_write_count": 0,
        }
        receipt = dict(stable_receipt)
        receipt["receipt_sha256"] = MODULE.sha256_bytes(
            MODULE.canonical_bytes(stable_receipt)
        )
        MODULE._atomic_json(change_root / "apply-receipt.json", receipt)
        args = argparse.Namespace(
            subject="english",
            change_id=change_id,
            authorization=change_id,
            artifact_root=artifact_root,
            cs408_repo=cs408_repo,
            english_repo=english_repo,
            english_source_id=source_id,
            english_date=study_date,
        )
        return args, {path: original for path, original in zip(targets, originals)}

    def test_target_allowlist_is_fixed_per_subject(self) -> None:
        root = Path("/tmp/example")
        self.assertEqual(
            tuple(path.relative_to(root).as_posix() for path in MODULE._targets(
                "cs408", root, "RAW-1", "2026-08-09"
            )),
            tuple(path.as_posix() for path in MODULE.CS408_PROJECTION_RELS),
        )
        self.assertEqual(
            tuple(path.relative_to(root).as_posix() for path in MODULE._targets(
                "english", root, "RAW-1", "2026-08-09"
            )),
            (
                "english_pipeline/views.py",
                "intake/views/2026-08-09/RAW-1-quick-capture.md",
            ),
        )
        with self.assertRaisesRegex(MODULE.ChangeWindowError, "change_subject_invalid"):
            MODULE._targets("math", root, "RAW-1", "2026-08-09")

    def test_restore_uses_content_addressed_preimage_exactly(self) -> None:
        with tempfile.TemporaryDirectory(prefix="projection-window-restore-") as raw:
            root = Path(raw)
            repo = root / "repo"
            repo.mkdir()
            target = repo / "derived.json"
            original = b'{"version":1}\n'
            changed = b'{"version":2}\n'
            target.write_bytes(changed)
            blob = root / "backups" / digest(original)
            blob.parent.mkdir()
            blob.write_bytes(original)
            row = {
                "targets": [
                    {
                        "relative_path": "derived.json",
                        "preimage": {
                            "path": str(blob),
                            "sha256": digest(original),
                        },
                    }
                ]
            }
            result = MODULE._restore_subject_locked("english", repo, row)
            self.assertEqual(target.read_bytes(), original)
            self.assertEqual(result["formal_write_count"], 0)

    def test_restore_rejects_tampered_backup_blob(self) -> None:
        with tempfile.TemporaryDirectory(prefix="projection-window-tamper-") as raw:
            root = Path(raw)
            repo = root / "repo"
            repo.mkdir()
            target = repo / "derived.json"
            target.write_bytes(b"changed")
            blob = root / "backup"
            blob.write_bytes(b"tampered")
            row = {
                "targets": [
                    {
                        "relative_path": "derived.json",
                        "preimage": {
                            "path": str(blob),
                            "sha256": digest(b"original"),
                        },
                    }
                ]
            }
            with self.assertRaisesRegex(MODULE.ChangeWindowError, "recovery_blob_invalid"):
                MODULE._restore_subject_locked("english", repo, row)

    def test_apply_and_rollback_require_exact_change_authorization(self) -> None:
        args = argparse.Namespace(change_id="CHANGE-12345678", authorization="wrong")
        with self.assertRaisesRegex(MODULE.ChangeWindowError, "change_authorization_mismatch"):
            MODULE.apply_change(args)
        with self.assertRaisesRegex(MODULE.ChangeWindowError, "change_authorization_mismatch"):
            MODULE.rollback_change(args)

    def test_english_builder_candidate_requires_explicit_hash(self) -> None:
        with tempfile.TemporaryDirectory(prefix="projection-window-builder-") as raw:
            repo = Path(raw)
            builder = repo / MODULE.ENGLISH_BUILDER_REL
            builder.parent.mkdir(parents=True)
            builder.write_bytes(b"candidate")
            projection = repo / "projection.md"
            projection.write_bytes(b"old projection")
            row = {
                "targets": [
                    {
                        "relative_path": MODULE.ENGLISH_BUILDER_REL.as_posix(),
                        "preimage": {"sha256": digest(b"old builder")},
                    },
                    {
                        "relative_path": "projection.md",
                        "preimage": {"sha256": digest(b"old projection")},
                    },
                ]
            }
            with self.assertRaisesRegex(
                MODULE.ChangeWindowError, "english_builder_candidate_hash_mismatch"
            ):
                MODULE._verify_target_preimages(
                    "english", repo, row, english_builder_sha=digest(b"other")
                )
            MODULE._verify_target_preimages(
                "english", repo, row, english_builder_sha=digest(b"candidate")
            )

    def test_english_builder_candidate_is_staged_outside_live_repo(self) -> None:
        with tempfile.TemporaryDirectory(prefix="projection-window-stage-builder-") as raw:
            root = Path(raw).resolve()
            english_repo = root / "english"
            cs408_repo = root / "cs408"
            change_root = root / "artifacts" / "change"
            english_repo.mkdir()
            cs408_repo.mkdir()
            candidate = root / "candidate.py"
            content = b"def render():\n    return 'v2'\n"
            candidate.write_bytes(content)
            args = argparse.Namespace(
                english_builder_candidate=candidate,
                expected_english_builder_sha256=digest(content),
            )
            staged_content, staged = MODULE._stage_english_builder_candidate(
                args,
                change_root,
                repos={"cs408": cs408_repo, "english": english_repo},
            )
            self.assertEqual(staged_content, content)
            self.assertEqual(staged["sha256"], digest(content))
            self.assertEqual(Path(staged["path"]).read_bytes(), content)
            inside_live = english_repo / "candidate.py"
            inside_live.write_bytes(content)
            args.english_builder_candidate = inside_live
            with self.assertRaisesRegex(
                MODULE.ChangeWindowError, "english_builder_candidate_inside_live_repo"
            ):
                MODULE._stage_english_builder_candidate(
                    args,
                    change_root,
                    repos={"cs408": cs408_repo, "english": english_repo},
                )

    def test_rollback_command_restores_only_allowlisted_preimages(self) -> None:
        with tempfile.TemporaryDirectory(prefix="projection-window-full-rollback-") as raw:
            args, originals = self._synthetic_english_rollback_fixture(Path(raw))
            receipt = MODULE.rollback_change(args)
            self.assertEqual(receipt["status"], "rolled_back_verified")
            self.assertEqual(receipt["formal_write_count"], 0)
            for path, original in originals.items():
                self.assertEqual(path.read_bytes(), original)

    def test_rollback_rejects_tampered_manifest_before_write(self) -> None:
        with tempfile.TemporaryDirectory(prefix="projection-window-manifest-tamper-") as raw:
            args, originals = self._synthetic_english_rollback_fixture(Path(raw))
            manifest_path = (
                args.artifact_root
                / "change-windows"
                / args.change_id
                / "recovery-manifest.json"
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["subjects"]["english"]["repo_root"] = str(Path(raw) / "elsewhere")
            MODULE._atomic_json(manifest_path, manifest)
            with self.assertRaisesRegex(
                MODULE.ChangeWindowError, "recovery_manifest_integrity_invalid"
            ):
                MODULE.rollback_change(args)
            for path, original in originals.items():
                self.assertNotEqual(path.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
