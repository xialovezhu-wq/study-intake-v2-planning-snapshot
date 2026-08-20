#!/usr/bin/env python3
"""Run a frozen historical math manifest through the isolated Luna v2 shadow path.

This command never calls the consumer and never writes a formal math card.  It
reconstructs only evidence that was available at the freeze, requests the two
Luna stages, and publishes content-addressed packages below the shadow tree.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Iterable, Mapping


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

import math_shadow_backaudit as backaudit  # noqa: E402
from preprocessor_core import (  # noqa: E402
    Candidate,
    FileLock,
    LOADED_CORE_SHA256,
    MathAdapter,
    PreprocessorError,
    Worker,
    canonical_bytes,
    load_config,
    load_json,
    math_adapter_build_sha256,
    math_processing_contract,
    semantic_evidence_refs_math_v2,
    sha256_file,
    sha256_value,
)


DEFAULT_CONFIG = ROOT / "config.json"


def _resolve_repo_file(repo: Path, raw: str, read_guard: Any | None = None) -> Path:
    path = (repo / raw).resolve()
    try:
        path.relative_to(repo.resolve())
    except ValueError as exc:
        raise PreprocessorError("math_shadow_replay_path_escape") from exc
    if not path.is_file():
        raise PreprocessorError("math_shadow_replay_file_missing")
    if read_guard is not None:
        read_guard.assert_allowed(path)
    return path


def _historical_formal_card(item: Mapping[str, Any]) -> dict[str, Any] | None:
    replay = item.get("replay_input")
    target = replay.get("formal_target") if isinstance(replay, Mapping) else None
    historical = target.get("historical_card") if isinstance(target, Mapping) else None
    if not isinstance(historical, Mapping):
        raise PreprocessorError("math_shadow_replay_formal_binding_missing")
    if historical.get("status") != "verified_unchanged":
        return None
    content = historical.get("content")
    digest = historical.get("sha256")
    if (
        not isinstance(content, str)
        or not isinstance(digest, str)
        or hashlib.sha256(content.encode("utf-8")).hexdigest() != digest
    ):
        raise PreprocessorError("math_shadow_replay_formal_preimage_invalid")
    return {
        "formal_id": item["formal_id"],
        "relative_path": historical.get("path"),
        "sha256": digest,
        "captured_sha256": digest,
        "hash_matches_capture": True,
        "content": content,
    }


def _source_bundle(
    item: Mapping[str, Any], repo: Path, max_images: int, read_guard: Any | None = None
) -> tuple[dict[str, Any] | None, tuple[Path, ...]]:
    replay = item.get("replay_input")
    source = replay.get("source_bundle") if isinstance(replay, Mapping) else None
    if source is None:
        return None, ()
    if not isinstance(source, Mapping):
        raise PreprocessorError("math_shadow_replay_source_invalid")
    manifest_path = _resolve_repo_file(
        repo, str(source.get("manifest_path") or ""), read_guard
    )
    if sha256_file(manifest_path) != source.get("manifest_sha256"):
        raise PreprocessorError("math_shadow_replay_source_manifest_drift")
    artifacts: list[dict[str, Any]] = []
    images: list[Path] = []
    raw_artifacts = source.get("artifacts")
    if not isinstance(raw_artifacts, list):
        raise PreprocessorError("math_shadow_replay_source_artifacts_invalid")
    for index, row in enumerate(raw_artifacts):
        if not isinstance(row, Mapping):
            raise PreprocessorError("math_shadow_replay_source_artifact_invalid")
        path = _resolve_repo_file(repo, str(row.get("path") or ""), read_guard)
        if sha256_file(path) != row.get("sha256"):
            raise PreprocessorError("math_shadow_replay_source_artifact_drift")
        is_image = path.suffix.lower() in {".png", ".jpg", ".jpeg", ".webp"}
        provided = is_image and len(images) < max_images
        artifacts.append(
            {
                "role": row.get("role"),
                "media_type": row.get("media_type"),
                "relative_path": str(path.relative_to(repo)),
                "sha256": row.get("sha256"),
                "size": path.stat().st_size,
                "evidence_ref": f"source_bundle.artifacts[{index}]",
                "provided_to_model": provided,
            }
        )
        if provided:
            images.append(path)
    manifest = source.get("manifest")
    locator = manifest.get("source_locator") if isinstance(manifest, Mapping) else None
    return (
        {
            "manifest_path": str(manifest_path.relative_to(repo)),
            "manifest_hash": source.get("manifest_sha256"),
            "source_locator": locator,
            "artifacts": artifacts,
        },
        tuple(images),
    )


def _augmented_capture(item: Mapping[str, Any]) -> dict[str, Any]:
    replay = item.get("replay_input")
    raw = replay.get("capture_event") if isinstance(replay, Mapping) else None
    snapshot = item.get("capture_snapshot")
    if not isinstance(raw, Mapping) or not isinstance(snapshot, Mapping):
        raise PreprocessorError("math_shadow_replay_capture_missing")
    capture = copy.deepcopy(dict(raw))
    target = raw.get("target") if isinstance(raw.get("target"), Mapping) else {}
    capture.update(
        {
            "formal_id": item.get("formal_id"),
            "source_locator": (
                (raw.get("source_bundle") or {}).get("source_locator")
                if isinstance(raw.get("source_bundle"), Mapping)
                else target.get("source_locator")
            ),
            "source_hash_before": target.get("source_hash_before"),
            "original_content_hash": snapshot.get("capture_content_hash"),
            "effective_evidence_hash": snapshot.get("effective_evidence_hash"),
            "effective_target_hash": snapshot.get("effective_target_hash"),
            "amendment_event_ids": snapshot.get("amendment_event_ids") or [],
            "amendment_count": len(snapshot.get("amendment_event_ids") or []),
            "identity_state": target.get("identity_state"),
            "active_freeze_ids": [],
        }
    )
    return capture


def _manifest_only_knowledge_snapshot(
    manifest: Mapping[str, Any], item: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Represent the missing historical network without consulting current data."""

    source_core = {
        "backaudit_manifest_sha256": manifest["manifest_sha256"],
        "replay_input_sha256": item["replay_input_sha256"],
        "historical_network_status": "not_frozen_in_manifest",
    }
    source_set_sha256 = sha256_value(source_core)
    coverage_manifest = {
        "complete": True,
        "scope": "frozen_manifest_inputs_only",
        "network_coverage_complete": False,
        "retrieval_status": "historical_network_unavailable",
        "unavailable_sources": ["historical_math_knowledge_network"],
        "post_freeze_current_network_excluded": True,
        "source_set_sha256": source_set_sha256,
    }
    snapshot_core = {
        "schema_version": "study-intake-math-historical-manifest-snapshot-v1",
        "source_set_sha256": source_set_sha256,
        "source_fingerprints": {},
        "current_card": {},
        "taxonomy_terms": [],
        "current_graph_neighborhood": {"nodes": [], "edges": []},
        "coverage_manifest": coverage_manifest,
        "formal_write_count": 0,
    }
    snapshot = {**snapshot_core, "snapshot_sha256": sha256_value(snapshot_core)}
    private = {
        "repo_root": None,
        "cards": [],
        "policy": {
            "broad_knowledge": [],
            "generic_error_causes": [],
            "generic_methods": [],
            "generic_traps": [],
            "broad_question_types": [],
            "non_evidence_markers": [],
            "execution_policy": {
                "current_mode": "SHADOW",
                "formal_write_in_shadow": False,
            },
        },
        "source_fingerprints": {},
        "source_set_sha256": source_set_sha256,
        "max_relationship_candidates": 5,
        "all_truth_terms": [],
        "historical_network_status": "not_frozen_in_manifest",
    }
    return snapshot, private


def build_replay_candidate(
    *,
    manifest: Mapping[str, Any],
    item: Mapping[str, Any],
    config: Mapping[str, Any],
    read_guard: Any | None = None,
) -> Candidate:
    """Build one manifest-bound candidate without consulting current formal text."""

    backaudit.verify_manifest(manifest)
    repo = Path(str(config["adapters"]["math"]["repo_root"])).resolve()
    profile = config.get("math_deep_v2")
    if not isinstance(profile, Mapping) or profile.get("mode") != "shadow":
        raise PreprocessorError("math_shadow_replay_mode_required")
    contract = math_processing_contract(config)
    if not isinstance(contract, Mapping):
        raise PreprocessorError("math_shadow_replay_contract_missing")
    capture = _augmented_capture(item)
    source_bundle, images = _source_bundle(
        item, repo, int(config["model"].get("max_images", 4)), read_guard
    )
    formal_card = _historical_formal_card(item)
    target_identity = MathAdapter._target_identity(capture)
    capsule = MathAdapter._target_group_capsule(capture)
    group_payload = {
        "target_group_key": target_identity["target_group_key"],
        "ordered_capture_ids": [item["capture_id"]],
        "capture_set_sha256": sha256_value(
            [
                {
                    "capture_id": item["capture_id"],
                    "effective_evidence_hash": capture.get("effective_evidence_hash"),
                    "effective_target_hash": capture.get("effective_target_hash"),
                }
            ]
        ),
        "captures": [capsule],
    }
    target_group = {
        **group_payload,
        "target_group_sha256": sha256_value(group_payload),
    }
    limitations = list(item.get("replay_limitations") or [])
    knowledge_snapshot, private_context = _manifest_only_knowledge_snapshot(
        manifest, item
    )
    if "historical_math_knowledge_network_unavailable" not in limitations:
        limitations.append("historical_math_knowledge_network_unavailable")
    model_input = {
        "capture": capture,
        "formal_card": formal_card,
        "source_bundle": source_bundle,
        "target_identity": target_identity,
        "target_group": target_group,
        "knowledge_distribution_snapshot": knowledge_snapshot,
        "image_evidence_refs": [
            row["evidence_ref"]
            for row in (source_bundle or {}).get("artifacts", [])
            if row.get("provided_to_model") is True
        ],
        "historical_replay": {
            "manifest_sha256": manifest["manifest_sha256"],
            "replay_input_sha256": item["replay_input_sha256"],
            "capture_event_sha256": item["capture_event_sha256"],
            "replay_status": item["replay_status"],
            "limitations": limitations,
            "post_freeze_current_mapping_excluded": True,
        },
    }
    image_refs = model_input["image_evidence_refs"]
    evidence_bundle_sha256 = sha256_value(model_input)
    evidence_manifest_sha256 = sha256_value(
        {
            "capture_original_content_hash": capture.get("original_content_hash"),
            "formal_card_hash": (
                item.get("replay_input", {})
                .get("formal_target", {})
                .get("historical_card", {})
                .get("sha256")
            ),
            "source_manifest_hash": (
                source_bundle.get("manifest_hash") if source_bundle else None
            ),
            "knowledge_snapshot_sha256": knowledge_snapshot["snapshot_sha256"],
            "knowledge_source_set_sha256": knowledge_snapshot["source_set_sha256"],
            "image_evidence_refs": image_refs,
            "historical_replay_manifest_sha256": manifest["manifest_sha256"],
            "historical_replay_input_sha256": item["replay_input_sha256"],
        }
    )
    snapshot = item["capture_snapshot"]
    historical = item["replay_input"]["formal_target"]["historical_card"]
    binding = {
        "adapter_version": config["adapters"]["math"]["adapter_version"],
        "original_content_hash": snapshot.get("capture_content_hash"),
        "effective_evidence_hash": snapshot.get("effective_evidence_hash"),
        "effective_target_hash": snapshot.get("effective_target_hash"),
        "amendment_event_ids": snapshot.get("amendment_event_ids") or [],
        "source_manifest_hash": (
            source_bundle.get("manifest_hash") if source_bundle else None
        ),
        "formal_card_hash": historical.get("sha256"),
        "capture_event_sha256": item["capture_event_sha256"],
        "replay_input_sha256": item["replay_input_sha256"],
        "backaudit_manifest_sha256": manifest["manifest_sha256"],
        "adapter_build_sha256": math_adapter_build_sha256(),
        "loaded_core_sha256": LOADED_CORE_SHA256,
        "processing_contract_sha256": contract["processing_contract_sha256"],
        "target_identity": target_identity,
        "target_group_sha256": target_group["target_group_sha256"],
        "capture_set_sha256": target_group["capture_set_sha256"],
        "evidence_manifest_sha256": evidence_manifest_sha256,
        "evidence_bundle_sha256": evidence_bundle_sha256,
        "knowledge_snapshot_sha256": knowledge_snapshot["snapshot_sha256"],
        "knowledge_source_set_sha256": knowledge_snapshot["source_set_sha256"],
        "image_evidence_refs": image_refs,
    }
    semantic_binding = dict(binding)
    semantic_binding.pop("loaded_core_sha256", None)
    return Candidate(
        subject="math",
        capture_id=str(item["capture_id"]),
        study_date=str(manifest["study_date"]),
        recorded_at=capture.get("recorded_at"),
        input_fingerprint=sha256_value(semantic_binding),
        input_binding=binding,
        model_input=model_input,
        allowed_evidence_refs=semantic_evidence_refs_math_v2(model_input),
        image_paths=images,
        target_label=str(target_identity["target_group_key"])[:160],
        canonical_state="historical_shadow_replay",
        sol_state="evaluation_only",
        private_context=private_context,
    )


def _tree_hashes(root: Path, relatives: Iterable[str]) -> dict[str, str]:
    values: dict[str, str] = {}
    for relative in relatives:
        path = root / relative
        if path.is_file():
            values[relative] = sha256_file(path)
        elif path.is_dir():
            for child in sorted(path.rglob("*")):
                if child.is_file():
                    values[str(child.relative_to(root))] = sha256_file(child)
    return values


def _protected_runtime_hashes(root: Path) -> dict[str, str]:
    return _tree_hashes(
        root,
        (
            "state/jobs/math",
            "state/latest/math",
            "state/offers",
            "state/adoptions/math",
            "packages/math",
            "receipts/math",
        ),
    )


def _formal_hashes(manifest: Mapping[str, Any], repo: Path) -> dict[str, str]:
    paths = {"错题知识网络/错题卡"} | {
        str(item["current_formal_target"]["path"])
        for item in manifest.get("items", [])
        if isinstance(item, Mapping)
        and isinstance(item.get("current_formal_target"), Mapping)
        and isinstance(item["current_formal_target"].get("path"), str)
    }
    ledger = manifest.get("selection_contract", {}).get("ledger_path")
    if isinstance(ledger, str):
        paths.add(ledger)
    return _tree_hashes(repo, sorted(paths))


def _selected_items(
    manifest: Mapping[str, Any], capture_ids: list[str]
) -> list[Mapping[str, Any]]:
    items = [item for item in manifest.get("items", []) if isinstance(item, Mapping)]
    if not capture_ids:
        return items
    wanted = set(capture_ids)
    selected = [item for item in items if item.get("capture_id") in wanted]
    if {item.get("capture_id") for item in selected} != wanted:
        raise PreprocessorError("math_shadow_replay_capture_not_in_manifest")
    return selected


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        description="Run frozen math evidence through isolated two-pass Luna shadow"
    )
    value.add_argument("--config", default=str(DEFAULT_CONFIG))
    value.add_argument("--manifest", required=True)
    value.add_argument("--capture-id", action="append", default=[])
    value.add_argument("--execute", action="store_true")
    value.add_argument("--continue-on-error", action="store_true")
    return value


def main() -> int:
    os.umask(0o077)
    args = parser().parse_args()
    try:
        config = load_config(Path(args.config).expanduser().resolve())
        manifest = load_json(Path(args.manifest).expanduser().resolve())
        backaudit.verify_manifest(manifest)
        items = _selected_items(manifest, args.capture_id)
        candidates = [
            build_replay_candidate(manifest=manifest, item=item, config=config)
            for item in items
        ]
        inspection = {
            "schema_version": "study-intake-math-shadow-replay-plan-v1",
            "study_date": manifest["study_date"],
            "manifest_sha256": manifest["manifest_sha256"],
            "candidate_count": len(candidates),
            "capture_ids": [candidate.capture_id for candidate in candidates],
            "requested_model": config["model"]["model"],
            "requested_reasoning_effort": config["model"]["reasoning_effort"],
            "mode": config["math_deep_v2"]["mode"],
            "execute": bool(args.execute),
            "formal_write_count": 0,
        }
        if not args.execute:
            print(json.dumps(inspection, ensure_ascii=False, sort_keys=True))
            return 0
        root = Path(str(config["runtime_root"])).resolve()
        repo = Path(str(config["adapters"]["math"]["repo_root"])).resolve()
        before_runtime = _protected_runtime_hashes(root)
        before_formal = _formal_hashes(manifest, repo)
        results: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        lock_path = Path(str(config["worker"]["lock_path"])).resolve()
        with FileLock(lock_path):
            worker = Worker(config)
            for candidate in candidates:
                try:
                    pointer = worker.publish_math_shadow_candidate(candidate)
                    results.append(
                        {
                            "capture_id": candidate.capture_id,
                            "status": "two_pass_ready",
                            "package_sha256": pointer["package_sha256"],
                            "pipeline_status": pointer["pipeline_status"],
                            "formal_write_count": 0,
                        }
                    )
                except PreprocessorError as exc:
                    failures.append(
                        {
                            "capture_id": candidate.capture_id,
                            "status": "failed",
                            "error_code": exc.code,
                            "formal_write_count": 0,
                        }
                    )
                    if not args.continue_on_error:
                        break
        after_runtime = _protected_runtime_hashes(root)
        after_formal = _formal_hashes(manifest, repo)
        if before_runtime != after_runtime:
            raise PreprocessorError("math_shadow_replay_mutated_production_runtime")
        if before_formal != after_formal:
            raise PreprocessorError("math_shadow_replay_mutated_formal_evidence")
        output = {
            **inspection,
            "status": "complete" if not failures else "partial_failed",
            "results": results,
            "failures": failures,
            "production_runtime_unchanged": True,
            "formal_evidence_unchanged": True,
            "formal_write_count": 0,
        }
        print(json.dumps(output, ensure_ascii=False, sort_keys=True))
        return 0 if not failures else 1
    except (OSError, backaudit.AuditError, PreprocessorError) as exc:
        code = exc.code if hasattr(exc, "code") else "math_shadow_replay_io_failed"
        print(
            json.dumps(
                {
                    "schema_version": "study-intake-math-shadow-replay-error-v1",
                    "status": "error",
                    "error_code": code,
                    "formal_write_count": 0,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
