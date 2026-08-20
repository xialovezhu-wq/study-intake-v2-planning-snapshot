#!/usr/bin/env python3
"""Run the candidate-bound three-subject direct-MCP smoke."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))
sys.path.insert(0, str(ROOT / "scripts"))

from controlled_replay_lane import load_exact_subject_tasks  # noqa: E402
from concurrent_dispatch import DispatchError  # noqa: E402
from mixed_luna_production import (  # noqa: E402
    ControlledReplaySubjectMixedAdapter,
    build_selection,
    close_adapters,
    load_math_new_business_task,
    prepare_adapters,
    production_runner,
    production_verifier,
)
from mixed_luna_stress import (  # noqa: E402
    MixedLunaStressError,
    RUN_COUNTS,
    run_mixed_stress,
)
from pre_model_p0_gate import (  # noqa: E402
    P0GateError,
    require_model_lane_ready,
)
from preprocess_dispatcher import ProductionDispatchRuntime, _stage_timeout  # noqa: E402
from preprocessor_core import (  # noqa: E402
    PreprocessorError,
    _processing_publication_host,
    load_config,
    sha256_value,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Candidate-bound three-subject mixed Luna stress",
        allow_abbrev=False,
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--english-controlled-spec", type=Path, required=True)
    parser.add_argument("--math-live-business-manifest", type=Path, required=True)
    parser.add_argument("--cs408-controlled-spec", type=Path, required=True)
    parser.add_argument("--campaign-id", required=True)
    parser.add_argument("--campaign-runtime-root", type=Path, required=True)
    parser.add_argument("--campaign-authority-key", type=Path, required=True)
    parser.add_argument(
        "--run-mode", choices=tuple(RUN_COUNTS), required=True
    )
    parser.add_argument("--execution-attempt", type=int, required=True)
    parser.add_argument("--max-workers", type=int)
    parser.add_argument("--barrier-timeout-seconds", type=float, default=60.0)
    parser.add_argument("--p0-matrix", type=Path, required=True)
    parser.add_argument("--p0-audit-root", type=Path, required=True)
    parser.add_argument("--golden-inventory", type=Path, required=True)
    parser.add_argument("--english-existing-inventory-receipt-root", type=Path)
    parser.add_argument("--english-existing-inventory-closure-sha256")
    parser.add_argument("--english-existing-inventory-authority-key", type=Path)
    return parser


def _workers(mode: str, raw: int | None) -> int:
    expected = sum(RUN_COUNTS[mode].values())
    value = expected if raw is None else raw
    if value != expected:
        raise MixedLunaStressError("mixed_production_worker_count_invalid")
    return value


def _bind_candidate_release(
    config: dict[str, Any], release_id: str
) -> dict[str, Any]:
    """Bind direct ProductionDispatchRuntime construction to its release.

    Normal Worker construction derives this field before creating CodexRunner.
    The mixed lane constructs the lower-level runtime directly, so it must add
    the same candidate binding after the immutable subject spec has proved the
    release identity.
    """

    existing = config.get("authority_release_id")
    if existing not in {None, release_id}:
        raise MixedLunaStressError("mixed_candidate_release_mismatch")
    bound = dict(config)
    bound["authority_release_id"] = release_id
    return bound


def _require_gate(args: argparse.Namespace) -> dict[str, Any]:
    try:
        evaluated = require_model_lane_ready(
            args.p0_matrix,
            args.p0_audit_root,
            args.golden_inventory,
            english_legacy_authorization_expansion_receipt_root=(
                args.english_existing_inventory_receipt_root
            ),
            english_legacy_authorization_expansion_closure_sha256=(
                args.english_existing_inventory_closure_sha256
            ),
            english_legacy_authorization_expansion_authority_key_path=(
                args.english_existing_inventory_authority_key
            ),
        )
    except P0GateError as exc:
        raise MixedLunaStressError("pre_model_p0_gate_blocked") from exc
    if evaluated.get("blocking_issue_ids") != []:
        raise MixedLunaStressError("mixed_model_gate_not_ready")
    return {
        "schema_version": "mixed_luna_stress_pre_model_gate_v1",
        "decision": "allow_three_subject_new_business_smoke_only",
        "english_lane": "candidate_bound_controlled_replay",
        "math_lane": "candidate_bound_new_business_capture",
        "cs408_lane": "candidate_bound_controlled_replay",
        "ordinary_english_allowed": True,
        "ordinary_golden_allowed": False,
        "global_model_lane_ready": True,
        "existing_inventory_closure_sha256": (
            args.english_existing_inventory_closure_sha256
        ),
        "matrix_sha256": evaluated["matrix_sha256"],
        "golden_inventory_sha256": evaluated["golden_inventory_sha256"],
        "model_call_count": 0,
        "formal_write_count": 0,
    }


def main() -> int:
    args = build_parser().parse_args()
    adapters = None
    try:
        workers = _workers(args.run_mode, args.max_workers)
        expected_attempt = {
            "smoke_3": 1,
            "smoke_20": 2,
            "full_108": 3,
        }[args.run_mode]
        if args.execution_attempt != expected_attempt:
            raise MixedLunaStressError("mixed_stage_attempt_invalid")
        gate = _require_gate(args)
        config = load_config(args.config)
        runtime_root = Path(str(config["runtime_root"])).resolve()
        processing_host = _processing_publication_host(
            config, runtime_root=runtime_root
        )
        english_config, english_release, english_scope, english_tasks, _english_decisions = (
            load_exact_subject_tasks(
                args.config,
                args.english_controlled_spec,
                subject="english",
                expected_count=1,
                expected_entry_count=3,
            )
        )
        cs_config, cs_release, cs_scope, cs_tasks, _cs_decisions = (
            load_exact_subject_tasks(
                args.config,
                args.cs408_controlled_spec,
                subject="cs408",
                expected_count=4,
            )
        )
        if english_release != cs_release:
            raise MixedLunaStressError("mixed_controlled_release_mismatch")
        if processing_host.candidate_release_id != english_release:
            raise MixedLunaStressError("mixed_candidate_release_mismatch")
        math_release = english_release
        math_config = _bind_candidate_release(config, math_release)
        english_config = _bind_candidate_release(english_config, english_release)
        cs_config = _bind_candidate_release(cs_config, cs_release)
        math_runtime = ProductionDispatchRuntime(
            math_config, "math", args.config
        )
        english_runtime = ProductionDispatchRuntime(
            english_config, "english", args.config
        )
        cs_runtime = ProductionDispatchRuntime(
            cs_config, "cs408", args.config
        )
        execution_runtime_sha256 = sha256_value(
            {"stage_runtime_root": str(runtime_root)}
        )
        math_execution_runtime_id = (
            f"MIXED-MATH-{args.execution_attempt}-"
            f"{execution_runtime_sha256[:24].upper()}"
        )
        math_scope, math_tasks, _math_live_candidates = load_math_new_business_task(
            config=math_config,
            release_id=math_release,
            live_manifest_path=args.math_live_business_manifest,
            runtime=math_runtime,
            execution_attempt=args.execution_attempt,
            execution_runtime_id=math_execution_runtime_id,
        )
        math = ControlledReplaySubjectMixedAdapter(
            subject="math",
            config=math_config,
            config_path=args.config,
            release_id=math_release,
            spec_sha256=math_scope,
            tasks=math_tasks,
            runtime=math_runtime,
            processing_host=math_runtime.processing_host,
            task_timeout_seconds=_stage_timeout(math_config, "math"),
        )
        english = ControlledReplaySubjectMixedAdapter(
            subject="english",
            config=english_config,
            config_path=args.config,
            release_id=english_release,
            spec_sha256=english_scope,
            tasks=english_tasks,
            runtime=english_runtime,
            processing_host=english_runtime.processing_host,
            task_timeout_seconds=_stage_timeout(english_config, "english"),
        )
        cs408 = ControlledReplaySubjectMixedAdapter(
            subject="cs408",
            config=cs_config,
            config_path=args.config,
            release_id=cs_release,
            spec_sha256=cs_scope,
            tasks=cs_tasks[:1],
            runtime=cs_runtime,
            processing_host=cs_runtime.processing_host,
            task_timeout_seconds=_stage_timeout(cs_config, "cs408"),
        )
        selection, adapters = build_selection(
            campaign_id=args.campaign_id,
            candidate_release_id=math_release,
            execution_runtime_sha256=execution_runtime_sha256,
            run_mode=args.run_mode,
            execution_attempt=args.execution_attempt,
            max_workers=workers,
            adapters=(english, math, cs408),
        )
        prepare_adapters(adapters)
        campaign_runtime_root = args.campaign_runtime_root.expanduser().resolve()
        campaign_authority_key = args.campaign_authority_key.expanduser().resolve()
        if (
            campaign_runtime_root == runtime_root
            or campaign_runtime_root.is_relative_to(runtime_root)
            or runtime_root.is_relative_to(campaign_runtime_root)
        ):
            raise MixedLunaStressError("mixed_campaign_execution_runtime_overlap")
        digest, path, summary = run_mixed_stress(
            selection,
            runtime_root=campaign_runtime_root,
            authority_key_path=campaign_authority_key,
            processing_host=processing_host,
            runner=production_runner(adapters),
            artifact_verifier=production_verifier(adapters),
            sol_state_reader=math_runtime.subject_sol.read_global,
            barrier_timeout_seconds=args.barrier_timeout_seconds,
        )
        output = {
            "status": summary["status"],
            "summary_sha256": digest,
            "summary_path": str(path),
            "run_mode": args.run_mode,
            "selected_task_count": summary["selected_task_count"],
            "global_peak_active": summary["global_peak_active"],
            "per_subject_peak_active": summary["per_subject_peak_active"],
            "model_call_count": summary["model_call_count"],
            "formal_write_count": 0,
            "sol_enabled": False,
            "pre_model_p0_gate": gate,
        }
        print(json.dumps(output, ensure_ascii=False, sort_keys=True))
        return 0 if summary["status"] == "passed" else 2
    except (MixedLunaStressError, PreprocessorError, DispatchError) as exc:
        print(
            json.dumps(
                {
                    "status": "failed_closed",
                    "error_code": getattr(exc, "code", str(exc)),
                    "model_call_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    finally:
        if adapters is not None:
            try:
                close_adapters(adapters)
            except MixedLunaStressError:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
