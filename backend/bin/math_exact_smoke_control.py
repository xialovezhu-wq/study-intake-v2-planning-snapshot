#!/usr/bin/env python3
"""Exact control surface for the authorized 2026-08-13 math smoke."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from concurrent_dispatch import DispatchError, LeaseStore  # noqa: E402
from core_dispatch_bridge import (  # noqa: E402
    producer_authority_binding,
    validate_fixed_model_contract,
)
from math_exact_smoke import (  # noqa: E402
    EXACT_ORDER,
    MathExactSmokeError,
    apply_exact_smoke_capture,
    authorize_exact_smoke,
    build_authorization_descriptor,
    preview_exact_smoke,
    reopen_execution_authorization,
)
from preprocessor_core import (  # noqa: E402
    PreprocessorError,
    load_config,
    release_identity,
)
from processing_plugin import (  # noqa: E402
    ProcessingPluginError,
    ProcessingPluginHost,
)


def _add_descriptor_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--target-release-id", required=True)
    parser.add_argument("--activation-id", required=True)
    parser.add_argument("--authority-generation", required=True)
    parser.add_argument("--authority-fingerprint", required=True)
    parser.add_argument("--producer-authority-fingerprint", required=True)


def _descriptor_from_args(args: argparse.Namespace) -> dict[str, Any]:
    return build_authorization_descriptor(
        target_release_id=args.target_release_id,
        activation_id=args.activation_id,
        authority_generation=args.authority_generation,
        authority_fingerprint=args.authority_fingerprint,
        producer_authority_fingerprint=(
            args.producer_authority_fingerprint
        ),
    )


def _load_live_control(
    config_path: Path,
) -> tuple[
    dict[str, Any],
    Path,
    str,
    Mapping[str, Any],
    Mapping[str, Any],
    ProcessingPluginHost,
]:
    config = load_config(config_path.expanduser().resolve())
    validate_fixed_model_contract(config)
    runtime_root = Path(str(config["runtime_root"])).resolve()
    release_id, _provenance = release_identity(config)
    store = LeaseStore(runtime_root)
    state = store.production_canary_status_read_only(
        "math", expected_release_id=release_id
    )
    if (
        not isinstance(state, Mapping)
        or state.get("state")
        not in {"armed", "continuous_concurrent_unlocked"}
        or state.get("luna_consumer_enabled") is not True
        or state.get("sol_enabled") is not False
        or state.get("formal_write_count") != 0
    ):
        raise MathExactSmokeError("math_smoke_canary_state_invalid")
    producer = producer_authority_binding(config, "math", release_id)
    if (
        state.get("producer_authority_fingerprint")
        != producer.get("authority_fingerprint")
    ):
        raise MathExactSmokeError("math_smoke_producer_authority_drift")
    plugin = config.get("processing_plugin")
    if not isinstance(plugin, Mapping):
        raise MathExactSmokeError("math_smoke_authority_host_missing")
    raw_subject_roots = config.get("subject_repo_roots")
    subject_roots = (
        {
            name: str(path)
            for name, path in raw_subject_roots.items()
            if name in {"math", "cs408", "english"}
            and isinstance(path, str)
            and Path(path).is_dir()
        }
        if isinstance(raw_subject_roots, Mapping)
        else {
            name: str(config["adapters"][name]["repo_root"])
            for name in ("math", "cs408", "english")
            if isinstance(config.get("adapters"), Mapping)
            and isinstance(config["adapters"].get(name), Mapping)
            and isinstance(config["adapters"][name].get("repo_root"), str)
            and Path(str(config["adapters"][name]["repo_root"])).is_dir()
        }
    )
    host = ProcessingPluginHost(
        plugin,
        runtime_root=runtime_root,
        candidate_release_id=release_id,
        subject_roots=subject_roots,
        require_authority_snapshot=True,
    )
    return config, runtime_root, release_id, state, producer, host


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Exact five-sample math smoke control",
        allow_abbrev=False,
    )
    commands = parser.add_subparsers(dest="command", required=True)
    descriptor = commands.add_parser("descriptor")
    _add_descriptor_arguments(descriptor)
    preview = commands.add_parser("preview")
    _add_descriptor_arguments(preview)
    reopen = commands.add_parser("preview-authorized")
    reopen.add_argument(
        "--authorization-receipt", required=True, type=Path
    )
    reopen.add_argument("--runtime-root", required=True, type=Path)
    authorize = commands.add_parser("authorize")
    authorize.add_argument("--config", required=True, type=Path)
    authorize.add_argument("--authorized-at")
    apply = commands.add_parser("apply")
    apply.add_argument("--config", required=True, type=Path)
    apply.add_argument(
        "--authorization-receipt", required=True, type=Path
    )
    apply.add_argument("--formal-id", required=True, choices=EXACT_ORDER)
    return parser


def _execute(args: argparse.Namespace) -> dict[str, Any]:
    if args.command in {"descriptor", "preview"}:
        descriptor = _descriptor_from_args(args)
        return (
            descriptor
            if args.command == "descriptor"
            else preview_exact_smoke(descriptor)
        )
    if args.command == "preview-authorized":
        digest, authorization = reopen_execution_authorization(
            args.authorization_receipt,
            runtime_root=args.runtime_root.expanduser().resolve(),
        )
        descriptor = authorization["authorization_descriptor"]
        if not isinstance(descriptor, Mapping):
            raise MathExactSmokeError(
                "math_smoke_authorization_receipt_invalid"
            )
        preview = preview_exact_smoke(descriptor)
        return {
            **preview,
            "execution_authorization_sha256": digest,
            "execution_authorization_path": str(
                args.authorization_receipt.expanduser().resolve()
            ),
        }
    (
        config,
        runtime_root,
        release_id,
        state,
        producer,
        host,
    ) = _load_live_control(args.config)
    math_repo_root = Path(str(config["adapters"]["math"]["repo_root"]))
    if args.command == "authorize":
        snapshot = host.subject_authority_snapshot("math")
        descriptor = build_authorization_descriptor(
            target_release_id=release_id,
            activation_id=str(state["activation_id"]),
            authority_generation=str(snapshot["generation"]),
            authority_fingerprint=str(snapshot["authority_fingerprint"]),
            producer_authority_fingerprint=str(
                producer["authority_fingerprint"]
            ),
        )
        digest, path, receipt = authorize_exact_smoke(
            descriptor,
            runtime_root=runtime_root,
            authority_snapshot=snapshot,
            authorized_at=args.authorized_at,
            math_repo_root=math_repo_root,
        )
        return {
            "status": "authorized",
            "execution_authorization_sha256": digest,
            "execution_authorization_path": str(path.resolve()),
            "target_release_id": release_id,
            "activation_id": state["activation_id"],
            "authority_generation": snapshot["generation"],
            "authority_fingerprint": snapshot["authority_fingerprint"],
            "authority_snapshot_count": receipt[
                "authority_snapshot_count"
            ],
            "authority_snapshot_mcp_tool_call_count": receipt[
                "authority_snapshot_mcp_tool_call_count"
            ],
            "model_mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
    digest, path, receipt = apply_exact_smoke_capture(
        args.authorization_receipt,
        formal_id=args.formal_id,
        runtime_root=runtime_root,
        authority_reader=lambda: host.subject_authority_snapshot("math"),
        math_repo_root=math_repo_root,
    )
    return {
        "status": "capture_applied",
        "formal_id": args.formal_id,
        "capture_apply_receipt_sha256": digest,
        "capture_apply_receipt_path": str(path.resolve()),
        "capture_event_id": receipt["capture_event_id"],
        "capture_event_sha256": receipt["capture_event_sha256"],
        "capture_idempotency_key": receipt["capture_idempotency_key"],
        "stage_manifest_sha256": receipt["stage_manifest_sha256"],
        "authority_snapshot_count": receipt["authority_snapshot_count"],
        "authority_snapshot_mcp_tool_call_count": receipt[
            "authority_snapshot_mcp_tool_call_count"
        ],
        "queue_write_count": 0,
        "model_mcp_tool_call_count": 0,
        "model_call_count": 0,
        "provider_request_count": 0,
        "formal_write_count": 0,
        "sol_enabled": False,
    }


def main() -> int:
    args = build_parser().parse_args()
    try:
        result = _execute(args)
    except (
        DispatchError,
        MathExactSmokeError,
        PreprocessorError,
        ProcessingPluginError,
    ) as exc:
        code = getattr(exc, "code", str(exc))
        print(
            json.dumps(
                {"status": "invalid", "error_code": code},
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
