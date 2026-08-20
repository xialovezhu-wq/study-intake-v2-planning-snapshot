#!/usr/bin/env python3
"""Capture or verify explicit read-only inputs for historical release tests."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from historical_test_input import (  # noqa: E402
    HistoricalTestInput,
    HistoricalTestInputError,
    capture_manifest,
    persist_content_addressed,
)


def _replay_binding(raw: str) -> tuple[str, str]:
    capture_id, marker, digest = raw.partition("=")
    if not marker or not capture_id or not digest:
        raise argparse.ArgumentTypeError("expected CAPTURE_ID=SHA256")
    return capture_id, digest


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    sub = root.add_subparsers(dest="command", required=True)
    capture = sub.add_parser("capture")
    capture.add_argument("--historical-manifest-root", type=Path, required=True)
    capture.add_argument("--runtime-data-root", type=Path, required=True)
    capture.add_argument("--formal-surface-root", type=Path, required=True)
    capture.add_argument("--historical-manifest", type=Path, required=True)
    capture.add_argument("--expected-manifest-sha256", required=True)
    capture.add_argument("--expected-manifest-content-sha256", required=True)
    capture.add_argument(
        "--expected-replay-input-sha256",
        action="append",
        type=_replay_binding,
        required=True,
    )
    capture.add_argument(
        "--source-root",
        type=Path,
        default=ROOT,
        help="successor source tree containing plugin/components.json and schemas",
    )
    capture.add_argument("--components-path", type=Path)
    capture.add_argument("--output-root", type=Path, required=True)
    verify = sub.add_parser("verify")
    verify.add_argument("--manifest", type=Path, required=True)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        if args.command == "capture":
            replay = dict(args.expected_replay_input_sha256)
            if len(replay) != len(args.expected_replay_input_sha256):
                raise HistoricalTestInputError("historical_test_input_replay_duplicate")
            value = capture_manifest(
                historical_manifest_root=args.historical_manifest_root,
                runtime_data_root=args.runtime_data_root,
                formal_surface_root=args.formal_surface_root,
                historical_manifest_path=args.historical_manifest,
                expected_manifest_sha256=args.expected_manifest_sha256,
                expected_manifest_content_sha256=args.expected_manifest_content_sha256,
                expected_replay_input_sha256=replay,
                fixture_root=args.output_root,
                source_root=args.source_root,
                component_registry_path=args.components_path,
            )
            path = persist_content_addressed(args.output_root, value)
            loaded = HistoricalTestInput.load(path)
        else:
            loaded = HistoricalTestInput.load(args.manifest)
        print(
            json.dumps(
                {
                    "status": "verified",
                    "manifest_path": str(loaded.path),
                    "manifest_sha256": loaded.file_sha256,
                    "historical_manifest_path": str(loaded.historical_manifest_path),
                    "historical_manifest_sha256": loaded.expected_manifest_sha256,
                    "historical_manifest_content_sha256": loaded.expected_manifest_content_sha256,
                    "verification_root": str(loaded.verification_root),
                    "external_verification_count": len(
                        loaded.external_verification_rows
                    ),
                    "allowed_file_count": len(loaded.files),
                    "formal_write_count": 0,
                    "model_call_count": 0,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    except (HistoricalTestInputError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(
            json.dumps(
                {"status": "error", "error_code": str(exc), "formal_write_count": 0, "model_call_count": 0},
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
