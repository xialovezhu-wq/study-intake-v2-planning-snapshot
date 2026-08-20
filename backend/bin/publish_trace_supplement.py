#!/usr/bin/env python3
"""Publish an exact private current-question teaching trace supplement."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import (  # noqa: E402
    PreprocessorError,
    publish_trace_supplement,
    utc_now,
)


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        description="Publish a content-addressed current-question trace supplement"
    )
    value.add_argument("--private-root", required=True)
    value.add_argument("--capture-id", required=True)
    value.add_argument("--context-id", required=True)
    value.add_argument("--item-id")
    value.add_argument("--evidence-manifest-sha256", required=True)
    value.add_argument("--created-at")
    value.add_argument(
        "--supplement-kind",
        choices=("legacy_backfill", "resolved_trace"),
        required=True,
    )
    value.add_argument("--resolution-receipt-sha256")
    value.add_argument(
        "--events-json",
        required=True,
        help="JSON event array, or - to read the array from stdin",
    )
    return value


def main() -> int:
    os.umask(0o077)
    args = parser().parse_args()
    try:
        raw = sys.stdin.read() if args.events_json == "-" else args.events_json
        events = json.loads(raw)
        if not isinstance(events, list):
            raise PreprocessorError("interaction_trace_events_json_invalid")
        result = publish_trace_supplement(
            private_root=Path(args.private_root),
            capture_id=args.capture_id,
            context_id=args.context_id,
            item_id=args.item_id,
            evidence_manifest_sha256=args.evidence_manifest_sha256,
            created_at=args.created_at or utc_now(),
            supplement_kind=args.supplement_kind,
            resolution_receipt_sha256=args.resolution_receipt_sha256,
            events=events,
        )
        emit(result)
        return 0
    except (json.JSONDecodeError, UnicodeError):
        error = "interaction_trace_events_json_invalid"
    except PreprocessorError as exc:
        error = exc.code
    emit(
        {
            "schema_version": "current-question-trace-supplement-cli-error-v1",
            "status": "error",
            "error_code": error,
            "formal_write_count": 0,
        }
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
