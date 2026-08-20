#!/usr/bin/env python3
"""Validate live math business fixtures without MCP, model, or writer access."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from math_live_business_fixture import (  # noqa: E402
    MathLiveBusinessFixtureError,
    validate_manifest,
    write_content_addressed_receipt,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--trusted-source-root", type=Path)
    parser.add_argument("--trusted-rollout-root", type=Path)
    parser.add_argument("--output-root", type=Path)
    args = parser.parse_args()
    try:
        result = validate_manifest(
            args.manifest,
            trusted_source_root=args.trusted_source_root,
            trusted_rollout_root=args.trusted_rollout_root,
        )
        envelope: dict[str, object] = dict(result)
        if args.output_root is not None:
            receipt_path, receipt_sha = write_content_addressed_receipt(
                result, args.output_root
            )
            envelope["receipt_path"] = str(receipt_path)
            envelope["receipt_sha256"] = receipt_sha
    except (MathLiveBusinessFixtureError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "status": "failed_closed",
                    "error_code": str(exc),
                    "fixture_write_count": 0,
                    "mcp_tool_call_count": 0,
                    "model_call_count": 0,
                    "formal_write_count": 0,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(envelope, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
