#!/usr/bin/env python3
"""Validate a frozen deferred-math fixture without model or write access."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from read_only_math_fixture import ReadOnlyMathFixtureError, validate_manifest  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = validate_manifest(args.manifest)
    except (ReadOnlyMathFixtureError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "status": "failed_closed",
                    "error_code": str(exc),
                    "fixture_write_count": 0,
                    "model_call_count": 0,
                    "formal_write_count": 0,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
