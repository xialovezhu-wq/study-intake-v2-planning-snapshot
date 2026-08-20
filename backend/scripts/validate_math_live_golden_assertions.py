#!/usr/bin/env python3
"""Validate one completed live-math package against its frozen business task."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from math_live_business_fixture import (  # noqa: E402
    MathLiveBusinessFixtureError,
    validate_manifest,
)
from math_live_golden_assertions import (  # noqa: E402
    MathLiveGoldenAssertionError,
    RESULT_SCHEMA,
    validate_result,
)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    root.add_argument("--manifest", type=Path, required=True)
    root.add_argument("--trusted-source-root", type=Path, required=True)
    root.add_argument("--capture-id", required=True)
    root.add_argument("--package", type=Path, required=True)
    root.add_argument("--report", type=Path, required=True)
    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        fixture = validate_manifest(
            args.manifest, trusted_source_root=args.trusted_source_root
        )
        task = next(
            row
            for row in fixture["tasks"]
            if row.get("capture_id") == args.capture_id
        )
        result = validate_result(task, args.package, args.report)
    except StopIteration:
        error_code = "math_live_golden_capture_not_in_frozen_batch"
    except MathLiveBusinessFixtureError as exc:
        error_code = exc.code
    except MathLiveGoldenAssertionError as exc:
        error_code = exc.code
    else:
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    print(
        json.dumps(
            {
                "schema_version": RESULT_SCHEMA,
                "status": "failed",
                "error_code": error_code,
                "model_call_count": 0,
                "formal_write_count": 0,
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
