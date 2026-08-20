#!/usr/bin/env python3
"""Read one exact non-formal Luna report through the restricted Sol view."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "lib") not in sys.path:
    sys.path.insert(0, str(ROOT / "lib"))

from subject_sol_contract import (  # noqa: E402
    SubjectSolContractError,
    SubjectSolRuntimeStore,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--report-sha256", required=True)
    args = parser.parse_args()
    try:
        value = SubjectSolRuntimeStore(
            args.runtime_root
        ).read_restricted_sol_review_candidate(args.report_sha256)
    except SubjectSolContractError as exc:
        sys.stderr.write(json.dumps({"error_code": exc.code}, sort_keys=True))
        return 2
    sys.stdout.write(json.dumps(value, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
