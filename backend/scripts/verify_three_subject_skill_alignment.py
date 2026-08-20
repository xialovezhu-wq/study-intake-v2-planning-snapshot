#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "lib"
if str(LIB) not in sys.path:
    sys.path.insert(0, str(LIB))

from three_subject_skill_alignment import AlignmentError, run_alignment


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--component-lock", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--phase", choices=("authoring", "build", "deployed"), required=True
    )
    args = parser.parse_args()
    try:
        result = run_alignment(
            source_root=args.source_root.resolve(),
            component_lock_path=args.component_lock.resolve(),
            output_root=args.output_root.resolve(),
            phase=args.phase,
        )
    except AlignmentError as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error": str(exc),
                    "phase": args.phase,
                    "root_sol_helper_subagent_count": 0,
                    "real_terra_call_count": 0,
                    "real_luna_call_count": 0,
                    "live_capture_created_count": 0,
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
