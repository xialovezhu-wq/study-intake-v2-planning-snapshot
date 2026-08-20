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

from sol_mcp_preflight import SolMCPPreflightError, run_preflight


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run one capture-free, model-free subject MCP preflight."
    )
    parser.add_argument("--subject", choices=("math", "cs408", "english"), required=True)
    parser.add_argument("--central-release-id", required=True)
    parser.add_argument("--component-lock", type=Path, required=True)
    parser.add_argument("--tool-policy", type=Path, required=True)
    parser.add_argument(
        "--runtime-root",
        type=Path,
        default=Path("/Users/xiazhibin/.codex/study-intake-preprocessor"),
    )
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument(
        "--invocation-mode",
        choices=("authoring_protocol_harness", "deployed_protocol_harness"),
        required=True,
    )
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    args = parser.parse_args()
    try:
        result = run_preflight(
            subject=args.subject,
            central_release_id=args.central_release_id,
            component_lock_path=args.component_lock,
            tool_policy_path=args.tool_policy,
            runtime_root=args.runtime_root,
            output_root=args.output_root,
            invocation_mode=args.invocation_mode,
            timeout_seconds=args.timeout_seconds,
        )
    except SolMCPPreflightError as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error_code": exc.code,
                    "detail": exc.detail,
                    "subject": args.subject,
                    "write_call_count": 0,
                    "formal_write_count": 0,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "live_capture_created_count": 0,
                    "live_capture_consumed_count": 0,
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
