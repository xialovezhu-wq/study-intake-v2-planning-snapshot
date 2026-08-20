#!/usr/bin/env python3
"""Execute the four historical release blockers with before/after hash evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from historical_test_input import (  # noqa: E402
    ENV_NAME,
    HistoricalTestInput,
    HistoricalTestInputError,
    persist_content_addressed,
)


TESTS = (
    "tests.test_math_shadow_backaudit.RealAugustFourthBackauditTests.test_real_august_fourth_selection_is_exactly_ten",
    "tests.test_math_shadow_replay.MathShadowReplayTests.test_plan_hash_checks_are_read_only",
    "tests.test_math_shadow_replay.MathShadowReplayTests.test_real_manifest_builds_ten_frozen_shadow_candidates",
    "tests.test_math_v2_core.MathV2CoreTests.test_historical_publish_uses_only_manifest_candidate",
)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--manifest", type=Path, required=True)
    value.add_argument("--source-root", type=Path, default=ROOT)
    value.add_argument("--output-root", type=Path, required=True)
    return value


def _hashes(snapshot: dict) -> dict[str, str]:
    return {
        "formal_surface_sha256": snapshot["formal_surface"]["content_sha256"],
        "protected_runtime_sha256": snapshot["protected_runtime"]["content_sha256"],
        "latest_pointer_sha256": snapshot["latest_pointer"]["content_sha256"],
        "historical_manifest_sha256": snapshot["historical_manifest"]["content_sha256"],
        "verification_sha256": snapshot["external_verification"]["content_sha256"],
    }


def main() -> int:
    args = parser().parse_args()
    try:
        source_root = args.source_root.expanduser().resolve()
        if source_root.is_symlink() or not (source_root / "tests").is_dir():
            raise HistoricalTestInputError("historical_blocking_test_source_invalid")
        test_input = HistoricalTestInput.load(args.manifest)
        rows = []
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        environment[ENV_NAME] = str(test_input.path)
        environment.pop("STUDY_PREPROCESSOR_RUN_REAL_RUNTIME_TESTS", None)
        for test_name in TESTS:
            before = test_input.snapshot()
            started = time.monotonic_ns()
            completed = subprocess.run(
                [sys.executable, "-m", "unittest", test_name],
                cwd=source_root,
                env=environment,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=False,
            )
            duration_ms = (time.monotonic_ns() - started) // 1_000_000
            after = test_input.snapshot()
            output = completed.stdout + completed.stderr
            ran = re.search(r"Ran\s+1\s+test", output) is not None
            skipped = re.search(r"skipped=(\d+)", output)
            skipped_count = int(skipped.group(1)) if skipped else 0
            unchanged = before == after
            passed = completed.returncode == 0 and ran and skipped_count == 0 and unchanged
            rows.append(
                {
                    "test": test_name,
                    "input_manifest_path": str(test_input.path),
                    "input_manifest_sha256": test_input.file_sha256,
                    "historical_manifest_path": str(test_input.historical_manifest_path),
                    "historical_manifest_sha256": test_input.expected_manifest_sha256,
                    "historical_manifest_content_sha256": test_input.expected_manifest_content_sha256,
                    "actually_executed": ran,
                    "skipped": skipped_count != 0,
                    "skipped_count": skipped_count,
                    "before": _hashes(before),
                    "after": _hashes(after),
                    "formal_surface_unchanged": before["formal_surface"] == after["formal_surface"],
                    "protected_runtime_unchanged": before["protected_runtime"] == after["protected_runtime"],
                    "latest_pointer_unchanged": before["latest_pointer"] == after["latest_pointer"],
                    "historical_manifest_unchanged": before["historical_manifest"] == after["historical_manifest"],
                    "verification_unchanged": before["external_verification"] == after["external_verification"],
                    "formal_write_count": 0,
                    "model_call_count": 0,
                    "duration_ms": duration_ms,
                    "return_code": completed.returncode,
                    "output_sha256": hashlib.sha256(output.encode("utf-8")).hexdigest(),
                    "conclusion": "pass" if passed else "fail",
                }
            )
            if not passed:
                break
        report = {
            "schema_version": "study-intake-historical-blocking-test-report-v1",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "source_root": str(source_root),
            "input_manifest_path": str(test_input.path),
            "input_manifest_sha256": test_input.file_sha256,
            "tests": rows,
            "expected_test_count": len(TESTS),
            "executed_test_count": len(rows),
            "skipped_count": sum(row["skipped_count"] for row in rows),
            "status": (
                "passed"
                if len(rows) == len(TESTS)
                and all(row["conclusion"] == "pass" for row in rows)
                else "failed"
            ),
            "formal_write_count": 0,
            "model_call_count": 0,
        }
        path = persist_content_addressed(args.output_root, report)
        print(
            json.dumps(
                {
                    "status": report["status"],
                    "report_path": str(path),
                    "report_sha256": path.stem,
                    "executed_test_count": len(rows),
                    "skipped_count": report["skipped_count"],
                    "formal_write_count": 0,
                    "model_call_count": 0,
                },
                sort_keys=True,
            )
        )
        return 0 if report["status"] == "passed" else 1
    except (HistoricalTestInputError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error_code": str(exc),
                    "formal_write_count": 0,
                    "model_call_count": 0,
                },
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
