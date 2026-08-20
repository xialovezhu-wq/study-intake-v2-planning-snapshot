#!/usr/bin/env python3
"""Verify 408 and English projection repairs without touching live state."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Sequence


SOURCE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SOURCE_ROOT / "lib"))

from projection_rebuild_verifier import (  # noqa: E402
    ProjectionRebuildError,
    canonical_bytes,
    sha256_bytes,
    verify_cs408_projection_rebuild,
    verify_english_projection_rebuild,
)


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_bytes(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--subject", choices=("cs408", "english", "all"), required=True)
    value.add_argument("--artifact-root", type=Path, required=True)
    value.add_argument(
        "--cs408-repo",
        type=Path,
        default=Path("/Users/xiazhibin/Documents/kaoyan-408"),
    )
    value.add_argument(
        "--english-repo",
        type=Path,
        default=Path("/Users/xiazhibin/Documents/kaoyan-english"),
    )
    value.add_argument("--english-source-id", default="RAW-ARTICLE-20260710-001")
    value.add_argument("--english-date", default="2026-08-06")
    value.add_argument("--output", type=Path)
    value.add_argument("--report", type=Path)
    return value


def run(args: argparse.Namespace) -> dict[str, Any]:
    subjects = ("cs408", "english") if args.subject == "all" else (args.subject,)
    results: dict[str, Any] = {}
    if "cs408" in subjects:
        results["cs408"] = verify_cs408_projection_rebuild(
            args.cs408_repo,
            args.artifact_root,
        )
    if "english" in subjects:
        results["english"] = verify_english_projection_rebuild(
            args.english_repo,
            args.artifact_root,
            source_id=args.english_source_id,
            study_date=args.english_date,
        )
    stable = {
        "schema_version": "study-intake-projection-rebuild-batch-v1",
        "status": (
            "verified_live_current"
            if all(row["live_gate_status"] == "closed" for row in results.values())
            else "verified_isolated_repair_live_change_required"
        ),
        "subjects": results,
        "model_call_count": 0,
        "formal_write_count": 0,
    }
    stable["batch_sha256"] = sha256_bytes(canonical_bytes(stable))
    return stable


def render_report(result: dict[str, Any]) -> str:
    lines = [
        "# Projection repair gate verification",
        "",
        f"- Status: `{result['status']}`",
        "- Execution mode: live sources read-only; rebuilds only in isolated copies",
        f"- Batch SHA-256: `{result['batch_sha256']}`",
        f"- Model calls: `{result['model_call_count']}`",
        f"- Formal writes: `{result['formal_write_count']}`",
        "",
    ]
    for subject in ("cs408", "english"):
        row = result.get("subjects", {}).get(subject)
        if not isinstance(row, dict):
            continue
        lines.extend(
            [
                f"## {subject}",
                "",
                f"- Issue: `{row['issue_id']}`",
                f"- Repair capability: `{row['repair_capability_status']}`",
                f"- Live gate: `{row['live_gate_status']}`",
                f"- Receipt SHA-256: `{row['receipt_sha256']}`",
                f"- Receipt artifact: `{row['receipt_artifact']['path']}`",
                f"- Boundary: {row['candidate_gate_effect']}",
                "",
                "### Checks",
                "",
            ]
        )
        for key, value in row["checks"].items():
            lines.append(f"- `{key}`: `{str(value).lower()}`")
        lines.extend(["", "### Candidate outputs", ""])
        outputs = row.get("candidate_outputs")
        if outputs is None:
            outputs = {"english_quick_capture_projection_v2": row["candidate_output"]}
        for name, output in outputs.items():
            lines.append(
                f"- `{name}`: `{output['sha256']}` ({output['byte_count']} bytes), `{output['path']}`"
            )
        lines.extend(
            [
                "",
                "### Change-window rollback boundary",
                "",
                "- User authorization is required before changing the live projection.",
                "- Canonical inputs must be rehashed under the subject lock.",
                "- Every derived target must be backed up content-addressably before atomic replacement.",
                "- Any failed audit restores the exact pre-change bytes; canonical ledgers stay unchanged.",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = run(args)
    except ProjectionRebuildError as exc:
        result = {
            "schema_version": "study-intake-projection-rebuild-batch-v1",
            "status": "failed_closed",
            "error_code": str(exc),
            "model_call_count": 0,
            "formal_write_count": 0,
        }
        sys.stdout.buffer.write(canonical_bytes(result))
        return 2
    if args.output:
        _atomic_json(args.output.expanduser().resolve(), result)
    if args.report:
        report_path = args.report.expanduser().resolve()
        report_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{report_path.name}.", dir=report_path.parent
        )
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(render_report(result).encode("utf-8"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, report_path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    sys.stdout.buffer.write(canonical_bytes(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
