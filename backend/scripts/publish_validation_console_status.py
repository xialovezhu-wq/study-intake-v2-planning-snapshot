#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = ROOT / "dashboard"
if str(DASHBOARD) not in sys.path:
    sys.path.insert(0, str(DASHBOARD))

from validation_console import (  # noqa: E402
    ENGINEERING_GATES,
    SUBJECTS,
    ValidationConsoleStore,
    atomic_write,
    sha256_bytes,
)


def parse_binding(raw: str) -> tuple[str, Path]:
    identifier, separator, path = raw.partition("=")
    if not separator or not identifier or not path:
        raise argparse.ArgumentTypeError("binding must be ID=/absolute/path")
    return identifier, Path(path)


def safe_source(path: Path, *, max_bytes: int) -> bytes:
    if not path.is_absolute():
        raise SystemExit(f"source path is not absolute: {path}")
    try:
        node = path.lstat()
        raw = path.read_bytes()
    except OSError as exc:
        raise SystemExit(f"source unavailable: {path}") from exc
    if path.is_symlink() or not path.is_file() or node.st_size > max_bytes:
        raise SystemExit(f"source is unsafe: {path}")
    return raw


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--skills", type=Path, required=True)
    parser.add_argument("--mcp-preflight", type=Path, required=True)
    parser.add_argument("--engineering", type=Path, required=True)
    parser.add_argument("--report", action="append", default=[], type=parse_binding)
    parser.add_argument("--audit-package", type=Path)
    args = parser.parse_args()
    state_root = (
        args.runtime_root.resolve()
        / "state"
        / "validation-console"
        / "releases"
        / args.release_id
    )
    store = ValidationConsoleStore(
        state_root,
        fixture_mode=False,
        central_release_id=args.release_id,
    )
    skills = json.loads(safe_source(args.skills, max_bytes=64 * 1024))
    mcp = json.loads(safe_source(args.mcp_preflight, max_bytes=64 * 1024))
    engineering = json.loads(safe_source(args.engineering, max_bytes=64 * 1024))
    if (
        not isinstance(skills, dict)
        or set(skills) != SUBJECTS
        or not isinstance(mcp, dict)
        or set(mcp) != SUBJECTS
        or not isinstance(engineering, dict)
        or set(engineering) != set(ENGINEERING_GATES)
    ):
        raise SystemExit("technical status input shape invalid")
    reports: dict[str, dict[str, object]] = {}
    for identifier, source in args.report:
        raw = safe_source(source, max_bytes=16 * 1024 * 1024)
        suffix = source.suffix if source.suffix in {".md", ".json"} else ".md"
        relative = f"{identifier}{suffix}"
        target = store.report_root / relative
        atomic_write(target, raw)
        reports[identifier] = {
            "relative_path": relative,
            "sha256": sha256_bytes(raw),
            "byte_count": len(raw),
        }
    audit_package: dict[str, object] = {"available": False}
    if args.audit_package is not None:
        raw = safe_source(args.audit_package, max_bytes=256 * 1024 * 1024)
        target = store.audit_root / args.audit_package.name
        atomic_write(target, raw)
        audit_package = {
            "available": True,
            "filename": target.name,
            "sha256": sha256_bytes(raw),
            "byte_count": len(raw),
        }
    status = {
        "schema_version": "study-intake-validation-console-technical-status-v1",
        "central_release_id": args.release_id,
        "execution_mode": "offline",
        "live_gate_locked": True,
        "manual_authorization_present": False,
        "production_accepted": False,
        "formal_write_count": 0,
        "skills": skills,
        "mcp_preflight": mcp,
        "engineering": engineering,
        "reports": reports,
        "audit_package": audit_package,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    reopened = store.publish_technical_status(status)
    print(
        json.dumps(
            {
                "status": "published",
                "technical_status_path": str(store.status_path),
                "technical_status_sha256": sha256_bytes(
                    store.status_path.read_bytes()
                ),
                "report_count": len(reports),
                "audit_package_available": audit_package["available"],
                "production_accepted": reopened["production_accepted"],
                "formal_write_count": 0,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
