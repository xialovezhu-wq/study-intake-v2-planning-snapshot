#!/usr/bin/env python3
"""CLI for the isolated EN-P0-006 English writer adapter."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from english_legacy_writer_adapter import (  # noqa: E402
    DEFAULT_SOURCE_ROOT,
    EnglishLegacyWriterError,
    MARKER_NAME,
    audit_work_item_batch,
    execute_request,
    prepare_isolated_copy,
    source_unchanged,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="EN-P0-006 isolated English authority writer"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser(
        "prepare-copy", help="copy the complete English formal store"
    )
    prepare.add_argument(
        "--source-root", type=Path, default=DEFAULT_SOURCE_ROOT
    )
    prepare.add_argument("--isolated-root", type=Path, required=True)

    execute = subparsers.add_parser(
        "execute", help="run one verified item request in a child process"
    )
    execute.add_argument("--request", type=Path, required=True)

    audit = subparsers.add_parser(
        "audit-work-item-batch",
        help="read-only stable-id and semantic-key preflight",
    )
    audit.add_argument("--work-item-batch", type=Path, required=True)
    audit.add_argument("--isolated-root", type=Path, required=True)
    audit.add_argument("--copy-id", required=True)
    audit.add_argument("--output", type=Path, required=True)

    check = subparsers.add_parser(
        "check-source", help="prove the source authority surfaces are unchanged"
    )
    check.add_argument("--isolated-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "prepare-copy":
            result = prepare_isolated_copy(args.source_root, args.isolated_root)
        elif args.command == "execute":
            result = execute_request(args.request)
        elif args.command == "audit-work-item-batch":
            result = audit_work_item_batch(
                args.work_item_batch, args.isolated_root, args.copy_id
            )
            if args.output.exists():
                raise EnglishLegacyWriterError("english_writer_audit_output_exists")
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            args.output.chmod(0o400)
        else:
            marker_path = args.isolated_root / MARKER_NAME
            marker = json.loads(marker_path.read_text(encoding="utf-8"))
            result = source_unchanged(marker)
            if result["unchanged"] is not True:
                raise EnglishLegacyWriterError("english_writer_source_hash_changed")
    except (EnglishLegacyWriterError, OSError, UnicodeError, json.JSONDecodeError) as exc:
        code = exc.code if isinstance(exc, EnglishLegacyWriterError) else "english_writer_cli_failed"
        print(json.dumps({"status": "failed", "error": code}, sort_keys=True))
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
