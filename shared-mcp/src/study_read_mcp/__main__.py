from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

from .config import RepositoryConfig
from .errors import StudyReadError
from .server import build_server
from .service import StudyReadService
from .runtime_binding import verify_runtime_binding


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="study-read-mcp")
    parser.add_argument("--stdio", action="store_true", help="serve over standard input/output")
    parser.add_argument(
        "--profile",
        choices=("ordinary", "background", "luna", "morning_preparation"),
        default="ordinary",
    )
    parser.add_argument("--subjects", default="math,cs408,english")
    parser.add_argument("--read-session-manifest", type=Path)
    parser.add_argument("--subject-root", type=Path)
    return parser.parse_args()


def main() -> None:
    verify_runtime_binding()
    args = parse_args()
    if not args.stdio:
        raise SystemExit("only --stdio transport is supported")
    subjects = {value.strip() for value in args.subjects.split(",") if value.strip()}
    if args.profile in {"background", "luna", "morning_preparation"} and len(subjects) != 1:
        raise SystemExit("isolated profile requires exactly one subject")
    config = RepositoryConfig.production()
    if args.subject_root is not None:
        if len(subjects) != 1:
            raise SystemExit("subject root override requires exactly one subject")
        subject = next(iter(subjects))
        try:
            subject_root = args.subject_root.expanduser().resolve(strict=True)
        except OSError as exc:
            raise SystemExit("subject root is invalid") from exc
        if not subject_root.is_dir():
            raise SystemExit("subject root is invalid")
        config = replace(config, **{f"{subject}_root": subject_root})
    try:
        from .session import ReadSession

        read_session = (
            ReadSession.load(args.read_session_manifest)
            if args.read_session_manifest is not None
            else None
        )
        service = StudyReadService(
            config,
            subjects=subjects,
            profile=args.profile,
            read_session=read_session,
            require_authority_snapshot=args.profile == "luna",
        )
    except StudyReadError as exc:
        raise SystemExit(exc.message) from exc
    server = build_server(service)
    try:
        server.run(transport="stdio")
    finally:
        service.close()


if __name__ == "__main__":
    main()
