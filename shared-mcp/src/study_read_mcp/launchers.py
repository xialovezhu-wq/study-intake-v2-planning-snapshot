from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

from .config import RepositoryConfig
from .errors import StudyReadError
from .preflight import (
    InfrastructurePreflightSession,
    PreflightPolicy,
    sha256_file,
)
from .release import SERVER_RELEASE
from .server import build_server
from .service import StudyReadService
from .session import ReadSession
from .runtime_binding import verify_runtime_binding


def _main(subject: str, argv: list[str] | None = None) -> None:
    verify_runtime_binding()
    parser = argparse.ArgumentParser(prog=f"study-read-mcp-{subject}")
    parser.add_argument("--stdio", action="store_true", help="serve over standard input/output")
    parser.add_argument("--read-session-manifest", type=Path, required=True)
    parser.add_argument("--preprocessor-root", type=Path, required=True)
    args = parser.parse_args(argv)
    if not args.stdio:
        raise SystemExit("only --stdio transport is supported")
    try:
        read_session = ReadSession.load(args.read_session_manifest)
        if read_session.subject != subject:
            raise StudyReadError(
                "READ_SESSION_SUBJECT_MISMATCH", "launcher subject does not match read session"
            )
        config = replace(
            RepositoryConfig.production(), preprocessor_root=args.preprocessor_root
        )
        service = StudyReadService(
            config,
            subjects={subject},
            profile="luna",
            read_session=read_session,
            require_authority_snapshot=True,
        )
    except StudyReadError as exc:
        raise SystemExit(exc.message) from exc
    server = build_server(service)
    try:
        server.run(transport="stdio")
    finally:
        service.close()


def _preflight_main(subject: str, argv: list[str] | None = None) -> None:
    runtime_root, release_id, manifest_sha256 = verify_runtime_binding()
    parser = argparse.ArgumentParser(
        prog=f"study-read-mcp-{subject}-infrastructure-preflight"
    )
    parser.add_argument("--stdio", action="store_true")
    parser.add_argument("--preflight-session-manifest", type=Path, required=True)
    parser.add_argument("--preprocessor-root", type=Path, required=True)
    parser.add_argument("--tool-policy", type=Path, required=True)
    args = parser.parse_args(argv)
    if not args.stdio:
        raise SystemExit("only --stdio transport is supported")
    try:
        session = InfrastructurePreflightSession.load(
            args.preflight_session_manifest
        )
        if session.subject != subject:
            raise StudyReadError(
                "PREFLIGHT_SUBJECT_MISMATCH",
                "launcher subject does not match infrastructure preflight session",
            )
        if (
            session.mcp_release_id != release_id
            or session.mcp_release_manifest_sha256 != manifest_sha256
            or session.mcp_server_release != SERVER_RELEASE
            or session.launcher_sha256
            != sha256_file(runtime_root / "scripts" / "sealed_launcher.py")
        ):
            raise StudyReadError(
                "PREFLIGHT_RELEASE_BINDING_MISMATCH",
                "infrastructure preflight release binding is invalid",
            )
        policy = PreflightPolicy.load(
            args.tool_policy,
            session.tool_policy_sha256,
        )
        config = replace(
            RepositoryConfig.production(),
            preprocessor_root=args.preprocessor_root,
        )
        service = StudyReadService(
            config,
            subjects={subject},
            profile="infrastructure_preflight",
            preflight_session=session,
            preflight_policy=policy,
        )
    except StudyReadError as exc:
        raise SystemExit(exc.message) from exc
    server = build_server(service)
    try:
        server.run(transport="stdio")
    finally:
        service.close()


def math_main() -> None:
    _main("math")


def cs408_main() -> None:
    _main("cs408")


def english_main() -> None:
    _main("english")


def main() -> None:
    parser = argparse.ArgumentParser(prog="study-read-mcp-subject-server", add_help=False)
    parser.add_argument("--subject", choices=("math", "cs408", "english"), required=True)
    parser.add_argument(
        "--profile",
        choices=("luna", "infrastructure_preflight"),
        default="luna",
    )
    args, remaining = parser.parse_known_args()
    if args.profile == "infrastructure_preflight":
        _preflight_main(args.subject, remaining)
    else:
        _main(args.subject, remaining)


if __name__ == "__main__":
    main()
