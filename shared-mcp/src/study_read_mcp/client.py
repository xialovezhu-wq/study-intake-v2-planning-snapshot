from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from .runtime_binding import (
    sealed_launcher_binding_arguments,
    sealed_launcher_path,
    sealed_python_environment,
    verify_runtime_binding,
)


ALLOWED_TOOLS = frozenset({
    "authority_bundle",
    "verify_evidence_batch",
    "math_read_bundle",
    "cs408_read_bundle",
    "cs408_morning_preparation_bundle",
    "english_read_bundle",
})
MAX_REQUEST_BYTES = 262_144


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="study-read-mcp-client")
    parser.add_argument(
        "--profile", choices=("ordinary", "background", "morning_preparation"), required=True
    )
    parser.add_argument("--subject", choices=("math", "cs408", "english"), required=True)
    parser.add_argument("--project-root", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--subject-root", type=Path)
    return parser.parse_args()


def read_request() -> tuple[str, dict[str, Any]]:
    raw = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
    if len(raw) > MAX_REQUEST_BYTES:
        raise ValueError("request exceeds fixed input limit")
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict) or set(value) != {"tool", "arguments"}:
        raise ValueError("request must contain only tool and arguments")
    tool = value["tool"]
    arguments = value["arguments"]
    if tool not in ALLOWED_TOOLS or not isinstance(arguments, dict):
        raise ValueError("request tool or arguments are invalid")
    return tool, arguments


async def call_tool(args: argparse.Namespace, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    project = args.project_root.expanduser().resolve()
    verify_runtime_binding(
        expected_root=project, require_expected_environment=True
    )
    child_environment = sealed_python_environment(project)
    launcher = sealed_launcher_path(project)
    launcher_args = sealed_launcher_binding_arguments(project)
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-I",
            "-S",
            str(launcher),
            *launcher_args,
            "--mode",
            "server",
            "--profile",
            args.profile,
            "--subjects",
            args.subject,
        ] + (
            ["--subject-root", str(args.subject_root.expanduser().resolve())]
            if args.subject_root is not None
            else []
        ),
        cwd=str(project),
        env=child_environment,
    )
    async with stdio_client(params, errlog=sys.stderr) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            result = await session.call_tool(tool, arguments)
            payload = result.structuredContent
            if not isinstance(payload, dict):
                for item in result.content:
                    text = getattr(item, "text", None)
                    if isinstance(text, str):
                        candidate = json.loads(text)
                        if isinstance(candidate, dict):
                            payload = candidate
                            break
            if not isinstance(payload, dict):
                raise RuntimeError("MCP tool did not return a structured object")
            return payload


def main() -> None:
    try:
        args = parse_args()
        tool, arguments = read_request()
        result = asyncio.run(call_tool(args, tool, arguments))
    except Exception as exc:
        sys.stderr.write(f"study-read-mcp-client: {type(exc).__name__}: {exc}\n")
        raise SystemExit(2) from exc
    sys.stdout.write(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
