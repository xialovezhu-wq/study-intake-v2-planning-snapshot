#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PLUGIN_ROOT = ROOT / "plugin/kaoyan-study-intake"
DEFAULT_MCP_RELEASES_ROOT = Path(
    "/Users/xiazhibin/.codex/local-study-read-mcp/releases"
)
DEFAULT_MCP_PYTHON = Path(
    "/Users/xiazhibin/Documents/Codex/local-study-read-mcp/.venv/bin/python"
)
DEFAULT_CODEX = Path("/Applications/ChatGPT.app/Contents/Resources/codex")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
RECEIPT_SCHEMA = "study-intake-model-driven-mcp-smoke-receipt-v2"
AUTHORITY_SCHEMA = "study-intake-model-driven-mcp-smoke-authority-v2"
AUTHORITY_PURPOSE = "model-driven-mcp-smoke-receipt-v2"
SUBJECT_SERVER = {
    "math": "kaoyan_math_read",
    "cs408": "kaoyan_cs408_read",
    "english": "kaoyan_english_read",
}
SUBJECT_COLLECTION = {
    "math": "formal_card_catalog",
    "cs408": "formal_wrong_item_catalog",
    "english": "article_catalog",
}


class SmokeError(RuntimeError):
    def __init__(self, code: str, summary: Mapping[str, Any] | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.summary = dict(summary or {})


def canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_argument(value: str) -> str:
    if SHA256_RE.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("must be a lowercase 64-character SHA-256")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one model-driven background MCP transport smoke and persist a "
            "content-addressed HMAC receipt."
        )
    )
    parser.add_argument(
        "--candidate-release-id",
        required=True,
        type=_sha256_argument,
        help="candidate release SHA-256 to bind into the MCP read session and receipt",
    )
    parser.add_argument(
        "--runtime-root",
        required=True,
        type=Path,
        help="persistent Study Intake runtime root; never created as a temporary root",
    )
    parser.add_argument(
        "--authority-key",
        type=Path,
        help="existing HMAC key (default: RUNTIME/dispatch/state/authority.key)",
    )
    parser.add_argument("--plugin-root", type=Path, default=DEFAULT_PLUGIN_ROOT)
    parser.add_argument(
        "--mcp-releases-root",
        type=Path,
        default=DEFAULT_MCP_RELEASES_ROOT,
    )
    parser.add_argument("--mcp-python", type=Path, default=DEFAULT_MCP_PYTHON)
    parser.add_argument("--codex-bin", type=Path, default=DEFAULT_CODEX)
    parser.add_argument(
        "--receipt-root",
        type=Path,
        help="persistent evidence root (default: RUNTIME/deployments/model-driven-mcp-smoke)",
    )
    parser.add_argument(
        "--subject",
        choices=tuple(SUBJECT_SERVER),
        default="math",
        help="subject-isolated MCP namespace to exercise",
    )
    parser.add_argument("--capture-id")
    parser.add_argument(
        "--article-id",
        default="2011-english-i-text-4",
        help="canonical article identity used only by the English smoke",
    )
    parser.add_argument("--study-date", default="2026-08-09")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    return parser


def _regular_file(path: Path, *, code: str, executable: bool = False) -> Path:
    candidate = path.expanduser().absolute()
    try:
        node = candidate.lstat()
    except OSError as exc:
        raise SmokeError(code) from exc
    if candidate.is_symlink() or not stat.S_ISREG(node.st_mode):
        raise SmokeError(code)
    if executable and not os.access(candidate, os.X_OK):
        raise SmokeError(code)
    return candidate


def _authority_key(path: Path) -> bytes:
    candidate = _regular_file(path, code="smoke_authority_key_invalid")
    try:
        mode = stat.S_IMODE(candidate.stat().st_mode)
        key = candidate.read_bytes()
    except OSError as exc:
        raise SmokeError("smoke_authority_key_invalid") from exc
    if mode & 0o077 or len(key) < 32 or len(key) > 4096:
        raise SmokeError("smoke_authority_key_invalid")
    return key


def _atomic_content_addressed_write(
    root: Path,
    *,
    category: str,
    value: bytes,
    suffix: str,
) -> tuple[Path, str]:
    digest = sha256_bytes(value)
    destination = root / category / "sha256" / digest[:2] / f"{digest}{suffix}"
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if destination.exists():
        if destination.is_symlink() or destination.read_bytes() != value:
            raise SmokeError("smoke_evidence_content_address_collision")
        return destination, digest
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{digest}.",
        dir=destination.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(value)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o400)
        os.replace(temporary, destination)
        destination.chmod(0o400)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return destination, digest


def _artifact_ref(
    receipt_root: Path,
    *,
    category: str,
    value: bytes,
    suffix: str,
) -> dict[str, Any]:
    path, digest = _atomic_content_addressed_write(
        receipt_root,
        category=f"artifacts/{category}",
        value=value,
        suffix=suffix,
    )
    return {
        "path": str(path),
        "sha256": digest,
        "byte_count": len(value),
    }


def _persist_receipt(
    receipt_root: Path,
    *,
    core: Mapping[str, Any],
    key: bytes,
) -> dict[str, Any]:
    stable_core = dict(core)
    authority = {
        "schema_version": AUTHORITY_SCHEMA,
        "algorithm": "HMAC-SHA256",
        "purpose": AUTHORITY_PURPOSE,
        "key_id": sha256_bytes(key),
        "hmac_sha256": hmac.new(
            key,
            canonical_bytes(stable_core),
            hashlib.sha256,
        ).hexdigest(),
    }
    receipt = {**stable_core, "authority": authority}
    receipt_bytes = canonical_bytes(receipt)
    path, digest = _atomic_content_addressed_write(
        receipt_root,
        category="receipts",
        value=receipt_bytes,
        suffix=".json",
    )
    return {
        "status": stable_core["status"],
        "candidate_release_id": stable_core["candidate_release_id"],
        "receipt_path": str(path),
        "receipt_sha256": digest,
        "authority_key_id": authority["key_id"],
        "model_call_count": stable_core["model_call_count"],
        "mcp_tool_call_count": stable_core["mcp_tool_call_count"],
        "formal_write_count": 0,
    }


def _processing_plugin_host_type() -> type[Any]:
    library = str(ROOT / "lib")
    if library not in sys.path:
        sys.path.insert(0, library)
    from processing_plugin import ProcessingPluginHost

    return ProcessingPluginHost


def _fixture_bytes() -> bytes:
    return bytes.fromhex(
        "89504e470d0a1a0a0000000d494844520000000100000001"
        "08060000001f15c4890000000d4944415408d763f8cfc0f0"
        "1f00050001ff89993d1d0000000049454e44ae426082"
    )


def _event_items(stdout: bytes) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return [
        event["item"]
        for event in events
        if event.get("type") == "item.completed"
        and isinstance(event.get("item"), dict)
        and str(event["item"].get("type") or "").replace("-", "_").casefold()
        in {"mcp_tool_call", "mcptoolcall"}
    ]


def _event_tool(item: Mapping[str, Any]) -> str:
    return str(item.get("tool") or item.get("name") or "")


def _event_server(item: Mapping[str, Any]) -> str:
    return str(item.get("server") or item.get("server_name") or "")


def _stable_ids_from_result(value: Any) -> set[str]:
    """Extract only IDs present in the persisted provider tool result."""

    result: set[str] = set()
    if isinstance(value, Mapping):
        stable_id = value.get("stable_id")
        if isinstance(stable_id, str) and stable_id:
            result.add(stable_id)
        for nested in value.values():
            result.update(_stable_ids_from_result(nested))
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        for nested in value:
            result.update(_stable_ids_from_result(nested))
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith(("{", "[")):
            try:
                result.update(_stable_ids_from_result(json.loads(stripped)))
            except json.JSONDecodeError:
                pass
    return result


def _fixture_capture(
    *,
    subject: str,
    capture_id: str,
    article_id: str,
    study_date: str,
    temp: Path,
) -> tuple[dict[str, Any], str, dict[str, str], list[dict[str, str]]]:
    scene = {
        "math": "formal_problem",
        "cs408": "formal_problem",
        "english": "intensive_reading",
    }[subject]
    capture_facts = {
        "capture_id": capture_id,
        "subject": subject,
        "study_date": study_date,
        "scene": scene,
        "transport_smoke_only": True,
    }
    facts_sha = sha256_bytes(canonical_bytes(capture_facts))
    identity: dict[str, str] = {
        "content_fingerprint": facts_sha,
        "source_id": article_id if subject == "english" else capture_id,
    }
    if subject == "math":
        identity["formal_id"] = capture_id
    elif subject == "cs408":
        identity["review_identity"] = capture_id
    else:
        identity["article_id"] = article_id

    artifacts: list[tuple[str, str, bytes, str]]
    if subject == "math":
        artifacts = [
            ("dialogue", "dialogue", canonical_bytes({"turns": [{"speaker": "user", "text": "smoke user turn"}]}), ".json"),
            ("learning-record", "learning_record", canonical_bytes({"result": "transport_smoke"}), ".json"),
            ("question-image", "question_image", _fixture_bytes(), ".png"),
            ("solution-text", "solution_text", b"# Solution evidence\n\nTransport-only smoke.\n", ".md"),
        ]
    elif subject == "cs408":
        artifacts = [
            ("dialogue", "dialogue", canonical_bytes({"turns": [{"speaker": "user", "text": "smoke 408 turn"}]}), ".json"),
            ("question-text", "question_text", b"Transport-only 408 question surface.\n", ".txt"),
        ]
    else:
        artifacts = [
            ("article-text", "article_text", b"Transport-only English article.\n", ".txt"),
            ("answer-key", "answer_key", b"Private post-attempt answer evidence.\n", ".txt"),
            ("explanation", "explanation", b"Transport-only explanation evidence.\n", ".txt"),
            ("sentence-events", "sentence_events", canonical_bytes({"events": []}), ".json"),
        ]
    rows: list[dict[str, str]] = []
    for artifact_id, kind, raw, suffix in artifacts:
        path = temp / f"{artifact_id}{suffix}"
        path.write_bytes(raw)
        rows.append(
            {
                "artifact_id": artifact_id,
                "artifact_kind": kind,
                "path": str(path),
                "sha256": sha256_bytes(raw),
            }
        )
    return capture_facts, scene, identity, rows


def _failure_receipt(
    *,
    receipt_root: Path,
    key: bytes,
    common: Mapping[str, Any],
    error_code: str,
    stdout: bytes,
    stderr: bytes,
    output_bytes: bytes | None,
    returncode: int | None,
    started_at: str,
    duration_ms: int,
    mcp_tool_call_count: int,
) -> dict[str, Any]:
    core = {
        **common,
        "schema_version": RECEIPT_SCHEMA,
        "status": "failed",
        "error_code": error_code,
        "returncode": returncode,
        "started_at": started_at,
        "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "duration_ms": duration_ms,
        "stdout": _artifact_ref(
            receipt_root,
            category="stdout",
            value=stdout,
            suffix=".jsonl",
        ),
        "stderr": _artifact_ref(
            receipt_root,
            category="stderr",
            value=stderr,
            suffix=".log",
        ),
        "model_output": (
            None
            if output_bytes is None
            else _artifact_ref(
                receipt_root,
                category="model-output",
                value=output_bytes,
                suffix=".json",
            )
        ),
        "model_call_count": 1,
        "mcp_tool_call_count": mcp_tool_call_count,
        "formal_write_count": 0,
    }
    return _persist_receipt(receipt_root, core=core, key=key)


def run_smoke(
    args: argparse.Namespace,
    *,
    command_runner: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
    host_type: type[Any] | None = None,
) -> dict[str, Any]:
    if args.timeout_seconds <= 0 or args.timeout_seconds > 3600:
        raise SmokeError("smoke_timeout_invalid")
    runtime_root = args.runtime_root.expanduser().absolute()
    if not runtime_root.is_dir() or runtime_root.is_symlink():
        raise SmokeError("smoke_runtime_root_invalid")
    receipt_root = (
        args.receipt_root.expanduser().absolute()
        if args.receipt_root is not None
        else runtime_root / "deployments/model-driven-mcp-smoke"
    )
    key_path = (
        args.authority_key.expanduser().absolute()
        if args.authority_key is not None
        else runtime_root / "dispatch/state/authority.key"
    )
    key = _authority_key(key_path)
    plugin_root = args.plugin_root.expanduser().absolute()
    component_lock_path = _regular_file(
        plugin_root / "component-lock.json",
        code="smoke_component_lock_invalid",
    )
    component_lock_bytes = component_lock_path.read_bytes()
    try:
        component_lock = json.loads(component_lock_bytes)
    except json.JSONDecodeError as exc:
        raise SmokeError("smoke_component_lock_invalid") from exc
    mcp_release_id = str(component_lock.get("mcp_release_id") or "")
    if SHA256_RE.fullmatch(mcp_release_id) is None:
        raise SmokeError("smoke_component_lock_invalid")
    mcp_root = args.mcp_releases_root.expanduser().absolute() / mcp_release_id
    if not mcp_root.is_dir() or mcp_root.is_symlink():
        raise SmokeError("smoke_mcp_release_invalid")
    mcp_python = _regular_file(
        args.mcp_python,
        code="smoke_mcp_python_invalid",
        executable=True,
    )
    subject = str(args.subject)
    server_name = SUBJECT_SERVER[subject]
    collection = SUBJECT_COLLECTION[subject]
    subject_launcher = _regular_file(
        mcp_python.with_name(f"study-read-mcp-{subject}"),
        code="smoke_mcp_launcher_invalid",
        executable=True,
    )
    codex = _regular_file(
        args.codex_bin,
        code="smoke_codex_binary_invalid",
        executable=True,
    )
    capture_id = str(
        args.capture_id
        or {
            "math": "GS-111",
            "cs408": "SMOKE-CS408-001",
            "english": "SMOKE-ENGLISH-001",
        }[subject]
    )
    host_class = host_type or _processing_plugin_host_type()
    with tempfile.TemporaryDirectory(prefix="model-mcp-smoke-") as temporary:
        temp = Path(temporary)
        host = host_class(
            {
                "enabled": True,
                "root": str(plugin_root),
                "component_lock_path": str(component_lock_path),
                "mcp_client_python": str(mcp_python),
                "mcp_project_root": str(mcp_root),
                "authority_key_path": str(key_path),
                "profile": "background",
                "timeout_seconds": 30,
            },
            runtime_root=runtime_root,
            candidate_release_id=args.candidate_release_id,
        )
        capture_facts, scene, capture_identity, capture_artifacts = _fixture_capture(
            subject=subject,
            capture_id=capture_id,
            article_id=str(args.article_id),
            study_date=args.study_date,
            temp=temp,
        )
        capture_facts_bytes = canonical_bytes(capture_facts)
        input_fingerprint = sha256_bytes(capture_facts_bytes)
        input_binding: dict[str, str] = {"capture_id": capture_id}
        if subject == "english":
            input_binding["article_id"] = str(args.article_id)
        context = host.open_read_session(
            subject=subject,
            capture_id=capture_id,
            study_date=args.study_date,
            input_fingerprint=input_fingerprint,
            input_binding=input_binding,
            capture_facts_sha256=sha256_bytes(capture_facts_bytes),
            capture_facts=capture_facts,
            capture_scene=scene,
            capture_identity=capture_identity,
            capture_artifacts=tuple(capture_artifacts),
            captured_at=f"{args.study_date}T12:00:00+08:00",
            provider_schema_sha256="2" * 64,
            canonical_schema_sha256="3" * 64,
            validator_sha256="4" * 64,
        )
        session_path = host.read_session_manifest_path(context)
        schema = temp / "output.schema.json"
        schema.write_bytes(
            canonical_bytes(
                {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["tool_used", "first_stable_id"],
                    "properties": {
                        "tool_used": {"type": "boolean", "const": True},
                        "first_stable_id": {"type": "string", "minLength": 1},
                    },
                }
            )
        )
        output = temp / "last.json"
        server_args = ["--stdio", "--read-session-manifest", str(session_path)]
        configs = [
            f"mcp_servers.{server_name}.command={json.dumps(str(subject_launcher))}",
            f"mcp_servers.{server_name}.args={json.dumps(server_args)}",
            f"mcp_servers.{server_name}.cwd={json.dumps(str(mcp_root))}",
            f"mcp_servers.{server_name}.required=true",
            f"mcp_servers.{server_name}.enabled=true",
            f'mcp_servers.{server_name}.enabled_tools=["get_task_context","read_task_artifact","list_records","get_records","search_records","query_relations"]',
            f'mcp_servers.{server_name}.default_tools_approval_mode="approve"',
            f"mcp_servers.{server_name}.startup_timeout_sec=20",
            f"mcp_servers.{server_name}.tool_timeout_sec=120",
            f"mcp_servers.{server_name}.env.PYTHONPATH={json.dumps(str(mcp_root / 'src'))}",
            f'mcp_servers.{server_name}.env.PYTHONUTF8="1"',
            f'mcp_servers.{server_name}.env.PYTHONDONTWRITEBYTECODE="1"',
        ]
        command = [
            str(codex),
            "exec",
            "--strict-config",
            "--ephemeral",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--cd",
            str(temp),
            "--model",
            "gpt-5.6-luna",
            "--config",
            'model_reasoning_effort="max"',
            "--config",
            'approval_policy="never"',
            "--config",
            "project_doc_max_bytes=0",
            "--config",
            "project_doc_fallback_filenames=[]",
            "--config",
            "features.shell_tool=false",
            "--config",
            "features.plugins=false",
            "--config",
            "agents.enabled=false",
            "--config",
            'web_search="disabled"',
        ]
        for value in configs:
            command.extend(["--config", value])
        command.extend(
            [
                "--output-schema",
                str(schema),
                "--output-last-message",
                str(output),
                "--json",
                "-",
            ]
        )
        prompt = (
            "Call get_task_context once. Read every returned artifact completely with "
            "read_task_artifact, following next_cursor. Then call list_records with "
            f"collection={collection} and page_size=1. Output tool_used=true "
            "and that list_records result's first stable_id. Do not invent an ID."
        ).encode("utf-8")
        common = {
            "candidate_release_id": args.candidate_release_id,
            "capture_id": capture_id,
            "study_date": args.study_date,
            "subject": subject,
            "server_name": server_name,
            "collection": collection,
            "plugin_name": component_lock.get("plugin_name"),
            "plugin_version": component_lock.get("plugin_version"),
            "component_lock_sha256": sha256_bytes(component_lock_bytes),
            "mcp_release_id": mcp_release_id,
            "mcp_release_manifest_sha256": component_lock.get(
                "mcp_release_manifest_sha256"
            ),
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
            "requested_service_tier": None,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
            "command_sha256": sha256_bytes(canonical_bytes(command)),
            "prompt_sha256": sha256_bytes(prompt),
            "read_session_manifest_sha256": sha256_bytes(
                Path(session_path).read_bytes()
            ),
        }
        started_at = dt.datetime.now(dt.timezone.utc).isoformat()
        started = time.monotonic()
        try:
            completed = command_runner(
                command,
                input=prompt,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=temp,
                timeout=args.timeout_seconds,
                env={
                    "PATH": "/usr/bin:/bin",
                    "PYTHONUTF8": "1",
                    "PYTHONDONTWRITEBYTECODE": "1",
                },
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            duration_ms = round((time.monotonic() - started) * 1000)
            summary = _failure_receipt(
                receipt_root=receipt_root,
                key=key,
                common=common,
                error_code="smoke_codex_timeout",
                stdout=exc.stdout or b"",
                stderr=exc.stderr or b"",
                output_bytes=output.read_bytes() if output.is_file() else None,
                returncode=None,
                started_at=started_at,
                duration_ms=duration_ms,
                mcp_tool_call_count=0,
            )
            raise SmokeError("smoke_codex_timeout", summary) from exc
        duration_ms = round((time.monotonic() - started) * 1000)
        stdout = completed.stdout or b""
        stderr = completed.stderr or b""
        output_bytes = output.read_bytes() if output.is_file() else None
        items = _event_items(stdout)
        if completed.returncode != 0:
            summary = _failure_receipt(
                receipt_root=receipt_root,
                key=key,
                common=common,
                error_code="smoke_codex_failed",
                stdout=stdout,
                stderr=stderr,
                output_bytes=output_bytes,
                returncode=completed.returncode,
                started_at=started_at,
                duration_ms=duration_ms,
                mcp_tool_call_count=len(items),
            )
            raise SmokeError("smoke_codex_failed", summary)
        try:
            payload = json.loads(output_bytes) if output_bytes is not None else None
        except json.JSONDecodeError:
            payload = None
        tools = [_event_tool(item) for item in items]
        list_items = [item for item in items if _event_tool(item) == "list_records"]
        list_result_ids = set().union(
            *(
                _stable_ids_from_result(item.get("result"))
                for item in list_items
            )
        ) if list_items else set()
        event_chain_valid = (
            bool(items)
            and all(_event_server(item) == server_name for item in items)
            and tools.count("get_task_context") == 1
            and tools.count("list_records") == 1
            and tools.count("read_task_artifact") >= len(capture_artifacts) + 1
        )
        if (
            not event_chain_valid
            or not isinstance(payload, Mapping)
            or payload.get("tool_used") is not True
            or not isinstance(payload.get("first_stable_id"), str)
            or payload["first_stable_id"] not in list_result_ids
        ):
            summary = _failure_receipt(
                receipt_root=receipt_root,
                key=key,
                common=common,
                error_code="smoke_mcp_event_or_output_invalid",
                stdout=stdout,
                stderr=stderr,
                output_bytes=output_bytes,
                returncode=completed.returncode,
                started_at=started_at,
                duration_ms=duration_ms,
                mcp_tool_call_count=len(items),
            )
            raise SmokeError("smoke_mcp_event_or_output_invalid", summary)
        event_summaries = [
            {
                "event_type": "item.completed",
                "item_type": item.get("type"),
                "server": _event_server(item),
                "tool": _event_tool(item),
                "arguments_sha256": sha256_bytes(
                    canonical_bytes(item.get("arguments"))
                ),
                "result_sha256": sha256_bytes(
                    canonical_bytes(item.get("result"))
                ),
                "result_present": item.get("result") is not None,
            }
            for item in items
        ]
        core = {
            **common,
            "schema_version": RECEIPT_SCHEMA,
            "status": "verified",
            "returncode": completed.returncode,
            "started_at": started_at,
            "completed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "duration_ms": duration_ms,
            "stdout": _artifact_ref(
                receipt_root,
                category="stdout",
                value=stdout,
                suffix=".jsonl",
            ),
            "stderr": _artifact_ref(
                receipt_root,
                category="stderr",
                value=stderr,
                suffix=".log",
            ),
            "model_output": _artifact_ref(
                receipt_root,
                category="model-output",
                value=output_bytes,
                suffix=".json",
            ),
            "mcp_events": event_summaries,
            "first_stable_id": payload["first_stable_id"],
            "runtime_attestation": "requested_unverified",
            "model_call_count": 1,
            "mcp_tool_call_count": len(items),
            "formal_write_count": 0,
        }
        return _persist_receipt(receipt_root, core=core, key=key)


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        summary = run_smoke(args)
    except SmokeError as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error_code": exc.code,
                    **exc.summary,
                    "formal_write_count": 0,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
