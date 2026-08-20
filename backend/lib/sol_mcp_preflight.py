"""Capture-free, model-free, subject-scoped MCP infrastructure preflight."""

from __future__ import annotations

import hashlib
import json
import os
import select
import signal
import stat
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from process_identity import (
    ProcessIdentityError,
    kernel_process_start_token,
    require_kernel_process_start_token,
)


SUBJECTS = frozenset({"math", "cs408", "english"})
PREFLIGHT_TOOLS = (
    "list_records",
    "get_records",
    "search_records",
    "query_relations",
)
SUBJECT_COLLECTIONS = {
    "math": "formal_card_catalog",
    "cs408": "formal_wrong_item_catalog",
    "english": "article_catalog",
}
SUBJECT_ROOTS = {
    "math": Path("/Users/xiazhibin/Documents/kaoyan-math"),
    "cs408": Path("/Users/xiazhibin/Documents/kaoyan-408"),
    "english": Path("/Users/xiazhibin/Documents/kaoyan-english"),
}
SHA256_CHARS = frozenset("0123456789abcdef")
WRITE_NAME_FRAGMENTS = frozenset(
    {
        "create",
        "update",
        "delete",
        "write",
        "upsert",
        "commit",
        "import",
        "migrate",
        "apply",
        "authoriz",
        "promot",
    }
)


class SolMCPPreflightError(RuntimeError):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(f"{code}:{detail}")
        self.code = code
        self.detail = detail


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and set(value) <= SHA256_CHARS
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _private_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path, 0o700)
    return path.resolve()


def atomic_write(path: Path, raw: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def content_addressed_receipt(
    directory: Path,
    core: Mapping[str, Any],
    *,
    digest_field: str = "receipt_sha256",
) -> tuple[Path, dict[str, Any]]:
    digest = sha256_bytes(canonical_bytes(core))
    value = {**dict(core), digest_field: digest}
    path = directory / f"{digest}.json"
    payload = canonical_bytes(value)
    if path.exists():
        if path.is_symlink() or not path.is_file() or path.read_bytes() != payload:
            raise SolMCPPreflightError(
                "receipt_no_clobber_conflict", str(path)
            )
    else:
        atomic_write(path, payload)
    reopened = json.loads(path.read_text(encoding="utf-8"))
    if reopened != value or sha256_file(path) != sha256_bytes(payload):
        raise SolMCPPreflightError("receipt_reopen_failed", str(path))
    return path, value


def read_root_identity(path: Path) -> str:
    supplied = path.expanduser()
    try:
        supplied_node = supplied.lstat()
        resolved = supplied.resolve(strict=True)
        node = resolved.stat()
    except OSError as exc:
        raise SolMCPPreflightError("database_path_or_permission_error", str(path)) from exc
    if supplied.is_symlink() or not stat.S_ISDIR(supplied_node.st_mode):
        raise SolMCPPreflightError("database_path_or_permission_error", str(path))
    return sha256_bytes(
        canonical_bytes(
            {
                "path": str(resolved),
                "device": int(node.st_dev),
                "inode": int(node.st_ino),
            }
        )
    )


def _safe_regular_file(path: Path, *, expected_sha256: str | None = None) -> Path:
    supplied = path.expanduser()
    try:
        node = supplied.lstat()
        resolved = supplied.resolve(strict=True)
    except OSError as exc:
        raise SolMCPPreflightError("release_closure_missing_asset", str(path)) from exc
    if supplied.is_symlink() or not stat.S_ISREG(node.st_mode):
        raise SolMCPPreflightError("release_closure_missing_asset", str(path))
    if expected_sha256 is not None and sha256_file(resolved) != expected_sha256:
        raise SolMCPPreflightError("release_closure_missing_asset", str(path))
    return resolved


def _safe_executable(path: Path) -> Path:
    supplied = path.expanduser()
    try:
        resolved = supplied.resolve(strict=True)
        node = resolved.stat()
    except OSError as exc:
        raise SolMCPPreflightError(
            "interpreter_or_dependency_error", str(path)
        ) from exc
    if (
        not supplied.is_absolute()
        or not stat.S_ISREG(node.st_mode)
        or not os.access(supplied, os.X_OK)
    ):
        raise SolMCPPreflightError(
            "interpreter_or_dependency_error", str(path)
        )
    # Preserve the venv entrypoint path.  Its relative symlink chain is part of
    # the sealed runtime contract and is required for Python to discover the
    # correct pyvenv.cfg and dependency site-packages directory.
    return supplied


def _validate_offline_runtime(
    *, runtime_root: Path, central_release_id: str,
) -> None:
    current = runtime_root / "current"
    try:
        resolved = current.resolve(strict=True)
        config = json.loads((resolved / "config.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SolMCPPreflightError("cwd_or_env_error", "central runtime unavailable") from exc
    if resolved.name != central_release_id or config.get("execution_mode") != "offline":
        raise SolMCPPreflightError("read_policy_violation", "central runtime is not offline")
    authorization = config.get("live_execution_gate", {}).get(
        "authorization_state_path"
    )
    if isinstance(authorization, str) and Path(authorization).exists():
        raise SolMCPPreflightError(
            "read_policy_violation", "manual authorization is present"
        )
    projection_path = Path(
        str(config.get("dashboard", {}).get("projection_path") or "")
    )
    try:
        projection = json.loads(projection_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SolMCPPreflightError(
            "cwd_or_env_error", "dashboard projection unavailable"
        ) from exc
    dispatchers = projection.get("dispatchers")
    if not isinstance(dispatchers, Mapping) or set(dispatchers) != SUBJECTS:
        raise SolMCPPreflightError(
            "read_policy_violation", "dispatcher safety projection unavailable"
        )
    for subject in SUBJECTS:
        dispatcher = dispatchers.get(subject)
        gate = (
            dispatcher.get("canary_gate")
            if isinstance(dispatcher, Mapping)
            else None
        )
        if (
            not isinstance(gate, Mapping)
            or gate.get("production_accepted") is not False
            or gate.get("active_task_count") != 0
            or gate.get("queue_depth") != 0
            or gate.get("model_call_count") != 0
            or gate.get("provider_request_count") != 0
            or gate.get("mcp_tool_call_count") != 0
            or gate.get("formal_write_count") != 0
        ):
            raise SolMCPPreflightError(
                "read_policy_violation",
                f"{subject} safety projection is not quiescent",
            )
    global_sol = projection.get("global_sol")
    if (
        isinstance(global_sol, Mapping)
        and global_sol.get("formal_write_count") not in {0, None}
    ):
        raise SolMCPPreflightError(
            "read_policy_violation", "central safety projection is not locked"
        )


def _read_component_binding(
    component_lock_path: Path, subject: str
) -> dict[str, Any]:
    path = _safe_regular_file(component_lock_path)
    try:
        lock = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise SolMCPPreflightError(
            "release_closure_missing_asset", "component lock invalid"
        ) from exc
    sealed = lock.get("mcp_sealed_runtime")
    preflight = lock.get("sol_mcp_preflight")
    if (
        not isinstance(lock, dict)
        or lock.get("formal_write_count") != 0
        or not isinstance(sealed, Mapping)
        or not isinstance(preflight, Mapping)
        or preflight.get("purpose") != "infrastructure_preflight"
        or preflight.get("enabled_tools") != list(PREFLIGHT_TOOLS)
        or preflight.get("write_call_count") != 0
        or preflight.get("formal_write_count") != 0
        or subject not in SUBJECTS
    ):
        raise SolMCPPreflightError(
            "release_closure_missing_asset", "preflight component binding invalid"
        )
    required_sealed = {
        "python_executable",
        "release_root",
        "release_id",
        "release_manifest_sha256",
        "sealed_launcher_path",
        "sealed_launcher_sha256",
    }
    if not required_sealed <= set(sealed):
        raise SolMCPPreflightError(
            "release_closure_missing_asset", "sealed MCP binding incomplete"
        )
    return {"lock": lock, "sealed": dict(sealed), "preflight": dict(preflight)}


class JsonRpcLineClient:
    def __init__(self, process: subprocess.Popen[str]) -> None:
        if process.stdin is None or process.stdout is None:
            raise SolMCPPreflightError("initialize_failed", "stdio pipes unavailable")
        self.process = process
        self.stdin = process.stdin
        self.stdout = process.stdout
        self.transcript: list[dict[str, Any]] = []
        self._next_id = 1

    def _record(self, direction: str, message: Mapping[str, Any]) -> None:
        self.transcript.append(
            {
                "sequence": len(self.transcript) + 1,
                "direction": direction,
                "message": dict(message),
            }
        )

    def send_notification(self, method: str, params: Mapping[str, Any]) -> None:
        value = {"jsonrpc": "2.0", "method": method, "params": dict(params)}
        self._record("client_to_server", value)
        self.stdin.write(json.dumps(value, separators=(",", ":")) + "\n")
        self.stdin.flush()

    def request(
        self,
        method: str,
        params: Mapping[str, Any],
        *,
        timeout_seconds: float = 30.0,
    ) -> dict[str, Any]:
        request_id = self._next_id
        self._next_id += 1
        value = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": dict(params),
        }
        self._record("client_to_server", value)
        self.stdin.write(json.dumps(value, separators=(",", ":")) + "\n")
        self.stdin.flush()
        deadline = time.monotonic() + timeout_seconds
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SolMCPPreflightError(
                    "timeout_or_no_progress", f"{method} timed out"
                )
            readable, _writable, _errors = select.select(
                [self.stdout], [], [], remaining
            )
            if not readable:
                raise SolMCPPreflightError(
                    "timeout_or_no_progress", f"{method} timed out"
                )
            line = self.stdout.readline()
            if line == "":
                raise SolMCPPreflightError(
                    "unknown_protocol_error", f"{method} reached EOF"
                )
            try:
                response = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SolMCPPreflightError(
                    "unknown_protocol_error", "non-JSON MCP response"
                ) from exc
            if not isinstance(response, dict):
                raise SolMCPPreflightError(
                    "unknown_protocol_error", "non-object MCP response"
                )
            self._record("server_to_client", response)
            if response.get("id") != request_id:
                continue
            if response.get("jsonrpc") != "2.0" or "error" in response:
                raise SolMCPPreflightError(
                    "unknown_protocol_error",
                    json.dumps(response.get("error"), ensure_ascii=False),
                )
            result = response.get("result")
            if not isinstance(result, dict):
                raise SolMCPPreflightError(
                    "tool_result_schema_mismatch", f"{method} result missing"
                )
            return result


def _tool_list(tools_result: Mapping[str, Any]) -> list[dict[str, Any]]:
    tools = tools_result.get("tools")
    if not isinstance(tools, list) or any(not isinstance(item, dict) for item in tools):
        raise SolMCPPreflightError(
            "tools_list_contract_mismatch", "tools/list is invalid"
        )
    names = [item.get("name") for item in tools]
    if names != list(PREFLIGHT_TOOLS) or len(names) != len(set(names)):
        raise SolMCPPreflightError(
            "tools_list_contract_mismatch", f"unexpected tools: {names}"
        )
    for tool in tools:
        name = str(tool.get("name") or "")
        lowered = name.lower()
        annotations = tool.get("annotations")
        if (
            any(fragment in lowered for fragment in WRITE_NAME_FRAGMENTS)
            or not isinstance(tool.get("inputSchema"), dict)
            or not isinstance(annotations, Mapping)
            or annotations.get("readOnlyHint") is not True
            or annotations.get("destructiveHint") is not False
        ):
            raise SolMCPPreflightError(
                "read_policy_violation", f"unsafe tool contract: {name}"
            )
    return [dict(item) for item in tools]


def _structured_tool_result(
    result: Mapping[str, Any], *, subject: str, session_id: str
) -> dict[str, Any]:
    if result.get("isError") is True:
        raise SolMCPPreflightError(
            "tool_result_schema_mismatch", "MCP tool returned isError"
        )
    structured = result.get("structuredContent")
    if not isinstance(structured, dict):
        raise SolMCPPreflightError(
            "tool_result_schema_mismatch", "structuredContent missing"
        )
    session = structured.get("preflight_session")
    items = structured.get("items")
    if (
        structured.get("ok") is not True
        or structured.get("subject") != subject
        or structured.get("profile") != "infrastructure_preflight"
        or structured.get("formal_write_count") != 0
        or structured.get("model_call_count") != 0
        or structured.get("write_call_count") != 0
        or structured.get("candidate_eligible") is not False
        or structured.get("production_evidence") is not False
        or structured.get("formal_write_allowed") is not False
        or not isinstance(session, Mapping)
        or session.get("preflight_session_id") != session_id
        or session.get("capture_id") != "absent"
        or session.get("production_task_id") != "absent"
        or not isinstance(items, list)
        or any(
            not isinstance(item, Mapping)
            or not str(item.get("evidence_ref") or "").startswith(
                f"mcp-item:{subject}:"
            )
            for item in items
        )
    ):
        raise SolMCPPreflightError(
            "tool_result_schema_mismatch", "preflight result contract mismatch"
        )
    return dict(structured)


def _call_receipt(
    *,
    output_root: Path,
    session_id: str,
    subject: str,
    call_index: int,
    tool_name: str,
    arguments: Mapping[str, Any],
    structured: Mapping[str, Any],
    started_at: str,
    completed_at: str,
) -> tuple[Path, dict[str, Any]]:
    items = structured.get("items")
    page = {
        key: structured.get(key)
        for key in (
            "total_count",
            "returned_count",
            "page_size",
            "complete",
            "truncated",
            "next_cursor",
            "query_sha256",
        )
    }
    core = {
        "schema_version": "sol-mcp-preflight-call-receipt-v1",
        "purpose": "infrastructure_preflight",
        "preflight_session_id": session_id,
        "subject": subject,
        "call_index": call_index,
        "tool_name": tool_name,
        "arguments_sha256": sha256_bytes(canonical_bytes(arguments)),
        "result_sha256": sha256_bytes(canonical_bytes(structured)),
        "result_summary": {
            "ok": structured.get("ok"),
            "subject": structured.get("subject"),
            "profile": structured.get("profile"),
            "generation": structured.get("generation"),
            "authority_fingerprint": structured.get("authority_fingerprint"),
            "item_count": len(items) if isinstance(items, list) else None,
            "page": page,
            "candidate_eligible": False,
            "production_evidence": False,
        },
        "started_at": started_at,
        "completed_at": completed_at,
        "status": "passed",
        "read_call_count": 1,
        "write_call_count": 0,
        "formal_write_count": 0,
    }
    return content_addressed_receipt(output_root / "calls", core)


def run_preflight(
    *,
    subject: str,
    central_release_id: str,
    component_lock_path: Path,
    tool_policy_path: Path,
    runtime_root: Path,
    output_root: Path,
    invocation_mode: str,
    timeout_seconds: float = 30.0,
) -> dict[str, Any]:
    if subject not in SUBJECTS or not is_sha256(central_release_id):
        raise SolMCPPreflightError("tool_argument_schema_mismatch", "invalid subject/release")
    runtime_root = runtime_root.expanduser().resolve(strict=True)
    _validate_offline_runtime(
        runtime_root=runtime_root, central_release_id=central_release_id
    )
    binding = _read_component_binding(component_lock_path, subject)
    sealed = binding["sealed"]
    preflight_binding = binding["preflight"]
    tool_policy = _safe_regular_file(
        tool_policy_path,
        expected_sha256=str(preflight_binding.get("tool_policy_sha256") or ""),
    )
    python_executable = _safe_executable(Path(str(sealed["python_executable"])))
    release_root = Path(str(sealed["release_root"])).resolve(strict=True)
    release_id = str(sealed["release_id"])
    manifest_sha256 = str(sealed["release_manifest_sha256"])
    launcher = _safe_regular_file(
        Path(str(sealed["sealed_launcher_path"])),
        expected_sha256=str(sealed["sealed_launcher_sha256"]),
    )
    release_manifest = _safe_regular_file(
        release_root / "release.json", expected_sha256=manifest_sha256
    )
    try:
        release_value = json.loads(release_manifest.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise SolMCPPreflightError(
            "release_closure_missing_asset", "MCP release manifest invalid"
        ) from exc
    if (
        release_root.name != release_id
        or release_value.get("release_id") != release_id
        or release_value.get("formal_write_count") != 0
        or not isinstance(release_value.get("server_release"), str)
    ):
        raise SolMCPPreflightError(
            "release_closure_missing_asset", "MCP release identity mismatch"
        )
    read_root = SUBJECT_ROOTS[subject].resolve(strict=True)
    private_root = _private_directory(output_root / "private" / subject)
    public_root = _private_directory(output_root / "receipts" / subject)
    started_at = utc_now()
    now = datetime.now(timezone.utc)
    session_core = {
        "schema_version": "study-read-mcp-infrastructure-preflight-session.v1",
        "purpose": "infrastructure_preflight",
        "preflight_session_id": (
            f"MCPPF-{subject.upper()}-{uuid.uuid4().hex[:24].upper()}"
        ),
        "subject": subject,
        "central_release_id": central_release_id,
        "mcp_release_id": release_id,
        "mcp_server_release": release_value["server_release"],
        "mcp_release_manifest_sha256": manifest_sha256,
        "launcher_sha256": sha256_file(launcher),
        "tool_policy_sha256": sha256_file(tool_policy),
        "read_root": str(read_root),
        "read_root_identity_sha256": read_root_identity(read_root),
        "created_at": now.isoformat(),
        "expires_at": (now + timedelta(minutes=10)).isoformat(),
        "capture_id_absent": True,
        "production_task_id_absent": True,
        "candidate_eligible": False,
        "production_evidence": False,
        "formal_write_allowed": False,
        "formal_write_count": 0,
    }
    session_path, session_value = content_addressed_receipt(
        private_root / "sessions",
        session_core,
        digest_field="manifest_sha256",
    )
    command = [
        str(python_executable),
        "-I",
        "-S",
        str(launcher),
        "--release-root",
        str(release_root),
        "--expected-release-id",
        release_id,
        "--expected-release-manifest-sha256",
        manifest_sha256,
        "--mode",
        "preflight-server",
        "--subject",
        subject,
        "--preflight-session-manifest",
        str(session_path),
        "--preprocessor-root",
        str(runtime_root),
        "--tool-policy",
        str(tool_policy),
    ]
    stderr_path = private_root / f"{session_value['manifest_sha256']}.stderr.log"
    transcript_path = private_root / f"{session_value['manifest_sha256']}.protocol.json"
    call_receipts: list[dict[str, Any]] = []
    process: subprocess.Popen[str] | None = None
    client: JsonRpcLineClient | None = None
    pid = 0
    pgid = 0
    start_token = ""
    cleanup_status = "failed"
    cursor_status = "not_applicable"
    try:
        with stderr_path.open("w", encoding="utf-8") as stderr_handle:
            os.chmod(stderr_path, 0o600)
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=stderr_handle,
                text=True,
                encoding="utf-8",
                errors="strict",
                bufsize=1,
                cwd=str(release_root),
                env={"PATH": "/usr/bin:/bin"},
                start_new_session=True,
            )
            pid = int(process.pid)
            pgid = int(os.getpgid(pid))
            if pgid != pid:
                raise SolMCPPreflightError(
                    "cleanup_or_process_leak", "preflight PID/PGID are not isolated"
                )
            start_token = kernel_process_start_token(pid)
            client = JsonRpcLineClient(process)
            initialized = client.request(
                "initialize",
                {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {
                        "name": "study-intake-sol-mcp-preflight",
                        "version": "1.0.0",
                    },
                },
                timeout_seconds=timeout_seconds,
            )
            server_info = initialized.get("serverInfo")
            if (
                not isinstance(server_info, Mapping)
                or server_info.get("name") != f"kaoyan_{subject}_read"
            ):
                raise SolMCPPreflightError(
                    "initialize_failed", "subject MCP server identity mismatch"
                )
            client.send_notification("notifications/initialized", {})
            tools_result = client.request(
                "tools/list", {}, timeout_seconds=timeout_seconds
            )
            _tool_list(tools_result)
            arguments: dict[str, Any] = {
                "collection": SUBJECT_COLLECTIONS[subject],
                "page_size": 1,
            }
            call_started = utc_now()
            first = client.request(
                "tools/call",
                {"name": "list_records", "arguments": arguments},
                timeout_seconds=timeout_seconds,
            )
            structured = _structured_tool_result(
                first,
                subject=subject,
                session_id=session_core["preflight_session_id"],
            )
            call_path, call_receipt = _call_receipt(
                output_root=public_root,
                session_id=session_core["preflight_session_id"],
                subject=subject,
                call_index=1,
                tool_name="list_records",
                arguments=arguments,
                structured=structured,
                started_at=call_started,
                completed_at=utc_now(),
            )
            call_receipts.append(
                {**call_receipt, "path": str(call_path)}
            )
            next_cursor = structured.get("next_cursor")
            if isinstance(next_cursor, str) and next_cursor:
                cursor_arguments = {**arguments, "cursor": next_cursor}
                cursor_started = utc_now()
                continued = client.request(
                    "tools/call",
                    {"name": "list_records", "arguments": cursor_arguments},
                    timeout_seconds=timeout_seconds,
                )
                continued_structured = _structured_tool_result(
                    continued,
                    subject=subject,
                    session_id=session_core["preflight_session_id"],
                )
                cursor_path, cursor_receipt = _call_receipt(
                    output_root=public_root,
                    session_id=session_core["preflight_session_id"],
                    subject=subject,
                    call_index=2,
                    tool_name="list_records",
                    arguments=cursor_arguments,
                    structured=continued_structured,
                    started_at=cursor_started,
                    completed_at=utc_now(),
                )
                call_receipts.append(
                    {**cursor_receipt, "path": str(cursor_path)}
                )
                cursor_status = "passed"
            require_kernel_process_start_token(pid, start_token)
            if process.stdin is not None:
                process.stdin.close()
            process.wait(timeout=10)
            if process.returncode != 0:
                raise SolMCPPreflightError(
                    "cleanup_or_process_leak",
                    f"MCP server exited {process.returncode}",
                )
            cleanup_status = "passed"
    except (ProcessIdentityError, subprocess.TimeoutExpired) as exc:
        raise SolMCPPreflightError(
            "cleanup_or_process_leak", str(exc)
        ) from exc
    finally:
        if client is not None:
            atomic_write(
                transcript_path,
                canonical_bytes(
                    {
                        "schema_version": "sol-mcp-preflight-protocol-transcript-v1",
                        "subject": subject,
                        "invocation_mode": invocation_mode,
                        "messages": client.transcript,
                    }
                ),
            )
        elif not transcript_path.exists():
            atomic_write(
                transcript_path,
                canonical_bytes(
                    {
                        "schema_version": "sol-mcp-preflight-protocol-transcript-v1",
                        "subject": subject,
                        "invocation_mode": invocation_mode,
                        "messages": [],
                    }
                ),
            )
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=3)
            except (OSError, subprocess.TimeoutExpired):
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except OSError:
                    pass
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pass
        if process is not None and process.poll() is None:
            cleanup_status = "failed"
    if cleanup_status != "passed" or process is None or process.poll() is None:
        raise SolMCPPreflightError(
            "cleanup_or_process_leak", "preflight process did not terminate"
        )
    stderr_sha256 = sha256_file(stderr_path)
    transcript_sha256 = sha256_file(transcript_path)
    completed_at = utc_now()
    terminal_core = {
        "schema_version": "sol-mcp-preflight-terminal-v1",
        "purpose": "infrastructure_preflight",
        "preflight_session_id": session_core["preflight_session_id"],
        "subject": subject,
        "central_release_id": central_release_id,
        "mcp_release_id": release_id,
        "launcher_sha256": sha256_file(launcher),
        "tool_policy_sha256": sha256_file(tool_policy),
        "session_manifest_sha256": session_value["manifest_sha256"],
        "protocol_transcript_sha256": transcript_sha256,
        "stderr_sha256": stderr_sha256,
        "pid": pid,
        "pgid": pgid,
        "start_token": start_token,
        "initialize": "passed",
        "tools_list": "passed",
        "read_allowlist": "passed",
        "minimal_read": "passed",
        "result_schema": "passed",
        "subject_scope": "passed",
        "cursor_test": cursor_status,
        "process_cleanup": "passed",
        "receipt_reopen": "passed",
        "status": "passed",
        "read_call_count": len(call_receipts),
        "write_call_count": 0,
        "model_call_count": 0,
        "provider_request_count": 0,
        "live_capture_created_count": 0,
        "live_capture_consumed_count": 0,
        "formal_write_count": 0,
        "started_at": started_at,
        "completed_at": completed_at,
    }
    terminal_path, terminal = content_addressed_receipt(
        public_root / "terminal", terminal_core
    )
    summary = {
        "status": "passed",
        "subject": subject,
        "invocation_mode": invocation_mode,
        "preflight_session_id": session_core["preflight_session_id"],
        "session_manifest_path": str(session_path),
        "session_manifest_sha256": session_value["manifest_sha256"],
        "call_receipts": call_receipts,
        "terminal_receipt_path": str(terminal_path),
        "terminal_receipt_sha256": terminal["receipt_sha256"],
        "protocol_transcript_path": str(transcript_path),
        "protocol_transcript_sha256": transcript_sha256,
        "stderr_path": str(stderr_path),
        "stderr_sha256": stderr_sha256,
        "read_call_count": len(call_receipts),
        "write_call_count": 0,
        "formal_write_count": 0,
        "model_call_count": 0,
        "provider_request_count": 0,
        "live_capture_created_count": 0,
        "live_capture_consumed_count": 0,
    }
    atomic_write(
        public_root / "latest-summary.json", canonical_bytes(summary)
    )
    return summary
