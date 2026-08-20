"""One branch, one child identity, one read session, one launcher process."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Mapping, Sequence

from live_execution_gate import LiveExecutionDenied, assert_external_launch_allowed


class ReadBranchError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def branch_request(plan: Mapping[str, Any], branch: Mapping[str, Any]) -> dict[str, Any]:
    core = {
        "schema_version": "read_branch_request_v1",
        "plan_id": plan["plan_id"],
        "plan_sha256": plan["plan_sha256"],
        "branch_id": branch["branch_id"],
        "subject": plan["subject"],
        "capture_id": plan["capture_id"],
        "frozen_task_sha256": plan["frozen_task_sha256"],
        "release_id": plan["release_id"],
        "activation_id": plan["activation_id"],
        "authority_snapshot_sha256": plan["authority_snapshot_sha256"],
        "generation": plan["generation"],
        "branch": copy.deepcopy(dict(branch)),
        "formal_write_count": 0,
    }
    return {**core, "request_sha256": sha256_value(core)}


def validate_cursor_chain(calls: Sequence[Mapping[str, Any]], *, session_id: str, branch_id: str) -> None:
    expected_sequence = 1
    cursors: set[str] = set()
    for call in calls:
        if (
            call.get("sequence") != expected_sequence
            or call.get("read_session_id") != session_id
            or call.get("branch_id") != branch_id
        ):
            raise ReadBranchError("branch_call_sequence_invalid")
        cursor = call.get("cursor")
        previous = call.get("previous_cursor")
        if previous is not None and previous not in cursors:
            raise ReadBranchError("branch_cursor_provenance_invalid")
        if cursor is not None:
            if not isinstance(cursor, str) or not cursor or cursor in cursors:
                raise ReadBranchError("branch_cursor_invalid")
            cursors.add(cursor)
        expected_sequence += 1


class SubprocessBranchWorker:
    """Run a fixture or authorized live branch through an isolated process."""

    def __init__(
        self,
        *,
        execution_config: Mapping[str, Any],
        command: Sequence[str],
        purpose: str = "fake_branch_worker",
        timeout_seconds: float = 30.0,
        authorization: Mapping[str, Any] | None = None,
    ) -> None:
        self.execution_config = copy.deepcopy(dict(execution_config))
        self.command = tuple(command)
        self.purpose = purpose
        self.timeout_seconds = timeout_seconds
        self.authorization = copy.deepcopy(dict(authorization)) if isinstance(authorization, Mapping) else None

    def run(
        self,
        request: Mapping[str, Any],
        *,
        wave_index: int,
        cancellation: threading.Event,
    ) -> dict[str, Any]:
        if cancellation.is_set():
            return self._terminal(request, "cancelled", wave_index, error_code="branch_cancelled_before_start")
        task_identity = {
            key: request[key]
            for key in ("subject", "capture_id", "release_id", "activation_id")
        }
        task_identity["capture_content_sha256"] = request["frozen_task_sha256"]
        try:
            gate = assert_external_launch_allowed(
                self.execution_config,
                purpose=self.purpose,
                command=self.command,
                task_identity=task_identity,
                authorization=self.authorization,
            )
        except LiveExecutionDenied as exc:
            raise ReadBranchError(exc.code) from exc
        started = time.monotonic()
        child_identity = "agent-" + uuid.uuid4().hex
        read_session_id = "MCPRS-" + uuid.uuid4().hex
        try:
            process = subprocess.Popen(
                list(self.command),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
        except OSError as exc:
            raise ReadBranchError("branch_process_launch_failed") from exc
        try:
            payload = {
                "request": copy.deepcopy(dict(request)),
                "wave_index": wave_index,
                "child_agent_id": child_identity,
                "read_session_id": read_session_id,
            }
            try:
                stdout, stderr = process.communicate(canonical_bytes(payload), timeout=self.timeout_seconds)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, 15)
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, 9)
                    process.wait(timeout=2)
                return self._terminal(
                    request, "timed_out", wave_index,
                    child_identity=child_identity, read_session_id=read_session_id,
                    launcher_pid=process.pid, error_code="branch_timeout",
                    duration_ms=round((time.monotonic() - started) * 1000, 3),
                )
            if process.returncode != 0:
                return self._terminal(
                    request, "failed", wave_index,
                    child_identity=child_identity, read_session_id=read_session_id,
                    launcher_pid=process.pid, error_code="branch_process_failed",
                    duration_ms=round((time.monotonic() - started) * 1000, 3),
                    stderr_sha256=hashlib.sha256(stderr).hexdigest(),
                )
            try:
                output = json.loads(stdout.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ReadBranchError("branch_output_invalid") from exc
            if not isinstance(output, Mapping):
                raise ReadBranchError("branch_output_invalid")
            calls = output.get("calls")
            if not isinstance(calls, list):
                raise ReadBranchError("branch_calls_invalid")
            validate_cursor_chain(calls, session_id=read_session_id, branch_id=str(request["branch_id"]))
            core = {
                "schema_version": "read_branch_result_v1",
                "plan_id": request["plan_id"],
                "plan_sha256": request["plan_sha256"],
                "request_sha256": request["request_sha256"],
                "branch_id": request["branch_id"],
                "subject": request["subject"],
                "capture_id": request["capture_id"],
                "frozen_task_sha256": request["frozen_task_sha256"],
                "release_id": request["release_id"],
                "activation_id": request["activation_id"],
                "authority_snapshot_sha256": request["authority_snapshot_sha256"],
                "generation": request["generation"],
                "status": "succeeded",
                "required": request["branch"]["required"],
                "purpose": request["branch"]["purpose"],
                "child_agent_id": child_identity,
                "parent_agent_id": output.get("parent_agent_id", "terra-parent-fixture"),
                "read_session_id": read_session_id,
                "mcp_launcher_pid": process.pid,
                "mcp_launcher_pgid": process.pid,
                "wave_index": wave_index,
                "calls": copy.deepcopy(calls),
                "evidence": copy.deepcopy(list(output.get("evidence") or [])),
                "findings": copy.deepcopy(list(output.get("findings") or [])),
                "conflicts": copy.deepcopy(list(output.get("conflicts") or [])),
                "missing_evidence": copy.deepcopy(list(output.get("missing_evidence") or [])),
                "pagination_closed": output.get("pagination_closed") is True,
                "duration_ms": round((time.monotonic() - started) * 1000, 3),
                "gate_decision": gate,
                "formal_write_count": 0,
            }
            return {**core, "result_sha256": sha256_value(core)}
        finally:
            if process.poll() is None:
                os.killpg(process.pid, 15)
                process.wait(timeout=2)
            for stream in (process.stdin, process.stdout, process.stderr):
                if stream is not None and not stream.closed:
                    stream.close()

    @staticmethod
    def _terminal(
        request: Mapping[str, Any], status: str, wave_index: int, *,
        child_identity: str | None = None, read_session_id: str | None = None,
        launcher_pid: int | None = None, error_code: str | None = None,
        duration_ms: float = 0.0, stderr_sha256: str | None = None,
    ) -> dict[str, Any]:
        core = {
            "schema_version": "read_branch_result_v1",
            "plan_id": request["plan_id"],
            "plan_sha256": request["plan_sha256"],
            "request_sha256": request["request_sha256"],
            "branch_id": request["branch_id"],
            "subject": request["subject"],
            "capture_id": request["capture_id"],
            "frozen_task_sha256": request["frozen_task_sha256"],
            "release_id": request["release_id"],
            "activation_id": request["activation_id"],
            "authority_snapshot_sha256": request["authority_snapshot_sha256"],
            "generation": request["generation"],
            "status": status,
            "required": request["branch"]["required"],
            "purpose": request["branch"]["purpose"],
            "child_agent_id": child_identity,
            "parent_agent_id": "terra-parent-fixture",
            "read_session_id": read_session_id,
            "mcp_launcher_pid": launcher_pid,
            "mcp_launcher_pgid": launcher_pid,
            "wave_index": wave_index,
            "calls": [], "evidence": [], "findings": [], "conflicts": [],
            "missing_evidence": [], "pagination_closed": False,
            "duration_ms": duration_ms, "error_code": error_code,
            "stderr_sha256": stderr_sha256, "formal_write_count": 0,
        }
        return {**core, "result_sha256": sha256_value(core)}
