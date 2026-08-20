#!/usr/bin/env python3
"""Run an isolated, read-only Luna Max validation selection without a cap.

The selection manifest decides the number of independent calls.  Every selected
task is released from one barrier and gets its own process group, schema, result,
and receipt.  No study adapter or formal writer is imported or invoked.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Mapping, Sequence


SELECTION_SCHEMA = "study-intake-read-only-validation-selection-v1"
RECEIPT_SCHEMA = "study-intake-read-only-luna-validation-receipt-v1"
SUMMARY_SCHEMA = "study-intake-read-only-luna-validation-summary-v1"
REQUIRED_MODEL = "gpt-5.6-luna"
REQUIRED_REASONING_EFFORT = "max"
SUBJECTS = {"math", "cs408", "english"}
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")
MAX_EVENT_BYTES = 16 * 1024 * 1024
FORMAL_GUARD_SCRIPT = Path(__file__).resolve().parents[1] / "bin" / "formal_surface_manifest.py"


class ValidationError(RuntimeError):
    pass


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _atomic_json(path: Path, value: Mapping[str, Any], *, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, raw_temp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temp = Path(raw_temp)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_canonical_bytes(value))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _load_selection(path: Path) -> list[dict[str, str]]:
    try:
        if path.is_symlink() or not path.is_file():
            raise ValidationError("selection_manifest_invalid")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationError("selection_manifest_invalid") from exc
    tasks = value.get("tasks") if isinstance(value, Mapping) else None
    if (
        not isinstance(value, Mapping)
        or value.get("schema_version") != SELECTION_SCHEMA
        or value.get("formal_write_count") != 0
        or not isinstance(tasks, list)
        or not tasks
    ):
        raise ValidationError("selection_manifest_invalid")
    selected: list[dict[str, str]] = []
    identities: set[str] = set()
    for task in tasks:
        task_id = task.get("task_id") if isinstance(task, Mapping) else None
        subject = task.get("subject") if isinstance(task, Mapping) else None
        if (
            not isinstance(task, Mapping)
            or set(task) != {"task_id", "subject"}
            or not isinstance(task_id, str)
            or SAFE_ID.fullmatch(task_id) is None
            or subject not in SUBJECTS
        ):
            raise ValidationError("selection_task_invalid")
        identity = _sha256(_canonical_bytes({"task_id": task_id, "subject": subject}))
        if identity in identities:
            raise ValidationError("selection_task_duplicate")
        identities.add(identity)
        selected.append({"task_id": task_id, "subject": str(subject)})
    return selected


def _safe_output_root(path: Path) -> Path:
    candidate = path.expanduser().absolute()
    if candidate in {Path("/"), Path.home()} or candidate.is_symlink():
        raise ValidationError("output_root_invalid")
    candidate.mkdir(parents=True, exist_ok=True, mode=0o700)
    resolved = candidate.resolve()
    if any(resolved.iterdir()):
        raise ValidationError("output_root_not_empty")
    os.chmod(resolved, 0o700)
    return resolved


def _verify_formal_surfaces(config_path: Path, baseline_path: Path) -> dict[str, Any]:
    config = config_path.expanduser().resolve()
    baseline = baseline_path.expanduser().resolve()
    if (
        not FORMAL_GUARD_SCRIPT.is_file()
        or config.is_symlink()
        or not config.is_file()
        or baseline.is_symlink()
        or not baseline.is_file()
    ):
        raise ValidationError("formal_surface_guard_input_invalid")
    try:
        guard_config = json.loads(config.read_text(encoding="utf-8"))
        release_template = json.loads(
            (FORMAL_GUARD_SCRIPT.parent.parent / "config.example.json").read_text(
                encoding="utf-8"
            )
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationError("formal_surface_guard_input_invalid") from exc
    guard_adapters = guard_config.get("adapters")
    template_adapters = release_template.get("adapters")
    if not isinstance(guard_adapters, Mapping) or not isinstance(
        template_adapters, Mapping
    ):
        raise ValidationError("formal_surface_guard_binding_mismatch")
    for subject in ("math", "cs408", "english"):
        guard_adapter = guard_adapters.get(subject)
        template_adapter = template_adapters.get(subject)
        if (
            not isinstance(guard_adapter, Mapping)
            or not isinstance(template_adapter, Mapping)
            or guard_adapter.get("repo_root") != template_adapter.get("repo_root")
        ):
            raise ValidationError("formal_surface_guard_binding_mismatch")
    guard_snapshot = guard_config.get("cs408_knowledge_snapshot")
    template_snapshot = release_template.get("cs408_knowledge_snapshot")
    if (
        not isinstance(guard_snapshot, Mapping)
        or not isinstance(template_snapshot, Mapping)
        or guard_snapshot.get("sources") != template_snapshot.get("sources")
    ):
        raise ValidationError("formal_surface_guard_binding_mismatch")
    completed = subprocess.run(
        [
            sys.executable,
            "-B",
            str(FORMAL_GUARD_SCRIPT),
            "verify",
            "--config",
            str(config),
            "--baseline",
            str(baseline),
            "--all-subjects",
        ],
        cwd=FORMAL_GUARD_SCRIPT.parent.parent,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if (
        completed.returncode != 0
        or len(completed.stdout) > 2 * 1024 * 1024
        or len(completed.stderr) > 64 * 1024
    ):
        raise ValidationError("formal_surface_guard_failed")
    try:
        value = json.loads(completed.stdout.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationError("formal_surface_guard_invalid") from exc
    if (
        not isinstance(value, Mapping)
        or value.get("subject_scope") != ["math", "cs408", "english"]
        or value.get("status") != "unchanged"
        or value.get("differences") != {}
        or value.get("formal_write_count") != 0
        or not isinstance(value.get("baseline_manifest_sha256"), str)
        or not isinstance(value.get("current_manifest_sha256"), str)
        or value["baseline_manifest_sha256"] != value["current_manifest_sha256"]
    ):
        raise ValidationError("formal_surface_guard_changed")
    return {
        "status": "unchanged",
        "manifest_sha256": value["current_manifest_sha256"],
        "config_file_sha256": _sha256(config.read_bytes()),
        "baseline_file_sha256": _sha256(baseline.read_bytes()),
        "formal_write_count": 0,
    }


def _task_schema(task: Mapping[str, str]) -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "additionalProperties": False,
        "required": ["task_id", "subject", "status"],
        "properties": {
            "task_id": {"type": "string", "const": task["task_id"]},
            "subject": {"type": "string", "const": task["subject"]},
            "status": {"type": "string", "const": "validated"},
        },
    }


def _prompt(task: Mapping[str, str]) -> str:
    return (
        "This is an isolated read-only runtime validation. Do not use tools. "
        "Return only the JSON object required by the supplied schema: "
        + json.dumps(
            {
                "task_id": task["task_id"],
                "subject": task["subject"],
                "status": "validated",
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


def _terminate_group(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _command(
    *, codex_path: Path, task_root: Path, schema_path: Path, result_path: Path
) -> list[str]:
    return [
        str(codex_path),
        "exec",
        "--strict-config",
        "--ephemeral",
        "--ignore-user-config",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--cd",
        str(task_root),
        "--model",
        REQUIRED_MODEL,
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
        "--output-schema",
        str(schema_path),
        "--output-last-message",
        str(result_path),
        "--json",
        "-",
    ]


def _validate_result(path: Path, task: Mapping[str, str]) -> tuple[str, int]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024:
            raise ValidationError("validation_result_invalid")
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationError("validation_result_invalid") from exc
    if value != {
        "task_id": task["task_id"],
        "subject": task["subject"],
        "status": "validated",
    }:
        raise ValidationError("validation_result_invalid")
    return _sha256(raw), len(raw)


def _run_one(
    task: Mapping[str, str],
    *,
    index: int,
    count: int,
    output_root: Path,
    codex_path: Path,
    timeout_seconds: int,
    ready: threading.Event,
    release: threading.Event,
) -> dict[str, Any]:
    task_root = output_root / "tasks" / f"{index:04d}-{task['task_id']}"
    task_root.mkdir(parents=True, exist_ok=False, mode=0o700)
    schema_path = task_root / "output.schema.json"
    result_path = task_root / "result.json"
    receipt_path = task_root / "receipt.json"
    schema = _task_schema(task)
    _atomic_json(schema_path, schema)
    prompt = _prompt(task)
    command = _command(
        codex_path=codex_path,
        task_root=task_root,
        schema_path=schema_path,
        result_path=result_path,
    )
    ready.set()
    if not release.wait(timeout=30):
        raise ValidationError("validation_release_timeout")
    submitted_at = _utc_now()
    started = time.monotonic()
    environment = dict(os.environ)
    environment.update(
        {
            "PYTHONDONTWRITEBYTECODE": "1",
            "STUDY_VALIDATION_EXPECTED_COUNT": str(count),
            "STUDY_VALIDATION_BARRIER_ROOT": str(output_root / "child-barrier"),
            "STUDY_VALIDATION_TASK_ID": task["task_id"],
            "STUDY_VALIDATION_SUBJECT": task["subject"],
        }
    )
    process: subprocess.Popen[bytes] | None = None
    stdout = b""
    stderr = b""
    status = "failed"
    error_code: str | None = None
    result_sha256: str | None = None
    result_bytes = 0
    try:
        process = subprocess.Popen(
            command,
            cwd=task_root,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        try:
            stdout, stderr = process.communicate(
                input=prompt.encode("utf-8"), timeout=timeout_seconds
            )
        except subprocess.TimeoutExpired:
            _terminate_group(process)
            stdout, stderr = process.communicate()
            error_code = "validation_timeout"
        if len(stdout) > MAX_EVENT_BYTES or len(stderr) > MAX_EVENT_BYTES:
            error_code = "validation_event_stream_too_large"
        elif process.returncode != 0 and error_code is None:
            error_code = f"validation_exit_{process.returncode}"
        elif error_code is None:
            result_sha256, result_bytes = _validate_result(result_path, task)
            status = "validated"
    except OSError as exc:
        error_code = f"validation_spawn_errno_{exc.errno}"
    except ValidationError as exc:
        error_code = str(exc)
    duration_ms = max(0, int((time.monotonic() - started) * 1000))
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "task_id": task["task_id"],
        "subject": task["subject"],
        "status": status,
        "error_code": error_code,
        "submitted_at": submitted_at,
        "finished_at": _utc_now(),
        "duration_ms": duration_ms,
        "requested_model": REQUIRED_MODEL,
        "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
        "runtime_identity_status": "requested_unverified",
        "runtime_model": None,
        "runtime_reasoning_effort": None,
        "command_sha256": _sha256(_canonical_bytes(command)),
        "prompt_sha256": _sha256(prompt.encode("utf-8")),
        "schema_sha256": _sha256(_canonical_bytes(schema)),
        "result_sha256": result_sha256,
        "result_bytes": result_bytes,
        "event_stdout_sha256": _sha256(stdout),
        "event_stderr_sha256": _sha256(stderr),
        "model_call_count": 1 if process is not None else 0,
        "formal_write_count": 0,
    }
    _atomic_json(receipt_path, receipt)
    return {**receipt, "receipt_path": str(receipt_path)}


def run_validation(
    *,
    selection_path: Path,
    output_root: Path,
    codex_path: Path,
    timeout_seconds: int,
    formal_config: Path,
    formal_baseline: Path,
) -> dict[str, Any]:
    formal_before = _verify_formal_surfaces(formal_config, formal_baseline)
    try:
        tasks = _load_selection(selection_path.expanduser().resolve())
        root = _safe_output_root(output_root)
        executable = codex_path.expanduser().resolve()
        if not executable.is_file() or not os.access(executable, os.X_OK):
            raise ValidationError("codex_not_executable")
        if not 30 <= timeout_seconds <= 1800:
            raise ValidationError("timeout_invalid")
        release = threading.Event()
        ready = [threading.Event() for _task in tasks]
        receipts: list[dict[str, Any] | None] = [None] * len(tasks)
        errors: list[BaseException] = []

        def invoke(index: int, task: Mapping[str, str]) -> None:
            try:
                receipts[index] = _run_one(
                    task,
                    index=index,
                    count=len(tasks),
                    output_root=root,
                    codex_path=executable,
                    timeout_seconds=timeout_seconds,
                    ready=ready[index],
                    release=release,
                )
            except BaseException as exc:
                errors.append(exc)

        threads = [
            threading.Thread(
                target=invoke,
                args=(index, task),
                name=f"read-only-luna-{task['task_id']}",
            )
            for index, task in enumerate(tasks)
        ]
        try:
            for thread in threads:
                thread.start()
        except RuntimeError as exc:
            release.set()
            for thread in threads:
                if thread.ident is not None:
                    thread.join()
            raise ValidationError("validation_thread_resource_unavailable") from exc
        deadline = time.monotonic() + 30
        for ready_event in ready:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not ready_event.wait(remaining):
                release.set()
                for thread in threads:
                    thread.join()
                raise ValidationError("validation_thread_setup_failed")
        if errors:
            release.set()
            for thread in threads:
                thread.join()
            raise ValidationError("validation_thread_setup_failed")
        released_at = _utc_now()
        release.set()
        for thread in threads:
            thread.join()
        if errors or any(receipt is None for receipt in receipts):
            raise ValidationError("validation_thread_failed")
        completed = [dict(receipt) for receipt in receipts if receipt is not None]
        submitted = [
            dt.datetime.fromisoformat(receipt["submitted_at"].replace("Z", "+00:00"))
            for receipt in completed
        ]
        start_spread_ms = int(
            (max(submitted) - min(submitted)).total_seconds() * 1000
        )
        summary = {
            "schema_version": SUMMARY_SCHEMA,
            "status": (
                "validated"
                if all(receipt["status"] == "validated" for receipt in completed)
                else "failed"
            ),
            "selection_sha256": _sha256(selection_path.resolve().read_bytes()),
            "selected_task_count": len(tasks),
            "started_task_count": sum(
                int(receipt["model_call_count"]) for receipt in completed
            ),
            "validated_task_count": sum(
                receipt["status"] == "validated" for receipt in completed
            ),
            "release_barrier_at": released_at,
            "start_spread_ms": start_spread_ms,
            "requested_model": REQUIRED_MODEL,
            "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
            "concurrency_policy": "all_selected_tasks_released_together_no_cap",
            "receipts": completed,
            "formal_write_count": 0,
        }
    finally:
        formal_after = _verify_formal_surfaces(formal_config, formal_baseline)
    if formal_before["manifest_sha256"] != formal_after["manifest_sha256"]:
        raise ValidationError("formal_surface_guard_changed")
    summary["formal_surface_guard"] = {
        "before": formal_before,
        "after": formal_after,
        "status": "unchanged",
    }
    _atomic_json(root / "summary.json", summary)
    return summary


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--selection", type=Path, required=True)
    value.add_argument("--output-root", type=Path, required=True)
    value.add_argument(
        "--codex-path",
        type=Path,
        default=Path("/Applications/ChatGPT.app/Contents/Resources/codex"),
    )
    value.add_argument("--timeout-seconds", type=int, default=900)
    value.add_argument("--formal-config", type=Path, required=True)
    value.add_argument("--formal-baseline", type=Path, required=True)
    return value


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = run_validation(
            selection_path=args.selection,
            output_root=args.output_root,
            codex_path=args.codex_path,
            timeout_seconds=args.timeout_seconds,
            formal_config=args.formal_config,
            formal_baseline=args.formal_baseline,
        )
    except ValidationError as exc:
        print(
            json.dumps(
                {
                    "schema_version": SUMMARY_SCHEMA,
                    "status": "failed",
                    "error_code": str(exc),
                    "requested_model": REQUIRED_MODEL,
                    "requested_reasoning_effort": REQUIRED_REASONING_EFFORT,
                    "formal_write_count": 0,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "validated" else 1


if __name__ == "__main__":
    raise SystemExit(main())
