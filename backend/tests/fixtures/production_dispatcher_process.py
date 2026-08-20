#!/usr/bin/env python3
"""Run one production-canary subject scanner in its own zero-model process.

The fixture deliberately keeps the canonical producer scanner and
ProductionDispatchRuntime.  Only the task runner is replaced with the
blocking protocol fixture so a test can observe all task processes at the
same barrier without opening MCP or starting a Provider.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

from core_dispatch_bridge import CoreCandidateSubprocessRunner  # noqa: E402
from preprocess_dispatcher import ProductionDispatchRuntime  # noqa: E402
from processing_plugin import ProcessingPluginHost  # noqa: E402
import preprocessor_core as core  # noqa: E402


def _atomic_json(path: Path, value: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{time.monotonic_ns()}.tmp"
    )
    temporary.write_text(
        json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _load_object(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError("dispatcher_fixture_config_invalid")
    return value


def _forbid_no_fast_mode_regressions(value: object) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            normalized = str(key).lower().replace("-", "_").replace(".", "_")
            if normalized == "service_tier":
                raise RuntimeError("dispatcher_fixture_service_tier_forbidden")
            _forbid_no_fast_mode_regressions(item)
    elif isinstance(value, list):
        for item in value:
            _forbid_no_fast_mode_regressions(item)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--subject", required=True, choices=("math", "cs408", "english"))
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--marker-root", required=True, type=Path)
    parser.add_argument("--runner", required=True, type=Path)
    args = parser.parse_args()

    config_path = args.config.resolve()
    marker_root = args.marker_root.resolve()
    marker_path = marker_root / "dispatchers" / f"{args.subject}.json"
    start_barrier = marker_root / "dispatcher-start"
    stop = threading.Event()
    guard_counts = {
        "model_call_count": 0,
        "provider_request_count": 0,
        "mcp_call_count": 0,
        "real_mcp_request_count": 0,
        "local_authority_snapshot_subprocess_count": 0,
        "unexpected_mcp_call_count": 0,
        "sol_call_count": 0,
        "formal_write_count": 0,
    }

    def _stop(_signum: int, _frame: object) -> None:
        stop.set()

    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, _stop)

    def _forbid_model_call(self, *unused_args, **unused_kwargs):
        guard_counts["model_call_count"] += 1
        guard_counts["provider_request_count"] += 1
        raise RuntimeError("zero_model_dispatcher_provider_call_forbidden")

    config = _load_object(config_path)
    _forbid_no_fast_mode_regressions(config)
    model_config = config.get("model")
    if (
        not isinstance(model_config, dict)
        or model_config.get("codex_path") != "/usr/bin/false"
    ):
        raise RuntimeError("dispatcher_fixture_provider_fail_closed_missing")
    processing_plugin = config.get("processing_plugin")
    if not isinstance(processing_plugin, dict):
        raise RuntimeError("dispatcher_fixture_processing_plugin_missing")
    lock_path = Path(
        str(processing_plugin.get("component_lock_path") or "")
    )
    component_lock = _load_object(lock_path)
    sealed_runtime = component_lock.get("mcp_sealed_runtime")
    if not isinstance(sealed_runtime, dict):
        raise RuntimeError("dispatcher_fixture_sealed_runtime_invalid")
    allowed_python = str(sealed_runtime.get("python_executable") or "")
    allowed_root = str(sealed_runtime.get("release_root") or "")
    allowed_launcher = str(sealed_runtime.get("sealed_launcher_path") or "")
    allowed_manifest_sha256 = str(
        sealed_runtime.get("release_manifest_sha256") or ""
    )
    allowed_launcher_sha256 = str(
        sealed_runtime.get("sealed_launcher_sha256") or ""
    )
    allowed_release_id = str(sealed_runtime.get("release_id") or "")
    allowed_root_path = Path(allowed_root)
    allowed_launcher_path = Path(allowed_launcher)
    manifest_path = allowed_root_path / "release.json"
    if (
        processing_plugin.get("mcp_client_python") != allowed_python
        or processing_plugin.get("mcp_project_root") != allowed_root
        or sealed_runtime.get("python_flags") != ["-I", "-S"]
        or not Path(allowed_python).is_file()
        or not allowed_root_path.is_dir()
        or allowed_root_path.name != allowed_release_id
        or allowed_launcher_path != allowed_root_path / "scripts/sealed_launcher.py"
        or not allowed_launcher_path.is_file()
        or not manifest_path.is_file()
        or hashlib.sha256(allowed_launcher_path.read_bytes()).hexdigest()
        != allowed_launcher_sha256
        or hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        != allowed_manifest_sha256
    ):
        raise RuntimeError("dispatcher_fixture_sealed_authority_runtime_invalid")
    original_mcp_call = ProcessingPluginHost._call

    def _guarded_mcp_call(self, subject, tool, arguments):
        if (
            tool != "authority_bundle"
            or subject != args.subject
            or str(self.python_path) != allowed_python
            or str(self.mcp_project_root) != allowed_root
            or str(self.mcp_sealed_launcher_path) != allowed_launcher
            or self._sealed_runtime_binding() != sealed_runtime
            or guard_counts["local_authority_snapshot_subprocess_count"] != 0
        ):
            guard_counts["unexpected_mcp_call_count"] += 1
            raise RuntimeError("zero_model_dispatcher_mcp_call_forbidden")
        guard_counts["local_authority_snapshot_subprocess_count"] += 1
        return original_mcp_call(self, subject, tool, arguments)

    # Worker construction is required for the canonical producer adapters,
    # but its model runner must never execute in this topology test.  The sole
    # MCP-shaped subprocess allowed is the component-lock-bound sealed local
    # authority snapshot required by the production batch-generation fence.
    core.CodexRunner.run = _forbid_model_call
    ProcessingPluginHost._call = _guarded_mcp_call
    runtime: ProductionDispatchRuntime | None = None
    heartbeat_sequence = 0
    base: dict[str, object] = {
        "schema_version": "zero-model-production-dispatcher-process-v1",
        "subject": args.subject,
        "pid": os.getpid(),
        "pgid": os.getpgid(0),
        "config_path": str(config_path),
        "config_sha256": core.sha256_file(config_path),
        "fixture_scope": "temp_zero_model_only",
        "default_worker_scanner": True,
        "scan_worker_factory": None,
        "sol_enabled": False,
        **guard_counts,
    }
    try:
        runtime = ProductionDispatchRuntime(config, args.subject, config_path)
        if getattr(runtime, "_scan_worker_factory", None) is not None:
            raise RuntimeError("dispatcher_fixture_noncanonical_scanner")
        store = runtime.dispatcher.lease_store
        gate = store.production_canary_status_read_only(args.subject)
        if not isinstance(gate, dict):
            raise RuntimeError("dispatcher_fixture_canary_missing")
        if gate.get("state") != "continuous_concurrent_unlocked":
            raise RuntimeError("dispatcher_fixture_canary_not_unlocked")
        if gate.get("luna_consumer_enabled") is not True:
            raise RuntimeError("dispatcher_fixture_consumer_disabled")
        if int(gate.get("continuous_concurrency_limit") or 0) != 20:
            raise RuntimeError("dispatcher_fixture_concurrency_limit_invalid")

        runtime.dispatcher.runner_factory = lambda _task, _context: (
            CoreCandidateSubprocessRunner(
                config_path,
                command=[sys.executable, str(args.runner.resolve())],
                lease_store=store,
            )
        )
        base.update(
            {
                "status": "waiting_for_dispatcher_barrier",
                "release_id": core.Worker(config).release_id,
                "activation_id": gate["activation_id"],
                "producer_authority_fingerprint": gate[
                    "producer_authority_fingerprint"
                ],
                "dispatcher_owner_id": runtime.dispatcher.owner_id,
                "effective_concurrency_limit": gate[
                    "continuous_concurrency_limit"
                ],
                "heartbeat_sequence": heartbeat_sequence,
                "heartbeat_monotonic_ns": time.monotonic_ns(),
                **guard_counts,
            }
        )
        _atomic_json(marker_path, base)

        while not start_barrier.exists() and not stop.wait(0.02):
            heartbeat_sequence += 1
            base.update(
                {
                    "heartbeat_sequence": heartbeat_sequence,
                    "heartbeat_monotonic_ns": time.monotonic_ns(),
                    **guard_counts,
                }
            )
            _atomic_json(marker_path, base)
        if stop.is_set():
            return 0

        handles, decisions = runtime.scan_and_submit()
        units = [handle.unit_sha256 for handle in handles]
        base.update(
            {
                "status": "running",
                "submitted_task_count": len(handles),
                "submitted_units": units,
                "decision_count": len(decisions),
                "decision_reasons": sorted(
                    {
                        str(row.get("reason"))
                        for row in decisions
                        if isinstance(row, dict)
                    }
                ),
                "eligible_decision_count": sum(
                    1
                    for row in decisions
                    if isinstance(row, dict) and row.get("eligible") is True
                ),
                "gate_after_scan": store.production_canary_status_read_only(
                    args.subject
                ),
                "heartbeat_sequence": heartbeat_sequence,
                "heartbeat_monotonic_ns": time.monotonic_ns(),
                **guard_counts,
            }
        )
        _atomic_json(marker_path, base)

        while not stop.wait(0.05):
            heartbeat_sequence += 1
            base.update(
                {
                    "heartbeat_sequence": heartbeat_sequence,
                    "heartbeat_monotonic_ns": time.monotonic_ns(),
                    "active_task_count": runtime.dispatcher.active_count,
                    **guard_counts,
                }
            )
            _atomic_json(marker_path, base)

        shutdown = runtime.dispatcher.emergency_cancel(
            timeout=20.0,
            error_code="zero_model_dispatcher_process_shutdown",
        )
        heartbeat_sequence += 1
        base.update(
            {
                "status": "drained",
                "heartbeat_sequence": heartbeat_sequence,
                "heartbeat_monotonic_ns": time.monotonic_ns(),
                "active_task_count": runtime.dispatcher.active_count,
                "shutdown": shutdown,
                **guard_counts,
            }
        )
        _atomic_json(marker_path, base)
        return 0
    except BaseException as exc:
        if runtime is not None:
            try:
                runtime.dispatcher.emergency_cancel(
                    timeout=20.0,
                    error_code="zero_model_dispatcher_fixture_failed",
                )
            except BaseException:
                pass
        base.update(
            {
                "status": "failed",
                "error_type": type(exc).__name__,
                "error": str(exc),
                "heartbeat_sequence": heartbeat_sequence,
                "heartbeat_monotonic_ns": time.monotonic_ns(),
                **guard_counts,
            }
        )
        _atomic_json(marker_path, base)
        raise


if __name__ == "__main__":
    raise SystemExit(main())
