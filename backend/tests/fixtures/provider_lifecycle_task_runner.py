#!/usr/bin/env python3
"""Temp-only supervisor using production cancellation and CodexRunner code."""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

from concurrent_dispatch import FrozenTask, Lease, LeaseStore  # noqa: E402
from preprocess_task_runner import (  # noqa: E402
    _CancellationCoordinator,
    _LivenessProbeCoordinator,
)
from preprocessor_core import CodexRunner  # noqa: E402


parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True, type=Path)
args = parser.parse_args()
request = json.loads(sys.stdin.buffer.read())
task = FrozenTask.from_mapping(request["task"])
lease = Lease(
    task.unit_sha256,
    str(request["lease_owner_id"]),
    int(request["lease_fence"]),
)
runtime_root = Path(os.environ["STUDY_PREPROCESS_RUNTIME_ROOT"])
store = LeaseStore(runtime_root)
store.verify_task_process_identity(
    task,
    lease,
    expected_sha256=str(request["supervisor_process_identity_sha256"]),
    expected_path=str(request["supervisor_process_identity_path"]),
    expected_launch_nonce=str(request["supervisor_launch_nonce"]),
)
config = json.loads(args.config.read_text(encoding="utf-8"))
runner = CodexRunner(config["model"], runtime_root)
runner.bind_dispatch_process_lifecycle(
    task=task, lease=lease, lease_store=store
)
cancellation = _CancellationCoordinator()
cancellation.bind(runner)
liveness = _LivenessProbeCoordinator()
liveness.bind(runner)


def request_cancel(_signum: int, _frame: object) -> None:
    cancellation.request()


def request_liveness(_signum: int, _frame: object) -> None:
    liveness.request()


previous = {
    signum: signal.signal(signum, request_cancel)
    for signum in (signal.SIGTERM, signal.SIGINT)
}
previous[signal.SIGUSR1] = signal.signal(
    signal.SIGUSR1, request_liveness
)
try:
    command = [str(Path(config["model"]["codex_path"])), "exec"]
    runner._invoke_subprocess(
        command,
        input=b"zero-model provider lifecycle fixture\n",
        timeout=300,
        cwd=Path(os.environ["STUDY_PREPROCESS_CONTEXT_ROOT"]),
        stage_name="english_analysis",
    )
    raise SystemExit(92)
except BaseException:
    if cancellation.requested.is_set():
        cancellation.join()
    raise
finally:
    liveness.close()
    for signum, handler in previous.items():
        signal.signal(signum, handler)
