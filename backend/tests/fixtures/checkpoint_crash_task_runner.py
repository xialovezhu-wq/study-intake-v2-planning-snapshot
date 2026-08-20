#!/usr/bin/env python3
"""Kill fence one immediately after the real core checkpoint event is sealed."""

from __future__ import annotations

import os
import signal
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bin"))

import preprocess_task_runner as task_runner  # noqa: E402


_original_call = task_runner._AnalysisCheckpointEventRecorder.__call__


def _crash_after_checkpoint(self, *args, **kwargs):
    result = _original_call(self, *args, **kwargs)
    if os.environ.get("STUDY_PREPROCESS_LEASE_FENCE") == "1":
        os.kill(os.getpid(), signal.SIGKILL)
    return result


task_runner._AnalysisCheckpointEventRecorder.__call__ = (
    _crash_after_checkpoint
)


if __name__ == "__main__":
    raise SystemExit(task_runner.main())
