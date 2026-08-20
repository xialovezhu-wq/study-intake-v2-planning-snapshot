#!/usr/bin/env python3
"""Temp-only Codex-shaped child that never calls a model or Provider."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path


marker = os.environ.get("ZERO_MODEL_CODEX_MARKER")
if not marker:
    raise SystemExit(91)
Path(marker).write_text(
    json.dumps(
        {
            "fixture_scope": "temp_zero_model_provider_process_only",
            "pid": os.getpid(),
            "pgid": os.getpgid(0),
        },
        sort_keys=True,
    )
    + "\n",
    encoding="utf-8",
)
mode = os.environ.get("ZERO_MODEL_PROVIDER_ACTIVITY_MODE", "frozen")
if mode == "busy":
    # Deliberately generate Darwin taskinfo/rusage deltas without producing
    # stdout or a signed application progress receipt.
    value = 1
    while True:
        value = (value * 48271) % 2147483647
elif mode == "frozen":
    while True:
        time.sleep(600)
elif mode == "burst_then_frozen":
    deadline = time.monotonic() + float(
        os.environ.get("ZERO_MODEL_PROVIDER_BURST_SECONDS", "0.36")
    )
    value = 1
    while time.monotonic() < deadline:
        value = (value * 48271) % 2147483647
    while True:
        time.sleep(600)
else:
    raise SystemExit(93)
