#!/usr/bin/env python3
"""Attempt representative shared core writes after this fence is stale."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import PreprocessorError, atomic_write_json  # noqa: E402


runtime = Path(sys.argv[1]).resolve()
gate = Path(sys.argv[2]).resolve()
ready = Path(sys.argv[3]).resolve()
ready.touch()
while not gate.exists():
    time.sleep(0.01)

targets = [
    runtime / "state/jobs/math/GROUP-OWNER.json",
    runtime / "state/jobs/math/GROUP-MEMBER.json",
    runtime / "state/latest/math/GROUP-OWNER.json",
    runtime / "state/latest/math/GROUP-MEMBER.json",
    runtime / "packages/objects" / ("f" * 64 + ".json"),
]
results = []
for path in targets:
    try:
        atomic_write_json(path, {"writer": "stale-fence-one"})
    except PreprocessorError as exc:
        results.append(exc.code)
    else:
        results.append("unexpected_write")
sys.stdout.write(json.dumps(results, sort_keys=True))
