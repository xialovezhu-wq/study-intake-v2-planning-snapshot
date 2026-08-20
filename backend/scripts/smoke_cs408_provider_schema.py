#!/usr/bin/env python3
"""Submit the exact portable 408 critic schema without dynamic enums."""

from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CODEX = Path("/Applications/ChatGPT.app/Contents/Resources/codex")
SCHEMA = ROOT / "schemas" / "luna-critical-review-v2.json"


def _events(payload: bytes) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for line in payload.splitlines():
        try:
            value = json.loads(line)
        except (UnicodeError, json.JSONDecodeError):
            continue
        if isinstance(value, dict):
            result.append(value)
    return result


def main() -> None:
    schema_bytes = SCHEMA.read_bytes()
    with tempfile.TemporaryDirectory(prefix="cs408-provider-schema-smoke-") as raw:
        output = Path(raw) / "last.json"
        command = [
            str(CODEX),
            "exec",
            "--strict-config",
            "--ephemeral",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--cd",
            raw,
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
            "--output-schema",
            str(SCHEMA),
            "--output-last-message",
            str(output),
            "--json",
            "-",
        ]
        completed = subprocess.run(
            command,
            input=(
                "Return a schema-valid synthetic 408 critical-review object. "
                "Use verdict reject, empty finding arrays, empty correction_resolutions, "
                "empty claim arrays wherever permitted, and one minimal synthetic atomic "
                "signal with evidence ref smoke:evidence. This is a provider-schema "
                "portability test only; do not use tools or files."
            ).encode("utf-8"),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=900,
            check=False,
            env={
                "PATH": "/usr/bin:/bin",
                "PYTHONUTF8": "1",
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )
        events = _events(completed.stdout)
        provider_rejected = any(
            "invalid_json_schema" in json.dumps(event, ensure_ascii=False)
            for event in events
        )
        result = {
            "schema_sha256": hashlib.sha256(schema_bytes).hexdigest(),
            "provider_schema_rejected": provider_rejected,
            "returncode": completed.returncode,
            "turn_started": any(
                event.get("type") in {"turn.started", "turn_started"}
                for event in events
            ),
            "output_present": output.is_file() and output.stat().st_size > 0,
            "provider_request_count": 1,
            "model_call_count": 1 if completed.returncode == 0 else 0,
            "formal_write_count": 0,
        }
        if completed.returncode != 0:
            result["safe_error_codes"] = sorted(
                {
                    str((event.get("error") or {}).get("code"))
                    for event in events
                    if isinstance(event.get("error"), dict)
                    and isinstance((event.get("error") or {}).get("code"), str)
                }
            )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        if completed.returncode != 0 or provider_rejected or not result["output_present"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
