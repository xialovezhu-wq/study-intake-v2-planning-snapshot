from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
for directory in (ROOT / "lib", ROOT / "bin"):
    if str(directory) not in sys.path:
        sys.path.insert(0, str(directory))

import preprocess_dispatcher as dispatcher
from concurrent_dispatch import DispatchError
from preprocessor_core import CodexRunner, PreprocessorError, load_config


def substitute(value: object, release_root: Path, runtime_root: Path) -> object:
    if isinstance(value, dict):
        return {key: substitute(item, release_root, runtime_root) for key, item in value.items()}
    if isinstance(value, list):
        return [substitute(item, release_root, runtime_root) for item in value]
    if isinstance(value, str):
        return value.replace("${RELEASE_ROOT}", str(release_root)).replace(
            "${RUNTIME_DATA_ROOT}", str(runtime_root)
        )
    return value


class OfflineExecutionGateV1Tests(unittest.TestCase):
    def test_release_config_is_explicitly_offline_and_role_separated(self) -> None:
        template = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary) / "runtime"
            value = substitute(template, ROOT, runtime)
            config_path = Path(temporary) / "config.json"
            config_path.write_text(json.dumps(value), encoding="utf-8")
            config = load_config(config_path)
        self.assertEqual(config["execution_mode"], "offline")
        self.assertTrue(config["live_execution_gate"]["default_locked"])
        self.assertEqual(config["models"]["orchestrator"]["model"], "gpt-5.6-terra")
        self.assertEqual(config["models"]["orchestrator"]["reasoning_effort"], "ultra")
        self.assertEqual(config["models"]["reader"]["model"], "gpt-5.6-luna")
        self.assertEqual(config["models"]["reader"]["reasoning_effort"], "max")
        self.assertFalse(config["models"]["reader"]["agents_enabled"])

    def test_offline_commands_fail_before_worker_or_scanner_construction(self) -> None:
        with self.assertRaises(DispatchError) as run_once:
            dispatcher._run_once(
                {"execution_mode": "offline"}, "math", Path("/nonexistent")
            )
        self.assertEqual(run_once.exception.code, "offline_run_once_forbidden")
        with self.assertRaises(DispatchError) as audit:
            dispatcher._audit({"execution_mode": "offline"}, "math")
        self.assertEqual(audit.exception.code, "offline_producer_scan_forbidden")
        runtime = object.__new__(dispatcher.ProductionDispatchRuntime)
        runtime.config = {"execution_mode": "offline"}
        with self.assertRaises(DispatchError) as scan:
            runtime.scan_and_submit()
        self.assertEqual(scan.exception.code, "offline_producer_scan_forbidden")

    def test_codex_runner_offline_tripwire_precedes_popen(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            runner = CodexRunner(
                {
                    "execution_mode": "offline",
                    "live_execution_gate": {
                        "enabled": True,
                        "default_locked": True,
                        "authorization_required": True,
                    },
                },
                Path(temporary),
            )
            with (
                mock.patch.dict(
                    os.environ,
                    {"STUDY_INTAKE_FIXTURE_EXECUTION": ""},
                ),
                mock.patch("preprocessor_core.subprocess.Popen") as popen,
            ):
                with self.assertRaises(PreprocessorError) as caught:
                    runner._invoke_subprocess(
                        [
                            "/Applications/ChatGPT.app/Contents/Resources/codex",
                            "exec",
                            "--model",
                            "gpt-5.6-terra",
                        ],
                        input=b"{}",
                        timeout=None,
                        cwd=Path(temporary),
                        stage_name="orchestrator",
                    )
                self.assertEqual(
                    caught.exception.code, "offline_external_launch_forbidden"
                )
                popen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
