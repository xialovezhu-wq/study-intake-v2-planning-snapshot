from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import read_only_luna_validation as validation  # noqa: E402


class ReadOnlyLunaValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="read-only-luna-validation-")
        self.root = Path(self.temp.name)
        self.fake_codex = self.root / "fake-codex"
        self.fake_codex.write_text(
            """#!/usr/bin/env python3
import json
import os
import sys
import time
from pathlib import Path

args = sys.argv[1:]
task_id = os.environ["STUDY_VALIDATION_TASK_ID"]
subject = os.environ["STUDY_VALIDATION_SUBJECT"]
expected = int(os.environ["STUDY_VALIDATION_EXPECTED_COUNT"])
barrier = Path(os.environ["STUDY_VALIDATION_BARRIER_ROOT"])
barrier.mkdir(parents=True, exist_ok=True)
(barrier / (task_id + ".argv.json")).write_text(
    json.dumps(args, sort_keys=True), encoding="utf-8"
)
deadline = time.monotonic() + 10
while len(list(barrier.glob("*.argv.json"))) < expected:
    if time.monotonic() >= deadline:
        raise SystemExit(91)
    time.sleep(0.01)
result = Path(args[args.index("--output-last-message") + 1])
result.write_text(
    json.dumps(
        {"task_id": task_id, "subject": subject, "status": "validated"},
        sort_keys=True,
    ),
    encoding="utf-8",
)
print(json.dumps({"type": "completed"}, sort_keys=True))
""",
            encoding="utf-8",
        )
        self.fake_codex.chmod(
            self.fake_codex.stat().st_mode | stat.S_IXUSR
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_selection(self, count: int) -> Path:
        path = self.root / f"selection-{count}.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": validation.SELECTION_SCHEMA,
                    "tasks": [
                        {
                            "task_id": f"task-{index:04d}",
                            "subject": ("math", "cs408", "english")[index % 3],
                        }
                        for index in range(count)
                    ],
                    "formal_write_count": 0,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        return path

    def run_batch(self, count: int) -> tuple[dict, Path]:
        output = self.root / f"output-{count}"
        guard = {
            "status": "unchanged",
            "manifest_sha256": "a" * 64,
            "formal_write_count": 0,
        }
        with mock.patch.object(
            validation,
            "_verify_formal_surfaces",
            side_effect=[guard, guard],
        ):
            result = validation.run_validation(
                selection_path=self.write_selection(count),
                output_root=output,
                codex_path=self.fake_codex,
                timeout_seconds=30,
                formal_config=self.root / "formal-config.json",
                formal_baseline=self.root / "formal-baseline.json",
            )
        return result, output

    def test_selected_twenty_are_all_started_without_a_concurrency_parameter(self) -> None:
        result, output = self.run_batch(20)
        self.assertEqual(result["status"], "validated")
        self.assertEqual(result["selected_task_count"], 20)
        self.assertEqual(result["started_task_count"], 20)
        self.assertEqual(result["validated_task_count"], 20)
        self.assertEqual(result["formal_write_count"], 0)
        task_roots = sorted((output / "tasks").iterdir())
        self.assertEqual(len(task_roots), 20)
        self.assertEqual(len({path.resolve() for path in task_roots}), 20)
        argv_files = sorted((output / "child-barrier").glob("*.argv.json"))
        self.assertEqual(len(argv_files), 20)
        for path in argv_files:
            argv = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(argv[argv.index("--model") + 1], "gpt-5.6-luna")
            self.assertIn('model_reasoning_effort="max"', argv)
            self.assertFalse(
                any("service_tier" in value for value in argv),
                argv,
            )
            self.assertIn("--ignore-user-config", argv)
            self.assertNotIn("--concurrency", argv)

    def test_selection_count_is_not_artificially_capped(self) -> None:
        selection = self.write_selection(6001)
        self.assertGreater(selection.stat().st_size, 256 * 1024)
        tasks = validation._load_selection(selection)
        self.assertEqual(len(tasks), 6001)

    def test_output_schema_declares_string_types_for_service_validation(self) -> None:
        schema = validation._task_schema(
            {"task_id": "math-runtime-check", "subject": "math"}
        )
        properties = schema["properties"]
        self.assertEqual(
            properties["task_id"],
            {"type": "string", "const": "math-runtime-check"},
        )
        self.assertEqual(
            properties["subject"],
            {"type": "string", "const": "math"},
        )
        self.assertEqual(
            properties["status"],
            {"type": "string", "const": "validated"},
        )

    def test_existing_output_is_rejected_without_overwrite(self) -> None:
        output = self.root / "nonempty-output"
        output.mkdir()
        sentinel = output / "keep.txt"
        sentinel.write_text("keep\n", encoding="utf-8")
        guard = {
            "status": "unchanged",
            "manifest_sha256": "a" * 64,
            "formal_write_count": 0,
        }
        with mock.patch.object(
            validation,
            "_verify_formal_surfaces",
            side_effect=[guard, guard],
        ):
            with self.assertRaisesRegex(validation.ValidationError, "output_root_not_empty"):
                validation.run_validation(
                    selection_path=self.write_selection(1),
                    output_root=output,
                    codex_path=self.fake_codex,
                    timeout_seconds=30,
                    formal_config=self.root / "formal-config.json",
                    formal_baseline=self.root / "formal-baseline.json",
                )
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep\n")

    def test_formal_manifest_change_rejects_validation_summary(self) -> None:
        before = {
            "status": "unchanged",
            "manifest_sha256": "a" * 64,
            "formal_write_count": 0,
        }
        after = {**before, "manifest_sha256": "b" * 64}
        with mock.patch.object(
            validation,
            "_verify_formal_surfaces",
            side_effect=[before, after],
        ):
            with self.assertRaisesRegex(
                validation.ValidationError, "formal_surface_guard_changed"
            ):
                validation.run_validation(
                    selection_path=self.write_selection(1),
                    output_root=self.root / "changed-output",
                    codex_path=self.fake_codex,
                    timeout_seconds=30,
                    formal_config=self.root / "formal-config.json",
                    formal_baseline=self.root / "formal-baseline.json",
                )


if __name__ == "__main__":
    unittest.main()
