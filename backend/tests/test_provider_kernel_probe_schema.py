from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "tests"))

from concurrent_dispatch import LeaseStore  # noqa: E402
from process_identity import (  # noqa: E402
    kernel_process_activity_delta,
    kernel_process_activity_snapshot,
    kernel_process_start_token,
)
from test_production_canary_admission import canary_task  # noqa: E402


SCHEMA_PATH = ROOT / "schemas/provider-kernel-probe-receipt-v1.json"
JSONSCHEMA_PYTHON = Path("/opt/miniconda3/envs/dl/bin/python")


def schema_errors(schema: dict, value: dict) -> list[dict]:
    script = r"""
import json, sys
from jsonschema import Draft202012Validator, FormatChecker
request = json.load(sys.stdin)
Draft202012Validator.check_schema(request["schema"])
errors = sorted(
    Draft202012Validator(
        request["schema"], format_checker=FormatChecker()
    ).iter_errors(request["instance"]),
    key=lambda error: tuple(str(part) for part in error.absolute_path),
)
json.dump([{"path": list(error.absolute_path), "message": error.message}
           for error in errors], sys.stdout, separators=(",", ":"))
"""
    completed = subprocess.run(
        [str(JSONSCHEMA_PYTHON), "-c", script],
        input=json.dumps({"schema": schema, "instance": value}),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(completed.stderr)
    return json.loads(completed.stdout)


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


class ProviderKernelProbeSchemaTests(unittest.TestCase):
    def test_real_builder_roundtrip_and_strict_negative_contract(self) -> None:
        schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as temp:
            runtime = Path(temp) / "runtime"
            store = LeaseStore(runtime)
            task = canary_task(190)
            decision = store.claim(
                task.unit_sha256,
                f"dispatcher-{os.getpid()}-{'a' * 32}",
                subject="math",
                task=task,
            )
            lease = decision.lease
            assert lease is not None
            context_root = (
                runtime
                / "dispatch/contexts"
                / task.unit_sha256
                / f"fence-{lease.fence}"
            )
            context_root.mkdir(parents=True, exist_ok=True)
            supervisor = subprocess.Popen(
                ["/bin/sh", "-c", "read line"],
                cwd=context_root,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            provider = subprocess.Popen(
                [sys.executable, "-c", "while True: pass"],
                cwd=context_root,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )
            try:
                store.publish_task_process_identity(
                    task,
                    lease,
                    child_pid=supervisor.pid,
                    child_pgid=os.getpgid(supervisor.pid),
                    process_start_token=kernel_process_start_token(
                        supervisor.pid
                    ),
                    launch_nonce=uuid.uuid4().hex,
                    launched_at=now(),
                    argv=["/bin/sh", "-c", "read line"],
                    executable_path=Path("/bin/sh"),
                    start_new_session=True,
                )
                token = kernel_process_start_token(provider.pid)
                identity = store.publish_provider_process_identity(
                    task,
                    lease,
                    stage_name="math_analysis",
                    provider_pid=provider.pid,
                    provider_pgid=os.getpgid(provider.pid),
                    process_start_token=token,
                    launch_nonce=uuid.uuid4().hex,
                    launched_at=now(),
                    argv=[sys.executable, "-c", "while True: pass"],
                    environment=dict(os.environ),
                    executable_path=Path(sys.executable),
                    cwd=context_root,
                    start_new_session=True,
                )
                baseline = kernel_process_activity_snapshot(
                    provider.pid,
                    expected_start_token=token,
                    expected_pgid=provider.pid,
                )
                final = kernel_process_activity_snapshot(
                    provider.pid,
                    expected_start_token=token,
                    expected_pgid=provider.pid,
                )
                delta = kernel_process_activity_delta(baseline, final)
                nonce_sha = hashlib.sha256(b"fixture-nonce").hexdigest()
                refs = store.publish_provider_kernel_probe_receipt(
                    task,
                    lease,
                    stage_name="math_analysis",
                    provider_process_identity_sha256=identity[
                        "provider_process_identity_sha256"
                    ],
                    provider_process_identity_path=identity[
                        "provider_process_identity_path"
                    ],
                    probe_nonce_sha256=nonce_sha,
                    previous_probe_receipt_sha256=None,
                    baseline_progress_receipt_sha256=None,
                    observed_progress_receipt_sha256=None,
                    control_channel_ok=True,
                    response_identity_verified=True,
                    signed_progress_delta_ok=False,
                    baseline_kernel_snapshot=baseline,
                    final_kernel_snapshot=final,
                    kernel_activity_delta=delta,
                    probe_started_at=now(),
                    probe_finished_at=now(),
                )
                receipt = json.loads(
                    Path(refs["provider_kernel_probe_receipt_path"])
                    .read_text(encoding="utf-8")
                )
                self.assertEqual(schema_errors(schema, receipt), [])
                verified = store.verify_provider_kernel_probe_receipt(
                    task,
                    lease,
                    stage_name="math_analysis",
                    probe_nonce_sha256=nonce_sha,
                    provider_kernel_probe_receipt_sha256=refs[
                        "provider_kernel_probe_receipt_sha256"
                    ],
                    provider_kernel_probe_receipt_path=refs[
                        "provider_kernel_probe_receipt_path"
                    ],
                )
                self.assertTrue(
                    verified["automatic_stall_cancellation_eligible"]
                )

                mutations = []
                missing = copy.deepcopy(receipt)
                missing.pop("baseline_kernel_snapshot")
                mutations.append(missing)
                wrong_token = copy.deepcopy(receipt)
                wrong_token["process_start_token"] = "bad-token"
                mutations.append(wrong_token)
                wrong_counter = copy.deepcopy(receipt)
                wrong_counter["kernel_activity_delta"]["counters"][
                    "cpu_user_nanoseconds"
                ] = -1
                mutations.append(wrong_counter)
                wrong_purpose = copy.deepcopy(receipt)
                wrong_purpose["authority"]["purpose"] = "wrong-purpose"
                mutations.append(wrong_purpose)
                extra = copy.deepcopy(receipt)
                extra["unexpected"] = True
                mutations.append(extra)
                for mutation in mutations:
                    self.assertTrue(schema_errors(schema, mutation))
            finally:
                for process in (provider, supervisor):
                    if process.poll() is None:
                        os.killpg(process.pid, 15)
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, 9)
                        process.wait(timeout=3)
                    for stream in (
                        process.stdin,
                        process.stdout,
                        process.stderr,
                    ):
                        if stream is not None:
                            stream.close()


if __name__ == "__main__":
    unittest.main()
