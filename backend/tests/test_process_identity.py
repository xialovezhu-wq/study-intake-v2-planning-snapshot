#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

import process_identity as identity_module  # noqa: E402
from process_identity import (  # noqa: E402
    ProcessIdentityError,
    kernel_process_start_token,
    parse_kernel_process_start_token,
    require_kernel_process_start_token,
)


@unittest.skipUnless(sys.platform == "darwin", "Darwin libproc contract")
class KernelProcessIdentityTests(unittest.TestCase):
    def test_current_and_child_process_get_distinct_kernel_tokens(self) -> None:
        current = kernel_process_start_token(os.getpid())
        parse_kernel_process_start_token(current, expected_pid=os.getpid())
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(2)"],
            start_new_session=True,
        )
        try:
            child = kernel_process_start_token(process.pid)
            parse_kernel_process_start_token(child, expected_pid=process.pid)
            self.assertNotEqual(current, child)
            self.assertEqual(
                require_kernel_process_start_token(process.pid, child), child
            )
        finally:
            process.terminate()
            process.wait(timeout=3)

    def test_pid_reuse_token_mismatch_is_fail_closed(self) -> None:
        pid = os.getpid()
        original = kernel_process_start_token(pid)
        _, seconds, microseconds = parse_kernel_process_start_token(
            original, expected_pid=pid
        )
        recycled = (
            f"darwin-libproc-bsdinfo-v1:{pid}:"
            f"{seconds}:{(microseconds + 1) % 1_000_000:06d}"
        )
        with mock.patch.object(
            identity_module,
            "kernel_process_start_token",
            return_value=recycled,
        ):
            with self.assertRaisesRegex(
                ProcessIdentityError, "kernel_process_start_token_mismatch"
            ):
                require_kernel_process_start_token(pid, original)

    def test_token_pid_binding_is_fail_closed(self) -> None:
        token = kernel_process_start_token(os.getpid())
        with self.assertRaisesRegex(
            ProcessIdentityError, "kernel_process_start_token_pid_mismatch"
        ):
            parse_kernel_process_start_token(
                token, expected_pid=os.getpid() + 1
            )


if __name__ == "__main__":
    unittest.main()
