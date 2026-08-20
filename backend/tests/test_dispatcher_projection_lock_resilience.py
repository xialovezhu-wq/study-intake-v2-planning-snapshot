from __future__ import annotations

import contextlib
import io
import sys
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

import preprocess_dispatcher as dispatcher  # noqa: E402
from dashboard_projection import DashboardProjectionError  # noqa: E402


class DispatcherProjectionLockResilienceTests(unittest.TestCase):
    def test_one_lock_timeout_retries_and_succeeds(self) -> None:
        with (
            mock.patch.object(
                dispatcher,
                "_write_subject_projections",
                side_effect=[
                    DashboardProjectionError("dashboard_projection_lock_busy"),
                    None,
                ],
            ) as writer,
            mock.patch.object(dispatcher.time, "sleep") as sleeper,
        ):
            result = dispatcher._write_subject_projections_resilient(
                {},
                "math",
                daemon_status="running",
                decisions=[],
                lease_status={},
                retry_delay_seconds=0,
            )
        self.assertTrue(result)
        self.assertEqual(writer.call_count, 2)
        sleeper.assert_called_once_with(0)

    def test_persistent_lock_timeout_is_observable_but_does_not_raise(self) -> None:
        stderr = io.StringIO()
        with (
            mock.patch.object(
                dispatcher,
                "_write_subject_projections",
                side_effect=DashboardProjectionError(
                    "dashboard_projection_lock_busy"
                ),
            ) as writer,
            mock.patch.object(dispatcher.time, "sleep"),
            contextlib.redirect_stderr(stderr),
        ):
            result = dispatcher._write_subject_projections_resilient(
                {},
                "math",
                daemon_status="running",
                decisions=[],
                lease_status={},
                retry_delay_seconds=0,
            )
        self.assertFalse(result)
        self.assertEqual(writer.call_count, 2)
        self.assertIn("dashboard_projection_lock_busy_skipped", stderr.getvalue())
        self.assertIn('"formal_write_count":0', stderr.getvalue())

    def test_non_lock_projection_error_remains_fail_closed(self) -> None:
        with mock.patch.object(
            dispatcher,
            "_write_subject_projections",
            side_effect=DashboardProjectionError(
                "dashboard_projection_schema_invalid"
            ),
        ):
            with self.assertRaisesRegex(
                DashboardProjectionError,
                "dashboard_projection_schema_invalid",
            ):
                dispatcher._write_subject_projections_resilient(
                    {},
                    "math",
                    daemon_status="running",
                    decisions=[],
                    lease_status={},
                    retry_delay_seconds=0,
                )


if __name__ == "__main__":
    unittest.main()
