"""Bounded lifecycle helpers for test-created subprocesses only.

The helpers deliberately keep the PID and PGID captured at launch.  A later
lookup of ``process.pid`` is not sufficient for a process-group kill because a
PID can be reused after a child exits.  Callers should use
``register_process`` immediately after ``Popen`` and pass that registration to
the cleanup helpers from ``finally`` blocks.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from dataclasses import dataclass
from collections.abc import Callable, Iterable
from typing import Any


@dataclass(frozen=True)
class ProcessRegistration:
    """Immutable launch identity for one test-owned process."""

    process: subprocess.Popen[Any]
    pid: int
    pgid: int
    launch_identity: str
    expected_command: str

    @property
    def owns_process_group(self) -> bool:
        """Whether the child created a private process group/session."""

        return self.pid == self.pgid


def register_process(
    process: subprocess.Popen[Any],
    *,
    require_private_group: bool = False,
) -> ProcessRegistration:
    """Capture and validate the exact PID/PGID immediately after launch."""

    pid = int(process.pid)
    if pid <= 0:
        raise AssertionError(f"invalid child pid: {pid}")
    expected_command = _expected_command(process)
    deadline = time.monotonic() + 5.0
    pgid = 0
    launch_identity = ""
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        try:
            pgid = int(os.getpgid(pid))
        except OSError:
            time.sleep(0.005)
            continue
        launch_identity, current_command = _read_process_snapshot(pid)
        command_ready = (
            not expected_command or expected_command in current_command
        )
        group_ready = not require_private_group or pgid == pid
        if pgid > 0 and launch_identity and command_ready and group_ready:
            break
        time.sleep(0.005)
    if pgid <= 0:
        raise AssertionError(f"invalid child pgid: {pgid}")
    if not launch_identity:
        raise AssertionError(f"child identity unavailable at registration: {pid}")
    current_identity, current_command = _read_process_snapshot(pid)
    if current_identity != launch_identity:
        raise AssertionError(
            f"child start identity changed at registration: pid={pid}"
        )
    if expected_command and expected_command not in current_command:
        raise AssertionError(
            f"child command identity mismatch at registration: pid={pid}"
        )
    registration = ProcessRegistration(
        process=process,
        pid=pid,
        pgid=pgid,
        launch_identity=launch_identity,
        expected_command=expected_command,
    )
    if require_private_group and not registration.owns_process_group:
        raise AssertionError(
            f"child did not create a private process group: "
            f"pid={registration.pid} pgid={registration.pgid}"
        )
    return registration


def _expected_command(process: subprocess.Popen[Any]) -> str:
    args = process.args
    if isinstance(args, (list, tuple)) and args:
        return str(args[0])
    if isinstance(args, str):
        return args.split()[0] if args.split() else ""
    return ""


def _read_process_snapshot(pid: int) -> tuple[str, str]:
    """Return stable OS start-time plus the current command for one PID."""

    completed = subprocess.run(
        ["/bin/ps", "-ww", "-p", str(pid), "-o", "lstart=", "-o", "command="],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        return "", ""
    parts = completed.stdout.strip().split(None, 5)
    if len(parts) < 6:
        return "", ""
    return " ".join(parts[:5]), " ".join(parts[5].split())


def _read_process_identity(pid: int) -> str:
    """Return the stable OS start-time token for one exact PID."""

    return _read_process_snapshot(pid)[0]


def _assert_signal_identity(registration: ProcessRegistration) -> None:
    """Fail closed instead of signalling a reused PID or changed command."""

    process = registration.process
    if process.poll() is not None:
        return
    try:
        current_pgid = os.getpgid(registration.pid)
    except ProcessLookupError:
        return
    if current_pgid != registration.pgid:
        raise AssertionError(
            f"test process pgid changed before cleanup: pid={registration.pid}"
        )
    current_identity, current_command = _read_process_snapshot(
        registration.pid
    )
    if (
        current_identity != registration.launch_identity
        or (
            registration.expected_command
            and registration.expected_command not in current_command
        )
    ):
        raise AssertionError(
            f"test process start-time or command changed before cleanup: "
            f"pid={registration.pid}"
        )


def _send_term(registration: ProcessRegistration) -> None:
    process = registration.process
    _assert_signal_identity(registration)
    try:
        if registration.owns_process_group:
            os.killpg(registration.pgid, signal.SIGTERM)
        elif process.poll() is None:
            process.terminate()
    except ProcessLookupError:
        pass


def _send_kill(registration: ProcessRegistration) -> None:
    process = registration.process
    _assert_signal_identity(registration)
    try:
        if registration.owns_process_group:
            os.killpg(registration.pgid, signal.SIGKILL)
        elif process.poll() is None:
            process.kill()
    except ProcessLookupError:
        pass


def _wait_after_signal(
    registration: ProcessRegistration,
    timeout: float,
) -> None:
    process = registration.process
    if process.poll() is not None:
        return
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        return


def _group_alive(registration: ProcessRegistration) -> bool:
    if not registration.owns_process_group:
        return registration.process.poll() is None
    try:
        os.killpg(registration.pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_group_absent(
    registration: ProcessRegistration,
    timeout: float,
) -> None:
    if not registration.owns_process_group:
        return
    deadline = time.monotonic() + timeout
    while _group_alive(registration) and time.monotonic() < deadline:
        time.sleep(0.01)
    if _group_alive(registration):
        raise AssertionError(
            f"test process group could not be reaped: "
            f"pid={registration.pid} pgid={registration.pgid}"
        )


def reap_process(
    registration: ProcessRegistration,
    *,
    natural_timeout: float = 0.0,
    term_timeout: float = 5.0,
    kill_timeout: float = 5.0,
) -> tuple[Any, Any]:
    """Finish one process using natural exit, TERM, KILL, then wait.

    ``finally`` callers can use this after any assertion or timeout.  The
    function always attempts to reap the original process object and returns
    whatever stdout/stderr ``communicate`` can collect.  It never signals a
    caller's process group when the child was launched without a private
    session.
    """

    process = registration.process
    if process.poll() is None and natural_timeout > 0:
        _wait_after_signal(registration, natural_timeout)
    if process.poll() is None or _group_alive(registration):
        _send_term(registration)
        _wait_after_signal(registration, term_timeout)
    if process.poll() is None or _group_alive(registration):
        _send_kill(registration)
        _wait_after_signal(registration, kill_timeout)
    if process.poll() is None or _group_alive(registration):
        # A private group may contain a child that keeps the supervisor's
        # pipes open.  The direct kill is a final supervisor-only fallback;
        # the bounded wait below makes a cleanup failure explicit.
        try:
            process.kill()
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=kill_timeout)
        except subprocess.TimeoutExpired as exc:
            raise AssertionError(
                f"test process could not be reaped: pid={registration.pid} "
                f"pgid={registration.pgid}"
            ) from exc
    else:
        # ``poll`` can observe the exit before wait has collected it.
        process.wait(timeout=kill_timeout)

    try:
        output = process.communicate(timeout=kill_timeout)
        _wait_group_absent(registration, kill_timeout)
        return output
    except subprocess.TimeoutExpired as exc:
        # Do not leave a pipe-owning descendant behind after a supervisor has
        # exited.  This path is only reachable when a child retained inherited
        # descriptors; the private group kill is still scoped to our launch
        # registration.
        _send_kill(registration)
        try:
            process.wait(timeout=kill_timeout)
        except subprocess.TimeoutExpired as wait_exc:
            raise AssertionError(
                f"test process pipes could not be closed: "
                f"pid={registration.pid} pgid={registration.pgid}"
            ) from wait_exc
        try:
            output = process.communicate(timeout=kill_timeout)
            _wait_group_absent(registration, kill_timeout)
            return output
        except subprocess.TimeoutExpired:
            raise AssertionError(
                f"test process output could not be collected: "
                f"pid={registration.pid} pgid={registration.pgid}"
            ) from exc


def communicate_with_cleanup(
    registration: ProcessRegistration,
    input: Any = None,
    *,
    timeout: float = 30.0,
    term_timeout: float = 5.0,
    kill_timeout: float = 5.0,
) -> tuple[Any, Any]:
    """Communicate normally and close the process safely on timeout/error."""

    process = registration.process
    try:
        return process.communicate(input, timeout=timeout)
    except subprocess.TimeoutExpired:
        return reap_process(
            registration,
            term_timeout=term_timeout,
            kill_timeout=kill_timeout,
        )
    except BaseException:
        if process.poll() is None:
            reap_process(
                registration,
                term_timeout=term_timeout,
                kill_timeout=kill_timeout,
            )
        raise


def stop_process(
    registration: ProcessRegistration,
    *,
    term_timeout: float = 5.0,
    kill_timeout: float = 5.0,
) -> tuple[Any, Any]:
    """Stop a running test process and return collected output."""

    return reap_process(
        registration,
        term_timeout=term_timeout,
        kill_timeout=kill_timeout,
    )


def process_absent(registration: ProcessRegistration) -> bool:
    """Return true only when both the PID and private PGID are gone."""

    process = registration.process
    if process.poll() is None:
        return False
    try:
        os.kill(registration.pid, 0)
    except ProcessLookupError:
        pass
    except PermissionError:
        return False
    else:
        return False
    if not registration.owns_process_group:
        return True
    try:
        os.killpg(registration.pgid, 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    return False


def wait_for_progress(
    condition: Callable[[], Any | None],
    *,
    progress_snapshot: Callable[[], Any],
    processes: Iterable[subprocess.Popen[Any]] = (),
    idle_timeout: float = 30.0,
    poll_interval: float = 0.02,
) -> Any:
    """Wait until condition succeeds, failing only after an idle interval.

    Marker, stage, heartbeat, log-size or counter changes should be returned by
    ``progress_snapshot``.  Per-process CPU time is included automatically.
    There is intentionally no total-duration deadline while progress continues.
    """

    tracked = tuple(processes)
    last_progress: Any = object()
    last_change = time.monotonic()
    while True:
        result = condition()
        if result is not None and result is not False:
            return result
        exited = {
            process.pid: process.returncode
            for process in tracked
            if process.poll() is not None
        }
        if exited:
            raise AssertionError(f"test process exited before condition: {exited}")
        cpu_times = tuple(
            (
                process.pid,
                subprocess.run(
                    ["/bin/ps", "-p", str(process.pid), "-o", "time="],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    text=True,
                    check=False,
                ).stdout.strip(),
            )
            for process in tracked
        )
        progress = (progress_snapshot(), cpu_times)
        if progress != last_progress:
            last_progress = progress
            last_change = time.monotonic()
        elif time.monotonic() - last_change >= idle_timeout:
            raise TimeoutError(
                f"test made no observable progress for {idle_timeout:.1f}s"
            )
        time.sleep(poll_interval)


__all__ = [
    "ProcessRegistration",
    "communicate_with_cleanup",
    "process_absent",
    "register_process",
    "reap_process",
    "stop_process",
    "wait_for_progress",
]
