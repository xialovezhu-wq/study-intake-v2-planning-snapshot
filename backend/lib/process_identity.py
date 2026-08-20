#!/usr/bin/env python3
"""Fail-closed Darwin process identity helpers.

Wall-clock strings returned by ``ps`` are only precise to one second and are
not sufficient to distinguish a recycled PID. Darwin's ``proc_pidinfo``
exposes the kernel-recorded process start timestamp with microsecond precision;
this module is the single implementation used by dispatcher supervisors and
their Provider children.
"""

from __future__ import annotations

import ctypes
import functools
import hashlib
import json
import os
import re
import sys
from typing import Any, Final, Mapping


PROC_PIDTBSDINFO: Final = 3
PROC_PIDTASKINFO: Final = 4
RUSAGE_INFO_V2: Final = 2
MAXCOMLEN: Final = 16
KERNEL_ACTIVITY_SNAPSHOT_SCHEMA: Final = (
    "study-intake-darwin-provider-kernel-activity-snapshot-v1"
)
KERNEL_ACTIVITY_COUNTERS: Final = (
    "cpu_user_nanoseconds",
    "cpu_system_nanoseconds",
    "context_switch_count",
    "mach_message_sent_count",
    "mach_message_received_count",
    "mach_syscall_count",
    "unix_syscall_count",
    "page_fault_count",
    "pagein_count",
    "disk_read_bytes",
    "disk_written_bytes",
)
KERNEL_PROCESS_START_TOKEN_RE: Final = re.compile(
    r"^darwin-libproc-bsdinfo-v1:(?P<pid>[1-9][0-9]*):"
    r"(?P<seconds>[1-9][0-9]*):(?P<microseconds>[0-9]{6})$"
)


class ProcessIdentityError(RuntimeError):
    """The exact live process identity could not be established."""


class _ProcBsdInfo(ctypes.Structure):
    """Exact public ``struct proc_bsdinfo`` layout from ``sys/proc_info.h``."""

    _fields_ = [
        ("pbi_flags", ctypes.c_uint32),
        ("pbi_status", ctypes.c_uint32),
        ("pbi_xstatus", ctypes.c_uint32),
        ("pbi_pid", ctypes.c_uint32),
        ("pbi_ppid", ctypes.c_uint32),
        ("pbi_uid", ctypes.c_uint32),
        ("pbi_gid", ctypes.c_uint32),
        ("pbi_ruid", ctypes.c_uint32),
        ("pbi_rgid", ctypes.c_uint32),
        ("pbi_svuid", ctypes.c_uint32),
        ("pbi_svgid", ctypes.c_uint32),
        ("rfu_1", ctypes.c_uint32),
        ("pbi_comm", ctypes.c_char * MAXCOMLEN),
        ("pbi_name", ctypes.c_char * (2 * MAXCOMLEN)),
        ("pbi_nfiles", ctypes.c_uint32),
        ("pbi_pgid", ctypes.c_uint32),
        ("pbi_pjobc", ctypes.c_uint32),
        ("e_tdev", ctypes.c_uint32),
        ("e_tpgid", ctypes.c_uint32),
        ("pbi_nice", ctypes.c_int32),
        ("pbi_start_tvsec", ctypes.c_uint64),
        ("pbi_start_tvusec", ctypes.c_uint64),
    ]


class _ProcTaskInfo(ctypes.Structure):
    """Exact public ``struct proc_taskinfo`` layout."""

    _fields_ = [
        ("pti_virtual_size", ctypes.c_uint64),
        ("pti_resident_size", ctypes.c_uint64),
        ("pti_total_user", ctypes.c_uint64),
        ("pti_total_system", ctypes.c_uint64),
        ("pti_threads_user", ctypes.c_uint64),
        ("pti_threads_system", ctypes.c_uint64),
        ("pti_policy", ctypes.c_int32),
        ("pti_faults", ctypes.c_int32),
        ("pti_pageins", ctypes.c_int32),
        ("pti_cow_faults", ctypes.c_int32),
        ("pti_messages_sent", ctypes.c_int32),
        ("pti_messages_received", ctypes.c_int32),
        ("pti_syscalls_mach", ctypes.c_int32),
        ("pti_syscalls_unix", ctypes.c_int32),
        ("pti_csw", ctypes.c_int32),
        ("pti_threadnum", ctypes.c_int32),
        ("pti_numrunning", ctypes.c_int32),
        ("pti_priority", ctypes.c_int32),
    ]


class _RUsageInfoV2(ctypes.Structure):
    """Exact public ``struct rusage_info_v2`` layout."""

    _fields_ = [
        ("ri_uuid", ctypes.c_uint8 * 16),
        ("ri_user_time", ctypes.c_uint64),
        ("ri_system_time", ctypes.c_uint64),
        ("ri_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_interrupt_wkups", ctypes.c_uint64),
        ("ri_pageins", ctypes.c_uint64),
        ("ri_wired_size", ctypes.c_uint64),
        ("ri_resident_size", ctypes.c_uint64),
        ("ri_phys_footprint", ctypes.c_uint64),
        ("ri_proc_start_abstime", ctypes.c_uint64),
        ("ri_proc_exit_abstime", ctypes.c_uint64),
        ("ri_child_user_time", ctypes.c_uint64),
        ("ri_child_system_time", ctypes.c_uint64),
        ("ri_child_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_child_interrupt_wkups", ctypes.c_uint64),
        ("ri_child_pageins", ctypes.c_uint64),
        ("ri_child_elapsed_abstime", ctypes.c_uint64),
        ("ri_diskio_bytesread", ctypes.c_uint64),
        ("ri_diskio_byteswritten", ctypes.c_uint64),
    ]


@functools.lru_cache(maxsize=1)
def _libproc() -> ctypes.CDLL:
    if sys.platform != "darwin":
        raise ProcessIdentityError("kernel_process_identity_unsupported")
    try:
        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    except OSError as exc:
        raise ProcessIdentityError("kernel_process_identity_unavailable") from exc
    library.proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    library.proc_pidinfo.restype = ctypes.c_int
    library.proc_pid_rusage.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
    ]
    library.proc_pid_rusage.restype = ctypes.c_int
    return library


def _proc_bsdinfo(pid: int) -> _ProcBsdInfo:
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 1:
        raise ProcessIdentityError("kernel_process_pid_invalid")
    info = _ProcBsdInfo()
    size = ctypes.sizeof(info)
    ctypes.set_errno(0)
    result = _libproc().proc_pidinfo(
        pid,
        PROC_PIDTBSDINFO,
        0,
        ctypes.byref(info),
        size,
    )
    if result != size or int(info.pbi_pid) != pid:
        raise ProcessIdentityError("kernel_process_identity_unavailable")
    seconds = int(info.pbi_start_tvsec)
    microseconds = int(info.pbi_start_tvusec)
    if seconds <= 0 or not 0 <= microseconds < 1_000_000:
        raise ProcessIdentityError("kernel_process_start_time_invalid")
    return info


def kernel_process_start_token(pid: int) -> str:
    """Return a PID-reuse-safe token for one currently live Darwin process."""

    info = _proc_bsdinfo(pid)
    return (
        "darwin-libproc-bsdinfo-v1:"
        f"{pid}:{int(info.pbi_start_tvsec)}:"
        f"{int(info.pbi_start_tvusec):06d}"
    )


def parse_kernel_process_start_token(
    token: str, *, expected_pid: int | None = None
) -> tuple[int, int, int]:
    if not isinstance(token, str):
        raise ProcessIdentityError("kernel_process_start_token_invalid")
    match = KERNEL_PROCESS_START_TOKEN_RE.fullmatch(token)
    if match is None:
        raise ProcessIdentityError("kernel_process_start_token_invalid")
    pid = int(match.group("pid"))
    seconds = int(match.group("seconds"))
    microseconds = int(match.group("microseconds"))
    if microseconds >= 1_000_000:
        raise ProcessIdentityError("kernel_process_start_token_invalid")
    if expected_pid is not None and pid != expected_pid:
        raise ProcessIdentityError("kernel_process_start_token_pid_mismatch")
    return pid, seconds, microseconds


def require_kernel_process_start_token(pid: int, expected_token: str) -> str:
    """Reobserve a live PID and reject a recycled or otherwise mismatched PID."""

    parse_kernel_process_start_token(expected_token, expected_pid=pid)
    observed_token = kernel_process_start_token(pid)
    if observed_token != expected_token:
        raise ProcessIdentityError("kernel_process_start_token_mismatch")
    return observed_token


def _unsigned_counter(value: int) -> int:
    """Preserve Darwin's cumulative 32-bit counters without sign extension."""

    return int(ctypes.c_uint32(int(value)).value)


def kernel_process_activity_snapshot(
    pid: int,
    *,
    expected_start_token: str,
    expected_pgid: int,
) -> dict[str, Any]:
    """Read PID-reuse-safe cumulative Provider activity from Darwin libproc.

    PID existence is deliberately absent from the evidence.  A usable sample
    requires the exact PID, its dedicated process group, and the kernel start
    token to match both before and after the two independent libproc reads.
    """

    if (
        isinstance(expected_pgid, bool)
        or not isinstance(expected_pgid, int)
        or expected_pgid != pid
    ):
        raise ProcessIdentityError("kernel_process_group_identity_invalid")
    require_kernel_process_start_token(pid, expected_start_token)
    try:
        observed_pgid = os.getpgid(pid)
    except OSError as exc:
        raise ProcessIdentityError("kernel_process_group_unavailable") from exc
    if observed_pgid != expected_pgid:
        raise ProcessIdentityError("kernel_process_group_identity_mismatch")

    task = _ProcTaskInfo()
    task_size = ctypes.sizeof(task)
    ctypes.set_errno(0)
    task_result = _libproc().proc_pidinfo(
        pid,
        PROC_PIDTASKINFO,
        0,
        ctypes.byref(task),
        task_size,
    )
    if task_result != task_size:
        raise ProcessIdentityError("kernel_process_task_activity_unavailable")

    usage = _RUsageInfoV2()
    ctypes.set_errno(0)
    usage_result = _libproc().proc_pid_rusage(
        pid,
        RUSAGE_INFO_V2,
        ctypes.byref(usage),
    )
    if usage_result != 0:
        raise ProcessIdentityError("kernel_process_rusage_unavailable")

    require_kernel_process_start_token(pid, expected_start_token)
    try:
        final_pgid = os.getpgid(pid)
    except OSError as exc:
        raise ProcessIdentityError("kernel_process_group_unavailable") from exc
    if final_pgid != expected_pgid:
        raise ProcessIdentityError("kernel_process_group_identity_mismatch")
    return {
        "schema_version": KERNEL_ACTIVITY_SNAPSHOT_SCHEMA,
        "signal_source": "darwin_libproc_taskinfo_rusage_v2",
        "provider_pid": pid,
        "provider_pgid": expected_pgid,
        "process_start_token": expected_start_token,
        "counters": {
            "cpu_user_nanoseconds": int(usage.ri_user_time),
            "cpu_system_nanoseconds": int(usage.ri_system_time),
            "context_switch_count": _unsigned_counter(task.pti_csw),
            "mach_message_sent_count": _unsigned_counter(
                task.pti_messages_sent
            ),
            "mach_message_received_count": _unsigned_counter(
                task.pti_messages_received
            ),
            "mach_syscall_count": _unsigned_counter(
                task.pti_syscalls_mach
            ),
            "unix_syscall_count": _unsigned_counter(
                task.pti_syscalls_unix
            ),
            "page_fault_count": _unsigned_counter(task.pti_faults),
            "pagein_count": max(
                _unsigned_counter(task.pti_pageins),
                int(usage.ri_pageins),
            ),
            "disk_read_bytes": int(usage.ri_diskio_bytesread),
            "disk_written_bytes": int(usage.ri_diskio_byteswritten),
        },
    }


def kernel_activity_snapshot_sha256(snapshot: Mapping[str, Any]) -> str:
    try:
        payload = json.dumps(
            snapshot,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ProcessIdentityError("kernel_activity_snapshot_invalid") from exc
    return hashlib.sha256(payload).hexdigest()


def kernel_process_activity_delta(
    baseline: Mapping[str, Any], current: Mapping[str, Any]
) -> dict[str, Any]:
    """Return monotonic deltas; a counter reset disables the signal."""

    identity_fields = (
        "schema_version",
        "signal_source",
        "provider_pid",
        "provider_pgid",
        "process_start_token",
    )
    if any(baseline.get(key) != current.get(key) for key in identity_fields):
        raise ProcessIdentityError("kernel_activity_identity_mismatch")
    baseline_counters = baseline.get("counters")
    current_counters = current.get("counters")
    if (
        not isinstance(baseline_counters, Mapping)
        or not isinstance(current_counters, Mapping)
        or set(baseline_counters) != set(KERNEL_ACTIVITY_COUNTERS)
        or set(current_counters) != set(KERNEL_ACTIVITY_COUNTERS)
    ):
        raise ProcessIdentityError("kernel_activity_snapshot_invalid")
    deltas: dict[str, int] = {}
    for key in KERNEL_ACTIVITY_COUNTERS:
        before = baseline_counters.get(key)
        after = current_counters.get(key)
        if (
            isinstance(before, bool)
            or not isinstance(before, int)
            or before < 0
            or isinstance(after, bool)
            or not isinstance(after, int)
            or after < before
        ):
            raise ProcessIdentityError("kernel_activity_counter_invalid")
        deltas[key] = after - before
    return {
        "signal_source": "darwin_libproc_taskinfo_rusage_v2",
        "provider_pid": int(current["provider_pid"]),
        "provider_pgid": int(current["provider_pgid"]),
        "process_start_token": str(current["process_start_token"]),
        "counters": deltas,
        "activity_detected": any(value > 0 for value in deltas.values()),
    }
