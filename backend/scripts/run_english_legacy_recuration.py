#!/usr/bin/env python3
"""Build and run the one-shot EN-P0-006 Luna direct-MCP lane."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import json
import os
from pathlib import Path
import sys
import threading
from typing import Any, Callable, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from english_legacy_recuration import (  # noqa: E402
    CONCURRENCY_ATTESTATION_SCHEMA,
    EnglishLegacyRecurationError,
    RUN_MODE_COUNTS,
    STAGE_EVENT_ORDER,
    build_authorized_work_items,
    publish_run_summary,
    reopen_work_item,
    reopen_work_item_batch,
    run_work_item,
    verify_quality_receipt,
    verify_run_summary,
    verify_smoke_run_summary,
)
from preprocessor_core import load_config  # noqa: E402


def _sha256(value: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise argparse.ArgumentTypeError("must be a lowercase SHA-256")
    return value


def _positive(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _subparser(subparsers: Any, name: str, help_text: str) -> argparse.ArgumentParser:
    return subparsers.add_parser(name, help=help_text, allow_abbrev=False)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        description=(
            "Run only the authorized EN-P0-006 Luna recuration lane. The command "
            "never imports or invokes a formal writer."
        ),
        allow_abbrev=False,
    )
    sub = root.add_subparsers(dest="command", required=True)

    build = _subparser(sub, "build-work-items", "materialize immutable work items")
    build.add_argument("--disposition-root", type=Path, required=True)
    build.add_argument("--authority-key", type=Path, required=True)
    build.add_argument("--authorization-expansion-closure-sha256", type=_sha256, required=True)
    build.add_argument("--output-root", type=Path, required=True)
    build.add_argument("--attempt", type=_positive, default=1)

    inspect = _subparser(sub, "inspect-batch", "reopen a batch without a model call")
    inspect.add_argument("--work-item-batch", type=Path, required=True)

    run_one = _subparser(sub, "run-item", "run exactly one immutable work item")
    run_one.add_argument("--config", type=Path, required=True)
    run_one.add_argument("--runtime-root", type=Path, required=True)
    run_one.add_argument("--work-item", type=Path, required=True)

    run_batch = _subparser(sub, "run-batch", "run selected distinct targets in parallel")
    run_batch.add_argument("--config", type=Path, required=True)
    run_batch.add_argument("--runtime-root", type=Path, required=True)
    run_batch.add_argument("--work-item-batch", type=Path, required=True)
    run_batch.add_argument(
        "--run-mode",
        choices=tuple(RUN_MODE_COUNTS),
        required=True,
        help="smoke_1 and smoke_10 must pass before full_98",
    )
    selection = run_batch.add_mutually_exclusive_group()
    selection.add_argument("--ordinal", type=_positive, action="append")
    selection.add_argument("--count", type=_positive)
    run_batch.add_argument(
        "--max-workers",
        type=_positive,
        help="full_98 requires this option explicitly set to 98",
    )
    run_batch.add_argument(
        "--barrier-timeout-seconds",
        type=_positive,
        default=30,
    )
    run_batch.add_argument("--smoke-1-summary-sha256", type=_sha256)
    run_batch.add_argument("--smoke-10-summary-sha256", type=_sha256)

    verify = _subparser(sub, "verify-quality", "reopen one HMAC quality receipt")
    verify.add_argument("--config", type=Path, required=True)
    verify.add_argument("--runtime-root", type=Path, required=True)
    verify.add_argument("--quality-receipt-sha256", type=_sha256, required=True)
    verify_summary = _subparser(
        sub, "verify-run-summary", "reopen one HMAC concurrency summary"
    )
    verify_summary.add_argument("--config", type=Path, required=True)
    verify_summary.add_argument("--runtime-root", type=Path, required=True)
    verify_summary.add_argument("--run-summary-sha256", type=_sha256, required=True)
    return root


def _emit(value: Mapping[str, Any]) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def _runtime_config(config_path: Path, runtime_root: Path) -> tuple[dict[str, Any], Path]:
    config = load_config(config_path.expanduser().resolve())
    runtime = runtime_root.expanduser().resolve()
    configured = Path(str(config.get("runtime_root") or "")).expanduser().resolve()
    if configured != runtime:
        raise EnglishLegacyRecurationError(
            "english_legacy_runtime_root_binding_mismatch"
        )
    return config, runtime


def _result_row(
    item: Mapping[str, Any],
    result: Mapping[str, Any],
) -> dict[str, Any]:
    if result.get("status") == "needs_rework":
        receipt = result.get("needs_rework_receipt_sha256")
        return {
            "ordinal": item["ordinal"],
            "target_id": item["target_id"],
            "status": "failed",
            "error_code": str(result.get("error_code") or "needs_rework"),
            "package_sha256": None,
            "quality_receipt_sha256": None,
            "failure_receipt_sha256": receipt,
            "model_call_count": int(result.get("model_call_count") or 0),
            "formal_write_count": 0,
        }
    return {
        "ordinal": item["ordinal"],
        "target_id": item["target_id"],
        "status": result["status"],
        "error_code": None,
        "package_sha256": result["package_sha256"],
        "quality_receipt_sha256": result["quality_receipt_sha256"],
        "failure_receipt_sha256": None,
        "model_call_count": int(result["model_call_count"]),
        "formal_write_count": 0,
    }


def _failed_row(
    item: Mapping[str, Any],
    error: EnglishLegacyRecurationError,
    *,
    observed_model_call_count: int,
) -> dict[str, Any]:
    failure_receipt = error.diagnostic.get("mcp_failure_receipt_sha256")
    if not isinstance(failure_receipt, str) or len(failure_receipt) != 64:
        failure_receipt = None
    return {
        "ordinal": item["ordinal"],
        "target_id": item["target_id"],
        "status": "failed",
        "error_code": error.code,
        "package_sha256": None,
        "quality_receipt_sha256": None,
        "failure_receipt_sha256": failure_receipt,
        "model_call_count": observed_model_call_count,
        "formal_write_count": 0,
    }


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _workers_for_mode(run_mode: str, explicit_workers: int | None) -> int:
    expected = RUN_MODE_COUNTS[run_mode]
    if run_mode == "full_98" and explicit_workers != 98:
        raise EnglishLegacyRecurationError(
            "english_legacy_full_requires_explicit_max_workers_98"
        )
    workers = explicit_workers or expected
    if workers != expected:
        raise EnglishLegacyRecurationError(
            "english_legacy_barrier_worker_count_mismatch"
        )
    return workers


class _ConcurrentRunState:
    """Thread-safe measured lifecycle for one common start-barrier cohort."""

    def __init__(
        self,
        selected: Sequence[Mapping[str, Any]],
        *,
        timeout_seconds: float,
    ) -> None:
        self._selected = tuple(selected)
        self._timeout_seconds = float(timeout_seconds)
        self._lock = threading.Lock()
        self._arrived = 0
        self._active = 0
        self._peak_active = 0
        self._released_at: str | None = None
        self._failure_code: str | None = None
        self._barrier_arrived_at: dict[int, str] = {}
        self._events: dict[int, list[dict[str, str]]] = {
            int(item["ordinal"]): [] for item in selected
        }
        self._barrier = threading.Barrier(
            len(selected), action=self._release, timeout=self._timeout_seconds
        )

    def _release(self) -> None:
        with self._lock:
            self._released_at = _now()

    def enter(self, item: Mapping[str, Any]) -> None:
        ordinal = int(item["ordinal"])
        arrived_at = _now()
        with self._lock:
            if ordinal in self._barrier_arrived_at:
                self._failure_code = "english_legacy_duplicate_worker_arrival"
                raise EnglishLegacyRecurationError(self._failure_code)
            self._barrier_arrived_at[ordinal] = arrived_at
            self._arrived += 1
            self._active += 1
            self._peak_active = max(self._peak_active, self._active)
        try:
            self._barrier.wait()
        except threading.BrokenBarrierError as exc:
            with self._lock:
                if self._failure_code is None:
                    self._failure_code = "english_legacy_start_barrier_failed"
            raise EnglishLegacyRecurationError(
                "english_legacy_start_barrier_failed"
            ) from exc

    def leave(self) -> None:
        with self._lock:
            self._active -= 1

    def observe(self, item: Mapping[str, Any], event: str) -> None:
        ordinal = int(item["ordinal"])
        with self._lock:
            events = self._events[ordinal]
            expected = (
                STAGE_EVENT_ORDER[len(events)]
                if len(events) < len(STAGE_EVENT_ORDER)
                else None
            )
            if event != expected:
                self._failure_code = "english_legacy_stage_order_invalid"
                raise EnglishLegacyRecurationError(self._failure_code)
            events.append({"name": event, "observed_at": _now()})

    def observed_model_call_count(self, item: Mapping[str, Any]) -> int:
        ordinal = int(item["ordinal"])
        with self._lock:
            return sum(
                event["name"].endswith("_submitted")
                for event in self._events[ordinal]
            )

    def attestation(
        self,
        *,
        run_mode: str,
        max_workers: int,
        rows: Sequence[Mapping[str, Any]],
        smoke_prerequisites: Mapping[str, str | None],
    ) -> dict[str, Any]:
        row_by_ordinal = {int(row["ordinal"]): row for row in rows}
        with self._lock:
            submitted_values = [
                event["observed_at"]
                for events in self._events.values()
                for event in events
                if event["name"] == "analysis_submitted"
            ]
            submitted_datetimes = [dt.datetime.fromisoformat(value) for value in submitted_values]
            first = min(submitted_datetimes).isoformat() if submitted_datetimes else None
            last = max(submitted_datetimes).isoformat() if submitted_datetimes else None
            spread = (
                int(round((max(submitted_datetimes) - min(submitted_datetimes)).total_seconds() * 1000))
                if submitted_datetimes
                else None
            )
            traces: list[dict[str, Any]] = []
            strict_count = 0
            exact_count = 0
            for item in self._selected:
                ordinal = int(item["ordinal"])
                events = [dict(event) for event in self._events[ordinal]]
                exact_sequence = tuple(event["name"] for event in events) == STAGE_EVENT_ORDER
                strict_count += int(exact_sequence)
                exact_count += int(exact_sequence)
                traces.append(
                    {
                        "ordinal": ordinal,
                        "target_id": str(item["target_id"]),
                        "barrier_arrived_at": self._barrier_arrived_at.get(ordinal),
                        "submitted_at": (
                            events[0]["observed_at"]
                            if events and events[0]["name"] == "analysis_submitted"
                            else None
                        ),
                        "stage_events": events,
                        "strict_stage_order_verified": exact_sequence,
                        "exact_two_calls_verified": exact_sequence,
                    }
                )
            result_gate = all(
                row_by_ordinal.get(int(item["ordinal"]), {}).get("status")
                == "succeeded"
                and row_by_ordinal[int(item["ordinal"])].get("model_call_count") == 2
                for item in self._selected
            )
            failure_code = self._failure_code
            if failure_code is None and not result_gate:
                failure_code = "english_legacy_work_item_quality_failed"
            if failure_code is None and strict_count != len(self._selected):
                failure_code = "english_legacy_stage_order_invalid"
            should_pass = (
                failure_code is None
                and self._arrived == len(self._selected)
                and self._released_at is not None
                and self._peak_active == len(self._selected)
                and strict_count == len(self._selected)
                and exact_count == len(self._selected)
            )
            return {
                "schema_version": CONCURRENCY_ATTESTATION_SCHEMA,
                "run_mode": run_mode,
                "max_workers": max_workers,
                "barrier_timeout_seconds": self._timeout_seconds,
                "barrier_expected": len(self._selected),
                "barrier_arrived": self._arrived,
                "barrier_released_at": self._released_at,
                "submitted_at_first": first,
                "submitted_at_last": last,
                "submitted_at_spread_ms": spread,
                "peak_active": self._peak_active,
                "item_traces": traces,
                "strict_stage_order_count": strict_count,
                "exact_two_call_count": exact_count,
                "smoke_prerequisites": dict(smoke_prerequisites),
                "gate_status": "passed" if should_pass else "failed_closed",
                "failure_code": None if should_pass else failure_code,
            }


def _run_selected(
    config: Mapping[str, Any],
    runtime_root: Path,
    selected: Sequence[Mapping[str, Any]],
    workers: int,
    *,
    run_mode: str,
    barrier_timeout_seconds: float,
    smoke_prerequisites: Mapping[str, str | None],
    item_runner: Callable[..., Mapping[str, Any]] = run_work_item,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    state = _ConcurrentRunState(
        selected, timeout_seconds=barrier_timeout_seconds
    )

    def execute(item: Mapping[str, Any]) -> Mapping[str, Any]:
        try:
            state.enter(item)
            return item_runner(
                config,
                runtime_root,
                item,
                stage_observer=lambda event: state.observe(item, event),
            )
        finally:
            state.leave()

    rows: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(execute, item): item
            for item in selected
        }
        for future in as_completed(futures):
            item = futures[future]
            try:
                rows.append(_result_row(item, future.result()))
            except EnglishLegacyRecurationError as exc:
                rows.append(
                    _failed_row(
                        item,
                        exc,
                        observed_model_call_count=state.observed_model_call_count(item),
                    )
                )
            except Exception as exc:
                rows.append(
                    _failed_row(
                        item,
                        EnglishLegacyRecurationError(
                            "english_legacy_unexpected_worker_failure"
                        ),
                        observed_model_call_count=state.observed_model_call_count(item),
                    )
                )
    checked_rows = sorted(rows, key=lambda row: int(row["ordinal"]))
    return checked_rows, state.attestation(
        run_mode=run_mode,
        max_workers=workers,
        rows=checked_rows,
        smoke_prerequisites=smoke_prerequisites,
    )


def main(argv: Sequence[str] | None = None) -> int:
    os.umask(0o077)
    args = parser().parse_args(argv)
    try:
        if args.command in {"build-work-items", "run-item", "run-batch"}:
            raise EnglishLegacyRecurationError(
                "english_legacy_recuration_authorization_revoked"
            )
        if args.command == "build-work-items":
            digest, path, manifest = build_authorized_work_items(
                disposition_root=args.disposition_root.expanduser().resolve(),
                authority_key_path=args.authority_key.expanduser().resolve(),
                authorization_expansion_closure_sha256=(
                    args.authorization_expansion_closure_sha256
                ),
                output_root=args.output_root.expanduser().resolve(),
                attempt=args.attempt,
            )
            _emit(
                {
                    "status": "materialized",
                    "work_item_batch_sha256": digest,
                    "work_item_batch_path": str(path),
                    "target_count": manifest["target_count"],
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
            )
            return 0
        if args.command == "inspect-batch":
            digest, batch, items = reopen_work_item_batch(args.work_item_batch)
            _emit(
                {
                    "status": "verified_not_run",
                    "work_item_batch_sha256": digest,
                    "target_count": batch["target_count"],
                    "reopened_work_item_count": len(items),
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
            )
            return 0
        config, runtime_root = _runtime_config(args.config, args.runtime_root)
        if args.command == "verify-quality":
            _emit(
                verify_quality_receipt(
                    config,
                    runtime_root,
                    args.quality_receipt_sha256,
                )
            )
            return 0
        if args.command == "verify-run-summary":
            _emit(
                verify_run_summary(
                    config,
                    runtime_root,
                    args.run_summary_sha256,
                )
            )
            return 0
        if args.command == "run-item":
            work_sha, item = reopen_work_item(args.work_item)
            result = run_work_item(config, runtime_root, item)
            _emit({"work_item_sha256": work_sha, **result})
            return 0 if result.get("status") in {"succeeded", "deduplicated"} else 2

        batch_sha, batch, items = reopen_work_item_batch(args.work_item_batch)
        run_mode = str(args.run_mode)
        expected_count = RUN_MODE_COUNTS[run_mode]
        if int(batch["target_count"]) != 98 or len(items) != 98:
            raise EnglishLegacyRecurationError(
                "english_legacy_en_p0_006_target_count_invalid"
            )
        if args.ordinal:
            requested = sorted(set(args.ordinal))
            if len(requested) != len(args.ordinal):
                raise EnglishLegacyRecurationError(
                    "english_legacy_duplicate_selection"
                )
        elif args.count is not None:
            if args.count > len(items):
                raise EnglishLegacyRecurationError(
                    "english_legacy_selection_out_of_range"
                )
            requested = list(range(1, args.count + 1))
        else:
            requested = list(range(1, expected_count + 1))
        if len(requested) != expected_count:
            raise EnglishLegacyRecurationError(
                "english_legacy_run_mode_selection_invalid"
            )
        if run_mode == "full_98" and requested != list(range(1, 99)):
            raise EnglishLegacyRecurationError(
                "english_legacy_full_selection_invalid"
            )
        item_by_ordinal = {int(item["ordinal"]): item for item in items}
        if any(ordinal not in item_by_ordinal for ordinal in requested):
            raise EnglishLegacyRecurationError(
                "english_legacy_selection_out_of_range"
            )
        selected = [item_by_ordinal[ordinal] for ordinal in requested]
        if (
            len({str(item["target_id"]) for item in selected}) != expected_count
            or len({int(item["ordinal"]) for item in selected}) != expected_count
            or len({row["work_item_sha256"] for row in batch["work_items"]}) != 98
        ):
            raise EnglishLegacyRecurationError(
                "english_legacy_distinct_work_item_requirement_failed"
            )
        attempts = {int(item["attempt"]) for item in items}
        if len(attempts) != 1:
            raise EnglishLegacyRecurationError(
                "english_legacy_batch_attempt_binding_invalid"
            )
        attempt = next(iter(attempts))
        if run_mode == "full_98":
            if (
                args.smoke_1_summary_sha256 is None
                or args.smoke_10_summary_sha256 is None
                or args.smoke_1_summary_sha256
                == args.smoke_10_summary_sha256
            ):
                raise EnglishLegacyRecurationError(
                    "english_legacy_smoke_prerequisites_required"
                )
        elif (
            args.smoke_1_summary_sha256 is not None
            or args.smoke_10_summary_sha256 is not None
        ):
            raise EnglishLegacyRecurationError(
                "english_legacy_unexpected_smoke_prerequisite"
            )
        workers = _workers_for_mode(run_mode, args.max_workers)
        smoke_prerequisites: dict[str, str | None] = {
            "smoke_1_summary_sha256": None,
            "smoke_10_summary_sha256": None,
        }
        if run_mode == "full_98":
            scope = {
                "authorization_expansion_closure_sha256": batch[
                    "authorization_expansion_closure_sha256"
                ],
                "batch_authorization_sha256": batch[
                    "batch_authorization_sha256"
                ],
                "inventory_sha256": batch["inventory_sha256"],
                "target_set_sha256": batch["target_set_sha256"],
            }
            verify_smoke_run_summary(
                config,
                runtime_root,
                args.smoke_1_summary_sha256,
                expected_mode="smoke_1",
                scope=scope,
                full_attempt=attempt,
            )
            verify_smoke_run_summary(
                config,
                runtime_root,
                args.smoke_10_summary_sha256,
                expected_mode="smoke_10",
                scope=scope,
                full_attempt=attempt,
            )
            smoke_prerequisites = {
                "smoke_1_summary_sha256": args.smoke_1_summary_sha256,
                "smoke_10_summary_sha256": args.smoke_10_summary_sha256,
            }
        started_at = dt.datetime.now(dt.timezone.utc).isoformat()
        rows, concurrency_attestation = _run_selected(
            config,
            runtime_root,
            selected,
            workers,
            run_mode=run_mode,
            barrier_timeout_seconds=args.barrier_timeout_seconds,
            smoke_prerequisites=smoke_prerequisites,
        )
        completed_at = dt.datetime.now(dt.timezone.utc).isoformat()
        summary_sha, summary_path, summary = publish_run_summary(
            config,
            runtime_root,
            work_item_batch_sha256=batch_sha,
            authorization_expansion_closure_sha256=batch[
                "authorization_expansion_closure_sha256"
            ],
            batch_authorization_sha256=batch["batch_authorization_sha256"],
            inventory_sha256=batch["inventory_sha256"],
            target_set_sha256=batch["target_set_sha256"],
            attempt=attempt,
            batch_target_count=int(batch["target_count"]),
            selected_ordinals=requested,
            results=rows,
            concurrency_attestation=concurrency_attestation,
            started_at=started_at,
            completed_at=completed_at,
        )
        _emit(
            {
                "status": summary["status"],
                "run_summary_sha256": summary_sha,
                "run_summary_path": str(summary_path),
                "selected_count": summary["selected_count"],
                "succeeded_count": summary["succeeded_count"],
                "deduplicated_count": summary["deduplicated_count"],
                "failed_count": summary["failed_count"],
                "run_mode": run_mode,
                "barrier_expected": concurrency_attestation["barrier_expected"],
                "barrier_arrived": concurrency_attestation["barrier_arrived"],
                "barrier_released_at": concurrency_attestation[
                    "barrier_released_at"
                ],
                "peak_active": concurrency_attestation["peak_active"],
                "submitted_at_spread_ms": concurrency_attestation[
                    "submitted_at_spread_ms"
                ],
                "model_call_count": summary["model_call_count"],
                "formal_write_count": 0,
            }
        )
        return 0 if summary["status"] == "passed" else 2
    except EnglishLegacyRecurationError as exc:
        _emit(
            {
                "status": "failed_closed",
                "error_code": exc.code,
                "model_call_count": 0,
                "formal_write_count": 0,
            }
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
