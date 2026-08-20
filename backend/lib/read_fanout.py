"""Bounded physical fan-out over unbounded logical read branches."""

from __future__ import annotations

import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Mapping

from orchestration_plan import validate_read_plan
from read_branch import branch_request


class ReadFanoutError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class ReadFanoutScheduler:
    def __init__(self, *, physical_slots: int) -> None:
        if isinstance(physical_slots, bool) or not isinstance(physical_slots, int) or physical_slots <= 0:
            raise ReadFanoutError("branch_physical_slots_invalid")
        self.physical_slots = physical_slots

    def run(
        self,
        plan: Mapping[str, Any],
        worker: Any,
        *,
        cancellation: threading.Event | None = None,
    ) -> dict[str, Any]:
        validated = validate_read_plan(plan)
        cancel = cancellation or threading.Event()
        branches = {row["branch_id"]: row for row in validated["branches"]}
        pending = set(branches)
        terminal: dict[str, dict[str, Any]] = {}
        waves: list[dict[str, Any]] = []
        maximum_active = 0
        started = time.monotonic()
        wave_index = 0
        while pending:
            ready = sorted(
                branch_id
                for branch_id in pending
                if set(branches[branch_id]["depends_on"]) <= set(terminal)
            )
            if not ready:
                raise ReadFanoutError("branch_scheduler_no_ready_work")
            selected = ready[: self.physical_slots]
            wave_index += 1
            wave_started = time.monotonic()
            maximum_active = max(maximum_active, len(selected))
            results: dict[str, dict[str, Any]] = {}
            with ThreadPoolExecutor(max_workers=len(selected), thread_name_prefix=f"read-wave-{wave_index}") as pool:
                futures = {
                    pool.submit(
                        worker.run,
                        branch_request(validated, branches[branch_id]),
                        wave_index=wave_index,
                        cancellation=cancel,
                    ): branch_id
                    for branch_id in selected
                }
                for future in as_completed(futures):
                    branch_id = futures[future]
                    try:
                        result = future.result()
                    except Exception as exc:
                        raise ReadFanoutError(f"branch_worker_exception:{branch_id}:{type(exc).__name__}") from exc
                    if result.get("branch_id") != branch_id or result.get("formal_write_count") != 0:
                        raise ReadFanoutError("branch_result_identity_invalid")
                    results[branch_id] = copy.deepcopy(dict(result))
            for branch_id in selected:
                terminal[branch_id] = results[branch_id]
                pending.remove(branch_id)
            waves.append(
                {
                    "wave_index": wave_index,
                    "branch_ids": selected,
                    "active_count": len(selected),
                    "duration_ms": round((time.monotonic() - wave_started) * 1000, 3),
                }
            )
        ordered = [terminal[row["branch_id"]] for row in validated["branches"]]
        return {
            "schema_version": "read_fanout_execution_v1",
            "plan_id": validated["plan_id"],
            "plan_sha256": validated["plan_sha256"],
            "logical_branch_count": len(branches),
            "physical_slot_count": self.physical_slots,
            "maximum_active_branch_count": maximum_active,
            "wave_count": len(waves),
            "waves": waves,
            "results": ordered,
            "pending_branch_count": 0,
            "terminal_branch_count": len(ordered),
            "dropped_branch_count": 0,
            "duration_ms": round((time.monotonic() - started) * 1000, 3),
            "formal_write_count": 0,
        }

    def run_many(
        self,
        plans: list[Mapping[str, Any]],
        workers: Mapping[str, Any],
        *,
        cancellation: threading.Event | None = None,
    ) -> dict[str, Any]:
        """Round-robin ready branches across tasks before filling a wave."""

        if not plans:
            raise ReadFanoutError("branch_task_set_empty")
        validated = [validate_read_plan(value) for value in plans]
        plan_ids = [value["plan_id"] for value in validated]
        if len(plan_ids) != len(set(plan_ids)) or set(workers) != set(plan_ids):
            raise ReadFanoutError("branch_task_worker_binding_invalid")
        branches = {
            value["plan_id"]: {
                row["branch_id"]: row for row in value["branches"]
            }
            for value in validated
        }
        pending = {
            plan_id: set(task_branches)
            for plan_id, task_branches in branches.items()
        }
        terminal: dict[str, dict[str, dict[str, Any]]] = {
            plan_id: {} for plan_id in plan_ids
        }
        waves: list[dict[str, Any]] = []
        cancel = cancellation or threading.Event()
        cursor = 0
        wave_index = 0
        started = time.monotonic()
        plan_by_id = {value["plan_id"]: value for value in validated}
        while any(pending.values()):
            selected: list[tuple[str, str]] = []
            attempted_without_selection = 0
            while len(selected) < self.physical_slots and attempted_without_selection < len(plan_ids):
                plan_id = plan_ids[cursor % len(plan_ids)]
                cursor += 1
                ready = sorted(
                    branch_id
                    for branch_id in pending[plan_id]
                    if set(branches[plan_id][branch_id]["depends_on"])
                    <= set(terminal[plan_id])
                    and (plan_id, branch_id) not in selected
                )
                if ready:
                    selected.append((plan_id, ready[0]))
                    attempted_without_selection = 0
                else:
                    attempted_without_selection += 1
            if not selected:
                raise ReadFanoutError("branch_scheduler_no_ready_work")
            wave_index += 1
            wave_started = time.monotonic()
            results: dict[tuple[str, str], dict[str, Any]] = {}
            with ThreadPoolExecutor(
                max_workers=len(selected),
                thread_name_prefix=f"fair-read-wave-{wave_index}",
            ) as pool:
                futures = {
                    pool.submit(
                        workers[plan_id].run,
                        branch_request(
                            plan_by_id[plan_id], branches[plan_id][branch_id]
                        ),
                        wave_index=wave_index,
                        cancellation=cancel,
                    ): (plan_id, branch_id)
                    for plan_id, branch_id in selected
                }
                for future in as_completed(futures):
                    key = futures[future]
                    results[key] = copy.deepcopy(dict(future.result()))
            for plan_id, branch_id in selected:
                terminal[plan_id][branch_id] = results[(plan_id, branch_id)]
                pending[plan_id].remove(branch_id)
            waves.append(
                {
                    "wave_index": wave_index,
                    "branches": [
                        {"plan_id": plan_id, "branch_id": branch_id}
                        for plan_id, branch_id in selected
                    ],
                    "duration_ms": round(
                        (time.monotonic() - wave_started) * 1000, 3
                    ),
                }
            )
        task_results: dict[str, dict[str, Any]] = {}
        for value in validated:
            plan_id = value["plan_id"]
            ordered = [
                terminal[plan_id][row["branch_id"]]
                for row in value["branches"]
            ]
            task_results[plan_id] = {
                "schema_version": "read_fanout_execution_v1",
                "plan_id": plan_id,
                "plan_sha256": value["plan_sha256"],
                "logical_branch_count": len(ordered),
                "physical_slot_count": self.physical_slots,
                "maximum_active_branch_count": min(
                    self.physical_slots, len(ordered)
                ),
                "wave_count": len(
                    {
                        result["wave_index"]
                        for result in ordered
                    }
                ),
                "waves": [
                    wave
                    for wave in waves
                    if any(
                        branch["plan_id"] == plan_id
                        for branch in wave["branches"]
                    )
                ],
                "results": ordered,
                "pending_branch_count": 0,
                "terminal_branch_count": len(ordered),
                "dropped_branch_count": 0,
                "duration_ms": round(
                    (time.monotonic() - started) * 1000, 3
                ),
                "formal_write_count": 0,
            }
        return {
            "schema_version": "read_fanout_fair_execution_v1",
            "fairness_policy": "round_robin_tasks",
            "physical_slot_count": self.physical_slots,
            "wave_count": len(waves),
            "waves": waves,
            "tasks": task_results,
            "formal_write_count": 0,
        }
