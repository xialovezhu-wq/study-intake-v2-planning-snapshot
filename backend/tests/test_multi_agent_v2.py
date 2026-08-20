from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
LIB = ROOT / "lib"
if str(LIB) not in sys.path:
    sys.path.insert(0, str(LIB))

from live_execution_gate import LiveExecutionDenied, assert_external_launch_allowed
from manual_capture_admission import (
    ManualAuthorizationStore,
    build_fixture_authorization,
    validate_authorization_for_task,
)
from multi_agent_events import MultiAgentEventError, parse_multi_agent_events
from multi_agent_runtime import MultiAgentTaskRuntime
from orchestration_plan import ReadPlanError, seal_read_plan, validate_read_plan
from read_branch import SubprocessBranchWorker
from read_bundle import (
    build_analysis_candidate,
    build_read_bundle,
    build_sol_handoff,
    review_candidate,
)
from read_fanout import ReadFanoutScheduler
from execution_quality_contract import decide_multi_agent_execution_quality
from subject_sol_contract import partition_multi_agent_sol_handoffs


SHA = "a" * 64


def branch(index: int, delay_ms: int, *, depends_on: list[str] | None = None, required: bool = True) -> dict:
    return {
        "branch_id": f"branch-{index:02d}",
        "purpose": f"read-domain-{index:02d}",
        "rationale": f"independent-domain-{index:02d}",
        "required": required,
        "depends_on": list(depends_on or []),
        "allowed_task_artifact_ids": ["artifact-dialogue"],
        "allowed_mcp_tools": ["get_task_context", "read_task_artifact", "get_records"],
        "collection_scope": [f"collection-{index:02d}"],
        "query_constraints": {"delay_ms": delay_ms},
        "maximum_calls": 8,
        "maximum_records": 100,
        "maximum_bytes": 65536,
        "completion_requirements": ["task-context", "artifact-complete", "library-evidence"],
        "failure_policy": "fail_closed" if required else "optional_missing",
        "expected_evidence_kinds": ["record"],
    }


def plan(branches: list[dict]) -> dict:
    return seal_read_plan(
        {
            "schema_version": "orchestration_read_plan_v1",
            "plan_id": "PLAN-FIXTURE-001",
            "subject": "math",
            "capture_id": "MFI-CAP-fixture",
            "frozen_task_sha256": "1" * 64,
            "release_id": "2" * 64,
            "activation_id": "3" * 64,
            "authority_snapshot_sha256": "4" * 64,
            "generation": "fixture-generation-1",
            "orchestrate_skill": {"id": "multi-agent-read-orchestrate", "version": "1.0.0", "sha256": "5" * 64},
            "terra_agent_contract_sha256": "6" * 64,
            "created_at": "2026-08-17T00:00:00+00:00",
            "branches": branches,
            "formal_write_count": 0,
        }
    )


def reidentify_plan(value: dict, *, plan_id: str, capture_id: str) -> dict:
    core = copy.deepcopy(value)
    core.pop("plan_sha256")
    core["plan_id"] = plan_id
    core["capture_id"] = capture_id
    return seal_read_plan(core)


class MultiAgentV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = ROOT / "tests" / "fixtures" / "fake_multi_agent_branch.py"
        self.execution = {
            "execution_mode": "fixture",
            "fixture_execution": {"allowed_executable_roots": [str(self.fixture.parent)]},
        }
        self.worker = SubprocessBranchWorker(
            execution_config=self.execution,
            command=[sys.executable, str(self.fixture)],
        )

    def test_true_parallel_wall_clock(self) -> None:
        value = plan([branch(1, 300), branch(2, 500), branch(3, 400)])
        started = time.monotonic()
        fanout = ReadFanoutScheduler(physical_slots=3).run(value, self.worker)
        elapsed = time.monotonic() - started
        self.assertEqual(fanout["terminal_branch_count"], 3)
        self.assertEqual(fanout["wave_count"], 1)
        self.assertEqual(fanout["maximum_active_branch_count"], 3)
        self.assertLess(elapsed, 1.05)
        self.assertGreater(elapsed, 0.45)
        self.assertEqual(len({row["mcp_launcher_pid"] for row in fanout["results"]}), 3)
        self.assertEqual(len({row["read_session_id"] for row in fanout["results"]}), 3)

    def test_eight_branches_run_in_waves_without_loss(self) -> None:
        value = plan([branch(index, 35) for index in range(1, 9)])
        fanout = ReadFanoutScheduler(physical_slots=3).run(value, self.worker)
        self.assertEqual(fanout["logical_branch_count"], 8)
        self.assertEqual(fanout["terminal_branch_count"], 8)
        self.assertEqual(fanout["dropped_branch_count"], 0)
        self.assertEqual(fanout["wave_count"], 3)
        self.assertEqual(fanout["maximum_active_branch_count"], 3)
        self.assertEqual({row["branch_id"] for row in fanout["results"]}, {f"branch-{index:02d}" for index in range(1, 9)})

    def test_twenty_logical_branches_are_never_truncated(self) -> None:
        value = plan([branch(index, 5) for index in range(1, 21)])
        fanout = ReadFanoutScheduler(physical_slots=4).run(value, self.worker)
        self.assertEqual(fanout["logical_branch_count"], 20)
        self.assertEqual(fanout["terminal_branch_count"], 20)
        self.assertEqual(fanout["dropped_branch_count"], 0)
        self.assertEqual(fanout["wave_count"], 5)
        self.assertEqual(fanout["maximum_active_branch_count"], 4)

    def test_round_robin_fairness_prevents_large_task_monopoly(self) -> None:
        large = reidentify_plan(
            plan([branch(index, 10) for index in range(1, 9)]),
            plan_id="PLAN-LARGE",
            capture_id="CAP-LARGE",
        )
        small = reidentify_plan(
            plan([branch(index, 10) for index in range(1, 3)]),
            plan_id="PLAN-SMALL",
            capture_id="CAP-SMALL",
        )
        execution = ReadFanoutScheduler(physical_slots=2).run_many(
            [large, small],
            {"PLAN-LARGE": self.worker, "PLAN-SMALL": self.worker},
        )
        first_wave_plans = {
            row["plan_id"] for row in execution["waves"][0]["branches"]
        }
        self.assertEqual(first_wave_plans, {"PLAN-LARGE", "PLAN-SMALL"})
        self.assertEqual(
            execution["tasks"]["PLAN-LARGE"]["terminal_branch_count"], 8
        )
        self.assertEqual(
            execution["tasks"]["PLAN-SMALL"]["terminal_branch_count"], 2
        )

    def test_dependency_chain_remains_serial(self) -> None:
        branches = [branch(1, 20), branch(2, 20, depends_on=["branch-01"]), branch(3, 20, depends_on=["branch-02"])]
        fanout = ReadFanoutScheduler(physical_slots=8).run(plan(branches), self.worker)
        self.assertEqual(fanout["wave_count"], 3)
        self.assertEqual([row["wave_index"] for row in fanout["results"]], [1, 2, 3])

    def test_read_bundle_and_issues_found_remain_sol_visible(self) -> None:
        value = plan([branch(1, 10), branch(2, 10)])
        fanout = ReadFanoutScheduler(physical_slots=2).run(value, self.worker)
        bundle = build_read_bundle(value, fanout)
        candidate = build_analysis_candidate(bundle)
        review = review_candidate(bundle, candidate, outcome="issues_found", issue_codes=["semantic_uncertainty"])
        handoff = build_sol_handoff(bundle, candidate, review)
        self.assertEqual(review["execution_status"], "succeeded")
        self.assertTrue(review["sol_review_ready"])
        self.assertFalse(review["quality_clean"])
        self.assertTrue(review["candidate_preserved"])
        self.assertTrue(review["risk_report_preserved"])
        self.assertFalse(review["automatic_retry"])
        self.assertFalse(handoff["formal_apply_authorized"])
        self.assertEqual(handoff["formal_write_count"], 0)
        decision = decide_multi_agent_execution_quality(
            quality_status="issues_found",
            technical_integrity_complete=True,
        )
        self.assertTrue(decision.sol_review_ready)
        self.assertFalse(decision.quality_clean)
        self.assertFalse(decision.automatic_retry)

    def test_host_runtime_produces_content_addressed_task_receipt(self) -> None:
        result = MultiAgentTaskRuntime(physical_slots=2).execute(
            plan=plan([branch(1, 5), branch(2, 5)]),
            branch_worker=self.worker,
            reviewer_outcome="accepted",
        )
        receipt = result["task_receipt"]
        self.assertEqual(receipt["execution_status"], "succeeded")
        self.assertTrue(receipt["sol_review_ready"])
        self.assertTrue(receipt["quality_clean"])
        self.assertRegex(receipt["receipt_sha256"], r"^[0-9a-f]{64}$")
        self.assertEqual(result["formal_write_count"], 0)

    def test_technical_integrity_error_requires_quarantine(self) -> None:
        value = plan([branch(1, 1)])
        fanout = ReadFanoutScheduler(physical_slots=1).run(value, self.worker)
        fanout["results"][0]["result_sha256"] = "0" * 64
        bundle = build_read_bundle(value, fanout)
        candidate = build_analysis_candidate(bundle)
        review = review_candidate(bundle, candidate, outcome="technical_quarantine", issue_codes=["receipt_chain_invalid"])
        handoff = build_sol_handoff(bundle, candidate, review)
        self.assertFalse(review["candidate_trusted"])
        self.assertTrue(review["diagnostic_review_ready"])
        self.assertIsNone(handoff["candidate_sha256"])

    def test_required_branch_failure_is_preserved_as_issue_not_retried(self) -> None:
        failed = branch(2, 0)
        failed["query_constraints"]["fail"] = True
        value = plan([branch(1, 5), failed])
        fanout = ReadFanoutScheduler(physical_slots=2).run(value, self.worker)
        bundle = build_read_bundle(value, fanout)
        self.assertEqual(bundle["required_failure_branch_ids"], ["branch-02"])
        candidate = build_analysis_candidate(bundle)
        review = review_candidate(
            bundle,
            candidate,
            outcome="issues_found",
            issue_codes=["required_branch_failed"],
        )
        self.assertTrue(review["sol_review_ready"])
        self.assertFalse(review["automatic_retry"])

    def test_timeout_reaps_exact_branch_process_group(self) -> None:
        slow_worker = SubprocessBranchWorker(
            execution_config=self.execution,
            command=[sys.executable, str(self.fixture)],
            timeout_seconds=0.05,
        )
        value = plan([branch(1, 500)])
        fanout = ReadFanoutScheduler(physical_slots=1).run(value, slow_worker)
        result = fanout["results"][0]
        self.assertEqual(result["status"], "timed_out")
        child_pid = result["mcp_launcher_pid"]
        with self.assertRaises(ProcessLookupError):
            os.kill(child_pid, 0)

    def test_pre_cancel_terminalizes_without_child_creation(self) -> None:
        cancellation = threading.Event()
        cancellation.set()
        value = plan([branch(1, 0)])
        fanout = ReadFanoutScheduler(physical_slots=1).run(
            value,
            self.worker,
            cancellation=cancellation,
        )
        result = fanout["results"][0]
        self.assertEqual(result["status"], "cancelled")
        self.assertIsNone(result["mcp_launcher_pid"])

    def test_issue_item_does_not_block_clean_item_or_quarantine_diagnostic(self) -> None:
        common = {
            "execution_status": "succeeded",
            "candidate_preserved": True,
            "risk_report_preserved": True,
            "automatic_retry": False,
            "diagnostic_review_ready": True,
            "formal_write_count": 0,
        }
        partition = partition_multi_agent_sol_handoffs(
            [
                {**common, "capture_id": "clean", "quality_status": "accepted", "sol_review_ready": True, "quality_clean": True},
                {**common, "capture_id": "issue", "quality_status": "issues_found", "sol_review_ready": True, "quality_clean": False},
                {**common, "capture_id": "quarantine", "quality_status": "technical_quarantine", "sol_review_ready": False, "quality_clean": False},
            ]
        )
        self.assertEqual(partition["sol_candidate_task_ids"], ["clean", "issue"])
        self.assertEqual(partition["issues_found_task_ids"], ["issue"])
        self.assertEqual(partition["diagnostic_task_ids"], ["quarantine"])
        self.assertEqual(partition["blocking_task_ids"], [])
        self.assertTrue(partition["sol_review_ready"])

    def test_invalid_plans_fail_closed(self) -> None:
        duplicate = branch(2, 0)
        duplicate["purpose"] = branch(1, 0)["purpose"]
        duplicate["rationale"] = "different words do not change semantics"
        duplicate["collection_scope"] = branch(1, 0)["collection_scope"]
        first = branch(1, 0)
        first["rationale"] = duplicate["rationale"]
        with self.assertRaises(ReadPlanError):
            plan([first, duplicate])
        cyclic = [branch(1, 0, depends_on=["branch-02"]), branch(2, 0, depends_on=["branch-01"])]
        with self.assertRaises(ReadPlanError):
            plan(cyclic)
        forbidden = branch(1, 0)
        forbidden["allowed_mcp_tools"] = ["write_records"]
        with self.assertRaises(ReadPlanError):
            plan([forbidden])

    def test_offline_tripwire_fires_before_popen(self) -> None:
        worker = SubprocessBranchWorker(
            execution_config={"execution_mode": "offline"},
            command=[sys.executable, str(self.fixture)],
        )
        with mock.patch("read_branch.subprocess.Popen") as popen:
            with self.assertRaises(Exception):
                ReadFanoutScheduler(physical_slots=1).run(plan([branch(1, 0)]), worker)
            popen.assert_not_called()

    def test_offline_tripwire_covers_every_forbidden_external_surface(self) -> None:
        for purpose in (
            "terra_orchestrator",
            "luna_reader",
            "terra_critical_reviewer",
            "provider_model_request",
            "production_mcp_launcher",
            "live_capture_consumer",
            "formal_writer",
        ):
            with self.subTest(purpose=purpose):
                with self.assertRaises(LiveExecutionDenied) as caught:
                    assert_external_launch_allowed(
                        {"execution_mode": "offline"},
                        purpose=purpose,
                        command=["/forbidden/child"],
                    )
                self.assertEqual(
                    caught.exception.code,
                    "offline_external_launch_forbidden",
                )
                self.assertFalse(caught.exception.evidence["child_process_created"])

    def test_fixture_rejects_real_codex_before_spawn(self) -> None:
        with self.assertRaises(LiveExecutionDenied) as caught:
            assert_external_launch_allowed(
                self.execution,
                purpose="fake_agent_worker",
                command=["/Applications/ChatGPT.app/Contents/Resources/codex", "exec", "--model", "gpt-5.6-terra"],
            )
        self.assertEqual(caught.exception.code, "fixture_real_codex_tripwire")

    def test_manual_authorization_exact_binding_and_relock(self) -> None:
        now = dt.datetime(2026, 8, 17, tzinfo=dt.timezone.utc)
        authorization = build_fixture_authorization(
            subject="math", capture_id="CAP-1", capture_content_sha256="1" * 64,
            release_id="2" * 64, activation_id="3" * 64, now=now,
        )
        validated = validate_authorization_for_task(
            authorization,
            task_identity={"subject": "math", "capture_id": "CAP-1", "capture_content_sha256": "1" * 64, "release_id": "2" * 64, "activation_id": "3" * 64},
            now=now + dt.timedelta(seconds=1),
        )
        self.assertEqual(validated["maximum_tasks"], 1)
        with tempfile.TemporaryDirectory() as temporary:
            store = ManualAuthorizationStore(Path(temporary), b"k" * 32)
            state = store.stage_fixture(
                authorization,
                now=now + dt.timedelta(seconds=1),
            )
            self.assertEqual(state["remaining_tasks"], 1)
            relocked = store.terminal_relock(authorization_sha256=authorization["authorization_sha256"], outcome="failed")
            self.assertEqual(relocked["state"]["status"], "locked")
            self.assertEqual(relocked["state"]["remaining_tasks"], 0)

    def test_multi_agent_event_parser_rejects_recursive_leaf(self) -> None:
        base = {
            "schema_version": "codex_multi_agent_event_fixture_v1",
            "status": "running", "payload_sha256": SHA, "formal_write_count": 0,
        }
        events = [
            {**base, "event_id": "e1", "sequence": 1, "event_type": "start", "role": "orchestrator", "parent_agent_id": None, "agent_id": "p", "branch_id": None, "model": "gpt-5.6-terra", "reasoning_effort": "ultra"},
            {**base, "event_id": "e2", "sequence": 2, "event_type": "spawn", "role": "reader", "parent_agent_id": "p", "agent_id": "c", "branch_id": "b", "model": "gpt-5.6-luna", "reasoning_effort": "max"},
            {**base, "event_id": "e3", "sequence": 3, "event_type": "spawn_child", "role": "reader", "parent_agent_id": "p", "agent_id": "c", "branch_id": "b", "model": "gpt-5.6-luna", "reasoning_effort": "max"},
        ]
        with self.assertRaises(MultiAgentEventError) as caught:
            parse_multi_agent_events(events)
        self.assertEqual(caught.exception.code, "reader_recursive_delegation_forbidden")

    def test_multi_agent_event_parser_accepts_eight_children_and_rejects_drift(self) -> None:
        base = {
            "schema_version": "codex_multi_agent_event_fixture_v1",
            "status": "running",
            "payload_sha256": SHA,
            "formal_write_count": 0,
        }
        events = [
            {
                **base,
                "event_id": "parent-start",
                "sequence": 1,
                "event_type": "start",
                "role": "orchestrator",
                "parent_agent_id": None,
                "agent_id": "parent",
                "branch_id": None,
                "model": "gpt-5.6-terra",
                "reasoning_effort": "ultra",
            }
        ]
        sequence = 1
        for index in range(8):
            child = f"child-{index}"
            branch_id = f"branch-{index}"
            sequence += 1
            events.append(
                {
                    **base,
                    "event_id": f"spawn-{index}",
                    "sequence": sequence,
                    "event_type": "spawn",
                    "role": "reader",
                    "parent_agent_id": "parent",
                    "agent_id": child,
                    "branch_id": branch_id,
                    "model": "gpt-5.6-luna",
                    "reasoning_effort": "max",
                }
            )
            sequence += 1
            events.append(
                {
                    **base,
                    "event_id": f"terminal-{index}",
                    "sequence": sequence,
                    "event_type": "terminal",
                    "role": "reader",
                    "parent_agent_id": "parent",
                    "agent_id": child,
                    "branch_id": branch_id,
                    "model": "gpt-5.6-luna",
                    "reasoning_effort": "max",
                    "status": "succeeded",
                }
            )
        receipt = parse_multi_agent_events(events)
        self.assertEqual(len(receipt["child_agent_ids"]), 8)
        self.assertEqual(receipt["event_count"], 17)

        duplicate = copy.deepcopy(events)
        duplicate[-1]["event_id"] = duplicate[-2]["event_id"]
        with self.assertRaises(MultiAgentEventError):
            parse_multi_agent_events(duplicate)
        out_of_order = copy.deepcopy(events)
        out_of_order[2]["sequence"] = 99
        with self.assertRaises(MultiAgentEventError):
            parse_multi_agent_events(out_of_order)
        wrong_model = copy.deepcopy(events)
        wrong_model[1]["model"] = "gpt-5.6-terra"
        with self.assertRaises(MultiAgentEventError):
            parse_multi_agent_events(wrong_model)

    def test_frozen_eight_child_codex_event_fixture_reopens(self) -> None:
        fixture = (
            ROOT
            / "tests"
            / "fixtures"
            / "multi_agent_v2"
            / "codex-events-8-children-v1.json"
        )
        events = json.loads(fixture.read_text(encoding="utf-8"))
        receipt = parse_multi_agent_events(events)
        self.assertEqual(receipt["event_count"], 17)
        self.assertEqual(len(receipt["child_agent_ids"]), 8)
        self.assertRegex(receipt["event_chain_sha256"], r"^[0-9a-f]{64}$")


if __name__ == "__main__":
    unittest.main()
