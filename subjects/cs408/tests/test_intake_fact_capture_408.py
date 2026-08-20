from __future__ import annotations

import hashlib
import hmac
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = PROJECT_ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import intake_fact_capture_408 as capture  # noqa: E402
import capture_hot_writer_408 as capture_hot  # noqa: E402
import capture_commit_index_408 as capture_commit  # noqa: E402


class IntakeFactCaptureTests(unittest.TestCase):
    GLOBAL_AUDIT_REPORT = {
        "result": "PASS",
        "items": [
            {
                "level": "PASS",
                "check": "bounded test aggregate",
                "message": "all committed objects passed",
            }
        ],
    }

    def _payload(self, **overrides: object) -> dict[str, object]:
        value: dict[str, object] = {
            "schema": capture.SCHEMA,
            "study_date": "2026-07-18",
            "timezone": "Asia/Shanghai",
            "idempotency_key": "capture:test:one",
            "stable_evidence_refs": [
                {
                    "kind": "details_card",
                    "locator": "obsidian://open?vault=408-details&file=cards%2Fsource-1",
                    "sha256": "a" * 64,
                }
            ],
            "source_facts": {
                "year": "2020",
                "subject": "OS",
                "question_type": "单项选择题",
                "source_id": "VISUAL_PENDING",
            },
            "user_facts": {
                "user_error_entry": "把状态变化发生的时点判断提前了",
                "user_error_provenance": "user_report",
                "first_action": "先标出状态发生变化的事件",
                "first_action_provenance": "user_report",
            },
            "identity_hint": {
                "status": "unknown",
                "mode": "unknown",
                "basis": "尚未执行正式库查重",
            },
            "answer_safe_context_anchor": "围绕状态转换事件判断执行时点的题目骨架",
            "missing_fields": ["formal_identity", "main_knowledge"],
            "formalization_authorized": True,
        }
        value.update(overrides)
        return value

    def _write(self, root: Path, value: dict[str, object], name: str = "capture.json") -> Path:
        path = root / name
        path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        return path

    def _prepare_global_audit_files(self, root: Path, batch_id: str) -> None:
        state = capture.status(None, repo_root=root)
        batch = state["batches"][batch_id]
        paths = [
            *capture.GLOBAL_AUDIT_TOOL_RELS,
            *capture._global_audit_object_paths(batch),
        ]
        for relative in paths:
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            if not path.exists():
                path.write_text(f"fixture:{relative.as_posix()}\n", encoding="utf-8")

    def _seal_global_audit(self, root: Path, batch_id: str) -> dict[str, object]:
        self._prepare_global_audit_files(root, batch_id)
        with mock.patch.object(
            capture,
            "_execute_global_audit",
            return_value=self.GLOBAL_AUDIT_REPORT,
        ):
            return capture.seal_global_audit(batch_id=batch_id, repo_root=root)

    def _close_pass(
        self, root: Path, batch_id: str, idempotency_key: str
    ) -> tuple[dict[str, object], dict[str, object]]:
        receipt = self._seal_global_audit(root, batch_id)
        closed = capture.close_batch(
            batch_id=batch_id,
            global_closeout="pass",
            global_audit_receipt_sha256=str(
                receipt["global_audit_receipt_sha256"]
            ),
            idempotency_key=idempotency_key,
            repo_root=root,
        )
        return closed, receipt

    def _active_curated_batch(
        self,
        root: Path,
        *,
        study_date: str = "2026-07-18",
        suffix: str = "audit",
        formal_id: str = "OS_2020_001",
    ) -> tuple[dict[str, object], str]:
        payload = self._payload(
            study_date=study_date,
            idempotency_key=f"capture:{suffix}",
        )
        captured = capture.capture(
            self._write(root, payload, f"{suffix}.json"), repo_root=root
        )
        batch = capture.start_batch(
            study_date,
            f"batch:{suffix}",
            repo_root=root,
        )
        capture.mark_result(
            batch_id=batch["batch_id"],
            capture_id=captured["capture_id"],
            outcome="curated",
            formal_id=formal_id,
            receipt_sha256=hashlib.sha256(
                f"receipt:{suffix}".encode("utf-8")
            ).hexdigest(),
            idempotency_key=f"result:{suffix}",
            repo_root=root,
        )
        return batch, captured["capture_id"]

    def _setup_review_event(
        self,
        root: Path,
        *,
        first_result: str = "wrong",
        provenance: str = "not_observed",
    ) -> str:
        policy = PROJECT_ROOT / "schema" / "morning-review-backflow-policy-v1.json"
        policy_target = root / capture.BACKFLOW_POLICY_REL
        policy_target.parent.mkdir(parents=True)
        policy_target.write_bytes(policy.read_bytes())
        queue = root / "queue.md"
        queue.write_text("---\nschema: morning_review_action_queue_v3\n---\n", encoding="utf-8")
        queue_hash = hashlib.sha256(queue.read_bytes()).hexdigest()
        session_id = "MR-review-capture"
        event_id = "RE-review-capture"
        started = {
            "schema": "morning_review_session_event_v1",
            "event_id": "session-start",
            "session_id": session_id,
            "sequence": 1,
            "timestamp": "2026-07-19T07:00:00+08:00",
            "event_type": "session_started",
            "payload": {
                "queue_schema": "morning_review_action_queue_v3",
                "queue_path": "queue.md",
                "queue_sha256": queue_hash,
                "review_date": "2026-07-19",
                "repair_gap": 3,
                "max_failed_repairs": 2,
                "items": [
                    {
                        "item_id": "MQ-01",
                        "source_id": "RU_OS_2020_001",
                        "item_kind": "formal_due",
                    }
                ],
            },
        }
        session_result = "blank" if first_result == "uncertain" else first_result
        first_event = {
            "schema": "morning_review_session_event_v1",
            "event_id": "session-first",
            "session_id": session_id,
            "sequence": 2,
            "timestamp": "2026-07-19T07:01:00+08:00",
            "event_type": "first_recorded",
            "payload": {
                "item_id": "MQ-01",
                "result": session_result,
                "confidence": "high",
                "response_seconds": 20.0,
                "prompt_level": "none",
                "first_break": "未观察到" if provenance == "not_observed" else "混淆状态转换时点",
                "first_break_provenance": provenance,
            },
        }
        bound_event = {
            "schema": "morning_review_session_event_v1",
            "event_id": "session-bound",
            "session_id": session_id,
            "sequence": 3,
            "timestamp": "2026-07-19T07:01:01+08:00",
            "event_type": "first_bound_to_loop",
            "payload": {
                "item_id": "MQ-01",
                "canonical_event_id": event_id,
            },
        }
        session_ledger = root / capture.MORNING_SESSION_REL / session_id / "events.jsonl"
        session_ledger.parent.mkdir(parents=True)
        session_ledger.write_text(
            "\n".join(
                json.dumps(row, ensure_ascii=False)
                for row in (started, first_event, bound_event)
            )
            + "\n",
            encoding="utf-8",
        )
        event = {
            "schema": capture.REVIEW_EVENT_SCHEMA,
            "event_kind": "outcome",
            "event_id": event_id,
            "idempotency_key": "morning:review-capture:first",
            "event_time": "2026-07-19T07:01:00+08:00",
            "observed_date": "2026-07-19",
            "source": "morning_review",
            "session_id": session_id,
            "item_id": "MQ-01",
            "source_id": "RU_OS_2020_001",
            "mechanism_key": "状态转换边界",
            "knowledge_point": "OS02-01",
            "formal_node_id": "OS_2020_001",
            "first_result": first_result,
            "confidence": "high",
            "prompt_level": "none",
            "first_break": "未观察到" if provenance == "not_observed" else "混淆状态转换时点",
            "first_break_provenance": provenance,
            "question_valid": True,
            "route": {
                "kind": "update_existing",
                "formal_write_authorized": False,
                "candidate": {"formal_node_id": "OS_2020_001"},
            },
            "formal_write_authorized": False,
            "generated_actions": [],
        }
        review_ledger = root / capture.REVIEW_LOOP_LEDGER_REL
        review_ledger.parent.mkdir(parents=True)
        review_ledger.write_text(json.dumps(event, ensure_ascii=False) + "\n", encoding="utf-8")
        return event_id

    def _neutral_intent_binding(
        self,
        root: Path,
        *,
        event_id: str,
        request_id: str,
        message_sha256: str,
        source_attestation_sha256: str = "1" * 64,
    ) -> dict[str, object]:
        display_sha = "d" * 64
        message_size = 12
        authorization = {
            "schema": "managed_408_explicit_authorization_binding_v1",
            "request_id": request_id,
            "display_receipt_sha256": display_sha,
            "authorization_message_sha256": message_sha256,
            "authorization_message_size_bytes": message_size,
        }
        binding_sha = hashlib.sha256(
            capture._canonical(authorization) + b"\n"
        ).hexdigest()
        stable = {
            "schema": "managed_408_save_current_result_intent_v1",
            "kind": "save_current_result_intent",
            "request_id": request_id,
            "source_transaction_kind": "prepared_option_turn",
            "source_transaction_attestation_sha256": source_attestation_sha256,
            "display_receipt_sha256": display_sha,
            "review_event_id": event_id,
            "observed_date": "2026-07-19",
            "session_review_date": "2026-07-19",
            "session_result": "independent_correct",
            "capture_status": "not_eligible",
            "authorization_message_sha256": message_sha256,
            "authorization_message_size_bytes": message_size,
            "authorization_binding_sha256": binding_sha,
            "authorization_body_stored": False,
            "formal_write_count": 0,
        }
        intent = {
            **stable,
            "intent_id": "MSCRI-"
            + hashlib.sha256(capture._canonical(stable) + b"\n")
            .hexdigest()[:24]
            .upper(),
        }
        raw = (
            json.dumps(intent, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n"
        ).encode("utf-8")
        intent_sha = hashlib.sha256(raw).hexdigest()
        relative = capture.MANAGED_RICH_INTENT_OBJECT_REL / f"{intent_sha}.json"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
        return {
            "source_transaction_kind": "prepared_option_turn",
            "source_transaction_attestation_sha256": source_attestation_sha256,
            "authorization_message_sha256": message_sha256,
            "authorization_binding_sha256": binding_sha,
            "save_request_id": request_id,
            "display_receipt_sha256": display_sha,
            "save_intent_receipt_sha256": intent_sha,
            "save_intent_receipt_ref": relative.as_posix(),
            "authorization_message_size_bytes": message_size,
        }

    def _write_needs_user_resolution_evidence(
        self,
        root: Path,
        runtime: Path,
        *,
        capture_row: dict[str, object],
        prior_batch_id: str,
        source_event_id: str,
        candidate_id: str,
        formal_id: str,
    ) -> tuple[str, str]:
        root = root.resolve()
        runtime = runtime.resolve()
        confirmation_event_id = "RE-needs-user-confirmation"
        confirmation = {
            "schema": capture.REVIEW_EVENT_SCHEMA,
            "event_kind": "candidate_confirmation",
            "event_id": confirmation_event_id,
            "event_time": "2026-07-19T08:00:00+08:00",
            "idempotency_key": f"confirm:{candidate_id}:{formal_id}",
            "candidate_id": candidate_id,
            "explicit_user_confirmation": True,
            "resolved_formal_node_id": formal_id,
            "handoff_skill": "kaoyan-408-wrong-intake",
            "formal_write_authorized": False,
        }
        review_ledger = root / capture.REVIEW_LOOP_LEDGER_REL
        with review_ledger.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(confirmation, ensure_ascii=False) + "\n")

        repo_key = capture._repo_key(root)
        transaction_id = "TXN-resolution-test"
        transaction_root = runtime / "transactions" / repo_key / transaction_id
        transaction_root.mkdir(parents=True)
        journal_path = transaction_root / "journal.json"
        items = [{"formal_id": formal_id, "job_id": "", "mode": "redo"}]
        source_event = capture._review_event_by_id(root, source_event_id)
        package = {
            "schema": "intake_package_v1",
            "formal_id": formal_id,
            "mode": "redo",
            "date_source": f"首答事件 {source_event_id}；用户确认归入 {formal_id}",
            "capture_binding": {
                "batch_id": prior_batch_id,
                "capture_id": capture_row["capture_id"],
                "capture_payload_sha256": capture_row["payload_sha256"],
                "stable_evidence_sha256": capture._sha256_value(source_event),
                "identity_resolution": f"用户明确确认归入 {formal_id}",
            },
        }
        idempotency_key = "sha256:resolution-test"
        payload_sha256 = "9" * 64
        journal = {
            "schema": capture.NORMAL_WAL_SCHEMA,
            "state": "committed",
            "transaction_id": transaction_id,
            "idempotency_key": idempotency_key,
            "payload_sha256": payload_sha256,
            "repo_root": str(root),
            "payload_identity": {"packages": [package], "queue_claims": []},
            "result_items": items,
        }
        journal_path.write_text(
            json.dumps(journal, ensure_ascii=False, sort_keys=True), encoding="utf-8"
        )
        (root / "节点总表.md").write_text("formal table\n", encoding="utf-8")
        (root / f"{formal_id}.md").write_text("safe card\n", encoding="utf-8")
        receipt = {
            "schema": capture.NORMAL_RECEIPT_SCHEMA,
            "status": "COMMITTED",
            "repo_root": str(root),
            "transaction_id": transaction_id,
            "idempotency_key": idempotency_key,
            "payload_sha256": payload_sha256,
            "journal": str(journal_path),
            "items": items,
            "changed_files": [
                str(root / "节点总表.md"),
                str(root / f"{formal_id}.md"),
            ],
            "committed_at": "2026-07-19T08:01:00+08:00",
        }
        receipt_root = runtime / "receipts" / repo_key
        receipt_root.mkdir(parents=True)
        receipt_path = receipt_root / (
            hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest() + ".json"
        )
        receipt_path.write_text(
            json.dumps(receipt, ensure_ascii=False, sort_keys=True), encoding="utf-8"
        )
        return confirmation_event_id, hashlib.sha256(receipt_path.read_bytes()).hexdigest()

    def _setup_ordinary_review_event(
        self,
        root: Path,
        *,
        source: str = "daily_practice",
        first_result: str = "wrong",
        provenance: str = "user_report",
        formal_node_id: str | None = "CO_2020_001",
        question_valid: bool = True,
    ) -> str:
        event_id = "RE-ordinary-capture"
        first_break = (
            "未观察到"
            if provenance == "not_observed"
            else "混淆了中断响应与服务程序的职责边界"
        )
        mapped = [formal_node_id] if formal_node_id else []
        candidate = (
            {
                "candidate_id": "RC-ordinary-capture",
                "formal_node_id": formal_node_id,
            }
            if formal_node_id
            else None
        )
        event = {
            "schema": capture.REVIEW_EVENT_SCHEMA,
            "event_kind": "outcome",
            "event_id": event_id,
            "idempotency_key": "ordinary:review-capture:first",
            "request_hash": "b" * 64,
            "event_time": "2026-07-23T20:10:00+08:00",
            "observed_date": "2026-07-23",
            "source": source,
            "session_id": "DP-2026-07-23",
            "item_id": "DP-Q1",
            "source_id": "EXAM408-2020-Q15",
            "mechanism_key": "KP:CO05-18",
            "knowledge_point": "CO05-18",
            "formal_node_id": formal_node_id,
            "mapped_formal_node_ids": mapped,
            "review_unit_id": None,
            "first_result": first_result,
            "confidence": "medium",
            "hint_used": False,
            "prompt_level": "none",
            "first_break": first_break,
            "first_break_provenance": provenance,
            "correction_result": "not_attempted",
            "response_seconds": 30.0,
            "latency_state": "normal",
            "cross_day": False,
            "question_valid": question_valid,
            "organic_baseline": True,
            "evidence_kind": "failure",
            "stable_retention": False,
            "fragile_mastery": False,
            "trigger_action_id": None,
            "generated_actions": [],
            "route": {
                "kind": "update_existing" if formal_node_id else "new_wrong_candidate",
                "formal_write_authorized": False,
                "candidate": candidate,
            },
            "legacy_import": False,
            "formal_write_authorized": False,
        }
        review_ledger = root / capture.REVIEW_LOOP_LEDGER_REL
        review_ledger.parent.mkdir(parents=True, exist_ok=True)
        review_ledger.write_text(
            json.dumps(event, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        return event_id

    def test_capture_is_durable_idempotent_and_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = self._write(root, self._payload())
            first = capture.capture(payload, repo_root=root)
            second = capture.capture(payload, repo_root=root)
            current = capture.status("2026-07-18", repo_root=root)
            audit = capture.audit(repo_root=root)
            fact_event = json.loads(
                (capture.capture_root(root) / "events.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()[0]
            )

        self.assertEqual(first["status"], "CAPTURED_PENDING_CURATION")
        self.assertTrue(first["created"])
        self.assertEqual(second["status"], "ALREADY_CAPTURED")
        self.assertFalse(second["created"])
        self.assertEqual(current["pending_capture_ids"], [first["capture_id"]])
        self.assertEqual(
            current["captures"][first["capture_id"]]["recorded_at"],
            fact_event["created_at"],
        )
        self.assertEqual(audit["status"], "PASS")
        self.assertEqual(audit["event_count"], 1)

    def test_fast_capture_keeps_all_formal_and_schedule_surfaces_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            protected = {
                "节点总表.md": b"formal-node-sentinel\n",
                "关系边表.md": b"formal-edge-sentinel\n",
                "复习单元总表.md": b"review-unit-sentinel\n",
                "原题复做轨总表.md": b"original-track-sentinel\n",
            }
            for relative, raw in protected.items():
                (root / relative).write_bytes(raw)
            before = {
                relative: hashlib.sha256((root / relative).read_bytes()).hexdigest()
                for relative in protected
            }

            result = capture.capture(
                self._write(root, self._payload()), repo_root=root
            )

            after = {
                relative: hashlib.sha256((root / relative).read_bytes()).hexdigest()
                for relative in protected
            }
        self.assertEqual(result["status"], "CAPTURED_PENDING_CURATION")
        self.assertEqual(before, after)
        self.assertFalse(result.get("formal_write_authorized", False))

    def test_morning_failure_event_becomes_one_authorized_fast_capture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_review_event(root)
            first = capture.capture_review_event(event_id, repo_root=root)
            second = capture.capture_review_event(event_id, repo_root=root)
            current = capture.status("2026-07-19", repo_root=root)
            report = capture.audit(repo_root=root)

        self.assertEqual(first["quality_status"], "awaiting_daily_curation")
        self.assertTrue(first["created"])
        self.assertFalse(first["formal_write_authorized"])
        self.assertEqual(second["status"], "ALREADY_CAPTURED")
        self.assertEqual(second["capture_id"], first["capture_id"])
        stored = current["captures"][first["capture_id"]]["capture"]
        self.assertTrue(stored["formalization_authorized"])
        self.assertEqual(stored["user_facts"]["user_error_entry"], "未观察到")
        self.assertEqual(
            {item["kind"] for item in stored["stable_evidence_refs"]},
            {
                "review_loop_event",
                "morning_session_start",
                "morning_session_first",
                "morning_session_queue",
            },
        )
        self.assertEqual(report["review_evidence_ref_count"], 4)

    def test_correct_morning_event_can_be_saved_neutrally_without_curation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_review_event(
                root, first_result="independent_correct"
            )
            first_binding = self._neutral_intent_binding(
                root,
                event_id=event_id,
                request_id="neutral-save-first",
                message_sha256="2" * 64,
            )
            first = capture.save_neutral_review_event(
                event_id,
                **first_binding,
                repo_root=root,
            )
            second_binding = self._neutral_intent_binding(
                root,
                event_id=event_id,
                request_id="neutral-save-second",
                message_sha256="4" * 64,
            )
            second = capture.save_neutral_review_event(
                event_id,
                **second_binding,
                repo_root=root,
            )
            current = capture.status("2026-07-19", repo_root=root)
            started = capture.start_batch(
                "2026-07-19", "neutral-save-must-not-freeze", repo_root=root
            )
            with mock.patch.object(
                capture,
                "_verify_neutral_source_transaction",
                return_value=None,
            ):
                report = capture.audit(repo_root=root)

        self.assertEqual(first["event_status"], "SAVED_NEUTRAL")
        self.assertEqual(first["status"], "saved_neutral")
        self.assertTrue(first["created"])
        self.assertEqual(second["event_status"], "ALREADY_SAVED_NEUTRAL")
        self.assertFalse(second["created"])
        self.assertEqual(second["neutral_save_id"], first["neutral_save_id"])
        self.assertEqual(current["capture_count"], 0)
        self.assertEqual(current["pending_capture_ids"], [])
        self.assertEqual(current["neutral_save_count"], 1)
        self.assertEqual(started["status"], "NO_PENDING_CAPTURE")
        stored = current["neutral_saves"][first["neutral_save_id"]]
        self.assertFalse(stored["curation_eligible"])
        self.assertFalse(stored["formalization_authorized"])
        self.assertEqual(report["neutral_save_count"], 1)

    def test_noncorrect_morning_event_cannot_be_saved_neutrally(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_review_event(root, first_result="wrong")
            binding = self._neutral_intent_binding(
                root,
                event_id=event_id,
                request_id="neutral-save-wrong",
                message_sha256="2" * 64,
            )
            with self.assertRaisesRegex(capture.CaptureError, "正确首答"):
                capture.save_neutral_review_event(
                    event_id,
                    **binding,
                    repo_root=root,
                )
            self.assertFalse(capture.capture_root(root).exists())

    def test_ordinary_review_event_becomes_unconfirmed_capture_without_morning_chain(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_ordinary_review_event(root)
            first = capture.capture_ordinary_review_event(event_id, repo_root=root)
            second = capture.capture_ordinary_review_event(event_id, repo_root=root)
            current = capture.status("2026-07-23", repo_root=root)
            report = capture.audit(repo_root=root)

        self.assertEqual(first["quality_status"], "captured_unconfirmed")
        self.assertEqual(first["status"], "CAPTURED_UNCONFIRMED")
        self.assertFalse(first["formalization_authorized"])
        self.assertFalse(first["formal_write_authorized"])
        self.assertEqual(second["status"], "ALREADY_CAPTURED")
        self.assertEqual(second["capture_id"], first["capture_id"])
        self.assertEqual(current["pending_capture_ids"], [])
        stored = current["captures"][first["capture_id"]]["capture"]
        self.assertFalse(stored["formalization_authorized"])
        self.assertEqual(
            {item["kind"] for item in stored["stable_evidence_refs"]},
            {"ordinary_review_loop_event"},
        )
        self.assertEqual(stored["study_date"], "2026-07-23")
        self.assertEqual(stored["source_facts"]["source_id"], "EXAM408-2020-Q15")
        self.assertEqual(stored["identity_hint"]["formal_id"], "CO_2020_001")
        self.assertEqual(
            stored["user_facts"]["user_error_provenance"], "user_report"
        )
        self.assertEqual(report["ordinary_review_evidence_ref_count"], 1)
        self.assertEqual(report["review_evidence_ref_count"], 1)

    def test_evening_ordinary_bridge_preserves_visible_provenance_but_not_authority(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_ordinary_review_event(
                root, source="evening_d0", provenance="visible_evidence"
            )
            result = capture.capture_ordinary_review_event(event_id, repo_root=root)
            stored = capture.status("2026-07-23", repo_root=root)["captures"][
                result["capture_id"]
            ]["capture"]

        self.assertEqual(stored["source_facts"]["question_type"], "晚间 D0 首答")
        self.assertEqual(
            stored["user_facts"]["user_error_provenance"], "visible_evidence"
        )
        self.assertFalse(stored["formalization_authorized"])

    def test_ordinary_bridge_does_not_upgrade_derived_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_ordinary_review_event(
                root, provenance="derived_classification", formal_node_id=None
            )
            result = capture.capture_ordinary_review_event(event_id, repo_root=root)
            stored = capture.status("2026-07-23", repo_root=root)["captures"][
                result["capture_id"]
            ]["capture"]

        self.assertEqual(stored["user_facts"]["user_error_entry"], "未观察到")
        self.assertEqual(
            stored["user_facts"]["user_error_provenance"], "not_observed"
        )
        self.assertEqual(stored["identity_hint"]["status"], "unknown")

    def test_ordinary_bridge_does_not_treat_not_observed_sentinel_as_direct_fact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_ordinary_review_event(
                root, provenance="user_report"
            )
            review_ledger = root / capture.REVIEW_LOOP_LEDGER_REL
            event = json.loads(review_ledger.read_text(encoding="utf-8"))
            event["first_break"] = "未观察到"
            review_ledger.write_text(
                json.dumps(event, ensure_ascii=False) + "\n", encoding="utf-8"
            )
            result = capture.capture_ordinary_review_event(event_id, repo_root=root)
            stored = capture.status("2026-07-23", repo_root=root)["captures"][
                result["capture_id"]
            ]["capture"]

        self.assertEqual(stored["user_facts"]["user_error_entry"], "未观察到")
        self.assertEqual(
            stored["user_facts"]["user_error_provenance"], "not_observed"
        )

    def test_manual_ordinary_event_ref_cannot_set_formalization_authorized(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_ordinary_review_event(root)
            event = capture._review_event_by_id(root, event_id)
            payload = capture._ordinary_review_capture_payload(event)
            payload["formalization_authorized"] = True
            with self.assertRaisesRegex(capture.CaptureError, "不能自动授权"):
                capture._capture_value(payload, repo_root=root)
            self.assertFalse(capture.capture_root(root).exists())

    def test_ordinary_bridge_rejects_morning_correct_or_invalid_outcome(self) -> None:
        cases = [
            {"source": "morning_review"},
            {"first_result": "independent_correct"},
            {"question_valid": False},
        ]
        for overrides in cases:
            with self.subTest(overrides=overrides), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                event_id = self._setup_ordinary_review_event(root, **overrides)
                with self.assertRaises(capture.CaptureError):
                    capture.capture_ordinary_review_event(event_id, repo_root=root)
                self.assertFalse(capture.capture_root(root).exists())

    def test_ordinary_audit_rejects_event_hash_drift(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_ordinary_review_event(root)
            capture.capture_ordinary_review_event(event_id, repo_root=root)
            review_ledger = root / capture.REVIEW_LOOP_LEDGER_REL
            event = json.loads(review_ledger.read_text(encoding="utf-8"))
            event["confidence"] = "low"
            review_ledger.write_text(
                json.dumps(event, ensure_ascii=False) + "\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(capture.CaptureError, "SHA-256"):
                capture.audit(repo_root=root)

    def test_ordinary_audit_checks_date_source_identity_and_direct_provenance(self) -> None:
        mutations = {
            "study_date": lambda payload: payload.update(study_date="2026-07-22"),
            "source_id": lambda payload: payload["source_facts"].update(
                source_id="EXAM408-2020-Q16"
            ),
            "formal identity": lambda payload: payload.update(
                identity_hint={
                    "status": "unknown",
                    "formal_id": "",
                    "mode": "unknown",
                    "basis": "正式身份留待日终依据稳定证据核验",
                }
            ),
            "provenance": lambda payload: payload.update(
                user_facts={
                    "user_error_entry": "未观察到",
                    "user_error_provenance": "not_observed",
                    "observed_at": "2026-07-23T20:10:00+08:00",
                }
            ),
        }
        expected_errors = {
            "study_date": "study_date",
            "source_id": "source_id",
            "formal identity": "formal identity",
            "provenance": "provenance",
        }
        for label, mutate in mutations.items():
            with self.subTest(field=label), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                event_id = self._setup_ordinary_review_event(root)
                event = capture._review_event_by_id(root, event_id)
                payload = capture._ordinary_review_capture_payload(event)
                payload["idempotency_key"] = f"ordinary:tamper:{label.replace(' ', '-')}"
                mutate(payload)
                capture._capture_value(payload, repo_root=root)
                with self.assertRaisesRegex(
                    capture.CaptureError, expected_errors[label]
                ):
                    capture.audit(repo_root=root)

    def test_morning_capture_rejects_provenance_upgrade_from_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_review_event(
                root, provenance="derived_classification"
            )
            review_ledger = root / capture.REVIEW_LOOP_LEDGER_REL
            event = json.loads(review_ledger.read_text(encoding="utf-8"))
            event["first_break_provenance"] = "user_report"
            review_ledger.write_text(
                json.dumps(event, ensure_ascii=False) + "\n", encoding="utf-8"
            )

            with self.assertRaisesRegex(
                capture.CaptureError, "first_break_provenance"
            ):
                capture.capture_review_event(event_id, repo_root=root)

            self.assertFalse(capture.capture_root(root).exists())

    def test_matching_user_report_provenance_is_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_review_event(root, provenance="user_report")
            result = capture.capture_review_event(event_id, repo_root=root)
            current = capture.status("2026-07-19", repo_root=root)

        stored = current["captures"][result["capture_id"]]["capture"]
        self.assertEqual(
            stored["user_facts"],
            {
                "user_error_entry": "混淆状态转换时点",
                "user_error_provenance": "user_report",
                "observed_at": "2026-07-19T07:01:00+08:00",
            },
        )

    def test_legacy_missing_provenance_stays_answer_safe(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_review_event(root, provenance="not_observed")
            session_ledger = (
                root
                / capture.MORNING_SESSION_REL
                / "MR-review-capture"
                / "events.jsonl"
            )
            session_events = [
                json.loads(line)
                for line in session_ledger.read_text(encoding="utf-8").splitlines()
            ]
            session_events[1]["payload"].pop("first_break_provenance")
            session_ledger.write_text(
                "\n".join(json.dumps(row, ensure_ascii=False) for row in session_events)
                + "\n",
                encoding="utf-8",
            )
            review_ledger = root / capture.REVIEW_LOOP_LEDGER_REL
            event = json.loads(review_ledger.read_text(encoding="utf-8"))
            event.pop("first_break_provenance")
            review_ledger.write_text(
                json.dumps(event, ensure_ascii=False) + "\n", encoding="utf-8"
            )

            result = capture.capture_review_event(event_id, repo_root=root)
            current = capture.status("2026-07-19", repo_root=root)

        stored = current["captures"][result["capture_id"]]["capture"]
        self.assertEqual(stored["user_facts"]["user_error_entry"], "未观察到")
        self.assertEqual(
            stored["user_facts"]["user_error_provenance"], "not_observed"
        )

    def test_morning_correct_event_is_not_fast_captured(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            event_id = self._setup_review_event(root, first_result="independent_correct")
            with self.assertRaisesRegex(capture.CaptureError, "不属于快速捕获范围"):
                capture.capture_review_event(event_id, repo_root=root)
            self.assertFalse(capture.capture_root(root).exists())

    def test_reused_idempotency_key_with_different_payload_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = self._write(root, self._payload(), "first.json")
            changed = self._payload(answer_safe_context_anchor="不同的安全上下文骨架")
            second = self._write(root, changed, "second.json")
            capture.capture(first, repo_root=root)
            with self.assertRaisesRegex(capture.CaptureError, "不同事实载荷"):
                capture.capture(second, repo_root=root)

    def test_temporary_locator_and_answer_bearing_fields_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            temporary = self._payload(
                stable_evidence_refs=[
                    {"kind": "image", "locator": "/private/tmp/question.png", "sha256": "b" * 64}
                ]
            )
            with self.assertRaisesRegex(capture.CaptureError, "临时路径"):
                capture.capture(self._write(root, temporary, "temporary.json"), repo_root=root)

            forbidden = self._payload()
            forbidden["correct_answer"] = "A"
            with self.assertRaisesRegex(capture.CaptureError, "受保护字段"):
                capture.capture(self._write(root, forbidden, "forbidden.json"), repo_root=root)

    def test_unobserved_user_error_cannot_be_inferred(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            value = self._payload(
                user_facts={
                    "user_error_entry": "大概是定义不熟",
                    "user_error_provenance": "not_observed",
                }
            )
            with self.assertRaisesRegex(capture.CaptureError, "不得写入推断错因"):
                capture.capture(self._write(root, value), repo_root=root)

    def test_daily_batch_uses_exact_explicit_date_and_records_terminal_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = capture.capture(
                self._write(root, self._payload(), "day-one.json"), repo_root=root
            )
            day_two = self._payload(
                study_date="2026-07-19", idempotency_key="capture:test:two"
            )
            capture.capture(self._write(root, day_two, "day-two.json"), repo_root=root)

            batch = capture.start_batch(
                "2026-07-18", "curation:start:2026-07-18", repo_root=root
            )
            self.assertEqual(batch["capture_ids"], [first["capture_id"]])
            result = capture.mark_result(
                batch_id=batch["batch_id"],
                capture_id=first["capture_id"],
                outcome="curated",
                formal_id="OS_2020_001",
                receipt_sha256="c" * 64,
                idempotency_key="curation:result:one",
                repo_root=root,
            )
            closed, _ = self._close_pass(
                root,
                batch["batch_id"],
                "curation:close:2026-07-18",
            )
            state = capture.status("2026-07-18", repo_root=root)
            ledger_audit = capture.audit(repo_root=root)

        self.assertEqual(result["outcome"], "curated")
        self.assertEqual(closed["status"], "COMPLETE")
        self.assertEqual(state["pending_capture_ids"], [])
        self.assertEqual(
            state["captures"][first["capture_id"]]["formal_id"], "OS_2020_001"
        )
        self.assertEqual(
            ledger_audit["aggregate_global_audit_receipt_count"], 1
        )

    def test_batch_cannot_close_before_every_capture_has_a_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = self._write(root, self._payload())
            capture.capture(payload, repo_root=root)
            batch = capture.start_batch(
                "2026-07-18", "curation:start:incomplete", repo_root=root
            )
            with self.assertRaisesRegex(capture.CaptureError, "未记录结果"):
                capture.close_batch(
                    batch_id=batch["batch_id"],
                    global_closeout="pass",
                    idempotency_key="curation:close:incomplete",
                    repo_root=root,
                )

    def test_new_pass_close_requires_verified_aggregate_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            batch, _ = self._active_curated_batch(root, suffix="receipt-required")
            with self.assertRaisesRegex(capture.CaptureError, "必须提供 aggregate"):
                capture.close_batch(
                    batch_id=batch["batch_id"],
                    global_closeout="pass",
                    idempotency_key="close:receipt-required",
                    repo_root=root,
                )

    def test_failed_item_cannot_close_with_global_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            captured = capture.capture(
                self._write(
                    root,
                    self._payload(idempotency_key="capture:failed-pass"),
                    "failed-pass.json",
                ),
                repo_root=root,
            )
            batch = capture.start_batch(
                "2026-07-18", "batch:failed-pass", repo_root=root
            )
            capture.mark_result(
                batch_id=batch["batch_id"],
                capture_id=captured["capture_id"],
                outcome="failed",
                reason="正式写入失败",
                idempotency_key="result:failed-pass",
                repo_root=root,
            )
            receipt = self._seal_global_audit(root, batch["batch_id"])
            ledger = capture.capture_root(root) / "events.jsonl"
            before = ledger.read_bytes()

            with self.assertRaisesRegex(
                capture.CaptureError,
                "不得声明 global_closeout=pass",
            ):
                capture.close_batch(
                    batch_id=batch["batch_id"],
                    global_closeout="pass",
                    global_audit_receipt_sha256=str(
                        receipt["global_audit_receipt_sha256"]
                    ),
                    idempotency_key="close:failed-pass",
                    repo_root=root,
                )

            self.assertEqual(before, ledger.read_bytes())
            closed = capture.close_batch(
                batch_id=batch["batch_id"],
                global_closeout="failed",
                idempotency_key="close:failed-correctly",
                repo_root=root,
            )
            self.assertEqual("PARTIAL", closed["status"])
            self.assertEqual(
                [captured["capture_id"]], closed["failed_capture_ids"]
            )

    def test_forged_content_addressed_receipt_without_attestation_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            batch, _ = self._active_curated_batch(root, suffix="forged-receipt")
            receipt_root = (
                capture.capture_root(root) / capture.GLOBAL_AUDIT_RECEIPT_DIR
            )
            receipt_root.mkdir(parents=True)
            raw = capture._canonical(
                {
                    "schema": capture.GLOBAL_AUDIT_RECEIPT_SCHEMA,
                    "audit_status": "PASS",
                }
            )
            digest = hashlib.sha256(raw).hexdigest()
            (receipt_root / f"{digest}.json").write_bytes(raw)
            with self.assertRaisesRegex(capture.CaptureError, "attestation"):
                capture.close_batch(
                    batch_id=batch["batch_id"],
                    global_closeout="pass",
                    global_audit_receipt_sha256=digest,
                    idempotency_key="close:forged-receipt",
                    repo_root=root,
                )

    def test_aggregate_receipt_survives_repository_relocation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            container = Path(tmp)
            original = container / "old-mount" / "kaoyan-408"
            relocated = container / "new-mount" / "kaoyan-408"
            original.mkdir(parents=True)
            batch, _ = self._active_curated_batch(
                original, suffix="repository-relocation"
            )
            receipt = self._seal_global_audit(original, batch["batch_id"])

            relocated.parent.mkdir(parents=True)
            original.rename(relocated)
            closed = capture.close_batch(
                batch_id=batch["batch_id"],
                global_closeout="pass",
                global_audit_receipt_sha256=str(
                    receipt["global_audit_receipt_sha256"]
                ),
                idempotency_key="close:repository-relocation",
                repo_root=relocated,
            )
            report = capture.audit(repo_root=relocated)

        self.assertEqual("COMPLETE", closed["status"])
        self.assertEqual("PASS", report["status"])
        self.assertEqual(1, report["aggregate_global_audit_receipt_count"])

    def test_aggregate_receipt_rejects_unsigned_origin_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            batch, _ = self._active_curated_batch(
                root, suffix="origin-tampering"
            )
            receipt = self._seal_global_audit(root, batch["batch_id"])
            receipt_path = Path(str(receipt["receipt_path"]))
            value = json.loads(receipt_path.read_bytes())
            value["repo_root"] = "/tampered/repository"
            raw = capture._canonical(value)
            digest = hashlib.sha256(raw).hexdigest()
            (receipt_path.parent / f"{digest}.json").write_bytes(raw)

            with self.assertRaisesRegex(capture.CaptureError, "attestation"):
                capture.close_batch(
                    batch_id=batch["batch_id"],
                    global_closeout="pass",
                    global_audit_receipt_sha256=digest,
                    idempotency_key="close:origin-tampering",
                    repo_root=root,
                )

    def test_aggregate_receipt_rejects_wrong_signed_relative_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            batch, _ = self._active_curated_batch(
                root, suffix="relative-identity"
            )
            receipt = self._seal_global_audit(root, batch["batch_id"])
            receipt_path = Path(str(receipt["receipt_path"]))
            value = json.loads(receipt_path.read_bytes())
            value["audit_tool_commitments"][0]["path"] = "scripts/wrong.py"
            value["audit_tool_commitments_sha256"] = capture._sha256_value(
                value["audit_tool_commitments"]
            )
            unsigned = {
                key: item
                for key, item in value.items()
                if key != "attestation"
            }
            key = capture._global_audit_key(
                capture.capture_root(root), create=False
            )
            value["attestation"]["signature"] = hmac.new(
                key,
                capture._canonical(unsigned),
                hashlib.sha256,
            ).hexdigest()
            raw = capture._canonical(value)
            digest = hashlib.sha256(raw).hexdigest()
            (receipt_path.parent / f"{digest}.json").write_bytes(raw)

            with self.assertRaisesRegex(capture.CaptureError, "相对路径身份非法"):
                capture.close_batch(
                    batch_id=batch["batch_id"],
                    global_closeout="pass",
                    global_audit_receipt_sha256=digest,
                    idempotency_key="close:relative-identity",
                    repo_root=root,
                )

    def test_aggregate_receipt_rejects_result_drift_after_seal(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            batch, _ = self._active_curated_batch(root, suffix="result-drift")
            receipt = self._seal_global_audit(root, batch["batch_id"])
            ledger = capture.capture_root(root) / "events.jsonl"
            events = [json.loads(line) for line in ledger.read_text().splitlines()]
            result = next(
                event
                for event in events
                if event.get("event_type") == "curation_item_result"
            )
            result["receipt_sha256"] = "f" * 64
            result_payload = {
                key: result.get(key)
                for key in (
                    "batch_id",
                    "capture_id",
                    "outcome",
                    "formal_id",
                    "receipt_sha256",
                    "verification_sha256",
                    "reason",
                )
            }
            result["result_sha256"] = capture._sha256_value(result_payload)
            ledger.write_text(
                "".join(
                    json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n"
                    for event in events
                ),
                encoding="utf-8",
            )
            capture.reconcile(repo_root=root)
            with self.assertRaisesRegex(capture.CaptureError, "逐题结果"):
                capture.close_batch(
                    batch_id=batch["batch_id"],
                    global_closeout="pass",
                    global_audit_receipt_sha256=str(
                        receipt["global_audit_receipt_sha256"]
                    ),
                    idempotency_key="close:result-drift",
                    repo_root=root,
                )

    def test_aggregate_receipt_rejects_object_drift_and_path_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            batch, _ = self._active_curated_batch(root, suffix="object-drift")
            receipt = self._seal_global_audit(root, batch["batch_id"])
            (root / "节点总表.md").write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(capture.CaptureError, "对象已漂移"):
                capture.close_batch(
                    batch_id=batch["batch_id"],
                    global_closeout="pass",
                    global_audit_receipt_sha256=str(
                        receipt["global_audit_receipt_sha256"]
                    ),
                    idempotency_key="close:object-drift",
                    repo_root=root,
                )

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            batch, _ = self._active_curated_batch(root, suffix="path-replacement")
            receipt = self._seal_global_audit(root, batch["batch_id"])
            path = Path(str(receipt["receipt_path"]))
            backup = path.with_suffix(".backup")
            path.replace(backup)
            path.symlink_to(backup)
            with self.assertRaisesRegex(capture.CaptureError, "符号链接"):
                capture.close_batch(
                    batch_id=batch["batch_id"],
                    global_closeout="pass",
                    global_audit_receipt_sha256=str(
                        receipt["global_audit_receipt_sha256"]
                    ),
                    idempotency_key="close:path-replacement",
                    repo_root=root,
                )

    def test_receipt_for_another_batch_cannot_close_current_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first, _ = self._active_curated_batch(root, suffix="wrong-batch-one")
            first_closed, receipt = self._close_pass(
                root, first["batch_id"], "close:wrong-batch-one"
            )
            self.assertEqual(first_closed["status"], "COMPLETE")
            second, _ = self._active_curated_batch(
                root,
                study_date="2026-07-19",
                suffix="wrong-batch-two",
                formal_id="OS_2020_002",
            )
            self._prepare_global_audit_files(root, second["batch_id"])
            with self.assertRaisesRegex(capture.CaptureError, "批次或逐题结果"):
                capture.close_batch(
                    batch_id=second["batch_id"],
                    global_closeout="pass",
                    global_audit_receipt_sha256=str(
                        receipt["global_audit_receipt_sha256"]
                    ),
                    idempotency_key="close:wrong-batch-two",
                    repo_root=root,
                )

    def test_legacy_unsealed_closed_event_remains_read_only_replayable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            batch, _ = self._active_curated_batch(root, suffix="legacy-close")
            self._close_pass(root, batch["batch_id"], "close:legacy-close")
            ledger = capture.capture_root(root) / "events.jsonl"
            events = [json.loads(line) for line in ledger.read_text().splitlines()]
            closed = events[-1]
            closed.pop("close_contract")
            closed.pop("global_audit_receipt_sha256")
            legacy_payload = {
                key: closed[key]
                for key in (
                    "batch_id",
                    "status",
                    "global_closeout",
                    "needs_user_capture_ids",
                    "failed_capture_ids",
                )
            }
            closed["close_sha256"] = capture._sha256_value(legacy_payload)
            ledger.write_text(
                "".join(
                    json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n"
                    for event in events
                ),
                encoding="utf-8",
            )
            capture.reconcile(repo_root=root)
            report = capture.audit(repo_root=root)

        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["legacy_unsealed_close_count"], 1)
        self.assertEqual(report["aggregate_global_audit_receipt_count"], 0)

    def test_unconfirmed_capture_is_not_selected_for_daily_batch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            value = self._payload(
                idempotency_key="capture:test:unconfirmed",
                formalization_authorized=False,
            )
            result = capture.capture(self._write(root, value), repo_root=root)
            batch = capture.start_batch(
                "2026-07-18", "curation:start:unconfirmed", repo_root=root
            )
            current = capture.status("2026-07-18", repo_root=root)

        self.assertEqual(result["quality_status"], "captured_unconfirmed")
        self.assertEqual(batch["status"], "NO_PENDING_CAPTURE")
        self.assertEqual(current["pending_capture_ids"], [])

    def test_direct_cold_authorization_is_rejected_without_any_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            value = self._payload(
                idempotency_key="capture:test:authorize-later",
                formalization_authorized=False,
            )
            captured = capture.capture(self._write(root, value), repo_root=root)
            truth = capture.capture_root(root)
            before_ledger = (truth / "events.jsonl").read_bytes()
            before_state = (truth / "state.json").read_bytes()
            with self.assertRaisesRegex(
                capture.CaptureError, "直接.*授权已禁用"
            ):
                capture.authorize_capture(
                    capture_id=captured["capture_id"],
                    idempotency_key="authorize:test:one",
                    repo_root=root,
                )
            after_direct_ledger = (truth / "events.jsonl").read_bytes()
            after_direct_state = (truth / "state.json").read_bytes()
            batch = capture.start_batch(
                "2026-07-18", "curation:start:authorized", repo_root=root
            )

        self.assertEqual(before_ledger, after_direct_ledger)
        self.assertEqual(before_state, after_direct_state)
        self.assertEqual("NO_PENDING_CAPTURE", batch["status"])

    def test_direct_hot_authorization_is_rejected_without_any_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            value = self._payload(
                idempotency_key="capture:test:authorize-hot",
                formalization_authorized=False,
            )
            captured = capture.capture(self._write(root, value), repo_root=root)
            truth = capture.capture_root(root)
            ledger = truth / "events.jsonl"
            state = truth / "state.json"
            derived = truth / ".capture-hot-writer-v1"
            with capture_hot.capture_ledger_lock(ledger) as token:
                capture_hot.cold_bootstrap(
                    ledger,
                    state,
                    derived,
                    lock_token=token,
                )
            before_ledger = ledger.read_bytes()
            before_state = state.read_bytes()
            with self.assertRaisesRegex(
                capture.CaptureError, "直接 capture hot 授权已禁用"
            ):
                capture.authorize_capture_hot(
                    capture_id=str(captured["capture_id"]),
                    idempotency_key="authorize:test:hot-wrapper",
                    repo_root=root,
                )
            after_ledger = ledger.read_bytes()
            after_state = state.read_bytes()
        self.assertEqual(before_ledger, after_ledger)
        self.assertEqual(before_state, after_state)

    def test_named_legacy_recovery_only_rebuilds_an_existing_authorization(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            value = self._payload(
                idempotency_key="capture:test:legacy-recovery",
                formalization_authorized=False,
            )
            captured = capture.capture(self._write(root, value), repo_root=root)
            truth = capture.capture_root(root)
            ledger = truth / "events.jsonl"
            state = truth / "state.json"
            bundle = capture_commit.cold_rebuild(
                ledger, state, state_replayer=capture.replay
            )
            fact_receipt = next(iter(bundle["receipts"].values()))
            derived = truth / ".capture-hot-writer-v1"
            with capture_hot.capture_ledger_lock(ledger) as token:
                capture_hot.cold_bootstrap(
                    ledger, state, derived, lock_token=token
                )
            key = "authorize:test:historical-existing"
            legacy_event = {
                "schema": capture.EVENT_SCHEMA,
                "event_type": "capture_authorized",
                "event_id": "CE-"
                + hashlib.sha256(
                    ("authorize:" + key).encode("utf-8")
                ).hexdigest()[:20],
                "idempotency_key": key,
                "created_at": "2026-07-18T02:00:00+00:00",
                "capture_id": captured["capture_id"],
            }
            line = (
                json.dumps(legacy_event, ensure_ascii=False, sort_keys=True)
                + "\n"
            ).encode("utf-8")
            with ledger.open("ab") as handle:
                handle.write(line)
            before_lines = ledger.read_bytes().count(b"\n")
            recovered = capture.recover_existing_legacy_authorization(
                capture_id=str(captured["capture_id"]),
                authorization_event_id=str(legacy_event["event_id"]),
                authorization_event_sha256=hashlib.sha256(line).hexdigest(),
                fact_receipt_sha256=str(fact_receipt["receipt_sha256"]),
                repo_root=root,
            )
            after_lines = ledger.read_bytes().count(b"\n")
            recovered_state = json.loads(state.read_text(encoding="utf-8"))

        self.assertEqual(
            "RECOVERED_EXISTING_LEGACY_AUTHORIZATION",
            recovered["status"],
        )
        self.assertEqual(0, recovered["event_append_count"])
        self.assertEqual(before_lines, after_lines)
        self.assertTrue(
            recovered_state["captures"][captured["capture_id"]][
                "formalization_authorized"
            ]
        )

    def test_needs_user_keeps_daily_batch_partial(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            first = capture.capture(
                self._write(root, self._payload(), "needs-user.json"), repo_root=root
            )
            batch = capture.start_batch(
                study_date="2026-07-18",
                idempotency_key="batch-needs-user",
                repo_root=root,
            )
            capture.mark_result(
                batch_id=batch["batch_id"],
                capture_id=first["capture_id"],
                outcome="needs_user",
                formal_id=None,
                receipt_sha256=None,
                reason="用户错误入口存在冲突，需要本人确认",
                idempotency_key="result-needs-user",
                repo_root=root,
            )
            closed, _ = self._close_pass(
                root, batch["batch_id"], "close-needs-user"
            )

        self.assertEqual(closed["status"], "PARTIAL")
        self.assertEqual(closed["needs_user_capture_ids"], [first["capture_id"]])
        self.assertEqual(closed["failed_capture_ids"], [])

    def test_resolved_needs_user_retries_same_set_and_carries_prior_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            runtime = root / "runtime"
            source_event_id = self._setup_review_event(
                root, provenance="visible_evidence"
            )
            review_ledger = root / capture.REVIEW_LOOP_LEDGER_REL
            source_event = json.loads(review_ledger.read_text(encoding="utf-8"))
            candidate_id = "RC-needs-user-resolution"
            source_event["formal_node_id"] = None
            source_event["mapped_formal_node_ids"] = [
                "OS_2020_001",
                "OS_2020_002",
            ]
            source_event["route"] = {
                "kind": "needs_user",
                "formal_write_authorized": False,
                "candidate": {
                    "candidate_id": candidate_id,
                    "formal_node_id": None,
                    "kind": "needs_user",
                    "status": "needs_user",
                },
            }
            review_ledger.write_text(
                json.dumps(source_event, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            unresolved = capture.capture_review_event(
                source_event_id, repo_root=root
            )
            other_payload = self._payload(
                study_date="2026-07-19",
                idempotency_key="capture:test:carried-success",
            )
            carried = capture.capture(
                self._write(root, other_payload, "carried.json"), repo_root=root
            )
            first_batch = capture.start_batch(
                "2026-07-19", "batch:needs-user:A01", repo_root=root
            )
            capture.mark_result(
                batch_id=first_batch["batch_id"],
                capture_id=carried["capture_id"],
                outcome="curated",
                formal_id="DS_2020_001",
                receipt_sha256="e" * 64,
                idempotency_key="result:carried:A01",
                repo_root=root,
            )
            capture.mark_result(
                batch_id=first_batch["batch_id"],
                capture_id=unresolved["capture_id"],
                outcome="needs_user",
                reason="复习单元映射两个正式节点，需要本人确认唯一身份",
                idempotency_key="result:needs-user:A01",
                repo_root=root,
            )
            first_closed, _ = self._close_pass(
                root, first_batch["batch_id"], "close:needs-user:A01"
            )
            capture_row = capture.status("2026-07-19", repo_root=root)[
                "captures"
            ][unresolved["capture_id"]]
            confirmation_event_id, receipt_sha = (
                self._write_needs_user_resolution_evidence(
                    root,
                    runtime,
                    capture_row=capture_row,
                    prior_batch_id=first_batch["batch_id"],
                    source_event_id=source_event_id,
                    candidate_id=candidate_id,
                    formal_id="OS_2020_001",
                )
            )
            resolved = capture.resolve_needs_user(
                study_date="2026-07-19",
                capture_id=unresolved["capture_id"],
                formal_id="OS_2020_001",
                confirmation_event_id=confirmation_event_id,
                normal_receipt_sha256=receipt_sha,
                idempotency_key="resolve:needs-user:A01",
                repo_root=root,
                runtime_root=runtime,
            )
            second_batch = capture.start_batch(
                "2026-07-19", "batch:needs-user:A02", repo_root=root
            )
            state_after_start = capture.status(None, repo_root=root)
            event_path = capture.capture_root(root) / "events.jsonl"
            before_carried_noop = event_path.read_bytes()
            carried_noop = capture.mark_result(
                batch_id=second_batch["batch_id"],
                capture_id=carried["capture_id"],
                outcome="curated",
                formal_id="DS_2020_001",
                receipt_sha256="e" * 64,
                idempotency_key="result:carried:A02:noop",
                repo_root=root,
            )
            self.assertEqual(event_path.read_bytes(), before_carried_noop)
            with self.assertRaisesRegex(capture.CaptureError, "不得重放 apply"):
                capture.mark_result(
                    batch_id=second_batch["batch_id"],
                    capture_id=unresolved["capture_id"],
                    outcome="curated",
                    formal_id="OS_2020_001",
                    receipt_sha256="f" * 64,
                    idempotency_key="result:needs-user:A02:wrong-receipt",
                    repo_root=root,
                )
            capture.mark_result(
                batch_id=second_batch["batch_id"],
                capture_id=unresolved["capture_id"],
                outcome="already_current",
                formal_id="OS_2020_001",
                verification_sha256="a" * 64,
                idempotency_key="result:needs-user:A02",
                repo_root=root,
            )
            second_closed, _ = self._close_pass(
                root, second_batch["batch_id"], "close:needs-user:A02"
            )
            with mock.patch.object(capture, "DEFAULT_RUNTIME_ROOT", runtime):
                report = capture.audit(repo_root=root)

        self.assertEqual(first_closed["status"], "PARTIAL")
        self.assertEqual(resolved["status"], "NEEDS_USER_RESOLVED")
        self.assertEqual(second_batch["attempt"], 2)
        self.assertEqual(
            second_batch["capture_set_sha256"],
            first_batch["capture_set_sha256"],
        )
        self.assertEqual(second_batch["capture_ids"], first_batch["capture_ids"])
        self.assertEqual(
            second_batch["retry_capture_ids"], [unresolved["capture_id"]]
        )
        self.assertEqual(
            second_batch["carried_capture_ids"], [carried["capture_id"]]
        )
        self.assertEqual(carried_noop["status"], "ALREADY_RECORDED")
        self.assertEqual(
            state_after_start["batches"][second_batch["batch_id"]]["results"][
                carried["capture_id"]
            ]["carried_from_batch_id"],
            first_batch["batch_id"],
        )
        self.assertEqual(second_closed["status"], "COMPLETE")
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["needs_user_resolution_count"], 1)
        self.assertEqual(report["carried_result_count"], 1)

    def test_needs_user_resolution_rejects_wrong_date_or_unbound_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            runtime = root / "runtime"
            source_event_id = self._setup_review_event(
                root, provenance="visible_evidence"
            )
            review_ledger = root / capture.REVIEW_LOOP_LEDGER_REL
            source_event = json.loads(review_ledger.read_text(encoding="utf-8"))
            candidate_id = "RC-needs-user-negative"
            source_event["formal_node_id"] = None
            source_event["route"] = {
                "kind": "needs_user",
                "formal_write_authorized": False,
                "candidate": {"candidate_id": candidate_id},
            }
            review_ledger.write_text(
                json.dumps(source_event, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
            unresolved = capture.capture_review_event(
                source_event_id, repo_root=root
            )
            batch = capture.start_batch(
                "2026-07-19", "batch:negative:A01", repo_root=root
            )
            capture.mark_result(
                batch_id=batch["batch_id"],
                capture_id=unresolved["capture_id"],
                outcome="needs_user",
                reason="需要确认唯一正式身份",
                idempotency_key="result:negative:A01",
                repo_root=root,
            )
            self._close_pass(root, batch["batch_id"], "close:negative:A01")
            capture_row = capture.status("2026-07-19", repo_root=root)[
                "captures"
            ][unresolved["capture_id"]]
            confirmation_event_id, receipt_sha = (
                self._write_needs_user_resolution_evidence(
                    root,
                    runtime,
                    capture_row=capture_row,
                    prior_batch_id=batch["batch_id"],
                    source_event_id=source_event_id,
                    candidate_id=candidate_id,
                    formal_id="OS_2020_001",
                )
            )
            with self.assertRaisesRegex(capture.CaptureError, "显式日期"):
                capture.resolve_needs_user(
                    study_date="2026-07-18",
                    capture_id=unresolved["capture_id"],
                    formal_id="OS_2020_001",
                    confirmation_event_id=confirmation_event_id,
                    normal_receipt_sha256=receipt_sha,
                    idempotency_key="resolve:negative:date",
                    repo_root=root,
                    runtime_root=runtime,
                )
            with self.assertRaisesRegex(capture.CaptureError, "唯一的 normal receipt"):
                capture.resolve_needs_user(
                    study_date="2026-07-19",
                    capture_id=unresolved["capture_id"],
                    formal_id="OS_2020_001",
                    confirmation_event_id=confirmation_event_id,
                    normal_receipt_sha256="0" * 64,
                    idempotency_key="resolve:negative:receipt",
                    repo_root=root,
                    runtime_root=runtime,
                )
            self.assertEqual(
                capture.status("2026-07-19", repo_root=root)["captures"][
                    unresolved["capture_id"]
                ]["quality_status"],
                "needs_user",
            )
            identity_only = capture.resolve_needs_user(
                study_date="2026-07-19",
                capture_id=unresolved["capture_id"],
                formal_id="OS_2020_001",
                confirmation_event_id=confirmation_event_id,
                idempotency_key="resolve:negative:identity-only",
                repo_root=root,
                runtime_root=runtime,
            )
            retry = capture.start_batch(
                "2026-07-19", "batch:negative:A02", repo_root=root
            )
            capture.mark_result(
                batch_id=retry["batch_id"],
                capture_id=unresolved["capture_id"],
                outcome="curated",
                formal_id="OS_2020_001",
                receipt_sha256="c" * 64,
                idempotency_key="result:negative:A02",
                repo_root=root,
            )
            final, _ = self._close_pass(
                root, retry["batch_id"], "close:negative:A02"
            )

        self.assertEqual(identity_only["resolution_kind"], "confirmed_identity")
        self.assertIsNone(identity_only["normal_receipt_sha256"])
        self.assertEqual(retry["attempt"], 2)
        self.assertEqual(final["status"], "COMPLETE")

    def test_already_current_is_audited_terminal_without_fake_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = capture.capture(
                self._write(root, self._payload(), "already-current.json"), repo_root=root
            )
            batch = capture.start_batch(
                "2026-07-18", "curation:start:already-current", repo_root=root
            )
            result = capture.mark_result(
                batch_id=batch["batch_id"],
                capture_id=first["capture_id"],
                outcome="already_current",
                formal_id="OS_2020_001",
                verification_sha256="d" * 64,
                idempotency_key="curation:result:already-current",
                repo_root=root,
            )
            closed, _ = self._close_pass(
                root,
                batch["batch_id"],
                "curation:close:already-current",
            )
            state = capture.status("2026-07-18", repo_root=root)

        self.assertEqual(result["receipt_sha256"], None)
        self.assertEqual(result["verification_sha256"], "d" * 64)
        self.assertEqual(closed["status"], "COMPLETE")
        self.assertEqual(
            state["captures"][first["capture_id"]]["quality_status"],
            "already_current",
        )

    def test_already_current_rejects_a_fake_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = capture.capture(
                self._write(root, self._payload(), "fake-receipt.json"), repo_root=root
            )
            batch = capture.start_batch(
                "2026-07-18", "curation:start:fake-receipt", repo_root=root
            )
            with self.assertRaisesRegex(capture.CaptureError, "不能伪造"):
                capture.mark_result(
                    batch_id=batch["batch_id"],
                    capture_id=first["capture_id"],
                    outcome="already_current",
                    formal_id="OS_2020_001",
                    receipt_sha256="c" * 64,
                    verification_sha256="d" * 64,
                    idempotency_key="curation:result:fake-receipt",
                    repo_root=root,
                )

    def test_batch_operations_replay_after_state_changes_or_close(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = capture.capture(
                self._write(root, self._payload(), "replay.json"), repo_root=root
            )
            batch = capture.start_batch(
                "2026-07-18", "batch:replay:start", repo_root=root
            )
            result_args = {
                "batch_id": batch["batch_id"],
                "capture_id": first["capture_id"],
                "outcome": "curated",
                "formal_id": "OS_2020_001",
                "receipt_sha256": "e" * 64,
                "idempotency_key": "batch:replay:result",
                "repo_root": root,
            }
            capture.mark_result(**result_args)
            _, receipt = self._close_pass(
                root, batch["batch_id"], "batch:replay:close"
            )
            event_path = capture.capture_root(root) / "events.jsonl"
            before = event_path.read_bytes()
            started_again = capture.start_batch(
                "2026-07-18", "batch:replay:start", repo_root=root
            )
            result_again = capture.mark_result(**result_args)
            closed_again = capture.close_batch(
                batch_id=batch["batch_id"],
                global_closeout="pass",
                global_audit_receipt_sha256=str(
                    receipt["global_audit_receipt_sha256"]
                ),
                idempotency_key="batch:replay:close",
                repo_root=root,
            )
            after = event_path.read_bytes()

        self.assertEqual(started_again["status"], "ALREADY_STARTED")
        self.assertEqual(result_again["status"], "ALREADY_RECORDED")
        self.assertEqual(closed_again["event_status"], "ALREADY_CLOSED")
        self.assertEqual(after, before)

    def test_same_result_or_close_with_new_key_is_semantic_noop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = capture.capture(
                self._write(root, self._payload(), "semantic.json"), repo_root=root
            )
            batch = capture.start_batch(
                "2026-07-18", "batch:semantic:start", repo_root=root
            )
            common = {
                "batch_id": batch["batch_id"],
                "capture_id": first["capture_id"],
                "outcome": "failed",
                "reason": "外部正式写入暂时失败",
                "repo_root": root,
            }
            capture.mark_result(
                **common, idempotency_key="batch:semantic:result:one"
            )
            path = capture.capture_root(root) / "events.jsonl"
            before_result_retry = path.read_bytes()
            repeated = capture.mark_result(
                **common, idempotency_key="batch:semantic:result:two"
            )
            after_result_retry = path.read_bytes()
            capture.close_batch(
                batch_id=batch["batch_id"],
                global_closeout="failed",
                idempotency_key="batch:semantic:close:one",
                repo_root=root,
            )
            before_close_retry = path.read_bytes()
            reclosed = capture.close_batch(
                batch_id=batch["batch_id"],
                global_closeout="failed",
                idempotency_key="batch:semantic:close:two",
                repo_root=root,
            )
            after_close_retry = path.read_bytes()

        self.assertEqual(repeated["status"], "ALREADY_RECORDED")
        self.assertEqual(before_result_retry, after_result_retry)
        self.assertEqual(reclosed["event_status"], "ALREADY_CLOSED")
        self.assertEqual(after_close_retry, before_close_retry)

    def test_only_one_active_batch_and_status_exposes_recovery_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = capture.capture(
                self._write(root, self._payload(), "active-one.json"), repo_root=root
            )
            batch = capture.start_batch(
                "2026-07-18", "batch:active:one", repo_root=root
            )
            day_two = self._payload(
                study_date="2026-07-19",
                idempotency_key="capture:active:day-two",
            )
            capture.capture(
                self._write(root, day_two, "active-two.json"), repo_root=root
            )
            with self.assertRaisesRegex(capture.CaptureError, "已有 active"):
                capture.start_batch(
                    "2026-07-19", "batch:active:two", repo_root=root
                )
            current = capture.status("2026-07-19", repo_root=root)
            self.assertEqual(current["active_batch"]["batch_id"], batch["batch_id"])
            self.assertEqual(current["active_batch"]["completed_count"], 0)
            self.assertIsNone(current["latest_date_batch"])
            capture.mark_result(
                batch_id=batch["batch_id"],
                capture_id=first["capture_id"],
                outcome="failed",
                reason="暂时失败",
                idempotency_key="batch:active:result",
                repo_root=root,
            )
            capture.close_batch(
                batch_id=batch["batch_id"],
                global_closeout="failed",
                idempotency_key="batch:active:close",
                repo_root=root,
            )
            second = capture.start_batch(
                "2026-07-19", "batch:active:two", repo_root=root
            )

        self.assertEqual(second["status"], "CURATION_BATCH_STARTED")

    def test_failed_same_set_retries_with_next_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = capture.capture(
                self._write(root, self._payload(), "retry.json"), repo_root=root
            )
            initial = capture.start_batch(
                "2026-07-18", "batch:attempt:one", repo_root=root
            )
            capture.mark_result(
                batch_id=initial["batch_id"],
                capture_id=first["capture_id"],
                outcome="failed",
                reason="正式写入失败，可稍后重试",
                idempotency_key="batch:attempt:result:one",
                repo_root=root,
            )
            capture.close_batch(
                batch_id=initial["batch_id"],
                global_closeout="failed",
                idempotency_key="batch:attempt:close:one",
                repo_root=root,
            )
            retry = capture.start_batch(
                "2026-07-18", "batch:attempt:two", repo_root=root
            )
            current = capture.status("2026-07-18", repo_root=root)

        self.assertEqual(initial["attempt"], 1)
        self.assertEqual(retry["attempt"], 2)
        self.assertEqual(initial["capture_set_sha256"], retry["capture_set_sha256"])
        self.assertNotEqual(initial["batch_id"], retry["batch_id"])
        self.assertEqual(
            current["captures"][first["capture_id"]]["current_batch_id"],
            retry["batch_id"],
        )

    def test_legacy_batch_without_attempt_replays_as_attempt_one(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            capture.capture(
                self._write(root, self._payload(), "legacy-batch.json"),
                repo_root=root,
            )
            started = capture.start_batch(
                "2026-07-18", "batch:legacy:start", repo_root=root
            )
            events = capture._load_events(capture.capture_root(root))
            event = events[-1]
            legacy_id = (
                f"CUR-20260718-{started['capture_set_sha256'][:12]}"
            )
            event["batch_schema"] = capture.LEGACY_BATCH_SCHEMA
            event.pop("attempt")
            event["batch_id"] = legacy_id
            state = capture.replay(events)

        self.assertEqual(state["batches"][legacy_id]["attempt"], 1)

    def test_post_threshold_capture_writes_release_neutral_binding_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            payload = self._payload(
                study_date="2026-08-17",
                idempotency_key="capture:producer-attestation",
            )
            with mock.patch.object(
                capture,
                "_now_utc",
                return_value="2026-08-17T07:00:00+00:00",
            ):
                receipt = capture.capture(
                    self._write(root, payload, "attested.json"),
                    repo_root=root,
                )
            self.assertEqual(receipt["producer_binding_status"], "attested")
            self.assertRegex(
                receipt["producer_binding_attestation_sha256"],
                r"^[0-9a-f]{64}$",
            )
            sidecar = Path(receipt["producer_binding_attestation_path"])
            self.assertTrue(sidecar.is_file())
            self.assertTrue(sidecar.is_relative_to(root.resolve()))
            value = json.loads(sidecar.read_text(encoding="utf-8"))
            self.assertEqual(value["capture_id"], receipt["capture_id"])
            self.assertEqual(
                value["capture_content_sha256"], receipt["payload_sha256"]
            )
            self.assertEqual(value["formal_write_count"], 0)
            self.assertFalse(
                {"release_id", "activation_id", "dispatcher_authority", "mcp_authority"}
                & set(value)
            )


if __name__ == "__main__":
    unittest.main()
