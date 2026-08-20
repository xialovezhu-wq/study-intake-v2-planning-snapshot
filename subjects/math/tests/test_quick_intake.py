from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.util
import io
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "数学一回滚复习系统" / "scripts" / "quick_intake.py"
SPEC = importlib.util.spec_from_file_location("quick_intake_test", SCRIPT_PATH)
assert SPEC and SPEC.loader
quick_intake = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(quick_intake)


class QuickIntakeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name)
        self.rollback = self.base / "数学一回滚复习系统"
        self.cards = self.base / "错题知识网络" / "错题卡"
        self.generated = self.base / "错题知识网络" / "生成"
        self.wiki = self.base / "错题知识网络" / "wiki"
        self.rollback.mkdir(parents=True)
        self.cards.mkdir(parents=True)
        self.generated.mkdir(parents=True)
        (self.wiki / "sources" / "wrong_cards").mkdir(parents=True)
        (self.wiki / "coverage").mkdir(parents=True)
        (self.wiki / "topics" / "knowledge_clusters").mkdir(parents=True)

        self.events_path = self.rollback / "快速入库事件.jsonl"
        self.lock_path = self.rollback / ".快速入库事件.jsonl.lock"
        self.source_staging_root = self.rollback / "快速入库来源"
        self.source_lock_path = self.rollback / ".快速入库来源.lock"
        self.review_log_path = self.rollback / "复习记录.jsonl"
        self.units_path = self.rollback / "复习单元.json"
        self.review_log_path.write_text("", encoding="utf-8")
        self.units_path.write_text("[]\n", encoding="utf-8")
        (self.generated / "marker.json").write_text('{"stable": true}\n', encoding="utf-8")

        patcher = mock.patch.multiple(
            quick_intake,
            ROOT=self.rollback,
            REPO_ROOT=self.base,
            EVENTS_PATH=self.events_path,
            LOCK_PATH=self.lock_path,
            SOURCE_STAGING_ROOT=self.source_staging_root,
            SOURCE_LOCK_PATH=self.source_lock_path,
            FORMAL_LOCK_PATH=self.rollback / ".正式层写入.lock",
            REVIEW_LOG_PATH=self.review_log_path,
            UNITS_PATH=self.units_path,
            CARDS_DIR=self.cards,
            WRONGNET_SNAPSHOT_PATH=self.generated / "wrong_questions.json",
            WIKI_ROOT=self.wiki,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        self.card_path = self.write_card("GS-629")

    def write_card(self, card_id: str) -> Path:
        path = self.cards / f"{card_id}_fixture.md"
        path.write_text(
            "\n".join(
                [
                    "---",
                    f"id: {card_id}",
                    f"title: {card_id} fixture",
                    "subject: 高等数学",
                    "status: 待复做",
                    "---",
                    "",
                    "fixture",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        return path

    @staticmethod
    def add_frontmatter_list_value(path: Path, field: str, value: str) -> None:
        text = path.read_text(encoding="utf-8")
        marker = text.find("\n---\n", 4)
        if marker < 0:
            raise AssertionError("fixture card has no frontmatter")
        addition = f"\n{field}:\n  - {json.dumps(value, ensure_ascii=False)}"
        path.write_text(text[:marker] + addition + text[marker:], encoding="utf-8")

    @staticmethod
    def score_event_id(seed: str = "1") -> str:
        return "SCORE-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]

    def append_score_event(
        self,
        *,
        attempt_id: str = "warmup:WQ-fixture:QI-fixture",
        card_id: str = "GS-629",
        score: int = 2,
        event_id: str | None = None,
    ) -> str:
        event_id = event_id or self.score_event_id(attempt_id)
        card_path = next(self.cards.glob(f"{card_id}_*.md"))
        event = {
            "event_id": event_id,
            "attempt_id": attempt_id,
            "date": "2026-07-18",
            "delivered_card_id": card_id,
            "delivered_card_source_version": hashlib.sha256(card_path.read_bytes()).hexdigest(),
            "score": score,
            "queue_id": "WQ-fixture",
            "queue_item_id": "QI-fixture",
            "match_mode": "knowledge_fallback",
            "anchor_card_id": "GS-645",
        }
        with self.review_log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, ensure_ascii=False) + "\n")
        return event_id

    def payload(
        self,
        *,
        attempt_id: str = "warmup:WQ-fixture:QI-fixture",
        score_event_id: str | None = None,
        formal_id: str = "GS-629",
        first_break: str = "没有先识别两平面交线",
        study_date: str = "2026-07-18",
    ) -> dict:
        return {
            "schema_version": quick_intake.CAPTURE_SCHEMA_V2,
            "attempt_id": attempt_id,
            "study_date": study_date,
            "target": {
                "kind": "formal_card",
                "formal_id": formal_id,
                "source_locator": None,
                "source_hash_before": None,
            },
            "score_event_id": score_event_id,
            "requested_action": "record_recurrence",
            "thread_ref": "fixture-thread",
            "source_bundle": None,
            "episode_evidence": {
                "solution_text": "fixture verified solution text",
                "user_answer_text": "fixture user reasoning",
                "teaching_turns": [],
            },
            "evidence": {
                "result": "wrong",
                "user_facts": [
                    {
                        "text": "本轮仍未识别第二条直线是两平面交线",
                        "origin": "user_observed",
                    }
                ],
                "independent_correct_steps": [],
                "first_break": {
                    "kind": "method_trigger",
                    "text": first_break,
                    "origin": "user_confirmed",
                },
                "later_breaks": [],
                "hints_needed": [
                    {
                        "text": "提示后才想到两个法向量叉乘",
                        "origin": "user_observed",
                    }
                ],
                "self_corrections": [],
                "mastery_score": 2,
                "mastery_source": "warmup_score" if score_event_id else "assistant_assessment",
                "score_basis": {
                    "text": "方法入口未独立触发",
                    "origin": "source_verified",
                },
                "unresolved": [],
            },
        }

    def write_json(self, name: str, value: dict) -> Path:
        path = self.base / name
        path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        return path

    def prepare_record_payload(self, payload: dict) -> dict:
        payload = json.loads(json.dumps(payload))
        if payload.get("source_bundle") is None:
            attempt_id = str(payload.get("attempt_id") or "missing-attempt")
            digest = hashlib.sha256(attempt_id.encode("utf-8")).hexdigest()[:12]
            question = self.base / f"contract-question-{digest}.png"
            question.write_bytes(b"\x89PNG\r\n\x1a\nquestion-" + digest.encode("ascii"))
            target = payload.get("target") or {}
            locator = (
                target.get("source_locator")
                if target.get("kind") == "new_source"
                else f"fixture:capture:{attempt_id}"
            )
            staged = self.stage_source(
                str(locator),
                [("question", question)],
                study_date=str(payload.get("study_date") or "2026-07-18"),
                solution_text=(payload.get("episode_evidence") or {}).get("solution_text"),
                name=f"stage-{digest}.json",
            )
            payload["source_bundle"] = {
                "manifest_path": staged["manifest_path"],
                "manifest_hash": staged["manifest_hash"],
            }
        return payload

    def invoke_record(
        self,
        payload: dict,
        *,
        name: str = "capture.json",
        bind_contract_artifacts: bool = True,
    ) -> dict:
        payload = (
            self.prepare_record_payload(payload)
            if bind_contract_artifacts
            else json.loads(json.dumps(payload))
        )
        path = self.write_json(name, payload)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            quick_intake.cmd_record(argparse.Namespace(payload_file=str(path)))
        return json.loads(output.getvalue())

    def invoke_stage(self, payload: dict, *, name: str = "stage.json") -> dict:
        path = self.write_json(name, payload)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            quick_intake.cmd_stage_source(argparse.Namespace(payload_file=str(path)))
        return json.loads(output.getvalue())

    def stage_source(
        self,
        locator: str,
        artifacts: list[tuple[str, Path]],
        *,
        study_date: str = "2026-07-18",
        name: str = "stage.json",
        solution_text: str | None = "fixture verified solution text",
    ) -> dict:
        staged_artifacts = list(artifacts)
        if solution_text is not None and not any(
            role == "solution_text" for role, _ in staged_artifacts
        ):
            digest = hashlib.sha256(locator.encode("utf-8")).hexdigest()[:12]
            solution_path = self.base / f"solution-text-{digest}.txt"
            solution_path.write_text(solution_text, encoding="utf-8", newline="")
            staged_artifacts.append(("solution_text", solution_path))
        return self.invoke_stage(
            {
                "schema_version": quick_intake.SOURCE_STAGE_SCHEMA,
                "study_date": study_date,
                "source_locator": locator,
                "artifacts": [
                    {"role": role, "path": str(path)}
                    for role, path in staged_artifacts
                ],
            },
            name=name,
        )

    def stage_complete_new_source(
        self, locator: str, question: Path, *, name: str = "stage.json"
    ) -> dict:
        solution = question.with_name(question.stem + "-solution" + question.suffix)
        solution.write_bytes(b"\x89PNG\r\n\x1a\nsolution-" + question.read_bytes())
        return self.stage_source(
            locator,
            [("question", question), ("solution", solution)],
            name=name,
        )

    def invoke_pending(self, day: str = "2026-07-18") -> dict:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            quick_intake.cmd_pending(argparse.Namespace(date=day))
        return json.loads(output.getvalue())

    def invoke_verify(self, day: str = "2026-07-18") -> dict:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            quick_intake.cmd_verify(argparse.Namespace(date=day))
        return json.loads(output.getvalue())

    def invoke_freeze(self, payload: dict) -> dict:
        path = self.write_json("freeze.json", payload)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            quick_intake.cmd_freeze(argparse.Namespace(payload_file=str(path)))
        return json.loads(output.getvalue())

    def freeze_existing(
        self,
        event_ids: list[str],
        formal_id: str = "GS-629",
        study_date: str = "2026-07-18",
    ) -> dict:
        return self.invoke_freeze(
            {
                "schema_version": quick_intake.FREEZE_SCHEMA,
                "study_date": study_date,
                "scope": "all_pending_for_date",
                "capture_event_ids": event_ids,
                "target_resolutions": [
                    {
                        "formal_id": formal_id,
                        "capture_event_ids": event_ids,
                        "identity_mode": "existing_formal",
                        "source_binding": None,
                    }
                ],
            }
        )

    def bind_frozen_source_refs(
        self,
        freeze: dict,
        formal_id: str = "GS-629",
    ) -> None:
        binding = next(
            item
            for item in freeze["formal_bindings"]
            if item["formal_id"] == formal_id
        )
        for source_ref in binding["fast_intake_source_refs"]:
            self.add_frontmatter_list_value(
                self.card_path,
                "fast_intake_source_refs",
                source_ref,
            )

    @staticmethod
    def artifact(path: Path, base: Path) -> dict[str, str]:
        return {
            "path": str(path.relative_to(base)),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }

    def build_wrongnet(
        self,
        formal_id: str = "GS-629",
        artifact_date: str = "2026-07-18",
    ) -> tuple[list[dict], str]:
        projection = quick_intake.current_formal_projections({formal_id})[formal_id]
        snapshot = {
            "updated_at": artifact_date,
            "cards": [projection],
            "similarities": [],
            "weak_relations": [],
        }
        (self.generated / "wrong_questions.json").write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        names = [
            "错题索引.md",
            "相似题清单.md",
            "强关联清单.md",
            "弱边审计清单.md",
            "专题链关联清单.md",
            "错题网络概览.md",
            "按知识点复做清单.md",
            "知识网络图.mmd",
        ]
        for name in names:
            (self.generated / name).write_text(f"fixture {formal_id}\n", encoding="utf-8")
        artifacts = [
            self.artifact(self.generated / "wrong_questions.json", self.base),
            *[self.artifact(self.generated / name, self.base) for name in names],
        ]
        return artifacts, quick_intake.sha256_value(projection)

    def build_wiki(
        self,
        projection_hash: str,
        formal_id: str = "GS-629",
        artifact_date: str = "2026-07-18",
    ) -> dict:
        card_path = next(self.cards.glob(f"{formal_id}_*.md"))
        projection = quick_intake.current_formal_projections({formal_id})[formal_id]
        projection_meta = projection["meta"]

        def yaml_list(field: str, values: object) -> list[str]:
            if not isinstance(values, list) or not values:
                return [f"{field}: []"]
            return [f"{field}:", *[f"  - {json.dumps(str(value), ensure_ascii=False)}" for value in values]]

        summary = self.wiki / "sources" / "wrong_cards" / f"SRC-WQ-{formal_id}.md"
        summary.write_text(
            "\n".join(
                [
                    "---",
                    f"wiki_id: SRC-WQ-{formal_id}",
                    f"subject: {json.dumps(str(projection_meta.get('subject') or ''), ensure_ascii=False)}",
                    f"last_updated: {artifact_date}",
                    f"formal_projection_sha256: {projection_hash}",
                    *yaml_list("knowledge", projection_meta.get("knowledge")),
                    *yaml_list("methods", projection_meta.get("methods")),
                    *yaml_list("error_causes", projection_meta.get("error_causes")),
                    "wrongnet_refs:",
                    f"  - \"{formal_id}\"",
                    "source_refs:",
                    f"  - \"{card_path.relative_to(self.base)}\"",
                    "wiki_refs:",
                    "  - \"MATHWIKI-KNOWLEDGE-001\"",
                    "---",
                    "",
                    formal_id,
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        cluster = self.wiki / "topics" / "knowledge_clusters" / "MATHWIKI-KNOWLEDGE-001_fixture.md"
        cluster.write_text(
            f"---\nwiki_id: MATHWIKI-KNOWLEDGE-001\n---\n\n{formal_id}\n",
            encoding="utf-8",
        )
        source_index = self.wiki / "sources" / "SRC-WRONGCARDS-INDEX_全量错题卡覆盖索引.md"
        matrix = self.wiki / "coverage" / "MATHWIKI-COVERAGE-MATRIX_错题卡多维编译矩阵.md"
        coverage = self.wiki / "coverage" / "MATHWIKI-COVERAGE-GS_fixture.md"
        for path in (source_index, matrix, coverage):
            path.write_text(f"| {formal_id} | fixture |\n", encoding="utf-8")
        return self.artifact(summary, self.base)

    def write_rollback_unit(
        self,
        card_hash: str,
        record_id: str | None = None,
        formal_id: str = "GS-629",
        study_date: str = "2026-07-18",
    ) -> str:
        record_id = record_id or f"{formal_id}|{study_date}|2"
        unit = {
            "复习单元ID": f"MATH1-GS-WQ-{formal_id.replace('-', '')}",
            "类型": "错题",
            "关联错题ID": formal_id,
            "定点同步状态": {
                "源版本": card_hash,
                "已处理错题事件ID": [record_id],
            },
            "调度事件历史": [
                {
                    "类型": "正式错题复发",
                    "事件ID": record_id,
                    "复发日期": study_date,
                    "同步日期": study_date,
                    "源版本": card_hash,
                }
            ],
        }
        self.units_path.write_text(
            json.dumps([unit], ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return quick_intake.sha256_value(unit)

    def closeout_payload(
        self,
        event_id: str,
        *,
        freeze: dict | None = None,
        study_date: str = "2026-07-18",
        artifact_date: str = "2026-07-18",
    ) -> dict:
        freeze = freeze or self.freeze_existing([event_id], study_date=study_date)
        freeze_event = next(
            event
            for event in quick_intake.load_jsonl(self.events_path)
            if event["event_id"] == freeze["freeze_id"]
        )
        snapshot = next(
            item
            for item in freeze_event["capture_snapshots"]
            if item["capture_event_id"] == event_id
        )
        self.add_frontmatter_list_value(
            self.card_path,
            "fast_intake_refs",
            quick_intake.capture_ref_token(snapshot),
        )
        self.bind_frozen_source_refs(freeze)
        self.card_path.write_text(
            self.card_path.read_text(encoding="utf-8") + "nightly fixture update\n",
            encoding="utf-8",
        )
        after_hash = hashlib.sha256(self.card_path.read_bytes()).hexdigest()
        record_id = f"GS-629|{study_date}|2"
        unit_hash = self.write_rollback_unit(
            after_hash,
            record_id=record_id,
            study_date=study_date,
        )
        wrongnet_artifacts, projection_hash = self.build_wrongnet(
            artifact_date=artifact_date
        )
        wiki_summary = self.build_wiki(
            projection_hash,
            artifact_date=artifact_date,
        )
        return {
            "schema_version": quick_intake.CLOSEOUT_SCHEMA,
            "freeze_id": freeze["freeze_id"],
            "model": "unknown",
            "formal_results": [
                {
                    "formal_id": "GS-629",
                    "operation": "updated",
                    "card_path_after": str(self.card_path.relative_to(self.base)),
                    "card_hash_after": after_hash,
                    "changed_fields": ["wrong_history", "method_gap"],
                }
            ],
            "capture_results": [
                {
                    "capture_event_id": event_id,
                    "outcome": "recurrence_recorded",
                    "formal_id": "GS-629",
                    "durable_record": {
                        "store": "rollback_unit",
                        "record_id": record_id,
                        "record_hash": unit_hash,
                    },
                }
            ],
            "batch_receipts": {
                "wrongnet": {
                    "rebuild_count": 1,
                    "artifacts": wrongnet_artifacts,
                    "target_projection_hashes": {"GS-629": projection_hash},
                },
                "relationships": {
                    "state": "proposal_only",
                    "proposals": [
                        {
                            "formal_id": "GS-629",
                            "decision": "无候选",
                            "evidence": "fixture target-only review",
                        }
                    ],
                },
                "wiki": [
                    {
                        "formal_id": "GS-629",
                        "summary_path": wiki_summary["path"],
                        "summary_hash": wiki_summary["sha256"],
                    }
                ],
                "visuals": [
                    {"formal_id": "GS-629", "state": "not_applicable", "artifacts": []}
                ],
            },
        }

    def refresh_receipt_for_current_card(
        self,
        receipt: dict,
        formal_id: str = "GS-629",
    ) -> None:
        card_path = next(self.cards.glob(f"{formal_id}_*.md"))
        after_hash = hashlib.sha256(card_path.read_bytes()).hexdigest()
        formal = next(
            item for item in receipt["formal_results"] if item["formal_id"] == formal_id
        )
        formal["card_hash_after"] = after_hash

        units = json.loads(self.units_path.read_text(encoding="utf-8"))
        unit = next(item for item in units if item.get("关联错题ID") == formal_id)
        unit["定点同步状态"]["源版本"] = after_hash
        self.units_path.write_text(
            json.dumps(units, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        unit_hash = quick_intake.sha256_value(unit)
        for capture_result in receipt["capture_results"]:
            if (
                capture_result["formal_id"] == formal_id
                and capture_result["durable_record"]["store"] == "rollback_unit"
            ):
                capture_result["durable_record"]["record_hash"] = unit_hash

        wrongnet_artifacts, projection_hash = self.build_wrongnet(formal_id)
        receipt["batch_receipts"]["wrongnet"]["artifacts"] = wrongnet_artifacts
        receipt["batch_receipts"]["wrongnet"]["target_projection_hashes"][formal_id] = (
            projection_hash
        )
        wiki_summary = self.build_wiki(projection_hash, formal_id)
        wiki_item = next(
            item for item in receipt["batch_receipts"]["wiki"] if item["formal_id"] == formal_id
        )
        wiki_item["summary_path"] = wiki_summary["path"]
        wiki_item["summary_hash"] = wiki_summary["sha256"]

    def invoke_close(self, payload: dict) -> dict:
        path = self.write_json("closeout.json", payload)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            quick_intake.cmd_close(argparse.Namespace(receipt_file=str(path)))
        return json.loads(output.getvalue())

    def invoke_amend(self, payload: dict) -> dict:
        path = self.write_json("amendment.json", payload)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            quick_intake.cmd_amend(argparse.Namespace(payload_file=str(path)))
        return json.loads(output.getvalue())

    def invoke_abort(self, freeze_id: str, reason: str = "用户确认撤销冻结计划") -> dict:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            quick_intake.cmd_abort_freeze(
                argparse.Namespace(freeze_id=freeze_id, reason=reason)
            )
        return json.loads(output.getvalue())

    def ledger_events(self) -> list[dict]:
        return quick_intake.load_jsonl(self.events_path)

    def test_01_warmup_capture_uses_delivered_card_and_small_score_reference(self) -> None:
        score_id = self.append_score_event()
        result = self.invoke_record(self.payload(score_event_id=score_id))
        events = self.ledger_events()

        self.assertEqual("recorded", result["status"])
        self.assertEqual("pending_nightly", result["state"])
        self.assertEqual(0, result["formal_write_count"])
        self.assertIs(type(result["formal_write_count"]), int)
        self.assertEqual("GS-629", result["formal_id"])
        self.assertEqual(score_id, result["score_event_id"])
        self.assertEqual(1, len(events))
        event = events[0]
        self.assertNotIn("formal_write_count", event)
        self.assertEqual("GS-629", event["target"]["formal_id"])
        self.assertEqual("GS-645", event["score_ref"]["anchor_card_id"])
        self.assertEqual("QI-fixture", event["score_ref"]["queue_item_id"])
        self.assertNotIn("delivered_evidence_snapshot", json.dumps(event, ensure_ascii=False))

    def test_02_score_target_conflict_fails_without_ledger_write(self) -> None:
        self.write_card("GS-630")
        score_id = self.append_score_event()
        payload = self.payload(score_event_id=score_id, formal_id="GS-630")

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "实际交付卡冲突"):
            self.invoke_record(payload)
        self.assertFalse(self.events_path.exists())

    def test_03_identical_capture_is_idempotent(self) -> None:
        score_id = self.append_score_event()
        payload = self.payload(score_event_id=score_id)

        first = self.invoke_record(payload)
        before = self.events_path.read_bytes()
        second = self.invoke_record(payload)

        self.assertEqual("recorded", first["status"])
        self.assertEqual("noop", second["status"])
        self.assertEqual(0, first["formal_write_count"])
        self.assertIs(type(first["formal_write_count"]), int)
        self.assertEqual(0, second["formal_write_count"])
        self.assertIs(type(second["formal_write_count"]), int)
        self.assertEqual(first["event_id"], second["event_id"])
        self.assertEqual(before, self.events_path.read_bytes())

    def test_04_same_identity_with_changed_evidence_fails_closed(self) -> None:
        score_id = self.append_score_event()
        self.invoke_record(self.payload(score_event_id=score_id))
        before = self.events_path.read_bytes()

        changed = self.payload(
            score_event_id=score_id,
            first_break="把方向向量与法向量混为一谈",
        )
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "拒绝覆盖"):
            self.invoke_record(changed, name="changed.json")
        self.assertEqual(before, self.events_path.read_bytes())

    def test_05_concurrent_duplicate_capture_writes_once(self) -> None:
        score_id = self.append_score_event()
        payload_path = self.write_json(
            "shared.json",
            self.prepare_record_payload(self.payload(score_event_id=score_id)),
        )
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                quick_intake.cmd_record(argparse.Namespace(payload_file=str(payload_path)))
            except BaseException as exc:  # pragma: no cover - reported below
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        with mock.patch("builtins.print"):
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=5)

        self.assertFalse(errors)
        self.assertEqual(1, len(self.ledger_events()))

    def test_06_malformed_ledger_fails_closed(self) -> None:
        self.events_path.write_text('{"broken":\n', encoding="utf-8")
        score_id = self.append_score_event()
        before = self.events_path.read_bytes()

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "不是有效 JSON"):
            self.invoke_record(self.payload(score_event_id=score_id))
        self.assertEqual(before, self.events_path.read_bytes())

    def test_07_pending_groups_multiple_captures_for_one_formal_card(self) -> None:
        first_score = self.append_score_event(attempt_id="warmup:WQ-fixture:QI-1")
        second_score = self.append_score_event(attempt_id="warmup:WQ-fixture:QI-2")
        self.invoke_record(
            self.payload(
                attempt_id="warmup:WQ-fixture:QI-1",
                score_event_id=first_score,
            ),
            name="first.json",
        )
        self.invoke_record(
            self.payload(
                attempt_id="warmup:WQ-fixture:QI-2",
                score_event_id=second_score,
            ),
            name="second.json",
        )

        pending = self.invoke_pending()
        self.assertEqual(2, pending["pending_count"])
        self.assertEqual(1, len(pending["target_groups"]))
        self.assertEqual("GS-629", pending["target_groups"][0]["target"])
        self.assertEqual(2, len(pending["target_groups"][0]["capture_event_ids"]))

    def test_08_closeout_requires_all_layers_and_is_idempotent(self) -> None:
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])
        failed = json.loads(json.dumps(receipt))
        failed["batch_receipts"]["wiki"][0]["summary_hash"] = "0" * 64

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "summary_hash"):
            self.invoke_close(failed)
        self.assertEqual(1, self.invoke_pending()["pending_count"])

        first = self.invoke_close(receipt)
        before = self.events_path.read_bytes()
        second = self.invoke_close(receipt)

        self.assertEqual("recorded", first["status"])
        self.assertEqual("noop", second["status"])
        self.assertEqual(first["closeout_id"], second["closeout_id"])
        self.assertEqual(before, self.events_path.read_bytes())
        status = self.invoke_pending()
        self.assertEqual(0, status["pending_count"])
        self.assertEqual(1, status["closed_count"])

    def test_09_record_does_not_mutate_formal_or_derived_layers(self) -> None:
        score_id = self.append_score_event()
        protected = {
            "card": self.card_path.read_bytes(),
            "units": self.units_path.read_bytes(),
            "review": self.review_log_path.read_bytes(),
            "generated": (self.generated / "marker.json").read_bytes(),
        }

        self.invoke_record(self.payload(score_event_id=score_id))

        self.assertEqual(protected["card"], self.card_path.read_bytes())
        self.assertEqual(protected["units"], self.units_path.read_bytes())
        self.assertEqual(protected["review"], self.review_log_path.read_bytes())
        self.assertEqual(protected["generated"], (self.generated / "marker.json").read_bytes())

    def test_10_closeout_rejects_card_hashes_not_grounded_in_capture_and_current_file(self) -> None:
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        original = self.card_path.read_bytes()
        self.card_path.write_bytes(original + b"drift\n")
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "capture 后已漂移"):
            self.freeze_existing([capture["event_id"]])
        self.card_path.write_bytes(original)
        receipt = self.closeout_payload(capture["event_id"])

        bad_after = json.loads(json.dumps(receipt))
        bad_after["formal_results"][0]["card_hash_after"] = "1" * 64
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "当前正式卡不一致"):
            self.invoke_close(bad_after)

        self.assertEqual(1, self.invoke_pending()["pending_count"])

    def test_11_success_can_consume_temp_payload_but_failure_keeps_it_for_retry(self) -> None:
        score_id = self.append_score_event()
        path = self.write_json(
            "consume.json",
            self.prepare_record_payload(self.payload(score_event_id=score_id)),
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            quick_intake.cmd_record(
                argparse.Namespace(payload_file=str(path), consume_payload_file=True)
            )
        result = json.loads(output.getvalue())

        self.assertTrue(result["payload_file_consumed"])
        self.assertFalse(path.exists())

        bad_path = self.write_json("bad-consume.json", {"schema_version": "broken"})
        with self.assertRaises(quick_intake.QuickIntakeError):
            quick_intake.cmd_record(
                argparse.Namespace(payload_file=str(bad_path), consume_payload_file=True)
            )
        self.assertTrue(bad_path.exists())

    def test_12_freeze_rejects_amendment_and_original_freeze_still_closes(self) -> None:
        score_id = self.append_score_event()
        original_payload = self.payload(score_event_id=score_id)
        capture = self.invoke_record(original_payload)
        freeze = self.freeze_existing([capture["event_id"]])
        amended_evidence = json.loads(json.dumps(original_payload["evidence"]))
        amended_evidence["first_break"]["text"] = "修订后的第一个方法触发断点"
        amendment = {
            "schema_version": quick_intake.AMENDMENT_SCHEMA,
            "capture_event_id": capture["event_id"],
            "reason": {"text": "补正第一断点", "origin": "user_confirmed"},
            "evidence": amended_evidence,
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "active freeze"):
            self.invoke_amend(amendment)
        receipt = self.closeout_payload(capture["event_id"], freeze=freeze)
        result = self.invoke_close(receipt)

        self.assertEqual("recorded", result["status"])
        self.assertEqual(0, self.invoke_pending()["pending_count"])
        units = json.loads(self.units_path.read_text(encoding="utf-8"))
        self.assertEqual(1, len(units[0]["调度事件历史"]))

    def test_13_new_source_freeze_requires_verified_source_artifact(self) -> None:
        source = self.base / "source.png"
        source.write_bytes(b"\x89PNG\r\n\x1a\nreal-source-bytes")
        staged = self.stage_complete_new_source(
            "attachment:fixture-source-1", source
        )
        source_hash = staged["manifest_hash"]
        payload = self.payload(
            attempt_id="source:fixture:1",
            score_event_id=None,
        )
        payload["target"] = {
            "kind": "new_source",
            "formal_id": None,
            "source_locator": "attachment:fixture-source-1",
            "source_hash_before": None,
        }
        payload["requested_action"] = "record_wrong"
        payload["source_bundle"] = {
            "manifest_path": staged["manifest_path"],
            "manifest_hash": staged["manifest_hash"],
        }
        capture = self.invoke_record(payload)
        freeze_payload = {
            "schema_version": quick_intake.FREEZE_SCHEMA,
            "study_date": "2026-07-18",
            "scope": "all_pending_for_date",
            "capture_event_ids": [capture["event_id"]],
            "target_resolutions": [
                {
                    "formal_id": "GS-700",
                    "capture_event_ids": [capture["event_id"]],
                    "identity_mode": "new_source_created",
                    "source_binding": {
                        "source_locator": "attachment:fixture-source-1",
                        "resolved_source_hash": source_hash,
                        "artifact_path": staged["manifest_path"],
                        "artifact_hash": "0" * 64,
                        "basis": "题图来源文件已验证，正式库查重未命中",
                    },
                }
            ],
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "来源文件哈希不一致"):
            self.invoke_freeze(freeze_payload)

        freeze_payload["target_resolutions"][0]["source_binding"]["artifact_hash"] = source_hash
        frozen = self.invoke_freeze(freeze_payload)
        self.assertEqual("recorded", frozen["status"])
        freeze_event = self.ledger_events()[-1]
        self.assertIsNone(freeze_event["targets"][0]["card_hash_before"])
        self.assertEqual(source_hash, freeze_event["targets"][0]["source_binding"]["resolved_source_hash"])

    def test_14_mastery_rejected_closes_without_fabricated_wrong_event(self) -> None:
        payload = self.payload(attempt_id="mastery:GS-629:2026-07-18", score_event_id=None)
        payload["requested_action"] = "mastery_candidate"
        payload["evidence"]["result"] = "correct"
        payload["evidence"]["mastery_score"] = 4
        payload["evidence"]["mastery_source"] = "assistant_assessment"
        capture = self.invoke_record(payload)
        pending_item = self.invoke_pending()["pending"][0]
        freeze = self.freeze_existing([capture["event_id"]])
        freeze_event = next(
            event
            for event in self.ledger_events()
            if event["event_id"] == freeze["freeze_id"]
        )
        snapshot = next(
            item
            for item in freeze_event["capture_snapshots"]
            if item["capture_event_id"] == capture["event_id"]
        )
        self.add_frontmatter_list_value(
            self.card_path,
            "fast_intake_refs",
            quick_intake.capture_ref_token(snapshot),
        )
        self.bind_frozen_source_refs(freeze)
        card_hash = hashlib.sha256(self.card_path.read_bytes()).hexdigest()
        wrongnet_artifacts, projection_hash = self.build_wrongnet()
        wiki_summary = self.build_wiki(projection_hash)
        receipt = {
            "schema_version": quick_intake.CLOSEOUT_SCHEMA,
            "freeze_id": freeze["freeze_id"],
            "model": "unknown",
            "formal_results": [
                {
                    "formal_id": "GS-629",
                    "operation": "updated",
                    "card_path_after": str(self.card_path.relative_to(self.base)),
                    "card_hash_after": card_hash,
                    "changed_fields": ["fast_intake_refs", "fast_intake_source_refs"],
                }
            ],
            "capture_results": [
                {
                    "capture_event_id": capture["event_id"],
                    "outcome": "mastery_rejected",
                    "formal_id": "GS-629",
                    "durable_record": {
                        "store": "closeout",
                        "record_id": capture["event_id"],
                        "record_hash": pending_item["effective_evidence_hash"],
                    },
                }
            ],
            "batch_receipts": {
                "wrongnet": {
                    "rebuild_count": 1,
                    "artifacts": wrongnet_artifacts,
                    "target_projection_hashes": {"GS-629": projection_hash},
                },
                "relationships": {
                    "state": "proposal_only",
                    "proposals": [
                        {
                            "formal_id": "GS-629",
                            "decision": "无候选",
                            "evidence": "fixture target-only review",
                        }
                    ],
                },
                "wiki": [
                    {
                        "formal_id": "GS-629",
                        "summary_path": wiki_summary["path"],
                        "summary_hash": wiki_summary["sha256"],
                    }
                ],
                "visuals": [
                    {"formal_id": "GS-629", "state": "not_applicable", "artifacts": []}
                ],
            },
        }

        result = self.invoke_close(receipt)
        self.assertEqual("recorded", result["status"])
        close_event = self.ledger_events()[-1]
        self.assertEqual("mastery_rejected", close_event["capture_results"][0]["outcome"])
        self.assertNotIn("wrong_event_id", close_event)

    def test_15_identical_closeout_is_noop_after_later_card_change(self) -> None:
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])
        first = self.invoke_close(receipt)
        self.card_path.write_text(
            self.card_path.read_text(encoding="utf-8") + "later unrelated edit\n",
            encoding="utf-8",
        )
        before_ledger = self.events_path.read_bytes()
        second = self.invoke_close(receipt)

        self.assertEqual("recorded", first["status"])
        self.assertEqual("noop", second["status"])
        self.assertEqual(before_ledger, self.events_path.read_bytes())

    def test_16_verified_new_source_can_create_and_close_new_formal_card(self) -> None:
        source = self.base / "new-source.png"
        source.write_bytes(b"\x89PNG\r\n\x1a\nverified-new-source")
        staged = self.stage_complete_new_source(
            "attachment:new-source-created", source
        )
        source_hash = staged["manifest_hash"]
        payload = self.payload(attempt_id="source:fixture:created", score_event_id=None)
        payload["target"] = {
            "kind": "new_source",
            "formal_id": None,
            "source_locator": "attachment:new-source-created",
            "source_hash_before": None,
        }
        payload["requested_action"] = "record_wrong"
        payload["source_bundle"] = {
            "manifest_path": staged["manifest_path"],
            "manifest_hash": staged["manifest_hash"],
        }
        capture = self.invoke_record(payload)
        freeze = self.invoke_freeze(
            {
                "schema_version": quick_intake.FREEZE_SCHEMA,
                "study_date": "2026-07-18",
                "scope": "all_pending_for_date",
                "capture_event_ids": [capture["event_id"]],
                "target_resolutions": [
                    {
                        "formal_id": "GS-700",
                        "capture_event_ids": [capture["event_id"]],
                        "identity_mode": "new_source_created",
                        "source_binding": {
                            "source_locator": "attachment:new-source-created",
                            "resolved_source_hash": source_hash,
                            "artifact_path": staged["manifest_path"],
                            "artifact_hash": source_hash,
                            "basis": "真实题图已验证且正式库查重未命中",
                        },
                    }
                ],
            }
        )
        freeze_event = next(
            event
            for event in quick_intake.load_jsonl(self.events_path)
            if event["event_id"] == freeze["freeze_id"]
        )
        frozen_snapshot = freeze_event["capture_snapshots"][0]
        frozen_binding = freeze_event["targets"][0]["source_binding"]
        new_card = self.cards / "GS-700_created.md"
        new_card.write_text(
            "\n".join(
                [
                    "---",
                    "id: GS-700",
                    "title: created fixture",
                    "status: 待复做",
                    "fast_intake_refs:",
                    f"  - {json.dumps(quick_intake.capture_ref_token(frozen_snapshot), ensure_ascii=False)}",
                    "fast_intake_source_refs:",
                    f"  - {json.dumps(quick_intake.source_ref_token(frozen_binding), ensure_ascii=False)}",
                    "---",
                    "",
                    "fixture source-backed card",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        after_hash = hashlib.sha256(new_card.read_bytes()).hexdigest()
        record_id = "GS-700|2026-07-18|1"
        unit_hash = self.write_rollback_unit(after_hash, record_id=record_id, formal_id="GS-700")
        wrongnet_artifacts, projection_hash = self.build_wrongnet("GS-700")
        wiki_summary = self.build_wiki(projection_hash, "GS-700")
        receipt = {
            "schema_version": quick_intake.CLOSEOUT_SCHEMA,
            "freeze_id": freeze["freeze_id"],
            "model": "unknown",
            "formal_results": [
                {
                    "formal_id": "GS-700",
                    "operation": "created",
                    "card_path_after": str(new_card.relative_to(self.base)),
                    "card_hash_after": after_hash,
                    "changed_fields": ["identity", "wrong_history", "method_gap"],
                }
            ],
            "capture_results": [
                {
                    "capture_event_id": capture["event_id"],
                    "outcome": "wrong_recorded",
                    "formal_id": "GS-700",
                    "durable_record": {
                        "store": "rollback_unit",
                        "record_id": record_id,
                        "record_hash": unit_hash,
                    },
                }
            ],
            "batch_receipts": {
                "wrongnet": {
                    "rebuild_count": 1,
                    "artifacts": wrongnet_artifacts,
                    "target_projection_hashes": {"GS-700": projection_hash},
                },
                "relationships": {
                    "state": "proposal_only",
                    "proposals": [
                        {
                            "formal_id": "GS-700",
                            "decision": "无候选",
                            "evidence": "fixture formal lookup found no duplicate",
                        }
                    ],
                },
                "wiki": [
                    {
                        "formal_id": "GS-700",
                        "summary_path": wiki_summary["path"],
                        "summary_hash": wiki_summary["sha256"],
                    }
                ],
                "visuals": [
                    {"formal_id": "GS-700", "state": "not_applicable", "artifacts": []}
                ],
            },
        }

        result = self.invoke_close(receipt)
        self.assertEqual("recorded", result["status"])
        self.assertEqual([capture["event_id"]], result["closed_event_ids"])

    def test_17_all_pending_freeze_allows_later_unrelated_capture(self) -> None:
        first_score = self.append_score_event(attempt_id="warmup:WQ-fixture:QI-first")
        first = self.invoke_record(
            self.payload(
                attempt_id="warmup:WQ-fixture:QI-first",
                score_event_id=first_score,
            )
        )
        freeze = self.freeze_existing([first["event_id"]])
        self.write_card("GS-630")
        second_score = self.append_score_event(
            attempt_id="warmup:WQ-fixture:QI-second",
            card_id="GS-630",
        )
        second = self.invoke_record(
            self.payload(
                attempt_id="warmup:WQ-fixture:QI-second",
                score_event_id=second_score,
                formal_id="GS-630",
            ),
            name="second-after-freeze.json",
        )
        receipt = self.closeout_payload(first["event_id"], freeze=freeze)
        result = self.invoke_close(receipt)

        self.assertEqual("recorded", result["status"])
        pending = self.invoke_pending()
        self.assertEqual(1, pending["pending_count"])
        self.assertEqual(second["event_id"], pending["pending"][0]["event_id"])

    def test_18_historical_pr_id_uses_its_actual_subject_coverage_row(self) -> None:
        coverage_gs = self.wiki / "coverage" / "MATHWIKI-COVERAGE-GS_fixture.md"
        coverage_la = self.wiki / "coverage" / "MATHWIKI-COVERAGE-LA_fixture.md"
        coverage_gs.write_text("| PR-001 | fixture |\n", encoding="utf-8")
        coverage_la.write_text("| LA-001 | fixture |\n", encoding="utf-8")

        self.assertEqual(coverage_gs, quick_intake.wiki_subject_coverage("PR-001"))

    def test_19_visual_receipt_parses_markdown_code_span_without_backtick(self) -> None:
        visual = self.base / "错题知识网络" / "可视化错题详情" / "高等数学" / "GS-629_fixture.md"
        visual.parent.mkdir(parents=True)
        visual.write_text("fixture visual\n", encoding="utf-8")
        visual_ref = str(visual.relative_to(self.base))
        self.card_path.write_text(
            self.card_path.read_text(encoding="utf-8") + f"- 视觉详情：`{visual_ref}`\n",
            encoding="utf-8",
        )

        result = quick_intake.verify_visual_receipts(
            [
                {
                    "formal_id": "GS-629",
                    "state": "passed",
                    "artifacts": [self.artifact(visual, self.base)],
                }
            ],
            {"GS-629": {}},
            {},
        )

        self.assertEqual(visual_ref, result[0]["artifacts"][0]["path"])

    def test_20_closeout_rejects_untrusted_self_reported_model(self) -> None:
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])
        receipt["model"] = "self-reported-premium-model"

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "受信模型"):
            self.invoke_close(receipt)

    def test_21_closeout_rejects_stale_wrongnet_projection_even_if_hashes_agree(self) -> None:
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])
        snapshot_path = self.generated / "wrong_questions.json"
        snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
        snapshot["cards"][0]["meta"]["title"] = "stale fabricated title"
        snapshot_path.write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        projection_hash = quick_intake.sha256_value(snapshot["cards"][0])
        receipt["batch_receipts"]["wrongnet"]["target_projection_hashes"]["GS-629"] = projection_hash
        for artifact in receipt["batch_receipts"]["wrongnet"]["artifacts"]:
            if artifact["path"].endswith("wrong_questions.json"):
                artifact["sha256"] = hashlib.sha256(snapshot_path.read_bytes()).hexdigest()
        wiki_item = receipt["batch_receipts"]["wiki"][0]
        summary = self.base / wiki_item["summary_path"]
        summary.write_text(
            summary.read_text(encoding="utf-8").replace(
                quick_intake.frontmatter_scalar(
                    quick_intake.markdown_frontmatter(summary.read_text(encoding="utf-8")),
                    "formal_projection_sha256",
                ),
                projection_hash,
            ),
            encoding="utf-8",
        )
        wiki_item["summary_hash"] = hashlib.sha256(summary.read_bytes()).hexdigest()

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "未反映当前正式卡"):
            self.invoke_close(receipt)

    def test_22_recurrence_cannot_reuse_event_that_existed_before_freeze(self) -> None:
        old_record_id = "GS-629|2026-07-18|1"
        original_hash = hashlib.sha256(self.card_path.read_bytes()).hexdigest()
        self.write_rollback_unit(original_hash, record_id=old_record_id)
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])
        final_hash = receipt["formal_results"][0]["card_hash_after"]
        unit_hash = self.write_rollback_unit(final_hash, record_id=old_record_id)
        durable = receipt["capture_results"][0]["durable_record"]
        durable["record_id"] = old_record_id
        durable["record_hash"] = unit_hash

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "processed events"):
            self.invoke_close(receipt)

    def test_23_formal_result_requires_matching_frontmatter_id(self) -> None:
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])
        self.card_path.write_text(
            self.card_path.read_text(encoding="utf-8").replace("id: GS-629", "id: GS-999", 1),
            encoding="utf-8",
        )
        receipt["formal_results"][0]["card_hash_after"] = hashlib.sha256(
            self.card_path.read_bytes()
        ).hexdigest()

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "frontmatter id"):
            self.invoke_close(receipt)

    def test_24_post_prepare_artifact_race_invalidates_and_can_retry(self) -> None:
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])
        expected_card = self.card_path.read_bytes()
        original_reverify = quick_intake.reverify_artifacts
        call_count = 0

        def race_after_append(artifacts: list[dict[str, str]]) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                self.card_path.write_text(
                    self.card_path.read_text(encoding="utf-8") + "concurrent change\n",
                    encoding="utf-8",
                )
            original_reverify(artifacts)

        with mock.patch.object(quick_intake, "reverify_artifacts", side_effect=race_after_append):
            with self.assertRaisesRegex(quick_intake.QuickIntakeError, "恢复 pending"):
                self.invoke_close(receipt)

        events = quick_intake.load_jsonl(self.events_path)
        self.assertEqual("closeout_invalidation", events[-1]["event_type"])
        self.assertEqual(1, self.invoke_pending()["pending_count"])
        invalidated_state = quick_intake.replay(events)
        self.assertNotIn(receipt["freeze_id"], invalidated_state["used_freezes"])

        self.card_path.write_bytes(expected_card)
        retry = self.invoke_close(receipt)
        final_events = quick_intake.load_jsonl(self.events_path)
        prepare_events = [
            event for event in final_events if event["event_type"] == "closeout_prepare"
        ]

        self.assertEqual("recorded", retry["status"])
        self.assertEqual(0, self.invoke_pending()["pending_count"])
        self.assertEqual([0, 1], [event["generation"] for event in prepare_events])
        self.assertEqual(2, len({event["event_id"] for event in prepare_events}))
        self.assertEqual(len(final_events), len({event["event_id"] for event in final_events}))

    def test_25_prepare_interruption_never_closes_and_retry_reuses_prepare(self) -> None:
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])
        original_reverify = quick_intake.reverify_artifacts
        call_count = 0

        def interrupt_after_prepare(artifacts: list[dict[str, str]]) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("simulated process interruption")
            original_reverify(artifacts)

        with mock.patch.object(
            quick_intake,
            "reverify_artifacts",
            side_effect=interrupt_after_prepare,
        ):
            with self.assertRaisesRegex(RuntimeError, "simulated process interruption"):
                self.invoke_close(receipt)

        interrupted_events = quick_intake.load_jsonl(self.events_path)
        self.assertEqual("closeout_prepare", interrupted_events[-1]["event_type"])
        self.assertEqual(1, self.invoke_pending()["pending_count"])
        self.assertFalse(
            any(event["event_type"] == "closeout" for event in interrupted_events)
        )

        retry = self.invoke_close(receipt)
        final_events = quick_intake.load_jsonl(self.events_path)
        self.assertEqual("recorded", retry["status"])
        self.assertEqual(0, self.invoke_pending()["pending_count"])
        self.assertEqual(
            1,
            sum(event["event_type"] == "closeout_prepare" for event in final_events),
        )
        self.assertEqual(
            1,
            sum(event["event_type"] == "closeout" for event in final_events),
        )

    def test_26_closeout_rejects_deleting_frozen_fast_intake_ref(self) -> None:
        old_ref = "capture-v1:MFI-CAP-000000000000000000000000:" + "a" * 64 + ":record_recurrence"
        self.add_frontmatter_list_value(self.card_path, "fast_intake_refs", old_ref)
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])
        old_line = f"  - {json.dumps(old_ref, ensure_ascii=False)}\n"
        self.card_path.write_text(
            self.card_path.read_text(encoding="utf-8").replace(old_line, "", 1),
            encoding="utf-8",
        )
        self.refresh_receipt_for_current_card(receipt)

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "fast_intake_refs"):
            self.invoke_close(receipt)

    def test_27_closeout_rejects_deleting_frozen_source_ref(self) -> None:
        old_ref = "source-v1:" + "b" * 64
        self.add_frontmatter_list_value(self.card_path, "fast_intake_source_refs", old_ref)
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])
        old_line = f"  - {json.dumps(old_ref, ensure_ascii=False)}\n"
        self.card_path.write_text(
            self.card_path.read_text(encoding="utf-8").replace(old_line, "", 1),
            encoding="utf-8",
        )
        self.refresh_receipt_for_current_card(receipt)

        with self.assertRaisesRegex(
            quick_intake.QuickIntakeError,
            "fast_intake_source_refs",
        ):
            self.invoke_close(receipt)

    def test_28_closeout_rejects_deleting_frozen_processed_event(self) -> None:
        old_record_id = "GS-629|2026-07-01|1"
        original_hash = hashlib.sha256(self.card_path.read_bytes()).hexdigest()
        self.write_rollback_unit(original_hash, record_id=old_record_id)
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "processed events"):
            self.invoke_close(receipt)

    def test_29_closeout_rejects_deleting_frozen_history_event(self) -> None:
        old_record_id = "GS-629|2026-07-01|1"
        new_record_id = "GS-629|2026-07-18|2"
        original_hash = hashlib.sha256(self.card_path.read_bytes()).hexdigest()
        self.write_rollback_unit(original_hash, record_id=old_record_id)
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        receipt = self.closeout_payload(capture["event_id"])

        units = json.loads(self.units_path.read_text(encoding="utf-8"))
        unit = units[0]
        unit["定点同步状态"]["已处理错题事件ID"] = [old_record_id, new_record_id]
        self.units_path.write_text(
            json.dumps(units, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        receipt["capture_results"][0]["durable_record"]["record_hash"] = (
            quick_intake.sha256_value(unit)
        )

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "历史删除"):
            self.invoke_close(receipt)

    def test_30_cross_day_closeout_keeps_study_and_artifact_dates_separate(self) -> None:
        study_date = "2026-07-17"
        artifact_date = "2026-07-18"
        payload = self.payload(
            attempt_id="historical:GS-629:2026-07-17",
            score_event_id=None,
            study_date=study_date,
        )
        capture = self.invoke_record(payload)
        freeze = self.freeze_existing(
            [capture["event_id"]],
            study_date=study_date,
        )
        receipt = self.closeout_payload(
            capture["event_id"],
            freeze=freeze,
            study_date=study_date,
            artifact_date=artifact_date,
        )

        result = self.invoke_close(receipt)

        self.assertEqual("recorded", result["status"])
        self.assertEqual(study_date, result["study_date"])
        self.assertEqual(artifact_date, result["artifact_date"])
        closeout = next(
            event
            for event in self.ledger_events()
            if event["event_type"] == "closeout"
        )
        self.assertEqual(study_date, closeout["study_date"])
        self.assertEqual(artifact_date, closeout["artifact_date"])
        unit = json.loads(self.units_path.read_text(encoding="utf-8"))[0]
        self.assertEqual(
            f"GS-629|{study_date}|2",
            unit["调度事件历史"][0]["事件ID"],
        )
        self.assertEqual(study_date, unit["调度事件历史"][0]["复发日期"])

    def test_31_artifact_date_cannot_precede_study_date(self) -> None:
        study_date = "2026-07-18"
        artifact_date = "2026-07-17"
        payload = self.payload(
            attempt_id="invalid-date:GS-629:2026-07-18",
            score_event_id=None,
            study_date=study_date,
        )
        capture = self.invoke_record(payload)
        receipt = self.closeout_payload(
            capture["event_id"],
            study_date=study_date,
            artifact_date=artifact_date,
        )

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "不得早于"):
            self.invoke_close(receipt)
        self.assertEqual(1, self.invoke_pending()["pending_count"])

    def test_32_pending_exposes_active_freeze_and_second_freeze_is_rejected(self) -> None:
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        freeze = self.freeze_existing([capture["event_id"]])

        pending = self.invoke_pending()
        self.assertEqual([freeze["freeze_id"]], pending["pending"][0]["active_freeze_ids"])
        self.assertEqual(freeze["freeze_id"], pending["active_freezes"][0]["freeze_id"])

        duplicate = self.freeze_existing([capture["event_id"]])
        self.assertEqual("noop", duplicate["status"])
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "active freeze"):
            self.invoke_freeze(
                {
                    "schema_version": quick_intake.FREEZE_SCHEMA,
                    "study_date": "2026-07-18",
                    "scope": "explicit_subset",
                    "capture_event_ids": [capture["event_id"]],
                    "target_resolutions": [
                        {
                            "formal_id": "GS-629",
                            "capture_event_ids": [capture["event_id"]],
                            "identity_mode": "existing_formal",
                            "source_binding": None,
                        }
                    ],
                }
            )

    def test_33_safe_abort_allows_amendment_and_new_freeze(self) -> None:
        score_id = self.append_score_event()
        original_payload = self.payload(score_event_id=score_id)
        capture = self.invoke_record(original_payload)
        freeze = self.freeze_existing([capture["event_id"]])

        aborted = self.invoke_abort(freeze["freeze_id"])
        self.assertEqual("freeze_aborted", aborted["state"])
        self.assertEqual([], self.invoke_pending()["active_freezes"])

        amended_evidence = json.loads(json.dumps(original_payload["evidence"]))
        amended_evidence["first_break"]["text"] = "用户确认后的新断点"
        self.invoke_amend(
            {
                "schema_version": quick_intake.AMENDMENT_SCHEMA,
                "capture_event_id": capture["event_id"],
                "reason": {"text": "核心事实已由用户更正", "origin": "user_confirmed"},
                "evidence": amended_evidence,
            }
        )
        new_freeze = self.freeze_existing([capture["event_id"]])
        self.assertNotEqual(freeze["freeze_id"], new_freeze["freeze_id"])

    def test_34_abort_rejects_preapplied_formal_or_rollback_work(self) -> None:
        score_id = self.append_score_event()
        capture = self.invoke_record(self.payload(score_event_id=score_id))
        freeze = self.freeze_existing([capture["event_id"]])
        receipt = self.closeout_payload(capture["event_id"], freeze=freeze)

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "不能自动 abort"):
            self.invoke_abort(freeze["freeze_id"])
        result = self.invoke_close(receipt)
        self.assertEqual("recorded", result["status"])

    def test_35_same_target_later_capture_can_rebase_without_stranding(self) -> None:
        first_score = self.append_score_event(attempt_id="warmup:WQ-fixture:QI-a")
        first = self.invoke_record(
            self.payload(
                attempt_id="warmup:WQ-fixture:QI-a",
                score_event_id=first_score,
            ),
            name="first-same-target.json",
        )
        freeze = self.freeze_existing([first["event_id"]])
        second_score = self.append_score_event(attempt_id="warmup:WQ-fixture:QI-b")
        second_payload = self.payload(
            attempt_id="warmup:WQ-fixture:QI-b",
            score_event_id=second_score,
            first_break="第二次作答的独立断点",
        )
        second = self.invoke_record(second_payload, name="second-same-target.json")
        receipt = self.closeout_payload(first["event_id"], freeze=freeze)

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "来源版本过期"):
            self.invoke_close(receipt)

        current_hash = hashlib.sha256(self.card_path.read_bytes()).hexdigest()
        self.invoke_amend(
            {
                "schema_version": quick_intake.AMENDMENT_SCHEMA,
                "capture_event_id": second["event_id"],
                "reason": {
                    "text": "同卡前序 freeze 已更新正式表示，重绑当前版本",
                    "origin": "source_verified",
                },
                "evidence": second_payload["evidence"],
                "target_patch": {"source_hash_before": current_hash},
            }
        )
        closed = self.invoke_close(receipt)
        self.assertEqual("recorded", closed["status"])
        next_freeze = self.freeze_existing([second["event_id"]])
        self.assertEqual([second["event_id"]], next_freeze["capture_event_ids"])

    def test_36_formal_rebase_requires_source_verified_origin(self) -> None:
        score_id = self.append_score_event()
        payload = self.payload(score_event_id=score_id)
        capture = self.invoke_record(payload)
        current_hash = hashlib.sha256(self.card_path.read_bytes()).hexdigest()

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "source_verified"):
            self.invoke_amend(
                {
                    "schema_version": quick_intake.AMENDMENT_SCHEMA,
                    "capture_event_id": capture["event_id"],
                    "reason": {"text": "未核验的版本重绑", "origin": "assistant_inferred"},
                    "evidence": payload["evidence"],
                    "target_patch": {"source_hash_before": current_hash},
                }
            )

    def test_37_formal_rebase_cannot_modify_evidence(self) -> None:
        score_id = self.append_score_event()
        payload = self.payload(score_event_id=score_id)
        capture = self.invoke_record(payload)
        current_hash = hashlib.sha256(self.card_path.read_bytes()).hexdigest()
        changed_evidence = json.loads(json.dumps(payload["evidence"]))
        changed_evidence["first_break"]["text"] = "伪装成版本重绑的事实修订"

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "不得同时修改 evidence"):
            self.invoke_amend(
                {
                    "schema_version": quick_intake.AMENDMENT_SCHEMA,
                    "capture_event_id": capture["event_id"],
                    "reason": {"text": "重绑当前正式卡版本", "origin": "source_verified"},
                    "evidence": changed_evidence,
                    "target_patch": {"source_hash_before": current_hash},
                }
            )

    def test_38_stage_source_is_idempotent_and_survives_temp_cleanup(self) -> None:
        question_a = self.base / "question-a.png"
        solution_a = self.base / "solution-a.png"
        question_b = self.base / "question-b.png"
        solution_b = self.base / "solution-b.png"
        question_bytes = b"\x89PNG\r\n\x1a\nquestion-fixture"
        solution_bytes = b"\x89PNG\r\n\x1a\nsolution-fixture"
        question_a.write_bytes(question_bytes)
        solution_a.write_bytes(solution_bytes)
        question_b.write_bytes(question_bytes)
        solution_b.write_bytes(solution_bytes)

        first = self.stage_source(
            "attachment:durable-fixture",
            [("question", question_a), ("solution", solution_a)],
            name="stage-first.json",
        )
        second = self.stage_source(
            "attachment:durable-fixture",
            [("solution", solution_b), ("question", question_b)],
            name="stage-second.json",
        )
        self.assertEqual("recorded", first["status"])
        self.assertEqual("noop", second["status"])
        self.assertEqual(first["manifest_hash"], second["manifest_hash"])

        for path in (question_a, solution_a, question_b, solution_b):
            path.unlink()
        document, manifest, children = quick_intake.validate_source_bundle_manifest(
            first["manifest_path"],
            "fixture.manifest",
            expected_hash=first["manifest_hash"],
            require_question=True,
        )
        self.assertEqual(3, len(document["artifacts"]))
        self.assertTrue(manifest.is_file())
        self.assertTrue(all(path.is_file() for path in children))

    def test_39_stage_source_conflict_fails_closed(self) -> None:
        source = self.base / "conflict.png"
        source.write_bytes(b"\x89PNG\r\n\x1a\nfirst")
        self.stage_source("attachment:conflict", [("question", source)])
        source.write_bytes(b"\x89PNG\r\n\x1a\nsecond")

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "不同文件"):
            self.stage_source(
                "attachment:conflict",
                [("question", source)],
                name="stage-conflict.json",
            )

    def test_40_stage_source_rejects_bad_magic_and_symlink(self) -> None:
        bad = self.base / "bad.png"
        bad.write_bytes(b"not-a-png")
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "扩展名不一致"):
            self.stage_source("attachment:bad", [("question", bad)])

        real = self.base / "real.png"
        link = self.base / "link.png"
        real.write_bytes(b"\x89PNG\r\n\x1a\nreal")
        link.symlink_to(real)
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "符号链接"):
            self.stage_source(
                "attachment:symlink",
                [("question", link)],
                name="stage-symlink.json",
            )

    def test_41_new_source_record_requires_staged_bundle(self) -> None:
        payload = self.payload(attempt_id="source:unstaged", score_event_id=None)
        payload["target"] = {
            "kind": "new_source",
            "formal_id": None,
            "source_locator": "attachment:unstaged",
            "source_hash_before": None,
        }
        payload["requested_action"] = "record_wrong"

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "stage-source"):
            self.invoke_record(payload, bind_contract_artifacts=False)

    def test_42_freeze_detects_tampered_bundle_child(self) -> None:
        source = self.base / "tamper-before-freeze.png"
        source.write_bytes(b"\x89PNG\r\n\x1a\noriginal")
        staged = self.stage_complete_new_source(
            "attachment:tamper-before-freeze", source
        )
        payload = self.payload(attempt_id="source:tamper-before-freeze", score_event_id=None)
        payload["target"] = {
            "kind": "new_source",
            "formal_id": None,
            "source_locator": "attachment:tamper-before-freeze",
            "source_hash_before": staged["manifest_hash"],
        }
        payload["source_bundle"] = {
            "manifest_path": staged["manifest_path"],
            "manifest_hash": staged["manifest_hash"],
        }
        payload["requested_action"] = "record_wrong"
        capture = self.invoke_record(payload)
        child = self.base / staged["artifacts"][0]["path"]
        child.write_bytes(b"\x89PNG\r\n\x1a\ntampered")

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "哈希与清单不一致"):
            self.invoke_freeze(
                {
                    "schema_version": quick_intake.FREEZE_SCHEMA,
                    "study_date": "2026-07-18",
                    "scope": "all_pending_for_date",
                    "capture_event_ids": [capture["event_id"]],
                    "target_resolutions": [
                        {
                            "formal_id": "GS-700",
                            "capture_event_ids": [capture["event_id"]],
                            "identity_mode": "new_source_created",
                            "source_binding": {
                                "source_locator": "attachment:tamper-before-freeze",
                                "resolved_source_hash": staged["manifest_hash"],
                                "artifact_path": staged["manifest_path"],
                                "artifact_hash": staged["manifest_hash"],
                                "basis": "fixture",
                            },
                        }
                    ],
                }
            )

    def test_43_close_gate_rechecks_bundle_children_after_freeze(self) -> None:
        source = self.base / "tamper-after-freeze.png"
        source.write_bytes(b"\x89PNG\r\n\x1a\noriginal")
        staged = self.stage_complete_new_source(
            "attachment:tamper-after-freeze", source
        )
        payload = self.payload(attempt_id="source:tamper-after-freeze", score_event_id=None)
        payload["target"] = {
            "kind": "new_source",
            "formal_id": None,
            "source_locator": "attachment:tamper-after-freeze",
            "source_hash_before": staged["manifest_hash"],
        }
        payload["source_bundle"] = {
            "manifest_path": staged["manifest_path"],
            "manifest_hash": staged["manifest_hash"],
        }
        payload["requested_action"] = "record_wrong"
        capture = self.invoke_record(payload)
        freeze = self.invoke_freeze(
            {
                "schema_version": quick_intake.FREEZE_SCHEMA,
                "study_date": "2026-07-18",
                "scope": "all_pending_for_date",
                "capture_event_ids": [capture["event_id"]],
                "target_resolutions": [
                    {
                        "formal_id": "GS-700",
                        "capture_event_ids": [capture["event_id"]],
                        "identity_mode": "new_source_created",
                        "source_binding": {
                            "source_locator": "attachment:tamper-after-freeze",
                            "resolved_source_hash": staged["manifest_hash"],
                            "artifact_path": staged["manifest_path"],
                            "artifact_hash": staged["manifest_hash"],
                            "basis": "fixture",
                        },
                    }
                ],
            }
        )
        freeze_event = next(
            event
            for event in self.ledger_events()
            if event["event_id"] == freeze["freeze_id"]
        )
        child = self.base / staged["artifacts"][0]["path"]
        child.write_bytes(b"\x89PNG\r\n\x1a\ntampered-after-freeze")
        state = quick_intake.replay(self.ledger_events())

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "哈希与清单不一致"):
            quick_intake.verify_freeze_current(freeze_event, state)

    def test_44_existing_formal_capture_freezes_new_attachment_as_supplemental(self) -> None:
        source = self.base / "existing-new-attachment.png"
        source.write_bytes(b"\x89PNG\r\n\x1a\nnew-attachment")
        staged = self.stage_source(
            "attachment:existing-formal-new-image",
            [("question", source)],
        )
        payload = self.payload(attempt_id="formal:new-attachment", score_event_id=None)
        payload["source_bundle"] = {
            "manifest_path": staged["manifest_path"],
            "manifest_hash": staged["manifest_hash"],
        }
        capture = self.invoke_record(payload)
        frozen = self.freeze_existing([capture["event_id"]])
        freeze_event = next(
            event
            for event in self.ledger_events()
            if event["event_id"] == frozen["freeze_id"]
        )
        supplemental = freeze_event["targets"][0]["supplemental_source_bundles"]
        self.assertEqual(1, len(supplemental))
        self.assertEqual(staged["manifest_path"], supplemental[0]["artifact_path"])
        self.assertEqual(
            [quick_intake.source_ref_token(supplemental[0])],
            frozen["formal_bindings"][0]["fast_intake_source_refs"],
        )

    def test_45_prepare_reverification_includes_every_bundle_child(self) -> None:
        source = self.base / "prepare-race.png"
        source.write_bytes(b"\x89PNG\r\n\x1a\nprepare-race")
        staged = self.stage_source("attachment:prepare-race", [("question", source)])
        binding = {
            "source_locator": "attachment:prepare-race",
            "resolved_source_hash": staged["manifest_hash"],
            "artifact_path": staged["manifest_path"],
            "artifact_hash": staged["manifest_hash"],
        }
        verified: dict[str, str] = {}
        quick_intake.add_verified_source_binding(verified, binding, "fixture.binding")
        self.assertEqual(3, len(verified))
        child = self.base / staged["artifacts"][0]["path"]
        child.write_bytes(b"\x89PNG\r\n\x1a\nchanged-after-prepare")

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "并发变化"):
            quick_intake.reverify_artifacts(
                [
                    {"path": path, "sha256": digest}
                    for path, digest in sorted(verified.items())
                ]
            )

    def test_46_stage_source_rejects_empty_and_oversized_files(self) -> None:
        empty = self.base / "empty.png"
        empty.write_bytes(b"")
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "空文件"):
            self.stage_source("attachment:empty", [("question", empty)])

        oversized = self.base / "oversized.png"
        oversized.write_bytes(b"\x89PNG\r\n\x1a\nlarge")
        with mock.patch.object(quick_intake, "MAX_SOURCE_ARTIFACT_BYTES", 8):
            with self.assertRaisesRegex(quick_intake.QuickIntakeError, "超过"):
                self.stage_source(
                    "attachment:oversized",
                    [("question", oversized)],
                    name="stage-oversized.json",
                )

    def test_47_capture_rejects_bundle_hash_date_and_locator_mismatch(self) -> None:
        source = self.base / "binding-mismatch.png"
        source.write_bytes(b"\x89PNG\r\n\x1a\nbinding")
        staged = self.stage_complete_new_source("attachment:binding-a", source)

        bad_hash = self.payload(attempt_id="source:bad-hash", score_event_id=None)
        bad_hash["target"] = {
            "kind": "new_source",
            "formal_id": None,
            "source_locator": "attachment:binding-a",
            "source_hash_before": None,
        }
        bad_hash["source_bundle"] = {
            "manifest_path": staged["manifest_path"],
            "manifest_hash": "0" * 64,
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "清单哈希"):
            self.invoke_record(bad_hash, name="bad-hash.json")

        bad_locator = self.payload(attempt_id="source:bad-locator", score_event_id=None)
        bad_locator["target"] = {
            "kind": "new_source",
            "formal_id": None,
            "source_locator": "attachment:binding-b",
            "source_hash_before": staged["manifest_hash"],
        }
        bad_locator["source_bundle"] = {
            "manifest_path": staged["manifest_path"],
            "manifest_hash": staged["manifest_hash"],
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "source_locator"):
            self.invoke_record(bad_locator, name="bad-locator.json")

        bad_date = self.payload(
            attempt_id="formal:bad-date",
            score_event_id=None,
            study_date="2026-07-17",
        )
        bad_date["source_bundle"] = {
            "manifest_path": staged["manifest_path"],
            "manifest_hash": staged["manifest_hash"],
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "study_date"):
            self.invoke_record(bad_date, name="bad-date.json")

    def test_48_concurrent_identical_staging_records_once(self) -> None:
        source = self.base / "concurrent.png"
        source.write_bytes(b"\x89PNG\r\n\x1a\nconcurrent")
        raw = {
            "schema_version": quick_intake.SOURCE_STAGE_SCHEMA,
            "study_date": "2026-07-18",
            "source_locator": "attachment:concurrent",
            "artifacts": [{"role": "question", "path": str(source)}],
        }
        payloads = [quick_intake.normalize_source_stage(raw) for _ in range(2)]
        statuses: list[str] = []
        errors: list[Exception] = []

        def worker(payload: dict) -> None:
            try:
                with quick_intake.exclusive_lock(quick_intake.SOURCE_LOCK_PATH):
                    status, _, _ = quick_intake.stage_source_bundle(payload)
                statuses.append(status)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(payload,)) for payload in payloads]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual([], errors)
        self.assertEqual(["noop", "recorded"], sorted(statuses))

    def test_49_legacy_new_source_can_amend_to_staged_bundle(self) -> None:
        attempt_id = "legacy:source:fixture"
        locator = "attachment:legacy-source"
        evidence = quick_intake.normalize_evidence(
            self.payload(attempt_id=attempt_id, score_event_id=None)["evidence"]
        )
        event_id = quick_intake.stable_event_id(
            "MFI-CAP",
            {"attempt_id": attempt_id, "target_identity": locator},
        )
        legacy_event = quick_intake.with_content_hash(
            {
                "schema_version": quick_intake.LEDGER_SCHEMA,
                "event_type": "capture",
                "event_id": event_id,
                "idempotency_key": quick_intake.sha256_value(
                    {"attempt_id": attempt_id, "target_identity": locator}
                ),
                "payload_hash": quick_intake.sha256_value({"legacy": True}),
                "recorded_at": "2026-07-18T12:00:00+08:00",
                "study_date": "2026-07-18",
                "attempt_id": attempt_id,
                "target": {
                    "kind": "new_source",
                    "formal_id": None,
                    "source_locator": locator,
                    "source_hash_before": None,
                    "identity_state": "needs_user",
                },
                "score_ref": None,
                "requested_action": "record_wrong",
                "thread_ref": None,
                "evidence": evidence,
                "initial_state": "pending_nightly",
            }
        )
        quick_intake.append_jsonl(self.events_path, legacy_event)
        source = self.base / "legacy.png"
        source.write_bytes(b"\x89PNG\r\n\x1a\nlegacy")
        staged = self.stage_source(locator, [("question", source)])
        result = self.invoke_amend(
            {
                "schema_version": quick_intake.AMENDMENT_SCHEMA,
                "capture_event_id": event_id,
                "reason": {"text": "补固化来源", "origin": "source_verified"},
                "evidence": evidence,
                "target_patch": {
                    "source_hash_before": staged["manifest_hash"],
                    "source_bundle": {
                        "manifest_path": staged["manifest_path"],
                        "manifest_hash": staged["manifest_hash"],
                    },
                },
            }
        )
        self.assertEqual("recorded", result["status"])
        pending = self.invoke_pending()["pending"][0]
        self.assertEqual("source_backed_amendment", pending["identity_state"])
        self.assertEqual(staged["manifest_hash"], pending["source_hash_before"])
        self.assertEqual(staged["manifest_path"], pending["source_bundle"]["manifest_path"])

    def test_50_existing_warmup_binds_contract_source_bundle(self) -> None:
        score_id = self.append_score_event()
        result = self.invoke_record(self.payload(score_event_id=score_id))
        self.assertEqual("recorded", result["status"])
        self.assertTrue(self.source_staging_root.exists())
        self.assertIsNotNone(result["source_manifest_path"])

    def test_51_verify_rechecks_source_manifest_and_children(self) -> None:
        source = self.base / "verify-source.png"
        source.write_bytes(b"\x89PNG\r\n\x1a\nverify")
        staged = self.stage_complete_new_source("attachment:verify-source", source)
        payload = self.payload(attempt_id="source:verify", score_event_id=None)
        payload["target"] = {
            "kind": "new_source",
            "formal_id": None,
            "source_locator": "attachment:verify-source",
            "source_hash_before": staged["manifest_hash"],
        }
        payload["source_bundle"] = {
            "manifest_path": staged["manifest_path"],
            "manifest_hash": staged["manifest_hash"],
        }
        payload["requested_action"] = "record_wrong"
        self.invoke_record(payload)
        verified = self.invoke_verify()
        self.assertEqual(1, verified["source_bundle_count"])
        self.assertEqual(3, verified["source_artifact_count"])

        child = self.base / staged["artifacts"][0]["path"]
        child.write_bytes(b"\x89PNG\r\n\x1a\ntampered")
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "哈希与清单不一致"):
            self.invoke_verify()

    def test_52_rollback_index_ignores_method_template_for_same_card(self) -> None:
        wrong_unit = {
            "复习单元ID": "MATH1-GS-WQ-GS629",
            "类型": "错题",
            "关联错题ID": "GS-629",
        }
        method_unit = {
            "复习单元ID": "MATH1-GS-METHOD-FIXTURE",
            "类型": "方法模板",
            "关联错题ID": "GS-629",
        }
        self.units_path.write_text(
            json.dumps([wrong_unit, method_unit], ensure_ascii=False),
            encoding="utf-8",
        )

        self.assertEqual({"GS-629": wrong_unit}, quick_intake.load_unit_index())

    def test_53_closeout_accepts_scheduler_source_version_and_older_bootstrap_events(self) -> None:
        capture = self.invoke_record(self.payload())
        receipt = self.closeout_payload(capture["event_id"])
        unit = json.loads(self.units_path.read_text(encoding="utf-8"))[0]
        semantic_version = quick_intake.scheduler_source_version_for_card(
            "GS-629",
            self.card_path,
        )
        unit["定点同步状态"]["源版本"] = semantic_version
        unit["定点同步状态"]["已处理错题事件ID"].insert(
            0,
            "GS-629|2026-07-17|1",
        )
        unit["调度事件历史"][0]["源版本"] = semantic_version
        self.units_path.write_text(
            json.dumps([unit], ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        receipt["capture_results"][0]["durable_record"]["record_hash"] = (
            quick_intake.sha256_value(unit)
        )

        result = self.invoke_close(receipt)

        self.assertEqual("recorded", result["status"])

    def test_54_closeout_rejects_unfrozen_same_day_bootstrap_event(self) -> None:
        capture = self.invoke_record(self.payload())
        receipt = self.closeout_payload(capture["event_id"])
        unit = json.loads(self.units_path.read_text(encoding="utf-8"))[0]
        unit["定点同步状态"]["已处理错题事件ID"].insert(
            0,
            "GS-629|2026-07-18|1",
        )
        self.units_path.write_text(
            json.dumps([unit], ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        receipt["capture_results"][0]["durable_record"]["record_hash"] = (
            quick_intake.sha256_value(unit)
        )

        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "非冻结批次"):
            self.invoke_close(receipt)

    def test_55_fresh_v2_binds_question_and_unique_solution_text_artifact(self) -> None:
        payload = self.payload(attempt_id="contract:positive", score_event_id=None)
        payload["episode_evidence"]["solution_text"] = "  first line\r\nsecond line  "
        payload["episode_evidence"]["user_answer_text"] = "用户先写出了第一步"
        payload["evidence"]["self_corrections"] = [
            {"text": "用户自行改正了符号", "origin": "user_confirmed"}
        ]

        result = self.invoke_record(payload)
        manifest_path = self.base / result["source_manifest_path"]
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        question_rows = [row for row in manifest["artifacts"] if row["role"] == "question"]
        solution_rows = [
            row for row in manifest["artifacts"] if row["role"] == "solution_text"
        ]

        self.assertEqual(quick_intake.SOURCE_BUNDLE_SCHEMA, manifest["schema_version"])
        self.assertEqual(1, len(question_rows))
        self.assertTrue(question_rows[0]["media_type"].startswith("image/"))
        self.assertEqual(1, len(solution_rows))
        solution_path = self.base / solution_rows[0]["path"]
        self.assertEqual(b"first line\nsecond line\n", solution_path.read_bytes())
        self.assertEqual(
            hashlib.sha256(solution_path.read_bytes()).hexdigest(),
            solution_rows[0]["sha256"],
        )
        event_text = json.dumps(self.ledger_events()[0], ensure_ascii=False)
        self.assertNotIn(str(self.base), event_text)
        self.assertEqual(
            "  first line\r\nsecond line  ",
            self.ledger_events()[0]["episode_evidence"]["solution_text"],
        )

    def test_56_solution_text_contract_fails_closed_before_capture_write(self) -> None:
        missing_bundle = self.payload(attempt_id="contract:missing-bundle", score_event_id=None)
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "stage-source"):
            self.invoke_record(missing_bundle, bind_contract_artifacts=False)

        question = self.base / "contract-question.png"
        question.write_bytes(b"\x89PNG\r\n\x1a\ncontract-question")
        missing_solution = self.stage_source(
            "contract:missing-solution",
            [("question", question)],
            solution_text=None,
            name="stage-missing-solution.json",
        )
        payload = self.payload(attempt_id="contract:missing-solution", score_event_id=None)
        payload["source_bundle"] = {
            "manifest_path": missing_solution["manifest_path"],
            "manifest_hash": missing_solution["manifest_hash"],
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "solution_text"):
            self.invoke_record(payload, bind_contract_artifacts=False)

        empty = self.base / "empty-solution.txt"
        empty.write_text("  \r\n", encoding="utf-8", newline="")
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "非空文本"):
            self.stage_source(
                "contract:empty-solution",
                [("question", question), ("solution_text", empty)],
                solution_text=None,
                name="stage-empty-solution.json",
            )

        first = self.base / "solution-a.txt"
        second = self.base / "solution-b.md"
        first.write_text("solution A", encoding="utf-8")
        second.write_text("solution B", encoding="utf-8")
        duplicate = self.stage_source(
            "contract:duplicate-solution",
            [("question", question), ("solution_text", first), ("solution_text", second)],
            solution_text=None,
            name="stage-duplicate-solution.json",
        )
        payload = self.payload(attempt_id="contract:duplicate-solution", score_event_id=None)
        payload["source_bundle"] = {
            "manifest_path": duplicate["manifest_path"],
            "manifest_hash": duplicate["manifest_hash"],
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "恰好绑定一个"):
            self.invoke_record(payload, bind_contract_artifacts=False)

        mismatch = self.stage_source(
            "contract:mismatch-solution",
            [("question", question)],
            solution_text="different verified text",
            name="stage-mismatch-solution.json",
        )
        payload = self.payload(attempt_id="contract:mismatch-solution", score_event_id=None)
        payload["source_bundle"] = {
            "manifest_path": mismatch["manifest_path"],
            "manifest_hash": mismatch["manifest_hash"],
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "不一致"):
            self.invoke_record(payload, bind_contract_artifacts=False)
        self.assertFalse(self.events_path.exists())

    def test_57_question_must_be_real_image_and_hash_bound(self) -> None:
        pdf = self.base / "question.pdf"
        pdf.write_bytes(b"%PDF-fixture")
        staged_pdf = self.stage_source("contract:pdf-question", [("question", pdf)])
        payload = self.payload(attempt_id="contract:pdf-question", score_event_id=None)
        payload["source_bundle"] = {
            "manifest_path": staged_pdf["manifest_path"],
            "manifest_hash": staged_pdf["manifest_hash"],
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "question image"):
            self.invoke_record(payload, bind_contract_artifacts=False)

        reference = self.base / "reference.png"
        reference.write_bytes(b"\x89PNG\r\n\x1a\nreference")
        no_question = self.stage_source(
            "contract:no-question", [("reference", reference)]
        )
        payload = self.payload(attempt_id="contract:no-question", score_event_id=None)
        payload["source_bundle"] = {
            "manifest_path": no_question["manifest_path"],
            "manifest_hash": no_question["manifest_hash"],
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "question"):
            self.invoke_record(payload, bind_contract_artifacts=False)

        question = self.base / "hash-bound-question.png"
        question.write_bytes(b"\x89PNG\r\n\x1a\nhash-bound")
        staged = self.stage_source("contract:hash-bound", [("question", question)])
        question_row = next(row for row in staged["artifacts"] if row["role"] == "question")
        (self.base / question_row["path"]).write_bytes(b"\x89PNG\r\n\x1a\ndrift")
        payload = self.payload(attempt_id="contract:hash-bound", score_event_id=None)
        payload["source_bundle"] = {
            "manifest_path": staged["manifest_path"],
            "manifest_hash": staged["manifest_hash"],
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "哈希与清单不一致"):
            self.invoke_record(payload, bind_contract_artifacts=False)
        self.assertFalse(self.events_path.exists())

    def test_58_local_absolute_paths_are_not_persisted(self) -> None:
        cases = {
            "solution": lambda row: row["episode_evidence"].__setitem__(
                "solution_text", "/Users/example/private-solution.txt"
            ),
            "answer": lambda row: row["episode_evidence"].__setitem__(
                "user_answer_text", "/private/tmp/user-answer.txt"
            ),
            "turn": lambda row: row["episode_evidence"].__setitem__(
                "teaching_turns",
                [
                    {
                        "speaker": "user",
                        "kind": "reasoning",
                        "text": "file:///tmp/private-turn.txt",
                        "origin": "user_observed",
                    }
                ],
            ),
            "evidence": lambda row: row["evidence"]["user_facts"][0].__setitem__(
                "text", "/Volumes/private/evidence.txt"
            ),
            "thread": lambda row: row.__setitem__("thread_ref", "~/private-thread"),
        }
        for index, (label, mutate) in enumerate(cases.items()):
            with self.subTest(field=label):
                payload = self.payload(
                    attempt_id=f"contract:path:{index}", score_event_id=None
                )
                mutate(payload)
                with self.assertRaisesRegex(quick_intake.QuickIntakeError, "本机绝对路径"):
                    self.invoke_record(payload)

        locator_payload = self.payload(
            attempt_id="contract:path:locator", score_event_id=None
        )
        locator_payload["target"] = {
            "kind": "new_source",
            "formal_id": None,
            "source_locator": "/Users/example/question.png",
            "source_hash_before": None,
        }
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "本机绝对路径"):
            self.invoke_record(locator_payload)
        self.assertFalse(self.events_path.exists())

    def test_59_deployment_identity_fields_are_rejected(self) -> None:
        cases = []
        top = self.payload(attempt_id="contract:release", score_event_id=None)
        top["release_id"] = "f" * 64
        cases.append(top)
        activation = self.payload(attempt_id="contract:activation", score_event_id=None)
        activation["target"]["activation_id"] = "activation-fixture"
        cases.append(activation)
        authority = self.payload(attempt_id="contract:authority", score_event_id=None)
        authority["episode_evidence"]["authority_fingerprint"] = "a" * 64
        cases.append(authority)
        mcp = self.payload(attempt_id="contract:mcp", score_event_id=None)
        mcp["evidence"]["mcp_authority"] = "fixture"
        cases.append(mcp)

        for payload in cases:
            with self.subTest(attempt_id=payload["attempt_id"]):
                with self.assertRaises(quick_intake.QuickIntakeError):
                    self.invoke_record(payload)
        self.assertFalse(self.events_path.exists())

    def test_60_fresh_v1_is_rejected_and_zero_external_calls_or_formal_writes(self) -> None:
        legacy = self.payload(attempt_id="contract:fresh-v1", score_event_id=None)
        legacy["schema_version"] = quick_intake.CAPTURE_SCHEMA
        with self.assertRaisesRegex(quick_intake.QuickIntakeError, "fresh Capture"):
            self.invoke_record(legacy, bind_contract_artifacts=False)

        protected = {
            "card": self.card_path.read_bytes(),
            "units": self.units_path.read_bytes(),
            "review": self.review_log_path.read_bytes(),
            "generated": (self.generated / "marker.json").read_bytes(),
        }
        def fail_external_call(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("quick intake attempted an external call")

        with (
            mock.patch("subprocess.run", side_effect=fail_external_call) as process_run,
            mock.patch("subprocess.Popen", side_effect=fail_external_call) as process_open,
            mock.patch(
                "socket.create_connection", side_effect=fail_external_call
            ) as network_call,
            mock.patch("urllib.request.urlopen", side_effect=fail_external_call) as http_call,
        ):
            result = self.invoke_record(
                self.payload(attempt_id="contract:zero-calls", score_event_id=None)
            )
        self.assertEqual("recorded", result["status"])
        self.assertEqual(0, process_run.call_count)
        self.assertEqual(0, process_open.call_count)
        self.assertEqual(0, network_call.call_count)
        self.assertEqual(0, http_call.call_count)
        self.assertEqual(protected["card"], self.card_path.read_bytes())
        self.assertEqual(protected["units"], self.units_path.read_bytes())
        self.assertEqual(protected["review"], self.review_log_path.read_bytes())
        self.assertEqual(protected["generated"], (self.generated / "marker.json").read_bytes())

    def test_61_post_threshold_capture_writes_release_neutral_binding_sidecar(self) -> None:
        payload = self.payload(
            attempt_id="contract:producer-attestation",
            score_event_id=None,
            study_date="2026-08-17",
        )
        with mock.patch.object(
            quick_intake,
            "now_local",
            return_value="2026-08-17T15:00:00+08:00",
        ):
            receipt = self.invoke_record(payload)
        self.assertEqual(receipt["producer_binding_status"], "attested")
        self.assertRegex(
            receipt["producer_binding_attestation_sha256"], r"^[0-9a-f]{64}$"
        )
        sidecar = Path(receipt["producer_binding_attestation_path"])
        self.assertTrue(sidecar.is_file())
        self.assertTrue(sidecar.is_relative_to(self.base.resolve()))
        value = json.loads(sidecar.read_text(encoding="utf-8"))
        self.assertEqual(value["capture_id"], receipt["event_id"])
        self.assertEqual(value["capture_content_sha256"], receipt["content_hash"])
        self.assertEqual(value["formal_write_count"], 0)
        self.assertFalse(
            {"release_id", "activation_id", "dispatcher_authority", "mcp_authority"}
            & set(value)
        )


if __name__ == "__main__":
    unittest.main()
