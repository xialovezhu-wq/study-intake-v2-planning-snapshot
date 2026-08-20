from __future__ import annotations

import concurrent.futures
import json
import hashlib
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from study_read_mcp.errors import StudyReadError
from study_read_mcp.envelope import success_envelope
from study_read_mcp.models import RouteContext
from study_read_mcp.morning import canonical_scope_hash
from study_read_mcp.release import SERVER_RELEASE
from study_read_mcp.server import _safe_call
from study_read_mcp.service import StudyReadService, canonical_mcp_item_ref
from study_read_mcp.session import ReadSession

from .helpers import make_fixture


class ServiceTests(unittest.TestCase):
    ROUTE = RouteContext(
        caller_skill_id="kaoyan-math-visual-review",
        caller_skill_version="2.0.0",
        plugin_version="0.1.0-canary.1",
        route_request_id="route-fixture-001",
        evidence_scope_hash="a" * 64,
    )
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.config = make_fixture(self.base)
        self.service = StudyReadService(self.config)

    def tearDown(self) -> None:
        self.service.close()
        self.temp.cleanup()

    def assert_common(self, result: dict) -> None:
        self.assertTrue(result["ok"])
        self.assertEqual(result["schema_version"], "study-read-mcp.v3")
        self.assertEqual(result["formal_write_count"], 0)
        self.assertEqual(result["model_call_count"], 0)
        self.assertTrue(result["authority_fingerprint"])
        self.assertTrue(result["generation"])
        self.assertEqual(result["read_route"]["consumed_duplicate_read_count"], 0)

    def _install_english_projection_fixture(self) -> Path:
        raw_path = self.config.english_root / "intake/events/2026-08-07/EVT-1.json"
        raw_bytes = raw_path.read_bytes()
        raw = json.loads(raw_bytes)
        effective = {
            "schema_version": "english_capture_event_v2",
            "event_id": "EVT-1",
            "event_type": "sentence_captured",
            "idempotency_key": "projection-fixture",
            "request_sha256": "5" * 64,
            "occurred_at": "2026-08-07T10:00:00+08:00",
            "article": {
                "article_id": "RAW-ARTICLE-FIXTURE",
                "source_id": "RAW-ARTICLE-FIXTURE",
                "source_article": "articles/fixture.md",
                "source_hash": "2" * 64,
            },
            "source": {
                "sentence_id": "S01",
                "source_sentence": "A safe sentence.",
                "sentence_hash": "3" * 64,
                "source_kind": "article",
            },
            "learning": {
                "first_translation": "fixture first translation",
                "user_evidence": ["unknown"],
                "evidence_origin": "synthetic_fixture",
                "hint_level": 1,
                "answer_protection": "practice_safe",
                "translation": "fixture translation",
                "explanation": "fixture explanation",
            },
            "candidates": [
                {
                    "item": "safe",
                    "candidate_type": "word",
                    "meaning": "safe meaning",
                    "decision": "long_term_candidate",
                    "tier_hint": "A",
                }
            ],
            "producer": {"name": "fixture", "role": "capture", "version": "1"},
            "formal_write_count": 0,
            "formal_writeback": False,
        }
        object_hash = lambda value: hashlib.sha256(
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        content_address = "d" * 64
        receipt = {
            "schema_version": "english_capture_migration_receipt_v1",
            "migration_id": f"EN-MIG-{content_address[:24].upper()}",
            "content_address": content_address,
            "migration_version": "fixture-v1-to-v2",
            "created_at": "2026-08-09T00:00:00Z",
            "source_event_id": "EVT-1",
            "source_event_path": str(raw_path),
            "source_event_file_sha256": hashlib.sha256(raw_bytes).hexdigest(),
            "source_event_object_sha256": object_hash(raw),
            "source_schema_version": "english_capture_event_v1",
            "mappings": [],
            "effective_event": effective,
            "effective_event_sha256": object_hash(effective),
            "formal_write_count": 0,
            "model_call_count": 0,
        }
        migration = (
            self.config.english_root
            / "intake/migrations/event-v2"
            / f"{receipt['migration_id']}.json"
        )
        migration.write_text(
            json.dumps(receipt, ensure_ascii=False), encoding="utf-8"
        )
        high_water = hashlib.sha256(
            f"EVT-1:{object_hash(effective)}".encode("utf-8")
        ).hexdigest()
        binding = {
            "schema_version": "english_quick_capture_projection_v2",
            "data_role": "projection",
            "source_id": "RAW-ARTICLE-FIXTURE",
            "study_date": "2026-08-07",
            "effective_event_ids": ["EVT-1"],
            "effective_event_count": 1,
            "effective_event_high_water_sha256": high_water,
            "formal_write_count": 0,
        }
        projection = (
            "<!-- study-intake-projection-binding-v1 "
            + json.dumps(
                binding,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + " -->\n"
            + "# 英语快速入库视图｜RAW-ARTICLE-FIXTURE\n\n"
            + "> 本页由 append-only capture events 重建；它是可重建视图，不是正式学习库。\n\n"
            + "- 日期筛选：2026-08-07\n"
            + "- 有效句子事件：1\n"
            + "- formal_write_count：0\n"
            + "- formal_writeback：none\n\n"
            + "## S01｜EVT-1\n\n"
            + "- 来源文章：articles/fixture.md\n"
            + "- 原句：A safe sentence.\n"
            + f"- 文章 source hash：`{'2' * 64}`\n"
            + f"- 句子 sentence hash：`{'3' * 64}`\n"
            + "- 第一遍翻译：fixture first translation\n"
            + "- 用户证据：unknown\n"
            + "- 证据来源：synthetic_fixture\n"
            + "- 提示层级：L1\n"
            + "- 答案保护：practice_safe\n"
            + "- 本轮译文：fixture translation\n"
            + "- 本轮讲解：fixture explanation\n\n"
            + "### 本句候选\n\n"
            + "- safe｜word｜safe meaning｜long_term_candidate｜A\n"
        )
        path = (
            self.config.english_root
            / "intake/views/2026-08-07/RAW-ARTICLE-FIXTURE-quick-capture.md"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(projection, encoding="utf-8")
        return path

    def luna_service(self, subject: str) -> StudyReadService:
        snapshot = self.service._adapter(subject).authority()
        core = {
            "schema_version": "study-read-mcp-read-session.v1",
            "read_session_id": f"MCPRS-{subject.upper()}-FIXTURE-0001",
            "subject": subject,
            "candidate_release_id": "a" * 64,
            "plugin_version": "0.2.0-test",
            "skill_id": f"background-{subject}-processing",
            "skill_version": "2.0.0",
            "mcp_server_release": SERVER_RELEASE,
            "generation": snapshot.generation,
            "authority_fingerprint": snapshot.fingerprint,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "formal_write_count": 0,
        }
        payload = json.dumps(
            core, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8") + b"\n"
        value = {
            **core,
            "manifest_sha256": hashlib.sha256(payload).hexdigest(),
        }
        path = self.base / f"{subject}-read-session.json"
        path.write_text(
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        os.chmod(path, 0o600)
        return StudyReadService(
            self.config, {subject}, profile="luna", read_session=ReadSession.load(path)
        )

    def test_authority_bundle_and_release_binding(self) -> None:
        result = self.service.authority_bundle(
            ["math", "cs408", "english"], ["authority", "formal_cards", "knowledge_nodes", "article", "preprocessor_release"]
        )
        self.assert_common(result)
        self.assertEqual(len([x for x in result["items"] if x.get("subject") in {"math", "cs408", "english"}]), 3)
        release = [x for x in result["items"] if x.get("capability") == "preprocessor_release"][0]
        self.assertRegex(release["release_id"], r"^[0-9a-f]{64}$")

    def test_thirty_parallel_authority_bindings_fit_the_internal_budget(self) -> None:
        background = StudyReadService(
            self.config, {"math"}, profile="background"
        )
        adapter = background._adapter("math")
        original_authority = adapter.authority
        workers = 30
        barrier = threading.Barrier(workers)
        route = self.ROUTE.model_copy(
            update={
                "caller_skill_id": "background-math-processing",
                "route_request_id": "bind-math-parallel-regression",
            }
        )

        def delayed_authority():
            barrier.wait(timeout=5)
            time.sleep(0.65)
            return original_authority()

        def bind_once(_index: int) -> dict:
            return background.authority_bundle(
                ["math"],
                ["authority", "preprocessor_release"],
                route_context=route,
            )

        try:
            with mock.patch.object(
                adapter, "authority", side_effect=delayed_authority
            ):
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=workers
                ) as pool:
                    results = list(pool.map(bind_once, range(workers)))
            self.assertEqual(len(results), workers)
            self.assertTrue(all(result["ok"] is True for result in results))
            self.assertTrue(
                all(result["read_route"] == route.model_dump(mode="json") for result in results)
            )
            self.assertTrue(
                all(result["server_release"] == SERVER_RELEASE for result in results)
            )
            self.assertTrue(
                all(result["formal_write_count"] == 0 for result in results)
            )
            self.assertTrue(
                all(result["model_call_count"] == 0 for result in results)
            )
        finally:
            background.close()

    def test_timeout_error_envelope_preserves_code_and_bound_route(self) -> None:
        route = self.ROUTE.model_copy(
            update={
                "caller_skill_id": "background-math-processing",
                "route_request_id": "bind-math-timeout-regression",
            }
        )

        def timeout(_context):
            raise StudyReadError(
                "TIMEOUT", "read exceeded its fixed internal deadline", True
            )

        result = _safe_call(
            "authority_bundle", route.model_dump(mode="json"), timeout
        )
        self.assertIs(result["ok"], False)
        self.assertEqual(result["schema_version"], "study-read-mcp.v3")
        self.assertEqual(result["server_release"], SERVER_RELEASE)
        self.assertEqual(result["read_route"], route.model_dump(mode="json"))
        self.assertEqual(result["error"]["code"], "TIMEOUT")
        self.assertIs(result["error"]["retryable"], True)
        self.assertEqual(result["formal_write_count"], 0)
        self.assertEqual(result["model_call_count"], 0)
        self.assertEqual(result["mcp_tool_call_count"], 0)

    def test_math_bundle_excludes_answers_and_marks_roles(self) -> None:
        result = self.service.math_read_bundle([
            {"op": "formal_cards", "ids": ["GS-001"]},
            {"op": "activity_window", "date_from": "2026-08-07", "date_to": "2026-08-07"},
            {"op": "direct_relations", "ids": ["GS-001"]},
        ])
        self.assert_common(result)
        card = result["items"][0]["items"][0]
        self.assertEqual(card["wrong_point"], "Fixture wrong point")
        self.assertEqual(card["error_causes"], ["condition"])
        self.assertEqual(card["method_gap"]["expected_first_action"], "Fixture action")
        self.assertNotIn("answer", card["method_gap"])
        encoded = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("SHOULD_NEVER_APPEAR", encoded)
        roles = {item["data_role"] for block in result["items"] for item in block["items"]}
        self.assertTrue({"formal", "event", "projection", "candidate"} <= roles)

    def test_408_bundle_has_no_question_or_answer_content(self) -> None:
        result = self.service.cs408_read_bundle([
            {"op": "curation_inventory", "study_date": "2026-08-07"},
            {"op": "knowledge_nodes", "ids": ["DS01-01-测试"]},
            {"op": "direct_edges", "ids": ["DS01-01-测试"]},
            {"op": "morning_state"},
            {"op": "review_identity", "ids": ["RID-1"]},
        ])
        self.assert_common(result)
        encoded = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("SHOULD_NEVER_APPEAR", encoded)
        self.assertNotIn("PRIVATE-1", encoded)
        self.assertEqual(self.service.adapters["cs408"].sqlite_opens, 1)
        inventory = result["items"][0]
        self.assertEqual(inventory["data_role"], "projection")
        self.assertEqual(inventory["items"][0]["data_role"], "projection")
        self.assertEqual(inventory["projection_event_binding"]["event_count"], 1)
        self.assertRegex(inventory["projection_event_binding"]["event_ledger_sha256"], r"^[0-9a-f]{64}$")

    def test_408_morning_preparation_profile_reads_exact_four_item_chunk(self) -> None:
        root = self.config.cs408_root
        dashboard = root / "wiki/study_vaults/408-full/StudyVault/00-Dashboard"
        (root / "wiki/study_vaults/408-full/input").mkdir(parents=True, exist_ok=True)
        (root / "复习单元卡").mkdir(parents=True, exist_ok=True)
        (root / "DS_2023_002.md").write_text("# formal\nprotected answer basis\n", encoding="utf-8")
        (root / "复习单元卡/RU_DS_FIXTURE.md").write_text(
            "# review unit\nprivate mechanism evidence\n", encoding="utf-8"
        )
        (root / "复习单元节点映射.md").write_text(
            "| ID | 科目 | 类型 | 机制 | 复习单元 |\n"
            "|---|---|---|---|---|\n"
            "| DS_2023_002 | 数据结构 | B | fixture | RU_DS_FIXTURE |\n",
            encoding="utf-8",
        )
        queue = dashboard / "2026-08-12-408晨间行动队列.md"
        queue.parent.mkdir(parents=True, exist_ok=True)
        queue.write_text(
            "---\nschema: morning_review_action_queue_v3\nreview_date: 2026-08-12\n---\n"
            "#### MQ-01 fixture\n- source_id: `SRC-1`\n- item_kind: derived_d0\n"
            "- review_unit_id: `RU_DS_FIXTURE`\n- formal_node_id: `DS_2023_002`\n",
            encoding="utf-8",
        )
        queue_sha = hashlib.sha256(queue.read_bytes()).hexdigest()
        item_ids = ["MQ-01"]
        scope = canonical_scope_hash("2026-08-12", queue_sha, item_ids)
        route = RouteContext(
            caller_skill_id="kaoyan-408-morning-control",
            caller_skill_version="3.0.0",
            plugin_version="0.3.0",
            route_request_id="morning-fixture-001",
            evidence_scope_hash=scope,
        )
        service = StudyReadService(
            self.config, {"cs408"}, profile="morning_preparation"
        )
        try:
            result = service.cs408_morning_preparation_bundle(
                {
                    "review_date": "2026-08-12",
                    "queue_sha256": queue_sha,
                    "item_ids": item_ids,
                },
                route_context=route,
            )
            self.assertEqual(result["profile"], "morning_preparation")
            self.assertEqual(result["mcp_tool_call_count"], 1)
            self.assertEqual(result["formal_write_count"], 0)
            self.assertEqual(result["learner_evidence_write_count"], 0)
            evidence = result["items"][0]["evidence"]
            self.assertEqual(
                {row["source_ref"] for row in evidence},
                {"DS_2023_002.md", "复习单元卡/RU_DS_FIXTURE.md"},
            )
        finally:
            service.close()

    def test_408_morning_preparation_rejects_unbound_scope(self) -> None:
        with self.assertRaises(StudyReadError) as context:
            StudyReadService(self.config, {"math"}, profile="morning_preparation")
        self.assertEqual(context.exception.code, "INVALID_ARGUMENT")

    def test_bound_route_context_is_echoed(self) -> None:
        result = self.service.math_read_bundle(
            [{"op": "formal_cards", "ids": ["GS-001"]}],
            route_context=self.ROUTE,
        )
        self.assertEqual(result["profile"], "ordinary")
        self.assertEqual(result["read_route"]["caller_skill_id"], "kaoyan-math-visual-review")
        self.assertEqual(result["read_route"]["route_request_id"], "route-fixture-001")

    def test_background_profile_requires_subject_bound_route(self) -> None:
        background = StudyReadService(self.config, {"math"}, profile="background")
        try:
            with self.assertRaises(StudyReadError) as context:
                background.math_read_bundle([{"op": "formal_cards", "ids": ["GS-001"]}])
            self.assertEqual(context.exception.code, "ROUTE_CONTEXT_REQUIRED")
            wrong = self.ROUTE.model_copy(update={"caller_skill_id": "background-cs408-processing"})
            with self.assertRaises(StudyReadError) as context:
                background.math_read_bundle(
                    [{"op": "formal_cards", "ids": ["GS-001"]}], route_context=wrong
                )
            self.assertEqual(context.exception.code, "ROUTE_SCOPE_MISMATCH")
            bound = self.ROUTE.model_copy(update={"caller_skill_id": "background-math-processing"})
            result = background.math_read_bundle(
                [{"op": "formal_cards", "ids": ["GS-001"]}], route_context=bound
            )
            self.assertEqual(result["profile"], "background")
        finally:
            background.close()

    def test_408_projection_event_mismatch_fails_closed(self) -> None:
        state_path = self.config.cs408_root / "wiki/study_vaults/408-full/state/intake-curation/state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["event_count"] = 2
        state_path.write_text(json.dumps(state), encoding="utf-8")
        with self.assertRaises(StudyReadError) as context:
            self.service.cs408_read_bundle([{"op": "curation_inventory", "study_date": "2026-08-07"}])
        self.assertEqual(context.exception.code, "PROJECTION_EVENT_MISMATCH")

    def test_408_review_projection_binds_canonical_high_water(self) -> None:
        ledger = self.config.cs408_root / "wiki/study_vaults/408-full/state/review-loop/events.jsonl"
        with ledger.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"event_id": "E-UNINDEXED"}) + "\n")
        luna = self.luna_service("cs408")
        try:
            with self.assertRaises(StudyReadError) as context:
                luna.luna_read("cs408", {"collection": "review_events"})
            self.assertEqual(context.exception.code, "PROJECTION_EVENT_MISMATCH")
        finally:
            luna.close()

    def test_408_morning_session_catalog_never_silently_selects_latest(self) -> None:
        state = (
            self.config.cs408_root
            / "wiki/study_vaults/408-full/state/morning-review/MR-2026-08-08-fixture/state.json"
        )
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text(
            json.dumps({
                "session_id": "MR-2026-08-08-fixture", "status": "closed",
                "review_date": "2026-08-08", "item_order": [],
            }),
            encoding="utf-8",
        )
        luna = self.luna_service("cs408")
        try:
            result = luna.luna_read(
                "cs408", {"collection": "morning_sessions", "page_size": 48}
            )
            self.assertEqual(result["total_count"], 2)
            self.assertEqual(
                {row["stable_id"] for row in result["items"]},
                {"MR-2026-08-07-fixture", "MR-2026-08-08-fixture"},
            )
        finally:
            luna.close()

    def test_408_study_vault_manifest_drift_does_not_masquerade_as_formal(self) -> None:
        manifest_path = (
            self.config.cs408_root / "wiki/study_vaults/408-full/manifest.json"
        )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["concept_count"] = 2
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        luna = self.luna_service("cs408")
        try:
            with self.assertRaises(StudyReadError) as context:
                luna.luna_read("cs408", {"collection": "knowledge_safe_notes"})
            self.assertEqual(context.exception.code, "PROJECTION_MANIFEST_MISMATCH")
            formal = luna.luna_read(
                "cs408", {"collection": "formal_knowledge_catalog", "page_size": 1}
            )
            self.assertEqual(formal["items"][0]["data_role"], "formal_knowledge_catalog")
        finally:
            luna.close()

    def test_english_bundle_excludes_questions(self) -> None:
        result = self.service.english_read_bundle({
            "article_id": "RAW-ARTICLE-FIXTURE", "sentence_ids": ["S01"], "terms": ["safe"],
            "study_date": "2026-08-07", "include": ["article", "sentences", "vocab_status", "day_events", "patterns", "coverage"],
        })
        self.assert_common(result)
        encoded = json.dumps(result, ensure_ascii=False)
        self.assertNotIn("SHOULD_NEVER_APPEAR", encoded)
        self.assertIn("A safe sentence.", encoded)

    def test_english_effective_events_bind_migration_and_preserve_hash_roles(self) -> None:
        raw_path = self.config.english_root / "intake/events/2026-08-07/EVT-1.json"
        raw_bytes = raw_path.read_bytes()
        raw = json.loads(raw_bytes)
        effective = json.loads(json.dumps(raw))
        effective["schema_version"] = "english_capture_event_v2"
        effective["article"]["source_id"] = "RAW-ARTICLE-FIXTURE"
        object_hash = lambda value: hashlib.sha256(
            json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        content_address = "c" * 64
        receipt = {
            "schema_version": "english_capture_migration_receipt_v1",
            "migration_id": f"EN-MIG-{content_address[:24].upper()}",
            "content_address": content_address,
            "migration_version": "fixture-v1-to-v2",
            "created_at": "2026-08-09T00:00:00Z",
            "source_event_id": "EVT-1",
            "source_event_path": str(raw_path),
            "source_event_file_sha256": hashlib.sha256(raw_bytes).hexdigest(),
            "source_event_object_sha256": object_hash(raw),
            "source_schema_version": "english_capture_event_v1",
            "mappings": [],
            "effective_event": effective,
            "effective_event_sha256": object_hash(effective),
            "formal_write_count": 0,
            "model_call_count": 0,
        }
        migration = self.config.english_root / "intake/migrations/event-v2" / f"{receipt['migration_id']}.json"
        migration.write_text(json.dumps(receipt, ensure_ascii=False), encoding="utf-8")
        luna = self.luna_service("english")
        ordinary = StudyReadService(self.config)
        try:
            result = luna.luna_read("english", {"collection": "effective_events"})
            self.assertEqual(result["total_count"], 1)
            item = result["items"][0]
            self.assertEqual(item["source_id"], "RAW-ARTICLE-FIXTURE")
            self.assertEqual(item["raw_file_sha256"], hashlib.sha256(raw_bytes).hexdigest())
            self.assertEqual(item["raw_object_sha256"], object_hash(raw))
            self.assertEqual(item["effective_object_sha256"], object_hash(effective))
            self.assertEqual(item["data_role"], "effective_event")
            day = ordinary.english_read_bundle({
                "study_date": "2026-08-07", "include": ["day_events"],
            })
            self.assertEqual(day["items"][0]["items"][0]["source_id"], "RAW-ARTICLE-FIXTURE")
            self.assertNotIn("artifact_sha256", day["items"][0]["items"][0])
        finally:
            luna.close()
            ordinary.close()

    def test_english_raw_article_roles_and_full_catalog_surfaces(self) -> None:
        luna = self.luna_service("english")
        try:
            catalog = luna.luna_read("english", {"collection": "article_catalog"})
            self.assertEqual(catalog["total_count"], 1)
            self.assertEqual(catalog["items"][0]["data_role"], "practice_safe_source_catalog")
            article = luna.luna_read("english", {
                "collection": "articles", "ids": ["RAW-ARTICLE-CORPUS"],
            })
            self.assertEqual(article["items"][0]["data_role"], "practice_safe_source")
            sentences = luna.luna_read("english", {
                "collection": "sentences", "article_id": "RAW-ARTICLE-CORPUS",
            })
            self.assertTrue(all(row["data_role"] != "formal" for row in sentences["items"]))
            pages = luna.luna_read("english", {"collection": "article_learning_catalog"})
            self.assertEqual(pages["total_count"], 1)
        finally:
            luna.close()

    def test_english_projection_bound_validates_effective_migration_and_current_bytes(self) -> None:
        self._install_english_projection_fixture()
        result = self.service.authority_bundle(
            ["english"], ["authority"], checks=["projection_bound"]
        )
        self.assert_common(result)
        subject = next(
            item for item in result["items"] if item.get("subject") == "english"
        )
        self.assertEqual(subject["checks"], {"projection_bound": True})

    def test_english_projection_bound_rejects_stale_projection_bytes(self) -> None:
        path = self._install_english_projection_fixture()
        path.write_text(path.read_text(encoding="utf-8") + "stale\n", encoding="utf-8")
        with self.assertRaises(StudyReadError) as context:
            self.service.authority_bundle(
                ["english"], ["authority"], checks=["projection_bound"]
            )
        self.assertEqual(context.exception.code, "PROJECTION_MANIFEST_MISMATCH")

    def test_english_projection_bound_rejects_tampered_high_water(self) -> None:
        path = self._install_english_projection_fixture()
        before = self.service._adapter("english").authority()
        text = path.read_text(encoding="utf-8")
        prefix = "<!-- study-intake-projection-binding-v1 "
        end = text.index(" -->\n")
        binding = json.loads(text[len(prefix):end])
        binding["effective_event_high_water_sha256"] = "0" * 64
        path.write_text(
            prefix
            + json.dumps(
                binding,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + text[end:],
            encoding="utf-8",
        )
        after = self.service._adapter("english").authority()
        self.assertNotEqual(after.generation, before.generation)
        self.assertNotEqual(after.fingerprint, before.fingerprint)
        with self.assertRaises(StudyReadError) as context:
            self.service.authority_bundle(
                ["english"], ["authority"], checks=["projection_bound"]
            )
        self.assertEqual(context.exception.code, "PROJECTION_MANIFEST_MISMATCH")

    def test_english_projection_bound_rejects_missing_projection_closure(self) -> None:
        path = self._install_english_projection_fixture()
        path.unlink()
        with self.assertRaises(StudyReadError) as context:
            self.service.authority_bundle(
                ["english"], ["authority"], checks=["projection_bound"]
            )
        self.assertEqual(context.exception.code, "PROJECTION_MANIFEST_MISMATCH")

    def test_expected_generation_and_hash_fail_closed(self) -> None:
        with self.assertRaisesRegex(StudyReadError, "generation") as context:
            self.service.math_read_bundle([{"op": "formal_cards", "ids": ["GS-001"]}], expected_generation="old")
        self.assertEqual(context.exception.code, "GENERATION_MISMATCH")
        with self.assertRaises(StudyReadError) as context:
            self.service.verify_evidence_batch([{"subject": "math", "artifact_kind": "formal_card", "stable_id": "GS-001", "expected_sha256": "f" * 64}])
        self.assertEqual(context.exception.code, "HASH_MISMATCH")

    def test_output_limit_fails_without_truncation(self) -> None:
        with self.assertRaises(StudyReadError) as context:
            success_envelope(
                subject="math", data_role="formal", authority_source_id="fixture",
                generation="g", authority_fingerprint="f" * 64,
                items=[{"value": "x" * 1024}], output_limit=128,
            )
        self.assertEqual(context.exception.code, "OUTPUT_LIMIT")

    def test_adapter_initialization_failure_is_isolated(self) -> None:
        self.service.close()
        (self.config.english_root / "bank/sentence_patterns.md").unlink()
        isolated = StudyReadService(self.config)
        try:
            result = isolated.authority_bundle(["math", "english"], ["authority"])
            math = next(x for x in result["items"] if x["subject"] == "math")
            english = next(x for x in result["items"] if x["subject"] == "english")
            self.assertTrue(math["available"])
            self.assertFalse(english["available"])
        finally:
            isolated.close()

    def test_luna_model_driven_paging_and_cursor_binding(self) -> None:
        cards = self.config.math_root / "错题知识网络/错题卡"
        for index in range(2, 57):
            marker = "TARGET-OUTSIDE-LOCAL-NEIGHBOR" if index == 56 else f"ordinary-{index}"
            (cards / f"GS-{index:03d}_fixture.md").write_text(
                f"---\nid: GS-{index:03d}\ntitle: {marker}\nwrong_point: {marker}\n---\n",
                encoding="utf-8",
            )
        luna = self.luna_service("math")
        try:
            first = luna.luna_read("math", {"collection": "catalog", "page_size": 48})
            self.assertEqual(first["total_count"], 56)
            self.assertEqual(first["returned_count"], 48)
            self.assertTrue(first["truncated"])
            self.assertIsNotNone(first["next_cursor"])
            self.assertRegex(first["next_cursor"], r"^c3_[0-9]+_[a-f0-9]{16}$")
            self.assertLess(len(first["next_cursor"]), 32)
            second = luna.luna_read(
                "math",
                {
                    "collection": "catalog",
                    "page_size": 48,
                    "cursor": first["next_cursor"],
                },
            )
            self.assertEqual(second["returned_count"], 8)
            self.assertTrue(second["complete"])
            searched = luna.luna_read(
                "math",
                {
                    "collection": "search",
                    "query": "TARGET-OUTSIDE-LOCAL-NEIGHBOR",
                    "page_size": 1,
                },
            )
            self.assertEqual(searched["total_count"], 1)
            self.assertEqual(searched["items"][0]["stable_id"], "GS-056")
            self.assertEqual(searched["mcp_tool_call_count"], 1)
            self.assertEqual(searched["formal_write_count"], 0)
            scoped = luna.luna_read(
                "math",
                {
                    "collection": "formal_cards",
                    "query": "TARGET-OUTSIDE-LOCAL-NEIGHBOR",
                },
            )
            self.assertEqual(
                [row["stable_id"] for row in scoped["items"]], ["GS-056"]
            )
            with self.assertRaises(StudyReadError) as context:
                luna.luna_read(
                    "math",
                    {
                        "collection": "formal_cards",
                        "page_size": 48,
                        "cursor": first["next_cursor"],
                    },
                )
            self.assertEqual(context.exception.code, "CURSOR_SESSION_MISMATCH")
            with self.assertRaises(StudyReadError) as context:
                luna.luna_read("cs408", {"collection": "knowledge_catalog"})
            self.assertEqual(context.exception.code, "READ_SESSION_SUBJECT_MISMATCH")
        finally:
            luna.close()

    def test_math_luna_finds_formal_relation_beyond_first_neighbor_page(self) -> None:
        cards = self.config.math_root / "错题知识网络/错题卡"
        for index in range(2, 63):
            source = f"GS-REL-{index:03d}"
            target = f"FAR-MATH-REL-{index:03d}"
            (cards / f"{source}_fixture.md").write_text(
                "---\n"
                f"id: {source}\n"
                f"title: relation fixture {index}\n"
                "method_gap:\n"
                f"  related_method_card_id: {target}\n"
                "---\n"
                f"# relation fixture {index}\n",
                encoding="utf-8",
            )
        luna = self.luna_service("math")
        try:
            first = luna.luna_read(
                "math",
                {
                    "collection": "formal_relation_declarations",
                    "page_size": 48,
                },
            )
            self.assertGreater(first["total_count"], 48)
            self.assertTrue(first["truncated"])
            second = luna.luna_read(
                "math",
                {
                    "collection": "formal_relation_declarations",
                    "page_size": 48,
                    "cursor": first["next_cursor"],
                },
            )
            far = next(
                row
                for row in second["items"]
                if str(row.get("target") or "").startswith("FAR-MATH-REL-")
            )
            self.assertNotIn(
                far["stable_id"],
                {row["stable_id"] for row in first["items"]},
            )
            searched = luna.luna_read(
                "math",
                {
                    "collection": "formal_relation_declarations",
                    "query": far["target"],
                    "page_size": 1,
                },
            )
            self.assertEqual(searched["total_count"], 1)
            self.assertEqual(
                searched["items"][0]["stable_id"], far["stable_id"]
            )
            self.assertEqual(
                searched["items"][0]["data_role"],
                "formal_declared_relation",
            )
            self.assertEqual(searched["formal_write_count"], 0)
        finally:
            luna.close()

    def test_luna_generation_drift_fails_closed(self) -> None:
        luna = self.luna_service("math")
        try:
            projection = self.config.math_root / "错题知识网络/生成/wrong_questions.json"
            projection.write_text('{"similarities":[],"weak_relations":[],"drift":true}', encoding="utf-8")
            with self.assertRaises(StudyReadError) as context:
                luna.luna_read("math", {"collection": "catalog"})
            self.assertIn(context.exception.code, {"GENERATION_MISMATCH", "AUTHORITY_DRIFT"})
        finally:
            luna.close()

    def test_408_luna_full_catalog_reaches_far_node_without_host_top_k(self) -> None:
        master = self.config.cs408_root / "节点总表.md"
        knowledge = self.config.cs408_root / "知识点标签表.md"
        with master.open("a", encoding="utf-8") as handle:
            for index in range(3, 61):
                marker = "FAR-408-TARGET" if index == 60 else f"ordinary-{index}"
                handle.write(
                    f"|DS_UNK_{index:03d}|SRC-{index}|未记录|数据结构|DS01 基本概念|"
                    f"DS01-{index:02d} {marker}|||选择题|{marker}|fixture|E01 fixture|"
                    "未记录|未记录|fixture|fixture|\n"
                )
        with knowledge.open("a", encoding="utf-8") as handle:
            for index in range(3, 61):
                marker = "FAR-408-TARGET" if index == 60 else f"ordinary-{index}"
                handle.write(f"- DS01-{index:02d} {marker}\n")
        luna = self.luna_service("cs408")
        try:
            first = luna.luna_read(
                "cs408", {"collection": "formal_wrong_item_catalog", "page_size": 48}
            )
            self.assertEqual(first["total_count"], 60)
            self.assertTrue(first["truncated"])
            second = luna.luna_read(
                "cs408",
                {
                    "collection": "formal_wrong_item_catalog",
                    "page_size": 48,
                    "cursor": first["next_cursor"],
                },
            )
            self.assertTrue(second["complete"])
            searched = luna.luna_read(
                "cs408",
                {"collection": "search", "query": "FAR-408-TARGET", "page_size": 1},
            )
            self.assertGreaterEqual(searched["total_count"], 1)
            self.assertIn(
                "DS_UNK_060", [row["stable_id"] for row in searched["items"]]
            )
        finally:
            luna.close()

    def test_english_luna_full_bank_reaches_far_item_and_mastered_items(self) -> None:
        bank = self.config.english_root / "bank/master_bank.csv"
        with bank.open("a", encoding="utf-8", newline="") as handle:
            import csv
            writer = csv.DictWriter(
                handle,
                fieldnames=[
                    "id", "date", "type", "item", "source_article",
                    "source_sentence", "meaning", "usage", "writing_value",
                    "tags", "review_note", "appear_count", "last_seen",
                ],
            )
            for index in range(2, 61):
                marker = "far_english_target" if index == 60 else f"ordinary_{index}"
                writer.writerow({
                    "id": f"V-{index}", "date": "2026-08-07", "type": "word",
                    "item": marker, "source_article": "RAW-ARTICLE-FIXTURE",
                    "source_sentence": "S01", "meaning": marker, "usage": marker,
                    "writing_value": "no", "tags": "fixture", "review_note": "",
                    "appear_count": "1", "last_seen": "2026-08-07",
                })
        luna = self.luna_service("english")
        try:
            first = luna.luna_read(
                "english", {"collection": "vocabulary", "page_size": 48}
            )
            self.assertEqual(first["total_count"], 60)
            second = luna.luna_read(
                "english",
                {
                    "collection": "vocabulary", "page_size": 48,
                    "cursor": first["next_cursor"],
                },
            )
            self.assertTrue(second["complete"])
            searched = luna.luna_read(
                "english",
                {"collection": "search", "query": "far_english_target", "page_size": 1},
            )
            self.assertEqual(searched["total_count"], 1)
            self.assertEqual(searched["items"][0]["stable_id"], "V-60")
            mastered = luna.luna_read(
                "english", {"collection": "mastered_items", "ids": ["known"]}
            )
            self.assertEqual(mastered["items"][0]["item"], "known")
        finally:
            luna.close()

    def test_luna_items_have_service_generated_canonical_evidence_refs(self) -> None:
        luna = self.luna_service("math")
        try:
            result = luna.luna_read(
                "math", {"collection": "formal_card_catalog", "page_size": 1}
            )
            item = result["items"][0]
            expected = canonical_mcp_item_ref(
                subject="math",
                collection="formal_card_catalog",
                stable_id=item["stable_id"],
                source_hash=item["source_hash"],
                generation=result["generation"],
            )
            self.assertEqual(item["evidence_ref"], expected)
            self.assertRegex(item["evidence_ref"], r"^mcp-item:math:[0-9a-f]{64}$")
            self.assertLessEqual(len(item["evidence_ref"]), 320)
            self.assertEqual(item["collection"], "formal_card_catalog")
            self.assertEqual(item["parser_version"], "math-canonical-adapter.v2")
            self.assertTrue(item["data_role"])
            self.assertNotEqual(
                expected,
                canonical_mcp_item_ref(
                    subject="math", collection="formal_card_records",
                    stable_id=item["stable_id"], source_hash=item["source_hash"],
                    generation=result["generation"],
                ),
            )
            with self.assertRaises(StudyReadError) as context:
                StudyReadService._model_visible_items(
                    subject="math", collection="formal_card_catalog",
                    generation=result["generation"],
                    items=[{
                        "stable_id": "GS-001", "source_hash": "a" * 64,
                        "data_role": "formal", "evidence_ref": "adapter-controlled",
                    }],
                )
            self.assertEqual(context.exception.code, "INTERNAL_SAFE")
        finally:
            luna.close()

    def test_math_luna_discovers_non_gs_ids_and_complete_frontmatter(self) -> None:
        cards = self.config.math_root / "错题知识网络/错题卡"
        for stable_id in ("LA-001", "PR-001"):
            (cards / f"{stable_id}_fixture.md").write_text(
                "---\n"
                f"id: {stable_id}\n"
                f"title: {stable_id} fixture\n"
                "subject: fixture\n"
                "method_gap:\n"
                "  enabled: true\n"
                "  related_method_card_ids:\n"
                "    - H01-001\n"
                "  review_priority: 3\n"
                "---\n"
                f"# {stable_id}\n\n## 完整正文\nfixture body\n",
                encoding="utf-8",
            )
        luna = self.luna_service("math")
        try:
            catalog = luna.luna_read(
                "math", {"collection": "formal_card_catalog", "ids": ["LA-001", "PR-001"]}
            )
            self.assertEqual(
                [row["stable_id"] for row in catalog["items"]], ["LA-001", "PR-001"]
            )
            record = luna.luna_read(
                "math", {"collection": "formal_card_records", "ids": ["LA-001"]}
            )["items"][0]
            self.assertEqual(
                record["frontmatter"]["method_gap"]["related_method_card_ids"],
                ["H01-001"],
            )
            self.assertEqual(record["frontmatter"]["method_gap"]["review_priority"], 3)
            self.assertIn("## 完整正文", record["document_markdown"])
            self.assertTrue(record["content_complete"])
        finally:
            luna.close()

    def test_408_canonical_tables_preserve_direction_and_typed_edges(self) -> None:
        luna = self.luna_service("cs408")
        try:
            nodes = luna.luna_read(
                "cs408", {"collection": "formal_nodes", "ids": ["DS_2023_002"]}
            )
            self.assertEqual(nodes["items"][0]["fields"]["主知识点"], "DS01-01 测试知识")
            relations = luna.luna_read(
                "cs408", {"collection": "formal_relationships", "page_size": 48}
            )
            directed = next(
                row for row in relations["items"]
                if row.get("relation_code") == "R06"
            )
            self.assertEqual(directed["source"], "DS_2023_002")
            self.assertEqual(directed["target"], "DS_2023_003")
            self.assertEqual(directed["canonical_relation_name"], "上下游知识链")
            primary = next(
                row for row in relations["items"]
                if row.get("relation_type") == "wrong_item_primary_knowledge"
            )
            self.assertEqual(primary["target"], "DS01-01")
            self.assertEqual(primary["data_role"], "formal_wrong_item_knowledge_relation")
        finally:
            luna.close()

    def test_english_luna_preserves_full_event_and_whole_pattern_card(self) -> None:
        luna = self.luna_service("english")
        try:
            events = luna.luna_read(
                "english", {"collection": "events", "study_date": "2026-08-07"}
            )
            event = events["items"][0]
            self.assertEqual(event["event_payload"]["learning"]["nested"], {"kept": True})
            self.assertEqual(event["event_payload"]["candidates"][0]["item"], "safe")
            self.assertTrue(event["content_complete"])
            patterns = luna.luna_read("english", {"collection": "patterns"})
            pattern = patterns["items"][0]
            self.assertEqual(pattern["stable_id"], "SP-001")
            self.assertIn("完整句型卡第一行", pattern["markdown"])
            self.assertIn("第二行保留", pattern["markdown"])
            self.assertFalse(pattern["is_truncated"])
            self.assertEqual(
                pattern["content_sha256"],
                hashlib.sha256(pattern["markdown"].encode("utf-8")).hexdigest(),
            )
        finally:
            luna.close()


if __name__ == "__main__":
    unittest.main()
