from __future__ import annotations

import contextlib
import csv
import hashlib
import io
import json
import re
import subprocess
import sys
import tempfile
import time
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

from english_pipeline.candidates import render_luna_candidate, validate_luna_candidate
from english_pipeline.cli import main as cli_main
from english_pipeline.constants import MASTERED_HEADER, MASTER_HEADER, REPO_ROOT, SP_FIELDS
from english_pipeline.errors import IdempotencyConflict, SourceHashMismatch, ValidationError
from english_pipeline.events import (
    RELEASE_NEUTRAL_FORBIDDEN_KEYS,
    SimulatedCaptureCrash,
    append_event,
    effective_sentence_events,
    load_events,
    exclusive_lock,
    validate_event,
)
from english_pipeline.formal import formal_hashes, formal_snapshot
from english_pipeline.nightly import freeze_nightly
from english_pipeline.util import atomic_write_json, canonical_bytes, file_sha256, object_sha256, parse_iso_date, sentence_sha256
from english_pipeline.views import (
    complete_article,
    quick_capture_projection_binding,
    render_quick_capture,
    write_quick_capture_view,
)
from english_pipeline.review_status import (
    append_review_status_record,
    build_review_status_proposals,
    effective_review_status,
    eligible_review_bank_rows,
    exact_item_occurrences,
)
from english_pipeline.writer import (
    SimulatedWriterCrash,
    apply_nightly,
    recover_nightly,
)


STUDY_DATE = "2026-08-05"
ARTICLE_HASH = "a" * 64


def canonical_source_bytes(value: bytes) -> bytes:
    text = value.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    normalized = "\n".join(line.rstrip() for line in text.split("\n")).rstrip("\n")
    return normalized.encode("utf-8")


def make_source_object(
    repo: Path,
    *,
    source_id: str,
    slug: str,
    sentences: list[str],
) -> tuple[Path, str, str]:
    article_locator = f"articles/{slug}.md"
    handoff_locator = f"raw/fixtures/{slug}/pipeline_handoff.json"
    payload_locator = "source/practice-safe.md"
    article_path = repo / article_locator
    handoff_path = repo / handoff_locator
    payload_path = handoff_path.parent / payload_locator
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    raw_payload = (
        f"# {slug} Practice Safe\r\n"
        f"source_id: {source_id}  \r\n\r\n"
        "## Article\r\n\r\n"
        + "\r\n\r\n".join(f"{sentence}\t" for sentence in sentences)
        + "\r\n\r\n## Questions\r\n\r\nNo protected answers are present.\r\n\r\n"
    ).encode("utf-8")
    payload_path.write_bytes(raw_payload)
    source_hash = hashlib.sha256(canonical_source_bytes(raw_payload)).hexdigest()
    handoff_path.write_text(
        json.dumps(
            {
                "schema_version": "english-learning-pipeline-handoff-v1",
                "source_id": source_id,
                "source_hash": f"sha256:{source_hash}",
                "canonical_payload": {
                    "path": payload_locator,
                    "normalization": "UTF-8; LF line endings; trailing whitespace removed per line; no final LF",
                    "visibility": "practice_safe",
                },
                "units": [],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    article_path.parent.mkdir(parents=True, exist_ok=True)
    article_path.write_text(
        "\n".join(
            [
                f"# {slug}",
                "",
                "## 基本信息",
                "",
                f"- source_id：{source_id}",
                f"- source_hash：sha256:{source_hash}",
                f"- pipeline_handoff：`{handoff_locator}`",
                "",
                "## 原文全文",
                "",
                *sentences,
                "",
            ]
        ),
        encoding="utf-8",
    )
    return article_path, article_locator, source_hash


def run_cli(argv: list[str]) -> tuple[int, dict | None, str]:
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        code = cli_main(argv)
    payload = json.loads(stdout.getvalue()) if stdout.getvalue().strip() else None
    return code, payload, stderr.getvalue()


def assert_json_schema_subset(
    testcase: unittest.TestCase,
    instance: object,
    schema: dict,
    *,
    root_schema: dict | None = None,
) -> None:
    root = root_schema or schema
    if "$ref" in schema:
        target: object = root
        for token in schema["$ref"].removeprefix("#/").split("/"):
            target = target[token]  # type: ignore[index]
        assert isinstance(target, dict)
        assert_json_schema_subset(testcase, instance, target, root_schema=root)
        return
    if "const" in schema:
        testcase.assertEqual(instance, schema["const"])
    if "enum" in schema:
        testcase.assertIn(instance, schema["enum"])
    declared_types = schema.get("type")
    if declared_types is not None:
        allowed = [declared_types] if isinstance(declared_types, str) else declared_types
        type_checks = {
            "object": lambda value: isinstance(value, dict),
            "string": lambda value: isinstance(value, str),
            "boolean": lambda value: isinstance(value, bool),
            "integer": lambda value: isinstance(value, int) and not isinstance(value, bool),
            "null": lambda value: value is None,
        }
        testcase.assertTrue(
            any(type_checks[item](instance) for item in allowed),
            f"{instance!r} does not match declared JSON type {allowed!r}",
        )
    if isinstance(instance, str):
        if "minLength" in schema:
            testcase.assertGreaterEqual(len(instance), schema["minLength"])
        if "pattern" in schema:
            testcase.assertRegex(instance, schema["pattern"])
    if isinstance(instance, dict):
        required = schema.get("required", [])
        testcase.assertTrue(set(required).issubset(instance))
        properties = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            testcase.assertEqual(set(instance) - set(properties), set())
        for key, value in instance.items():
            if key in properties:
                assert_json_schema_subset(
                    testcase,
                    value,
                    properties[key],
                    root_schema=root,
                )


def capture_request(
    *,
    key: str = "capture-1",
    source_id: str = "RAW-TEST-001",
    sentence_id: str = "S01",
    sentence: str = "A lasting reward can change how people think.",
    first_translation: str | None = "持久的回报会改变人们的想法。",
    evidence_states: list[str] | None = None,
    evidence_origin: str = "live_user",
    candidates: list[dict] | None = None,
    source_kind: str = "article",
    answer_protection: str = "practice_safe",
) -> dict:
    return {
        "event_type": "sentence_captured",
        "idempotency_key": key,
        "occurred_at": f"{STUDY_DATE}T09:00:00Z",
        "article": {
            "article_id": source_id,
            "source_id": source_id,
            "source_article": f"raw/{source_id}.json",
            "source_hash": ARTICLE_HASH,
        },
        "source": {
            "sentence_id": sentence_id,
            "source_sentence": sentence,
            "source_kind": source_kind,
        },
        "learning": {
            "first_translation": first_translation,
            "user_evidence_verbatim": None if first_translation is None else "lasting 不会",
            "evidence_states": list(evidence_states or []),
            "evidence_origin": evidence_origin,
            "user_evidence": ["unknown"] if evidence_states else [],
            "answer_protection": answer_protection,
            "hint_level": 0,
            "translation": "持久的回报会改变人们的想法。",
            "explanation": "lasting 在这里表示持续时间长。",
            "first_breakpoint": "lasting",
            "restatement": "lasting 是持久的。",
        },
        "candidates": list(candidates or []),
    }


def word_candidate(item: str = "lasting") -> dict:
    return {
        "item": item,
        "candidate_type": "单词",
        "meaning": "持久的；长久的。",
        "usage": "形容词，表示某事物持续很长时间。",
        "review_note": "lasting 是持久的，不是最后的。",
        "decision": "bbdc_candidate",
        "tier_hint": "A",
        "reason": "用户明确不会",
    }


def make_formal_repo(root: Path) -> None:
    bank = root / "bank"
    bank.mkdir(parents=True)
    with (bank / "master_bank.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MASTER_HEADER, lineterminator="\n")
        writer.writeheader()
        writer.writerow(
            {
                "id": "20260805-001",
                "date": STUDY_DATE,
                "type": "单词",
                "item": "existing",
                "source_article": "fixture",
                "source_sentence": "An existing row remains stable.",
                "meaning": "已有的",
                "usage": "existing",
                "writing_value": "一般",
                "tags": "fixture",
                "review_note": "stable",
                "appear_count": "1",
                "last_seen": STUDY_DATE,
            }
        )
    with (bank / "mastered_items.csv").open("w", encoding="utf-8", newline="") as handle:
        csv.DictWriter(handle, fieldnames=MASTERED_HEADER, lineterminator="\n").writeheader()
    (bank / "sentence_patterns.md").write_text("# 句式结构卡片库\n\n## 卡片区\n", encoding="utf-8")


def luna_candidate(event: dict, *, items: list[dict] | None = None, candidate_id: str = "LUNA-20260805-001") -> dict:
    source_id = event["article"]["source_id"]
    return {
        "schema_version": "english_luna_candidate_v1",
        "candidate_id": candidate_id,
        "study_date": STUDY_DATE,
        "source_id": source_id,
        "article_id": source_id,
        "article_source_hash": event["article"]["source_hash"],
        "created_at": f"{STUDY_DATE}T12:00:00Z",
        "producer": {"role": "luna_candidate_consumer", "model": "gpt-5.6-luna", "prompt_version": "v1"},
        "runtime_identity": {
            "requested": {"model": "gpt-5.6-luna", "reasoning_effort": "max"},
            "observed": {"model": "gpt-5.6-luna", "reasoning_effort": "max"},
            "provenance": {"source": "runtime_attestation", "reference": "fixture-attestation", "observed_at": f"{STUDY_DATE}T12:00:00Z"},
            "status": "confirmed",
        },
        "capture_event_ids": [event["event_id"]],
        "capture_event_sha256": {event["event_id"]: object_sha256(event)},
        "formal_write_count": 0,
        "formal_writeback": "none",
        "validation": {"status": "PASS", "blockers": []},
        "items": list(items or []),
    }


def lasting_item(event: dict) -> dict:
    return {
        "item_id": "ITEM-025",
        "sequence": 25,
        "item": "lasting",
        "candidate_type": "单词",
        "candidate_status": "familiarity_candidate",
        "tier": "A",
        "source_event_id": event["event_id"],
        "source_article": event["article"]["source_article"],
        "source_kind": "article",
        "source_sentence": event["source"]["source_sentence"],
        "article_source_hash": event["article"]["source_hash"],
        "sentence_hash": event["source"]["sentence_hash"],
        "evidence_states": ["unknown_observed", "guided_understood"],
        "evidence_origin": "live_user",
        "user_evidence": ["unknown"],
        "bank_status": "existing_bank",
        "bank_match_ids": ["20260726-025"],
        "mastered_status": "clear",
        "grounding": {
            "status": "passed",
            "user_evidence_ref": event["event_id"],
            "writing_pattern": {"id": "WRITING-APPROVED-PATTERN-013", "status": "approved"},
            "writing_vocabulary": {"id": "WRITING-APPROVED-VOCAB-046", "status": "approved"},
            "syllabus_occurrence": {"id": "SYL-lasting", "status": "verified_text"},
            "sentence_pattern": {"id": "SP-004", "status": "optional"},
            "old_word_sources": [
                {"id": "20260621-001", "item": "reputation", "status": "D7"},
                {"id": "20260621-002", "item": "strained relations with", "status": "D7"},
            ],
            "naturalness_check": "PASS",
        },
        "card": {
            "meaning": "持久的；长久的。",
            "source_translation": "养育孩子可能带来持久的回报。",
            "usage": "lasting 是形容词，表示某事物持续很长时间，如 lasting influence、lasting happiness。",
            "review_note": "lasting 是“持久的”，不是“最后的”。",
            "adopted_pattern": "We must acknowledge the fact that + 完整分句。",
            "old_word_example": "We must acknowledge the fact that sharing experience and expertise can build lasting trust, enhance a team's reputation, and prevent strained relations with colleagues.",
            "example_translation": "我们必须承认，分享经验和专业知识能够建立持久的信任、提升团队声誉，并避免与同事关系紧张。",
            "structure_breakdown": "that 引导 fact 的同位语从句；can 后并列 build、enhance 和 prevent。",
            "review_old_words": ["reputation", "strained relations with"],
        },
    }


def master_insert_action(event_id: str, item: str = "lasting") -> dict:
    return {
        "action_id": f"ACT-{item}",
        "action_type": "master_bank_insert",
        "reason": "source-backed nightly candidate",
        "source_capture_event_ids": [event_id],
        "row": {
            "id": "",
            "date": STUDY_DATE,
            "type": "单词",
            "item": item,
            "source_article": "raw/fixture.json",
            "source_sentence": "A lasting reward can change how people think.",
            "meaning": "持久的",
            "usage": "lasting reward",
            "writing_value": "一般",
            "tags": "阅读高频",
            "review_note": "lasting is not last",
            "appear_count": 1,
            "last_seen": STUDY_DATE,
        },
    }


def sp_action(event_id: str) -> dict:
    values = {
        "title": "We must acknowledge the fact that",
        "骨架": "S + V + O(同位从)",
        "难度等级": "复合句",
        "基本句型": "主谓宾",
        "从句类型": "同位语从句",
        "场景标签": "写作模板",
        "可复用程度": "高",
        "中文解释": "用 that 从句说明 fact 的具体内容。",
        "结构拆解": ["主句：S + V + O", "从句：that + S + V"],
        "生成模板": "We must acknowledge the fact that + 完整分句",
        "相关词汇/搭配": "acknowledge the fact",
        "常用变体": "recognize the fact that",
        "来源与示例": ["A real source sentence.（fixture，2026-08-05）"],
        "use_count": 0,
        "last_used": STUDY_DATE,
    }
    assert set(values) == set(SP_FIELDS)
    return {
        "action_id": "ACT-SP",
        "action_type": "sentence_pattern_append",
        "reason": "reusable structure",
        "source_capture_event_ids": [event_id],
        "fields": values,
    }


def sol_actions(manifest_path: Path, event_id: str, *, actions: list[dict], unresolved: list[dict] | None = None) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return {
        "schema_version": "english_sol_actions_v1",
        "action_set_id": "SOL-ACTIONS-001",
        "batch_id": manifest["batch_id"],
        "batch_manifest_sha256": file_sha256(manifest_path),
        "created_at": f"{STUDY_DATE}T22:00:00Z",
        "producer": {"role": "sol_nightly_reviewer", "model": "gpt-5.6-sol", "prompt_version": "v1"},
        "formal_write_count": 0,
        "formal_writeback": "none",
        "actions": actions,
        "unresolved": list(unresolved or []),
    }


class CaptureEventTests(unittest.TestCase):
    @staticmethod
    def _valid_sentence_event() -> dict:
        with tempfile.TemporaryDirectory() as folder:
            receipt = append_event(
                Path(folder) / "intake",
                capture_request(
                    candidates=[word_candidate()],
                    evidence_states=["unknown_observed"],
                ),
            )
            return json.loads(Path(receipt["event_path"]).read_text(encoding="utf-8"))

    def test_nested_release_and_authority_identity_fields_are_rejected(self) -> None:
        cases: list[tuple[str, dict]] = []

        completion_release = capture_request(key="nested-completion-release")
        completion_release["completion"] = {"release_id": "a" * 64}
        cases.append(("completion.release_id", completion_release))

        authority_fingerprint = capture_request(key="nested-authority-fingerprint")
        authority_fingerprint["learning"]["restatement"] = {
            "metadata": {"authority_fingerprint": "b" * 64}
        }
        cases.append(("learning authority_fingerprint", authority_fingerprint))

        deep_mcp_identity = capture_request(
            key="deep-mcp-authority",
            candidates=[word_candidate()],
            evidence_states=["unknown_observed"],
        )
        deep_mcp_identity["candidates"][0]["reason"] = {
            "audit": [{"runtime": {"mcp_authority_generation": "generation-1"}}]
        }
        cases.append(("deep mcp authority", deep_mcp_identity))

        for label, request in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as folder:
                with self.assertRaises(ValidationError):
                    append_event(Path(folder) / "intake", request)

    def test_capture_coverage_signal_rows_are_exact_and_release_neutral(self) -> None:
        valid = self._valid_sentence_event()
        validate_event(valid)
        for extra_key in ("release_id", "authority", "note"):
            with self.subTest(extra_key=extra_key):
                malformed = deepcopy(valid)
                malformed["capture_coverage"]["signals"][0][extra_key] = (
                    "a" * 64 if extra_key == "release_id" else "forbidden"
                )
                with self.assertRaises(ValidationError):
                    validate_event(malformed)

    def test_sentence_completion_is_rejected_but_article_completion_remains_valid(self) -> None:
        sentence = self._valid_sentence_event()
        sentence["completion"] = {
            "study_date": STUDY_DATE,
            "effective_capture_event_ids": [],
            "capture_event_sha256": {},
        }
        with self.assertRaises(ValidationError):
            validate_event(sentence)

        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            state = base / "intake"
            repo = base / "repo"
            make_formal_repo(repo)
            receipt = append_event(state, capture_request())
            completed = complete_article(
                state,
                repo,
                article_id="RAW-TEST-001",
                idempotency_key="valid-article-completed",
                study_date=STUDY_DATE,
            )
            completion_event = next(
                event
                for event in load_events(state)
                if event["event_id"] == completed["completion_event_id"]
            )
            self.assertEqual(
                set(completion_event["completion"]),
                {
                    "study_date",
                    "effective_capture_event_ids",
                    "capture_event_sha256",
                },
            )
            self.assertEqual(completed["effective_capture_event_ids"], [receipt["capture_id"]])
            validate_event(completion_event)

    def test_legal_sentence_v2_contract_and_historical_v1_read_remain_valid(self) -> None:
        event = self._valid_sentence_event()
        self.assertEqual(event["schema_version"], "english_capture_event_v2")
        self.assertTrue(RELEASE_NEUTRAL_FORBIDDEN_KEYS.isdisjoint(event))
        validate_event(event)

        legacy = deepcopy(event)
        legacy["schema_version"] = "english_capture_event_v1"
        legacy.pop("observed_signals")
        legacy.pop("source_signal_ids")
        legacy.pop("capture_coverage")
        validate_event(legacy)

    def test_idempotent_replay_and_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            request = capture_request(candidates=[word_candidate()], evidence_states=["unknown_observed"])
            first = append_event(state, request)
            replay = append_event(state, request)
            self.assertEqual(first["capture_id"], replay["capture_id"])
            self.assertEqual(replay["status"], "idempotent_noop")
            self.assertEqual(len(list((state / "events").glob("*/*.json"))), 1)
            changed = deepcopy(request)
            changed["learning"]["translation"] = "changed"
            with self.assertRaises(IdempotencyConflict):
                append_event(state, changed)

    def test_receipt_is_rebuilt_after_event_only_crash(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            request = capture_request()
            with self.assertRaises(SimulatedCaptureCrash):
                append_event(state, request, fault_after_event=True)
            event_path = next((state / "events").glob("*/*.json"))
            before = event_path.read_bytes()
            replay = append_event(state, request)
            self.assertEqual(replay["status"], "idempotent_noop")
            self.assertTrue(Path(replay["receipt_path"]).is_file())
            self.assertEqual(event_path.read_bytes(), before)

    def test_correction_is_append_only_and_cannot_cross_sentence(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            original_request = capture_request(first_translation="原始翻译")
            original = append_event(state, original_request)
            correction = capture_request(key="correction-1", first_translation="更正翻译")
            correction["event_type"] = "sentence_correction"
            correction["supersedes_event_id"] = original["capture_id"]
            correction["correction_reason"] = "用户明确更正"
            append_event(state, correction)
            events = load_events(state)
            old = next(row for row in events if row["event_id"] == original["capture_id"])
            self.assertEqual(old["learning"]["first_translation"], "原始翻译")
            self.assertEqual(effective_sentence_events(events)[0]["learning"]["first_translation"], "更正翻译")
            cross = capture_request(key="correction-cross", sentence_id="S02")
            cross["event_type"] = "sentence_correction"
            cross["supersedes_event_id"] = effective_sentence_events(events)[0]["event_id"]
            cross["correction_reason"] = "invalid cross-sentence correction"
            with self.assertRaises(ValidationError):
                append_event(state, cross)

    def test_sentence_hash_and_answer_protection_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            bad_hash = capture_request()
            bad_hash["source"]["sentence_hash"] = "0" * 64
            with self.assertRaises(SourceHashMismatch):
                append_event(state, bad_hash)
            locked = capture_request(source_kind="explanation", answer_protection="practice_safe")
            with self.assertRaises(ValidationError):
                append_event(state, locked)
            unlocked = capture_request(key="unlocked", source_kind="explanation", answer_protection="unlocked")
            append_event(state, unlocked)
            view = render_quick_capture(load_events(state), article_id="RAW-TEST-001")
            self.assertIn("受保护解析正文不在可重建视图回显", view)
            self.assertNotIn(unlocked["source"]["source_sentence"], view)

    def test_zero_candidate_and_explicit_absent_translation(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            request = capture_request(first_translation=None, candidates=[], evidence_states=[])
            request["learning"]["user_evidence_verbatim"] = None
            receipt = append_event(state, request)
            event = load_events(state)[0]
            self.assertEqual(receipt["status"], "created")
            self.assertIsNone(event["learning"]["first_translation"])
            self.assertEqual(event["candidates"], [])

    def test_cli_capture_rebuilds_canonical_view(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            repo = base / "repo"
            state = base / "intake"
            sentence = "A real sentence is captured."
            article_path, article_locator, source_hash = make_source_object(
                repo,
                source_id="RAW-CLI-001",
                slug="cli-source",
                sentences=[sentence],
            )
            argv = [
                "capture", "--repo-root", str(repo), "--state-dir", str(state),
                "--idempotency-key", "cli-1", "--source-id", "RAW-CLI-001",
                "--source-article", str(article_path), "--article-sha256", source_hash,
                "--sentence-id", "S01", "--source-sentence", sentence,
                "--occurred-at", f"{STUDY_DATE}T09:00:00Z",
            ]
            blocked = [
                "publish_quick_flush_intent",
                "freeze_nightly",
                "apply_nightly",
                "recover_nightly",
                "validate_luna_candidate_file",
                "render_luna_candidate_file",
            ]
            with contextlib.ExitStack() as stack:
                for name in blocked:
                    stack.enter_context(
                        patch(
                            f"english_pipeline.cli.{name}",
                            side_effect=AssertionError(f"unexpected capture side effect: {name}"),
                        )
                    )
                code, receipt, stderr = run_cli(argv)
            self.assertEqual(code, 0)
            self.assertEqual(stderr, "")
            assert receipt is not None
            expected = state / "views" / STUDY_DATE / "RAW-CLI-001-quick-capture.md"
            self.assertEqual(Path(receipt["view_path"]).resolve(), expected.resolve())
            self.assertEqual(receipt["projection_status"], "rendered")
            self.assertTrue(expected.is_file())
            rendered = expected.read_text(encoding="utf-8")
            self.assertTrue(rendered.startswith("<!-- study-intake-projection-binding-v1 "))
            event = load_events(state)[0]
            event_path = Path(receipt["event_path"])
            self.assertEqual(event["article"]["source_article"], article_locator)
            self.assertEqual(event["article"]["source_hash"], source_hash)
            self.assertEqual(event_path.read_bytes(), canonical_bytes(event))
            self.assertEqual(file_sha256(event_path), receipt["event_sha256"])
            self.assertEqual(receipt["event_sha256"], object_sha256(event))
            self.assertNotIn(str(repo), event_path.read_text(encoding="utf-8"))
            self.assertEqual(event["formal_write_count"], 0)
            self.assertEqual(receipt["formal_write_count"], 0)
            self.assertFalse((repo / "bank").exists())
            binding = quick_capture_projection_binding(
                [event], article_id="RAW-CLI-001", study_date=STUDY_DATE
            )
            self.assertEqual(binding["data_role"], "projection")
            self.assertEqual(binding["effective_event_ids"], [event["event_id"]])
            self.assertEqual(binding["formal_write_count"], 0)

    def test_direct_flags_and_input_json_share_source_object_validation(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            repo = base / "repo"
            sentence = "A stable source object binds both capture inputs."
            article_path, article_locator, source_hash = make_source_object(
                repo,
                source_id="RAW-SHARED-001",
                slug="shared-source",
                sentences=[sentence],
            )
            direct_state = base / "direct-state"
            direct = [
                "capture", "--repo-root", str(repo), "--state-dir", str(direct_state),
                "--idempotency-key", "shared-direct", "--source-id", "RAW-SHARED-001",
                "--source-article", str(article_path), "--article-sha256", source_hash,
                "--sentence-id", "S01", "--source-sentence", sentence,
                "--occurred-at", f"{STUDY_DATE}T09:00:00Z",
            ]
            direct_code, direct_receipt, direct_error = run_cli(direct)
            self.assertEqual((direct_code, direct_error), (0, ""))
            assert direct_receipt is not None

            request = capture_request(
                key="shared-json",
                source_id="RAW-SHARED-001",
                sentence=sentence,
                first_translation=None,
            )
            request["article"]["source_article"] = article_locator
            request["article"]["source_hash"] = source_hash
            request["learning"]["user_evidence_verbatim"] = None
            input_path = base / "capture-request.json"
            input_path.write_text(json.dumps(request, ensure_ascii=False), encoding="utf-8")
            json_state = base / "json-state"
            json_code, json_receipt, json_error = run_cli(
                [
                    "capture", "--repo-root", str(repo), "--state-dir", str(json_state),
                    "--input-json", str(input_path),
                ]
            )
            self.assertEqual((json_code, json_error), (0, ""))
            assert json_receipt is not None
            direct_event = load_events(direct_state)[0]
            json_event = load_events(json_state)[0]
            self.assertEqual(direct_event["article"], json_event["article"])
            self.assertEqual(direct_event["article"]["source_article"], article_locator)
            for event, receipt in (
                (direct_event, direct_receipt),
                (json_event, json_receipt),
            ):
                self.assertEqual(event["article"]["source_hash"], source_hash)
                self.assertEqual(file_sha256(Path(receipt["event_path"])), receipt["event_sha256"])

            wrong_direct_state = base / "wrong-direct"
            wrong_direct = list(direct)
            wrong_direct[wrong_direct.index(str(direct_state))] = str(wrong_direct_state)
            wrong_direct[wrong_direct.index(source_hash)] = "0" * 64
            code, _, error = run_cli(wrong_direct)
            self.assertEqual(code, 2)
            self.assertIn("canonical article hash mismatch", error)
            self.assertFalse((wrong_direct_state / "events").exists())

            wrong_request = deepcopy(request)
            wrong_request["article"]["source_hash"] = "0" * 64
            wrong_input = base / "wrong-request.json"
            wrong_input.write_text(json.dumps(wrong_request, ensure_ascii=False), encoding="utf-8")
            wrong_json_state = base / "wrong-json"
            code, _, error = run_cli(
                [
                    "capture", "--repo-root", str(repo), "--state-dir", str(wrong_json_state),
                    "--input-json", str(wrong_input),
                ]
            )
            self.assertEqual(code, 2)
            self.assertIn("canonical article hash mismatch", error)
            self.assertFalse((wrong_json_state / "events").exists())

            outside_state = base / "outside-state"
            outside = list(direct)
            outside[outside.index(str(direct_state))] = str(outside_state)
            outside[outside.index(str(article_path))] = str(base / "outside.md")
            (base / "outside.md").write_text("outside", encoding="utf-8")
            code, _, error = run_cli(outside)
            self.assertEqual(code, 2)
            self.assertIn("repository file", error)
            self.assertFalse((outside_state / "events").exists())

            escape = repo / "articles" / "escape.md"
            escape.symlink_to(base / "outside.md")
            symlink_request = deepcopy(request)
            symlink_request["article"]["source_article"] = "articles/escape.md"
            symlink_input = base / "symlink-request.json"
            symlink_input.write_text(json.dumps(symlink_request, ensure_ascii=False), encoding="utf-8")
            symlink_state = base / "symlink-state"
            code, _, error = run_cli(
                [
                    "capture", "--repo-root", str(repo), "--state-dir", str(symlink_state),
                    "--input-json", str(symlink_input),
                ]
            )
            self.assertEqual(code, 2)
            self.assertIn("repository file", error)
            self.assertFalse((symlink_state / "events").exists())

    def test_historical_v1_and_v2_event_bytes_remain_unchanged(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            builders = [base / "builder-v1", base / "builder-v2"]
            requests = [capture_request(key="legacy-v1"), capture_request(key="legacy-v2")]
            fixtures: list[tuple[Path, bytes]] = []
            state = base / "state"
            for index, (builder, request) in enumerate(zip(builders, requests), start=1):
                receipt = append_event(builder, request)
                event = json.loads(Path(receipt["event_path"]).read_text(encoding="utf-8"))
                if index == 1:
                    event["schema_version"] = "english_capture_event_v1"
                    event.pop("observed_signals", None)
                    event.pop("source_signal_ids", None)
                    event.pop("capture_coverage", None)
                target = state / "events" / STUDY_DATE / f"{event['event_id']}.json"
                target.parent.mkdir(parents=True, exist_ok=True)
                raw = (json.dumps(event, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
                target.write_bytes(raw)
                fixtures.append((target, raw))
            replay = append_event(state, requests[1])
            self.assertEqual(replay["status"], "idempotent_noop")
            append_event(state, capture_request(key="new-after-history", sentence_id="S02"))
            for path, raw in fixtures:
                self.assertEqual(path.read_bytes(), raw)

    def test_capture_receipt_v2_schema_has_strict_stage_boundary(self) -> None:
        schema = json.loads(
            (REPO_ROOT / "schema/english_pipeline/capture-receipt-v2.schema.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            schema["properties"]["schema_version"]["const"],
            "english_capture_receipt_v2",
        )
        stages = schema["properties"]["stages"]
        self.assertFalse(stages["additionalProperties"])
        self.assertIs(stages["properties"]["event_written"]["const"], True)
        self.assertIs(stages["properties"]["schema_validated"]["const"], True)
        self.assertIs(stages["properties"]["dispatcher_accepted"]["const"], False)
        self.assertIs(stages["properties"]["package_visible"]["const"], False)
        self.assertEqual(
            schema["properties"]["projection_status"]["enum"],
            ["pending", "rendered", "failed"],
        )

    def test_pending_rendered_and_failed_capture_receipts_match_v2_schema(self) -> None:
        schema = json.loads(
            (REPO_ROOT / "schema/english_pipeline/capture-receipt-v2.schema.json").read_text(
                encoding="utf-8"
            )
        )
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            repo = base / "repo"
            sentence = "A pending receipt remains valid before projection finishes."
            article_path, _, source_hash = make_source_object(
                repo,
                source_id="RAW-RECEIPT-001",
                slug="receipt-source",
                sentences=[sentence],
            )

            def argv_for(state: Path, key: str) -> list[str]:
                return [
                    "capture", "--repo-root", str(repo), "--state-dir", str(state),
                    "--idempotency-key", key, "--source-id", "RAW-RECEIPT-001",
                    "--source-article", str(article_path), "--article-sha256", source_hash,
                    "--sentence-id", "S01", "--source-sentence", sentence,
                    "--occurred-at", f"{STUDY_DATE}T09:00:00Z",
                ]

            rendered_state = base / "rendered-state"
            pending_receipts: list[dict] = []

            def inspect_pending_then_render(*args, **kwargs):
                paths = list((rendered_state / "receipts" / "capture").glob("*/*.json"))
                self.assertEqual(len(paths), 1)
                pending = json.loads(paths[0].read_text(encoding="utf-8"))
                assert_json_schema_subset(self, pending, schema)
                pending_receipts.append(pending)
                return write_quick_capture_view(*args, **kwargs)

            with patch(
                "english_pipeline.cli.write_quick_capture_view",
                side_effect=inspect_pending_then_render,
            ):
                code, rendered, stderr = run_cli(argv_for(rendered_state, "receipt-rendered"))
            self.assertEqual((code, stderr), (0, ""))
            assert rendered is not None
            self.assertEqual(len(pending_receipts), 1)
            pending = pending_receipts[0]
            self.assertEqual(pending["projection_status"], "pending")
            self.assertIsNone(pending["view_path"])
            self.assertIsNone(pending["view_sha256"])
            self.assertIsNone(pending["projection_error"])
            self.assertEqual(pending["formal_write_count"], 0)
            assert_json_schema_subset(self, rendered, schema)
            self.assertEqual(rendered["projection_status"], "rendered")
            self.assertEqual(rendered["formal_write_count"], 0)
            persisted_rendered = json.loads(
                Path(rendered["receipt_path"]).read_text(encoding="utf-8")
            )
            assert_json_schema_subset(self, persisted_rendered, schema)

            failed_state = base / "failed-state"
            with patch(
                "english_pipeline.cli.write_quick_capture_view",
                side_effect=RuntimeError("fixture projection failure"),
            ):
                code, failed, stderr = run_cli(argv_for(failed_state, "receipt-failed"))
            self.assertEqual((code, stderr), (2, ""))
            assert failed is not None
            assert_json_schema_subset(self, failed, schema)
            self.assertEqual(failed["status"], "capture_saved_but_projection_failed")
            self.assertEqual(failed["projection_status"], "failed")
            self.assertIsNone(failed["view_path"])
            self.assertIsNone(failed["view_sha256"])
            self.assertEqual(failed["projection_error"], "fixture projection failure")
            self.assertEqual(failed["formal_write_count"], 0)
            persisted_failed = json.loads(
                Path(failed["receipt_path"]).read_text(encoding="utf-8")
            )
            assert_json_schema_subset(self, persisted_failed, schema)

    def test_quick_capture_projection_binding_is_order_independent(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            append_event(state, capture_request(key="binding-a"))
            second = capture_request(key="binding-b", sentence_id="S02")
            second["source"]["source_sentence"] = "Another real sentence is captured."
            second["source"]["sentence_hash"] = sentence_sha256(
                second["source"]["source_sentence"]
            )
            append_event(state, second)
            events = load_events(state)
            forward = quick_capture_projection_binding(
                events, article_id="RAW-TEST-001", study_date=STUDY_DATE
            )
            reverse = quick_capture_projection_binding(
                list(reversed(events)),
                article_id="RAW-TEST-001",
                study_date=STUDY_DATE,
            )
            self.assertEqual(forward, reverse)
            self.assertEqual(forward["effective_event_count"], 2)
            self.assertRegex(
                forward["effective_event_high_water_sha256"], r"^[0-9a-f]{64}$"
            )

    def test_complete_article_exports_abc_without_formal_writes(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            repo = base / "repo"
            state = base / "intake"
            make_formal_repo(repo)
            append_event(state, capture_request(candidates=[word_candidate()], evidence_states=["unknown_observed"]))
            before = formal_hashes(repo)
            receipt = complete_article(
                state,
                repo,
                article_id="RAW-TEST-001",
                idempotency_key="complete-1",
                study_date=STUDY_DATE,
            )
            self.assertEqual(receipt["source_id"], "RAW-TEST-001")
            self.assertEqual(receipt["tier_counts"], {"A": 1, "B": 0, "C": 0})
            self.assertTrue(receipt["formal_sources_unchanged"])
            self.assertEqual(before, formal_hashes(repo))
            self.assertTrue(Path(receipt["export_json"]).is_file())

    def test_capture_p95_is_bounded_in_temp_state(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            durations: list[float] = []
            for index in range(40):
                request = capture_request(
                    key=f"perf-{index}",
                    sentence_id=f"S{index + 1:02d}",
                    sentence=f"Real fixture sentence number {index} remains answer safe.",
                )
                started = time.perf_counter()
                append_event(state, request)
                durations.append(time.perf_counter() - started)
            p95 = sorted(durations)[int(len(durations) * 0.95) - 1]
            self.assertLess(p95, 0.5, f"capture p95 too high: {p95:.6f}s")

    def test_post_threshold_event_writes_release_neutral_binding_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            request = capture_request(key="producer-attestation")
            request["occurred_at"] = "2026-08-17T07:00:00+00:00"
            receipt = append_event(state, request)
            self.assertEqual(receipt["producer_binding_status"], "attested")
            self.assertRegex(
                receipt["producer_binding_attestation_sha256"],
                r"^[0-9a-f]{64}$",
            )
            sidecar = Path(receipt["producer_binding_attestation_path"])
            self.assertTrue(sidecar.is_file())
            self.assertTrue(sidecar.is_relative_to(Path(folder).resolve()))
            value = json.loads(sidecar.read_text(encoding="utf-8"))
            self.assertEqual(value["capture_id"], receipt["capture_id"])
            self.assertEqual(value["capture_content_sha256"], receipt["event_sha256"])
            self.assertEqual(value["formal_write_count"], 0)
            self.assertFalse(
                {"release_id", "activation_id", "dispatcher_authority", "mcp_authority"}
                & set(value)
            )


class ReviewStatusPolicyTests(unittest.TestCase):
    def _bank(self) -> list[dict]:
        return [{"id": "20260701-001", "item": "lasting"}]

    def test_exact_token_span_rejects_substring(self) -> None:
        self.assertEqual(exact_item_occurrences("This article lasts.", "art"), [])
        match = exact_item_occurrences("A lasting change.", "lasting")
        self.assertEqual([(row["start"], row["end"]) for row in match], [(2, 9)])

    def test_same_day_explicit_unknown_dominates_later_nonreport(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            state = Path(name)
            first = capture_request(
                key="day-first",
                sentence_id="S01",
                candidates=[word_candidate("lasting")],
                evidence_states=["unknown_observed"],
            )
            second = capture_request(
                key="day-second",
                sentence_id="S02",
                sentence="The effect may be lasting.",
                first_translation="这种影响可能会持续。",
                candidates=[],
                evidence_states=[],
            )
            second["occurred_at"] = f"{STUDY_DATE}T10:00:00Z"
            append_event(state, first)
            append_event(state, second)
            proposals = build_review_status_proposals(
                load_events(state), self._bank(), study_date=STUDY_DATE
            )
            self.assertEqual(proposals["daily_explicit_unknown_terms"], ["lasting"])
            self.assertEqual(proposals["review_exclusion_proposals"], [])

    def test_previous_mastery_is_reactivated_after_later_failure(self) -> None:
        with tempfile.TemporaryDirectory() as name:
            state = Path(name)
            append_event(
                state,
                capture_request(
                    key="reactivate",
                    candidates=[word_candidate("lasting")],
                    evidence_states=["unknown_observed"],
                ),
            )
            proposals = build_review_status_proposals(
                load_events(state),
                self._bank(),
                study_date=STUDY_DATE,
                current_status={"lasting": {"status": "mastered_sentence_nonreport"}},
            )
            self.assertEqual(len(proposals["reactivation_proposals"]), 1)
            self.assertEqual(
                proposals["reactivation_proposals"][0]["reason"],
                "explicit_unknown_or_mistranslated_after_mastery",
            )

    def test_sol_confirmed_exclusion_and_reactivation_change_selector_only(self) -> None:
        evidence = [{"capture_event_id": "EVT-1", "spans": [{"start": 2, "end": 9}]}]
        excluded = append_review_status_record(
            [],
            event_type="exclude_from_review",
            item="lasting",
            bank_id="20260701-001",
            study_date=STUDY_DATE,
            source_capture_event_ids=["EVT-1"],
            sentence_evidence=evidence,
            reason="appeared_in_sentence_but_not_reported_unknown",
        )
        self.assertEqual(eligible_review_bank_rows(self._bank(), [excluded]), [])
        reactivated = append_review_status_record(
            [excluded],
            event_type="reactivate_for_review",
            item="lasting",
            bank_id="20260701-001",
            study_date=STUDY_DATE,
            source_capture_event_ids=["EVT-2"],
            sentence_evidence=evidence,
            reason="explicit_unknown_or_mistranslated_after_mastery",
        )
        self.assertEqual(
            effective_review_status([excluded, reactivated])["lasting"]["status"],
            "unmastered_reactivated",
        )
        self.assertEqual(len(eligible_review_bank_rows(self._bank(), [excluded, reactivated])), 1)


class CandidateTests(unittest.TestCase):
    def test_study_date_uses_asia_shanghai_and_rejects_cross_midnight_package(self) -> None:
        self.assertEqual(parse_iso_date("2026-08-05T15:59:59Z"), "2026-08-05")
        self.assertEqual(parse_iso_date("2026-08-05T16:00:00Z"), "2026-08-06")
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            request = capture_request()
            request["occurred_at"] = "2026-08-05T16:00:00Z"
            append_event(state, request)
            event = load_events(state)[0]
            candidate = luna_candidate(event, items=[])
            with self.assertRaises(ValidationError):
                validate_luna_candidate(candidate, state_dir=state)
    def test_lasting_golden_renderer(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            append_event(state, capture_request(candidates=[word_candidate()], evidence_states=["unknown_observed"]))
            event = load_events(state)[0]
            candidate = luna_candidate(event, items=[lasting_item(event)])
            validate_luna_candidate(candidate, state_dir=state)
            rendered = render_luna_candidate(candidate)
            golden = (Path(__file__).parent / "golden" / "lasting.md").read_text(encoding="utf-8")
            self.assertEqual(rendered, golden)

    def test_mastery_gate_requires_independent_live_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            append_event(state, capture_request(evidence_states=["unknown_observed"]))
            event = load_events(state)[0]
            item = lasting_item(event)
            item["candidate_status"] = "mastery_candidate"
            item["mastered_status"] = "mastery_proposed"
            item["mastery_proposal"] = {
                "evidence_kind": "independent_correct_use",
                "evidence_sentence": "I used lasting correctly.",
                "evidence_context": "transfer check",
                "proof_note": "independent",
            }
            with self.assertRaises(ValidationError):
                validate_luna_candidate(luna_candidate(event, items=[item]), state_dir=state)
            item["evidence_states"] = ["independent_correct_use"]
            item["evidence_origin"] = "synthetic_fixture"
            with self.assertRaises(ValidationError):
                validate_luna_candidate(luna_candidate(event, items=[item]), state_dir=state)

    def test_zero_item_luna_package_is_valid(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            append_event(state, capture_request())
            candidate = luna_candidate(load_events(state)[0], items=[])
            result = validate_luna_candidate(candidate, state_dir=state)
            self.assertEqual(result["item_count"], 0)

    def test_runtime_identity_unverified_is_eligible_but_mismatch_and_unavailable_block(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            state = Path(folder) / "intake"
            append_event(state, capture_request())
            event = load_events(state)[0]
            unverified = luna_candidate(event, items=[])
            unverified["runtime_identity"] = {
                "requested": {"model": "gpt-5.6-luna", "reasoning_effort": "max"},
                "observed": {"model": "gpt-5.6-luna", "reasoning_effort": "max"},
                "provenance": {"source": "request_only", "reference": None, "observed_at": None},
                "status": "requested_unverified",
            }
            validate_luna_candidate(unverified, state_dir=state)

            mismatch = luna_candidate(event, items=[])
            mismatch["runtime_identity"] = {
                "requested": {"model": "gpt-5.6-luna", "reasoning_effort": "max"},
                "observed": {"model": "another-model", "reasoning_effort": "max"},
                "provenance": {"source": "process_metadata", "reference": "pid:fixture", "observed_at": f"{STUDY_DATE}T12:00:00Z"},
                "status": "mismatch",
            }
            with self.assertRaises(ValidationError):
                validate_luna_candidate(mismatch, state_dir=state)
            mismatch["validation"] = {"status": "BLOCKED", "blockers": ["runtime identity mismatch"]}
            validate_luna_candidate(mismatch, state_dir=state)

            unavailable = luna_candidate(event, items=[])
            unavailable["runtime_identity"] = {
                "requested": {"model": "gpt-5.6-luna", "reasoning_effort": "max"},
                "observed": {"model": "unknown", "reasoning_effort": "unknown"},
                "provenance": {"source": "none", "reference": None, "observed_at": None},
                "status": "unavailable",
            }
            unavailable["validation"] = {"status": "BLOCKED", "blockers": ["runtime identity unavailable"]}
            validate_luna_candidate(unavailable, state_dir=state)


class NightlyWriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.repo = self.base / "repo"
        self.state = self.base / "intake"
        make_formal_repo(self.repo)
        append_event(self.state, capture_request(candidates=[word_candidate()], evidence_states=["unknown_observed"]))
        self.event = load_events(self.state)[0]

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_candidate(self, *, items: list[dict] | None = None) -> Path:
        candidate = luna_candidate(self.event, items=items if items is not None else [lasting_item(self.event)])
        path = self.state / "candidates" / STUDY_DATE / f"{candidate['candidate_id']}.json"
        atomic_write_json(path, candidate)
        return path

    def freeze(self, *, items: list[dict] | None = None) -> Path:
        candidate_path = self.write_candidate(items=items)
        path, manifest = freeze_nightly(self.state, self.repo, study_date=STUDY_DATE, candidate_paths=[candidate_path])
        self.assertEqual(manifest["status"], "frozen")
        return path

    def write_actions(self, manifest_path: Path, actions: list[dict], unresolved: list[dict] | None = None) -> Path:
        payload = sol_actions(manifest_path, self.event["event_id"], actions=actions, unresolved=unresolved)
        path = manifest_path.with_suffix(".actions.json")
        atomic_write_json(path, payload)
        return path

    def test_freeze_distinguishes_needs_preprocess_noop_and_zero_item_coverage(self) -> None:
        _, missing = freeze_nightly(self.state, self.repo, study_date=STUDY_DATE)
        self.assertEqual(missing["status"], "needs_preprocess")
        self.assertEqual(missing["uncovered_capture_event_ids"], [self.event["event_id"]])
        candidate_path = self.write_candidate(items=[])
        _, covered = freeze_nightly(self.state, self.repo, study_date=STUDY_DATE, candidate_paths=[candidate_path])
        self.assertEqual(covered["status"], "frozen")
        self.assertEqual(covered["candidate_documents"][0]["runtime_identity_status"], "confirmed")
        self.assertEqual(covered["runtime_identity_summary"]["confirmed"], ["LUNA-20260805-001"])
        self.assertEqual(covered["runtime_identity_summary"]["requested_unverified"], [])
        self.assertEqual(covered["source_snapshot"]["sentence_hashes"][self.event["event_id"]], self.event["source"]["sentence_hash"])
        _, noop = freeze_nightly(self.state, self.repo, study_date="2026-08-04")
        self.assertEqual(noop["status"], "NOOP")

    def test_dry_run_partial_and_zero_formal_writes(self) -> None:
        manifest = self.freeze()
        unresolved = [{
            "action_id": "UNRES-1", "status": "needs_user", "reason": "sense merge is ambiguous",
            "source_capture_event_ids": [self.event["event_id"]], "target": "master_bank", "item": "ambiguous",
        }]
        actions = self.write_actions(manifest, [master_insert_action(self.event["event_id"])], unresolved)
        before = formal_hashes(self.repo)
        _, receipt = apply_nightly(self.state, self.repo, manifest_path=manifest, actions_path=actions)
        self.assertEqual(receipt["status"], "DRY_RUN_PARTIAL")
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertFalse(receipt["formal_files_changed"])
        self.assertEqual(before, formal_hashes(self.repo))
        self.assertEqual({row["result"] for row in receipt["action_results"]}, {"would_apply", "needs_user"})

    def test_prehash_drift_fails_closed(self) -> None:
        manifest = self.freeze()
        actions = self.write_actions(manifest, [master_insert_action(self.event["event_id"])])
        with (self.repo / "bank" / "master_bank.csv").open("a", encoding="utf-8") as handle:
            handle.write("\n")
        _, receipt = apply_nightly(self.state, self.repo, manifest_path=manifest, actions_path=actions)
        self.assertEqual(receipt["status"], "CAS_CONFLICT")
        self.assertEqual(receipt["formal_write_count"], 0)

    def test_idempotent_update_is_skipped(self) -> None:
        manifest = self.freeze()
        update = {
            "action_id": "ACT-UPDATE", "action_type": "master_bank_update", "reason": "no change",
            "source_capture_event_ids": [self.event["event_id"]], "record_id": "20260805-001",
            "updates": {"review_note": "stable"},
        }
        actions = self.write_actions(manifest, [update])
        _, receipt = apply_nightly(self.state, self.repo, manifest_path=manifest, actions_path=actions)
        self.assertEqual(receipt["status"], "DRY_RUN_NO_ACTION")
        self.assertEqual(receipt["action_results"][0]["result"], "skipped")

    def test_mastery_action_requires_frozen_mastery_candidate(self) -> None:
        manifest = self.freeze()
        mastered = {
            "action_id": "ACT-MASTERED", "action_type": "mastered_insert", "reason": "claim",
            "source_capture_event_ids": [self.event["event_id"]], "mastery_evidence_kind": "independent_correct_use",
            "row": {"item": "lasting", "matched_id": "", "matched_type": "单词", "mastered_date": STUDY_DATE,
                    "evidence_sentence": "I used lasting.", "evidence_context": "transfer", "proof_note": "independent"},
        }
        actions = self.write_actions(manifest, [mastered])
        with self.assertRaises(ValidationError):
            apply_nightly(self.state, self.repo, manifest_path=manifest, actions_path=actions)

    def test_crash_recovery_rolls_back_temp_formal_copy(self) -> None:
        manifest = self.freeze()
        actions = self.write_actions(
            manifest,
            [master_insert_action(self.event["event_id"]), sp_action(self.event["event_id"])],
        )
        before = formal_hashes(self.repo)
        batch_id = json.loads(manifest.read_text(encoding="utf-8"))["batch_id"]
        with self.assertRaises(SimulatedWriterCrash):
            apply_nightly(
                self.state,
                self.repo,
                manifest_path=manifest,
                actions_path=actions,
                apply=True,
                authorization=batch_id,
                fault_after_replacements=1,
            )
        result = recover_nightly(self.state, self.repo)
        self.assertEqual(result["status"], "PASS")
        self.assertIn("RECOVERED_ROLLBACK", {row["status"] for row in result["recovered"]})
        self.assertEqual(before, formal_hashes(self.repo))

    def test_successful_temp_apply_allocates_ids_and_receipts(self) -> None:
        manifest = self.freeze()
        actions = self.write_actions(
            manifest,
            [master_insert_action(self.event["event_id"]), sp_action(self.event["event_id"])],
        )
        before = formal_hashes(self.repo)
        batch_id = json.loads(manifest.read_text(encoding="utf-8"))["batch_id"]
        receipt_path, receipt = apply_nightly(
            self.state,
            self.repo,
            manifest_path=manifest,
            actions_path=actions,
            apply=True,
            authorization=batch_id,
        )
        self.assertEqual(receipt["status"], "APPLIED")
        self.assertEqual(receipt["formal_write_count"], 2)
        self.assertTrue(receipt["formal_files_changed"])
        self.assertNotEqual(before, receipt["formal_posthashes"])
        self.assertTrue(receipt_path.is_file())
        assigned = {row.get("assigned_id") for row in receipt["action_results"]}
        self.assertIn("20260805-002", assigned)
        self.assertIn("SP-001", assigned)
        transaction = next((self.state / "nightly").glob("*/*/transactions/*/transaction.json"))
        self.assertEqual(json.loads(transaction.read_text(encoding="utf-8"))["status"], "closed")

    def test_duplicate_insert_after_apply_is_idempotently_skipped(self) -> None:
        first_manifest = self.freeze()
        first_actions = self.write_actions(first_manifest, [master_insert_action(self.event["event_id"])])
        first_batch = json.loads(first_manifest.read_text(encoding="utf-8"))["batch_id"]
        apply_nightly(self.state, self.repo, manifest_path=first_manifest, actions_path=first_actions, apply=True, authorization=first_batch)
        second_manifest, _ = freeze_nightly(
            self.state,
            self.repo,
            study_date=STUDY_DATE,
            candidate_paths=[self.state / "candidates" / STUDY_DATE / "LUNA-20260805-001.json"],
        )
        second_actions = self.write_actions(second_manifest, [master_insert_action(self.event["event_id"])])
        _, receipt = apply_nightly(self.state, self.repo, manifest_path=second_manifest, actions_path=second_actions)
        self.assertEqual(receipt["status"], "DRY_RUN_NO_ACTION")
        self.assertEqual(receipt["action_results"][0]["result"], "skipped")

    def test_committed_without_receipt_requires_recovery_and_rolls_back(self) -> None:
        manifest = self.freeze()
        actions = self.write_actions(
            manifest,
            [master_insert_action(self.event["event_id"]), sp_action(self.event["event_id"])],
        )
        before = formal_hashes(self.repo)
        batch_id = json.loads(manifest.read_text(encoding="utf-8"))["batch_id"]
        with self.assertRaises(SimulatedWriterCrash):
            apply_nightly(
                self.state,
                self.repo,
                manifest_path=manifest,
                actions_path=actions,
                apply=True,
                authorization=batch_id,
                fault_after_commit=True,
            )
        with self.assertRaisesRegex(ValidationError, "recovery_required"):
            apply_nightly(self.state, self.repo, manifest_path=manifest, actions_path=actions)
        recovery = recover_nightly(self.state, self.repo)
        self.assertTrue(Path(recovery["receipt_path"]).is_file())
        self.assertEqual(before, formal_hashes(self.repo))

    def test_writer_lock_serializes_competing_dry_run(self) -> None:
        manifest = self.freeze()
        actions = self.write_actions(manifest, [master_insert_action(self.event["event_id"])])
        command = [
            sys.executable,
            str(REPO_ROOT / "scripts/english_learning_pipeline.py"),
            "apply-nightly",
            "--state-dir",
            str(self.state),
            "--repo-root",
            str(self.repo),
            "--manifest",
            str(manifest),
            "--actions",
            str(actions),
            "--dry-run",
        ]
        with exclusive_lock(self.state / "locks" / "formal.lock"):
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            time.sleep(0.15)
            self.assertIsNone(process.poll(), "writer did not wait for formal lock")
        stdout, stderr = process.communicate(timeout=5)
        self.assertEqual(process.returncode, 0, stderr)
        self.assertEqual(json.loads(stdout)["status"], "DRY_RUN_VALID")

    def test_sentence_pattern_merge_first_and_idempotent(self) -> None:
        first_manifest = self.freeze()
        first_actions = self.write_actions(first_manifest, [sp_action(self.event["event_id"])])
        first_batch = json.loads(first_manifest.read_text(encoding="utf-8"))["batch_id"]
        apply_nightly(self.state, self.repo, manifest_path=first_manifest, actions_path=first_actions, apply=True, authorization=first_batch)
        candidate_path = self.state / "candidates" / STUDY_DATE / "LUNA-20260805-001.json"
        second_manifest, _ = freeze_nightly(self.state, self.repo, study_date=STUDY_DATE, candidate_paths=[candidate_path])
        duplicate_append = self.write_actions(second_manifest, [sp_action(self.event["event_id"])])
        with self.assertRaisesRegex(ValidationError, "must use sentence_pattern_merge"):
            apply_nightly(self.state, self.repo, manifest_path=second_manifest, actions_path=duplicate_append)
        merge = {
            "action_id": "ACT-SP-MERGE",
            "action_type": "sentence_pattern_merge",
            "reason": "merge source evidence before creating a duplicate",
            "source_capture_event_ids": [self.event["event_id"]],
            "existing_sp_id": "SP-001",
            "additions": {"来源与示例": ["Second real source.（fixture，2026-08-05）"], "常用变体": ["recognize that"], "相关词汇/搭配": ["lasting trust"], "use_count_increment": 0, "last_used": STUDY_DATE},
        }
        merge_actions = self.write_actions(second_manifest, [merge])
        second_batch = json.loads(second_manifest.read_text(encoding="utf-8"))["batch_id"]
        _, applied = apply_nightly(self.state, self.repo, manifest_path=second_manifest, actions_path=merge_actions, apply=True, authorization=second_batch)
        self.assertEqual(applied["action_results"][0]["result"], "applied")
        third_manifest, _ = freeze_nightly(self.state, self.repo, study_date=STUDY_DATE, candidate_paths=[candidate_path])
        third_actions = self.write_actions(third_manifest, [merge])
        _, replay = apply_nightly(self.state, self.repo, manifest_path=third_manifest, actions_path=third_actions)
        self.assertEqual(replay["action_results"][0]["result"], "skipped")


class FormalContractTests(unittest.TestCase):
    def test_live_sentence_patterns_have_heading_plus_fourteen_fields(self) -> None:
        snapshot = formal_snapshot(REPO_ROOT)
        self.assertEqual(snapshot["sentence_patterns"]["field_count"], 15)
        # The formal library is append-only.  Its exact content is protected by
        # the release formal-surface hash guard, while this schema test only
        # enforces that the previously accepted corpus has not shrunk.
        self.assertGreaterEqual(snapshot["sentence_patterns"]["card_count"], 22)


class ShadowFixtureTests(unittest.TestCase):
    def test_three_isolated_articles_twenty_sentences_temp_state_only(self) -> None:
        stats = {"articles": 0, "sentences": 0, "candidates": 0, "failures": 0}
        with tempfile.TemporaryDirectory() as folder:
            base = Path(folder)
            repo = base / "repo"
            state = base / "intake"
            sentence_groups = [
                [f"Article one sentence {index} preserves source evidence." for index in range(1, 8)],
                [f"Article two sentence {index} remains practice safe." for index in range(1, 8)],
                [f"Article three sentence {index} uses isolated text only." for index in range(1, 7)],
            ]
            for article_index, sentences in enumerate(sentence_groups, start=1):
                source_id = f"RAW-SHADOW-{article_index:03d}"
                article_path, article_locator, source_hash = make_source_object(
                    repo,
                    source_id=source_id,
                    slug=f"shadow-{article_index}",
                    sentences=sentences,
                )
                stats["articles"] += 1
                for index, sentence in enumerate(sentences, start=1):
                    candidates = [word_candidate(re.findall(r"[A-Za-z]+", sentence)[0])] if stats["sentences"] % 4 == 0 else []
                    argv = [
                        "capture", "--repo-root", str(repo), "--state-dir", str(state),
                        "--idempotency-key", f"shadow-{source_id}-{index}",
                        "--source-id", source_id, "--source-article", str(article_path),
                        "--article-sha256", source_hash, "--sentence-id", f"S{index:02d}",
                        "--source-sentence", sentence, "--evidence-origin", "synthetic_fixture",
                        "--user-evidence", "synthetic_fixture",
                        "--occurred-at", f"{STUDY_DATE}T09:00:00Z",
                    ]
                    if candidates:
                        argv.extend(["--candidate-json", json.dumps(candidates[0], ensure_ascii=False)])
                    code, receipt, _ = run_cli(argv)
                    if code == 0 and receipt is not None:
                        stats["sentences"] += 1
                        stats["candidates"] += len(candidates)
                        event = next(
                            row
                            for row in load_events(state)
                            if row["event_id"] == receipt["capture_id"]
                        )
                        self.assertEqual(event["article"]["source_article"], article_locator)
                        self.assertEqual(event["article"]["source_hash"], source_hash)
                        self.assertEqual(receipt["formal_write_count"], 0)
                        self.assertEqual(receipt["formal_writeback"], "none")
                    else:
                        stats["failures"] += 1
            self.assertEqual(stats, {"articles": 3, "sentences": 20, "candidates": 5, "failures": 0})
            self.assertEqual(len(list((state / "events").glob("*/*.json"))), 20)
            self.assertFalse((repo / "bank").exists())


if __name__ == "__main__":
    unittest.main()
