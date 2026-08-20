from __future__ import annotations

import csv
import hashlib
import json
import os
import sqlite3
from pathlib import Path

from study_read_mcp.config import RepositoryConfig


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def make_fixture(base: Path) -> RepositoryConfig:
    math = base / "math"
    card = math / "错题知识网络/错题卡/GS-001_fixture.md"
    write_text(card, """---
id: GS-001
title: Fixture card
subject: 高等数学
chapter: 极限
status: 待复做
knowledge:
- 极限
methods:
- 定义检查
related:
- GS-002
wrong_point: Fixture wrong point
error_causes:
- condition
method_gap:
  enabled: true
  method_trigger: Fixture trigger
  expected_first_action: Fixture action
  missed_action: Fixture missed
  confidence: high
  answer: SHOULD_NEVER_APPEAR
answer: SHOULD_NEVER_APPEAR
---
# Fixture
## 标准答案
SHOULD_NEVER_APPEAR
""")
    write_json(math / "错题知识网络/生成/wrong_questions.json", {
        "updated_at": "2026-08-07", "cards": [],
        "similarities": [{"a": "GS-001", "b": "GS-002", "level": "direct", "score": 0.8, "reasons": ["declared"], "source": "fixture"}],
        "weak_relations": [{"a": "GS-001", "b": "GS-003", "level": "weak", "score": 0.2, "reasons": ["candidate"], "source": "fixture"}],
    })
    write_text(math / "错题知识网络/知识点库.md", """# 知识点库

## 高等数学

- 极限

## 线性代数

- 矩阵

## 概率论与数理统计

- 随机变量

## 错因标签

- condition

## 方法标签

- 定义检查

## 专题链标签

- 极限专题

## 陷阱标签

- 条件遗漏

## 历史正式卡已用细分知识标签（2026-07-10 补登记）

- 历史细分
""")
    write_text(math / "数学一回滚复习系统/快速入库事件.jsonl", json.dumps({
        "event_id": "MFI-1", "event_type": "capture", "study_date": "2026-08-07",
        "target": {"formal_id": "GS-001"}, "initial_state": "pending_nightly",
        "evidence": {"result": "wrong", "mastery_score": 2, "private": "SHOULD_NEVER_APPEAR"},
    }, ensure_ascii=False) + "\n")
    write_text(math / "数学一回滚复习系统/复习记录.jsonl", json.dumps({
        "event_id": "SCORE-1", "date": "2026-08-07", "delivered_card_id": "GS-001",
        "anchor_card_id": "GS-002", "score": 2, "attempt_type": "review", "match_mode": "exact",
        "advances_long_term": False, "delivered_evidence_snapshot": {"answer": "SHOULD_NEVER_APPEAR"},
    }, ensure_ascii=False) + "\n")
    write_text(math / "数学一回滚复习系统/学习前5题记录.jsonl", "{}\n")

    cs408 = base / "cs408"
    write_text(cs408 / "节点总表.md", """# 节点总表

|ID|来源ID|年份|科目|主模块|主知识点|副知识点|命中知识点|题型|核心考点|模糊概念|错因标签|首次做题日期|最近复做日期|最近错误记录|详情入口|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|DS_2023_002|SRC-1|2023|数据结构|DS01 基本概念|DS01-01 测试知识|DS01-02 相邻知识|DS01-01 测试知识；DS01-02 相邻知识|选择题|fixture core|fixture ambiguity|E01 fixture|2026-08-07|2026-08-07|fixture error|fixture detail|
|DS_2023_003|SRC-2|2023|数据结构|DS01 基本概念|DS01-02 相邻知识||DS01-02 相邻知识|选择题|fixture neighbor|fixture ambiguity|E01 fixture|2026-08-07|2026-08-07|fixture error|fixture detail|
""")
    write_text(cs408 / "知识点标签表.md", """# 408 知识点标签表

# 一、数据结构 DS

## DS01 基本概念

- DS01-01 测试知识
- DS01-02 相邻知识
""")
    write_text(cs408 / "关系规则.md", """# 关系规则

## R01 同一核心考点
## R02 同一题型模板
## R03 同一错因标签
## R04 同一模糊概念
## R05 易混概念对比
## R06 上下游知识链
## R07 跨科目相似思想
## R08 同年份同模块
## R09 高频考点聚类
""")
    write_text(cs408 / "关系边表.md", """# 关系边表

|起点ID|终点ID|关系类型|联系强度|关联原因|复盘优先级|
|---|---|---|---|---|---|
|DS_2023_002|DS_2023_003|R06 上下游知识链|中|fixture directed relation|中|
""")
    vault = cs408 / "wiki/study_vaults/408-full"
    write_json(vault / "manifest.json", {"kind": "fixture", "concept_count": 1})
    write_text(vault / "StudyVault/01-DS/DS01-01-测试.md", """---
keywords: fixture, concept
---
# DS01-01 测试
| 所属科目 | 数据结构 |
## Related Notes
- [[DS01-02-相邻]]
> 不包含原题答案
""")
    write_json(vault / "state/intake-curation/state.json", {
        "schema": "fixture", "event_count": 1, "batches": {},
        "captures": {"CAP-20260807-001": {"capture_id": "CAP-20260807-001", "study_date": "2026-08-07", "formal_id": "DS_2023_002", "quality_status": "pending", "formalization_authorized": False, "payload_sha256": "0" * 64}},
        "pending_by_date": {"2026-08-07": ["CAP-20260807-001"]},
    })
    write_text(vault / "state/intake-curation/events.jsonl", "{}\n")
    hot = vault / "state/review-loop/hot-state"
    build = "RHS-AABBCCDD"
    write_json(hot / "manifest.json", {"projection_build_id": build, "manifest_sha256": "1" * 64, "formal_write_count": 0, "answer_safe": True})
    review_event = {
        "event_id": "E-1", "event_kind": "answer", "session_id": "S-1",
        "item_id": "I-1", "identity": "RID-1", "observed_date": "2026-08-07",
        "evidence_kind": "event",
    }
    review_line = json.dumps(review_event, ensure_ascii=False, separators=(",", ":")) + "\n"
    write_text(vault / "state/review-loop/events.jsonl", review_line)
    db = hot / "databases" / f"{build}.sqlite3"
    db.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db)
    connection.execute("CREATE TABLE events(event_id TEXT,idempotency_key TEXT,event_kind TEXT,source TEXT,session_id TEXT,item_id TEXT,identity TEXT,observed_date TEXT,evidence_kind TEXT,event_json TEXT,event_sha256 TEXT,line_sha256 TEXT,byte_offset INTEGER,byte_length INTEGER,line_number INTEGER)")
    connection.execute("INSERT INTO events(event_id,event_kind,session_id,item_id,identity,observed_date,evidence_kind,byte_offset,byte_length,line_number) VALUES(?,?,?,?,?,?,?,?,?,?)", ("E-1", "answer", "S-1", "I-1", "RID-1", "2026-08-07", "event", 0, len(review_line.encode("utf-8")), 1))
    connection.commit()
    connection.close()
    write_json(vault / "state/morning-review/MR-2026-08-07-fixture/state.json", {"session_id": "MR-2026-08-07-fixture", "status": "active", "review_date": "2026-08-07", "item_order": ["PRIVATE-1", "PRIVATE-2"], "items": {"PRIVATE-1": {"answer": "SHOULD_NEVER_APPEAR"}}})

    english = base / "english"
    write_json(english / "raw/articles/exam-reading-corpus/2020/text-1.json", {
        "schema": "exam-reading-corpus-v1",
        "source_id": "RAW-ARTICLE-CORPUS",
        "reference_id": "EXAM-READING-2020-T1",
        "visibility": "practice_safe",
        "passage_paragraphs": ["A complete practice-safe corpus paragraph."],
        "questions": [],
    })
    write_json(english / "raw/articles/2026-08-07-fixture/pack/dataset/pipeline_handoff.json", {
        "schema_version": "english-learning-pipeline-handoff-v1", "source_id": "RAW-ARTICLE-FIXTURE",
        "source_hash": "sha256:" + "2" * 64,
        "units": [
            {"sentence_id": "S01", "source_kind": "article", "source_sentence": "A safe sentence.", "sentence_hash": "sha256:" + "3" * 64},
            {"sentence_id": "Q01", "source_kind": "question", "source_sentence": "SHOULD_NEVER_APPEAR", "sentence_hash": "sha256:" + "4" * 64},
        ],
        "questions": [{"correct_answer": "SHOULD_NEVER_APPEAR"}],
    })
    bank = english / "bank/master_bank.csv"
    bank.parent.mkdir(parents=True, exist_ok=True)
    with bank.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["id", "date", "type", "item", "source_article", "source_sentence", "meaning", "usage", "writing_value", "tags", "review_note", "appear_count", "last_seen"])
        writer.writeheader()
        writer.writerow({"id": "V-1", "date": "2026-08-07", "type": "word", "item": "safe", "source_article": "RAW-ARTICLE-FIXTURE", "source_sentence": "S01", "meaning": "安全", "usage": "safe", "writing_value": "yes", "tags": "fixture", "review_note": "", "appear_count": "1", "last_seen": "2026-08-07"})
    mastered = english / "bank/mastered_items.csv"
    with mastered.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "item", "matched_id", "matched_type", "mastered_date",
                "evidence_sentence", "evidence_context", "proof_note",
            ],
        )
        writer.writeheader()
        writer.writerow({
            "item": "known", "matched_id": "V-KNOWN", "matched_type": "word",
            "mastered_date": "2026-08-01", "evidence_sentence": "known sentence",
            "evidence_context": "fixture", "proof_note": "explicit",
        })
    write_text(english / "bank/sentence_patterns.md", """# Patterns

## 卡片区

## SP-001｜It is safe to

完整句型卡第一行。
第二行保留，不得按行截断。
""")
    write_text(english / "articles/2026-08-07-fixture-learning-page.md", """# Fixture learning page

- source_id：RAW-ARTICLE-CORPUS

## 原文全文

A complete practice-safe corpus paragraph.
""")
    event_schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": [
            "schema_version", "event_id", "event_type", "idempotency_key",
            "request_sha256", "occurred_at", "article", "producer",
            "formal_write_count", "formal_writeback",
        ],
        "properties": {},
        "additionalProperties": True,
    }
    schema_v1 = dict(event_schema)
    schema_v1["properties"] = {
        "schema_version": {"const": "english_capture_event_v1"}
    }
    schema_v2 = dict(event_schema)
    schema_v2["properties"] = {
        "schema_version": {"const": "english_capture_event_v2"}
    }
    write_json(
        english / "schema/english_pipeline/capture-event-v1.schema.json", schema_v1
    )
    write_json(
        english / "schema/english_pipeline/capture-event-v2.schema.json", schema_v2
    )
    write_json(english / "intake/events/2026-08-07/EVT-1.json", {
        "schema_version": "english_capture_event_v1", "event_id": "EVT-1",
        "event_type": "capture", "idempotency_key": "idem-1",
        "request_sha256": "5" * 64, "occurred_at": "2026-08-07T10:00:00+08:00",
        "article": {"article_id": "RAW-ARTICLE-FIXTURE"},
        "source": {"sentence_id": "S01", "source_sentence": "A safe sentence."},
        "learning": {"first_translation": "fixture first translation", "nested": {"kept": True}},
        "candidates": [{"candidate_type": "word", "item": "safe"}],
        "producer": {"name": "fixture", "role": "capture", "version": "1"},
        "formal_write_count": 0, "formal_writeback": False,
        "private": "SHOULD_NEVER_APPEAR",
    })
    (english / "intake/receipts").mkdir(parents=True, exist_ok=True)
    (english / "intake/migrations/event-v2").mkdir(parents=True, exist_ok=True)

    pre = base / "preprocessor"
    release_id = hashlib.sha256(b"fixture-release").hexdigest()
    release = pre / "releases" / release_id
    write_json(release / "release.json", {"component_inventory": {"data_formats": {}}})
    (pre / "packages/objects").mkdir(parents=True, exist_ok=True)
    object_id = hashlib.sha256(b"fixture-object").hexdigest()
    write_json(pre / "packages/objects" / f"{object_id}.json", {"private": "SHOULD_NEVER_APPEAR"})
    os.symlink(release, pre / "current")
    return RepositoryConfig(math, cs408, english, pre)
