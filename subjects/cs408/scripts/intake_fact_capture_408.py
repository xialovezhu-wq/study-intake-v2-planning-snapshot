#!/usr/bin/env python3
"""408 学习中事实捕获账本。

该账本只保存日终仍无法可靠重建的答案安全事实和稳定证据定位。它不是正式
错题库，不分配正式节点，不写关系、索引、日期记录或复习调度。日终编纂以显式
study_date 冻结待处理集合，再逐题交给正式单写入口。
"""

from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import hmac
import json
import os
import re
import secrets
import stat
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator
from zoneinfo import ZoneInfo

from intake_lib_408 import FORMAL_ID_RE, scan_leak
from bounded_jsonl_index_408 import (
    BoundedJsonlIndexError,
    lookup_event as lookup_indexed_event,
)
from linked_practice_source_408 import (
    LinkedPracticeSourceError,
    resolve_selection_receipt,
    resolve_source as resolve_linked_practice_source,
    validate_selection_receipt,
    validate_source_for_practice,
)
from review_feedback_loop import (
    ReviewLoopError,
    morning_session_review_date,
    normalize_morning_first_classification,
)
from producer_binding_attestation_408 import (
    ProducerBindingError,
    publish_attestation,
)


SCHEMA = "intake_fact_capture_v1"
EVENT_SCHEMA = "intake_fact_capture_event_v1"
STATE_SCHEMA = "intake_fact_capture_state_v2"
LEGACY_BATCH_SCHEMA = "intake_daily_curation_batch_v1"
BATCH_SCHEMA_V2 = "intake_daily_curation_batch_v2"
BATCH_SCHEMA = "intake_daily_curation_batch_v3"
TIMEZONE = "Asia/Shanghai"
REVIEW_EVENT_SCHEMA = "review_event_v1"
BACKFLOW_POLICY_SCHEMA = "morning_review_backflow_policy_v1"
BACKFLOW_POLICY_REL = Path("schema/morning-review-backflow-policy-v1.json")
REVIEW_LOOP_LEDGER_REL = Path(
    "wiki/study_vaults/408-full/state/review-loop/events.jsonl"
)
REVIEW_LOOP_EVENT_INDEX_REL = Path(
    "wiki/study_vaults/408-full/state/review-loop/event-index"
)
MORNING_SESSION_REL = Path("wiki/study_vaults/408-full/state/morning-review")
PERSONALIZATION_STATE_REL = Path(
    "wiki/study_vaults/408-full/state/personalization"
)
MORNING_FAILURE_RESULTS = {"partial", "uncertain", "wrong"}
MORNING_CORRECT_RESULTS = {"independent_correct", "fragile_correct"}
NEUTRAL_SAVE_STATUS = "saved_neutral"
MANAGED_RICH_INTENT_OBJECT_REL = Path(
    "wiki/study_vaults/408-full/state/managed-408-rich-intake-turn/objects/intent"
)
SAVE_REQUEST_RE = re.compile(r"^[A-Za-z0-9_.:@-]{1,160}$")
MAX_SAVE_AUTHORIZATION_MESSAGE_BYTES = 4096
MAX_HOT_MORNING_QUEUE_BYTES = 1024 * 1024
ORDINARY_REVIEW_SOURCES = {"daily_practice", "evening_d0"}
ORDINARY_REVIEW_REF_KIND = "ordinary_review_loop_event"
CURRENT_QUESTION_EVIDENCE_REF_KIND = "current_question_evidence_bundle_v1"
CURRENT_QUESTION_FAILURE_STANDING_POLICY = (
    "current_question_failure_standing_policy_v1"
)
REVIEW_FIRST_BREAK_PROVENANCES = {
    "user_report",
    "visible_evidence",
    "not_observed",
    "derived_classification",
}
REVIEW_NONE_BREAKS = {
    "",
    "none",
    "无",
    "未观察到",
    "未记录",
    "未可靠观察到",
    "not_observed",
}
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/+-]{2,255}$")
SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
TEMP_LOCATOR_MARKERS = (
    "/tmp/",
    "/private/tmp/",
    "/var/folders/",
    "/private/var/folders/",
    "file://",
)
FORBIDDEN_CAPTURE_KEYS = {
    "full_question",
    "question_stem",
    "complete_stem",
    "correct_answer",
    "standard_answer",
    "wrong_answer",
    "user_answer",
    "options",
    "full_response",
    "full_explanation",
    "analysis_text",
    "attachment_path",
    "temporary_path",
    "image_path",
    "完整题干",
    "正确答案",
    "标准答案",
    "错误答案",
    "用户答案",
    "选项",
    "完整解析",
}
SOURCE_FACT_KEYS = {"year", "subject", "question_type", "source_id", "details_id"}
USER_FACT_KEYS = {
    "user_error_entry",
    "user_error_provenance",
    "first_action",
    "first_action_provenance",
    "observed_at",
}
IDENTITY_KEYS = {"status", "formal_id", "mode", "basis"}
PROVENANCE = {"user_report", "visible_evidence", "not_observed"}
IDENTITY_STATUSES = {"existing", "new_candidate", "unknown"}
IDENTITY_MODES = {"new", "redo", "unknown"}
RESULT_OUTCOMES = {"curated", "already_current", "needs_user", "failed"}
SUCCESS_OUTCOMES = {"curated", "already_current"}
DEFAULT_RUNTIME_ROOT = Path("~/.codex/kaoyan-408-intake").expanduser().resolve()
PRODUCER_BINDING_DESCRIPTOR_PATH = (
    Path(__file__).resolve().parents[1] / "schema" / "producer-binding-v1.json"
)
NORMAL_RECEIPT_SCHEMA = "intake_batch_receipt_v1"
NORMAL_WAL_SCHEMA = "intake_batch_wal_v1"
MAX_NORMAL_RECEIPT_BYTES = 2 * 1024 * 1024
MAX_NORMAL_WAL_BYTES = 8 * 1024 * 1024
GLOBAL_AUDIT_RECEIPT_SCHEMA = "intake_daily_global_audit_receipt_v1"
GLOBAL_AUDIT_RECEIPT_KIND = "daily_curation_global_audit"
GLOBAL_AUDIT_CLOSE_CONTRACT = "daily_curation_close_v3"
GLOBAL_AUDIT_VERSION = "daily_curation_global_audit_v1"
GLOBAL_AUDIT_RECEIPT_DIR = "global-audit-receipts"
GLOBAL_AUDIT_KEY_FILE = ".global-audit.key"
GLOBAL_AUDIT_TIMEOUT_SECONDS = 600
GLOBAL_AUDIT_MAX_REPORT_BYTES = 512 * 1024
GLOBAL_AUDIT_TOOL_RELS = (
    Path("scripts/intake_validate_408.py"),
    Path("scripts/audit_network_health.py"),
    Path("scripts/check_safe_node_cards.py"),
    Path("scripts/audit_extended.py"),
    Path("scripts/audit_review_schedule.py"),
)
GLOBAL_AUDIT_OBJECT_RELS = (
    Path("节点总表.md"),
    Path("关系边表.md"),
    Path("年份索引.md"),
    Path("知识点命中索引.md"),
    Path("错题日期索引.md"),
    Path("概念词典.md"),
    Path("复习单元总表.md"),
    Path("复习单元节点映射.md"),
    Path("原题复做轨总表.md"),
)


class CaptureError(RuntimeError):
    """事实捕获合同或账本一致性错误。"""


def _batch_attempt(event: dict[str, Any]) -> int:
    raw = event.get("attempt", 1)
    if not isinstance(raw, int) or isinstance(raw, bool) or raw < 1:
        raise CaptureError("日终批次 attempt 必须是正整数")
    return raw


def _batch_id(study_date: str, capture_set_sha: str, attempt: int) -> str:
    return (
        f"CUR-{study_date.replace('-', '')}-{capture_set_sha[:12]}-A{attempt:02d}"
    )


def _next_batch_attempt(
    state: dict[str, Any], study_date: str, capture_set_sha: str
) -> int:
    prior = [
        int(batch.get("attempt", 1))
        for batch in state.get("batches", {}).values()
        if batch.get("study_date") == study_date
        and batch.get("capture_set_sha256") == capture_set_sha
    ]
    return max(prior, default=0) + 1


def _repo_key(repo: Path) -> str:
    return hashlib.sha256(str(repo.resolve()).encode("utf-8")).hexdigest()[:20]


def _parse_timestamp(value: object, label: str) -> dt.datetime:
    text = str(value or "").strip()
    if not text:
        raise CaptureError(f"{label} 缺失")
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CaptureError(f"{label} 不是合法 ISO 时间") from exc
    if parsed.tzinfo is None:
        raise CaptureError(f"{label} 必须带时区")
    return parsed.astimezone(dt.timezone.utc)


def _morning_actual_observed_date(event: dict[str, Any]) -> str:
    actual = _parse_timestamp(
        event.get("event_time"), "review event.event_time"
    ).astimezone(ZoneInfo(TIMEZONE)).date().isoformat()
    if "session_review_date" in event and event.get("observed_date") != actual:
        raise CaptureError(
            "新 review event 的 observed_date 与上海实际作答日期不一致"
        )
    return actual


def capture_root(repo_root: str | Path) -> Path:
    repo = Path(repo_root).expanduser().resolve()
    return repo / "wiki" / "study_vaults" / "408-full" / "state" / "intake-curation"


def _resolve_repo(repo_root: str | Path) -> Path:
    repo = Path(repo_root).expanduser().resolve()
    if not repo.is_dir():
        raise CaptureError(f"repo 不存在：{repo}")
    return repo


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _sha256_value(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_file_bounded(path: Path, *, maximum: int, label: str) -> str:
    try:
        before = path.lstat()
    except OSError as exc:
        raise CaptureError(f"{label} 不可读") from exc
    if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
        raise CaptureError(f"{label} 必须是非链接普通文件")
    if before.st_size > maximum:
        raise CaptureError(f"{label} 超过热路径大小上限")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            opened = os.fstat(handle.fileno())
            if (
                opened.st_dev != before.st_dev
                or opened.st_ino != before.st_ino
                or opened.st_size != before.st_size
            ):
                raise CaptureError(f"{label} 在打开前发生漂移")
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
            after = os.fstat(handle.fileno())
    except OSError as exc:
        raise CaptureError(f"{label} 读取失败") from exc
    if (
        after.st_dev != before.st_dev
        or after.st_ino != before.st_ino
        or after.st_size != before.st_size
        or after.st_mtime_ns != before.st_mtime_ns
    ):
        raise CaptureError(f"{label} 在哈希期间发生漂移")
    return digest.hexdigest()


def _now_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _validate_date(value: object, label: str = "study_date") -> str:
    text = str(value or "").strip()
    if not DATE_RE.fullmatch(text):
        raise CaptureError(f"{label} 必须是 YYYY-MM-DD")
    try:
        dt.date.fromisoformat(text)
    except ValueError as exc:
        raise CaptureError(f"{label} 不是有效日期：{text}") from exc
    return text


def _validate_id(value: object, label: str) -> str:
    text = str(value or "").strip()
    if not SAFE_ID_RE.fullmatch(text):
        raise CaptureError(f"{label} 格式非法")
    return text


def _validate_sha(value: object, label: str) -> str:
    text = str(value or "").strip()
    if not SHA256_RE.fullmatch(text):
        raise CaptureError(f"{label} 必须是小写 SHA-256")
    return text


def _normalize_review_break_provenance(value: object, first_break: object) -> str:
    provenance = str(value or "")
    if provenance in REVIEW_FIRST_BREAK_PROVENANCES:
        return provenance
    if provenance:
        raise CaptureError("review event 的 first_break_provenance 非法")
    return (
        "not_observed"
        if str(first_break or "").strip().lower() in REVIEW_NONE_BREAKS
        else "derived_classification"
    )


def _canonical_morning_session_result(first: dict[str, Any]) -> str:
    try:
        result, _ = normalize_morning_first_classification(
            str(first.get("result") or ""),
            choice_result=str(first.get("choice_result") or "not_applicable"),
            reasoning_result=str(
                first.get("reasoning_result") or "not_observed"
            ),
            prompt_level=str(first.get("prompt_level") or ""),
        )
    except ReviewLoopError as exc:
        raise CaptureError(str(exc)) from exc
    return result


def _assert_no_forbidden_keys(value: Any, path: str = "capture") -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            if str(key) in FORBIDDEN_CAPTURE_KEYS:
                raise CaptureError(f"{path}.{key} 是受保护字段，禁止进入事实账本")
            _assert_no_forbidden_keys(nested, f"{path}.{key}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _assert_no_forbidden_keys(nested, f"{path}[{index}]")


def _assert_answer_safe(label: str, value: object) -> str:
    text = str(value or "").strip()
    fails, warns = scan_leak(text)
    if fails or warns:
        reasons = "；".join(fails + warns)
        raise CaptureError(f"{label} 未通过答案安全扫描：{reasons}")
    return text


def _stable_locator(value: object, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise CaptureError(f"{label} 不能为空")
    lowered = text.lower()
    if any(marker in lowered for marker in TEMP_LOCATOR_MARKERS):
        raise CaptureError(f"{label} 不能使用临时路径或 file URI")
    return _assert_answer_safe(label, text)


def _validate_capture_payload(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise CaptureError("capture 输入必须是 JSON object")
    _assert_no_forbidden_keys(raw)
    allowed = {
        "schema",
        "study_date",
        "timezone",
        "idempotency_key",
        "stable_evidence_refs",
        "source_facts",
        "user_facts",
        "identity_hint",
        "answer_safe_context_anchor",
        "missing_fields",
        "formalization_authorized",
        "authorization_policy",
    }
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise CaptureError(f"capture 含未授权字段：{unknown}")
    if raw.get("schema") != SCHEMA:
        raise CaptureError(f"schema 必须是 {SCHEMA}")
    study_date = _validate_date(raw.get("study_date"))
    timezone = str(raw.get("timezone") or TIMEZONE).strip()
    if timezone != TIMEZONE:
        raise CaptureError(f"timezone 当前必须显式使用 {TIMEZONE}")
    idempotency_key = _validate_id(raw.get("idempotency_key"), "idempotency_key")

    refs = raw.get("stable_evidence_refs")
    if not isinstance(refs, list) or not refs:
        raise CaptureError("stable_evidence_refs 至少需要一项耐久证据定位")
    normalized_refs: list[dict[str, str]] = []
    for index, item in enumerate(refs, 1):
        if not isinstance(item, dict) or set(item) != {"kind", "locator", "sha256"}:
            raise CaptureError(
                f"stable_evidence_refs#{index} 必须恰含 kind/locator/sha256"
            )
        normalized_refs.append(
            {
                "kind": _assert_answer_safe(f"stable_evidence_refs#{index}.kind", item["kind"]),
                "locator": _stable_locator(
                    item["locator"], f"stable_evidence_refs#{index}.locator"
                ),
                "sha256": _validate_sha(
                    item["sha256"], f"stable_evidence_refs#{index}.sha256"
                ),
            }
        )

    source_facts = raw.get("source_facts") or {}
    if not isinstance(source_facts, dict) or set(source_facts) - SOURCE_FACT_KEYS:
        raise CaptureError("source_facts 含未授权字段")
    normalized_source = {
        key: _assert_answer_safe(f"source_facts.{key}", value)
        for key, value in source_facts.items()
        if str(value or "").strip()
    }
    subject = normalized_source.get("subject")
    if subject and subject not in {"DS", "CO", "OS", "CN", "未确认"}:
        raise CaptureError("source_facts.subject 必须是 DS/CO/OS/CN/未确认")

    user_facts = raw.get("user_facts") or {}
    if not isinstance(user_facts, dict) or set(user_facts) - USER_FACT_KEYS:
        raise CaptureError("user_facts 含未授权字段")
    normalized_user = {
        key: _assert_answer_safe(f"user_facts.{key}", value)
        for key, value in user_facts.items()
        if str(value or "").strip()
    }
    error_provenance = normalized_user.get("user_error_provenance", "not_observed")
    if error_provenance not in PROVENANCE:
        raise CaptureError("user_error_provenance 非法")
    error_entry = normalized_user.get("user_error_entry", "未观察到")
    if error_provenance == "not_observed" and error_entry != "未观察到":
        raise CaptureError("未观察到用户错因时不得写入推断错因")
    normalized_user["user_error_entry"] = error_entry
    normalized_user["user_error_provenance"] = error_provenance
    if "first_action" in normalized_user:
        first_provenance = normalized_user.get("first_action_provenance")
        if first_provenance not in PROVENANCE - {"not_observed"}:
            raise CaptureError("记录 first_action 时必须给出直接 provenance")

    identity = raw.get("identity_hint") or {"status": "unknown", "mode": "unknown"}
    if not isinstance(identity, dict) or set(identity) - IDENTITY_KEYS:
        raise CaptureError("identity_hint 含未授权字段")
    status = str(identity.get("status") or "unknown").strip()
    mode = str(identity.get("mode") or "unknown").strip()
    if status not in IDENTITY_STATUSES or mode not in IDENTITY_MODES:
        raise CaptureError("identity_hint.status/mode 非法")
    formal_id = str(identity.get("formal_id") or "").strip()
    if formal_id and not FORMAL_ID_RE.fullmatch(formal_id):
        raise CaptureError("identity_hint.formal_id 非法")
    if status == "existing" and (not formal_id or mode != "redo"):
        raise CaptureError("existing 身份必须绑定合法 formal_id 且 mode=redo")
    if status != "existing" and formal_id:
        raise CaptureError("只有 existing 身份可以在捕获阶段绑定正式 ID")
    normalized_identity = {
        "status": status,
        "mode": mode,
        "formal_id": formal_id,
        "basis": _assert_answer_safe("identity_hint.basis", identity.get("basis", "")),
    }

    anchor = _assert_answer_safe(
        "answer_safe_context_anchor", raw.get("answer_safe_context_anchor")
    )
    if not anchor:
        raise CaptureError("answer_safe_context_anchor 不能为空")
    missing = raw.get("missing_fields") or []
    if not isinstance(missing, list) or not all(isinstance(item, str) for item in missing):
        raise CaptureError("missing_fields 必须是字符串数组")
    normalized_missing = sorted(
        {_assert_answer_safe("missing_fields", item) for item in missing if item.strip()}
    )
    authorized = raw.get("formalization_authorized")
    if not isinstance(authorized, bool):
        raise CaptureError("formalization_authorized 必须显式为 boolean")
    if (
        any(ref["kind"] == ORDINARY_REVIEW_REF_KIND for ref in normalized_refs)
        and authorized
    ):
        raise CaptureError("ordinary review event 不能自动授权 formalization")

    current_question_refs = [
        ref
        for ref in normalized_refs
        if ref["kind"] == CURRENT_QUESTION_EVIDENCE_REF_KIND
    ]
    authorization_policy = str(raw.get("authorization_policy") or "").strip()
    if current_question_refs:
        if len(current_question_refs) != 1 or len(normalized_refs) != 1:
            raise CaptureError("current-question capture 必须唯一绑定私有证据包")
        if authorization_policy != CURRENT_QUESTION_FAILURE_STANDING_POLICY:
            raise CaptureError("current-question capture 缺少固定失败回流 policy")
        if authorized is not True:
            raise CaptureError("current-question failure policy 必须进入日终待编纂清单")
    elif authorization_policy:
        raise CaptureError("非 current-question capture 不得声明该 standing policy")

    normalized = {
        "schema": SCHEMA,
        "study_date": study_date,
        "timezone": timezone,
        "idempotency_key": idempotency_key,
        "stable_evidence_refs": normalized_refs,
        "source_facts": normalized_source,
        "user_facts": normalized_user,
        "identity_hint": normalized_identity,
        "answer_safe_context_anchor": anchor,
        "missing_fields": normalized_missing,
        "formalization_authorized": authorized,
    }
    if authorization_policy:
        normalized["authorization_policy"] = authorization_policy
    return normalized


def _read_json(path: str | Path) -> Any:
    try:
        return json.loads(Path(path).expanduser().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CaptureError(f"读取 JSON 失败：{exc}") from exc


def _load_backflow_policy(repo: Path) -> dict[str, Any]:
    path = repo / BACKFLOW_POLICY_REL
    raw = _read_json(path)
    required = {
        "schema",
        "enabled",
        "effective_date",
        "timezone",
        "eligible_source",
        "eligible_first_results",
        "fast_capture_formalization_authorized",
        "formal_write_authorized",
        "daily_curation_requires_explicit_date",
        "authorization_basis",
    }
    if not isinstance(raw, dict) or set(raw) != required:
        raise CaptureError("晨间回流 policy 字段不完整或含未知字段")
    if raw.get("schema") != BACKFLOW_POLICY_SCHEMA or raw.get("enabled") is not True:
        raise CaptureError("晨间回流 policy 未启用或 schema 非法")
    _validate_date(raw.get("effective_date"), "policy.effective_date")
    if raw.get("timezone") != TIMEZONE or raw.get("eligible_source") != "morning_review":
        raise CaptureError("晨间回流 policy 的时区或来源非法")
    if set(raw.get("eligible_first_results") or []) != MORNING_FAILURE_RESULTS:
        raise CaptureError("晨间回流 policy 的首答结果集合非法")
    if raw.get("fast_capture_formalization_authorized") is not True:
        raise CaptureError("晨间回流 policy 未授权进入日终清单")
    if raw.get("formal_write_authorized") is not False:
        raise CaptureError("晨间回流 policy 不能授权晨间正式写入")
    if raw.get("daily_curation_requires_explicit_date") is not True:
        raise CaptureError("晨间回流 policy 必须要求显式日期日终编纂")
    _assert_answer_safe("policy.authorization_basis", raw.get("authorization_basis"))
    return raw


def _review_event_by_id(repo: Path, event_id: str) -> dict[str, Any]:
    event_value = _validate_id(event_id, "event_id")
    path = repo / REVIEW_LOOP_LEDGER_REL
    if not path.is_file():
        raise CaptureError("统一复盘事件账本不存在")
    hot_state_dir = repo / REVIEW_LOOP_LEDGER_REL.parent / "hot-state"
    index_dir = repo / REVIEW_LOOP_EVENT_INDEX_REL
    if (hot_state_dir / "manifest.json").is_file():
        try:
            from review_hot_state_408 import (
                HotStateError,
                lookup_event as lookup_hot_review_event,
            )

            hot = lookup_hot_review_event(
                repo,
                event_id=event_value,
                state_dir=hot_state_dir,
            )
        except (ImportError, HotStateError) as exc:
            raise CaptureError("统一复盘 hot state 精确校验失败") from exc
        matches = [hot["event"]] if hot.get("status") == "found" else []
    elif (index_dir / "manifest.json").is_file():
        try:
            indexed = lookup_indexed_event(path, index_dir, event_value)
        except BoundedJsonlIndexError as exc:
            raise CaptureError("统一复盘 event 精确索引校验失败") from exc
        matches = [indexed["event"]] if indexed is not None else []
    else:
        # Recovery compatibility only. The managed answer hot path bootstraps
        # this exact index before it can capture a canonical review event.
        matches = []
        seen: set[str] = set()
        for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise CaptureError(f"统一复盘事件账本第 {line_no} 行损坏：{exc}") from exc
            current_id = str(event.get("event_id") or "") if isinstance(event, dict) else ""
            if current_id in seen or not current_id:
                raise CaptureError("统一复盘事件账本含重复或空 event_id")
            seen.add(current_id)
            if current_id == event_value:
                matches.append(event)
    if len(matches) != 1:
        raise CaptureError(f"未找到唯一 review event：{event_value}")
    event = matches[0]
    if event.get("schema") != REVIEW_EVENT_SCHEMA:
        raise CaptureError("review event schema 非法")
    return event


def _validate_linked_personalization_manifest(
    path: Path, *, expected_formal_node_id: str | None = None
) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CaptureError("linked-practice personalization manifest 不可读") from exc
    assets = payload.get("question_assets") if isinstance(payload, dict) else None
    formal_id = str(payload.get("formal_node_id") or "")
    if (
        payload.get("schema") != "question_source_manifest_v1"
        or payload.get("answer_isolated") is not True
        or not FORMAL_ID_RE.fullmatch(formal_id)
        or (expected_formal_node_id and formal_id != expected_formal_node_id)
        or not isinstance(assets, list)
        or not assets
        or payload.get("question_asset_count") != len(assets)
    ):
        raise CaptureError("linked-practice personalization manifest 未通过题面隔离门禁")
    for asset in assets:
        if not isinstance(asset, dict) or set(asset) != {
            "path",
            "sha256",
            "size_bytes",
        }:
            raise CaptureError("linked-practice question asset 记录非法")
        asset_path = Path(str(asset.get("path") or "")).expanduser()
        digest = str(asset.get("sha256") or "")
        size = asset.get("size_bytes")
        if (
            not asset_path.is_absolute()
            or not asset_path.is_file()
            or not asset_path.name.startswith("question-")
            or "solution" in asset_path.name.lower()
            or not SHA256_RE.fullmatch(digest)
            or not isinstance(size, int)
            or size < 0
            or asset_path.stat().st_size != size
            or _sha256_file(asset_path) != digest
        ):
            raise CaptureError("linked-practice question asset 校验失败")
    return formal_id


def _linked_source_path(repo: Path, locator: object) -> tuple[Path, str]:
    raw = _stable_locator(locator, "linked_practice.source_ref")
    try:
        return resolve_linked_practice_source(repo, raw)
    except LinkedPracticeSourceError as exc:
        raise CaptureError(str(exc)) from exc


def _session_evidence_binding(
    repo: Path, event: dict[str, Any]
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    Path,
    str,
    dict[str, Any] | None,
]:
    session_id = str(event.get("session_id") or "")
    item_id = str(event.get("item_id") or "")
    if not SESSION_ID_RE.fullmatch(session_id):
        raise CaptureError("review event session_id 非法")
    session_dir = (repo / MORNING_SESSION_REL / session_id).resolve()
    try:
        session_dir.relative_to((repo / MORNING_SESSION_REL).resolve())
    except ValueError as exc:
        raise CaptureError("morning session 路径越界") from exc
    session_ledger = session_dir / "events.jsonl"
    if not session_ledger.is_file():
        raise CaptureError("morning session 事件账本不存在")
    try:
        session_events = [
            json.loads(line)
            for line in session_ledger.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        started = session_events[0]
    except (IndexError, json.JSONDecodeError) as exc:
        raise CaptureError("morning session 缺少有效 session_started") from exc
    for sequence, session_event in enumerate(session_events, 1):
        if (
            session_event.get("schema") != "morning_review_session_event_v1"
            or session_event.get("session_id") != session_id
            or session_event.get("sequence") != sequence
        ):
            raise CaptureError("morning session 事件序列非法")
    if (
        started.get("schema") != "morning_review_session_event_v1"
        or started.get("event_type") != "session_started"
        or started.get("session_id") != session_id
    ):
        raise CaptureError("morning session 首事件非法")
    payload = started.get("payload") or {}
    if payload.get("review_date") != morning_session_review_date(event):
        raise CaptureError("review event 日期与 morning session 不一致")
    queue_item = next(
        (row for row in payload.get("items", []) if row.get("item_id") == item_id),
        None,
    )
    followup_events = [
        row
        for row in session_events[1:]
        if row.get("event_type") == "followup_scheduled"
        and (row.get("payload") or {}).get("item_id") == item_id
    ]
    if queue_item is not None and followup_events:
        raise CaptureError("morning session item 同时出现在不可变队列与 followup 事件")
    linked: dict[str, Any] | None = None
    if queue_item is None:
        if len(followup_events) != 1:
            raise CaptureError("动态 morning item 缺少唯一 followup_scheduled 证据")
        followup_event = followup_events[0]
        item = followup_event.get("payload") or {}
        practice_mode = str(item.get("practice_mode") or "")
        expected_kind = (
            "related_original" if practice_mode == "original" else "grounded_variant"
        )
        for label in (
            "scheduling_key",
            "trigger_event_id",
            "parent_item_id",
            "context_slice_id",
            "candidate_id",
            "source_id",
            "source_ref",
            "source_sha256",
            "selection_receipt_ref",
            "selection_receipt_sha256",
            "mechanism_key",
            "knowledge_point",
        ):
            if not str(item.get(label) or ""):
                raise CaptureError(f"followup_scheduled 缺少 {label}")
        if practice_mode not in {"original", "variant"} or item.get(
            "item_kind"
        ) != expected_kind:
            raise CaptureError("followup_scheduled 的 practice_mode/item_kind 非法")
        source_path, source_ref = _linked_source_path(repo, item.get("source_ref"))
        source_sha = _validate_sha(
            item.get("source_sha256"), "followup_scheduled.source_sha256"
        )
        if _sha256_file(source_path) != source_sha:
            raise CaptureError("linked-practice source 已偏离调度快照")
        trigger = _review_event_by_id(repo, str(item.get("trigger_event_id")))
        if (
            trigger.get("event_kind") != "outcome"
            or trigger.get("source") != "morning_review"
            or trigger.get("session_id") != session_id
            or trigger.get("item_id") != item.get("parent_item_id")
            or trigger.get("practice_mode")
        ):
            raise CaptureError("followup_scheduled 的 parent trigger 非法")
        for label in (
            "practice_mode",
            "anchor_formal_node_id",
            "variant_of_formal_node_id",
            "trigger_event_id",
            "context_slice_id",
            "candidate_id",
            "source_ref",
            "source_sha256",
            "selection_receipt_ref",
            "selection_receipt_sha256",
        ):
            event_label = (
                "parent_trigger_event_id" if label == "trigger_event_id" else label
            )
            projected = source_ref if label == "source_ref" else item.get(label)
            if projected != event.get(event_label):
                raise CaptureError(
                    f"review event 与 followup_scheduled 的 {label} 不一致"
                )
        for label in ("mechanism_key", "knowledge_point"):
            if item.get(label) != event.get(label):
                raise CaptureError(
                    f"review event 未保真保存 followup_scheduled 的 {label}"
                )
        formal_id = item.get("formal_node_id") or None
        variant_of = item.get("variant_of_formal_node_id") or None
        anchor_formal = item.get("anchor_formal_node_id") or None
        if practice_mode == "original":
            if (
                not formal_id
                or variant_of
                or event.get("formal_node_id") != formal_id
                or event.get("actual_question_formal_node_id") != formal_id
            ):
                raise CaptureError("original followup 的真实原题身份绑定非法")
        elif (
            formal_id
            or not variant_of
            or anchor_formal != variant_of
            or event.get("formal_node_id") is not None
            or event.get("actual_question_formal_node_id") is not None
        ):
            raise CaptureError("variant followup 污染了原题真实作答身份")
        try:
            verified_source = validate_source_for_practice(
                repo,
                source_ref,
                source_sha,
                practice_mode=practice_mode,
                formal_node_id=(str(formal_id) if formal_id else None),
                variant_of_formal_node_id=(str(variant_of) if variant_of else None),
            )
        except LinkedPracticeSourceError as exc:
            raise CaptureError(str(exc)) from exc
        if (
            verified_source["path"] != source_path
            or verified_source["source_ref"] != source_ref
        ):
            raise CaptureError("linked-practice source identity 不一致")
        try:
            verified_selection = validate_selection_receipt(
                repo,
                item.get("selection_receipt_ref"),
                item.get("selection_receipt_sha256"),
                expected={
                    "slice_id": item.get("context_slice_id"),
                    "trigger_event_id": item.get("trigger_event_id"),
                    "candidate_id": item.get("candidate_id"),
                    "practice_mode": item.get("practice_mode"),
                    "formal_node_id": item.get("formal_node_id"),
                    "anchor_formal_node_id": item.get(
                        "anchor_formal_node_id"
                    ),
                    "source_id": item.get("source_id"),
                    "source_ref": source_ref,
                    "source_sha256": source_sha,
                    "mechanism_key": item.get("mechanism_key"),
                    "knowledge_point": item.get("knowledge_point"),
                    "review_unit_id": item.get("review_unit_id"),
                },
                source_path=source_path,
            )
        except LinkedPracticeSourceError as exc:
            raise CaptureError(str(exc)) from exc
        selection_path = verified_selection["path"]
        selection_ref = verified_selection["selection_receipt_ref"]
        if selection_ref != item.get("selection_receipt_ref"):
            raise CaptureError("linked-practice selection receipt identity 不一致")
        if event.get("followup_event_id") != followup_event.get("event_id"):
            raise CaptureError("review event 未绑定唯一 followup_scheduled 事件")
        linked = {
            "followup_event": followup_event,
            "parent_trigger": trigger,
            "source_path": source_path,
            "source_ref": source_ref,
            "selection_path": selection_path,
            "selection_ref": selection_ref,
        }
    else:
        if not isinstance(queue_item, dict):
            raise CaptureError("morning session item 非法")
        item = queue_item
    if item.get("source_id") != event.get("source_id"):
        raise CaptureError("review event 与 morning session item/source 不一致")
    first_events = [
        row
        for row in session_events[1:]
        if row.get("event_type") in {"first_recorded", "first_reconciled_from_loop"}
        and (row.get("payload") or {}).get("item_id") == item_id
    ]
    if len(first_events) != 1:
        raise CaptureError("review event 尚未唯一投影为 morning session 首答")
    first = first_events[0].get("payload") or {}
    binding_events = [
        row
        for row in session_events[1:]
        if row.get("event_type") == "first_bound_to_loop"
        and (row.get("payload") or {}).get("item_id") == item_id
        and (row.get("payload") or {}).get("canonical_event_id")
        == event.get("event_id")
    ]
    reconciled_binding = first.get("canonical_event_id") == event.get("event_id")
    if not reconciled_binding and len(binding_events) != 1:
        raise CaptureError("review event 尚未由 morning session 首答唯一绑定")
    session_result = _canonical_morning_session_result(first)
    for field, projected, canonical in (
        ("result", session_result, event.get("first_result")),
        ("confidence", first.get("confidence"), event.get("confidence")),
        ("prompt_level", first.get("prompt_level"), event.get("prompt_level")),
        ("first_break", first.get("first_break"), event.get("first_break")),
        (
            "first_break_provenance",
            _normalize_review_break_provenance(
                first.get("first_break_provenance"), first.get("first_break")
            ),
            _normalize_review_break_provenance(
                event.get("first_break_provenance"), event.get("first_break")
            ),
        ),
        (
            "event_time",
            first.get("recorded_at") or first_events[0].get("timestamp"),
            event.get("event_time"),
        ),
    ):
        if projected != canonical:
            raise CaptureError(f"review event 与 morning session 首答 {field} 不一致")
    queue_raw = str(payload.get("queue_path") or "")
    queue_path = (repo / queue_raw).resolve()
    try:
        queue_rel = str(queue_path.relative_to(repo))
    except ValueError as exc:
        raise CaptureError("morning queue 路径越界") from exc
    expected_hash = _validate_sha(payload.get("queue_sha256"), "session.queue_sha256")
    if not queue_path.is_file() or _sha256_file(queue_path) != expected_hash:
        raise CaptureError("morning queue 缺失或已偏离 session 启动快照")
    return started, first_events[0], queue_path, queue_rel, linked


def _review_capture_payload_from_verified_evidence(
    repo: Path,
    event: dict[str, Any],
    policy: dict[str, Any],
    *,
    started: dict[str, Any],
    first_event: dict[str, Any],
    queue_path: Path,
    queue_rel: str,
    linked: dict[str, Any] | None,
    queue_sha256: str | None = None,
    extra_stable_refs: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    if (
        event.get("schema") != REVIEW_EVENT_SCHEMA
        or event.get("event_kind") != "outcome"
        or event.get("source") != "morning_review"
        or event.get("question_valid") is not True
        or event.get("formal_write_authorized") is not False
    ):
        raise CaptureError("只允许捕获有效的晨间首答 outcome")
    first_result = str(event.get("first_result") or "")
    if first_result not in set(policy["eligible_first_results"]):
        raise CaptureError("该晨间首答结果不属于快速捕获范围")
    observed_date = _validate_date(
        _morning_actual_observed_date(event), "event.observed_date"
    )
    if observed_date < str(policy["effective_date"]):
        raise CaptureError("review event 早于晨间回流 policy 生效日期")
    event_id = str(event["event_id"])
    session_id = str(event["session_id"])
    session_ledger_rel = str(MORNING_SESSION_REL / session_id / "events.jsonl")
    event_locator = f"{REVIEW_LOOP_LEDGER_REL}#event_id={event_id}"
    session_locator = f"{session_ledger_rel}#sequence=1"
    first_locator = f"{session_ledger_rel}#sequence={first_event['sequence']}"

    route = event.get("route") or {}
    candidate = route.get("candidate") or {}
    formal_id = str(event.get("formal_node_id") or "")
    if not formal_id and route.get("kind") == "update_existing":
        formal_id = str(candidate.get("formal_node_id") or "")
    if formal_id and not FORMAL_ID_RE.fullmatch(formal_id):
        raise CaptureError("review event 的正式节点身份非法")
    if formal_id:
        identity = {
            "status": "existing",
            "formal_id": formal_id,
            "mode": "redo",
            "basis": f"canonical review event 明确绑定 {formal_id}",
        }
    else:
        identity = {
            "status": "unknown",
            "mode": "unknown",
            "basis": "正式身份留待日终依据稳定证据核验",
        }

    first_break = str(event.get("first_break") or "").strip()
    provenance = _normalize_review_break_provenance(
        event.get("first_break_provenance"), first_break
    )
    if provenance in {"user_report", "visible_evidence"} and first_break:
        user_facts = {
            "user_error_entry": first_break,
            "user_error_provenance": provenance,
            "observed_at": str(event.get("event_time") or ""),
        }
    else:
        user_facts = {
            "user_error_entry": "未观察到",
            "user_error_provenance": "not_observed",
            "observed_at": str(event.get("event_time") or ""),
        }

    source_id = str(event.get("source_id") or "")
    context = str(
        event.get("knowledge_point")
        or event.get("mechanism_key")
        or source_id
    )
    source_facts: dict[str, str] = {
        "question_type": (
            "关联原题复做"
            if event.get("practice_mode") == "original"
            else "有据变式练习"
            if event.get("practice_mode") == "variant"
            else "晨间主动回忆"
        ),
        "source_id": source_id,
        "details_id": event_id,
    }
    if formal_id:
        subject, year, _ = formal_id.split("_", 2)
        source_facts["subject"] = subject
        if year.isdigit():
            source_facts["year"] = year
    else:
        variant_origin = str(event.get("variant_of_formal_node_id") or "")
        if FORMAL_ID_RE.fullmatch(variant_origin):
            source_facts["subject"] = variant_origin.split("_", 1)[0]
        else:
            source_facts["subject"] = "未确认"
    missing = []
    if not formal_id:
        missing.append("formal_identity")
    if not event.get("knowledge_point"):
        missing.append("main_knowledge")
    if user_facts["user_error_provenance"] == "not_observed":
        missing.append("user_error_entry")

    stable_refs = [
        {
            "kind": "review_loop_event",
            "locator": event_locator,
            "sha256": _sha256_value(event),
        },
        {
            "kind": "morning_session_start",
            "locator": session_locator,
            "sha256": _sha256_value(started),
        },
        {
            "kind": "morning_session_first",
            "locator": first_locator,
            "sha256": _sha256_value(first_event),
        },
        {
            "kind": "morning_session_queue",
            "locator": queue_rel,
            "sha256": queue_sha256 or _sha256_file(queue_path),
        },
    ]
    stable_refs.extend(extra_stable_refs or [])
    if linked is not None:
        followup_event = linked["followup_event"]
        trigger = linked["parent_trigger"]
        stable_refs.extend(
            [
                {
                    "kind": "linked_practice_followup",
                    "locator": (
                        f"{session_ledger_rel}#sequence={followup_event['sequence']}"
                    ),
                    "sha256": _sha256_value(followup_event),
                },
                {
                    "kind": "linked_practice_parent_trigger",
                    "locator": (
                        f"{REVIEW_LOOP_LEDGER_REL}#event_id={trigger['event_id']}"
                    ),
                    "sha256": _sha256_value(trigger),
                },
                {
                    "kind": "linked_practice_source",
                    "locator": linked["source_ref"],
                    "sha256": _sha256_file(linked["source_path"]),
                },
                {
                    "kind": "personalization_selection_receipt",
                    "locator": linked["selection_ref"],
                    "sha256": _sha256_file(linked["selection_path"]),
                },
            ]
        )

    return {
        "schema": SCHEMA,
        "study_date": observed_date,
        "timezone": TIMEZONE,
        "idempotency_key": f"review-event:{event_id}:fast-capture:v1",
        "stable_evidence_refs": stable_refs,
        "source_facts": source_facts,
        "user_facts": user_facts,
        "identity_hint": identity,
        "answer_safe_context_anchor": f"晨间复盘 {source_id} 的 {context} 非正确首答证据",
        "missing_fields": missing,
        "formalization_authorized": bool(
            policy["fast_capture_formalization_authorized"]
        ),
    }


def _review_capture_payload(
    repo: Path, event: dict[str, Any], policy: dict[str, Any]
) -> dict[str, Any]:
    """Cold compatibility bridge for historical and explicit recovery callers.

    The managed morning answer path uses bounded receipts and must call
    ``build_verified_morning_capture_payload`` instead.  This bridge preserves
    the existing recovery behavior and may scan the one session ledger.
    """

    started, first_event, queue_path, queue_rel, linked = _session_evidence_binding(
        repo, event
    )
    return _review_capture_payload_from_verified_evidence(
        repo,
        event,
        policy,
        started=started,
        first_event=first_event,
        queue_path=queue_path,
        queue_rel=queue_rel,
        queue_sha256=None,
        linked=linked,
    )


def build_verified_morning_capture_payload(
    repo_root: str | Path,
    event: dict[str, Any],
    *,
    session_started_event: dict[str, Any],
    session_first_event: dict[str, Any],
    session_binding_event: dict[str, Any],
    linked_followup_event: dict[str, Any] | None = None,
    parent_trigger_event: dict[str, Any] | None = None,
    receipt_refs: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Build one morning capture from already exact-read, receipt-bound evidence.

    This function performs no review, session, or capture ledger lookup.  It
    revalidates the supplied event identities, the immutable queue snapshot,
    and any linked-practice source/selection receipts before returning a fully
    normalized answer-safe capture payload.
    """

    repo = _resolve_repo(repo_root)
    policy = _load_backflow_policy(repo)
    if not isinstance(event, dict):
        raise CaptureError("managed morning capture 缺少 canonical event")
    event_id = _validate_id(event.get("event_id"), "event_id")
    session_id = str(event.get("session_id") or "")
    item_id = _validate_id(event.get("item_id"), "item_id")
    if not SESSION_ID_RE.fullmatch(session_id):
        raise CaptureError("review event session_id 非法")

    def session_event(
        value: dict[str, Any], event_type: str, label: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if (
            not isinstance(value, dict)
            or value.get("schema") != "morning_review_session_event_v1"
            or value.get("session_id") != session_id
            or value.get("event_type") != event_type
            or not isinstance(value.get("sequence"), int)
            or int(value["sequence"]) <= 0
            or not isinstance(value.get("payload"), dict)
        ):
            raise CaptureError(f"{label} 的 session event 身份非法")
        return value, value["payload"]

    started, started_payload = session_event(
        session_started_event, "session_started", "morning session start"
    )
    first_event, first = session_event(
        session_first_event, "first_recorded", "morning session first"
    )
    binding_event, binding = session_event(
        session_binding_event,
        "first_bound_to_loop",
        "morning session binding",
    )
    if started.get("sequence") != 1:
        raise CaptureError("morning session start 必须是 sequence=1")
    if started_payload.get("review_date") != morning_session_review_date(event):
        raise CaptureError("review event 日期与 morning session 不一致")
    if (
        first.get("item_id") != item_id
        or binding.get("item_id") != item_id
        or binding.get("canonical_event_id") != event_id
    ):
        raise CaptureError("morning 首答或 canonical binding 身份不一致")

    session_result = _canonical_morning_session_result(first)
    comparisons = (
        ("result", session_result, event.get("first_result")),
        ("choice_result", first.get("choice_result"), event.get("choice_result")),
        (
            "reasoning_result",
            first.get("reasoning_result"),
            event.get("reasoning_result"),
        ),
        ("confidence", first.get("confidence"), event.get("confidence")),
        ("prompt_level", first.get("prompt_level"), event.get("prompt_level")),
        ("first_break", first.get("first_break"), event.get("first_break")),
        (
            "first_break_provenance",
            _normalize_review_break_provenance(
                first.get("first_break_provenance"), first.get("first_break")
            ),
            _normalize_review_break_provenance(
                event.get("first_break_provenance"), event.get("first_break")
            ),
        ),
        (
            "event_time",
            first.get("recorded_at") or first_event.get("timestamp"),
            event.get("event_time"),
        ),
    )
    for field, projected, canonical in comparisons:
        if projected != canonical:
            raise CaptureError(
                f"review event 与 receipt-bound morning 首答 {field} 不一致"
            )

    queue_raw = str(started_payload.get("queue_path") or "")
    queue_path = (repo / queue_raw).resolve()
    try:
        queue_rel = str(queue_path.relative_to(repo))
    except ValueError as exc:
        raise CaptureError("morning queue 路径越界") from exc
    queue_sha = _validate_sha(
        started_payload.get("queue_sha256"), "session.queue_sha256"
    )
    if (
        _sha256_file_bounded(
            queue_path,
            maximum=MAX_HOT_MORNING_QUEUE_BYTES,
            label="morning queue",
        )
        != queue_sha
    ):
        raise CaptureError("morning queue 缺失或已偏离 session 启动快照")
    queue_items = started_payload.get("items")
    if not isinstance(queue_items, list):
        raise CaptureError("morning session start 缺少不可变 items")
    queue_matches = [
        row
        for row in queue_items
        if isinstance(row, dict) and row.get("item_id") == item_id
    ]
    if len(queue_matches) > 1:
        raise CaptureError("morning queue 含重复 item_id")

    linked: dict[str, Any] | None = None
    if queue_matches:
        if linked_followup_event is not None or parent_trigger_event is not None:
            raise CaptureError("queue item 不得伪装成 linked followup")
        item = queue_matches[0]
    else:
        if linked_followup_event is None or parent_trigger_event is None:
            raise CaptureError("dynamic morning item 缺少有界 followup/trigger 证据")
        followup_event, item = session_event(
            linked_followup_event,
            "followup_scheduled",
            "linked-practice followup",
        )
        if item.get("item_id") != item_id:
            raise CaptureError("linked-practice followup item 身份不一致")
        practice_mode = str(item.get("practice_mode") or "")
        expected_kind = (
            "related_original" if practice_mode == "original" else "grounded_variant"
        )
        for label in (
            "scheduling_key",
            "trigger_event_id",
            "parent_item_id",
            "context_slice_id",
            "candidate_id",
            "source_id",
            "source_ref",
            "source_sha256",
            "selection_receipt_ref",
            "selection_receipt_sha256",
            "mechanism_key",
            "knowledge_point",
        ):
            if not str(item.get(label) or ""):
                raise CaptureError(f"followup_scheduled 缺少 {label}")
        if (
            practice_mode not in {"original", "variant"}
            or item.get("item_kind") != expected_kind
        ):
            raise CaptureError("followup_scheduled 的 practice_mode/item_kind 非法")

        trigger = parent_trigger_event
        if (
            trigger.get("schema") != REVIEW_EVENT_SCHEMA
            or trigger.get("event_kind") != "outcome"
            or trigger.get("source") != "morning_review"
            or trigger.get("event_id") != item.get("trigger_event_id")
            or trigger.get("session_id") != session_id
            or trigger.get("item_id") != item.get("parent_item_id")
            or trigger.get("practice_mode")
        ):
            raise CaptureError("followup_scheduled 的 parent trigger 非法")

        source_path, source_ref = _linked_source_path(repo, item.get("source_ref"))
        source_sha = _validate_sha(
            item.get("source_sha256"), "followup_scheduled.source_sha256"
        )
        if _sha256_file(source_path) != source_sha:
            raise CaptureError("linked-practice source 已偏离调度快照")
        for label in (
            "practice_mode",
            "anchor_formal_node_id",
            "variant_of_formal_node_id",
            "trigger_event_id",
            "context_slice_id",
            "candidate_id",
            "source_ref",
            "source_sha256",
            "selection_receipt_ref",
            "selection_receipt_sha256",
        ):
            event_label = (
                "parent_trigger_event_id" if label == "trigger_event_id" else label
            )
            projected = source_ref if label == "source_ref" else item.get(label)
            if projected != event.get(event_label):
                raise CaptureError(
                    f"review event 与 followup_scheduled 的 {label} 不一致"
                )
        for label in ("mechanism_key", "knowledge_point"):
            if item.get(label) != event.get(label):
                raise CaptureError(
                    f"review event 未保真保存 followup_scheduled 的 {label}"
                )
        formal_id = item.get("formal_node_id") or None
        variant_of = item.get("variant_of_formal_node_id") or None
        anchor_formal = item.get("anchor_formal_node_id") or None
        if practice_mode == "original":
            if (
                not formal_id
                or variant_of
                or event.get("formal_node_id") != formal_id
                or event.get("actual_question_formal_node_id") != formal_id
            ):
                raise CaptureError("original followup 的真实原题身份绑定非法")
        elif (
            formal_id
            or not variant_of
            or anchor_formal != variant_of
            or event.get("formal_node_id") is not None
            or event.get("actual_question_formal_node_id") is not None
        ):
            raise CaptureError("variant followup 污染了原题真实作答身份")
        try:
            verified_source = validate_source_for_practice(
                repo,
                source_ref,
                source_sha,
                practice_mode=practice_mode,
                formal_node_id=(str(formal_id) if formal_id else None),
                variant_of_formal_node_id=(str(variant_of) if variant_of else None),
            )
            verified_selection = validate_selection_receipt(
                repo,
                item.get("selection_receipt_ref"),
                item.get("selection_receipt_sha256"),
                expected={
                    "slice_id": item.get("context_slice_id"),
                    "trigger_event_id": item.get("trigger_event_id"),
                    "candidate_id": item.get("candidate_id"),
                    "practice_mode": item.get("practice_mode"),
                    "formal_node_id": item.get("formal_node_id"),
                    "anchor_formal_node_id": item.get("anchor_formal_node_id"),
                    "source_id": item.get("source_id"),
                    "source_ref": source_ref,
                    "source_sha256": source_sha,
                    "mechanism_key": item.get("mechanism_key"),
                    "knowledge_point": item.get("knowledge_point"),
                    "review_unit_id": item.get("review_unit_id"),
                },
                source_path=source_path,
            )
        except LinkedPracticeSourceError as exc:
            raise CaptureError(str(exc)) from exc
        if (
            verified_source["path"] != source_path
            or verified_source["source_ref"] != source_ref
        ):
            raise CaptureError("linked-practice source identity 不一致")
        if event.get("followup_event_id") != followup_event.get("event_id"):
            raise CaptureError("review event 未绑定唯一 followup_scheduled 事件")
        linked = {
            "followup_event": followup_event,
            "parent_trigger": trigger,
            "source_path": source_path,
            "source_ref": source_ref,
            "selection_path": verified_selection["path"],
            "selection_ref": verified_selection["selection_receipt_ref"],
        }

    if item.get("source_id") != event.get("source_id"):
        raise CaptureError("review event 与 receipt-bound morning item/source 不一致")
    session_ledger_rel = str(MORNING_SESSION_REL / session_id / "events.jsonl")
    extra_refs = [
        {
            "kind": "morning_session_binding",
            "locator": (
                f"{session_ledger_rel}#sequence={binding_event['sequence']}"
            ),
            "sha256": _sha256_value(binding_event),
        }
    ]
    extra_refs.extend(receipt_refs or [])
    raw = _review_capture_payload_from_verified_evidence(
        repo,
        event,
        policy,
        started=started,
        first_event=first_event,
        queue_path=queue_path,
        queue_rel=queue_rel,
        linked=linked,
        queue_sha256=queue_sha,
        extra_stable_refs=extra_refs,
    )
    return _validate_capture_payload(raw)


def _ordinary_formal_identity(event: dict[str, Any]) -> dict[str, str]:
    """Resolve only a conflict-free formal identity already present in the event."""

    candidates: set[str] = set()
    direct = str(event.get("formal_node_id") or "").strip()
    if direct:
        if not FORMAL_ID_RE.fullmatch(direct):
            raise CaptureError("ordinary review event 的 formal_node_id 非法")
        candidates.add(direct)
    mapped = event.get("mapped_formal_node_ids") or []
    if not isinstance(mapped, list):
        raise CaptureError("ordinary review event 的 mapped_formal_node_ids 非法")
    for value in mapped:
        node_id = str(value or "").strip()
        if not FORMAL_ID_RE.fullmatch(node_id):
            raise CaptureError("ordinary review event 含非法 mapped formal identity")
    route = event.get("route") or {}
    if not isinstance(route, dict):
        raise CaptureError("ordinary review event 的 route 非法")
    if route.get("formal_write_authorized") is not False:
        raise CaptureError("ordinary review event 的 route 不能授权正式写入")
    candidate = route.get("candidate") or {}
    if candidate and not isinstance(candidate, dict):
        raise CaptureError("ordinary review event 的 candidate 非法")
    route_formal = str(candidate.get("formal_node_id") or "").strip()
    if route_formal:
        if not FORMAL_ID_RE.fullmatch(route_formal):
            raise CaptureError("ordinary review event 的 route formal identity 非法")
        if route.get("kind") == "update_existing":
            candidates.add(route_formal)
    if len(candidates) > 1:
        raise CaptureError("ordinary review event 的 formal identity 冲突")
    if candidates:
        formal_id = next(iter(candidates))
        if mapped and formal_id not in mapped:
            raise CaptureError("ordinary review event 的 formal identity 与映射冲突")
        return {
            "status": "existing",
            "formal_id": formal_id,
            "mode": "redo",
            "basis": f"canonical ordinary review event 明确绑定 {formal_id}",
        }
    return {
        "status": "unknown",
        "formal_id": "",
        "mode": "unknown",
        "basis": "正式身份留待日终依据稳定证据核验",
    }


def _ordinary_review_capture_payload(
    event: dict[str, Any],
) -> dict[str, Any]:
    """Build an unconfirmed capture from one ordinary canonical outcome."""

    if (
        event.get("schema") != REVIEW_EVENT_SCHEMA
        or event.get("event_kind") != "outcome"
        or event.get("source") not in ORDINARY_REVIEW_SOURCES
        or event.get("question_valid") is not True
        or event.get("formal_write_authorized") is not False
    ):
        raise CaptureError("只允许绑定有效且未授权正式写入的普通学习 outcome")
    first_result = str(event.get("first_result") or "")
    if first_result not in MORNING_FAILURE_RESULTS:
        raise CaptureError("该普通学习首答结果不属于快速捕获范围")
    event_id = _validate_id(event.get("event_id"), "event_id")
    study_date = _validate_date(
        event.get("observed_date"), "ordinary event.observed_date"
    )
    source_id = _assert_answer_safe(
        "ordinary event.source_id", event.get("source_id")
    )
    if not source_id:
        raise CaptureError("ordinary review event 缺少 source_id")
    identity = _ordinary_formal_identity(event)

    first_break = str(event.get("first_break") or "").strip()
    provenance = _normalize_review_break_provenance(
        event.get("first_break_provenance"), first_break
    )
    if (
        provenance in {"user_report", "visible_evidence"}
        and first_break
        and first_break.lower() not in REVIEW_NONE_BREAKS
    ):
        user_facts = {
            "user_error_entry": first_break,
            "user_error_provenance": provenance,
            "observed_at": str(event.get("event_time") or ""),
        }
    else:
        user_facts = {
            "user_error_entry": "未观察到",
            "user_error_provenance": "not_observed",
            "observed_at": str(event.get("event_time") or ""),
        }

    source = str(event["source"])
    source_facts: dict[str, str] = {
        "question_type": (
            "日常练习首答" if source == "daily_practice" else "晚间 D0 首答"
        ),
        "source_id": source_id,
        "details_id": event_id,
    }
    formal_id = identity["formal_id"]
    if formal_id:
        subject, year, _ = formal_id.split("_", 2)
        source_facts["subject"] = subject
        if year.isdigit():
            source_facts["year"] = year
    else:
        source_facts["subject"] = "未确认"

    missing = []
    if not formal_id:
        missing.append("formal_identity")
    if not event.get("knowledge_point"):
        missing.append("main_knowledge")
    if user_facts["user_error_provenance"] == "not_observed":
        missing.append("user_error_entry")
    context = str(
        event.get("knowledge_point")
        or event.get("mechanism_key")
        or source_id
    )
    event_locator = f"{REVIEW_LOOP_LEDGER_REL}#event_id={event_id}"
    return {
        "schema": SCHEMA,
        "study_date": study_date,
        "timezone": TIMEZONE,
        "idempotency_key": f"ordinary-review-event:{event_id}:fast-capture:v1",
        "stable_evidence_refs": [
            {
                "kind": ORDINARY_REVIEW_REF_KIND,
                "locator": event_locator,
                "sha256": _sha256_value(event),
            }
        ],
        "source_facts": source_facts,
        "user_facts": user_facts,
        "identity_hint": identity,
        "answer_safe_context_anchor": (
            f"{source} {source_id} 的 {context} 非正确首答证据"
        ),
        "missing_fields": missing,
        # A canonical outcome is evidence, not formalization authorization.
        "formalization_authorized": False,
    }


def build_ordinary_review_capture_payload(
    event: dict[str, Any],
) -> dict[str, Any]:
    """Return the authoritative normalized payload for one ordinary outcome.

    The caller must independently prove that ``event`` is the exact committed
    canonical outcome.  This function performs the same answer-safety,
    eligibility, identity, and authorization checks as the legacy cold bridge,
    but does not read either ledger and does not write any state.
    """

    return _validate_capture_payload(_ordinary_review_capture_payload(event))


def _atomic_write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    tmp = Path(temporary)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if tmp.exists():
            tmp.unlink()


@contextmanager
def _ledger_lock(root: Path) -> Iterator[None]:
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".lock"
    with lock_path.open("a+") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _load_events(root: Path) -> list[dict[str, Any]]:
    path = root / "events.jsonl"
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line_no, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            continue
        try:
            event = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CaptureError(f"events.jsonl 第 {line_no} 行损坏：{exc}") from exc
        if not isinstance(event, dict) or event.get("schema") != EVENT_SCHEMA:
            raise CaptureError(f"events.jsonl 第 {line_no} 行 schema 非法")
        events.append(event)
    return events


def _event_index(events: list[dict[str, Any]]) -> tuple[set[str], dict[str, dict[str, Any]]]:
    event_ids: set[str] = set()
    by_key: dict[str, dict[str, Any]] = {}
    for event in events:
        event_id = str(event.get("event_id") or "")
        key = str(event.get("idempotency_key") or "")
        if not event_id or event_id in event_ids:
            raise CaptureError(f"账本含重复或空 event_id：{event_id}")
        if not key or key in by_key:
            raise CaptureError(f"账本含重复或空 idempotency_key：{key}")
        event_ids.add(event_id)
        by_key[key] = event
    return event_ids, by_key


def _needs_user_resolution_material(event: dict[str, Any]) -> dict[str, Any]:
    material = {
        "resolution_kind": event.get("resolution_kind"),
        "study_date": event.get("study_date"),
        "prior_batch_id": event.get("prior_batch_id"),
        "capture_id": event.get("capture_id"),
        "formal_id": event.get("formal_id"),
        "source_review_event_id": event.get("source_review_event_id"),
        "source_review_event_sha256": event.get("source_review_event_sha256"),
        "confirmation_event_id": event.get("confirmation_event_id"),
        "confirmation_event_sha256": event.get("confirmation_event_sha256"),
        "normal_receipt_sha256": event.get("normal_receipt_sha256"),
        "normal_receipt_transaction_id": event.get(
            "normal_receipt_transaction_id"
        ),
        "normal_receipt_journal_sha256": event.get(
            "normal_receipt_journal_sha256"
        ),
    }
    if material["resolution_kind"] not in {
        "confirmed_identity",
        "confirmed_identity_with_existing_receipt",
    }:
        raise CaptureError("needs_user resolution kind 非法")
    _validate_date(material["study_date"], "resolution.study_date")
    for label in (
        "prior_batch_id",
        "capture_id",
        "source_review_event_id",
        "confirmation_event_id",
    ):
        _validate_id(material[label], f"resolution.{label}")
    if not FORMAL_ID_RE.fullmatch(str(material["formal_id"] or "")):
        raise CaptureError("needs_user resolution formal_id 非法")
    for label in ("source_review_event_sha256", "confirmation_event_sha256"):
        _validate_sha(material[label], f"resolution.{label}")
    receipt_labels = (
        "normal_receipt_sha256",
        "normal_receipt_transaction_id",
        "normal_receipt_journal_sha256",
    )
    if material["resolution_kind"] == "confirmed_identity":
        if any(material[label] is not None for label in receipt_labels):
            raise CaptureError("仅身份确认的 resolution 不得混入 normal receipt")
    else:
        _validate_sha(
            material["normal_receipt_sha256"],
            "resolution.normal_receipt_sha256",
        )
        _validate_id(
            material["normal_receipt_transaction_id"],
            "resolution.normal_receipt_transaction_id",
        )
        _validate_sha(
            material["normal_receipt_journal_sha256"],
            "resolution.normal_receipt_journal_sha256",
        )
    return material


def _neutral_save_material(event: dict[str, Any]) -> dict[str, Any]:
    material = {
        "neutral_save_id": event.get("neutral_save_id"),
        "study_date": event.get("study_date"),
        "timezone": event.get("timezone"),
        "review_event_id": event.get("review_event_id"),
        "review_event_sha256": event.get("review_event_sha256"),
        "source_transaction_kind": event.get("source_transaction_kind"),
        "source_transaction_attestation_sha256": event.get(
            "source_transaction_attestation_sha256"
        ),
        "authorization_message_sha256": event.get(
            "authorization_message_sha256"
        ),
        "authorization_binding_sha256": event.get(
            "authorization_binding_sha256"
        ),
        "save_request_id": event.get("save_request_id"),
        "display_receipt_sha256": event.get("display_receipt_sha256"),
        "save_intent_receipt_sha256": event.get(
            "save_intent_receipt_sha256"
        ),
        "save_intent_receipt_ref": event.get("save_intent_receipt_ref"),
        "authorization_message_size_bytes": event.get(
            "authorization_message_size_bytes"
        ),
        "formalization_authorized": event.get("formalization_authorized"),
        "curation_eligible": event.get("curation_eligible"),
        "status": event.get("status"),
    }
    _validate_id(material["neutral_save_id"], "neutral_save.neutral_save_id")
    _validate_date(material["study_date"], "neutral_save.study_date")
    if material["timezone"] != TIMEZONE:
        raise CaptureError("neutral save timezone 非法")
    _validate_id(material["review_event_id"], "neutral_save.review_event_id")
    for label in (
        "review_event_sha256",
        "source_transaction_attestation_sha256",
        "authorization_message_sha256",
        "authorization_binding_sha256",
        "display_receipt_sha256",
        "save_intent_receipt_sha256",
    ):
        _validate_sha(material[label], f"neutral_save.{label}")
    request_id = str(material["save_request_id"] or "")
    if not SAVE_REQUEST_RE.fullmatch(request_id):
        raise CaptureError("neutral save request_id 非法")
    expected_intent_ref = str(
        MANAGED_RICH_INTENT_OBJECT_REL
        / f"{material['save_intent_receipt_sha256']}.json"
    )
    if material["save_intent_receipt_ref"] != expected_intent_ref:
        raise CaptureError("neutral save intent receipt 引用非法")
    message_size = material["authorization_message_size_bytes"]
    if (
        not isinstance(message_size, int)
        or isinstance(message_size, bool)
        or not 0 < message_size <= MAX_SAVE_AUTHORIZATION_MESSAGE_BYTES
    ):
        raise CaptureError("neutral save authorization message size 非法")
    if material["source_transaction_kind"] not in {
        "prepared_option_turn",
        "rich_morning_turn",
        "explicit_fast_intake_turn",
    }:
        raise CaptureError("neutral save source transaction kind 非法")
    if (
        material["formalization_authorized"] is not False
        or material["curation_eligible"] is not False
        or material["status"] != NEUTRAL_SAVE_STATUS
    ):
        raise CaptureError("neutral save 不得进入错题编纂或正式写入")
    return material


def replay(events: list[dict[str, Any]]) -> dict[str, Any]:
    _event_index(events)
    captures: dict[str, dict[str, Any]] = {}
    neutral_saves: dict[str, dict[str, Any]] = {}
    neutral_review_events: set[str] = set()
    batches: dict[str, dict[str, Any]] = {}
    result_event_ids: dict[tuple[str, str], str] = {}
    for event in events:
        event_type = event.get("event_type")
        if event_type == "fact_captured":
            capture_id = str(event["capture_id"])
            if capture_id in captures:
                raise CaptureError(f"重复 capture_id：{capture_id}")
            captures[capture_id] = {
                "capture_id": capture_id,
                "study_date": event["capture"]["study_date"],
                "recorded_at": event["created_at"],
                "payload_sha256": event["payload_sha256"],
                "quality_status": (
                    "awaiting_daily_curation"
                    if event["capture"]["formalization_authorized"]
                    else "captured_unconfirmed"
                ),
                "current_batch_id": None,
                "formal_id": event["capture"]["identity_hint"].get("formal_id") or None,
                "last_error": None,
                "formalization_authorized": event["capture"][
                    "formalization_authorized"
                ],
                "capture": event["capture"],
            }
        elif event_type == "neutral_saved":
            material = _neutral_save_material(event)
            if event.get("payload_sha256") != _sha256_value(material):
                raise CaptureError("neutral save payload_sha256 非法")
            save_id = str(material["neutral_save_id"])
            review_event_id = str(material["review_event_id"])
            if save_id in neutral_saves or review_event_id in neutral_review_events:
                raise CaptureError("同一复盘事件只能有一份中性保存记录")
            neutral_review_events.add(review_event_id)
            neutral_saves[save_id] = dict(material)
        elif event_type == "capture_authorized":
            capture_id = str(event["capture_id"])
            capture = captures.get(capture_id)
            if not capture:
                raise CaptureError(f"授权事件引用未知 capture：{capture_id}")
            if capture["quality_status"] != "captured_unconfirmed":
                raise CaptureError(f"capture 当前状态不能授权：{capture_id}")
            capture["formalization_authorized"] = True
            capture["quality_status"] = "awaiting_daily_curation"
        elif event_type == "needs_user_resolved":
            material = _needs_user_resolution_material(event)
            if event.get("resolution_sha256") != _sha256_value(material):
                raise CaptureError("needs_user resolution_sha256 非法")
            capture_id = str(material["capture_id"])
            capture = captures.get(capture_id)
            if not capture:
                raise CaptureError(f"needs_user resolution 引用未知 capture：{capture_id}")
            if capture["study_date"] != material["study_date"]:
                raise CaptureError("needs_user resolution 与 capture 学习日期不一致")
            if capture["quality_status"] != "needs_user":
                raise CaptureError(
                    f"只有 needs_user capture 可以追加 resolution：{capture_id}"
                )
            prior_batch = batches.get(str(material["prior_batch_id"]))
            if (
                not prior_batch
                or prior_batch.get("status") != "PARTIAL"
                or capture_id not in prior_batch.get("capture_ids", [])
                or (prior_batch.get("results", {}).get(capture_id) or {}).get(
                    "outcome"
                )
                != "needs_user"
                or capture.get("current_batch_id") != prior_batch.get("batch_id")
            ):
                raise CaptureError(
                    "needs_user resolution 未绑定已关闭 PARTIAL 批次的原始缺口"
                )
            capture["quality_status"] = "needs_user_resolved"
            capture["formal_id"] = material["formal_id"]
            capture["last_error"] = None
            capture["resolution"] = material
        elif event_type == "curation_batch_started":
            batch_id = str(event["batch_id"])
            capture_ids = list(event.get("capture_ids") or [])
            if batch_id in batches or not capture_ids:
                raise CaptureError(f"日终批次非法或重复：{batch_id}")
            if any(batch.get("status") == "active" for batch in batches.values()):
                raise CaptureError("同一时刻只允许一个 active 日终批次")
            if capture_ids != sorted(set(capture_ids)):
                raise CaptureError("日终批次 capture_ids 必须排序且唯一")
            study_date = _validate_date(event.get("study_date"), "batch.study_date")
            if event.get("timezone") != TIMEZONE:
                raise CaptureError("日终批次 timezone 非法")
            capture_set_sha = _validate_sha(
                event.get("capture_set_sha256"), "batch.capture_set_sha256"
            )
            if capture_set_sha != _sha256_value(capture_ids):
                raise CaptureError("日终批次 capture_set_sha256 与完整集合不一致")
            batch_schema = event.get("batch_schema")
            if batch_schema not in {
                LEGACY_BATCH_SCHEMA,
                BATCH_SCHEMA_V2,
                BATCH_SCHEMA,
            }:
                raise CaptureError("日终批次 schema 非法")
            attempt = _batch_attempt(event)
            prior_attempts = [
                int(batch.get("attempt", 1))
                for batch in batches.values()
                if batch.get("study_date") == study_date
                and batch.get("capture_set_sha256") == capture_set_sha
            ]
            if attempt != max(prior_attempts, default=0) + 1:
                raise CaptureError("同一冻结集合的日终批次 attempt 不连续")
            if batch_schema in {BATCH_SCHEMA_V2, BATCH_SCHEMA} and batch_id != _batch_id(
                study_date, capture_set_sha, attempt
            ):
                raise CaptureError("日终批次 ID 与日期、集合、attempt 不一致")
            if batch_schema == LEGACY_BATCH_SCHEMA and (
                attempt != 1
                or batch_id
                != f"CUR-{study_date.replace('-', '')}-{capture_set_sha[:12]}"
            ):
                raise CaptureError("legacy 日终批次身份非法")
            retry_of_batch_id = event.get("retry_of_batch_id")
            retry_capture_ids = list(event.get("retry_capture_ids") or [])
            carried_capture_ids = list(event.get("carried_capture_ids") or [])
            if attempt == 1:
                if retry_of_batch_id is not None or retry_capture_ids or carried_capture_ids:
                    raise CaptureError("首个日终 attempt 不得声明 retry/carry")
                retry_capture_ids = list(capture_ids)
            else:
                if batch_schema != BATCH_SCHEMA:
                    raise CaptureError("只有 v3 日终批次可以表达同集合重试")
                retry_of_batch_id = _validate_id(
                    retry_of_batch_id, "batch.retry_of_batch_id"
                )
                if (
                    retry_capture_ids != sorted(set(retry_capture_ids))
                    or carried_capture_ids != sorted(set(carried_capture_ids))
                    or sorted(retry_capture_ids + carried_capture_ids) != capture_ids
                    or set(retry_capture_ids) & set(carried_capture_ids)
                ):
                    raise CaptureError("日终重试的 retry/carry 分区非法")
                prior_batch = batches.get(retry_of_batch_id)
                if (
                    not prior_batch
                    or prior_batch.get("status") != "PARTIAL"
                    or prior_batch.get("study_date") != study_date
                    or prior_batch.get("capture_set_sha256") != capture_set_sha
                    or prior_batch.get("capture_ids") != capture_ids
                    or int(prior_batch.get("attempt", 1)) != attempt - 1
                    or set(prior_batch.get("results", {})) != set(capture_ids)
                ):
                    raise CaptureError("日终重试未绑定同集合的前一关闭 attempt")
                prior_success = sorted(
                    capture_id
                    for capture_id, value in prior_batch["results"].items()
                    if value.get("outcome") in SUCCESS_OUTCOMES
                )
                prior_retry = sorted(set(capture_ids) - set(prior_success))
                if carried_capture_ids != prior_success or retry_capture_ids != prior_retry:
                    raise CaptureError("日终重试分区与前一 attempt 结果不一致")
            retry_capture_id_set = set(retry_capture_ids)
            carried_capture_id_set = set(carried_capture_ids)
            for capture_id in capture_ids:
                capture = captures.get(capture_id)
                if not capture:
                    raise CaptureError(f"日终批次引用未知 capture：{capture_id}")
                if capture["study_date"] != study_date:
                    raise CaptureError("日终批次不能混入其他学习日期的 capture")
                if not capture["formalization_authorized"]:
                    raise CaptureError("日终批次不能包含未授权 capture")
                if capture_id in retry_capture_id_set:
                    allowed = {
                        "awaiting_daily_curation",
                        "curation_failed",
                        "needs_user_resolved",
                    }
                    if capture["quality_status"] not in allowed:
                        raise CaptureError(
                            f"capture 当前状态不能进入日终批次：{capture_id}"
                        )
                    if attempt > 1:
                        prior_outcome = prior_batch["results"][capture_id]["outcome"]
                        if (
                            prior_outcome == "needs_user"
                            and capture["quality_status"] != "needs_user_resolved"
                        ) or (
                            prior_outcome == "failed"
                            and capture["quality_status"] != "curation_failed"
                        ):
                            raise CaptureError("日终重试项未达到对应的可重试状态")
                    capture["quality_status"] = "curating"
                    capture["current_batch_id"] = batch_id
                elif capture_id in carried_capture_id_set:
                    prior_result = prior_batch["results"][capture_id]
                    expected_status = prior_result["outcome"]
                    if (
                        capture["quality_status"] != expected_status
                        or capture.get("formal_id") != prior_result.get("formal_id")
                    ):
                        raise CaptureError("日终重试 carry 项与前一成功结果不一致")
            batch_row = {
                "batch_id": batch_id,
                "study_date": study_date,
                "capture_ids": capture_ids,
                "capture_set_sha256": capture_set_sha,
                "attempt": attempt,
                "started_at": event.get("created_at"),
                "status": "active",
                "results": {},
            }
            if retry_of_batch_id is not None:
                batch_row.update(
                    {
                        "retry_of_batch_id": retry_of_batch_id,
                        "retry_capture_ids": retry_capture_ids,
                        "carried_capture_ids": carried_capture_ids,
                    }
                )
            batches[batch_id] = batch_row
        elif event_type == "curation_item_result":
            batch_id = str(event["batch_id"])
            capture_id = str(event["capture_id"])
            batch = batches.get(batch_id)
            capture = captures.get(capture_id)
            if not batch or not capture or capture_id not in batch["capture_ids"]:
                raise CaptureError("日终结果未绑定有效 batch/capture")
            if batch["status"] != "active":
                raise CaptureError("已关闭的日终批次不能再记录结果")
            if capture_id in batch["results"]:
                raise CaptureError(f"同一批次重复记录 capture 结果：{capture_id}")
            outcome = str(event["outcome"])
            if outcome not in RESULT_OUTCOMES:
                raise CaptureError(f"非法日终结果：{outcome}")
            carried_from_batch_id = event.get("carried_from_batch_id")
            carried_from_result_event_id = event.get("carried_from_result_event_id")
            if carried_from_batch_id is not None:
                if outcome not in SUCCESS_OUTCOMES:
                    raise CaptureError("carry 结果只能复用既有成功终态")
                source_batch = batches.get(str(carried_from_batch_id))
                source = (
                    source_batch.get("results", {}).get(capture_id)
                    if source_batch
                    else None
                )
                if (
                    batch.get("retry_of_batch_id") != carried_from_batch_id
                    or capture_id not in batch.get("carried_capture_ids", [])
                    or not source
                    or source.get("outcome") not in SUCCESS_OUTCOMES
                    or result_event_ids.get(
                        (str(carried_from_batch_id), capture_id)
                    )
                    != carried_from_result_event_id
                    or any(
                        source.get(key) != event.get(key)
                        for key in (
                            "outcome",
                            "formal_id",
                            "receipt_sha256",
                            "verification_sha256",
                            "reason",
                        )
                    )
                ):
                    raise CaptureError("carry 结果未精确绑定前一 attempt 的成功事件")
            elif capture_id in batch.get("carried_capture_ids", []):
                raise CaptureError("carry capture 缺少来源结果绑定")
            result_row = {
                "outcome": outcome,
                "formal_id": event.get("formal_id"),
                "receipt_sha256": event.get("receipt_sha256"),
                "verification_sha256": event.get("verification_sha256"),
                "reason": event.get("reason"),
            }
            if carried_from_batch_id is not None:
                result_row.update(
                    {
                        "carried_from_batch_id": carried_from_batch_id,
                        "carried_from_result_event_id": carried_from_result_event_id,
                    }
                )
            batch["results"][capture_id] = result_row
            result_event_ids[(batch_id, capture_id)] = str(event.get("event_id") or "")
            capture["current_batch_id"] = batch_id
            if outcome == "curated":
                capture["quality_status"] = "curated"
                capture["formal_id"] = event["formal_id"]
                capture["last_error"] = None
            elif outcome == "already_current":
                capture["quality_status"] = "already_current"
                capture["formal_id"] = event["formal_id"]
                capture["last_error"] = None
            elif outcome == "needs_user":
                capture["quality_status"] = "needs_user"
                capture["last_error"] = event.get("reason")
            else:
                capture["quality_status"] = "curation_failed"
                capture["last_error"] = event.get("reason")
        elif event_type == "curation_batch_closed":
            batch_id = str(event["batch_id"])
            batch = batches.get(batch_id)
            if not batch:
                raise CaptureError(f"关闭未知日终批次：{batch_id}")
            if batch["status"] != "active":
                raise CaptureError(f"日终批次已关闭：{batch_id}")
            missing = sorted(set(batch["capture_ids"]) - set(batch["results"]))
            if missing:
                raise CaptureError(f"日终批次收尾前仍缺结果：{missing}")
            global_closeout = str(event.get("global_closeout"))
            if global_closeout not in {"pass", "failed"}:
                raise CaptureError("日终批次 global_closeout 非法")
            failed = sorted(
                capture_id
                for capture_id, value in batch["results"].items()
                if value["outcome"] == "failed"
            )
            needs_user = sorted(
                capture_id
                for capture_id, value in batch["results"].items()
                if value["outcome"] == "needs_user"
            )
            expected_status = (
                "COMPLETE"
                if global_closeout == "pass" and not failed and not needs_user
                else "PARTIAL"
            )
            close_contract = event.get("close_contract")
            audit_receipt_sha = event.get("global_audit_receipt_sha256")
            if close_contract is None:
                # Historical v1/v2 close events predate aggregate audit receipts.
                # They remain replayable but are never emitted by new code.
                if audit_receipt_sha is not None:
                    raise CaptureError("legacy 日终收尾不得混入 aggregate receipt")
            elif close_contract == GLOBAL_AUDIT_CLOSE_CONTRACT:
                if global_closeout == "pass":
                    if failed:
                        raise CaptureError(
                            "failed 逐题结果不得声明 global_closeout=pass"
                        )
                    _validate_sha(
                        audit_receipt_sha,
                        "close.global_audit_receipt_sha256",
                    )
                elif audit_receipt_sha is not None:
                    raise CaptureError("failed 日终收尾不得声明 PASS audit receipt")
            else:
                raise CaptureError("日终批次 close_contract 非法")
            close_payload = {
                "batch_id": batch_id,
                "status": expected_status,
                "global_closeout": global_closeout,
                "needs_user_capture_ids": needs_user,
                "failed_capture_ids": failed,
            }
            if close_contract is not None:
                close_payload.update(
                    {
                        "close_contract": close_contract,
                        "global_audit_receipt_sha256": audit_receipt_sha,
                    }
                )
            if any(event.get(key) != value for key, value in close_payload.items()):
                raise CaptureError("日终批次收尾字段与逐题结果不一致")
            if event.get("close_sha256") != _sha256_value(close_payload):
                raise CaptureError("日终批次 close_sha256 非法")
            batch["status"] = expected_status
            batch["global_closeout"] = global_closeout
            batch["close_contract"] = close_contract or "legacy_unsealed"
            batch["global_audit_receipt_sha256"] = audit_receipt_sha
            batch["closed_at"] = event["created_at"]
        else:
            raise CaptureError(f"未知 event_type：{event_type}")
    pending_by_date: dict[str, list[str]] = {}
    for capture_id, capture in captures.items():
        if capture["quality_status"] in {
            "awaiting_daily_curation",
            "curation_failed",
            "needs_user_resolved",
        }:
            current_batch = batches.get(str(capture.get("current_batch_id") or ""))
            if current_batch and current_batch.get("status") == "active":
                continue
            pending_by_date.setdefault(capture["study_date"], []).append(capture_id)
    for values in pending_by_date.values():
        values.sort()
    state = {
        "schema": STATE_SCHEMA,
        "event_count": len(events),
        "captures": captures,
        "batches": batches,
        "pending_by_date": pending_by_date,
    }
    if neutral_saves:
        state["neutral_saves"] = neutral_saves
    return state


def _append_events(
    root: Path,
    events: list[dict[str, Any]],
    additions: list[dict[str, Any]],
) -> None:
    if not additions:
        raise CaptureError("追加事件集合不能为空")
    updated = [*events, *additions]
    state = replay(updated)
    ledger_text = "".join(
        json.dumps(item, ensure_ascii=False, sort_keys=True) + "\n" for item in updated
    )
    _atomic_write(root / "events.jsonl", ledger_text)
    _atomic_write(
        root / "state.json",
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
    )


def _append_event(root: Path, events: list[dict[str, Any]], event: dict[str, Any]) -> None:
    _append_events(root, events, [event])


def _capture_value(raw: Any, *, repo_root: str | Path = ".") -> dict[str, Any]:
    payload = _validate_capture_payload(raw)
    root = capture_root(repo_root)
    payload_hash = _sha256_value(payload)
    key = payload["idempotency_key"]
    capture_id = f"CAP-{payload['study_date'].replace('-', '')}-{hashlib.sha256(key.encode()).hexdigest()[:12]}"
    with _ledger_lock(root):
        events = _load_events(root)
        _, by_key = _event_index(events)
        existing = by_key.get(key)
        if existing:
            if existing.get("event_type") != "fact_captured" or existing.get("payload_sha256") != payload_hash:
                raise CaptureError("idempotency_key 已被不同事实载荷使用")
            current = replay(events)["captures"][existing["capture_id"]]
            try:
                producer_binding = publish_attestation(
                    descriptor_path=PRODUCER_BINDING_DESCRIPTOR_PATH,
                    repo_root=Path(repo_root),
                    subject="cs408",
                    capture_id=str(existing["capture_id"]),
                    capture_content_sha256=str(existing["payload_sha256"]),
                    recorded_at=str(existing["created_at"]),
                )
            except (
                OSError,
                ValueError,
                json.JSONDecodeError,
                ProducerBindingError,
            ) as exc:
                raise CaptureError(
                    "Capture Producer binding attestation 失败"
                ) from exc
            return {
                "status": "ALREADY_CAPTURED",
                "capture_id": existing["capture_id"],
                "quality_status": current["quality_status"],
                "payload_sha256": payload_hash,
                "created": False,
                "formal_write_count": 0,
                "producer_binding_status": producer_binding["status"],
                "producer_binding_attestation_path": producer_binding[
                    "attestation_path"
                ],
                "producer_binding_attestation_sha256": producer_binding[
                    "attestation_sha256"
                ],
            }
        event = {
            "schema": EVENT_SCHEMA,
            "event_type": "fact_captured",
            "event_id": f"CE-{hashlib.sha256(('capture:' + key).encode()).hexdigest()[:20]}",
            "idempotency_key": key,
            "created_at": _now_utc(),
            "capture_id": capture_id,
            "payload_sha256": payload_hash,
            "capture": payload,
        }
        _append_event(root, events, event)
        try:
            producer_binding = publish_attestation(
                descriptor_path=PRODUCER_BINDING_DESCRIPTOR_PATH,
                repo_root=Path(repo_root),
                subject="cs408",
                capture_id=capture_id,
                capture_content_sha256=payload_hash,
                recorded_at=str(event["created_at"]),
            )
        except (
            OSError,
            ValueError,
            json.JSONDecodeError,
            ProducerBindingError,
        ) as exc:
            raise CaptureError(
                "Capture Producer binding attestation 失败"
            ) from exc
    return {
        "status": (
            "CAPTURED_PENDING_CURATION"
            if payload["formalization_authorized"]
            else "CAPTURED_UNCONFIRMED"
        ),
        "capture_id": capture_id,
        "quality_status": (
            "awaiting_daily_curation"
            if payload["formalization_authorized"]
            else "captured_unconfirmed"
        ),
        "payload_sha256": payload_hash,
        "created": True,
        "formal_write_count": 0,
        "producer_binding_status": producer_binding["status"],
        "producer_binding_attestation_path": producer_binding[
            "attestation_path"
        ],
        "producer_binding_attestation_sha256": producer_binding[
            "attestation_sha256"
        ],
    }


def capture(payload_path: str | Path, *, repo_root: str | Path = ".") -> dict[str, Any]:
    return _capture_value(_read_json(payload_path), repo_root=repo_root)


def capture_review_event(
    event_id: str, *, repo_root: str | Path = "."
) -> dict[str, Any]:
    repo = _resolve_repo(repo_root)
    policy = _load_backflow_policy(repo)
    event = _review_event_by_id(repo, event_id)
    payload = _review_capture_payload(repo, event, policy)
    result = _capture_value(payload, repo_root=repo)
    return {
        **result,
        "review_event_id": event["event_id"],
        "study_date": payload["study_date"],
        "policy_schema": policy["schema"],
        "formal_write_authorized": False,
    }


def build_neutral_review_event(
    event_id: str,
    *,
    source_transaction_kind: str,
    source_transaction_attestation_sha256: str,
    authorization_message_sha256: str,
    authorization_binding_sha256: str,
    save_request_id: str,
    display_receipt_sha256: str,
    save_intent_receipt_sha256: str,
    save_intent_receipt_ref: str,
    authorization_message_size_bytes: int,
    repo_root: str | Path = ".",
    created_at: str | None = None,
) -> dict[str, Any]:
    """Build one canonical neutral-save event without changing capture state."""

    repo = _resolve_repo(repo_root)
    event = _review_event_by_id(repo, event_id)
    if (
        event.get("event_kind") != "outcome"
        or event.get("source") not in {"morning_review", "daily_practice"}
        or event.get("question_valid") is not True
        or event.get("formal_write_authorized") is not False
        or event.get("first_result") not in MORNING_CORRECT_RESULTS
    ):
        raise CaptureError("只有有效的正确首答可以中性保存")
    study_date = _validate_date(
        event.get("observed_date"), "neutral_save.observed_date"
    )
    event_id_value = _validate_id(event.get("event_id"), "review_event_id")
    transaction_kind = str(source_transaction_kind or "")
    allowed_kind = {
        "morning_review": {"prepared_option_turn", "rich_morning_turn"},
        "daily_practice": {"explicit_fast_intake_turn"},
    }
    if transaction_kind not in allowed_kind[str(event["source"])]:
        raise CaptureError("neutral save source transaction kind 非法")
    transaction_sha = _validate_sha(
        source_transaction_attestation_sha256,
        "source_transaction_attestation_sha256",
    )
    message_sha = _validate_sha(
        authorization_message_sha256, "authorization_message_sha256"
    )
    binding_sha = _validate_sha(
        authorization_binding_sha256, "authorization_binding_sha256"
    )
    request_id = str(save_request_id or "")
    if not SAVE_REQUEST_RE.fullmatch(request_id):
        raise CaptureError("neutral save request_id 非法")
    display_sha = _validate_sha(
        display_receipt_sha256, "display_receipt_sha256"
    )
    intent_sha = _validate_sha(
        save_intent_receipt_sha256, "save_intent_receipt_sha256"
    )
    intent_ref = str(save_intent_receipt_ref or "")
    expected_intent_ref = str(
        MANAGED_RICH_INTENT_OBJECT_REL / f"{intent_sha}.json"
    )
    if intent_ref != expected_intent_ref:
        raise CaptureError("neutral save intent receipt 引用非法")
    if (
        not isinstance(authorization_message_size_bytes, int)
        or isinstance(authorization_message_size_bytes, bool)
        or not 0
        < authorization_message_size_bytes
        <= MAX_SAVE_AUTHORIZATION_MESSAGE_BYTES
    ):
        raise CaptureError("neutral save authorization message size 非法")
    idempotency_key = f"neutral-save:{event_id_value}:v1"
    neutral_save_id = (
        f"NS-{study_date.replace('-', '')}-"
        f"{hashlib.sha256(idempotency_key.encode()).hexdigest()[:12]}"
    )
    material = {
        "neutral_save_id": neutral_save_id,
        "study_date": study_date,
        "timezone": TIMEZONE,
        "review_event_id": event_id_value,
        "review_event_sha256": _sha256_value(event),
        "source_transaction_kind": transaction_kind,
        "source_transaction_attestation_sha256": transaction_sha,
        "authorization_message_sha256": message_sha,
        "authorization_binding_sha256": binding_sha,
        "save_request_id": request_id,
        "display_receipt_sha256": display_sha,
        "save_intent_receipt_sha256": intent_sha,
        "save_intent_receipt_ref": intent_ref,
        "authorization_message_size_bytes": authorization_message_size_bytes,
        "formalization_authorized": False,
        "curation_eligible": False,
        "status": NEUTRAL_SAVE_STATUS,
    }
    payload_sha = _sha256_value(material)
    event_row = {
        "schema": EVENT_SCHEMA,
        "event_type": "neutral_saved",
        "event_id": (
            "NSE-"
            + hashlib.sha256(("neutral:" + event_id_value).encode()).hexdigest()[:20]
        ),
        "idempotency_key": idempotency_key,
        "created_at": created_at or _now_utc(),
        "payload_sha256": payload_sha,
        **material,
    }
    if _neutral_save_material(event_row) != material:
        raise CaptureError("neutral save canonical event 非法")
    return event_row


def save_neutral_review_event(
    event_id: str,
    *,
    source_transaction_kind: str,
    source_transaction_attestation_sha256: str,
    authorization_message_sha256: str,
    authorization_binding_sha256: str,
    save_request_id: str,
    display_receipt_sha256: str,
    save_intent_receipt_sha256: str,
    save_intent_receipt_ref: str,
    authorization_message_size_bytes: int,
    repo_root: str | Path = ".",
) -> dict[str, Any]:
    """Durably preserve one correct morning result without creating a wrong item.

    The managed caller must first validate the current authorization message and
    the completed answer transaction.  This ledger stores only their hashes and
    the canonical review event identity; it never stores the message body.
    """

    repo = _resolve_repo(repo_root)
    event_row = build_neutral_review_event(
        event_id,
        source_transaction_kind=source_transaction_kind,
        source_transaction_attestation_sha256=(
            source_transaction_attestation_sha256
        ),
        authorization_message_sha256=authorization_message_sha256,
        authorization_binding_sha256=authorization_binding_sha256,
        save_request_id=save_request_id,
        display_receipt_sha256=display_receipt_sha256,
        save_intent_receipt_sha256=save_intent_receipt_sha256,
        save_intent_receipt_ref=save_intent_receipt_ref,
        authorization_message_size_bytes=authorization_message_size_bytes,
        repo_root=repo,
    )
    root = capture_root(repo)
    root.mkdir(parents=True, exist_ok=True)
    ledger = root / "events.jsonl"
    state_path = root / "state.json"
    if not ledger.exists() and not state_path.exists():
        _atomic_write(ledger, "")
        _atomic_write(
            state_path,
            json.dumps(replay([]), ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
        )
    elif not ledger.is_file() or not state_path.is_file():
        raise CaptureError("中性保存事实账本未完整初始化")
    hot_root = root / ".capture-hot-writer-v1"
    try:
        import capture_hot_writer_408 as capture_hot

        with capture_hot.capture_ledger_lock(ledger) as lock_token:
            if not (hot_root / capture_hot.JOINT_MANIFEST_FILENAME).is_file():
                capture_hot.cold_bootstrap(
                    ledger,
                    state_path,
                    hot_root,
                    lock_token=lock_token,
                )
            committed = capture_hot.save_neutral_event(
                ledger,
                state_path,
                hot_root,
                lock_token=lock_token,
                event=event_row,
            )
    except (ImportError, OSError, RuntimeError) as exc:
        raise CaptureError(f"中性保存热事务失败：{exc}") from exc
    if committed.get("status") not in {
        "SAVED_NEUTRAL",
        "ALREADY_SAVED_NEUTRAL",
    } or not isinstance(committed.get("neutral_material"), dict):
        raise CaptureError("中性保存热事务终态非法")
    return {
        "event_status": committed["status"],
        **dict(committed["neutral_material"]),
        "created": bool(committed.get("created")),
        "receipt_sha256": committed.get("receipt_sha256"),
        "formal_write_authorized": False,
    }


def capture_ordinary_review_event(
    event_id: str, *, repo_root: str | Path = "."
) -> dict[str, Any]:
    """Capture ordinary review evidence without inferring curation authority."""

    repo = _resolve_repo(repo_root)
    event = _review_event_by_id(repo, event_id)
    payload = _ordinary_review_capture_payload(event)
    result = _capture_value(payload, repo_root=repo)
    return {
        **result,
        "ordinary_review_event_id": event["event_id"],
        "review_source": event["source"],
        "study_date": payload["study_date"],
        "formalization_authorized": False,
        "formal_write_authorized": False,
    }


def authorize_capture(
    *,
    capture_id: str,
    idempotency_key: str,
    repo_root: str | Path = ".",
) -> dict[str, Any]:
    """Rejected legacy surface retained only for a precise migration error."""

    raise CaptureError(
        "直接 capture_id/idempotency_key 授权已禁用；"
        "新授权必须经 managed current-message intent 入口"
    )


def authorize_capture_hot(
    *,
    capture_id: str,
    idempotency_key: str,
    repo_root: str | Path = ".",
    hot_root: str | Path | None = None,
    full: bool = False,
) -> dict[str, Any]:
    """Rejected legacy surface; it can no longer append an authorization."""

    raise CaptureError(
        "直接 capture hot 授权已禁用；恢复只能核验已存在的 canonical authorization event"
    )


def recover_existing_legacy_authorization(
    *,
    capture_id: str,
    authorization_event_id: str,
    authorization_event_sha256: str,
    fact_receipt_sha256: str,
    repo_root: str | Path = ".",
    hot_root: str | Path | None = None,
) -> dict[str, Any]:
    """Recover projections for one already-present proofless legacy event.

    This named cold-path recovery never appends an authorization.  It requires
    the exact existing legacy event line and the deterministic fact-capture
    commit receipt, then rebuilds only replayable capture state and hot indexes.
    """

    capture_id = _validate_id(capture_id, "capture_id")
    authorization_event_id = _validate_id(
        authorization_event_id, "authorization_event_id"
    )
    _validate_sha(
        authorization_event_sha256, "authorization_event_sha256"
    )
    _validate_sha(fact_receipt_sha256, "fact_receipt_sha256")
    repo = _resolve_repo(repo_root)
    root = capture_root(repo)
    ledger = root / "events.jsonl"
    state = root / "state.json"
    if not ledger.is_file() or not state.is_file():
        raise CaptureError("capture truth files 尚未准备")
    try:
        import capture_commit_index_408 as capture_commit
        import capture_hot_writer_408 as capture_hot
    except ImportError as exc:
        raise CaptureError("capture recovery runtime 不可用") from exc
    derived = (
        Path(hot_root).expanduser()
        if hot_root is not None
        else root / ".capture-hot-writer-v1"
    )
    if not derived.is_absolute():
        derived = repo / derived
    derived = derived.resolve()
    try:
        derived.relative_to(repo)
    except ValueError as exc:
        raise CaptureError("capture hot root 越出仓库") from exc
    with capture_hot.capture_ledger_lock(ledger) as token:
        raw_ledger = ledger.read_bytes()
        events: list[dict[str, Any]] = []
        matched_line: bytes | None = None
        for raw_line in raw_ledger.splitlines(keepends=True):
            if not raw_line.strip():
                continue
            if not raw_line.endswith(b"\n"):
                raise CaptureError("capture ledger 尾行不完整")
            try:
                current = json.loads(raw_line.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise CaptureError("capture ledger 含非法事件") from exc
            if not isinstance(current, dict):
                raise CaptureError("capture ledger 含非对象事件")
            events.append(current)
            if current.get("event_id") == authorization_event_id:
                if matched_line is not None:
                    raise CaptureError("legacy authorization event_id 重复")
                matched_line = raw_line
                authorization_event = current
        if matched_line is None:
            raise CaptureError("legacy authorization event 不存在")
        if hashlib.sha256(matched_line).hexdigest() != authorization_event_sha256:
            raise CaptureError("legacy authorization event SHA-256 不匹配")
        legacy_keys = {
            "schema",
            "event_type",
            "event_id",
            "idempotency_key",
            "created_at",
            "capture_id",
        }
        if (
            set(authorization_event) != legacy_keys
            or authorization_event.get("schema") != EVENT_SCHEMA
            or authorization_event.get("event_type") != "capture_authorized"
            or authorization_event.get("capture_id") != capture_id
        ):
            raise CaptureError("目标不是 proofless historical authorization")
        expected_state = replay(events)
        current_capture = expected_state["captures"].get(capture_id)
        if (
            not isinstance(current_capture, dict)
            or current_capture.get("formalization_authorized") is not True
            or current_capture.get("quality_status")
            != "awaiting_daily_curation"
        ):
            raise CaptureError("legacy authorization replay 未形成授权终态")
        with tempfile.TemporaryDirectory() as temporary:
            expected_state_path = Path(temporary) / "state.json"
            expected_state_path.write_text(
                json.dumps(
                    expected_state,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            try:
                bundle = capture_commit.cold_rebuild(
                    ledger,
                    expected_state_path,
                    state_replayer=replay,
                )
            except capture_commit.CommitIndexError as exc:
                raise CaptureError(
                    f"legacy recovery receipt rebuild 失败：{exc}"
                ) from exc
        matching_fact_receipts = [
            receipt
            for receipt in bundle["receipts"].values()
            if receipt.get("capture_id") == capture_id
            and receipt.get("receipt_sha256") == fact_receipt_sha256
        ]
        if len(matching_fact_receipts) != 1:
            raise CaptureError("fact receipt 未精确绑定 legacy capture")
        before_event_count = len(events)
        _atomic_write(
            state,
            json.dumps(
                expected_state,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )
        try:
            rebuilt = capture_hot.cold_bootstrap(
                ledger,
                state,
                derived,
                lock_token=token,
                reconcile=True,
            )
        except capture_hot.CaptureHotWriterError as exc:
            raise CaptureError(
                f"legacy authorization hot rebuild 失败：{exc}"
            ) from exc
        if int(rebuilt.get("ledger_event_count", -1)) != before_event_count:
            raise CaptureError("legacy recovery 意外改变 event count")
    return {
        "schema": "legacy_capture_authorization_recovery_v1",
        "status": "RECOVERED_EXISTING_LEGACY_AUTHORIZATION",
        "capture_id": capture_id,
        "authorization_event_id": authorization_event_id,
        "authorization_event_sha256": authorization_event_sha256,
        "fact_receipt_sha256": fact_receipt_sha256,
        "quality_status": "awaiting_daily_curation",
        "event_append_count": 0,
        "formal_write_count": 0,
        "hot_rebuild": rebuilt,
    }


def _bounded_external_file(
    path: Path,
    parent: Path,
    *,
    maximum: int,
    label: str,
) -> Path:
    try:
        parent_resolved = parent.resolve(strict=True)
        path_resolved = path.resolve(strict=True)
        path_resolved.relative_to(parent_resolved)
        metadata = path.lstat()
    except (OSError, ValueError) as exc:
        raise CaptureError(f"{label} 缺失或越界") from exc
    if (
        path.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_size > maximum
    ):
        raise CaptureError(f"{label} 必须是有界的非链接普通文件")
    return path_resolved


def _normal_receipt_resolution_evidence(
    repo: Path,
    *,
    capture_row: dict[str, Any],
    prior_batch_id: str,
    formal_id: str,
    source_event: dict[str, Any],
    confirmation_event: dict[str, Any],
    expected_receipt_sha256: str,
    runtime_root: Path,
) -> dict[str, str]:
    receipt_sha = _validate_sha(
        expected_receipt_sha256, "normal_receipt_sha256"
    )
    runtime = runtime_root.expanduser().resolve()
    receipt_root = runtime / "receipts" / _repo_key(repo)
    if not receipt_root.is_dir() or receipt_root.is_symlink():
        raise CaptureError("normal receipt 根目录缺失或不安全")
    matches: list[Path] = []
    for candidate in sorted(receipt_root.glob("*.json")):
        if candidate.is_symlink() or not candidate.is_file():
            continue
        path = _bounded_external_file(
            candidate,
            receipt_root,
            maximum=MAX_NORMAL_RECEIPT_BYTES,
            label="normal receipt",
        )
        if _sha256_file(path) == receipt_sha:
            matches.append(path)
    if len(matches) != 1:
        raise CaptureError("未找到唯一的 normal receipt 内容实体")
    receipt_path = matches[0]
    receipt = _read_json(receipt_path)
    if (
        not isinstance(receipt, dict)
        or receipt.get("schema") != NORMAL_RECEIPT_SCHEMA
        or receipt.get("status") != "COMMITTED"
    ):
        raise CaptureError("normal receipt schema/status 非法")
    try:
        receipt_repo = Path(str(receipt.get("repo_root") or "")).resolve(
            strict=True
        )
    except OSError as exc:
        raise CaptureError("normal receipt repo_root 非法") from exc
    if receipt_repo != repo:
        raise CaptureError("normal receipt 绑定了其他仓库")
    items = receipt.get("items")
    if (
        not isinstance(items, list)
        or len(items) != 1
        or not isinstance(items[0], dict)
        or items[0].get("formal_id") != formal_id
        or items[0].get("mode") != "redo"
    ):
        raise CaptureError("normal receipt 未唯一绑定确认后的 redo 身份")
    changed_files = receipt.get("changed_files")
    required_targets = {str(repo / "节点总表.md"), str(repo / f"{formal_id}.md")}
    if (
        not isinstance(changed_files, list)
        or not required_targets.issubset({str(value) for value in changed_files})
    ):
        raise CaptureError("normal receipt 未发布确认身份的必要正式表面")
    idempotency_key = str(receipt.get("idempotency_key") or "")
    expected_name = (
        hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest() + ".json"
    )
    if not idempotency_key or receipt_path.name != expected_name:
        raise CaptureError("normal receipt 路径与幂等键不一致")
    transaction_id = _validate_id(
        receipt.get("transaction_id"), "normal receipt.transaction_id"
    )
    transaction_root = runtime / "transactions" / _repo_key(repo)
    journal_raw = Path(str(receipt.get("journal") or "")).expanduser()
    if not journal_raw.is_absolute():
        raise CaptureError("normal receipt journal 必须是绝对路径")
    journal_path = _bounded_external_file(
        journal_raw,
        transaction_root,
        maximum=MAX_NORMAL_WAL_BYTES,
        label="normal receipt WAL",
    )
    journal = _read_json(journal_path)
    if (
        not isinstance(journal, dict)
        or journal.get("schema") != NORMAL_WAL_SCHEMA
        or journal.get("state") != "committed"
    ):
        raise CaptureError("normal receipt WAL schema/state 非法")
    for label in (
        "transaction_id",
        "idempotency_key",
        "payload_sha256",
        "repo_root",
    ):
        if journal.get(label) != receipt.get(label):
            raise CaptureError(f"normal receipt/WAL {label} 不一致")
    if journal.get("result_items") != items:
        raise CaptureError("normal receipt/WAL 正式结果不一致")
    payload_identity = journal.get("payload_identity")
    packages = (
        payload_identity.get("packages")
        if isinstance(payload_identity, dict)
        else None
    )
    if not isinstance(packages, list):
        raise CaptureError("normal receipt WAL 缺少 capture-bound package")
    source_event_sha = _sha256_value(source_event)
    matching_packages = []
    for package in packages:
        if not isinstance(package, dict):
            continue
        binding = package.get("capture_binding")
        if not isinstance(binding, dict):
            continue
        if (
            binding.get("batch_id") == prior_batch_id
            and binding.get("capture_id") == capture_row.get("capture_id")
            and binding.get("capture_payload_sha256")
            == capture_row.get("payload_sha256")
            and binding.get("stable_evidence_sha256") == source_event_sha
            and package.get("formal_id") == formal_id
            and package.get("mode") == "redo"
        ):
            matching_packages.append(package)
    if len(matching_packages) != 1:
        raise CaptureError("normal receipt WAL 未唯一绑定原 capture 与首答证据")
    package = matching_packages[0]
    if (
        str(source_event.get("event_id") or "")
        not in str(package.get("date_source") or "")
        or formal_id not in str(
            (package.get("capture_binding") or {}).get("identity_resolution") or ""
        )
    ):
        raise CaptureError("normal receipt WAL 缺少答案安全的身份确认链")
    if _parse_timestamp(
        receipt.get("committed_at"), "normal receipt.committed_at"
    ) < _parse_timestamp(
        confirmation_event.get("event_time"), "confirmation.event_time"
    ):
        raise CaptureError("normal receipt 早于用户身份确认事件")
    return {
        "normal_receipt_sha256": receipt_sha,
        "normal_receipt_transaction_id": transaction_id,
        "normal_receipt_journal_sha256": _sha256_file(journal_path),
    }


def _validate_needs_user_resolution_evidence(
    repo: Path,
    *,
    capture_row: dict[str, Any],
    prior_batch_id: str,
    formal_id: str,
    confirmation_event_id: str,
    normal_receipt_sha256: str | None,
    runtime_root: Path,
) -> dict[str, Any]:
    refs = [
        ref
        for ref in (capture_row.get("capture", {}).get("stable_evidence_refs") or [])
        if ref.get("kind") in {"review_loop_event", ORDINARY_REVIEW_REF_KIND}
    ]
    if len(refs) != 1:
        raise CaptureError("needs_user resolution 需要唯一 canonical review event")
    locator = str(refs[0].get("locator") or "")
    marker = "#event_id="
    if marker not in locator:
        raise CaptureError("needs_user resolution 的 review event 定位非法")
    source_event_id = _validate_id(
        locator.rsplit(marker, 1)[1], "resolution.source_review_event_id"
    )
    source_event = _review_event_by_id(repo, source_event_id)
    source_event_sha = _sha256_value(source_event)
    if refs[0].get("sha256") != source_event_sha:
        raise CaptureError("needs_user resolution 的源 review event 已漂移")
    route = source_event.get("route") or {}
    candidate = route.get("candidate") or {}
    candidate_id = str(candidate.get("candidate_id") or "")
    if route.get("kind") != "needs_user" or not candidate_id:
        raise CaptureError("源 review event 不是可由用户解决的身份候选")
    confirmation_id = _validate_id(
        confirmation_event_id, "confirmation_event_id"
    )
    confirmation = _review_event_by_id(repo, confirmation_id)
    if (
        confirmation.get("event_kind") != "candidate_confirmation"
        or confirmation.get("explicit_user_confirmation") is not True
        or confirmation.get("formal_write_authorized") is not False
        or confirmation.get("candidate_id") != candidate_id
        or confirmation.get("resolved_formal_node_id") != formal_id
    ):
        raise CaptureError("用户确认事件未唯一解决该 capture 的正式身份")
    receipt_evidence: dict[str, str | None]
    if normal_receipt_sha256 is None:
        resolution_kind = "confirmed_identity"
        receipt_evidence = {
            "normal_receipt_sha256": None,
            "normal_receipt_transaction_id": None,
            "normal_receipt_journal_sha256": None,
        }
    else:
        resolution_kind = "confirmed_identity_with_existing_receipt"
        receipt_evidence = _normal_receipt_resolution_evidence(
            repo,
            capture_row=capture_row,
            prior_batch_id=prior_batch_id,
            formal_id=formal_id,
            source_event=source_event,
            confirmation_event=confirmation,
            expected_receipt_sha256=normal_receipt_sha256,
            runtime_root=runtime_root,
        )
    return {
        "resolution_kind": resolution_kind,
        "study_date": capture_row["study_date"],
        "prior_batch_id": prior_batch_id,
        "capture_id": capture_row["capture_id"],
        "formal_id": formal_id,
        "source_review_event_id": source_event_id,
        "source_review_event_sha256": source_event_sha,
        "confirmation_event_id": confirmation_id,
        "confirmation_event_sha256": _sha256_value(confirmation),
        **receipt_evidence,
    }


def resolve_needs_user(
    *,
    study_date: str,
    capture_id: str,
    formal_id: str,
    confirmation_event_id: str,
    normal_receipt_sha256: str | None = None,
    idempotency_key: str,
    repo_root: str | Path = ".",
    runtime_root: str | Path = DEFAULT_RUNTIME_ROOT,
) -> dict[str, Any]:
    date = _validate_date(study_date)
    capture_id = _validate_id(capture_id, "capture_id")
    if not FORMAL_ID_RE.fullmatch(formal_id):
        raise CaptureError("formal_id 非法")
    confirmation_event_id = _validate_id(
        confirmation_event_id, "confirmation_event_id"
    )
    receipt_sha = (
        _validate_sha(normal_receipt_sha256, "normal_receipt_sha256")
        if normal_receipt_sha256 is not None
        else None
    )
    key = _validate_id(idempotency_key, "idempotency_key")
    repo = _resolve_repo(repo_root)
    root = capture_root(repo)
    runtime = Path(runtime_root).expanduser().resolve()
    with _ledger_lock(root):
        events = _load_events(root)
        _, by_key = _event_index(events)
        state = replay(events)
        existing = by_key.get(key)
        if existing:
            if existing.get("event_type") != "needs_user_resolved":
                raise CaptureError("idempotency_key 已被其他事件使用")
            material = _needs_user_resolution_material(existing)
            if (
                material["study_date"] != date
                or material["capture_id"] != capture_id
                or material["formal_id"] != formal_id
                or material["confirmation_event_id"] != confirmation_event_id
                or material["normal_receipt_sha256"] != receipt_sha
            ):
                raise CaptureError("idempotency_key 已被不同 needs_user resolution 使用")
            return {
                "status": "ALREADY_RESOLVED",
                **material,
                "resolution_sha256": existing["resolution_sha256"],
                "created": False,
            }
        current = state["captures"].get(capture_id)
        if not current or current.get("study_date") != date:
            raise CaptureError("显式日期未唯一绑定目标 capture")
        if current.get("quality_status") != "needs_user":
            resolution = current.get("resolution")
            if isinstance(resolution, dict):
                if (
                    resolution.get("study_date") == date
                    and resolution.get("formal_id") == formal_id
                    and resolution.get("confirmation_event_id")
                    == confirmation_event_id
                    and resolution.get("normal_receipt_sha256") == receipt_sha
                ):
                    return {
                        "status": "ALREADY_RESOLVED",
                        **resolution,
                        "resolution_sha256": _sha256_value(resolution),
                        "created": False,
                    }
            raise CaptureError("只有已关闭批次中的 needs_user capture 可以补录确认")
        prior_batch_id = str(current.get("current_batch_id") or "")
        prior_batch = state["batches"].get(prior_batch_id)
        if not prior_batch or prior_batch.get("status") != "PARTIAL":
            raise CaptureError("needs_user capture 未绑定已关闭 PARTIAL 批次")
        material = _validate_needs_user_resolution_evidence(
            repo,
            capture_row=current,
            prior_batch_id=prior_batch_id,
            formal_id=formal_id,
            confirmation_event_id=confirmation_event_id,
            normal_receipt_sha256=receipt_sha,
            runtime_root=runtime,
        )
        event = {
            "schema": EVENT_SCHEMA,
            "event_type": "needs_user_resolved",
            "event_id": f"CE-{hashlib.sha256(('resolve-needs-user:' + key).encode()).hexdigest()[:20]}",
            "idempotency_key": key,
            "created_at": _now_utc(),
            "resolution_sha256": _sha256_value(material),
            **material,
        }
        _append_event(root, events, event)
    return {
        "status": "NEEDS_USER_RESOLVED",
        **material,
        "resolution_sha256": event["resolution_sha256"],
        "created": True,
    }


def start_batch(
    study_date: str,
    idempotency_key: str,
    *,
    repo_root: str | Path = ".",
) -> dict[str, Any]:
    date = _validate_date(study_date)
    key = _validate_id(idempotency_key, "idempotency_key")
    root = capture_root(repo_root)
    with _ledger_lock(root):
        events = _load_events(root)
        _, by_key = _event_index(events)
        state = replay(events)
        existing = by_key.get(key)
        if existing:
            if (
                existing.get("event_type") != "curation_batch_started"
                or existing.get("study_date") != date
            ):
                raise CaptureError("idempotency_key 已被不同日终清单使用")
            return {
                "status": "ALREADY_STARTED",
                "batch_id": existing["batch_id"],
                "study_date": date,
                "capture_ids": existing["capture_ids"],
                "capture_set_sha256": existing["capture_set_sha256"],
                "attempt": _batch_attempt(existing),
                "retry_of_batch_id": existing.get("retry_of_batch_id"),
                "retry_capture_ids": list(
                    existing.get("retry_capture_ids") or existing["capture_ids"]
                ),
                "carried_capture_ids": list(
                    existing.get("carried_capture_ids") or []
                ),
                "created": False,
            }
        active = [
            batch
            for batch in state["batches"].values()
            if batch.get("status") == "active"
        ]
        if active:
            current = active[0]
            raise CaptureError(
                "已有 active 日终批次，必须先恢复或关闭："
                f"{current['batch_id']} ({current['study_date']})"
            )
        pending_capture_ids = list(state["pending_by_date"].get(date, []))
        if not pending_capture_ids:
            return {
                "status": "NO_PENDING_CAPTURE",
                "study_date": date,
                "capture_ids": [],
                "created": False,
            }
        pending_set = set(pending_capture_ids)
        latest_by_set: dict[str, dict[str, Any]] = {}
        for prior in state["batches"].values():
            if prior.get("study_date") != date or prior.get("status") != "PARTIAL":
                continue
            set_sha = str(prior.get("capture_set_sha256") or "")
            current = latest_by_set.get(set_sha)
            if current is None or int(prior.get("attempt", 1)) > int(
                current.get("attempt", 1)
            ):
                latest_by_set[set_sha] = prior
        retry_candidates: list[dict[str, Any]] = []
        blocked_retry_capture_ids: set[str] = set()
        for prior in latest_by_set.values():
            results = prior.get("results") or {}
            retry_ids = sorted(
                capture_id
                for capture_id, value in results.items()
                if value.get("outcome") not in SUCCESS_OUTCOMES
            )
            if not retry_ids or not (set(retry_ids) & pending_set):
                continue
            ready = all(
                (
                    results[capture_id].get("outcome") == "failed"
                    and state["captures"][capture_id].get("quality_status")
                    == "curation_failed"
                )
                or (
                    results[capture_id].get("outcome") == "needs_user"
                    and state["captures"][capture_id].get("quality_status")
                    == "needs_user_resolved"
                )
                for capture_id in retry_ids
            )
            if ready:
                retry_candidates.append(prior)
            else:
                blocked_retry_capture_ids.update(retry_ids)
        retry_of: dict[str, Any] | None = None
        if retry_candidates:
            retry_of = min(
                retry_candidates,
                key=lambda batch: (
                    str(batch.get("started_at") or ""),
                    str(batch.get("batch_id") or ""),
                ),
            )
            capture_ids = list(retry_of["capture_ids"])
            capture_set_sha = str(retry_of["capture_set_sha256"])
            attempt = int(retry_of.get("attempt", 1)) + 1
            retry_capture_ids = sorted(
                capture_id
                for capture_id, value in retry_of["results"].items()
                if value.get("outcome") not in SUCCESS_OUTCOMES
            )
            carried_capture_ids = sorted(
                set(capture_ids) - set(retry_capture_ids)
            )
        else:
            capture_ids = sorted(pending_set - blocked_retry_capture_ids)
            if not capture_ids:
                return {
                    "status": "RETRY_BLOCKED",
                    "study_date": date,
                    "capture_ids": [],
                    "blocked_capture_ids": sorted(blocked_retry_capture_ids),
                    "created": False,
                }
            capture_set_sha = _sha256_value(capture_ids)
            attempt = _next_batch_attempt(state, date, capture_set_sha)
            retry_capture_ids = list(capture_ids)
            carried_capture_ids = []
        batch_id = _batch_id(date, capture_set_sha, attempt)
        event = {
            "schema": EVENT_SCHEMA,
            "event_type": "curation_batch_started",
            "event_id": f"CE-{hashlib.sha256(('start:' + key).encode()).hexdigest()[:20]}",
            "idempotency_key": key,
            "created_at": _now_utc(),
            "batch_id": batch_id,
            "batch_schema": BATCH_SCHEMA,
            "study_date": date,
            "timezone": TIMEZONE,
            "capture_ids": capture_ids,
            "capture_set_sha256": capture_set_sha,
            "attempt": attempt,
        }
        if retry_of is not None:
            event.update(
                {
                    "retry_of_batch_id": retry_of["batch_id"],
                    "retry_capture_ids": retry_capture_ids,
                    "carried_capture_ids": carried_capture_ids,
                }
            )
        additions = [event]
        if retry_of is not None:
            source_result_event_ids = {
                (str(row.get("batch_id") or ""), str(row.get("capture_id") or "")):
                str(row.get("event_id") or "")
                for row in events
                if row.get("event_type") == "curation_item_result"
            }
            for carried_capture_id in carried_capture_ids:
                source = retry_of["results"][carried_capture_id]
                source_result_event_id = source_result_event_ids.get(
                    (str(retry_of["batch_id"]), carried_capture_id)
                )
                if not source_result_event_id:
                    raise CaptureError("前一 attempt 的成功结果事件缺失")
                result_payload = {
                    "batch_id": batch_id,
                    "capture_id": carried_capture_id,
                    "outcome": source["outcome"],
                    "formal_id": source.get("formal_id"),
                    "receipt_sha256": source.get("receipt_sha256"),
                    "verification_sha256": source.get("verification_sha256"),
                    "reason": source.get("reason"),
                }
                carry_key = f"curation-carry:{batch_id}:{carried_capture_id}"
                additions.append(
                    {
                        "schema": EVENT_SCHEMA,
                        "event_type": "curation_item_result",
                        "event_id": f"CE-{hashlib.sha256(('carry:' + carry_key).encode()).hexdigest()[:20]}",
                        "idempotency_key": carry_key,
                        "created_at": _now_utc(),
                        "result_sha256": _sha256_value(result_payload),
                        "carried_from_batch_id": retry_of["batch_id"],
                        "carried_from_result_event_id": source_result_event_id,
                        **result_payload,
                    }
                )
        _append_events(root, events, additions)
    return {
        "status": "CURATION_BATCH_STARTED",
        "batch_id": batch_id,
        "study_date": date,
        "capture_ids": capture_ids,
        "capture_set_sha256": capture_set_sha,
        "attempt": attempt,
        "retry_of_batch_id": retry_of["batch_id"] if retry_of else None,
        "retry_capture_ids": retry_capture_ids,
        "carried_capture_ids": carried_capture_ids,
        "created": True,
    }


def mark_result(
    *,
    batch_id: str,
    capture_id: str,
    outcome: str,
    idempotency_key: str,
    formal_id: str | None = None,
    receipt_sha256: str | None = None,
    verification_sha256: str | None = None,
    reason: str | None = None,
    repo_root: str | Path = ".",
) -> dict[str, Any]:
    batch_id = _validate_id(batch_id, "batch_id")
    capture_id = _validate_id(capture_id, "capture_id")
    key = _validate_id(idempotency_key, "idempotency_key")
    if outcome not in RESULT_OUTCOMES:
        raise CaptureError(f"outcome 必须是 {sorted(RESULT_OUTCOMES)}")
    if outcome == "curated":
        if not formal_id or not FORMAL_ID_RE.fullmatch(formal_id):
            raise CaptureError("curated 结果必须绑定合法 formal_id")
        if verification_sha256 is not None:
            raise CaptureError("curated 只能绑定 receipt_sha256")
        receipt_sha256 = _validate_sha(receipt_sha256, "receipt_sha256")
        verification_sha256 = None
        reason = None
    elif outcome == "already_current":
        if not formal_id or not FORMAL_ID_RE.fullmatch(formal_id):
            raise CaptureError("already_current 结果必须绑定合法 formal_id")
        if receipt_sha256 is not None:
            raise CaptureError("already_current 不能伪造 receipt_sha256")
        verification_sha256 = _validate_sha(
            verification_sha256, "verification_sha256"
        )
        receipt_sha256 = None
        reason = None
    else:
        if formal_id is not None or receipt_sha256 is not None or verification_sha256 is not None:
            raise CaptureError("needs_user/failed 不能绑定正式成功字段")
        formal_id = None
        receipt_sha256 = None
        verification_sha256 = None
        reason = _assert_answer_safe("reason", reason)
        if not reason:
            raise CaptureError("needs_user/failed 必须提供答案安全 reason")
    root = capture_root(repo_root)
    result_payload = {
        "batch_id": batch_id,
        "capture_id": capture_id,
        "outcome": outcome,
        "formal_id": formal_id,
        "receipt_sha256": receipt_sha256,
        "verification_sha256": verification_sha256,
        "reason": reason,
    }
    payload_hash = _sha256_value(result_payload)
    with _ledger_lock(root):
        events = _load_events(root)
        _, by_key = _event_index(events)
        state = replay(events)
        existing = by_key.get(key)
        if existing:
            if (
                existing.get("event_type") != "curation_item_result"
                or existing.get("result_sha256") != payload_hash
            ):
                raise CaptureError("idempotency_key 已被不同日终结果使用")
            return {"status": "ALREADY_RECORDED", **result_payload, "created": False}
        batch = state["batches"].get(batch_id)
        if not batch or capture_id not in batch["capture_ids"]:
            raise CaptureError("结果未绑定当前日终批次的 capture")
        if batch["status"] != "active":
            raise CaptureError("已关闭的日终批次不能再记录结果")
        capture_row = state["captures"].get(capture_id) or {}
        resolution = capture_row.get("resolution")
        if isinstance(resolution, dict) and outcome in SUCCESS_OUTCOMES:
            if formal_id != resolution.get("formal_id"):
                raise CaptureError(
                    "已补录 needs_user 的正式成功结果必须使用确认后的 formal_id"
                )
            if (
                resolution.get("resolution_kind")
                == "confirmed_identity_with_existing_receipt"
                and outcome != "already_current"
            ):
                raise CaptureError(
                    "已有精确 normal receipt 的补录项不得重放 apply；"
                    "必须 seal 后记录 already_current"
                )
        prior = batch["results"].get(capture_id)
        if prior:
            comparable = {
                "batch_id": batch_id,
                "capture_id": capture_id,
                **{
                    label: prior.get(label)
                    for label in (
                        "outcome",
                        "formal_id",
                        "receipt_sha256",
                        "verification_sha256",
                        "reason",
                    )
                },
            }
            if _sha256_value(comparable) != payload_hash:
                raise CaptureError("同一日终批次已记录不同的 capture 结果")
            return {"status": "ALREADY_RECORDED", **result_payload, "created": False}
        event = {
            "schema": EVENT_SCHEMA,
            "event_type": "curation_item_result",
            "event_id": f"CE-{hashlib.sha256(('result:' + key).encode()).hexdigest()[:20]}",
            "idempotency_key": key,
            "created_at": _now_utc(),
            "result_sha256": payload_hash,
            **result_payload,
        }
        _append_event(root, events, event)
    return {"status": "CURATION_RESULT_RECORDED", **result_payload, "created": True}


def _global_audit_batch_snapshot(batch: dict[str, Any]) -> dict[str, Any]:
    capture_ids = list(batch.get("capture_ids") or [])
    results = batch.get("results") or {}
    result_rows = []
    for capture_id in capture_ids:
        if capture_id not in results:
            continue
        row = {
            "capture_id": capture_id,
            "outcome": results[capture_id].get("outcome"),
            "formal_id": results[capture_id].get("formal_id"),
            "receipt_sha256": results[capture_id].get("receipt_sha256"),
            "verification_sha256": results[capture_id].get("verification_sha256"),
            "reason": results[capture_id].get("reason"),
        }
        if results[capture_id].get("carried_from_batch_id") is not None:
            row.update(
                {
                    "carried_from_batch_id": results[capture_id][
                        "carried_from_batch_id"
                    ],
                    "carried_from_result_event_id": results[capture_id][
                        "carried_from_result_event_id"
                    ],
                }
            )
        result_rows.append(row)
    return {
        "batch_id": batch.get("batch_id"),
        "study_date": batch.get("study_date"),
        "attempt": int(batch.get("attempt", 1)),
        "capture_ids": capture_ids,
        "capture_set_sha256": batch.get("capture_set_sha256"),
        "result_count": len(result_rows),
        "results": result_rows,
        "results_sha256": _sha256_value(result_rows),
    }


def _bounded_audit_file(repo: Path, relative: Path, label: str) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise CaptureError(f"{label} 路径非法")
    candidate = repo / relative
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(repo)
    except (OSError, ValueError) as exc:
        raise CaptureError(f"{label} 缺失或越界：{relative}") from exc
    if resolved != candidate.absolute() or candidate.is_symlink():
        raise CaptureError(f"{label} 不得经过符号链接：{relative}")
    mode = candidate.stat().st_mode
    if not stat.S_ISREG(mode):
        raise CaptureError(f"{label} 不是普通文件：{relative}")
    return candidate


def _audit_file_commitments(
    repo: Path, relative_paths: tuple[Path, ...] | list[Path], label: str
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for relative in sorted(set(relative_paths), key=lambda value: value.as_posix()):
        path = _bounded_audit_file(repo, relative, label)
        rows.append(
            {
                "path": relative.as_posix(),
                "sha256": _sha256_file(path),
                "size_bytes": path.stat().st_size,
            }
        )
    return rows


def _validate_signed_audit_origin(receipt: dict[str, Any]) -> None:
    """Validate the signed origin hint without binding it to today's mount."""
    raw = receipt.get("repo_root")
    if not isinstance(raw, str) or not raw or "\x00" in raw:
        raise CaptureError("aggregate audit receipt repo_root 来源提示非法")
    origin = Path(raw)
    if not origin.is_absolute() or str(origin) != raw:
        raise CaptureError("aggregate audit receipt repo_root 来源提示非法")


def _validate_audit_commitment_identity(
    commitments: object,
    expected_paths: tuple[Path, ...] | list[Path],
    label: str,
) -> None:
    """Bind a signed receipt to the exact repository-relative audit surface."""
    if not isinstance(commitments, list):
        raise CaptureError(f"{label} 相对路径身份非法")
    expected = sorted(
        {path.as_posix() for path in expected_paths}
    )
    actual: list[str] = []
    for row in commitments:
        if not isinstance(row, dict) or set(row) != {
            "path",
            "sha256",
            "size_bytes",
        }:
            raise CaptureError(f"{label} 相对路径身份非法")
        relative = row.get("path")
        digest = row.get("sha256")
        size = row.get("size_bytes")
        if (
            not isinstance(relative, str)
            or not isinstance(digest, str)
            or not SHA256_RE.fullmatch(digest)
            or not isinstance(size, int)
            or isinstance(size, bool)
            or size < 0
        ):
            raise CaptureError(f"{label} 相对路径身份非法")
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != relative:
            raise CaptureError(f"{label} 相对路径身份非法")
        actual.append(relative)
    if actual != expected:
        raise CaptureError(f"{label} 相对路径身份非法")


def _global_audit_object_paths(batch: dict[str, Any]) -> list[Path]:
    paths = list(GLOBAL_AUDIT_OBJECT_RELS)
    for result in (batch.get("results") or {}).values():
        formal_id = result.get("formal_id")
        if formal_id:
            if not FORMAL_ID_RE.fullmatch(str(formal_id)):
                raise CaptureError("aggregate audit 遇到非法 formal_id")
            paths.append(Path(f"{formal_id}.md"))
    return paths


def _global_audit_command(repo: Path) -> list[str]:
    del repo
    return [
        "python3",
        "scripts/intake_validate_408.py",
        "--repo",
        ".",
        "--full",
        "--json",
    ]


def _normalize_global_audit_report(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict) or raw.get("result") != "PASS":
        raise CaptureError("aggregate global audit 未返回 PASS")
    items = raw.get("items")
    if not isinstance(items, list) or len(items) > 2048:
        raise CaptureError("aggregate global audit items 非法或过大")
    normalized_items: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            raise CaptureError("aggregate global audit item 必须是 object")
        level = str(item.get("level") or "")
        if level not in {"PASS", "WARN"}:
            raise CaptureError("aggregate global audit 含非 PASS item")
        normalized_items.append(
            {
                "level": level,
                "check": str(item.get("check") or ""),
                "message": str(item.get("message") or ""),
            }
        )
    normalized = {"result": "PASS", "items": normalized_items}
    if len(_canonical(normalized)) > GLOBAL_AUDIT_MAX_REPORT_BYTES:
        raise CaptureError("aggregate global audit report 超过有界大小")
    return normalized


def _execute_global_audit(repo: Path) -> dict[str, Any]:
    command = [
        sys.executable,
        str(repo / "scripts" / "intake_validate_408.py"),
        "--repo",
        str(repo),
        "--full",
        "--json",
    ]
    try:
        completed = subprocess.run(
            command,
            cwd=repo,
            capture_output=True,
            text=True,
            timeout=GLOBAL_AUDIT_TIMEOUT_SECONDS,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise CaptureError("aggregate global audit 运行超时") from exc
    if completed.returncode != 0:
        tail = (completed.stdout or completed.stderr or "").strip()[-1000:]
        raise CaptureError(f"aggregate global audit 失败：{tail}")
    try:
        raw = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise CaptureError("aggregate global audit 输出不是 JSON") from exc
    return _normalize_global_audit_report(raw)


def _global_audit_key(root: Path, *, create: bool) -> bytes:
    key_path = root / GLOBAL_AUDIT_KEY_FILE
    if create and not key_path.exists():
        root.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(
                key_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
        except FileExistsError:
            descriptor = -1
        if descriptor >= 0:
            try:
                key = secrets.token_bytes(32)
                os.write(descriptor, key)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    try:
        resolved = key_path.resolve(strict=True)
    except OSError as exc:
        raise CaptureError("aggregate audit 签名密钥缺失") from exc
    if resolved != key_path.absolute() or key_path.is_symlink():
        raise CaptureError("aggregate audit 签名密钥不得经过符号链接")
    mode = key_path.stat().st_mode
    if not stat.S_ISREG(mode) or stat.S_IMODE(mode) != 0o600:
        raise CaptureError("aggregate audit 签名密钥必须是 0600 普通文件")
    key = key_path.read_bytes()
    if len(key) != 32:
        raise CaptureError("aggregate audit 签名密钥长度非法")
    return key


def _global_audit_receipt_root(root: Path, *, create: bool) -> Path:
    receipt_root = root / GLOBAL_AUDIT_RECEIPT_DIR
    if create:
        receipt_root.mkdir(parents=True, exist_ok=True)
    try:
        resolved = receipt_root.resolve(strict=True)
    except OSError as exc:
        raise CaptureError("aggregate audit receipt 目录缺失") from exc
    if resolved != receipt_root.absolute() or receipt_root.is_symlink():
        raise CaptureError("aggregate audit receipt 目录不得经过符号链接")
    if not receipt_root.is_dir():
        raise CaptureError("aggregate audit receipt 根不是目录")
    return receipt_root


def seal_global_audit(
    *, batch_id: str, repo_root: str | Path = "."
) -> dict[str, Any]:
    repo = _resolve_repo(repo_root)
    batch_id = _validate_id(batch_id, "batch_id")
    root = capture_root(repo)
    with _ledger_lock(root):
        events = _load_events(root)
        state = replay(events)
        batch = state["batches"].get(batch_id)
        if not batch or batch.get("status") != "active":
            raise CaptureError("aggregate audit 只接受 active 日终批次")
        missing = sorted(set(batch["capture_ids"]) - set(batch["results"]))
        if missing:
            raise CaptureError(f"aggregate audit 前仍缺逐题结果：{missing}")
        snapshot = _global_audit_batch_snapshot(batch)
        tool_before = _audit_file_commitments(
            repo, list(GLOBAL_AUDIT_TOOL_RELS), "aggregate audit tool"
        )
        object_before = _audit_file_commitments(
            repo,
            _global_audit_object_paths(batch),
            "aggregate audited object",
        )
        report = _execute_global_audit(repo)
        tool_after = _audit_file_commitments(
            repo, list(GLOBAL_AUDIT_TOOL_RELS), "aggregate audit tool"
        )
        object_after = _audit_file_commitments(
            repo,
            _global_audit_object_paths(batch),
            "aggregate audited object",
        )
        if tool_before != tool_after or object_before != object_after:
            raise CaptureError("aggregate audit 运行期间审计工具或对象发生漂移")
        command = _global_audit_command(repo)
        unsigned = {
            "schema": GLOBAL_AUDIT_RECEIPT_SCHEMA,
            "kind": GLOBAL_AUDIT_RECEIPT_KIND,
            "audit_version": GLOBAL_AUDIT_VERSION,
            "audit_status": "PASS",
            "repo_root": str(repo),
            "batch_snapshot": snapshot,
            "batch_snapshot_sha256": _sha256_value(snapshot),
            "audit_command": command,
            "audit_command_sha256": _sha256_value(command),
            "audit_tool_commitments": tool_after,
            "audit_tool_commitments_sha256": _sha256_value(tool_after),
            "audited_object_commitments": object_after,
            "audited_object_commitments_sha256": _sha256_value(object_after),
            "audit_report": report,
            "audit_report_sha256": _sha256_value(report),
        }
        key = _global_audit_key(root, create=True)
        signature = hmac.new(key, _canonical(unsigned), hashlib.sha256).hexdigest()
        receipt = {
            **unsigned,
            "attestation": {
                "algorithm": "hmac-sha256",
                "key_id": hashlib.sha256(key).hexdigest(),
                "signature": signature,
            },
        }
        receipt_bytes = _canonical(receipt)
        digest = hashlib.sha256(receipt_bytes).hexdigest()
        receipt_root = _global_audit_receipt_root(root, create=True)
        path = receipt_root / f"{digest}.json"
        if path.exists():
            existing = _bounded_audit_file(
                receipt_root, Path(path.name), "aggregate audit receipt"
            ).read_bytes()
            if existing != receipt_bytes:
                raise CaptureError("aggregate audit receipt 内容寻址冲突")
        else:
            _atomic_write(path, receipt_bytes.decode("utf-8"))
    return {
        "status": "GLOBAL_AUDIT_RECEIPT_SEALED",
        "batch_id": batch_id,
        "global_audit_receipt_sha256": digest,
        "receipt_path": str(path),
    }


def _validate_global_audit_receipt(
    repo: Path,
    root: Path,
    batch: dict[str, Any],
    expected_sha256: object,
    *,
    require_current_objects: bool,
) -> dict[str, Any]:
    expected_sha = _validate_sha(
        expected_sha256, "global_audit_receipt_sha256"
    )
    receipt_root = _global_audit_receipt_root(root, create=False)
    path = _bounded_audit_file(
        receipt_root,
        Path(f"{expected_sha}.json"),
        "aggregate audit receipt",
    )
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha:
        raise CaptureError("aggregate audit receipt 内容哈希不一致")
    try:
        receipt = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise CaptureError("aggregate audit receipt 不是 JSON") from exc
    if not isinstance(receipt, dict) or raw != _canonical(receipt):
        raise CaptureError("aggregate audit receipt 不是规范化内容寻址实体")
    attestation = receipt.get("attestation")
    if not isinstance(attestation, dict):
        raise CaptureError("aggregate audit receipt 缺 attestation")
    unsigned = {key: value for key, value in receipt.items() if key != "attestation"}
    key = _global_audit_key(root, create=False)
    expected_signature = hmac.new(
        key, _canonical(unsigned), hashlib.sha256
    ).hexdigest()
    if (
        attestation.get("algorithm") != "hmac-sha256"
        or attestation.get("key_id") != hashlib.sha256(key).hexdigest()
        or not hmac.compare_digest(
            str(attestation.get("signature") or ""), expected_signature
        )
    ):
        raise CaptureError("aggregate audit receipt attestation 非法")
    if (
        receipt.get("schema") != GLOBAL_AUDIT_RECEIPT_SCHEMA
        or receipt.get("kind") != GLOBAL_AUDIT_RECEIPT_KIND
        or receipt.get("audit_version") != GLOBAL_AUDIT_VERSION
        or receipt.get("audit_status") != "PASS"
    ):
        raise CaptureError("aggregate audit receipt 身份或 PASS 状态非法")
    # repo_root is an HMAC-covered origin hint, not a stable repository
    # identity: an intact repository may move between mounts.  Stable identity
    # is enforced below by the signed batch and exact repository-relative audit
    # surfaces, while an unsigned/tampered origin still fails attestation.
    _validate_signed_audit_origin(receipt)
    snapshot = _global_audit_batch_snapshot(batch)
    if (
        receipt.get("batch_snapshot") != snapshot
        or receipt.get("batch_snapshot_sha256") != _sha256_value(snapshot)
    ):
        raise CaptureError("aggregate audit receipt 与批次或逐题结果不一致")
    command = _global_audit_command(repo)
    if (
        receipt.get("audit_command") != command
        or receipt.get("audit_command_sha256") != _sha256_value(command)
    ):
        raise CaptureError("aggregate audit command/version 绑定非法")
    report = _normalize_global_audit_report(receipt.get("audit_report"))
    if receipt.get("audit_report_sha256") != _sha256_value(report):
        raise CaptureError("aggregate audit report 承诺非法")
    tools = receipt.get("audit_tool_commitments")
    objects = receipt.get("audited_object_commitments")
    if (
        receipt.get("audit_tool_commitments_sha256") != _sha256_value(tools)
        or receipt.get("audited_object_commitments_sha256")
        != _sha256_value(objects)
    ):
        raise CaptureError("aggregate audit 对象承诺哈希非法")
    _validate_audit_commitment_identity(
        tools,
        list(GLOBAL_AUDIT_TOOL_RELS),
        "aggregate audit tool commitments",
    )
    _validate_audit_commitment_identity(
        objects,
        _global_audit_object_paths(batch),
        "aggregate audited object commitments",
    )
    if require_current_objects:
        expected_tools = _audit_file_commitments(
            repo, list(GLOBAL_AUDIT_TOOL_RELS), "aggregate audit tool"
        )
        expected_objects = _audit_file_commitments(
            repo,
            _global_audit_object_paths(batch),
            "aggregate audited object",
        )
        if tools != expected_tools or objects != expected_objects:
            raise CaptureError("aggregate audit 后工具或审计对象已漂移")
    return receipt


def _persisted_close_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "batch_id": event["batch_id"],
        "status": event["status"],
        "global_closeout": event["global_closeout"],
        "needs_user_capture_ids": list(event.get("needs_user_capture_ids") or []),
        "failed_capture_ids": list(event.get("failed_capture_ids") or []),
    }
    if event.get("close_contract") is not None:
        payload.update(
            {
                "close_contract": event.get("close_contract"),
                "global_audit_receipt_sha256": event.get(
                    "global_audit_receipt_sha256"
                ),
            }
        )
    return payload


def close_batch(
    *,
    batch_id: str,
    global_closeout: str,
    idempotency_key: str,
    global_audit_receipt_sha256: str | None = None,
    repo_root: str | Path = ".",
) -> dict[str, Any]:
    batch_id = _validate_id(batch_id, "batch_id")
    key = _validate_id(idempotency_key, "idempotency_key")
    if global_closeout not in {"pass", "failed"}:
        raise CaptureError("global_closeout 必须是 pass 或 failed")
    repo = _resolve_repo(repo_root)
    root = capture_root(repo)
    with _ledger_lock(root):
        events = _load_events(root)
        _, by_key = _event_index(events)
        state = replay(events)
        existing = by_key.get(key)
        if existing:
            if (
                existing.get("event_type") != "curation_batch_closed"
                or existing.get("batch_id") != batch_id
                or existing.get("global_closeout") != global_closeout
            ):
                raise CaptureError("idempotency_key 已被不同批次收尾使用")
            persisted = _persisted_close_payload(existing)
            persisted_receipt = persisted.get("global_audit_receipt_sha256")
            if (
                global_audit_receipt_sha256 is not None
                and global_audit_receipt_sha256 != persisted_receipt
            ):
                raise CaptureError("日终批次已经按不同 aggregate receipt 关闭")
            if existing.get("close_sha256") != _sha256_value(persisted):
                raise CaptureError("已记录批次收尾的 close_sha256 非法")
            return {"event_status": "ALREADY_CLOSED", **persisted, "created": False}
        batch = state["batches"].get(batch_id)
        if not batch:
            raise CaptureError("未知 batch_id")
        if batch["status"] != "active":
            close_events = [
                event
                for event in events
                if event.get("event_type") == "curation_batch_closed"
                and event.get("batch_id") == batch_id
            ]
            if len(close_events) != 1:
                raise CaptureError("已关闭批次缺少唯一收尾事件")
            closed = close_events[0]
            if closed.get("global_closeout") != global_closeout:
                raise CaptureError("日终批次已经按不同语义关闭")
            persisted = _persisted_close_payload(closed)
            persisted_receipt = persisted.get("global_audit_receipt_sha256")
            if (
                global_audit_receipt_sha256 is not None
                and global_audit_receipt_sha256 != persisted_receipt
            ):
                raise CaptureError("日终批次已经按不同 aggregate receipt 关闭")
            if closed.get("close_sha256") != _sha256_value(persisted):
                raise CaptureError("已关闭批次的 close_sha256 非法")
            return {"event_status": "ALREADY_CLOSED", **persisted, "created": False}
        missing = sorted(set(batch["capture_ids"]) - set(batch["results"]))
        if missing:
            raise CaptureError(f"日终批次仍有未记录结果：{missing}")
        failed = sorted(
            capture_id
            for capture_id, value in batch["results"].items()
            if value["outcome"] == "failed"
        )
        needs_user = sorted(
            capture_id
            for capture_id, value in batch["results"].items()
            if value["outcome"] == "needs_user"
        )
        status = (
            "COMPLETE"
            if global_closeout == "pass" and not failed and not needs_user
            else "PARTIAL"
        )
        if global_closeout == "pass":
            if failed:
                raise CaptureError(
                    "failed 逐题结果不得声明 global_closeout=pass"
                )
            if global_audit_receipt_sha256 is None:
                raise CaptureError(
                    "global_closeout=pass 必须提供 aggregate audit receipt"
                )
            _validate_global_audit_receipt(
                repo,
                root,
                batch,
                global_audit_receipt_sha256,
                require_current_objects=True,
            )
        elif global_audit_receipt_sha256 is not None:
            raise CaptureError("global_closeout=failed 不得提供 PASS audit receipt")
        result_payload = {
            "batch_id": batch_id,
            "status": status,
            "global_closeout": global_closeout,
            "needs_user_capture_ids": needs_user,
            "failed_capture_ids": failed,
            "close_contract": GLOBAL_AUDIT_CLOSE_CONTRACT,
            "global_audit_receipt_sha256": global_audit_receipt_sha256,
        }
        result_hash = _sha256_value(result_payload)
        event = {
            "schema": EVENT_SCHEMA,
            "event_type": "curation_batch_closed",
            "event_id": f"CE-{hashlib.sha256(('close:' + key).encode()).hexdigest()[:20]}",
            "idempotency_key": key,
            "created_at": _now_utc(),
            "close_sha256": result_hash,
            **result_payload,
        }
        _append_event(root, events, event)
    return {"event_status": "CURATION_BATCH_CLOSED", **result_payload, "created": True}


def status(study_date: str | None, *, repo_root: str | Path = ".") -> dict[str, Any]:
    root = capture_root(repo_root)
    events = _load_events(root)
    state = replay(events)
    if not study_date:
        return state
    date = _validate_date(study_date)
    captures = {
        capture_id: value
        for capture_id, value in state["captures"].items()
        if value["study_date"] == date
    }
    neutral_saves = {
        save_id: value
        for save_id, value in state.get("neutral_saves", {}).items()
        if value["study_date"] == date
    }
    def batch_summary(batch: dict[str, Any] | None) -> dict[str, Any] | None:
        if batch is None:
            return None
        return {
            "batch_id": batch["batch_id"],
            "study_date": batch["study_date"],
            "attempt": int(batch.get("attempt", 1)),
            "status": batch["status"],
            "capture_set_sha256": batch["capture_set_sha256"],
            "completed_count": len(batch.get("results") or {}),
            "total_count": len(batch.get("capture_ids") or []),
            "retry_of_batch_id": batch.get("retry_of_batch_id"),
            "retry_capture_ids": list(batch.get("retry_capture_ids") or []),
            "carried_capture_ids": list(batch.get("carried_capture_ids") or []),
            "close_contract": batch.get("close_contract"),
            "global_audit_receipt_sha256": batch.get(
                "global_audit_receipt_sha256"
            ),
        }

    active = next(
        (
            batch
            for batch in state["batches"].values()
            if batch.get("status") == "active"
        ),
        None,
    )
    dated_batches = [
        batch
        for batch in state["batches"].values()
        if batch.get("study_date") == date
    ]
    latest = max(
        dated_batches,
        key=lambda batch: (int(batch.get("attempt", 1)), str(batch["batch_id"])),
        default=None,
    )
    return {
        "schema": STATE_SCHEMA,
        "study_date": date,
        "capture_count": len(captures),
        "neutral_save_count": len(neutral_saves),
        "pending_capture_ids": state["pending_by_date"].get(date, []),
        "captures": captures,
        "neutral_saves": neutral_saves,
        "active_batch": batch_summary(active),
        "latest_date_batch": batch_summary(latest),
    }


def reconcile(*, repo_root: str | Path = ".") -> dict[str, Any]:
    root = capture_root(repo_root)
    with _ledger_lock(root):
        events = _load_events(root)
        state = replay(events)
        _atomic_write(
            root / "state.json",
            json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        )
    return {"status": "RECONCILED", "event_count": len(events)}


def _audit_ordinary_review_binding(
    repo: Path,
    capture_id: str,
    stored_capture: dict[str, Any],
    refs: list[dict[str, Any]],
) -> int:
    ordinary_refs = [
        ref for ref in refs if ref.get("kind") == ORDINARY_REVIEW_REF_KIND
    ]
    if len(ordinary_refs) != 1:
        raise CaptureError(f"{capture_id} 缺少唯一普通学习 event 证据")
    forbidden_chain = {
        "review_loop_event",
        "morning_session_start",
        "morning_session_first",
        "morning_session_queue",
        "linked_practice_followup",
        "linked_practice_parent_trigger",
        "linked_practice_source",
        "personalization_selection_receipt",
    }
    kinds = {str(ref.get("kind")) for ref in refs}
    if kinds & forbidden_chain:
        raise CaptureError(f"{capture_id} 的普通学习证据不得混入晨间证据链")
    ref = ordinary_refs[0]
    locator = str(ref.get("locator") or "")
    prefix = f"{REVIEW_LOOP_LEDGER_REL}#event_id="
    if not locator.startswith(prefix):
        raise CaptureError(f"{capture_id} 的普通学习 event locator 非法")
    event = _review_event_by_id(repo, locator[len(prefix) :])
    expected_sha = _validate_sha(
        ref.get("sha256"), f"{capture_id}.ordinary_review_loop_event.sha256"
    )
    if _sha256_value(event) != expected_sha:
        raise CaptureError(f"{capture_id} 的 ordinary_review_loop_event SHA-256 不一致")

    expected = _validate_capture_payload(_ordinary_review_capture_payload(event))
    if stored_capture.get("study_date") != expected["study_date"]:
        raise CaptureError(f"{capture_id} 的 study_date 与 ordinary review event 不一致")
    if (
        stored_capture.get("source_facts", {}).get("source_id")
        != expected["source_facts"]["source_id"]
    ):
        raise CaptureError(f"{capture_id} 的 source_id 与 ordinary review event 不一致")
    if stored_capture.get("identity_hint") != expected["identity_hint"]:
        raise CaptureError(f"{capture_id} 的 formal identity 与 ordinary review event 不一致")
    if stored_capture.get("user_facts") != expected["user_facts"]:
        raise CaptureError(f"{capture_id} 的直接 provenance 与 ordinary review event 不一致")
    if stored_capture.get("formalization_authorized") is not False:
        raise CaptureError(f"{capture_id} 不得由 ordinary review event 自动授权正式编纂")
    if stored_capture != expected:
        raise CaptureError(f"{capture_id} 的 ordinary review capture 载荷与事件绑定不一致")
    return 1


def _audit_review_evidence_refs(repo: Path, state: dict[str, Any]) -> int:
    checked = 0
    for capture_id, row in state["captures"].items():
        refs = row["capture"].get("stable_evidence_refs") or []
        kinds = {str(ref.get("kind")) for ref in refs}
        if ORDINARY_REVIEW_REF_KIND in kinds:
            checked += _audit_ordinary_review_binding(
                repo, capture_id, row["capture"], refs
            )
            continue
        if "review_loop_event" not in kinds:
            continue
        required = {
            "review_loop_event",
            "morning_session_start",
            "morning_session_first",
            "morning_session_queue",
        }
        if not required.issubset(kinds):
            raise CaptureError(f"{capture_id} 缺少晨间回流证据链")
        linked_required = {
            "linked_practice_followup",
            "linked_practice_parent_trigger",
            "linked_practice_source",
            "personalization_selection_receipt",
        }
        review_ref = next(
            (ref for ref in refs if ref.get("kind") == "review_loop_event"), None
        )
        review_prefix = f"{REVIEW_LOOP_LEDGER_REL}#event_id="
        if review_ref and str(review_ref.get("locator", "")).startswith(review_prefix):
            bound_review = _review_event_by_id(
                repo, str(review_ref["locator"])[len(review_prefix) :]
            )
            if bound_review.get("practice_mode") in {"original", "variant"} and not (
                linked_required.issubset(kinds)
            ):
                raise CaptureError(f"{capture_id} 的动态关联练习缺少专用证据链")
            if bound_review.get("practice_mode") in {"original", "variant"}:
                source_ref_row = next(
                    ref
                    for ref in refs
                    if ref.get("kind") == "linked_practice_source"
                )
                selection_ref_row = next(
                    ref
                    for ref in refs
                    if ref.get("kind") == "personalization_selection_receipt"
                )
                source_path, source_ref = _linked_source_path(
                    repo, source_ref_row.get("locator")
                )
                try:
                    verified_source = validate_source_for_practice(
                        repo,
                        source_ref,
                        source_ref_row.get("sha256"),
                        practice_mode=str(bound_review.get("practice_mode")),
                        formal_node_id=bound_review.get("formal_node_id"),
                        variant_of_formal_node_id=bound_review.get(
                            "variant_of_formal_node_id"
                        ),
                    )
                    validate_selection_receipt(
                        repo,
                        selection_ref_row.get("locator"),
                        selection_ref_row.get("sha256"),
                        expected={
                            "slice_id": bound_review.get("context_slice_id"),
                            "trigger_event_id": bound_review.get(
                                "parent_trigger_event_id"
                            ),
                            "candidate_id": bound_review.get("candidate_id"),
                            "practice_mode": bound_review.get("practice_mode"),
                            "formal_node_id": bound_review.get("formal_node_id"),
                            "anchor_formal_node_id": bound_review.get(
                                "anchor_formal_node_id"
                            ),
                            "source_id": bound_review.get("source_id"),
                            "source_ref": source_ref,
                            "source_sha256": bound_review.get("source_sha256"),
                            "mechanism_key": bound_review.get("mechanism_key"),
                            "knowledge_point": bound_review.get("knowledge_point"),
                            "review_unit_id": bound_review.get("review_unit_id"),
                        },
                        source_path=verified_source["path"],
                    )
                except LinkedPracticeSourceError as exc:
                    raise CaptureError(str(exc)) from exc
        if kinds & linked_required and not linked_required.issubset(kinds):
            raise CaptureError(f"{capture_id} 缺少关联练习证据链")
        for ref in refs:
            kind = str(ref.get("kind"))
            locator = str(ref.get("locator"))
            expected_sha = str(ref.get("sha256"))
            if kind in {"review_loop_event", "linked_practice_parent_trigger"}:
                prefix = f"{REVIEW_LOOP_LEDGER_REL}#event_id="
                if not locator.startswith(prefix):
                    raise CaptureError(f"{capture_id} 的 review event locator 非法")
                event = _review_event_by_id(repo, locator[len(prefix) :])
                if kind == "linked_practice_parent_trigger" and (
                    event.get("event_kind") != "outcome"
                    or event.get("practice_mode")
                ):
                    raise CaptureError(f"{capture_id} 的 linked parent trigger 非法")
                actual_sha = _sha256_value(event)
            elif kind in {
                "morning_session_start",
                "morning_session_first",
                "linked_practice_followup",
            }:
                marker = "#sequence="
                if marker not in locator:
                    raise CaptureError(f"{capture_id} 的 session locator 非法")
                relative, raw_sequence = locator.rsplit(marker, 1)
                try:
                    sequence = int(raw_sequence)
                except ValueError as exc:
                    raise CaptureError(f"{capture_id} 的 session sequence 非法") from exc
                if kind == "morning_session_start" and sequence != 1:
                    raise CaptureError(f"{capture_id} 的 session start sequence 非法")
                path = (repo / relative).resolve()
                try:
                    path.relative_to(repo)
                except ValueError as exc:
                    raise CaptureError(f"{capture_id} 的 session locator 越界") from exc
                try:
                    session_events = [
                        json.loads(line)
                        for line in path.read_text(encoding="utf-8").splitlines()
                        if line.strip()
                    ]
                    selected = session_events[sequence - 1]
                except (OSError, IndexError, json.JSONDecodeError) as exc:
                    raise CaptureError(f"{capture_id} 的 session 事件不可读") from exc
                if selected.get("sequence") != sequence:
                    raise CaptureError(f"{capture_id} 的 session sequence 漂移")
                if kind == "linked_practice_followup" and selected.get(
                    "event_type"
                ) != "followup_scheduled":
                    raise CaptureError(f"{capture_id} 的 followup 事件类型非法")
                actual_sha = _sha256_value(selected)
            elif kind in {
                "morning_session_queue",
                "linked_practice_source",
                "personalization_selection_receipt",
            }:
                if kind == "linked_practice_source":
                    path, _ = _linked_source_path(repo, locator)
                elif kind == "personalization_selection_receipt":
                    try:
                        path, _, _ = resolve_selection_receipt(repo, locator)
                    except LinkedPracticeSourceError as exc:
                        raise CaptureError(str(exc)) from exc
                else:
                    path = (repo / locator).resolve()
                    try:
                        path.relative_to(repo)
                    except ValueError as exc:
                        raise CaptureError(f"{capture_id} 的 queue locator 越界") from exc
                if not path.is_file():
                    raise CaptureError(f"{capture_id} 的文件证据缺失")
                actual_sha = _sha256_file(path)
            else:
                continue
            if actual_sha != expected_sha:
                raise CaptureError(f"{capture_id} 的 {kind} SHA-256 不一致")
            checked += 1
    return checked


def _verify_neutral_source_transaction(
    repo: Path, material: dict[str, Any]
) -> None:
    source_kind = str(material["source_transaction_kind"])
    source_sha = str(material["source_transaction_attestation_sha256"])

    def read_json(path: Path, *, expected_sha: str | None = None) -> tuple[dict[str, Any], bytes]:
        try:
            resolved = path.resolve()
            resolved.relative_to(repo)
            info = path.lstat()
        except (OSError, ValueError) as exc:
            raise CaptureError("中性保存 source transaction 回执缺失") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise CaptureError("中性保存 source transaction 回执类型非法")
        try:
            raw = path.read_bytes()
            value = json.loads(raw)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise CaptureError("中性保存 source transaction 回执损坏") from exc
        if not isinstance(value, dict):
            raise CaptureError("中性保存 source transaction 回执内容非法")
        if expected_sha is not None and hashlib.sha256(raw).hexdigest() != expected_sha:
            raise CaptureError("中性保存 source transaction 回执哈希漂移")
        return value, raw

    def canonical(value: Any, *, trailing_lf: bool) -> bytes:
        raw = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return raw + (b"\n" if trailing_lf else b"")

    try:
        if source_kind == "prepared_option_turn":
            root = repo / (
                "wiki/study_vaults/408-full/state/managed-408-turn-broker"
            )
            attestation, _ = read_json(
                root / "objects" / "transactions" / f"{source_sha}.json",
                expected_sha=source_sha,
            )
            request_id = str(attestation.get("request_id") or "")
            payload = dict(attestation)
            transaction_id = payload.pop("transaction_id", None)
            expected_id = (
                "MTA-"
                + hashlib.sha256(canonical(payload, trailing_lf=True))
                .hexdigest()[:24]
                .upper()
            )
            if (
                attestation.get("schema")
                != "managed_408_transaction_attestation_v1"
                or attestation.get("kind") != "frozen_prepared_option_turn"
                or transaction_id != expected_id
                or attestation.get("display_receipt_sha256")
                != material["display_receipt_sha256"]
                or attestation.get("review_event_id")
                != material["review_event_id"]
                or attestation.get("session_result")
                not in MORNING_CORRECT_RESULTS
                or attestation.get("reply_body_stored") is not False
                or attestation.get("formal_write_count") != 0
                or not SHA256_RE.fullmatch(
                    str(attestation.get("broker_build_sha256") or "")
                )
            ):
                raise CaptureError("中性保存 option source transaction 绑定非法")
            key_sha = hashlib.sha256(
                canonical(
                    {"namespace": "transaction", "logical_key": request_id},
                    trailing_lf=True,
                )
            ).hexdigest()
            pointer, _ = read_json(
                root / "pointers" / "transaction" / f"{key_sha}.json"
            )
            if pointer != {
                "schema": "managed_408_turn_broker_pointer_v1",
                "namespace": "transaction",
                "key_sha256": key_sha,
                "object_sha256": source_sha,
            }:
                raise CaptureError("中性保存 option source pointer 绑定非法")
        elif source_kind == "rich_morning_turn":
            root = repo / (
                "wiki/study_vaults/408-full/state/managed-408-rich-intake-turn"
            )
            attestation, _ = read_json(
                root / "objects" / "attestation" / f"{source_sha}.json",
                expected_sha=source_sha,
            )
            request_id = str(attestation.get("request_id") or "")
            payload = dict(attestation)
            attestation_id = payload.pop("attestation_id", None)
            expected_id = (
                "MRI-"
                + hashlib.sha256(canonical(payload, trailing_lf=True))
                .hexdigest()[:24]
                .upper()
            )
            if (
                attestation.get("schema")
                != "managed_408_rich_intake_attestation_v1"
                or attestation.get("kind") != "rich_morning_finalized"
                or attestation_id != expected_id
                or attestation.get("display_receipt_sha256")
                != material["display_receipt_sha256"]
                or attestation.get("review_event_id")
                != material["review_event_id"]
                or attestation.get("reply_body_stored") is not False
                or attestation.get("formal_write_count") != 0
                or not SHA256_RE.fullmatch(
                    str(attestation.get("broker_build_sha256") or "")
                )
                or not SHA256_RE.fullmatch(
                    str(attestation.get("rich_build_sha256") or "")
                )
            ):
                raise CaptureError("中性保存 rich source transaction 绑定非法")
            key_sha = hashlib.sha256(
                canonical(
                    {
                        "namespace": "attestation_request",
                        "logical_key": request_id,
                    },
                    trailing_lf=True,
                )
            ).hexdigest()
            pointer, _ = read_json(
                root
                / "pointers"
                / "attestation_request"
                / f"{key_sha}.json"
            )
            if pointer != {
                "schema": "managed_408_rich_intake_pointer_v1",
                "namespace": "attestation_request",
                "key_sha256": key_sha,
                "object_sha256": source_sha,
            }:
                raise CaptureError("中性保存 rich source pointer 绑定非法")
        elif source_kind == "explicit_fast_intake_turn":
            if source_sha != material["save_intent_receipt_sha256"]:
                raise CaptureError("中性保存 fast source intent 绑定非法")
        else:
            raise CaptureError("中性保存 source transaction kind 非法")
    except CaptureError as exc:
        raise CaptureError(
            "中性保存 source transaction 回执验证失败"
        ) from exc
    except (OSError, RuntimeError, ValueError) as exc:
        raise CaptureError("中性保存 source transaction 回执验证失败") from exc


def _audit_neutral_saves(repo: Path, state: dict[str, Any]) -> int:
    checked = 0
    for save_id, material in state.get("neutral_saves", {}).items():
        event = _review_event_by_id(repo, str(material["review_event_id"]))
        intent_ref = str(material["save_intent_receipt_ref"])
        intent_path = (repo / intent_ref).resolve()
        try:
            intent_path.relative_to(repo)
            info = intent_path.lstat()
        except (ValueError, OSError) as exc:
            raise CaptureError(f"{save_id} 的保存意图回执缺失") from exc
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
            raise CaptureError(f"{save_id} 的保存意图回执类型非法")
        try:
            intent_raw = intent_path.read_bytes()
            intent = json.loads(intent_raw)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise CaptureError(f"{save_id} 的保存意图回执损坏") from exc
        source_kind = material.get("source_transaction_kind")
        is_morning = source_kind in {
            "prepared_option_turn",
            "rich_morning_turn",
        }
        expected_intent_schema = (
            "managed_408_save_current_result_intent_v1"
            if is_morning
            else "managed_408_fast_intake_intent_v1"
        )
        expected_intent_kind = (
            "save_current_result_intent"
            if is_morning
            else "explicit_fast_intake_intent"
        )
        if (
            not isinstance(intent, dict)
            or hashlib.sha256(intent_raw).hexdigest()
            != material.get("save_intent_receipt_sha256")
            or intent.get("schema") != expected_intent_schema
            or intent.get("kind") != expected_intent_kind
            or intent.get("request_id") != material.get("save_request_id")
            or intent.get("display_receipt_sha256")
            != material.get("display_receipt_sha256")
            or intent.get("authorization_message_sha256")
            != material.get("authorization_message_sha256")
            or intent.get("authorization_message_size_bytes")
            != material.get("authorization_message_size_bytes")
            or intent.get("authorization_binding_sha256")
            != material.get("authorization_binding_sha256")
            or intent.get("authorization_body_stored") is not False
            or intent.get("formal_write_count") != 0
        ):
            raise CaptureError(f"{save_id} 的保存意图证据绑定非法")
        if is_morning and (
            intent.get("source_transaction_kind") != source_kind
            or intent.get("source_transaction_attestation_sha256")
            != material.get("source_transaction_attestation_sha256")
            or intent.get("review_event_id") != material.get("review_event_id")
        ):
            raise CaptureError(f"{save_id} 的晨间保存意图来源绑定非法")
        if (not is_morning) and (
            source_kind != "explicit_fast_intake_turn"
            or material.get("source_transaction_attestation_sha256")
            != material.get("save_intent_receipt_sha256")
            or intent.get("observed_date") != material.get("study_date")
            or event.get("session_id") != intent.get("session_id")
            or event.get("item_id") != intent.get("item_id")
            or event.get("first_result")
            != intent.get("canonical_first_result")
        ):
            raise CaptureError(f"{save_id} 的快速入库保存意图来源绑定非法")
        _verify_neutral_source_transaction(repo, material)
        authorization_material = {
            "schema": "managed_408_explicit_authorization_binding_v1",
            "request_id": material["save_request_id"],
            "display_receipt_sha256": material["display_receipt_sha256"],
            "authorization_message_sha256": material[
                "authorization_message_sha256"
            ],
            "authorization_message_size_bytes": material[
                "authorization_message_size_bytes"
            ],
        }
        if (
            event.get("event_kind") != "outcome"
            or event.get("source")
            != ("morning_review" if is_morning else "daily_practice")
            or event.get("question_valid") is not True
            or event.get("formal_write_authorized") is not False
            or event.get("first_result") not in MORNING_CORRECT_RESULTS
            or event.get("observed_date") != material.get("study_date")
            or _sha256_value(event) != material.get("review_event_sha256")
            or material.get("formalization_authorized") is not False
            or material.get("curation_eligible") is not False
            or material.get("status") != NEUTRAL_SAVE_STATUS
            or hashlib.sha256(
                _canonical(authorization_material) + b"\n"
            ).hexdigest()
            != material.get("authorization_binding_sha256")
        ):
            raise CaptureError(f"{save_id} 的中性保存证据链非法")
        checked += 1
    return checked


def audit(*, repo_root: str | Path = ".") -> dict[str, Any]:
    repo = _resolve_repo(repo_root)
    root = capture_root(repo)
    events = _load_events(root)
    for event in events:
        if event.get("event_type") != "fact_captured":
            continue
        normalized = _validate_capture_payload(event.get("capture"))
        if normalized != event.get("capture"):
            raise CaptureError(
                f"{event.get('capture_id')} 的事实载荷不是规范化答案安全形式"
            )
        if event.get("payload_sha256") != _sha256_value(normalized):
            raise CaptureError(f"{event.get('capture_id')} 的 payload_sha256 不一致")
    expected = replay(events)
    needs_user_resolution_count = 0
    carried_result_count = 0
    for event in events:
        if event.get("event_type") == "needs_user_resolved":
            material = _needs_user_resolution_material(event)
            capture_row = expected["captures"].get(str(material["capture_id"]))
            if not isinstance(capture_row, dict):
                raise CaptureError("needs_user resolution 引用未知 capture")
            verified = _validate_needs_user_resolution_evidence(
                repo,
                capture_row=capture_row,
                prior_batch_id=str(material["prior_batch_id"]),
                formal_id=str(material["formal_id"]),
                confirmation_event_id=str(material["confirmation_event_id"]),
                normal_receipt_sha256=(
                    str(material["normal_receipt_sha256"])
                    if material["normal_receipt_sha256"] is not None
                    else None
                ),
                runtime_root=DEFAULT_RUNTIME_ROOT,
            )
            if verified != material:
                raise CaptureError("needs_user resolution 与外部证据链不一致")
            needs_user_resolution_count += 1
        elif (
            event.get("event_type") == "curation_item_result"
            and event.get("carried_from_batch_id") is not None
        ):
            carried_result_count += 1
    aggregate_receipt_count = 0
    legacy_unsealed_close_count = 0
    for event in events:
        if event.get("event_type") != "curation_batch_closed":
            continue
        if event.get("close_contract") is None:
            legacy_unsealed_close_count += 1
            continue
        if event.get("global_closeout") != "pass":
            continue
        batch = expected["batches"].get(str(event.get("batch_id") or ""))
        if batch is None:
            raise CaptureError("aggregate audit receipt 引用未知已关闭批次")
        _validate_global_audit_receipt(
            repo,
            root,
            batch,
            event.get("global_audit_receipt_sha256"),
            require_current_objects=False,
        )
        aggregate_receipt_count += 1
    state_path = root / "state.json"
    if not state_path.is_file():
        raise CaptureError("state.json 缺失；运行 reconcile")
    try:
        actual = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise CaptureError(f"state.json 损坏：{exc}") from exc
    if actual != expected:
        raise CaptureError("state.json 与事件账本重放结果不一致；运行 reconcile")
    review_ref_count = _audit_review_evidence_refs(repo, expected)
    neutral_save_count = _audit_neutral_saves(repo, expected)
    ordinary_ref_count = sum(
        1
        for row in expected["captures"].values()
        if any(
            ref.get("kind") == ORDINARY_REVIEW_REF_KIND
            for ref in row["capture"].get("stable_evidence_refs") or []
        )
    )
    return {
        "status": "PASS",
        "event_count": len(events),
        "capture_count": len(expected["captures"]),
        "neutral_save_count": neutral_save_count,
        "batch_count": len(expected["batches"]),
        "review_evidence_ref_count": review_ref_count,
        "ordinary_review_evidence_ref_count": ordinary_ref_count,
        "needs_user_resolution_count": needs_user_resolution_count,
        "carried_result_count": carried_result_count,
        "aggregate_global_audit_receipt_count": aggregate_receipt_count,
        "legacy_unsealed_close_count": legacy_unsealed_close_count,
    }


def _print(value: dict[str, Any]) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def main() -> int:
    parser = argparse.ArgumentParser(description="408 学习中事实捕获与日终编纂清单账本")
    parser.add_argument("--repo", default=".")
    sub = parser.add_subparsers(dest="command", required=True)

    capture_parser = sub.add_parser("capture")
    capture_parser.add_argument("payload")

    review_capture_parser = sub.add_parser("capture-review-event")
    review_capture_parser.add_argument("--event-id", required=True)

    ordinary_capture_parser = sub.add_parser("capture-ordinary-review-event")
    ordinary_capture_parser.add_argument("--event-id", required=True)

    authorize_parser = sub.add_parser("authorize")
    authorize_parser.add_argument("--capture-id", required=True)
    authorize_parser.add_argument("--idempotency-key", required=True)

    authorize_hot_parser = sub.add_parser("authorize-hot")
    authorize_hot_parser.add_argument("--capture-id", required=True)
    authorize_hot_parser.add_argument("--idempotency-key", required=True)
    authorize_hot_parser.add_argument("--capture-hot-root")
    authorize_hot_parser.add_argument("--full", action="store_true")

    recover_legacy_parser = sub.add_parser(
        "recover-existing-legacy-authorization"
    )
    recover_legacy_parser.add_argument("--capture-id", required=True)
    recover_legacy_parser.add_argument(
        "--authorization-event-id", required=True
    )
    recover_legacy_parser.add_argument(
        "--authorization-event-sha256", required=True
    )
    recover_legacy_parser.add_argument(
        "--fact-receipt-sha256", required=True
    )
    recover_legacy_parser.add_argument("--capture-hot-root")

    resolve_parser = sub.add_parser("resolve-needs-user")
    resolve_parser.add_argument("--date", required=True)
    resolve_parser.add_argument("--capture-id", required=True)
    resolve_parser.add_argument("--formal-id", required=True)
    resolve_parser.add_argument("--confirmation-event-id", required=True)
    resolve_parser.add_argument("--normal-receipt-sha256")
    resolve_parser.add_argument("--idempotency-key", required=True)

    start_parser = sub.add_parser("start-batch")
    start_parser.add_argument("--date", required=True)
    start_parser.add_argument("--idempotency-key", required=True)

    result_parser = sub.add_parser("mark-result")
    result_parser.add_argument("--batch-id", required=True)
    result_parser.add_argument("--capture-id", required=True)
    result_parser.add_argument("--outcome", choices=sorted(RESULT_OUTCOMES), required=True)
    result_parser.add_argument("--idempotency-key", required=True)
    result_parser.add_argument("--formal-id")
    result_parser.add_argument("--receipt-sha256")
    result_parser.add_argument("--verification-sha256")
    result_parser.add_argument("--reason")

    close_parser = sub.add_parser("close-batch")
    close_parser.add_argument("--batch-id", required=True)
    close_parser.add_argument("--global-closeout", choices=("pass", "failed"), required=True)
    close_parser.add_argument("--idempotency-key", required=True)
    close_parser.add_argument("--global-audit-receipt-sha256")

    seal_audit_parser = sub.add_parser("seal-global-audit")
    seal_audit_parser.add_argument("--batch-id", required=True)

    status_parser = sub.add_parser("status")
    status_parser.add_argument("--date")
    sub.add_parser("reconcile")
    sub.add_parser("audit")
    args = parser.parse_args()

    try:
        if args.command == "capture":
            value = capture(args.payload, repo_root=args.repo)
        elif args.command == "capture-review-event":
            value = capture_review_event(args.event_id, repo_root=args.repo)
        elif args.command == "capture-ordinary-review-event":
            value = capture_ordinary_review_event(
                args.event_id, repo_root=args.repo
            )
        elif args.command == "authorize":
            value = authorize_capture(
                capture_id=args.capture_id,
                idempotency_key=args.idempotency_key,
                repo_root=args.repo,
            )
        elif args.command == "authorize-hot":
            value = authorize_capture_hot(
                capture_id=args.capture_id,
                idempotency_key=args.idempotency_key,
                repo_root=args.repo,
                hot_root=args.capture_hot_root,
                full=args.full,
            )
        elif args.command == "recover-existing-legacy-authorization":
            value = recover_existing_legacy_authorization(
                capture_id=args.capture_id,
                authorization_event_id=args.authorization_event_id,
                authorization_event_sha256=(
                    args.authorization_event_sha256
                ),
                fact_receipt_sha256=args.fact_receipt_sha256,
                repo_root=args.repo,
                hot_root=args.capture_hot_root,
            )
        elif args.command == "resolve-needs-user":
            value = resolve_needs_user(
                study_date=args.date,
                capture_id=args.capture_id,
                formal_id=args.formal_id,
                confirmation_event_id=args.confirmation_event_id,
                normal_receipt_sha256=args.normal_receipt_sha256,
                idempotency_key=args.idempotency_key,
                repo_root=args.repo,
            )
        elif args.command == "start-batch":
            value = start_batch(
                args.date, args.idempotency_key, repo_root=args.repo
            )
        elif args.command == "mark-result":
            value = mark_result(
                batch_id=args.batch_id,
                capture_id=args.capture_id,
                outcome=args.outcome,
                idempotency_key=args.idempotency_key,
                formal_id=args.formal_id,
                receipt_sha256=args.receipt_sha256,
                verification_sha256=args.verification_sha256,
                reason=args.reason,
                repo_root=args.repo,
            )
        elif args.command == "close-batch":
            value = close_batch(
                batch_id=args.batch_id,
                global_closeout=args.global_closeout,
                idempotency_key=args.idempotency_key,
                global_audit_receipt_sha256=args.global_audit_receipt_sha256,
                repo_root=args.repo,
            )
        elif args.command == "seal-global-audit":
            value = seal_global_audit(
                batch_id=args.batch_id,
                repo_root=args.repo,
            )
        elif args.command == "status":
            value = status(args.date, repo_root=args.repo)
        elif args.command == "reconcile":
            value = reconcile(repo_root=args.repo)
        else:
            value = audit(repo_root=args.repo)
    except (CaptureError, OSError) as exc:
        _print({"status": "FAILED", "error": str(exc)})
        return 1
    _print(value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
