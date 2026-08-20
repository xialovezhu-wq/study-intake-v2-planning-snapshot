#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    from producer_binding_attestation import (
        ProducerBindingError,
        publish_attestation,
    )
except ModuleNotFoundError:
    _producer_binding_spec = importlib.util.spec_from_file_location(
        "math_producer_binding_attestation",
        Path(__file__).with_name("producer_binding_attestation.py"),
    )
    if _producer_binding_spec is None or _producer_binding_spec.loader is None:
        raise
    _producer_binding_module = importlib.util.module_from_spec(
        _producer_binding_spec
    )
    _producer_binding_spec.loader.exec_module(_producer_binding_module)
    ProducerBindingError = _producer_binding_module.ProducerBindingError
    publish_attestation = _producer_binding_module.publish_attestation


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
EVENTS_PATH = ROOT / "快速入库事件.jsonl"
LOCK_PATH = ROOT / ".快速入库事件.jsonl.lock"
SOURCE_STAGING_ROOT = ROOT / "快速入库来源"
SOURCE_LOCK_PATH = ROOT / ".快速入库来源.lock"
FORMAL_LOCK_PATH = ROOT / ".正式层写入.lock"
REVIEW_LOG_PATH = ROOT / "复习记录.jsonl"
UNITS_PATH = ROOT / "复习单元.json"
CARDS_DIR = REPO_ROOT / "错题知识网络" / "错题卡"
WRONGNET_SNAPSHOT_PATH = REPO_ROOT / "错题知识网络" / "生成" / "wrong_questions.json"
WIKI_ROOT = REPO_ROOT / "错题知识网络" / "wiki"
WRONGNET_TOOL_PATH = REPO_ROOT / "错题知识网络" / "scripts" / "wrongnet.py"
PRODUCER_BINDING_DESCRIPTOR_PATH = ROOT / "schema" / "producer-binding-v1.json"

CAPTURE_SCHEMA = "math-fast-intake-capture-v1"
CAPTURE_SCHEMA_V2 = "math-fast-intake-capture-v2"
AMENDMENT_SCHEMA = "math-fast-intake-amendment-v1"
SOURCE_STAGE_SCHEMA = "math-fast-intake-source-stage-v1"
SOURCE_BUNDLE_SCHEMA = "math-fast-intake-source-bundle-v1"
FREEZE_SCHEMA = "math-fast-intake-freeze-v1"
CLOSEOUT_SCHEMA = "math-fast-intake-closeout-v2"
LEDGER_SCHEMA = "math-fast-intake-ledger-v1"
STATUS_SCHEMA = "math-fast-intake-status-v1"

CARD_ID_PATTERN = re.compile(r"^(GS|LA|PR)-\d{3,}$")
ATTEMPT_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:/-]{1,240}$")
EVENT_ID_PATTERN = re.compile(
    r"^(SCORE|MFI-CAP|MFI-AMD|MFI-FREEZE|MFI-ABORT|MFI-PREP|MFI-CLOSE|MFI-INVALID)-[0-9a-f]{24}$"
)
HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")

ALLOWED_ACTIONS = {
    "record_wrong",
    "record_recurrence",
    "update_representation",
    "mastery_candidate",
}
ALLOWED_RESULTS = {"wrong", "unstable", "correct", "unresolved"}
ALLOWED_ORIGINS = {
    "user_observed",
    "user_confirmed",
    "source_verified",
    "assistant_inferred",
    "assistant_explained",
    "unresolved",
}
USER_EVIDENCE_ORIGINS = {"user_observed", "user_confirmed"}
ALLOWED_BREAK_KINDS = {
    "knowledge",
    "concept",
    "condition",
    "method_trigger",
    "method",
    "calculation",
    "expression",
    "identity",
    "unknown",
}
HISTORY_KINDS = {"wrong", "recurrence", "mastery", "representation", "none"}
SOURCE_ROLES = {"question", "solution", "solution_text", "user_work", "reference"}
TEACHING_SPEAKERS = {"user", "assistant"}
TEACHING_KINDS = {
    "reasoning", "hint", "correction", "explanation", "restatement", "answer"
}
MAX_EPISODE_EVIDENCE_BYTES = 128 * 1024
SOURCE_MEDIA = {
    ".png": ("image/png", b"\x89PNG\r\n\x1a\n"),
    ".jpg": ("image/jpeg", b"\xff\xd8\xff"),
    ".jpeg": ("image/jpeg", b"\xff\xd8\xff"),
    ".webp": ("image/webp", None),
    ".pdf": ("application/pdf", b"%PDF-"),
}
SOLUTION_TEXT_MEDIA = {
    ".txt": "text/plain; charset=utf-8",
    ".md": "text/markdown; charset=utf-8",
}
LOCAL_PATH_MARKERS = (
    "/Users/",
    "/Volumes/",
    "/private/",
    "/var/",
    "/tmp/",
    "file://",
    "~/",
)
MAX_SOURCE_ARTIFACTS = 8
MAX_SOURCE_ARTIFACT_BYTES = 25 * 1024 * 1024
MAX_SOURCE_BUNDLE_BYTES = 100 * 1024 * 1024


class QuickIntakeError(ValueError):
    """快速证据事件无法在身份与完整性门禁内安全处理。"""


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def stable_event_id(prefix: str, value: Any) -> str:
    return f"{prefix}-{sha256_value(value)[:24]}"


def now_local() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


def validate_date(value: Any, field: str = "study_date") -> str:
    if not isinstance(value, str):
        raise QuickIntakeError(f"{field} 必须是 YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise QuickIntakeError(f"{field} 不是有效日期：{value}") from exc
    if parsed.isoformat() != value:
        raise QuickIntakeError(f"{field} 必须是 YYYY-MM-DD")
    return value


def validate_hash(value: Any, field: str, *, required: bool = True) -> str | None:
    if value in (None, "") and not required:
        return None
    if not isinstance(value, str) or not HASH_PATTERN.fullmatch(value):
        raise QuickIntakeError(f"{field} 必须是 64 位小写 SHA-256")
    return value


def require_text(value: Any, field: str, *, max_length: int = 4000) -> str:
    if not isinstance(value, str) or not value.strip():
        raise QuickIntakeError(f"{field} 必须是非空文本")
    text = value.strip()
    if len(text) > max_length:
        raise QuickIntakeError(f"{field} 超过 {max_length} 字符")
    return text


def reject_local_absolute_paths(value: Any, field: str) -> None:
    if isinstance(value, str):
        if any(marker in value for marker in LOCAL_PATH_MARKERS) or re.search(
            r"(?<![A-Za-z0-9_])[A-Za-z]:[\\/]", value
        ):
            raise QuickIntakeError(f"{field} 不得包含本机绝对路径")
        return
    if isinstance(value, dict):
        for key, nested in value.items():
            reject_local_absolute_paths(nested, f"{field}.{key}")
        return
    if isinstance(value, list):
        for index, nested in enumerate(value):
            reject_local_absolute_paths(nested, f"{field}[{index}]")


def canonical_solution_text(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise QuickIntakeError(f"{field} 必须是非空文本")
    text = value.replace("\r\n", "\n").replace("\r", "\n").strip()
    text = require_text(text, field, max_length=65536)
    reject_local_absolute_paths(text, field)
    return text


def canonical_solution_text_bytes(value: Any, field: str) -> bytes:
    return (canonical_solution_text(value, field) + "\n").encode("utf-8")


def normalize_atom(value: Any, field: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise QuickIntakeError(f"{field} 必须是对象")
    unexpected = sorted(set(value) - {"text", "origin"})
    if unexpected:
        raise QuickIntakeError(f"{field} 含未知字段：{', '.join(unexpected)}")
    text = require_text(value.get("text"), f"{field}.text")
    origin = value.get("origin")
    if origin not in ALLOWED_ORIGINS:
        raise QuickIntakeError(f"{field}.origin 无效：{origin}")
    return {"text": text, "origin": origin}


def normalize_atom_list(value: Any, field: str) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise QuickIntakeError(f"{field} 必须是数组")
    if len(value) > 20:
        raise QuickIntakeError(f"{field} 最多保留 20 条")
    return [normalize_atom(item, f"{field}[{index}]") for index, item in enumerate(value)]


def normalize_break(value: Any, field: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise QuickIntakeError(f"{field} 必须是对象")
    unexpected = sorted(set(value) - {"kind", "text", "origin"})
    if unexpected:
        raise QuickIntakeError(f"{field} 含未知字段：{', '.join(unexpected)}")
    kind = value.get("kind")
    if kind not in ALLOWED_BREAK_KINDS:
        raise QuickIntakeError(f"{field}.kind 无效：{kind}")
    atom = normalize_atom(
        {"text": value.get("text"), "origin": value.get("origin")},
        field,
    )
    return {"kind": kind, **atom}


def normalize_break_list(value: Any, field: str) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise QuickIntakeError(f"{field} 必须是数组")
    if len(value) > 12:
        raise QuickIntakeError(f"{field} 最多保留 12 条")
    return [normalize_break(item, f"{field}[{index}]") for index, item in enumerate(value)]


def normalize_evidence(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise QuickIntakeError("evidence 必须是对象")
    required = {
        "result",
        "user_facts",
        "independent_correct_steps",
        "first_break",
        "later_breaks",
        "hints_needed",
        "self_corrections",
        "mastery_score",
        "mastery_source",
        "score_basis",
        "unresolved",
    }
    missing = sorted(required - set(value))
    unexpected = sorted(set(value) - required)
    if missing:
        raise QuickIntakeError(f"evidence 缺少字段：{', '.join(missing)}")
    if unexpected:
        raise QuickIntakeError(f"evidence 含未知字段：{', '.join(unexpected)}")

    result = value.get("result")
    if result not in ALLOWED_RESULTS:
        raise QuickIntakeError(f"evidence.result 无效：{result}")
    user_facts = normalize_atom_list(value.get("user_facts"), "evidence.user_facts")
    if not user_facts:
        raise QuickIntakeError("evidence.user_facts 至少需要一条真实学习证据")
    if not any(item["origin"] in USER_EVIDENCE_ORIGINS for item in user_facts):
        raise QuickIntakeError("evidence.user_facts 必须包含用户观察或用户确认的证据")

    correct_steps = normalize_atom_list(
        value.get("independent_correct_steps"),
        "evidence.independent_correct_steps",
    )
    first_break = normalize_break(value.get("first_break"), "evidence.first_break")
    later_breaks = normalize_break_list(value.get("later_breaks"), "evidence.later_breaks")
    hints_needed = normalize_atom_list(value.get("hints_needed"), "evidence.hints_needed")
    self_corrections = normalize_atom_list(
        value.get("self_corrections"),
        "evidence.self_corrections",
    )
    unresolved = normalize_atom_list(value.get("unresolved"), "evidence.unresolved")

    mastery_score = value.get("mastery_score")
    if isinstance(mastery_score, bool) or not isinstance(mastery_score, int):
        raise QuickIntakeError("evidence.mastery_score 必须是 0-5 的整数")
    if mastery_score < 0 or mastery_score > 5:
        raise QuickIntakeError("evidence.mastery_score 必须是 0-5 的整数")
    mastery_source = require_text(
        value.get("mastery_source"),
        "evidence.mastery_source",
        max_length=120,
    )
    score_basis = normalize_atom(value.get("score_basis"), "evidence.score_basis")
    return {
        "result": result,
        "user_facts": user_facts,
        "independent_correct_steps": correct_steps,
        "first_break": first_break,
        "later_breaks": later_breaks,
        "hints_needed": hints_needed,
        "self_corrections": self_corrections,
        "mastery_score": mastery_score,
        "mastery_source": mastery_source,
        "score_basis": score_basis,
        "unresolved": unresolved,
    }


def normalize_episode_evidence(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "solution_text", "user_answer_text", "teaching_turns"
    }:
        raise QuickIntakeError(
            "episode_evidence 必须只含 solution_text、user_answer_text、teaching_turns"
        )
    solution_text = value.get("solution_text")
    canonical_solution_text(solution_text, "episode_evidence.solution_text")
    user_answer_text = require_text(
        value.get("user_answer_text"),
        "episode_evidence.user_answer_text",
        max_length=32768,
    )
    raw_turns = value.get("teaching_turns")
    if not isinstance(raw_turns, list) or len(raw_turns) > 64:
        raise QuickIntakeError("episode_evidence.teaching_turns 必须是不超过 64 条的数组")
    turns: list[dict[str, str]] = []
    for index, row in enumerate(raw_turns):
        field = f"episode_evidence.teaching_turns[{index}]"
        if not isinstance(row, dict) or set(row) != {
            "speaker", "kind", "text", "origin"
        }:
            raise QuickIntakeError(f"{field} 字段不完整")
        if row.get("speaker") not in TEACHING_SPEAKERS:
            raise QuickIntakeError(f"{field}.speaker 无效")
        if row.get("kind") not in TEACHING_KINDS:
            raise QuickIntakeError(f"{field}.kind 无效")
        if row.get("origin") not in ALLOWED_ORIGINS:
            raise QuickIntakeError(f"{field}.origin 无效")
        turns.append(
            {
                "speaker": row["speaker"],
                "kind": row["kind"],
                "text": require_text(row.get("text"), f"{field}.text", max_length=8192),
                "origin": row["origin"],
            }
        )
    result = {
        "solution_text": solution_text,
        "user_answer_text": user_answer_text,
        "teaching_turns": turns,
    }
    if len(canonical_json(result).encode("utf-8")) > MAX_EPISODE_EVIDENCE_BYTES:
        raise QuickIntakeError("episode_evidence 总大小超过 128 KiB")
    return result


def read_json_document(path_value: str) -> dict[str, Any]:
    if path_value == "-":
        raw = sys.stdin.readline()
    else:
        path = Path(path_value).expanduser()
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise QuickIntakeError(f"无法读取 JSON：{path}") from exc
    if not raw.strip():
        raise QuickIntakeError("JSON 输入为空")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise QuickIntakeError(f"JSON 格式无效：{exc.msg}") from exc
    if not isinstance(value, dict):
        raise QuickIntakeError("JSON 顶层必须是对象")
    return value


def validate_consumable_payload_path(path_value: str) -> Path:
    if path_value == "-":
        raise QuickIntakeError("标准输入 payload 不能使用 --consume-payload-file")
    path = Path(path_value).expanduser()
    if path.is_symlink():
        raise QuickIntakeError("拒绝删除符号链接 payload")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise QuickIntakeError(f"待消费 payload 不存在：{path}") from exc
    allowed_roots = {Path("/private/tmp").resolve(), Path(tempfile.gettempdir()).resolve()}
    if not any(resolved == root or root in resolved.parents for root in allowed_roots):
        raise QuickIntakeError("--consume-payload-file 只允许删除系统临时目录中的普通文件")
    if not resolved.is_file():
        raise QuickIntakeError("待消费 payload 必须是普通文件")
    return resolved


def consume_payload_file(path: Path | None) -> bool:
    if path is None:
        return False
    try:
        path.unlink()
    except OSError as exc:
        raise QuickIntakeError(f"快速事件已写入，但临时 payload 清理失败：{path}") from exc
    return True


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise QuickIntakeError(f"无法读取事件账本：{path}") from exc
    for line_number, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise QuickIntakeError(f"事件账本第 {line_number} 行不是有效 JSON") from exc
        if not isinstance(event, dict):
            raise QuickIntakeError(f"事件账本第 {line_number} 行不是对象")
        if event.get("schema_version") != LEDGER_SCHEMA:
            raise QuickIntakeError(f"事件账本第 {line_number} 行 schema_version 无效")
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not EVENT_ID_PATTERN.fullmatch(event_id):
            raise QuickIntakeError(f"事件账本第 {line_number} 行 event_id 无效")
        content_hash = event.get("content_hash")
        validate_hash(content_hash, f"事件账本第 {line_number} 行 content_hash")
        unhashed = {key: value for key, value in event.items() if key != "content_hash"}
        if sha256_value(unhashed) != content_hash:
            raise QuickIntakeError(f"事件账本第 {line_number} 行完整性校验失败")
        events.append(event)
    return events


def append_jsonl(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def ledger_hash() -> str:
    return file_sha256(EVENTS_PATH) if EVENTS_PATH.exists() else hashlib.sha256(b"").hexdigest()


def resolve_repo_artifact(path_value: Any, field: str) -> Path:
    relative = require_text(path_value, field, max_length=1000)
    path = Path(relative)
    if path.is_absolute():
        raise QuickIntakeError(f"{field} 必须是仓库相对路径")
    try:
        resolved = (REPO_ROOT / path).resolve(strict=True)
    except OSError as exc:
        raise QuickIntakeError(f"{field} 指向的文件不存在：{relative}") from exc
    repo = REPO_ROOT.resolve()
    if repo not in resolved.parents or not resolved.is_file():
        raise QuickIntakeError(f"{field} 必须指向仓库内普通文件")
    return resolved


def source_bundle_id(study_date: str, source_locator: str) -> str:
    return sha256_value(
        {"study_date": study_date, "source_locator": source_locator}
    )[:24]


def validate_source_media(
    path: Path,
    data: bytes,
    field: str,
) -> tuple[str, str]:
    suffix = path.suffix.lower()
    if suffix not in SOURCE_MEDIA:
        raise QuickIntakeError(
            f"{field} 只允许 PNG、JPEG、WebP 或 PDF 来源文件"
        )
    media_type, signature = SOURCE_MEDIA[suffix]
    if signature is not None and not data.startswith(signature):
        raise QuickIntakeError(f"{field} 的文件内容与扩展名不一致")
    if suffix == ".webp" and not (
        len(data) >= 12 and data.startswith(b"RIFF") and data[8:12] == b"WEBP"
    ):
        raise QuickIntakeError(f"{field} 不是有效的 WebP 文件头")
    normalized_suffix = ".jpg" if suffix in {".jpg", ".jpeg"} else suffix
    return media_type, normalized_suffix


def normalize_source_artifact_bytes(
    role: str,
    path: Path,
    data: bytes,
    field: str,
    *,
    require_canonical: bool = False,
) -> tuple[str, str, bytes]:
    if role != "solution_text":
        media_type, suffix = validate_source_media(path, data, field)
        return media_type, suffix, data
    suffix = path.suffix.lower()
    media_type = SOLUTION_TEXT_MEDIA.get(suffix)
    if media_type is None:
        raise QuickIntakeError(f"{field} 的 solution_text 只允许 UTF-8 .txt 或 .md")
    try:
        decoded = data.decode("utf-8")
    except UnicodeError as exc:
        raise QuickIntakeError(f"{field} 的 solution_text 必须是 UTF-8") from exc
    canonical = canonical_solution_text_bytes(decoded, field)
    if require_canonical and canonical != data:
        raise QuickIntakeError(f"{field} 的 solution_text 字节不是规范形式")
    return media_type, suffix, canonical


def normalize_source_stage(value: dict[str, Any]) -> dict[str, Any]:
    required = {"schema_version", "study_date", "source_locator", "artifacts"}
    missing = sorted(required - set(value))
    unexpected = sorted(set(value) - required)
    if missing:
        raise QuickIntakeError(f"来源固化输入缺少字段：{', '.join(missing)}")
    if unexpected:
        raise QuickIntakeError(f"来源固化输入含未知字段：{', '.join(unexpected)}")
    if value.get("schema_version") != SOURCE_STAGE_SCHEMA:
        raise QuickIntakeError(f"schema_version 必须是 {SOURCE_STAGE_SCHEMA}")
    study_date = validate_date(value.get("study_date"))
    locator = require_text(value.get("source_locator"), "source_locator", max_length=1000)
    reject_local_absolute_paths(locator, "source_locator")
    raw_artifacts = value.get("artifacts")
    if not isinstance(raw_artifacts, list) or not raw_artifacts:
        raise QuickIntakeError("artifacts 必须是非空数组")
    if len(raw_artifacts) > MAX_SOURCE_ARTIFACTS:
        raise QuickIntakeError(f"artifacts 最多允许 {MAX_SOURCE_ARTIFACTS} 个文件")

    normalized: list[dict[str, Any]] = []
    seen_paths: set[Path] = set()
    seen_hashes: set[str] = set()
    total_size = 0
    for index, item in enumerate(raw_artifacts):
        field = f"artifacts[{index}]"
        if not isinstance(item, dict) or set(item) != {"role", "path"}:
            raise QuickIntakeError(f"{field} 必须只含 role 和 path")
        role = item.get("role")
        if role not in SOURCE_ROLES:
            raise QuickIntakeError(f"{field}.role 无效：{role}")
        path_text = require_text(item.get("path"), f"{field}.path", max_length=2000)
        source = Path(path_text).expanduser()
        if source.is_symlink():
            raise QuickIntakeError(f"{field}.path 不得是符号链接")
        try:
            resolved = source.resolve(strict=True)
        except OSError as exc:
            raise QuickIntakeError(f"{field}.path 指向的文件不存在：{path_text}") from exc
        if not resolved.is_file():
            raise QuickIntakeError(f"{field}.path 必须是普通文件")
        if resolved in seen_paths:
            raise QuickIntakeError(f"artifacts 重复引用同一文件：{path_text}")
        seen_paths.add(resolved)
        try:
            data = resolved.read_bytes()
        except OSError as exc:
            raise QuickIntakeError(f"无法读取来源文件：{path_text}") from exc
        if not data:
            raise QuickIntakeError(f"{field}.path 不得为空文件")
        media_type, suffix, data = normalize_source_artifact_bytes(
            role, resolved, data, f"{field}.path"
        )
        size = len(data)
        if size > MAX_SOURCE_ARTIFACT_BYTES:
            raise QuickIntakeError(
                f"{field}.path 超过 {MAX_SOURCE_ARTIFACT_BYTES} 字节上限"
            )
        total_size += size
        if total_size > MAX_SOURCE_BUNDLE_BYTES:
            raise QuickIntakeError(
                f"来源包总大小超过 {MAX_SOURCE_BUNDLE_BYTES} 字节上限"
            )
        digest = hashlib.sha256(data).hexdigest()
        if digest in seen_hashes:
            raise QuickIntakeError("artifacts 包含内容完全相同的重复文件")
        seen_hashes.add(digest)
        normalized.append(
            {
                "role": role,
                "data": data,
                "sha256": digest,
                "size": size,
                "media_type": media_type,
                "suffix": suffix,
            }
        )
    normalized.sort(key=lambda item: (item["role"], item["sha256"]))
    return {
        "study_date": study_date,
        "source_locator": locator,
        "bundle_id": source_bundle_id(study_date, locator),
        "artifacts": normalized,
    }


def source_manifest_document(payload: dict[str, Any]) -> dict[str, Any]:
    bundle_dir = SOURCE_STAGING_ROOT / payload["study_date"] / payload["bundle_id"]
    role_counts: dict[str, int] = {}
    artifacts: list[dict[str, Any]] = []
    for item in payload["artifacts"]:
        role = item["role"]
        role_counts[role] = role_counts.get(role, 0) + 1
        filename = f"{role}_{role_counts[role]:02d}{item['suffix']}"
        destination = bundle_dir / filename
        artifacts.append(
            {
                "role": role,
                "path": str(destination.relative_to(REPO_ROOT)),
                "sha256": item["sha256"],
                "size": item["size"],
                "media_type": item["media_type"],
            }
        )
    return {
        "schema_version": SOURCE_BUNDLE_SCHEMA,
        "bundle_id": payload["bundle_id"],
        "study_date": payload["study_date"],
        "source_locator": payload["source_locator"],
        "artifacts": artifacts,
    }


def canonical_json_bytes(value: Any) -> bytes:
    return (canonical_json(value) + "\n").encode("utf-8")


def fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def validate_source_bundle_manifest(
    path_value: Any,
    field: str,
    *,
    expected_hash: str | None = None,
    expected_date: str | None = None,
    expected_locator: str | None = None,
    require_question: bool = False,
    require_question_image: bool = False,
    required_roles: set[str] | None = None,
    expected_solution_text: str | None = None,
) -> tuple[dict[str, Any], Path, list[Path]]:
    path_text = require_text(path_value, field, max_length=2000)
    relative = Path(path_text)
    if relative.is_absolute():
        raise QuickIntakeError(f"{field} 必须是仓库相对路径")
    candidate = REPO_ROOT / relative
    if candidate.is_symlink():
        raise QuickIntakeError(f"{field} 不得是符号链接")
    manifest_path = resolve_repo_artifact(path_text, field)
    try:
        staging_root = SOURCE_STAGING_ROOT.resolve(strict=True)
    except OSError as exc:
        raise QuickIntakeError("来源固化目录不存在") from exc
    if staging_root not in manifest_path.parents or manifest_path.name != "manifest.json":
        raise QuickIntakeError(f"{field} 必须指向来源固化目录中的 manifest.json")
    manifest_hash = file_sha256(manifest_path)
    if expected_hash is not None and manifest_hash != expected_hash:
        raise QuickIntakeError(f"{field} 当前清单哈希与绑定不一致")
    try:
        document = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QuickIntakeError(f"{field} 不是有效来源清单") from exc
    expected_fields = {
        "schema_version",
        "bundle_id",
        "study_date",
        "source_locator",
        "artifacts",
    }
    if not isinstance(document, dict) or set(document) != expected_fields:
        raise QuickIntakeError(f"{field} 来源清单字段不完整")
    if document.get("schema_version") != SOURCE_BUNDLE_SCHEMA:
        raise QuickIntakeError(f"{field} 来源清单 schema_version 无效")
    study_date = validate_date(document.get("study_date"), f"{field}.study_date")
    locator = require_text(
        document.get("source_locator"),
        f"{field}.source_locator",
        max_length=1000,
    )
    reject_local_absolute_paths(locator, f"{field}.source_locator")
    bundle_id = document.get("bundle_id")
    if bundle_id != source_bundle_id(study_date, locator):
        raise QuickIntakeError(f"{field}.bundle_id 与日期、来源定位不一致")
    expected_manifest = SOURCE_STAGING_ROOT / study_date / bundle_id / "manifest.json"
    if manifest_path != expected_manifest.resolve(strict=True):
        raise QuickIntakeError(f"{field} 所在目录与来源清单身份不一致")
    if expected_date is not None and study_date != expected_date:
        raise QuickIntakeError(f"{field}.study_date 与 capture 不一致")
    if expected_locator is not None and locator != expected_locator:
        raise QuickIntakeError(f"{field}.source_locator 与 capture 不一致")

    raw_artifacts = document.get("artifacts")
    if not isinstance(raw_artifacts, list) or not raw_artifacts:
        raise QuickIntakeError(f"{field}.artifacts 必须是非空数组")
    if len(raw_artifacts) > MAX_SOURCE_ARTIFACTS:
        raise QuickIntakeError(f"{field}.artifacts 数量超过上限")
    child_paths: list[Path] = []
    listed_names: set[str] = set()
    listed_hashes: set[str] = set()
    total_size = 0
    roles: set[str] = set()
    question_image_count = 0
    solution_text_paths: list[Path] = []
    for index, item in enumerate(raw_artifacts):
        item_field = f"{field}.artifacts[{index}]"
        if not isinstance(item, dict) or set(item) != {
            "role",
            "path",
            "sha256",
            "size",
            "media_type",
        }:
            raise QuickIntakeError(f"{item_field} 字段不完整")
        role = item.get("role")
        if role not in SOURCE_ROLES:
            raise QuickIntakeError(f"{item_field}.role 无效：{role}")
        roles.add(role)
        artifact_text = require_text(item.get("path"), f"{item_field}.path", max_length=2000)
        artifact_relative = Path(artifact_text)
        if artifact_relative.is_absolute():
            raise QuickIntakeError(f"{item_field}.path 必须是仓库相对路径")
        artifact_candidate = REPO_ROOT / artifact_relative
        if artifact_candidate.is_symlink():
            raise QuickIntakeError(f"{item_field}.path 不得是符号链接")
        artifact = resolve_repo_artifact(artifact_text, f"{item_field}.path")
        if artifact.parent != manifest_path.parent:
            raise QuickIntakeError(f"{item_field}.path 必须与清单位于同一来源包")
        if artifact.name in listed_names:
            raise QuickIntakeError(f"{field}.artifacts 重复引用文件")
        listed_names.add(artifact.name)
        expected_artifact_hash = validate_hash(item.get("sha256"), f"{item_field}.sha256")
        if expected_artifact_hash in listed_hashes:
            raise QuickIntakeError(f"{field}.artifacts 存在重复内容哈希")
        listed_hashes.add(expected_artifact_hash)
        if file_sha256(artifact) != expected_artifact_hash:
            raise QuickIntakeError(f"{item_field} 当前文件哈希与清单不一致")
        size = item.get("size")
        if isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise QuickIntakeError(f"{item_field}.size 无效")
        if size > MAX_SOURCE_ARTIFACT_BYTES or artifact.stat().st_size != size:
            raise QuickIntakeError(f"{item_field}.size 与当前文件不一致或超过上限")
        total_size += size
        if total_size > MAX_SOURCE_BUNDLE_BYTES:
            raise QuickIntakeError(f"{field} 来源包总大小超过上限")
        data = artifact.read_bytes()
        media_type, _, canonical_data = normalize_source_artifact_bytes(
            role,
            artifact,
            data,
            f"{item_field}.path",
            require_canonical=role == "solution_text",
        )
        if canonical_data != data:
            raise QuickIntakeError(f"{item_field}.path 字节不规范")
        if item.get("media_type") != media_type:
            raise QuickIntakeError(f"{item_field}.media_type 与文件不一致")
        if role == "question" and media_type.startswith("image/"):
            question_image_count += 1
        if role == "solution_text":
            solution_text_paths.append(artifact)
        child_paths.append(artifact)
    if require_question and "question" not in roles:
        raise QuickIntakeError(f"{field} 对新题至少需要一个 question 文件")
    if require_question_image and question_image_count == 0:
        raise QuickIntakeError(f"{field} 至少需要一张真实 question image")
    if required_roles is not None and not required_roles.issubset(roles):
        missing_roles = sorted(required_roles - roles)
        raise QuickIntakeError(
            f"{field} 缺少必须的来源角色：{', '.join(missing_roles)}"
        )
    if expected_solution_text is not None:
        if len(solution_text_paths) != 1:
            raise QuickIntakeError(f"{field} 必须恰好绑定一个 solution_text artifact")
        expected_bytes = canonical_solution_text_bytes(
            expected_solution_text, "episode_evidence.solution_text"
        )
        if solution_text_paths[0].read_bytes() != expected_bytes:
            raise QuickIntakeError(
                f"{field} 的 solution_text artifact 与 episode_evidence.solution_text 不一致"
            )
    actual_names = {entry.name for entry in manifest_path.parent.iterdir()}
    if actual_names != listed_names | {"manifest.json"}:
        raise QuickIntakeError(f"{field} 来源包含未声明文件或缺失文件")
    return document, manifest_path, child_paths


def normalize_source_bundle_reference(
    value: Any,
    field: str,
    *,
    expected_date: str | None = None,
    expected_locator: str | None = None,
    require_question: bool = False,
    require_question_image: bool = False,
    required_roles: set[str] | None = None,
    expected_solution_text: str | None = None,
) -> dict[str, str] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {"manifest_path", "manifest_hash"}:
        raise QuickIntakeError(f"{field} 必须只含 manifest_path 和 manifest_hash")
    manifest_hash = validate_hash(value.get("manifest_hash"), f"{field}.manifest_hash")
    document, manifest_path, _ = validate_source_bundle_manifest(
        value.get("manifest_path"),
        f"{field}.manifest_path",
        expected_hash=manifest_hash,
        expected_date=expected_date,
        expected_locator=expected_locator,
        require_question=require_question,
        require_question_image=require_question_image,
        required_roles=required_roles,
        expected_solution_text=expected_solution_text,
    )
    return {
        "manifest_path": str(manifest_path.relative_to(REPO_ROOT.resolve())),
        "manifest_hash": manifest_hash,
        "source_locator": document["source_locator"],
        "study_date": document["study_date"],
    }


def source_bundle_binding(reference: dict[str, str]) -> dict[str, str]:
    return {
        "source_locator": reference["source_locator"],
        "resolved_source_hash": reference["manifest_hash"],
        "artifact_path": reference["manifest_path"],
        "artifact_hash": reference["manifest_hash"],
    }


def normalize_hash_artifacts(value: Any, field: str) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise QuickIntakeError(f"{field} 必须是数组")
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for index, item in enumerate(value):
        item_field = f"{field}[{index}]"
        if not isinstance(item, dict) or set(item) != {"path", "sha256"}:
            raise QuickIntakeError(f"{item_field} 必须只含 path 和 sha256")
        path = resolve_repo_artifact(item.get("path"), f"{item_field}.path")
        relative = str(path.relative_to(REPO_ROOT.resolve()))
        if relative in seen:
            raise QuickIntakeError(f"{field} 重复引用：{relative}")
        seen.add(relative)
        expected_hash = validate_hash(item.get("sha256"), f"{item_field}.sha256")
        if file_sha256(path) != expected_hash:
            raise QuickIntakeError(f"{item_field} 当前文件哈希与回执不一致")
        normalized.append({"path": relative, "sha256": expected_hash})
    normalized.sort(key=lambda item: item["path"])
    return normalized


@contextmanager
def exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def stage_source_bundle(payload: dict[str, Any]) -> tuple[str, dict[str, Any], Path]:
    document = source_manifest_document(payload)
    manifest_bytes = canonical_json_bytes(document)
    bundle_dir = SOURCE_STAGING_ROOT / payload["study_date"] / payload["bundle_id"]
    manifest_path = bundle_dir / "manifest.json"
    if SOURCE_STAGING_ROOT.is_symlink():
        raise QuickIntakeError("来源固化根目录不得是符号链接")
    SOURCE_STAGING_ROOT.mkdir(parents=True, exist_ok=True)
    staging_root_resolved = SOURCE_STAGING_ROOT.resolve(strict=True)
    if REPO_ROOT.resolve() not in staging_root_resolved.parents:
        raise QuickIntakeError("来源固化根目录必须位于当前仓库内")
    day_dir = bundle_dir.parent
    if day_dir.is_symlink():
        raise QuickIntakeError("来源固化日期目录不得是符号链接")
    day_dir.mkdir(parents=True, exist_ok=True)

    if bundle_dir.exists():
        if not bundle_dir.is_dir() or not manifest_path.is_file():
            raise QuickIntakeError("同一来源身份已有不完整固化目录，拒绝覆盖")
        validate_source_bundle_manifest(
            str(manifest_path.relative_to(REPO_ROOT)),
            "已有来源清单",
            expected_date=payload["study_date"],
            expected_locator=payload["source_locator"],
        )
        if manifest_path.read_bytes() != manifest_bytes:
            raise QuickIntakeError("同一日期与来源定位出现不同文件，拒绝覆盖")
        return "noop", document, manifest_path

    temp_dir = Path(
        tempfile.mkdtemp(prefix=f".{payload['bundle_id']}.", dir=day_dir)
    )
    try:
        payload_by_hash = {item["sha256"]: item for item in payload["artifacts"]}
        for artifact in document["artifacts"]:
            item = payload_by_hash[artifact["sha256"]]
            destination = temp_dir / Path(artifact["path"]).name
            with destination.open("xb") as handle:
                handle.write(item["data"])
                handle.flush()
                os.fsync(handle.fileno())
            if file_sha256(destination) != artifact["sha256"]:
                raise QuickIntakeError("来源文件固化后哈希校验失败")
        staged_manifest = temp_dir / "manifest.json"
        with staged_manifest.open("xb") as handle:
            handle.write(manifest_bytes)
            handle.flush()
            os.fsync(handle.fileno())
        fsync_directory(temp_dir)
        os.replace(temp_dir, bundle_dir)
        fsync_directory(day_dir)
    except Exception:
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        raise

    validate_source_bundle_manifest(
        str(manifest_path.relative_to(REPO_ROOT)),
        "新来源清单",
        expected_hash=file_sha256(manifest_path),
        expected_date=payload["study_date"],
        expected_locator=payload["source_locator"],
    )
    return "recorded", document, manifest_path


def cmd_stage_source(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    consumable_path = (
        validate_consumable_payload_path(args.payload_file)
        if getattr(args, "consume_payload_file", False)
        else None
    )
    payload = normalize_source_stage(read_json_document(args.payload_file))
    with exclusive_lock(SOURCE_LOCK_PATH):
        status, document, manifest_path = stage_source_bundle(payload)
    response = {
        "status": status,
        "state": "source_staged",
        "bundle_id": document["bundle_id"],
        "study_date": document["study_date"],
        "source_locator": document["source_locator"],
        "manifest_path": str(manifest_path.relative_to(REPO_ROOT)),
        "manifest_hash": file_sha256(manifest_path),
        "artifacts": document["artifacts"],
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
        "payload_file_consumed": consume_payload_file(consumable_path),
    }
    print(json.dumps(response, ensure_ascii=False, sort_keys=True))


def load_review_events() -> list[dict[str, Any]]:
    if not REVIEW_LOG_PATH.exists():
        return []
    values: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        REVIEW_LOG_PATH.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise QuickIntakeError(f"复习记录第 {line_number} 行不是有效 JSON") from exc
        if not isinstance(value, dict):
            raise QuickIntakeError(f"复习记录第 {line_number} 行不是对象")
        values.append(value)
    return values


def score_reference(score_event_id: Any) -> dict[str, Any] | None:
    if score_event_id in (None, ""):
        return None
    if not isinstance(score_event_id, str) or not re.fullmatch(r"SCORE-[0-9a-f]{24}", score_event_id):
        raise QuickIntakeError("score_event_id 无效")
    matches = [event for event in load_review_events() if event.get("event_id") == score_event_id]
    if len(matches) != 1:
        raise QuickIntakeError(
            f"评分事件身份必须唯一：{score_event_id}（实际 {len(matches)} 条）"
        )
    event = matches[0]
    required = (
        "attempt_id",
        "date",
        "delivered_card_id",
        "delivered_card_source_version",
        "score",
        "queue_id",
        "queue_item_id",
        "match_mode",
        "anchor_card_id",
    )
    missing = [field for field in required if event.get(field) in (None, "")]
    if missing:
        raise QuickIntakeError(f"评分事件缺少快速引用字段：{', '.join(missing)}")
    card_id = str(event["delivered_card_id"])
    if not CARD_ID_PATTERN.fullmatch(card_id):
        raise QuickIntakeError("评分事件 delivered_card_id 无效")
    source_version = validate_hash(
        event.get("delivered_card_source_version"),
        "评分事件 delivered_card_source_version",
    )
    score = event.get("score")
    if isinstance(score, bool) or not isinstance(score, int) or not 0 <= score <= 5:
        raise QuickIntakeError("评分事件 score 无效")
    return {
        "event_id": score_event_id,
        "attempt_id": event["attempt_id"],
        "study_date": event["date"],
        "formal_id": card_id,
        "source_version": source_version,
        "score": score,
        "queue_id": event["queue_id"],
        "queue_item_id": event["queue_item_id"],
        "match_mode": event["match_mode"],
        "anchor_card_id": event["anchor_card_id"],
    }


def formal_card_identity(card_id: str) -> tuple[Path, str]:
    if not CARD_ID_PATTERN.fullmatch(card_id):
        raise QuickIntakeError(f"无效正式错题 ID：{card_id}")
    matches = sorted(CARDS_DIR.glob(f"{card_id}_*.md"))
    if len(matches) != 1:
        raise QuickIntakeError(
            f"正式错题卡身份必须唯一：{card_id}（实际 {len(matches)} 张）"
        )
    path = matches[0]
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def normalize_target(value: Any, score_ref: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise QuickIntakeError("target 必须是对象")
    allowed = {"kind", "formal_id", "source_locator", "source_hash_before"}
    unexpected = sorted(set(value) - allowed)
    if unexpected:
        raise QuickIntakeError(f"target 含未知字段：{', '.join(unexpected)}")
    kind = value.get("kind")
    if kind not in {"formal_card", "new_source"}:
        raise QuickIntakeError("target.kind 必须是 formal_card 或 new_source")
    formal_id = value.get("formal_id")
    source_locator = value.get("source_locator")
    source_hash_before = value.get("source_hash_before")

    if score_ref is not None:
        if kind != "formal_card":
            raise QuickIntakeError("带评分回执的快速事件必须指向 formal_card")
        if formal_id != score_ref["formal_id"]:
            raise QuickIntakeError("target.formal_id 与评分事件实际交付卡冲突")
        if source_hash_before not in (None, "", score_ref["source_version"]):
            raise QuickIntakeError("target.source_hash_before 与评分事件来源版本冲突")
        formal_card_identity(formal_id)
        return {
            "kind": "formal_card",
            "formal_id": formal_id,
            "source_locator": None,
            "source_hash_before": score_ref["source_version"],
            "identity_state": "verified_score_delivery",
        }

    if kind == "formal_card":
        if not isinstance(formal_id, str):
            raise QuickIntakeError("formal_card 需要 target.formal_id")
        path, actual_hash = formal_card_identity(formal_id)
        if source_hash_before not in (None, "", actual_hash):
            raise QuickIntakeError("target.source_hash_before 与当前正式卡不一致")
        return {
            "kind": "formal_card",
            "formal_id": formal_id,
            "source_locator": str(path.relative_to(REPO_ROOT)),
            "source_hash_before": actual_hash,
            "identity_state": "verified_formal",
        }

    if formal_id not in (None, ""):
        raise QuickIntakeError("new_source 不得预先分配正式 ID")
    locator = require_text(source_locator, "target.source_locator", max_length=1000)
    source_hash = validate_hash(
        source_hash_before,
        "target.source_hash_before",
        required=False,
    )
    return {
        "kind": "new_source",
        "formal_id": None,
        "source_locator": locator,
        "source_hash_before": source_hash,
        "identity_state": "needs_user" if source_hash is None else "source_backed",
    }


def normalize_capture(value: dict[str, Any]) -> dict[str, Any]:
    schema_version = value.get("schema_version")
    if schema_version != CAPTURE_SCHEMA_V2:
        raise QuickIntakeError(f"fresh Capture 必须使用 {CAPTURE_SCHEMA_V2}")
    required = {
        "schema_version",
        "attempt_id",
        "study_date",
        "target",
        "score_event_id",
        "requested_action",
        "thread_ref",
        "source_bundle",
        "episode_evidence",
        "evidence",
    }
    missing = sorted(required - set(value))
    unexpected = sorted(set(value) - required)
    if missing:
        raise QuickIntakeError(f"捕获输入缺少字段：{', '.join(missing)}")
    if unexpected:
        raise QuickIntakeError(f"捕获输入含未知字段：{', '.join(unexpected)}")
    attempt_id = value.get("attempt_id")
    if not isinstance(attempt_id, str) or not ATTEMPT_ID_PATTERN.fullmatch(attempt_id):
        raise QuickIntakeError("attempt_id 只能含字母、数字、点、下划线、冒号、斜杠和连字符")
    study_date = validate_date(value.get("study_date"))
    score_ref = score_reference(value.get("score_event_id"))
    if score_ref is not None:
        if score_ref["attempt_id"] != attempt_id:
            raise QuickIntakeError("attempt_id 与评分事件不一致")
        if score_ref["study_date"] != study_date:
            raise QuickIntakeError("study_date 与评分事件不一致")
    action = value.get("requested_action")
    if action not in ALLOWED_ACTIONS:
        raise QuickIntakeError(f"requested_action 无效：{action}")
    thread_ref = value.get("thread_ref")
    if thread_ref is not None:
        thread_ref = require_text(thread_ref, "thread_ref", max_length=500)
    target = normalize_target(value.get("target"), score_ref)
    episode_evidence = normalize_episode_evidence(value.get("episode_evidence"))
    source_bundle = normalize_source_bundle_reference(
        value.get("source_bundle"),
        "source_bundle",
        expected_date=study_date,
        expected_locator=(
            target["source_locator"] if target["kind"] == "new_source" else None
        ),
        require_question=True,
        require_question_image=True,
        required_roles={"question", "solution_text"},
        expected_solution_text=episode_evidence["solution_text"],
    )
    if source_bundle is None:
        raise QuickIntakeError(
            "fresh Capture 必须先 stage-source 并绑定 question image 与 solution_text artifact"
        )
    if target["kind"] == "new_source":
        manifest_hash = source_bundle["manifest_hash"]
        if target["source_hash_before"] not in (None, manifest_hash):
            raise QuickIntakeError("target.source_hash_before 与 source_bundle 清单哈希冲突")
        target["source_hash_before"] = manifest_hash
        target["identity_state"] = "source_backed"
    evidence = normalize_evidence(value.get("evidence"))
    if score_ref is not None and evidence["mastery_score"] != score_ref["score"]:
        raise QuickIntakeError("evidence.mastery_score 与评分事件分数不一致")
    normalized = {
        "schema_version": schema_version,
        "attempt_id": attempt_id,
        "study_date": study_date,
        "target": target,
        "score_ref": score_ref,
        "requested_action": action,
        "thread_ref": thread_ref,
        "source_bundle": source_bundle,
        "evidence": evidence,
    }
    normalized["episode_evidence"] = episode_evidence
    reject_local_absolute_paths(normalized, "capture")
    return normalized


def with_content_hash(event: dict[str, Any]) -> dict[str, Any]:
    result = dict(event)
    result["content_hash"] = sha256_value(result)
    return result


def replay(events: list[dict[str, Any]]) -> dict[str, Any]:
    captures: dict[str, dict[str, Any]] = {}
    amendments: dict[str, list[dict[str, Any]]] = {}
    freezes: dict[str, dict[str, Any]] = {}
    aborted_freezes: set[str] = set()
    prepares: dict[str, dict[str, Any]] = {}
    committed_prepares: dict[str, str] = {}
    invalidated_prepares: set[str] = set()
    used_freezes: dict[str, str] = {}
    closed_by: dict[str, str] = {}
    closeouts: dict[str, dict[str, Any]] = {}
    invalidated_closeouts: set[str] = set()
    seen_ids: set[str] = set()
    for event in events:
        event_id = event["event_id"]
        if event_id in seen_ids:
            raise QuickIntakeError(f"事件账本存在重复 event_id：{event_id}")
        seen_ids.add(event_id)
        event_type = event.get("event_type")
        if event_type == "capture":
            captures[event_id] = event
            amendments[event_id] = []
            continue
        if event_type == "amendment":
            capture_id = event.get("capture_event_id")
            if capture_id not in captures:
                raise QuickIntakeError(f"修订引用未知 capture：{capture_id}")
            amendments[capture_id].append(event)
            continue
        if event_type == "freeze":
            capture_ids = event.get("capture_event_ids")
            if not isinstance(capture_ids, list) or not capture_ids:
                raise QuickIntakeError(f"freeze 缺少 capture_event_ids：{event_id}")
            for capture_id in capture_ids:
                if capture_id not in captures:
                    raise QuickIntakeError(f"freeze 引用未知 capture：{capture_id}")
            freezes[event_id] = event
            continue
        if event_type == "freeze_abort":
            freeze_id = event.get("freeze_id")
            freeze = freezes.get(freeze_id)
            if freeze is None or freeze_id in aborted_freezes:
                raise QuickIntakeError(f"freeze_abort 引用无效 freeze：{freeze_id}")
            if freeze_id in used_freezes:
                raise QuickIntakeError(f"已关闭 freeze 不得 abort：{freeze_id}")
            if event.get("capture_event_ids") != freeze.get("capture_event_ids"):
                raise QuickIntakeError(f"freeze_abort 的 capture 集合不一致：{freeze_id}")
            aborted_freezes.add(freeze_id)
            continue
        if event_type == "closeout_prepare":
            freeze_id = event.get("freeze_id")
            if freeze_id not in freezes:
                raise QuickIntakeError(f"closeout prepare 引用未知 freeze：{freeze_id}")
            if freeze_id in aborted_freezes:
                raise QuickIntakeError(f"closeout prepare 引用已 abort freeze：{freeze_id}")
            capture_ids = event.get("capture_event_ids")
            if not isinstance(capture_ids, list) or not capture_ids:
                raise QuickIntakeError(f"closeout prepare 缺少 capture_event_ids：{event_id}")
            for capture_id in capture_ids:
                if capture_id not in captures:
                    raise QuickIntakeError(f"closeout prepare 引用未知 capture：{capture_id}")
            prepares[event_id] = event
            continue
        if event_type == "closeout":
            freeze_id = event.get("freeze_id")
            if freeze_id not in freezes:
                raise QuickIntakeError(f"closeout 引用未知 freeze：{freeze_id}")
            if freeze_id in aborted_freezes:
                raise QuickIntakeError(f"closeout 引用已 abort freeze：{freeze_id}")
            prepare_id = event.get("prepare_id")
            if prepare_id is not None:
                prepare = prepares.get(prepare_id)
                if prepare is None:
                    raise QuickIntakeError(f"closeout 引用未知 prepare：{prepare_id}")
                if prepare_id in invalidated_prepares:
                    raise QuickIntakeError(f"closeout 引用已失效 prepare：{prepare_id}")
                if prepare_id in committed_prepares:
                    raise QuickIntakeError(f"closeout prepare 被重复提交：{prepare_id}")
                if prepare.get("freeze_id") != freeze_id:
                    raise QuickIntakeError(f"closeout 与 prepare 的 freeze 不一致：{prepare_id}")
                if (
                    prepare.get("request_hash") != event.get("request_hash")
                    or prepare.get("payload_hash") != event.get("payload_hash")
                    or prepare.get("generation") != event.get("generation")
                ):
                    raise QuickIntakeError(f"closeout 与 prepare 的回执身份不一致：{prepare_id}")
            if freeze_id in used_freezes:
                raise QuickIntakeError(f"freeze 被重复关闭：{freeze_id}")
            capture_ids = event.get("capture_event_ids")
            if not isinstance(capture_ids, list) or not capture_ids:
                raise QuickIntakeError(f"closeout 缺少 capture_event_ids：{event_id}")
            for capture_id in capture_ids:
                if capture_id not in captures:
                    raise QuickIntakeError(f"closeout 引用未知 capture：{capture_id}")
                if capture_id in closed_by:
                    raise QuickIntakeError(f"capture 被重复关闭：{capture_id}")
                closed_by[capture_id] = event_id
            if prepare_id is not None:
                if capture_ids != prepare.get("capture_event_ids"):
                    raise QuickIntakeError(f"closeout 与 prepare 的 capture 不一致：{prepare_id}")
                committed_prepares[prepare_id] = event_id
            used_freezes[freeze_id] = event_id
            closeouts[event_id] = event
            continue
        if event_type == "closeout_invalidation":
            prepare_id = event.get("prepare_id")
            if prepare_id is not None:
                prepare = prepares.get(prepare_id)
                if (
                    prepare is None
                    or prepare_id in invalidated_prepares
                    or prepare_id in committed_prepares
                ):
                    raise QuickIntakeError(f"closeout invalidation 引用无效 prepare：{prepare_id}")
                if event.get("freeze_id") != prepare.get("freeze_id"):
                    raise QuickIntakeError(f"invalidation 与 prepare 的 freeze 不一致：{prepare_id}")
                if event.get("capture_event_ids") != prepare.get("capture_event_ids"):
                    raise QuickIntakeError(f"invalidation 与 prepare 的 capture 不一致：{prepare_id}")
                invalidated_prepares.add(prepare_id)
                continue

            # 兼容旧版“先关闭、再失效”的账本。失效时必须同时释放 freeze，
            # 否则 capture 虽恢复 pending，但永远无法再次收口。
            closeout_id = event.get("closeout_id")
            closeout = closeouts.get(closeout_id)
            if closeout is None or closeout_id in invalidated_closeouts:
                raise QuickIntakeError(f"closeout invalidation 引用无效：{closeout_id}")
            for capture_id in closeout["capture_event_ids"]:
                if closed_by.get(capture_id) != closeout_id:
                    raise QuickIntakeError(f"invalidation 无法恢复 capture：{capture_id}")
                del closed_by[capture_id]
            freeze_id = closeout.get("freeze_id")
            if used_freezes.get(freeze_id) != closeout_id:
                raise QuickIntakeError(f"invalidation 无法释放 freeze：{freeze_id}")
            del used_freezes[freeze_id]
            invalidated_closeouts.add(closeout_id)
            continue
        raise QuickIntakeError(f"未知事件类型：{event_type}")
    return {
        "captures": captures,
        "amendments": amendments,
        "freezes": freezes,
        "aborted_freezes": aborted_freezes,
        "prepares": prepares,
        "committed_prepares": committed_prepares,
        "invalidated_prepares": invalidated_prepares,
        "used_freezes": used_freezes,
        "closed_by": closed_by,
        "closeouts": closeouts,
        "invalidated_closeouts": invalidated_closeouts,
        "seen_ids": seen_ids,
    }


def _producer_binding_attestation(event: dict[str, Any]) -> dict[str, Any]:
    try:
        return publish_attestation(
            descriptor_path=PRODUCER_BINDING_DESCRIPTOR_PATH,
            repo_root=REPO_ROOT,
            subject="math",
            capture_id=str(event["event_id"]),
            capture_content_sha256=str(event["content_hash"]),
            recorded_at=str(event["recorded_at"]),
        )
    except (OSError, ValueError, json.JSONDecodeError, ProducerBindingError) as exc:
        raise QuickIntakeError("快速事件 Producer binding attestation 失败") from exc


def capture_response(
    event: dict[str, Any],
    status: str,
    elapsed_ms: float,
    producer_binding: dict[str, Any],
) -> dict[str, Any]:
    target = event["target"]
    score_ref = event.get("score_ref")
    source_bundle = event.get("source_bundle")
    return {
        "status": status,
        "state": "pending_nightly",
        "formal_write_count": 0,
        "event_id": event["event_id"],
        "capture_schema_version": event.get(
            "capture_schema_version", CAPTURE_SCHEMA
        ),
        "formal_id": target.get("formal_id"),
        "source_locator": target.get("source_locator"),
        "source_manifest_path": (
            source_bundle.get("manifest_path")
            if isinstance(source_bundle, dict)
            else None
        ),
        "source_manifest_hash": (
            source_bundle.get("manifest_hash")
            if isinstance(source_bundle, dict)
            else None
        ),
        "score_event_id": score_ref.get("event_id") if isinstance(score_ref, dict) else None,
        "content_hash": event["content_hash"],
        "recorded_at": event["recorded_at"],
        "producer_binding_status": producer_binding["status"],
        "producer_binding_attestation_path": producer_binding[
            "attestation_path"
        ],
        "producer_binding_attestation_sha256": producer_binding[
            "attestation_sha256"
        ],
        "elapsed_ms": round(elapsed_ms, 3),
    }


def cmd_record(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    consumable_path = (
        validate_consumable_payload_path(args.payload_file)
        if getattr(args, "consume_payload_file", False)
        else None
    )
    payload = normalize_capture(read_json_document(args.payload_file))
    target_identity = payload["target"].get("formal_id") or payload["target"].get("source_locator")
    idempotency = {
        "attempt_id": payload["attempt_id"],
        "target_identity": target_identity,
    }
    event_id = stable_event_id("MFI-CAP", idempotency)
    payload_hash = sha256_value(payload)
    with exclusive_lock(LOCK_PATH):
        events = load_jsonl(EVENTS_PATH)
        state = replay(events)
        existing = state["captures"].get(event_id)
        if existing is not None:
            if existing.get("payload_hash") != payload_hash:
                raise QuickIntakeError(
                    f"同一快速事件身份出现不同证据，拒绝覆盖：{event_id}"
                )
            response = capture_response(
                existing,
                "noop",
                (time.perf_counter() - started) * 1000,
                _producer_binding_attestation(existing),
            )
            response["payload_file_consumed"] = consume_payload_file(consumable_path)
            print(json.dumps(response, ensure_ascii=False, sort_keys=True))
            return
        event_body = {
            "schema_version": LEDGER_SCHEMA,
            "event_type": "capture",
            "event_id": event_id,
            "idempotency_key": sha256_value(idempotency),
            "payload_hash": payload_hash,
            "recorded_at": now_local(),
            "study_date": payload["study_date"],
            "attempt_id": payload["attempt_id"],
            "target": payload["target"],
            "score_ref": payload["score_ref"],
            "requested_action": payload["requested_action"],
            "thread_ref": payload["thread_ref"],
            "source_bundle": payload["source_bundle"],
            "evidence": payload["evidence"],
            "initial_state": "pending_nightly",
        }
        if payload["schema_version"] == CAPTURE_SCHEMA_V2:
            event_body["capture_schema_version"] = CAPTURE_SCHEMA_V2
            event_body["episode_evidence"] = payload["episode_evidence"]
        event = with_content_hash(event_body)
        append_jsonl(EVENTS_PATH, event)
        verified = replay(load_jsonl(EVENTS_PATH))["captures"].get(event_id)
        if verified is None or verified.get("content_hash") != event["content_hash"]:
            raise QuickIntakeError("快速事件写入后固定校验失败")
        producer_binding = _producer_binding_attestation(event)
    response = capture_response(
        event,
        "recorded",
        (time.perf_counter() - started) * 1000,
        producer_binding,
    )
    response["payload_file_consumed"] = consume_payload_file(consumable_path)
    print(json.dumps(response, ensure_ascii=False, sort_keys=True))


def normalize_amendment(value: dict[str, Any]) -> dict[str, Any]:
    required = {"schema_version", "capture_event_id", "reason", "evidence"}
    missing = sorted(required - set(value))
    unexpected = sorted(set(value) - required - {"target_patch"})
    if missing:
        raise QuickIntakeError(f"修订输入缺少字段：{', '.join(missing)}")
    if unexpected:
        raise QuickIntakeError(f"修订输入含未知字段：{', '.join(unexpected)}")
    if value.get("schema_version") != AMENDMENT_SCHEMA:
        raise QuickIntakeError(f"schema_version 必须是 {AMENDMENT_SCHEMA}")
    capture_id = value.get("capture_event_id")
    if not isinstance(capture_id, str) or not re.fullmatch(r"MFI-CAP-[0-9a-f]{24}", capture_id):
        raise QuickIntakeError("capture_event_id 无效")
    target_patch = value.get("target_patch")
    if target_patch is not None:
        if not isinstance(target_patch, dict) or not target_patch:
            raise QuickIntakeError("target_patch 必须是非空对象")
        unexpected_patch = sorted(
            set(target_patch) - {"source_hash_before", "source_bundle"}
        )
        if unexpected_patch:
            raise QuickIntakeError(
                "target_patch 含未知字段：" + ", ".join(unexpected_patch)
            )
        normalized_patch: dict[str, Any] = {}
        if "source_hash_before" in target_patch:
            normalized_patch["source_hash_before"] = validate_hash(
                target_patch.get("source_hash_before"),
                "target_patch.source_hash_before",
            )
        if "source_bundle" in target_patch:
            normalized_patch["source_bundle"] = normalize_source_bundle_reference(
                target_patch.get("source_bundle"),
                "target_patch.source_bundle",
            )
            if normalized_patch["source_bundle"] is None:
                raise QuickIntakeError("target_patch.source_bundle 不得为 null")
        target_patch = normalized_patch
    return {
        "capture_event_id": capture_id,
        "reason": normalize_atom(value.get("reason"), "reason"),
        "evidence": normalize_evidence(value.get("evidence")),
        "target_patch": target_patch,
    }


def cmd_amend(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    payload = normalize_amendment(read_json_document(args.payload_file))
    payload_hash = sha256_value(payload)
    event_id = stable_event_id(
        "MFI-AMD",
        {"capture_event_id": payload["capture_event_id"], "payload_hash": payload_hash},
    )
    with exclusive_lock(LOCK_PATH):
        events = load_jsonl(EVENTS_PATH)
        state = replay(events)
        capture_id = payload["capture_event_id"]
        if capture_id not in state["captures"]:
            raise QuickIntakeError(f"修订引用未知 capture：{capture_id}")
        if capture_id in state["closed_by"]:
            raise QuickIntakeError(f"已夜间关闭的 capture 不得修订：{capture_id}")
        if payload["target_patch"] is not None:
            capture = state["captures"][capture_id]
            capture_target = capture["target"]
            patch = payload["target_patch"]
            source_bundle = patch.get("source_bundle")
            if source_bundle is not None:
                validate_source_bundle_manifest(
                    source_bundle["manifest_path"],
                    "target_patch.source_bundle.manifest_path",
                    expected_hash=source_bundle["manifest_hash"],
                    expected_date=capture["study_date"],
                    expected_locator=(
                        capture_target.get("source_locator")
                        if capture_target.get("kind") == "new_source"
                        else None
                    ),
                    require_question=capture_target.get("kind") == "new_source",
                )
                if (
                    capture_target.get("kind") == "new_source"
                    and patch.get("source_hash_before") != source_bundle["manifest_hash"]
                ):
                    raise QuickIntakeError(
                        "new_source amendment 必须同时把 source_hash_before 绑定到清单哈希"
                    )
            if capture_target.get("kind") == "formal_card":
                if payload["reason"]["origin"] != "source_verified":
                    raise QuickIntakeError(
                        "formal_card target_patch 的 reason.origin 必须是 source_verified"
                    )
                amendments = state["amendments"][capture_id]
                current_evidence = (
                    amendments[-1]["evidence"]
                    if amendments
                    else state["captures"][capture_id]["evidence"]
                )
                if sha256_value(payload["evidence"]) != sha256_value(current_evidence):
                    raise QuickIntakeError(
                        "formal_card target_patch 只允许重绑来源版本，不得同时修改 evidence"
                    )
                if "source_hash_before" in patch:
                    _, current_hash = formal_card_identity(capture_target["formal_id"])
                    if patch["source_hash_before"] != current_hash:
                        raise QuickIntakeError(
                            "formal_card target_patch 必须绑定当前唯一正式卡哈希"
                        )
        existing = next((event for event in events if event["event_id"] == event_id), None)
        if existing is not None:
            status = "noop"
            event = existing
        else:
            active_freezes = sorted(
                freeze_id
                for freeze_id, freeze in state["freezes"].items()
                if capture_id in freeze.get("capture_event_ids", [])
                and freeze_id not in state["used_freezes"]
                and freeze_id not in state["aborted_freezes"]
            )
            if active_freezes:
                raise QuickIntakeError(
                    "capture 已进入 active freeze，不得再修订；"
                    "表示层补正应在原 freeze 收口后另记 update_representation"
                )
            event = with_content_hash(
                {
                    "schema_version": LEDGER_SCHEMA,
                    "event_type": "amendment",
                    "event_id": event_id,
                    "capture_event_id": capture_id,
                    "payload_hash": payload_hash,
                    "recorded_at": now_local(),
                    "reason": payload["reason"],
                    "evidence": payload["evidence"],
                    "target_patch": payload["target_patch"],
                }
            )
            append_jsonl(EVENTS_PATH, event)
            replay(load_jsonl(EVENTS_PATH))
            status = "recorded"
    print(
        json.dumps(
            {
                "status": status,
                "state": "pending_nightly",
                "event_id": event_id,
                "capture_event_id": payload["capture_event_id"],
                "content_hash": event["content_hash"],
                "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


def effective_target(
    capture: dict[str, Any],
    amendments: list[dict[str, Any]],
) -> dict[str, Any]:
    target = dict(capture["target"])
    for amendment in amendments:
        patch = amendment.get("target_patch")
        if not isinstance(patch, dict):
            continue
        source_hash = patch.get("source_hash_before")
        if source_hash:
            target["source_hash_before"] = source_hash
            target["identity_state"] = "source_backed_amendment"
    return target


def effective_source_bundle(
    capture: dict[str, Any],
    amendments: list[dict[str, Any]],
) -> dict[str, str] | None:
    source_bundle = capture.get("source_bundle")
    for amendment in amendments:
        patch = amendment.get("target_patch")
        if isinstance(patch, dict) and isinstance(patch.get("source_bundle"), dict):
            source_bundle = patch["source_bundle"]
    return source_bundle if isinstance(source_bundle, dict) else None


def pending_item(
    capture: dict[str, Any],
    amendments: list[dict[str, Any]],
) -> dict[str, Any]:
    effective_evidence = amendments[-1]["evidence"] if amendments else capture["evidence"]
    target = effective_target(capture, amendments)
    source_bundle = effective_source_bundle(capture, amendments)
    return {
        "event_id": capture["event_id"],
        "capture_schema_version": capture.get(
            "capture_schema_version", CAPTURE_SCHEMA
        ),
        "study_date": capture["study_date"],
        "recorded_at": capture["recorded_at"],
        "formal_id": target.get("formal_id"),
        "source_locator": target.get("source_locator"),
        "source_hash_before": target.get("source_hash_before"),
        "identity_state": target.get("identity_state"),
        "source_bundle": source_bundle,
        "episode_evidence": capture.get("episode_evidence"),
        "episode_evidence_hash": (
            sha256_value(capture["episode_evidence"])
            if isinstance(capture.get("episode_evidence"), dict)
            else None
        ),
        "requested_action": capture.get("requested_action"),
        "score_ref": capture.get("score_ref"),
        "original_content_hash": capture["content_hash"],
        "amendment_count": len(amendments),
        "amendment_event_ids": [event["event_id"] for event in amendments],
        "effective_evidence_hash": sha256_value(effective_evidence),
        "effective_target_hash": sha256_value(target),
        "evidence": effective_evidence,
    }


def status_document(
    events: list[dict[str, Any]],
    study_date: str | None,
    *,
    snapshot_hash: str | None = None,
) -> dict[str, Any]:
    state = replay(events)
    active_freezes = [
        freeze
        for freeze_id, freeze in state["freezes"].items()
        if freeze_id not in state["used_freezes"]
        and freeze_id not in state["aborted_freezes"]
    ]
    active_by_capture: dict[str, list[str]] = {}
    for freeze in active_freezes:
        for capture_id in freeze["capture_event_ids"]:
            active_by_capture.setdefault(capture_id, []).append(freeze["event_id"])
    selected = [
        capture
        for capture in state["captures"].values()
        if study_date is None or capture.get("study_date") == study_date
    ]
    selected.sort(key=lambda item: (item.get("recorded_at", ""), item["event_id"]))
    pending = []
    for capture in selected:
        capture_id = capture["event_id"]
        if capture_id in state["closed_by"]:
            continue
        item = pending_item(capture, state["amendments"][capture_id])
        item["active_freeze_ids"] = sorted(active_by_capture.get(capture_id, []))
        pending.append(item)
    closed = [
        {
            "event_id": capture["event_id"],
            "formal_id": capture["target"].get("formal_id"),
            "closeout_id": state["closed_by"][capture["event_id"]],
        }
        for capture in selected
        if capture["event_id"] in state["closed_by"]
    ]
    grouped: dict[str, list[str]] = {}
    for item in pending:
        key = item.get("formal_id") or f"source:{item.get('source_locator')}"
        grouped.setdefault(key, []).append(item["event_id"])
    return {
        "schema_version": STATUS_SCHEMA,
        "study_date": study_date,
        "pending_count": len(pending),
        "closed_count": len(closed),
        "pending": pending,
        "closed": closed,
        "target_groups": [
            {"target": key, "capture_event_ids": value}
            for key, value in sorted(grouped.items())
        ],
        "active_freezes": [
            {
                "freeze_id": freeze["event_id"],
                "study_date": freeze["study_date"],
                "scope": freeze["scope"],
                "capture_event_ids": freeze["capture_event_ids"],
                "formal_ids": [target["formal_id"] for target in freeze["targets"]],
                "recorded_at": freeze["recorded_at"],
            }
            for freeze in sorted(active_freezes, key=lambda item: item["event_id"])
            if any(
                capture_id in {capture["event_id"] for capture in selected}
                for capture_id in freeze["capture_event_ids"]
            )
        ],
        "ledger_hash": snapshot_hash if snapshot_hash is not None else ledger_hash(),
    }


def cmd_pending(args: argparse.Namespace) -> None:
    study_date = validate_date(args.date) if args.date else None
    with exclusive_lock(LOCK_PATH):
        events = load_jsonl(EVENTS_PATH)
        snapshot_hash = ledger_hash()
    document = status_document(events, study_date, snapshot_hash=snapshot_hash)
    print(json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True))


def event_prefix(events: list[dict[str, Any]]) -> dict[str, Any]:
    durable = [event for event in events if event.get("event_type") != "freeze"]
    identity = [
        {"event_id": event["event_id"], "content_hash": event["content_hash"]}
        for event in durable
    ]
    return {"event_count": len(durable), "events_hash": sha256_value(identity)}


def resolve_source_artifact(path_value: Any, field: str) -> Path:
    text = require_text(path_value, field, max_length=2000)
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise QuickIntakeError(f"{field} 指向的来源文件不存在：{text}") from exc
    repo = REPO_ROOT.resolve()
    if repo not in resolved.parents or not resolved.is_file():
        raise QuickIntakeError(f"{field} 必须指向仓库内持久化来源文件")
    return resolved


def validate_bound_source_artifact(
    path_value: Any,
    expected_hash: str,
    field: str,
    *,
    expected_reference: dict[str, str] | None = None,
    expected_date: str | None = None,
    expected_locator: str | None = None,
    require_question: bool = False,
) -> tuple[Path, list[Path]]:
    artifact = resolve_source_artifact(path_value, field)
    if file_sha256(artifact) != expected_hash:
        raise QuickIntakeError(f"{field} 当前文件哈希与绑定不一致")
    if expected_reference is not None:
        expected_path = resolve_source_artifact(
            expected_reference["manifest_path"],
            f"{field}.captured_manifest",
        )
        if artifact != expected_path or expected_hash != expected_reference["manifest_hash"]:
            raise QuickIntakeError(f"{field} 与 capture 的 source_bundle 不一致")
        _, manifest, children = validate_source_bundle_manifest(
            expected_reference["manifest_path"],
            f"{field}.captured_manifest",
            expected_hash=expected_reference["manifest_hash"],
            expected_date=expected_date,
            expected_locator=expected_locator,
            require_question=require_question,
        )
        return manifest, children
    try:
        staging_root = SOURCE_STAGING_ROOT.resolve(strict=True)
    except OSError:
        staging_root = None
    if staging_root is not None and staging_root in artifact.parents:
        _, manifest, children = validate_source_bundle_manifest(
            str(artifact.relative_to(REPO_ROOT.resolve())),
            field,
            expected_hash=expected_hash,
            expected_date=expected_date,
            expected_locator=expected_locator,
            require_question=require_question,
        )
        return manifest, children
    return artifact, []


def card_path_if_unique(card_id: str) -> tuple[Path | None, str | None]:
    if not CARD_ID_PATTERN.fullmatch(card_id):
        raise QuickIntakeError(f"无效正式错题 ID：{card_id}")
    matches = sorted(CARDS_DIR.glob(f"{card_id}_*.md"))
    if len(matches) > 1:
        raise QuickIntakeError(f"正式错题卡身份不唯一：{card_id}（实际 {len(matches)} 张）")
    if not matches:
        return None, None
    return matches[0], file_sha256(matches[0])


def normalize_freeze(value: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    required = {
        "schema_version",
        "study_date",
        "scope",
        "capture_event_ids",
        "target_resolutions",
    }
    missing = sorted(required - set(value))
    unexpected = sorted(set(value) - required)
    if missing:
        raise QuickIntakeError(f"夜间冻结输入缺少字段：{', '.join(missing)}")
    if unexpected:
        raise QuickIntakeError(f"夜间冻结输入含未知字段：{', '.join(unexpected)}")
    if value.get("schema_version") != FREEZE_SCHEMA:
        raise QuickIntakeError(f"schema_version 必须是 {FREEZE_SCHEMA}")
    study_date = validate_date(value.get("study_date"))
    scope = value.get("scope")
    if scope not in {"all_pending_for_date", "explicit_subset"}:
        raise QuickIntakeError("scope 必须是 all_pending_for_date 或 explicit_subset")
    capture_ids = value.get("capture_event_ids")
    if not isinstance(capture_ids, list) or not capture_ids:
        raise QuickIntakeError("capture_event_ids 必须是非空数组")
    if not all(isinstance(item, str) for item in capture_ids):
        raise QuickIntakeError("capture_event_ids 只能包含事件 ID")
    if len(capture_ids) != len(set(capture_ids)):
        raise QuickIntakeError("capture_event_ids 不得重复")
    capture_ids = sorted(capture_ids)

    state = replay(events)
    active_conflicts = sorted(
        freeze_id
        for freeze_id, freeze in state["freezes"].items()
        if freeze_id not in state["used_freezes"]
        and freeze_id not in state["aborted_freezes"]
        and set(capture_ids).intersection(freeze.get("capture_event_ids", []))
    )
    if active_conflicts:
        raise QuickIntakeError(
            "capture 已进入 active freeze；请复用或先安全 abort："
            + ", ".join(active_conflicts)
        )
    selected: dict[str, tuple[dict[str, Any], list[dict[str, Any]]]] = {}
    for capture_id in capture_ids:
        capture = state["captures"].get(capture_id)
        if capture is None:
            raise QuickIntakeError(f"夜间冻结引用未知 capture：{capture_id}")
        if capture_id in state["closed_by"]:
            raise QuickIntakeError(f"夜间冻结引用已关闭 capture：{capture_id}")
        if capture.get("study_date") != study_date:
            raise QuickIntakeError(f"capture 与 study_date 不一致：{capture_id}")
        selected[capture_id] = (capture, state["amendments"][capture_id])

    if scope == "all_pending_for_date":
        all_pending = sorted(
            capture_id
            for capture_id, capture in state["captures"].items()
            if capture.get("study_date") == study_date and capture_id not in state["closed_by"]
        )
        if capture_ids != all_pending:
            raise QuickIntakeError("all_pending_for_date 必须覆盖冻结时当天全部 pending capture")

    raw_resolutions = value.get("target_resolutions")
    if not isinstance(raw_resolutions, list) or not raw_resolutions:
        raise QuickIntakeError("target_resolutions 必须是非空数组")
    targets: list[dict[str, Any]] = []
    assigned: set[str] = set()
    formal_ids: set[str] = set()
    unit_index = load_unit_index()
    for index, resolution in enumerate(raw_resolutions):
        field = f"target_resolutions[{index}]"
        if not isinstance(resolution, dict):
            raise QuickIntakeError(f"{field} 必须是对象")
        expected_fields = {"formal_id", "capture_event_ids", "identity_mode", "source_binding"}
        if set(resolution) != expected_fields:
            raise QuickIntakeError(f"{field} 字段不完整")
        formal_id = resolution.get("formal_id")
        if not isinstance(formal_id, str) or not CARD_ID_PATTERN.fullmatch(formal_id):
            raise QuickIntakeError(f"{field}.formal_id 无效")
        if formal_id in formal_ids:
            raise QuickIntakeError(f"同一正式 ID 只能有一个冻结目标：{formal_id}")
        formal_ids.add(formal_id)
        grouped_ids = resolution.get("capture_event_ids")
        if not isinstance(grouped_ids, list) or not grouped_ids:
            raise QuickIntakeError(f"{field}.capture_event_ids 必须是非空数组")
        if not all(isinstance(item, str) and item in selected for item in grouped_ids):
            raise QuickIntakeError(f"{field}.capture_event_ids 含未冻结 capture")
        if len(grouped_ids) != len(set(grouped_ids)):
            raise QuickIntakeError(f"{field}.capture_event_ids 不得重复")
        grouped_ids = sorted(grouped_ids)
        overlap = assigned.intersection(grouped_ids)
        if overlap:
            raise QuickIntakeError("capture 被分配到多个正式目标：" + ", ".join(sorted(overlap)))
        assigned.update(grouped_ids)
        mode = resolution.get("identity_mode")
        if mode not in {"existing_formal", "new_source_created", "new_source_merged"}:
            raise QuickIntakeError(f"{field}.identity_mode 无效")
        capture_targets = [effective_target(*selected[capture_id]) for capture_id in grouped_ids]
        capture_bundles = [
            effective_source_bundle(*selected[capture_id]) for capture_id in grouped_ids
        ]
        bundle_bindings_by_path: dict[str, dict[str, str]] = {}
        for capture_id, target, source_bundle in zip(
            grouped_ids,
            capture_targets,
            capture_bundles,
        ):
            if source_bundle is None:
                continue
            validate_source_bundle_manifest(
                source_bundle["manifest_path"],
                f"{field}.capture[{capture_id}].source_bundle",
                expected_hash=source_bundle["manifest_hash"],
                expected_date=study_date,
                expected_locator=(
                    target.get("source_locator")
                    if target.get("kind") == "new_source"
                    else None
                ),
                require_question=target.get("kind") == "new_source",
            )
            binding = source_bundle_binding(source_bundle)
            existing_binding = bundle_bindings_by_path.get(binding["artifact_path"])
            if existing_binding is not None and existing_binding != binding:
                raise QuickIntakeError(f"{field} 同一来源清单出现冲突绑定")
            bundle_bindings_by_path[binding["artifact_path"]] = binding
        captured_bundle_bindings = [
            bundle_bindings_by_path[key] for key in sorted(bundle_bindings_by_path)
        ]
        path_before, hash_before = card_path_if_unique(formal_id)
        source_binding: dict[str, Any] | None = None
        supplemental_source_bundles: list[dict[str, str]] = []

        if mode == "existing_formal":
            if resolution.get("source_binding") is not None:
                raise QuickIntakeError(f"{field}.source_binding 对 existing_formal 必须为 null")
            if path_before is None or hash_before is None:
                raise QuickIntakeError(f"existing_formal 在冻结时不存在：{formal_id}")
            for target in capture_targets:
                if target.get("kind") != "formal_card" or target.get("formal_id") != formal_id:
                    raise QuickIntakeError(f"{field} 与 capture 正式身份冲突")
                if target.get("source_hash_before") != hash_before:
                    raise QuickIntakeError(f"{field} 正式卡在 capture 后已漂移，必须先重新裁决")
            supplemental_source_bundles = captured_bundle_bindings
        else:
            if any(target.get("kind") != "new_source" for target in capture_targets):
                raise QuickIntakeError(f"{field} 的 new_source 模式只能接收 new_source capture")
            bindings = resolution.get("source_binding")
            expected_binding_fields = {
                "source_locator",
                "resolved_source_hash",
                "artifact_path",
                "artifact_hash",
                "basis",
            }
            if not isinstance(bindings, dict) or set(bindings) != expected_binding_fields:
                raise QuickIntakeError(f"{field}.source_binding 字段不完整")
            locator = require_text(bindings.get("source_locator"), f"{field}.source_binding.source_locator")
            resolved_hash = validate_hash(
                bindings.get("resolved_source_hash"),
                f"{field}.source_binding.resolved_source_hash",
            )
            artifact = resolve_source_artifact(
                bindings.get("artifact_path"),
                f"{field}.source_binding.artifact_path",
            )
            artifact_hash = validate_hash(
                bindings.get("artifact_hash"),
                f"{field}.source_binding.artifact_hash",
            )
            if artifact_hash != resolved_hash:
                raise QuickIntakeError(f"{field}.source_binding 来源文件哈希不一致")
            basis = require_text(bindings.get("basis"), f"{field}.source_binding.basis")
            if len(captured_bundle_bindings) > 1:
                raise QuickIntakeError(f"{field} 同一 new_source 不能绑定多个来源包")
            captured_reference = None
            if captured_bundle_bindings:
                captured = captured_bundle_bindings[0]
                captured_reference = {
                    "manifest_path": captured["artifact_path"],
                    "manifest_hash": captured["artifact_hash"],
                    "source_locator": captured["source_locator"],
                    "study_date": study_date,
                }
            validate_bound_source_artifact(
                str(artifact.relative_to(REPO_ROOT.resolve())),
                artifact_hash,
                f"{field}.source_binding.artifact_path",
                expected_reference=captured_reference,
                expected_date=study_date,
                expected_locator=locator,
                require_question=captured_reference is not None,
            )
            for target in capture_targets:
                if target.get("source_locator") != locator:
                    raise QuickIntakeError(f"{field}.source_locator 与 capture 不一致")
                captured_hash = target.get("source_hash_before")
                if captured_hash is not None and captured_hash != resolved_hash:
                    raise QuickIntakeError(f"{field}.resolved_source_hash 与 capture 不一致")
            source_binding = {
                "source_locator": locator,
                "resolved_source_hash": resolved_hash,
                "artifact_path": str(artifact.relative_to(REPO_ROOT.resolve())),
                "artifact_hash": artifact_hash,
                "basis": basis,
            }
            if mode == "new_source_created" and path_before is not None:
                raise QuickIntakeError(f"new_source_created 的正式 ID 在冻结前已存在：{formal_id}")
            if mode == "new_source_merged" and path_before is None:
                raise QuickIntakeError(f"new_source_merged 的正式 ID 在冻结时不存在：{formal_id}")

        if path_before is not None:
            card_id_before, fast_refs_before, source_refs_before = formal_frontmatter_state(
                path_before
            )
            if card_id_before != formal_id:
                raise QuickIntakeError(f"{field} 正式卡 frontmatter id 不一致")
        else:
            fast_refs_before = set()
            source_refs_before = set()
        targets.append(
            {
                "formal_id": formal_id,
                "capture_event_ids": grouped_ids,
                "identity_mode": mode,
                "source_binding": source_binding,
                "supplemental_source_bundles": supplemental_source_bundles,
                "card_path_before": str(path_before.relative_to(REPO_ROOT)) if path_before else None,
                "card_hash_before": hash_before,
                "fast_intake_refs_before": sorted(fast_refs_before),
                "fast_intake_source_refs_before": sorted(source_refs_before),
                "rollback_before": rollback_unit_snapshot(unit_index.get(formal_id)),
            }
        )

    if assigned != set(capture_ids):
        missing_targets = sorted(set(capture_ids) - assigned)
        raise QuickIntakeError("存在未分配正式目标的 capture：" + ", ".join(missing_targets))

    snapshots = []
    for capture_id in capture_ids:
        capture, amendments = selected[capture_id]
        target = effective_target(capture, amendments)
        source_bundle = effective_source_bundle(capture, amendments)
        evidence = amendments[-1]["evidence"] if amendments else capture["evidence"]
        snapshots.append(
            {
                "capture_event_id": capture_id,
                "capture_content_hash": capture["content_hash"],
                "amendment_event_ids": [event["event_id"] for event in amendments],
                "effective_evidence_hash": sha256_value(evidence),
                "effective_target_hash": sha256_value(target),
                "effective_source_bundle_hash": (
                    sha256_value(source_bundle) if source_bundle is not None else None
                ),
                "episode_evidence_hash": (
                    sha256_value(capture["episode_evidence"])
                    if isinstance(capture.get("episode_evidence"), dict)
                    else None
                ),
                "requested_action": capture["requested_action"],
            }
        )
    targets.sort(key=lambda item: item["formal_id"])
    return {
        "freeze_schema_version": FREEZE_SCHEMA,
        "study_date": study_date,
        "scope": scope,
        "capture_event_ids": capture_ids,
        "capture_snapshots": snapshots,
        "targets": targets,
        "ledger_prefix": event_prefix(events),
    }


def freeze_response(event: dict[str, Any], status: str, elapsed_ms: float) -> dict[str, Any]:
    snapshots = {
        item["capture_event_id"]: item for item in event["capture_snapshots"]
    }
    return {
        "status": status,
        "state": "nightly_frozen",
        "freeze_id": event["event_id"],
        "capture_event_ids": event["capture_event_ids"],
        "formal_ids": [target["formal_id"] for target in event["targets"]],
        "formal_bindings": [
            {
                "formal_id": target["formal_id"],
                "fast_intake_refs": [
                    capture_ref_token(snapshots[capture_id])
                    for capture_id in target["capture_event_ids"]
                ],
                "fast_intake_source_ref": (
                    source_ref_token(target["source_binding"])
                    if target["source_binding"] is not None
                    else None
                ),
                "fast_intake_source_refs": [
                    source_ref_token(binding)
                    for binding in (
                        ([target["source_binding"]] if target["source_binding"] else [])
                        + target.get("supplemental_source_bundles", [])
                    )
                ],
            }
            for target in event["targets"]
        ],
        "content_hash": event["content_hash"],
        "elapsed_ms": round(elapsed_ms, 3),
    }


def cmd_freeze(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    consumable_path = (
        validate_consumable_payload_path(args.payload_file)
        if getattr(args, "consume_payload_file", False)
        else None
    )
    raw = read_json_document(args.payload_file)
    request_hash = sha256_value(raw)
    with exclusive_lock(LOCK_PATH):
        events = load_jsonl(EVENTS_PATH)
        state = replay(events)
        matching = [
            event
            for event in events
            if event.get("event_type") == "freeze"
            and event.get("request_hash") == request_hash
        ]
        if len(matching) > 1:
            raise QuickIntakeError("同一 freeze request_hash 在账本中不唯一")
        if matching and matching[0]["event_id"] not in state["aborted_freezes"]:
            event = matching[0]
            status = "noop"
        else:
            payload = normalize_freeze(raw, events)
            event_id = stable_event_id("MFI-FREEZE", payload)
            existing_by_id = next(
                (candidate for candidate in events if candidate["event_id"] == event_id),
                None,
            )
            if existing_by_id is not None:
                if event_id in state["aborted_freezes"]:
                    raise QuickIntakeError(
                        "相同 freeze 已 abort；修订或扩充目标后创建新 freeze"
                    )
                event = existing_by_id
                status = "noop"
            else:
                event = with_content_hash(
                    {
                        **payload,
                        "schema_version": LEDGER_SCHEMA,
                        "event_type": "freeze",
                        "event_id": event_id,
                        "request_hash": request_hash,
                        "recorded_at": now_local(),
                    }
                )
                append_jsonl(EVENTS_PATH, event)
                replay(load_jsonl(EVENTS_PATH))
                status = "recorded"
    response = freeze_response(event, status, (time.perf_counter() - started) * 1000)
    response["payload_file_consumed"] = consume_payload_file(consumable_path)
    print(json.dumps(response, ensure_ascii=False, sort_keys=True))


def markdown_frontmatter(text: str) -> str:
    if not text.startswith("---\n"):
        return ""
    marker = text.find("\n---\n", 4)
    return text[4:marker] if marker >= 0 else ""


def frontmatter_scalar(meta: str, key: str) -> str:
    match = re.search(rf"(?m)^{re.escape(key)}:\s*['\"]?([^'\"\n]+)", meta)
    return match.group(1).strip() if match else ""


def frontmatter_list(meta: str, key: str) -> list[str]:
    values: list[str] = []
    active = False
    for line in meta.splitlines():
        if re.fullmatch(rf"{re.escape(key)}:\s*", line):
            active = True
            continue
        if active and re.match(r"^[A-Za-z_][A-Za-z0-9_]*:", line):
            break
        if active:
            match = re.match(r"^\s*-\s+(.*)$", line)
            if match:
                values.append(match.group(1).strip().strip("'\""))
    return values


def capture_ref_token(snapshot: dict[str, Any]) -> str:
    return (
        f"capture-v1:{snapshot['capture_event_id']}:"
        f"{snapshot['effective_evidence_hash']}:{snapshot['requested_action']}"
    )


def source_ref_token(binding: dict[str, Any]) -> str:
    return "source-v1:" + sha256_value(
        {
            "source_locator": binding["source_locator"],
            "resolved_source_hash": binding["resolved_source_hash"],
        }
    )


def formal_frontmatter_state(path: Path) -> tuple[str, set[str], set[str]]:
    meta = markdown_frontmatter(path.read_text(encoding="utf-8"))
    return (
        frontmatter_scalar(meta, "id"),
        set(frontmatter_list(meta, "fast_intake_refs")),
        set(frontmatter_list(meta, "fast_intake_source_refs")),
    )


_SCHEDULER_COMPAT_MODULE: Any | None = None


def scheduler_source_version_for_card(formal_id: str, card_path: Path) -> str:
    """Recompute the targeted scheduler's canonical source version for one card.

    The rollback scheduler historically used a semantic canonical hash while the
    closeout writer used the raw Markdown hash.  Both hashes remain independently
    protected: the formal layer verifies raw bytes, and this helper verifies that
    a semantic rollback source version was produced from those same final bytes.
    """

    global _SCHEDULER_COMPAT_MODULE
    if _SCHEDULER_COMPAT_MODULE is None:
        scheduler_path = Path(__file__).with_name("scheduler.py")
        spec = importlib.util.spec_from_file_location(
            "math_rollback_scheduler_for_quick_intake",
            scheduler_path,
        )
        if spec is None or spec.loader is None:
            raise QuickIntakeError("无法加载回滚调度器来验证源版本")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        _SCHEDULER_COMPAT_MODULE = module
    scheduler = _SCHEDULER_COMPAT_MODULE
    data = scheduler.parse_card_frontmatter(card_path)
    if not data:
        raise QuickIntakeError(f"正式卡缺少可解析 frontmatter：{formal_id}")
    events = scheduler.extract_explicit_wrong_events(formal_id, data)
    metadata = scheduler.wrongnet_metadata_from_card(formal_id, data)
    method_fingerprint = scheduler.method_gap_fingerprint(data)
    return scheduler.source_sync_version(
        formal_id,
        data,
        metadata,
        events,
        method_fingerprint,
    )


def allowed_rollback_source_versions(
    formal_id: str,
    card_path: Path,
    raw_card_hash: str,
) -> set[str]:
    return {
        raw_card_hash,
        scheduler_source_version_for_card(formal_id, card_path),
    }


def add_verified_artifact(store: dict[str, str], path: Path) -> str:
    resolved = path.resolve(strict=True)
    relative = str(resolved.relative_to(REPO_ROOT.resolve()))
    digest = file_sha256(resolved)
    existing = store.get(relative)
    if existing is not None and existing != digest:
        raise QuickIntakeError(f"同一验证文件出现两个哈希：{relative}")
    store[relative] = digest
    return digest


def add_verified_source_binding(
    store: dict[str, str],
    binding: dict[str, Any],
    field: str,
) -> None:
    artifact_hash = validate_hash(binding.get("artifact_hash"), f"{field}.artifact_hash")
    artifact, children = validate_bound_source_artifact(
        binding.get("artifact_path"),
        artifact_hash,
        f"{field}.artifact_path",
    )
    add_verified_artifact(store, artifact)
    for child in children:
        add_verified_artifact(store, child)


def canonical_unit_hash(unit: dict[str, Any]) -> str:
    return sha256_value(unit)


def load_unit_index() -> dict[str, dict[str, Any]]:
    try:
        raw = json.loads(UNITS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QuickIntakeError("无法验证复习单元权威状态") from exc
    if not isinstance(raw, list):
        raise QuickIntakeError("复习单元.json 顶层必须是数组")
    result: dict[str, dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        if item.get("类型") != "错题":
            continue
        formal_id = item.get("关联错题ID")
        if not isinstance(formal_id, str):
            continue
        if formal_id in result:
            raise QuickIntakeError(f"回滚层同一正式 ID 出现多个单元：{formal_id}")
        result[formal_id] = item
    return result


def rollback_unit_snapshot(unit: dict[str, Any] | None) -> dict[str, Any] | None:
    if unit is None:
        return None
    sync = unit.get("定点同步状态") if isinstance(unit.get("定点同步状态"), dict) else {}
    processed = sync.get("已处理错题事件ID")
    if not isinstance(processed, list):
        processed = []
    history = unit.get("调度事件历史")
    if not isinstance(history, list):
        history = []
    history_ids = [
        item.get("事件ID")
        for item in history
        if isinstance(item, dict) and isinstance(item.get("事件ID"), str)
    ]
    return {
        "unit_id": unit.get("复习单元ID"),
        "unit_hash": canonical_unit_hash(unit),
        "processed_event_ids": sorted(
            item for item in processed if isinstance(item, str)
        ),
        "history_event_ids": sorted(history_ids),
    }


def freeze_formal_state_changed(freeze: dict[str, Any]) -> bool:
    unit_index = load_unit_index()
    for target in freeze["targets"]:
        formal_id = target["formal_id"]
        path, current_hash = card_path_if_unique(formal_id)
        current_path = str(path.relative_to(REPO_ROOT)) if path is not None else None
        if current_path != target.get("card_path_before"):
            return True
        if current_hash != target.get("card_hash_before"):
            return True
        current_rollback = rollback_unit_snapshot(unit_index.get(formal_id))
        if current_rollback != target.get("rollback_before"):
            return True
    return False


def cmd_abort_freeze(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    freeze_id = args.freeze_id
    if not isinstance(freeze_id, str) or not re.fullmatch(
        r"MFI-FREEZE-[0-9a-f]{24}", freeze_id
    ):
        raise QuickIntakeError("freeze_id 无效")
    reason = require_text(args.reason, "reason", max_length=1000)
    with exclusive_lock(FORMAL_LOCK_PATH):
        with exclusive_lock(LOCK_PATH):
            events = load_jsonl(EVENTS_PATH)
            state = replay(events)
            freeze = state["freezes"].get(freeze_id)
            if freeze is None:
                raise QuickIntakeError("freeze_id 未知")
            existing = next(
                (
                    event
                    for event in events
                    if event.get("event_type") == "freeze_abort"
                    and event.get("freeze_id") == freeze_id
                ),
                None,
            )
            if existing is not None:
                event = existing
                status = "noop"
            else:
                if freeze_id in state["used_freezes"]:
                    raise QuickIntakeError("已成功关闭的 freeze 不得 abort")
                if freeze_formal_state_changed(freeze):
                    raise QuickIntakeError(
                        "freeze 后正式卡或回滚单元已变化，不能自动 abort；"
                        "必须先显式裁决并人工核对已写正式状态"
                    )
                event = with_content_hash(
                    {
                        "schema_version": LEDGER_SCHEMA,
                        "event_type": "freeze_abort",
                        "event_id": stable_event_id(
                            "MFI-ABORT", {"freeze_id": freeze_id}
                        ),
                        "freeze_id": freeze_id,
                        "capture_event_ids": freeze["capture_event_ids"],
                        "reason": reason,
                        "aborted_at": now_local(),
                    }
                )
                replay([*events, event])
                append_jsonl(EVENTS_PATH, event)
                status = "recorded"
    print(
        json.dumps(
            {
                "status": status,
                "state": "freeze_aborted",
                "freeze_id": freeze_id,
                "capture_event_ids": event["capture_event_ids"],
                "content_hash": event["content_hash"],
                "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


def verify_freeze_current(freeze: dict[str, Any], state: dict[str, Any]) -> None:
    for snapshot in freeze["capture_snapshots"]:
        capture_id = snapshot["capture_event_id"]
        capture = state["captures"].get(capture_id)
        if capture is None or capture_id in state["closed_by"]:
            raise QuickIntakeError(f"冻结 capture 已不存在或已关闭：{capture_id}")
        amendments = state["amendments"][capture_id]
        evidence = amendments[-1]["evidence"] if amendments else capture["evidence"]
        target = effective_target(capture, amendments)
        source_bundle = effective_source_bundle(capture, amendments)
        if capture["content_hash"] != snapshot["capture_content_hash"]:
            raise QuickIntakeError(f"capture 内容哈希与 freeze 不一致：{capture_id}")
        if [event["event_id"] for event in amendments] != snapshot["amendment_event_ids"]:
            raise QuickIntakeError(f"freeze 后出现 amendment：{capture_id}")
        if sha256_value(evidence) != snapshot["effective_evidence_hash"]:
            raise QuickIntakeError(f"有效证据哈希与 freeze 不一致：{capture_id}")
        if sha256_value(target) != snapshot["effective_target_hash"]:
            raise QuickIntakeError(f"有效目标哈希与 freeze 不一致：{capture_id}")
        if "effective_source_bundle_hash" in snapshot:
            current_bundle_hash = (
                sha256_value(source_bundle) if source_bundle is not None else None
            )
            if current_bundle_hash != snapshot["effective_source_bundle_hash"]:
                raise QuickIntakeError(f"来源包哈希与 freeze 不一致：{capture_id}")
        current_episode_hash = (
            sha256_value(capture["episode_evidence"])
            if isinstance(capture.get("episode_evidence"), dict)
            else None
        )
        if snapshot.get("episode_evidence_hash") != current_episode_hash:
            raise QuickIntakeError(f"整题教学证据与 freeze 不一致：{capture_id}")
        if source_bundle is not None:
            validate_source_bundle_manifest(
                source_bundle["manifest_path"],
                f"freeze.capture[{capture_id}].source_bundle",
                expected_hash=source_bundle["manifest_hash"],
                expected_date=capture["study_date"],
                expected_locator=(
                    target.get("source_locator")
                    if target.get("kind") == "new_source"
                    else None
                ),
                require_question=target.get("kind") == "new_source",
            )
    frozen_capture_ids = set(freeze["capture_event_ids"])
    frozen_formal_ids = {target["formal_id"] for target in freeze["targets"]}
    for capture_id, capture in state["captures"].items():
        if capture_id in frozen_capture_ids or capture_id in state["closed_by"]:
            continue
        amendments = state["amendments"][capture_id]
        target = effective_target(capture, amendments)
        formal_id = target.get("formal_id")
        if formal_id not in frozen_formal_ids:
            continue
        _, current_hash = card_path_if_unique(formal_id)
        if target.get("source_hash_before") != current_hash:
            raise QuickIntakeError(
                f"active freeze 后出现同目标且来源版本过期的 capture：{capture_id}；"
                "先用 source-verified amendment 将该 capture rebase 到当前正式卡哈希"
            )
    # all_pending_for_date 约束的是 freeze 创建时的全量快照。freeze 之后
    # 新增的无关 capture 留给后续批次，不得使已经开始正式工作的旧 freeze 失效。


def normalize_formal_results(
    value: Any,
    freeze: dict[str, Any],
    verified: dict[str, str],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    if not isinstance(value, list):
        raise QuickIntakeError("formal_results 必须是数组")
    frozen_targets = {target["formal_id"]: target for target in freeze["targets"]}
    results: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(value):
        field = f"formal_results[{index}]"
        if not isinstance(item, dict) or set(item) != {
            "formal_id",
            "operation",
            "card_path_after",
            "card_hash_after",
            "changed_fields",
        }:
            raise QuickIntakeError(f"{field} 字段不完整")
        formal_id = item.get("formal_id")
        target = frozen_targets.get(formal_id)
        if target is None or formal_id in results:
            raise QuickIntakeError(f"{field}.formal_id 不在 freeze 或重复")
        card_path, actual_hash = formal_card_identity(formal_id)
        supplied_path = resolve_repo_artifact(item.get("card_path_after"), f"{field}.card_path_after")
        if supplied_path != card_path.resolve():
            raise QuickIntakeError(f"{field}.card_path_after 与正式卡身份不一致")
        after_hash = validate_hash(item.get("card_hash_after"), f"{field}.card_hash_after")
        if actual_hash != after_hash:
            raise QuickIntakeError(f"{field}.card_hash_after 与当前正式卡不一致")
        changed_fields = item.get("changed_fields")
        if not isinstance(changed_fields, list) or not all(
            isinstance(entry, str) and entry.strip() for entry in changed_fields
        ):
            raise QuickIntakeError(f"{field}.changed_fields 必须是文本数组")
        changed_fields = [entry.strip() for entry in changed_fields]
        operation = item.get("operation")
        before_hash = target.get("card_hash_before")
        expected_operation = "created" if target["identity_mode"] == "new_source_created" else None
        if expected_operation == "created":
            if operation != "created" or before_hash is not None or not changed_fields:
                raise QuickIntakeError(f"{field} 新卡必须声明 created 且有 changed_fields")
        elif operation == "updated":
            if before_hash == after_hash or not changed_fields:
                raise QuickIntakeError(f"{field} updated 的哈希或 changed_fields 无效")
        elif operation == "unchanged":
            if before_hash != after_hash or changed_fields:
                raise QuickIntakeError(f"{field} unchanged 的哈希或 changed_fields 无效")
        else:
            raise QuickIntakeError(f"{field}.operation 无效")
        card_id_after, fast_refs_after, source_refs_after = formal_frontmatter_state(card_path)
        if card_id_after != formal_id:
            raise QuickIntakeError(f"{field} 正式卡 frontmatter id 不一致")
        source_refs_before = set(target["fast_intake_source_refs_before"])
        expected_source_refs: set[str] = set()
        primary_binding = target.get("source_binding")
        all_bindings = (
            ([primary_binding] if isinstance(primary_binding, dict) else [])
            + target.get("supplemental_source_bundles", [])
        )
        for binding_index, binding in enumerate(all_bindings):
            binding_field = f"{field}.source_bindings[{binding_index}]"
            if binding.get("artifact_hash") != binding.get("resolved_source_hash"):
                raise QuickIntakeError(f"{binding_field} 来源哈希字段不一致")
            add_verified_source_binding(verified, binding, binding_field)
            expected_source_ref = source_ref_token(binding)
            if binding is primary_binding and expected_source_ref in source_refs_before:
                raise QuickIntakeError(f"{field} 新来源正式卡缺少新增结构化 source binding")
            expected_source_refs.add(expected_source_ref)
        new_source_refs = expected_source_refs - source_refs_before
        if new_source_refs and operation == "unchanged":
            raise QuickIntakeError(f"{field} 新来源引用要求正式卡发生更新")
        if target["identity_mode"].startswith("new_source"):
            if target["identity_mode"] == "new_source_merged" and operation != "updated":
                raise QuickIntakeError(f"{field} new_source_merged 必须更新正式卡")
        if source_refs_after != source_refs_before | expected_source_refs:
            raise QuickIntakeError(f"{field} fast_intake_source_refs 未精确保留冻结集合")
        add_verified_artifact(verified, card_path)
        normalized = {
            "formal_id": formal_id,
            "operation": operation,
            "card_path_after": str(card_path.relative_to(REPO_ROOT)),
            "card_hash_after": after_hash,
            "changed_fields": changed_fields,
            "fast_intake_refs_after": sorted(fast_refs_after),
            "fast_intake_source_refs_after": sorted(source_refs_after),
        }
        results[formal_id] = normalized
    if set(results) != set(frozen_targets):
        raise QuickIntakeError("formal_results 必须与 freeze 目标一一对应")
    ordered = [results[key] for key in sorted(results)]
    return ordered, results


def verify_rollback_record(
    durable: dict[str, Any],
    formal_id: str,
    after_hash: str,
    card_path: Path,
    unit_index: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if set(durable) != {"store", "record_id", "record_hash"}:
        raise QuickIntakeError("durable_record 字段不完整")
    if durable.get("store") != "rollback_unit":
        raise QuickIntakeError("做错或复发结果必须绑定 rollback_unit")
    record_id = require_text(durable.get("record_id"), "durable_record.record_id")
    record_hash = validate_hash(durable.get("record_hash"), "durable_record.record_hash")
    unit = unit_index.get(formal_id)
    if unit is None:
        raise QuickIntakeError(f"回滚层缺少目标单元：{formal_id}")
    if canonical_unit_hash(unit) != record_hash:
        raise QuickIntakeError(f"回滚单元哈希与回执不一致：{formal_id}")
    sync = unit.get("定点同步状态")
    allowed_versions = allowed_rollback_source_versions(formal_id, card_path, after_hash)
    if not isinstance(sync, dict) or sync.get("源版本") not in allowed_versions:
        raise QuickIntakeError(f"回滚单元源版本未对齐正式卡：{formal_id}")
    processed = sync.get("已处理错题事件ID")
    if not isinstance(processed, list) or record_id not in processed:
        raise QuickIntakeError(f"回滚单元未处理 durable record：{record_id}")
    return {"store": "rollback_unit", "record_id": record_id, "record_hash": record_hash}


def verify_rollback_batch(
    results: dict[str, dict[str, Any]],
    freeze: dict[str, Any],
    formal_by_id: dict[str, dict[str, Any]],
    unit_index: dict[str, dict[str, Any]],
) -> None:
    for target in freeze["targets"]:
        formal_id = target["formal_id"]
        wrong_results = [
            results[capture_id]
            for capture_id in target["capture_event_ids"]
            if results[capture_id]["outcome"] in {"wrong_recorded", "recurrence_recorded"}
        ]
        if not wrong_results:
            continue
        if formal_by_id[formal_id]["operation"] == "unchanged":
            raise QuickIntakeError(f"做错或复发必须更新或创建正式卡：{formal_id}")
        unit = unit_index.get(formal_id)
        if unit is None:
            raise QuickIntakeError(f"回滚层缺少目标单元：{formal_id}")
        before = target.get("rollback_before")
        if before is not None and unit.get("复习单元ID") != before.get("unit_id"):
            raise QuickIntakeError(f"回滚单元身份在 freeze 后发生变化：{formal_id}")
        sync = unit.get("定点同步状态")
        processed = sync.get("已处理错题事件ID") if isinstance(sync, dict) else None
        if not isinstance(processed, list) or not all(isinstance(item, str) for item in processed):
            raise QuickIntakeError(f"回滚单元 processed event 结构无效：{formal_id}")
        expected_ids = [item["durable_record"]["record_id"] for item in wrong_results]
        if len(expected_ids) != len(set(expected_ids)):
            raise QuickIntakeError(f"多个 capture 不得复用同一回滚事件：{formal_id}")
        for record_id in expected_ids:
            parts = record_id.split("|")
            if len(parts) < 3 or parts[0] != formal_id or parts[1] != freeze["study_date"]:
                raise QuickIntakeError(f"回滚事件身份或日期与 freeze 不一致：{record_id}")
        processed_set = set(processed)
        expected_set = set(expected_ids)
        before_processed = set(before["processed_event_ids"]) if before is not None else set()
        if expected_set.intersection(before_processed):
            raise QuickIntakeError(f"本批回滚 processed events 在 freeze 前已存在：{formal_id}")
        historical_baseline = processed_set - before_processed - expected_set
        for record_id in historical_baseline:
            parts = record_id.split("|")
            try:
                event_date = date.fromisoformat(parts[1]) if len(parts) == 3 else None
                ordinal = int(parts[2]) if len(parts) == 3 else 0
            except (TypeError, ValueError):
                event_date = None
                ordinal = 0
            if (
                len(parts) != 3
                or parts[0] != formal_id
                or event_date is None
                or event_date >= date.fromisoformat(freeze["study_date"])
                or ordinal <= 0
            ):
                raise QuickIntakeError(
                    f"回滚单元出现非冻结批次且非旧历史基线的事件：{record_id}"
                )
        if before is None:
            if processed_set != historical_baseline | expected_set:
                raise QuickIntakeError(f"新回滚单元 processed events 与本批 capture 不精确匹配：{formal_id}")
        else:
            if processed_set != before_processed | historical_baseline | expected_set:
                raise QuickIntakeError(f"回滚 processed events 与冻结批次不精确匹配：{formal_id}")

        history = unit.get("调度事件历史")
        if not isinstance(history, list):
            history = []
        history_by_id: dict[str, list[dict[str, Any]]] = {}
        for entry in history:
            if isinstance(entry, dict) and isinstance(entry.get("事件ID"), str):
                history_by_id.setdefault(entry["事件ID"], []).append(entry)
        recurrence_ids = {
            item["durable_record"]["record_id"]
            for item in wrong_results
            if item["outcome"] == "recurrence_recorded"
        }
        for record_id in recurrence_ids:
            matches = history_by_id.get(record_id, [])
            if len(matches) != 1:
                raise QuickIntakeError(f"复发回滚事件历史不唯一：{record_id}")
            entry = matches[0]
            card_path = REPO_ROOT / formal_by_id[formal_id]["card_path_after"]
            allowed_versions = allowed_rollback_source_versions(
                formal_id,
                card_path,
                formal_by_id[formal_id]["card_hash_after"],
            )
            if (
                entry.get("类型") != "正式错题复发"
                or entry.get("复发日期") != freeze["study_date"]
                or entry.get("源版本") not in allowed_versions
                or entry.get("源版本") != sync.get("源版本")
            ):
                raise QuickIntakeError(f"复发回滚事件没有绑定本批日期与正式卡：{record_id}")
        before_history = set(before["history_event_ids"]) if before is not None else set()
        after_history = set(history_by_id)
        if not before_history.issubset(after_history):
            raise QuickIntakeError(f"回滚历史删除了 freeze 前事件：{formal_id}")
        if not recurrence_ids.issubset(after_history - before_history):
            raise QuickIntakeError(f"复发事件不是 freeze 后新增：{formal_id}")


def normalize_capture_results(
    value: Any,
    freeze: dict[str, Any],
    state: dict[str, Any],
    formal_by_id: dict[str, dict[str, Any]],
    verified: dict[str, str],
) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise QuickIntakeError("capture_results 必须是数组")
    snapshot_by_id = {
        snapshot["capture_event_id"]: snapshot for snapshot in freeze["capture_snapshots"]
    }
    formal_for_capture = {
        capture_id: target["formal_id"]
        for target in freeze["targets"]
        for capture_id in target["capture_event_ids"]
    }
    allowed_outcomes = {
        "record_wrong": {"wrong_recorded"},
        "record_recurrence": {"recurrence_recorded"},
        "update_representation": {"representation_updated", "representation_already_current"},
        "mastery_candidate": {"mastery_confirmed", "mastery_rejected"},
    }
    unit_index = load_unit_index()
    add_verified_artifact(verified, UNITS_PATH)
    results: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(value):
        field = f"capture_results[{index}]"
        if not isinstance(item, dict) or set(item) != {
            "capture_event_id",
            "outcome",
            "formal_id",
            "durable_record",
        }:
            raise QuickIntakeError(f"{field} 字段不完整")
        capture_id = item.get("capture_event_id")
        snapshot = snapshot_by_id.get(capture_id)
        if snapshot is None or capture_id in results:
            raise QuickIntakeError(f"{field}.capture_event_id 不在 freeze 或重复")
        formal_id = item.get("formal_id")
        if formal_id != formal_for_capture[capture_id]:
            raise QuickIntakeError(f"{field}.formal_id 与 freeze 映射冲突")
        outcome = item.get("outcome")
        if outcome not in allowed_outcomes[snapshot["requested_action"]]:
            raise QuickIntakeError(f"{field}.outcome 与 requested_action 冲突")
        formal = formal_by_id[formal_id]
        durable = item.get("durable_record")
        if not isinstance(durable, dict):
            raise QuickIntakeError(f"{field}.durable_record 必须是对象")
        if outcome in {"wrong_recorded", "recurrence_recorded"}:
            durable = verify_rollback_record(
                durable,
                formal_id,
                formal["card_hash_after"],
                REPO_ROOT / formal["card_path_after"],
                unit_index,
            )
        else:
            if set(durable) != {"store", "record_id", "record_hash"}:
                raise QuickIntakeError(f"{field}.durable_record 字段不完整")
            store_name = durable.get("store")
            record_id = durable.get("record_id")
            record_hash = validate_hash(durable.get("record_hash"), f"{field}.durable_record.record_hash")
            if outcome in {"representation_updated", "mastery_confirmed"}:
                if store_name != "formal_card" or record_id != formal_id:
                    raise QuickIntakeError(f"{field} 必须绑定 formal_card")
                if record_hash != formal["card_hash_after"]:
                    raise QuickIntakeError(f"{field} 正式卡 durable hash 不一致")
                if outcome == "representation_updated" and formal["operation"] == "unchanged":
                    raise QuickIntakeError(f"{field} representation_updated 不能对应 unchanged")
                if outcome == "mastery_confirmed":
                    card_path, _ = formal_card_identity(formal_id)
                    card_meta = markdown_frontmatter(card_path.read_text(encoding="utf-8"))
                    if frontmatter_scalar(card_meta, "status") != "已掌握":
                        raise QuickIntakeError(f"{field} 没有可验证的已掌握状态")
                    if formal["operation"] == "unchanged":
                        raise QuickIntakeError(f"{field} mastery_confirmed 必须持久化本批裁决")
                    unit = unit_index.get(formal_id)
                    if unit is not None and (
                        unit.get("已掌握") is not True
                        or unit.get("掌握状态") != "已掌握"
                        or not isinstance(unit.get("定点同步状态"), dict)
                        or unit["定点同步状态"].get("源版本") != formal["card_hash_after"]
                    ):
                        raise QuickIntakeError(f"{field} 正式卡与回滚单元掌握状态不一致")
            else:
                if store_name != "closeout" or record_id != capture_id:
                    raise QuickIntakeError(f"{field} 必须由 closeout 保存裁决")
                if record_hash != snapshot["effective_evidence_hash"]:
                    raise QuickIntakeError(f"{field} closeout durable hash 不一致")
                if outcome == "representation_already_current" and formal["operation"] != "unchanged":
                    raise QuickIntakeError(f"{field} already_current 必须对应 unchanged")
            durable = {"store": store_name, "record_id": record_id, "record_hash": record_hash}
        results[capture_id] = {
            "capture_event_id": capture_id,
            "outcome": outcome,
            "formal_id": formal_id,
            "durable_record": durable,
        }
    if set(results) != set(snapshot_by_id):
        raise QuickIntakeError("capture_results 必须与 freeze capture 一一对应")
    snapshots_for_target = {
        target["formal_id"]: [snapshot_by_id[item] for item in target["capture_event_ids"]]
        for target in freeze["targets"]
    }
    for target in freeze["targets"]:
        formal_id = target["formal_id"]
        before_refs = set(target["fast_intake_refs_before"])
        after_refs = set(formal_by_id[formal_id]["fast_intake_refs_after"])
        expected_refs = (
            {capture_ref_token(snapshot) for snapshot in snapshots_for_target[formal_id]}
            if formal_by_id[formal_id]["operation"] != "unchanged"
            else set()
        )
        if after_refs != before_refs | expected_refs:
            raise QuickIntakeError(f"正式卡 fast_intake_refs 与冻结 capture 不精确匹配：{formal_id}")
    verify_rollback_batch(results, freeze, formal_by_id, unit_index)
    return [results[key] for key in sorted(results)]


def current_formal_projections(formal_ids: set[str]) -> dict[str, dict[str, Any]]:
    spec = importlib.util.spec_from_file_location("math_fast_intake_wrongnet", WRONGNET_TOOL_PATH)
    if spec is None or spec.loader is None:
        raise QuickIntakeError("无法加载 canonical wrongnet parser")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except (OSError, ImportError, SyntaxError) as exc:
        raise QuickIntakeError("无法加载 canonical wrongnet parser") from exc
    module.BASE_DIR = REPO_ROOT / "错题知识网络"
    module.CARDS_DIR = CARDS_DIR
    module.OUT_DIR = REPO_ROOT / "错题知识网络" / "生成"
    try:
        cards = module.read_cards()
    except (OSError, ValueError, TypeError) as exc:
        raise QuickIntakeError("canonical wrongnet parser 无法读取正式卡") from exc
    grouped: dict[str, list[dict[str, Any]]] = {}
    for card in cards:
        card_id = card.get("id")
        if card_id in formal_ids:
            grouped.setdefault(card_id, []).append(card)
    projections: dict[str, dict[str, Any]] = {}
    for formal_id in sorted(formal_ids):
        matches = grouped.get(formal_id, [])
        if len(matches) != 1:
            raise QuickIntakeError(f"canonical wrongnet 正式目标身份不唯一：{formal_id}")
        card = matches[0]
        projections[formal_id] = {
            "id": card["id"],
            "path": card["relpath"],
            "meta": card["meta"],
            "topic_chains": card.get("topic_chains", []),
        }
    return projections


def verify_wrongnet_receipt(
    value: Any,
    formal_by_id: dict[str, dict[str, Any]],
    verified: dict[str, str],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], str]:
    if not isinstance(value, dict) or set(value) != {
        "rebuild_count",
        "artifacts",
        "target_projection_hashes",
    }:
        raise QuickIntakeError("batch_receipts.wrongnet 字段不完整")
    if value.get("rebuild_count") != 1:
        raise QuickIntakeError("夜间批次必须只声明一次 successful wrongnet rebuild")
    artifacts = normalize_hash_artifacts(value.get("artifacts"), "batch_receipts.wrongnet.artifacts")
    required_paths = {
        "错题知识网络/生成/wrong_questions.json",
        "错题知识网络/生成/错题索引.md",
        "错题知识网络/生成/相似题清单.md",
        "错题知识网络/生成/强关联清单.md",
        "错题知识网络/生成/弱边审计清单.md",
        "错题知识网络/生成/专题链关联清单.md",
        "错题知识网络/生成/错题网络概览.md",
        "错题知识网络/生成/按知识点复做清单.md",
        "错题知识网络/生成/知识网络图.mmd",
    }
    if {item["path"] for item in artifacts} != required_paths:
        raise QuickIntakeError("wrongnet artifacts 必须精确覆盖本次 rebuild 的九个生成物")
    for item in artifacts:
        verified[item["path"]] = item["sha256"]
    try:
        snapshot = json.loads(WRONGNET_SNAPSHOT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QuickIntakeError("wrong_questions.json 无法解析") from exc
    artifact_date = validate_date(snapshot.get("updated_at"), "wrongnet.updated_at")
    cards = snapshot.get("cards")
    if not isinstance(cards, list):
        raise QuickIntakeError("wrongnet 主快照缺少 cards")
    by_id: dict[str, list[dict[str, Any]]] = {}
    for card in cards:
        if isinstance(card, dict) and isinstance(card.get("id"), str):
            by_id.setdefault(card["id"], []).append(card)
    supplied_hashes = value.get("target_projection_hashes")
    if not isinstance(supplied_hashes, dict) or set(supplied_hashes) != set(formal_by_id):
        raise QuickIntakeError("target_projection_hashes 必须与正式目标集一致")
    current_projections = current_formal_projections(set(formal_by_id))
    normalized_hashes: dict[str, str] = {}
    for formal_id, formal in formal_by_id.items():
        matches = by_id.get(formal_id, [])
        if len(matches) != 1:
            raise QuickIntakeError(f"wrongnet 主快照目标身份不唯一：{formal_id}")
        projection = matches[0]
        expected_path = str(Path(formal["card_path_after"]).relative_to("错题知识网络"))
        if projection.get("path") != expected_path:
            raise QuickIntakeError(f"wrongnet 目标路径与正式卡不一致：{formal_id}")
        digest = validate_hash(supplied_hashes.get(formal_id), f"target_projection_hashes.{formal_id}")
        if sha256_value(projection) != digest:
            raise QuickIntakeError(f"wrongnet 目标 projection hash 不一致：{formal_id}")
        if sha256_value(current_projections[formal_id]) != digest:
            raise QuickIntakeError(f"wrongnet 目标 projection 未反映当前正式卡：{formal_id}")
        normalized_hashes[formal_id] = digest
    return (
        {
            "rebuild_count": 1,
            "artifact_date": artifact_date,
            "artifacts": artifacts,
            "target_projection_hashes": {
                key: normalized_hashes[key] for key in sorted(normalized_hashes)
            },
        },
        current_projections,
        artifact_date,
    )


def row_count(path: Path, formal_id: str) -> int:
    pattern = re.compile(rf"^\|\s*{re.escape(formal_id)}\s*\|")
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if pattern.match(line))


def wiki_subject_coverage(formal_id: str) -> Path:
    candidates: list[Path] = []
    for path in sorted((WIKI_ROOT / "coverage").glob("MATHWIKI-COVERAGE-??_*.md")):
        count = row_count(path, formal_id)
        if count > 1:
            raise QuickIntakeError(f"Wiki 学科覆盖表重复目标行：{path.name}")
        if count == 1:
            candidates.append(path)
    if len(candidates) != 1:
        raise QuickIntakeError(f"Wiki 学科覆盖表目标身份不唯一：{formal_id}")
    return candidates[0]


def wiki_cluster_index() -> dict[str, Path]:
    result: dict[str, Path] = {}
    for path in WIKI_ROOT.rglob("*.md"):
        meta = markdown_frontmatter(path.read_text(encoding="utf-8", errors="replace"))
        wiki_id = frontmatter_scalar(meta, "wiki_id")
        if wiki_id and "INDEX" not in wiki_id:
            if wiki_id in result:
                raise QuickIntakeError(f"Wiki 稳定 ID 重复：{wiki_id}")
            result[wiki_id] = path
    return result


def verify_wiki_receipts(
    value: Any,
    formal_by_id: dict[str, dict[str, Any]],
    projection_hashes: dict[str, str],
    target_projections: dict[str, dict[str, Any]],
    artifact_date: str,
    verified: dict[str, str],
) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise QuickIntakeError("batch_receipts.wiki 必须是数组")
    receipts: dict[str, dict[str, Any]] = {}
    clusters = wiki_cluster_index()
    source_index = WIKI_ROOT / "sources" / "SRC-WRONGCARDS-INDEX_全量错题卡覆盖索引.md"
    matrix = WIKI_ROOT / "coverage" / "MATHWIKI-COVERAGE-MATRIX_错题卡多维编译矩阵.md"
    for fixed in (source_index, matrix):
        if not fixed.exists():
            raise QuickIntakeError(f"Wiki 固定索引缺失：{fixed}")
        add_verified_artifact(verified, fixed)
    for index, item in enumerate(value):
        field = f"batch_receipts.wiki[{index}]"
        if not isinstance(item, dict) or set(item) != {"formal_id", "summary_path", "summary_hash"}:
            raise QuickIntakeError(f"{field} 字段不完整")
        formal_id = item.get("formal_id")
        if formal_id not in formal_by_id or formal_id in receipts:
            raise QuickIntakeError(f"{field}.formal_id 不在正式目标或重复")
        expected_summary = WIKI_ROOT / "sources" / "wrong_cards" / f"SRC-WQ-{formal_id}.md"
        summary = resolve_repo_artifact(item.get("summary_path"), f"{field}.summary_path")
        if summary != expected_summary.resolve():
            raise QuickIntakeError(f"{field}.summary_path 不是目标 source summary")
        summary_hash = validate_hash(item.get("summary_hash"), f"{field}.summary_hash")
        if file_sha256(summary) != summary_hash:
            raise QuickIntakeError(f"{field}.summary_hash 与当前文件不一致")
        text = summary.read_text(encoding="utf-8")
        if any(ord(char) < 32 and char not in "\n\r\t" for char in text):
            raise QuickIntakeError(f"{field} 含控制字符")
        meta = markdown_frontmatter(text)
        required_meta_fields = {
            "wiki_id",
            "subject",
            "knowledge",
            "methods",
            "error_causes",
            "wrongnet_refs",
            "source_refs",
            "wiki_refs",
            "last_updated",
            "formal_projection_sha256",
        }
        missing_meta = sorted(
            key
            for key in required_meta_fields
            if not re.search(rf"(?m)^{re.escape(key)}:", meta)
        )
        if missing_meta:
            raise QuickIntakeError(f"{field} 缺少必要字段：{', '.join(missing_meta)}")
        if frontmatter_scalar(meta, "wiki_id") != f"SRC-WQ-{formal_id}":
            raise QuickIntakeError(f"{field} wiki_id 不一致")
        if frontmatter_scalar(meta, "last_updated") != artifact_date:
            raise QuickIntakeError(f"{field} last_updated 不是本次产物日期")
        if frontmatter_scalar(meta, "formal_projection_sha256") != projection_hashes[formal_id]:
            raise QuickIntakeError(f"{field} formal_projection_sha256 过期")
        projection_meta = target_projections[formal_id].get("meta") or {}
        if frontmatter_scalar(meta, "subject") != str(projection_meta.get("subject") or ""):
            raise QuickIntakeError(f"{field} subject 与当前 wrongnet projection 不一致")
        for projected_field in ("knowledge", "methods", "error_causes"):
            raw_values = projection_meta.get(projected_field)
            if isinstance(raw_values, list):
                expected_values = {
                    str(item).strip() for item in raw_values if str(item).strip()
                }
            elif raw_values in (None, ""):
                expected_values = set()
            else:
                expected_values = {str(raw_values).strip()}
            if set(frontmatter_list(meta, projected_field)) != expected_values:
                raise QuickIntakeError(
                    f"{field} {projected_field} 与当前 wrongnet projection 不一致"
                )
        if formal_id not in set(frontmatter_list(meta, "wrongnet_refs")):
            raise QuickIntakeError(f"{field} wrongnet_refs 缺少目标 ID")
        expected_source = formal_by_id[formal_id]["card_path_after"]
        if expected_source not in set(frontmatter_list(meta, "source_refs")):
            raise QuickIntakeError(f"{field} source_refs 未指向当前正式卡")
        wiki_refs = [
            ref
            for ref in frontmatter_list(meta, "wiki_refs")
            if ref.startswith((
                "MATHWIKI-KNOWLEDGE-",
                "MATHWIKI-METHOD-CLUSTER-",
                "MATHWIKI-ERROR-CLUSTER-",
                "MATHWIKI-ACTION-GAP-",
            ))
        ]
        if not wiki_refs:
            raise QuickIntakeError(f"{field} 没有可验证簇引用")
        for ref in wiki_refs:
            cluster = clusters.get(ref)
            if cluster is None:
                raise QuickIntakeError(f"{field} 引用不存在的簇：{ref}")
            cluster_text = cluster.read_text(encoding="utf-8")
            if not re.search(rf"(?<![A-Z0-9-]){re.escape(formal_id)}(?![A-Z0-9-])", cluster_text):
                raise QuickIntakeError(f"{field} 目标未进入引用簇：{ref}")
            add_verified_artifact(verified, cluster)
        coverage = wiki_subject_coverage(formal_id)
        for index_path in (source_index, coverage, matrix):
            if row_count(index_path, formal_id) != 1:
                raise QuickIntakeError(f"{field} 在 Wiki 索引中不是唯一一行：{index_path.name}")
            add_verified_artifact(verified, index_path)
        add_verified_artifact(verified, summary)
        receipts[formal_id] = {
            "formal_id": formal_id,
            "summary_path": str(summary.relative_to(REPO_ROOT.resolve())),
            "summary_hash": summary_hash,
            "formal_projection_sha256": projection_hashes[formal_id],
            "cluster_refs": sorted(wiki_refs),
            "verification": "builtin_target_parity_and_lint_v1",
        }
    if set(receipts) != set(formal_by_id):
        raise QuickIntakeError("Wiki 回执必须与正式目标一一对应")
    return [receipts[key] for key in sorted(receipts)]


def verify_relationship_receipt(value: Any, formal_ids: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {"state", "proposals"}:
        raise QuickIntakeError("batch_receipts.relationships 字段不完整")
    state = value.get("state")
    if state not in {"passed", "proposal_only", "not_applicable"}:
        raise QuickIntakeError("relationships.state 无效")
    proposals = value.get("proposals")
    if not isinstance(proposals, list):
        raise QuickIntakeError("relationships.proposals 必须是数组")
    normalized = []
    seen: set[str] = set()
    for index, proposal in enumerate(proposals):
        field = f"relationships.proposals[{index}]"
        if not isinstance(proposal, dict) or set(proposal) != {"formal_id", "decision", "evidence"}:
            raise QuickIntakeError(f"{field} 字段不完整")
        formal_id = proposal.get("formal_id")
        if formal_id not in formal_ids or formal_id in seen:
            raise QuickIntakeError(f"{field}.formal_id 无效或重复")
        seen.add(formal_id)
        decision = proposal.get("decision")
        if decision not in {"保留提案", "删除提案", "需用户裁决", "无候选"}:
            raise QuickIntakeError(f"{field}.decision 无效")
        evidence = require_text(proposal.get("evidence"), f"{field}.evidence")
        normalized.append({"formal_id": formal_id, "decision": decision, "evidence": evidence})
    if state != "not_applicable" and seen != formal_ids:
        raise QuickIntakeError("relationship proposals 必须覆盖全部正式目标")
    if state == "not_applicable" and proposals:
        raise QuickIntakeError("not_applicable 不得携带 relationship proposals")
    return {"state": state, "proposals": sorted(normalized, key=lambda item: item["formal_id"])}


def verify_visual_receipts(
    value: Any,
    formal_by_id: dict[str, dict[str, Any]],
    verified: dict[str, str],
) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise QuickIntakeError("batch_receipts.visuals 必须是数组")
    receipts: dict[str, dict[str, Any]] = {}
    for index, item in enumerate(value):
        field = f"batch_receipts.visuals[{index}]"
        if not isinstance(item, dict) or set(item) != {"formal_id", "state", "artifacts"}:
            raise QuickIntakeError(f"{field} 字段不完整")
        formal_id = item.get("formal_id")
        if formal_id not in formal_by_id or formal_id in receipts:
            raise QuickIntakeError(f"{field}.formal_id 无效或重复")
        state = item.get("state")
        if state not in {"passed", "not_applicable"}:
            raise QuickIntakeError(f"{field}.state 无效")
        artifacts = normalize_hash_artifacts(item.get("artifacts"), f"{field}.artifacts")
        card_path, _ = formal_card_identity(formal_id)
        card_text = card_path.read_text(encoding="utf-8")
        refs = sorted(
            set(
                match.rstrip("' ")
                for match in re.findall(
                    r"错题知识网络/(?:可视化错题详情|assets/visual_wrong_questions)/"
                    r"[^\"'`\n]+?\.(?:md|png|jpe?g|webp|gif|svg)",
                    card_text,
                    flags=re.IGNORECASE,
                )
            )
        )
        if refs:
            if state != "passed" or {entry["path"] for entry in artifacts} != set(refs):
                raise QuickIntakeError(f"{field} 必须逐一验证正式卡中的视觉引用")
        elif state != "not_applicable" or artifacts:
            raise QuickIntakeError(f"{field} 无视觉引用时必须为 not_applicable")
        for artifact in artifacts:
            verified[artifact["path"]] = artifact["sha256"]
        receipts[formal_id] = {"formal_id": formal_id, "state": state, "artifacts": artifacts}
    if set(receipts) != set(formal_by_id):
        raise QuickIntakeError("visual 回执必须与正式目标一一对应")
    return [receipts[key] for key in sorted(receipts)]


def normalize_closeout(value: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    required = {
        "schema_version",
        "freeze_id",
        "model",
        "formal_results",
        "capture_results",
        "batch_receipts",
    }
    missing = sorted(required - set(value))
    unexpected = sorted(set(value) - required)
    if missing:
        raise QuickIntakeError(f"夜间回执缺少字段：{', '.join(missing)}")
    if unexpected:
        raise QuickIntakeError(f"夜间回执含未知字段：{', '.join(unexpected)}")
    if value.get("schema_version") != CLOSEOUT_SCHEMA:
        raise QuickIntakeError(f"schema_version 必须是 {CLOSEOUT_SCHEMA}")
    freeze_id = value.get("freeze_id")
    freeze = state["freezes"].get(freeze_id)
    if freeze is None:
        raise QuickIntakeError("freeze_id 未知")
    if freeze_id in state["aborted_freezes"]:
        raise QuickIntakeError("freeze 已 abort，不得 close")
    if freeze_id in state["used_freezes"]:
        raise QuickIntakeError("freeze 已被其他 closeout 使用")
    verify_freeze_current(freeze, state)
    model = require_text(value.get("model"), "model", max_length=200)
    if model != "unknown":
        raise QuickIntakeError("当前 closeout 没有受信模型注入；model 必须写 unknown")
    verified: dict[str, str] = {}
    formal_results, formal_by_id = normalize_formal_results(
        value.get("formal_results"), freeze, verified
    )
    capture_results = normalize_capture_results(
        value.get("capture_results"), freeze, state, formal_by_id, verified
    )
    batch = value.get("batch_receipts")
    if not isinstance(batch, dict) or set(batch) != {"wrongnet", "relationships", "wiki", "visuals"}:
        raise QuickIntakeError("batch_receipts 必须完整包含 wrongnet、relationships、wiki、visuals")
    wrongnet, target_projections, artifact_date = verify_wrongnet_receipt(
        batch.get("wrongnet"), formal_by_id, verified
    )
    if date.fromisoformat(artifact_date) < date.fromisoformat(freeze["study_date"]):
        raise QuickIntakeError("artifact_date 不得早于 study_date")
    if date.fromisoformat(artifact_date) > date.today():
        raise QuickIntakeError("artifact_date 不得晚于当前日期")
    relationships = verify_relationship_receipt(
        batch.get("relationships"), set(formal_by_id)
    )
    wiki = verify_wiki_receipts(
        batch.get("wiki"),
        formal_by_id,
        wrongnet["target_projection_hashes"],
        target_projections,
        artifact_date,
        verified,
    )
    visuals = verify_visual_receipts(batch.get("visuals"), formal_by_id, verified)
    return {
        "closeout_schema_version": CLOSEOUT_SCHEMA,
        "freeze_id": freeze_id,
        "study_date": freeze["study_date"],
        "artifact_date": artifact_date,
        "model": model,
        "formal_results": formal_results,
        "capture_results": capture_results,
        "batch_receipts": {
            "wrongnet": wrongnet,
            "relationships": relationships,
            "wiki": wiki,
            "visuals": visuals,
            "target_parity": "verified_builtin_fields_refs_membership_v2",
            "target_lint": "verified_builtin_required_fields_and_control_chars_v2",
        },
        "verified_artifacts": [
            {"path": path, "sha256": digest}
            for path, digest in sorted(verified.items())
        ],
    }


def reverify_artifacts(artifacts: list[dict[str, str]]) -> None:
    for item in artifacts:
        path = resolve_repo_artifact(item["path"], "verified_artifacts.path")
        if file_sha256(path) != item["sha256"]:
            raise QuickIntakeError(f"close 前文件发生并发变化：{item['path']}")


def make_prepare_invalidation(prepare: dict[str, Any], reason: str) -> dict[str, Any]:
    prepare_id = prepare["event_id"]
    return with_content_hash(
        {
            "schema_version": LEDGER_SCHEMA,
            "event_type": "closeout_invalidation",
            "event_id": stable_event_id("MFI-INVALID", {"prepare_id": prepare_id}),
            "prepare_id": prepare_id,
            "freeze_id": prepare["freeze_id"],
            "capture_event_ids": prepare["capture_event_ids"],
            "reason": require_text(reason, "invalidation.reason", max_length=1000),
            "invalidated_at": now_local(),
        }
    )


def cmd_close(args: argparse.Namespace) -> None:
    started = time.perf_counter()
    consumable_path = (
        validate_consumable_payload_path(args.receipt_file)
        if getattr(args, "consume_receipt_file", False)
        else None
    )
    raw = read_json_document(args.receipt_file)
    request_hash = sha256_value(raw)
    # 正式层锁必须由所有正式写者共享。锁内的 prepare 不改变
    # pending，只有最终复核后追加的 closeout commit 才会在 replay 中关闭 capture。
    with exclusive_lock(FORMAL_LOCK_PATH):
        with exclusive_lock(LOCK_PATH):
            events = load_jsonl(EVENTS_PATH)
            state = replay(events)
            existing = next(
                (
                    closeout
                    for closeout_id, closeout in state["closeouts"].items()
                    if closeout.get("request_hash") == request_hash
                    and closeout_id not in state["invalidated_closeouts"]
                ),
                None,
            )
            if existing is not None:
                event = existing
                event_id = event["event_id"]
                status = "noop"
            else:
                payload = normalize_closeout(raw, state)
                payload_hash = sha256_value(payload)
                freeze_id = payload["freeze_id"]
                capture_event_ids = [
                    item["capture_event_id"] for item in payload["capture_results"]
                ]
                active_prepares = [
                    prepare
                    for prepare_id, prepare in state["prepares"].items()
                    if prepare.get("freeze_id") == freeze_id
                    and prepare_id not in state["invalidated_prepares"]
                    and prepare_id not in state["committed_prepares"]
                ]
                prepare = next(
                    (
                        candidate
                        for candidate in active_prepares
                        if candidate.get("request_hash") == request_hash
                        and candidate.get("payload_hash") == payload_hash
                    ),
                    None,
                )

                if prepare is None:
                    # 中断后使用不同回执时，先明确失效同一 freeze 的未提交
                    # prepare，避免永久占用一个无法恢复的中间态。
                    for stale in active_prepares:
                        invalidation = make_prepare_invalidation(
                            stale,
                            "未提交 prepare 已被新的 closeout 回执替代",
                        )
                        append_jsonl(EVENTS_PATH, invalidation)
                        events.append(invalidation)
                    if active_prepares:
                        state = replay(events)

                    prior_generations = [
                        candidate.get("generation")
                        for candidate in state["prepares"].values()
                        if candidate.get("freeze_id") == freeze_id
                        and isinstance(candidate.get("generation"), int)
                        and not isinstance(candidate.get("generation"), bool)
                    ]
                    generation = max(prior_generations, default=-1) + 1
                    prepare_id = stable_event_id(
                        "MFI-PREP",
                        {
                            "freeze_id": freeze_id,
                            "payload_hash": payload_hash,
                            "generation": generation,
                        },
                    )
                    reverify_artifacts(payload["verified_artifacts"])
                    prepare = with_content_hash(
                        {
                            **payload,
                            "schema_version": LEDGER_SCHEMA,
                            "event_type": "closeout_prepare",
                            "event_id": prepare_id,
                            "request_hash": request_hash,
                            "payload_hash": payload_hash,
                            "generation": generation,
                            "freeze_id": freeze_id,
                            "capture_event_ids": capture_event_ids,
                            "prepared_at": now_local(),
                        }
                    )
                    replay([*events, prepare])
                    append_jsonl(EVENTS_PATH, prepare)
                    events.append(prepare)
                else:
                    prepare_id = prepare["event_id"]
                    generation = prepare["generation"]

                try:
                    reverify_artifacts(payload["verified_artifacts"])
                except QuickIntakeError as exc:
                    invalidation = make_prepare_invalidation(prepare, str(exc))
                    replay([*events, invalidation])
                    append_jsonl(EVENTS_PATH, invalidation)
                    raise QuickIntakeError(
                        "closeout prepare 后产物发生并发变化，已追加失效事件并恢复 pending 可重试状态"
                    ) from exc

                event_id = stable_event_id("MFI-CLOSE", {"prepare_id": prepare_id})
                event = with_content_hash(
                    {
                        **payload,
                        "schema_version": LEDGER_SCHEMA,
                        "event_type": "closeout",
                        "event_id": event_id,
                        "prepare_id": prepare_id,
                        "request_hash": request_hash,
                        "payload_hash": payload_hash,
                        "generation": generation,
                        "freeze_id": freeze_id,
                        "capture_event_ids": capture_event_ids,
                        "closed_at": now_local(),
                    }
                )
                committed_state = replay([*events, event])
                if any(
                    committed_state["closed_by"].get(capture_id) != event_id
                    for capture_id in capture_event_ids
                ):
                    raise QuickIntakeError("closeout commit 预回放未关闭全部 capture")
                append_jsonl(EVENTS_PATH, event)
                replay(load_jsonl(EVENTS_PATH))
                status = "recorded"
    response = {
        "status": status,
        "state": "nightly_closed",
        "closeout_id": event_id,
        "freeze_id": event["freeze_id"],
        "study_date": event["study_date"],
        "artifact_date": event.get("artifact_date"),
        "closed_event_ids": event["capture_event_ids"],
        "content_hash": event["content_hash"],
        "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
        "receipt_file_consumed": consume_payload_file(consumable_path),
    }
    print(json.dumps(response, ensure_ascii=False, sort_keys=True))


def cmd_verify(args: argparse.Namespace) -> None:
    study_date = validate_date(args.date) if args.date else None
    with exclusive_lock(LOCK_PATH):
        events = load_jsonl(EVENTS_PATH)
        snapshot_hash = ledger_hash()
    state = replay(events)
    source_bundle_count = 0
    source_artifact_count = 0
    for capture_id, capture in state["captures"].items():
        if study_date is not None and capture.get("study_date") != study_date:
            continue
        amendments = state["amendments"][capture_id]
        target = effective_target(capture, amendments)
        source_bundle = effective_source_bundle(capture, amendments)
        if source_bundle is None:
            continue
        document, _, children = validate_source_bundle_manifest(
            source_bundle["manifest_path"],
            f"verify.capture[{capture_id}].source_bundle",
            expected_hash=source_bundle["manifest_hash"],
            expected_date=capture["study_date"],
            expected_locator=(
                target.get("source_locator")
                if target.get("kind") == "new_source"
                else None
            ),
            require_question=target.get("kind") == "new_source",
        )
        source_bundle_count += 1
        source_artifact_count += len(document["artifacts"])
    document = status_document(events, study_date, snapshot_hash=snapshot_hash)
    print(
        json.dumps(
            {
                "status": "verified",
                "event_count": len(events),
                "study_date": study_date,
                "pending_count": document["pending_count"],
                "closed_count": document["closed_count"],
                "source_bundle_count": source_bundle_count,
                "source_artifact_count": source_artifact_count,
                "ledger_hash": document["ledger_hash"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="数学学习中的快速错题证据事件与夜间收口回执"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    stage_parser = subparsers.add_parser(
        "stage-source",
        help="把新的临时题图、解析图原子固化为仓库内可校验来源包",
    )
    stage_parser.add_argument("--payload-file", required=True, help="来源固化 JSON 文件")
    stage_parser.add_argument(
        "--consume-payload-file",
        action="store_true",
        help="成功或 noop 后删除系统临时目录中的来源固化 payload",
    )
    stage_parser.set_defaults(func=cmd_stage_source)

    record_parser = subparsers.add_parser(
        "record",
        help="只追加一条快速证据事件；不改正式卡、回滚单元、wrongnet 或 Wiki",
    )
    record_parser.add_argument("--payload-file", required=True, help="捕获 JSON 文件；- 表示从标准输入读取一行")
    record_parser.add_argument(
        "--consume-payload-file",
        action="store_true",
        help="成功或 noop 后删除系统临时目录中的 payload；验证失败时保留以便重试",
    )
    record_parser.set_defaults(func=cmd_record)

    amend_parser = subparsers.add_parser(
        "amend",
        help="追加修订事件，不覆盖原始 capture",
    )
    amend_parser.add_argument("--payload-file", required=True, help="修订 JSON 文件；- 表示从标准输入读取一行")
    amend_parser.set_defaults(func=cmd_amend)

    pending_parser = subparsers.add_parser(
        "pending",
        help="按事件回放列出尚未夜间收口的权威目标集",
    )
    pending_parser.add_argument("--date", help="只查看 YYYY-MM-DD 当日事件")
    pending_parser.set_defaults(func=cmd_pending)

    freeze_parser = subparsers.add_parser(
        "freeze",
        help="夜间正式写入前冻结 capture、amendment、来源身份与正式卡前哈希",
    )
    freeze_parser.add_argument("--payload-file", required=True, help="夜间冻结 JSON 文件")
    freeze_parser.add_argument(
        "--consume-payload-file",
        action="store_true",
        help="成功或 noop 后删除系统临时目录中的冻结 payload",
    )
    freeze_parser.set_defaults(func=cmd_freeze)

    abort_parser = subparsers.add_parser(
        "abort-freeze",
        help="仅在正式卡与回滚状态未变化时追加式放弃 active freeze",
    )
    abort_parser.add_argument("--freeze-id", required=True, help="待放弃的 MFI-FREEZE 事件 ID")
    abort_parser.add_argument("--reason", required=True, help="用户确认的放弃原因")
    abort_parser.set_defaults(func=cmd_abort_freeze)

    close_parser = subparsers.add_parser(
        "close",
        help="所有正式层通过后追加一次夜间 closeout 回执",
    )
    close_parser.add_argument("--receipt-file", required=True, help="夜间回执 JSON；- 表示从标准输入读取一行")
    close_parser.add_argument(
        "--consume-receipt-file",
        action="store_true",
        help="成功或 noop 后删除系统临时目录中的回执文件",
    )
    close_parser.set_defaults(func=cmd_close)

    verify_parser = subparsers.add_parser("verify", help="校验账本完整性与 pending 状态")
    verify_parser.add_argument("--date", help="只汇总 YYYY-MM-DD 当日事件")
    verify_parser.set_defaults(func=cmd_verify)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.func(args)
    except (QuickIntakeError, OSError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    main()
