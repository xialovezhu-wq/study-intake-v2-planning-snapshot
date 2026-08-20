#!/usr/bin/env python3
"""Read-only post-write lint for targeted kaoyan-math formal cards."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config.json"
DISALLOWED_CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
BARE_SPACING_COMMAND_RE = re.compile(r"(?<!\\)\b(?:qquad|quad)\b")
FORMAL_ID_RE = re.compile(r"(?:GS|LA|PR)-\d+")
CAPTURE_REF_RE = re.compile(
    r"^capture-v1:MFI-CAP-[a-f0-9]{24}:[a-f0-9]{64}:"
    r"(?:record_wrong|record_recurrence|update_representation|mastery_candidate)$"
)
SOURCE_REF_RE = re.compile(r"^source-v1:[a-f0-9]{64}$")
FREEZE_ID_RE = re.compile(r"^MFI-FREEZE-[a-f0-9]{24}$")
LEDGER_RELATIVE_PATH = Path("数学一回滚复习系统/快速入库事件.jsonl")
LEDGER_SCHEMA = "math-fast-intake-ledger-v1"
REQUIRED_SCALARS = (
    "id",
    "title",
    "subject",
    "source",
    "date",
    "status",
    "wrong_point",
    "answer",
)
REQUIRED_LISTS = (
    "fast_intake_refs",
    "wrong_history",
    "mastery_history",
    "knowledge",
    "error_causes",
    "methods",
    "traps",
)
REQUIRED_BODY_SECTIONS = ("题目摘要", "标准答案", "复做提醒")
REQUIRED_METHOD_SECTION_ALTERNATIVES = (
    "正确入口",
    "方法入口",
    "核心检查",
    "具体错点",
    "方法断点",
)


def _line_column(text: str, offset: int) -> tuple[int, int]:
    line = text.count("\n", 0, offset) + 1
    previous = text.rfind("\n", 0, offset)
    return line, offset - previous


def lint_text(text: str) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []

    def add(code: str, offset: int, message: str) -> None:
        line, column = _line_column(text, offset)
        issues.append(
            {"code": code, "line": line, "column": column, "message": message}
        )

    for match in DISALLOWED_CONTROL_RE.finditer(text):
        add("disallowed_control_character", match.start(), "存在不可打印控制字符")
    for match in re.finditer(r"\t", text):
        add("tab_character", match.start(), "正式卡不得包含制表符；它可能来自损坏的 LaTeX 命令")
    for match in BARE_SPACING_COMMAND_RE.finditer(text):
        add(
            "bare_latex_spacing_command",
            match.start(),
            f"LaTeX 间距命令 {match.group(0)} 缺少反斜杠",
        )
    if text.count("$$") % 2:
        add("unbalanced_display_math", text.rfind("$$"), "独立公式的双美元定界符未闭合")
    for opening, closing, code in (
        (r"\(", r"\)", "unbalanced_inline_math"),
        (r"\[", r"\]", "unbalanced_bracket_math"),
    ):
        if text.count(opening) != text.count(closing):
            offset = max(text.rfind(opening), text.rfind(closing), 0)
            add(code, offset, "LaTeX 数学定界符数量不匹配")
    return issues


def _frontmatter(text: str) -> tuple[str, str] | None:
    if not text.startswith("---\n"):
        return None
    end = text.find("\n---\n", 4)
    if end < 0:
        return None
    return text[4:end], text[end + 5 :]


def _frontmatter_scalar(meta: str, key: str) -> str:
    match = re.search(rf"(?m)^{re.escape(key)}:\s*['\"]?([^'\"\n]+)", meta)
    return match.group(1).strip() if match else ""


def _frontmatter_list(meta: str, key: str) -> list[str]:
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


def _structural_issue(text: str, code: str, needle: str, message: str) -> dict[str, Any]:
    offset = text.find(needle)
    line, column = _line_column(text, max(offset, 0))
    return {"code": code, "line": line, "column": column, "message": message}


def _source_ref_token(source_locator: str, resolved_source_hash: str) -> str:
    payload = json.dumps(
        {
            "source_locator": source_locator,
            "resolved_source_hash": resolved_source_hash,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "source-v1:" + hashlib.sha256(payload).hexdigest()


def _quick_sha256(value: object) -> str:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _canonical_quick_intake(repo: Path) -> Any:
    path = repo / "数学一回滚复习系统/scripts/quick_intake.py"
    if not path.is_file():
        raise ValueError("canonical_quick_intake_missing")
    name = "math_formal_lint_quick_intake_" + hashlib.sha256(
        str(path.resolve()).encode("utf-8")
    ).hexdigest()[:16]
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("canonical_quick_intake_invalid")
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except (OSError, ImportError, SyntaxError) as exc:
        raise ValueError("canonical_quick_intake_invalid") from exc
    module.REPO_ROOT = repo.resolve()
    module.EVENTS_PATH = repo / LEDGER_RELATIVE_PATH
    module.CARDS_DIR = repo / "错题知识网络/错题卡"
    module.SOURCE_STAGING_ROOT = repo / "数学一回滚复习系统/快速入库来源"
    return module


def _freeze_bindings(repo: Path, freeze_id: str) -> dict[str, dict[str, set[str]]]:
    if not FREEZE_ID_RE.fullmatch(freeze_id):
        raise ValueError("freeze_id_invalid")
    ledger_path = repo / LEDGER_RELATIVE_PATH
    canonical = _canonical_quick_intake(repo)
    try:
        rows = canonical.load_jsonl(ledger_path)
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("ledger_invalid") from exc
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict) or row.get("schema_version") != LEDGER_SCHEMA:
            raise ValueError("ledger_invalid")
        event_id = row.get("event_id")
        content_hash = row.get("content_hash")
        if not isinstance(event_id, str) or event_id in seen:
            raise ValueError("ledger_duplicate_event_id")
        seen.add(event_id)
        unhashed = {key: value for key, value in row.items() if key != "content_hash"}
        if (
            not isinstance(content_hash, str)
            or not re.fullmatch(r"[a-f0-9]{64}", content_hash)
            or _quick_sha256(unhashed) != content_hash
        ):
            raise ValueError("ledger_content_hash_invalid")
    matches = [row for row in rows if row.get("event_id") == freeze_id]
    if len(matches) != 1 or matches[0].get("event_type") != "freeze":
        raise ValueError("freeze_not_unique")
    freeze = matches[0]
    if freeze.get("freeze_schema_version") != canonical.FREEZE_SCHEMA:
        raise ValueError("freeze_schema_invalid")
    freeze_index = rows.index(freeze)
    if freeze.get("ledger_prefix") != canonical.event_prefix(rows[:freeze_index]):
        raise ValueError("freeze_ledger_prefix_invalid")
    try:
        state = canonical.replay(rows)
        if (
            freeze_id in state.get("aborted_freezes", set())
            or freeze_id in state.get("used_freezes", {})
        ):
            raise ValueError("freeze_not_active")
        canonical.verify_freeze_current(freeze, state)
    except ValueError as exc:
        raise ValueError("freeze_not_current") from exc
    snapshots = {
        row.get("capture_event_id"): row
        for row in freeze.get("capture_snapshots", [])
        if isinstance(row, dict) and isinstance(row.get("capture_event_id"), str)
    }
    capture_ids = freeze.get("capture_event_ids")
    if (
        not isinstance(capture_ids, list)
        or len(capture_ids) != len(set(capture_ids))
        or set(snapshots) != set(capture_ids)
    ):
        raise ValueError("freeze_snapshot_set_invalid")
    bindings: dict[str, dict[str, set[str]]] = {}
    assigned: set[str] = set()
    for target in freeze.get("targets", []):
        if not isinstance(target, dict):
            raise ValueError("freeze_target_invalid")
        formal_id = target.get("formal_id")
        grouped = target.get("capture_event_ids")
        if (
            not isinstance(formal_id, str)
            or not isinstance(grouped, list)
            or not grouped
            or any(capture_id not in snapshots or capture_id in assigned for capture_id in grouped)
        ):
            raise ValueError("freeze_target_binding_invalid")
        assigned.update(grouped)
        capture_refs = set(target.get("fast_intake_refs_before") or [])
        capture_refs.update(canonical.capture_ref_token(snapshots[capture_id]) for capture_id in grouped)
        source_refs = set(target.get("fast_intake_source_refs_before") or [])
        source_bindings = (
            ([target["source_binding"]] if isinstance(target.get("source_binding"), dict) else [])
            + [
                row
                for row in target.get("supplemental_source_bundles", [])
                if isinstance(row, dict)
            ]
        )
        for binding in source_bindings:
            locator = binding.get("source_locator")
            resolved_hash = binding.get("resolved_source_hash")
            if not isinstance(locator, str) or not isinstance(resolved_hash, str):
                raise ValueError("freeze_source_binding_invalid")
            expected = canonical.source_ref_token(binding)
            if expected != _source_ref_token(locator, resolved_hash):
                raise ValueError("canonical_source_token_mismatch")
            source_refs.add(expected)
        if formal_id in bindings:
            raise ValueError("freeze_formal_id_duplicate")
        bindings[formal_id] = {
            "fast_intake_refs": capture_refs,
            "fast_intake_source_refs": source_refs,
        }
    if assigned != set(capture_ids):
        raise ValueError("freeze_target_set_mismatch")
    return bindings


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def lint_card_structure(
    text: str,
    path: Path,
    repo: Path,
    expected_binding: dict[str, set[str]] | None = None,
) -> list[dict[str, Any]]:
    repo = repo.resolve()
    issues: list[dict[str, Any]] = []
    parts = _frontmatter(text)
    if parts is None:
        return [
            _structural_issue(
                text,
                "frontmatter_missing_or_unclosed",
                "---",
                "正式卡必须包含闭合的 YAML frontmatter",
            )
        ]
    meta, body = parts
    formal_id = _frontmatter_scalar(meta, "id")
    filename_id = path.name.split("_", 1)[0]
    if not FORMAL_ID_RE.fullmatch(formal_id):
        issues.append(
            _structural_issue(text, "formal_id_invalid", "id:", "frontmatter id 无效")
        )
    elif filename_id != formal_id:
        issues.append(
            _structural_issue(
                text,
                "formal_id_filename_mismatch",
                "id:",
                "frontmatter id 与文件名中的正式 ID 不一致",
            )
        )
    for key in REQUIRED_SCALARS:
        if not _frontmatter_scalar(meta, key):
            issues.append(
                _structural_issue(
                    text,
                    "required_frontmatter_field_missing",
                    f"{key}:",
                    f"正式卡缺少非空字段 {key}",
                )
            )
    lists = {key: _frontmatter_list(meta, key) for key in REQUIRED_LISTS}
    for key, values in lists.items():
        if not values:
            issues.append(
                _structural_issue(
                    text,
                    "required_frontmatter_list_missing",
                    f"{key}:",
                    f"正式卡缺少非空列表 {key}",
                )
            )
    capture_ref_values = lists.get("fast_intake_refs", [])
    capture_refs = set(capture_ref_values)
    if len(capture_refs) != len(capture_ref_values):
        issues.append(
            _structural_issue(
                text,
                "capture_ref_duplicate",
                "fast_intake_refs:",
                "fast_intake_refs 不得包含重复条目",
            )
        )
    for value in capture_refs:
        if not CAPTURE_REF_RE.fullmatch(value):
            issues.append(
                _structural_issue(
                    text,
                    "capture_ref_invalid",
                    value,
                    "fast_intake_refs 条目格式无效",
                )
            )
    source_ref_values = _frontmatter_list(meta, "fast_intake_source_refs")
    source_refs = set(source_ref_values)
    if len(source_refs) != len(source_ref_values):
        issues.append(
            _structural_issue(
                text,
                "source_ref_duplicate",
                "fast_intake_source_refs:",
                "fast_intake_source_refs 不得包含重复条目",
            )
        )
    for value in source_refs:
        if not SOURCE_REF_RE.fullmatch(value):
            issues.append(
                _structural_issue(
                    text,
                    "source_ref_invalid",
                    value,
                    "fast_intake_source_refs 条目格式无效",
                )
            )
    if expected_binding is not None:
        if capture_refs != expected_binding["fast_intake_refs"]:
            issues.append(
                _structural_issue(
                    text,
                    "capture_binding_freeze_mismatch",
                    "fast_intake_refs:",
                    "fast_intake_refs 与指定 freeze 的规范 token 集合不一致",
                )
            )
        if source_refs != expected_binding["fast_intake_source_refs"]:
            issues.append(
                _structural_issue(
                    text,
                    "source_binding_freeze_mismatch",
                    "fast_intake_source_refs:",
                    "fast_intake_source_refs 与指定 freeze 的规范 token 集合不一致",
                )
            )
    for section in REQUIRED_BODY_SECTIONS:
        if not re.search(rf"(?m)^##\s+{re.escape(section)}\s*$", body):
            issues.append(
                _structural_issue(
                    text,
                    "required_body_section_missing",
                    section,
                    f"正式卡正文缺少章节 {section}",
                )
            )
    if not any(
        re.search(rf"(?m)^##\s+{re.escape(section)}\s*$", body)
        for section in REQUIRED_METHOD_SECTION_ALTERNATIVES
    ):
        issues.append(
            _structural_issue(
                text,
                "required_method_section_missing",
                "##",
                "正式卡正文缺少方法、错点或正确入口章节",
            )
        )

    lecture_refs = _frontmatter_list(meta, "lecture_refs")
    manifests = [value for value in lecture_refs if value.endswith("manifest.json")]
    expected_source_refs: set[str] = set()
    for relative in manifests:
        candidate = (repo / relative).resolve()
        try:
            candidate.relative_to(repo)
        except ValueError:
            issues.append(
                _structural_issue(
                    text,
                    "source_manifest_path_escape",
                    relative,
                    "来源 manifest 路径越出数学仓库",
                )
            )
            continue
        try:
            manifest = json.loads(candidate.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            issues.append(
                _structural_issue(
                    text,
                    "source_manifest_invalid",
                    relative,
                    "来源 manifest 缺失或不是有效 JSON",
                )
            )
            continue
        locator = manifest.get("source_locator") if isinstance(manifest, dict) else None
        artifacts = manifest.get("artifacts") if isinstance(manifest, dict) else None
        if not isinstance(locator, str) or not locator.strip() or not isinstance(artifacts, list):
            issues.append(
                _structural_issue(
                    text,
                    "source_manifest_shape_invalid",
                    relative,
                    "来源 manifest 缺少 source_locator 或 artifacts",
                )
            )
            continue
        manifest_hash = _sha256_file(candidate)
        expected_source_refs.add(_source_ref_token(locator, manifest_hash))
        for artifact in artifacts:
            if not isinstance(artifact, dict):
                issues.append(
                    _structural_issue(
                        text,
                        "source_artifact_shape_invalid",
                        relative,
                        "来源 artifact 不是对象",
                    )
                )
                continue
            artifact_relative = artifact.get("path")
            expected_hash = artifact.get("sha256")
            if not isinstance(artifact_relative, str) or not isinstance(expected_hash, str):
                issues.append(
                    _structural_issue(
                        text,
                        "source_artifact_binding_missing",
                        relative,
                        "来源 artifact 缺少路径或 SHA-256",
                    )
                )
                continue
            artifact_path = (repo / artifact_relative).resolve()
            try:
                artifact_path.relative_to(repo)
                actual_hash = _sha256_file(artifact_path)
            except (ValueError, OSError):
                actual_hash = None
            if actual_hash != expected_hash:
                issues.append(
                    _structural_issue(
                        text,
                        "source_artifact_hash_mismatch",
                        artifact_relative,
                        "来源 artifact 缺失或哈希与 manifest 不一致",
                    )
                )
    if source_refs and not manifests:
        issues.append(
            _structural_issue(
                text,
                "source_manifest_reference_missing",
                "fast_intake_source_refs:",
                "存在结构化来源引用，但 lecture_refs 未列出来源 manifest",
            )
        )
    missing_bindings = sorted(expected_source_refs - source_refs)
    stale_bindings = sorted(source_refs - expected_source_refs)
    for value in missing_bindings:
        issues.append(
            _structural_issue(
                text,
                "source_binding_missing",
                "fast_intake_source_refs:",
                f"来源 manifest 未绑定到正式卡：{value}",
            )
        )
    for value in stale_bindings:
        issues.append(
            _structural_issue(
                text,
                "source_binding_unresolved",
                value,
                "正式卡来源引用无法由列出的 manifest 独立重算",
            )
        )
    return issues


def _load_config(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("config_invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("config_invalid")
    return value


def _resolve_card_root(config: dict[str, Any]) -> tuple[Path, Path]:
    try:
        repo = Path(config["adapters"]["math"]["repo_root"]).expanduser().resolve()
    except (KeyError, TypeError) as exc:
        raise ValueError("math_repo_root_missing") from exc
    card_root = (repo / "错题知识网络" / "错题卡").resolve()
    if not card_root.is_dir():
        raise ValueError("math_card_root_missing")
    return repo, card_root


def _resolve_cards(
    repo: Path,
    card_root: Path,
    raw_cards: Iterable[str],
    formal_ids: Iterable[str],
) -> list[Path]:
    result: list[Path] = []
    for raw in raw_cards:
        candidate = Path(raw)
        if not candidate.is_absolute():
            candidate = repo / candidate
        path = candidate.resolve(strict=True)
        try:
            path.relative_to(card_root)
        except ValueError as exc:
            raise ValueError("card_outside_formal_root") from exc
        if path.suffix.lower() != ".md":
            raise ValueError("card_not_markdown")
        result.append(path)
    for formal_id in formal_ids:
        if not re.fullmatch(r"(?:GS|LA|PR)-\d+", formal_id):
            raise ValueError("formal_id_invalid")
        matches = sorted(card_root.glob(f"{formal_id}_*.md"))
        if len(matches) != 1:
            raise ValueError("formal_id_not_unique")
        result.append(matches[0].resolve())
    unique = sorted(set(result), key=lambda path: str(path))
    if not unique:
        raise ValueError("no_cards_selected")
    return unique


def build_report(
    repo: Path,
    cards: Iterable[Path],
    *,
    freeze_id: str | None = None,
) -> dict[str, Any]:
    bindings = _freeze_bindings(repo, freeze_id) if freeze_id is not None else None
    rows: list[dict[str, Any]] = []
    issue_count = 0
    for path in cards:
        try:
            text = path.read_text(encoding="utf-8")
            expected_binding: dict[str, set[str]] | None = None
            issues = lint_text(text)
            if bindings is not None:
                parts = _frontmatter(text)
                formal_id = _frontmatter_scalar(parts[0], "id") if parts else ""
                expected_binding = bindings.get(formal_id)
                if expected_binding is None:
                    issues.append(
                        _structural_issue(
                            text,
                            "card_not_in_freeze",
                            "id:",
                            "所选正式卡不属于指定 freeze",
                        )
                    )
            issues += lint_card_structure(
                text,
                path,
                repo,
                expected_binding=expected_binding,
            )
        except UnicodeError:
            issues = [
                {
                    "code": "invalid_utf8",
                    "line": 1,
                    "column": 1,
                    "message": "正式卡不是有效 UTF-8",
                }
            ]
        issue_count += len(issues)
        rows.append(
            {
                "path": str(path.relative_to(repo)),
                "status": "passed" if not issues else "failed",
                "issues": issues,
            }
        )
    return {
        "schema_version": "math-formal-card-lint-v1",
        "status": "passed" if issue_count == 0 else "failed",
        "freeze_id": freeze_id,
        "card_count": len(rows),
        "issue_count": issue_count,
        "cards": rows,
        "formal_write_count": 0,
    }


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description="只读检查数学正式卡的公式与控制字符")
    value.add_argument("--config", default=str(DEFAULT_CONFIG))
    value.add_argument("--card", action="append", default=[])
    value.add_argument("--formal-id", action="append", default=[])
    value.add_argument("--freeze-id")
    return value


def main() -> int:
    os.umask(0o077)
    args = parser().parse_args()
    try:
        config = _load_config(Path(args.config).expanduser().resolve())
        repo, card_root = _resolve_card_root(config)
        cards = _resolve_cards(repo, card_root, args.card, args.formal_id)
        report = build_report(repo, cards, freeze_id=args.freeze_id)
    except (OSError, ValueError) as exc:
        report = {
            "schema_version": "math-formal-card-lint-v1",
            "status": "error",
            "error_code": str(exc),
            "formal_write_count": 0,
        }
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
        return 2
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
