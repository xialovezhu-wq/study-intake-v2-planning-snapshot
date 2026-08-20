from __future__ import annotations

import hashlib
import re
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

from ..envelope import success_envelope
from ..errors import StudyReadError
from ..models import CS408Query, PagedReadRequest
from ..safeio import SafeReader
from .base import AuthoritySnapshot, BaseAdapter


NODE_ID = re.compile(r"^(?:DS|CO|OS|CN)\d{2}-\d{2}-[\w.-]{1,100}$", re.UNICODE)
FORMAL_NODE_ID = re.compile(r"^(?:DS|CO|OS|CN)_(?:\d{4}|UNK)_\d{3}$")
SUBJECT_CODE = re.compile(r"^(DS|CO|OS|CN)$")
MODULE_CODE = re.compile(r"^(DS|CO|OS|CN)\d{2}$")
KNOWLEDGE_CODE = re.compile(r"^(DS|CO|OS|CN)\d{2}-\d{2}$")
RELATION_CODE = re.compile(r"^R0[1-9]$")
WIKILINK = re.compile(r"\[\[([^\]|#]+)(?:#[^\]|]+)?(?:\|[^\]]+)?\]\]")

MASTER_COLUMNS = (
    "ID", "来源ID", "年份", "科目", "主模块", "主知识点", "副知识点",
    "命中知识点", "题型", "核心考点", "模糊概念", "错因标签",
    "首次做题日期", "最近复做日期", "最近错误记录", "详情入口",
)
RELATION_COLUMNS = (
    "起点ID", "终点ID", "关系类型", "联系强度", "关联原因", "复盘优先级",
)
RELATION_STRENGTHS = {"强", "中", "弱"}
RELATION_PRIORITIES = {"高", "中", "低"}
KNOWLEDGE_PLACEHOLDERS = {"待补充", "暂无明确副知识点"}


def _split_table_row(line: str) -> list[str]:
    value = line.strip()
    if not (value.startswith("|") and value.endswith("|")):
        return []
    return [cell.strip() for cell in value[1:-1].split("|")]


def _is_separator(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-{2,}:?", cell) for cell in cells)


def _parse_table(text: str, columns: tuple[str, ...]) -> list[tuple[int, dict[str, str]]]:
    lines = text.splitlines()
    start: int | None = None
    for index, line in enumerate(lines):
        if tuple(_split_table_row(line)) == columns:
            if index + 1 >= len(lines) or not _is_separator(_split_table_row(lines[index + 1])):
                raise StudyReadError("SUBJECT_UNAVAILABLE", "408 formal table separator is invalid")
            start = index + 2
            break
    if start is None:
        raise StudyReadError("SUBJECT_UNAVAILABLE", "408 formal table header was not found")
    output: list[tuple[int, dict[str, str]]] = []
    for index in range(start, len(lines)):
        cells = _split_table_row(lines[index])
        if not cells:
            break
        if len(cells) != len(columns):
            raise StudyReadError("SUBJECT_UNAVAILABLE", "408 formal table row width is invalid")
        output.append((index + 1, dict(zip(columns, cells))))
    return output


def _split_code_name(value: str) -> tuple[str, str]:
    parts = value.strip().split(maxsplit=1)
    return parts[0], parts[1].strip() if len(parts) == 2 else ""


def _split_multi(value: str) -> list[str]:
    return [part.strip() for part in re.split(r"[；;]", value or "") if part.strip()]


def _node_summary(text: str) -> dict[str, Any]:
    title = None
    keywords: list[str] = []
    area = None
    for line in text.splitlines():
        if title is None and line.startswith("# "):
            title = line[2:].strip()
        if line.startswith("keywords:"):
            keywords = [x.strip() for x in line.split(":", 1)[1].split(",") if x.strip()]
        if "| 所属科目 |" in line:
            cells = [x.strip() for x in line.strip("|").split("|")]
            if len(cells) >= 2:
                area = cells[1]
    return {"title": title, "area": area, "keywords": keywords}


class CS408Adapter(BaseAdapter):
    subject = "cs408"
    authority_source_id = "cs408.formal-events-manifest-projection"
    capabilities = frozenset({"curation_inventory", "knowledge_nodes", "direct_edges", "morning_state", "review_identity"})

    def __init__(self, root: Path, *, reader: SafeReader | None = None) -> None:
        super().__init__(reader or SafeReader({"cs408": root}))
        self.root = self.reader.roots["cs408"]
        self.formal_nodes_table = self.reader.exact("cs408", "节点总表.md")
        self.knowledge_labels = self.reader.exact("cs408", "知识点标签表.md")
        self.formal_relations_table = self.reader.exact("cs408", "关系边表.md")
        self.relation_rules = self.reader.exact("cs408", "关系规则.md")
        self.vault = self.reader.exact("cs408", "wiki", "study_vaults", "408-full")
        self.vault_manifest = self.reader.exact("cs408", "wiki", "study_vaults", "408-full", "manifest.json")
        self.study_vault = self.reader.exact("cs408", "wiki", "study_vaults", "408-full", "StudyVault")
        self.curation_state = self.reader.exact("cs408", "wiki", "study_vaults", "408-full", "state", "intake-curation", "state.json")
        self.curation_events = self.reader.exact("cs408", "wiki", "study_vaults", "408-full", "state", "intake-curation", "events.jsonl")
        self.hot_manifest = self.reader.exact("cs408", "wiki", "study_vaults", "408-full", "state", "review-loop", "hot-state", "manifest.json")
        self.review_events = self.reader.exact(
            "cs408", "wiki", "study_vaults", "408-full", "state", "review-loop",
            "events.jsonl",
        )
        self.morning_dir = self.reader.exact("cs408", "wiki", "study_vaults", "408-full", "state", "morning-review")
        self._node_index: dict[str, Path] = {}
        self._vault_manifest_identity = None
        self._morning_dir_identity = None
        self._morning_index: dict[str, Path] = {}
        self._formal_identity: tuple[Any, ...] | None = None
        self._formal_nodes: dict[str, dict[str, Any]] = {}
        self._formal_knowledge: dict[str, dict[str, Any]] = {}
        self._formal_relation_rows: list[dict[str, Any]] = []
        self._sqlite: sqlite3.Connection | None = None
        self._sqlite_manifest_sha: str | None = None
        self.sqlite_opens = 0
        self.directory_scans = 0
        self._refresh_formal_sources()
        self._refresh_node_index()
        self._refresh_morning_index()

    def close(self) -> None:
        if self._sqlite is not None:
            self._sqlite.close()
            self._sqlite = None
            self._sqlite_manifest_sha = None

    def _refresh_formal_sources(self) -> None:
        identity = tuple(
            self.reader.identity(path)
            for path in (
                self.formal_nodes_table, self.knowledge_labels,
                self.formal_relations_table, self.relation_rules,
            )
        )
        if identity == self._formal_identity:
            return

        master_text, master_hash, _ = self.reader.text(self.formal_nodes_table)
        node_index: dict[str, dict[str, Any]] = {}
        for line_number, fields in _parse_table(master_text, MASTER_COLUMNS):
            stable_id = fields["ID"]
            if not FORMAL_NODE_ID.fullmatch(stable_id):
                raise StudyReadError("SUBJECT_UNAVAILABLE", "408 formal node id is invalid")
            if stable_id in node_index:
                raise StudyReadError("SUBJECT_UNAVAILABLE", "408 formal node id is duplicated")
            node_index[stable_id] = {
                "stable_id": stable_id,
                "fields": fields,
                "source_line": line_number,
                "source_hash": master_hash,
                "data_role": "formal_wrong_item",
                "content_complete": True,
            }

        knowledge_text, knowledge_hash, _ = self.reader.text(self.knowledge_labels)
        knowledge: dict[str, dict[str, Any]] = {}
        current_subject: str | None = None
        current_module: str | None = None
        for line_number, line in enumerate(knowledge_text.splitlines(), start=1):
            subject_match = re.fullmatch(r"#\s+.+?\s+(DS|CO|OS|CN)\s*", line)
            if subject_match:
                current_subject = subject_match.group(1)
                current_module = None
                if current_subject in knowledge:
                    raise StudyReadError("SUBJECT_UNAVAILABLE", "408 knowledge subject is duplicated")
                knowledge[current_subject] = {
                    "stable_id": current_subject,
                    "knowledge_kind": "subject",
                    "name": line.rsplit(" ", 1)[0].lstrip("# ").strip(),
                    "source_line": line_number,
                    "source_hash": knowledge_hash,
                    "data_role": "formal_knowledge",
                    "content_complete": True,
                }
                continue
            module_match = re.fullmatch(r"##\s+((?:DS|CO|OS|CN)\d{2})\s+(.+?)\s*", line)
            if module_match and current_subject is not None:
                code, name = module_match.groups()
                if not code.startswith(current_subject) or code in knowledge:
                    raise StudyReadError("SUBJECT_UNAVAILABLE", "408 knowledge module is invalid")
                current_module = code
                knowledge[code] = {
                    "stable_id": code,
                    "knowledge_kind": "module",
                    "subject": current_subject,
                    "name": name.strip(),
                    "source_line": line_number,
                    "source_hash": knowledge_hash,
                    "data_role": "formal_knowledge",
                    "content_complete": True,
                }
                continue
            point_match = re.fullmatch(
                r"\s*-\s+((?:DS|CO|OS|CN)\d{2}-\d{2})\s+(.+?)\s*", line
            )
            if point_match and current_subject is not None and current_module is not None:
                code, name = point_match.groups()
                if not code.startswith(current_module + "-") or code in knowledge:
                    raise StudyReadError("SUBJECT_UNAVAILABLE", "408 knowledge point is invalid")
                knowledge[code] = {
                    "stable_id": code,
                    "knowledge_kind": "point",
                    "subject": current_subject,
                    "module": current_module,
                    "name": name.strip(),
                    "source_line": line_number,
                    "source_hash": knowledge_hash,
                    "data_role": "formal_knowledge",
                    "content_complete": True,
                }

        rules_text, _, _ = self.reader.text(self.relation_rules)
        rules: dict[str, str] = {}
        for line in rules_text.splitlines():
            matched = re.fullmatch(r"##\s+(R0[1-9])\s+(.+?)\s*", line)
            if matched:
                rules[matched.group(1)] = matched.group(2).strip()
        if set(rules) != {f"R0{value}" for value in range(1, 10)}:
            raise StudyReadError("SUBJECT_UNAVAILABLE", "408 relation rule registry is incomplete")

        relation_text, relation_hash, _ = self.reader.text(self.formal_relations_table)
        relation_rows: list[dict[str, Any]] = []
        seen_relations: set[str] = set()
        for line_number, fields in _parse_table(relation_text, RELATION_COLUMNS):
            source, target = fields["起点ID"], fields["终点ID"]
            code, declared_name = _split_code_name(fields["关系类型"])
            if source not in node_index or target not in node_index or source == target:
                raise StudyReadError("SUBJECT_UNAVAILABLE", "408 relation endpoint is invalid")
            if not RELATION_CODE.fullmatch(code) or code not in rules:
                raise StudyReadError("SUBJECT_UNAVAILABLE", "408 relation type is invalid")
            if fields["联系强度"] not in RELATION_STRENGTHS:
                raise StudyReadError("SUBJECT_UNAVAILABLE", "408 relation strength is invalid")
            if fields["复盘优先级"] not in RELATION_PRIORITIES or not fields["关联原因"]:
                raise StudyReadError("SUBJECT_UNAVAILABLE", "408 relation metadata is invalid")
            digest = hashlib.sha256(
                f"{source}\0{target}\0{code}".encode("utf-8")
            ).hexdigest()
            stable_id = f"408REL-{digest}"
            if stable_id in seen_relations:
                raise StudyReadError("SUBJECT_UNAVAILABLE", "408 formal relation is duplicated")
            seen_relations.add(stable_id)
            relation_rows.append({
                "stable_id": stable_id,
                "source": source,
                "target": target,
                "relation_code": code,
                "relation_name": declared_name,
                "canonical_relation_name": rules[code],
                "relation_label_matches_rule": declared_name == rules[code],
                "strength": fields["联系强度"],
                "reason": fields["关联原因"],
                "review_priority": fields["复盘优先级"],
                "source_line": line_number,
                "source_hash": relation_hash,
                "data_role": "formal_wrong_item_relation",
                "content_complete": True,
            })

        self._formal_nodes = node_index
        self._formal_knowledge = knowledge
        self._formal_relation_rows = relation_rows
        self._formal_identity = identity
        self._vault_manifest_identity = None

    def _refresh_node_index(self) -> None:
        self._refresh_formal_sources()
        manifest_identity = self.reader.identity(self.vault_manifest)
        if manifest_identity == self._vault_manifest_identity:
            return
        index: dict[str, Path] = {}
        for path in self.study_vault.rglob("*.md"):
            if path.is_symlink() or not path.is_file():
                continue
            node_id = path.stem
            if not NODE_ID.fullmatch(node_id):
                continue
            code = node_id.split("-", 2)[0] + "-" + node_id.split("-", 2)[1]
            if code not in self._formal_knowledge:
                continue
            text, _, _ = self.reader.text(path)
            heading_code = None
            for line in text.splitlines():
                matched = re.fullmatch(r"#\s+((?:DS|CO|OS|CN)\d{2}-\d{2})(?:\s+.*)?", line)
                if matched:
                    heading_code = matched.group(1)
                    break
            if heading_code != code:
                continue
            index[node_id] = self.reader.validate_indexed_path("cs408", path)
        self._node_index = index
        self._vault_manifest_identity = manifest_identity
        self.directory_scans += 1

    def _refresh_morning_index(self) -> None:
        identity = self.reader.identity(self.morning_dir)
        if identity == self._morning_dir_identity:
            return
        index: dict[str, Path] = {}
        for child in self.morning_dir.iterdir():
            if child.is_symlink() or not child.is_dir() or not child.name.startswith("MR-"):
                continue
            state_path = child / "state.json"
            if state_path.is_file() and not state_path.is_symlink():
                index[child.name] = self.reader.validate_indexed_path("cs408", state_path)
        self._morning_index = index
        self._morning_dir_identity = identity
        self.directory_scans += 1

    def authority(self) -> AuthoritySnapshot:
        self.assert_available()
        with self._lock:
            self._refresh_formal_sources()
            self._refresh_node_index()
            bindings: list[tuple[str, str]] = []
            sources: list[dict[str, Any]] = []
            for source_id, role, path in (
                ("cs408.formal-wrong-items", "formal", self.formal_nodes_table),
                ("cs408.formal-knowledge", "formal", self.knowledge_labels),
                ("cs408.formal-relations", "formal", self.formal_relations_table),
                ("cs408.relation-rules", "formal", self.relation_rules),
                ("cs408.vault-manifest", "projection", self.vault_manifest),
                ("cs408.curation-state", "projection", self.curation_state),
                ("cs408.curation-events", "event", self.curation_events),
                ("cs408.hot-state-manifest", "projection", self.hot_manifest),
                ("cs408.review-events", "event", self.review_events),
            ):
                digest, _ = self.reader.digest(path)
                bindings.append((source_id, digest))
                sources.append({"capability": source_id, "data_role": role, "available": True, "sha256": digest})
            node_digest = self.reader.set_fingerprint(
                (node_id, self.reader.digest(path)[0])
                for node_id, path in self._node_index.items()
            )
            bindings.append(("cs408.study-vault-safe-projection", node_digest))
            sources.append({
                "capability": "cs408.study-vault-safe-projection",
                "data_role": "projection", "available": True, "sha256": node_digest,
            })
            fingerprint = self.digest_bindings(bindings)
            return AuthoritySnapshot(
                self.subject, self.generation(self.subject, fingerprint),
                fingerprint, tuple(sources),
            )

    @staticmethod
    def _validate_node_id(value: str) -> str:
        SafeReader.validate_stable_id(value, "408 node id")
        if not NODE_ID.fullmatch(value):
            raise StudyReadError("INVALID_ARGUMENT", "invalid 408 node id")
        return value

    def _knowledge_node(self, node_id: str) -> tuple[dict[str, Any], Path]:
        self._refresh_node_index()
        node_id = self._validate_node_id(node_id)
        path = self._node_index.get(node_id)
        if path is None:
            raise StudyReadError("NOT_FOUND", "formal 408 knowledge node was not found")
        text, digest, _ = self.reader.text(path)
        return {
            "stable_id": node_id,
            **_node_summary(text),
            "sha256": digest,
            "document_markdown": text,
            "content_complete": True,
            "data_role": "safe_projection",
        }, path

    @staticmethod
    def _edge_type(section: str | None) -> str:
        del section
        return "related_note_untyped"

    def _direct_edges(self, node_id: str) -> tuple[list[dict[str, Any]], Path]:
        _, path = self._knowledge_node(node_id)
        text, _, _ = self.reader.text(path)
        edges = []
        seen: set[tuple[str, str]] = set()
        section: str | None = None
        for line_number, line in enumerate(text.splitlines(), start=1):
            heading = re.fullmatch(r"#{1,6}\s+(.+?)\s*", line)
            if heading:
                section = heading.group(1)
            for raw in WIKILINK.findall(line):
                target = Path(raw).name
                relation_type = self._edge_type(section)
                identity = (target, relation_type)
                if (
                    target == node_id
                    or identity in seen
                    or not NODE_ID.fullmatch(target)
                ):
                    continue
                seen.add(identity)
                edges.append({
                    "source": node_id,
                    "target": target,
                    "relation_type": relation_type,
                    "relation_authority": "study_vault_wikilink",
                    "source_section": section,
                    "source_line": line_number,
                    "data_role": "projection_navigation",
                })
        return edges, path

    @staticmethod
    def _record_id(prefix: str, *parts: str) -> str:
        digest = hashlib.sha256("\0".join(parts).encode("utf-8")).hexdigest()
        return f"{prefix}-{digest}"

    def _formal_node_records(self) -> list[dict[str, Any]]:
        self._refresh_formal_sources()
        return [dict(self._formal_nodes[key]) for key in sorted(self._formal_nodes)]

    def _formal_knowledge_records(self) -> list[dict[str, Any]]:
        self._refresh_formal_sources()
        return [dict(self._formal_knowledge[key]) for key in sorted(self._formal_knowledge)]

    def _knowledge_taxonomy_edges(self) -> list[dict[str, Any]]:
        self._refresh_formal_sources()
        output: list[dict[str, Any]] = []
        for item in self._formal_knowledge.values():
            kind = item["knowledge_kind"]
            if kind == "module":
                source = str(item["subject"])
                relation_type = "subject_contains_module"
            elif kind == "point":
                source = str(item["module"])
                relation_type = "module_contains_knowledge"
            else:
                continue
            target = str(item["stable_id"])
            output.append({
                "stable_id": self._record_id(
                    "408KREL", relation_type, source, target
                ),
                "source": source,
                "target": target,
                "relation_type": relation_type,
                "source_hash": item["source_hash"],
                "data_role": "formal_knowledge_relation",
                "content_complete": True,
            })
        return output

    def _wrong_item_knowledge_edges(self) -> list[dict[str, Any]]:
        self._refresh_formal_sources()
        output: list[dict[str, Any]] = []
        fields = (
            ("主知识点", "wrong_item_primary_knowledge"),
            ("副知识点", "wrong_item_secondary_knowledge"),
            ("命中知识点", "wrong_item_hits_knowledge"),
        )
        for node in self._formal_nodes.values():
            node_id = str(node["stable_id"])
            values = node["fields"]
            for field, relation_type in fields:
                for declared in _split_multi(str(values.get(field) or "")):
                    code, name = _split_code_name(declared)
                    target = self._formal_knowledge.get(code)
                    if target is None and declared in KNOWLEDGE_PLACEHOLDERS:
                        output.append({
                            "stable_id": self._record_id(
                                "408KGAP", relation_type, node_id, declared
                            ),
                            "source": node_id,
                            "declared_label": declared,
                            "relation_type": f"{relation_type}_evidence_gap",
                            "source_hash": node["source_hash"],
                            "data_role": "formal_evidence_gap",
                            "content_complete": True,
                        })
                        continue
                    if target is None or target.get("knowledge_kind") != "point":
                        raise StudyReadError(
                            "SUBJECT_UNAVAILABLE", "408 wrong-item knowledge code is invalid"
                        )
                    canonical_name = str(target.get("name") or "")
                    output.append({
                        "stable_id": self._record_id(
                            "408WKREL", relation_type, node_id, code
                        ),
                        "source": node_id,
                        "target": code,
                        "declared_label": declared,
                        "declared_name": name,
                        "canonical_name": canonical_name,
                        "label_matches_authority": not name or name == canonical_name,
                        "relation_type": relation_type,
                        "source_hash": node["source_hash"],
                        "data_role": "formal_wrong_item_knowledge_relation",
                        "content_complete": True,
                    })
        return output

    def _all_formal_relationships(self) -> list[dict[str, Any]]:
        self._refresh_formal_sources()
        return sorted(
            [dict(row) for row in self._formal_relation_rows]
            + self._wrong_item_knowledge_edges()
            + self._knowledge_taxonomy_edges(),
            key=lambda row: row["stable_id"],
        )

    def _assert_safe_projection_bound(self) -> None:
        manifest, _, _ = self.reader.json(self.vault_manifest)
        count = manifest.get("concept_count") if isinstance(manifest, dict) else None
        if isinstance(count, bool) or not isinstance(count, int) or count != len(self._node_index):
            raise StudyReadError(
                "PROJECTION_MANIFEST_MISMATCH",
                "408 StudyVault projection does not match its manifest concept count",
                True,
            )

    def _curation_projection(self) -> tuple[dict[str, Any], dict[str, Any]]:
        state, state_sha256, _ = self.reader.json(self.curation_state)
        if not isinstance(state, dict):
            raise StudyReadError("SUBJECT_UNAVAILABLE", "408 curation state has an invalid schema")
        events_text, events_sha256, _ = self.reader.text(self.curation_events)
        event_lines = [line for line in events_text.splitlines() if line.strip()]
        event_count = state.get("event_count")
        if isinstance(event_count, bool) or not isinstance(event_count, int) or event_count != len(event_lines):
            raise StudyReadError(
                "PROJECTION_EVENT_MISMATCH",
                "408 curation projection does not match the event ledger high-water mark",
                True,
            )
        last_event_id = None
        if event_lines:
            try:
                last_event = __import__("json").loads(event_lines[-1])
            except (TypeError, ValueError) as exc:
                raise StudyReadError("PROJECTION_EVENT_MISMATCH", "408 event ledger high-water row is invalid", True) from exc
            if isinstance(last_event, dict):
                last_event_id = last_event.get("event_id")
        return state, {
            "projection_sha256": state_sha256,
            "event_ledger_sha256": events_sha256,
            "event_count": event_count,
            "last_event_id": last_event_id,
            "data_role": "projection_event_binding",
        }

    def _curation_inventory(
        self, study_date: date | None, capture_ids: list[str]
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        state, binding = self._curation_projection()
        captures = state.get("captures", {})
        if not isinstance(captures, dict):
            captures = {}
        selected = set(capture_ids)
        if selected:
            for value in selected:
                SafeReader.validate_stable_id(value, "capture id")
        output: list[dict[str, Any]] = []
        for capture_id, value in captures.items():
            if not isinstance(value, dict):
                continue
            if selected and capture_id not in selected:
                continue
            capture = value.get("capture") if isinstance(value.get("capture"), dict) else {}
            row_date = value.get("study_date") or capture.get("study_date")
            if study_date is not None and row_date != study_date.isoformat():
                continue
            output.append({
                "capture_id": capture_id, "study_date": row_date, "formal_id": value.get("formal_id"),
                "quality_status": value.get("quality_status"), "formalization_authorized": value.get("formalization_authorized"),
                "current_batch_id": value.get("current_batch_id"), "payload_sha256": value.get("payload_sha256"),
                "data_role": "projection",
            })
        return output, binding

    def _morning_state(self, session_ids: list[str]) -> tuple[list[dict[str, Any]], list[Path]]:
        self._refresh_morning_index()
        if session_ids:
            ids = []
            for value in session_ids:
                SafeReader.validate_stable_id(value, "morning session id")
                ids.append(value)
        elif self._morning_index:
            ids = sorted(self._morning_index)
        else:
            return [], []
        result: list[dict[str, Any]] = []
        paths: list[Path] = []
        for session_id in ids:
            path = self._morning_index.get(session_id)
            if path is None:
                raise StudyReadError("NOT_FOUND", "morning session was not found")
            paths.append(path)
            state, digest, _ = self.reader.json(path)
            if not isinstance(state, dict):
                raise StudyReadError("SUBJECT_UNAVAILABLE", "morning state has an invalid schema")
            order = state.get("item_order") if isinstance(state.get("item_order"), list) else []
            result.append({
                "session_id": state.get("session_id") or session_id, "status": state.get("status"),
                "review_date": state.get("review_date"), "item_count": len(order),
                "state_sha256": digest, "data_role": "runtime_state",
            })
        return result, paths

    def _connect_projection(self) -> tuple[sqlite3.Connection, dict[str, Any], Path]:
        manifest, _, _ = self.reader.json(self.hot_manifest)
        if not isinstance(manifest, dict):
            raise StudyReadError("SUBJECT_UNAVAILABLE", "hot-state manifest has an invalid schema")
        build = manifest.get("projection_build_id")
        manifest_sha = manifest.get("manifest_sha256")
        if not isinstance(build, str) or not re.fullmatch(r"RHS-[A-F0-9]+", build):
            raise StudyReadError("SUBJECT_UNAVAILABLE", "hot-state projection build id is invalid")
        db = self.reader.exact("cs408", "wiki", "study_vaults", "408-full", "state", "review-loop", "hot-state", "databases", f"{build}.sqlite3")
        if self._sqlite is not None and self._sqlite_manifest_sha != manifest_sha:
            self.close()
        if self._sqlite is None:
            try:
                self._sqlite = sqlite3.connect(f"{db.as_uri()}?mode=ro", uri=True, timeout=0.1, check_same_thread=False)
                self._sqlite.execute("PRAGMA query_only=ON")
                self._sqlite.execute("PRAGMA busy_timeout=100")
                self._sqlite_manifest_sha = str(manifest_sha)
                self.sqlite_opens += 1
            except sqlite3.DatabaseError as exc:
                self.close()
                message = str(exc).lower()
                code = "SQLITE_BUSY" if "locked" in message or "busy" in message else "SQLITE_CORRUPT"
                raise StudyReadError(code, "408 projection could not be opened", code == "SQLITE_BUSY") from exc
        return self._sqlite, manifest, db

    def _canonical_review_high_water(self) -> tuple[dict[str, Any], Any]:
        result = self.reader.read_bytes(self.review_events)
        count = 0
        last_event_id: str | None = None
        byte_end = 0
        for line in result.data.splitlines(keepends=True):
            byte_end += len(line)
            if not line.strip():
                continue
            try:
                value = __import__("json").loads(line)
            except (UnicodeError, ValueError) as exc:
                raise StudyReadError(
                    "PROJECTION_EVENT_MISMATCH", "canonical review ledger is not valid JSONL"
                ) from exc
            if not isinstance(value, dict) or not isinstance(value.get("event_id"), str):
                raise StudyReadError(
                    "PROJECTION_EVENT_MISMATCH", "canonical review ledger event identity is invalid"
                )
            count += 1
            last_event_id = value["event_id"]
        return {
            "event_ledger_sha256": result.sha256,
            "event_count": count,
            "last_event_id": last_event_id,
            "byte_length": len(result.data),
            "data_role": "projection_event_binding",
        }, result.identity

    @staticmethod
    def _assert_review_projection_bound(
        connection: sqlite3.Connection, canonical: dict[str, Any]
    ) -> dict[str, Any]:
        count = connection.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        last = connection.execute(
            "SELECT event_id,line_number,byte_offset,byte_length FROM events "
            "ORDER BY line_number DESC,event_id DESC LIMIT 1"
        ).fetchone()
        last_event_id = last[0] if last else None
        last_line_number = last[1] if last else 0
        indexed_byte_end = (
            int(last[2]) + int(last[3])
            if last and last[2] is not None and last[3] is not None else None
        )
        if (
            count != canonical["event_count"]
            or last_event_id != canonical["last_event_id"]
            or last_line_number != canonical["event_count"]
            or indexed_byte_end != canonical["byte_length"]
        ):
            raise StudyReadError(
                "PROJECTION_EVENT_MISMATCH",
                "408 review projection does not match the canonical event high-water",
                True,
            )
        return {
            **canonical,
            "projection_event_count": count,
            "projection_last_event_id": last_event_id,
            "projection_last_line_number": last_line_number,
            "projection_indexed_byte_end": indexed_byte_end,
        }

    def _review_identity(self, identities: list[str]) -> tuple[list[dict[str, Any]], Path]:
        for value in identities:
            SafeReader.validate_stable_id(value, "review identity")
        connection, manifest, db = self._connect_projection()
        placeholders = ",".join("?" for _ in identities)
        sql = (
            "SELECT event_id,event_kind,session_id,item_id,identity,observed_date,evidence_kind,line_number "
            f"FROM events WHERE identity IN ({placeholders}) ORDER BY line_number DESC LIMIT 96"
        )
        try:
            canonical, canonical_identity = self._canonical_review_high_water()
            connection.execute("BEGIN")
            binding = self._assert_review_projection_bound(connection, canonical)
            rows = connection.execute(sql, identities).fetchall()
            connection.rollback()
            if self.reader.identity(self.review_events) != canonical_identity:
                raise StudyReadError(
                    "SOURCE_CHANGED_DURING_READ", "canonical review ledger changed during read", True
                )
        except StudyReadError:
            try:
                connection.rollback()
            except sqlite3.DatabaseError:
                pass
            raise
        except sqlite3.DatabaseError as exc:
            try:
                connection.rollback()
            except sqlite3.DatabaseError:
                pass
            message = str(exc).lower()
            code = "SQLITE_BUSY" if "locked" in message or "busy" in message else "SQLITE_CORRUPT"
            raise StudyReadError(code, "408 projection query failed", code == "SQLITE_BUSY") from exc
        keys = ("event_id", "event_kind", "session_id", "item_id", "identity", "observed_date", "evidence_kind", "line_number")
        return [
            dict(
                zip(keys, row), data_role="projection",
                projection_build_id=manifest.get("projection_build_id"),
                projection_event_binding=binding,
            )
            for row in rows
        ], db

    def _all_review_events(self) -> tuple[list[dict[str, Any]], Path]:
        connection, manifest, db = self._connect_projection()
        try:
            canonical, canonical_identity = self._canonical_review_high_water()
            connection.execute("BEGIN")
            binding = self._assert_review_projection_bound(connection, canonical)
            rows = connection.execute(
                "SELECT event_id,event_kind,session_id,item_id,identity,observed_date,"
                "evidence_kind,line_number FROM events ORDER BY line_number,event_id"
            ).fetchall()
            connection.rollback()
            if self.reader.identity(self.review_events) != canonical_identity:
                raise StudyReadError(
                    "SOURCE_CHANGED_DURING_READ", "canonical review ledger changed during read", True
                )
        except StudyReadError:
            try:
                connection.rollback()
            except sqlite3.DatabaseError:
                pass
            raise
        except sqlite3.DatabaseError as exc:
            try:
                connection.rollback()
            except sqlite3.DatabaseError:
                pass
            message = str(exc).lower()
            code = "SQLITE_BUSY" if "locked" in message or "busy" in message else "SQLITE_CORRUPT"
            raise StudyReadError(
                code, "408 projection query failed", code == "SQLITE_BUSY"
            ) from exc
        keys = (
            "event_id", "event_kind", "session_id", "item_id", "identity",
            "observed_date", "evidence_kind", "line_number",
        )
        source_hash = str(manifest.get("manifest_sha256") or "")
        return [
            {
                **dict(zip(keys, row)),
                "stable_id": str(row[0]),
                "source_hash": source_hash,
                "data_role": "projection",
                "projection_build_id": manifest.get("projection_build_id"),
                "projection_event_binding": binding,
            }
            for row in rows
        ], db

    @staticmethod
    def _matches(value: Any, query: str) -> bool:
        return query.casefold() in __import__("json").dumps(
            value, ensure_ascii=False, sort_keys=True
        ).casefold()

    def luna_records(self, request: PagedReadRequest) -> list[dict[str, Any]]:
        """Build a deterministic subject-only result set with no hidden top-k."""

        collection = request.collection
        allowed = {
            "formal_wrong_item_catalog", "formal_nodes",
            "formal_knowledge_catalog", "knowledge_catalog", "knowledge_nodes",
            "formal_relationships", "relationships", "wrong_item_knowledge_edges",
            "knowledge_taxonomy_edges", "knowledge_safe_notes", "concept_note_catalog",
            "concept_notes", "knowledge_navigation_links", "search",
            "curation_inventory", "morning_sessions", "review_events",
        }
        if collection not in allowed:
            raise StudyReadError("INVALID_ARGUMENT", "unsupported 408 Luna collection")
        query = (request.query or "").strip()
        requested = set(request.ids)
        if collection == "search" and not query:
            raise StudyReadError("INVALID_ARGUMENT", "408 search requires a query")
        if collection in {"formal_wrong_item_catalog", "formal_nodes"}:
            rows = self._formal_node_records()
            if requested:
                for value in requested:
                    SafeReader.validate_stable_id(value, "408 formal node id")
                rows = [row for row in rows if row["stable_id"] in requested]
            if collection == "formal_wrong_item_catalog":
                rows = [
                    {
                        "stable_id": row["stable_id"],
                        "source_hash": row["source_hash"],
                        "subject": row["fields"].get("科目"),
                        "module": row["fields"].get("主模块"),
                        "primary_knowledge": row["fields"].get("主知识点"),
                        "question_type": row["fields"].get("题型"),
                        "data_role": "formal_wrong_item_catalog",
                    }
                    for row in rows
                ]
            if query:
                rows = [row for row in rows if self._matches(row, query)]
            return rows
        if collection in {
            "formal_knowledge_catalog", "knowledge_catalog", "knowledge_nodes"
        }:
            rows = self._formal_knowledge_records()
            if requested:
                for value in requested:
                    SafeReader.validate_stable_id(value, "408 knowledge id")
                rows = [row for row in rows if row["stable_id"] in requested]
            if collection in {"formal_knowledge_catalog", "knowledge_catalog"}:
                rows = [
                    {
                        "stable_id": row["stable_id"],
                        "source_hash": row["source_hash"],
                        "knowledge_kind": row["knowledge_kind"],
                        "name": row["name"],
                        "subject": row.get("subject"),
                        "module": row.get("module"),
                        "data_role": "formal_knowledge_catalog",
                    }
                    for row in rows
                ]
            if query:
                rows = [row for row in rows if self._matches(row, query)]
            return rows
        if collection in {
            "formal_relationships", "relationships", "wrong_item_knowledge_edges",
            "knowledge_taxonomy_edges",
        }:
            if collection == "wrong_item_knowledge_edges":
                rows = self._wrong_item_knowledge_edges()
            elif collection == "knowledge_taxonomy_edges":
                rows = self._knowledge_taxonomy_edges()
            else:
                rows = self._all_formal_relationships()
            if requested:
                for value in requested:
                    SafeReader.validate_stable_id(value, "408 relation endpoint id")
                rows = [
                    row for row in rows
                    if requested & {
                        str(row.get("stable_id") or ""),
                        str(row.get("source") or ""),
                        str(row.get("target") or ""),
                    }
                ]
            if query:
                rows = [row for row in rows if self._matches(row, query)]
            return rows
        if collection in {
            "knowledge_safe_notes", "concept_note_catalog", "concept_notes"
        }:
            self._refresh_node_index()
            self._assert_safe_projection_bound()
            output: list[dict[str, Any]] = []
            for node_id in sorted(self._node_index):
                if requested and node_id not in requested:
                    continue
                item, _ = self._knowledge_node(node_id)
                if query and not self._matches(item, query):
                    continue
                if collection == "concept_note_catalog":
                    item = {
                        "stable_id": node_id,
                        "source_hash": item["sha256"],
                        "title": item.get("title"),
                        "area": item.get("area"),
                        "data_role": "safe_projection_catalog",
                    }
                else:
                    item["source_hash"] = item.pop("sha256")
                output.append(item)
            return output
        if collection == "knowledge_navigation_links":
            self._refresh_node_index()
            self._assert_safe_projection_bound()
            output = []
            for node_id in sorted(self._node_index):
                if requested and node_id not in requested:
                    continue
                edges, path = self._direct_edges(node_id)
                source_hash, _ = self.reader.digest(path)
                for edge in edges:
                    target = str(edge.get("target") or "")
                    relation_type = "related_note_untyped"
                    item = {
                        **edge,
                        "stable_id": self._record_id(
                            "408NAV", node_id, target, relation_type
                        ),
                        "source_hash": source_hash,
                    }
                    if query and not self._matches(item, query):
                        continue
                    output.append(item)
            return sorted(output, key=lambda row: row["stable_id"])
        if collection == "search":
            combined: list[dict[str, Any]] = []
            for nested in (
                "formal_nodes", "knowledge_nodes", "formal_relationships",
            ):
                nested_request = request.model_copy(
                    update={"collection": nested, "ids": [], "cursor": None, "query": None}
                )
                for row in self.luna_records(nested_request):
                    if self._matches(row, query):
                        combined.append({"matched_collection": nested, **row})
            return sorted(
                combined,
                key=lambda row: (
                    str(row.get("matched_collection")), str(row.get("stable_id"))
                ),
            )
        if collection == "curation_inventory":
            rows, binding = self._curation_inventory(request.study_date, request.ids)
            projection_hash = str(binding["projection_sha256"])
            output = [
                {
                    **row,
                    "stable_id": str(row.get("capture_id")),
                    "source_hash": projection_hash,
                    "projection_event_binding": binding,
                }
                for row in rows
            ]
            if query:
                output = [row for row in output if self._matches(row, query)]
            return output
        if collection == "morning_sessions":
            rows, _ = self._morning_state(request.ids)
            output = [
                {
                    **row,
                    "stable_id": str(row.get("session_id")),
                    "source_hash": str(row.get("state_sha256")),
                }
                for row in rows
            ]
            if query:
                output = [row for row in output if self._matches(row, query)]
            return output
        rows, _ = self._all_review_events()
        if requested:
            rows = [
                row for row in rows
                if requested & {
                    str(row.get("identity") or ""), str(row.get("item_id") or ""),
                    str(row.get("session_id") or ""), str(row.get("event_id") or ""),
                }
            ]
        if query:
            rows = [row for row in rows if self._matches(row, query)]
        return rows

    def read_bundle(
        self, queries: list[CS408Query], expected_generation: str | None = None,
        expected_release: str | None = None, expected_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        if not 1 <= len(queries) <= 24:
            raise StudyReadError("INVALID_ARGUMENT", "408 query count must be between 1 and 24")
        with self._lock:
            snapshot = self.authority()
            self.bind_expected(snapshot, expected_generation, expected_release, expected_fingerprint)
            tracked = {self.vault_manifest, self.curation_state, self.curation_events, self.hot_manifest}
            before = self.capture_identities(tracked, self.reader)
            output: list[dict[str, Any]] = []
            for query in queries:
                if query.op == "curation_inventory":
                    inventory, binding = self._curation_inventory(query.study_date, query.ids)
                    output.append({
                        "operation": query.op,
                        "data_role": "projection",
                        "projection_event_binding": binding,
                        "items": inventory,
                    })
                elif query.op == "knowledge_nodes":
                    values = []
                    for node_id in query.ids:
                        self._refresh_node_index()
                        path = self._node_index.get(self._validate_node_id(node_id))
                        if path is None:
                            raise StudyReadError("NOT_FOUND", "formal 408 knowledge node was not found")
                        tracked.add(path)
                        before[path] = self.reader.identity(path)
                        item, path = self._knowledge_node(node_id)
                        values.append(item)
                    output.append({"operation": query.op, "items": values})
                elif query.op == "direct_edges":
                    values = []
                    for node_id in query.ids:
                        self._refresh_node_index()
                        path = self._node_index.get(self._validate_node_id(node_id))
                        if path is None:
                            raise StudyReadError("NOT_FOUND", "formal 408 knowledge node was not found")
                        tracked.add(path)
                        before[path] = self.reader.identity(path)
                        edges, path = self._direct_edges(node_id)
                        values.extend(edges)
                    output.append({"operation": query.op, "items": values})
                elif query.op == "morning_state":
                    morning, paths = self._morning_state(query.ids)
                    for path in paths:
                        tracked.add(path)
                        before.setdefault(path, self.reader.identity(path))
                    output.append({"operation": query.op, "items": morning})
                elif query.op == "review_identity":
                    connection, manifest, db = self._connect_projection()
                    tracked.add(db)
                    before[db] = self.reader.identity(db)
                    review, _ = self._review_identity(query.ids)
                    output.append({"operation": query.op, "items": review})
            before.update({path: self.reader.identity(path) for path in tracked if path not in before})
            self.assert_unchanged(before, self.reader)
            rebound = self.authority()
            if rebound.fingerprint != snapshot.fingerprint:
                raise StudyReadError("SOURCE_CHANGED_DURING_READ", "408 authority changed during bundle read", True)
            return success_envelope(
                subject=self.subject, data_role="mixed", authority_source_id=self.authority_source_id,
                generation=snapshot.generation, authority_fingerprint=snapshot.fingerprint, items=output,
                warnings=["SQLite results are manifest-bound materialized projections, not formal authority"],
            )
