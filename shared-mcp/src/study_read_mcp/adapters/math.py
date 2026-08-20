from __future__ import annotations

import hashlib
import re
from datetime import date
from pathlib import Path
from typing import Any

from ..envelope import success_envelope
from ..errors import StudyReadError
from ..models import MathQuery, PagedReadRequest
from ..safeio import SafeReader
from .base import AuthoritySnapshot, BaseAdapter


MATH_ID_TOKEN = re.compile(
    r"(?<![\w.-])([A-Za-z][A-Za-z0-9]{0,31}-[A-Za-z0-9][A-Za-z0-9.-]{0,95})(?![\w.-])"
)
KNOWLEDGE_ID = re.compile(r"^MKN-[0-9a-f]{64}$")
KNOWLEDGE_CATEGORY_ID = re.compile(r"^MKC-[0-9a-f]{64}$")
ALLOWED_CARD_FIELDS = frozenset({
    "id", "title", "subject", "chapter", "question_type", "date", "status",
    "difficulty", "priority", "knowledge", "methods", "traps", "related",
    "wrong_point", "error_causes", "method_gap",
})
SAFE_METHOD_GAP_FIELDS = frozenset({
    "enabled", "knowledge_gap_or_method_gap", "method_trigger", "expected_method",
    "expected_first_action", "missed_action", "action_gap_type", "confidence",
    "need_user_confirmation", "evidence_origin", "next_reminder",
})
KNOWLEDGE_SECTIONS = {
    "高等数学": "knowledge",
    "线性代数": "knowledge",
    "概率论与数理统计": "knowledge",
    "历史正式卡已用细分知识标签（2026-07-10 补登记）": "knowledge",
    "错因标签": "error_cause",
    "方法标签": "method",
    "专题链标签": "topic_chain",
    "陷阱标签": "trap",
}


def _frontmatter_scalar(value: str) -> Any:
    value = value.strip().strip('"\'')
    if value in {"[]", "[ ]"}:
        return []
    if value.casefold() in {"null", "none", "~"}:
        return None
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)
    return value


def _parse_full_frontmatter(text: str) -> dict[str, Any]:
    """Parse the complete YAML subset used by the formal card corpus."""

    if not text.startswith("---\n"):
        return {}
    end = text.find("\n---", 4)
    if end < 0:
        return {}
    lines = text[4:end].splitlines()
    result: dict[str, Any] = {}
    current_key: str | None = None
    current_nested: str | None = None
    index = 0

    def indent(raw: str) -> int:
        return len(raw) - len(raw.lstrip(" "))

    def collect_block(
        start: int, minimum: int, folded: bool
    ) -> tuple[str, int]:
        values: list[str] = []
        cursor = start
        while cursor < len(lines):
            raw = lines[cursor]
            if raw.strip() and indent(raw) < minimum:
                break
            values.append(raw[minimum:] if raw.strip() else "")
            cursor += 1
        if not folded:
            return "\n".join(values).strip(), cursor
        paragraphs: list[str] = []
        current: list[str] = []
        for value in values:
            if value.strip():
                current.append(value.strip())
            elif current:
                paragraphs.append(" ".join(current))
                current = []
        if current:
            paragraphs.append(" ".join(current))
        return "\n".join(paragraphs).strip(), cursor

    while index < len(lines):
        raw = lines[index]
        index += 1
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        depth = indent(raw)
        if stripped.startswith("- "):
            if current_key is None:
                continue
            if depth >= 4 and current_nested is not None:
                parent = result.setdefault(current_key, {})
                if not isinstance(parent, dict):
                    parent = {}
                    result[current_key] = parent
                target = parent.setdefault(current_nested, [])
            else:
                target = result.setdefault(current_key, [])
            if isinstance(target, list):
                target.append(_frontmatter_scalar(stripped[2:].strip()))
            continue
        if ":" not in stripped:
            continue
        key, value = stripped.split(":", 1)
        key, value = key.strip(), value.strip()
        if depth == 0:
            current_key = key
            current_nested = None
            if value in {">", ">-", ">+", "|", "|-", "|+"}:
                result[key], index = collect_block(
                    index, 2, value.startswith(">")
                )
            else:
                result[key] = _frontmatter_scalar(value) if value else []
            continue
        if current_key is None:
            continue
        parent = result.get(current_key)
        if not isinstance(parent, dict):
            parent = {}
            result[current_key] = parent
        current_nested = key
        if value in {">", ">-", ">+", "|", "|-", "|+"}:
            parent[key], index = collect_block(
                index, depth + 2, value.startswith(">")
            )
        else:
            parent[key] = _frontmatter_scalar(value) if value else []
    return result


class MathAdapter(BaseAdapter):
    subject = "math"
    authority_source_id = "math.formal-events-projection"
    capabilities = frozenset({"formal_cards", "activity_window", "direct_relations", "review_snapshot"})

    def __init__(self, root: Path, *, reader: SafeReader | None = None) -> None:
        super().__init__(reader or SafeReader({"math": root}))
        self.root = self.reader.roots["math"]
        self.cards_dir = self.reader.exact("math", "错题知识网络", "错题卡")
        self.knowledge_library = self.reader.exact(
            "math", "错题知识网络", "知识点库.md"
        )
        self.projection = self.reader.exact("math", "错题知识网络", "生成", "wrong_questions.json")
        self.capture_ledger = self.reader.exact("math", "数学一回滚复习系统", "快速入库事件.jsonl")
        self.review_ledger = self.reader.exact("math", "数学一回滚复习系统", "复习记录.jsonl")
        self.warmup_ledger = self.reader.exact("math", "数学一回滚复习系统", "学习前5题记录.jsonl")
        self._card_dir_identity = None
        self._card_index: dict[str, Path] = {}
        self._knowledge_identity = None
        self._knowledge_index: dict[str, dict[str, Any]] = {}
        self._knowledge_categories: dict[str, dict[str, Any]] = {}
        self.directory_scans = 0
        self._refresh_card_index()
        self._refresh_knowledge_catalog()

    def _refresh_card_index(self) -> None:
        identity = self.reader.identity(self.cards_dir)
        if identity == self._card_dir_identity:
            return
        index: dict[str, Path] = {}
        for path in sorted(self.cards_dir.glob("*.md")):
            if path.is_symlink() or not path.is_file():
                continue
            if path.name.casefold() == "readme.md":
                continue
            path = self.reader.validate_indexed_path("math", path)
            text, _, _ = self.reader.text(path)
            card_id = _parse_full_frontmatter(text).get("id")
            if not isinstance(card_id, str):
                raise StudyReadError(
                    "SUBJECT_UNAVAILABLE", "formal math card has no stable frontmatter id"
                )
            SafeReader.validate_stable_id(card_id, "math card id")
            if path.name.split("_", 1)[0] != card_id:
                raise StudyReadError(
                    "SUBJECT_UNAVAILABLE", "math card id does not match its filename"
                )
            if card_id in index:
                raise StudyReadError(
                    "SUBJECT_UNAVAILABLE", "formal math card id is duplicated"
                )
            index[card_id] = path
        self._card_index = index
        self._card_dir_identity = identity
        self.directory_scans += 1

    def _refresh_knowledge_catalog(self) -> None:
        identity = self.reader.identity(self.knowledge_library)
        if identity == self._knowledge_identity:
            return
        text, source_hash, _ = self.reader.text(self.knowledge_library)
        index: dict[str, dict[str, Any]] = {}
        categories: dict[str, dict[str, Any]] = {}
        section: str | None = None
        namespace: str | None = None
        labels_by_namespace: set[tuple[str, str]] = set()
        for line_number, line in enumerate(text.splitlines(), start=1):
            heading = re.fullmatch(r"##\s+(.+?)\s*", line)
            if heading:
                section = heading.group(1).strip()
                namespace = KNOWLEDGE_SECTIONS.get(section)
                if namespace is not None:
                    category_id = "MKC-" + hashlib.sha256(
                        f"{namespace}\0{section}".encode("utf-8")
                    ).hexdigest()
                    categories[category_id] = {
                        "stable_id": category_id,
                        "namespace": namespace,
                        "category": section,
                        "source_line": line_number,
                        "source_hash": source_hash,
                        "data_role": "formal_taxonomy_category",
                        "content_complete": True,
                    }
                continue
            bullet = re.fullmatch(r"\s*-\s+(.+?)\s*", line)
            if bullet is None or section is None or namespace is None:
                continue
            label = bullet.group(1).strip()
            identity_key = (namespace, label)
            if identity_key in labels_by_namespace:
                raise StudyReadError(
                    "SUBJECT_UNAVAILABLE", "math taxonomy contains a duplicate role-bound label"
                )
            labels_by_namespace.add(identity_key)
            stable_id = "MKN-" + hashlib.sha256(
                f"{namespace}\0{section}\0{label}".encode("utf-8")
            ).hexdigest()
            index[stable_id] = {
                "stable_id": stable_id,
                "namespace": namespace,
                "category": section,
                "canonical_label": label,
                "source_line": line_number,
                "source_hash": source_hash,
                "data_role": "formal_taxonomy_label",
                "content_complete": True,
            }
        self._knowledge_index = index
        self._knowledge_categories = categories
        self._knowledge_identity = identity
        self.directory_scans += 1

    def authority(self) -> AuthoritySnapshot:
        self.assert_available()
        with self._lock:
            self._refresh_card_index()
            self._refresh_knowledge_catalog()
            bindings: list[tuple[str, str]] = []
            sources: list[dict[str, Any]] = []
            for source_id, role, path in (
                ("math.formal-card-index", "formal", self.cards_dir),
                ("math.formal-taxonomy", "formal", self.knowledge_library),
                ("math.wrong-questions", "projection", self.projection),
                ("math.fast-intake-events", "event", self.capture_ledger),
                ("math.review-events", "event", self.review_ledger),
                ("math.warmup-events", "event", self.warmup_ledger),
            ):
                if path.is_dir():
                    digest = self.reader.set_fingerprint(
                        (stable_id, self.reader.digest(indexed_path)[0])
                        for stable_id, indexed_path in self._card_index.items()
                    )
                else:
                    digest, _ = self.reader.digest(path)
                bindings.append((source_id, digest))
                sources.append({"capability": source_id, "data_role": role, "available": True, "sha256": digest})
            fingerprint = self.digest_bindings(bindings)
            return AuthoritySnapshot(self.subject, self.generation(self.subject, fingerprint), fingerprint, tuple(sources))

    @staticmethod
    def _validate_id(value: str) -> str:
        SafeReader.validate_stable_id(value, "math id")
        return value

    def _formal_card(self, card_id: str, fields: list[str]) -> tuple[dict[str, Any], Path]:
        self._refresh_card_index()
        card_id = self._validate_id(card_id)
        path = self._card_index.get(card_id)
        if path is None:
            raise StudyReadError("NOT_FOUND", "formal math card was not found")
        text, digest, _ = self.reader.text(path)
        summary = _parse_full_frontmatter(text)
        selected = ALLOWED_CARD_FIELDS if not fields else frozenset(fields)
        if not selected <= ALLOWED_CARD_FIELDS:
            raise StudyReadError("INVALID_ARGUMENT", "unsupported math field projection")
        item = {key: value for key, value in summary.items() if key in selected}
        method_gap = item.get("method_gap")
        if isinstance(method_gap, dict):
            item["method_gap"] = {
                key: value for key, value in method_gap.items()
                if key in SAFE_METHOD_GAP_FIELDS
            }
        item.update({"stable_id": card_id, "sha256": digest, "data_role": "formal"})
        return item, path

    def _full_formal_card(self, card_id: str) -> tuple[dict[str, Any], Path]:
        self._refresh_card_index()
        card_id = self._validate_id(card_id)
        path = self._card_index.get(card_id)
        if path is None:
            raise StudyReadError("NOT_FOUND", "formal math card was not found")
        text, digest, _ = self.reader.text(path)
        frontmatter = _parse_full_frontmatter(text)
        return {
            **frontmatter,
            "frontmatter": frontmatter,
            "stable_id": card_id,
            "source_hash": digest,
            "document_markdown": text,
            "content_complete": True,
            "data_role": "formal",
        }, path

    @staticmethod
    def _validate_knowledge_id(value: str) -> str:
        SafeReader.validate_stable_id(value, "math knowledge id")
        if not (KNOWLEDGE_ID.fullmatch(value) or KNOWLEDGE_CATEGORY_ID.fullmatch(value)):
            raise StudyReadError("INVALID_ARGUMENT", "invalid math knowledge id")
        return value

    def _knowledge_node(self, stable_id: str) -> tuple[dict[str, Any], Path]:
        self._refresh_knowledge_catalog()
        stable_id = self._validate_knowledge_id(stable_id)
        item = self._knowledge_index.get(stable_id) or self._knowledge_categories.get(stable_id)
        if item is None:
            raise StudyReadError("NOT_FOUND", "formal math knowledge node was not found")
        return dict(item), self.knowledge_library

    @staticmethod
    def _date_in(value: Any, start: date, end: date) -> bool:
        if not isinstance(value, str):
            return False
        try:
            parsed = date.fromisoformat(value[:10])
        except ValueError:
            return False
        return start <= parsed <= end

    def _activity(self, start: date, end: date, ids: set[str] | None = None) -> list[dict[str, Any]]:
        captures, _, _ = self.reader.jsonl(self.capture_ledger)
        reviews, _, _ = self.reader.jsonl(self.review_ledger)
        items: list[dict[str, Any]] = []
        for row in captures:
            target = row.get("target") if isinstance(row.get("target"), dict) else {}
            formal_id = target.get("formal_id")
            if not self._date_in(row.get("study_date") or row.get("recorded_at"), start, end):
                continue
            if ids and formal_id not in ids:
                continue
            evidence = row.get("evidence") if isinstance(row.get("evidence"), dict) else {}
            items.append({
                "event_id": row.get("event_id"), "event_kind": "capture", "study_date": row.get("study_date"),
                "formal_id": formal_id, "state": row.get("initial_state"), "result": evidence.get("result"),
                "mastery_score": evidence.get("mastery_score"), "data_role": "event",
            })
        for row in reviews:
            formal_id = row.get("delivered_card_id")
            if not self._date_in(row.get("date"), start, end):
                continue
            if ids and formal_id not in ids:
                continue
            items.append({
                "event_id": row.get("event_id"), "event_kind": "review", "study_date": row.get("date"),
                "formal_id": formal_id, "anchor_card_id": row.get("anchor_card_id"), "score": row.get("score"),
                "attempt_type": row.get("attempt_type"), "match_mode": row.get("match_mode"),
                "advances_long_term": row.get("advances_long_term"), "data_role": "event",
            })
        return items

    def _relations(self, card_ids: list[str]) -> list[dict[str, Any]]:
        ids = {self._validate_id(value) for value in card_ids}
        projection, digest, _ = self.reader.json(self.projection)
        if not isinstance(projection, dict):
            raise StudyReadError("SUBJECT_UNAVAILABLE", "math projection has an invalid schema")
        output: list[dict[str, Any]] = []
        for role, key in (("projection", "similarities"), ("candidate", "weak_relations")):
            for edge in projection.get(key, []):
                if not isinstance(edge, dict) or not ({edge.get("a"), edge.get("b")} & ids):
                    continue
                output.append({
                    "a": edge.get("a"), "b": edge.get("b"), "level": edge.get("level"),
                    "score": edge.get("score"), "reasons": edge.get("reasons", []),
                    "source": edge.get("source"), "data_role": role, "projection_sha256": digest,
                })
        return output

    def _taxonomy_label_index(self, namespace: str) -> dict[str, str]:
        self._refresh_knowledge_catalog()
        output: dict[str, str] = {}
        for stable_id, node in sorted(self._knowledge_index.items()):
            if node.get("namespace") != namespace:
                continue
            label = str(node.get("canonical_label") or "").strip().casefold()
            if label:
                output[label] = stable_id
        return output

    @staticmethod
    def _relation_id(
        relation_type: str, source: str, target: str, source_hash: str
    ) -> str:
        digest = hashlib.sha256(
            f"{relation_type}\0{source}\0{target}\0{source_hash}".encode("utf-8")
        ).hexdigest()
        return f"MREL-{digest[:24]}"

    def _formal_card_relations(self) -> list[dict[str, Any]]:
        """Return explicit card relations without projection-derived ranking."""

        role_fields = (
            ("knowledge", "knowledge", "wrong_item_has_knowledge"),
            ("methods", "method", "wrong_item_has_method_label"),
            ("error_causes", "error_cause", "wrong_item_has_error_cause"),
            ("traps", "trap", "wrong_item_has_trap"),
        )
        role_indexes = {
            namespace: self._taxonomy_label_index(namespace)
            for _, namespace, _ in role_fields
        }
        output: list[dict[str, Any]] = []
        for card_id in sorted(self._card_index):
            card, _ = self._full_formal_card(card_id)
            source_hash = str(card["source_hash"])
            frontmatter = card["frontmatter"]
            for field, namespace, relation_type in role_fields:
                values = frontmatter.get(field)
                if not isinstance(values, list):
                    values = [values] if isinstance(values, str) else []
                for term in values:
                    label = str(term).strip()
                    if not label:
                        continue
                    target = role_indexes[namespace].get(label.casefold())
                    target_kind = "formal_taxonomy_label"
                    target_resolved = target is not None
                    if target is None:
                        target = "MKN-" + hashlib.sha256(
                            f"{namespace}\0unregistered\0{label}".encode("utf-8")
                        ).hexdigest()
                        target_kind = "unregistered_formal_card_label"
                    output.append({
                        "stable_id": self._relation_id(
                            relation_type, card_id, target, source_hash
                        ),
                        "source_hash": source_hash,
                        "source": card_id,
                        "target": target,
                        "target_label": label,
                        "target_namespace": namespace,
                        "target_kind": target_kind,
                        "target_resolved": target_resolved,
                        "relation_type": relation_type,
                        "relation_authority": "formal_card_frontmatter",
                        "data_role": "formal_declared_relation",
                    })
            related = frontmatter.get("related")
            if not isinstance(related, list):
                related = [related] if isinstance(related, str) else []
            seen_targets: set[str] = set()
            for raw in related:
                for matched in MATH_ID_TOKEN.finditer(str(raw)):
                    target = matched.group(1)
                    if (
                        target not in self._card_index
                        or target == card_id
                        or target in seen_targets
                    ):
                        continue
                    seen_targets.add(target)
                    relation_type = "declared_related_untyped"
                    output.append({
                        "stable_id": self._relation_id(
                            relation_type, card_id, target, source_hash
                        ),
                        "source_hash": source_hash,
                        "source": card_id,
                        "target": target,
                        "relation_type": relation_type,
                        "relation_authority": "formal_card_frontmatter",
                        "data_role": "formal_declared_relation",
                    })
            method_gap = frontmatter.get("method_gap")
            if isinstance(method_gap, dict):
                method_refs: list[str] = []
                for key in (
                    "related_method_card_id", "related_method_card_ids",
                    "secondary_method_card_ids",
                ):
                    raw = method_gap.get(key)
                    if isinstance(raw, list):
                        method_refs.extend(str(value).strip() for value in raw)
                    elif isinstance(raw, str):
                        method_refs.append(raw.strip())
                for target in sorted({value for value in method_refs if value}):
                    if target in {"待匹配", "null", "None"}:
                        continue
                    SafeReader.validate_stable_id(target, "math method reference")
                    relation_type = "wrong_item_has_method_gap_method"
                    output.append({
                        "stable_id": self._relation_id(
                            relation_type, card_id, target, source_hash
                        ),
                        "source_hash": source_hash,
                        "source": card_id,
                        "target": target,
                        "relation_type": relation_type,
                        "relation_authority": "formal_card_method_gap",
                        "data_role": "formal_declared_relation",
                    })
        return output

    def _taxonomy_relations(self) -> list[dict[str, Any]]:
        """Expose only the category-to-label taxonomy declared by 知识点库.md."""

        self._refresh_knowledge_catalog()
        output: list[dict[str, Any]] = []
        categories = {
            (row["namespace"], row["category"]): row
            for row in self._knowledge_categories.values()
        }
        for node in self._knowledge_index.values():
            parent = categories[(node["namespace"], node["category"])]
            relation_type = "category_contains_taxonomy_label"
            output.append({
                "stable_id": self._relation_id(
                    relation_type,
                    str(parent["stable_id"]),
                    str(node["stable_id"]),
                    str(node["source_hash"]),
                ),
                "source_hash": node["source_hash"],
                "source": parent["stable_id"],
                "source_label": parent["category"],
                "target": node["stable_id"],
                "target_label": node["canonical_label"],
                "namespace": node["namespace"],
                "relation_type": relation_type,
                "relation_authority": "formal_taxonomy_section",
                "data_role": "formal_taxonomy_relation",
            })
        return output

    def _all_luna_relations(
        self, *, include_formal: bool = True, include_candidates: bool = True
    ) -> list[dict[str, Any]]:
        output = (
            self._formal_card_relations() + self._taxonomy_relations()
            if include_formal
            else []
        )
        if not include_candidates:
            return sorted(output, key=lambda row: row["stable_id"])
        projection, digest, _ = self.reader.json(self.projection)
        if not isinstance(projection, dict):
            raise StudyReadError(
                "SUBJECT_UNAVAILABLE", "math projection has an invalid schema"
            )
        for role, key in (("projection", "similarities"), ("candidate", "weak_relations")):
            rows = projection.get(key, [])
            if not isinstance(rows, list):
                continue
            for index, edge in enumerate(rows):
                if not isinstance(edge, dict):
                    continue
                source = str(edge.get("a") or "")
                target = str(edge.get("b") or "")
                stable = hashlib.sha256(
                    f"{key}\0{index}\0{source}\0{target}".encode("utf-8")
                ).hexdigest()
                output.append({
                    "stable_id": f"MREL-{stable[:24]}",
                    "source_hash": digest,
                    "source": source,
                    "target": target,
                    "level": edge.get("level"),
                    "score": edge.get("score"),
                    "reasons": edge.get("reasons", []),
                    "relation_source": edge.get("source"),
                    "relation_type": "wrong_item_similarity",
                    "relation_authority": "wrong_questions_projection",
                    "data_role": role,
                })
        return sorted(output, key=lambda row: row["stable_id"])

    @staticmethod
    def _text_match(value: Any, query: str) -> bool:
        return query.casefold() in __import__("json").dumps(
            value, ensure_ascii=False, sort_keys=True
        ).casefold()

    def luna_records(self, request: PagedReadRequest) -> list[dict[str, Any]]:
        """Return a deterministic full-library result set before pagination."""

        collection = request.collection
        allowed = {
            "catalog", "formal_card_catalog", "formal_cards", "formal_card_records",
            "knowledge_catalog", "knowledge_nodes", "math_taxonomy_items",
            "relationships", "formal_relation_declarations",
            "knowledge_relationships", "relationship_candidates", "activity", "search",
        }
        if collection not in allowed:
            raise StudyReadError("INVALID_ARGUMENT", "unsupported math Luna collection")
        self._refresh_card_index()
        self._refresh_knowledge_catalog()
        requested = set(request.ids)
        query = (request.query or "").strip()
        if collection == "search" and not query:
            raise StudyReadError("INVALID_ARGUMENT", "math search requires a query")
        if collection in {
            "catalog", "formal_card_catalog", "formal_cards", "formal_card_records"
        }:
            if requested:
                for value in requested:
                    self._validate_id(value)
            output: list[dict[str, Any]] = []
            for card_id in sorted(self._card_index):
                if requested and card_id not in requested:
                    continue
                if collection in {"catalog", "formal_card_catalog"}:
                    summary, _ = self._formal_card(card_id, [])
                    item = {
                        "stable_id": card_id,
                        "source_hash": summary["sha256"],
                        "title": summary.get("title"),
                        "subject": summary.get("subject"),
                        "chapter": summary.get("chapter"),
                        "question_type": summary.get("question_type"),
                        "status": summary.get("status"),
                        "data_role": "formal_catalog",
                    }
                else:
                    item, _ = self._full_formal_card(card_id)
                if query and not self._text_match(item, query):
                    continue
                output.append(item)
            return output
        if collection in {"knowledge_catalog", "knowledge_nodes", "math_taxonomy_items"}:
            if requested:
                for value in requested:
                    self._validate_knowledge_id(value)
            output = []
            combined_nodes = {
                **self._knowledge_categories,
                **self._knowledge_index,
            }
            for stable_id in sorted(combined_nodes):
                if requested and stable_id not in requested:
                    continue
                item, _ = self._knowledge_node(stable_id)
                if collection == "knowledge_catalog":
                    item = {
                        "stable_id": stable_id,
                        "source_hash": item["source_hash"],
                        "namespace": item.get("namespace"),
                        "category": item.get("category"),
                        "canonical_label": item.get("canonical_label"),
                        "data_role": "formal_knowledge_catalog",
                    }
                if query and not self._text_match(item, query):
                    continue
                output.append(item)
            return output
        if collection in {
            "relationships", "formal_relation_declarations",
            "knowledge_relationships", "relationship_candidates",
        }:
            for value in requested:
                SafeReader.validate_stable_id(value, "math relation endpoint id")
            output = self._all_luna_relations(
                include_formal=collection != "relationship_candidates",
                include_candidates=collection == "relationship_candidates",
            )
            if collection == "knowledge_relationships":
                output = [
                    row for row in output
                    if row.get("relation_type") in {
                        "wrong_item_has_knowledge", "category_contains_taxonomy_label"
                    }
                ]
            if requested:
                output = [
                    row for row in output
                    if requested & {
                        str(row.get("source") or ""),
                        str(row.get("target") or ""),
                        str(row.get("stable_id") or ""),
                    }
                ]
            if query:
                output = [row for row in output if self._text_match(row, query)]
            return output
        if collection == "search":
            combined: list[dict[str, Any]] = []
            for nested in (
                "formal_card_records", "math_taxonomy_items",
                "formal_relation_declarations", "relationship_candidates",
            ):
                nested_request = request.model_copy(
                    update={"collection": nested, "ids": [], "cursor": None, "query": None}
                )
                for row in self.luna_records(nested_request):
                    if self._text_match(row, query):
                        combined.append({"matched_collection": nested, **row})
            return sorted(
                combined,
                key=lambda row: (
                    str(row.get("matched_collection")), str(row.get("stable_id"))
                ),
            )
        if requested:
            for value in requested:
                SafeReader.validate_stable_id(value, "math activity id")
        start = request.study_date or date.min
        end = request.study_date or date.max
        capture_sha, _ = self.reader.digest(self.capture_ledger)
        review_sha, _ = self.reader.digest(self.review_ledger)
        output = []
        for index, item in enumerate(self._activity(start, end, requested or None)):
            stable_id = str(item.get("event_id") or f"activity-{index:08d}")
            item = dict(item)
            item.update({
                "stable_id": stable_id,
                "source_hash": capture_sha if item.get("event_kind") == "capture" else review_sha,
            })
            output.append(item)
        if query:
            output = [row for row in output if self._text_match(row, query)]
        return sorted(output, key=lambda row: row["stable_id"])

    def read_bundle(
        self, queries: list[MathQuery], expected_generation: str | None = None,
        expected_release: str | None = None, expected_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        if not 1 <= len(queries) <= 24:
            raise StudyReadError("INVALID_ARGUMENT", "math query count must be between 1 and 24")
        with self._lock:
            snapshot = self.authority()
            self.bind_expected(snapshot, expected_generation, expected_release, expected_fingerprint)
            tracked = {self.projection, self.capture_ledger, self.review_ledger, self.warmup_ledger}
            before = self.capture_identities(tracked, self.reader)
            output: list[dict[str, Any]] = []
            for query in queries:
                if query.op == "formal_cards":
                    cards = []
                    for value in query.ids:
                        self._refresh_card_index()
                        path = self._card_index.get(self._validate_id(value))
                        if path is None:
                            raise StudyReadError("NOT_FOUND", "formal math card was not found")
                        tracked.add(path)
                        before[path] = self.reader.identity(path)
                        card, path = self._formal_card(value, query.fields)
                        cards.append(card)
                    output.append({"operation": query.op, "items": cards})
                elif query.op == "activity_window":
                    output.append({"operation": query.op, "items": self._activity(query.date_from, query.date_to)})
                elif query.op == "direct_relations":
                    output.append({"operation": query.op, "items": self._relations(query.ids)})
                elif query.op == "review_snapshot":
                    ids = {self._validate_id(x) for x in query.ids}
                    cards = []
                    for value in query.ids:
                        self._refresh_card_index()
                        path = self._card_index.get(self._validate_id(value))
                        if path is None:
                            raise StudyReadError("NOT_FOUND", "formal math card was not found")
                        tracked.add(path)
                        before[path] = self.reader.identity(path)
                        card, path = self._formal_card(value, query.fields)
                        cards.append(card)
                    output.append({
                        "operation": query.op,
                        "items": [{"card": card, "activity": self._activity(date.min, date.max, ids)} for card in cards],
                    })
            before.update({path: self.reader.identity(path) for path in tracked if path not in before})
            self.assert_unchanged(before, self.reader)
            rebound = self.authority()
            if rebound.fingerprint != snapshot.fingerprint:
                raise StudyReadError("SOURCE_CHANGED_DURING_READ", "math authority changed during bundle read", True)
            return success_envelope(
                subject=self.subject, data_role="mixed", authority_source_id=self.authority_source_id,
                generation=snapshot.generation, authority_fingerprint=snapshot.fingerprint, items=output,
                warnings=["wrong_questions.json relations are projection or candidate data, never formal authority"],
            )
