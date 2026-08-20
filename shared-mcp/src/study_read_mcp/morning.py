from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

from .envelope import assert_expected, success_envelope
from .errors import StudyReadError
from .models import CS408MorningPreparationRequest
from .release import SERVER_RELEASE
from .safeio import SafeReader


QUEUE_ITEM = re.compile(
    r"(?m)^#{3,6}\s+(?P<item>(?:MQ|DQ|OQ|AOQ)-[A-Za-z0-9_.:-]+)(?:\s+[^\n]*)?$"
)
FORMAL_ID = re.compile(r"^(?:DS|CO|OS|CN)_(?:\d{4}|UNK)_\d{3}$")
REVIEW_UNIT_ID = re.compile(r"^RU_[A-Za-z0-9_]{3,100}$")


def canonical_scope_hash(
    review_date: str, queue_sha256: str, item_ids: list[str]
) -> str:
    value = {
        "review_date": review_date,
        "queue_sha256": queue_sha256,
        "item_ids": item_ids,
    }
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _field(section: str, name: str) -> str | None:
    match = re.search(rf"(?m)^-\s+{re.escape(name)}:\s*(.+?)\s*$", section)
    if match is None:
        return None
    value = match.group(1).strip()
    if value.startswith("`") and value.endswith("`"):
        value = value[1:-1].strip()
    return value or None


def _table_rows(text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for line in text.splitlines():
        value = line.strip()
        if not (value.startswith("|") and value.endswith("|")):
            continue
        cells = [cell.strip() for cell in value[1:-1].split("|")]
        if cells and not all(re.fullmatch(r":?-{2,}:?", cell) for cell in cells):
            rows.append(cells)
    return rows


class CS408MorningPreparationReader:
    """Resolve only immutable queue-bound evidence for one four-item chunk."""

    authority_source_id = "cs408.morning-preparation-exact-evidence"

    def __init__(self, root: Path) -> None:
        self.reader = SafeReader({"cs408": root})
        self.root = self.reader.roots["cs408"]
        self.dashboard = self.reader.exact(
            "cs408", "wiki", "study_vaults", "408-full", "StudyVault", "00-Dashboard"
        )
        self.input_root = self.reader.exact(
            "cs408", "wiki", "study_vaults", "408-full", "input"
        )
        self.card_root = self.reader.exact("cs408", "复习单元卡")
        self.master = self.reader.exact("cs408", "节点总表.md")
        self.mapping = self.reader.exact("cs408", "复习单元节点映射.md")

    def _queue(self, request: CS408MorningPreparationRequest) -> tuple[Path, str, dict[str, dict[str, str]]]:
        name = f"{request.review_date.isoformat()}-408晨间行动队列.md"
        path = self.reader.exact(
            "cs408", "wiki", "study_vaults", "408-full", "StudyVault", "00-Dashboard", name
        )
        text, digest, _identity = self.reader.text(path)
        if digest != request.queue_sha256:
            raise StudyReadError("HASH_MISMATCH", "morning queue hash does not match expectation", True)
        date_match = re.search(r"(?m)^review_date:\s*(\d{4}-\d{2}-\d{2})\s*$", text)
        if date_match is None or date_match.group(1) != request.review_date.isoformat():
            raise StudyReadError("INVALID_ARGUMENT", "morning queue date is invalid")
        headings = list(QUEUE_ITEM.finditer(text))
        items: dict[str, dict[str, str]] = {}
        for index, heading in enumerate(headings):
            end = headings[index + 1].start() if index + 1 < len(headings) else len(text)
            section = text[heading.start():end]
            item_id = heading.group("item")
            source_id = _field(section, "source_id")
            item_kind = _field(section, "item_kind")
            if not source_id or not item_kind:
                raise StudyReadError("INVALID_ARGUMENT", "morning queue item is incomplete")
            item = {
                "item_id": item_id,
                "source_id": source_id,
                "item_kind": item_kind,
            }
            for field in (
                "action_id", "trigger_event_id", "review_stage", "review_unit_id", "formal_node_id"
            ):
                value = _field(section, field)
                if value:
                    item[field] = value
            items[item_id] = item
        if len(items) != len(headings) or not items:
            raise StudyReadError("INVALID_ARGUMENT", "morning queue item identities are invalid")
        if any(item_id not in items for item_id in request.item_ids):
            raise StudyReadError("NOT_FOUND", "requested morning item is not in the bound queue")
        positions = [list(items).index(item_id) for item_id in request.item_ids]
        if positions != list(range(positions[0], positions[0] + len(positions))):
            raise StudyReadError("INVALID_ARGUMENT", "morning chunk item_ids must be contiguous and ordered")
        return path, text, items

    def _indexes(self) -> tuple[dict[str, str], dict[str, str], dict[str, str], dict[Path, str]]:
        tracked: dict[Path, str] = {}
        master_text, master_sha, _ = self.reader.text(self.master)
        mapping_text, mapping_sha, _ = self.reader.text(self.mapping)
        tracked[self.master] = master_sha
        tracked[self.mapping] = mapping_sha

        source_to_formal: dict[str, str] = {}
        for cells in _table_rows(master_text):
            if len(cells) >= 2 and FORMAL_ID.fullmatch(cells[0]):
                source_to_formal.setdefault(cells[1], cells[0])

        formal_to_unit: dict[str, str] = {}
        unit_to_formal: dict[str, str] = {}
        for cells in _table_rows(mapping_text):
            if len(cells) < 5 or not FORMAL_ID.fullmatch(cells[0]):
                continue
            unit = cells[4]
            if not REVIEW_UNIT_ID.fullmatch(unit):
                continue
            formal_to_unit[cells[0]] = unit
            unit_to_formal.setdefault(unit, cells[0])
        return source_to_formal, formal_to_unit, unit_to_formal, tracked

    def _read_evidence(self, path: Path, role: str) -> dict[str, Any]:
        path = self.reader.validate_indexed_path("cs408", path)
        text, digest, _identity = self.reader.text(path)
        relative = path.relative_to(self.root).as_posix()
        return {
            "role": role,
            "source_ref": relative,
            "sha256": digest,
            "content": text,
        }

    def read(
        self,
        request: CS408MorningPreparationRequest,
        *,
        route: dict[str, Any],
        expected_generation: str | None = None,
        expected_release: str | None = None,
        expected_fingerprint: str | None = None,
    ) -> dict[str, Any]:
        queue_path, queue_text, queue_items = self._queue(request)
        source_to_formal, formal_to_unit, unit_to_formal, tracked = self._indexes()
        queue_sha, _ = self.reader.digest(queue_path)
        tracked[queue_path] = queue_sha
        output: list[dict[str, Any]] = []

        for item_id in request.item_ids:
            item = dict(queue_items[item_id])
            source_id = item["source_id"]
            formal = item.get("formal_node_id") or source_to_formal.get(source_id)
            unit = item.get("review_unit_id")
            if unit is None and source_id.startswith("RU_"):
                unit = source_id
            if formal is None and unit is not None:
                formal = unit_to_formal.get(unit)
            if unit is None and formal is not None:
                unit = formal_to_unit.get(formal)

            evidence: list[dict[str, Any]] = []
            if formal is not None:
                if not FORMAL_ID.fullmatch(formal):
                    raise StudyReadError("INVALID_ARGUMENT", "formal node identity is invalid")
                formal_path = self.reader.exact("cs408", f"{formal}.md", must_exist=False)
                if formal_path.exists():
                    evidence.append(self._read_evidence(formal_path, "formal_node_mechanism"))
            if unit is not None:
                if not REVIEW_UNIT_ID.fullmatch(unit):
                    raise StudyReadError("INVALID_ARGUMENT", "review unit identity is invalid")
                card_path = self.reader.exact("cs408", "复习单元卡", f"{unit}.md", must_exist=False)
                if card_path.exists():
                    evidence.append(self._read_evidence(card_path, "review_unit_boundary"))
            anchor = re.fullmatch(r"([^/#]+\.md)#Q\d+", source_id)
            if anchor is not None:
                anchored = self.reader.exact(
                    "cs408", "wiki", "study_vaults", "408-full", "input", anchor.group(1), must_exist=False
                )
                if anchored.exists():
                    evidence.append(self._read_evidence(anchored, "immutable_safe_queue_source"))
            if not evidence:
                raise StudyReadError("NOT_FOUND", f"no exact evidence resolved for {item_id}")
            output.append({**item, "resolved_formal_node_id": formal, "resolved_review_unit_id": unit, "evidence": evidence})

        bindings: list[tuple[str, str]] = [(path.relative_to(self.root).as_posix(), digest) for path, digest in tracked.items()]
        for row in output:
            for evidence in row["evidence"]:
                bindings.append((evidence["source_ref"], evidence["sha256"]))
        fingerprint = hashlib.sha256(
            "\n".join(f"{name}:{digest}" for name, digest in sorted(set(bindings))).encode("utf-8")
        ).hexdigest()
        generation = f"cs408-morning-{fingerprint[:20]}"
        assert_expected(
            generation=generation,
            release=SERVER_RELEASE,
            fingerprint=fingerprint,
            expected_generation=expected_generation,
            expected_release=expected_release,
            expected_fingerprint=expected_fingerprint,
        )
        result = success_envelope(
            subject="cs408",
            data_role="protected_morning_preparation_evidence",
            authority_source_id=self.authority_source_id,
            generation=generation,
            authority_fingerprint=fingerprint,
            items=output,
            route_context=route,
            profile="morning_preparation",
            output_limit=524_288,
            warnings=["Protected evidence is for isolated pack authoring and verification only"],
        )
        result["queue_sha256"] = request.queue_sha256
        result["queue_ref"] = queue_path.relative_to(self.root).as_posix()
        result["evidence_scope_hash"] = canonical_scope_hash(
            request.review_date.isoformat(), request.queue_sha256, request.item_ids
        )
        result["mcp_tool_call_count"] = 1
        result["learner_evidence_write_count"] = 0
        return result
