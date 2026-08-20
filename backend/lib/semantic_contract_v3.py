"""Deterministic semantic contracts shared by study intake subjects.

The helpers in this module are pure or read-only.  They never call a model and
never write a formal study repository.  They turn frozen source material into
content-addressed evidence, coverage, atomic-signal and relationship views.
"""

from __future__ import annotations

import ast
import copy
import hashlib
import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
FORMAL_408_ID_RE = re.compile(r"^(?:DS|CO|OS|CN)_(?:\d{4}|UNK)_\d{3}$")
KNOWLEDGE_408_ID_RE = re.compile(r"^(?:DS|CO|OS|CN)\d{2}-\d{2}$")
ATOMIC_SIGNAL_TYPES = frozenset(
    {"knowledge", "method", "error", "trap", "question_type"}
)
ATOMIC_IMPORTANCE = frozenset({"primary", "secondary"})
ATOMIC_ERROR_ROLES = frozenset(
    {"first_error", "later_error", "historical_error", "current_error", "none"}
)
TRUTH_MATCH_STATUSES = frozenset({"exact", "alias", "related", "unmatched"})
CORRECTION_RESOLUTIONS = frozenset({"applied", "not_applicable", "unresolved"})
CORRECTION_DELTA_ENCODING = "canonical_json"
CORRECTION_JSON_PATH_RE = re.compile(
    r"^\$(?:(?:\.[A-Za-z_][A-Za-z0-9_]*)|(?:\[[0-9]+\]))*$"
)


class SemanticContractError(ValueError):
    """A deterministic semantic or evidence contract failed closed."""


def canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def sha256_value(value: object) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def encode_correction_delta(value: object) -> dict[str, str]:
    """Encode an arbitrary JSON value inside a strict structured-output object."""

    return {
        "encoding": CORRECTION_DELTA_ENCODING,
        "canonical_json": json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ),
    }


def decode_correction_delta(value: object) -> object:
    """Decode the strict wrapper while preserving read-only legacy values."""

    if not isinstance(value, Mapping) or not (
        "encoding" in value or "canonical_json" in value
    ):
        return value
    if (
        set(value) != {"encoding", "canonical_json"}
        or value.get("encoding") != CORRECTION_DELTA_ENCODING
        or not isinstance(value.get("canonical_json"), str)
        or not value["canonical_json"]
    ):
        raise SemanticContractError("correction_delta_encoding_invalid")
    try:
        decoded = json.loads(str(value["canonical_json"]))
    except json.JSONDecodeError as exc:
        raise SemanticContractError("correction_delta_json_invalid") from exc
    if encode_correction_delta(decoded) != dict(value):
        raise SemanticContractError("correction_delta_json_not_canonical")
    return decoded


def normalize_text(value: object) -> str:
    return " ".join(unicodedata.normalize("NFC", str(value or "")).split())


def build_authority_key(
    *,
    subject: str,
    capture_id: str,
    input_fingerprint: str,
    semantic_contract_sha256: str,
    release_id: str,
    generation: int,
) -> dict[str, Any]:
    if subject not in {"math", "cs408", "english"}:
        raise SemanticContractError("authority_subject_invalid")
    if not capture_id or generation < 1:
        raise SemanticContractError("authority_identity_invalid")
    for label, value in (
        ("input_fingerprint", input_fingerprint),
        ("semantic_contract_sha256", semantic_contract_sha256),
        ("release_id", release_id),
    ):
        if not SHA256_RE.fullmatch(value):
            raise SemanticContractError(f"authority_{label}_invalid")
    core = {
        "schema_version": "study-intake-authority-key-v1",
        "subject": subject,
        "capture_id": capture_id,
        "input_fingerprint": input_fingerprint,
        "semantic_contract_sha256": semantic_contract_sha256,
        "release_id": release_id,
        "generation": generation,
    }
    return {**core, "authority_key_sha256": sha256_value(core)}


def build_evidence_manifest(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, record in enumerate(records):
        evidence_id = str(record.get("evidence_id") or f"evidence-{index + 1:04d}")
        digest = str(record.get("sha256") or "")
        size = record.get("bytes", record.get("size"))
        if "data" in record:
            data = record["data"]
            raw = data if isinstance(data, bytes) else str(data).encode("utf-8")
            derived_digest = hashlib.sha256(raw).hexdigest()
            derived_size = len(raw)
            if digest and digest != derived_digest:
                raise SemanticContractError("evidence_manifest_data_hash_mismatch")
            if size is not None and size != derived_size:
                raise SemanticContractError("evidence_manifest_data_size_mismatch")
            digest = derived_digest
            size = derived_size
        sequence = record.get("sequence", index + 1)
        if (
            evidence_id in seen
            or not SHA256_RE.fullmatch(digest)
            or isinstance(size, bool)
            or not isinstance(size, int)
            or size < 0
            or isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or sequence < 1
        ):
            raise SemanticContractError("evidence_manifest_record_invalid")
        seen.add(evidence_id)
        normalized.append(
            {
                "evidence_id": evidence_id,
                "kind": str(record.get("kind") or record.get("role") or "unknown"),
                "source": str(record.get("source") or record.get("source_kind") or "unknown"),
                "role": str(record.get("role") or "supporting"),
                "sequence": sequence,
                "bytes": size,
                "sha256": digest,
                "evidence_ref": str(record.get("evidence_ref") or evidence_id),
            }
        )
    normalized.sort(key=lambda row: (int(row["sequence"]), str(row["evidence_id"])))
    if [row["sequence"] for row in normalized] != sorted(
        row["sequence"] for row in normalized
    ):
        raise SemanticContractError("evidence_manifest_order_invalid")
    core = {
        "schema_version": "study-intake-evidence-manifest-v1",
        "objects": normalized,
        "object_count": len(normalized),
        "total_bytes": sum(int(row["bytes"]) for row in normalized),
    }
    return {**core, "evidence_manifest_sha256": sha256_value(core)}


def build_coverage_manifest(
    sources: Sequence[Mapping[str, Any]], *, scope: str
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for source in sources:
        total = source.get("total_entries")
        scanned = source.get("scanned_entries")
        matched = source.get("matched_entries")
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in (total, scanned, matched)):
            raise SemanticContractError("coverage_counts_invalid")
        if scanned > total or matched > scanned:
            raise SemanticContractError("coverage_counts_inconsistent")
        complete = bool(source.get("complete"))
        reason = str(source.get("truncation_reason") or "")
        if complete != (scanned == total) or (not complete and not reason):
            raise SemanticContractError("coverage_completeness_invalid")
        digest = str(source.get("sha256") or "")
        if not SHA256_RE.fullmatch(digest):
            raise SemanticContractError("coverage_source_hash_invalid")
        rows.append(
            {
                "source": str(source.get("source") or source.get("kind") or "unknown"),
                "sha256": digest,
                "bytes": int(source.get("bytes", source.get("size", 0))),
                "total_entries": total,
                "scanned_entries": scanned,
                "matched_entries": matched,
                "complete": complete,
                "truncation_reason": reason or None,
            }
        )
    rows.sort(key=lambda row: str(row["source"]))
    core = {
        "schema_version": "study-intake-coverage-manifest-v1",
        "scope": scope,
        "sources": rows,
        "complete": all(bool(row["complete"]) for row in rows),
    }
    return {**core, "coverage_manifest_sha256": sha256_value(core)}


def validate_atomic_signals(
    signals: Sequence[Mapping[str, Any]], *, allowed_evidence_refs: Iterable[str]
) -> list[dict[str, Any]]:
    allowed = set(allowed_evidence_refs)
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, signal in enumerate(signals):
        signal_id = str(signal.get("signal_id") or f"SIG-{index + 1:04d}")
        signal_type = str(signal.get("signal_type") or "")
        importance = str(signal.get("importance") or signal.get("priority") or "")
        error_role = str(signal.get("error_role") or signal.get("role") or "none")
        refs = signal.get("evidence_refs")
        confidence = str(signal.get("confidence") or "")
        status = str(signal.get("truth_library_match_status") or "unmatched")
        if (
            signal_id in seen
            or signal_type not in ATOMIC_SIGNAL_TYPES
            or importance not in ATOMIC_IMPORTANCE
            or error_role not in ATOMIC_ERROR_ROLES
            or confidence not in {"low", "medium", "high"}
            or status not in TRUTH_MATCH_STATUSES
            or not isinstance(refs, list)
            or not refs
            or any(not isinstance(ref, str) or ref not in allowed for ref in refs)
        ):
            raise SemanticContractError("atomic_signal_invalid")
        canonical_term = normalize_text(signal.get("canonical_term"))
        surface_form = normalize_text(signal.get("surface_form"))
        provenance = normalize_text(signal.get("provenance"))
        specificity = normalize_text(signal.get("specificity"))
        boundary = normalize_text(signal.get("applicability_boundary"))
        if not all((canonical_term, surface_form, provenance, specificity, boundary)):
            raise SemanticContractError("atomic_signal_content_missing")
        seen.add(signal_id)
        normalized.append(
            {
                "signal_id": signal_id,
                "signal_type": signal_type,
                "canonical_term": canonical_term,
                "surface_form": surface_form,
                "importance": importance,
                "provenance": provenance,
                "evidence_refs": list(refs),
                "confidence": confidence,
                "error_role": error_role,
                "specificity": specificity,
                "applicability_boundary": boundary,
                "truth_library_match_status": status,
            }
        )
    normalized.sort(key=lambda row: str(row["signal_id"]))
    return normalized


def validate_signal_resolution(
    signals: Sequence[Mapping[str, Any]],
    matrix: Sequence[Mapping[str, Any]],
    novelty: Sequence[Mapping[str, Any]],
    unmatched: Sequence[Mapping[str, Any]],
) -> None:
    signal_ids = {str(row.get("signal_id") or "") for row in signals}
    outcomes: dict[str, int] = {signal_id: 0 for signal_id in signal_ids}
    for rows, result_kind in (
        (matrix, "matched"),
        (novelty, "novelty"),
        (unmatched, "needs_user_decision"),
    ):
        for row in rows:
            signal_id = str(row.get("signal_id") or "")
            if signal_id not in outcomes:
                raise SemanticContractError("signal_resolution_unknown_signal")
            if result_kind == "matched" and row.get("match_status") not in {
                "exact", "alias", "related"
            }:
                raise SemanticContractError("signal_match_status_invalid")
            outcomes[signal_id] += 1
    if any(count != 1 for count in outcomes.values()):
        raise SemanticContractError("signal_resolution_not_total")


def validate_correction_resolutions(
    findings: Sequence[Mapping[str, Any]],
    resolutions: Sequence[Mapping[str, Any]],
) -> None:
    finding_id_values = [
        str(
            row.get("finding_id")
            or row.get("required_correction_id")
            or row.get("correction_id")
            or ""
        )
        for row in findings
    ]
    all_ids = set(finding_id_values)
    required_ids = {
        finding_id
        for finding_id, row in zip(finding_id_values, findings)
        if row.get("severity") in {"error", "blocking"}
    }
    if "" in all_ids:
        raise SemanticContractError("correction_finding_id_missing")
    if len(all_ids) != len(finding_id_values):
        raise SemanticContractError("correction_finding_id_duplicate")
    by_id: dict[str, Mapping[str, Any]] = {}
    for resolution in resolutions:
        finding_id = str(resolution.get("finding_id") or "")
        if finding_id in by_id or finding_id not in all_ids:
            raise SemanticContractError("correction_resolution_binding_invalid")
        status = str(resolution.get("resolution") or "")
        paths = resolution.get("affected_json_paths")
        if (
            status not in CORRECTION_RESOLUTIONS
            or not isinstance(paths, list)
            or not paths
            or any(
                not isinstance(path, str)
                or not CORRECTION_JSON_PATH_RE.fullmatch(path)
                for path in paths
            )
        ):
            raise SemanticContractError("correction_resolution_invalid")
        if status == "applied" and (
            "before" not in resolution or "after" not in resolution
        ):
            raise SemanticContractError("correction_delta_missing")
        for field in ("before", "after"):
            if field in resolution:
                decode_correction_delta(resolution[field])
        by_id[finding_id] = resolution
    if not required_ids.issubset(by_id) or any(
        by_id[finding_id].get("resolution") == "unresolved"
        for finding_id in required_ids
    ):
        raise SemanticContractError("required_correction_unresolved")


def validate_applied_correction_targets(
    document: Mapping[str, Any],
    resolutions: Sequence[Mapping[str, Any]],
) -> None:
    def resolve(path: str) -> Any:
        if not CORRECTION_JSON_PATH_RE.fullmatch(path):
            raise SemanticContractError("correction_target_path_invalid")
        if path == "$":
            return document
        current: Any = document
        for name, raw_index in re.findall(r"(?:^|\.)([^.\[\]]+)|\[(\d+)\]", path[1:]):
            if name:
                if not isinstance(current, Mapping) or name not in current:
                    raise SemanticContractError("correction_target_path_missing")
                current = current[name]
            else:
                index = int(raw_index)
                if not isinstance(current, list) or index >= len(current):
                    raise SemanticContractError("correction_target_path_missing")
                current = current[index]
        return current

    for resolution in resolutions:
        if resolution.get("resolution") != "applied":
            continue
        paths = list(resolution.get("affected_json_paths") or [])
        after = decode_correction_delta(resolution.get("after"))
        for path in paths:
            actual = resolve(str(path))
            expected = (
                after.get(path)
                if isinstance(after, Mapping) and path in after
                else after
                if len(paths) == 1
                else None
            )
            if expected != actual:
                raise SemanticContractError("correction_delta_not_applied")


def _markdown_rows(text: str) -> tuple[list[dict[str, str]], int]:
    header: list[str] | None = None
    rows: list[dict[str, str]] = []
    scanned = 0
    for raw in text.splitlines():
        line = raw.strip()
        if not line.startswith("|") or not line.endswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if not cells or all(re.fullmatch(r":?-{3,}:?", cell or "") for cell in cells):
            continue
        scanned += 1
        if header is None:
            header = cells
            continue
        if len(cells) == len(header):
            rows.append(dict(zip(header, cells)))
    return rows, scanned


def _read_source(repo: Path, relative: str, maximum: int) -> tuple[str, dict[str, Any]]:
    path = (repo / relative).resolve()
    try:
        path.relative_to(repo.resolve())
    except ValueError as exc:
        raise SemanticContractError("source_outside_repo") from exc
    raw = path.read_bytes()
    if not raw or len(raw) > maximum:
        raise SemanticContractError("source_size_invalid")
    try:
        text = raw.decode("utf-8")
    except UnicodeError as exc:
        raise SemanticContractError("source_decode_invalid") from exc
    return text, {
        "relative_path": relative,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }


def _knowledge_ids(value: object) -> list[str]:
    return sorted(set(re.findall(r"(?:DS|CO|OS|CN)\d{2}-\d{2}", str(value or ""))))


def build_cs408_exact_snapshot(
    repo: Path,
    *,
    source_id: str,
    public_text: str,
    source_paths: Mapping[str, str],
    maximum_source_bytes: int,
    context_limits: Mapping[str, int],
    controlled_aliases: Mapping[str, Sequence[str]] | None = None,
) -> dict[str, Any]:
    required = {"taxonomy", "nodes", "relations", "review_records", "review_unit_mapping"}
    if set(source_paths) != required:
        raise SemanticContractError("cs408_source_set_invalid")
    texts: dict[str, str] = {}
    meta: dict[str, dict[str, Any]] = {}
    parsed: dict[str, list[dict[str, str]]] = {}
    scanned_counts: dict[str, int] = {}
    controlled_aliases = controlled_aliases or {}
    for kind in sorted(required):
        text, row = _read_source(repo, source_paths[kind], maximum_source_bytes)
        rows, scanned = _markdown_rows(text)
        texts[kind] = text
        row["source_kind"] = kind
        meta[kind] = row
        parsed[kind] = rows
        scanned_counts[kind] = scanned

    mapping_rows = parsed["review_unit_mapping"]
    resolved_formal_ids: set[str] = set()
    if FORMAL_408_ID_RE.fullmatch(source_id):
        resolved_formal_ids.add(source_id)
    if source_id.startswith("RU_"):
        for row in mapping_rows:
            if source_id in {row.get("主复习单元ID"), row.get("次复习单元ID")}:
                formal_id = str(row.get("正式节点ID") or "")
                if FORMAL_408_ID_RE.fullmatch(formal_id):
                    resolved_formal_ids.add(formal_id)

    node_rows = parsed["nodes"]
    resolved_nodes: list[dict[str, Any]] = []
    resolved_knowledge: set[str] = set()
    for row in node_rows:
        formal_id = str(row.get("ID") or "")
        if formal_id not in resolved_formal_ids:
            continue
        main = _knowledge_ids(row.get("主知识点"))
        secondary = _knowledge_ids(row.get("副知识点"))
        hit = _knowledge_ids(row.get("命中知识点"))
        resolved_knowledge.update(main + secondary + hit)
        resolved_nodes.append(
            {
                "formal_id": formal_id,
                "main_knowledge_ids": main,
                "secondary_knowledge_ids": secondary,
                "hit_knowledge_ids": hit,
                "main_knowledge": normalize_text(row.get("主知识点")),
                "core_focus": normalize_text(row.get("核心考点")),
                "error_tags": [
                    value.strip()
                    for value in re.split(r"[；;]", str(row.get("错因标签") or ""))
                    if value.strip()
                ],
                "latest_error_summary": normalize_text(row.get("最近错误记录")),
            }
        )

    query = re.sub(r"[^0-9a-z\u3400-\u9fff]+", "", f"{source_id} {public_text}".lower())
    taxonomy_rows: list[dict[str, Any]] = []
    chapter_code = ""
    chapter_name = ""
    for line in texts["taxonomy"].splitlines():
        heading = re.match(r"^##\s+((?:DS|CO|OS|CN)\d{2})\s+(.+?)\s*$", line)
        if heading:
            chapter_code, chapter_name = heading.group(1), heading.group(2).strip()
            continue
        match = re.match(r"^-\s+((?:DS|CO|OS|CN)\d{2}-\d{2})\s+(.+?)\s*$", line)
        if not match:
            continue
        knowledge_id, name = match.group(1), match.group(2).strip()
        normalized_name = re.sub(r"[^0-9a-z\u3400-\u9fff]+", "", name.lower())
        exact_identity = knowledge_id in resolved_knowledge
        exact_term = bool(normalized_name and normalized_name in query)
        alias_knowledge_ids = {
            knowledge
            for alias, knowledge_ids in controlled_aliases.items()
            if (
                str(alias).upper() in source_id.upper()
                or (
                    (normalized_alias := re.sub(
                        r"[^0-9a-z\u3400-\u9fff]+",
                        "",
                        str(alias).lower(),
                    ))
                    and normalized_alias in query
                )
            )
            for knowledge in knowledge_ids
        }
        exact_alias = knowledge_id in alias_knowledge_ids
        if not (exact_identity or exact_term or exact_alias):
            continue
        taxonomy_rows.append(
            {
                "knowledge_id": knowledge_id,
                "knowledge_name": name,
                "chapter_code": chapter_code,
                "chapter_name": chapter_name,
                "match_status": "exact" if exact_identity or exact_term else "alias",
                "match_basis": (
                    "resolved_formal_node" if exact_identity else "public_signal" if exact_term else "controlled_alias"
                ),
                "_deterministic_rank": (
                    0 if exact_identity and exact_term else
                    1 if exact_identity else
                    2 if exact_term else 3
                ),
            }
        )
        resolved_knowledge.add(knowledge_id)
    taxonomy_rows.sort(
        key=lambda row: (
            int(row["_deterministic_rank"]),
            str(row["knowledge_id"]),
        )
    )
    for row in taxonomy_rows:
        row.pop("_deterministic_rank", None)
    taxonomy_position = {
        str(row["knowledge_id"]): index
        for index, row in enumerate(taxonomy_rows)
    }
    resolved_nodes.sort(
        key=lambda row: (
            min(
                (
                    taxonomy_position.get(str(knowledge_id), len(taxonomy_position) + 1)
                    for knowledge_id in [
                        *(row.get("main_knowledge_ids") or []),
                        *(row.get("secondary_knowledge_ids") or []),
                        *(row.get("hit_knowledge_ids") or []),
                    ]
                ),
                default=len(taxonomy_position) + 1,
            ),
            str(row.get("formal_id") or ""),
        )
    )

    direct_edges: list[dict[str, Any]] = []
    for row in parsed["relations"]:
        start = str(row.get("起点ID") or "")
        end = str(row.get("终点ID") or "")
        if start not in resolved_formal_ids and end not in resolved_formal_ids:
            continue
        direct_edges.append(
            {
                "start_id": start,
                "end_id": end,
                "relationship_type": normalize_text(row.get("关系类型")),
                "strength": normalize_text(row.get("联系强度")),
                "reason": normalize_text(row.get("关联原因")),
                "source_edge_status": "existing_formal_edge",
                "current_capture_action_status": "proposal_only",
            }
        )
    direct_edges.sort(key=lambda row: (str(row["start_id"]), str(row["end_id"]), str(row["relationship_type"])))

    timeline = [
        {
            "review_date": normalize_text(row.get("日期")),
            "formal_id": normalize_text(row.get("ID")),
            "source_id": normalize_text(row.get("来源ID")),
            "action": normalize_text(row.get("动作")),
            "error_summary": normalize_text(row.get("错误记录")),
        }
        for row in parsed["review_records"]
        if str(row.get("ID") or "") in resolved_formal_ids
    ]
    timeline.sort(key=lambda row: (str(row["review_date"]), str(row["formal_id"])))

    # A review unit may resolve to more than one formal node.  Preserve the
    # relevance order already established above instead of flattening every
    # bound node's main knowledge into one undifferentiated primary set.  The
    # first row is the current-question primary identity; the remaining rows
    # still participate in history and complete direct-edge coverage.
    primary_node = resolved_nodes[0] if resolved_nodes else None
    primary_formal_node_ids = (
        [str(primary_node.get("formal_id") or "")]
        if isinstance(primary_node, Mapping)
        and str(primary_node.get("formal_id") or "")
        else []
    )
    secondary_formal_node_ids = [
        str(row.get("formal_id") or "")
        for row in resolved_nodes[1:]
        if str(row.get("formal_id") or "")
    ]
    primary_knowledge_ids = [
        str(knowledge_id)
        for knowledge_id in (
            (primary_node.get("main_knowledge_ids") or [])
            if isinstance(primary_node, Mapping)
            else []
        )
        if str(knowledge_id)
    ]
    if not primary_knowledge_ids and isinstance(primary_node, Mapping):
        primary_knowledge_ids = [
            str(knowledge_id)
            for knowledge_id in primary_node.get("hit_knowledge_ids") or []
            if str(knowledge_id)
        ][:1]

    def ordered_knowledge_ids(values: Iterable[object]) -> list[str]:
        unique = {str(value) for value in values if str(value)}
        return sorted(
            unique,
            key=lambda knowledge_id: (
                taxonomy_position.get(knowledge_id, len(taxonomy_position) + 1),
                knowledge_id,
            ),
        )

    secondary_knowledge_ids = ordered_knowledge_ids(
        knowledge_id
        for row in resolved_nodes
        for knowledge_id in [
            *(row.get("main_knowledge_ids") or []),
            *(row.get("secondary_knowledge_ids") or []),
        ]
        if str(knowledge_id) not in set(primary_knowledge_ids)
    )
    hit_knowledge_ids = ordered_knowledge_ids(
        knowledge_id
        for row in resolved_nodes
        for knowledge_id in row.get("hit_knowledge_ids") or []
    )

    matched_counts = {
        "taxonomy": len(taxonomy_rows),
        "nodes": len(resolved_nodes),
        "relations": len(direct_edges),
        "review_records": len(timeline),
        "review_unit_mapping": sum(
            1
            for row in mapping_rows
            if source_id in {row.get("主复习单元ID"), row.get("次复习单元ID")}
        ),
    }
    total_counts = {kind: len(parsed[kind]) for kind in required}
    total_counts["taxonomy"] = len(
        re.findall(
            r"^-\s+(?:DS|CO|OS|CN)\d{2}-\d{2}\s+.+$",
            texts["taxonomy"],
            re.MULTILINE,
        )
    )
    coverage = build_coverage_manifest(
        [
            {
                "source": kind,
                "sha256": meta[kind]["sha256"],
                "bytes": meta[kind]["bytes"],
                "total_entries": total_counts[kind],
                "scanned_entries": total_counts[kind],
                "matched_entries": matched_counts[kind],
                "complete": True,
            }
            for kind in sorted(required)
        ],
        scope="cs408_relevant_network",
    )
    taxonomy_limit = int(context_limits.get("taxonomy", len(taxonomy_rows)))
    history_limit = int(context_limits.get("history", len(resolved_nodes)))
    relation_limit = int(context_limits.get("relations", len(direct_edges)))
    timeline_limit = int(context_limits.get("timeline", len(timeline)))
    core = {
        "schema_version": "study-intake-408-knowledge-snapshot-v2",
        "snapshot_scope": "complete_scan_deterministic_context",
        "source_identity": {
            "source_id": source_id,
            "resolved_formal_node_ids": sorted(resolved_formal_ids),
            "resolved_knowledge_ids": sorted(resolved_knowledge),
            "primary_formal_node_ids": primary_formal_node_ids,
            "secondary_formal_node_ids": secondary_formal_node_ids,
            "primary_knowledge_ids": primary_knowledge_ids,
            "secondary_knowledge_ids": secondary_knowledge_ids,
            "hit_knowledge_ids": hit_knowledge_ids,
        },
        "sources": meta,
        "coverage_manifest": coverage,
        "taxonomy_candidates": taxonomy_rows[:taxonomy_limit],
        "historical_wrong_candidates": resolved_nodes[:history_limit],
        "historical_review_timeline": timeline[-timeline_limit:],
        "relationship_candidates": direct_edges[:relation_limit],
        "related_formal_questions": sorted(
            {
                edge["end_id"] if edge["start_id"] in resolved_formal_ids else edge["start_id"]
                for edge in direct_edges
            }
        ),
        "existing_formal_edges": direct_edges,
        "context_truncation": {
            "taxonomy": max(0, len(taxonomy_rows) - taxonomy_limit),
            "history": max(0, len(resolved_nodes) - history_limit),
            "relations": max(0, len(direct_edges) - relation_limit),
            "timeline": max(0, len(timeline) - timeline_limit),
        },
        "formal_write_count": 0,
    }
    return {**core, "snapshot_payload_sha256": sha256_value(core)}


def parse_mermaid_graph(text: str) -> dict[str, Any]:
    nodes: dict[str, str] = {}
    edges: list[dict[str, str]] = []
    edge_re = re.compile(
        r"^\s*([A-Za-z0-9_:-]+)\s*(-->|---|-.->|==>)\s*([A-Za-z0-9_:-]+)"
    )
    node_re = re.compile(r'^\s*([A-Za-z0-9_:-]+)\s*[\[({]{1,2}["\']?(.+?)["\']?[\])}]{1,2}\s*$')
    for raw in text.splitlines():
        edge = edge_re.search(raw)
        if edge:
            edges.append({"start_id": edge.group(1), "end_id": edge.group(3), "operator": edge.group(2)})
            continue
        node = node_re.search(raw)
        if node:
            nodes[node.group(1)] = normalize_text(node.group(2))
    edges.sort(key=lambda row: (row["start_id"], row["end_id"], row["operator"]))
    return {"nodes": nodes, "edges": edges}


def relevant_mermaid_neighborhood(
    graph: Mapping[str, Any], *, seeds: Iterable[str]
) -> dict[str, Any]:
    labels = graph.get("nodes") if isinstance(graph.get("nodes"), Mapping) else {}

    def canonical_id(value: object) -> str:
        return re.sub(r"[^A-Za-z0-9]+", "", str(value or "")).upper()

    requested_seeds = {str(seed) for seed in seeds if seed}
    requested_canonical = {canonical_id(seed) for seed in requested_seeds}
    seed_set = {
        str(node_id)
        for node_id, label in labels.items()
        if canonical_id(node_id) in requested_canonical
        or any(
            canonical_id(token) in requested_canonical
            for token in re.findall(r"[A-Za-z]{1,8}[-_ ]?\d{1,6}", str(label))
        )
    }
    seed_set.update(
        seed for seed in requested_seeds if seed in labels
    )
    edges = [
        copy.deepcopy(row)
        for row in graph.get("edges", [])
        if row.get("start_id") in seed_set or row.get("end_id") in seed_set
    ]
    node_ids = seed_set | {
        str(value)
        for edge in edges
        for value in (edge.get("start_id"), edge.get("end_id"))
        if value
    }
    return {
        "requested_seed_ids": sorted(requested_seeds),
        "seed_node_ids": sorted(seed_set),
        "nodes": [
            {"node_id": node_id, "label": normalize_text(labels.get(node_id))}
            for node_id in sorted(node_ids)
        ],
        "direct_edges": edges,
    }


def source_transitive_closure_manifest(
    source_path: Path, *, root_selectors: Sequence[str], subject: str
) -> dict[str, Any]:
    source = source_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(source_path))
    definitions: dict[str, ast.AST] = {}
    methods: dict[str, ast.AST] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            definitions[node.name] = node
        if isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    methods[f"{node.name}.{child.name}"] = child
    queue = list(root_selectors)
    selected: dict[str, ast.AST] = {}
    while queue:
        selector = queue.pop(0)
        if selector in selected:
            continue
        node = methods.get(selector) or definitions.get(selector)
        if node is None:
            raise SemanticContractError("semantic_closure_root_missing")
        selected[selector] = node
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                if isinstance(child.func, ast.Name) and child.func.id in definitions:
                    queue.append(child.func.id)
                elif (
                    isinstance(child.func, ast.Attribute)
                    and isinstance(child.func.value, ast.Name)
                    and child.func.value.id == "self"
                ):
                    owner = selector.split(".", 1)[0] if "." in selector else ""
                    qualified = f"{owner}.{child.func.attr}" if owner else ""
                    if qualified in methods:
                        queue.append(qualified)
    components: dict[str, str] = {}
    for selector, node in sorted(selected.items()):
        segment = ast.get_source_segment(source, node)
        if not segment:
            raise SemanticContractError("semantic_closure_component_unreadable")
        components[selector] = hashlib.sha256(segment.encode("utf-8")).hexdigest()
    core = {
        "schema_version": "study-intake-subject-transitive-code-closure-v1",
        "subject": subject,
        "components": components,
    }
    return {**core, "code_closure_sha256": sha256_value(core)}


def source_file_set_manifest(
    root: Path, relative_paths: Sequence[str]
) -> dict[str, Any]:
    resolved_root = root.resolve()
    rows: list[dict[str, Any]] = []
    for relative in sorted(set(relative_paths)):
        path = (resolved_root / relative).resolve()
        try:
            path.relative_to(resolved_root)
        except ValueError as exc:
            raise SemanticContractError("semantic_source_outside_root") from exc
        if not path.is_file():
            raise SemanticContractError("semantic_source_missing")
        rows.append(
            {
                "relative_path": relative,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    core = {
        "schema_version": "study-intake-semantic-source-file-set-v1",
        "files": rows,
        "file_count": len(rows),
    }
    return {**core, "file_set_sha256": sha256_value(core)}
