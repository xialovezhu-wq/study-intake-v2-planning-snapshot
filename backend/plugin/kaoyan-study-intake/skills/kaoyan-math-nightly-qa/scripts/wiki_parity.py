#!/usr/bin/env python3
"""Read-only identity and target-freshness checks for the math LLM Wiki."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path


CARD_ID_RE = re.compile(r"^(GS|LA|PR)-\d{3,}$")
ROW_ID_RE = re.compile(r"^\|\s*((?:GS|LA|PR)-\d{3,})\s*\|")
REF_RE = re.compile(r"MATHWIKI-(?:KNOWLEDGE-\d+|METHOD-CLUSTER-\d+|ERROR-CLUSTER-\d+|ACTION-GAP-\d+)")


def canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def frontmatter(text: str) -> str:
    if not text.startswith("---\n"):
        return ""
    marker = text.find("\n---\n", 4)
    return text[4:marker] if marker >= 0 else ""


def scalar(meta: str, key: str) -> str:
    match = re.search(rf"(?m)^{re.escape(key)}:\s*['\"]?([^'\"\n]+)", meta)
    return match.group(1).strip() if match else ""


def list_field(meta: str, key: str) -> list[str]:
    lines = meta.splitlines()
    values: list[str] = []
    active = False
    for line in lines:
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


def row_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    result: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        match = ROW_ID_RE.match(line)
        if match:
            result.add(match.group(1))
    return result


def row_count(path: Path, card_id: str) -> int:
    if not path.exists():
        return 0
    pattern = re.compile(rf"^\|\s*{re.escape(card_id)}\s*\|")
    return sum(
        1
        for line in path.read_text(encoding="utf-8").splitlines()
        if pattern.match(line)
    )


def formal_ids(card_dir: Path) -> set[str]:
    result: set[str] = set()
    for path in card_dir.glob("*.md"):
        candidate = path.name.split("_", 1)[0]
        if CARD_ID_RE.fullmatch(candidate):
            result.add(candidate)
    return result


def source_ids(summary_dir: Path) -> set[str]:
    result: set[str] = set()
    for path in summary_dir.glob("SRC-WQ-*.md"):
        candidate = path.stem.removeprefix("SRC-WQ-")
        if CARD_ID_RE.fullmatch(candidate):
            result.add(candidate)
    return result


def cluster_index(wiki: Path) -> dict[str, Path]:
    result: dict[str, Path] = {}
    for directory in (
        wiki / "topics/knowledge_clusters",
        wiki / "methods/method_clusters",
        wiki / "error_patterns/error_clusters",
        wiki / "methods/action_gap_clusters",
    ):
        if not directory.exists():
            continue
        for path in directory.glob("*.md"):
            meta = frontmatter(path.read_text(encoding="utf-8"))
            wiki_id = scalar(meta, "wiki_id")
            if wiki_id and "INDEX" not in wiki_id:
                result[wiki_id] = path
    return result


def target_check(
    card_id: str,
    summary_dir: Path,
    clusters: dict[str, Path],
    expected_date: str,
    generated_cards: dict[str, list[dict]],
) -> dict:
    path = summary_dir / f"SRC-WQ-{card_id}.md"
    if not path.exists():
        return {"id": card_id, "summary": "missing", "fresh": False, "cluster_refs": [], "cluster_membership_failures": ["summary missing"]}
    text = path.read_text(encoding="utf-8")
    meta = frontmatter(text)
    last_updated = scalar(meta, "last_updated")
    refs = sorted(set(REF_RE.findall(meta)))
    failures: list[str] = []
    field_mismatches: list[str] = []
    generated_matches = generated_cards.get(card_id, [])
    generated = generated_matches[0] if len(generated_matches) == 1 else None
    projection_hash = ""
    if generated is None:
        field_mismatches.append(
            f"generated card identity count={len(generated_matches)}"
        )
    else:
        projection_hash = canonical_sha256(generated)
        if scalar(meta, "formal_projection_sha256") != projection_hash:
            field_mismatches.append("formal_projection_sha256")
        card_meta = generated.get("meta") or {}
        if scalar(meta, "subject") != str(card_meta.get("subject") or ""):
            field_mismatches.append("subject")
        for field in ("knowledge", "methods", "error_causes"):
            source_values = {str(value).strip() for value in card_meta.get(field, []) if str(value).strip()}
            summary_values = set(list_field(meta, field))
            if source_values != summary_values:
                field_mismatches.append(field)
        if card_id not in set(list_field(meta, "wrongnet_refs")):
            field_mismatches.append("wrongnet_refs")
        expected_path = "错题知识网络/" + str(generated.get("path") or "")
        if expected_path not in set(list_field(meta, "source_refs")):
            field_mismatches.append("source_refs")
    for ref in refs:
        cluster = clusters.get(ref)
        if cluster is None:
            failures.append(f"{ref}: missing cluster file")
        elif not re.search(rf"(?<![A-Z0-9-]){re.escape(card_id)}(?![A-Z0-9-])", cluster.read_text(encoding="utf-8")):
            failures.append(f"{ref}: target not listed")
    return {
        "id": card_id,
        "summary": str(path),
        "last_updated": last_updated or None,
        "formal_projection_sha256": projection_hash or None,
        "fresh": (not expected_date or last_updated == expected_date) and not field_mismatches,
        "field_mismatches": field_mismatches,
        "cluster_refs": refs,
        "cluster_membership_failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", required=True, help="kaoyan-math repository root")
    parser.add_argument("--date", default="", help="expected target last_updated date")
    parser.add_argument("--id", action="append", default=[], dest="ids", help="target formal ID; repeatable")
    parser.add_argument(
        "--global",
        action="store_true",
        dest="global_identity",
        help="also require full-library identity parity; target-only is the nightly default",
    )
    args = parser.parse_args()
    if not args.ids and not args.global_identity:
        parser.error("nightly target verification requires at least one --id")
    if args.ids and not args.date:
        parser.error("target verification requires --date YYYY-MM-DD")

    repo = Path(args.repo).expanduser().resolve()
    net = repo / "错题知识网络"
    wiki = net / "wiki"
    summary_dir = wiki / "sources/wrong_cards"

    paths = {
        "source_index": wiki / "sources/SRC-WRONGCARDS-INDEX_全量错题卡覆盖索引.md",
        "coverage_gs": wiki / "coverage/MATHWIKI-COVERAGE-GS_高等数学错题卡覆盖表.md",
        "coverage_la": wiki / "coverage/MATHWIKI-COVERAGE-LA_线性代数错题卡覆盖表.md",
        "matrix": wiki / "coverage/MATHWIKI-COVERAGE-MATRIX_错题卡多维编译矩阵.md",
    }

    formal = formal_ids(net / "错题卡")
    summaries = source_ids(summary_dir)
    source_index = row_ids(paths["source_index"])
    coverage = row_ids(paths["coverage_gs"]) | row_ids(paths["coverage_la"])
    matrix = row_ids(paths["matrix"])
    clusters = cluster_index(wiki)
    generated_path = net / "生成/wrong_questions.json"
    generated_cards: dict[str, list[dict]] = {}
    if generated_path.exists():
        raw = json.loads(generated_path.read_text(encoding="utf-8"))
        for item in raw.get("cards", []):
            if isinstance(item, dict) and isinstance(item.get("id"), str):
                generated_cards.setdefault(item["id"], []).append(item)

    layers = {
        "source_summaries": summaries,
        "source_index": source_index,
        "subject_coverage": coverage,
        "matrix": matrix,
    }
    parity = {}
    identity_ok = True
    for name, ids in layers.items():
        missing = sorted(formal - ids)
        orphan = sorted(ids - formal)
        parity[name] = {"count": len(ids), "missing": missing, "orphan": orphan}
        identity_ok = identity_ok and not missing and not orphan

    requested = list(dict.fromkeys(args.ids))
    invalid = [card_id for card_id in requested if not CARD_ID_RE.fullmatch(card_id)]
    targets = [
        target_check(card_id, summary_dir, clusters, args.date, generated_cards)
        for card_id in requested
        if card_id not in invalid
    ]
    for item in targets:
        card_id = item["id"]
        counts = {
            "source_index": row_count(paths["source_index"], card_id),
            "coverage_gs": row_count(paths["coverage_gs"], card_id),
            "coverage_la": row_count(paths["coverage_la"], card_id),
            "matrix": row_count(paths["matrix"], card_id),
        }
        item["index_row_counts"] = counts
        item["index_identity_unique"] = (
            counts["source_index"] == 1
            and counts["coverage_gs"] + counts["coverage_la"] == 1
            and counts["matrix"] == 1
        )
    target_ok = all(
        item["summary"] != "missing"
        and item["fresh"]
        and item["cluster_refs"]
        and not item["cluster_membership_failures"]
        and item["index_identity_unique"]
        for item in targets
    ) and not invalid

    result = {
        "repo": str(repo),
        "formal_count": len(formal),
        "identity_parity": parity,
        "cluster_file_count": len(clusters),
        "requested_targets": requested,
        "invalid_targets": invalid,
        "targets": targets,
        "global_identity_required": args.global_identity,
        "global_identity_ok": identity_ok,
        "ok": target_ok and (identity_ok if args.global_identity else True),
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
