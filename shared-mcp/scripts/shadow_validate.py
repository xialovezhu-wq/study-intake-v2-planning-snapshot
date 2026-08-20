from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Iterable

from study_read_mcp.service import StudyReadService


def hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def tree_hash(paths: Iterable[Path]) -> tuple[str, int]:
    rows = []
    for path in sorted(set(paths), key=lambda value: str(value)):
        if path.is_file() and not path.is_symlink():
            rows.append((str(path), hash_file(path)))
    body = "\n".join(f"{path}:{digest}" for path, digest in rows).encode()
    return hashlib.sha256(body).hexdigest(), len(rows)


def protected_files() -> list[Path]:
    math = Path("/Users/xiazhibin/Documents/kaoyan-math")
    cs408 = Path("/Users/xiazhibin/Documents/kaoyan-408/wiki/study_vaults/408-full")
    english = Path("/Users/xiazhibin/Documents/kaoyan-english")
    pre = Path("/Users/xiazhibin/.codex/study-intake-preprocessor")
    files = list((math / "错题知识网络/错题卡").glob("*.md"))
    files += [
        math / "错题知识网络/生成/wrong_questions.json",
        math / "数学一回滚复习系统/快速入库事件.jsonl",
        math / "数学一回滚复习系统/复习记录.jsonl",
        math / "数学一回滚复习系统/学习前5题记录.jsonl",
    ]
    files += list((cs408 / "StudyVault").rglob("*.md"))
    files += [
        cs408 / "manifest.json", cs408 / "state/intake-curation/state.json",
        cs408 / "state/intake-curation/events.jsonl", cs408 / "state/review-loop/hot-state/manifest.json",
    ]
    files += list((english / "raw/articles").rglob("*.json"))
    files += [english / "bank/master_bank.csv", english / "bank/sentence_patterns.md"]
    files += list((english / "intake/events").rglob("*.json"))
    files += list((english / "intake/receipts").rglob("*.json"))
    target = (pre / "current").resolve(strict=True)
    files += [target / "release.json"]
    files += list((pre / "state").glob("latest*")) if (pre / "state").exists() else []
    files += list((pre / "dashboard").glob("*summary*.json"))
    return [path for path in files if path.exists()]


def main() -> None:
    files = protected_files()
    before, count = tree_hash(files)
    service = StudyReadService()
    try:
        results = {
            "authority": service.authority_bundle(["math", "cs408", "english"], ["authority", "preprocessor_release"]),
            "math": service.math_read_bundle([
                {"op": "formal_cards", "ids": ["GS-032"], "fields": ["id", "title", "chapter", "status", "related"]},
                {"op": "activity_window", "date_from": "2026-08-01", "date_to": "2026-08-07"},
                {"op": "direct_relations", "ids": ["GS-032"]},
            ]),
            "cs408": service.cs408_read_bundle([
                {"op": "knowledge_nodes", "ids": ["DS06-21-关键路径"]},
                {"op": "direct_edges", "ids": ["DS06-21-关键路径"]},
                {"op": "curation_inventory", "study_date": "2026-08-06"},
            ]),
            "english": service.english_read_bundle({
                "article_id": "RAW-ARTICLE-20260710-001", "sentence_ids": ["S01", "S02"],
                "terms": ["happiness", "media"], "include": ["article", "sentences", "vocab_status", "patterns", "coverage"],
            }),
        }
    finally:
        service.close()
    after_files = protected_files()
    after, after_count = tree_hash(after_files)
    print(json.dumps({
        "schema": "study-read-mcp-shadow-validation.v1", "protected_file_count_before": count,
        "protected_file_count_after": after_count, "protected_hash_before": before,
        "protected_hash_after": after, "protected_surfaces_unchanged": before == after and count == after_count,
        "calls": {name: {"ok": result.get("ok"), "generation": result.get("generation"),
                          "formal_write_count": result.get("formal_write_count"), "model_call_count": result.get("model_call_count")}
                  for name, result in results.items()},
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

