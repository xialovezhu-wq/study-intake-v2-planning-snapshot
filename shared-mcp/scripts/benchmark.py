from __future__ import annotations

import argparse
import json
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable

from study_read_mcp.safeio import json_size
from study_read_mcp.service import StudyReadService


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * fraction))))
    return ordered[index]


def measure(fn: Callable[[], object], count: int) -> tuple[dict[str, float], object]:
    values = []
    result = None
    for _ in range(count):
        started = time.perf_counter()
        result = fn()
        values.append((time.perf_counter() - started) * 1000)
    return {
        "count": count, "p50_ms": round(percentile(values, 0.50), 3),
        "p95_ms": round(percentile(values, 0.95), 3), "p99_ms": round(percentile(values, 0.99), 3),
        "max_ms": round(max(values), 3), "mean_ms": round(statistics.fmean(values), 3),
    }, result


def concurrent_measure(fn: Callable[[], object], workers: int) -> dict[str, float]:
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_timed, fn) for _ in range(workers)]
        values = [future.result() for future in futures]
    wall = time.perf_counter() - started
    return {
        "workers": workers, "p50_ms": round(percentile(values, 0.50), 3),
        "p95_ms": round(percentile(values, 0.95), 3), "p99_ms": round(percentile(values, 0.99), 3),
        "max_ms": round(max(values), 3), "throughput_rps": round(workers / wall, 2),
    }


def _timed(fn: Callable[[], object]) -> float:
    started = time.perf_counter()
    fn()
    return (time.perf_counter() - started) * 1000


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=200)
    args = parser.parse_args()
    service = StudyReadService()
    calls = {
        "authority": lambda: service.authority_bundle(["math", "cs408", "english"], ["authority"]),
        "math": lambda: service.math_read_bundle([
            {"op": "formal_cards", "ids": ["GS-032"], "fields": ["id", "title", "chapter", "status", "related"]},
            {"op": "activity_window", "date_from": "2026-08-01", "date_to": "2026-08-07"},
            {"op": "direct_relations", "ids": ["GS-032"]},
        ]),
        "cs408": lambda: service.cs408_read_bundle([
            {"op": "knowledge_nodes", "ids": ["DS06-21-关键路径"]},
            {"op": "direct_edges", "ids": ["DS06-21-关键路径"]},
            {"op": "curation_inventory", "study_date": "2026-08-06"},
        ]),
        "english": lambda: service.english_read_bundle({
            "article_id": "RAW-ARTICLE-20260710-001", "sentence_ids": ["S01", "S02"],
            "terms": ["happiness", "media"], "include": ["article", "sentences", "vocab_status", "patterns", "coverage"],
        }),
    }
    report: dict[str, object] = {"schema": "study-read-mcp-benchmark.v1", "sequential": {}, "concurrency": {}}
    try:
        for name, call in calls.items():
            metrics, result = measure(call, args.count)
            metrics["return_bytes"] = json_size(result)
            report["sequential"][name] = metrics
        for workers in (8, 32):
            report["concurrency"][str(workers)] = {
                name: concurrent_measure(call, workers) for name, call in calls.items()
            }
        caches = {}
        for subject, adapter in service.adapters.items():
            stats = adapter.reader.cache.stats()
            total = stats["hits"] + stats["misses"]
            stats["hit_ratio"] = round(stats["hits"] / total, 4) if total else 0.0
            stats["directory_scans"] = adapter.directory_scans
            if subject == "cs408":
                stats["sqlite_opens"] = adapter.sqlite_opens
            caches[subject] = stats
        report["cache"] = caches
    finally:
        service.close()
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

