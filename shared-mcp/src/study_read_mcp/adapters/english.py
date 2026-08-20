from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from jsonschema import ValidationError as JsonSchemaValidationError
from jsonschema.validators import validator_for

from ..envelope import success_envelope
from ..errors import StudyReadError
from ..models import EnglishRequest, PagedReadRequest
from ..safeio import SafeReader
from .base import AuthoritySnapshot, BaseAdapter


class EnglishAdapter(BaseAdapter):
    subject = "english"
    authority_source_id = "english.articles-bank-patterns-events"
    capabilities = frozenset({"article", "sentences", "vocab_status", "day_events", "patterns", "coverage"})
    _PROJECTION_PREFIX = "<!-- study-intake-projection-binding-v1 "
    _PROJECTION_SUFFIX = " -->\n"
    _PROJECTION_SCHEMA = "english_quick_capture_projection_v2"
    _LOCAL_ZONE = ZoneInfo("Asia/Shanghai")

    def __init__(self, root: Path, *, reader: SafeReader | None = None) -> None:
        super().__init__(reader or SafeReader({"english": root}))
        self.root = self.reader.roots["english"]
        self.articles_dir = self.reader.exact("english", "raw", "articles")
        self.master_bank = self.reader.exact("english", "bank", "master_bank.csv")
        self.mastered_items = self.reader.exact("english", "bank", "mastered_items.csv")
        self.patterns = self.reader.exact("english", "bank", "sentence_patterns.md")
        self.event_schema_v1 = self.reader.exact(
            "english", "schema", "english_pipeline", "capture-event-v1.schema.json"
        )
        self.event_schema_v2 = self.reader.exact(
            "english", "schema", "english_pipeline", "capture-event-v2.schema.json"
        )
        self.events_dir = self.reader.exact("english", "intake", "events")
        self.receipts_dir = self.reader.exact("english", "intake", "receipts")
        self.corpus_dir = self.root / "raw" / "articles" / "exam-reading-corpus"
        self.learning_pages_dir = self.root / "articles"
        self.migrations_dir = self.root / "intake" / "migrations" / "event-v2"
        self._articles_identity = None
        self._article_index: dict[str, Path] = {}
        self._learning_pages_identity = None
        self._learning_page_index: dict[str, Path] = {}
        self._migrations_identity = None
        self._migration_index: dict[str, Path] = {}
        self._events_root_identity = None
        self._event_day_dirs: dict[str, Path] = {}
        self._event_days: dict[str, tuple[Any, list[Path]]] = {}
        self.directory_scans = 0
        self._refresh_article_index()
        self._refresh_learning_page_index()
        self._refresh_migration_index()
        self._refresh_event_day_index()

    @staticmethod
    def _optional_directory(root: Path, candidate: Path) -> Path | None:
        if not candidate.exists():
            return None
        if candidate.is_symlink() or not candidate.is_dir():
            raise StudyReadError("SUBJECT_UNAVAILABLE", "English authority directory is unsafe")
        try:
            candidate.resolve(strict=True).relative_to(root)
        except (OSError, ValueError) as exc:
            raise StudyReadError("SUBJECT_UNAVAILABLE", "English authority directory escaped its root") from exc
        return candidate.resolve(strict=True)

    def _refresh_learning_page_index(self) -> None:
        directory = self._optional_directory(self.root, self.learning_pages_dir)
        if directory is None:
            self._learning_page_index = {}
            return
        identity = self.reader.identity(directory)
        if identity == self._learning_pages_identity:
            return
        index: dict[str, Path] = {}
        for path in sorted(directory.glob("*.md")):
            if path.is_symlink() or not path.is_file():
                continue
            path = self.reader.validate_indexed_path("english", path)
            text, _, _ = self.reader.text(path)
            matched = re.search(r"^-\s*source_id[：:]\s*([^\s`]+)\s*$", text, re.MULTILINE)
            stable_id = matched.group(1) if matched else f"LEARNING-PAGE-{path.stem}"
            SafeReader.validate_stable_id(stable_id, "English learning page id")
            if stable_id in index:
                raise StudyReadError("SUBJECT_UNAVAILABLE", "English learning page id is duplicated")
            index[stable_id] = path
        self._learning_page_index = index
        self._learning_pages_identity = identity
        self.directory_scans += 1

    def _refresh_migration_index(self) -> None:
        directory = self._optional_directory(self.root, self.migrations_dir)
        if directory is None:
            self._migration_index = {}
            return
        identity = self.reader.identity(directory)
        if identity == self._migrations_identity:
            return
        index: dict[str, Path] = {}
        for path in sorted(directory.glob("*.json")):
            if path.is_symlink() or not path.is_file():
                continue
            path = self.reader.validate_indexed_path("english", path)
            value, _, _ = self.reader.json(path)
            event_id = value.get("source_event_id") if isinstance(value, dict) else None
            if not isinstance(event_id, str) or not event_id:
                raise StudyReadError("SUBJECT_UNAVAILABLE", "English migration event id is invalid")
            if event_id in index:
                raise StudyReadError("SUBJECT_UNAVAILABLE", "English event has duplicate migrations")
            index[event_id] = path
        self._migration_index = index
        self._migrations_identity = identity
        self.directory_scans += 1

    def _refresh_event_day_index(self) -> None:
        identity = self.reader.identity(self.events_dir)
        if identity == self._events_root_identity:
            return
        index: dict[str, Path] = {}
        for child in self.events_dir.iterdir():
            if child.is_symlink() or not child.is_dir():
                continue
            try:
                date.fromisoformat(child.name)
            except ValueError:
                continue
            index[child.name] = self.reader.validate_indexed_path("english", child)
        self._event_day_dirs = index
        self._events_root_identity = identity
        self.directory_scans += 1

    def _refresh_article_index(self) -> None:
        identity = self.reader.identity(self.articles_dir)
        index: dict[str, Path] = {}
        for child in self.articles_dir.iterdir():
            if child.is_symlink() or not child.is_dir() or child.name == "exam-reading-corpus":
                continue
            candidates = list(child.glob("*/dataset/pipeline_handoff.json"))
            candidates += list(child.glob("*/dataset/*_practice_safe.json"))
            candidates += list(child.glob("*/dataset/*_ingest.json"))
            if not candidates:
                continue
            preferred = next((p for p in candidates if p.name == "pipeline_handoff.json"), candidates[0])
            preferred = self.reader.validate_indexed_path("english", preferred)
            index[child.name] = preferred
            try:
                payload, _, _ = self.reader.json(preferred)
            except StudyReadError:
                continue
            if isinstance(payload, dict):
                source_id = payload.get("source_id")
                if not source_id:
                    package = payload.get("package_metadata") if isinstance(payload.get("package_metadata"), dict) else {}
                    source_id = package.get("source_id")
                if isinstance(source_id, str) and len(source_id) <= 160 and "/" not in source_id:
                    index[source_id] = preferred
        corpus = self._optional_directory(self.root, self.corpus_dir)
        if corpus is not None:
            for path in sorted(corpus.rglob("*.json")):
                if path.is_symlink() or not path.is_file():
                    continue
                path = self.reader.validate_indexed_path("english", path)
                payload, _, _ = self.reader.json(path)
                source_id = payload.get("source_id") if isinstance(payload, dict) else None
                if not isinstance(source_id, str) or not source_id:
                    raise StudyReadError("SUBJECT_UNAVAILABLE", "English corpus source id is invalid")
                SafeReader.validate_stable_id(source_id, "English corpus source id")
                existing = index.get(source_id)
                # The canonical practice-safe corpus intentionally supersedes older
                # per-intake artifacts that carry the same source_id.  The older
                # bytes remain authority-bound through their unique path aliases;
                # exact source-ID reads deterministically select the corpus record.
                index[source_id] = path
        self._article_index = index
        self._articles_identity = identity
        self.directory_scans += 1

    def _article_bindings(self) -> list[tuple[str, str]]:
        unique = {path for path in self._article_index.values()}
        return [
            (
                "english.article."
                + hashlib.sha256(
                    path.relative_to(self.root).as_posix().encode()
                ).hexdigest()[:24],
                self.reader.digest(path)[0],
            )
            for path in unique
        ]

    @classmethod
    def _event_study_date(cls, occurred_at: Any) -> str:
        if not isinstance(occurred_at, str) or not occurred_at:
            raise StudyReadError(
                "PROJECTION_MANIFEST_MISMATCH",
                "English projection event has no valid occurrence time",
                True,
            )
        try:
            parsed = datetime.fromisoformat(occurred_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise StudyReadError(
                "PROJECTION_MANIFEST_MISMATCH",
                "English projection event has no valid occurrence time",
                True,
            ) from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=cls._LOCAL_ZONE)
        return parsed.astimezone(cls._LOCAL_ZONE).date().isoformat()

    @staticmethod
    def _projection_safe_name(value: str) -> str:
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-.")
        return cleaned or "article"

    @classmethod
    def _projection_binding(
        cls,
        effective_events: list[dict[str, Any]],
        *,
        source_id: str,
        study_date: str,
    ) -> dict[str, Any]:
        ordered = sorted(effective_events, key=lambda event: str(event["event_id"]))
        rows = [
            f"{event['event_id']}:{cls._object_sha256(event)}"
            for event in ordered
        ]
        return {
            "schema_version": cls._PROJECTION_SCHEMA,
            "data_role": "projection",
            "source_id": source_id,
            "study_date": study_date,
            "effective_event_ids": [event["event_id"] for event in ordered],
            "effective_event_count": len(ordered),
            "effective_event_high_water_sha256": hashlib.sha256(
                "\n".join(rows).encode("utf-8")
            ).hexdigest(),
            "formal_write_count": 0,
        }

    @classmethod
    def _render_quick_capture_projection(
        cls,
        effective_events: list[dict[str, Any]],
        *,
        source_id: str,
        study_date: str,
    ) -> bytes:
        binding = cls._projection_binding(
            effective_events,
            source_id=source_id,
            study_date=study_date,
        )
        lines = [
            cls._PROJECTION_PREFIX
            + json.dumps(
                binding,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + cls._PROJECTION_SUFFIX.rstrip("\n"),
            f"# 英语快速入库视图｜{source_id}",
            "",
            "> 本页由 append-only capture events 重建；它是可重建视图，不是正式学习库。",
            "",
            f"- 日期筛选：{study_date}",
            f"- 有效句子事件：{len(effective_events)}",
            "- formal_write_count：0",
            "- formal_writeback：none",
            "",
        ]
        for event in effective_events:
            source = event["source"]
            learning = event["learning"]
            protected_explanation = source.get("source_kind") == "explanation"
            lines.extend(
                [
                    f"## {source['sentence_id']}｜{event['event_id']}",
                    "",
                    f"- 来源文章：{event['article']['source_article']}",
                    (
                        "- 原句：[受保护解析正文不在可重建视图回显]"
                        if protected_explanation
                        else f"- 原句：{source['source_sentence']}"
                    ),
                    f"- 文章 source hash：`{event['article']['source_hash']}`",
                    f"- 句子 sentence hash：`{source['sentence_hash']}`",
                    (
                        "- 第一遍翻译：[不回显]"
                        if protected_explanation
                        else f"- 第一遍翻译：{learning.get('first_translation', '')}"
                    ),
                    f"- 用户证据：{'|'.join(learning.get('user_evidence', []))}",
                    f"- 证据来源：{learning.get('evidence_origin', '')}",
                    f"- 提示层级：L{learning.get('hint_level', 0)}",
                    f"- 答案保护：{learning.get('answer_protection', '')}",
                ]
            )
            if learning.get("translation") and not protected_explanation:
                lines.append(f"- 本轮译文：{learning['translation']}")
            if learning.get("explanation") and not protected_explanation:
                lines.append(f"- 本轮讲解：{learning['explanation']}")
            if event.get("supersedes_event_id"):
                lines.append(f"- 更正替代：{event['supersedes_event_id']}")
            lines.extend(["", "### 本句候选", ""])
            candidates = event.get("candidates", [])
            if not candidates:
                lines.append("- 无")
            for candidate in ([] if protected_explanation else candidates):
                lines.append(
                    f"- {candidate['item']}｜{candidate['candidate_type']}｜"
                    f"{candidate.get('meaning', '')}｜{candidate['decision']}｜"
                    f"{candidate.get('tier_hint', '待分层')}"
                )
            lines.append("")
        return ("\n".join(lines).rstrip() + "\n").encode("utf-8")

    def _effective_projection_scopes(
        self,
    ) -> tuple[dict[tuple[str, str], list[dict[str, Any]]], tuple[Path, ...]]:
        raw_paths = self._raw_event_path_map()
        migrations = self._migration_by_event()
        dependencies: set[Path] = set(raw_paths.values())
        dependencies.update(self._migration_index.values())
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for event_id, path in sorted(raw_paths.items()):
            raw_event, _, _ = self.reader.json(path)
            if not isinstance(raw_event, dict):
                raise StudyReadError(
                    "PROJECTION_MANIFEST_MISMATCH",
                    "English projection event is not an object",
                    True,
                )
            migration = migrations.get(event_id)
            effective = migration["effective_event"] if migration else raw_event
            if migration is None:
                _, validation = self._validate_event_payload(effective)
                if not validation["valid"]:
                    raise StudyReadError(
                        "PROJECTION_MANIFEST_MISMATCH",
                        "English projection event does not validate",
                        True,
                    )
            if effective.get("event_type") not in {
                "sentence_captured",
                "sentence_correction",
            }:
                continue
            article = effective.get("article")
            source_id = article.get("source_id") if isinstance(article, dict) else None
            article_id = article.get("article_id") if isinstance(article, dict) else None
            if (
                not isinstance(source_id, str)
                or not source_id
                or article_id != source_id
            ):
                raise StudyReadError(
                    "PROJECTION_MANIFEST_MISMATCH",
                    "English projection event has no article identity",
                    True,
                )
            SafeReader.validate_stable_id(source_id, "English projection source id")
            study_date = self._event_study_date(effective.get("occurred_at"))
            grouped.setdefault((source_id, study_date), []).append(effective)
        effective_scopes: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for scope, events in grouped.items():
            superseded = {
                str(event["supersedes_event_id"])
                for event in events
                if event.get("event_type") == "sentence_correction"
                and event.get("supersedes_event_id")
            }
            current = [
                event for event in events
                if str(event.get("event_id")) not in superseded
            ]
            if current:
                effective_scopes[scope] = current
        return effective_scopes, tuple(sorted(dependencies))

    def _quick_capture_projection_paths(self) -> tuple[Path, ...]:
        views_root = self._optional_directory(self.root, self.root / "intake" / "views")
        projection_paths: list[Path] = []
        if views_root is not None:
            for path in sorted(views_root.glob("*/*-quick-capture.md")):
                if path.is_symlink() or not path.is_file():
                    raise StudyReadError(
                        "PROJECTION_MANIFEST_MISMATCH",
                        "English projection path is unsafe",
                        True,
                    )
                projection_paths.append(
                    self.reader.validate_indexed_path("english", path)
                )
        return tuple(projection_paths)

    def _verify_quick_capture_projections(self) -> None:
        scopes, dependencies = self._effective_projection_scopes()
        projection_paths = self._quick_capture_projection_paths()
        expected_paths = {
            self.root
            / "intake"
            / "views"
            / study_date
            / f"{self._projection_safe_name(source_id)}-quick-capture.md"
            for source_id, study_date in scopes
        }
        actual_paths = set(projection_paths)
        if expected_paths != actual_paths:
            raise StudyReadError(
                "PROJECTION_MANIFEST_MISMATCH",
                "English quick-capture projection closure is incomplete",
                True,
            )
        identities = self.capture_identities(
            (*dependencies, *projection_paths), self.reader
        )
        for (source_id, study_date), events in sorted(scopes.items()):
            path = (
                self.root
                / "intake"
                / "views"
                / study_date
                / f"{self._projection_safe_name(source_id)}-quick-capture.md"
            )
            expected = self._render_quick_capture_projection(
                events,
                source_id=source_id,
                study_date=study_date,
            )
            stored = self.reader.read_bytes(path).data
            if stored != expected:
                raise StudyReadError(
                    "PROJECTION_MANIFEST_MISMATCH",
                    "English quick-capture projection does not match canonical effective events",
                    True,
                )
        try:
            self.assert_unchanged(identities, self.reader)
        except StudyReadError as exc:
            raise StudyReadError(
                "PROJECTION_MANIFEST_MISMATCH",
                "English projection authority changed during validation",
                True,
            ) from exc

    def authority_checks(self, checks: list[str]) -> dict[str, bool]:
        if "projection_bound" in checks:
            self._verify_quick_capture_projections()
        return super().authority_checks(checks)

    def authority(self) -> AuthoritySnapshot:
        self.assert_available()
        with self._lock:
            self._refresh_article_index()
            bindings = self._article_bindings()
            sources: list[dict[str, Any]] = []
            for source_id, role, path in (
                ("english.master-bank", "formal", self.master_bank),
                ("english.mastered-items", "formal", self.mastered_items),
                ("english.sentence-patterns", "formal", self.patterns),
                ("english.capture-event-schema-v1", "schema", self.event_schema_v1),
                ("english.capture-event-schema-v2", "schema", self.event_schema_v2),
            ):
                digest, _ = self.reader.digest(path)
                bindings.append((source_id, digest))
                sources.append({"capability": source_id, "data_role": role, "available": True, "sha256": digest})
            article_digest = self.digest_bindings(self._article_bindings())
            sources.append({"capability": "english.article-index", "data_role": "practice_safe_source_index", "available": True, "sha256": article_digest})
            bindings.append(("english.article-index", article_digest))
            self._refresh_learning_page_index()
            learning_bindings = [
                (stable_id, self.reader.digest(path)[0])
                for stable_id, path in sorted(self._learning_page_index.items())
            ]
            learning_digest = self.reader.set_fingerprint(learning_bindings)
            bindings.append(("english.article-learning-pages", learning_digest))
            sources.append({
                "capability": "english.article-learning-pages",
                "data_role": "learning_page_projection_index",
                "available": True,
                "sha256": learning_digest,
                "record_count": len(learning_bindings),
            })
            self._refresh_event_day_index()
            event_bindings: list[tuple[str, str]] = []
            for day, directory in sorted(self._event_day_dirs.items()):
                try:
                    parsed = date.fromisoformat(day)
                except ValueError:
                    continue
                for path in self._day_event_paths(parsed):
                    event_bindings.append(
                        (f"event:{day}:{path.name}", self.reader.digest(path)[0])
                    )
            event_digest = self.reader.set_fingerprint(event_bindings)
            bindings.append(("english.events-index", event_digest))
            sources.append({"capability": "english.events-index", "data_role": "event", "available": True, "sha256": event_digest})
            migrations = self._migration_records()
            migration_digest = self.reader.set_fingerprint(
                (row["migration_id"], row["migration_file_sha256"]) for row in migrations
            )
            effective_digest = self.reader.set_fingerprint(
                (row["event_id"], row["effective_object_sha256"]) for row in migrations
            )
            bindings.extend([
                ("english.event-migrations", migration_digest),
                ("english.effective-events", effective_digest),
            ])
            sources.extend([
                {
                    "capability": "english.event-migrations", "data_role": "migration_receipt",
                    "available": True, "sha256": migration_digest,
                    "record_count": len(migrations),
                },
                {
                    "capability": "english.effective-events", "data_role": "effective_event_index",
                    "available": True, "sha256": effective_digest,
                    "record_count": len(migrations),
                },
            ])
            projection_bindings = [
                (
                    path.relative_to(self.root).as_posix(),
                    self.reader.digest(path)[0],
                )
                for path in self._quick_capture_projection_paths()
            ]
            projection_digest = self.reader.set_fingerprint(projection_bindings)
            bindings.append(("english.quick-capture-projections", projection_digest))
            sources.append({
                "capability": "english.quick-capture-projections",
                "data_role": "projection_index",
                "available": True,
                "sha256": projection_digest,
                "record_count": len(projection_bindings),
            })
            fingerprint = self.digest_bindings(bindings)
            return AuthoritySnapshot(self.subject, self.generation(self.subject, fingerprint), fingerprint, tuple(sources))

    def _article_path(self, article_id: str) -> Path:
        SafeReader.validate_stable_id(article_id, "article id")
        self._refresh_article_index()
        path = self._article_index.get(article_id)
        if path is None:
            raise StudyReadError("NOT_FOUND", "English article was not found")
        return path

    def _article(self, article_id: str) -> tuple[dict[str, Any], Path]:
        path = self._article_path(article_id)
        payload, digest, _ = self.reader.json(path)
        if not isinstance(payload, dict):
            raise StudyReadError("SUBJECT_UNAVAILABLE", "English article has an invalid schema")
        sentences: list[dict[str, Any]] = []
        source_id = payload.get("source_id")
        source_hash = payload.get("source_hash")
        units = payload.get("units")
        if isinstance(units, list):
            for unit in units:
                if not isinstance(unit, dict) or unit.get("source_kind") != "article":
                    continue
                sentences.append({
                    "sentence_id": unit.get("sentence_id"), "text": unit.get("source_sentence"),
                    "source_hash": unit.get("sentence_hash"), "data_role": "raw_source",
                    "source_role": "raw_ingest_unit",
                })
        elif isinstance(payload.get("passage_paragraphs"), list):
            for ordinal, paragraph in enumerate(payload["passage_paragraphs"], start=1):
                if not isinstance(paragraph, str):
                    raise StudyReadError(
                        "SUBJECT_UNAVAILABLE", "English corpus paragraph is invalid"
                    )
                sentences.append({
                    "sentence_id": f"P{ordinal:02d}",
                    "text": paragraph,
                    "source_hash": hashlib.sha256(paragraph.encode("utf-8")).hexdigest(),
                    "data_role": "practice_safe_source",
                    "source_role": "practice_safe_corpus_paragraph",
                })
        else:
            package = payload.get("package_metadata") if isinstance(payload.get("package_metadata"), dict) else {}
            source_id = source_id or package.get("source_id") or article_id
            article = payload.get("article") if isinstance(payload.get("article"), dict) else {}
            paragraphs = article.get("paragraphs") if isinstance(article.get("paragraphs"), list) else []
            for paragraph in paragraphs:
                if not isinstance(paragraph, dict):
                    continue
                nested = paragraph.get("sentences")
                if isinstance(nested, list):
                    for sentence in nested:
                        if isinstance(sentence, dict) and isinstance(sentence.get("text"), str):
                            sentences.append({"sentence_id": sentence.get("id"), "text": sentence.get("text"), "data_role": "raw_source", "source_role": "raw_ingest_sentence"})
                elif isinstance(paragraph.get("english"), str):
                    sentences.append({"sentence_id": paragraph.get("id"), "text": paragraph.get("english"), "data_role": "raw_source", "source_role": "raw_ingest_paragraph"})
        visibility = payload.get("visibility")
        role = "practice_safe_source" if visibility == "practice_safe" else "raw_source"
        return {
            "article_id": source_id or article_id, "requested_article_id": article_id,
            "semantic_source_hash": source_hash, "artifact_sha256": digest, "sentences": sentences,
            "article_text": "\n".join(
                str(sentence.get("text") or "") for sentence in sentences
            ),
            "sentence_count": len(sentences), "data_role": role,
            "source_role": "practice_safe_corpus" if role == "practice_safe_source" else "raw_ingest_artifact",
        }, path

    def _vocab(self, terms: list[str]) -> list[dict[str, Any]]:
        rows, bank_sha, _ = self.reader.csv_rows(self.master_bank)
        by_term: dict[str, list[dict[str, str]]] = {}
        for row in rows:
            item = row.get("item", "").strip().casefold()
            if item:
                by_term.setdefault(item, []).append(row)
        output = []
        for term in terms:
            matches = by_term.get(term.strip().casefold(), [])
            if not matches:
                output.append({"term": term, "status": "not_in_master_bank", "data_role": "formal", "bank_sha256": bank_sha})
                continue
            for row in matches[:4]:
                output.append({
                    "term": term, "status": "present", "id": row.get("id"), "type": row.get("type"),
                    "item": row.get("item"), "meaning": row.get("meaning"), "usage": row.get("usage"),
                    "writing_value": row.get("writing_value"), "tags": row.get("tags"),
                    "appear_count": row.get("appear_count"), "last_seen": row.get("last_seen"),
                    "data_role": "formal", "bank_sha256": bank_sha,
                })
        return output

    def _day_event_paths(self, value: date) -> list[Path]:
        day = value.isoformat()
        self._refresh_event_day_index()
        directory = self._event_day_dirs.get(day)
        if directory is None:
            return []
        identity = self.reader.identity(directory)
        cached = self._event_days.get(day)
        if cached and cached[0] == identity:
            return cached[1]
        paths = [self.reader.validate_indexed_path("english", p) for p in directory.glob("*.json") if p.is_file() and not p.is_symlink()]
        paths.sort(key=lambda p: p.name)
        self._event_days[day] = (identity, paths)
        self.directory_scans += 1
        return paths

    @staticmethod
    def _object_sha256(value: Any) -> str:
        return hashlib.sha256(
            json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
        ).hexdigest()

    def _raw_event_path_map(self) -> dict[str, Path]:
        self._refresh_event_day_index()
        output: dict[str, Path] = {}
        for day in sorted(self._event_day_dirs):
            try:
                parsed = date.fromisoformat(day)
            except ValueError:
                continue
            for path in self._day_event_paths(parsed):
                value, _, _ = self.reader.json(path)
                event_id = value.get("event_id") if isinstance(value, dict) else None
                if not isinstance(event_id, str) or not event_id:
                    raise StudyReadError("SUBJECT_UNAVAILABLE", "English raw event id is invalid")
                if event_id in output:
                    raise StudyReadError("SUBJECT_UNAVAILABLE", "English raw event id is duplicated")
                output[event_id] = path
        return output

    def _migration_records(self) -> list[dict[str, Any]]:
        self._refresh_migration_index()
        raw_paths = self._raw_event_path_map()
        output: list[dict[str, Any]] = []
        for event_id, path in sorted(self._migration_index.items()):
            receipt, receipt_sha, _ = self.reader.json(path)
            if not isinstance(receipt, dict):
                raise StudyReadError("SUBJECT_UNAVAILABLE", "English migration receipt is invalid")
            raw_path = raw_paths.get(event_id)
            if raw_path is None:
                raise StudyReadError("SUBJECT_UNAVAILABLE", "English migration source event is missing")
            raw_event, raw_file_sha, _ = self.reader.json(raw_path)
            effective = receipt.get("effective_event")
            if not isinstance(raw_event, dict) or not isinstance(effective, dict):
                raise StudyReadError("SUBJECT_UNAVAILABLE", "English migration event payload is invalid")
            raw_object_sha = self._object_sha256(raw_event)
            effective_object_sha = self._object_sha256(effective)
            _, effective_validation = self._validate_event_payload(effective)
            content_address = receipt.get("content_address")
            migration_id = receipt.get("migration_id")
            if (
                receipt.get("schema_version") != "english_capture_migration_receipt_v1"
                or receipt.get("source_event_id") != event_id
                or effective.get("event_id") != event_id
                or receipt.get("source_event_file_sha256") != raw_file_sha
                or receipt.get("source_event_object_sha256") != raw_object_sha
                or receipt.get("effective_event_sha256") != effective_object_sha
                or not isinstance(content_address, str)
                or not re.fullmatch(r"[0-9a-f]{64}", content_address)
                or migration_id != f"EN-MIG-{content_address[:24].upper()}"
                or receipt.get("formal_write_count") != 0
                or not effective_validation["valid"]
            ):
                raise StudyReadError(
                    "SUBJECT_UNAVAILABLE", "English migration receipt binding is invalid"
                )
            output.append({
                "migration_id": migration_id,
                "event_id": event_id,
                "migration_file_sha256": receipt_sha,
                "raw_file_sha256": raw_file_sha,
                "raw_object_sha256": raw_object_sha,
                "effective_object_sha256": effective_object_sha,
                "effective_event": effective,
                "content_address": content_address,
            })
        return output

    def _migration_by_event(self) -> dict[str, dict[str, Any]]:
        return {row["event_id"]: row for row in self._migration_records()}

    def _day_events(self, value: date) -> tuple[list[dict[str, Any]], list[Path]]:
        paths = self._day_event_paths(value)
        migrations = self._migration_by_event()
        output = []
        for path in paths:
            event, digest, _ = self.reader.json(path)
            if not isinstance(event, dict):
                continue
            event_id = str(event.get("event_id") or path.stem)
            migration = migrations.get(event_id)
            effective = migration["effective_event"] if migration else event
            source = effective.get("source") if isinstance(effective.get("source"), dict) else {}
            article = effective.get("article") if isinstance(effective.get("article"), dict) else {}
            raw_object_sha = self._object_sha256(event)
            output.append({
                "event_id": event_id, "event_type": effective.get("event_type"),
                "study_date": effective.get("study_date") or value.isoformat(),
                "article_id": article.get("article_id"),
                "source_id": article.get("source_id") or effective.get("source_id") or source.get("source_id"),
                "status": effective.get("status") or effective.get("decision"),
                "raw_file_sha256": digest,
                "raw_object_sha256": raw_object_sha,
                "effective_object_sha256": (
                    migration["effective_object_sha256"] if migration else raw_object_sha
                ),
                "source_role": "raw_capture_event",
                "data_role": "effective_event_summary" if migration else "raw_event_summary",
            })
        return output, paths

    def _validate_event_payload(self, event: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        schema_version = event.get("schema_version")
        if schema_version == "english_capture_event_v1":
            schema_path = self.event_schema_v1
        elif schema_version == "english_capture_event_v2":
            schema_path = self.event_schema_v2
        else:
            raise StudyReadError(
                "SUBJECT_UNAVAILABLE", "English capture event schema version is unsupported"
            )
        schema, _, _ = self.reader.json(schema_path)
        if not isinstance(schema, dict):
            raise StudyReadError(
                "SUBJECT_UNAVAILABLE", "English capture event schema is invalid"
            )
        try:
            validator_class = validator_for(schema)
            validator_class.check_schema(schema)
            validator_class(schema).validate(event)
        except JsonSchemaValidationError as exc:
            return str(schema_version), {
                "status": "schema_mismatch",
                "valid": False,
                "error_path": "/".join(str(value) for value in exc.absolute_path),
            }
        except Exception as exc:
            raise StudyReadError(
                "SUBJECT_UNAVAILABLE", "English capture event schema could not be applied"
            ) from exc
        return str(schema_version), {"status": "valid", "valid": True}

    def _event_record(
        self, event: dict[str, Any], digest: str, fallback_id: str,
        migration: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        effective = migration["effective_event"] if migration else event
        schema_version, schema_validation = self._validate_event_payload(effective)
        source = effective.get("source") if isinstance(effective.get("source"), dict) else {}
        article = effective.get("article") if isinstance(effective.get("article"), dict) else {}
        event_id = str(effective.get("event_id") or fallback_id)
        study_date = str(effective.get("study_date") or effective.get("occurred_at") or "")[:10]
        raw_object_sha = self._object_sha256(event)
        effective_object_sha = self._object_sha256(effective)
        return {
            "stable_id": f"{study_date}:{event_id}",
            "event_id": event_id,
            "event_type": effective.get("event_type"),
            "schema_version": schema_version,
            "schema_validation": schema_validation,
            "study_date": study_date,
            "article_id": article.get("article_id") or effective.get("source_id"),
            "source_id": article.get("source_id") or effective.get("source_id"),
            "sentence_id": source.get("sentence_id"),
            "source_sentence": source.get("source_sentence"),
            "sentence_hash": source.get("sentence_hash"),
            "raw_event_payload": event,
            "event_payload": effective,
            "content_complete": True,
            "raw_file_sha256": digest,
            "raw_object_sha256": raw_object_sha,
            "effective_object_sha256": effective_object_sha,
            "migration_id": migration.get("migration_id") if migration else None,
            "migration_file_sha256": migration.get("migration_file_sha256") if migration else None,
            "source_hash": effective_object_sha,
            "source_role": "raw_capture_event",
            "data_role": (
                ("effective_event" if migration else "raw_event")
                if schema_validation["valid"] else "event_schema_mismatch"
            ),
        }

    def _pattern_records(self) -> list[dict[str, Any]]:
        text, source_hash, _ = self.reader.text(self.patterns)
        lines = text.splitlines()
        starts: list[tuple[int, str, str]] = []
        for index, line in enumerate(lines):
            matched = re.fullmatch(r"##\s+(SP-\d{3})(?:[｜|]\s*|\s+)(.+?)\s*", line)
            if matched:
                starts.append((index, matched.group(1), matched.group(2).strip()))
        if not starts:
            raise StudyReadError(
                "SUBJECT_UNAVAILABLE", "English sentence pattern cards were not found"
            )
        output: list[dict[str, Any]] = []
        seen: set[str] = set()
        for ordinal, (start, stable_id, title) in enumerate(starts, start=1):
            if stable_id in seen:
                raise StudyReadError(
                    "SUBJECT_UNAVAILABLE", "English sentence pattern id is duplicated"
                )
            seen.add(stable_id)
            end = starts[ordinal][0] if ordinal < len(starts) else len(lines)
            markdown = "\n".join(lines[start:end]).rstrip() + "\n"
            raw = markdown.encode("utf-8")
            if len(raw) > 80_000:
                raise StudyReadError(
                    "RECORD_TOO_LARGE", "English sentence pattern card exceeds its record bound"
                )
            output.append({
                "stable_id": stable_id,
                "title": title,
                "ordinal": ordinal,
                "start_line": start + 1,
                "end_line": end,
                "markdown": markdown,
                "content_sha256": hashlib.sha256(raw).hexdigest(),
                "source_hash": source_hash,
                "byte_length": len(raw),
                "is_truncated": False,
                "content_complete": True,
                "data_role": "formal_pattern_card",
            })
        return output

    def _all_event_records(self, study_date: date | None = None) -> list[dict[str, Any]]:
        self._refresh_event_day_index()
        days = [study_date.isoformat()] if study_date is not None else sorted(self._event_day_dirs)
        output: list[dict[str, Any]] = []
        migrations = self._migration_by_event()
        for day in days:
            try:
                parsed = date.fromisoformat(day)
            except ValueError:
                continue
            for path in self._day_event_paths(parsed):
                value, digest, _ = self.reader.json(path)
                if isinstance(value, dict):
                    event_id = str(value.get("event_id") or path.stem)
                    output.append(
                        self._event_record(value, digest, path.stem, migrations.get(event_id))
                    )
        return output

    @staticmethod
    def _matches(value: Any, query: str) -> bool:
        return query.casefold() in __import__("json").dumps(
            value, ensure_ascii=False, sort_keys=True
        ).casefold()

    def _unique_article_entries(self) -> list[tuple[str, Path]]:
        self._refresh_article_index()
        by_path: dict[Path, str] = {}
        for indexed_id, path in sorted(self._article_index.items()):
            payload, _, _ = self.reader.json(path)
            source_id = payload.get("source_id") if isinstance(payload, dict) else None
            canonical = str(source_id or indexed_id)
            previous = by_path.get(path)
            if previous is None or indexed_id == canonical:
                by_path[path] = canonical
        entries = sorted(
            ((stable_id, path) for path, stable_id in by_path.items()),
            key=lambda row: row[0],
        )
        corpus = self._optional_directory(self.root, self.corpus_dir)
        if corpus is not None:
            canonical = [
                row for row in entries
                if self.reader._contained(corpus, row[1])
            ]
            if canonical:
                return canonical
        return entries

    def _learning_page_records(self, catalog_only: bool) -> list[dict[str, Any]]:
        self._refresh_learning_page_index()
        output: list[dict[str, Any]] = []
        for source_id, path in sorted(self._learning_page_index.items()):
            text, source_hash, _ = self.reader.text(path)
            raw = text.encode("utf-8")
            title = next(
                (line[2:].strip() for line in text.splitlines() if line.startswith("# ")),
                None,
            )
            if catalog_only:
                output.append({
                    "stable_id": source_id,
                    "source_hash": source_hash,
                    "title": title,
                    "byte_length": len(raw),
                    "source_role": "saved_learning_page",
                    "data_role": "article_learning_page_catalog",
                })
                continue
            offset = 0
            ordinal = 1
            while offset < len(raw) or (not raw and ordinal == 1):
                end = min(offset + 32_000, len(raw))
                while end > offset:
                    try:
                        chunk = raw[offset:end].decode("utf-8")
                        break
                    except UnicodeDecodeError:
                        end -= 1
                else:
                    chunk = ""
                stable_id = f"{source_id}:page:{ordinal:04d}"
                output.append({
                    "stable_id": stable_id,
                    "article_id": source_id,
                    "source_hash": source_hash,
                    "chunk_index": ordinal,
                    "byte_offset": offset,
                    "byte_length": end - offset,
                    "markdown": chunk,
                    "content_complete": end == len(raw),
                    "source_role": "saved_learning_page",
                    "data_role": "article_learning_page_chunk",
                })
                offset = end
                ordinal += 1
                if not raw:
                    break
        return output

    def _relation_records(self) -> list[dict[str, Any]]:
        rows, source_hash, _ = self.reader.csv_rows(self.master_bank)
        output: list[dict[str, Any]] = []
        for index, row in enumerate(rows):
            source = str(row.get("id") or f"VOCAB-{index:08d}")
            article_id = str(row.get("source_article") or "").strip()
            sentence_id = str(row.get("source_sentence") or "").strip()
            if article_id:
                digest = hashlib.sha256(
                    f"{source}\0article\0{article_id}".encode("utf-8")
                ).hexdigest()
                output.append({
                    "stable_id": f"ENREL-{digest[:32]}",
                    "source": source,
                    "target": article_id,
                    "relation_type": "vocabulary_observed_in_article",
                    "source_hash": source_hash,
                    "data_role": "formal_bank_relation",
                })
            if article_id and sentence_id:
                target = f"{article_id}:{sentence_id}"
                digest = hashlib.sha256(
                    f"{source}\0sentence\0{target}".encode("utf-8")
                ).hexdigest()
                output.append({
                    "stable_id": f"ENREL-{digest[:32]}",
                    "source": source,
                    "target": target,
                    "relation_type": "vocabulary_observed_in_sentence",
                    "source_hash": source_hash,
                    "data_role": "formal_bank_relation",
                })
        return sorted(output, key=lambda row: row["stable_id"])

    def luna_records(self, request: PagedReadRequest) -> list[dict[str, Any]]:
        """Return full saved English library collections before deterministic paging."""

        collection = request.collection
        allowed = {
            "article_catalog", "articles", "sentences", "vocabulary", "mastered_items",
            "patterns", "events", "raw_events", "effective_events", "search",
            "article_learning_catalog", "article_learning_pages", "relations",
        }
        if collection not in allowed:
            raise StudyReadError("INVALID_ARGUMENT", "unsupported English Luna collection")
        query = (request.query or "").strip()
        requested = set(request.ids)
        if collection == "search" and not query:
            raise StudyReadError("INVALID_ARGUMENT", "English search requires a query")
        if collection == "article_catalog":
            output = []
            for article_id, path in self._unique_article_entries():
                article, _ = self._article(article_id)
                stable_id = str(article.get("article_id") or article_id)
                if requested and stable_id not in requested and article_id not in requested:
                    continue
                output.append({
                    "stable_id": stable_id,
                    "source_hash": article["artifact_sha256"],
                    "requested_article_id": article_id,
                    "sentence_count": article["sentence_count"],
                    "source_role": article["source_role"],
                    "data_role": f"{article['data_role']}_catalog",
                })
            if query:
                output = [row for row in output if self._matches(row, query)]
            return sorted(output, key=lambda row: row["stable_id"])
        if collection == "articles":
            self._refresh_article_index()
            if request.article_id:
                entries = [(request.article_id, self._article_path(request.article_id))]
            else:
                entries = self._unique_article_entries()
            output = []
            for article_id, path in entries:
                article, _ = self._article(article_id)
                stable_id = str(article.get("article_id") or article_id)
                if requested and stable_id not in requested and article_id not in requested:
                    continue
                item = {
                    **article,
                    "stable_id": stable_id,
                    "source_hash": article["artifact_sha256"],
                    "content_complete": True,
                }
                item.pop("artifact_sha256", None)
                if query and not self._matches(item, query):
                    continue
                output.append(item)
            return sorted(output, key=lambda row: row["stable_id"])
        if collection == "sentences":
            self._refresh_article_index()
            if request.article_id:
                entries = [(request.article_id, self._article_path(request.article_id))]
            else:
                entries = self._unique_article_entries()
            output = []
            for article_id, path in entries:
                article, _ = self._article(article_id)
                canonical_article = str(article.get("article_id") or article_id)
                for index, sentence in enumerate(article["sentences"]):
                    sentence_id = str(sentence.get("sentence_id") or f"S{index + 1:04d}")
                    stable_id = f"{canonical_article}:{sentence_id}"
                    if requested and sentence_id not in requested and stable_id not in requested:
                        continue
                    text = str(sentence.get("text") or "")
                    source_hash = str(
                        sentence.get("source_hash")
                        or hashlib.sha256(text.encode()).hexdigest()
                    )
                    if source_hash.startswith("sha256:"):
                        source_hash = source_hash[7:]
                    output.append({
                        "stable_id": stable_id,
                        "article_id": canonical_article,
                        "sentence_id": sentence_id,
                        "text": text,
                        "source_hash": source_hash,
                        "source_role": sentence.get("source_role") or article.get("source_role"),
                        "data_role": sentence.get("data_role") or article.get("data_role"),
                    })
            if query:
                output = [row for row in output if self._matches(row, query)]
            return sorted(output, key=lambda row: row["stable_id"])
        if collection in {"vocabulary", "mastered_items"}:
            path = self.master_bank if collection == "vocabulary" else self.mastered_items
            rows, digest, _ = self.reader.csv_rows(path)
            output = []
            for index, row in enumerate(rows):
                item = str(row.get("item") or "")
                stable = str(
                    row.get("id") or row.get("matched_id")
                    or f"{collection}-{hashlib.sha256(f'{index}\0{item}'.encode()).hexdigest()[:24]}"
                )
                if requested and stable not in requested and item not in requested:
                    continue
                output.append({
                    "stable_id": stable,
                    "source_hash": digest,
                    **dict(row),
                    "data_role": "formal",
                })
            if query:
                output = [row for row in output if self._matches(row, query)]
            return output
        if collection == "patterns":
            output = [
                row for row in self._pattern_records()
                if not requested or row["stable_id"] in requested
            ]
            if query:
                output = [row for row in output if self._matches(row, query)]
            return output
        if collection in {"events", "raw_events", "effective_events"}:
            rows = self._all_event_records(request.study_date)
            if collection == "raw_events":
                rows = [
                    {
                        **row,
                        "event_payload": row["raw_event_payload"],
                        "source_hash": row["raw_object_sha256"],
                        "data_role": "raw_event",
                    }
                    for row in rows
                ]
            elif collection == "effective_events":
                rows = [row for row in rows if row.get("data_role") == "effective_event"]
            if requested:
                rows = [
                    row for row in rows
                    if row["stable_id"] in requested or row["event_id"] in requested
                ]
            if query:
                rows = [row for row in rows if self._matches(row, query)]
            return rows
        if collection in {"article_learning_catalog", "article_learning_pages"}:
            rows = self._learning_page_records(collection == "article_learning_catalog")
            if requested:
                rows = [
                    row for row in rows
                    if row["stable_id"] in requested or row.get("article_id") in requested
                ]
            if query:
                rows = [row for row in rows if self._matches(row, query)]
            return rows
        if collection == "relations":
            rows = self._relation_records()
            if requested:
                for value in requested:
                    SafeReader.validate_stable_id(value, "English relation endpoint id")
                rows = [
                    row for row in rows
                    if requested & {
                        str(row.get("stable_id") or ""), str(row.get("source") or ""),
                        str(row.get("target") or ""),
                    }
                ]
            if query:
                rows = [row for row in rows if self._matches(row, query)]
            return rows
        combined: list[dict[str, Any]] = []
        for nested in (
            "articles", "sentences", "vocabulary", "mastered_items", "patterns",
            "events", "article_learning_pages", "relations",
        ):
            nested_request = request.model_copy(
                update={"collection": nested, "ids": [], "cursor": None, "query": None}
            )
            for row in self.luna_records(nested_request):
                if self._matches(row, query):
                    combined.append({"matched_collection": nested, **row})
        return sorted(
            combined,
            key=lambda row: (str(row.get("matched_collection")), str(row.get("stable_id"))),
        )

    def _patterns(self, terms: list[str]) -> list[dict[str, Any]]:
        lowered = [(term, term.casefold()) for term in terms if term.strip()]
        return [
            {
                **record,
                "patterns_sha256": record["source_hash"],
            }
            for record in self._pattern_records()
            if not lowered
            or any(term in record["markdown"].casefold() for _, term in lowered)
        ]

    def read_bundle(self, request: EnglishRequest) -> dict[str, Any]:
        with self._lock:
            snapshot = self.authority()
            self.bind_expected(snapshot, request.expected_generation, request.expected_release, request.expected_fingerprint)
            tracked = {self.master_bank, self.patterns}
            before = self.capture_identities(tracked, self.reader)
            article: dict[str, Any] | None = None
            article_path: Path | None = None
            if request.article_id:
                article_path = self._article_path(request.article_id)
                tracked.add(article_path)
                before[article_path] = self.reader.identity(article_path)
                article, article_path = self._article(request.article_id)
            output: list[dict[str, Any]] = []
            include = set(request.include)
            if "article" in include and article is not None:
                output.append({"operation": "article", "items": [{k: v for k, v in article.items() if k != "sentences"}]})
            if "sentences" in include and article is not None:
                wanted = set(request.sentence_ids)
                sentences = article["sentences"]
                if wanted:
                    sentences = [row for row in sentences if row.get("sentence_id") in wanted]
                    missing = wanted - {row.get("sentence_id") for row in sentences}
                    if missing:
                        raise StudyReadError("NOT_FOUND", "one or more sentence ids were not found")
                output.append({"operation": "sentences", "items": sentences})
            if "vocab_status" in include:
                output.append({"operation": "vocab_status", "items": self._vocab(request.terms)})
            if "day_events" in include:
                day_paths = self._day_event_paths(request.study_date)
                day_dir = self._event_day_dirs.get(request.study_date.isoformat())
                if day_dir is not None:
                    tracked.add(day_dir)
                    before[day_dir] = self.reader.identity(day_dir)
                for path in day_paths:
                    tracked.add(path)
                    before[path] = self.reader.identity(path)
                events, paths = self._day_events(request.study_date)
                output.append({"operation": "day_events", "items": events})
            if "patterns" in include:
                output.append({"operation": "patterns", "items": self._patterns(request.terms)})
            if "coverage" in include and article is not None:
                rows, bank_sha, _ = self.reader.csv_rows(self.master_bank)
                source_ids = {str(article.get("article_id")), str(article.get("requested_article_id"))}
                linked = [row for row in rows if row.get("source_article") in source_ids]
                total = int(article.get("sentence_count") or 0)
                linked_sentences = {row.get("source_sentence") for row in linked if row.get("source_sentence")}
                output.append({"operation": "coverage", "items": [{
                    "article_id": article.get("article_id"), "sentence_count": total,
                    "bank_linked_rows": len(linked), "bank_linked_sentence_count": len(linked_sentences),
                    "coverage_ratio": (len(linked_sentences) / total if total else 0.0),
                    "data_role": "projection", "bank_sha256": bank_sha,
                }]})
            before.update({path: self.reader.identity(path) for path in tracked if path not in before})
            self.assert_unchanged(before, self.reader)
            rebound = self.authority()
            if rebound.fingerprint != snapshot.fingerprint:
                raise StudyReadError("SOURCE_CHANGED_DURING_READ", "English authority changed during bundle read", True)
            return success_envelope(
                subject=self.subject, data_role="mixed", authority_source_id=self.authority_source_id,
                generation=snapshot.generation, authority_fingerprint=snapshot.fingerprint, items=output,
                warnings=["article questions, answer keys, explanations, and protected source pages are never returned"],
            )
