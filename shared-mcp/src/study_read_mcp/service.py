from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any, Callable

from pydantic import ValidationError

from .adapters import CS408Adapter, EnglishAdapter, MathAdapter
from .capture import ArtifactPage, CaptureStore
from .config import RepositoryConfig
from .envelope import success_envelope
from .errors import StudyReadError
from .models import (
    CS408MorningPreparationRequest, CS408Query, EnglishRequest, EvidenceItem,
    MathQuery, PagedReadRequest, RouteContext,
)
from .morning import CS408MorningPreparationReader, canonical_scope_hash
from .paging import page_records, query_binding
from .preflight import InfrastructurePreflightSession, PreflightPolicy
from .preprocessor import PreprocessorAuthority
from .safeio import SafeReader
from .session import ReadSession


ALL_CAPABILITIES = frozenset(
    MathAdapter.capabilities | CS408Adapter.capabilities | EnglishAdapter.capabilities |
    {"authority", "evidence_hash", "preprocessor_release"}
)

SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PARSER_VERSIONS = {
    "math": "math-canonical-adapter.v2",
    "cs408": "cs408-canonical-adapter.v2",
    "english": "english-canonical-adapter.v2",
}

FOCUSED_COLLECTIONS = {
    "math": frozenset({
        "formal_card_catalog", "formal_card_records", "knowledge_catalog",
        "math_taxonomy_items", "activity",
    }),
    "cs408": frozenset({
        "formal_wrong_item_catalog", "formal_nodes", "formal_knowledge_catalog",
        "knowledge_nodes", "knowledge_safe_notes", "curation_inventory",
        "morning_sessions", "review_events",
    }),
    "english": frozenset({
        "article_catalog", "articles", "sentences", "vocabulary",
        "mastered_items", "patterns", "events", "raw_events", "effective_events",
        "article_learning_catalog", "article_learning_pages",
    }),
}

# Authority binding scans the subject's complete canonical source set.  A
# 500 ms budget was below the observed service time when many independent
# background workers opened read sessions at once, so valid read-only
# responses were converted into TIMEOUT envelopes under intended fan-out.
# Keep a fixed internal bound, but leave enough headroom for the supported
# 30-worker lane; the host still enforces its separate 30 second process cap.
AUTHORITY_DEADLINE_SECONDS = 5.0


def canonical_mcp_item_ref(
    *, subject: str, collection: str, stable_id: str,
    source_hash: str, generation: str,
) -> str:
    """Bind one model-visible item to its exact MCP query context.

    The compact reference is deliberately created by the service, not by an
    adapter.  A consumer can recompute it from the item plus the response and
    tool-call bindings without trusting model or adapter supplied metadata.
    """

    binding = {
        "collection": collection,
        "generation": generation,
        "source_hash": source_hash,
        "stable_id": stable_id,
    }
    digest = hashlib.sha256(
        json.dumps(
            binding, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    return f"mcp-item:{subject}:{digest}"


class StudyReadService:
    def __init__(
        self, config: RepositoryConfig | None = None, subjects: set[str] | None = None,
        profile: str = "ordinary", read_session: ReadSession | None = None,
        require_authority_snapshot: bool = False,
        preflight_session: InfrastructurePreflightSession | None = None,
        preflight_policy: PreflightPolicy | None = None,
    ) -> None:
        self.config = config or RepositoryConfig.production()
        self.subjects = subjects or {"math", "cs408", "english"}
        self.profile = profile
        self.read_session = read_session
        self.preflight_session = preflight_session
        self.preflight_policy = preflight_policy
        if self.profile not in {
            "ordinary",
            "background",
            "luna",
            "morning_preparation",
            "infrastructure_preflight",
        }:
            raise StudyReadError("INVALID_ARGUMENT", "profile is unsupported")
        if not self.subjects or not self.subjects <= {"math", "cs408", "english"}:
            raise StudyReadError("INVALID_ARGUMENT", "subjects must be a non-empty supported set")
        if self.profile in {
            "background",
            "luna",
            "morning_preparation",
            "infrastructure_preflight",
        } and len(self.subjects) != 1:
            raise StudyReadError("INVALID_ARGUMENT", "isolated profiles require one subject")
        if self.profile == "morning_preparation" and self.subjects != {"cs408"}:
            raise StudyReadError("INVALID_ARGUMENT", "morning preparation is 408-only")
        if self.profile == "luna":
            if read_session is None:
                raise StudyReadError("READ_SESSION_REQUIRED", "Luna profile requires a read session")
            if self.subjects != {read_session.subject}:
                raise StudyReadError("READ_SESSION_SUBJECT_MISMATCH", "read session subject is not enabled")
            if require_authority_snapshot and (
                not read_session.snapshot_bound
                or read_session.authority_snapshot_root is None
            ):
                raise StudyReadError(
                    "AUTHORITY_SNAPSHOT_REQUIRED",
                    "production Luna reads require a frozen authority snapshot",
                )
            if read_session.snapshot_bound and read_session.subject == "math":
                self.config = replace(
                    self.config, math_root=read_session.authority_snapshot_root
                )
            elif read_session.snapshot_bound and read_session.subject == "cs408":
                self.config = replace(
                    self.config, cs408_root=read_session.authority_snapshot_root
                )
            elif read_session.snapshot_bound:
                self.config = replace(
                    self.config, english_root=read_session.authority_snapshot_root
                )
        elif self.profile == "infrastructure_preflight":
            if (
                read_session is not None
                or preflight_session is None
                or preflight_policy is None
                or self.subjects != {preflight_session.subject}
            ):
                raise StudyReadError(
                    "PREFLIGHT_SESSION_REQUIRED",
                    "infrastructure preflight requires one subject-bound session and policy",
                )
            expected_root = {
                "math": self.config.math_root,
                "cs408": self.config.cs408_root,
                "english": self.config.english_root,
            }[preflight_session.subject].resolve(strict=True)
            try:
                active_release = (self.config.preprocessor_root / "current").resolve(
                    strict=True
                )
            except OSError as exc:
                raise StudyReadError(
                    "PREFLIGHT_CENTRAL_RELEASE_UNAVAILABLE",
                    "central deployed release is unavailable",
                ) from exc
            if (
                expected_root != preflight_session.read_root
                or active_release.name != preflight_session.central_release_id
            ):
                raise StudyReadError(
                    "PREFLIGHT_CENTRAL_BINDING_MISMATCH",
                    "infrastructure preflight central/read-root binding is invalid",
                )
        elif read_session is not None or preflight_session is not None:
            raise StudyReadError("READ_SESSION_PROFILE_MISMATCH", "read sessions are only valid for Luna")
        self.adapters: dict[str, Any] = {}
        self.adapter_errors: dict[str, str] = {}
        self._adapter_lock = threading.RLock()
        self._factories: dict[str, Callable[[], Any]] = {
            "math": lambda: MathAdapter(self.config.math_root),
            "cs408": lambda: CS408Adapter(self.config.cs408_root),
            "english": lambda: EnglishAdapter(self.config.english_root),
        }
        self.morning_reader = (
            CS408MorningPreparationReader(self.config.cs408_root)
            if self.profile == "morning_preparation"
            else None
        )
        try:
            self.preprocessor = PreprocessorAuthority(self.config.preprocessor_root)
        except Exception:
            self.preprocessor = None
        self.capture_store: CaptureStore | None = None
        if self.profile == "luna" and self.read_session is not None and self.read_session.capture_bound:
            self.capture_store = CaptureStore(self.config.preprocessor_root, self.read_session)

    def close(self) -> None:
        adapter = self.adapters.get("cs408")
        if adapter is not None:
            adapter.close()

    def _adapter(self, subject: str) -> Any:
        if subject not in self.subjects:
            raise StudyReadError("INVALID_ARGUMENT", "subject is not enabled in this profile")
        if subject in self.adapter_errors:
            raise StudyReadError("SUBJECT_UNAVAILABLE", self.adapter_errors.get(subject, "subject adapter is unavailable"), True)
        with self._adapter_lock:
            adapter = self.adapters.get(subject)
            if adapter is not None:
                return adapter
            try:
                adapter = self._factories[subject]()
            except Exception as exc:
                self.adapter_errors[subject] = "adapter initialization failed safely"
                raise StudyReadError("SUBJECT_UNAVAILABLE", self.adapter_errors[subject], True) from exc
            self.adapters[subject] = adapter
            return adapter

    @staticmethod
    def _deadline(started: float, seconds: float) -> None:
        if time.monotonic() - started > seconds:
            raise StudyReadError("TIMEOUT", "read exceeded its fixed internal deadline", True)

    def _route(self, route_context: RouteContext | None, subject: str | None = None) -> dict[str, Any]:
        if route_context is None:
            if self.profile in {"background", "luna", "morning_preparation"}:
                raise StudyReadError("ROUTE_CONTEXT_REQUIRED", "isolated reads require a bound route context")
            return {
                "caller_skill_id": "unattributed-legacy",
                "caller_skill_version": "legacy",
                "plugin_version": "legacy",
                "route_request_id": "legacy-unattributed",
                "evidence_scope_hash": "0" * 64,
                "read_route": "mcp",
                "chunk_index": 1,
                "chunk_count": 1,
                "consumed_duplicate_read_count": 0,
            }
        value = route_context.model_dump(mode="json")
        if self.profile in {"background", "luna"} and subject is not None:
            expected = f"background-{subject}-processing"
            if route_context.caller_skill_id != expected:
                raise StudyReadError("ROUTE_SCOPE_MISMATCH", "background caller skill does not match subject")
        return value

    def cs408_morning_preparation_bundle(
        self,
        raw_request: dict[str, Any],
        route_context: RouteContext | None = None,
        **expected: Any,
    ) -> dict[str, Any]:
        if self.profile != "morning_preparation" or self.morning_reader is None:
            raise StudyReadError("PROFILE_MISMATCH", "morning preparation tool requires its isolated profile")
        started = time.monotonic()
        route = self._route(route_context, "cs408")
        if route_context is None or route_context.caller_skill_id != "kaoyan-408-morning-control":
            raise StudyReadError("ROUTE_SCOPE_MISMATCH", "morning preparation caller is not authorized")
        try:
            request = CS408MorningPreparationRequest.model_validate(raw_request)
        except ValidationError as exc:
            raise StudyReadError("INVALID_ARGUMENT", "morning preparation request schema is invalid") from exc
        expected_scope = canonical_scope_hash(
            request.review_date.isoformat(), request.queue_sha256, request.item_ids
        )
        if route_context.evidence_scope_hash != expected_scope:
            raise StudyReadError("ROUTE_SCOPE_MISMATCH", "morning preparation evidence scope is not bound")
        result = self.morning_reader.read(request, route=route, **expected)
        self._deadline(started, 2.0)
        return result

    @staticmethod
    def _model_visible_items(
        *, subject: str, collection: str, generation: str,
        items: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, dict):
                raise StudyReadError(
                    "INTERNAL_SAFE", "Luna adapter returned a non-object item"
                )
            if "evidence_ref" in item:
                raise StudyReadError(
                    "INTERNAL_SAFE",
                    "Luna adapters must not provide canonical evidence references",
                )
            stable_id = item.get("stable_id")
            source_hash = item.get("source_hash")
            data_role = item.get("data_role")
            if not isinstance(stable_id, str):
                raise StudyReadError(
                    "INTERNAL_SAFE", "Luna adapter item has no stable id"
                )
            SafeReader.validate_stable_id(stable_id, "Luna item stable id")
            if not isinstance(source_hash, str) or not SHA256_RE.fullmatch(source_hash):
                raise StudyReadError(
                    "INTERNAL_SAFE", "Luna adapter item has no canonical source hash"
                )
            if not isinstance(data_role, str) or not data_role:
                raise StudyReadError(
                    "INTERNAL_SAFE", "Luna adapter item has no explicit data role"
                )
            evidence_ref = canonical_mcp_item_ref(
                subject=subject,
                collection=collection,
                stable_id=stable_id,
                source_hash=source_hash,
                generation=generation,
            )
            if len(evidence_ref) > 320:
                raise StudyReadError(
                    "INTERNAL_SAFE", "canonical evidence reference exceeds its bound"
                )
            output.append({
                **item,
                "collection": collection,
                "parser_version": PARSER_VERSIONS[subject],
                "evidence_ref": evidence_ref,
            })
        return output

    def luna_read(self, subject: str, raw_request: dict[str, Any]) -> dict[str, Any]:
        if self.profile != "luna" or self.read_session is None:
            raise StudyReadError("INVALID_ARGUMENT", "model-driven reads require the Luna profile")
        if subject != self.read_session.subject or subject not in self.subjects:
            raise StudyReadError("READ_SESSION_SUBJECT_MISMATCH", "cross-subject reads are forbidden")
        try:
            request = PagedReadRequest.model_validate(raw_request)
        except ValidationError as exc:
            raise StudyReadError("INVALID_ARGUMENT", "Luna page request is invalid") from exc
        adapter = self._adapter(subject)
        before = adapter.authority()
        adapter.bind_expected(
            before,
            self.read_session.generation,
            self.read_session.mcp_server_release,
            self.read_session.authority_fingerprint,
        )
        query_core = request.model_dump(mode="json", exclude={"cursor"})
        query_sha256 = query_binding(
            {
                "read_session_id": self.read_session.read_session_id,
                "subject": subject,
                "query": query_core,
            }
        )
        records = adapter.luna_records(request)
        page_items, page = page_records(
            records,
            page_size=request.page_size,
            cursor=request.cursor,
            query_sha256=query_sha256,
            session_sha256=self.read_session.manifest_sha256,
            generation=before.generation,
        )
        page_items = self._model_visible_items(
            subject=subject,
            collection=request.collection,
            generation=before.generation,
            items=page_items,
        )
        after = adapter.authority()
        if (
            after.generation != before.generation
            or after.fingerprint != before.fingerprint
            or after.release != before.release
        ):
            raise StudyReadError(
                "SOURCE_CHANGED_DURING_READ",
                "subject authority changed during the model-driven read",
                True,
            )
        read_session_payload = self._public_read_session(
            subject=subject,
            generation=before.generation,
            authority_fingerprint=before.fingerprint,
        )
        result = success_envelope(
            subject=subject,
            data_role="model_selected_read_page",
            authority_source_id=adapter.authority_source_id,
            generation=before.generation,
            authority_fingerprint=before.fingerprint,
            items=page_items,
            warnings=[
                "The model selected this collection and page; no host-side ranking or neighbor preselection was applied."
            ],
            route_context=self.read_session.route(),
            profile="luna",
            page=page,
            read_session=read_session_payload,
            preprocessor_release=self.read_session.candidate_release_id,
        )
        result["mcp_tool_call_count"] = 1
        return result

    def _focused_session(self, subject: str) -> tuple[ReadSession, CaptureStore]:
        if self.profile != "luna" or self.read_session is None:
            raise StudyReadError("INVALID_ARGUMENT", "focused reads require the Luna profile")
        if subject != self.read_session.subject or subject not in self.subjects:
            raise StudyReadError("READ_SESSION_SUBJECT_MISMATCH", "cross-subject reads are forbidden")
        if not self.read_session.capture_bound or self.capture_store is None:
            raise StudyReadError(
                "CAPTURE_BINDING_REQUIRED",
                "focused Luna tools require a capture-bound read-session v2",
            )
        return self.read_session, self.capture_store

    def _public_read_session(
        self, *, subject: str, generation: str, authority_fingerprint: str,
    ) -> dict[str, Any]:
        if self.read_session is None:
            raise StudyReadError(
                "READ_SESSION_REQUIRED", "Luna profile requires a read session"
            )
        payload = {
            "schema_version": self.read_session.schema_version,
            "read_session_id": self.read_session.read_session_id,
            "manifest_sha256": self.read_session.manifest_sha256,
            "candidate_release_id": self.read_session.candidate_release_id,
            "plugin_version": self.read_session.plugin_version,
            "subject": subject,
            "generation": generation,
            "authority_fingerprint": authority_fingerprint,
            "skill_id": self.read_session.skill_id,
            "skill_version": self.read_session.skill_version,
            "mcp_server_release": self.read_session.mcp_server_release,
            "formal_write_count": 0,
        }
        if self.read_session.snapshot_bound:
            payload.update({
                "authority_snapshot_manifest_sha256": (
                    self.read_session.authority_snapshot_manifest_sha256
                ),
                "authority_snapshot_receipt_sha256": (
                    self.read_session.authority_snapshot_receipt_sha256
                ),
            })
        if self.read_session.capture_bound and self.capture_store is not None:
            payload.update({
                "capture_id": self.capture_store.manifest.capture_id,
                "capture_manifest_sha256": self.capture_store.manifest.manifest_sha256,
                "artifact_ids": list(self.read_session.artifact_ids),
            })
        return payload

    def _preflight_authority(self, subject: str) -> tuple[Any, Any]:
        session = self.preflight_session
        policy = self.preflight_policy
        if (
            self.profile != "infrastructure_preflight"
            or session is None
            or policy is None
            or subject != session.subject
            or subject not in self.subjects
        ):
            raise StudyReadError(
                "PREFLIGHT_SESSION_REQUIRED",
                "infrastructure preflight is not bound to this subject",
            )
        adapter = self._adapter(subject)
        snapshot = adapter.authority()
        if snapshot.release != session.mcp_server_release:
            raise StudyReadError(
                "PREFLIGHT_RELEASE_BINDING_MISMATCH",
                "subject adapter release does not match the preflight session",
            )
        return adapter, snapshot

    def _preflight_envelope(
        self,
        *,
        subject: str,
        data_role: str,
        snapshot: Any,
        items: list[dict[str, Any]],
        page: dict[str, Any] | None = None,
        warnings: list[str] | None = None,
    ) -> dict[str, Any]:
        session = self.preflight_session
        if session is None:
            raise StudyReadError(
                "PREFLIGHT_SESSION_REQUIRED",
                "infrastructure preflight session is unavailable",
            )
        result = success_envelope(
            subject=subject,
            data_role=data_role,
            authority_source_id=self._adapter(subject).authority_source_id,
            generation=snapshot.generation,
            authority_fingerprint=snapshot.fingerprint,
            items=items,
            warnings=warnings or [],
            route_context=session.route(),
            profile="infrastructure_preflight",
            page=page,
            preprocessor_release=session.central_release_id,
        )
        result.update(
            {
                "preflight_session": session.public_binding(),
                "candidate_eligible": False,
                "production_evidence": False,
                "formal_write_allowed": False,
                "write_call_count": 0,
                "mcp_tool_call_count": 1,
            }
        )
        return result

    def _preflight_library_read(
        self, subject: str, request: PagedReadRequest, data_role: str,
    ) -> dict[str, Any]:
        if request.collection not in FOCUSED_COLLECTIONS[subject] | {"search"}:
            raise StudyReadError(
                "INVALID_ARGUMENT",
                "collection is not exposed by this subject preflight facade",
            )
        session = self.preflight_session
        if session is None:
            raise StudyReadError(
                "PREFLIGHT_SESSION_REQUIRED",
                "infrastructure preflight session is unavailable",
            )
        adapter, before = self._preflight_authority(subject)
        query_core = request.model_dump(mode="json", exclude={"cursor"})
        query_sha256 = query_binding(
            {
                "preflight_session_id": session.preflight_session_id,
                "subject": subject,
                "query": query_core,
            }
        )
        records = adapter.luna_records(request)
        page_items, page = page_records(
            records,
            page_size=request.page_size,
            cursor=request.cursor,
            query_sha256=query_sha256,
            session_sha256=session.manifest_sha256,
            generation=before.generation,
        )
        page_items = self._model_visible_items(
            subject=subject,
            collection=request.collection,
            generation=before.generation,
            items=page_items,
        )
        after = adapter.authority()
        if after != before:
            raise StudyReadError(
                "SOURCE_CHANGED_DURING_READ",
                "subject authority changed during infrastructure preflight",
                True,
            )
        return self._preflight_envelope(
            subject=subject,
            data_role=data_role,
            snapshot=before,
            items=page_items,
            page=page,
            warnings=[
                "Infrastructure preflight output is non-candidate, non-production evidence."
            ],
        )

    def _bound_authority(self, subject: str) -> tuple[Any, Any]:
        session, _ = self._focused_session(subject)
        adapter = self._adapter(subject)
        snapshot = adapter.authority()
        adapter.bind_expected(
            snapshot, session.generation, session.mcp_server_release,
            session.authority_fingerprint,
        )
        return adapter, snapshot

    def _focused_envelope(
        self, *, subject: str, data_role: str, snapshot: Any,
        items: list[dict[str, Any]], page: dict[str, Any] | None = None,
        warnings: list[str] | None = None,
    ) -> dict[str, Any]:
        session, _ = self._focused_session(subject)
        result = success_envelope(
            subject=subject,
            data_role=data_role,
            authority_source_id=self._adapter(subject).authority_source_id,
            generation=snapshot.generation,
            authority_fingerprint=snapshot.fingerprint,
            items=items,
            warnings=warnings or [],
            route_context=session.route(),
            profile="luna",
            page=page,
            read_session=self._public_read_session(
                subject=subject,
                generation=snapshot.generation,
                authority_fingerprint=snapshot.fingerprint,
            ),
            preprocessor_release=session.candidate_release_id,
        )
        result["mcp_tool_call_count"] = 1
        return result

    def get_task_context(self, subject: str) -> dict[str, Any]:
        session, store = self._focused_session(subject)
        adapter, before = self._bound_authority(subject)
        store.verify_all()
        manifest = store.manifest
        item = {
            "stable_id": manifest.capture_id,
            "collection": "task_context",
            "capture_id": manifest.capture_id,
            "subject": manifest.subject,
            "study_date": manifest.study_date,
            "scene": manifest.scene,
            "captured_at": manifest.captured_at,
            "identity": manifest.identity,
            "artifacts": [artifact.public_index() for artifact in manifest.artifacts],
            "source_hash": manifest.manifest_sha256,
            "data_role": "immutable_capture_identity",
            "evidence_ref": canonical_mcp_item_ref(
                subject=subject, collection="task_context",
                stable_id=manifest.capture_id, source_hash=manifest.manifest_sha256,
                generation=before.generation,
            ),
        }
        after = adapter.authority()
        if after != before:
            raise StudyReadError(
                "SOURCE_CHANGED_DURING_READ", "subject authority changed during task context read", True
            )
        return self._focused_envelope(
            subject=subject, data_role="immutable_task_context", snapshot=before,
            items=[item],
            page={
                "total_count": 1,
                "returned_count": 1,
                "offset": 0,
                "page_size": 1,
                "next_cursor": None,
                "truncated": False,
                "complete": True,
                "query_sha256": query_binding({
                    "read_session_id": session.read_session_id,
                    "subject": subject,
                    "operation": "get_task_context",
                }),
            },
            warnings=[
                "Task context contains immutable identity and an artifact index only; use read_task_artifact for content."
            ],
        )

    def read_task_artifact(
        self, subject: str, artifact_id: str, cursor: str | None = None,
        max_bytes: int = 16_384,
    ) -> tuple[dict[str, Any], ArtifactPage]:
        _, store = self._focused_session(subject)
        SafeReader.validate_stable_id(artifact_id, "artifact id")
        adapter, before = self._bound_authority(subject)
        artifact_page = store.read_artifact(artifact_id, cursor, max_bytes)
        item = dict(artifact_page.envelope_item)
        item["collection"] = "task_artifact"
        item["stable_id"] = artifact_id
        item["source_hash"] = str(item["sha256"])
        item["evidence_ref"] = canonical_mcp_item_ref(
            subject=subject, collection="task_artifact", stable_id=artifact_id,
            source_hash=str(item["source_hash"]), generation=before.generation,
        )
        after = adapter.authority()
        if after != before:
            raise StudyReadError(
                "SOURCE_CHANGED_DURING_READ", "subject authority changed during artifact read", True
            )
        envelope = self._focused_envelope(
            subject=subject, data_role="immutable_task_artifact", snapshot=before,
            items=[item], page=artifact_page.page,
        )
        envelope["content_return_bytes"] = (
            len(artifact_page.image_bytes)
            if artifact_page.image_bytes is not None
            else int(artifact_page.page["returned_count"])
        )
        return envelope, artifact_page

    def _focused_library_read(
        self, subject: str, request: PagedReadRequest, data_role: str,
    ) -> dict[str, Any]:
        if self.profile == "infrastructure_preflight":
            return self._preflight_library_read(
                subject, request, f"preflight_{data_role}"
            )
        self._focused_session(subject)
        if request.collection not in FOCUSED_COLLECTIONS[subject] | {"search"}:
            raise StudyReadError("INVALID_ARGUMENT", "collection is not exposed by this subject facade")
        result = self.luna_read(subject, request.model_dump(mode="json"))
        result["data_role"] = data_role
        return result

    def list_records(
        self, subject: str, collection: str, cursor: str | None = None,
        page_size: int = 24, article_id: str | None = None,
        study_date: date | None = None,
    ) -> dict[str, Any]:
        return self._focused_library_read(
            subject,
            PagedReadRequest(
                collection=collection, cursor=cursor, page_size=page_size,
                article_id=article_id, study_date=study_date,
            ),
            "subject_catalog_page",
        )

    def get_records(
        self, subject: str, collection: str, ids: list[str],
        cursor: str | None = None, page_size: int = 24,
    ) -> dict[str, Any]:
        if not ids:
            raise StudyReadError("INVALID_ARGUMENT", "get_records requires at least one stable id")
        if self.profile == "infrastructure_preflight":
            adapter, snapshot = self._preflight_authority(subject)
        else:
            session, _ = self._focused_session(subject)
            adapter = self._adapter(subject)
            snapshot = adapter.authority()
            adapter.bind_expected(
                snapshot, session.generation, session.mcp_server_release,
                session.authority_fingerprint,
            )
        probe = PagedReadRequest(collection=collection, ids=ids, page_size=48)
        if collection not in FOCUSED_COLLECTIONS[subject]:
            raise StudyReadError("INVALID_ARGUMENT", "collection is not exposed by this subject facade")
        found = {
            str(row.get("stable_id")) for row in adapter.luna_records(probe)
            if isinstance(row.get("stable_id"), str)
        }
        missing = set(ids) - found
        if missing:
            raise StudyReadError("NOT_FOUND", "one or more exact stable ids were not found")
        return self._focused_library_read(
            subject,
            PagedReadRequest(
                collection=collection, ids=ids, cursor=cursor, page_size=page_size,
            ),
            "subject_exact_records_page",
        )

    def search_records(
        self, subject: str, query: str, cursor: str | None = None,
        page_size: int = 24,
    ) -> dict[str, Any]:
        return self._focused_library_read(
            subject,
            PagedReadRequest(
                collection="search", query=query, cursor=cursor, page_size=page_size,
            ),
            "subject_search_page",
        )

    def query_relations(
        self, subject: str, ids: list[str], relation_types: list[str] | None = None,
        include_candidates: bool = False, cursor: str | None = None,
        page_size: int = 24,
    ) -> dict[str, Any]:
        if self.profile == "infrastructure_preflight":
            preflight_session = self.preflight_session
            if preflight_session is None:
                raise StudyReadError(
                    "PREFLIGHT_SESSION_REQUIRED",
                    "infrastructure preflight session is unavailable",
                )
            session_id = preflight_session.preflight_session_id
            session_sha256 = preflight_session.manifest_sha256
        else:
            session, _ = self._focused_session(subject)
            session_id = session.read_session_id
            session_sha256 = session.manifest_sha256
        if not ids:
            raise StudyReadError("INVALID_ARGUMENT", "query_relations requires at least one endpoint id")
        if len(ids) > 48 or len(ids) != len(set(ids)):
            raise StudyReadError("INVALID_ARGUMENT", "relation endpoint ids are invalid")
        for value in ids:
            SafeReader.validate_stable_id(value, "relation endpoint id")
        relation_types = relation_types or []
        if len(relation_types) > 24 or len(relation_types) != len(set(relation_types)):
            raise StudyReadError("INVALID_ARGUMENT", "relation types are invalid")
        adapter, before = (
            self._preflight_authority(subject)
            if self.profile == "infrastructure_preflight"
            else self._bound_authority(subject)
        )
        collections = {
            "math": ["formal_relation_declarations"] + (["relationship_candidates"] if include_candidates else []),
            "cs408": ["formal_relationships"],
            "english": ["relations"],
        }[subject]
        records: list[dict[str, Any]] = []
        for collection in collections:
            request = PagedReadRequest(collection=collection, ids=ids, page_size=48)
            for row in adapter.luna_records(request):
                if relation_types and str(row.get("relation_type") or "") not in relation_types:
                    continue
                records.append({"matched_collection": collection, **row})
        records.sort(key=lambda row: (str(row.get("matched_collection")), str(row.get("stable_id"))))
        binding = query_binding({
            "read_session_id": session_id,
            "subject": subject,
            "operation": "query_relations",
            "ids": ids,
            "relation_types": relation_types,
            "include_candidates": include_candidates,
        })
        page_items, page = page_records(
            records, page_size=page_size, cursor=cursor, query_sha256=binding,
            session_sha256=session_sha256, generation=before.generation,
        )
        page_items = self._model_visible_items(
            subject=subject, collection="relations", generation=before.generation,
            items=page_items,
        )
        after = adapter.authority()
        if after != before:
            raise StudyReadError(
                "SOURCE_CHANGED_DURING_READ", "subject authority changed during relation read", True
            )
        if self.profile == "infrastructure_preflight":
            return self._preflight_envelope(
                subject=subject,
                data_role="preflight_subject_relation_page",
                snapshot=before,
                items=page_items,
                page=page,
            )
        return self._focused_envelope(
            subject=subject, data_role="subject_relation_page", snapshot=before,
            items=page_items, page=page,
        )

    def authority_bundle(
        self, subjects: list[str], capabilities: list[str],
        expected_generation: dict[str, str] | None = None,
        expected_release: dict[str, str] | None = None,
        expected_fingerprint: dict[str, str] | None = None,
        checks: list[str] | None = None, route_context: RouteContext | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        route = self._route(route_context)
        if not 1 <= len(subjects) <= 3 or len(set(subjects)) != len(subjects):
            raise StudyReadError("INVALID_ARGUMENT", "subjects must contain one to three unique values")
        if not 1 <= len(capabilities) <= 16 or not set(capabilities) <= ALL_CAPABILITIES:
            raise StudyReadError("INVALID_ARGUMENT", "capabilities contain an unsupported value")
        allowed_checks = {"exists", "parseable", "release_bound", "projection_bound"}
        if checks and (len(checks) > 8 or not set(checks) <= allowed_checks):
            raise StudyReadError("INVALID_ARGUMENT", "checks contain an unsupported value")
        for mapping in (expected_generation, expected_release, expected_fingerprint):
            if mapping is not None and (not set(mapping) <= set(subjects) or any(not isinstance(value, str) for value in mapping.values())):
                raise StudyReadError("INVALID_ARGUMENT", "expected bindings must be strings keyed by requested subject")
        items: list[dict[str, Any]] = []
        fingerprints: list[tuple[str, str]] = []
        generations: list[str] = []
        for subject in subjects:
            if subject not in {"math", "cs408", "english"} or subject not in self.subjects:
                raise StudyReadError("INVALID_ARGUMENT", "subject is unsupported or disabled")
            try:
                adapter = self._adapter(subject)
            except StudyReadError as exc:
                items.append({"subject": subject, "available": False, "error_code": "SUBJECT_UNAVAILABLE", "capabilities": []})
                continue
            snapshot = adapter.authority()
            adapter.bind_expected(
                snapshot, (expected_generation or {}).get(subject), (expected_release or {}).get(subject),
                (expected_fingerprint or {}).get(subject),
            )
            requested = sorted(set(capabilities) & (adapter.capabilities | {"authority", "evidence_hash", "preprocessor_release"}))
            items.append({
                "subject": subject, "available": True, "generation": snapshot.generation,
                "adapter_release": snapshot.release, "authority_fingerprint": snapshot.fingerprint,
                "capabilities": requested, "sources": list(snapshot.sources),
                "checks": adapter.authority_checks(checks or []),
            })
            fingerprints.append((subject, snapshot.fingerprint))
            generations.append(snapshot.generation)
        preprocessor_release = None
        if "preprocessor_release" in capabilities:
            if self.preprocessor is None:
                items.append({"subject": "shared", "capability": "preprocessor_release", "available": False, "error_code": "SUBJECT_UNAVAILABLE"})
            else:
                binding = self.preprocessor.bind_current()
                preprocessor_release = binding.release_id
                items.append({
                    "subject": "shared", "capability": "preprocessor_release", "available": True,
                    "release_id": binding.release_id, "manifest_sha256": binding.manifest_sha256,
                    "data_role": "release_manifest",
                })
                fingerprints.append(("preprocessor", binding.manifest_sha256))
        self._deadline(started, AUTHORITY_DEADLINE_SECONDS)
        fingerprint = SafeReader.set_fingerprint(fingerprints)
        generation = "multi-" + hashlib.sha256("\n".join(sorted(generations)).encode()).hexdigest()[:20]
        return success_envelope(
            subject="multi", data_role="authority", authority_source_id="study.authority-core",
            generation=generation, authority_fingerprint=fingerprint, preprocessor_release=preprocessor_release,
            items=items, output_limit=32_768, route_context=route, profile=self.profile,
        )

    def _resolve_evidence(self, item: EvidenceItem) -> tuple[Path, str, str, str | None]:
        subject, kind, stable_id = item.subject, item.artifact_kind, item.stable_id
        adapter = self._adapter(subject)
        role = "formal"
        manifest_binding = None
        if kind == "package_object":
            if self.preprocessor is None:
                raise StudyReadError("SUBJECT_UNAVAILABLE", "preprocessor authority is unavailable")
            path, digest, binding = self.preprocessor.package_object(stable_id)
            return path, digest, "candidate_metadata_only", binding.manifest_sha256
        if subject == "math":
            if kind == "formal_card":
                _, path = adapter._formal_card(stable_id, [])
            elif kind == "wrong_questions_projection" and stable_id == "current":
                path, role = adapter.projection, "projection"
            elif kind == "capture_ledger" and stable_id == "current":
                path, role = adapter.capture_ledger, "event"
            elif kind == "review_ledger" and stable_id == "current":
                path, role = adapter.review_ledger, "event"
            else:
                raise StudyReadError("INVALID_ARGUMENT", "unsupported math artifact kind or id")
            digest, _ = adapter.reader.digest(path)
        elif subject == "cs408":
            if kind == "knowledge_node":
                _, path = adapter._knowledge_node(stable_id)
            elif kind == "curation_state" and stable_id == "current":
                path, role = adapter.curation_state, "projection"
                _, binding = adapter._curation_projection()
                manifest_binding = binding["event_ledger_sha256"]
            elif kind == "hot_manifest" and stable_id == "current":
                path, role = adapter.hot_manifest, "projection"
            elif kind == "projection_db" and stable_id == "current":
                _, manifest, path = adapter._connect_projection()
                role = "projection"
                manifest_binding = manifest.get("manifest_sha256")
            else:
                raise StudyReadError("INVALID_ARGUMENT", "unsupported 408 artifact kind or id")
            digest, _ = adapter.reader.digest(path)
        elif subject == "english":
            if kind == "article":
                path = adapter._article_path(stable_id)
            elif kind == "master_bank" and stable_id == "current":
                path = adapter.master_bank
            elif kind == "sentence_patterns" and stable_id == "current":
                path = adapter.patterns
            elif kind == "event":
                try:
                    day, event_id = stable_id.split(":", 1)
                    parsed = date.fromisoformat(day)
                except (ValueError, AttributeError) as exc:
                    raise StudyReadError("INVALID_ARGUMENT", "English event id must be DATE:EVENT_ID") from exc
                SafeReader.validate_stable_id(event_id, "event id")
                paths = {path.stem: path for path in adapter._day_event_paths(parsed)}
                path = paths.get(event_id)
                if path is None:
                    raise StudyReadError("NOT_FOUND", "English event was not found")
                role = "event"
            else:
                raise StudyReadError("INVALID_ARGUMENT", "unsupported English artifact kind or id")
            digest, _ = adapter.reader.digest(path)
        else:
            raise StudyReadError("INVALID_ARGUMENT", "unsupported subject")
        return path, digest, role, manifest_binding

    def verify_evidence_batch(
        self, raw_items: list[dict[str, Any]], expected_generation: dict[str, str] | None = None,
        expected_release: dict[str, str] | None = None, route_context: RouteContext | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        route = self._route(route_context)
        if not 1 <= len(raw_items) <= 64:
            raise StudyReadError("INVALID_ARGUMENT", "evidence item count must be between 1 and 64")
        try:
            items = [EvidenceItem.model_validate(value) for value in raw_items]
        except ValidationError as exc:
            raise StudyReadError("INVALID_ARGUMENT", "evidence item schema is invalid") from exc
        snapshots = {}
        for subject in {item.subject for item in items}:
            adapter = self._adapter(subject)
            snapshot = adapter.authority()
            adapter.bind_expected(snapshot, (expected_generation or {}).get(subject), (expected_release or {}).get(subject), None)
            snapshots[subject] = snapshot
        output = []
        for item in items:
            _, digest, role, manifest = self._resolve_evidence(item)
            matches = item.expected_sha256 is None or item.expected_sha256 == digest
            if not matches:
                raise StudyReadError("HASH_MISMATCH", "one or more evidence hashes do not match")
            output.append({
                "subject": item.subject, "artifact_kind": item.artifact_kind, "stable_id": item.stable_id,
                "exists": True, "declared_sha256": item.expected_sha256, "actual_sha256": digest,
                "hash_matches": matches, "data_role": role, "generation": snapshots[item.subject].generation,
                "manifest_binding": manifest,
            })
        self._deadline(started, 2.0)
        fingerprint = SafeReader.set_fingerprint((subject, snap.fingerprint) for subject, snap in snapshots.items())
        generation = "multi-" + hashlib.sha256("\n".join(sorted(s.generation for s in snapshots.values())).encode()).hexdigest()[:20]
        return success_envelope(
            subject="multi", data_role="evidence_verification", authority_source_id="study.evidence-core",
            generation=generation, authority_fingerprint=fingerprint, items=output, output_limit=65_536,
            route_context=route, profile=self.profile,
        )

    def math_read_bundle(
        self, raw_queries: list[dict[str, Any]], route_context: RouteContext | None = None,
        **expected: Any,
    ) -> dict[str, Any]:
        started = time.monotonic()
        route = self._route(route_context, "math")
        try:
            queries = [MathQuery.model_validate(value) for value in raw_queries]
        except ValidationError as exc:
            raise StudyReadError("INVALID_ARGUMENT", "math query schema is invalid") from exc
        result = self._adapter("math").read_bundle(queries, **expected)
        result["read_route"] = route
        result["profile"] = self.profile
        self._deadline(started, 2.0)
        return result

    def cs408_read_bundle(
        self, raw_queries: list[dict[str, Any]], route_context: RouteContext | None = None,
        **expected: Any,
    ) -> dict[str, Any]:
        started = time.monotonic()
        route = self._route(route_context, "cs408")
        try:
            queries = [CS408Query.model_validate(value) for value in raw_queries]
        except ValidationError as exc:
            raise StudyReadError("INVALID_ARGUMENT", "408 query schema is invalid") from exc
        result = self._adapter("cs408").read_bundle(queries, **expected)
        result["read_route"] = route
        result["profile"] = self.profile
        self._deadline(started, 2.0)
        return result

    def english_read_bundle(
        self, raw_request: dict[str, Any], route_context: RouteContext | None = None,
    ) -> dict[str, Any]:
        started = time.monotonic()
        route = self._route(route_context, "english")
        try:
            request = EnglishRequest.model_validate(raw_request)
        except ValidationError as exc:
            raise StudyReadError("INVALID_ARGUMENT", "English request schema is invalid") from exc
        result = self._adapter("english").read_bundle(request)
        result["read_route"] = route
        result["profile"] = self.profile
        self._deadline(started, 2.0)
        return result
