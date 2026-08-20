from __future__ import annotations

import fcntl
import json
import os
import re
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .constants import EVIDENCE_STATES, USER_EVIDENCE
from .errors import IdempotencyConflict, SourceHashMismatch, ValidationError
from .producer_binding_attestation import (
    ProducerBindingError,
    publish_attestation,
)
from .review_status import canonical_machine_decision, exact_item_occurrences
from .util import atomic_write_json, canonical_bytes, object_sha256, parse_iso_date, sentence_sha256, utc_now


EVENT_SCHEMA_VERSION = "english_capture_event_v2"
LEGACY_EVENT_SCHEMA_VERSION = "english_capture_event_v1"
CAPTURE_RECEIPT_VERSION = "english_capture_receipt_v2"
PRODUCER_BINDING_DESCRIPTOR_PATH = (
    Path(__file__).resolve().parents[1]
    / "schema"
    / "english_pipeline"
    / "producer-binding-v1.json"
)

# Capture is a producer-owned, release-neutral fact object.  These names are
# the deployment and runtime identity fields used by the Dispatcher, canary,
# release migration and MCP authority contracts; none may be smuggled into a
# Capture through an otherwise legal nested object.
RELEASE_NEUTRAL_FORBIDDEN_KEYS = frozenset(
    {
        "release_id",
        "source_release_id",
        "target_release_id",
        "base_release_id",
        "candidate_release_id",
        "mcp_release_id",
        "mcp_server_release",
        "activation_id",
        "source_activation_id",
        "target_activation_id",
        "authority",
        "authority_fingerprint",
        "authority_generation",
        "producer_authority",
        "producer_authority_fingerprint",
        "dispatcher_authority",
        "dispatcher_authority_fingerprint",
        "dispatcher_authority_generation",
        "mcp_authority",
        "mcp_authority_fingerprint",
        "mcp_authority_generation",
    }
)


class SimulatedCaptureCrash(BaseException):
    """Test-only crash injected after durable event creation and before receipt."""


@contextmanager
def exclusive_lock(path: Path) -> Iterator[Path]:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield path
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _event_files(state_dir: Path) -> list[Path]:
    return sorted((state_dir / "events").glob("*/*.json"))


def iter_events(state_dir: Path) -> Iterator[dict[str, Any]]:
    migration_root = state_dir / "migrations" / "event-v2"
    migrations: dict[str, dict[str, Any]] = {}
    for migration_path in sorted(migration_root.glob("EN-MIG-*.json")) if migration_root.is_dir() else []:
        try:
            receipt = json.loads(migration_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ValidationError(f"invalid migration receipt JSON {migration_path}") from exc
        event_id = str(receipt.get("source_event_id", "")) if isinstance(receipt, dict) else ""
        if not event_id or event_id in migrations or receipt.get("migration_id") != migration_path.stem:
            raise ValidationError(f"invalid or duplicate migration receipt: {migration_path}")
        migrations[event_id] = receipt
    for path in _event_files(state_dir):
        try:
            with path.open(encoding="utf-8") as handle:
                event = json.load(handle)
        except json.JSONDecodeError as exc:
            raise ValidationError(f"invalid event JSON {path}: {exc}") from exc
        if not isinstance(event, dict):
            raise ValidationError(f"event must be object: {path}")
        try:
            validate_event(event)
        except ValidationError:
            receipt = migrations.get(str(event.get("event_id", "")))
            if receipt is None:
                raise
            from .migrations import validate_migration_receipt

            event = validate_migration_receipt(
                receipt,
                source_event=event,
                source_path=path,
            )
        yield event


def load_events(state_dir: Path) -> list[dict[str, Any]]:
    return list(iter_events(state_dir))


def event_by_id(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for event in events:
        event_id = str(event.get("event_id", ""))
        if not event_id or event_id in result:
            raise ValidationError(f"duplicate or missing event_id: {event_id!r}")
        result[event_id] = event
    return result


def effective_sentence_events(
    events: list[dict[str, Any]],
    *,
    article_id: str | None = None,
    study_date: str | None = None,
) -> list[dict[str, Any]]:
    sentence_events = [
        event
        for event in events
        if event.get("event_type") in {"sentence_captured", "sentence_correction"}
        and (article_id is None or event.get("article", {}).get("article_id") == article_id)
        and (study_date is None or parse_iso_date(str(event.get("occurred_at"))) == study_date)
    ]
    superseded = {
        str(event["supersedes_event_id"])
        for event in sentence_events
        if event.get("event_type") == "sentence_correction"
    }
    return [event for event in sentence_events if event.get("event_id") not in superseded]


def _assert_release_neutral(value: Any, path: str = "capture_event") -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            key_text = str(key)
            if key_text in RELEASE_NEUTRAL_FORBIDDEN_KEYS:
                raise ValidationError(
                    f"{path}.{key_text} is a deployment/runtime identity field"
                )
            _assert_release_neutral(nested, f"{path}.{key_text}")
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _assert_release_neutral(nested, f"{path}[{index}]")


def validate_event(event: dict[str, Any]) -> None:
    _assert_release_neutral(event)
    allowed = {
        "schema_version", "event_id", "event_type", "idempotency_key", "request_sha256", "occurred_at",
        "article", "source", "learning", "candidates", "supersedes_event_id", "correction_reason",
        "completion", "producer", "formal_write_count", "formal_writeback",
        "observed_signals", "source_signal_ids", "capture_coverage",
    }
    extras = sorted(set(event) - allowed)
    if extras:
        raise ValidationError(f"capture event has unsupported fields: {extras}")
    required = {
        "schema_version",
        "event_id",
        "event_type",
        "idempotency_key",
        "request_sha256",
        "occurred_at",
        "article",
        "producer",
        "formal_write_count",
        "formal_writeback",
    }
    missing = sorted(required - event.keys())
    if missing:
        raise ValidationError(f"capture event missing fields: {missing}")
    if event["schema_version"] not in {EVENT_SCHEMA_VERSION, LEGACY_EVENT_SCHEMA_VERSION}:
        raise ValidationError("capture event schema_version mismatch")
    if not re.fullmatch(r"EVT-[0-9]{8}-[A-F0-9]{16}", str(event["event_id"])):
        raise ValidationError("capture event_id is invalid")
    if not re.fullmatch(r"[a-f0-9]{64}", str(event["request_sha256"])):
        raise ValidationError("capture request_sha256 is invalid")
    if event["formal_write_count"] != 0 or event["formal_writeback"] != "none":
        raise ValidationError("capture event must have zero formal writes")
    if event["event_type"] not in {"sentence_captured", "sentence_correction", "article_completed"}:
        raise ValidationError(f"unsupported event_type: {event['event_type']}")
    article = event.get("article")
    if not isinstance(article, dict) or not article.get("article_id") or not article.get("source_article") or not article.get("source_id"):
        raise ValidationError("article_id, source_id and source_article are required")
    if article["article_id"] != article["source_id"]:
        raise ValidationError("article_id is a compatibility alias and must equal canonical source_id")
    if not isinstance(article.get("source_hash"), str) or not re.fullmatch(r"[a-f0-9]{64}", article["source_hash"]):
        raise ValidationError("article source_hash is required and must be lowercase SHA-256")
    article_allowed = {"article_id", "source_id", "source_article", "source_hash", "reference_id", "title"}
    if set(article) - article_allowed:
        raise ValidationError(f"article has unsupported fields: {sorted(set(article) - article_allowed)}")
    producer = event.get("producer")
    if not isinstance(producer, dict) or set(producer) != {"role", "name", "version"}:
        raise ValidationError("producer must contain exactly role, name and version")
    if producer["role"] != "foreground_producer" or not str(producer["name"]).strip() or not str(producer["version"]).strip():
        raise ValidationError("producer identity is invalid")
    if event["event_type"] in {"sentence_captured", "sentence_correction"}:
        if "completion" in event:
            raise ValidationError("sentence events must not contain completion")
        source = event.get("source")
        learning = event.get("learning")
        candidates = event.get("candidates")
        if not isinstance(source, dict) or not source.get("source_sentence") or not source.get("sentence_id"):
            raise ValidationError("sentence event source is incomplete")
        if set(source) != {"sentence_id", "source_sentence", "sentence_hash", "source_kind"}:
            raise ValidationError("sentence source fields do not match schema")
        actual = sentence_sha256(str(source["source_sentence"]))
        if source.get("sentence_hash") != actual:
            raise SourceHashMismatch(f"sentence hash mismatch: expected {actual}, got {source.get('sentence_hash')}")
        if not isinstance(learning, dict) or not isinstance(learning.get("user_evidence"), list):
            raise ValidationError("sentence event learning evidence is incomplete")
        learning_allowed = {
            "first_translation", "user_evidence_verbatim", "evidence_states", "evidence_origin", "user_evidence",
            "answer_protection", "hint_level", "translation", "explanation", "first_breakpoint", "restatement",
        }
        if set(learning) - learning_allowed:
            raise ValidationError(f"learning has unsupported fields: {sorted(set(learning) - learning_allowed)}")
        if "user_evidence_verbatim" not in learning or learning["user_evidence_verbatim"] is not None and not isinstance(learning["user_evidence_verbatim"], str):
            raise ValidationError("sentence event must preserve user_evidence_verbatim")
        if "first_translation" not in learning or learning["first_translation"] is not None and not isinstance(learning["first_translation"], str):
            raise ValidationError("sentence event must explicitly preserve first_translation or null")
        if learning.get("evidence_origin") not in {"live_user", "synthetic_fixture"}:
            raise ValidationError("sentence event evidence_origin is invalid")
        states = learning.get("evidence_states")
        if not isinstance(states, list) or sorted(set(states) - EVIDENCE_STATES):
            raise ValidationError("sentence event evidence_states are invalid")
        unknown = sorted(set(learning["user_evidence"]) - USER_EVIDENCE)
        if unknown:
            raise ValidationError(f"unsupported user evidence: {unknown}")
        if not isinstance(candidates, list):
            raise ValidationError("candidates must be a list")
        candidate_allowed = {"item", "candidate_type", "meaning", "usage", "review_note", "decision", "tier_hint", "reason", "source_signal_ids"}
        for index, candidate in enumerate(candidates, start=1):
            if not isinstance(candidate, dict) or set(candidate) - candidate_allowed:
                raise ValidationError(f"candidate {index} has unsupported fields")
            if not {"item", "candidate_type", "meaning", "decision"}.issubset(candidate):
                raise ValidationError(f"candidate {index} is incomplete")
            if candidate["decision"] not in {
                "long_term_candidate", "bbdc_candidate", "article_only", "not_recommended"
            }:
                raise ValidationError(f"candidate {index} decision is invalid")
        if event["schema_version"] == EVENT_SCHEMA_VERSION:
            _validate_signal_coverage(event)
        if source.get("source_kind") == "explanation" and learning.get("answer_protection") != "unlocked":
            raise ValidationError("explanation capture requires answer_protection=unlocked")
    if event["event_type"] == "sentence_correction":
        if not event.get("supersedes_event_id") or not event.get("correction_reason"):
            raise ValidationError("correction requires supersedes_event_id and correction_reason")
    if event["event_type"] == "article_completed" and not isinstance(event.get("completion"), dict):
        raise ValidationError("article_completed requires completion snapshot")
    if event["event_type"] == "article_completed":
        completion = event["completion"]
        if set(completion) != {"study_date", "effective_capture_event_ids", "capture_event_sha256"}:
            raise ValidationError("article completion fields do not match schema")


def _validate_signal_coverage(event: dict[str, Any]) -> None:
    signals = event.get("observed_signals")
    source_signal_ids = event.get("source_signal_ids")
    coverage = event.get("capture_coverage")
    if not isinstance(signals, list) or not isinstance(source_signal_ids, list) or not isinstance(coverage, dict):
        raise ValidationError("capture v2 requires observed_signals, source_signal_ids and capture_coverage")
    signal_ids: list[str] = []
    for index, signal in enumerate(signals, start=1):
        required = {
            "signal_id", "signal_type", "canonical_term", "surface_form",
            "knowledge_state", "provenance", "evidence_refs",
        }
        if not isinstance(signal, dict) or set(signal) != required:
            raise ValidationError(f"observed signal {index} fields do not match schema")
        if signal["signal_type"] not in {"vocabulary", "phrase", "structure"}:
            raise ValidationError(f"observed signal {index} signal_type is invalid")
        if signal["knowledge_state"] not in {"unknown", "mistranslated", "structure_unresolved"}:
            raise ValidationError(f"observed signal {index} knowledge_state is invalid")
        if not str(signal["canonical_term"]).strip() or not str(signal["surface_form"]).strip():
            raise ValidationError(f"observed signal {index} term is empty")
        if not isinstance(signal["evidence_refs"], list) or not signal["evidence_refs"]:
            raise ValidationError(f"observed signal {index} lacks evidence refs")
        signal_ids.append(str(signal["signal_id"]))
    if len(signal_ids) != len(set(signal_ids)) or source_signal_ids != signal_ids:
        raise ValidationError("source_signal_ids must exactly preserve observed signal order")
    rows = coverage.get("signals")
    if (
        set(coverage) != {"schema_version", "declared_signal_count", "covered_signal_count", "signals"}
        or coverage.get("schema_version") != "english_capture_coverage_v2"
        or not isinstance(rows, list)
        or any(
            not isinstance(row, dict)
            or set(row) != {"signal_id", "terminal_status"}
            for row in rows
        )
        or coverage.get("declared_signal_count") != len(signal_ids)
        or coverage.get("covered_signal_count") != len(signal_ids)
        or [row.get("signal_id") for row in rows if isinstance(row, dict)] != signal_ids
        or any(row.get("terminal_status") not in {"candidate", "unmatched"} for row in rows if isinstance(row, dict))
    ):
        raise ValidationError("capture coverage does not exactly cover observed signals")
    known = set(signal_ids)
    assigned: list[str] = []
    for candidate in event.get("candidates", []):
        refs = candidate.get("source_signal_ids")
        if not isinstance(refs, list) or not refs or not set(refs).issubset(known):
            raise ValidationError("capture v2 candidate must bind source_signal_ids")
        assigned.extend(refs)
    terminal = {
        row["signal_id"]: row["terminal_status"]
        for row in rows
    }
    for signal_id in signal_ids:
        expected = "candidate" if signal_id in assigned else "unmatched"
        if terminal[signal_id] != expected:
            raise ValidationError("capture coverage terminal status disagrees with candidate bindings")


def _derive_signal_contract(semantic: dict[str, Any]) -> None:
    if semantic.get("event_type") not in {"sentence_captured", "sentence_correction"}:
        return
    source = semantic.get("source", {})
    learning = semantic.get("learning", {})
    sentence = str(source.get("source_sentence", ""))
    candidates = semantic.get("candidates", [])
    supplied = semantic.get("observed_signals")
    signals: list[dict[str, Any]] = []
    if isinstance(supplied, list):
        signals = json.loads(json.dumps(supplied, ensure_ascii=False))
    seen_terms: set[tuple[str, str]] = set()
    for signal in signals:
        seen_terms.add((str(signal.get("signal_type")), str(signal.get("canonical_term", "")).casefold()))

    evidence_states = set(learning.get("user_evidence", []))
    default_word_state = "mistranslated" if "mistranslated" in evidence_states else "unknown"
    for candidate in candidates:
        candidate["decision"] = canonical_machine_decision(str(candidate.get("decision", "")))
        candidate_type = candidate.get("candidate_type")
        if candidate_type not in {"单词", "词组", "熟词僻义", "句型", "长难句"}:
            continue
        signal_type = "vocabulary" if candidate_type in {"单词", "熟词僻义"} else ("phrase" if candidate_type == "词组" else "structure")
        canonical = str(candidate.get("item", "")).strip()
        key = (signal_type, canonical.casefold())
        if key not in seen_terms:
            matches = exact_item_occurrences(sentence, canonical)
            signal = {
                "signal_id": "SIG-" + object_sha256({"type": signal_type, "term": canonical.casefold(), "sentence": source.get("sentence_hash")})[:16].upper(),
                "signal_type": signal_type,
                "canonical_term": canonical,
                "surface_form": matches[0]["surface_form"] if matches else canonical,
                "knowledge_state": "structure_unresolved" if signal_type == "structure" else default_word_state,
                "provenance": "explicit_capture_candidate",
                "evidence_refs": ["learning.user_evidence_verbatim", "source.source_sentence"],
            }
            signals.append(signal)
            seen_terms.add(key)

    breakpoint = str(learning.get("first_breakpoint", "")).strip()
    if breakpoint and evidence_states & {"unknown", "mistranslated"}:
        matches = exact_item_occurrences(sentence, breakpoint)
        key = ("vocabulary", breakpoint.casefold())
        if matches and key not in seen_terms:
            signals.append(
                {
                    "signal_id": "SIG-" + object_sha256({"type": "vocabulary", "term": breakpoint.casefold(), "sentence": source.get("sentence_hash")})[:16].upper(),
                    "signal_type": "vocabulary",
                    "canonical_term": breakpoint,
                    "surface_form": matches[0]["surface_form"],
                    "knowledge_state": default_word_state,
                    "provenance": "explicit_first_breakpoint",
                    "evidence_refs": ["learning.first_breakpoint", "source.source_sentence"],
                }
            )
    if "structure_trap" in evidence_states and not any(signal.get("signal_type") == "structure" for signal in signals):
        canonical = breakpoint or "unresolved_sentence_structure"
        signals.append(
            {
                "signal_id": "SIG-" + object_sha256({"type": "structure", "term": canonical, "sentence": source.get("sentence_hash")})[:16].upper(),
                "signal_type": "structure",
                "canonical_term": canonical,
                "surface_form": breakpoint or sentence,
                "knowledge_state": "structure_unresolved",
                "provenance": "explicit_structure_trap",
                "evidence_refs": ["learning.first_breakpoint", "learning.user_evidence_verbatim"],
            }
        )

    signal_ids = [str(signal["signal_id"]) for signal in signals]
    for candidate in candidates:
        existing = candidate.get("source_signal_ids")
        if isinstance(existing, list) and existing:
            continue
        canonical = str(candidate.get("item", "")).casefold()
        refs = [
            signal["signal_id"]
            for signal in signals
            if str(signal.get("canonical_term", "")).casefold() == canonical
        ]
        if refs:
            candidate["source_signal_ids"] = refs
    assigned = {
        signal_id
        for candidate in candidates
        for signal_id in candidate.get("source_signal_ids", [])
    }
    semantic["observed_signals"] = signals
    semantic["source_signal_ids"] = signal_ids
    semantic["capture_coverage"] = {
        "schema_version": "english_capture_coverage_v2",
        "declared_signal_count": len(signals),
        "covered_signal_count": len(signals),
        "signals": [
            {
                "signal_id": signal_id,
                "terminal_status": "candidate" if signal_id in assigned else "unmatched",
            }
            for signal_id in signal_ids
        ],
    }


def _build_event(request: dict[str, Any]) -> dict[str, Any]:
    request_copy = json.loads(json.dumps(request, ensure_ascii=False))
    occurred_at = str(request_copy.pop("occurred_at", "") or utc_now())
    idempotency_key = str(request_copy.get("idempotency_key", "")).strip()
    if not idempotency_key:
        raise ValidationError("idempotency_key is required")
    semantic = {key: value for key, value in request_copy.items() if key != "idempotency_key"}
    if "source" in semantic:
        source = semantic["source"]
        sentence = str(source.get("source_sentence", ""))
        expected = sentence_sha256(sentence)
        supplied = source.get("sentence_hash")
        if supplied and supplied != expected:
            raise SourceHashMismatch(f"sentence hash mismatch: expected {expected}, got {supplied}")
        source["sentence_hash"] = expected
    _derive_signal_contract(semantic)
    request_sha = object_sha256(semantic)
    date_token = parse_iso_date(occurred_at).replace("-", "")
    identity = object_sha256({"idempotency_key": idempotency_key, "request_sha256": request_sha})[:16].upper()
    event: dict[str, Any] = {
        "schema_version": EVENT_SCHEMA_VERSION,
        "event_id": f"EVT-{date_token}-{identity}",
        **semantic,
        "idempotency_key": idempotency_key,
        "request_sha256": request_sha,
        "occurred_at": occurred_at,
        "producer": request_copy.get(
            "producer",
            {"role": "foreground_producer", "name": "english_learning_pipeline", "version": "0.1.0"},
        ),
        "formal_write_count": 0,
        "formal_writeback": "none",
    }
    validate_event(event)
    return event


def _producer_binding(
    state_dir: Path, event: Mapping[str, Any]
) -> dict[str, Any]:
    try:
        return publish_attestation(
            descriptor_path=PRODUCER_BINDING_DESCRIPTOR_PATH,
            repo_root=state_dir.parent,
            subject="english",
            capture_id=str(event["event_id"]),
            capture_content_sha256=object_sha256(event),
            recorded_at=str(event["occurred_at"]),
        )
    except (
        OSError,
        ValueError,
        json.JSONDecodeError,
        ProducerBindingError,
    ) as exc:
        raise ValidationError(
            "English Producer binding attestation failed"
        ) from exc


def _bind_receipt_attestation(
    receipt: dict[str, Any], producer_binding: Mapping[str, Any]
) -> dict[str, Any]:
    receipt.update(
        {
            "producer_binding_status": producer_binding["status"],
            "producer_binding_attestation_path": producer_binding[
                "attestation_path"
            ],
            "producer_binding_attestation_sha256": producer_binding[
                "attestation_sha256"
            ],
        }
    )
    return receipt


def append_event(
    state_dir: Path,
    request: dict[str, Any],
    *,
    fault_after_event: bool = False,
) -> dict[str, Any]:
    state_dir = state_dir.resolve()
    lock_path = state_dir / "locks" / "events.lock"
    with exclusive_lock(lock_path):
        events = load_events(state_dir)
        proposed = _build_event(request)
        existing_by_key = {
            str(event.get("idempotency_key")): event
            for event in events
            if event.get("idempotency_key")
        }
        existing = existing_by_key.get(proposed["idempotency_key"])
        if existing:
            if existing.get("request_sha256") != proposed["request_sha256"]:
                raise IdempotencyConflict(
                    f"idempotency key {proposed['idempotency_key']!r} already binds "
                    f"{existing.get('request_sha256')}; proposed {proposed['request_sha256']}"
                )
            receipt_path = _capture_receipt_path(state_dir, existing)
            producer_binding = _producer_binding(state_dir, existing)
            if receipt_path.is_file():
                with receipt_path.open(encoding="utf-8") as handle:
                    original = json.load(handle)
            else:
                original = _bind_receipt_attestation(
                    _capture_receipt(state_dir, existing, "created"),
                    producer_binding,
                )
                original["receipt_path"] = str(receipt_path)
                original["original_status"] = "created"
                original["replayed"] = False
                atomic_write_json(receipt_path, original)
            replay = dict(original)
            replay["status"] = "idempotent_noop"
            replay["original_status"] = "created"
            replay["replayed"] = True
            if "producer_binding_status" not in replay:
                _bind_receipt_attestation(replay, producer_binding)
            return replay

        if proposed["event_type"] == "sentence_correction":
            by_id = event_by_id(events)
            target_id = proposed["supersedes_event_id"]
            target = by_id.get(target_id)
            if target is None:
                raise ValidationError(f"superseded event not found: {target_id}")
            if target.get("event_type") not in {"sentence_captured", "sentence_correction"}:
                raise ValidationError("only sentence events can be superseded")
            already_superseded = {
                event.get("supersedes_event_id")
                for event in events
                if event.get("event_type") == "sentence_correction"
            }
            if target_id in already_superseded:
                raise ValidationError("correction must supersede the current effective event")
            if target.get("article", {}).get("article_id") != proposed.get("article", {}).get("article_id"):
                raise ValidationError("correction cannot cross article_id")
            if target.get("article", {}).get("source_id") != proposed.get("article", {}).get("source_id"):
                raise ValidationError("correction cannot cross source_id")
            if target.get("source", {}).get("sentence_id") != proposed.get("source", {}).get("sentence_id"):
                raise ValidationError("correction cannot cross sentence_id")

        day = parse_iso_date(proposed["occurred_at"])
        event_path = state_dir / "events" / day / f"{proposed['event_id']}.json"
        event_path.parent.mkdir(parents=True, exist_ok=True)
        payload = canonical_bytes(proposed)
        descriptor, temp_name = tempfile.mkstemp(prefix=f".{proposed['event_id']}.", dir=event_path.parent)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temp_name, event_path)
            except FileExistsError as exc:
                raise IdempotencyConflict(f"immutable event path already exists: {event_path}") from exc
            directory_fd = os.open(event_path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
        producer_binding = _producer_binding(state_dir, proposed)
        if fault_after_event:
            raise SimulatedCaptureCrash(str(event_path))
        receipt = _bind_receipt_attestation(
            _capture_receipt(state_dir, proposed, "created"),
            producer_binding,
        )
        receipt_path = _capture_receipt_path(state_dir, proposed)
        receipt["receipt_path"] = str(receipt_path)
        receipt["original_status"] = "created"
        receipt["replayed"] = False
        atomic_write_json(receipt_path, receipt)
        return receipt


def _capture_receipt(state_dir: Path, event: dict[str, Any], status: str) -> dict[str, Any]:
    event_day = parse_iso_date(str(event["occurred_at"]))
    event_path = state_dir / "events" / event_day / f"{event['event_id']}.json"
    event_hash = object_sha256(event)
    receipt_id = f"CAPTURE-{event['event_id'][4:]}"
    return {
        "schema_version": CAPTURE_RECEIPT_VERSION,
        "receipt_id": receipt_id,
        "capture_id": event["event_id"],
        "status": status,
        "event_path": str(event_path),
        "event_sha256": event_hash,
        "request_sha256": event["request_sha256"],
        "formal_write_count": 0,
        "formal_writeback": "none",
        "supersedes_event_id": event.get("supersedes_event_id"),
        "projection_status": "pending",
        "view_path": None,
        "view_sha256": None,
        "projection_error": None,
        "stages": {
            "event_written": True,
            "schema_validated": True,
            "dispatcher_accepted": False,
            "package_visible": False,
        },
    }


def _capture_receipt_path(state_dir: Path, event: dict[str, Any]) -> Path:
    event_day = parse_iso_date(str(event["occurred_at"]))
    receipt_id = f"CAPTURE-{event['event_id'][4:]}"
    return state_dir / "receipts" / "capture" / event_day / f"{receipt_id}.json"
