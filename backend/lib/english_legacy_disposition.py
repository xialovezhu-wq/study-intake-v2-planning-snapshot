"""Fail-closed EN-P0-006 inventory and disposition control contracts.

The compatibility v2 lane records explicit single-target choices.  The v3 lane
binds one explicit user batch intent to one exact, independently reviewed
inventory and deterministically materializes one HMAC event and receipt per
target.  Neither lane changes an English formal source.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import csv
import datetime as dt
import functools
import hashlib
import hmac
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any


ISSUE_ID = "EN-P0-006"
SUBJECT = "english"
INVENTORY_SCHEMA = "english_legacy_target_inventory_v2"
RECEIPT_SCHEMA = "english_legacy_disposition_receipt_v2"
CLOSURE_SCHEMA = "english_legacy_disposition_closure_v2"
TEMPLATE_SCHEMA = "english_legacy_disposition_review_template_v2"
INVENTORY_V3_SCHEMA = "english_legacy_target_inventory_v3"
INVENTORY_REVIEW_RECEIPT_SCHEMA = (
    "english_legacy_inventory_independent_review_receipt_v1"
)
BATCH_INTENT_SCHEMA = "english_legacy_batch_authorization_intent_v1"
BATCH_AUTHORIZATION_SCHEMA = "english_legacy_batch_authorization_v1"
TARGET_AUTHORIZATION_EVENT_SCHEMA = "english_legacy_target_authorization_event_v3"
RECEIPT_V3_SCHEMA = "english_legacy_disposition_receipt_v3"
AUTHORIZATION_EXPANSION_CLOSURE_SCHEMA = (
    "english_legacy_authorization_expansion_closure_v1"
)
REMEDIATION_GATE_SCHEMA = "en_p0_006_remediation_gate_v1"
DISPOSITIONS = (
    "legacy_attestation",
    "deterministic_recuration",
    "rollback",
)
TARGET_KINDS = (
    "article_learning_page",
    "master_bank_row",
    "sentence_pattern_card",
)
REVIEW_VERDICTS = (
    "attestable",
    "evidence_incomplete",
    "recuration_recommended",
    "rollback_supported",
)
ORIGIN_OPERATIONS = (
    "created",
    "updated",
    "representation_reorder_only",
)
AUTHORIZED_OPERATIONS = (
    "luna_recuration",
    "sol_review",
    "sol_apply",
)
ROLLOUT_PATH = Path(
    "/Users/xiazhibin/.codex/sessions/2026/07/10/"
    "rollout-2026-07-10T19-44-57-019f4bd8-56dc-7a82-b029-155c11740feb.jsonl"
)
ROLLOUT_SHA256 = "1784977be267adc486a63f3f6b0d63ed60ef451765cdc353add82fc172be720d"
MUTATION_WINDOW_START = "2026-08-06T12:11:00Z"
MUTATION_WINDOW_END = "2026-08-06T12:19:00Z"
ROLLOUT_WINDOW_SHA256 = (
    "044fe0b600519f58e278f463197500d58e026e4ed74fded268dc14a8f521ea75"
)
ROLLOUT_WINDOW_EVENT_SHA256S = (
    "611540048bf65f57cdc155d7d7fb9d3f432d0b10d272e5405176876ddb42ead9",
    "90f77648ee820aa29d78b041f4ef903372ed4b62da0135301b29420e330e9992",
    "afc4f4989547c09b105135ab780ec3fcd74354ba5735a2e9087b36a87c695248",
    "8ac02e9b32b75456e7aad868b9fe0f8e364aceb0d5e00d58a636b4579bc11cea",
    "41a20bcf3ba7d473744f577227e1c52d6806e925ccb788753a97928dc45ce149",
    "4ec7adfa8724fc889b255e4d0206b8c98946689cddb2fb155df31c2dcbe5b66d",
    "862208afee326a02e6ad83109e54cc68097370596f53dd8811a99e3ca6ae027f",
    "c35bf6134f6a6feaed80588bfd4f3404e81eb6a742c66f94ecf5b69be607049f",
    "e4c477e3db414f91b6ebc5519a31a201750533a246db6c9a0afc8af91460db41",
    "e720a4361bb0df8edbdaf376252bda541398d036b3f7bb8efcf9f0e3cba18470",
    "f14dc12b69518441033d3ff77ed72817021c55a4f128c226ede9d67196fa789a",
)
ORIGINATING_THREAD_ID = "019f4bd8-56dc-7a82-b029-155c11740feb"
WRITE_SET_AUTHORITY_GENERATION = "english-legacy-en-p0-006-write-set-v3"
ARTICLE_RECORD_ID = "articles/2026-07-10-2011-english-i-text-4.md"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MASTER_BANK_PATH = Path(
    "/Users/xiazhibin/Documents/kaoyan-english/bank/master_bank.csv"
)
PATTERN_PATH = Path(
    "/Users/xiazhibin/Documents/kaoyan-english/bank/sentence_patterns.md"
)
ARTICLE_PATH = Path(
    "/Users/xiazhibin/Documents/kaoyan-english/articles/"
    "2026-07-10-2011-english-i-text-4.md"
)
MASTERED_ITEMS_PATH = Path(
    "/Users/xiazhibin/Documents/kaoyan-english/bank/mastered_items.csv"
)
ENGLISH_ROOT = MASTER_BANK_PATH.parents[1]
RELATION_REBUILD_SCRIPT = (
    ENGLISH_ROOT / "scripts/build_writing_vocabulary_relationship_graph.py"
)
PROJECTION_SIDE_EFFECT_PATHS = (
    (
        "relation_nodes",
        ENGLISH_ROOT
        / "raw/reference_relations/writing-vocabulary-foundation/nodes.jsonl",
    ),
    (
        "relation_edges",
        ENGLISH_ROOT
        / "raw/reference_relations/writing-vocabulary-foundation/edges.jsonl",
    ),
    (
        "relation_lookup",
        ENGLISH_ROOT
        / "raw/reference_relations/writing-vocabulary-foundation/lookup.json",
    ),
    (
        "relation_summary",
        ENGLISH_ROOT
        / "raw/reference_relations/writing-vocabulary-foundation/summary.json",
    ),
    (
        "relation_manifest",
        ENGLISH_ROOT
        / "raw/reference_relations/writing-vocabulary-foundation/manifest.json",
    ),
    (
        "relation_wiki_projection",
        ENGLISH_ROOT / "wiki/relationships/作文-大纲词-错词关系图谱.md",
    ),
)
ALLOWED_AUTHORITY_PATHS = {
    str(MASTER_BANK_PATH.resolve()): "master_bank_row",
    str(PATTERN_PATH.resolve()): "sentence_pattern_card",
    str(ARTICLE_PATH.resolve()): "article_learning_page",
    str(MASTERED_ITEMS_PATH.resolve()): "mastered_items_boundary",
}
HISTORICAL_AUTHORITY_ROLES = {
    str(MASTER_BANK_PATH.resolve()): "english_legacy_master_bank_snapshot",
    str(PATTERN_PATH.resolve()): "english_legacy_sentence_patterns_snapshot",
    str(ARTICLE_PATH.resolve()): "english_legacy_article_snapshot",
    str(MASTERED_ITEMS_PATH.resolve()): "english_legacy_mastered_items_snapshot",
}


class EnglishLegacyDispositionError(ValueError):
    """Stable fail-closed error used by the CLI and the P0 gate."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise EnglishLegacyDispositionError("legacy_value_not_canonical") from exc


def json_file_bytes(value: Any) -> bytes:
    return canonical_bytes(value) + b"\n"


def value_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def document_sha256(value: Any) -> str:
    return hashlib.sha256(json_file_bytes(value)).hexdigest()


@functools.lru_cache(maxsize=1)
def _historical_test_input_from_environment():
    from historical_test_input import HistoricalTestInput

    return HistoricalTestInput.from_env()


def _authority_read_path(path: Path) -> Path:
    role = HISTORICAL_AUTHORITY_ROLES.get(str(path.expanduser().resolve()))
    if role is None or not os.environ.get(
        "STUDY_PREPROCESSOR_HISTORICAL_TEST_INPUT_MANIFEST"
    ):
        return path
    try:
        test_input = _historical_test_input_from_environment()
        candidates = test_input.paths_for_role(role)
        if len(candidates) != 1:
            raise ValueError("historical authority snapshot missing")
        candidate = candidates[0]
        test_input.assert_allowed(candidate)
        return candidate
    except (OSError, RuntimeError, ValueError) as exc:
        raise EnglishLegacyDispositionError(
            "legacy_historical_authority_snapshot_invalid"
        ) from exc


def file_sha256(path: Path) -> str:
    path = _authority_read_path(path)
    digest = hashlib.sha256()
    try:
        if path.is_symlink() or not path.is_file():
            raise OSError("unsafe file")
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
    except OSError as exc:
        raise EnglishLegacyDispositionError("legacy_authority_file_unreadable") from exc
    return digest.hexdigest()


def load_json(path: Path, code: str, *, max_bytes: int = 8 * 1024 * 1024) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > max_bytes:
            raise OSError("unsafe json")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EnglishLegacyDispositionError(code) from exc
    if not isinstance(value, dict):
        raise EnglishLegacyDispositionError(code)
    return value


def _mapping(value: Any, code: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise EnglishLegacyDispositionError(code)
    return dict(value)


def _sequence(value: Any, code: str) -> list[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise EnglishLegacyDispositionError(code)
    return list(value)


def _nonempty(value: Any, code: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise EnglishLegacyDispositionError(code)
    return value


def _sha256(value: Any, code: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise EnglishLegacyDispositionError(code)
    return value


def _timestamp(value: Any, code: str) -> str:
    text = _nonempty(value, code)
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = dt.datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise EnglishLegacyDispositionError(code) from exc
    if parsed.tzinfo is None:
        raise EnglishLegacyDispositionError(code)
    return text


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _authority_core(authority: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "issue_id": ISSUE_ID,
        "subject": SUBJECT,
        "generation": authority["generation"],
        "files": authority["files"],
    }


def _expected_target_id(kind: str, record_id: str) -> str:
    return f"{kind}:{record_id}"


def _target_set_sha256(targets: Sequence[Mapping[str, Any]]) -> str:
    return value_sha256(
        [
            {
                "target_id": row["target_id"],
                "target_kind": row["target_kind"],
                "record_id": row["record_id"],
                "authority_path": row["authority_path"],
                "authority_file_sha256": row["authority_file_sha256"],
                "current_object_sha256": row["current_object_sha256"],
            }
            for row in targets
        ]
    )


def _seal_shape(value: Any, *, purpose: str) -> dict[str, Any]:
    seal = _mapping(value, "legacy_seal_invalid")
    if set(seal) != {"algorithm", "purpose", "hmac_sha256"}:
        raise EnglishLegacyDispositionError("legacy_seal_invalid")
    if seal.get("algorithm") != "HMAC-SHA256" or seal.get("purpose") != purpose:
        raise EnglishLegacyDispositionError("legacy_seal_invalid")
    _sha256(seal.get("hmac_sha256"), "legacy_seal_invalid")
    return seal


def _validate_authority(value: Any) -> dict[str, Any]:
    authority = _mapping(value, "legacy_inventory_authority_invalid")
    if set(authority) != {"generation", "fingerprint", "files"}:
        raise EnglishLegacyDispositionError("legacy_inventory_authority_invalid")
    _nonempty(authority.get("generation"), "legacy_inventory_authority_invalid")
    _sha256(authority.get("fingerprint"), "legacy_inventory_authority_invalid")
    files = _sequence(authority.get("files"), "legacy_inventory_authority_invalid")
    checked: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw in files:
        row = _mapping(raw, "legacy_inventory_authority_invalid")
        if set(row) != {"authority_path", "sha256", "role"}:
            raise EnglishLegacyDispositionError("legacy_inventory_authority_invalid")
        path = _nonempty(row.get("authority_path"), "legacy_inventory_authority_invalid")
        if path in seen or path not in ALLOWED_AUTHORITY_PATHS:
            raise EnglishLegacyDispositionError("legacy_inventory_authority_invalid")
        if row.get("role") != ALLOWED_AUTHORITY_PATHS[path]:
            raise EnglishLegacyDispositionError("legacy_inventory_authority_invalid")
        seen.add(path)
        _sha256(row.get("sha256"), "legacy_inventory_authority_invalid")
        checked.append({"authority_path": path, "sha256": row["sha256"], "role": row["role"]})
    if seen != set(ALLOWED_AUTHORITY_PATHS) or checked != sorted(
        checked, key=lambda row: row["authority_path"]
    ):
        raise EnglishLegacyDispositionError("legacy_inventory_authority_invalid")
    authority["files"] = checked
    if authority["fingerprint"] != value_sha256(_authority_core(authority)):
        raise EnglishLegacyDispositionError("legacy_inventory_authority_mismatch")
    return authority


def _validate_mcp_authority(value: Any) -> dict[str, str]:
    authority = _mapping(value, "legacy_inventory_mcp_authority_invalid")
    if set(authority) != {"generation", "authority_fingerprint"}:
        raise EnglishLegacyDispositionError(
            "legacy_inventory_mcp_authority_invalid"
        )
    return {
        "generation": _nonempty(
            authority.get("generation"),
            "legacy_inventory_mcp_authority_invalid",
        ),
        "authority_fingerprint": _sha256(
            authority.get("authority_fingerprint"),
            "legacy_inventory_mcp_authority_invalid",
        ),
    }


def _validate_target(value: Any, authority: Mapping[str, Any]) -> dict[str, Any]:
    target = _mapping(value, "legacy_inventory_target_invalid")
    required = {
        "target_id",
        "target_kind",
        "record_id",
        "authority_path",
        "authority_file_sha256",
        "current_object_sha256",
        "prehash_status",
        "prehash_sha256",
        "originating_thread_id",
        "independent_review",
    }
    if set(target) != required or target.get("target_kind") not in TARGET_KINDS:
        raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")
    kind = str(target["target_kind"])
    record_id = _nonempty(target.get("record_id"), "legacy_inventory_target_invalid")
    if target.get("target_id") != _expected_target_id(kind, record_id):
        raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")
    path = _nonempty(target.get("authority_path"), "legacy_inventory_target_invalid")
    if ALLOWED_AUTHORITY_PATHS.get(path) != kind:
        raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")
    file_map = {row["authority_path"]: row["sha256"] for row in authority["files"]}
    if target.get("authority_file_sha256") != file_map.get(path):
        raise EnglishLegacyDispositionError("legacy_inventory_target_authority_mismatch")
    _sha256(target.get("current_object_sha256"), "legacy_inventory_target_invalid")
    if target.get("prehash_status") not in {"verified", "evidence_incomplete"}:
        raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")
    prehash = target.get("prehash_sha256")
    if target["prehash_status"] == "verified":
        _sha256(prehash, "legacy_inventory_target_invalid")
    elif prehash is not None:
        raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")
    _nonempty(target.get("originating_thread_id"), "legacy_inventory_target_invalid")
    review = _mapping(target.get("independent_review"), "legacy_inventory_review_invalid")
    if set(review) != {"status", "verdict", "receipt_sha256"}:
        raise EnglishLegacyDispositionError("legacy_inventory_review_invalid")
    if review.get("status") != "completed" or review.get("verdict") not in REVIEW_VERDICTS:
        raise EnglishLegacyDispositionError("legacy_inventory_review_invalid")
    _sha256(review.get("receipt_sha256"), "legacy_inventory_review_invalid")
    return target


def validate_inventory_v2(value: Mapping[str, Any]) -> dict[str, Any]:
    inventory = dict(value)
    required = {
        "schema_version",
        "issue_id",
        "subject",
        "originating_thread_id",
        "inventory_id",
        "inventory_status",
        "authority",
        "targets",
        "target_count",
        "target_set_sha256",
        "unidentified_target_count",
        "mastered_items_boundary",
        "independent_inventory_review",
        "model_call_count",
        "formal_write_count",
        "issued_at",
        "authority_key_id",
        "seal",
    }
    if (
        set(inventory) != required
        or inventory.get("schema_version") != INVENTORY_SCHEMA
        or inventory.get("issue_id") != ISSUE_ID
        or inventory.get("subject") != SUBJECT
        or inventory.get("inventory_status") != "complete"
        or inventory.get("unidentified_target_count") != 0
        or inventory.get("model_call_count") != 0
        or inventory.get("formal_write_count") != 0
    ):
        raise EnglishLegacyDispositionError("legacy_inventory_invalid")
    _nonempty(inventory.get("originating_thread_id"), "legacy_inventory_invalid")
    _nonempty(inventory.get("inventory_id"), "legacy_inventory_invalid")
    authority = _validate_authority(inventory.get("authority"))
    rows = _sequence(inventory.get("targets"), "legacy_inventory_invalid")
    if not rows:
        raise EnglishLegacyDispositionError("legacy_inventory_invalid")
    targets = [_validate_target(row, authority) for row in rows]
    target_ids = [row["target_id"] for row in targets]
    identities = [(row["target_kind"], row["record_id"]) for row in targets]
    if (
        target_ids != sorted(target_ids)
        or len(target_ids) != len(set(target_ids))
        or len(identities) != len(set(identities))
    ):
        raise EnglishLegacyDispositionError("legacy_inventory_duplicate_target")
    if inventory.get("target_count") != len(targets):
        raise EnglishLegacyDispositionError("legacy_inventory_target_count_mismatch")
    if inventory.get("target_set_sha256") != _target_set_sha256(targets):
        raise EnglishLegacyDispositionError("legacy_inventory_target_set_mismatch")
    mastered = _mapping(
        inventory.get("mastered_items_boundary"),
        "legacy_mastered_items_boundary_invalid",
    )
    if set(mastered) != {
        "authority_path",
        "sha256",
        "row_count",
        "inference_forbidden",
        "used_for_disposition",
    }:
        raise EnglishLegacyDispositionError("legacy_mastered_items_boundary_invalid")
    if (
        mastered.get("authority_path") != str(MASTERED_ITEMS_PATH.resolve())
        or mastered.get("sha256")
        != {row["authority_path"]: row["sha256"] for row in authority["files"]}.get(
            str(MASTERED_ITEMS_PATH.resolve())
        )
        or not isinstance(mastered.get("row_count"), int)
        or mastered.get("row_count") < 0
        or mastered.get("inference_forbidden") is not True
        or mastered.get("used_for_disposition") is not False
    ):
        raise EnglishLegacyDispositionError("legacy_mastered_items_inference_forbidden")
    review = _mapping(
        inventory.get("independent_inventory_review"),
        "legacy_inventory_review_invalid",
    )
    if set(review) != {"status", "receipt_sha256"} or review.get("status") != "completed":
        raise EnglishLegacyDispositionError("legacy_inventory_review_invalid")
    _sha256(review.get("receipt_sha256"), "legacy_inventory_review_invalid")
    _timestamp(inventory.get("issued_at"), "legacy_inventory_issued_at_invalid")
    _sha256(inventory.get("authority_key_id"), "legacy_inventory_key_id_invalid")
    _seal_shape(inventory.get("seal"), purpose="english-legacy-target-inventory-v2")
    inventory["authority"] = authority
    inventory["targets"] = targets
    return inventory


def validate_receipt_v2(value: Mapping[str, Any]) -> dict[str, Any]:
    receipt = dict(value)
    required = {
        "schema_version",
        "issue_id",
        "subject",
        "inventory",
        "target",
        "disposition",
        "user_authorization",
        "authority_key_id",
        "model_call_count",
        "formal_write_count",
        "issued_at",
        "seal",
    }
    if (
        set(receipt) != required
        or receipt.get("schema_version") != RECEIPT_SCHEMA
        or receipt.get("issue_id") != ISSUE_ID
        or receipt.get("subject") != SUBJECT
        or receipt.get("disposition") not in DISPOSITIONS
        or receipt.get("model_call_count") != 0
        or receipt.get("formal_write_count") != 0
    ):
        raise EnglishLegacyDispositionError("legacy_disposition_receipt_invalid")
    inventory = _mapping(receipt.get("inventory"), "legacy_disposition_receipt_invalid")
    if set(inventory) != {
        "inventory_id",
        "inventory_sha256",
        "target_set_sha256",
        "authority_generation",
        "authority_fingerprint",
    }:
        raise EnglishLegacyDispositionError("legacy_disposition_receipt_invalid")
    _nonempty(inventory.get("inventory_id"), "legacy_disposition_receipt_invalid")
    _sha256(inventory.get("inventory_sha256"), "legacy_disposition_receipt_invalid")
    _sha256(inventory.get("target_set_sha256"), "legacy_disposition_receipt_invalid")
    _nonempty(inventory.get("authority_generation"), "legacy_disposition_receipt_invalid")
    _sha256(inventory.get("authority_fingerprint"), "legacy_disposition_receipt_invalid")
    target = _mapping(receipt.get("target"), "legacy_disposition_receipt_invalid")
    if set(target) != {
        "target_id",
        "target_kind",
        "record_id",
        "authority_path",
        "authority_file_sha256",
        "current_object_sha256",
    }:
        raise EnglishLegacyDispositionError("legacy_disposition_receipt_invalid")
    if target.get("target_kind") not in TARGET_KINDS or target.get("target_id") != _expected_target_id(
        str(target.get("target_kind")),
        _nonempty(target.get("record_id"), "legacy_disposition_receipt_invalid"),
    ):
        raise EnglishLegacyDispositionError("legacy_disposition_receipt_invalid")
    _nonempty(target.get("authority_path"), "legacy_disposition_receipt_invalid")
    _sha256(target.get("authority_file_sha256"), "legacy_disposition_receipt_invalid")
    _sha256(target.get("current_object_sha256"), "legacy_disposition_receipt_invalid")
    event = _mapping(receipt.get("user_authorization"), "legacy_user_authorization_invalid")
    if set(event) != {
        "event_id",
        "event_type",
        "issue_id",
        "inventory_sha256",
        "target_id",
        "disposition",
        "authorization_scope",
        "user_message_sha256",
        "authorized_at",
    }:
        raise EnglishLegacyDispositionError("legacy_user_authorization_invalid")
    if (
        event.get("event_type") != "explicit_user_english_legacy_disposition"
        or event.get("issue_id") != ISSUE_ID
        or event.get("inventory_sha256") != inventory["inventory_sha256"]
        or event.get("target_id") != target["target_id"]
        or event.get("disposition") != receipt["disposition"]
        or event.get("authorization_scope") != "single_target_only"
    ):
        raise EnglishLegacyDispositionError("legacy_user_authorization_binding_mismatch")
    _nonempty(event.get("event_id"), "legacy_user_authorization_invalid")
    _sha256(event.get("user_message_sha256"), "legacy_user_authorization_invalid")
    authorized_at = _timestamp(event.get("authorized_at"), "legacy_user_authorization_invalid")
    issued_at = _timestamp(receipt.get("issued_at"), "legacy_receipt_issued_at_invalid")
    if dt.datetime.fromisoformat(authorized_at.replace("Z", "+00:00")) > dt.datetime.fromisoformat(
        issued_at.replace("Z", "+00:00")
    ):
        raise EnglishLegacyDispositionError("legacy_authorization_after_receipt")
    _sha256(receipt.get("authority_key_id"), "legacy_receipt_key_id_invalid")
    _seal_shape(receipt.get("seal"), purpose="english-legacy-disposition-receipt-v2")
    return receipt


def validate_closure_v2(value: Mapping[str, Any]) -> dict[str, Any]:
    closure = dict(value)
    required = {
        "schema_version",
        "issue_id",
        "subject",
        "inventory_sha256",
        "target_set_sha256",
        "authority_generation",
        "authority_fingerprint",
        "receipt_sha256s",
        "target_count",
        "user_authorized_target_count",
        "unidentified_target_count",
        "omitted_target_count",
        "duplicate_target_count",
        "unknown_target_count",
        "authority_key_id",
        "model_call_count",
        "formal_write_count",
        "issued_at",
        "seal",
    }
    if (
        set(closure) != required
        or closure.get("schema_version") != CLOSURE_SCHEMA
        or closure.get("issue_id") != ISSUE_ID
        or closure.get("subject") != SUBJECT
        or closure.get("unidentified_target_count") != 0
        or closure.get("omitted_target_count") != 0
        or closure.get("duplicate_target_count") != 0
        or closure.get("unknown_target_count") != 0
        or closure.get("model_call_count") != 0
        or closure.get("formal_write_count") != 0
    ):
        raise EnglishLegacyDispositionError("legacy_disposition_closure_invalid")
    for field in (
        "inventory_sha256",
        "target_set_sha256",
        "authority_fingerprint",
        "authority_key_id",
    ):
        _sha256(closure.get(field), "legacy_disposition_closure_invalid")
    _nonempty(closure.get("authority_generation"), "legacy_disposition_closure_invalid")
    digests = _sequence(closure.get("receipt_sha256s"), "legacy_disposition_closure_invalid")
    if not digests or any(SHA256_RE.fullmatch(str(row)) is None for row in digests):
        raise EnglishLegacyDispositionError("legacy_disposition_closure_invalid")
    if digests != sorted(digests) or len(digests) != len(set(digests)):
        raise EnglishLegacyDispositionError("legacy_disposition_duplicate_receipt")
    count = closure.get("target_count")
    if (
        isinstance(count, bool)
        or not isinstance(count, int)
        or count <= 0
        or closure.get("user_authorized_target_count") != count
        or len(digests) != count
    ):
        raise EnglishLegacyDispositionError("legacy_disposition_closure_count_mismatch")
    _timestamp(closure.get("issued_at"), "legacy_closure_issued_at_invalid")
    _seal_shape(closure.get("seal"), purpose="english-legacy-disposition-closure-v2")
    return closure


def _read_master_bank_rows(path: Path) -> dict[str, tuple[dict[str, str], str]]:
    path = _authority_read_path(path)
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames or "id" not in reader.fieldnames:
                raise ValueError("missing id")
            rows: dict[str, tuple[dict[str, str], str]] = {}
            for row in reader:
                record_id = str(row.get("id") or "")
                if not record_id or record_id in rows:
                    raise ValueError("duplicate id")
                normalized = {str(key): str(row.get(key) or "") for key in reader.fieldnames}
                rows[record_id] = (normalized, value_sha256(normalized))
    except (OSError, UnicodeError, csv.Error, ValueError) as exc:
        raise EnglishLegacyDispositionError("legacy_master_bank_parse_failed") from exc
    return rows


def _read_pattern_cards(path: Path) -> dict[str, str]:
    path = _authority_read_path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise EnglishLegacyDispositionError("legacy_pattern_parse_failed") from exc
    matches = list(re.finditer(r"(?m)^## (SP-[0-9]{3})(?:｜|\|).*$", text))
    cards: dict[str, str] = {}
    for index, match in enumerate(matches):
        record_id = match.group(1)
        if record_id in cards:
            raise EnglishLegacyDispositionError("legacy_pattern_parse_failed")
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        cards[record_id] = hashlib.sha256(text[match.start():end].encode("utf-8")).hexdigest()
    return cards


def _live_object_hash(target: Mapping[str, Any]) -> str:
    kind = target["target_kind"]
    record_id = target["record_id"]
    if kind == "master_bank_row":
        row = _read_master_bank_rows(MASTER_BANK_PATH).get(record_id)
        if row is None:
            raise EnglishLegacyDispositionError("legacy_inventory_target_missing")
        return row[1]
    if kind == "sentence_pattern_card":
        digest = _read_pattern_cards(PATTERN_PATH).get(record_id)
        if digest is None:
            raise EnglishLegacyDispositionError("legacy_inventory_target_missing")
        return digest
    if kind == "article_learning_page":
        if record_id != "articles/2026-07-10-2011-english-i-text-4.md":
            raise EnglishLegacyDispositionError("legacy_inventory_target_missing")
        return file_sha256(ARTICLE_PATH)
    raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")


def verify_inventory_live(inventory: Mapping[str, Any]) -> None:
    checked = validate_inventory_v2(inventory)
    for row in checked["authority"]["files"]:
        if file_sha256(Path(row["authority_path"])) != row["sha256"]:
            raise EnglishLegacyDispositionError("legacy_inventory_stale_authority")
    for target in checked["targets"]:
        if _live_object_hash(target) != target["current_object_sha256"]:
            raise EnglishLegacyDispositionError("legacy_inventory_stale_target")
    mastered = checked["mastered_items_boundary"]
    try:
        with _authority_read_path(MASTERED_ITEMS_PATH).open(
            "r", encoding="utf-8-sig", newline=""
        ) as handle:
            row_count = sum(1 for _ in csv.DictReader(handle))
    except (OSError, UnicodeError, csv.Error) as exc:
        raise EnglishLegacyDispositionError("legacy_mastered_items_unreadable") from exc
    if row_count != mastered["row_count"]:
        raise EnglishLegacyDispositionError("legacy_inventory_stale_mastered_items")


class EnglishLegacyDispositionStore:
    """Content-addressed HMAC store for EN-P0-006 control receipts."""

    def __init__(self, receipt_root: Path, authority_key_path: Path) -> None:
        self.root = receipt_root.expanduser().absolute()
        self.authority_key_path = authority_key_path.expanduser().absolute()

    def _key(self) -> bytes:
        try:
            if self.authority_key_path.is_symlink() or not self.authority_key_path.is_file():
                raise OSError("unsafe key")
            mode = self.authority_key_path.stat().st_mode & 0o777
            if mode & 0o077:
                raise OSError("permissive key")
            key = self.authority_key_path.read_bytes()
        except OSError as exc:
            raise EnglishLegacyDispositionError("legacy_authority_key_unavailable") from exc
        if len(key) < 32:
            raise EnglishLegacyDispositionError("legacy_authority_key_invalid")
        return key

    def _key_id(self) -> str:
        return hashlib.sha256(self._key()).hexdigest()

    def _seal(self, core: Mapping[str, Any], *, purpose: str) -> dict[str, Any]:
        value = dict(core)
        if "seal" in value:
            raise EnglishLegacyDispositionError("legacy_value_already_sealed")
        value["seal"] = {
            "algorithm": "HMAC-SHA256",
            "purpose": purpose,
            "hmac_sha256": hmac.new(
                self._key(), canonical_bytes(value), hashlib.sha256
            ).hexdigest(),
        }
        return value

    def _verify_seal(self, value: Mapping[str, Any], *, purpose: str) -> None:
        core = dict(value)
        seal = _seal_shape(core.pop("seal", None), purpose=purpose)
        expected = hmac.new(self._key(), canonical_bytes(core), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(seal["hmac_sha256"], expected):
            raise EnglishLegacyDispositionError("legacy_receipt_hmac_invalid")

    def _kind_root(self, kind: str) -> Path:
        if kind not in {"inventories", "dispositions", "closures"}:
            raise EnglishLegacyDispositionError("legacy_receipt_kind_invalid")
        if self.root.exists() and self.root.is_symlink():
            raise EnglishLegacyDispositionError("legacy_receipt_root_unsafe")
        kind_root = self.root / kind
        if kind_root.exists() and kind_root.is_symlink():
            raise EnglishLegacyDispositionError("legacy_receipt_root_unsafe")
        return kind_root

    def _publish(self, kind: str, value: Mapping[str, Any]) -> tuple[str, Path]:
        payload = json_file_bytes(value)
        digest = hashlib.sha256(payload).hexdigest()
        path = self._kind_root(kind) / "sha256" / digest[:2] / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.exists():
            if path.read_bytes() != payload:
                raise EnglishLegacyDispositionError("legacy_content_address_conflict")
            return digest, path
        descriptor, name = tempfile.mkstemp(prefix=f".{digest}.", dir=path.parent)
        temporary = Path(name)
        try:
            os.fchmod(descriptor, 0o400)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_bytes() != payload:
                    raise EnglishLegacyDispositionError("legacy_content_address_conflict")
            os.chmod(path, 0o400)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass
        return digest, path

    def _read(self, kind: str, digest: str) -> dict[str, Any]:
        _sha256(digest, "legacy_content_address_invalid")
        path = self._kind_root(kind) / "sha256" / digest[:2] / f"{digest}.json"
        value = load_json(path, "legacy_content_address_invalid")
        if document_sha256(value) != digest:
            raise EnglishLegacyDispositionError("legacy_content_address_invalid")
        return value

    def seal_inventory(self, core: Mapping[str, Any]) -> tuple[str, Path, dict[str, Any]]:
        value = dict(core)
        if "authority_key_id" in value or "seal" in value:
            raise EnglishLegacyDispositionError("legacy_inventory_draft_invalid")
        value["authority_key_id"] = self._key_id()
        sealed = self._seal(value, purpose="english-legacy-target-inventory-v2")
        checked = validate_inventory_v2(sealed)
        self._verify_seal(checked, purpose="english-legacy-target-inventory-v2")
        verify_inventory_live(checked)
        digest, path = self._publish("inventories", checked)
        return digest, path, checked

    def verify_inventory(self, digest: str) -> dict[str, Any]:
        inventory = validate_inventory_v2(self._read("inventories", digest))
        self._verify_seal(inventory, purpose="english-legacy-target-inventory-v2")
        if inventory["authority_key_id"] != self._key_id():
            raise EnglishLegacyDispositionError("legacy_authority_key_id_mismatch")
        verify_inventory_live(inventory)
        return inventory

    def issue_target_receipt(
        self,
        inventory_sha256: str,
        *,
        target_id: str,
        disposition: str,
        user_authorization: Mapping[str, Any],
        issued_at: str | None = None,
    ) -> tuple[str, Path, dict[str, Any]]:
        inventory = self.verify_inventory(inventory_sha256)
        target = next(
            (row for row in inventory["targets"] if row["target_id"] == target_id),
            None,
        )
        if target is None:
            raise EnglishLegacyDispositionError("legacy_disposition_unknown_target")
        if disposition not in DISPOSITIONS:
            raise EnglishLegacyDispositionError("legacy_disposition_invalid_choice")
        binding = {
            key: target[key]
            for key in (
                "target_id",
                "target_kind",
                "record_id",
                "authority_path",
                "authority_file_sha256",
                "current_object_sha256",
            )
        }
        core = {
            "schema_version": RECEIPT_SCHEMA,
            "issue_id": ISSUE_ID,
            "subject": SUBJECT,
            "inventory": {
                "inventory_id": inventory["inventory_id"],
                "inventory_sha256": inventory_sha256,
                "target_set_sha256": inventory["target_set_sha256"],
                "authority_generation": inventory["authority"]["generation"],
                "authority_fingerprint": inventory["authority"]["fingerprint"],
            },
            "target": binding,
            "disposition": disposition,
            "user_authorization": dict(user_authorization),
            "authority_key_id": self._key_id(),
            "model_call_count": 0,
            "formal_write_count": 0,
            "issued_at": issued_at or _utc_now(),
        }
        receipt = validate_receipt_v2(
            self._seal(core, purpose="english-legacy-disposition-receipt-v2")
        )
        self._verify_seal(receipt, purpose="english-legacy-disposition-receipt-v2")
        digest, path = self._publish("dispositions", receipt)
        return digest, path, receipt

    def verify_target_receipt(
        self, digest: str, *, inventory: Mapping[str, Any], inventory_sha256: str
    ) -> dict[str, Any]:
        receipt = validate_receipt_v2(self._read("dispositions", digest))
        self._verify_seal(receipt, purpose="english-legacy-disposition-receipt-v2")
        if receipt["authority_key_id"] != self._key_id():
            raise EnglishLegacyDispositionError("legacy_authority_key_id_mismatch")
        if (
            receipt["inventory"]["inventory_sha256"] != inventory_sha256
            or receipt["inventory"]["inventory_id"] != inventory["inventory_id"]
            or receipt["inventory"]["target_set_sha256"] != inventory["target_set_sha256"]
            or receipt["inventory"]["authority_generation"]
            != inventory["authority"]["generation"]
            or receipt["inventory"]["authority_fingerprint"]
            != inventory["authority"]["fingerprint"]
        ):
            raise EnglishLegacyDispositionError("legacy_disposition_stale_inventory")
        target = next(
            (
                row
                for row in inventory["targets"]
                if row["target_id"] == receipt["target"]["target_id"]
            ),
            None,
        )
        if target is None:
            raise EnglishLegacyDispositionError("legacy_disposition_unknown_target")
        expected = {
            key: target[key]
            for key in (
                "target_id",
                "target_kind",
                "record_id",
                "authority_path",
                "authority_file_sha256",
                "current_object_sha256",
            )
        }
        if receipt["target"] != expected:
            raise EnglishLegacyDispositionError("legacy_disposition_target_mismatch")
        return receipt

    def issue_closure(
        self,
        inventory_sha256: str,
        receipt_sha256s: Sequence[str],
        *,
        issued_at: str | None = None,
    ) -> tuple[str, Path, dict[str, Any]]:
        inventory = self.verify_inventory(inventory_sha256)
        digests = list(receipt_sha256s)
        if digests != sorted(digests) or len(digests) != len(set(digests)):
            raise EnglishLegacyDispositionError("legacy_disposition_duplicate_receipt")
        receipts = [
            self.verify_target_receipt(
                digest, inventory=inventory, inventory_sha256=inventory_sha256
            )
            for digest in digests
        ]
        target_ids = [row["target"]["target_id"] for row in receipts]
        if len(target_ids) != len(set(target_ids)):
            raise EnglishLegacyDispositionError("legacy_disposition_duplicate_target")
        inventory_ids = [row["target_id"] for row in inventory["targets"]]
        unknown = sorted(set(target_ids) - set(inventory_ids))
        omitted = sorted(set(inventory_ids) - set(target_ids))
        if unknown:
            raise EnglishLegacyDispositionError("legacy_disposition_unknown_target")
        if omitted:
            raise EnglishLegacyDispositionError("legacy_disposition_omitted_target")
        core = {
            "schema_version": CLOSURE_SCHEMA,
            "issue_id": ISSUE_ID,
            "subject": SUBJECT,
            "inventory_sha256": inventory_sha256,
            "target_set_sha256": inventory["target_set_sha256"],
            "authority_generation": inventory["authority"]["generation"],
            "authority_fingerprint": inventory["authority"]["fingerprint"],
            "receipt_sha256s": digests,
            "target_count": len(inventory_ids),
            "user_authorized_target_count": len(target_ids),
            "unidentified_target_count": 0,
            "omitted_target_count": 0,
            "duplicate_target_count": 0,
            "unknown_target_count": 0,
            "authority_key_id": self._key_id(),
            "model_call_count": 0,
            "formal_write_count": 0,
            "issued_at": issued_at or _utc_now(),
        }
        closure = validate_closure_v2(
            self._seal(core, purpose="english-legacy-disposition-closure-v2")
        )
        self._verify_seal(closure, purpose="english-legacy-disposition-closure-v2")
        digest, path = self._publish("closures", closure)
        return digest, path, closure

    def verify_closure(self, digest: str) -> dict[str, Any]:
        closure = validate_closure_v2(self._read("closures", digest))
        self._verify_seal(closure, purpose="english-legacy-disposition-closure-v2")
        if closure["authority_key_id"] != self._key_id():
            raise EnglishLegacyDispositionError("legacy_authority_key_id_mismatch")
        inventory = self.verify_inventory(closure["inventory_sha256"])
        if (
            closure["target_set_sha256"] != inventory["target_set_sha256"]
            or closure["authority_generation"] != inventory["authority"]["generation"]
            or closure["authority_fingerprint"] != inventory["authority"]["fingerprint"]
            or closure["target_count"] != inventory["target_count"]
        ):
            raise EnglishLegacyDispositionError("legacy_disposition_authority_mismatch")
        receipts = [
            self.verify_target_receipt(
                digest,
                inventory=inventory,
                inventory_sha256=closure["inventory_sha256"],
            )
            for digest in closure["receipt_sha256s"]
        ]
        target_ids = [row["target"]["target_id"] for row in receipts]
        expected = [row["target_id"] for row in inventory["targets"]]
        if len(target_ids) != len(set(target_ids)):
            raise EnglishLegacyDispositionError("legacy_disposition_duplicate_target")
        if sorted(target_ids) != sorted(expected):
            unknown = set(target_ids) - set(expected)
            raise EnglishLegacyDispositionError(
                "legacy_disposition_unknown_target"
                if unknown
                else "legacy_disposition_omitted_target"
            )
        return {
            "schema_version": "english_legacy_disposition_verification_v2",
            "status": "verified_complete",
            "issue_id": ISSUE_ID,
            "inventory_sha256": closure["inventory_sha256"],
            "closure_sha256": digest,
            "target_count": inventory["target_count"],
            "receipt_count": len(receipts),
            "authority_generation": inventory["authority"]["generation"],
            "authority_fingerprint": inventory["authority"]["fingerprint"],
            "mastered_items_inference_used": False,
            "model_call_count": 0,
            "formal_write_count": 0,
        }


def build_review_template(requirements: Mapping[str, Any]) -> dict[str, Any]:
    """Build a review-only 97-target template from the bound v1 evidence.

    The result deliberately remains evidence_incomplete and contains no user
    choice.  It is not accepted by validate_inventory_v2 and cannot be sealed.
    """

    required = {
        "schema_version",
        "issue_id",
        "originating_thread_id",
        "inventory_status",
        "identified_target_count",
        "unidentified_target_count_lower_bound",
        "disposition_receipt_count",
        "allowed_dispositions",
        "target_groups",
        "mastered_items_state",
        "closure_rule",
        "model_call_count",
        "formal_write_count",
    }
    if (
        set(requirements) != required
        or requirements.get("schema_version")
        != "study-intake-english-legacy-disposition-requirements-v1"
        or requirements.get("issue_id") != ISSUE_ID
        or requirements.get("inventory_status") != "evidence_incomplete"
        or tuple(requirements.get("allowed_dispositions") or ()) != DISPOSITIONS
        or requirements.get("disposition_receipt_count") != 0
        or requirements.get("model_call_count") != 0
        or requirements.get("formal_write_count") != 0
    ):
        raise EnglishLegacyDispositionError("legacy_requirements_invalid")
    groups = _sequence(requirements.get("target_groups"), "legacy_requirements_invalid")
    by_kind = {
        str(row.get("target_kind")): dict(row)
        for row in groups
        if isinstance(row, Mapping)
    }
    if set(by_kind) != set(TARGET_KINDS):
        raise EnglishLegacyDispositionError("legacy_requirements_invalid")
    path_by_kind = {
        "master_bank_row": MASTER_BANK_PATH,
        "sentence_pattern_card": PATTERN_PATH,
        "article_learning_page": ARTICLE_PATH,
    }
    for kind, path in path_by_kind.items():
        group = by_kind[kind]
        if (
            group.get("authority_path") != str(path.resolve())
            or group.get("current_file_sha256") != file_sha256(path)
        ):
            raise EnglishLegacyDispositionError("legacy_requirements_stale")
    mastered = _mapping(requirements.get("mastered_items_state"), "legacy_requirements_invalid")
    if (
        mastered.get("authority_path") != str(MASTERED_ITEMS_PATH.resolve())
        or mastered.get("current_sha256") != file_sha256(MASTERED_ITEMS_PATH)
        or mastered.get("row_count") != 0
        or mastered.get("status") != "unchanged_no_rows"
    ):
        raise EnglishLegacyDispositionError("legacy_requirements_stale")

    master_rows = _read_master_bank_rows(MASTER_BANK_PATH)
    selected_master = sorted(
        record_id
        for record_id, (row, _digest) in master_rows.items()
        if row.get("last_seen") == "2026-08-06"
    )
    master_group = by_kind["master_bank_row"]
    observed_set_sha = hashlib.sha256(
        json_file_bytes(selected_master)
    ).hexdigest()
    if (
        len(selected_master) != master_group.get("observed_target_count")
        or observed_set_sha != master_group.get("observed_target_id_set_sha256")
    ):
        raise EnglishLegacyDispositionError("legacy_requirements_target_set_mismatch")
    pattern_group = by_kind["sentence_pattern_card"]
    pattern_ids = sorted(_sequence(pattern_group.get("observed_target_ids"), "legacy_requirements_invalid"))
    pattern_cards = _read_pattern_cards(PATTERN_PATH)
    if (
        len(pattern_ids) != pattern_group.get("observed_target_count")
        or hashlib.sha256(json_file_bytes(pattern_ids)).hexdigest()
        != pattern_group.get("observed_target_id_set_sha256")
        or any(record_id not in pattern_cards for record_id in pattern_ids)
    ):
        raise EnglishLegacyDispositionError("legacy_requirements_target_set_mismatch")

    authority_files = sorted(
        [
            {
                "authority_path": str(path.resolve()),
                "sha256": file_sha256(path),
                "role": ALLOWED_AUTHORITY_PATHS[str(path.resolve())],
            }
            for path in (MASTER_BANK_PATH, PATTERN_PATH, ARTICLE_PATH, MASTERED_ITEMS_PATH)
        ],
        key=lambda row: row["authority_path"],
    )
    authority = {
        "generation": "english-legacy-en-p0-006-review-template-v2",
        "files": authority_files,
    }
    authority["fingerprint"] = value_sha256(_authority_core(authority))
    thread_id = _nonempty(requirements.get("originating_thread_id"), "legacy_requirements_invalid")

    def pending_target(kind: str, record_id: str, object_sha: str) -> dict[str, Any]:
        path = str(path_by_kind[kind].resolve())
        return {
            "target_id": _expected_target_id(kind, record_id),
            "target_kind": kind,
            "record_id": record_id,
            "authority_path": path,
            "authority_file_sha256": file_sha256(path_by_kind[kind]),
            "current_object_sha256": object_sha,
            "prehash_status": "evidence_incomplete",
            "prehash_sha256": None,
            "originating_thread_id": thread_id,
            "independent_review": {
                "status": "pending",
                "verdict": None,
                "receipt_sha256": None,
            },
            "user_selection": {
                "disposition": None,
                "authorization_event_id": None,
                "user_message_sha256": None,
            },
        }

    targets = [
        pending_target("master_bank_row", record_id, master_rows[record_id][1])
        for record_id in selected_master
    ]
    targets.extend(
        pending_target("sentence_pattern_card", record_id, pattern_cards[record_id])
        for record_id in pattern_ids
    )
    targets.append(
        pending_target(
            "article_learning_page",
            "articles/2026-07-10-2011-english-i-text-4.md",
            file_sha256(ARTICLE_PATH),
        )
    )
    targets = sorted(targets, key=lambda row: row["target_id"])
    if len(targets) != requirements.get("identified_target_count"):
        raise EnglishLegacyDispositionError("legacy_requirements_target_set_mismatch")
    unidentified = requirements.get("unidentified_target_count_lower_bound")
    if isinstance(unidentified, bool) or not isinstance(unidentified, int) or unidentified < 1:
        raise EnglishLegacyDispositionError("legacy_requirements_invalid")
    return {
        "schema_version": TEMPLATE_SCHEMA,
        "issue_id": ISSUE_ID,
        "subject": SUBJECT,
        "status": "review_required_not_signable",
        "inventory_status": "evidence_incomplete",
        "requirements_sha256": document_sha256(requirements),
        "originating_thread_id": thread_id,
        "authority": authority,
        "identified_target_count": len(targets),
        "unidentified_target_count_lower_bound": unidentified,
        "targets": targets,
        "unresolved_targets": [
            {
                "placeholder_id": "UNRESOLVED-001",
                "reason": "The historical mutation proof boundary remains unidentified; independent inventory review must resolve it before sealing.",
            }
        ],
        "allowed_dispositions": list(DISPOSITIONS),
        "mastered_items_boundary": {
            "authority_path": str(MASTERED_ITEMS_PATH.resolve()),
            "sha256": file_sha256(MASTERED_ITEMS_PATH),
            "row_count": 0,
            "inference_forbidden": True,
            "used_for_disposition": False,
        },
        "disposition_receipt_count": 0,
        "closure_eligible": False,
        "model_call_count": 0,
        "formal_write_count": 0,
    }


def encode_user_message_sha256(message: str) -> str:
    """Return a stable digest helper without persisting the user message."""

    return hashlib.sha256(message.encode("utf-8")).hexdigest()


def _read_pattern_card_objects(path: Path) -> dict[str, dict[str, Any]]:
    path = _authority_read_path(path)
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise EnglishLegacyDispositionError("legacy_pattern_parse_failed") from exc
    matches = list(re.finditer(r"(?m)^## (SP-[0-9]{3})(?:｜|\|).*$", text))
    cards: dict[str, dict[str, Any]] = {}
    for index, match in enumerate(matches):
        record_id = match.group(1)
        if record_id in cards:
            raise EnglishLegacyDispositionError("legacy_pattern_parse_failed")
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        content = text[match.start():end]
        cards[record_id] = {
            "record_id": record_id,
            "start_line": text.count("\n", 0, match.start()) + 1,
            "end_line": text.count("\n", 0, end) + 1,
            "content": content,
            "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        }
    if not cards:
        raise EnglishLegacyDispositionError("legacy_pattern_parse_failed")
    return cards


def _rollout_patch_events(path: Path) -> list[dict[str, Any]]:
    if path.expanduser().resolve() != ROLLOUT_PATH.resolve():
        raise EnglishLegacyDispositionError("legacy_rollout_path_invalid")
    rows: list[dict[str, Any]] = []
    window_event_sha256s: list[str] = []
    window_hasher = hashlib.sha256()
    last_timestamp: str | None = None
    try:
        with path.open("rb") as handle:
            remaining = os.fstat(handle.fileno()).st_size
            while remaining > 0:
                raw_bytes = handle.readline(remaining)
                if not raw_bytes:
                    break
                remaining -= len(raw_bytes)
                if not raw_bytes.endswith(b"\n"):
                    if remaining == 0:
                        break
                    raise EnglishLegacyDispositionError(
                        "legacy_rollout_parse_failed"
                    )
                raw_line = raw_bytes.decode("utf-8")
                value = json.loads(raw_line)
                if not isinstance(value, Mapping):
                    raise ValueError("not mapping")
                timestamp = value.get("timestamp")
                payload = value.get("payload")
                if isinstance(timestamp, str):
                    if last_timestamp is not None and timestamp < last_timestamp:
                        raise EnglishLegacyDispositionError(
                            "legacy_rollout_timestamp_regression"
                        )
                    last_timestamp = timestamp
                if (
                    not isinstance(timestamp, str)
                    or timestamp < MUTATION_WINDOW_START
                    or timestamp >= MUTATION_WINDOW_END
                    or value.get("type") != "event_msg"
                    or not isinstance(payload, Mapping)
                    or payload.get("type") != "patch_apply_end"
                ):
                    continue
                event_sha256 = hashlib.sha256(raw_bytes).hexdigest()
                window_event_sha256s.append(event_sha256)
                window_hasher.update(raw_bytes)
                if payload.get("success") is not True:
                    raise EnglishLegacyDispositionError(
                        "legacy_mutation_window_contains_failed_patch"
                    )
                call_id = _nonempty(
                    payload.get("call_id"), "legacy_mutation_event_invalid"
                )
                changes = _mapping(
                    payload.get("changes"), "legacy_mutation_event_invalid"
                )
                if not changes:
                    raise EnglishLegacyDispositionError(
                        "legacy_mutation_event_invalid"
                    )
                checked_changes: list[dict[str, str]] = []
                for raw_path, raw_change in changes.items():
                    authority_path = str(Path(str(raw_path)).expanduser().resolve())
                    if authority_path not in {
                        str(MASTER_BANK_PATH.resolve()),
                        str(PATTERN_PATH.resolve()),
                        str(ARTICLE_PATH.resolve()),
                    }:
                        raise EnglishLegacyDispositionError(
                            "legacy_mutation_window_scope_violation"
                        )
                    change = _mapping(
                        raw_change, "legacy_mutation_event_invalid"
                    )
                    diff = _nonempty(
                        change.get("unified_diff"), "legacy_mutation_event_invalid"
                    )
                    checked_changes.append(
                        {
                            "authority_path": authority_path,
                            "unified_diff": diff,
                            "unified_diff_sha256": hashlib.sha256(
                                diff.encode("utf-8")
                            ).hexdigest(),
                        }
                    )
                rows.append(
                    {
                        "timestamp": timestamp,
                        "call_id": call_id,
                        "changes": sorted(
                            checked_changes, key=lambda row: row["authority_path"]
                        ),
                        "rollout_event_sha256": hashlib.sha256(
                            raw_bytes
                        ).hexdigest(),
                    }
                )
    except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as exc:
        if isinstance(exc, EnglishLegacyDispositionError):
            raise
        raise EnglishLegacyDispositionError("legacy_rollout_parse_failed") from exc
    if (
        tuple(window_event_sha256s) != ROLLOUT_WINDOW_EVENT_SHA256S
        or window_hasher.hexdigest() != ROLLOUT_WINDOW_SHA256
    ):
        raise EnglishLegacyDispositionError("legacy_rollout_window_hash_mismatch")
    rows.sort(key=lambda row: (row["timestamp"], row["call_id"]))
    if len(rows) != 11 or len({row["call_id"] for row in rows}) != len(rows):
        raise EnglishLegacyDispositionError("legacy_mutation_call_set_mismatch")
    return rows


def _diff_hunks(diff: str) -> list[tuple[int, str]]:
    matches = list(
        re.finditer(
            r"(?m)^@@ -[0-9]+(?:,[0-9]+)? \+([0-9]+)(?:,[0-9]+)? @@.*$",
            diff,
        )
    )
    if not matches:
        raise EnglishLegacyDispositionError("legacy_mutation_hunk_invalid")
    return [
        (
            int(match.group(1)),
            diff[
                match.start() : (
                    matches[index + 1].start()
                    if index + 1 < len(matches)
                    else len(diff)
                )
            ],
        )
        for index, match in enumerate(matches)
    ]


def _csv_patch_rows(diff: str) -> tuple[dict[str, str], dict[str, str]]:
    try:
        with _authority_read_path(MASTER_BANK_PATH).open(
            "r", encoding="utf-8-sig", newline=""
        ) as handle:
            fieldnames = csv.DictReader(handle).fieldnames
        if not fieldnames or "id" not in fieldnames:
            raise ValueError("missing id")
        removed: dict[str, str] = {}
        added: dict[str, str] = {}
        for line in diff.splitlines():
            if not line.startswith(("+", "-")) or line.startswith(("+++", "---")):
                continue
            values = next(csv.reader([line[1:]]))
            if len(values) != len(fieldnames):
                continue
            normalized = {
                str(key): str(value) for key, value in zip(fieldnames, values)
            }
            record_id = normalized.get("id", "")
            if not record_id:
                continue
            destination = added if line.startswith("+") else removed
            prior = destination.get(record_id)
            digest = value_sha256(normalized)
            if prior is not None and prior != digest:
                raise EnglishLegacyDispositionError(
                    "legacy_mutation_duplicate_row_version"
                )
            destination[record_id] = digest
        return removed, added
    except (OSError, UnicodeError, csv.Error, ValueError) as exc:
        raise EnglishLegacyDispositionError("legacy_mutation_csv_parse_failed") from exc


def _signed_pattern_blocks(diff: str, sign: str) -> dict[str, list[str]]:
    blocks: dict[str, list[str]] = {}
    active: str | None = None
    for line in diff.splitlines():
        if line.startswith(("+++", "---")):
            active = None
            continue
        if not line.startswith(sign):
            active = None
            continue
        content = line[1:]
        match = re.match(r"^## (SP-[0-9]{3})(?:｜|\|)", content)
        if match:
            active = match.group(1)
            blocks.setdefault(active, []).append(content)
        elif active is not None:
            blocks[active].append(content)
    return blocks


def _pattern_target_ids(
    diff: str,
    cards: Mapping[str, Mapping[str, Any]],
) -> tuple[set[str], set[str], set[str], int]:
    added_blocks = _signed_pattern_blocks(diff, "+")
    removed_blocks = _signed_pattern_blocks(diff, "-")
    explicit_ids = {
        record_id
        for line in diff.splitlines()
        if line.startswith(("+", "-"))
        and not line.startswith(("+++", "---"))
        for record_id in re.findall(r"SP-[0-9]{3}", line)
    }
    touched = set(explicit_ids)
    unmatched_hunks = 0
    ordered_cards = sorted(cards.values(), key=lambda row: int(row["start_line"]))
    for new_start, hunk in _diff_hunks(diff):
        hunk_ids = set(re.findall(r"SP-[0-9]{3}", hunk))
        if hunk_ids:
            continue
        containing = [
            str(row["record_id"])
            for row in ordered_cards
            if int(row["start_line"]) <= new_start <= int(row["end_line"])
        ]
        if len(containing) != 1:
            unmatched_hunks += 1
        else:
            touched.add(containing[0])
    created = set(added_blocks) - set(removed_blocks)
    def without_trailing_blanks(lines: Sequence[str]) -> list[str]:
        checked = list(lines)
        while checked and not checked[-1].strip():
            checked.pop()
        return checked

    moved = {
        record_id
        for record_id in set(added_blocks) & set(removed_blocks)
        if without_trailing_blanks(added_blocks[record_id])
        == without_trailing_blanks(removed_blocks[record_id])
    }
    return touched, created, moved, unmatched_hunks


def _target_set_sha256_v3(targets: Sequence[Mapping[str, Any]]) -> str:
    return value_sha256(
        [
            {
                "target_id": row["target_id"],
                "target_kind": row["target_kind"],
                "record_id": row["record_id"],
                "ordinal": row["ordinal"],
                "historical_operation": row["historical_operation"],
                "authority_path": row["authority_path"],
                "authority_file_sha256": row["authority_file_sha256"],
                "current_object_sha256": row["current_object_sha256"],
                "origin_postimage_sha256": row["origin_postimage_sha256"],
            }
            for row in targets
        ]
    )


def _projection_side_effect_inventory() -> list[dict[str, Any]]:
    script_sha256 = file_sha256(RELATION_REBUILD_SCRIPT)
    rows = [
        {
            "side_effect_id": side_effect_id,
            "path": str(path.resolve()),
            "sha256": file_sha256(path),
            "role": "derived_projection",
            "counts_as_formal_target": False,
            "rebuild_script_path": str(RELATION_REBUILD_SCRIPT.resolve()),
            "rebuild_script_sha256": script_sha256,
            "verification_mode": "deterministic_rebuild_verify_only",
        }
        for side_effect_id, path in PROJECTION_SIDE_EFFECT_PATHS
    ]
    if len(rows) != 6 or len({row["path"] for row in rows}) != len(rows):
        raise EnglishLegacyDispositionError(
            "legacy_projection_side_effect_inventory_invalid"
        )
    return rows


def _validate_projection_side_effect_inventory(
    value: Any, *, require_live_reconciliation: bool
) -> list[dict[str, Any]]:
    rows = _sequence(value, "legacy_projection_side_effect_inventory_invalid")
    checked: list[dict[str, Any]] = []
    expected_ids = [row[0] for row in PROJECTION_SIDE_EFFECT_PATHS]
    for raw in rows:
        row = _mapping(raw, "legacy_projection_side_effect_inventory_invalid")
        if (
            set(row)
            != {
                "side_effect_id",
                "path",
                "sha256",
                "role",
                "counts_as_formal_target",
                "rebuild_script_path",
                "rebuild_script_sha256",
                "verification_mode",
            }
            or row.get("role") != "derived_projection"
            or row.get("counts_as_formal_target") is not False
            or row.get("verification_mode")
            != "deterministic_rebuild_verify_only"
        ):
            raise EnglishLegacyDispositionError(
                "legacy_projection_side_effect_inventory_invalid"
            )
        _nonempty(
            row.get("side_effect_id"),
            "legacy_projection_side_effect_inventory_invalid",
        )
        _nonempty(row.get("path"), "legacy_projection_side_effect_inventory_invalid")
        _nonempty(
            row.get("rebuild_script_path"),
            "legacy_projection_side_effect_inventory_invalid",
        )
        _sha256(row.get("sha256"), "legacy_projection_side_effect_inventory_invalid")
        _sha256(
            row.get("rebuild_script_sha256"),
            "legacy_projection_side_effect_inventory_invalid",
        )
        checked.append(dict(row))
    if (
        len(checked) != 6
        or [row["side_effect_id"] for row in checked] != expected_ids
        or len({row["path"] for row in checked}) != len(checked)
    ):
        raise EnglishLegacyDispositionError(
            "legacy_projection_side_effect_inventory_invalid"
        )
    if require_live_reconciliation and checked != _projection_side_effect_inventory():
        raise EnglishLegacyDispositionError(
            "legacy_projection_side_effect_inventory_drift"
        )
    return checked


def build_complete_inventory_v3_draft(
    *,
    rollout_path: Path = ROLLOUT_PATH,
    inventory_id: str,
    independent_review_receipt_sha256: str,
    issued_at: str,
    mcp_authority_generation: str,
    mcp_authority_fingerprint: str,
) -> dict[str, Any]:
    """Reconstruct the complete EN-P0-006 target set from immutable evidence."""

    _nonempty(inventory_id, "legacy_inventory_invalid")
    _sha256(
        independent_review_receipt_sha256,
        "legacy_inventory_review_invalid",
    )
    _timestamp(issued_at, "legacy_inventory_issued_at_invalid")
    checked_mcp_generation = _nonempty(
        mcp_authority_generation, "legacy_inventory_mcp_authority_invalid"
    )
    checked_mcp_fingerprint = _sha256(
        mcp_authority_fingerprint, "legacy_inventory_mcp_authority_invalid"
    )
    events = _rollout_patch_events(rollout_path)
    master_rows = _read_master_bank_rows(MASTER_BANK_PATH)
    pattern_cards = _read_pattern_card_objects(PATTERN_PATH)
    master_added: dict[str, str] = {}
    master_removed: dict[str, str] = {}
    pattern_touched: set[str] = set()
    pattern_created: set[str] = set()
    pattern_moved: set[str] = set()
    target_evidence: dict[str, list[dict[str, str]]] = {}
    article_touched = False
    unmatched_hunks = 0

    def bind_evidence(target_id: str, event: Mapping[str, Any], change: Mapping[str, Any]) -> None:
        row = {
            "call_id": str(event["call_id"]),
            "timestamp": str(event["timestamp"]),
            "unified_diff_sha256": str(change["unified_diff_sha256"]),
            "rollout_event_sha256": str(event["rollout_event_sha256"]),
        }
        bucket = target_evidence.setdefault(target_id, [])
        if row not in bucket:
            bucket.append(row)

    for event in events:
        for change in event["changes"]:
            path = change["authority_path"]
            diff = change["unified_diff"]
            if path == str(MASTER_BANK_PATH.resolve()):
                removed, added = _csv_patch_rows(diff)
                for record_id, digest in removed.items():
                    if record_id in master_removed and master_removed[record_id] != digest:
                        raise EnglishLegacyDispositionError(
                            "legacy_mutation_duplicate_row_version"
                        )
                    master_removed[record_id] = digest
                    bind_evidence(
                        _expected_target_id("master_bank_row", record_id),
                        event,
                        change,
                    )
                for record_id, digest in added.items():
                    if record_id in master_added and master_added[record_id] != digest:
                        raise EnglishLegacyDispositionError(
                            "legacy_mutation_duplicate_row_version"
                        )
                    master_added[record_id] = digest
                    bind_evidence(
                        _expected_target_id("master_bank_row", record_id),
                        event,
                        change,
                    )
            elif path == str(PATTERN_PATH.resolve()):
                touched, created, moved, misses = _pattern_target_ids(
                    diff, pattern_cards
                )
                unmatched_hunks += misses
                pattern_touched.update(touched)
                pattern_created.update(created)
                pattern_moved.update(moved)
                for record_id in touched:
                    bind_evidence(
                        _expected_target_id("sentence_pattern_card", record_id),
                        event,
                        change,
                    )
            elif path == str(ARTICLE_PATH.resolve()):
                article_touched = True
                for _new_start, _hunk in _diff_hunks(diff):
                    pass
                bind_evidence(
                    _expected_target_id("article_learning_page", ARTICLE_RECORD_ID),
                    event,
                    change,
                )

    mutation_master_ids = set(master_added) | set(master_removed)
    if not mutation_master_ids or unmatched_hunks != 0 or not article_touched:
        raise EnglishLegacyDispositionError("legacy_mutation_write_set_incomplete")
    if set(master_removed) - set(master_added):
        raise EnglishLegacyDispositionError("legacy_mutation_deleted_target_forbidden")
    if not pattern_touched or pattern_touched - set(pattern_cards):
        raise EnglishLegacyDispositionError("legacy_mutation_write_set_incomplete")

    current_master_candidates = {
        record_id
        for record_id, (row, _digest) in master_rows.items()
        if row.get("last_seen") == "2026-08-06"
    }
    current_pattern_candidates = {
        record_id
        for record_id, row in pattern_cards.items()
        if "2026-08-06" in str(row["content"])
    } | pattern_moved
    missing_from_current = sorted(
        {
            _expected_target_id("master_bank_row", record_id)
            for record_id in mutation_master_ids - current_master_candidates
        }
        | {
            _expected_target_id("sentence_pattern_card", record_id)
            for record_id in pattern_touched - current_pattern_candidates
        }
    )
    unexpected_current = sorted(
        {
            _expected_target_id("master_bank_row", record_id)
            for record_id in current_master_candidates - mutation_master_ids
        }
        | {
            _expected_target_id("sentence_pattern_card", record_id)
            for record_id in current_pattern_candidates - pattern_touched
        }
    )
    if missing_from_current or unexpected_current:
        raise EnglishLegacyDispositionError("legacy_inventory_bidirectional_diff_nonzero")

    authority_files = sorted(
        [
            {
                "authority_path": str(path.resolve()),
                "sha256": file_sha256(path),
                "role": ALLOWED_AUTHORITY_PATHS[str(path.resolve())],
            }
            for path in (
                MASTER_BANK_PATH,
                PATTERN_PATH,
                ARTICLE_PATH,
                MASTERED_ITEMS_PATH,
            )
        ],
        key=lambda row: row["authority_path"],
    )
    authority = {
        "generation": WRITE_SET_AUTHORITY_GENERATION,
        "files": authority_files,
    }
    authority["fingerprint"] = value_sha256(_authority_core(authority))
    mcp_authority = {
        "generation": checked_mcp_generation,
        "authority_fingerprint": checked_mcp_fingerprint,
    }
    authority_sha_by_path = {
        row["authority_path"]: row["sha256"] for row in authority_files
    }

    targets: list[dict[str, Any]] = []

    def append_target(
        *,
        kind: str,
        record_id: str,
        operation: str,
        object_sha256: str,
        prehash_sha256: str | None,
    ) -> None:
        target_id = _expected_target_id(kind, record_id)
        path = {
            "master_bank_row": MASTER_BANK_PATH,
            "sentence_pattern_card": PATTERN_PATH,
            "article_learning_page": ARTICLE_PATH,
        }[kind]
        evidence = sorted(
            target_evidence.get(target_id, []),
            key=lambda row: (row["timestamp"], row["call_id"]),
        )
        if not evidence:
            raise EnglishLegacyDispositionError("legacy_mutation_target_without_evidence")
        targets.append(
            {
                "target_id": target_id,
                "target_kind": kind,
                "record_id": record_id,
                "ordinal": 0,
                "historical_operation": operation,
                "authority_path": str(path.resolve()),
                "authority_file_sha256": authority_sha_by_path[str(path.resolve())],
                "current_object_sha256": object_sha256,
                "origin_postimage_sha256": object_sha256,
                "authority_generation": mcp_authority["generation"],
                "authority_fingerprint": mcp_authority[
                    "authority_fingerprint"
                ],
                "prehash_status": (
                    "verified" if prehash_sha256 is not None else "evidence_incomplete"
                ),
                "prehash_sha256": prehash_sha256,
                "originating_thread_id": ORIGINATING_THREAD_ID,
                "origin_write_evidence": evidence,
                "independent_identity_review": {
                    "status": "completed",
                    "method": "mutation_write_set_to_current_authority_reconciliation",
                    "inventory_review_receipt_sha256": independent_review_receipt_sha256,
                },
            }
        )

    for record_id in sorted(mutation_master_ids):
        row = master_rows.get(record_id)
        if row is None:
            raise EnglishLegacyDispositionError("legacy_inventory_target_missing")
        append_target(
            kind="master_bank_row",
            record_id=record_id,
            operation="updated" if record_id in master_removed else "created",
            object_sha256=row[1],
            prehash_sha256=master_removed.get(record_id),
        )
    for record_id in sorted(pattern_touched):
        operation = "updated"
        if record_id in pattern_moved:
            operation = "representation_reorder_only"
        elif record_id in pattern_created:
            operation = "created"
        append_target(
            kind="sentence_pattern_card",
            record_id=record_id,
            operation=operation,
            object_sha256=str(pattern_cards[record_id]["sha256"]),
            prehash_sha256=(
                str(pattern_cards[record_id]["sha256"])
                if operation == "representation_reorder_only"
                else None
            ),
        )
    append_target(
        kind="article_learning_page",
        record_id=ARTICLE_RECORD_ID,
        operation="updated",
        object_sha256=file_sha256(ARTICLE_PATH),
        prehash_sha256=None,
    )
    targets.sort(key=lambda row: row["target_id"])
    for ordinal, target in enumerate(targets, start=1):
        target["ordinal"] = ordinal
    if len({row["target_id"] for row in targets}) != len(targets):
        raise EnglishLegacyDispositionError("legacy_inventory_duplicate_target")
    target_set_sha256 = _target_set_sha256_v3(targets)
    try:
        with _authority_read_path(MASTERED_ITEMS_PATH).open(
            "r", encoding="utf-8-sig", newline=""
        ) as handle:
            mastered_count = sum(1 for _ in csv.DictReader(handle))
    except (OSError, UnicodeError, csv.Error) as exc:
        raise EnglishLegacyDispositionError("legacy_mastered_items_unreadable") from exc
    call_proofs = [
        {
            "call_id": row["call_id"],
            "timestamp": row["timestamp"],
            "changed_paths": [change["authority_path"] for change in row["changes"]],
            "change_set_sha256": value_sha256(
                [
                    {
                        "authority_path": change["authority_path"],
                        "unified_diff_sha256": change["unified_diff_sha256"],
                    }
                    for change in row["changes"]
                ]
            ),
            "rollout_event_sha256": row["rollout_event_sha256"],
        }
        for row in events
    ]
    return {
        "schema_version": INVENTORY_V3_SCHEMA,
        "issue_id": ISSUE_ID,
        "subject": SUBJECT,
        "originating_thread_id": ORIGINATING_THREAD_ID,
        "inventory_id": inventory_id,
        "inventory_status": "complete",
        "authority": authority,
        "mcp_authority": mcp_authority,
        "write_set_proof": {
            "rollout_path": str(rollout_path.resolve()),
            "rollout_sha256": ROLLOUT_SHA256,
            "mutation_window_start": MUTATION_WINDOW_START,
            "mutation_window_end": MUTATION_WINDOW_END,
            "successful_apply_patch_call_count": len(call_proofs),
            "successful_apply_patch_calls": call_proofs,
            "mutation_target_count": len(targets),
            "current_authority_candidate_count": (
                len(current_master_candidates) + len(current_pattern_candidates) + 1
            ),
            "missing_from_current_authority": [],
            "unexpected_current_candidates": [],
            "unmatched_mutation_call_count": 0,
            "unmatched_hunk_count": 0,
            "projection_side_effects": _projection_side_effect_inventory(),
        },
        "targets": targets,
        "target_count": len(targets),
        "target_set_sha256": target_set_sha256,
        "unidentified_target_count": 0,
        "omitted_target_count": 0,
        "duplicate_target_count": 0,
        "unknown_target_count": 0,
        "mastered_items_boundary": {
            "authority_path": str(MASTERED_ITEMS_PATH.resolve()),
            "sha256": file_sha256(MASTERED_ITEMS_PATH),
            "row_count": mastered_count,
            "inference_forbidden": True,
            "used_for_disposition": False,
        },
        "independent_inventory_review": {
            "status": "completed",
            "method": "bidirectional_zero_diff",
            "receipt_sha256": independent_review_receipt_sha256,
            "reviewed_target_set_sha256": target_set_sha256,
        },
        "model_call_count": 0,
        "formal_write_count": 0,
        "issued_at": issued_at,
    }


def build_inventory_independent_review_receipt_draft(
    *,
    review_id: str,
    reviewer_identity: str,
    reviewed_at: str,
    mcp_authority_generation: str,
    mcp_authority_fingerprint: str,
    rollout_path: Path = ROLLOUT_PATH,
) -> dict[str, Any]:
    """Build the exact independent-review statement that may be HMAC sealed."""

    _nonempty(review_id, "legacy_inventory_review_invalid")
    _nonempty(reviewer_identity, "legacy_inventory_review_invalid")
    _timestamp(reviewed_at, "legacy_inventory_review_invalid")
    subject = build_complete_inventory_v3_draft(
        rollout_path=rollout_path,
        inventory_id="EN-P0-006-INDEPENDENT-REVIEW-SUBJECT",
        independent_review_receipt_sha256="0" * 64,
        issued_at=reviewed_at,
        mcp_authority_generation=mcp_authority_generation,
        mcp_authority_fingerprint=mcp_authority_fingerprint,
    )
    return {
        "schema_version": INVENTORY_REVIEW_RECEIPT_SCHEMA,
        "issue_id": ISSUE_ID,
        "subject": SUBJECT,
        "review_id": review_id,
        "reviewer_identity": reviewer_identity,
        "review_method": (
            "independent_mutation_to_authority_bidirectional_review"
        ),
        "status": "completed",
        "verdict": "complete_zero_diff_verified",
        "rollout_sha256": ROLLOUT_SHA256,
        "authority_generation": subject["mcp_authority"]["generation"],
        "authority_fingerprint": subject["mcp_authority"][
            "authority_fingerprint"
        ],
        "target_set_sha256": subject["target_set_sha256"],
        "target_count": subject["target_count"],
        "write_set_proof_sha256": value_sha256(subject["write_set_proof"]),
        "unidentified_target_count": 0,
        "omitted_target_count": 0,
        "duplicate_target_count": 0,
        "unknown_target_count": 0,
        "reviewed_at": reviewed_at,
        "model_call_count": 0,
        "formal_write_count": 0,
    }


def validate_inventory_independent_review_receipt_v1(
    value: Mapping[str, Any], *, require_live_reconciliation: bool = True
) -> dict[str, Any]:
    receipt = dict(value)
    if set(receipt) != {
        "schema_version",
        "issue_id",
        "subject",
        "review_id",
        "reviewer_identity",
        "review_method",
        "status",
        "verdict",
        "rollout_sha256",
        "authority_generation",
        "authority_fingerprint",
        "target_set_sha256",
        "target_count",
        "write_set_proof_sha256",
        "unidentified_target_count",
        "omitted_target_count",
        "duplicate_target_count",
        "unknown_target_count",
        "reviewed_at",
        "model_call_count",
        "formal_write_count",
        "authority_key_id",
        "seal",
    }:
        raise EnglishLegacyDispositionError("legacy_inventory_review_invalid")
    if (
        receipt.get("schema_version") != INVENTORY_REVIEW_RECEIPT_SCHEMA
        or receipt.get("issue_id") != ISSUE_ID
        or receipt.get("subject") != SUBJECT
        or receipt.get("review_method")
        != "independent_mutation_to_authority_bidirectional_review"
        or receipt.get("status") != "completed"
        or receipt.get("verdict") != "complete_zero_diff_verified"
        or receipt.get("rollout_sha256") != ROLLOUT_SHA256
        or any(
            receipt.get(field) != 0
            for field in (
                "unidentified_target_count",
                "omitted_target_count",
                "duplicate_target_count",
                "unknown_target_count",
                "model_call_count",
                "formal_write_count",
            )
        )
    ):
        raise EnglishLegacyDispositionError("legacy_inventory_review_invalid")
    for field in ("review_id", "reviewer_identity", "authority_generation"):
        _nonempty(receipt.get(field), "legacy_inventory_review_invalid")
    for field in (
        "authority_fingerprint",
        "target_set_sha256",
        "write_set_proof_sha256",
        "authority_key_id",
    ):
        _sha256(receipt.get(field), "legacy_inventory_review_invalid")
    count = receipt.get("target_count")
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise EnglishLegacyDispositionError("legacy_inventory_review_invalid")
    reviewed_at = _timestamp(
        receipt.get("reviewed_at"), "legacy_inventory_review_invalid"
    )
    _seal_shape(
        receipt.get("seal"),
        purpose="english-legacy-inventory-independent-review-receipt-v1",
    )
    if not require_live_reconciliation:
        return receipt
    expected = build_inventory_independent_review_receipt_draft(
        review_id=str(receipt["review_id"]),
        reviewer_identity=str(receipt["reviewer_identity"]),
        reviewed_at=reviewed_at,
        mcp_authority_generation=str(receipt["authority_generation"]),
        mcp_authority_fingerprint=str(receipt["authority_fingerprint"]),
    )
    core = dict(receipt)
    core.pop("authority_key_id")
    core.pop("seal")
    if core != expected:
        raise EnglishLegacyDispositionError(
            "legacy_inventory_review_reconciliation_mismatch"
        )
    return receipt


def validate_inventory_v3(
    value: Mapping[str, Any], *, require_live_reconciliation: bool = True
) -> dict[str, Any]:
    inventory = dict(value)
    if inventory.get("schema_version") != INVENTORY_V3_SCHEMA:
        raise EnglishLegacyDispositionError("legacy_inventory_invalid")
    if set(inventory) != {
        "schema_version",
        "issue_id",
        "subject",
        "originating_thread_id",
        "inventory_id",
        "inventory_status",
        "authority",
        "mcp_authority",
        "write_set_proof",
        "targets",
        "target_count",
        "target_set_sha256",
        "unidentified_target_count",
        "omitted_target_count",
        "duplicate_target_count",
        "unknown_target_count",
        "mastered_items_boundary",
        "independent_inventory_review",
        "model_call_count",
        "formal_write_count",
        "issued_at",
        "authority_key_id",
        "seal",
    }:
        raise EnglishLegacyDispositionError("legacy_inventory_invalid")
    _sha256(inventory.get("authority_key_id"), "legacy_inventory_key_id_invalid")
    _seal_shape(
        inventory.get("seal"), purpose="english-legacy-target-inventory-v3"
    )
    if (
        inventory.get("issue_id") != ISSUE_ID
        or inventory.get("subject") != SUBJECT
        or inventory.get("originating_thread_id") != ORIGINATING_THREAD_ID
        or inventory.get("inventory_status") != "complete"
        or inventory.get("unidentified_target_count") != 0
        or inventory.get("omitted_target_count") != 0
        or inventory.get("duplicate_target_count") != 0
        or inventory.get("unknown_target_count") != 0
        or inventory.get("model_call_count") != 0
        or inventory.get("formal_write_count") != 0
    ):
        raise EnglishLegacyDispositionError("legacy_inventory_invalid")
    authority = _validate_authority(inventory.get("authority"))
    mcp_authority = _validate_mcp_authority(inventory.get("mcp_authority"))
    targets = _sequence(inventory.get("targets"), "legacy_inventory_invalid")
    checked_targets: list[dict[str, Any]] = []
    for expected_ordinal, raw in enumerate(targets, start=1):
        target = _mapping(raw, "legacy_inventory_target_invalid")
        if set(target) != {
            "target_id",
            "target_kind",
            "record_id",
            "ordinal",
            "historical_operation",
            "authority_path",
            "authority_file_sha256",
            "current_object_sha256",
            "origin_postimage_sha256",
            "authority_generation",
            "authority_fingerprint",
            "prehash_status",
            "prehash_sha256",
            "originating_thread_id",
            "origin_write_evidence",
            "independent_identity_review",
        }:
            raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")
        kind = target.get("target_kind")
        record_id = _nonempty(
            target.get("record_id"), "legacy_inventory_target_invalid"
        )
        if (
            kind not in TARGET_KINDS
            or target.get("target_id") != _expected_target_id(str(kind), record_id)
            or target.get("ordinal") != expected_ordinal
            or target.get("historical_operation") not in ORIGIN_OPERATIONS
            or target.get("originating_thread_id") != ORIGINATING_THREAD_ID
            or target.get("authority_generation")
            != mcp_authority["generation"]
            or target.get("authority_fingerprint")
            != mcp_authority["authority_fingerprint"]
        ):
            raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")
        path = _nonempty(
            target.get("authority_path"), "legacy_inventory_target_invalid"
        )
        authority_map = {
            row["authority_path"]: row["sha256"] for row in authority["files"]
        }
        if (
            ALLOWED_AUTHORITY_PATHS.get(path) != kind
            or target.get("authority_file_sha256") != authority_map.get(path)
        ):
            raise EnglishLegacyDispositionError(
                "legacy_inventory_target_authority_mismatch"
            )
        for field in ("current_object_sha256", "origin_postimage_sha256"):
            _sha256(target.get(field), "legacy_inventory_target_invalid")
        if target.get("current_object_sha256") != target.get(
            "origin_postimage_sha256"
        ):
            raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")
        prehash_status = target.get("prehash_status")
        if prehash_status == "verified":
            _sha256(target.get("prehash_sha256"), "legacy_inventory_target_invalid")
        elif prehash_status != "evidence_incomplete" or target.get(
            "prehash_sha256"
        ) is not None:
            raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")
        evidence = _sequence(
            target.get("origin_write_evidence"), "legacy_inventory_target_invalid"
        )
        if not evidence:
            raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")
        for evidence_row in evidence:
            checked = _mapping(evidence_row, "legacy_inventory_target_invalid")
            if set(checked) != {
                "call_id",
                "timestamp",
                "unified_diff_sha256",
                "rollout_event_sha256",
            }:
                raise EnglishLegacyDispositionError("legacy_inventory_target_invalid")
            _nonempty(checked.get("call_id"), "legacy_inventory_target_invalid")
            _timestamp(checked.get("timestamp"), "legacy_inventory_target_invalid")
            _sha256(
                checked.get("unified_diff_sha256"),
                "legacy_inventory_target_invalid",
            )
            _sha256(
                checked.get("rollout_event_sha256"),
                "legacy_inventory_target_invalid",
            )
        identity_review = _mapping(
            target.get("independent_identity_review"),
            "legacy_inventory_review_invalid",
        )
        if (
            set(identity_review)
            != {"status", "method", "inventory_review_receipt_sha256"}
            or identity_review.get("status") != "completed"
            or identity_review.get("method")
            != "mutation_write_set_to_current_authority_reconciliation"
        ):
            raise EnglishLegacyDispositionError("legacy_inventory_review_invalid")
        _sha256(
            identity_review.get("inventory_review_receipt_sha256"),
            "legacy_inventory_review_invalid",
        )
        checked_targets.append(target)
    target_ids = [row["target_id"] for row in checked_targets]
    if (
        not target_ids
        or target_ids != sorted(target_ids)
        or len(target_ids) != len(set(target_ids))
        or inventory.get("target_count") != len(target_ids)
        or inventory.get("target_set_sha256")
        != _target_set_sha256_v3(checked_targets)
    ):
        raise EnglishLegacyDispositionError("legacy_inventory_target_set_mismatch")
    write_set_proof = _mapping(
        inventory.get("write_set_proof"),
        "legacy_inventory_write_set_proof_invalid",
    )
    _validate_projection_side_effect_inventory(
        write_set_proof.get("projection_side_effects"),
        require_live_reconciliation=require_live_reconciliation,
    )
    if not require_live_reconciliation:
        return inventory
    review = _mapping(
        inventory.get("independent_inventory_review"),
        "legacy_inventory_review_invalid",
    )
    receipt_sha256 = _sha256(
        review.get("receipt_sha256"), "legacy_inventory_review_invalid"
    )
    expected = build_complete_inventory_v3_draft(
        rollout_path=Path(
            _mapping(
                inventory.get("write_set_proof"),
                "legacy_inventory_write_set_proof_invalid",
            ).get("rollout_path", "")
        ),
        inventory_id=_nonempty(
            inventory.get("inventory_id"), "legacy_inventory_invalid"
        ),
        independent_review_receipt_sha256=receipt_sha256,
        issued_at=_timestamp(
            inventory.get("issued_at"), "legacy_inventory_issued_at_invalid"
        ),
        mcp_authority_generation=str(mcp_authority["generation"]),
        mcp_authority_fingerprint=str(
            mcp_authority["authority_fingerprint"]
        ),
    )
    core = dict(inventory)
    core.pop("authority_key_id")
    core.pop("seal")
    if core != expected:
        raise EnglishLegacyDispositionError("legacy_inventory_reconciliation_mismatch")
    return inventory


def validate_batch_intent_v1(value: Mapping[str, Any]) -> dict[str, Any]:
    intent = dict(value)
    if set(intent) != {
        "schema_version",
        "issue_id",
        "subject",
        "intent_id",
        "authorization_type",
        "inventory_scope",
        "disposition",
        "authorized_operations",
        "exact_inventory_only",
        "one_shot",
        "user_message_sha256",
        "source_thread_id",
        "source_turn_id",
        "authorized_at",
        "model_call_count",
        "formal_write_count",
        "authority_key_id",
        "seal",
    }:
        raise EnglishLegacyDispositionError("legacy_batch_intent_invalid")
    if (
        intent.get("schema_version") != BATCH_INTENT_SCHEMA
        or intent.get("issue_id") != ISSUE_ID
        or intent.get("subject") != SUBJECT
        or intent.get("authorization_type") != "explicit_user_batch_authorization"
        or intent.get("inventory_scope")
        != "first_independently_verified_complete_inventory"
        or intent.get("disposition") != "deterministic_recuration"
        or tuple(intent.get("authorized_operations") or ()) != AUTHORIZED_OPERATIONS
        or intent.get("exact_inventory_only") is not True
        or intent.get("one_shot") is not True
        or intent.get("model_call_count") != 0
        or intent.get("formal_write_count") != 0
    ):
        raise EnglishLegacyDispositionError("legacy_batch_intent_invalid")
    for field in ("intent_id", "source_thread_id", "source_turn_id"):
        _nonempty(intent.get(field), "legacy_batch_intent_invalid")
    _sha256(intent.get("user_message_sha256"), "legacy_batch_intent_invalid")
    _timestamp(intent.get("authorized_at"), "legacy_batch_intent_invalid")
    _sha256(intent.get("authority_key_id"), "legacy_batch_intent_invalid")
    _seal_shape(
        intent.get("seal"), purpose="english-legacy-batch-authorization-intent-v1"
    )
    return intent


def validate_batch_authorization_v1(value: Mapping[str, Any]) -> dict[str, Any]:
    authorization = dict(value)
    if set(authorization) != {
        "schema_version",
        "issue_id",
        "subject",
        "batch_authorization_id",
        "intent_sha256",
        "inventory_sha256",
        "target_set_sha256",
        "target_count",
        "authority_generation",
        "authority_fingerprint",
        "disposition",
        "authorized_operations",
        "exact_inventory_only",
        "user_message_sha256",
        "authorized_at",
        "materialized_at",
        "model_call_count",
        "formal_write_count",
        "authority_key_id",
        "seal",
    }:
        raise EnglishLegacyDispositionError("legacy_batch_authorization_invalid")
    if (
        authorization.get("schema_version") != BATCH_AUTHORIZATION_SCHEMA
        or authorization.get("issue_id") != ISSUE_ID
        or authorization.get("subject") != SUBJECT
        or authorization.get("disposition") != "deterministic_recuration"
        or tuple(authorization.get("authorized_operations") or ())
        != AUTHORIZED_OPERATIONS
        or authorization.get("exact_inventory_only") is not True
        or authorization.get("model_call_count") != 0
        or authorization.get("formal_write_count") != 0
    ):
        raise EnglishLegacyDispositionError("legacy_batch_authorization_invalid")
    _nonempty(
        authorization.get("batch_authorization_id"),
        "legacy_batch_authorization_invalid",
    )
    for field in (
        "intent_sha256",
        "inventory_sha256",
        "target_set_sha256",
        "authority_fingerprint",
        "user_message_sha256",
        "authority_key_id",
    ):
        _sha256(authorization.get(field), "legacy_batch_authorization_invalid")
    count = authorization.get("target_count")
    if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
        raise EnglishLegacyDispositionError("legacy_batch_authorization_invalid")
    _nonempty(
        authorization.get("authority_generation"),
        "legacy_batch_authorization_invalid",
    )
    authorized_at = _timestamp(
        authorization.get("authorized_at"), "legacy_batch_authorization_invalid"
    )
    materialized_at = _timestamp(
        authorization.get("materialized_at"),
        "legacy_batch_authorization_invalid",
    )
    if dt.datetime.fromisoformat(authorized_at.replace("Z", "+00:00")) > dt.datetime.fromisoformat(
        materialized_at.replace("Z", "+00:00")
    ):
        raise EnglishLegacyDispositionError("legacy_batch_authorization_time_invalid")
    _seal_shape(
        authorization.get("seal"), purpose="english-legacy-batch-authorization-v1"
    )
    return authorization


def validate_target_authorization_event_v3(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    event = dict(value)
    if set(event) != {
        "schema_version",
        "event_id",
        "event_type",
        "issue_id",
        "subject",
        "parent_batch_authorization_sha256",
        "inventory_sha256",
        "target_set_sha256",
        "ordinal",
        "target_id",
        "target_kind",
        "record_id",
        "historical_operation",
        "current_object_sha256",
        "origin_postimage_sha256",
        "disposition",
        "authorized_operations",
        "authorization_scope",
        "idempotency_key",
        "user_message_sha256",
        "authorized_at",
        "materialized_at",
        "authority_key_id",
        "seal",
    }:
        raise EnglishLegacyDispositionError("legacy_target_authorization_event_invalid")
    if (
        event.get("schema_version") != TARGET_AUTHORIZATION_EVENT_SCHEMA
        or event.get("event_type")
        != "materialized_user_english_legacy_disposition"
        or event.get("issue_id") != ISSUE_ID
        or event.get("subject") != SUBJECT
        or event.get("target_kind") not in TARGET_KINDS
        or event.get("historical_operation") not in ORIGIN_OPERATIONS
        or event.get("disposition") != "deterministic_recuration"
        or tuple(event.get("authorized_operations") or ()) != AUTHORIZED_OPERATIONS
        or event.get("authorization_scope") != "exact_target_from_batch"
    ):
        raise EnglishLegacyDispositionError("legacy_target_authorization_event_invalid")
    for field in ("event_id", "record_id"):
        _nonempty(event.get(field), "legacy_target_authorization_event_invalid")
    if event.get("target_id") != _expected_target_id(
        str(event["target_kind"]), str(event["record_id"])
    ):
        raise EnglishLegacyDispositionError("legacy_target_authorization_event_invalid")
    ordinal = event.get("ordinal")
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal <= 0:
        raise EnglishLegacyDispositionError("legacy_target_authorization_event_invalid")
    for field in (
        "parent_batch_authorization_sha256",
        "inventory_sha256",
        "target_set_sha256",
        "current_object_sha256",
        "origin_postimage_sha256",
        "user_message_sha256",
        "authority_key_id",
    ):
        _sha256(event.get(field), "legacy_target_authorization_event_invalid")
    key = _nonempty(
        event.get("idempotency_key"), "legacy_target_authorization_event_invalid"
    )
    if not key.startswith("sha256:") or SHA256_RE.fullmatch(key[7:]) is None:
        raise EnglishLegacyDispositionError("legacy_target_authorization_event_invalid")
    _timestamp(event.get("authorized_at"), "legacy_target_authorization_event_invalid")
    _timestamp(
        event.get("materialized_at"), "legacy_target_authorization_event_invalid"
    )
    _seal_shape(
        event.get("seal"),
        purpose="english-legacy-target-authorization-event-v3",
    )
    return event


def validate_receipt_v3(value: Mapping[str, Any]) -> dict[str, Any]:
    receipt = dict(value)
    if set(receipt) != {
        "schema_version",
        "issue_id",
        "subject",
        "batch_authorization_sha256",
        "authorization_event_sha256",
        "inventory_sha256",
        "target_set_sha256",
        "target",
        "disposition",
        "authority_key_id",
        "model_call_count",
        "formal_write_count",
        "issued_at",
        "seal",
    }:
        raise EnglishLegacyDispositionError("legacy_disposition_receipt_invalid")
    if (
        receipt.get("schema_version") != RECEIPT_V3_SCHEMA
        or receipt.get("issue_id") != ISSUE_ID
        or receipt.get("subject") != SUBJECT
        or receipt.get("disposition") != "deterministic_recuration"
        or receipt.get("model_call_count") != 0
        or receipt.get("formal_write_count") != 0
    ):
        raise EnglishLegacyDispositionError("legacy_disposition_receipt_invalid")
    for field in (
        "batch_authorization_sha256",
        "authorization_event_sha256",
        "inventory_sha256",
        "target_set_sha256",
        "authority_key_id",
    ):
        _sha256(receipt.get(field), "legacy_disposition_receipt_invalid")
    target = _mapping(receipt.get("target"), "legacy_disposition_receipt_invalid")
    if set(target) != {
        "ordinal",
        "target_id",
        "target_kind",
        "record_id",
        "historical_operation",
        "current_object_sha256",
        "origin_postimage_sha256",
    }:
        raise EnglishLegacyDispositionError("legacy_disposition_receipt_invalid")
    if (
        target.get("target_kind") not in TARGET_KINDS
        or target.get("historical_operation") not in ORIGIN_OPERATIONS
        or target.get("target_id")
        != _expected_target_id(
            str(target.get("target_kind")),
            _nonempty(target.get("record_id"), "legacy_disposition_receipt_invalid"),
        )
    ):
        raise EnglishLegacyDispositionError("legacy_disposition_receipt_invalid")
    ordinal = target.get("ordinal")
    if isinstance(ordinal, bool) or not isinstance(ordinal, int) or ordinal <= 0:
        raise EnglishLegacyDispositionError("legacy_disposition_receipt_invalid")
    _sha256(target.get("current_object_sha256"), "legacy_disposition_receipt_invalid")
    _sha256(target.get("origin_postimage_sha256"), "legacy_disposition_receipt_invalid")
    _timestamp(receipt.get("issued_at"), "legacy_disposition_receipt_invalid")
    _seal_shape(
        receipt.get("seal"), purpose="english-legacy-disposition-receipt-v3"
    )
    return receipt


def validate_authorization_expansion_closure_v1(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    closure = dict(value)
    if set(closure) != {
        "schema_version",
        "issue_id",
        "subject",
        "batch_authorization_sha256",
        "intent_sha256",
        "inventory_sha256",
        "target_set_sha256",
        "authority_generation",
        "authority_fingerprint",
        "authorized_operations",
        "target_authorizations",
        "target_count",
        "materialized_target_count",
        "unidentified_target_count",
        "omitted_target_count",
        "duplicate_target_count",
        "unknown_target_count",
        "authority_key_id",
        "model_call_count",
        "formal_write_count",
        "issued_at",
        "seal",
    }:
        raise EnglishLegacyDispositionError(
            "legacy_authorization_expansion_closure_invalid"
        )
    if (
        closure.get("schema_version") != AUTHORIZATION_EXPANSION_CLOSURE_SCHEMA
        or closure.get("issue_id") != ISSUE_ID
        or closure.get("subject") != SUBJECT
        or tuple(closure.get("authorized_operations") or ())
        != AUTHORIZED_OPERATIONS
        or any(
            closure.get(field) != 0
            for field in (
                "unidentified_target_count",
                "omitted_target_count",
                "duplicate_target_count",
                "unknown_target_count",
                "model_call_count",
                "formal_write_count",
            )
        )
    ):
        raise EnglishLegacyDispositionError(
            "legacy_authorization_expansion_closure_invalid"
        )
    for field in (
        "batch_authorization_sha256",
        "intent_sha256",
        "inventory_sha256",
        "target_set_sha256",
        "authority_fingerprint",
        "authority_key_id",
    ):
        _sha256(
            closure.get(field), "legacy_authorization_expansion_closure_invalid"
        )
    _nonempty(
        closure.get("authority_generation"),
        "legacy_authorization_expansion_closure_invalid",
    )
    count = closure.get("target_count")
    rows = _sequence(
        closure.get("target_authorizations"),
        "legacy_authorization_expansion_closure_invalid",
    )
    if (
        isinstance(count, bool)
        or not isinstance(count, int)
        or count <= 0
        or closure.get("materialized_target_count") != count
        or len(rows) != count
    ):
        raise EnglishLegacyDispositionError(
            "legacy_authorization_expansion_closure_count_mismatch"
        )
    expected_ordinal = 1
    seen_targets: set[str] = set()
    seen_events: set[str] = set()
    seen_receipts: set[str] = set()
    for raw in rows:
        row = _mapping(raw, "legacy_authorization_expansion_closure_invalid")
        if set(row) != {
            "ordinal",
            "target_id",
            "authorization_event_sha256",
            "disposition_receipt_sha256",
        } or row.get("ordinal") != expected_ordinal:
            raise EnglishLegacyDispositionError(
                "legacy_authorization_expansion_closure_invalid"
            )
        expected_ordinal += 1
        target_id = _nonempty(
            row.get("target_id"), "legacy_authorization_expansion_closure_invalid"
        )
        event_sha = _sha256(
            row.get("authorization_event_sha256"),
            "legacy_authorization_expansion_closure_invalid",
        )
        receipt_sha = _sha256(
            row.get("disposition_receipt_sha256"),
            "legacy_authorization_expansion_closure_invalid",
        )
        if (
            target_id in seen_targets
            or event_sha in seen_events
            or receipt_sha in seen_receipts
        ):
            raise EnglishLegacyDispositionError(
                "legacy_authorization_expansion_duplicate_target"
            )
        seen_targets.add(target_id)
        seen_events.add(event_sha)
        seen_receipts.add(receipt_sha)
    _timestamp(
        closure.get("issued_at"),
        "legacy_authorization_expansion_closure_invalid",
    )
    _seal_shape(
        closure.get("seal"),
        purpose="english-legacy-authorization-expansion-closure-v1",
    )
    return closure


class EnglishLegacyDispositionV3Store(EnglishLegacyDispositionStore):
    """Batch-authorized, per-target HMAC control plane for EN-P0-006."""

    _V3_KINDS = {
        "inventories",
        "inventory-reviews",
        "batch-intents",
        "batch-authorizations",
        "authorization-events",
        "dispositions",
        "authorization-closures",
    }

    def _kind_root(self, kind: str) -> Path:
        if kind not in self._V3_KINDS:
            raise EnglishLegacyDispositionError("legacy_receipt_kind_invalid")
        if self.root.exists() and self.root.is_symlink():
            raise EnglishLegacyDispositionError("legacy_receipt_root_unsafe")
        kind_root = self.root / kind
        if kind_root.exists() and kind_root.is_symlink():
            raise EnglishLegacyDispositionError("legacy_receipt_root_unsafe")
        return kind_root

    def seal_inventory_v3(
        self, core: Mapping[str, Any]
    ) -> tuple[str, Path, dict[str, Any]]:
        value = dict(core)
        if "authority_key_id" in value or "seal" in value:
            raise EnglishLegacyDispositionError("legacy_inventory_draft_invalid")
        review = _mapping(
            value.get("independent_inventory_review"),
            "legacy_inventory_review_invalid",
        )
        review_receipt_sha256 = _sha256(
            review.get("receipt_sha256"), "legacy_inventory_review_invalid"
        )
        review_receipt = self.verify_inventory_independent_review_receipt(
            review_receipt_sha256
        )
        if (
            review_receipt["target_set_sha256"] != value.get("target_set_sha256")
            or review_receipt["target_count"] != value.get("target_count")
            or review_receipt["authority_generation"]
            != _mapping(
                value.get("mcp_authority"),
                "legacy_inventory_mcp_authority_invalid",
            ).get("generation")
            or review_receipt["authority_fingerprint"]
            != _mapping(
                value.get("mcp_authority"),
                "legacy_inventory_mcp_authority_invalid",
            ).get("authority_fingerprint")
            or review_receipt["write_set_proof_sha256"]
            != value_sha256(
                _mapping(
                    value.get("write_set_proof"),
                    "legacy_inventory_write_set_proof_invalid",
                )
            )
        ):
            raise EnglishLegacyDispositionError(
                "legacy_inventory_review_binding_mismatch"
            )
        if dt.datetime.fromisoformat(
            str(review_receipt["reviewed_at"]).replace("Z", "+00:00")
        ) > dt.datetime.fromisoformat(
            _timestamp(
                value.get("issued_at"), "legacy_inventory_issued_at_invalid"
            ).replace("Z", "+00:00")
        ):
            raise EnglishLegacyDispositionError(
                "legacy_inventory_review_after_inventory"
            )
        value["authority_key_id"] = self._key_id()
        inventory = validate_inventory_v3(
            self._seal(value, purpose="english-legacy-target-inventory-v3")
        )
        self._verify_seal(
            inventory, purpose="english-legacy-target-inventory-v3"
        )
        digest, path = self._publish("inventories", inventory)
        return digest, path, inventory

    def seal_inventory_independent_review_receipt(
        self, core: Mapping[str, Any]
    ) -> tuple[str, Path, dict[str, Any]]:
        value = dict(core)
        if "authority_key_id" in value or "seal" in value:
            raise EnglishLegacyDispositionError("legacy_inventory_review_invalid")
        value["authority_key_id"] = self._key_id()
        receipt = validate_inventory_independent_review_receipt_v1(
            self._seal(
                value,
                purpose=(
                    "english-legacy-inventory-independent-review-receipt-v1"
                ),
            )
        )
        self._verify_seal(
            receipt,
            purpose="english-legacy-inventory-independent-review-receipt-v1",
        )
        digest, path = self._publish("inventory-reviews", receipt)
        return digest, path, receipt

    def verify_inventory_independent_review_receipt(
        self, digest: str, *, require_live_reconciliation: bool = True
    ) -> dict[str, Any]:
        receipt = validate_inventory_independent_review_receipt_v1(
            self._read("inventory-reviews", digest),
            require_live_reconciliation=require_live_reconciliation,
        )
        self._verify_seal(
            receipt,
            purpose="english-legacy-inventory-independent-review-receipt-v1",
        )
        if receipt["authority_key_id"] != self._key_id():
            raise EnglishLegacyDispositionError("legacy_authority_key_id_mismatch")
        return receipt

    def verify_inventory_v3(
        self, digest: str, *, require_live_reconciliation: bool = True
    ) -> dict[str, Any]:
        inventory = validate_inventory_v3(
            self._read("inventories", digest),
            require_live_reconciliation=require_live_reconciliation,
        )
        self._verify_seal(
            inventory, purpose="english-legacy-target-inventory-v3"
        )
        if inventory["authority_key_id"] != self._key_id():
            raise EnglishLegacyDispositionError("legacy_authority_key_id_mismatch")
        review = self.verify_inventory_independent_review_receipt(
            inventory["independent_inventory_review"]["receipt_sha256"],
            require_live_reconciliation=require_live_reconciliation,
        )
        if (
            review["target_set_sha256"] != inventory["target_set_sha256"]
            or review["target_count"] != inventory["target_count"]
            or review["authority_generation"]
            != inventory["mcp_authority"]["generation"]
            or review["authority_fingerprint"]
            != inventory["mcp_authority"]["authority_fingerprint"]
            or review["write_set_proof_sha256"]
            != value_sha256(inventory["write_set_proof"])
        ):
            raise EnglishLegacyDispositionError(
                "legacy_inventory_review_binding_mismatch"
            )
        return inventory

    def seal_batch_intent(
        self, core: Mapping[str, Any]
    ) -> tuple[str, Path, dict[str, Any]]:
        value = dict(core)
        if "authority_key_id" in value or "seal" in value:
            raise EnglishLegacyDispositionError("legacy_batch_intent_invalid")
        value["authority_key_id"] = self._key_id()
        intent = validate_batch_intent_v1(
            self._seal(
                value, purpose="english-legacy-batch-authorization-intent-v1"
            )
        )
        self._verify_seal(
            intent, purpose="english-legacy-batch-authorization-intent-v1"
        )
        digest, path = self._publish("batch-intents", intent)
        return digest, path, intent

    def verify_batch_intent(self, digest: str) -> dict[str, Any]:
        intent = validate_batch_intent_v1(self._read("batch-intents", digest))
        self._verify_seal(
            intent, purpose="english-legacy-batch-authorization-intent-v1"
        )
        if intent["authority_key_id"] != self._key_id():
            raise EnglishLegacyDispositionError("legacy_authority_key_id_mismatch")
        return intent

    def _intent_consumption_path(self, intent_sha256: str) -> Path:
        _sha256(intent_sha256, "legacy_batch_intent_invalid")
        root = self.root / "intent-consumptions" / "sha256" / intent_sha256[:2]
        if self.root.exists() and self.root.is_symlink():
            raise EnglishLegacyDispositionError("legacy_receipt_root_unsafe")
        if root.exists() and root.is_symlink():
            raise EnglishLegacyDispositionError("legacy_receipt_root_unsafe")
        return root / f"{intent_sha256}.json"

    def _claim_intent(
        self,
        *,
        intent_sha256: str,
        inventory_sha256: str,
        batch_authorization_sha256: str,
    ) -> None:
        core = {
            "schema_version": "english_legacy_batch_intent_consumption_v1",
            "intent_sha256": intent_sha256,
            "inventory_sha256": inventory_sha256,
            "batch_authorization_sha256": batch_authorization_sha256,
            "authority_key_id": self._key_id(),
        }
        marker = self._seal(
            core, purpose="english-legacy-batch-intent-consumption-v1"
        )
        payload = json_file_bytes(marker)
        path = self._intent_consumption_path(intent_sha256)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.exists():
            if path.read_bytes() == payload:
                return
            raise EnglishLegacyDispositionError(
                "legacy_batch_intent_already_consumed"
            )
        descriptor, name = tempfile.mkstemp(
            prefix=f".{intent_sha256}.", dir=path.parent
        )
        temporary = Path(name)
        try:
            os.fchmod(descriptor, 0o400)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_bytes() != payload:
                    raise EnglishLegacyDispositionError(
                        "legacy_batch_intent_already_consumed"
                    )
            os.chmod(path, 0o400)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def materialize_batch_authorization(
        self,
        *,
        intent_sha256: str,
        inventory_sha256: str,
    ) -> tuple[str, Path, dict[str, Any]]:
        intent = self.verify_batch_intent(intent_sha256)
        inventory = self.verify_inventory_v3(inventory_sha256)
        authorized_at = _timestamp(
            intent["authorized_at"], "legacy_batch_intent_invalid"
        )
        materialized_at = _timestamp(
            inventory["issued_at"], "legacy_inventory_issued_at_invalid"
        )
        if dt.datetime.fromisoformat(authorized_at.replace("Z", "+00:00")) > dt.datetime.fromisoformat(
            materialized_at.replace("Z", "+00:00")
        ):
            raise EnglishLegacyDispositionError(
                "legacy_batch_inventory_predates_intent"
            )
        identity = value_sha256(
            {
                "intent_sha256": intent_sha256,
                "inventory_sha256": inventory_sha256,
                "target_set_sha256": inventory["target_set_sha256"],
            }
        )
        core = {
            "schema_version": BATCH_AUTHORIZATION_SCHEMA,
            "issue_id": ISSUE_ID,
            "subject": SUBJECT,
            "batch_authorization_id": f"EN-P0-006-BATCH-{identity[:20]}",
            "intent_sha256": intent_sha256,
            "inventory_sha256": inventory_sha256,
            "target_set_sha256": inventory["target_set_sha256"],
            "target_count": inventory["target_count"],
            "authority_generation": inventory["mcp_authority"]["generation"],
            "authority_fingerprint": inventory["mcp_authority"][
                "authority_fingerprint"
            ],
            "disposition": "deterministic_recuration",
            "authorized_operations": list(AUTHORIZED_OPERATIONS),
            "exact_inventory_only": True,
            "user_message_sha256": intent["user_message_sha256"],
            "authorized_at": authorized_at,
            "materialized_at": materialized_at,
            "model_call_count": 0,
            "formal_write_count": 0,
            "authority_key_id": self._key_id(),
        }
        authorization = validate_batch_authorization_v1(
            self._seal(core, purpose="english-legacy-batch-authorization-v1")
        )
        self._verify_seal(
            authorization, purpose="english-legacy-batch-authorization-v1"
        )
        digest, path = self._publish("batch-authorizations", authorization)
        self._claim_intent(
            intent_sha256=intent_sha256,
            inventory_sha256=inventory_sha256,
            batch_authorization_sha256=digest,
        )
        return digest, path, authorization

    def verify_batch_authorization(
        self, digest: str, *, require_live_inventory: bool = True
    ) -> dict[str, Any]:
        authorization = validate_batch_authorization_v1(
            self._read("batch-authorizations", digest)
        )
        self._verify_seal(
            authorization, purpose="english-legacy-batch-authorization-v1"
        )
        if authorization["authority_key_id"] != self._key_id():
            raise EnglishLegacyDispositionError("legacy_authority_key_id_mismatch")
        intent = self.verify_batch_intent(authorization["intent_sha256"])
        inventory = self.verify_inventory_v3(
            authorization["inventory_sha256"],
            require_live_reconciliation=require_live_inventory,
        )
        expected = {
            "intent_sha256": authorization["intent_sha256"],
            "inventory_sha256": authorization["inventory_sha256"],
            "target_set_sha256": inventory["target_set_sha256"],
        }
        identity = value_sha256(expected)
        if (
            authorization["batch_authorization_id"]
            != f"EN-P0-006-BATCH-{identity[:20]}"
            or authorization["target_set_sha256"]
            != inventory["target_set_sha256"]
            or authorization["target_count"] != inventory["target_count"]
            or authorization["authority_generation"]
            != inventory["mcp_authority"]["generation"]
            or authorization["authority_fingerprint"]
            != inventory["mcp_authority"]["authority_fingerprint"]
            or authorization["user_message_sha256"]
            != intent["user_message_sha256"]
            or authorization["authorized_at"] != intent["authorized_at"]
            or authorization["materialized_at"] != inventory["issued_at"]
        ):
            raise EnglishLegacyDispositionError(
                "legacy_batch_authorization_binding_mismatch"
            )
        marker_path = self._intent_consumption_path(
            authorization["intent_sha256"]
        )
        marker = load_json(marker_path, "legacy_batch_intent_consumption_invalid")
        self._verify_seal(
            marker, purpose="english-legacy-batch-intent-consumption-v1"
        )
        if (
            marker.get("intent_sha256") != authorization["intent_sha256"]
            or marker.get("inventory_sha256") != authorization["inventory_sha256"]
            or marker.get("batch_authorization_sha256") != digest
            or marker.get("authority_key_id") != self._key_id()
        ):
            raise EnglishLegacyDispositionError(
                "legacy_batch_intent_consumption_invalid"
            )
        return authorization

    @staticmethod
    def _target_authorization_binding(target: Mapping[str, Any]) -> dict[str, Any]:
        return {
            key: target[key]
            for key in (
                "ordinal",
                "target_id",
                "target_kind",
                "record_id",
                "historical_operation",
                "current_object_sha256",
                "origin_postimage_sha256",
            )
        }

    def _build_target_event(
        self,
        *,
        batch_authorization_sha256: str,
        authorization: Mapping[str, Any],
        target: Mapping[str, Any],
    ) -> dict[str, Any]:
        idempotency_digest = value_sha256(
            {
                "batch_authorization_sha256": batch_authorization_sha256,
                "target_id": target["target_id"],
                "ordinal": target["ordinal"],
            }
        )
        core = {
            "schema_version": TARGET_AUTHORIZATION_EVENT_SCHEMA,
            "event_id": (
                f"EN-P0-006-TARGET-AUTH-{target['ordinal']:03d}-"
                f"{idempotency_digest[:12]}"
            ),
            "event_type": "materialized_user_english_legacy_disposition",
            "issue_id": ISSUE_ID,
            "subject": SUBJECT,
            "parent_batch_authorization_sha256": batch_authorization_sha256,
            "inventory_sha256": authorization["inventory_sha256"],
            "target_set_sha256": authorization["target_set_sha256"],
            **self._target_authorization_binding(target),
            "disposition": "deterministic_recuration",
            "authorized_operations": list(AUTHORIZED_OPERATIONS),
            "authorization_scope": "exact_target_from_batch",
            "idempotency_key": f"sha256:{idempotency_digest}",
            "user_message_sha256": authorization["user_message_sha256"],
            "authorized_at": authorization["authorized_at"],
            "materialized_at": authorization["materialized_at"],
            "authority_key_id": self._key_id(),
        }
        return validate_target_authorization_event_v3(
            self._seal(
                core, purpose="english-legacy-target-authorization-event-v3"
            )
        )

    def _build_target_receipt(
        self,
        *,
        batch_authorization_sha256: str,
        authorization: Mapping[str, Any],
        target: Mapping[str, Any],
        event_sha256: str,
    ) -> dict[str, Any]:
        core = {
            "schema_version": RECEIPT_V3_SCHEMA,
            "issue_id": ISSUE_ID,
            "subject": SUBJECT,
            "batch_authorization_sha256": batch_authorization_sha256,
            "authorization_event_sha256": event_sha256,
            "inventory_sha256": authorization["inventory_sha256"],
            "target_set_sha256": authorization["target_set_sha256"],
            "target": self._target_authorization_binding(target),
            "disposition": "deterministic_recuration",
            "authority_key_id": self._key_id(),
            "model_call_count": 0,
            "formal_write_count": 0,
            "issued_at": authorization["materialized_at"],
        }
        return validate_receipt_v3(
            self._seal(core, purpose="english-legacy-disposition-receipt-v3")
        )

    def expand_batch_authorization(
        self, batch_authorization_sha256: str
    ) -> tuple[str, Path, dict[str, Any]]:
        authorization = self.verify_batch_authorization(
            batch_authorization_sha256
        )
        inventory = self.verify_inventory_v3(authorization["inventory_sha256"])
        target_authorizations: list[dict[str, Any]] = []
        for target in inventory["targets"]:
            event = self._build_target_event(
                batch_authorization_sha256=batch_authorization_sha256,
                authorization=authorization,
                target=target,
            )
            self._verify_seal(
                event, purpose="english-legacy-target-authorization-event-v3"
            )
            event_sha256, _event_path = self._publish(
                "authorization-events", event
            )
            receipt = self._build_target_receipt(
                batch_authorization_sha256=batch_authorization_sha256,
                authorization=authorization,
                target=target,
                event_sha256=event_sha256,
            )
            self._verify_seal(
                receipt, purpose="english-legacy-disposition-receipt-v3"
            )
            receipt_sha256, _receipt_path = self._publish(
                "dispositions", receipt
            )
            target_authorizations.append(
                {
                    "ordinal": target["ordinal"],
                    "target_id": target["target_id"],
                    "authorization_event_sha256": event_sha256,
                    "disposition_receipt_sha256": receipt_sha256,
                }
            )
        core = {
            "schema_version": AUTHORIZATION_EXPANSION_CLOSURE_SCHEMA,
            "issue_id": ISSUE_ID,
            "subject": SUBJECT,
            "batch_authorization_sha256": batch_authorization_sha256,
            "intent_sha256": authorization["intent_sha256"],
            "inventory_sha256": authorization["inventory_sha256"],
            "target_set_sha256": authorization["target_set_sha256"],
            "authority_generation": authorization["authority_generation"],
            "authority_fingerprint": authorization["authority_fingerprint"],
            "authorized_operations": list(AUTHORIZED_OPERATIONS),
            "target_authorizations": target_authorizations,
            "target_count": authorization["target_count"],
            "materialized_target_count": len(target_authorizations),
            "unidentified_target_count": 0,
            "omitted_target_count": 0,
            "duplicate_target_count": 0,
            "unknown_target_count": 0,
            "authority_key_id": self._key_id(),
            "model_call_count": 0,
            "formal_write_count": 0,
            "issued_at": authorization["materialized_at"],
        }
        closure = validate_authorization_expansion_closure_v1(
            self._seal(
                core,
                purpose="english-legacy-authorization-expansion-closure-v1",
            )
        )
        self._verify_seal(
            closure,
            purpose="english-legacy-authorization-expansion-closure-v1",
        )
        digest, path = self._publish("authorization-closures", closure)
        return digest, path, closure

    def verify_target_authorization_event_v3(
        self,
        digest: str,
        *,
        batch_authorization_sha256: str,
        authorization: Mapping[str, Any],
        inventory: Mapping[str, Any],
    ) -> dict[str, Any]:
        event = validate_target_authorization_event_v3(
            self._read("authorization-events", digest)
        )
        self._verify_seal(
            event, purpose="english-legacy-target-authorization-event-v3"
        )
        if event["authority_key_id"] != self._key_id():
            raise EnglishLegacyDispositionError("legacy_authority_key_id_mismatch")
        target = next(
            (
                row
                for row in inventory["targets"]
                if row["target_id"] == event["target_id"]
            ),
            None,
        )
        if target is None:
            raise EnglishLegacyDispositionError("legacy_disposition_unknown_target")
        expected = self._build_target_event(
            batch_authorization_sha256=batch_authorization_sha256,
            authorization=authorization,
            target=target,
        )
        if event != expected:
            raise EnglishLegacyDispositionError(
                "legacy_target_authorization_binding_mismatch"
            )
        return event

    def verify_receipt_v3(
        self,
        digest: str,
        *,
        batch_authorization_sha256: str,
        authorization: Mapping[str, Any],
        inventory: Mapping[str, Any],
    ) -> dict[str, Any]:
        receipt = validate_receipt_v3(self._read("dispositions", digest))
        self._verify_seal(
            receipt, purpose="english-legacy-disposition-receipt-v3"
        )
        if receipt["authority_key_id"] != self._key_id():
            raise EnglishLegacyDispositionError("legacy_authority_key_id_mismatch")
        event = self.verify_target_authorization_event_v3(
            receipt["authorization_event_sha256"],
            batch_authorization_sha256=batch_authorization_sha256,
            authorization=authorization,
            inventory=inventory,
        )
        target = next(
            row
            for row in inventory["targets"]
            if row["target_id"] == event["target_id"]
        )
        expected = self._build_target_receipt(
            batch_authorization_sha256=batch_authorization_sha256,
            authorization=authorization,
            target=target,
            event_sha256=receipt["authorization_event_sha256"],
        )
        if receipt != expected:
            raise EnglishLegacyDispositionError(
                "legacy_disposition_receipt_binding_mismatch"
            )
        return receipt

    def verify_authorization_expansion_closure(
        self, digest: str, *, require_live_inventory: bool = True
    ) -> dict[str, Any]:
        closure = validate_authorization_expansion_closure_v1(
            self._read("authorization-closures", digest)
        )
        self._verify_seal(
            closure,
            purpose="english-legacy-authorization-expansion-closure-v1",
        )
        if closure["authority_key_id"] != self._key_id():
            raise EnglishLegacyDispositionError("legacy_authority_key_id_mismatch")
        authorization = self.verify_batch_authorization(
            closure["batch_authorization_sha256"],
            require_live_inventory=require_live_inventory,
        )
        inventory = self.verify_inventory_v3(
            authorization["inventory_sha256"],
            require_live_reconciliation=require_live_inventory,
        )
        if (
            closure["intent_sha256"] != authorization["intent_sha256"]
            or closure["inventory_sha256"] != authorization["inventory_sha256"]
            or closure["target_set_sha256"] != authorization["target_set_sha256"]
            or closure["target_count"] != authorization["target_count"]
            or closure["authority_generation"]
            != authorization["authority_generation"]
            or closure["authority_fingerprint"]
            != authorization["authority_fingerprint"]
        ):
            raise EnglishLegacyDispositionError(
                "legacy_authorization_expansion_binding_mismatch"
            )
        expected_ids = [row["target_id"] for row in inventory["targets"]]
        observed_ids: list[str] = []
        for mapping, target in zip(
            closure["target_authorizations"], inventory["targets"]
        ):
            if (
                mapping["ordinal"] != target["ordinal"]
                or mapping["target_id"] != target["target_id"]
            ):
                raise EnglishLegacyDispositionError(
                    "legacy_authorization_expansion_target_mismatch"
                )
            event = self.verify_target_authorization_event_v3(
                mapping["authorization_event_sha256"],
                batch_authorization_sha256=closure[
                    "batch_authorization_sha256"
                ],
                authorization=authorization,
                inventory=inventory,
            )
            receipt = self.verify_receipt_v3(
                mapping["disposition_receipt_sha256"],
                batch_authorization_sha256=closure[
                    "batch_authorization_sha256"
                ],
                authorization=authorization,
                inventory=inventory,
            )
            if receipt["authorization_event_sha256"] != mapping[
                "authorization_event_sha256"
            ] or event["target_id"] != mapping["target_id"]:
                raise EnglishLegacyDispositionError(
                    "legacy_authorization_expansion_target_mismatch"
                )
            observed_ids.append(mapping["target_id"])
        if observed_ids != expected_ids:
            raise EnglishLegacyDispositionError(
                "legacy_authorization_expansion_target_mismatch"
            )
        return {
            "schema_version": "english_legacy_authorization_expansion_verification_v1",
            "status": "verified_complete",
            "issue_id": ISSUE_ID,
            "authorization_expansion_closure_sha256": digest,
            "batch_authorization_sha256": closure[
                "batch_authorization_sha256"
            ],
            "inventory_sha256": closure["inventory_sha256"],
            "target_set_sha256": closure["target_set_sha256"],
            "target_count": closure["target_count"],
            "event_count": len(closure["target_authorizations"]),
            "receipt_count": len(closure["target_authorizations"]),
            "model_call_count": 0,
            "formal_write_count": 0,
        }

    def verify_existing_inventory_no_recuration_required(
        self, digest: str, *, require_live_inventory: bool = True
    ) -> dict[str, Any]:
        """Close EN-P0-006 as existing history without Luna or a writer.

        The authorization-expansion artifacts are retained only as historical,
        per-target HMAC evidence.  This verifier does not revive their former
        execution authority.  It reopens all 98 child receipts, reconstructs the
        mutation write-set, and verifies every current formal object by identity
        and object hash before returning a zero-model, zero-write outcome.
        """

        verification = self.verify_authorization_expansion_closure(
            digest, require_live_inventory=require_live_inventory
        )
        closure = self.reopen_verified_authorization_expansion_closure(
            digest, require_live_inventory=require_live_inventory
        )
        inventory = self.verify_inventory_v3(
            closure["inventory_sha256"],
            require_live_reconciliation=require_live_inventory,
        )
        targets = inventory["targets"]
        kind_counts = {
            kind: sum(row["target_kind"] == kind for row in targets)
            for kind in TARGET_KINDS
        }
        sp_022 = [
            row
            for row in targets
            if row["target_id"] == "sentence_pattern_card:SP-022"
        ]
        proof = _mapping(
            inventory.get("write_set_proof"),
            "legacy_inventory_write_set_proof_invalid",
        )
        if (
            inventory.get("target_count") != 98
            or kind_counts
            != {
                "article_learning_page": 1,
                "master_bank_row": 88,
                "sentence_pattern_card": 9,
            }
            or len(sp_022) != 1
            or sp_022[0].get("historical_operation")
            != "representation_reorder_only"
            or proof.get("mutation_target_count") != 98
            or proof.get("current_authority_candidate_count") != 98
            or proof.get("missing_from_current_authority") != []
            or proof.get("unexpected_current_candidates") != []
            or proof.get("unmatched_mutation_call_count") != 0
            or proof.get("unmatched_hunk_count") != 0
            or any(
                inventory.get(field) != 0
                for field in (
                    "unidentified_target_count",
                    "omitted_target_count",
                    "duplicate_target_count",
                    "unknown_target_count",
                )
            )
            or verification.get("event_count") != 98
            or verification.get("receipt_count") != 98
            or verification.get("model_call_count") != 0
            or verification.get("formal_write_count") != 0
        ):
            raise EnglishLegacyDispositionError(
                "legacy_existing_inventory_closure_incomplete"
            )
        return {
            "schema_version": (
                "english_legacy_authorization_expansion_verification_v1"
            ),
            "status": "verified_complete",
            "outcome": (
                "verified_existing_inventory_no_recuration_required"
            ),
            "issue_id": ISSUE_ID,
            "authorization_expansion_closure_sha256": digest,
            "historical_batch_authorization_sha256": closure[
                "batch_authorization_sha256"
            ],
            "inventory_sha256": closure["inventory_sha256"],
            "inventory_review_receipt_sha256": inventory[
                "independent_inventory_review"
            ]["receipt_sha256"],
            "target_set_sha256": closure["target_set_sha256"],
            "target_count": 98,
            "target_kind_counts": kind_counts,
            "sp_022_verified": True,
            "mutation_write_set_verified": True,
            "current_object_identity_and_hash_verified_count": 98,
            "per_target_hmac_receipt_count": 98,
            "unidentified_target_count": 0,
            "omitted_target_count": 0,
            "duplicate_target_count": 0,
            "unknown_target_count": 0,
            "luna_recuration_allowed": False,
            "sol_parent_batch_allowed": False,
            "model_call_count": 0,
            "formal_write_count": 0,
        }

    def reopen_verified_authorization_expansion_closure(
        self, digest: str, *, require_live_inventory: bool = True
    ) -> dict[str, Any]:
        """Return the complete closure only after every child proof is verified."""

        self.verify_authorization_expansion_closure(
            digest, require_live_inventory=require_live_inventory
        )
        closure = validate_authorization_expansion_closure_v1(
            self._read("authorization-closures", digest)
        )
        self._verify_seal(
            closure,
            purpose="english-legacy-authorization-expansion-closure-v1",
        )
        return closure

    def evaluate_remediation_gate(
        self, authorization_expansion_closure_sha256: str
    ) -> dict[str, Any]:
        self.verify_existing_inventory_no_recuration_required(
            authorization_expansion_closure_sha256
        )
        raise EnglishLegacyDispositionError(
            "legacy_recuration_authorization_revoked"
        )
