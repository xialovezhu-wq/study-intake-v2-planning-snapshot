"""Fail-closed EN-P0-006 writer for an isolated English authority copy.

The adapter deliberately has no production-root mode.  It reopens one staged
Sol batch and one HMAC-sealed item review, locates the canonical object by both
stable id and a normalized semantic key, and emits an unsealed result directory
for :class:`IsolatedWriterAdapterPublisher`.

Only ``update_existing``, ``already_current`` and ``reject`` are representable.
Create, merge, delete, re-key and identity-changing updates are rejected.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import csv
import datetime as dt
import fcntl
import hashlib
import hmac
import io
import json
import multiprocessing
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import tempfile
import unicodedata
import uuid
from typing import Any

from subject_sol_contract import (
    SubjectSolRuntimeStore,
    SubjectSolContractError,
    _canonical_bytes,
    _document_sha256,
    _json_file_bytes,
    _safe_component,
    validate_english_legacy_recuration_sol_batch_v1,
    validate_english_legacy_sol_item_review_receipt_v1,
)
from english_legacy_recuration import (
    EnglishLegacyRecurationError,
    reopen_work_item_batch,
)


ISSUE_ID = "EN-P0-006"
SUBJECT = "english"
REQUEST_SCHEMA = "english_legacy_writer_request_v1"
MARKER_SCHEMA = "english_legacy_isolated_copy_marker_v1"
INSPECTION_SCHEMA = "english_legacy_writer_inspection_v1"
TARGET_SET_AUDIT_SCHEMA = "english_legacy_writer_target_set_audit_v1"
DEFAULT_SOURCE_ROOT = Path("/Users/xiazhibin/Documents/kaoyan-english")
MARKER_NAME = ".english-legacy-isolated-copy-v1.json"
MASTER_RELATIVE_PATH = PurePosixPath("bank/master_bank.csv")
PATTERN_RELATIVE_PATH = PurePosixPath("bank/sentence_patterns.md")
ARTICLE_RELATIVE_PATH = PurePosixPath(
    "articles/2026-07-10-2011-english-i-text-4.md"
)
MASTERED_ITEMS_RELATIVE_PATH = PurePosixPath("bank/mastered_items.csv")
AUTHORITY_RELATIVE_PATHS = (
    MASTER_RELATIVE_PATH,
    PATTERN_RELATIVE_PATH,
    ARTICLE_RELATIVE_PATH,
    MASTERED_ITEMS_RELATIVE_PATH,
)
TARGET_KINDS = {
    "master_bank_row",
    "sentence_pattern_card",
    "article_learning_page",
}
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PATTERN_HEADING_RE = re.compile(
    r"(?m)^## (SP-[0-9]{3})(?:｜|\|)([^\r\n]*)$"
)
_SOURCE_ID_RE = re.compile(r"(?m)^- source_id[：:]\s*(\S.*?)\s*$")


class EnglishLegacyWriterError(ValueError):
    """Stable, machine-readable fail-closed adapter error."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _value_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    try:
        if path.is_symlink() or not path.is_file():
            raise OSError("unsafe file")
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        return digest.hexdigest()
    except OSError as exc:
        raise EnglishLegacyWriterError("english_writer_authority_file_unreadable") from exc


def _load_json(path: Path, code: str, *, max_bytes: int = 16 * 1024 * 1024) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > max_bytes:
            raise OSError("unsafe json")
        payload = path.read_bytes()
        value = json.loads(payload.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EnglishLegacyWriterError(code) from exc
    if not isinstance(value, Mapping):
        raise EnglishLegacyWriterError(code)
    return dict(value)


def _write_json(path: Path, value: Mapping[str, Any], *, mode: int = 0o600) -> None:
    payload = _json_file_bytes(dict(value))
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _atomic_replace_bytes(path: Path, payload: bytes) -> None:
    descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(name)
    try:
        os.fchmod(descriptor, path.stat().st_mode & 0o777)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _safe_authority_path(root: Path, relative: PurePosixPath) -> Path:
    if relative.is_absolute() or ".." in relative.parts:
        raise EnglishLegacyWriterError("english_writer_authority_path_invalid")
    root_resolved = root.resolve()
    path = root_resolved.joinpath(*relative.parts)
    try:
        if path.is_symlink() or not path.is_file():
            raise OSError("unsafe authority")
        resolved = path.resolve(strict=True)
        resolved.relative_to(root_resolved)
    except (OSError, ValueError) as exc:
        raise EnglishLegacyWriterError("english_writer_authority_path_invalid") from exc
    return resolved


def authority_manifest(root: Path) -> dict[str, Any]:
    """Hash the three writable objects plus the mastered-items boundary."""

    resolved = root.resolve()
    files = [
        {
            "relative_path": relative.as_posix(),
            "sha256": _file_sha256(_safe_authority_path(resolved, relative)),
        }
        for relative in AUTHORITY_RELATIVE_PATHS
    ]
    return {
        "root": str(resolved),
        "files": files,
        "manifest_sha256": _value_sha256(files),
    }


def tree_manifest(root: Path) -> dict[str, Any]:
    """Hash a complete store tree without following symlinks."""

    resolved = root.resolve()
    entries: list[dict[str, str]] = []
    try:
        for path in sorted(resolved.rglob("*"), key=lambda item: item.as_posix()):
            relative = path.relative_to(resolved).as_posix()
            if path.is_symlink():
                entries.append(
                    {
                        "relative_path": relative,
                        "type": "symlink",
                        "sha256": hashlib.sha256(
                            os.readlink(path).encode("utf-8")
                        ).hexdigest(),
                    }
                )
            elif path.is_file():
                entries.append(
                    {
                        "relative_path": relative,
                        "type": "file",
                        "sha256": _file_sha256(path),
                    }
                )
    except OSError as exc:
        raise EnglishLegacyWriterError("english_writer_tree_manifest_failed") from exc
    return {
        "root": str(resolved),
        "entry_count": len(entries),
        "manifest_sha256": _value_sha256(entries),
    }


def prepare_isolated_copy(source_root: Path, isolated_root: Path) -> dict[str, Any]:
    """Create a complete, non-overwriting authority copy with a safety marker."""

    source = source_root.resolve()
    destination = isolated_root.resolve()
    if source == destination or destination == DEFAULT_SOURCE_ROOT.resolve():
        raise EnglishLegacyWriterError("english_writer_real_authority_target_forbidden")
    if destination.exists():
        raise EnglishLegacyWriterError("english_writer_isolated_root_exists")
    source_before = authority_manifest(source)
    source_tree_before = tree_manifest(source)
    try:
        shutil.copytree(source, destination, symlinks=True)
        copied = authority_manifest(destination)
        if [row["sha256"] for row in copied["files"]] != [
            row["sha256"] for row in source_before["files"]
        ]:
            raise EnglishLegacyWriterError("english_writer_copy_hash_mismatch")
        copied_tree = tree_manifest(destination)
        if copied_tree["manifest_sha256"] != source_tree_before["manifest_sha256"]:
            raise EnglishLegacyWriterError("english_writer_copy_tree_hash_mismatch")
        source_after = authority_manifest(source)
        source_tree_after = tree_manifest(source)
        if source_after["manifest_sha256"] != source_before["manifest_sha256"]:
            raise EnglishLegacyWriterError("english_writer_source_changed_during_copy")
        if source_tree_after["manifest_sha256"] != source_tree_before["manifest_sha256"]:
            raise EnglishLegacyWriterError("english_writer_source_tree_changed_during_copy")
        marker = {
            "schema_version": MARKER_SCHEMA,
            "issue_id": ISSUE_ID,
            "subject": SUBJECT,
            "copy_id": f"EN-P0-006-COPY-{uuid.uuid4().hex}",
            "source_root": str(source),
            "isolated_root": str(destination),
            "source_authority_manifest_sha256": source_before["manifest_sha256"],
            "initial_copy_authority_manifest_sha256": copied["manifest_sha256"],
            "source_tree_manifest_sha256": source_tree_before["manifest_sha256"],
            "initial_copy_tree_manifest_sha256": copied_tree["manifest_sha256"],
            "created_at": _utc_now(),
            "formal_write_count": 0,
        }
        marker["marker_sha256"] = _value_sha256(marker)
        _write_json(destination / MARKER_NAME, marker, mode=0o400)
        return marker
    except BaseException:
        if destination.exists() and destination != source:
            shutil.rmtree(destination)
        raise


def _validate_marker(isolated_root: Path, copy_id: str) -> dict[str, Any]:
    root = isolated_root.resolve()
    if root == DEFAULT_SOURCE_ROOT.resolve():
        raise EnglishLegacyWriterError("english_writer_real_authority_target_forbidden")
    marker = _load_json(root / MARKER_NAME, "english_writer_isolated_marker_invalid")
    required = {
        "schema_version", "issue_id", "subject", "copy_id", "source_root",
        "isolated_root", "source_authority_manifest_sha256",
        "initial_copy_authority_manifest_sha256", "source_tree_manifest_sha256",
        "initial_copy_tree_manifest_sha256", "created_at",
        "formal_write_count", "marker_sha256",
    }
    core = dict(marker)
    claimed = core.pop("marker_sha256", None)
    if (
        set(marker) != required
        or marker.get("schema_version") != MARKER_SCHEMA
        or marker.get("issue_id") != ISSUE_ID
        or marker.get("subject") != SUBJECT
        or marker.get("formal_write_count") != 0
        or marker.get("copy_id") != copy_id
        or marker.get("isolated_root") != str(root)
        or marker.get("source_root") == str(root)
        or not isinstance(claimed, str)
        or not hmac.compare_digest(claimed, _value_sha256(core))
    ):
        raise EnglishLegacyWriterError("english_writer_isolated_marker_invalid")
    for field in (
        "source_authority_manifest_sha256",
        "initial_copy_authority_manifest_sha256",
        "source_tree_manifest_sha256",
        "initial_copy_tree_manifest_sha256",
    ):
        if _SHA256_RE.fullmatch(str(marker.get(field))) is None:
            raise EnglishLegacyWriterError("english_writer_isolated_marker_invalid")
    for relative in AUTHORITY_RELATIVE_PATHS:
        _safe_authority_path(root, relative)
    return marker


def _require_content_addressed(path: Path, root: Path, digest: str, code: str) -> None:
    expected = root / "sha256" / digest[:2] / f"{digest}.json"
    try:
        if (
            path.resolve(strict=True) != expected.resolve(strict=True)
            or path.is_symlink()
            or path.stat().st_mode & 0o777 != 0o400
        ):
            raise OSError("wrong path")
    except OSError as exc:
        raise EnglishLegacyWriterError(code) from exc


def _verify_review_seal(review: Mapping[str, Any], key_path: Path) -> None:
    document = dict(review)
    seal = document.pop("seal", None)
    if not isinstance(seal, Mapping) or set(seal) != {
        "algorithm", "purpose", "hmac_sha256"
    }:
        raise EnglishLegacyWriterError("english_writer_review_seal_invalid")
    try:
        if key_path.is_symlink():
            raise OSError("unsafe key")
        mode = key_path.stat().st_mode & 0o777
        key = key_path.read_bytes()
    except OSError as exc:
        raise EnglishLegacyWriterError("english_writer_review_key_unavailable") from exc
    if mode & 0o077 or len(key) < 32:
        raise EnglishLegacyWriterError("english_writer_review_key_invalid")
    purpose = "english-legacy-sol-item-review-receipt-v1"
    expected = hmac.new(
        key,
        _canonical_bytes({"purpose": purpose, "payload": document}),
        hashlib.sha256,
    ).hexdigest()
    if (
        seal.get("algorithm") != "HMAC-SHA256"
        or seal.get("purpose") != purpose
        or not isinstance(seal.get("hmac_sha256"), str)
        or not hmac.compare_digest(str(seal["hmac_sha256"]), expected)
    ):
        raise EnglishLegacyWriterError("english_writer_review_hmac_invalid")


def _verified_inputs(request: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, Any], str]:
    runtime = Path(str(request["runtime_root"])).resolve()
    batch_path = Path(str(request["batch_path"]))
    review_path = Path(str(request["review_receipt_path"]))
    try:
        batch_payload = batch_path.read_bytes()
        review_payload = review_path.read_bytes()
        batch_raw = json.loads(batch_payload.decode("utf-8"))
        review_raw = json.loads(review_payload.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EnglishLegacyWriterError("english_writer_verified_input_unreadable") from exc
    if not isinstance(batch_raw, Mapping) or not isinstance(review_raw, Mapping):
        raise EnglishLegacyWriterError("english_writer_verified_input_invalid")
    if _json_file_bytes(batch_raw) != batch_payload or _json_file_bytes(review_raw) != review_payload:
        raise EnglishLegacyWriterError("english_writer_verified_input_not_canonical")
    try:
        batch = validate_english_legacy_recuration_sol_batch_v1(batch_raw)
        review = validate_english_legacy_sol_item_review_receipt_v1(
            review_raw, batch=batch
        )
    except SubjectSolContractError as exc:
        raise EnglishLegacyWriterError(f"english_writer_contract_{exc}") from exc
    batch_sha = hashlib.sha256(batch_payload).hexdigest()
    review_sha = hashlib.sha256(review_payload).hexdigest()
    _require_content_addressed(
        batch_path,
        runtime / "dispatch" / "english-legacy" / "sol-batches",
        batch_sha,
        "english_writer_parent_batch_not_authoritative",
    )
    _require_content_addressed(
        review_path,
        runtime / "dispatch" / "writer-adapter-receipts"
        / "english-legacy-item-review",
        review_sha,
        "english_writer_item_review_not_authoritative",
    )
    _verify_review_seal(
        review,
        runtime / "dispatch" / "state" / "external-authorities"
        / "sol-writer-adapter.key",
    )
    if review["batch_sha256"] != _document_sha256(batch):
        raise EnglishLegacyWriterError("english_writer_parent_batch_hash_mismatch")
    return batch, review, review_sha


def _verified_runtime_state(
    request: Mapping[str, Any],
    batch: Mapping[str, Any],
    review: Mapping[str, Any],
    review_sha: str,
) -> str:
    """Reopen the current Sol lease without mutating the control plane."""

    runtime = Path(str(request["runtime_root"])).resolve()
    global_path = runtime / "dispatch/state/global-sol-writer.json"
    state_path = (
        runtime / "dispatch/state/english-legacy-sol-batches"
        / f"{_safe_component(str(batch['batch_id']))}.json"
    )
    global_raw = _load_json(
        global_path, "english_writer_global_sol_state_unavailable"
    )
    state_raw = _load_json(
        state_path, "english_writer_item_sol_state_unavailable"
    )
    batch_sha = _document_sha256(batch)
    try:
        global_state = SubjectSolRuntimeStore._validate_global(global_raw)
        state = SubjectSolRuntimeStore._validate_english_legacy_state(
            state_raw, batch=batch, batch_sha256=batch_sha
        )
    except SubjectSolContractError as exc:
        raise EnglishLegacyWriterError(
            f"english_writer_runtime_state_{exc}"
        ) from exc
    active = global_state.get("active_writer")
    if not isinstance(active, Mapping):
        raise EnglishLegacyWriterError("english_writer_verified_review_not_current")
    item = state["items"][int(review["ordinal"]) - 1]
    attempts = item["attempts"]
    expected_active_status = "failed" if review["decision"] == "reject" else "applying"
    expected_item_status = "pending" if review["decision"] == "reject" else "reviewed"
    if (
        active.get("batch_id") != batch["batch_id"]
        or active.get("subject") != SUBJECT
        or active.get("daily_sol_batch_sha256") != batch_sha
        or active.get("fencing_token") != review["fencing_token"]
        or active.get("review_receipt_sha256") != review_sha
        or active.get("status") != expected_active_status
        or active.get("current_item") != review["target_id"]
        or state.get("status") != "active"
        or state.get("fencing_token") != review["fencing_token"]
        or state.get("attempt") != review["attempt"]
        or state.get("current_ordinal") != review["ordinal"]
        or state.get("current_target_id") != review["target_id"]
        or state.get("current_review_receipt_sha256") != review_sha
        or state.get("current_checkpoint_sha256")
        != review["previous_checkpoint_sha256"]
        or item.get("status") != expected_item_status
        or not attempts
        or attempts[-1].get("attempt") != review["attempt"]
        or attempts[-1].get("fencing_token") != review["fencing_token"]
        or attempts[-1].get("review_receipt_sha256") != review_sha
        or attempts[-1].get("apply_receipt_sha256") is not None
        or attempts[-1].get("failure_receipt_sha256") is not None
    ):
        raise EnglishLegacyWriterError("english_writer_verified_review_not_current")
    return _value_sha256(
        {
            "global_state_sha256": _file_sha256(global_path),
            "item_state_sha256": _file_sha256(state_path),
            "review_receipt_sha256": review_sha,
        }
    )


def validate_request(value: Mapping[str, Any]) -> dict[str, Any]:
    request = dict(value)
    required = {
        "schema_version", "mode", "runtime_root", "batch_path",
        "review_receipt_path", "isolated_root", "result_dir", "copy_id",
        "desired_object", "fault_injection",
    }
    if (
        set(request) != required
        or request.get("schema_version") != REQUEST_SCHEMA
        or request.get("mode") not in {"dry_run", "apply"}
        or request.get("fault_injection") not in {None, "after_replace_before_verify"}
    ):
        raise EnglishLegacyWriterError("english_writer_request_invalid")
    for field in (
        "runtime_root", "batch_path", "review_receipt_path", "isolated_root",
        "result_dir", "copy_id",
    ):
        if not isinstance(request.get(field), str) or not request[field].strip():
            raise EnglishLegacyWriterError("english_writer_request_invalid")
    for field in ("runtime_root", "batch_path", "review_receipt_path", "isolated_root", "result_dir"):
        if not Path(request[field]).is_absolute():
            raise EnglishLegacyWriterError("english_writer_request_path_not_absolute")
    desired = request.get("desired_object")
    if desired is not None and not isinstance(desired, (str, Mapping)):
        raise EnglishLegacyWriterError("english_writer_desired_object_invalid")
    marker = _validate_marker(Path(request["isolated_root"]), request["copy_id"])
    result = Path(request["result_dir"]).resolve()
    protected_roots = {
        Path(str(marker["source_root"])).resolve(),
        DEFAULT_SOURCE_ROOT.resolve(),
    }
    if any(result == protected or protected in result.parents for protected in protected_roots):
        raise EnglishLegacyWriterError("english_writer_result_in_real_authority_forbidden")
    batch, review, review_sha = _verified_inputs(request)
    _verified_runtime_state(request, batch, review, review_sha)
    return request


def _normalize_semantic(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def _split_target(review: Mapping[str, Any]) -> tuple[str, str]:
    kind = str(review["target_kind"])
    target_id = str(review["target_id"])
    prefix = f"{kind}:"
    if kind not in TARGET_KINDS or not target_id.startswith(prefix):
        raise EnglishLegacyWriterError("english_writer_target_identity_invalid")
    record_id = target_id[len(prefix):]
    if not record_id:
        raise EnglishLegacyWriterError("english_writer_target_identity_invalid")
    return kind, record_id


def _read_master(path: Path) -> tuple[list[str], list[dict[str, str]], bytes]:
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text, newline=""))
        if not reader.fieldnames or "id" not in reader.fieldnames:
            raise ValueError("missing header")
        header = [str(field) for field in reader.fieldnames]
        rows = [
            {field: str(row.get(field) or "") for field in header}
            for row in reader
        ]
    except (UnicodeError, csv.Error, ValueError) as exc:
        raise EnglishLegacyWriterError("english_writer_master_bank_parse_failed") from exc
    ids = [row["id"] for row in rows]
    if not all(ids) or len(ids) != len(set(ids)):
        raise EnglishLegacyWriterError("english_writer_master_bank_identity_ambiguous")
    return header, rows, raw


def _master_semantic_key(row: Mapping[str, str]) -> str:
    return _value_sha256(
        {
            "type": _normalize_semantic(str(row.get("type") or "")),
            "item": _normalize_semantic(str(row.get("item") or "")),
        }
    )


def _master_object_hash(row: Mapping[str, str], header: Sequence[str]) -> str:
    return _value_sha256({field: str(row.get(field) or "") for field in header})


def _master_row_bounds(raw: bytes, record_id: str) -> tuple[str, int, int, str]:
    """Return decoded text and exact CSV record bounds for one stable id."""

    try:
        text = raw.decode("utf-8-sig")
        stream = io.StringIO(text, newline="")
        reader = csv.reader(stream)
        header = next(reader)
        id_index = header.index("id")
        matches: list[tuple[int, int, str]] = []
        while True:
            start = stream.tell()
            try:
                row = next(reader)
            except StopIteration:
                break
            end = stream.tell()
            if len(row) != len(header):
                raise ValueError("row width")
            if row[id_index] == record_id:
                segment = text[start:end]
                newline = "\r\n" if segment.endswith("\r\n") else "\n"
                matches.append((start, end, newline))
    except (UnicodeError, csv.Error, ValueError, StopIteration) as exc:
        raise EnglishLegacyWriterError("english_writer_master_bank_parse_failed") from exc
    if len(matches) != 1:
        raise EnglishLegacyWriterError("english_writer_stable_id_not_unique")
    start, end, newline = matches[0]
    return text, start, end, newline


def _read_patterns(path: Path) -> tuple[str, list[dict[str, Any]], bytes]:
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeError as exc:
        raise EnglishLegacyWriterError("english_writer_pattern_parse_failed") from exc
    matches = list(_PATTERN_HEADING_RE.finditer(text))
    cards: list[dict[str, Any]] = []
    ids: set[str] = set()
    for index, match in enumerate(matches):
        record_id = match.group(1)
        if record_id in ids:
            raise EnglishLegacyWriterError("english_writer_pattern_identity_ambiguous")
        ids.add(record_id)
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        segment = text[match.start():end]
        cards.append(
            {
                "id": record_id,
                "title": match.group(2),
                "semantic_key": _normalize_semantic(match.group(2)),
                "start": match.start(),
                "end": end,
                "segment": segment,
                "sha256": hashlib.sha256(segment.encode("utf-8")).hexdigest(),
            }
        )
    return text, cards, raw


def _article_source_id(text: str) -> str:
    match = _SOURCE_ID_RE.search(text)
    if match is None:
        raise EnglishLegacyWriterError("english_writer_article_semantic_key_missing")
    return _normalize_semantic(match.group(1))


def _article_matches(root: Path, semantic_key: str) -> list[str]:
    articles = root / "articles"
    matches: list[str] = []
    try:
        paths = sorted(articles.rglob("*.md"))
    except OSError as exc:
        raise EnglishLegacyWriterError("english_writer_article_scan_failed") from exc
    for path in paths:
        if path.is_symlink() or not path.is_file():
            raise EnglishLegacyWriterError("english_writer_article_scan_unsafe")
        try:
            key = _article_source_id(path.read_text(encoding="utf-8"))
        except EnglishLegacyWriterError as exc:
            if exc.code == "english_writer_article_semantic_key_missing":
                continue
            raise
        except (OSError, UnicodeError) as exc:
            raise EnglishLegacyWriterError("english_writer_article_scan_failed") from exc
        if key == semantic_key:
            matches.append(path.relative_to(root).as_posix())
    return matches


def _locate(root: Path, review: Mapping[str, Any]) -> dict[str, Any]:
    kind, record_id = _split_target(review)
    if kind == "master_bank_row":
        path = _safe_authority_path(root, MASTER_RELATIVE_PATH)
        header, rows, raw = _read_master(path)
        id_rows = [row for row in rows if row["id"] == record_id]
        if len(id_rows) != 1:
            raise EnglishLegacyWriterError("english_writer_stable_id_not_unique")
        row = id_rows[0]
        key = _master_semantic_key(row)
        semantic_rows = [candidate for candidate in rows if _master_semantic_key(candidate) == key]
        if len(semantic_rows) != 1 or semantic_rows[0]["id"] != record_id:
            raise EnglishLegacyWriterError("english_writer_semantic_key_not_unique")
        return {
            "kind": kind, "record_id": record_id, "path": path,
            "header": header, "rows": rows, "raw": raw, "object": row,
            "object_sha256": _master_object_hash(row, header),
            "semantic_key": key,
        }
    if kind == "sentence_pattern_card":
        path = _safe_authority_path(root, PATTERN_RELATIVE_PATH)
        text, cards, raw = _read_patterns(path)
        id_cards = [card for card in cards if card["id"] == record_id]
        if len(id_cards) != 1:
            raise EnglishLegacyWriterError("english_writer_stable_id_not_unique")
        card = id_cards[0]
        semantic_cards = [candidate for candidate in cards if candidate["semantic_key"] == card["semantic_key"]]
        if len(semantic_cards) != 1 or semantic_cards[0]["id"] != record_id:
            raise EnglishLegacyWriterError("english_writer_semantic_key_not_unique")
        return {
            "kind": kind, "record_id": record_id, "path": path,
            "text": text, "cards": cards, "raw": raw,
            "object": card["segment"], "object_sha256": card["sha256"],
            "semantic_key": card["semantic_key"], "start": card["start"],
            "end": card["end"],
        }
    relative = PurePosixPath(record_id)
    if relative.suffix != ".md" or not relative.parts or relative.parts[0] != "articles":
        raise EnglishLegacyWriterError("english_writer_article_record_id_invalid")
    path = _safe_authority_path(root, relative)
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeError as exc:
        raise EnglishLegacyWriterError("english_writer_article_parse_failed") from exc
    key = _article_source_id(text)
    matches = _article_matches(root, key)
    if matches != [relative.as_posix()]:
        raise EnglishLegacyWriterError("english_writer_semantic_key_not_unique")
    return {
        "kind": kind, "record_id": record_id, "path": path, "raw": raw,
        "object": text, "object_sha256": hashlib.sha256(raw).hexdigest(),
        "semantic_key": key,
    }


def _desired_payload(located: Mapping[str, Any], desired: Any) -> tuple[Any, bytes, str]:
    kind = located["kind"]
    if kind == "master_bank_row":
        if not isinstance(desired, Mapping):
            raise EnglishLegacyWriterError("english_writer_desired_master_row_invalid")
        header = list(located["header"])
        if set(desired) != set(header):
            raise EnglishLegacyWriterError("english_writer_desired_master_row_invalid")
        normalized = {field: str(desired[field]) for field in header}
        if normalized["id"] != located["record_id"]:
            raise EnglishLegacyWriterError("english_writer_identity_change_forbidden")
        if _master_semantic_key(normalized) != located["semantic_key"]:
            raise EnglishLegacyWriterError("english_writer_semantic_rekey_forbidden")
        text, start, end, newline = _master_row_bounds(
            bytes(located["raw"]), str(located["record_id"])
        )
        output = io.StringIO(newline="")
        writer = csv.writer(output, lineterminator=newline)
        writer.writerow([normalized[field] for field in header])
        had_bom = bytes(located["raw"]).startswith(b"\xef\xbb\xbf")
        payload = (text[:start] + output.getvalue() + text[end:]).encode("utf-8")
        if had_bom:
            payload = b"\xef\xbb\xbf" + payload
        return normalized, payload, _master_object_hash(normalized, header)
    if not isinstance(desired, str):
        raise EnglishLegacyWriterError("english_writer_desired_text_invalid")
    payload_object = desired
    if kind == "sentence_pattern_card":
        matches = list(_PATTERN_HEADING_RE.finditer(desired))
        if len(matches) != 1 or matches[0].start() != 0 or matches[0].group(1) != located["record_id"]:
            raise EnglishLegacyWriterError("english_writer_desired_pattern_invalid")
        if _normalize_semantic(matches[0].group(2)) != located["semantic_key"]:
            raise EnglishLegacyWriterError("english_writer_semantic_rekey_forbidden")
        text = str(located["text"])
        payload = (
            text[: int(located["start"])] + desired + text[int(located["end"]):]
        ).encode("utf-8")
        return payload_object, payload, hashlib.sha256(desired.encode("utf-8")).hexdigest()
    if _article_source_id(desired) != located["semantic_key"]:
        raise EnglishLegacyWriterError("english_writer_semantic_rekey_forbidden")
    payload = desired.encode("utf-8")
    return payload_object, payload, hashlib.sha256(payload).hexdigest()


def _unchanged_non_target(pre: Mapping[str, Any], post: Mapping[str, Any]) -> bool:
    if pre["kind"] == "master_bank_row":
        header = list(pre["header"])
        pre_hashes = {row["id"]: _master_object_hash(row, header) for row in pre["rows"]}
        post_hashes = {row["id"]: _master_object_hash(row, header) for row in post["rows"]}
    elif pre["kind"] == "sentence_pattern_card":
        pre_hashes = {card["id"]: card["sha256"] for card in pre["cards"]}
        post_hashes = {card["id"]: card["sha256"] for card in post["cards"]}
    else:
        return True
    target = str(pre["record_id"])
    pre_hashes.pop(target, None)
    post_hashes.pop(target, None)
    return pre_hashes == post_hashes


def _worker_logic(request: Mapping[str, Any]) -> dict[str, Any]:
    batch, review, review_sha = _verified_inputs(request)
    runtime_state_sha = _verified_runtime_state(
        request, batch, review, review_sha
    )
    root = Path(str(request["isolated_root"])).resolve()
    _validate_marker(root, str(request["copy_id"]))
    lock_path = root / ".english-legacy-writer.lock"
    if lock_path.is_symlink():
        raise EnglishLegacyWriterError("english_writer_lock_path_unsafe")
    try:
        descriptor = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if not lock_path.is_file():
            raise EnglishLegacyWriterError("english_writer_lock_path_unsafe")
    else:
        os.close(descriptor)
    transaction_id = f"EN-P0-006-TXN-{uuid.uuid4().hex}"
    with lock_path.open("r+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        located = _locate(root, review)
        pre_object = str(located["object_sha256"])
        pre_file = _file_sha256(Path(located["path"]))
        pre_authority = authority_manifest(root)
        pre_authority_manifest = pre_authority["manifest_sha256"]
        pre_authority_files = {
            row["relative_path"]: row["sha256"] for row in pre_authority["files"]
        }
        if pre_object != review["current_object_sha256"]:
            raise EnglishLegacyWriterError("english_writer_object_cas_mismatch")
        base = {
            "batch_id": batch["batch_id"],
            "batch_sha256": _document_sha256(batch),
            "target_id": review["target_id"],
            "target_kind": review["target_kind"],
            "ordinal": review["ordinal"],
            "fencing_token": review["fencing_token"],
            "attempt": review["attempt"],
            "previous_checkpoint_sha256": review["previous_checkpoint_sha256"],
            "review_receipt_sha256": review_sha,
            "authority_checkpoint_sha256": review["authority_checkpoint_sha256"],
            "pre_authority_file_sha256": pre_file,
            "pre_authority_manifest_sha256": pre_authority_manifest,
            "pre_object_sha256": pre_object,
            "semantic_key_sha256": str(located["semantic_key"]),
            "transaction_id": transaction_id,
            "runtime_state_sha256": runtime_state_sha,
        }
        if review["decision"] == "reject":
            return {
                **base, "outcome": "reject", "reason": review["reason"],
                "transaction_state": "failed", "retryable": False,
                "formal_write_count": 0,
            }
        desired = request.get("desired_object")
        if review["decision"] == "already_current":
            if desired is not None or review["desired_object_sha256"] is not None:
                raise EnglishLegacyWriterError("english_writer_already_current_payload_forbidden")
            return {
                **base, "outcome": "already_current",
                "post_authority_file_sha256": pre_file,
                "post_object_sha256": pre_object,
                "transaction_state": "already_current", "operations": [],
                "formal_write_count": 0,
            }
        if desired is None:
            raise EnglishLegacyWriterError("english_writer_desired_object_required")
        _desired, replacement, desired_hash = _desired_payload(located, desired)
        if desired_hash != review["desired_object_sha256"]:
            raise EnglishLegacyWriterError("english_writer_desired_object_hash_mismatch")
        if desired_hash == pre_object:
            raise EnglishLegacyWriterError("english_writer_update_not_material")
        if request["mode"] == "dry_run":
            return {
                **base, "outcome": "planned_update",
                "post_object_sha256": desired_hash,
                "transaction_state": "dry_run", "operations": [],
                "formal_write_count": 0,
            }
        snapshot_path = Path(str(request["result_dir"])) / "rollback-snapshot.bin"
        snapshot_path.write_bytes(bytes(located["raw"]))
        snapshot_path.chmod(0o400)
        replaced = False
        try:
            reopened = _locate(root, review)
            if _verified_runtime_state(
                request, batch, review, review_sha
            ) != runtime_state_sha:
                raise EnglishLegacyWriterError("english_writer_runtime_state_cas_mismatch")
            if (
                reopened["object_sha256"] != pre_object
                or _file_sha256(Path(reopened["path"])) != pre_file
            ):
                raise EnglishLegacyWriterError("english_writer_object_cas_mismatch")
            _atomic_replace_bytes(Path(reopened["path"]), replacement)
            replaced = True
            if request.get("fault_injection") == "after_replace_before_verify":
                raise EnglishLegacyWriterError("english_writer_injected_after_replace")
            post = _locate(root, review)
            post_file = _file_sha256(Path(post["path"]))
            post_authority = authority_manifest(root)
            post_authority_manifest = post_authority["manifest_sha256"]
            post_authority_files = {
                row["relative_path"]: row["sha256"]
                for row in post_authority["files"]
            }
            changed_authority_paths = {
                relative for relative in pre_authority_files
                if pre_authority_files[relative] != post_authority_files[relative]
            }
            target_relative = Path(post["path"]).relative_to(root).as_posix()
            if post["object_sha256"] != desired_hash:
                raise EnglishLegacyWriterError("english_writer_postimage_hash_mismatch")
            if not _unchanged_non_target(reopened, post):
                raise EnglishLegacyWriterError("english_writer_non_target_changed")
            if changed_authority_paths != {target_relative}:
                raise EnglishLegacyWriterError("english_writer_non_target_authority_changed")
            authority_evidence = {
                "previous_rolling_authority_sha256": review["authority_checkpoint_sha256"],
                "target_id": review["target_id"],
                "pre_authority_file_sha256": pre_file,
                "post_authority_file_sha256": post_file,
                "pre_authority_manifest_sha256": pre_authority_manifest,
                "post_authority_manifest_sha256": post_authority_manifest,
                "pre_object_sha256": pre_object,
                "post_object_sha256": desired_hash,
            }
            post_authority = _value_sha256(authority_evidence)
            return {
                **base, "outcome": "committed",
                "post_authority_file_sha256": post_file,
                "post_object_sha256": desired_hash,
                "post_authority_sha256": post_authority,
                "authority_evidence": authority_evidence,
                "transaction_state": "committed",
                "operations": [{
                    "operation": "update_existing",
                    "target_id": review["target_id"],
                    "target_kind": review["target_kind"],
                    "before_object_sha256": pre_object,
                    "after_object_sha256": desired_hash,
                }],
                "formal_write_count": 1,
            }
        except BaseException as exc:
            rollback_state = "failed"
            if replaced:
                try:
                    _atomic_replace_bytes(Path(located["path"]), bytes(located["raw"]))
                    restored = _locate(root, review)
                    if (
                        restored["object_sha256"] != pre_object
                        or _file_sha256(Path(restored["path"])) != pre_file
                        or authority_manifest(root)["manifest_sha256"]
                        != pre_authority_manifest
                    ):
                        raise EnglishLegacyWriterError("english_writer_rollback_hash_mismatch")
                    rollback_state = "rolled_back"
                except BaseException:
                    rollback_state = "failed"
            code = exc.code if isinstance(exc, EnglishLegacyWriterError) else "english_writer_apply_failed"
            return {
                **base, "outcome": "runtime_failure", "reason": code,
                "transaction_state": rollback_state, "retryable": True,
                "post_authority_file_sha256": _file_sha256(Path(located["path"])),
                "post_object_sha256": _locate(root, review)["object_sha256"],
                "formal_write_count": 0,
            }


def _worker_entry(request: dict[str, Any], outcome_path: str) -> None:
    exit_code = 0
    try:
        outcome = _worker_logic(request)
        if outcome.get("outcome") == "runtime_failure":
            exit_code = 17
    except EnglishLegacyWriterError as exc:
        outcome = {"outcome": "runtime_failure", "reason": exc.code}
        exit_code = 17
    except BaseException:
        outcome = {"outcome": "runtime_failure", "reason": "english_writer_worker_unhandled"}
        exit_code = 18
    _write_json(Path(outcome_path), outcome, mode=0o600)
    if exit_code:
        raise SystemExit(exit_code)


def _receipt_base(outcome: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "issue_id": ISSUE_ID,
        "batch_id": outcome["batch_id"],
        "batch_sha256": outcome["batch_sha256"],
        "subject": SUBJECT,
        "target_id": outcome["target_id"],
        "target_kind": outcome["target_kind"],
        "ordinal": outcome["ordinal"],
        "fencing_token": outcome["fencing_token"],
        "attempt": outcome["attempt"],
        "previous_checkpoint_sha256": outcome["previous_checkpoint_sha256"],
    }


def execute_request(request_path: Path) -> dict[str, Any]:
    """Run one isolated writer item in a real child process."""

    request = validate_request(
        _load_json(request_path, "english_writer_request_unreadable")
    )
    batch, review, review_sha = _verified_inputs(request)
    result_dir = Path(request["result_dir"])
    if result_dir.exists():
        raise EnglishLegacyWriterError("english_writer_result_directory_exists")
    result_dir.mkdir(parents=True, mode=0o700)
    request_sha = _file_sha256(request_path)
    outcome_path = result_dir / ".worker-outcome.json"
    started_at = _utc_now()
    context = multiprocessing.get_context("fork")
    process = context.Process(
        target=_worker_entry,
        args=(dict(request), str(outcome_path)),
        name="english-legacy-isolated-writer",
    )
    process.start()
    process.join()
    stopped_at = _utc_now()
    outcome = _load_json(outcome_path, "english_writer_worker_outcome_missing")
    try:
        outcome_path.unlink()
    except OSError:
        pass
    process_evidence = {
        "adapter_run_id": f"EN-P0-006-RUN-{uuid.uuid4().hex}",
        "pid": int(process.pid or 0),
        "started_at": started_at,
        "stopped_at": stopped_at,
        "terminal_state": "stopped",
        "exit_code": int(process.exitcode or 0),
        "request_sha256": request_sha,
        "runtime_state_sha256": outcome.get("runtime_state_sha256"),
    }
    _write_json(result_dir / "writer-process-evidence.json", process_evidence, mode=0o400)
    process_sha = _file_sha256(result_dir / "writer-process-evidence.json")
    writer_process = {
        "adapter_run_id": process_evidence["adapter_run_id"],
        "pid": process_evidence["pid"],
        "terminal_state": "stopped",
        "exit_code": process_evidence["exit_code"],
        "stopped_at": stopped_at,
        "evidence_sha256": process_sha,
    }
    trusted_defaults = {
        "batch_id": batch["batch_id"],
        "batch_sha256": _document_sha256(batch),
        "target_id": review["target_id"],
        "target_kind": review["target_kind"],
        "ordinal": review["ordinal"],
        "fencing_token": review["fencing_token"],
        "attempt": review["attempt"],
        "previous_checkpoint_sha256": review["previous_checkpoint_sha256"],
        "review_receipt_sha256": review_sha,
        "authority_checkpoint_sha256": review["authority_checkpoint_sha256"],
        "pre_authority_file_sha256": _value_sha256({
            "state": "authority_file_unavailable",
            "target_id": review["target_id"],
        }),
        "pre_object_sha256": review["current_object_sha256"],
        "transaction_id": f"EN-P0-006-TXN-{uuid.uuid4().hex}",
        "transaction_state": "failed",
        "retryable": True,
        "formal_write_count": 0,
    }
    for field, value in trusted_defaults.items():
        outcome.setdefault(field, value)
    for field in (
        "batch_id", "batch_sha256", "target_id", "target_kind", "ordinal",
        "fencing_token", "attempt", "previous_checkpoint_sha256",
        "review_receipt_sha256", "authority_checkpoint_sha256",
    ):
        if outcome[field] != trusted_defaults[field]:
            raise EnglishLegacyWriterError("english_writer_worker_outcome_identity_mismatch")
    transaction_evidence = {
        "transaction_id": outcome["transaction_id"],
        "state": outcome["transaction_state"],
        "target_id": outcome["target_id"],
        "pre_authority_file_sha256": outcome["pre_authority_file_sha256"],
        "post_authority_file_sha256": outcome.get(
            "post_authority_file_sha256", outcome["pre_authority_file_sha256"]
        ),
        "pre_object_sha256": outcome["pre_object_sha256"],
        "post_object_sha256": outcome.get(
            "post_object_sha256", outcome["pre_object_sha256"]
        ),
        "ended_at": stopped_at,
    }
    _write_json(result_dir / "transaction-evidence.json", transaction_evidence, mode=0o400)
    transaction_sha = _file_sha256(result_dir / "transaction-evidence.json")
    transaction = {
        "transaction_id": outcome["transaction_id"],
        "state": outcome["transaction_state"],
        "ended_at": stopped_at,
        "evidence_sha256": transaction_sha,
    }
    common_summary = {
        "schema_version": INSPECTION_SCHEMA,
        "mode": request["mode"],
        "batch_id": batch["batch_id"],
        "target_id": review["target_id"],
        "target_kind": review["target_kind"],
        "ordinal": review["ordinal"],
        "review_receipt_sha256": review_sha,
        "decision": review["decision"],
        "outcome": outcome["outcome"],
        "pre_object_sha256": outcome["pre_object_sha256"],
        "post_object_sha256": outcome.get(
            "post_object_sha256", outcome["pre_object_sha256"]
        ),
        "writer_process_evidence_sha256": process_sha,
        "transaction_evidence_sha256": transaction_sha,
        "formal_write_count": int(outcome.get("formal_write_count", 0)),
        "completed_at": stopped_at,
    }
    _write_json(result_dir / "inspection.json", common_summary, mode=0o400)
    if request["mode"] == "dry_run":
        return common_summary
    if outcome["outcome"] in {"already_current", "committed"}:
        status = str(outcome["outcome"])
        apply_core = {
            **_receipt_base(outcome),
            "review_receipt_sha256": review_sha,
            "pre_authority_sha256": outcome["authority_checkpoint_sha256"],
            "post_authority_sha256": (
                outcome["authority_checkpoint_sha256"]
                if status == "already_current"
                else outcome["post_authority_sha256"]
            ),
            "pre_object_sha256": outcome["pre_object_sha256"],
            "post_object_sha256": outcome["post_object_sha256"],
            "operations": outcome["operations"],
            "writer_process": writer_process,
            "transaction_end": transaction,
            "status": status,
            "formal_write_count": outcome["formal_write_count"],
            "completed_at": stopped_at,
        }
        _write_json(result_dir / "apply.json", apply_core, mode=0o400)
        return common_summary
    recovery_position = {
        "copy_id": request["copy_id"],
        "target_id": review["target_id"],
        "pre_authority_file_sha256": outcome["pre_authority_file_sha256"],
        "pre_object_sha256": outcome["pre_object_sha256"],
        "rollback_snapshot_sha256": (
            _file_sha256(result_dir / "rollback-snapshot.bin")
            if (result_dir / "rollback-snapshot.bin").exists() else None
        ),
        "transaction_state": outcome["transaction_state"],
    }
    _write_json(result_dir / "recovery-position.json", recovery_position, mode=0o400)
    recovery_sha = _file_sha256(result_dir / "recovery-position.json")
    failure_core = {
        **_receipt_base(outcome),
        "review_receipt_sha256": review_sha,
        "failure_stage": (
            "sol_review" if outcome["outcome"] == "reject" else "formal_apply"
        ),
        "authority_checkpoint_sha256": outcome["authority_checkpoint_sha256"],
        "reason": str(outcome.get("reason") or "english_writer_failed"),
        "recovery_position_sha256": recovery_sha,
        "writer_process": writer_process,
        "transaction_end": transaction,
        "retryable": bool(outcome.get("retryable", False)),
        "formal_write_count": 0,
        "failed_at": stopped_at,
    }
    _write_json(result_dir / "failure.json", failure_core, mode=0o400)
    return common_summary


def audit_work_item_batch(
    work_item_batch_path: Path,
    isolated_root: Path,
    copy_id: str,
) -> dict[str, Any]:
    """Read-only preflight for a real EN-P0-006 target set.

    This is intentionally not a Sol review and cannot produce apply receipts.
    It exists to prove that every frozen stable id and semantic key can be
    reopened from a complete isolated authority copy before model or writer use.
    """

    marker = _validate_marker(isolated_root, copy_id)
    try:
        batch_sha, batch, items = reopen_work_item_batch(work_item_batch_path)
    except EnglishLegacyRecurationError as exc:
        raise EnglishLegacyWriterError(
            f"english_writer_work_item_batch_{exc}"
        ) from exc
    rows: list[dict[str, Any]] = []
    for item in items:
        try:
            located = _locate(
                isolated_root.resolve(),
                {
                    "target_kind": item["target_kind"],
                    "target_id": item["target_id"],
                },
            )
            actual = str(located["object_sha256"])
            current_matches = actual == item["current_object_sha256"]
            desired_matches = actual == item["desired_object_sha256"]
            outcome = (
                "already_current_eligible"
                if current_matches and desired_matches
                else "review_required"
            )
            reason = None
        except EnglishLegacyWriterError as exc:
            actual = None
            current_matches = False
            desired_matches = False
            outcome = "reject"
            reason = exc.code
        rows.append(
            {
                "ordinal": item["ordinal"],
                "target_id": item["target_id"],
                "target_kind": item["target_kind"],
                "current_object_sha256": item["current_object_sha256"],
                "desired_object_sha256": item["desired_object_sha256"],
                "actual_object_sha256": actual,
                "stable_id_and_semantic_key_reopened": outcome != "reject",
                "current_matches": current_matches,
                "desired_matches": desired_matches,
                "outcome": outcome,
                "reason": reason,
            }
        )
    source_check = source_unchanged(marker)
    counts = {
        "already_current_eligible": sum(
            row["outcome"] == "already_current_eligible" for row in rows
        ),
        "review_required": sum(row["outcome"] == "review_required" for row in rows),
        "reject": sum(row["outcome"] == "reject" for row in rows),
    }
    return {
        "schema_version": TARGET_SET_AUDIT_SCHEMA,
        "issue_id": ISSUE_ID,
        "subject": SUBJECT,
        "work_item_batch_sha256": batch_sha,
        "remediation_batch_id": items[0]["remediation_batch_id"],
        "target_count": batch["target_count"],
        "copy_id": copy_id,
        "isolated_root": str(isolated_root.resolve()),
        "source_unchanged": source_check,
        "counts": counts,
        "items": rows,
        "status": (
            "preflight_passed"
            if counts["review_required"] == 0
            and counts["reject"] == 0
            and source_check["unchanged"] is True
            else "preflight_failed"
        ),
        "model_call_count": 0,
        "formal_write_count": 0,
        "completed_at": _utc_now(),
    }


def source_unchanged(marker: Mapping[str, Any]) -> dict[str, Any]:
    """Re-hash the source authority surfaces named by an isolated marker."""

    try:
        checked = _validate_marker(
            Path(str(marker["isolated_root"])), str(marker["copy_id"])
        )
    except (KeyError, TypeError) as exc:
        raise EnglishLegacyWriterError(
            "english_writer_isolated_marker_invalid"
        ) from exc
    source = Path(str(checked["source_root"]))
    current = authority_manifest(source)
    current_tree = tree_manifest(source)
    return {
        "source_root": str(source.resolve()),
        "expected_manifest_sha256": checked["source_authority_manifest_sha256"],
        "actual_manifest_sha256": current["manifest_sha256"],
        "expected_tree_manifest_sha256": checked["source_tree_manifest_sha256"],
        "actual_tree_manifest_sha256": current_tree["manifest_sha256"],
        "unchanged": current["manifest_sha256"]
        == checked["source_authority_manifest_sha256"]
        and current_tree["manifest_sha256"]
        == checked["source_tree_manifest_sha256"],
        "checked_at": _utc_now(),
    }
