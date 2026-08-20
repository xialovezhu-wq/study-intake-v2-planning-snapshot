from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

from .candidates import render_luna_candidate_file, validate_luna_candidate_file
from .constants import DEFAULT_STATE_DIR, EVIDENCE_STATES, REPO_ROOT, USER_EVIDENCE
from .errors import PipelineError, SourceHashMismatch, ValidationError
from .events import append_event
from .nightly import freeze_nightly, pipeline_status, validate_events_report
from .quick_flush import publish_quick_flush_intent
from .util import atomic_write_json, file_sha256, load_json, parse_iso_date
from .views import complete_article, write_quick_capture_view
from .writer import apply_nightly, recover_nightly


def _common_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=REPO_ROOT,
        help=f"English repository root (default: {REPO_ROOT})",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=DEFAULT_STATE_DIR,
        help=f"Canonical pipeline state root (default: {DEFAULT_STATE_DIR})",
    )
    return parser


def build_parser() -> argparse.ArgumentParser:
    common = _common_parser()
    parser = argparse.ArgumentParser(
        prog="english_learning_pipeline.py",
        description="Append-only English learning capture and deterministic nightly formal writer.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture = subparsers.add_parser("capture", parents=[common], help="Append one immutable sentence capture or correction event.")
    capture.add_argument("--input-json", type=Path, help="Complete semantic capture request JSON; direct flags are ignored.")
    capture.add_argument("--idempotency-key")
    capture.add_argument("--article-id", help="Compatibility alias; when supplied it must equal --source-id.")
    capture.add_argument("--source-article")
    capture.add_argument("--source-id")
    capture.add_argument("--reference-id", default="")
    capture.add_argument("--title", default="")
    capture.add_argument("--article-sha256", help="Required SHA-256 from the answer-free canonical article handoff; never inferred from article Markdown.")
    capture.add_argument("--sentence-id")
    capture.add_argument("--source-sentence")
    capture.add_argument("--sentence-sha256", help="SHA-256 of the normalized exact source sentence.")
    capture.add_argument(
        "--source-kind",
        choices=["article", "question", "option", "explanation", "user_provided"],
        default="article",
    )
    capture.add_argument("--first-translation", help="Preserved verbatim; omitted is stored explicitly as null.")
    capture.add_argument("--user-evidence", help="User's evidence wording preserved verbatim; omitted is null.")
    capture.add_argument("--evidence-state", action="append", choices=sorted(EVIDENCE_STATES), default=[])
    capture.add_argument("--evidence-kind", action="append", choices=sorted(USER_EVIDENCE), default=[])
    capture.add_argument("--evidence-origin", choices=["live_user", "synthetic_fixture"], default="live_user")
    capture.add_argument("--translation", default="")
    capture.add_argument("--explanation", default="")
    capture.add_argument("--first-breakpoint", default="")
    capture.add_argument("--restatement", default="")
    capture.add_argument("--hint-level", type=int, choices=range(0, 6), default=0)
    capture.add_argument(
        "--answer-protection",
        choices=["practice_safe", "unlocked", "not_applicable"],
        default="practice_safe",
    )
    capture.add_argument(
        "--candidate-json",
        action="append",
        default=[],
        help="Candidate JSON object/list, either inline JSON or a JSON file path; repeatable.",
    )
    capture.add_argument("--supersedes", help="Current effective capture event id; creates a correction event.")
    capture.add_argument("--correction-reason")
    capture.add_argument("--occurred-at", help="ISO-8601 timestamp; defaults to current UTC time.")
    capture.add_argument(
        "--quick-flush",
        action="store_true",
        help=(
            "Publish one signed, event-bound immediate microbatch intent. "
            "This does not alter the ordinary 5-capture/180-second policy."
        ),
    )

    complete = subparsers.add_parser("complete-article", parents=[common], help="Append article_completed and render immediate output-only A/B/C export.")
    complete.add_argument("--source-id", help="Canonical source identity.")
    complete.add_argument("--article-id", help="Compatibility alias; must equal --source-id when both are supplied.")
    complete.add_argument("--idempotency-key", required=True)
    complete.add_argument("--date", dest="study_date")
    complete.add_argument("--output-dir", type=Path)

    view = subparsers.add_parser("render-view", parents=[common], help="Rebuild quick-capture Markdown from immutable events.")
    view.add_argument("--source-id", help="Canonical source identity.")
    view.add_argument("--article-id", help="Compatibility alias; must equal --source-id when both are supplied.")
    view.add_argument("--date", dest="study_date")
    view.add_argument("--output", type=Path)

    validate_candidate = subparsers.add_parser("validate-candidate", parents=[common], help="Validate one Luna candidate JSON against repo and capture evidence.")
    validate_candidate.add_argument("candidate", type=Path)
    validate_candidate.add_argument("--without-event-check", action="store_true")

    render_candidate = subparsers.add_parser("render-candidate", parents=[common], help="Render a validated Luna candidate as lasting-style Markdown.")
    render_candidate.add_argument("candidate", type=Path)
    render_candidate.add_argument("--output", type=Path)

    freeze = subparsers.add_parser("freeze-nightly", parents=[common], help="Freeze a date-bounded immutable manifest from validated Luna candidates.")
    freeze.add_argument("--date", dest="study_date", required=True)
    freeze.add_argument("--candidate", type=Path, action="append", default=[])
    freeze.add_argument("--output", type=Path)

    apply_parser = subparsers.add_parser("apply-nightly", parents=[common], help="Validate and simulate/apply typed Sol actions through the deterministic writer.")
    apply_parser.add_argument("--manifest", type=Path, required=True)
    apply_parser.add_argument("--actions", type=Path, required=True)
    mode = apply_parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Explicit dry-run; this is also the default.")
    mode.add_argument("--apply", action="store_true", help="Apply safe actions; requires exact --authorization batch id.")
    apply_parser.add_argument("--authorization", help="Exact frozen batch id; required only with --apply.")
    apply_parser.add_argument("--authorized-at", help="ISO-8601 authorization time recorded in receipt.")

    recover = subparsers.add_parser("recover-nightly", parents=[common], help="Recover an interrupted prepared formal transaction under the writer lock.")
    recover.add_argument("--transaction", type=Path, help="Specific transaction.json; defaults to all pending transactions.")

    subparsers.add_parser("status", parents=[common], help="Report event, candidate, manifest and receipt counts.")
    validate_events = subparsers.add_parser("validate-events", parents=[common], help="Validate immutable event files and emit hashes/identities without source text.")
    validate_events.add_argument("--date", dest="study_date")
    return parser


def _load_candidate_arguments(values: list[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for value in values:
        path = Path(value)
        loaded: Any
        if path.is_file():
            loaded = json.loads(path.read_text(encoding="utf-8"))
        else:
            loaded = json.loads(value)
        if isinstance(loaded, list):
            result.extend(loaded)
        elif isinstance(loaded, dict):
            result.append(loaded)
        else:
            raise ValidationError("candidate JSON must be an object or array")
    return result


_SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
_HANDOFF_SCHEMA = "english-learning-pipeline-handoff-v1"
_HANDOFF_NORMALIZATION = (
    "UTF-8; LF line endings; trailing whitespace removed per line; no final LF"
)


def _resolve_repo_file(repo_root: Path, locator: str, *, label: str) -> tuple[Path, str]:
    if not isinstance(locator, str) or not locator.strip():
        raise ValidationError(f"{label} locator is required")
    root = repo_root.resolve(strict=True)
    supplied = Path(locator)
    candidate = supplied if supplied.is_absolute() else root / supplied
    try:
        resolved = candidate.resolve(strict=True)
        relative = resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise ValidationError(f"{label} locator must resolve to a repository file") from exc
    if not resolved.is_file():
        raise ValidationError(f"{label} locator must resolve to a regular file")
    return resolved, relative.as_posix()


def _article_metadata_value(article_text: str, key: str) -> str:
    pattern = re.compile(
        rf"^\s*-\s*{re.escape(key)}\s*[：:]\s*`?([^`\r\n]+?)`?\s*$",
        re.MULTILINE,
    )
    values = [match.group(1).strip() for match in pattern.finditer(article_text)]
    if len(values) != 1 or not values[0]:
        raise ValidationError(f"article page must contain exactly one {key} binding")
    return values[0]


def _resolve_canonical_payload(
    repo_root: Path,
    handoff_path: Path,
    locator: str,
) -> Path:
    if not isinstance(locator, str) or not locator.strip():
        raise ValidationError("canonical payload locator must be repository-contained and relative")
    relative_locator = Path(locator)
    if relative_locator.is_absolute() or ".." in relative_locator.parts:
        raise ValidationError("canonical payload locator must be repository-contained and relative")
    root = repo_root.resolve(strict=True)
    candidates: set[Path] = set()
    base = handoff_path.parent
    while True:
        try:
            resolved = (base / relative_locator).resolve(strict=True)
            resolved.relative_to(root)
        except (OSError, ValueError):
            pass
        else:
            if resolved.is_file():
                candidates.add(resolved)
        if base == root:
            break
        try:
            base.relative_to(root)
        except ValueError as exc:
            raise ValidationError("handoff file is outside repository root") from exc
        base = base.parent
    if len(candidates) != 1:
        raise ValidationError(
            "canonical payload locator must resolve to exactly one repository file"
        )
    return next(iter(candidates))


def _canonical_handoff_source_bytes(path: Path) -> bytes:
    try:
        text = path.read_bytes().decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ValidationError("canonical payload must be readable UTF-8 text") from exc
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = "\n".join(line.rstrip() for line in normalized.split("\n")).rstrip("\n")
    if not normalized:
        raise ValidationError("canonical payload must not be empty")
    return normalized.encode("utf-8")


def _validate_capture_source_object(
    repo_root: Path,
    request: dict[str, Any],
) -> dict[str, Any]:
    article = request.get("article")
    if not isinstance(article, dict):
        raise ValidationError("capture request article must be an object")
    source_id = article.get("source_id")
    source_hash = article.get("source_hash")
    if not isinstance(source_id, str) or not source_id.strip():
        raise ValidationError("capture request article.source_id is required")
    if not isinstance(source_hash, str) or not _SHA256_RE.fullmatch(source_hash):
        raise ValidationError("capture request article.source_hash must be lowercase SHA-256")

    article_path, article_locator = _resolve_repo_file(
        repo_root,
        article.get("source_article"),
        label="source_article",
    )
    try:
        article_text = article_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ValidationError("source_article must be readable UTF-8 text") from exc

    page_source_id = _article_metadata_value(article_text, "source_id")
    page_source_hash = _article_metadata_value(article_text, "source_hash")
    handoff_locator = _article_metadata_value(article_text, "pipeline_handoff")
    if page_source_id != source_id:
        raise SourceHashMismatch(
            f"article source_id mismatch: expected {source_id}, got {page_source_id}"
        )
    if Path(handoff_locator).is_absolute() or ".." in Path(handoff_locator).parts:
        raise ValidationError("pipeline_handoff locator must be repository-relative")
    handoff_path, _ = _resolve_repo_file(
        repo_root,
        handoff_locator,
        label="pipeline_handoff",
    )
    try:
        handoff = load_json(handoff_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ValidationError("pipeline handoff must be valid JSON") from exc
    canonical_payload = handoff.get("canonical_payload")
    if (
        handoff.get("schema_version") != _HANDOFF_SCHEMA
        or handoff.get("source_id") != source_id
        or not isinstance(canonical_payload, dict)
        or canonical_payload.get("visibility") != "practice_safe"
        or canonical_payload.get("normalization") != _HANDOFF_NORMALIZATION
    ):
        raise ValidationError("pipeline handoff identity or canonical payload contract is invalid")
    payload_path = _resolve_canonical_payload(
        repo_root,
        handoff_path,
        canonical_payload.get("path"),
    )
    actual_hash = hashlib.sha256(_canonical_handoff_source_bytes(payload_path)).hexdigest()
    expected_prefixed = f"sha256:{actual_hash}"
    declared_hashes = {
        "request": source_hash,
        "article": page_source_hash,
        "handoff": handoff.get("source_hash"),
    }
    if (
        declared_hashes["request"] != actual_hash
        or declared_hashes["article"] != expected_prefixed
        or declared_hashes["handoff"] != expected_prefixed
    ):
        raise SourceHashMismatch(
            "canonical article hash mismatch: "
            f"actual {actual_hash}, declarations {declared_hashes}"
        )
    article["source_article"] = article_locator
    article["source_hash"] = actual_hash
    return request


def _capture_request(args: argparse.Namespace, repo_root: Path) -> dict[str, Any]:
    if args.input_json:
        request = load_json(args.input_json)
        request.pop("schema_version", None)
        request.pop("event_id", None)
        request.pop("request_sha256", None)
        request.pop("formal_write_count", None)
        request.pop("formal_writeback", None)
    else:
        required = {
            "idempotency_key": args.idempotency_key,
            "source_article": args.source_article,
            "source_id": args.source_id,
            "article_sha256": args.article_sha256,
            "sentence_id": args.sentence_id,
            "source_sentence": args.source_sentence,
        }
        missing = [name for name, value in required.items() if value is None or value == []]
        if missing:
            raise ValidationError(f"capture direct flags missing: {missing}")
        article_id = args.article_id or args.source_id
        if args.article_id and args.article_id != args.source_id:
            raise ValidationError("--article-id must equal canonical --source-id")
        if args.supersedes and not args.correction_reason:
            raise ValidationError("--supersedes requires --correction-reason")
        article_hash = args.article_sha256
        article = {
            "article_id": article_id,
            "source_article": args.source_article,
            "source_id": args.source_id,
            "source_hash": article_hash,
        }
        for key in ("source_id", "reference_id", "title"):
            value = getattr(args, key)
            if value:
                article[key] = value
        source = {
            "sentence_id": args.sentence_id,
            "source_sentence": args.source_sentence,
            "source_kind": args.source_kind,
        }
        if args.sentence_sha256:
            source["sentence_hash"] = args.sentence_sha256
        learning = {
            "first_translation": args.first_translation,
            "user_evidence_verbatim": args.user_evidence,
            "user_evidence": args.evidence_kind,
            "evidence_states": args.evidence_state,
            "evidence_origin": args.evidence_origin,
            "answer_protection": args.answer_protection,
            "hint_level": args.hint_level,
            "translation": args.translation,
            "explanation": args.explanation,
            "first_breakpoint": args.first_breakpoint,
            "restatement": args.restatement,
        }
        request = {
            "event_type": "sentence_correction" if args.supersedes else "sentence_captured",
            "idempotency_key": args.idempotency_key,
            "article": article,
            "source": source,
            "learning": learning,
            "candidates": _load_candidate_arguments(args.candidate_json),
        }
        if args.supersedes:
            request["supersedes_event_id"] = args.supersedes
            request["correction_reason"] = args.correction_reason
        if args.occurred_at:
            request["occurred_at"] = args.occurred_at
    return _validate_capture_source_object(repo_root, request)


def run(args: argparse.Namespace) -> dict[str, Any]:
    state_dir = args.state_dir.resolve()
    repo_root = args.repo_root.resolve()
    if args.command == "capture":
        request = _capture_request(args, repo_root)
        receipt = append_event(state_dir, request)
        event = load_json(Path(receipt["event_path"]))
        source_id = event["article"]["source_id"]
        study_date = parse_iso_date(event["occurred_at"])
        try:
            view_path, _ = write_quick_capture_view(
                state_dir,
                article_id=source_id,
                study_date=study_date,
            )
            receipt["projection_status"] = "rendered"
            receipt["view_path"] = str(view_path)
            receipt["view_sha256"] = file_sha256(view_path)
            receipt["projection_error"] = None
        except Exception as exc:
            receipt["status"] = "capture_saved_but_projection_failed"
            receipt["projection_status"] = "failed"
            receipt["view_path"] = None
            receipt["view_sha256"] = None
            receipt["projection_error"] = str(exc)
        canonical_receipt = dict(receipt)
        if receipt.get("replayed"):
            canonical_receipt["status"] = "created"
            canonical_receipt["replayed"] = False
        atomic_write_json(Path(receipt["receipt_path"]), canonical_receipt)
        if args.quick_flush:
            quick_flush_receipt = publish_quick_flush_intent(
                state_dir,
                event=event,
                capture_receipt=canonical_receipt,
            )
            receipt["quick_flush"] = quick_flush_receipt
        return receipt
    if args.command == "complete-article":
        source_id = args.source_id or args.article_id
        if not source_id:
            raise ValidationError("complete-article requires --source-id")
        if args.source_id and args.article_id and args.source_id != args.article_id:
            raise ValidationError("--article-id must equal canonical --source-id")
        return complete_article(
            state_dir,
            repo_root,
            article_id=source_id,
            idempotency_key=args.idempotency_key,
            study_date=args.study_date,
            output_dir=args.output_dir,
        )
    if args.command == "render-view":
        source_id = args.source_id or args.article_id
        if args.source_id and args.article_id and args.source_id != args.article_id:
            raise ValidationError("--article-id must equal canonical --source-id")
        path, text = write_quick_capture_view(
            state_dir,
            article_id=source_id,
            study_date=args.study_date,
            output=args.output,
        )
        return {"schema_version": "english_view_receipt_v1", "status": "rendered", "path": str(path), "bytes": len(text.encode('utf-8')), "formal_write_count": 0}
    if args.command == "validate-candidate":
        return validate_luna_candidate_file(
            args.candidate,
            state_dir=None if args.without_event_check else state_dir,
        )
    if args.command == "render-candidate":
        _, text = render_luna_candidate_file(args.candidate, args.output)
        return {"schema_version": "english_candidate_render_receipt_v1", "status": "rendered", "output": str(args.output) if args.output else None, "markdown": None if args.output else text, "formal_write_count": 0}
    if args.command == "freeze-nightly":
        path, manifest = freeze_nightly(
            state_dir,
            repo_root,
            study_date=args.study_date,
            candidate_paths=args.candidate or None,
            output=args.output,
        )
        return {
            "schema_version": "english_freeze_receipt_v1",
            "status": manifest["status"],
            "batch_id": manifest["batch_id"],
            "study_date": manifest["study_date"],
            "manifest": str(path),
            "candidate_count": len(manifest["candidate_documents"]),
            "capture_event_count": len(manifest["capture_event_ids"]),
            "uncovered_capture_event_ids": manifest["uncovered_capture_event_ids"],
            "runtime_identity_summary": manifest["runtime_identity_summary"],
            "formal_write_count": 0,
            "formal_writeback": "none",
        }
    if args.command == "apply-nightly":
        path, receipt = apply_nightly(
            state_dir,
            repo_root,
            manifest_path=args.manifest,
            actions_path=args.actions,
            apply=args.apply,
            authorization=args.authorization,
            authorized_at=args.authorized_at,
        )
        return {**receipt, "receipt_path": str(path)}
    if args.command == "recover-nightly":
        return recover_nightly(state_dir, repo_root, transaction_path=args.transaction)
    if args.command == "status":
        return pipeline_status(state_dir)
    if args.command == "validate-events":
        return validate_events_report(state_dir, study_date=args.study_date)
    raise ValidationError(f"unknown command: {args.command}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = run(args)
    except (PipelineError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(
            json.dumps(
                {"schema_version": "english_pipeline_error_v1", "status": "ERROR", "error_type": type(exc).__name__, "message": str(exc)},
                ensure_ascii=False,
                indent=2,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if result.get("status") in {"CAS_CONFLICT", "FAILED", "capture_saved_but_projection_failed"}:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
