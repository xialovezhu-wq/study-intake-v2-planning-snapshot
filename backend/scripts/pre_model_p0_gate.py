#!/usr/bin/env python3
"""Fail closed before any Luna or Golden model call while a P0 is open."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from english_legacy_disposition import (  # noqa: E402
    EnglishLegacyDispositionError,
    EnglishLegacyDispositionStore,
    EnglishLegacyDispositionV3Store,
)
from subject_sol_contract import (  # noqa: E402
    SubjectSolContractError,
    SubjectSolRuntimeStore,
)


MATRIX_SCHEMA = "study-intake-three-subject-p0-matrix-v1"
RESULT_SCHEMA = "study-intake-pre-model-p0-gate-result-v1"
GOLDEN_SCHEMA = "study-intake-zero-model-golden-inventory-v1"
SUBJECTS = ("math", "cs408", "english")
GATE_STATUSES = {"closed", "open", "needs_user_decision"}
ALLOWED_DISPOSITIONS = (
    "legacy_attestation",
    "deterministic_recuration",
    "rollback",
)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class P0GateError(RuntimeError):
    pass


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_json(path: Path, code: str, *, max_bytes: int = 4 * 1024 * 1024) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > max_bytes:
            raise OSError("unsafe input")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise P0GateError(code) from exc
    if not isinstance(value, dict):
        raise P0GateError(code)
    return value


def _audit_p0s(
    audit_root: Path,
    bindings: Mapping[str, str],
) -> dict[str, dict[str, Any]]:
    root = audit_root.expanduser().resolve()
    rows: dict[str, dict[str, Any]] = {}
    for subject in SUBJECTS:
        path = root / subject / "issue-register.json"
        expected_sha = bindings.get(subject)
        if (
            path.is_symlink()
            or not path.is_file()
            or not isinstance(expected_sha, str)
            or SHA256_RE.fullmatch(expected_sha) is None
            or _sha256_file(path) != expected_sha
        ):
            raise P0GateError("p0_audit_binding_invalid")
        register = _load_json(path, "p0_audit_register_invalid")
        issues = register.get("issues")
        if not isinstance(issues, list):
            raise P0GateError("p0_audit_register_invalid")
        for issue in issues:
            if not isinstance(issue, Mapping):
                raise P0GateError("p0_audit_register_invalid")
            priority = issue.get("priority")
            if priority is None:
                priority = issue.get("severity")
            if priority != "P0":
                continue
            issue_id = issue.get("issue_id") or issue.get("id")
            if not isinstance(issue_id, str) or not issue_id or issue_id in rows:
                raise P0GateError("p0_audit_issue_invalid")
            rows[issue_id] = {
                "subject": subject,
                "title": issue.get("title"),
                "audit_status": issue.get("status"),
            }
    return rows


def _validate_closure_checks(raw: object, *, gate_status: str) -> list[dict[str, str]]:
    if not isinstance(raw, list):
        raise P0GateError("p0_closure_checks_invalid")
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, Mapping) or set(item) != {
            "check_id",
            "status",
            "evidence",
        }:
            raise P0GateError("p0_closure_checks_invalid")
        check_id = item.get("check_id")
        status = item.get("status")
        evidence = item.get("evidence")
        if (
            not isinstance(check_id, str)
            or not check_id
            or check_id in seen
            or status not in {"passed", "pending", "blocked"}
            or not isinstance(evidence, str)
            or not evidence
        ):
            raise P0GateError("p0_closure_checks_invalid")
        seen.add(check_id)
        rows.append(
            {"check_id": check_id, "status": str(status), "evidence": evidence}
        )
    if gate_status == "closed" and (
        not rows or any(row["status"] != "passed" for row in rows)
    ):
        raise P0GateError("p0_closed_without_passed_checks")
    return rows


def _validate_legacy_disposition(
    raw: object,
    *,
    gate_status: str,
    signed_v2_closure: Mapping[str, Any] | None = None,
    verified_existing_inventory_closure: Mapping[str, Any] | None = None,
    verified_execution_closure: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(raw, Mapping) or set(raw) != {
        "inventory_status",
        "identified_target_count",
        "unidentified_target_count",
        "disposition_receipt_count",
        "allowed_dispositions",
        "mastered_items_state",
    }:
        raise P0GateError("en_p0_006_disposition_invalid")
    identified = raw.get("identified_target_count")
    unidentified = raw.get("unidentified_target_count")
    receipts = raw.get("disposition_receipt_count")
    mastered = raw.get("mastered_items_state")
    if (
        raw.get("inventory_status") not in {"evidence_incomplete", "complete"}
        or not isinstance(identified, int)
        or identified < 0
        or not isinstance(unidentified, int)
        or unidentified < 0
        or not isinstance(receipts, int)
        or receipts < 0
        or tuple(raw.get("allowed_dispositions") or ()) != ALLOWED_DISPOSITIONS
        or not isinstance(mastered, Mapping)
        or set(mastered) != {"row_count", "sha256", "status"}
        or not isinstance(mastered.get("row_count"), int)
        or mastered.get("row_count") < 0
        or not isinstance(mastered.get("sha256"), str)
        or SHA256_RE.fullmatch(str(mastered.get("sha256"))) is None
        or mastered.get("status")
        not in {"unchanged_no_rows", "contains_rows_needs_disposition"}
    ):
        raise P0GateError("en_p0_006_disposition_invalid")
    if gate_status == "needs_user_decision":
        if (
            signed_v2_closure is not None
            or verified_existing_inventory_closure is not None
            or verified_execution_closure is not None
        ):
            raise P0GateError("en_p0_006_matrix_not_closed")
        if receipts >= identified and unidentified == 0 and identified > 0:
            raise P0GateError("en_p0_006_requires_signed_v2_closure")
    elif gate_status == "closed":
        if (
            raw.get("inventory_status") != "complete"
            or identified <= 0
            or unidentified != 0
            or receipts != identified
        ):
            raise P0GateError("en_p0_006_must_need_user_decision")
        closure_authority_count = sum(
            row is not None
            for row in (
                signed_v2_closure,
                verified_existing_inventory_closure,
                verified_execution_closure,
            )
        )
        if closure_authority_count > 1:
            raise P0GateError("en_p0_006_multiple_closure_authorities")
        if verified_existing_inventory_closure is not None:
            existing = verified_existing_inventory_closure
            if (
                existing.get("status") != "verified_complete"
                or existing.get("outcome")
                != "verified_existing_inventory_no_recuration_required"
                or existing.get("target_count") != identified
                or existing.get("current_object_identity_and_hash_verified_count")
                != identified
                or existing.get("per_target_hmac_receipt_count") != receipts
                or existing.get("sp_022_verified") is not True
                or existing.get("mutation_write_set_verified") is not True
                or existing.get("luna_recuration_allowed") is not False
                or existing.get("sol_parent_batch_allowed") is not False
                or any(
                    existing.get(field) != 0
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
                raise P0GateError(
                    "en_p0_006_existing_inventory_closure_mismatch"
                )
        elif verified_execution_closure is not None:
            execution = verified_execution_closure.get("closure")
            if (
                verified_execution_closure.get("schema_version")
                != "verified_english_legacy_execution_closure_v1"
                or verified_execution_closure.get("status")
                != "verified_complete"
                or not isinstance(execution, Mapping)
                or execution.get("status") != "verified_complete"
                or execution.get("target_count") != identified
                or execution.get("committed_count", 0)
                + execution.get("already_current_count", 0)
                != identified
                or execution.get("failed_count") != 0
                or execution.get("omitted_count") != 0
                or execution.get("unknown_count") != 0
                or execution.get("duplicate_count") != 0
                or execution.get("recovery_pending_count") != 0
            ):
                raise P0GateError(
                    "en_p0_006_execution_closure_mismatch"
                )
        elif signed_v2_closure is not None:
            if (
                signed_v2_closure.get("status") != "verified_complete"
                or signed_v2_closure.get("target_count") != identified
                or signed_v2_closure.get("receipt_count") != receipts
                or signed_v2_closure.get("mastered_items_inference_used") is not False
                or signed_v2_closure.get("model_call_count") != 0
                or signed_v2_closure.get("formal_write_count") != 0
            ):
                raise P0GateError("en_p0_006_signed_v2_closure_mismatch")
        else:
            raise P0GateError(
                "en_p0_006_requires_verified_closure"
            )
    else:
        raise P0GateError("en_p0_006_must_need_user_decision")
    return dict(raw)


def _verify_legacy_closure(
    *,
    receipt_root: Path | None,
    closure_sha256: str | None,
    authority_key_path: Path | None,
) -> dict[str, Any] | None:
    supplied = (receipt_root, closure_sha256, authority_key_path)
    if all(value is None for value in supplied):
        return None
    if any(value is None for value in supplied):
        raise P0GateError("en_p0_006_signed_v2_closure_args_incomplete")
    assert receipt_root is not None
    assert closure_sha256 is not None
    assert authority_key_path is not None
    try:
        return EnglishLegacyDispositionStore(
            receipt_root,
            authority_key_path,
        ).verify_closure(closure_sha256)
    except EnglishLegacyDispositionError as exc:
        raise P0GateError(f"en_p0_006_{exc.code}") from exc


def _verify_existing_inventory_closure(
    *,
    receipt_root: Path | None,
    authorization_expansion_closure_sha256: str | None,
    authority_key_path: Path | None,
) -> dict[str, Any] | None:
    supplied = (
        receipt_root,
        authorization_expansion_closure_sha256,
        authority_key_path,
    )
    if all(value is None for value in supplied):
        return None
    if any(value is None for value in supplied):
        raise P0GateError("en_p0_006_remediation_gate_args_incomplete")
    assert receipt_root is not None
    assert authorization_expansion_closure_sha256 is not None
    assert authority_key_path is not None
    try:
        return EnglishLegacyDispositionV3Store(
            receipt_root,
            authority_key_path,
        ).verify_existing_inventory_no_recuration_required(
            authorization_expansion_closure_sha256
        )
    except EnglishLegacyDispositionError as exc:
        raise P0GateError(f"en_p0_006_{exc.code}") from exc


def _verify_execution_closure(
    *,
    runtime_root: Path | None,
    batch_id: str | None,
    closure_sha256: str | None,
) -> dict[str, Any] | None:
    supplied = (runtime_root, batch_id, closure_sha256)
    if all(value is None for value in supplied):
        return None
    if any(value is None for value in supplied):
        raise P0GateError("en_p0_006_execution_closure_args_incomplete")
    assert runtime_root is not None
    assert batch_id is not None
    assert closure_sha256 is not None
    try:
        result = SubjectSolRuntimeStore(
            runtime_root
        ).reopen_verified_english_legacy_execution_closure(
            batch_id,
            closure_sha256=closure_sha256,
        )
    except SubjectSolContractError as exc:
        raise P0GateError(f"en_p0_006_{exc.code}") from exc
    if result.get("status") != "verified_complete":
        raise P0GateError("en_p0_006_execution_closure_not_complete")
    return result


def evaluate_gate(
    matrix_path: Path,
    audit_root: Path,
    golden_inventory_path: Path,
    *,
    english_legacy_receipt_root: Path | None = None,
    english_legacy_closure_sha256: str | None = None,
    english_legacy_authority_key_path: Path | None = None,
    english_legacy_authorization_expansion_receipt_root: Path | None = None,
    english_legacy_authorization_expansion_closure_sha256: str | None = None,
    english_legacy_authorization_expansion_authority_key_path: Path | None = None,
    english_legacy_execution_runtime_root: Path | None = None,
    english_legacy_execution_batch_id: str | None = None,
    english_legacy_execution_closure_sha256: str | None = None,
) -> dict[str, Any]:
    matrix = _load_json(matrix_path, "p0_matrix_invalid")
    if set(matrix) != {
        "schema_version",
        "audit_register_sha256",
        "issues",
        "golden_inventory_sha256",
        "model_call_count",
        "formal_write_count",
    }:
        raise P0GateError("p0_matrix_invalid")
    bindings = matrix.get("audit_register_sha256")
    issues = matrix.get("issues")
    if (
        matrix.get("schema_version") != MATRIX_SCHEMA
        or not isinstance(bindings, Mapping)
        or set(bindings) != set(SUBJECTS)
        or not isinstance(issues, list)
        or matrix.get("model_call_count") != 0
        or matrix.get("formal_write_count") != 0
    ):
        raise P0GateError("p0_matrix_invalid")
    audit_rows = _audit_p0s(
        audit_root,
        {str(key): str(value) for key, value in bindings.items()},
    )

    golden_path = golden_inventory_path.expanduser().resolve()
    golden_sha = matrix.get("golden_inventory_sha256")
    if (
        not isinstance(golden_sha, str)
        or SHA256_RE.fullmatch(golden_sha) is None
        or golden_path.is_symlink()
        or not golden_path.is_file()
        or _sha256_file(golden_path) != golden_sha
    ):
        raise P0GateError("golden_inventory_binding_invalid")
    golden = _load_json(golden_path, "golden_inventory_invalid")
    if (
        golden.get("schema_version") != GOLDEN_SCHEMA
        or golden.get("status") != "static_fixtures_ready_model_not_run"
        or golden.get("semantic_assertion_status")
        != "pending_model_replay_after_p0_gate"
        or golden.get("model_call_count") != 0
        or golden.get("formal_write_count") != 0
    ):
        raise P0GateError("golden_inventory_invalid")

    en_p0_006_raw = next(
        (
            row
            for row in issues
            if isinstance(row, Mapping) and row.get("issue_id") == "EN-P0-006"
        ),
        None,
    )
    legacy_closure: dict[str, Any] | None = None
    existing_inventory_closure = _verify_existing_inventory_closure(
        receipt_root=english_legacy_authorization_expansion_receipt_root,
        authorization_expansion_closure_sha256=(
            english_legacy_authorization_expansion_closure_sha256
        ),
        authority_key_path=(
            english_legacy_authorization_expansion_authority_key_path
        ),
    )
    execution_closure = _verify_execution_closure(
        runtime_root=english_legacy_execution_runtime_root,
        batch_id=english_legacy_execution_batch_id,
        closure_sha256=english_legacy_execution_closure_sha256,
    )
    if isinstance(en_p0_006_raw, Mapping) and en_p0_006_raw.get("gate_status") == "closed":
        if execution_closure is not None and any(
            value is not None
            for value in (
                english_legacy_receipt_root,
                english_legacy_closure_sha256,
                english_legacy_authority_key_path,
            )
        ):
            raise P0GateError("en_p0_006_multiple_closure_authorities")
        legacy_closure = _verify_legacy_closure(
            receipt_root=english_legacy_receipt_root,
            closure_sha256=english_legacy_closure_sha256,
            authority_key_path=english_legacy_authority_key_path,
        )
    elif any(
        value is not None
        for value in (
            english_legacy_receipt_root,
            english_legacy_closure_sha256,
            english_legacy_authority_key_path,
        )
    ):
        raise P0GateError("en_p0_006_matrix_not_closed")
    if existing_inventory_closure is not None and (
        not isinstance(en_p0_006_raw, Mapping)
        or en_p0_006_raw.get("gate_status") != "closed"
    ):
        raise P0GateError("en_p0_006_matrix_not_closed")
    if execution_closure is not None and (
        not isinstance(en_p0_006_raw, Mapping)
        or en_p0_006_raw.get("gate_status") != "closed"
    ):
        raise P0GateError("en_p0_006_matrix_not_closed")

    matrix_rows: dict[str, dict[str, Any]] = {}
    for raw in issues:
        if not isinstance(raw, Mapping) or set(raw) != {
            "issue_id",
            "subject",
            "gate_status",
            "implementation_state",
            "remaining_action",
            "closure_checks",
            "requires_user_disposition",
            "legacy_disposition",
        }:
            raise P0GateError("p0_matrix_issue_invalid")
        issue_id = raw.get("issue_id")
        subject = raw.get("subject")
        status = raw.get("gate_status")
        if (
            not isinstance(issue_id, str)
            or issue_id in matrix_rows
            or issue_id not in audit_rows
            or subject != audit_rows[issue_id]["subject"]
            or status not in GATE_STATUSES
            or not isinstance(raw.get("implementation_state"), str)
            or not raw.get("implementation_state")
            or not isinstance(raw.get("remaining_action"), str)
            or not raw.get("remaining_action")
            or not isinstance(raw.get("requires_user_disposition"), bool)
        ):
            raise P0GateError("p0_matrix_issue_invalid")
        closure_checks = _validate_closure_checks(
            raw.get("closure_checks"), gate_status=str(status)
        )
        legacy = raw.get("legacy_disposition")
        if issue_id == "EN-P0-006":
            existing_inventory_closed = (
                status == "closed"
                and raw.get("implementation_state")
                == "verified_existing_inventory_no_recuration_required"
            )
            expected_user_disposition = not existing_inventory_closed
            if raw.get("requires_user_disposition") is not expected_user_disposition:
                raise P0GateError("en_p0_006_disposition_invalid")
            legacy = _validate_legacy_disposition(
                legacy,
                gate_status=str(status),
                signed_v2_closure=legacy_closure,
                verified_existing_inventory_closure=(
                    existing_inventory_closure
                ),
                verified_execution_closure=execution_closure,
            )
        elif raw.get("requires_user_disposition") is not False or legacy is not None:
            raise P0GateError("p0_matrix_issue_invalid")
        matrix_rows[issue_id] = {
            "issue_id": issue_id,
            "subject": subject,
            "gate_status": status,
            "implementation_state": raw["implementation_state"],
            "remaining_action": raw["remaining_action"],
            "closure_checks": closure_checks,
            "requires_user_disposition": raw["requires_user_disposition"],
            "legacy_disposition": legacy,
        }
    if set(matrix_rows) != set(audit_rows):
        raise P0GateError("p0_matrix_issue_set_mismatch")

    counts = Counter(str(row["gate_status"]) for row in matrix_rows.values())
    blockers = sorted(
        issue_id
        for issue_id, row in matrix_rows.items()
        if row["gate_status"] != "closed"
    )
    result = {
        "schema_version": RESULT_SCHEMA,
        "status": "ready" if not blockers else "blocked",
        "decision": "allow_model_lane" if not blockers else "block_model_lane",
        "matrix_path": str(matrix_path.expanduser().resolve()),
        "matrix_sha256": _sha256_file(matrix_path),
        "audit_root": str(audit_root.expanduser().resolve()),
        "golden_inventory_path": str(golden_path),
        "golden_inventory_sha256": golden_sha,
        "issue_count": len(matrix_rows),
        "closed_count": counts["closed"],
        "open_count": counts["open"],
        "needs_user_decision_count": counts["needs_user_decision"],
        "blocking_issue_ids": blockers,
        "model_call_count": 0,
        "formal_write_count": 0,
    }
    if legacy_closure is not None:
        result["english_legacy_disposition_closure"] = legacy_closure
    if existing_inventory_closure is not None:
        result["english_legacy_existing_inventory_closure"] = (
            existing_inventory_closure
        )
    if execution_closure is not None:
        result["english_legacy_execution_closure"] = execution_closure
    return result


def require_model_lane_ready(
    matrix_path: Path,
    audit_root: Path,
    golden_inventory_path: Path,
    *,
    english_legacy_receipt_root: Path | None = None,
    english_legacy_closure_sha256: str | None = None,
    english_legacy_authority_key_path: Path | None = None,
    english_legacy_authorization_expansion_receipt_root: Path | None = None,
    english_legacy_authorization_expansion_closure_sha256: str | None = None,
    english_legacy_authorization_expansion_authority_key_path: Path | None = None,
    english_legacy_execution_runtime_root: Path | None = None,
    english_legacy_execution_batch_id: str | None = None,
    english_legacy_execution_closure_sha256: str | None = None,
) -> dict[str, Any]:
    result = evaluate_gate(
        matrix_path,
        audit_root,
        golden_inventory_path,
        english_legacy_receipt_root=english_legacy_receipt_root,
        english_legacy_closure_sha256=english_legacy_closure_sha256,
        english_legacy_authority_key_path=english_legacy_authority_key_path,
        english_legacy_authorization_expansion_receipt_root=(
            english_legacy_authorization_expansion_receipt_root
        ),
        english_legacy_authorization_expansion_closure_sha256=(
            english_legacy_authorization_expansion_closure_sha256
        ),
        english_legacy_authorization_expansion_authority_key_path=(
            english_legacy_authorization_expansion_authority_key_path
        ),
        english_legacy_execution_runtime_root=(
            english_legacy_execution_runtime_root
        ),
        english_legacy_execution_batch_id=english_legacy_execution_batch_id,
        english_legacy_execution_closure_sha256=(
            english_legacy_execution_closure_sha256
        ),
    )
    if result["status"] != "ready":
        raise P0GateError("pre_model_p0_gate_blocked")
    return result


def require_en_p0_006_remediation_lane_ready(
    matrix_path: Path,
    audit_root: Path,
    golden_inventory_path: Path,
    *,
    english_legacy_authorization_expansion_receipt_root: Path,
    english_legacy_authorization_expansion_closure_sha256: str,
    english_legacy_authorization_expansion_authority_key_path: Path,
) -> dict[str, Any]:
    result = evaluate_gate(
        matrix_path,
        audit_root,
        golden_inventory_path,
        english_legacy_authorization_expansion_receipt_root=(
            english_legacy_authorization_expansion_receipt_root
        ),
        english_legacy_authorization_expansion_closure_sha256=(
            english_legacy_authorization_expansion_closure_sha256
        ),
        english_legacy_authorization_expansion_authority_key_path=(
            english_legacy_authorization_expansion_authority_key_path
        ),
    )
    closure = result.get("english_legacy_existing_inventory_closure")
    if (
        isinstance(closure, Mapping)
        and closure.get("outcome")
        == "verified_existing_inventory_no_recuration_required"
    ):
        raise P0GateError("en_p0_006_recuration_authorization_revoked")
    raise P0GateError("en_p0_006_remediation_gate_blocked")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(allow_abbrev=False)
    root.add_argument("--matrix", type=Path, required=True)
    root.add_argument("--audit-root", type=Path, required=True)
    root.add_argument("--golden-inventory", type=Path, required=True)
    root.add_argument("--english-legacy-receipt-root", type=Path)
    root.add_argument("--english-legacy-closure-sha256")
    root.add_argument("--english-legacy-authority-key", type=Path)
    root.add_argument(
        "--english-legacy-authorization-expansion-receipt-root",
        type=Path,
    )
    root.add_argument(
        "--english-legacy-authorization-expansion-closure-sha256"
    )
    root.add_argument(
        "--english-legacy-authorization-expansion-authority-key",
        type=Path,
    )
    root.add_argument("--english-legacy-execution-runtime-root", type=Path)
    root.add_argument("--english-legacy-execution-batch-id")
    root.add_argument("--english-legacy-execution-closure-sha256")
    return root


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        result = evaluate_gate(
            args.matrix,
            args.audit_root,
            args.golden_inventory,
            english_legacy_receipt_root=args.english_legacy_receipt_root,
            english_legacy_closure_sha256=args.english_legacy_closure_sha256,
            english_legacy_authority_key_path=args.english_legacy_authority_key,
            english_legacy_authorization_expansion_receipt_root=(
                args.english_legacy_authorization_expansion_receipt_root
            ),
            english_legacy_authorization_expansion_closure_sha256=(
                args.english_legacy_authorization_expansion_closure_sha256
            ),
            english_legacy_authorization_expansion_authority_key_path=(
                args.english_legacy_authorization_expansion_authority_key
            ),
            english_legacy_execution_runtime_root=(
                args.english_legacy_execution_runtime_root
            ),
            english_legacy_execution_batch_id=(
                args.english_legacy_execution_batch_id
            ),
            english_legacy_execution_closure_sha256=(
                args.english_legacy_execution_closure_sha256
            ),
        )
    except P0GateError as exc:
        print(
            json.dumps(
                {
                    "schema_version": RESULT_SCHEMA,
                    "status": "invalid",
                    "decision": "block_model_lane",
                    "error_code": str(exc),
                    "model_call_count": 0,
                    "formal_write_count": 0,
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "ready" else 3


if __name__ == "__main__":
    raise SystemExit(main())
