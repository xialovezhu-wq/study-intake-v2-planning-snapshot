"""Offline-first commissioning control plane for Study Intake.

The production instance is locked and cannot issue live authorizations,
campaigns, handoffs, or promotion.  The same state machine is fully exercised
against an isolated fixture root in tests.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping


SUBJECTS = frozenset({"math", "cs408", "english"})
STAGES = frozenset({"terra", "luna"})
TERMINAL_OUTCOMES = frozenset(
    {"succeeded", "failed", "cancelled", "timed_out"}
)
QUALITY_OUTCOMES = frozenset(
    {"accepted", "corrected", "issues_found", "technical_quarantine"}
)
ENGINEERING_GATES = (
    "validation_console_frontend",
    "validation_console_backend",
    "production_issuer",
    "campaign_control",
    "promotion_gate",
    "immutable_build",
    "transactional_deploy",
    "data_integrity",
)
NONCE_RE = re.compile(r"^[A-Za-z0-9._:-]{16,160}$")
ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,160}$")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")


class ValidationConsoleError(RuntimeError):
    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def canonical_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValidationConsoleError("timestamp_invalid", "时间戳无效。")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationConsoleError("timestamp_invalid", "时间戳无效。") from exc
    if parsed.utcoffset() is None:
        raise ValidationConsoleError("timestamp_invalid", "时间戳无效。")
    return parsed.astimezone(timezone.utc)


def atomic_write(path: Path, raw: bytes, mode: int = 0o600) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, mode)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _safe_object(path: Path, *, max_bytes: int = 2 * 1024 * 1024) -> dict[str, Any]:
    try:
        node = path.lstat()
        if path.is_symlink() or not path.is_file() or node.st_size > max_bytes:
            raise ValidationConsoleError("state_invalid", "控制台状态文件无效。", 503)
        value = json.loads(path.read_text(encoding="utf-8"))
    except ValidationConsoleError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValidationConsoleError("state_invalid", "控制台状态文件无效。", 503) from exc
    if not isinstance(value, dict):
        raise ValidationConsoleError("state_invalid", "控制台状态文件无效。", 503)
    return value


def _is_sha(value: Any) -> bool:
    return isinstance(value, str) and SHA_RE.fullmatch(value) is not None


class ValidationConsoleStore:
    def __init__(
        self,
        root: Path,
        *,
        fixture_mode: bool,
        central_release_id: str,
    ) -> None:
        if not _is_sha(central_release_id):
            raise ValidationConsoleError("release_invalid", "Release 身份无效。")
        self.root = root.expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        self.fixture_mode = fixture_mode
        self.central_release_id = central_release_id
        self.state_path = self.root / "state.json"
        self.status_path = self.root / "technical-status.json"
        self.secret_path = self.root / "csrf-secret.bin"
        self.receipt_root = self.root / "receipts"
        self.report_root = self.root / "reports"
        self.audit_root = self.root / "audit-packages"
        self._ensure_secret()
        if not self.state_path.exists():
            self._write_state(self._initial_state())
        else:
            self._validate_state(self._read_state())

    def _ensure_secret(self) -> None:
        if self.secret_path.exists():
            try:
                node = self.secret_path.lstat()
                if (
                    self.secret_path.is_symlink()
                    or not self.secret_path.is_file()
                    or node.st_size != 32
                    or stat_mode(node.st_mode) != 0o600
                ):
                    raise ValidationConsoleError(
                        "csrf_secret_invalid", "CSRF secret 无效。", 503
                    )
            except OSError as exc:
                raise ValidationConsoleError(
                    "csrf_secret_invalid", "CSRF secret 无效。", 503
                ) from exc
            return
        descriptor = os.open(
            self.secret_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(secrets.token_bytes(32))
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            os.chmod(self.secret_path, 0o600)

    def _initial_state(self) -> dict[str, Any]:
        now = utc_now()
        return {
            "schema_version": "study-intake-validation-console-state-v1",
            "mode": "fixture" if self.fixture_mode else "production",
            "revision": 0,
            "central_release_id": self.central_release_id,
            "execution_mode": "fixture" if self.fixture_mode else "offline",
            "live_gate": "locked",
            "manual_authorization_present": False,
            "production_accepted": False,
            "formal_write_count": 0,
            "emergency_locked": True,
            "terra_state": "locked",
            "luna_state": "locked",
            "sol_handoff_state": "not_ready",
            "active_authorizations": {},
            "consumed_authorizations": {},
            "campaigns": {},
            "used_nonces": [],
            "fixture_promotion_apply_count": 0,
            "production_promotion_attempt_count": 0,
            "created_at": now,
            "updated_at": now,
        }

    def _validate_state(self, value: Mapping[str, Any]) -> None:
        expected_keys = {
            "schema_version",
            "mode",
            "revision",
            "central_release_id",
            "execution_mode",
            "live_gate",
            "manual_authorization_present",
            "production_accepted",
            "formal_write_count",
            "emergency_locked",
            "terra_state",
            "luna_state",
            "sol_handoff_state",
            "active_authorizations",
            "consumed_authorizations",
            "campaigns",
            "used_nonces",
            "fixture_promotion_apply_count",
            "production_promotion_attempt_count",
            "created_at",
            "updated_at",
        }
        if (
            set(value) != expected_keys
            or value.get("schema_version")
            != "study-intake-validation-console-state-v1"
            or value.get("mode")
            != ("fixture" if self.fixture_mode else "production")
            or value.get("central_release_id") != self.central_release_id
            or isinstance(value.get("revision"), bool)
            or not isinstance(value.get("revision"), int)
            or value["revision"] < 0
            or value.get("live_gate") != "locked"
            or value.get("production_accepted") is not False
            or value.get("formal_write_count") != 0
            or value.get("production_promotion_attempt_count") != 0
            or not isinstance(value.get("active_authorizations"), dict)
            or not isinstance(value.get("consumed_authorizations"), dict)
            or not isinstance(value.get("campaigns"), dict)
            or not isinstance(value.get("used_nonces"), list)
        ):
            raise ValidationConsoleError("state_invalid", "控制台状态无效。", 503)
        if not self.fixture_mode and (
            value.get("execution_mode") != "offline"
            or value.get("manual_authorization_present") is not False
            or value.get("active_authorizations") != {}
        ):
            raise ValidationConsoleError(
                "production_state_not_locked", "生产控制台没有保持锁定。", 503
            )
        _timestamp(value.get("created_at"))
        _timestamp(value.get("updated_at"))

    def _read_state(self) -> dict[str, Any]:
        value = _safe_object(self.state_path)
        self._validate_state(value)
        return value

    def _write_state(self, value: Mapping[str, Any]) -> dict[str, Any]:
        self._validate_state(value)
        atomic_write(self.state_path, canonical_bytes(value))
        reopened = _safe_object(self.state_path)
        self._validate_state(reopened)
        if reopened != dict(value):
            raise ValidationConsoleError(
                "state_reopen_failed", "控制台状态重开校验失败。", 503
            )
        return reopened

    def csrf_token(self) -> str:
        secret = self.secret_path.read_bytes()
        payload = f"validation-console:{self.central_release_id}".encode("utf-8")
        return hmac.new(secret, payload, hashlib.sha256).hexdigest()

    def check_csrf(self, token: str | None) -> None:
        if not isinstance(token, str) or not hmac.compare_digest(
            token, self.csrf_token()
        ):
            raise ValidationConsoleError("csrf_invalid", "CSRF 校验失败。", 403)

    def _check_write_request(
        self,
        *,
        csrf_token: str | None,
        nonce: str | None,
        expected_revision: int | None,
    ) -> tuple[dict[str, Any], str]:
        self.check_csrf(csrf_token)
        if not isinstance(nonce, str) or NONCE_RE.fullmatch(nonce) is None:
            raise ValidationConsoleError("nonce_invalid", "Nonce 无效。")
        state = self._read_state()
        if expected_revision != state["revision"]:
            raise ValidationConsoleError(
                "state_precondition_failed", "控制台状态已变化，请刷新。", 409
            )
        if nonce in state["used_nonces"]:
            raise ValidationConsoleError("nonce_replayed", "Nonce 已使用。", 409)
        return state, nonce

    def _commit(
        self, state: dict[str, Any], *, nonce: str, changes: Mapping[str, Any]
    ) -> dict[str, Any]:
        used = list(state["used_nonces"])
        used.append(nonce)
        if len(used) > 256:
            used = used[-256:]
        value = {
            **state,
            **dict(changes),
            "revision": int(state["revision"]) + 1,
            "used_nonces": used,
            "updated_at": utc_now(),
        }
        return self._write_state(value)

    def _receipt(
        self, category: str, core: Mapping[str, Any]
    ) -> tuple[Path, dict[str, Any]]:
        digest = sha256_bytes(canonical_bytes(core))
        value = {**dict(core), "receipt_sha256": digest}
        path = self.receipt_root / category / f"{digest}.json"
        payload = canonical_bytes(value)
        if path.exists():
            if path.is_symlink() or not path.is_file() or path.read_bytes() != payload:
                raise ValidationConsoleError(
                    "receipt_no_clobber_conflict", "Receipt 身份冲突。", 409
                )
        else:
            atomic_write(path, payload)
        if _safe_object(path) != value:
            raise ValidationConsoleError(
                "receipt_reopen_failed", "Receipt 重开校验失败。", 503
            )
        return path, value

    def publish_technical_status(self, value: Mapping[str, Any]) -> dict[str, Any]:
        required = {
            "schema_version",
            "central_release_id",
            "execution_mode",
            "live_gate_locked",
            "manual_authorization_present",
            "production_accepted",
            "formal_write_count",
            "skills",
            "mcp_preflight",
            "engineering",
            "reports",
            "audit_package",
            "updated_at",
        }
        if (
            set(value) != required
            or value.get("schema_version")
            != "study-intake-validation-console-technical-status-v1"
            or value.get("central_release_id") != self.central_release_id
            or value.get("execution_mode") != "offline"
            or value.get("live_gate_locked") is not True
            or value.get("manual_authorization_present") is not False
            or value.get("production_accepted") is not False
            or value.get("formal_write_count") != 0
            or not isinstance(value.get("skills"), Mapping)
            or not isinstance(value.get("mcp_preflight"), Mapping)
            or not isinstance(value.get("engineering"), Mapping)
            or set(value["skills"]) != SUBJECTS
            or set(value["mcp_preflight"]) != SUBJECTS
            or set(value["engineering"]) != set(ENGINEERING_GATES)
            or any(
                value[section][subject] not in {"passed", "failed", "pending"}
                for section in ("skills", "mcp_preflight")
                for subject in SUBJECTS
            )
            or any(
                value["engineering"][gate]
                not in {"passed", "failed", "pending"}
                for gate in ENGINEERING_GATES
            )
            or not isinstance(value.get("reports"), Mapping)
            or not isinstance(value.get("audit_package"), Mapping)
        ):
            raise ValidationConsoleError(
                "technical_status_invalid", "技术状态无效。"
            )
        _timestamp(value.get("updated_at"))
        atomic_write(self.status_path, canonical_bytes(value))
        reopened = _safe_object(self.status_path)
        if reopened != dict(value):
            raise ValidationConsoleError(
                "technical_status_reopen_failed", "技术状态重开失败。", 503
            )
        return reopened

    def technical_status(self) -> dict[str, Any]:
        if not self.status_path.exists():
            return {
                "schema_version": "study-intake-validation-console-technical-status-v1",
                "central_release_id": self.central_release_id,
                "execution_mode": "offline",
                "live_gate_locked": True,
                "manual_authorization_present": False,
                "production_accepted": False,
                "formal_write_count": 0,
                "skills": {subject: "pending" for subject in SUBJECTS},
                "mcp_preflight": {subject: "pending" for subject in SUBJECTS},
                "engineering": {gate: "pending" for gate in ENGINEERING_GATES},
                "reports": {},
                "audit_package": {},
                "updated_at": utc_now(),
            }
        return _safe_object(self.status_path)

    def public_state(self) -> dict[str, Any]:
        state = self._read_state()
        technical = self.technical_status()
        return {
            "schema_version": "study-intake-validation-console-public-state-v1",
            "revision": state["revision"],
            "mode": state["mode"],
            "current_release": self.central_release_id,
            "execution_mode": state["execution_mode"].upper(),
            "live_gate": "LOCKED",
            "authorization": "ABSENT"
            if not state["manual_authorization_present"]
            else "PRESENT",
            "production_accepted": False,
            "formal_write_count": 0,
            "skills": technical["skills"],
            "mcp_preflight": technical["mcp_preflight"],
            "engineering": technical["engineering"],
            "terra": {
                "state": state["terra_state"],
                "action_enabled": self.fixture_mode
                and state["terra_state"] == "eligible",
            },
            "luna": {
                "state": state["luna_state"],
                "action_enabled": self.fixture_mode
                and state["luna_state"] == "waiting_for_verified_plan",
                "branches": {
                    "planned": 0,
                    "waiting": 0,
                    "running": 0,
                    "terminal": 0,
                },
            },
            "sol_handoff": {
                "state": state["sol_handoff_state"],
                "action_enabled": self.fixture_mode
                and state["sol_handoff_state"]
                == "technical_integrity_passed",
                "calls_sol_model": False,
                "formal_write_allowed": False,
            },
            "reports": technical["reports"],
            "audit_package": technical["audit_package"],
            "emergency_locked": state["emergency_locked"],
            "only_real_model_capture_validation_remains": all(
                technical[section][subject] == "passed"
                for section in ("skills", "mcp_preflight")
                for subject in SUBJECTS
            )
            and all(
                technical["engineering"][gate] == "passed"
                for gate in ENGINEERING_GATES
            ),
            "csrf_token": self.csrf_token(),
        }

    def issue_authorization(
        self,
        stage: str,
        payload: Mapping[str, Any],
        *,
        csrf_token: str | None,
        nonce: str | None,
        expected_revision: int | None,
    ) -> dict[str, Any]:
        if not self.fixture_mode:
            raise ValidationConsoleError(
                "offline_locked",
                "生产控制台保持 OFFLINE；本轮不会签发真实授权。",
                423,
            )
        if stage not in STAGES:
            raise ValidationConsoleError("stage_invalid", "Stage 无效。")
        state, checked_nonce = self._check_write_request(
            csrf_token=csrf_token,
            nonce=nonce,
            expected_revision=expected_revision,
        )
        subject = payload.get("subject")
        capture_id = payload.get("capture_id")
        capture_sha = payload.get("capture_content_sha256")
        activation_id = payload.get("activation_id")
        user_receipt_sha256 = payload.get("user_authorization_receipt_sha256")
        if (
            subject not in SUBJECTS
            or not isinstance(capture_id, str)
            or ID_RE.fullmatch(capture_id) is None
            or not _is_sha(capture_sha)
            or not _is_sha(activation_id)
            or not _is_sha(user_receipt_sha256)
            or payload.get("maximum_tasks") != 1
            or payload.get("formal_write_allowed") is not False
        ):
            raise ValidationConsoleError(
                "authorization_contract_invalid", "授权字段不完整。"
            )
        if stage == "luna" and payload.get("verified_terra_plan_sha256") is None:
            raise ValidationConsoleError(
                "terra_plan_required", "Luna 必须绑定已验证的 Terra read plan。", 409
            )
        if stage == "luna" and not _is_sha(payload.get("verified_terra_plan_sha256")):
            raise ValidationConsoleError(
                "terra_plan_required", "Terra read plan 身份无效。", 409
            )
        now = datetime.now(timezone.utc)
        expires = now + timedelta(minutes=10)
        issuer_core = {
            "schema_version": "study-intake-production-issuer-receipt-v1",
            "fixture_only": True,
            "subject": subject,
            "capture_id": capture_id,
            "capture_content_sha256": capture_sha,
            "release_id": self.central_release_id,
            "activation_id": activation_id,
            "allow_terra": True,
            "allow_luna": True,
            "maximum_tasks": 1,
            "not_before": now.isoformat(),
            "expires_at": expires.isoformat(),
            "nonce": checked_nonce,
            "user_authorization_receipt_sha256": user_receipt_sha256,
            "formal_write_allowed": False,
            "formal_write_count": 0,
        }
        issuer_path, issuer_receipt = self._receipt("issuer", issuer_core)
        live_core = {
            "schema_version": "study-intake-manual-live-authorization-v1",
            "subject": subject,
            "capture_id": capture_id,
            "capture_content_sha256": capture_sha,
            "release_id": self.central_release_id,
            "activation_id": activation_id,
            "allow_terra": True,
            "allow_luna": True,
            "maximum_tasks": 1,
            "not_before": now.isoformat(),
            "expires_at": expires.isoformat(),
            "nonce": checked_nonce,
            "issuer_authorization_receipt_sha256": issuer_receipt[
                "receipt_sha256"
            ],
            "formal_write_allowed": False,
        }
        live_authorization_sha256 = sha256_bytes(canonical_bytes(live_core))
        live_authorization = {
            **live_core,
            "authorization_sha256": live_authorization_sha256,
        }
        live_path = (
            self.receipt_root
            / "live-authorizations"
            / f"{live_authorization_sha256}.json"
        )
        atomic_write(live_path, canonical_bytes(live_authorization))
        if _safe_object(live_path) != live_authorization:
            raise ValidationConsoleError(
                "authorization_reopen_failed", "Authorization 重开失败。", 503
            )
        core = {
            "schema_version": "study-intake-stage-authorization-v1",
            "fixture_only": True,
            "stage": stage,
            "subject": subject,
            "capture_id": capture_id,
            "capture_content_sha256": capture_sha,
            "central_release_id": self.central_release_id,
            "activation_id": activation_id,
            "live_authorization_sha256": live_authorization_sha256,
            "issuer_authorization_receipt_sha256": issuer_receipt[
                "receipt_sha256"
            ],
            "maximum_tasks": 1,
            "not_before": now.isoformat(),
            "expires_at": expires.isoformat(),
            "nonce": checked_nonce,
            "verified_terra_plan_sha256": payload.get(
                "verified_terra_plan_sha256"
            ),
            "formal_write_allowed": False,
            "formal_write_count": 0,
        }
        path, receipt = self._receipt("authorizations", core)
        active = dict(state["active_authorizations"])
        active[stage] = {
            "receipt_sha256": receipt["receipt_sha256"],
            "receipt_path": str(path),
            "live_authorization_sha256": live_authorization_sha256,
            "live_authorization_path": str(live_path),
            "issuer_receipt_sha256": issuer_receipt["receipt_sha256"],
            "issuer_receipt_path": str(issuer_path),
            "remaining_tasks": 1,
            "expires_at": receipt["expires_at"],
        }
        changes: dict[str, Any] = {
            "active_authorizations": active,
            "manual_authorization_present": True,
            "emergency_locked": False,
            f"{stage}_state": "authorization_issued",
        }
        committed = self._commit(state, nonce=checked_nonce, changes=changes)
        return {
            "status": "fixture_authorization_issued",
            "stage": stage,
            "receipt_sha256": receipt["receipt_sha256"],
            "receipt_path": str(path),
            "live_authorization_sha256": live_authorization_sha256,
            "live_authorization_path": str(live_path),
            "issuer_receipt_sha256": issuer_receipt["receipt_sha256"],
            "issuer_receipt_path": str(issuer_path),
            "revision": committed["revision"],
            "formal_write_count": 0,
        }

    def terminal_relock(
        self,
        stage: str,
        outcome: str,
        *,
        csrf_token: str | None,
        nonce: str | None,
        expected_revision: int | None,
    ) -> dict[str, Any]:
        if not self.fixture_mode:
            raise ValidationConsoleError("offline_locked", "生产路径保持锁定。", 423)
        if stage not in STAGES or outcome not in TERMINAL_OUTCOMES:
            raise ValidationConsoleError("terminal_invalid", "终态无效。")
        state, checked_nonce = self._check_write_request(
            csrf_token=csrf_token,
            nonce=nonce,
            expected_revision=expected_revision,
        )
        active = dict(state["active_authorizations"])
        authorization = active.pop(stage, None)
        if not isinstance(authorization, Mapping):
            raise ValidationConsoleError(
                "authorization_missing", "没有可消费的 stage 授权。", 409
            )
        core = {
            "schema_version": "study-intake-stage-terminal-relock-v1",
            "fixture_only": True,
            "stage": stage,
            "outcome": outcome,
            "authorization_receipt_sha256": authorization["receipt_sha256"],
            "remaining_tasks": 0,
            "relocked": True,
            "formal_write_count": 0,
            "completed_at": utc_now(),
        }
        path, receipt = self._receipt("terminal-relock", core)
        consumed = dict(state["consumed_authorizations"])
        consumed[str(authorization["receipt_sha256"])] = receipt["receipt_sha256"]
        changes = {
            "active_authorizations": active,
            "consumed_authorizations": consumed,
            "manual_authorization_present": bool(active),
            "emergency_locked": not bool(active),
            f"{stage}_state": "relocked",
        }
        committed = self._commit(state, nonce=checked_nonce, changes=changes)
        return {
            "status": "fixture_terminal_relocked",
            "stage": stage,
            "outcome": outcome,
            "receipt_sha256": receipt["receipt_sha256"],
            "receipt_path": str(path),
            "remaining_tasks": 0,
            "revision": committed["revision"],
            "formal_write_count": 0,
        }

    def emergency_lock(
        self,
        *,
        csrf_token: str | None,
        nonce: str | None,
        expected_revision: int | None,
    ) -> dict[str, Any]:
        state, checked_nonce = self._check_write_request(
            csrf_token=csrf_token,
            nonce=nonce,
            expected_revision=expected_revision,
        )
        revoked = sorted(state["active_authorizations"])
        core = {
            "schema_version": "study-intake-validation-emergency-lock-v1",
            "fixture_only": self.fixture_mode,
            "revoked_stages": revoked,
            "live_gate": "locked",
            "production_accepted": False,
            "formal_write_count": 0,
            "created_at": utc_now(),
        }
        path, receipt = self._receipt("emergency-lock", core)
        committed = self._commit(
            state,
            nonce=checked_nonce,
            changes={
                "active_authorizations": {},
                "manual_authorization_present": False,
                "emergency_locked": True,
                "terra_state": "locked",
                "luna_state": "locked",
            },
        )
        return {
            "status": "locked",
            "revoked_stages": revoked,
            "receipt_sha256": receipt["receipt_sha256"],
            "receipt_path": str(path),
            "revision": committed["revision"],
            "formal_write_count": 0,
        }

    def create_campaign(
        self,
        payload: Mapping[str, Any],
        *,
        csrf_token: str | None,
        nonce: str | None,
        expected_revision: int | None,
    ) -> dict[str, Any]:
        if not self.fixture_mode:
            raise ValidationConsoleError(
                "offline_locked",
                "生产控制台不会创建或释放真实 campaign。",
                423,
            )
        state, checked_nonce = self._check_write_request(
            csrf_token=csrf_token,
            nonce=nonce,
            expected_revision=expected_revision,
        )
        captures = payload.get("captures")
        if (
            not isinstance(captures, list)
            or not 1 <= len(captures) <= 20
            or any(
                not isinstance(item, Mapping)
                or item.get("subject") not in SUBJECTS
                or not isinstance(item.get("capture_id"), str)
                or ID_RE.fullmatch(str(item["capture_id"])) is None
                or not _is_sha(item.get("capture_content_sha256"))
                or not _is_sha(item.get("activation_id"))
                or not _is_sha(item.get("user_authorization_receipt_sha256"))
                for item in captures
            )
        ):
            raise ValidationConsoleError("campaign_invalid", "Campaign 字段无效。")
        identities = [
            (str(item["subject"]), str(item["capture_id"])) for item in captures
        ]
        if len(identities) != len(set(identities)):
            raise ValidationConsoleError("campaign_duplicate", "Campaign 含重复 Capture。")
        campaign_id = "VCAMP-" + sha256_bytes(
            canonical_bytes({"captures": captures, "nonce": checked_nonce})
        )[:24]
        authorizations = [
            {
                "authorization_id": "VAUTH-"
                + sha256_bytes(
                    canonical_bytes(
                        {
                            "campaign_id": campaign_id,
                            "subject": item["subject"],
                            "capture_id": item["capture_id"],
                        }
                    )
                )[:24],
                "subject": item["subject"],
                "capture_id": item["capture_id"],
                "capture_content_sha256": item["capture_content_sha256"],
                "release_id": self.central_release_id,
                "activation_id": item["activation_id"],
                "allow_terra": True,
                "allow_luna": True,
                "maximum_tasks": 1,
                "user_authorization_receipt_sha256": item[
                    "user_authorization_receipt_sha256"
                ],
                "formal_write_allowed": False,
            }
            for item in captures
        ]
        core = {
            "schema_version": "study-intake-validation-fixture-campaign-v1",
            "fixture_only": True,
            "campaign_id": campaign_id,
            "captures": list(captures),
            "authorizations": authorizations,
            "backlog_scan_allowed": False,
            "worker_start_count": 0,
            "model_call_count": 0,
            "formal_write_count": 0,
            "created_at": utc_now(),
        }
        path, receipt = self._receipt("campaigns", core)
        campaigns = dict(state["campaigns"])
        campaigns[campaign_id] = {
            "receipt_sha256": receipt["receipt_sha256"],
            "capture_count": len(captures),
            "state": "fixture_ready",
        }
        committed = self._commit(
            state, nonce=checked_nonce, changes={"campaigns": campaigns}
        )
        return {
            "status": "fixture_campaign_ready",
            "campaign_id": campaign_id,
            "authorization_count": len(authorizations),
            "receipt_sha256": receipt["receipt_sha256"],
            "receipt_path": str(path),
            "revision": committed["revision"],
            "backlog_scan_count": 0,
            "model_call_count": 0,
            "formal_write_count": 0,
        }

    def export_handoff(
        self,
        payload: Mapping[str, Any],
        *,
        csrf_token: str | None,
        nonce: str | None,
        expected_revision: int | None,
    ) -> dict[str, Any]:
        if not self.fixture_mode:
            raise ValidationConsoleError(
                "handoff_not_ready",
                "没有真实终态任务；本轮不会生成业务 Sol handoff。",
                409,
            )
        state, checked_nonce = self._check_write_request(
            csrf_token=csrf_token,
            nonce=nonce,
            expected_revision=expected_revision,
        )
        quality = payload.get("quality_outcome")
        if quality not in QUALITY_OUTCOMES:
            raise ValidationConsoleError("handoff_invalid", "Handoff 质量状态无效。")
        package_kind = (
            "diagnostic"
            if quality == "technical_quarantine"
            else "risk"
            if quality == "issues_found"
            else "normal"
        )
        core = {
            "schema_version": "study-intake-validation-fixture-handoff-v1",
            "fixture_only": True,
            "quality_outcome": quality,
            "package_kind": package_kind,
            "calls_sol_model": False,
            "formal_write_allowed": False,
            "formal_write_count": 0,
            "created_at": utc_now(),
        }
        path, receipt = self._receipt("handoffs", core)
        committed = self._commit(
            state,
            nonce=checked_nonce,
            changes={"sol_handoff_state": "exported"},
        )
        return {
            "status": "fixture_handoff_exported",
            "package_kind": package_kind,
            "receipt_sha256": receipt["receipt_sha256"],
            "receipt_path": str(path),
            "revision": committed["revision"],
            "calls_sol_model": False,
            "formal_write_count": 0,
        }

    def promotion_preview(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        receipts = payload.get("live_stage_receipts")
        eligible = (
            self.fixture_mode
            and isinstance(receipts, list)
            and len(receipts) == 3
            and all(_is_sha(value) for value in receipts)
        )
        return {
            "schema_version": "study-intake-promotion-preview-v1",
            "eligible": eligible,
            "production_apply_allowed": False,
            "missing": [] if eligible else ["three_distinct_live_stage_receipts"],
            "production_accepted": False,
            "formal_write_count": 0,
        }

    def promotion_apply(
        self,
        payload: Mapping[str, Any],
        *,
        csrf_token: str | None,
        nonce: str | None,
        expected_revision: int | None,
    ) -> dict[str, Any]:
        if not self.fixture_mode:
            raise ValidationConsoleError(
                "offline_locked",
                "本轮禁止 production promotion。",
                423,
            )
        state, checked_nonce = self._check_write_request(
            csrf_token=csrf_token,
            nonce=nonce,
            expected_revision=expected_revision,
        )
        preview = self.promotion_preview(payload)
        if preview["eligible"] is not True:
            raise ValidationConsoleError(
                "promotion_evidence_incomplete", "Promotion 证据不完整。", 409
            )
        core = {
            "schema_version": "study-intake-validation-fixture-promotion-v1",
            "fixture_only": True,
            "live_stage_receipts": payload["live_stage_receipts"],
            "production_state_changed": False,
            "production_accepted": False,
            "formal_write_count": 0,
            "created_at": utc_now(),
        }
        path, receipt = self._receipt("promotion", core)
        committed = self._commit(
            state,
            nonce=checked_nonce,
            changes={
                "fixture_promotion_apply_count": int(
                    state["fixture_promotion_apply_count"]
                )
                + 1
            },
        )
        return {
            "status": "fixture_promotion_gate_passed_without_production_apply",
            "receipt_sha256": receipt["receipt_sha256"],
            "receipt_path": str(path),
            "revision": committed["revision"],
            "production_state_changed": False,
            "production_accepted": False,
            "formal_write_count": 0,
        }

    def campaigns_public(self) -> dict[str, Any]:
        state = self._read_state()
        return {
            "schema_version": "study-intake-validation-campaigns-public-v1",
            "campaigns": state["campaigns"],
            "production_campaign_count": 0,
            "formal_write_count": 0,
        }

    def report(self, report_id: str) -> tuple[bytes, str]:
        if not isinstance(report_id, str) or ID_RE.fullmatch(report_id) is None:
            raise ValidationConsoleError("report_id_invalid", "报告 ID 无效。")
        technical = self.technical_status()
        descriptor = technical.get("reports", {}).get(report_id)
        if not isinstance(descriptor, Mapping):
            raise ValidationConsoleError("report_not_found", "没有这个报告。", 404)
        relative = descriptor.get("relative_path")
        expected_sha = descriptor.get("sha256")
        if not isinstance(relative, str) or not _is_sha(expected_sha):
            raise ValidationConsoleError("report_invalid", "报告描述无效。", 503)
        path = self.report_root / relative
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(self.report_root.resolve())
            node = resolved.lstat()
            raw = resolved.read_bytes()
        except (OSError, ValueError) as exc:
            raise ValidationConsoleError("report_invalid", "报告文件无效。", 503) from exc
        if resolved.is_symlink() or not resolved.is_file() or sha256_bytes(raw) != expected_sha:
            raise ValidationConsoleError("report_invalid", "报告文件无效。", 503)
        content_type = (
            "application/json; charset=utf-8"
            if resolved.suffix == ".json"
            else "text/markdown; charset=utf-8"
        )
        return raw, content_type

    def audit_package(self) -> bytes:
        descriptor = self.technical_status().get("audit_package")
        if (
            not isinstance(descriptor, Mapping)
            or descriptor.get("available") is not True
            or not isinstance(descriptor.get("filename"), str)
            or not _is_sha(descriptor.get("sha256"))
        ):
            raise ValidationConsoleError(
                "audit_package_not_found", "审核包尚未发布。", 404
            )
        path = self.audit_root / str(descriptor["filename"])
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(self.audit_root.resolve())
            raw = resolved.read_bytes()
        except (OSError, ValueError) as exc:
            raise ValidationConsoleError(
                "audit_package_invalid", "审核包文件无效。", 503
            ) from exc
        if resolved.is_symlink() or not resolved.is_file() or sha256_bytes(raw) != descriptor["sha256"]:
            raise ValidationConsoleError(
                "audit_package_invalid", "审核包文件无效。", 503
            )
        return raw


def stat_mode(mode: int) -> int:
    return mode & 0o777
