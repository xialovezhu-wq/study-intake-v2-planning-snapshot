"""Exact one-shot manual live authorization and automatic relock state."""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import hmac
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Mapping


SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SUBJECTS = frozenset({"math", "cs408", "english"})
AUTH_SCHEMA = "study-intake-manual-live-authorization-v1"


class ManualAdmissionError(RuntimeError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def sha256_value(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def _time(value: Any, label: str) -> dt.datetime:
    if not isinstance(value, str):
        raise ManualAdmissionError(f"{label}_invalid")
    try:
        parsed = dt.datetime.fromisoformat(value[:-1] + "+00:00" if value.endswith("Z") else value)
    except ValueError as exc:
        raise ManualAdmissionError(f"{label}_invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ManualAdmissionError(f"{label}_invalid")
    return parsed.astimezone(dt.timezone.utc)


def validate_authorization(
    value: Mapping[str, Any], *, now: dt.datetime | None = None
) -> dict[str, Any]:
    expected = {
        "schema_version", "subject", "capture_id", "capture_content_sha256",
        "release_id", "activation_id", "allow_terra", "allow_luna",
        "maximum_tasks", "not_before", "expires_at", "nonce",
        "issuer_authorization_receipt_sha256", "formal_write_allowed",
        "authorization_sha256",
    }
    if set(value) != expected or value.get("schema_version") != AUTH_SCHEMA:
        raise ManualAdmissionError("manual_live_authorization_shape_invalid")
    subject = value.get("subject")
    if subject not in SUBJECTS:
        raise ManualAdmissionError("manual_live_authorization_subject_invalid")
    for key in (
        "capture_content_sha256", "release_id", "activation_id",
        "issuer_authorization_receipt_sha256", "authorization_sha256",
    ):
        if not isinstance(value.get(key), str) or SHA256_RE.fullmatch(str(value[key])) is None:
            raise ManualAdmissionError("manual_live_authorization_hash_invalid")
    if (
        not isinstance(value.get("capture_id"), str)
        or not value["capture_id"]
        or not isinstance(value.get("nonce"), str)
        or not 16 <= len(value["nonce"]) <= 256
        or value.get("allow_terra") is not True
        or value.get("allow_luna") is not True
        or value.get("maximum_tasks") != 1
        or value.get("formal_write_allowed") is not False
    ):
        raise ManualAdmissionError("manual_live_authorization_policy_invalid")
    core = {key: copy.deepcopy(value[key]) for key in sorted(expected - {"authorization_sha256"})}
    if sha256_value(core) != value.get("authorization_sha256"):
        raise ManualAdmissionError("manual_live_authorization_digest_invalid")
    not_before = _time(value.get("not_before"), "authorization_not_before")
    expires_at = _time(value.get("expires_at"), "authorization_expires_at")
    observed = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    if expires_at <= not_before or expires_at - not_before > dt.timedelta(minutes=15):
        raise ManualAdmissionError("manual_live_authorization_window_invalid")
    if observed < not_before:
        raise ManualAdmissionError("manual_live_authorization_not_started")
    if observed >= expires_at:
        raise ManualAdmissionError("manual_live_authorization_expired")
    return copy.deepcopy(dict(value))


def validate_authorization_for_task(
    value: Mapping[str, Any], *, task_identity: Mapping[str, Any], now: dt.datetime | None = None
) -> dict[str, Any]:
    authorization = validate_authorization(value, now=now)
    expected = {
        "subject": authorization["subject"],
        "capture_id": authorization["capture_id"],
        "capture_content_sha256": authorization["capture_content_sha256"],
        "release_id": authorization["release_id"],
        "activation_id": authorization["activation_id"],
    }
    if any(task_identity.get(key) != wanted for key, wanted in expected.items()):
        raise ManualAdmissionError("manual_live_authorization_task_mismatch")
    return authorization


def build_fixture_authorization(
    *, subject: str, capture_id: str, capture_content_sha256: str,
    release_id: str, activation_id: str, now: dt.datetime | None = None,
) -> dict[str, Any]:
    issued = (now or dt.datetime.now(dt.timezone.utc)).astimezone(dt.timezone.utc)
    core = {
        "schema_version": AUTH_SCHEMA,
        "subject": subject,
        "capture_id": capture_id,
        "capture_content_sha256": capture_content_sha256,
        "release_id": release_id,
        "activation_id": activation_id,
        "allow_terra": True,
        "allow_luna": True,
        "maximum_tasks": 1,
        "not_before": issued.isoformat(),
        "expires_at": (issued + dt.timedelta(minutes=2)).isoformat(),
        "nonce": "fixture-" + hashlib.sha256(f"{subject}:{capture_id}:{issued.isoformat()}".encode()).hexdigest(),
        "issuer_authorization_receipt_sha256": "f" * 64,
        "formal_write_allowed": False,
    }
    return {**core, "authorization_sha256": sha256_value(core)}


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    payload = canonical_bytes(value)
    descriptor, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(raw, path)
    finally:
        if os.path.exists(raw):
            os.unlink(raw)


class ManualAuthorizationStore:
    """Persist one exact authorization and consume it once on any terminal."""

    def __init__(self, runtime_root: Path, key: bytes) -> None:
        self.root = runtime_root.resolve() / "dispatch" / "manual-live-authorization-v1"
        if len(key) < 32:
            raise ManualAdmissionError("manual_live_authorization_key_invalid")
        self.key = bytes(key)

    def _seal(self, value: Mapping[str, Any], purpose: str) -> str:
        return hmac.new(self.key, canonical_bytes({"purpose": purpose, "value": value}), hashlib.sha256).hexdigest()

    def stage_fixture(
        self,
        authorization: Mapping[str, Any],
        *,
        now: dt.datetime | None = None,
    ) -> dict[str, Any]:
        validated = validate_authorization(authorization, now=now)
        core = {
            "schema_version": "study-intake-live-execution-gate-state-v1",
            "status": "armed_once",
            "authorization": validated,
            "remaining_tasks": 1,
            "formal_write_count": 0,
        }
        state = {**core, "hmac_sha256": self._seal(core, "manual-live-gate-state")}
        _atomic_json(self.root / "state.json", state)
        return state

    def terminal_relock(self, *, authorization_sha256: str, outcome: str) -> dict[str, Any]:
        if not SHA256_RE.fullmatch(authorization_sha256) or outcome not in {
            "succeeded", "failed", "cancelled", "timed_out"
        }:
            raise ManualAdmissionError("manual_live_terminal_invalid")
        core = {
            "schema_version": "study-intake-manual-live-authorization-consumption-v1",
            "authorization_sha256": authorization_sha256,
            "outcome": outcome,
            "status": "consumed_relocked",
            "remaining_tasks": 0,
            "formal_write_count": 0,
        }
        receipt = {**core, "hmac_sha256": self._seal(core, "manual-live-consumption")}
        _atomic_json(self.root / "receipts" / f"{sha256_value(receipt)}.json", receipt)
        locked_core = {
            "schema_version": "study-intake-live-execution-gate-state-v1",
            "status": "locked",
            "authorization": None,
            "remaining_tasks": 0,
            "formal_write_count": 0,
        }
        locked = {**locked_core, "hmac_sha256": self._seal(locked_core, "manual-live-gate-state")}
        _atomic_json(self.root / "state.json", locked)
        return {"state": locked, "receipt": receipt}
