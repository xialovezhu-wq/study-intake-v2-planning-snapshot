from __future__ import annotations

import copy
import datetime as dt
import hashlib
import hmac
import json
import os
import subprocess
import tempfile
import threading
import unittest
import uuid
from pathlib import Path

import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "tests"))

from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    DispatchError,
    LeaseStore,
)
from dashboard.concurrency_campaign import (  # noqa: E402
    SCHEMA_VERSION as CAMPAIGN_SCHEMA_VERSION,
    V2_TOP_LEVEL_KEYS,
    campaign_contract_error,
    campaign_runtime_error,
)
from process_identity import kernel_process_start_token  # noqa: E402
from scripts import publish_concurrency_campaign as publisher  # noqa: E402
from test_production_canary_admission import (  # noqa: E402
    GroundedRunner,
    IdentityPublishingFixtureRunner,
    authority,
    canary_task,
    grounded_stage,
)


SUBJECTS = ("math", "cs408", "english")
RELEASE_ID = "a" * 64
ACTIVATED_AT = "2026-08-11T00:00:00Z"


def canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def sha(value: object) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()


class HoldingIdentityRunner:
    """Zero-model runner whose supervisor interval stays open at a barrier."""

    def __init__(
        self,
        store: LeaseStore,
        entered: threading.Event,
        release: threading.Event,
    ) -> None:
        self.store = store
        self.entered = entered
        self.release = release

    def run_analysis(self, task, context):
        process = subprocess.Popen(
            ["/bin/sh", "-c", "read fixture_line"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        refs = self.store.publish_task_process_identity(
            task,
            context.lease,
            child_pid=process.pid,
            child_pgid=os.getpgid(process.pid),
            process_start_token=kernel_process_start_token(process.pid),
            launch_nonce=uuid.uuid4().hex,
            launched_at=(
                dt.datetime.now(dt.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            ),
            argv=["/bin/sh", "-c", "read fixture_line"],
            executable_path=Path("/bin/sh"),
            start_new_session=True,
        )
        self.entered.set()
        if not self.release.wait(5):
            process.kill()
            process.wait(timeout=2)
            raise RuntimeError("fixture_barrier_timeout")
        stdout, stderr = process.communicate(b"fixture\n", timeout=2)
        self.store.publish_task_process_exit(
            task,
            context.lease,
            process_identity_sha256=refs["process_identity_sha256"],
            process_identity_path=refs["process_identity_path"],
            returncode=int(process.returncode or 0),
            termination_reason="completed",
            reaped=True,
            process_absent=True,
            pgid_absent=True,
            stdout_sha256=hashlib.sha256(stdout).hexdigest(),
            stdout_size=len(stdout),
            stderr_sha256=hashlib.sha256(stderr).hexdigest(),
            stderr_size=len(stderr),
            finished_at=(
                dt.datetime.now(dt.timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            ),
        )
        return grounded_stage("analysis", task)

    def run_critical_review(self, task, _draft, _context):
        return grounded_stage("critical_review", task)

    def cancel(self, _context):
        return None


class CampaignFixture:
    def __init__(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.runtime = self.root / "runtime"
        self.release_root = self.root / "release"
        self.release_root.mkdir()
        self.evidence_root = self.root / "evidence"
        self.state_root = self.root / "state"
        self.evidence_root.mkdir()
        self.state_root.mkdir()
        self.state_path = self.state_root / "campaign.json"
        self.store = LeaseStore(self.runtime)
        self.states: dict[str, dict] = {}
        for subject in SUBJECTS:
            self.store.begin_subject_drain(subject)
            self.states[subject] = self.store.activate_production_canary(
                subject,
                release_id=RELEASE_ID,
                producer_authority=authority(subject, RELEASE_ID),
                activated_at=ACTIVATED_AT,
            )
        self.dispatch_key = self.store.authority_key_path
        self.deployment_key = self.root / "deployment-authority.key"
        self.deployment_key.write_bytes(b"d" * 32)
        self.deployment_key.chmod(0o600)
        self._write_release()
        self.activation_receipt = self._write_global_activation()

    def close(self) -> None:
        self.temp.cleanup()

    def _write_release(self) -> None:
        config = {
            "model": {
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
            "dispatch": {
                "production_canary": {
                    "enabled": True,
                    "status": "production_canary_active",
                    "admission": "first_post_activation_producer_capture",
                    "keep_backlog_drained": True,
                    "post_activation_only": True,
                    "initial_canary_inflight_limit": 1,
                    "continuous_concurrency_limit": 20,
                }
            },
        }
        formal = {
            "schema_version": "study-intake-formal-surface-gate-v1",
            "status": "passed",
            "verification_status": "unchanged",
            "baseline_file_sha256": "1" * 64,
            "config_file_sha256": "2" * 64,
            "baseline_manifest_sha256": "3" * 64,
            "current_manifest_sha256": "3" * 64,
            "differences": {},
        }
        manifest = {
            "schema_version": "study-intake-preprocessor-release-v2",
            "release_id": RELEASE_ID,
            "model_contract": dict(publisher.TARGET_MODEL_REQUEST_CONTRACT),
            "test_results": {
                "formal_surface_gate": formal,
            },
            "formal_write_count": 0,
        }
        (self.release_root / "config.json").write_text(
            json.dumps(config, sort_keys=True), encoding="utf-8"
        )
        (self.release_root / "release.json").write_text(
            json.dumps(manifest, sort_keys=True), encoding="utf-8"
        )

    def _write_global_activation(self) -> Path:
        release_manifest_sha = hashlib.sha256(
            (self.release_root / "release.json").read_bytes()
        ).hexdigest()
        watermarks = {
            subject: self.states[subject]["producer_high_watermark_sha256"]
            for subject in SUBJECTS
        }
        core = {
            "schema_version": (
                "study-intake-three-subject-canary-activation-receipt-v2"
            ),
            "status": "production_canary_active",
            "activation_id": sha(
                {"release": RELEASE_ID, "activated_at": ACTIVATED_AT}
            ),
            "release_id": RELEASE_ID,
            "activated_at": ACTIVATED_AT,
            "post_activation_only": True,
            "historical_backlog_drained": True,
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
            "requested_service_tier": None,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
            "producer_high_watermark_sha256s": watermarks,
            "slots": {
                subject: {
                    "subject": subject,
                    "producer_high_watermark_sha256": watermarks[subject],
                    "state": "armed",
                    "capture_id": None,
                    "completion_receipt_sha256": None,
                }
                for subject in SUBJECTS
            },
            "canary_manifest_sha256": "4" * 64,
            "release_manifest_sha256": release_manifest_sha,
            "pre_activation_verification_sha256": "5" * 64,
            "post_activation_verification_sha256": "6" * 64,
            "service_release_ids": {
                "math": RELEASE_ID,
                "cs408": RELEASE_ID,
                "english": RELEASE_ID,
                "dashboard": RELEASE_ID,
            },
            "deployment_prepare_receipt_sha256": "7" * 64,
            "model_call_count": 0,
            "provider_request_count": 0,
            "real_luna_runs": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "production_accepted": False,
        }
        key = self.deployment_key.read_bytes()
        value = {
            **core,
            "authority": {
                "schema_version": "study-intake-deployment-authority-v1",
                "algorithm": "HMAC-SHA256",
                "key_id": hashlib.sha256(key).hexdigest(),
                "purpose": "three-subject-production-canary-activation",
                "hmac_sha256": hmac.new(
                    key, canonical(core), hashlib.sha256
                ).hexdigest(),
            },
        }
        raw = canonical(value)
        digest = hashlib.sha256(raw).hexdigest()
        path = self.root / f"{digest}.json"
        path.write_bytes(raw)
        return path

    def _seal_dispatch(self, core: dict, *, purpose: str) -> dict:
        value = copy.deepcopy(core)
        value.pop("authority", None)
        key = self.dispatch_key.read_bytes()
        return {
            **value,
            "authority": {
                "schema_version": "study-intake-dispatch-authority-v1",
                "algorithm": "HMAC-SHA256",
                "key_id": hashlib.sha256(key).hexdigest(),
                "purpose": purpose,
                "hmac_sha256": hmac.new(
                    key,
                    canonical({"purpose": purpose, "payload": value}),
                    hashlib.sha256,
                ).hexdigest(),
            },
        }

    def _read_verified_dispatch(
        self,
        path: Path,
        *,
        digest: str | None,
        purpose: str,
    ) -> dict:
        try:
            path.resolve(strict=True).relative_to(self.runtime.resolve())
            raw = path.read_bytes()
            if digest is not None and hashlib.sha256(raw).hexdigest() != digest:
                raise AssertionError("fixture_dispatch_digest_mismatch")
            value = json.loads(raw)
            if raw != canonical(value) + b"\n":
                raise AssertionError("fixture_dispatch_encoding_mismatch")
            self.store._verify_seal(value, purpose=purpose)
        except (
            OSError,
            ValueError,
            TypeError,
            AssertionError,
            DispatchError,
        ) as exc:
            raise publisher.CampaignPublishError(
                "campaign_fixture_current_v3_invalid"
            ) from exc
        return value

    @staticmethod
    def _write_atomic_fixture(path: Path, value: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.chmod(0o600)
        path.write_bytes(canonical(value) + b"\n")

    @staticmethod
    def _publish_fixture_content(root: Path, value: dict) -> tuple[str, Path]:
        raw = canonical(value) + b"\n"
        digest = hashlib.sha256(raw).hexdigest()
        path = root / "sha256" / digest[:2] / f"{digest}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.read_bytes() != raw:
            raise publisher.CampaignPublishError(
                "campaign_fixture_content_collision"
            )
        path.write_bytes(raw)
        return digest, path

    @staticmethod
    def _publish_fixture_bytes(
        root: Path, raw: bytes, *, suffix: str
    ) -> tuple[str, Path]:
        digest = hashlib.sha256(raw).hexdigest()
        path = root / "sha256" / digest[:2] / f"{digest}.{suffix}"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and path.read_bytes() != raw:
            raise publisher.CampaignPublishError(
                "campaign_fixture_content_collision"
            )
        path.write_bytes(raw)
        return digest, path

    def _read_fixture_content(self, path: Path, *, digest: str) -> dict:
        try:
            path.resolve(strict=True).relative_to(self.runtime.resolve())
            raw = path.read_bytes()
            value = json.loads(raw)
            if (
                hashlib.sha256(raw).hexdigest() != digest
                or raw != canonical(value) + b"\n"
            ):
                raise AssertionError("fixture_content_binding_mismatch")
        except (OSError, ValueError, TypeError, AssertionError) as exc:
            raise publisher.CampaignPublishError(
                "campaign_fixture_current_v3_invalid"
            ) from exc
        return value

    def _mirror_success_chain_as_legacy_v2(self, terminal: dict) -> None:
        package = self._read_fixture_content(
            Path(str(terminal.get("package_path") or "")),
            digest=str(terminal.get("package_sha256") or ""),
        )
        package["schema_version"] = "study-intake-concurrent-package-v1"
        package_sha, package_path = self._publish_fixture_content(
            self.runtime / "dispatch/packages", package
        )
        package_ref = "study-intake-dispatch-package://sha256/" + package_sha

        selected = terminal.get("selected")
        if not isinstance(selected, dict):
            raise publisher.CampaignPublishError(
                "campaign_fixture_current_v3_invalid"
            )
        report = {
            "schema_version": "study-intake-dispatch-report-v1",
            "unit_sha256": selected["unit_sha256"],
            "subject": terminal["subject"],
            "capture_id": selected["producer_unit_id"],
            "release_id": terminal["release_id"],
            "package_ref": package_ref,
            "package_sha256": package_sha,
            "analysis": copy.deepcopy(package["analysis"]),
            "critical_review": copy.deepcopy(package["critical_review"]),
            "formal_write_count": 0,
        }
        report_json_sha, _report_path = self._publish_fixture_content(
            self.runtime / "dispatch/reports/json", report
        )
        report_json_ref = (
            "study-intake-report://sha256/" + report_json_sha
        )
        markdown = (
            f"# {terminal['subject']} Luna report\n\n"
            f"- capture_id: {selected['producer_unit_id']}\n"
            f"- unit_sha256: {selected['unit_sha256']}\n"
            f"- report_json_ref: {report_json_ref}\n"
            f"- package_ref: {package_ref}\n"
            "- formal_write_count: 0\n"
        ).encode("utf-8")
        report_markdown_sha, _markdown_path = self._publish_fixture_bytes(
            self.runtime / "dispatch/reports/markdown",
            markdown,
            suffix="md",
        )
        report_markdown_ref = (
            "study-intake-report-markdown://sha256/"
            + report_markdown_sha
        )

        receipt = self._read_verified_dispatch(
            Path(str(terminal.get("completion_receipt_path") or "")),
            digest=str(terminal.get("completion_receipt_sha256") or ""),
            purpose="dispatch-receipt",
        )
        receipt_core = copy.deepcopy(receipt)
        receipt_core.pop("authority", None)
        receipt_core.update(
            {
                "package_path": str(package_path),
                "package_ref": package_ref,
                "package_sha256": package_sha,
                "report_json_ref": report_json_ref,
                "report_json_sha256": report_json_sha,
                "report_markdown_ref": report_markdown_ref,
                "report_markdown_sha256": report_markdown_sha,
            }
        )
        receipt_v2 = self._seal_dispatch(
            receipt_core, purpose="dispatch-receipt"
        )
        receipt_sha, receipt_path = self._publish_fixture_content(
            self.runtime / "dispatch/receipts", receipt_v2
        )

        completion_path = Path(str(terminal.get("completion_path") or ""))
        completion = self._read_verified_dispatch(
            completion_path,
            digest=str(terminal.get("completion_sha256") or ""),
            purpose="dispatch-completion",
        )
        completion_core = copy.deepcopy(completion)
        completion_core.pop("authority", None)
        completion_core.update(
            {
                "receipt_sha256": receipt_sha,
                "receipt_path": str(receipt_path),
                "package_path": str(package_path),
                "package_ref": package_ref,
                "package_sha256": package_sha,
                "report_json_ref": report_json_ref,
                "report_json_sha256": report_json_sha,
                "report_markdown_ref": report_markdown_ref,
                "report_markdown_sha256": report_markdown_sha,
            }
        )
        completion_v2 = self._seal_dispatch(
            completion_core, purpose="dispatch-completion"
        )
        self._write_atomic_fixture(completion_path, completion_v2)
        completion_sha = hashlib.sha256(
            canonical(completion_v2) + b"\n"
        ).hexdigest()

        terminal.update(
            {
                "completion_path": str(completion_path),
                "completion_sha256": completion_sha,
                "completion_receipt_path": str(receipt_path),
                "completion_receipt_sha256": receipt_sha,
                "package_path": str(package_path),
                "package_ref": package_ref,
                "package_sha256": package_sha,
                "report_json_ref": report_json_ref,
                "report_json_sha256": report_json_sha,
                "report_markdown_ref": report_markdown_ref,
                "report_markdown_sha256": report_markdown_sha,
            }
        )

    def _mirror_current_canary_as_legacy_v2(self) -> None:
        """Project authenticated current v3 canary surfaces into v2 fixtures.

        The campaign-v2 publisher is intentionally a historical verifier.  Its
        tests must not make the production LeaseStore emit obsolete contracts,
        so this fixture verifies the current v3 chain first and then creates a
        separately signed v2 projection for that verifier.
        """

        terminal_rewrites: dict[str, str] = {}
        index_rewrites: dict[str, str] = {}
        any_v3 = False
        for subject in SUBJECTS:
            state_path = (
                self.runtime
                / "dispatch/state/production-canary"
                / f"{subject}.json"
            )
            state = self._read_verified_dispatch(
                state_path,
                digest=None,
                purpose="dispatch-production-canary-state",
            )
            if (
                state.get("schema_version")
                == "study-intake-production-canary-state-v2"
            ):
                index_rewrites[subject] = str(state["terminal_index_sha256"])
                continue
            if (
                state.get("schema_version")
                != "study-intake-production-canary-state-v3"
            ):
                raise publisher.CampaignPublishError(
                    "campaign_fixture_current_v3_invalid"
                )
            any_v3 = True
            index_path = Path(str(state.get("terminal_index_path") or ""))
            index = self._read_verified_dispatch(
                index_path,
                digest=str(state.get("terminal_index_sha256") or ""),
                purpose="dispatch-production-canary-terminal-index",
            )
            if (
                index.get("schema_version")
                != "study-intake-production-canary-terminal-index-v3"
                or int(index.get("terminal_by_outcome", {}).get("stalled", 0))
                != 0
            ):
                raise publisher.CampaignPublishError(
                    "campaign_fixture_current_v3_invalid"
                )

            units = copy.deepcopy(dict(index.get("units") or {}))
            for unit_sha256, entry in units.items():
                history = entry.get("history")
                if not isinstance(history, list) or not history:
                    raise publisher.CampaignPublishError(
                        "campaign_fixture_current_v3_invalid"
                    )
                converted_history: list[dict] = []
                prior_v2_sha: str | None = None
                for history_row in history:
                    old_sha = str(
                        history_row.get("terminal_receipt_sha256") or ""
                    )
                    old_path = Path(
                        str(history_row.get("terminal_receipt_path") or "")
                    )
                    terminal = self._read_verified_dispatch(
                        old_path,
                        digest=old_sha,
                        purpose="dispatch-production-canary-terminal",
                    )
                    if (
                        terminal.get("schema_version")
                        != "study-intake-production-canary-terminal-receipt-v3"
                        or terminal.get("outcome") == "stalled"
                    ):
                        raise publisher.CampaignPublishError(
                            "campaign_fixture_current_v3_invalid"
                        )
                    terminal_core = copy.deepcopy(terminal)
                    terminal_core.pop("authority", None)
                    terminal_core["schema_version"] = (
                        "study-intake-production-canary-terminal-receipt-v2"
                    )
                    if "prior_terminal_receipt_sha256" in terminal_core:
                        terminal_core["prior_terminal_receipt_sha256"] = (
                            prior_v2_sha
                        )
                    if terminal_core.get("outcome") == "succeeded":
                        self._mirror_success_chain_as_legacy_v2(
                            terminal_core
                        )
                    terminal_v2 = self._seal_dispatch(
                        terminal_core,
                        purpose="dispatch-production-canary-terminal",
                    )
                    new_sha, new_path = self._publish_fixture_content(
                        self.store.production_canary_receipt_root
                        / subject
                        / str(state["activation_id"]),
                        terminal_v2,
                    )
                    terminal_rewrites[old_sha] = new_sha
                    converted_row = copy.deepcopy(history_row)
                    converted_row["terminal_receipt_sha256"] = new_sha
                    converted_row["terminal_receipt_path"] = str(new_path)
                    converted_row["prior_terminal_receipt_sha256"] = (
                        prior_v2_sha
                    )
                    converted_history.append(converted_row)
                    prior_v2_sha = new_sha

                latest_row = converted_history[-1]
                entry["history"] = converted_history
                entry["terminal_receipt_sha256"] = latest_row[
                    "terminal_receipt_sha256"
                ]
                entry["terminal_receipt_path"] = latest_row[
                    "terminal_receipt_path"
                ]

                terminal_latest = self._read_verified_dispatch(
                    Path(str(entry["terminal_receipt_path"])),
                    digest=str(entry["terminal_receipt_sha256"]),
                    purpose="dispatch-production-canary-terminal",
                )
                for key in (
                    "package_ref",
                    "package_sha256",
                    "report_json_ref",
                    "report_json_sha256",
                    "report_markdown_ref",
                    "report_markdown_sha256",
                    "report_reopen_status",
                ):
                    entry[key] = terminal_latest.get(key)
                selected = terminal_latest.get("selected")
                if not isinstance(selected, dict):
                    raise publisher.CampaignPublishError(
                        "campaign_fixture_current_v3_invalid"
                    )
                queue_path = (
                    self.store.production_canary_queue_root
                    / subject
                    / str(state["activation_id"])
                    / (
                        str(selected["producer_input_contract_sha256"])
                        + ".json"
                    )
                )
                queue = self._read_verified_dispatch(
                    queue_path,
                    digest=None,
                    purpose="dispatch-production-canary-queue",
                )
                queue_core = copy.deepcopy(queue)
                queue_core.pop("authority", None)
                queue_core["terminal_receipt_sha256"] = entry[
                    "terminal_receipt_sha256"
                ]
                queue_core["terminal_receipt_path"] = entry[
                    "terminal_receipt_path"
                ]
                self._write_atomic_fixture(
                    queue_path,
                    self._seal_dispatch(
                        queue_core,
                        purpose="dispatch-production-canary-queue",
                    ),
                )

            counts = copy.deepcopy(dict(index["terminal_by_outcome"]))
            counts.pop("stalled", None)
            index_core = copy.deepcopy(index)
            index_core.pop("authority", None)
            index_core["schema_version"] = (
                "study-intake-production-canary-terminal-index-v2"
            )
            index_core["terminal_by_outcome"] = counts
            index_core["units"] = units
            index_v2 = self._seal_dispatch(
                index_core,
                purpose="dispatch-production-canary-terminal-index",
            )
            index_sha, index_v2_path = self._publish_fixture_content(
                self.store.production_canary_terminal_index_root
                / subject
                / str(state["activation_id"]),
                index_v2,
            )
            index_rewrites[subject] = index_sha

            state_core = copy.deepcopy(state)
            state_core.pop("authority", None)
            state_core["schema_version"] = (
                "study-intake-production-canary-state-v2"
            )
            state_core["terminal_index_sha256"] = index_sha
            state_core["terminal_index_path"] = str(index_v2_path)
            state_core["terminal_by_outcome"] = counts
            last_sha = state_core.get("last_terminal_receipt_sha256")
            if last_sha is not None:
                state_core["last_terminal_receipt_sha256"] = (
                    terminal_rewrites[str(last_sha)]
                )
                state_core["last_terminal_receipt_path"] = next(
                    entry["terminal_receipt_path"]
                    for entry in units.values()
                    if entry["terminal_receipt_sha256"]
                    == state_core["last_terminal_receipt_sha256"]
                )
            self._write_atomic_fixture(
                state_path,
                self._seal_dispatch(
                    state_core,
                    purpose="dispatch-production-canary-state",
                ),
            )

        if not any_v3:
            return
        telemetry_path = (
            self.runtime
            / "dispatch/state/production-canary-concurrency-telemetry.json"
        )
        telemetry = self._read_verified_dispatch(
            telemetry_path,
            digest=None,
            purpose="dispatch-production-canary-concurrency-telemetry",
        )
        telemetry_core = copy.deepcopy(telemetry)
        telemetry_core.pop("authority", None)
        telemetry_core["terminal_index_sha256_by_subject"] = index_rewrites
        for key in (
            "terminal_by_outcome_by_subject",
            "terminal_by_outcome_global",
        ):
            value = telemetry_core.get(key)
            if key.endswith("_by_subject"):
                for counts in value.values():
                    counts.pop("stalled", None)
            else:
                value.pop("stalled", None)
        for rows in telemetry_core[
            "terminal_failure_bindings_by_subject"
        ].values():
            for row in rows:
                old_sha = str(row["terminal_receipt_sha256"])
                row["terminal_receipt_sha256"] = terminal_rewrites[old_sha]
        self._write_atomic_fixture(
            telemetry_path,
            self._seal_dispatch(
                telemetry_core,
                purpose="dispatch-production-canary-concurrency-telemetry",
            ),
        )

    def build(self, *, scope: str = "zero_model_fixture_v2"):
        self._mirror_current_canary_as_legacy_v2()
        return publisher.build_projection(
            processing_runtime_root=self.runtime,
            dispatch_authority_key=self.dispatch_key,
            candidate_release_root=self.release_root,
            candidate_release_id=RELEASE_ID,
            deployment_authority_key=self.deployment_key,
            canary_activation_receipt=self.activation_receipt,
            evidence_scope=scope,
        )

    def run_subject(self, subject: str, index: int) -> None:
        task = canary_task(
            index,
            subject=subject,
            release_id=RELEASE_ID,
            recorded_at="2026-08-11T00:00:01Z",
        )
        queue_entry = self.store.materialize_production_canary_task(task)
        queue_path = (
            self.store.production_canary_queue_root
            / subject
            / self.states[subject]["activation_id"]
            / f"{queue_entry['producer_input_contract_sha256']}.json"
        )
        if not queue_path.is_file():
            raise AssertionError("activation-scoped queue fixture missing")
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: IdentityPublishingFixtureRunner(
                GroundedRunner(), LeaseStore(self.runtime)
            ),
            stage_timeout_seconds=3,
            production_canary=True,
        )
        result = dispatcher.submit(task).wait(5)
        if result.outcome != "succeeded":
            raise AssertionError(result.error_code)


class PublishConcurrencyCampaignV2Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = CampaignFixture()

    def tearDown(self) -> None:
        self.fixture.close()

    def test_async_armed_subjects_publish_test_scope_without_barrier(self) -> None:
        projection, _source, _formal = self.fixture.build()
        self.assertIsNone(campaign_contract_error(projection))
        self.assertEqual(projection["status"], "test_evidence_only")
        self.assertFalse(projection["current_release_usable"])
        self.assertEqual(projection["concurrency"]["global_peak_active"], 0)
        self.assertFalse(projection["concurrency"]["overlap_observed"])
        self.assertTrue(
            all(
                row["status"] == "awaiting_first_capture"
                for row in projection["subjects"].values()
            )
        )
        self.assertTrue(
            all(
                row["provider_execution_contract_status"]
                == "zero_model_fixture_not_applicable"
                and row["provider_stage_identity_sha256s"]
                == {"analysis": None, "critical_review": None}
                and row["provider_stage_exit_sha256s"]
                == {"analysis": None, "critical_review": None}
                and row["provider_executable_sha256"] is None
                and row["task_runner_executable_sha256"] is None
                for row in projection["subjects"].values()
            )
        )
        self.assertEqual(
            campaign_runtime_error(
                projection, expected_release_id=RELEASE_ID
            ),
            "campaign_evidence_scope_not_live",
        )

    def test_one_subject_success_reopens_complete_report_and_package(self) -> None:
        self.fixture.run_subject("math", 1)
        projection, source, formal = self.fixture.build()
        math = projection["subjects"]["math"]
        self.assertEqual(math["status"], "verified")
        self.assertEqual(math["package_status"], "reopen_verified")
        self.assertEqual(math["report_status"], "reopen_verified")
        self.assertEqual(
            math["report_reopen_status"],
            "json_markdown_package_verified",
        )
        self.assertEqual(
            math["report_json_ref"],
            "study-intake-report://sha256/"
            + math["report_json_sha256"],
        )
        self.assertEqual(
            math["report_markdown_ref"],
            "study-intake-report-markdown://sha256/"
            + math["report_markdown_sha256"],
        )
        self.assertEqual(
            math["package_ref"],
            "study-intake-dispatch-package://sha256/"
            + math["package_sha256"],
        )
        self.assertEqual(math["analysis_status"], "completed")
        self.assertEqual(math["critical_review_status"], "completed")
        self.assertEqual(
            math["provider_execution_contract_status"],
            "zero_model_fixture_not_applicable",
        )
        self.assertGreater(math["runtime_observed_mcp_tool_call_count"], 0)
        self.assertEqual(math["mcp_canonical_call_count"], 0)
        self.assertEqual(
            projection["subjects"]["cs408"]["status"],
            "awaiting_first_capture",
        )
        self.assertEqual(
            projection["subjects"]["english"]["status"],
            "awaiting_first_capture",
        )
        self.assertEqual(projection["concurrency"]["global_peak_active"], 1)
        evidence = publisher._publish_release_evidence_v2(
            self.fixture.evidence_root,
            projection=projection,
            source_binding=source,
            formal_guard=formal,
        )
        state_sha = publisher._publish_atomic(
            self.fixture.state_path, projection
        )
        self.assertEqual(
            hashlib.sha256(self.fixture.state_path.read_bytes()).hexdigest(),
            state_sha,
        )
        self.assertRegex(
            evidence["concurrency_receipt"]["sha256"], r"^[0-9a-f]{64}$"
        )

    def test_fixture_cannot_masquerade_as_real_production(self) -> None:
        self.fixture.run_subject("math", 2)
        with self.assertRaisesRegex(
            publisher.CampaignPublishError,
            "campaign_candidate_release_immutable_verification_failed",
        ):
            self.fixture.build(scope="real_production_hmac_v2")

    def test_no_fast_provider_identity_is_recomputed_and_fail_closed(
        self,
    ) -> None:
        expected = publisher._executable_binding_v2(
            Path("/bin/sh"), code="fixture_executable_invalid"
        )
        argv = [
            "/bin/sh",
            "exec",
            "--strict-config",
            "--ephemeral",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--model",
            "gpt-5.6-luna",
            "--config",
            'model_reasoning_effort="max"',
            "--json",
            "-",
        ]
        environment_key_names = ["PATH", "SAFE_FIXTURE"]
        identity = {
            "argv_policy_version": publisher.PROVIDER_ARGV_POLICY_VERSION,
            "argv": argv,
            "argv_sha256": hashlib.sha256(canonical(argv)).hexdigest(),
            "environment_policy_version": (
                publisher.PROVIDER_ENVIRONMENT_POLICY_VERSION
            ),
            "environment_key_names": environment_key_names,
            "environment_key_names_sha256": hashlib.sha256(
                canonical(environment_key_names)
            ).hexdigest(),
            "forbidden_environment_key_matches": [],
            "forbidden_environment_value_key_matches": [],
            "executable_path": expected["path"],
            "executable_sha256": expected["sha256"],
        }
        publisher._verify_no_fast_provider_identity_v2(
            identity, expected_executable=expected
        )

        tampered = dict(identity)
        tampered["argv_sha256"] = "f" * 64
        with self.assertRaisesRegex(
            publisher.CampaignPublishError,
            "campaign_provider_execution_contract_invalid",
        ):
            publisher._verify_no_fast_provider_identity_v2(
                tampered, expected_executable=expected
            )

        with self.assertRaisesRegex(
            publisher.CampaignPublishError,
            "campaign_provider_execution_contract_invalid",
        ):
            publisher._verify_no_fast_provider_identity_v2(
                identity,
                expected_executable={
                    "path": expected["path"],
                    "sha256": "e" * 64,
                },
            )

        priority = dict(identity)
        priority_argv = [*argv[:-1], "service_tier=priority", "-"]
        priority["argv"] = priority_argv
        priority["argv_sha256"] = hashlib.sha256(
            canonical(priority_argv)
        ).hexdigest()
        with self.assertRaisesRegex(
            publisher.CampaignPublishError,
            "campaign_provider_fast_mode_argv_forbidden",
        ):
            publisher._verify_no_fast_provider_identity_v2(
                priority, expected_executable=expected
            )

        forbidden_environment = dict(identity)
        forbidden_keys = ["PATH", "SERVICE-TIER"]
        forbidden_environment["environment_key_names"] = forbidden_keys
        forbidden_environment["environment_key_names_sha256"] = (
            hashlib.sha256(canonical(forbidden_keys)).hexdigest()
        )
        with self.assertRaisesRegex(
            publisher.CampaignPublishError,
            "campaign_provider_fast_mode_environment_forbidden",
        ):
            publisher._verify_no_fast_provider_identity_v2(
                forbidden_environment, expected_executable=expected
            )

    def test_real_scope_supervisor_must_match_release_task_runner(self) -> None:
        self.fixture.run_subject("math", 21)
        state = json.loads(
            (
                self.fixture.runtime
                / "dispatch/state/production-canary/math.json"
            ).read_text(encoding="utf-8")
        )
        terminal = json.loads(
            Path(state["last_terminal_receipt_path"]).read_text(
                encoding="utf-8"
            )
        )
        execution = terminal["process_execution"]
        selected = terminal["selected"]
        with self.assertRaisesRegex(
            publisher.CampaignPublishError,
            "campaign_task_runner_executable_binding_invalid",
        ):
            publisher._verify_task_process_closure_v2(
                self.fixture.runtime,
                self.fixture.dispatch_key.read_bytes(),
                subject="math",
                release_id=RELEASE_ID,
                unit_sha256=selected["unit_sha256"],
                frozen_payload_sha256=selected["frozen_payload_sha256"],
                identity_sha256=execution[
                    "supervisor_process_identity_sha256"
                ],
                identity_path=execution[
                    "supervisor_process_identity_path"
                ],
                exit_sha256=execution["supervisor_process_exit_sha256"],
                exit_path=execution["supervisor_process_exit_path"],
                process_execution=execution,
                require_real_provider_closure=False,
                require_release_task_runner=True,
                execution_contract={
                    "release_verified": True,
                    "provider_executable": None,
                    "task_runner_executable": {
                        "path": str(
                            self.fixture.release_root
                            / "bin/preprocess_task_runner.py"
                        ),
                        "sha256": "f" * 64,
                    },
                },
            )

    def test_two_stage_provider_closure_reopens_to_hash_only_public_proof(
        self,
    ) -> None:
        key = self.fixture.dispatch_key.read_bytes()
        unit_sha = "d" * 64
        frozen_sha = "e" * 64
        supervisor_sha = "f" * 64
        supervisor_path = str(self.fixture.runtime / "supervisor.json")
        context_root = str(self.fixture.runtime / "dispatch/contexts/task")
        capture_id = "POST-math-provider-fixture"
        owner_id = "dispatcher-100-" + "1" * 32
        executable = publisher._executable_binding_v2(
            Path("/bin/sh"), code="fixture_executable_invalid"
        )
        argv = [
            "/bin/sh",
            "exec",
            "--strict-config",
            "--ephemeral",
            "--ignore-user-config",
            "--ignore-rules",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "--model",
            "gpt-5.6-luna",
            "--config",
            'model_reasoning_effort="max"',
            "--json",
            "-",
        ]
        environment_keys = ["PATH", "SAFE_FIXTURE"]

        def publish_hmac(
            directory: str, purpose: str, core: dict
        ) -> tuple[str, str]:
            authority = {
                "schema_version": "study-intake-dispatch-authority-v1",
                "algorithm": "HMAC-SHA256",
                "key_id": hashlib.sha256(key).hexdigest(),
                "purpose": purpose,
                "hmac_sha256": hmac.new(
                    key,
                    canonical({"purpose": purpose, "payload": core}),
                    hashlib.sha256,
                ).hexdigest(),
            }
            value = {**core, "authority": authority}
            raw = canonical(value) + b"\n"
            digest = hashlib.sha256(raw).hexdigest()
            path = (
                self.fixture.runtime
                / "dispatch"
                / directory
                / "math"
                / unit_sha
                / "fence-1"
                / "sha256"
                / digest[:2]
                / f"{digest}.json"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            return digest, str(path)

        stages = {}
        for index, role in enumerate(
            ("analysis", "critical_review"), start=1
        ):
            stage_name = f"math_{role}"
            launched_at = f"2026-08-11T00:00:0{index}Z"
            finished_at = f"2026-08-11T00:00:1{index}Z"
            identity_core = {
                "schema_version": "study-intake-provider-process-identity-v1",
                "role": "codex_exec_provider_child",
                "stage_name": stage_name,
                "subject": "math",
                "release_id": RELEASE_ID,
                "unit_sha256": unit_sha,
                "frozen_payload_sha256": frozen_sha,
                "capture_id": capture_id,
                "owner_id": owner_id,
                "lease_fence": 1,
                "supervisor_process_identity_sha256": supervisor_sha,
                "supervisor_process_identity_path": supervisor_path,
                "supervisor_pid": 100,
                "supervisor_pgid": 100,
                "provider_pid": 200 + index,
                "provider_pgid": 200 + index,
                "process_start_token": (
                    f"darwin-libproc-bsdinfo-v1:{200 + index}:1:000001"
                ),
                "launch_nonce": f"{index:032x}",
                "launched_at": launched_at,
                "argv_policy_version": publisher.PROVIDER_ARGV_POLICY_VERSION,
                "argv": argv,
                "argv_sha256": hashlib.sha256(canonical(argv)).hexdigest(),
                "environment_policy_version": (
                    publisher.PROVIDER_ENVIRONMENT_POLICY_VERSION
                ),
                "environment_key_names": environment_keys,
                "environment_key_names_sha256": hashlib.sha256(
                    canonical(environment_keys)
                ).hexdigest(),
                "forbidden_environment_key_matches": [],
                "forbidden_environment_value_key_matches": [],
                "executable_path": executable["path"],
                "executable_sha256": executable["sha256"],
                "cwd": context_root,
                "context_root": context_root,
                "start_new_session": True,
                "provider_request_started": True,
                "requested_service_tier": None,
                "fast_mode_requested": False,
                "fast_mode_effective": "not_requested",
                "formal_write_count": 0,
                "sol_enabled": False,
            }
            identity_sha, identity_path = publish_hmac(
                "provider-process-identities",
                "dispatch-provider-process-identity",
                identity_core,
            )
            exit_core = {
                "schema_version": "study-intake-provider-process-exit-v1",
                "stage_name": stage_name,
                "subject": "math",
                "release_id": RELEASE_ID,
                "unit_sha256": unit_sha,
                "frozen_payload_sha256": frozen_sha,
                "capture_id": capture_id,
                "owner_id": owner_id,
                "lease_fence": 1,
                "provider_process_identity_sha256": identity_sha,
                "provider_process_identity_path": identity_path,
                "provider_pid": 200 + index,
                "provider_pgid": 200 + index,
                "process_start_token": (
                    f"darwin-libproc-bsdinfo-v1:{200 + index}:1:000001"
                ),
                "returncode": 0,
                "termination_reason": "completed",
                "reaped": True,
                "process_absent": True,
                "pgid_absent": True,
                "late_result_publish_allowed": True,
                "finished_at": finished_at,
                "formal_write_count": 0,
                "sol_enabled": False,
            }
            exit_sha, exit_path = publish_hmac(
                "provider-process-exits",
                "dispatch-provider-process-exit",
                exit_core,
            )
            stages[stage_name] = {
                "provider_process_identity_sha256": identity_sha,
                "provider_process_identity_path": identity_path,
                "provider_process_exit_sha256": exit_sha,
                "provider_process_exit_path": exit_path,
            }

        proof = publisher._verify_real_provider_closures_v2(
            self.fixture.runtime,
            key,
            subject="math",
            release_id=RELEASE_ID,
            unit_sha256=unit_sha,
            frozen_payload_sha256=frozen_sha,
            supervisor_identity_sha256=supervisor_sha,
            supervisor_identity={
                "owner_id": owner_id,
                "lease_fence": 1,
                "child_pid": 100,
                "child_pgid": 100,
                "context_root": context_root,
            },
            execution_contract={
                "release_verified": True,
                "provider_executable": executable,
                "task_runner_executable": None,
            },
            process_execution={
                "canonical_task_runner": True,
                "provider_process_closure_required": True,
                "supervisor_process_identity_path": supervisor_path,
                "provider_stages": stages,
            },
        )
        self.assertEqual(
            proof["status"],
            "verified_no_fast_mode_argv_environment",
        )
        self.assertEqual(
            set(proof["stage_identity_sha256s"]),
            {"analysis", "critical_review"},
        )
        self.assertNotIn("argv", proof)
        self.assertNotIn("environment_key_names", proof)
        self.assertNotIn("executable_path", proof)

    def test_hmac_terminal_index_tamper_fails_closed(self) -> None:
        self.fixture.run_subject("math", 3)
        state = json.loads(
            (
                self.fixture.runtime
                / "dispatch/state/production-canary/math.json"
            ).read_text(encoding="utf-8")
        )
        index_path = Path(state["terminal_index_path"])
        value = json.loads(index_path.read_text(encoding="utf-8"))
        value["terminal_task_count"] = 99
        index_path.chmod(0o600)
        index_path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaises(publisher.CampaignPublishError):
            self.fixture.build()

    def test_half_open_peak_recomputes_overlap_without_barrier(self) -> None:
        intervals = [
            {
                "subject": "math",
                "unit_sha256": "1" * 64,
                "started_at": "2026-08-11T00:00:00Z",
                "finished_at": "2026-08-11T00:00:02Z",
            },
            {
                "subject": "cs408",
                "unit_sha256": "2" * 64,
                "started_at": "2026-08-11T00:00:01Z",
                "finished_at": "2026-08-11T00:00:03Z",
            },
            {
                "subject": "english",
                "unit_sha256": "3" * 64,
                "started_at": "2026-08-11T00:00:02Z",
                "finished_at": "2026-08-11T00:00:04Z",
            },
        ]
        peak, overlap_subjects, subject_peaks = publisher._half_open_peak_v2(
            intervals, observed_at="2026-08-11T00:00:05Z"
        )
        self.assertEqual(peak, 2)
        self.assertEqual(overlap_subjects, sorted(SUBJECTS))
        self.assertEqual(
            subject_peaks, {"math": 1, "cs408": 1, "english": 1}
        )

    def test_three_fixture_supervisors_produce_recomputed_peak_three(self) -> None:
        release = threading.Event()
        entered = {subject: threading.Event() for subject in SUBJECTS}
        dispatcher = ConcurrentDispatcher(
            self.fixture.runtime,
            lambda task, _context: HoldingIdentityRunner(
                LeaseStore(self.fixture.runtime),
                entered[str(task.frozen_payload["subject"])],
                release,
            ),
            stage_timeout_seconds=6,
            production_canary=True,
        )
        handles = []
        for index, subject in enumerate(SUBJECTS, start=10):
            task = canary_task(
                index,
                subject=subject,
                release_id=RELEASE_ID,
                recorded_at="2026-08-11T00:00:01Z",
            )
            self.fixture.store.materialize_production_canary_task(task)
            handles.append(dispatcher.submit(task))
        self.assertTrue(all(event.wait(3) for event in entered.values()))
        release.set()
        self.assertTrue(
            all(handle.wait(8).outcome == "succeeded" for handle in handles)
        )
        projection, _source, _formal = self.fixture.build()
        self.assertGreaterEqual(
            projection["concurrency"]["global_peak_active"], 3
        )
        self.assertEqual(
            projection["concurrency"]["calculation_source"],
            "hmac_terminal_index_task_supervisor_lifecycle",
        )
        self.assertEqual(
            projection["concurrency"]["telemetry_cross_check"],
            "matched_non_authoritative",
        )

    def test_cli_is_v2_only_and_requires_explicit_scope(self) -> None:
        parser = publisher.build_parser()
        action_names = {action.dest for action in parser._actions}
        self.assertIn("evidence_scope", action_names)
        self.assertNotIn("summary_sha256", action_names)
        self.assertNotIn("canary_terminal_receipt", action_names)

    def test_v2_schema_topology_and_legacy_schema_bytes_are_locked(self) -> None:
        campaign_schema_path = (
            ROOT / "schemas/three-subject-concurrency-campaign-v2.json"
        )
        evidence_schema_path = (
            ROOT / "schemas/three-subject-concurrency-evidence-v2.json"
        )
        subject_schema_path = ROOT / "schemas/subject-canary-evidence-v2.json"
        source_schema_path = ROOT / "schemas/concurrency-source-binding-v2.json"
        legacy_schema_path = (
            ROOT / "schemas/three-subject-concurrency-evidence-v1.json"
        )
        campaign_schema = json.loads(
            campaign_schema_path.read_text(encoding="utf-8")
        )
        evidence_schema = json.loads(
            evidence_schema_path.read_text(encoding="utf-8")
        )
        subject_schema = json.loads(
            subject_schema_path.read_text(encoding="utf-8")
        )
        source_schema = json.loads(
            source_schema_path.read_text(encoding="utf-8")
        )
        self.assertEqual(
            campaign_schema["properties"]["schema_version"]["const"],
            CAMPAIGN_SCHEMA_VERSION,
        )
        self.assertEqual(set(campaign_schema["required"]), V2_TOP_LEVEL_KEYS)
        self.assertEqual(
            evidence_schema["properties"]["schema_version"]["const"],
            "study-intake-three-subject-concurrency-evidence-v2",
        )
        self.assertEqual(
            subject_schema["properties"]["schema_version"]["const"],
            "study-intake-subject-canary-evidence-v2",
        )
        self.assertEqual(
            source_schema["properties"]["schema_version"]["const"],
            "study-intake-concurrency-source-binding-v2",
        )
        campaign_subject_required = set(
            campaign_schema["$defs"]["subject"]["required"]
        )
        self.assertTrue(
            {
                "provider_execution_contract_status",
                "provider_stage_identity_sha256s",
                "provider_stage_exit_sha256s",
                "provider_executable_sha256",
                "task_runner_executable_sha256",
            }.issubset(campaign_subject_required)
        )
        self.assertIn(
            "provider_execution_contract_status",
            subject_schema["required"],
        )
        self.assertIn(
            "provider_execution_contract_status_by_subject",
            evidence_schema["required"],
        )
        source_subject_required = set(
            source_schema["$defs"]["subjectBinding"]["required"]
        )
        self.assertIn(
            "provider_stage_identity_sha256s",
            source_subject_required,
        )
        self.assertEqual(
            hashlib.sha256(legacy_schema_path.read_bytes()).hexdigest(),
            "338474f2fbc882f03bac1de095524b1f80eaad8faf7bafa85080e371d32efca3",
        )


if __name__ == "__main__":
    unittest.main()
