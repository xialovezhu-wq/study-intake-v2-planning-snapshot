from __future__ import annotations

import copy
import contextlib
import json
import hashlib
import io
import os
import plistlib
import stat
import subprocess
import sys
import tempfile
import unittest
import datetime as dt
from pathlib import Path
from typing import Any, Mapping
from typing import Callable
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import release_manager as release  # noqa: E402


LIVE_CONFIG = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/config.json"
)


def flatten(value: object, prefix: tuple[str, ...] = ()) -> dict[tuple[str, ...], object]:
    if isinstance(value, dict):
        result: dict[tuple[str, ...], object] = {}
        for key, nested in value.items():
            result.update(flatten(nested, prefix + (str(key),)))
        return result
    return {prefix: value}


MIGRATION_ALLOWED_PATHS = {
    ("worker", "poll_interval_seconds"),
    ("worker", "english_poll_interval_seconds"),
    ("worker", "max_jobs_per_scan"),
    ("model", "max_images"),
    ("model", "service_tier"),
    ("cs408_deep_v2", "controlled_contract_path"),
    ("cs408_deep_v2", "stage_timeout_seconds"),
    ("cs408_deep_v2", "soft_runtime_warning_seconds"),
    ("cs408_deep_v2", "stall_timeout_seconds"),
    ("cs408_deep_v2", "stall_probe_interval_seconds"),
    ("cs408_deep_v2", "stall_probe_required_consecutive_failures"),
    ("math_deep_v2", "max_chinese_chars"),
    ("math_deep_v2", "min_complete_chinese_chars"),
    ("math_deep_v2", "stage_timeout_seconds"),
    ("math_deep_v2", "soft_runtime_warning_seconds"),
    ("math_deep_v2", "stall_timeout_seconds"),
    ("math_deep_v2", "stall_probe_interval_seconds"),
    ("math_deep_v2", "stall_probe_required_consecutive_failures"),
    ("math_deep_v2", "gs111_cold_replay_max_seconds"),
    ("math_deep_v2", "three_replay_p95_max_seconds"),
    ("math_deep_v2", "mode"),
    ("math_deep_v2", "critical_review_output_schema"),
    ("math_deep_v2", "critical_review_prompt_version"),
    ("math_deep_v2", "package_output_schema"),
    ("adapters", "math", "enabled"),
    ("math_knowledge_snapshot", "enabled"),
    ("math_knowledge_snapshot", "max_source_bytes"),
    ("math_knowledge_snapshot", "max_distribution_terms"),
    ("math_knowledge_snapshot", "max_local_neighbors"),
    ("math_knowledge_snapshot", "max_relationship_candidates"),
    ("math_knowledge_snapshot", "max_snapshot_bytes"),
    ("math_knowledge_snapshot", "golden_regressions", "GS-111"),
    ("math_knowledge_snapshot", "sources", "graph"),
    ("math_knowledge_snapshot", "sources", "projection"),
    ("math_knowledge_snapshot", "sources", "taxonomy"),
    ("math_knowledge_snapshot", "sources", "relationship_policy"),
    ("cs408_knowledge_snapshot", "sources", "review_unit_mapping"),
    ("cs408_knowledge_snapshot", "controlled_aliases", "二分查找"),
    ("cs408_deep_v2", "package_output_schema"),
    ("dispatch", "authority_required"),
    ("dispatch", "heartbeat_interval_seconds"),
    ("dispatch", "lease_ttl_seconds"),
    ("dispatch", "infrastructure_recovery_attempts"),
    ("dispatch", "production_canary", "enabled"),
    ("dispatch", "production_canary", "status"),
    ("dispatch", "production_canary", "admission"),
    ("dispatch", "production_canary", "keep_backlog_drained"),
    ("dispatch", "production_canary", "post_activation_only"),
    ("dispatch", "production_canary", "initial_canary_inflight_limit"),
    ("dispatch", "production_canary", "continuous_concurrency_limit"),
    ("dashboard", "max_items_per_subject"),
    ("dashboard", "projection_schema_version"),
    ("english_two_pass_v1", "enabled"),
    ("english_two_pass_v1", "analysis_output_schema"),
    ("english_two_pass_v1", "critical_review_output_schema"),
    ("english_two_pass_v1", "package_output_schema"),
    ("english_two_pass_v1", "controlled_contract_path"),
    ("english_two_pass_v1", "analysis_prompt_version"),
    ("english_two_pass_v1", "critical_review_prompt_version"),
    ("english_two_pass_v1", "max_prompt_bytes"),
    ("english_two_pass_v1", "max_output_bytes"),
    ("english_two_pass_v1", "stage_timeout_seconds"),
}


class ReleaseManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="preprocessor-release-test-")
        self.base = Path(self.temp.name)
        self.source = self.base / "source"
        (self.source / "bin").mkdir(parents=True)
        (self.source / "bin" / "worker.py").write_text("print('ok')\n", encoding="utf-8")
        (self.source / "bin" / "preprocess_dispatcher.py").write_text(
            "print('fixture dispatcher')\n", encoding="utf-8"
        )
        (self.source / "state").mkdir()
        (self.source / "state" / "secret.json").write_text("secret\n", encoding="utf-8")
        (self.source / "schemas").mkdir()
        (self.source / "schemas" / "preprocess-package-v3.json").write_text(
            "{}\n", encoding="utf-8"
        )
        for role in release.REQUIRED_MODEL_CONTRACT["roles"].values():
            for key in ("agent_config", "tool_policy"):
                relative = Path(role[key])
                target = self.source / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((ROOT / relative).read_bytes())
        (self.source / "config.example.json").write_text(
            json.dumps({
                "execution_mode": "offline",
                "runtime_root": "${RUNTIME_DATA_ROOT}",
                "release": {"manifest_path": "${RELEASE_ROOT}/release.json"},
                "live_execution_gate": {
                    "enabled": True,
                    "default_locked": True,
                    "authorization_required": True,
                    "authorization_state_path": (
                        "${RUNTIME_DATA_ROOT}/dispatch/"
                        "manual-live-authorization-v1/state.json"
                    ),
                },
                "fixture_execution": {
                    "allowed_executable_roots": [
                        "${RELEASE_ROOT}/tests/fixtures"
                    ]
                },
                "branch_scheduler": {
                    "logical_branch_limit": None,
                    "physical_concurrency_mode": "dynamic",
                    "configured_maximum_active_branches": 8,
                    "overflow_policy": "queue_in_waves",
                    "drop_policy": "never",
                    "fairness_policy": "round_robin_tasks",
                    "stall_timeout_seconds": 120,
                },
                "models": {
                    role: {
                        "model": contract["model"],
                        "reasoning_effort": contract["reasoning_effort"],
                        "agents_enabled": contract["agents_enabled"],
                        "fresh_context": True,
                        "sandbox_mode": contract.get(
                            "sandbox_mode",
                            "workspace-write"
                            if role == "orchestrator"
                            else "read-only",
                        ),
                        "agent_config_path": (
                            "${RELEASE_ROOT}/" + contract["agent_config"]
                        ),
                        "tool_policy_path": (
                            "${RELEASE_ROOT}/" + contract["tool_policy"]
                        ),
                    }
                    for role, contract in release.REQUIRED_MODEL_CONTRACT[
                        "roles"
                    ].items()
                },
                "model": {
                    "model": "gpt-5.6-luna",
                    "reasoning_effort": "max",
                },
                "math_deep_v2": {
                    "package_output_schema": (
                        "${RELEASE_ROOT}/schemas/preprocess-package-v3.json"
                    )
                },
                "cs408_deep_v2": {
                    "package_output_schema": (
                        "${RELEASE_ROOT}/schemas/preprocess-package-v3.json"
                    )
                },
                "english_two_pass_v1": {
                    "package_output_schema": (
                        "${RELEASE_ROOT}/schemas/preprocess-package-v3.json"
                    )
                },
                "adapters": {
                    "math": {"enabled": True},
                    "cs408": {"enabled": True},
                    "english": {"enabled": True},
                },
                "dispatch": {
                    "production_canary": {
                        "enabled": True,
                        "status": "production_canary_active",
                        "admission": "first_post_activation_producer_capture",
                        "keep_backlog_drained": True,
                        "initial_canary_inflight_limit": 1,
                        "continuous_concurrency_limit": 20,
                        "post_activation_only": True,
                    }
                },
            }),
            encoding="utf-8",
        )
        self.release_base = self.base / "release-base"
        self.runtime_data = self.base / "runtime-data"
        self.launchagents = self.base / "LaunchAgents"
        self.launchagents.mkdir()
        (self.source / "launchagents").mkdir()
        for service in release.CONCURRENT_TOPOLOGY:
            payload = {
                "Label": service["label"],
                "ProgramArguments": [
                    "/usr/bin/true",
                    "${CURRENT_ROOT}",
                ],
                "WorkingDirectory": "${CURRENT_ROOT}",
                "RunAtLoad": True,
                "KeepAlive": True,
            }
            (self.source / str(service["template"])).write_bytes(
                plistlib.dumps(payload, sort_keys=True)
            )

    def tearDown(self) -> None:
        if self.base.exists():
            release._make_tree_removable(self.base)
        self.temp.cleanup()

    def test_successor_canary_reuses_authenticated_discovery_floor(self) -> None:
        previous_release_id = "a" * 64
        activated_at = "2026-08-15T12:26:18.469151+00:00"
        snapshots = {}
        for subject in release.RESUMABLE_SUBJECTS:
            gate = {
                "subject": subject,
                "release_id": previous_release_id,
                "status": "production_canary_active",
                "state": "armed",
                "activated_at": activated_at,
                "post_activation_only": True,
                "producer_capture_enabled": True,
                "luna_consumer_enabled": True,
                "active_task_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            }
            snapshots[subject] = {
                "existed": True,
                "value": release.canonical_bytes(gate),
            }
        observed = release._previous_canary_continuity_activated_at(
            snapshots,
            previous_release_id=previous_release_id,
        )
        self.assertEqual(observed.isoformat(), activated_at)

        quiescent = copy.deepcopy(snapshots)
        english = json.loads(quiescent["english"]["value"])
        english["state"] = "failed_drained"
        english["luna_consumer_enabled"] = False
        quiescent["english"]["value"] = release.canonical_bytes(english)
        observed = release._previous_canary_continuity_activated_at(
            quiescent,
            previous_release_id=previous_release_id,
        )
        self.assertEqual(observed.isoformat(), activated_at)

        invalid_quiescent = copy.deepcopy(quiescent)
        english = json.loads(invalid_quiescent["english"]["value"])
        english["luna_consumer_enabled"] = True
        invalid_quiescent["english"]["value"] = release.canonical_bytes(
            english
        )
        with self.assertRaisesRegex(
            release.ReleaseError, "previous_canary_continuity_invalid"
        ):
            release._previous_canary_continuity_activated_at(
                invalid_quiescent,
                previous_release_id=previous_release_id,
            )

        drifted = copy.deepcopy(snapshots)
        english = json.loads(drifted["english"]["value"])
        english["activated_at"] = "2026-08-15T12:27:00+00:00"
        drifted["english"]["value"] = release.canonical_bytes(english)
        with self.assertRaisesRegex(
            release.ReleaseError, "previous_canary_continuity_invalid"
        ):
            release._previous_canary_continuity_activated_at(
                drifted,
                previous_release_id=previous_release_id,
            )

    def test_historical_canary_activation_receipt_schemas_are_byte_stable(
        self,
    ) -> None:
        expected = {
            "three-subject-canary-activation-receipt-v2.json": (
                "58be4202884b1b0cd54609ae8c6b2a3e0c496212c833ffd3496975d482a24b83"
            ),
            "three-subject-canary-activation-receipt-v3.json": (
                "bdf101230906ad2f23909f6ededb605415e7bed361479ec546064cd26e257733"
            ),
            "three-subject-canary-activation-receipt-v4.json": (
                "77c99095729ec9e1a984f410782917bd3c5cfce5037ed5ba4f4084735e1a8003"
            ),
        }
        for filename, digest in expected.items():
            with self.subTest(filename=filename):
                self.assertEqual(
                    release.sha256_file(ROOT / "schemas" / filename), digest
                )
                if filename.endswith(("v3.json", "v4.json")):
                    self.assertEqual(
                        release.sha256_file(
                            ROOT
                            / "plugin"
                            / "kaoyan-study-intake"
                            / "schemas"
                            / filename
                        ),
                        digest,
                    )

    def test_cs408_terminal_retirement_descriptor_matches_runtime_exactly(
        self,
    ) -> None:
        sys.path.insert(0, str(ROOT / "lib"))
        import subject_sol_contract as subject_sol

        self.assertEqual(
            release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR,
            subject_sol.CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION,
        )
        self.assertEqual(
            release._authorization_descriptor_sha256(
                release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR
            ),
            subject_sol._value_sha256(
                subject_sol.CS408_TERMINAL_BATCH_RETIREMENT_AUTHORIZATION
            ),
        )
        self.assertEqual(
            release._authorization_descriptor_sha256(
                release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR
            ),
            "7539cbcaddfc8e6fa5755473a9e71db8a6916141cc40dee5b594221a2cd0a889",
        )

    def test_english_review_repair_descriptor_is_not_in_daily_cli(
        self,
    ) -> None:
        sys.path.insert(0, str(ROOT / "lib"))
        import concurrent_dispatch

        descriptor_sha256 = release._authorization_descriptor_sha256(
            concurrent_dispatch.ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
        )
        self.assertEqual(
            descriptor_sha256,
            release.ENGLISH_PRESERVED_REVIEW_REPAIR_DESCRIPTOR_SHA256,
        )
        self.assertEqual(
            release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_GATE_SHA256,
            "df28fd72271e9f5008285919fada76905ccddf0d6e59ca8b4e330d3c2af69fbe",
        )
        self.assertEqual(
            release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_WRITER_REVISION,
            4,
        )
        self.assertEqual(
            release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_AUTHORITY_GENERATION,
            "english-317672cabdaba8030c2b",
        )
        self.assertEqual(
            release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_AUTHORITY_FINGERPRINT,
            "317672cabdaba8030c2b2b36accaf5da35ee2d96efbdc0db48137d83d7b44f13",
        )
        root_parser = release.parser()
        subparsers = next(
            action
            for action in root_parser._actions
            if isinstance(getattr(action, "choices", None), dict)
            and "activate-canary" in action.choices
        )
        help_text = subparsers.choices["activate-canary"].format_help()
        self.assertNotIn(descriptor_sha256, "".join(help_text.split()))
        self.assertNotIn("english-preserved-review", help_text)

    def test_exact_20260813_english_preimage_reconstruction_is_byte_exact(
        self,
    ) -> None:
        key = b"k" * 32
        base = {
            "schema_version": "study-intake-production-canary-state-v3",
            "subject": "english",
            "release_id": "b" * 64,
            "state": "failed_drained",
            "status": "production_canary_active",
            "luna_consumer_enabled": False,
            "active_task_count": 0,
            "canary_queue_count": 1,
            "formal_write_count": 0,
            "sol_enabled": False,
            "oldest_pending_age_seconds": 10,
            "updated_at": "2026-08-13T00:00:00Z",
            "authority": {"old": True},
        }
        base_raw = release.canonical_bytes(base)
        core = copy.deepcopy(base)
        core.pop("authority")
        core["oldest_pending_age_seconds"] = 11
        core["updated_at"] = "2026-08-13T00:00:01Z"
        key_id = hashlib.sha256(key).hexdigest()
        mac = __import__("hmac").new(
            key,
            json.dumps(
                {
                    "purpose": "dispatch-production-canary-state",
                    "payload": core,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        core["authority"] = {
            "schema_version": "study-intake-dispatch-authority-v1",
            "algorithm": "HMAC-SHA256",
            "key_id": key_id,
            "purpose": "dispatch-production-canary-state",
            "hmac_sha256": mac,
        }
        expected_raw = release.canonical_bytes(core)
        descriptor = copy.deepcopy(
            release.EXACT_20260813_FENCED_TRANSACTION_RECOVERY
        )
        descriptor["previous_release_id"] = "b" * 64
        descriptor["english_reconstruction"] = {
            "base_preimage_sha256": hashlib.sha256(base_raw).hexdigest(),
            "oldest_pending_age_seconds": 11,
            "updated_at": "2026-08-13T00:00:01Z",
            "authority_key_id": key_id,
            "authority_hmac_sha256": mac,
            "physical_sha256": hashlib.sha256(expected_raw).hexdigest(),
        }
        with mock.patch.object(
            release,
            "EXACT_20260813_FENCED_TRANSACTION_RECOVERY",
            descriptor,
        ):
            self.assertEqual(
                release._exact_20260813_reconstruct_english_canary_state(
                    base_preimage=base_raw, authority_key=key
                ),
                expected_raw,
            )
            with self.assertRaisesRegex(
                release.ReleaseError,
                "exact_20260813_english_preimage_invalid",
            ):
                release._exact_20260813_reconstruct_english_canary_state(
                    base_preimage=base_raw + b" ", authority_key=key
                )

    def test_exact_20260813_fenced_target_rejects_current_or_service_drift(
        self,
    ) -> None:
        target_id = "a" * 64
        target = self.base / "releases" / target_id
        target.mkdir(parents=True)
        target = target.resolve()
        active_link = self.base / "current"
        active_link.symlink_to(target)
        topology = [
            {"name": name, "verify": ["launchctl", "print", name]}
            for name in ("math", "cs408", "english", "dashboard")
        ]
        snapshot = {
            "schema_version": release.PROCESS_SNAPSHOT_SCHEMA,
            "observed_at": "2026-08-13T00:00:00+00:00",
            "legacy_worker_pids": [],
            "dispatcher_pids": [],
            "dashboard_pids": [],
            "luna_pids": [],
            "process_set_sha256": "f" * 64,
        }

        def stopped(_command: object) -> subprocess.CompletedProcess[str]:
            return subprocess.CompletedProcess([], 113, "", "")

        self.assertEqual(
            release._exact_20260813_require_fenced_target(
                active_link=active_link,
                target=target,
                target_release_id=target_id,
                target_topology=topology,
                runner=stopped,
                process_inspector=lambda *_args: snapshot,
                port_owner_inspector=lambda: [],
            ),
            snapshot,
        )
        active_link.unlink()
        other = self.base / "releases" / ("b" * 64)
        other.mkdir()
        active_link.symlink_to(other)
        with self.assertRaisesRegex(
            release.ReleaseError, "exact_20260813_current_drift"
        ):
            release._exact_20260813_require_fenced_target(
                active_link=active_link,
                target=target,
                target_release_id=target_id,
                target_topology=topology,
                runner=stopped,
                process_inspector=lambda *_args: snapshot,
                port_owner_inspector=lambda: [],
            )
        active_link.unlink()
        active_link.symlink_to(target)
        with self.assertRaisesRegex(
            release.ReleaseError, "exact_20260813_services_not_stopped"
        ):
            release._exact_20260813_require_fenced_target(
                active_link=active_link,
                target=target,
                target_release_id=target_id,
                target_topology=topology,
                runner=lambda _command: subprocess.CompletedProcess(
                    [], 0, "", ""
                ),
                process_inspector=lambda *_args: snapshot,
                port_owner_inspector=lambda: [],
            )

    def test_exact_20260813_file_binding_rejects_drift_and_symlink(
        self,
    ) -> None:
        path = self.base / "exact.json"
        raw = release.canonical_bytes({"exact": True})
        path.write_bytes(raw)
        digest = hashlib.sha256(raw).hexdigest()
        self.assertEqual(
            release._exact_20260813_json(
                path,
                expected_sha256=digest,
                error_code="exact_fixture_invalid",
            ),
            {"exact": True},
        )
        with self.assertRaisesRegex(
            release.ReleaseError, "exact_fixture_invalid"
        ):
            release._exact_20260813_json(
                path,
                expected_sha256="0" * 64,
                error_code="exact_fixture_invalid",
            )
        link = self.base / "exact-link.json"
        link.symlink_to(path)
        with self.assertRaisesRegex(
            release.ReleaseError, "exact_fixture_invalid"
        ):
            release._exact_20260813_json(
                link,
                expected_sha256=digest,
                error_code="exact_fixture_invalid",
            )

    def test_exact_20260813_ordinary_config_accepts_only_bound_mixed_state(
        self,
    ) -> None:
        path = self.base / "config.toml"
        exact_preimage = (
            b'[mcp_servers.kaoyan_read]\ncommand = "/bin/true"\n\n'
            b'[other]\nvalue = "preimage"\n'
        )
        _start, _end, mcp_profile = release._ordinary_mcp_profile_span(
            exact_preimage
        )
        exact_preimage_sha256 = hashlib.sha256(exact_preimage).hexdigest()
        mcp_profile_sha256 = hashlib.sha256(mcp_profile).hexdigest()
        target_postimage = exact_preimage.replace(b"preimage", b"target")
        target_postimage_sha256 = hashlib.sha256(
            target_postimage
        ).hexdigest()

        path.write_bytes(target_postimage)
        self.assertEqual(
            release._exact_20260813_validate_ordinary_config_postimage(
                path,
                target_postimage_sha256=target_postimage_sha256,
                exact_preimage_sha256=exact_preimage_sha256,
                exact_preimage_mcp_profile_sha256=mcp_profile_sha256,
            ),
            "target_postimage",
        )
        path.write_bytes(exact_preimage)
        self.assertEqual(
            release._exact_20260813_validate_ordinary_config_postimage(
                path,
                target_postimage_sha256=target_postimage_sha256,
                exact_preimage_sha256=exact_preimage_sha256,
                exact_preimage_mcp_profile_sha256=mcp_profile_sha256,
            ),
            "exact_preimage",
        )
        with self.assertRaisesRegex(
            release.ReleaseError,
            "exact_20260813_external_postimage_drift",
        ):
            release._exact_20260813_validate_ordinary_config_postimage(
                path,
                target_postimage_sha256=target_postimage_sha256,
                exact_preimage_sha256=exact_preimage_sha256,
                exact_preimage_mcp_profile_sha256="0" * 64,
            )
        path.write_bytes(exact_preimage + b"# third state\n")
        with self.assertRaisesRegex(
            release.ReleaseError,
            "exact_20260813_external_postimage_drift",
        ):
            release._exact_20260813_validate_ordinary_config_postimage(
                path,
                target_postimage_sha256=target_postimage_sha256,
                exact_preimage_sha256=exact_preimage_sha256,
                exact_preimage_mcp_profile_sha256=mcp_profile_sha256,
            )

    def test_exact_20260813_prepare_must_remain_ledger_head(self) -> None:
        prepare_sha256 = "a" * 64
        prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "prepared_at": "2026-08-13T09:27:44.351711+00:00",
        }
        older = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "prepared_at": "2026-08-13T08:45:22.893475+00:00",
        }
        receipts = {prepare_sha256: prepare, "b" * 64: older}
        release._exact_20260813_require_last_prepare(
            receipts,
            prepare_sha256=prepare_sha256,
            prepare=prepare,
        )
        receipts["c" * 64] = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "prepared_at": "2026-08-13T09:28:00+00:00",
        }
        with self.assertRaisesRegex(
            release.ReleaseError,
            "exact_20260813_later_deployment_transaction",
        ):
            release._exact_20260813_require_last_prepare(
                receipts,
                prepare_sha256=prepare_sha256,
                prepare=prepare,
            )

    def _exact_20260813_recovery_fixture(
        self,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        descriptor = copy.deepcopy(
            release.EXACT_20260813_FENCED_TRANSACTION_RECOVERY
        )
        target_id = "a" * 64
        previous_id = "b" * 64
        release_base = self.base / "exact-release-base"
        target = release_base / "releases" / target_id
        previous = release_base / "releases" / previous_id
        target.mkdir(parents=True)
        previous.mkdir()
        descriptor.update(
            {
                "release_base": str(release_base),
                "active_link": str(release_base / "current"),
                "target_release_id": target_id,
                "previous_release_id": previous_id,
            }
        )
        state_root = self.base / "exact-canary"
        state_root.mkdir()
        canary_snapshots: dict[str, dict[str, Any]] = {}
        previous_hashes: dict[str, str] = {}
        for subject in release.RESUMABLE_SUBJECTS:
            raw = release.canonical_bytes({"subject": subject, "old": True})
            digest = hashlib.sha256(raw).hexdigest()
            previous_hashes[subject] = digest
            canary_snapshots[subject] = {
                "path": str(state_root / f"{subject}.json"),
                "existed": True,
                "value": raw,
                "mode": 0o600,
                "sha256": digest,
            }
        descriptor["previous_canary_state_sha256s"] = previous_hashes
        english_recovery = {
            "recovery_receipt_sha256": descriptor[
                "english_recovery_receipt_sha256"
            ],
            "recovery_receipt_path": descriptor[
                "english_recovery_receipt_path"
            ],
            "rollback_token": "c" * 64,
        }
        retirement = {
            "retirement_receipt_sha256": descriptor[
                "cs408_retirement_receipt_sha256"
            ],
            "retirement_receipt_path": descriptor[
                "cs408_retirement_receipt_path"
            ],
            "rollback_token": "d" * 64,
        }
        context = {
            "release_base": release_base,
            "runtime_root": release_base,
            "active_link": release_base / "current",
            "target": target,
            "previous": previous,
            "prepare": {"release_id": target_id},
            "failed_postcommit": {
                "drain_error_code": "canary_activation_failure_gate_invalid:math"
            },
            "target_topology": [],
            "previous_topology": [
                {
                    "name": name,
                    "label": f"fixture.{name}",
                    "disable": ["disable", name],
                    "bootout": ["bootout", name],
                    "enable": ["enable", name],
                    "bootstrap": ["bootstrap", name],
                    "verify": ["verify", name],
                }
                for name in ("math", "cs408", "english", "dashboard")
            ],
            "stopped_process_snapshot": {},
            "english_recovery": english_recovery,
            "cs408_retirement": retirement,
            "accounting": {"formal_write_count": 0},
            "canary_snapshots": canary_snapshots,
            "launchagent_inventory": {},
            "launchagent_backup_root": release_base / "launchagent-backup",
            "external_inventory": {},
            "external_backup_root": release_base / "external-backup",
        }
        return descriptor, context

    def test_exact_20260813_recovery_orders_compensation_and_closes_gate(
        self,
    ) -> None:
        descriptor, context = self._exact_20260813_recovery_fixture()
        events: list[str] = []
        english_rollback = {
            "canary_state_restored_sha256": "1" * 64,
            "rollback_receipt_sha256": "2" * 64,
        }
        cs408_rollback = {
            "rollback_receipt_sha256": "3" * 64,
        }

        def rollback_english(**_values: object) -> dict[str, Any]:
            events.append("english")
            return english_rollback

        def rollback_cs408(**_values: object) -> dict[str, Any]:
            events.append("cs408")
            return cs408_rollback

        written: dict[str, Any] = {}
        service_commands: list[tuple[str, ...]] = []

        def service_runner(
            command: object,
        ) -> subprocess.CompletedProcess[str]:
            row = tuple(str(item) for item in command)  # type: ignore[arg-type]
            service_commands.append(row)
            return subprocess.CompletedProcess(row, 0, "", "")

        def write_receipt(
            _base: Path, value: Mapping[str, Any]
        ) -> str:
            written.update(copy.deepcopy(dict(value)))
            return "4" * 64

        with (
            mock.patch.object(
                release,
                "EXACT_20260813_FENCED_TRANSACTION_RECOVERY",
                descriptor,
            ),
            mock.patch.object(
                release, "_exact_20260813_preflight", return_value=context
            ),
            mock.patch.object(
                release,
                "_rollback_subject_batch_recovery",
                side_effect=rollback_english,
            ),
            mock.patch.object(
                release,
                "_rollback_cs408_terminal_batch_retirement",
                side_effect=rollback_cs408,
            ),
            mock.patch.object(
                release,
                "_reopen_subject_batch_recovery_rollback",
                return_value={
                    "deployment_canary_state_postimage_sha256": descriptor[
                        "previous_canary_state_sha256s"
                    ]["english"]
                },
            ),
            mock.patch.object(
                release,
                "_reopen_cs408_terminal_batch_retirement_rollback",
                return_value={"rollback_receipt_reopened": True},
            ),
            mock.patch.object(release, "_restore_launchagent_plists"),
            mock.patch.object(
                release,
                "_restore_external_profiles",
                return_value={"status": "restored"},
            ),
            mock.patch.object(release, "_switch_link"),
            mock.patch.object(
                release, "_deployment_receipt_objects", return_value={}
            ),
            mock.patch.object(
                release,
                "_cs408_retirement_postcommit_closes_prepare",
                return_value=True,
            ),
            mock.patch.object(
                release, "_write_deployment_receipt", side_effect=write_receipt
            ),
            mock.patch.object(
                release,
                "_assert_no_unresolved_deployment_transaction",
                side_effect=AssertionError("no check may follow closure write"),
            ) as assert_gate,
        ):
            result = release.recover_exact_20260813_fenced_transaction(
                runner=service_runner
            )
        assert_gate.assert_not_called()
        self.assertEqual(events, ["english", "cs408"])
        self.assertEqual(
            service_commands,
            [
                ("disable", "math"),
                ("bootout", "math"),
                ("disable", "cs408"),
                ("bootout", "cs408"),
                ("disable", "english"),
                ("bootout", "english"),
                ("enable", "dashboard"),
                ("bootstrap", "dashboard"),
                ("verify", "dashboard"),
            ],
        )
        self.assertEqual(result["status"], "rolled_back")
        self.assertEqual(result["deployment_transaction_gate"], "passed")
        self.assertEqual(written["status"], "rolled_back")
        self.assertEqual(written["rollback_errors"], [])
        self.assertEqual(
            written["restored_paused_services"],
            ["cs408", "english", "math"],
        )
        self.assertEqual(
            written["previous_canary_state_restore_proofs"]["english"][
                "restored_sha256"
            ],
            descriptor["previous_canary_state_sha256s"]["english"],
        )

    def test_exact_20260813_recovery_failure_keeps_target_fenced_without_closure(
        self,
    ) -> None:
        descriptor, context = self._exact_20260813_recovery_fixture()
        with (
            mock.patch.object(
                release,
                "EXACT_20260813_FENCED_TRANSACTION_RECOVERY",
                descriptor,
            ),
            mock.patch.object(
                release, "_exact_20260813_preflight", return_value=context
            ),
            mock.patch.object(
                release,
                "_rollback_subject_batch_recovery",
                side_effect=release.ReleaseError("injected"),
            ),
            mock.patch.object(
                release, "_run_service_deactivation_best_effort"
            ) as deactivate,
            mock.patch.object(release, "_switch_link") as switch,
            mock.patch.object(
                release, "_write_deployment_receipt"
            ) as write_receipt,
        ):
            with self.assertRaisesRegex(
                release.ReleaseError,
                "exact_20260813_recovery_incomplete_target_fenced",
            ):
                release.recover_exact_20260813_fenced_transaction(
                    runner=lambda _command: subprocess.CompletedProcess(
                        [], 0, "", ""
                    )
                )
        deactivate.assert_called_once()
        switch.assert_called_once_with(
            Path(descriptor["active_link"]),
            Path(descriptor["release_base"])
            / "releases"
            / descriptor["target_release_id"],
            "b4d2cd209f5b75a7-exact-fenced",
        )
        write_receipt.assert_not_called()

    def test_exact_20260813_recovery_cli_is_removed(
        self,
    ) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            release.parser().parse_args(
                ["recover-exact-20260813-b4d2-fenced-transaction"]
            )

    def test_exact_20260814_forward_cli_is_removed(
        self,
    ) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            release.parser().parse_args(
                ["recover-exact-20260814-03a5-6043-forward-transaction"]
            )

    def test_exact_20260814_9223_closure_is_hard_bound_and_queue_stable(
        self,
    ) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            release.parser().parse_args(
                ["close-exact-20260814-9223-cbc6-fenced-transaction"]
            )
        descriptor = release.EXACT_20260814_9223_CBC6_FENCED_CLOSURE
        prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "operation": "activate",
            "release_id": descriptor["failed_target_release_id"],
            "expected_current": descriptor["source_release_id"],
            "observed_current": descriptor["source_release_id"],
            "canary_manifest_sha256": descriptor["canary_manifest_sha256"],
            "math_pending_queue_migration_authorization": {
                "migration_intent_sha256": descriptor[
                    "migration_intent_sha256"
                ]
            },
            "formal_write_count": 0,
        }
        failed = {
            "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
            "status": "rollback_incomplete_cancel_or_fence_failed",
            "release_id": descriptor["failed_target_release_id"],
            "prepare_receipt_sha256": descriptor["prepare_receipt_sha256"],
            "error_code": "three_subject_canary_arm_failed:math",
            "drain_error_code": "canary_activation_failure_gate_invalid:math",
            "formal_write_count": 0,
        }
        proof = {
            "schema_version": (
                "study-intake-exact-fenced-math-migration-closure-v1"
            ),
            "descriptor_sha256": release.sha256_bytes(
                release.canonical_bytes(descriptor)
            ),
            "prepare_receipt_sha256": descriptor["prepare_receipt_sha256"],
            "failed_postcommit_receipt_sha256": descriptor[
                "failed_postcommit_receipt_sha256"
            ],
            "failed_target_release_id": descriptor[
                "failed_target_release_id"
            ],
            "source_release_id": descriptor["source_release_id"],
            "migration_intent_sha256": descriptor["migration_intent_sha256"],
            "control_release_id": "c" * 64,
            "current_before": descriptor["failed_target_release_id"],
            "current_after": descriptor["source_release_id"],
            "math_gate_state_after": "inactive_rolled_back",
            "math_gate_sha256_after": "d" * 64,
            "source_queue_sha256s": dict(descriptor["source_queue_sha256s"]),
            "source_queue_count": 4,
            "old_queue_bytes_unchanged": True,
            "target_queue_count": 0,
            "capture_write_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "sol_call_count": 0,
            "formal_write_count": 0,
            "authority": {},
        }
        closure = {
            "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
            "operation": "activate",
            "release_id": descriptor["failed_target_release_id"],
            "status": "closed_fenced_math_migration_source_restored",
            "prepare_receipt_sha256": descriptor["prepare_receipt_sha256"],
            "failed_postcommit_receipt_sha256": descriptor[
                "failed_postcommit_receipt_sha256"
            ],
            "restored_current_release_id": descriptor["source_release_id"],
            "exact_fenced_math_migration_closure": proof,
            "formal_write_count": 0,
        }
        receipts = {
            descriptor["failed_postcommit_receipt_sha256"]: failed,
        }
        with mock.patch.object(release, "_verify_deployment_authority"):
            self.assertTrue(
                release._exact_20260814_9223_cbc6_fenced_closes_prepare(
                    prepare_receipt_sha256=descriptor[
                        "prepare_receipt_sha256"
                    ],
                    prepare=prepare,
                    postcommit=closure,
                    receipts=receipts,
                    release_base=self.base,
                )
            )
            changed = copy.deepcopy(closure)
            changed["exact_fenced_math_migration_closure"][
                "source_queue_sha256s"
            ] = {
                **descriptor["source_queue_sha256s"],
                next(iter(descriptor["source_queue_sha256s"])): "e" * 64,
            }
            self.assertFalse(
                release._exact_20260814_9223_cbc6_fenced_closes_prepare(
                    prepare_receipt_sha256=descriptor[
                        "prepare_receipt_sha256"
                    ],
                    prepare=prepare,
                    postcommit=changed,
                    receipts=receipts,
                    release_base=self.base,
                )
            )

    def test_exact_20260815_fenced_closure_preserves_four_unclaimed_queues(
        self,
    ) -> None:
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            release.parser().parse_args(
                ["close-exact-20260815-07f5-530f-fenced-transaction"]
            )
        descriptor = release.EXACT_20260815_07F5_530F_FENCED_CLOSURE
        prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "operation": "activate",
            "release_id": descriptor["target_release_id"],
            "expected_current": descriptor["source_release_id"],
            "observed_current": descriptor["source_release_id"],
            "canary_manifest_sha256": descriptor["canary_manifest_sha256"],
            "math_pending_queue_migration_authorization": {
                "migration_intent_sha256": descriptor[
                    "migration_intent_sha256"
                ]
            },
            "formal_write_count": 0,
        }
        failed = {
            "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
            "status": "rollback_incomplete_cancel_or_fence_failed",
            "release_id": descriptor["target_release_id"],
            "prepare_receipt_sha256": descriptor["prepare_receipt_sha256"],
            "error_code": "three_subject_canary_heartbeat_missing",
            "formal_write_count": 0,
        }
        proof = {
            "schema_version": (
                "study-intake-exact-fenced-math-queue-preservation-v1"
            ),
            "descriptor_sha256": release.sha256_bytes(
                release.canonical_bytes(descriptor)
            ),
            "target_release_id": descriptor["target_release_id"],
            "migration_commit_sha256": descriptor[
                "migration_commit_sha256"
            ],
            "target_queue_sha256s": dict(descriptor["target_queue_sha256s"]),
            "observed_gate_sha256s": {
                subject: str(index) * 64
                for index, subject in enumerate(
                    release.RESUMABLE_SUBJECTS, start=1
                )
            },
            "queue_count": 4,
            "queues_pending_unclaimed": True,
            "services_stopped": True,
            "target_preserved": True,
            "capture_write_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "sol_call_count": 0,
            "formal_write_count": 0,
            "authority": {},
        }
        closure = {
            "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
            "operation": "activate",
            "release_id": descriptor["target_release_id"],
            "status": "closed_fenced_math_queues_preserved_for_resume",
            "prepare_receipt_sha256": descriptor["prepare_receipt_sha256"],
            "failed_postcommit_receipt_sha256": descriptor[
                "failed_postcommit_receipt_sha256"
            ],
            "exact_fenced_math_queue_preservation": proof,
            "formal_write_count": 0,
        }
        with mock.patch.object(release, "_verify_deployment_authority"):
            self.assertTrue(
                release._exact_20260815_07f5_530f_fenced_closes_prepare(
                    prepare_receipt_sha256=descriptor[
                        "prepare_receipt_sha256"
                    ],
                    prepare=prepare,
                    postcommit=closure,
                    receipts={
                        descriptor["failed_postcommit_receipt_sha256"]: failed
                    },
                    release_base=self.base,
                )
            )
            changed = copy.deepcopy(closure)
            changed["exact_fenced_math_queue_preservation"][
                "queues_pending_unclaimed"
            ] = False
            self.assertFalse(
                release._exact_20260815_07f5_530f_fenced_closes_prepare(
                    prepare_receipt_sha256=descriptor[
                        "prepare_receipt_sha256"
                    ],
                    prepare=prepare,
                    postcommit=changed,
                    receipts={
                        descriptor["failed_postcommit_receipt_sha256"]: failed
                    },
                    release_base=self.base,
                )
            )

    def test_exact_20260816_9ab6_closure_is_hard_bound(self) -> None:
        for command in (
            "close-exact-20260816-9ab6-fenced-transaction",
            "close-exact-20260816-5a39-fenced-transaction",
        ):
            with self.subTest(command=command), contextlib.redirect_stderr(
                io.StringIO()
            ), self.assertRaises(SystemExit):
                release.parser().parse_args([command])
        descriptor = release.EXACT_20260816_9AB6_FENCED_CLOSURE
        prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "operation": "activate",
            "release_id": descriptor["target_release_id"],
            "expected_current": descriptor["source_release_id"],
            "observed_current": descriptor["source_release_id"],
            "canary_manifest_sha256": descriptor["canary_manifest_sha256"],
            "formal_write_count": 0,
        }
        failed = {
            "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
            "status": "rollback_incomplete_cancel_or_fence_failed",
            "release_id": descriptor["target_release_id"],
            "prepare_receipt_sha256": descriptor["prepare_receipt_sha256"],
            "error_code": "three_subject_canary_arm_failed:math",
            "drain_error_code": "canary_activation_failure_status_invalid:cs408",
            "formal_write_count": 0,
        }
        proof = {
            "schema_version": "study-intake-exact-20260816-9ab6-fenced-source-restore-v1",
            "descriptor_sha256": release.sha256_bytes(
                release.canonical_bytes(descriptor)
            ),
            "prepare_receipt_sha256": descriptor["prepare_receipt_sha256"],
            "failed_postcommit_receipt_sha256": descriptor[
                "failed_postcommit_receipt_sha256"
            ],
            "current_before": descriptor["target_release_id"],
            "current_after": descriptor["source_release_id"],
            "target_math_gate_sha256_before": descriptor[
                "target_math_gate_sha256"
            ],
            "source_gate_sha256s_before": dict(
                descriptor["source_gate_sha256s"]
            ),
            "old_math_activation_receipt_sha256": descriptor[
                "old_math_activation_receipt_sha256"
            ],
            "old_math_activation_id": descriptor["old_math_activation_id"],
            "old_math_activated_at": descriptor["old_math_activated_at"],
            "old_math_producer_authority_fingerprint": descriptor[
                "old_math_producer_authority_fingerprint"
            ],
            "old_math_high_watermark_sha256": descriptor[
                "old_math_high_watermark_sha256"
            ],
            "math_gate_sha256_after": "d" * 64,
            "telemetry_sha256_after": "e" * 64,
            "source_gate_count": 2,
            "old_capture_queue_bytes_unchanged": True,
            "queue_write_count": 0,
            "capture_write_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "sol_call_count": 0,
            "formal_write_count": 0,
            "authority": {},
        }
        closure = {
            "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
            "operation": "activate",
            "release_id": descriptor["target_release_id"],
            "status": "closed_fenced_source_restored",
            "prepare_receipt_sha256": descriptor["prepare_receipt_sha256"],
            "failed_postcommit_receipt_sha256": descriptor[
                "failed_postcommit_receipt_sha256"
            ],
            "restored_current_release_id": descriptor["source_release_id"],
            "exact_20260816_fenced_source_restore_closure": proof,
            "formal_write_count": 0,
        }
        activation = {
            "schema_version": "study-intake-production-canary-activation-receipt-v2",
            "subject": "math",
            "release_id": descriptor["source_release_id"],
            "activation_id": descriptor["old_math_activation_id"],
            "activated_at": descriptor["old_math_activated_at"],
            "producer_authority_fingerprint": descriptor[
                "old_math_producer_authority_fingerprint"
            ],
            "producer_high_watermark_sha256": descriptor[
                "old_math_high_watermark_sha256"
            ],
            "formal_write_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "sol_enabled": False,
        }
        with (
            mock.patch.object(release, "_verify_deployment_authority"),
            mock.patch.object(release, "_verify_recovery_v2_dispatch_authority"),
            mock.patch.object(release, "_exact_20260813_json", return_value=activation),
        ):
            receipts = {descriptor["failed_postcommit_receipt_sha256"]: failed}
            self.assertTrue(
                release._exact_20260816_9ab6_fenced_closes_prepare(
                    prepare_receipt_sha256=descriptor["prepare_receipt_sha256"],
                    prepare=prepare,
                    postcommit=closure,
                    receipts=receipts,
                    release_base=self.base,
                )
            )
            changed = copy.deepcopy(closure)
            changed["exact_20260816_fenced_source_restore_closure"][
                "source_gate_sha256s_before"
            ]["cs408"] = "f" * 64
            self.assertFalse(
                release._exact_20260816_9ab6_fenced_closes_prepare(
                    prepare_receipt_sha256=descriptor["prepare_receipt_sha256"],
                    prepare=prepare,
                    postcommit=changed,
                    receipts=receipts,
                    release_base=self.base,
                )
            )

            successor = release.EXACT_20260816_5A39_FENCED_CLOSURE
            successor_prepare = copy.deepcopy(prepare)
            successor_prepare.update(
                {
                    "release_id": successor["target_release_id"],
                    "canary_manifest_sha256": successor[
                        "canary_manifest_sha256"
                    ],
                }
            )
            successor_failed = copy.deepcopy(failed)
            successor_failed.update(
                {
                    "release_id": successor["target_release_id"],
                    "prepare_receipt_sha256": successor[
                        "prepare_receipt_sha256"
                    ],
                }
            )
            successor_proof = copy.deepcopy(proof)
            successor_proof.update(
                {
                    "descriptor_sha256": release.sha256_bytes(
                        release.canonical_bytes(successor)
                    ),
                    "prepare_receipt_sha256": successor[
                        "prepare_receipt_sha256"
                    ],
                    "failed_postcommit_receipt_sha256": successor[
                        "failed_postcommit_receipt_sha256"
                    ],
                    "current_before": successor["target_release_id"],
                    "target_math_gate_sha256_before": successor[
                        "target_math_gate_sha256"
                    ],
                }
            )
            successor_closure = copy.deepcopy(closure)
            successor_closure.update(
                {
                    "release_id": successor["target_release_id"],
                    "prepare_receipt_sha256": successor[
                        "prepare_receipt_sha256"
                    ],
                    "failed_postcommit_receipt_sha256": successor[
                        "failed_postcommit_receipt_sha256"
                    ],
                    "exact_20260816_fenced_source_restore_closure": (
                        successor_proof
                    ),
                }
            )
            self.assertTrue(
                release._exact_20260816_9ab6_fenced_closes_prepare(
                    prepare_receipt_sha256=successor[
                        "prepare_receipt_sha256"
                    ],
                    prepare=successor_prepare,
                    postcommit=successor_closure,
                    receipts={
                        successor["failed_postcommit_receipt_sha256"]: (
                            successor_failed
                        )
                    },
                    release_base=self.base,
                    descriptor=successor,
                )
            )

    def test_stale_failed_heartbeat_cannot_fail_new_activation(self) -> None:
        gate = {"state": "failed_drained", "blocking_reason": "old_failure"}
        self.assertFalse(
            release._is_target_activation_failed_status(
                {"target_activation_heartbeat": False}, gate
            )
        )
        self.assertTrue(
            release._is_target_activation_failed_status(
                {"target_activation_heartbeat": True}, gate
            )
        )

    def test_running_service_progress_uses_cpu_and_heartbeat_changes(self) -> None:
        heartbeat_root = self.base / "heartbeat-progress"
        heartbeat_root.mkdir()
        first = release._running_service_progress_token(
            service_proofs=[{"pid": os.getpid()}],
            heartbeat_root=heartbeat_root,
        )
        heartbeat = heartbeat_root / "math.json"
        heartbeat.write_text("{}\n", encoding="utf-8")
        second = release._running_service_progress_token(
            service_proofs=[{"pid": os.getpid()}],
            heartbeat_root=heartbeat_root,
        )
        self.assertNotEqual(first, second)

    def test_exact_20260814_staged_gate_proof_is_exact_helper_shape(
        self,
    ) -> None:
        descriptor = release.EXACT_20260814_6043_FORWARD_RECOVERY
        proof = {
            "schema_version": (
                "study-intake-english-preserved-review-repair-"
                "target-gate-stage-v1"
            ),
            "subject": "english",
            "status": "staged_paused_drained",
            "target_release_id": descriptor["target_release_id"],
            "target_activation_id": descriptor["english_activation_id"],
            "target_generation": descriptor["english_generation"],
            "target_subject_authority_fingerprint": descriptor[
                "english_subject_authority_fingerprint"
            ],
            "target_producer_authority_fingerprint": descriptor[
                "english_producer_authority_fingerprint"
            ],
            "staged_target_canary_state_sha256": descriptor["gate_sha256s"][
                "english"
            ],
            "luna_consumer_enabled": False,
            "active_task_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "formal_write_count": 0,
        }
        self.assertEqual(len(proof), 15)
        self.assertEqual(
            release.sha256_bytes(release.canonical_bytes(proof)),
            "98dbb0719855e2b56435e0eee90097e482b01b7d6dce80aeec25de37e2571f95",
        )
        self.assertEqual(
            descriptor["staged_gate_proof_sha256"],
            "98dbb0719855e2b56435e0eee90097e482b01b7d6dce80aeec25de37e2571f95",
        )

    def test_exact_20260814_claim_gate_inode_drift_is_pre_mutation(
        self,
    ) -> None:
        drain = {
            "claim_gate": {
                "path": "/fixture/worker.lock",
                "device": 7,
                "inode": 11,
                "held": True,
            }
        }
        live = {
            "path": "/fixture/worker.lock",
            "device": 7,
            "inode": 12,
            "held": True,
        }
        with self.assertRaisesRegex(
            release.ReleaseError,
            "exact_20260814_claim_gate_continuity_drift",
        ):
            release._exact_20260814_validate_claim_gate_continuity(
                drain, live
            )
        for malformed in ({}, [True], [2, 1], [1, 1], [0]):
            with self.subTest(malformed=malformed), self.assertRaisesRegex(
                release.ReleaseError,
                "exact_20260814_port_owner_snapshot_invalid",
            ):
                release._exact_20260814_validate_port_owners(malformed)
        self.assertEqual(
            release._exact_20260814_fence_failure_code([]),
            "exact_20260814_forward_recovery_incomplete_target_fenced",
        )
        self.assertEqual(
            release._exact_20260814_fence_failure_code(["residual_owner"]),
            "exact_20260814_forward_recovery_incomplete_fence_incomplete",
        )

    def test_exact_20260814_partial_arm_failure_pauses_target_gate(
        self,
    ) -> None:
        descriptor = copy.deepcopy(
            release.EXACT_20260814_6043_FORWARD_RECOVERY
        )
        release_base = self.base / "forward-release-base"
        runtime_root = self.base / "forward-runtime"
        target = release_base / "releases" / descriptor["target_release_id"]
        target.mkdir(parents=True)
        gate_root = runtime_root / "dispatch" / "state" / "production-canary"
        gate_root.mkdir(parents=True)
        for subject in release.RESUMABLE_SUBJECTS:
            value = {
                "subject": subject,
                "release_id": (
                    descriptor["target_release_id"]
                    if subject == "math"
                    else descriptor["previous_release_id"]
                ),
                "active_task_count": 0,
                "formal_write_count": 0,
            }
            (gate_root / f"{subject}.json").write_bytes(
                release.canonical_bytes(value)
            )
        descriptor.update(
            {
                "release_base": str(release_base),
                "active_link": str(release_base / "current"),
                "claim_gate_path": str(runtime_root / "worker.lock"),
            }
        )
        topology = [
            {
                "name": name,
                "label": f"fixture.{name}",
                "disable": ["disable", name],
                "bootout": ["bootout", name],
                "verify": ["verify", name],
                "disabled_status": ["disabled", name],
            }
            for name in ("math", "cs408", "english", "dashboard")
        ]
        context = {
            "target": target,
            "active_link": release_base / "current",
            "topology": topology,
            "verified": {"runtime_data_root": str(runtime_root)},
        }
        paused: list[str] = []

        def pause_subject(**values: object) -> dict[str, Any]:
            subject = str(values["subject"])
            paused.append(subject)
            return {
                "subject": subject,
                "active_count": 0,
                "claimed_total": 0,
                "canary_gate": {
                    "release_id": descriptor["target_release_id"],
                    "luna_consumer_enabled": False,
                    "active_task_count": 0,
                    "formal_write_count": 0,
                },
            }

        residual_snapshot = {
            "legacy_worker_pids": [],
            "dispatcher_pids": [123],
            "dashboard_pids": [],
            "luna_pids": [],
        }
        fenced_evidence: dict[str, Any] = {}

        def seal_evidence(
            value: Mapping[str, Any], **_values: object
        ) -> dict[str, Any]:
            result = copy.deepcopy(dict(value))
            result["authority"] = {"fixture": True}
            fenced_evidence.update(result)
            return result
        with (
            mock.patch.object(
                release,
                "EXACT_20260814_6043_FORWARD_RECOVERY",
                descriptor,
            ),
            mock.patch.object(
                release,
                "_exact_20260814_6043_forward_preflight",
                return_value=context,
            ),
            mock.patch.object(
                release,
                "_claim_gate_path",
                return_value=runtime_root / "worker.lock",
            ),
            mock.patch.object(
                release,
                "_claim_gate_lock",
                side_effect=lambda *_args, **_kwargs: contextlib.nullcontext(),
            ) as claim_lock,
            mock.patch.object(
                release,
                "_english_preserved_review_repair_apply",
                side_effect=release.ReleaseError("injected_after_math_arm"),
            ),
            mock.patch.object(
                release,
                "_reopen_english_preserved_review_repair",
                side_effect=release.ReleaseError("no_repair"),
            ),
            mock.patch.object(
                release,
                "_run_service_deactivation_best_effort",
                return_value=["service_bootout_failed:math"],
            ) as deactivate,
            mock.patch.object(
                release,
                "_run_subject_batch_recovery_json",
                side_effect=pause_subject,
            ),
            mock.patch.object(
                release, "_seal_deployment_value", side_effect=seal_evidence
            ),
            mock.patch.object(
                release,
                "_write_deployment_receipt",
                return_value="f" * 64,
            ),
            mock.patch.object(release, "_run_service_activation") as activate,
        ):
            with self.assertRaisesRegex(
                release.ReleaseError,
                "exact_20260814_forward_recovery_incomplete_fence_incomplete",
            ):
                release.recover_exact_20260814_6043_forward_transaction(
                    runner=lambda _command: subprocess.CompletedProcess(
                        [], 0, "", ""
                    ),
                    process_inspector=lambda *_args: residual_snapshot,
                    port_owner_inspector=lambda: [],
                )
        deactivate.assert_called_once()
        activate.assert_not_called()
        claim_lock.assert_called_once()
        self.assertEqual(paused, ["math"])
        self.assertEqual(fenced_evidence["status"], "fence_incomplete")
        self.assertEqual(
            fenced_evidence["process_snapshot"]["dispatcher_pids"], [123]
        )
        self.assertTrue(
            any(
                str(error).startswith("process_snapshot_invalid:")
                for error in fenced_evidence["fence_errors"]
            )
        )

    def test_exact_20260814_postcommit_reopen_failure_never_compensates(
        self,
    ) -> None:
        descriptor = copy.deepcopy(
            release.EXACT_20260814_6043_FORWARD_RECOVERY
        )
        release_base = self.base / "commit-point-release-base"
        target = release_base / "releases" / descriptor["target_release_id"]
        target.mkdir(parents=True)
        descriptor.update(
            {
                "release_base": str(release_base),
                "active_link": str(release_base / "current"),
                "claim_gate_path": str(release_base / "worker.lock"),
            }
        )
        runtime_canary = {
            "slots": {
                subject: {
                    "producer_high_watermark_sha256": subject[0] * 64,
                }
                for subject in release.RESUMABLE_SUBJECTS
            }
        }
        context = {
            "target": target,
            "active_link": release_base / "current",
            "launchagent_dir": self.launchagents,
            "topology": [],
            "verified": {"runtime_data_root": str(release_base)},
            "runtime_canary": runtime_canary,
            "rendered_plists": {},
            "checked_canary": {"release_manifest_sha256": "a" * 64},
            "staged_gate_proof": {"exact": "staged"},
            "prepare": {
                "previous_service_topology": [],
                "service_activation_policy": {},
                "drain_verification": {},
            },
            "stopped_process_snapshot": {},
        }
        repair = {"repair_receipt_sha256": "b" * 64}
        reopened = {"exact": "reopened"}
        arm = {"exact": "arm"}
        binding = {"exact": "binding"}
        bound_arm = {
            **arm,
            "english_preserved_review_repair_binding": binding,
        }
        post = {
            "service_release_ids": {
                name: descriptor["target_release_id"]
                for name in ("math", "cs408", "english", "dashboard")
            }
        }
        activation_receipt = {"exact": "activation", "authority": {}}
        write_values: list[dict[str, Any]] = []

        def write_receipt(
            _base: Path, value: Mapping[str, Any]
        ) -> str:
            copied = copy.deepcopy(dict(value))
            write_values.append(copied)
            digest = release.sha256_bytes(release.canonical_bytes(copied))
            destination = _base / "deployments" / f"{digest}.json"
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(release.canonical_bytes(copied))
            if len(write_values) == 1:
                raise OSError("injected_after_activation_replace")
            return "0" * 64

        live_gate = {
            "path": descriptor["claim_gate_path"],
            "device": 1,
            "inode": 2,
            "held": True,
        }
        patches = [
            mock.patch.object(
                release,
                "EXACT_20260814_6043_FORWARD_RECOVERY",
                descriptor,
            ),
            mock.patch.object(
                release,
                "_claim_gate_lock",
                return_value=contextlib.nullcontext(live_gate),
            ),
            mock.patch.object(
                release,
                "_exact_20260814_6043_forward_preflight",
                return_value=context,
            ),
            mock.patch.object(
                release,
                "_claim_gate_path",
                return_value=Path(descriptor["claim_gate_path"]),
            ),
            mock.patch.object(
                release,
                "_english_preserved_review_repair_apply",
                return_value=repair,
            ),
            mock.patch.object(
                release,
                "_reopen_english_preserved_review_repair",
                return_value=reopened,
            ),
            mock.patch.object(
                release,
                "_validate_english_preserved_review_repair_reopen",
                return_value=reopened,
            ),
            mock.patch.object(
                release, "_run_canary_pre_activation_verifier", return_value=arm
            ),
            mock.patch.object(
                release, "_validate_canary_preflight_proof", return_value=arm
            ),
            mock.patch.object(
                release,
                "_bind_english_preserved_review_repair_to_canary_proof",
                return_value=bound_arm,
            ),
            mock.patch.object(release, "_run_service_activation"),
            mock.patch.object(
                release, "_default_post_canary_verifier", return_value=post
            ),
            mock.patch.object(
                release, "_validate_post_canary_proof", return_value=post
            ),
            mock.patch.object(release, "_canary_public_slots", return_value={}),
            mock.patch.object(
                release,
                "_seal_deployment_value",
                side_effect=[activation_receipt, {"exact": "forward", "authority": {}}],
            ),
            mock.patch.object(
                release,
                "_validate_three_subject_canary_activation_receipt",
                return_value=activation_receipt,
            ),
            mock.patch.object(
                release, "_deployment_receipt_objects", return_value={}
            ),
            mock.patch.object(
                release,
                "_english_review_repair_postcommit_closes_prepare",
                return_value=True,
            ),
            mock.patch.object(
                release,
                "_exact_20260814_6043_receipt_only_closes_prepare",
                return_value=True,
            ),
            mock.patch.object(
                release, "_write_deployment_receipt", side_effect=write_receipt
            ),
            mock.patch.object(
                release,
                "_assert_no_unresolved_deployment_transaction",
                side_effect=release.ReleaseError("injected_postcommit_reopen"),
            ),
            mock.patch.object(
                release, "_run_service_deactivation_best_effort"
            ),
            mock.patch.object(release, "_run_subject_batch_recovery_json"),
            mock.patch.object(
                release, "_rollback_english_preserved_review_repair"
            ),
        ]
        with contextlib.ExitStack() as stack:
            entered = [stack.enter_context(patch) for patch in patches]
            deactivate = entered[-3]
            pause = entered[-2]
            rollback = entered[-1]
            with self.assertRaisesRegex(
                release.ReleaseError, "injected_postcommit_reopen"
            ):
                release.recover_exact_20260814_6043_forward_transaction()
        self.assertEqual(len(write_values), 2)
        self.assertEqual(write_values[0], activation_receipt)
        self.assertEqual(write_values[1]["status"], "committed")
        deactivate.assert_not_called()
        pause.assert_not_called()
        rollback.assert_not_called()

    def test_exact_20260814_uncertain_closure_rejects_wrong_bytes(
        self,
    ) -> None:
        release_base = self.base / "wrong-closure"
        closure = {"status": "committed", "formal_write_count": 0}
        digest = release.sha256_bytes(release.canonical_bytes(closure))
        destination = release_base / "deployments" / f"{digest}.json"
        destination.parent.mkdir(parents=True)
        destination.write_bytes(release.canonical_bytes({"wrong": True}))
        with self.assertRaisesRegex(
            release.ReleaseError,
            "exact_20260814_closure_commit_uncertain",
        ):
            release._exact_20260814_reopen_committed_closure(
                release_base=release_base,
                closure=closure,
                expected_sha256=digest,
            )
        destination.unlink()
        with (
            mock.patch.object(
                release,
                "_write_deployment_receipt",
                side_effect=OSError("injected_without_replace"),
            ),
            self.assertRaisesRegex(
                release.ReleaseError,
                "exact_20260814_closure_commit_uncertain",
            ),
        ):
            release._exact_20260814_write_or_reopen_deployment_receipt(
                release_base=release_base, value=closure
            )
        destination.write_bytes(release.canonical_bytes(closure))
        with mock.patch.object(
            release,
            "_write_deployment_receipt",
            return_value="0" * 64,
        ):
            self.assertEqual(
                release._exact_20260814_write_or_reopen_deployment_receipt(
                    release_base=release_base, value=closure
                ),
                digest,
            )
        destination.unlink()
        with (
            mock.patch.object(
                release,
                "_write_deployment_receipt",
                return_value=digest,
            ),
            self.assertRaisesRegex(
                release.ReleaseError,
                "exact_20260814_closure_commit_uncertain",
            ),
        ):
            release._exact_20260814_write_or_reopen_deployment_receipt(
                release_base=release_base, value=closure
            )

    def test_release_helpers_accept_real_dispatcher_retire_and_reopen_shapes(
        self,
    ) -> None:
        sys.path.insert(0, str(ROOT / "lib"))
        sys.path.insert(0, str(ROOT / "bin"))
        import preprocess_dispatcher as dispatcher_runtime
        import subject_sol_contract as subject_sol

        writer = {
            "schema_version": "subject_sol_writer_v1",
            "subject": "cs408",
            "revision": 2,
            "batch_id": None,
        }
        writer_sha = subject_sol._document_sha256(writer)
        pointer = {
            "retirement_receipt_sha256": "4" * 64,
            "retirement_receipt_path": str(
                self.base / "real-runtime-retirement.json"
            ),
            "writer_state_after_sha256": writer_sha,
        }
        receipt = {
            "writer_state_after": writer,
            "writer_state_after_sha256": writer_sha,
            "rollback_token": "5" * 64,
            "authority_snapshot_sha256s": ["6" * 64, "7" * 64],
        }
        store = subject_sol.SubjectSolRuntimeStore.__new__(
            subject_sol.SubjectSolRuntimeStore
        )
        store._read_writer_locked = lambda _subject: writer
        retire_payload = store._cs408_retirement_result_locked(
            pointer=pointer, receipt=receipt, idempotent=False
        )

        class ShapeRunner:
            def __init__(inner_self, payload: Mapping[str, Any]) -> None:
                inner_self.payload = dict(payload)

            def __call__(inner_self, _command):
                return {
                    "returncode": 0,
                    "stdout": json.dumps(inner_self.payload),
                }

        accepted_retire = (
            release._retire_cs408_terminal_batch_before_canary_arm(
                target=self.source,
                runner=ShapeRunner(retire_payload),
            )
        )
        self.assertEqual(accepted_retire["status"], "retired")
        self.assertEqual(
            accepted_retire["schema_version"],
            "study-intake-cs408-terminal-batch-retirement-result-v1",
        )

        reopen_store_payload = {
            **retire_payload,
            "schema_version": (
                "study-intake-cs408-terminal-batch-retirement-reopen-result-v1"
            ),
            "status": "reopened",
            "idempotent": True,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
        }

        class ReopenStore:
            @staticmethod
            def reopen_authorized_cs408_terminal_batch_retirement(
                *, retirement_receipt_path
            ):
                del retirement_receipt_path
                return reopen_store_payload

        dispatch_runtime = dispatcher_runtime.ProductionDispatchRuntime.__new__(
            dispatcher_runtime.ProductionDispatchRuntime
        )
        dispatch_runtime.subject = "cs408"
        dispatch_runtime.subject_sol = ReopenStore()
        reopen_payload = (
            dispatch_runtime.reopen_cs408_terminal_batch_retirement(
                retirement_receipt=None,
                expected_terminal_receipt_sha256=(
                    release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                        "terminal_receipt_sha256"
                    ]
                ),
            )
        )
        accepted_reopen = (
            release._reopen_cs408_terminal_batch_retirement_after_uncertain_attempt(
                target=self.source,
                runner=ShapeRunner(reopen_payload),
            )
        )
        self.assertEqual(accepted_reopen["status"], "reopened")
        self.assertTrue(accepted_reopen["reopen_read_only"])
        self.assertEqual(accepted_reopen["mcp_tool_call_count"], 0)

    def test_external_status_script_contract_rejects_symlink_and_hash_drift(
        self,
    ) -> None:
        scripts = {
            "math_status_script": self.base / "math-status.py",
            "cs408_status_script": self.base / "cs408-status.py",
            "english_events_module": (
                self.base / "english/english_pipeline/events.py"
            ),
        }
        for source_name, path in scripts.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"# {source_name}\n", encoding="utf-8")
        config = {
            "adapters": {
                "math": {
                    "status_script": str(scripts["math_status_script"])
                },
                "cs408": {
                    "status_script": str(scripts["cs408_status_script"])
                },
                "english": {
                    "repo_root": str(self.base / "english"),
                },
            }
        }
        expected = {
            source_name: hashlib.sha256(path.read_bytes()).hexdigest()
            for source_name, path in scripts.items()
        }
        plugin_root = self.source / "plugin" / "kaoyan-study-intake"
        plugin_root.mkdir(parents=True, exist_ok=True)
        component_lock_path = plugin_root / "component-lock.json"
        component_registry_path = plugin_root / "components.json"

        def write_bindings(values: dict[str, str] | None = None) -> None:
            digests = values or expected
            external_sources = {
                source_name: {
                    "path": str(path),
                    "sha256": digests[source_name],
                }
                for source_name, path in scripts.items()
            }
            payload = {"external_runtime_sources": external_sources}
            component_lock_path.write_text(
                json.dumps(payload) + "\n", encoding="utf-8"
            )
            component_registry_path.write_text(
                json.dumps(payload) + "\n", encoding="utf-8"
            )

        write_bindings()
        self.assertEqual(
            release._external_status_script_sha256(config, "cs408"),
            expected["cs408_status_script"],
        )

        link = self.base / "cs408-status-link.py"
        link.symlink_to(scripts["cs408_status_script"])
        linked = json.loads(json.dumps(config))
        linked["adapters"]["cs408"]["status_script"] = str(link)
        with self.assertRaisesRegex(
            release.ReleaseError, "release_cs408_status_script_invalid"
        ):
            release._external_status_script_sha256(linked, "cs408")

        matching = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps(
                {
                    "status_script_sha256": expected["cs408_status_script"],
                    "processing_contract_sha256": "a" * 64,
                }
            ).encode("utf-8"),
            stderr=b"",
        )
        with mock.patch.object(release.subprocess, "run", return_value=matching):
            release._verify_target_external_status_script_contract(
                config, release_root=self.source
            )

        drifted_bindings = dict(expected)
        drifted_bindings["english_events_module"] = "b" * 64
        write_bindings(drifted_bindings)
        with mock.patch.object(release.subprocess, "run", return_value=matching):
            with self.assertRaisesRegex(
                release.ReleaseError,
                "release_external_runtime_source_binding_mismatch",
            ):
                release._verify_target_external_status_script_contract(
                    config, release_root=self.source
                )
        write_bindings()

        drifted = subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout=json.dumps(
                {
                    "status_script_sha256": "b" * 64,
                    "processing_contract_sha256": "a" * 64,
                }
            ).encode("utf-8"),
            stderr=b"",
        )
        with mock.patch.object(release.subprocess, "run", return_value=drifted):
            with self.assertRaisesRegex(
                release.ReleaseError,
                "release_external_runtime_source_binding_mismatch",
            ):
                release._verify_target_external_status_script_contract(
                    config, release_root=self.source
                )

        legacy_sources = {
            "cs408_status_script": {
                "path": str(scripts["cs408_status_script"]),
                "sha256": expected["cs408_status_script"],
            }
        }
        legacy_payload = {"external_runtime_sources": legacy_sources}
        component_lock_path.write_text(
            json.dumps(legacy_payload) + "\n", encoding="utf-8"
        )
        component_registry_path.write_text(
            json.dumps(legacy_payload) + "\n", encoding="utf-8"
        )
        with mock.patch.object(release.subprocess, "run", return_value=matching):
            release._verify_target_external_status_script_contract(
                config, release_root=self.source
            )

    def test_build_verification_does_not_probe_live_runtime_bindings(self) -> None:
        (self.source / "lib").mkdir()
        (self.source / "lib" / "preprocessor_core.py").write_text(
            "# fixture core\n", encoding="utf-8"
        )
        with mock.patch.object(
            release,
            "_verify_target_external_status_script_contract",
            side_effect=release.ReleaseError("live_runtime_probe_forbidden"),
        ) as live_probe:
            built = self.build(passed=True)
            live_probe.assert_not_called()
            with self.assertRaisesRegex(
                release.ReleaseError, "live_runtime_probe_forbidden"
            ):
                release.verify_release(Path(str(built["release_dir"])))
            live_probe.assert_called_once()

    def test_target_config_requires_all_three_package_v3_bindings(self) -> None:
        template = json.loads(
            (self.source / "config.example.json").read_text(encoding="utf-8")
        )
        config = release.substitute(
            template,
            {
                "${RELEASE_ROOT}": str(self.source),
                "${RUNTIME_DATA_ROOT}": str(self.runtime_data),
            },
        )
        release._validate_target_release_config(
            config, release_root=self.source
        )
        for profile_name in (
            "math_deep_v2",
            "cs408_deep_v2",
            "english_two_pass_v1",
        ):
            with self.subTest(profile=profile_name):
                invalid = json.loads(json.dumps(config))
                invalid[profile_name]["package_output_schema"] = str(
                    self.source / "schemas" / "preprocess-package-v2.json"
                )
                with self.assertRaisesRegex(
                    release.ReleaseError, "release_target_contract_invalid"
                ):
                    release._validate_target_release_config(
                        invalid, release_root=self.source
                    )

    @staticmethod
    def passed_test_results() -> dict:
        return {
            "schema_version": "study-intake-release-test-results-v1",
            "status": "passed",
            "suites": [
                {"name": name, "status": "passed", "test_count": 1, "skipped_count": 0}
                for name in ("core", "dashboard")
            ],
            "formal_surface_gate": {
                "schema_version": release.FORMAL_GATE_SCHEMA,
                "status": "not_run",
                "verification_status": "not_run",
                "baseline_file_sha256": None,
                "config_file_sha256": None,
                "baseline_manifest_sha256": None,
                "current_manifest_sha256": None,
                "differences": None,
            },
            "real_model_call_count": 0,
            "formal_write_count": 0,
        }

    @staticmethod
    def passed_formal_gate() -> dict:
        return {
            "schema_version": release.FORMAL_GATE_SCHEMA,
            "status": "passed",
            "verification_status": "unchanged",
            "baseline_file_sha256": "a" * 64,
            "config_file_sha256": "b" * 64,
            "baseline_manifest_sha256": "c" * 64,
            "current_manifest_sha256": "c" * 64,
            "differences": {},
        }

    @staticmethod
    def post_activation_proof(**values) -> dict:
        return {
            "schema_version": release.POST_ACTIVATION_SCHEMA,
            "status": "verified_infrastructure_paused",
            "release_id": values["release_id"],
            "service_count": 4,
            "running_service_count": 1,
            "disabled_service_count": 3,
            "dispatcher_heartbeat_count": 0,
            "subject_controls": [
                {
                    "subject": subject,
                    "enabled": False,
                    "running": False,
                    "draining": True,
                    "active_count": 0,
                    "claimed_total": 0,
                }
                for subject in ("math", "cs408", "english")
            ],
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
            "formal_write_count": 0,
        }

    @staticmethod
    def post_rollback_proof(**values) -> dict:
        return {
            "schema_version": release.POST_ACTIVATION_SCHEMA,
            "status": "verified_rollback_paused",
            "release_id": values["release_id"],
            "service_count": 4,
            "running_service_count": 1,
            "disabled_service_count": 3,
            "dispatcher_heartbeat_count": 0,
            "subject_controls": [
                {
                    "subject": subject,
                    "enabled": False,
                    "running": False,
                    "draining": True,
                    "active_count": 0,
                    "claimed_total": 0,
                }
                for subject in ("math", "cs408", "english")
            ],
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
            "formal_write_count": 0,
        }

    @staticmethod
    def canary_preflight_proof(**values) -> dict:
        apply = values["apply"]
        context = values["canary_context"]
        return {
            "schema_version": release.THREE_SUBJECT_CANARY_PREFLIGHT_SCHEMA,
            "status": (
                "verified_historical_backlog_drained_and_slots_armed"
                if apply
                else "verified_historical_backlog_drained_and_slots_planned"
            ),
            "release_id": values["release_id"],
            "historical_backlog_drained": True,
            "post_activation_only": True,
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
            "requested_service_tier": None,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
            "subjects": [
                {
                    "subject": subject,
                    "gate_state": "armed" if apply else "planned",
                    "historical_eligible_count": 2,
                    "excluded_by_high_watermark_count": 2 if apply else None,
                    "canary_queue_count": 0 if apply else None,
                    "active_count": 0,
                    "claimed_total": 0,
                    "initial_canary_inflight_limit": 1,
                    "continuous_concurrency_limit": 20,
                    "requested_service_tier": None,
                    "fast_mode_requested": False,
                    "fast_mode_effective": "not_requested",
                    "producer_high_watermark_sha256": (
                        context["slots"][subject][
                            "producer_high_watermark_sha256"
                        ]
                        if apply
                        else None
                    ),
                    "queue_classification_sha256": "a" * 64 if apply else None,
                    "status_sha256": f"{index}" * 64,
                    "audit_sha256": f"{index + 3}" * 64,
                }
                for index, subject in enumerate(
                    release.RESUMABLE_SUBJECTS, start=1
                )
            ],
            "verification_model_call_count": 0,
            "verification_provider_request_count": 0,
            "verification_mcp_tool_call_count": len(
                release.RESUMABLE_SUBJECTS
            ),
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def english_review_repair_preview_fixture(self) -> dict:
        for module_root in (ROOT / "lib", ROOT / "bin"):
            if str(module_root) not in sys.path:
                sys.path.insert(0, str(module_root))
        import concurrent_dispatch

        descriptor = copy.deepcopy(
            concurrent_dispatch.ENGLISH_PRESERVED_REVIEW_REPAIR_AUTHORIZATION
        )
        path_keys = {
            "queue",
            "gate",
            "terminal_index",
            "completion",
            "lease",
            "task_detail",
            "task",
            "raw_output",
            "mcp_transport",
            "stage_execution_receipt",
            "processing_receipt",
            "mcp_failure_receipt",
            "canary_terminal_receipt",
            "subject_terminal_receipt",
            "original_preclaim_receipt",
            "rollover_receipt",
            "recovery_supersede_receipt",
            "subject_batch",
            "subject_batch_pointer",
            "subject_writer",
        }
        raw = {
            "schema_version": (
                "study-intake-english-preserved-review-repair-preview-v1"
            ),
            "status": "ready",
            "authorization_descriptor": descriptor,
            "authorization_descriptor_sha256": (
                release.ENGLISH_PRESERVED_REVIEW_REPAIR_DESCRIPTOR_SHA256
            ),
            "paths": {
                key: str((self.base / "review-preview" / key).resolve())
                for key in path_keys
            },
            "observed_historical_model_call_count": 1,
            "observed_historical_provider_request_count": 41,
            "observed_historical_mcp_tool_call_count": 40,
            "source_gate_preimage_sha256": (
                release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_GATE_SHA256
            ),
            "staged_target_canary_state_sha256": None,
            "target_release_id": None,
            "target_activation_id": None,
            "target_generation": None,
            "target_subject_authority_fingerprint": None,
            "target_producer_authority_fingerprint": None,
            "new_task_count": 0,
            "new_queue_count": 0,
            "capture_replay_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "sol_call_count": 0,
            "formal_write_count": 0,
        }
        return release._validate_english_preserved_review_repair_preview(raw)

    @staticmethod
    def canary_post_activation_proof(**values) -> dict:
        context = values["canary_context"]
        release_id = values["release_id"]
        return {
            "schema_version": release.THREE_SUBJECT_CANARY_POST_ACTIVATION_SCHEMA,
            "status": "verified_production_canary_active",
            "release_id": release_id,
            "service_count": 4,
            "running_service_count": 4,
            "disabled_service_count": 0,
            "dispatcher_heartbeat_count": 3,
            "post_activation_only": True,
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
            "requested_service_tier": None,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
            "service_release_ids": {
                name: release_id
                for name in ("math", "cs408", "english", "dashboard")
            },
            "subject_controls": [
                {
                    "subject": subject,
                    "enabled": True,
                    "running": True,
                    "draining": True,
                    "gate_state": "armed",
                    "post_activation_only": True,
                    "initial_canary_inflight_limit": 1,
                    "continuous_concurrency_limit": 20,
                    "requested_service_tier": None,
                    "fast_mode_requested": False,
                    "fast_mode_effective": "not_requested",
                    "producer_capture_enabled": True,
                    "luna_consumer_enabled": True,
                    "active_count": 0,
                    "claimed_total": 0,
                    "producer_high_watermark_sha256": context["slots"][subject][
                        "producer_high_watermark_sha256"
                    ],
                }
                for subject in release.RESUMABLE_SUBJECTS
            ],
            "model": release.REQUIRED_MODEL_CONTRACT["model"],
            "reasoning_effort": release.REQUIRED_MODEL_CONTRACT[
                "reasoning_effort"
            ],
            "verification_model_call_count": 0,
            "verification_provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "production_accepted": False,
        }

    @staticmethod
    def canary_rollback_proof(**values) -> dict:
        return {
            "schema_version": "study-intake-three-subject-canary-rollback-v1",
            "status": "canary_activation_rolled_back",
            "release_id": values["release_id"],
            "producer_queues_preserved": True,
            "subjects": [
                {
                    "subject": subject,
                    "state": "inactive_rolled_back",
                    "producer_queue_preserved": True,
                    "status_sha256": f"{index}" * 64,
                }
                for index, subject in enumerate(
                    release.RESUMABLE_SUBJECTS, start=1
                )
            ],
            "formal_write_count": 0,
        }

    def build(self, *, passed: bool = False) -> dict:
        if not passed:
            return release.build_release(
                source_root=self.source,
                release_base=self.release_base,
                runtime_data_root=self.runtime_data,
                skip_tests=True,
            )
        with (
            mock.patch.object(
                release, "run_tests", return_value=self.passed_test_results()
            ),
            mock.patch.object(
                release,
                "verify_fixture_formal_surfaces",
                return_value=self.passed_formal_gate(),
            ),
            mock.patch.object(
                release,
                "_load_historical_fixture",
                return_value=object(),
            ),
        ):
            return release.build_release(
                source_root=self.source,
                release_base=self.release_base,
                runtime_data_root=self.runtime_data,
                skip_tests=False,
                historical_test_input_manifest=(
                    self.base / "historical-fixture.json"
                ),
            )

    def build_pre_target_runtime_release(self, *, passed: bool = True) -> dict:
        legacy_schema = self.source / "schemas" / "preprocess-package-v2.json"
        legacy_schema.write_text("{}\n", encoding="utf-8")
        template_path = self.source / "config.example.json"
        template = json.loads(template_path.read_text(encoding="utf-8"))
        for profile_name in (
            "math_deep_v2",
            "cs408_deep_v2",
            "english_two_pass_v1",
        ):
            template[profile_name]["package_output_schema"] = (
                "${RELEASE_ROOT}/schemas/preprocess-package-v2.json"
            )
        template_path.write_text(json.dumps(template), encoding="utf-8")
        with mock.patch.object(
            release, "_validate_target_release_config", return_value=None
        ):
            return self.build(passed=passed)

    def build_historical_priority(self, *, passed: bool = False) -> dict:
        template_path = self.source / "config.example.json"
        template = json.loads(template_path.read_text(encoding="utf-8"))
        template["model"]["service_tier"] = "priority"
        template_path.write_text(json.dumps(template), encoding="utf-8")
        with (
            mock.patch.object(
                release,
                "REQUIRED_MODEL_CONTRACT",
                release.HISTORICAL_PRIORITY_MODEL_CONTRACT,
            ),
            mock.patch.object(
                release, "_model_contract_config_matches", return_value=True
            ),
        ):
            return self.build(passed=passed)

    def publish_content_json(self, value: dict) -> Path:
        raw = release.canonical_bytes(value)
        digest = release.sha256_bytes(raw)
        path = self.base / f"{digest}.json"
        path.write_bytes(raw)
        path.chmod(0o444)
        return path

    def install_current_plists(self, target: Path, active: Path) -> None:
        rendered = release._render_launchagent_plists(
            target,
            release.verify_rollback_release(target)["service_topology"],
            active,
            self.runtime_data.resolve(),
        )
        for label, raw in rendered.items():
            path = self.launchagents / f"{label}.plist"
            path.write_bytes(raw)
            path.chmod(0o644)

    def install_external_target_fixture(self, target: Path) -> tuple[dict, dict]:
        """Attach a valid sealed-profile surface to a synthetic release target."""

        import shutil

        verified = release.verify_release(target)
        release._make_tree_removable(target)
        plugin = target / "plugin" / "kaoyan-study-intake"
        shutil.copytree(
            ROOT / "plugin" / "kaoyan-study-intake",
            plugin,
            dirs_exist_ok=True,
        )
        components = json.loads(
            (plugin / "components.json").read_text(encoding="utf-8")
        )
        mcp = components["mcp"]
        runtime = {
            "python_executable": mcp["python_executable"],
            "python_flags": mcp["python_flags"],
            "sealed_launcher_path": mcp["sealed_launcher_path"],
            "sealed_launcher_sha256": mcp["sealed_launcher_sha256"],
            "release_root": mcp["release_root"],
            "release_id": mcp["release_id"],
            "release_manifest_sha256": mcp["release_manifest_sha256"],
        }
        (plugin / "component-lock.json").write_bytes(
            release.canonical_bytes({"mcp_sealed_runtime": runtime})
        )
        return verified, components

    @staticmethod
    def producer_authority(
        *, subject: str, release_id: str, index: int
    ) -> dict:
        core = {
            "schema_version": "study-intake-producer-authority-v1",
            "subject": subject,
            "release_id": release_id,
            "loaded_core_sha256": f"{index}" * 64,
            "processing_contract_sha256": f"{index + 3}" * 64,
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
            "formal_write_count": 0,
        }
        fingerprint = release.sha256_bytes(
            json.dumps(
                core,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        return {**core, "authority_fingerprint": fingerprint}

    @staticmethod
    def canary_v2_controls() -> dict:
        return {
            "schema_version": "study-intake-production-canary-state-v2",
            "post_activation_only": True,
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
            "keep_backlog_drained": True,
            "requested_service_tier": None,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
        }

    def make_canary_manifest(self, built: dict) -> Path:
        release_id = str(built["release_id"])
        indexes = {
            subject: index
            for index, subject in enumerate(release.RESUMABLE_SUBJECTS, start=1)
        }
        prepared = release.prepare_canary_manifest(
            release_base=self.release_base,
            release_id=release_id,
            output_dir=self.base / "canary-manifests",
            authority_resolver=lambda **values: self.producer_authority(
                subject=values["subject"],
                release_id=values["release_id"],
                index=indexes[values["subject"]],
            ),
        )
        return Path(prepared["path"])

    def make_three_subject_resume_receipt(
        self, built: dict, active: Path
    ) -> tuple[Path, dict[str, Path], Path, Path]:
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        verified = release.verify_rollback_release(target)
        formal = verified["test_results"]["formal_surface_gate"]
        formal_surface_sha256 = str(formal["current_manifest_sha256"])
        fingerprints = {
            "math": "1" * 64,
            "cs408": "2" * 64,
            "english": "3" * 64,
        }
        subject_paths: dict[str, Path] = {}
        subject_hashes: dict[str, str] = {}
        for index, subject in enumerate(release.RESUMABLE_SUBJECTS, start=1):
            evidence = {
                "schema_version": release.THREE_SUBJECT_RESUME_EVIDENCE_SCHEMA,
                "status": "passed",
                "subject": subject,
                "release_id": release_id,
                "mcp_authority_fingerprint": fingerprints[subject],
                "mcp_read_completed": True,
                "mcp_read_call_count": index,
                "analysis_completed": True,
                "report_generated": True,
                "report_sha256": f"{index + 3:x}" * 64,
                "requested_service_tier": "priority",
                "effective_service_tier": "requested_unverified",
                "model_call_count": 1,
                "provider_request_count": 1,
                "real_luna_run_count": 1,
                "formal_surface_sha256": formal_surface_sha256,
                "formal_write_count": 0,
                "sol_enabled": False,
                "production_accepted": False,
                "evidence_refs": [f"test:{subject}:report"],
            }
            path = self.publish_content_json(evidence)
            subject_paths[subject] = path
            subject_hashes[subject] = release.sha256_file(path)
        concurrency = {
            "schema_version": release.THREE_SUBJECT_CONCURRENCY_EVIDENCE_SCHEMA,
            "status": "passed",
            "release_id": release_id,
            "subjects": list(release.RESUMABLE_SUBJECTS),
            "barrier_expected": 3,
            "barrier_arrived": 3,
            "global_peak_active": 3,
            "per_subject_peak_active": {
                subject: 1 for subject in release.RESUMABLE_SUBJECTS
            },
            "mcp_read_subject_count": 3,
            "analysis_completed_subject_count": 3,
            "report_generated_count": 3,
            "real_luna_run_count": 3,
            "requested_service_tier": "priority",
            "effective_service_tier": "requested_unverified",
            "model_call_count": 3,
            "provider_request_count": 3,
            "mcp_authority_fingerprints": fingerprints,
            "subject_receipt_sha256s": subject_hashes,
            "formal_surface_sha256": formal_surface_sha256,
            "formal_write_count": 0,
            "sol_enabled": False,
            "production_accepted": False,
        }
        concurrency_path = self.publish_content_json(concurrency)
        acceptance = {
            "schema_version": release.THREE_SUBJECT_RESUME_ACCEPTANCE_SCHEMA,
            "status": "passed",
            "release_id": release_id,
            "release_manifest_sha256": release.sha256_file(
                target / "release.json"
            ),
            "subject_receipts": {
                subject: {
                    "path": str(subject_paths[subject]),
                    "sha256": subject_hashes[subject],
                }
                for subject in release.RESUMABLE_SUBJECTS
            },
            "concurrency_receipt": {
                "path": str(concurrency_path),
                "sha256": release.sha256_file(concurrency_path),
            },
            "mcp_authority_fingerprints": fingerprints,
            "formal_surface_guard": {
                "baseline_file_sha256": formal["baseline_file_sha256"],
                "config_file_sha256": formal["config_file_sha256"],
                "baseline_manifest_sha256": formal[
                    "baseline_manifest_sha256"
                ],
                "before_manifest_sha256": formal["current_manifest_sha256"],
                "after_manifest_sha256": formal["current_manifest_sha256"],
            },
            "seal_model_call_count": 0,
            "seal_provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        acceptance_path = self.publish_content_json(acceptance)
        receipt_path = self.base / "three-subject-resume-receipt.json"
        sealed = release.seal_three_subject_resume_acceptance(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            acceptance_manifest=acceptance_path,
            output_path=receipt_path,
        )
        self.assertEqual(sealed["status"], "sealed")
        return receipt_path, subject_paths, concurrency_path, acceptance_path

    class FakeRunner:
        def __init__(self, *, fail_at: int | None = None) -> None:
            self.commands: list[list[str]] = []
            self.fail_at = fail_at

        def __call__(self, command):
            self.commands.append(list(command))
            if command and command[-1] == "drain":
                subject = command[command.index("--subject") + 1]
                stdout = json.dumps({
                    "subject": subject,
                    "draining": True,
                    "active_count": 0,
                    "stale_count": 0,
                    "claimed_total": 0,
                })
            elif command and command[-1] == "canary-readiness":
                subject = command[command.index("--subject") + 1]
                stdout = json.dumps({
                    "schema_version": "study-intake-canary-readiness-v1",
                    "subject": subject,
                    "readiness": "ready",
                    "reason": "ready",
                    "authority_generation": f"{subject}-generation",
                    "authority_fingerprint": "a" * 64,
                    "batch_id": None,
                    "batch_sha256": None,
                    "writer_revision": 0,
                    "writer_batch_id": None,
                    "source_generation": None,
                    "original_preclaim_failure_receipt_sha256": None,
                    "original_preclaim_failure_receipt_path": None,
                    "preserved_queue_entry_sha256": None,
                    "subsequent_attempt_receipt_sha256s": [],
                    "preserved_task": None,
                    "read_only": True,
                    "authority_snapshot_count": 1,
                    "mcp_tool_call_count": 1,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                })
            elif command and command[-1] == "audit":
                subject = command[command.index("--subject") + 1]
                lease_status = {
                    "schema_version": (
                        "study-intake-dispatch-subject-status-read-only-v1"
                    ),
                    "subject": subject,
                    "draining": True,
                    "active_count": 0,
                    "stale_count": 0,
                    "claimed_total": 0,
                    "read_only": True,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                    "observed_at": "2026-08-11T00:00:00+00:00",
                }
                stdout = json.dumps({
                    "schema_version": (
                        "study-intake-dispatch-backlog-audit-v1"
                    ),
                    "subject": subject,
                    "eligible_count": 0,
                    "historical_eligible_count": 0,
                    "excluded_by_high_watermark_count": 0,
                    "canary_queue_count": 0,
                    "post_activation_unmaterialized_count": 0,
                    "read_only": True,
                    "draining": True,
                    "active_count": 0,
                    "claimed_total": 0,
                    "lease_status": lease_status,
                    "eligible_units": [],
                    "decisions": [],
                    "canary_gate": None,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                })
            else:
                stdout = ""
            return {
                "returncode": 1 if len(self.commands) == self.fail_at else 0,
                "stdout": stdout,
            }

    def make_drain_receipt(
        self,
        previous: str,
        target: str,
        active: Path,
        *,
        operation: str = "activate",
    ) -> Path:
        path = self.base / f"drain-{previous[:8]}-{target[:8]}.json"
        receipt = release.create_drain_receipt(
            release_base=self.release_base,
            active_link=active,
            target_release_id=target,
            output_path=path,
            operation=operation,
        )
        self.assertEqual(receipt["previous_release_id"], previous)
        return path

    def write_scanner_drain_receipt(
        self,
        filename_target_release_id: str,
        *,
        target_release_id: str | None = None,
        schema_version: str | None = None,
        formal_write_count: int = 0,
        tamper_authority: bool = False,
        noncanonical: bool = False,
        filename_suffix: str = "",
    ) -> Path:
        core = {
            "schema_version": schema_version or release.DRAIN_RECEIPT_SCHEMA,
            "target_release_id": target_release_id or filename_target_release_id,
            "formal_write_count": formal_write_count,
        }
        receipt = release._seal_deployment_value(
            core,
            release_base=self.release_base,
            purpose="release-drain-receipt",
        )
        if tamper_authority:
            receipt["authority"] = {
                **receipt["authority"],
                "hmac_sha256": "0" * 64,
            }
        path = (
            self.release_base
            / "deployments"
            / f"drain-{filename_target_release_id}{filename_suffix}.json"
        )
        if noncanonical:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(receipt, ensure_ascii=False, sort_keys=True, indent=2),
                encoding="utf-8",
            )
        else:
            release.atomic_json(path, receipt)
        return path

    @staticmethod
    def superseded_recovery_receipts(
        historical_prepare_sha256: str,
    ) -> tuple[dict[str, dict], dict, bytes]:
        descriptor = release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS.get(
            historical_prepare_sha256
        )
        if descriptor is None:
            raise release.ReleaseError("deployment_recovery_marker_v2_invalid")
        topology = ["fixture-topology"]
        policy = {"fixture": "policy"}
        historical_prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "operation": descriptor["historical_operation"],
            "release_id": descriptor["historical_release_id"],
            "expected_current": descriptor[
                "historical_expected_current_release_id"
            ],
            "observed_current": descriptor[
                "historical_expected_current_release_id"
            ],
            "active_link": descriptor["historical_active_link"],
            "rollback_path": {
                "release_dir": (
                    "/Users/xiazhibin/.codex/study-intake-preprocessor/releases/"
                    + descriptor["historical_expected_current_release_id"]
                ),
                "release_id": descriptor["historical_expected_current_release_id"],
                "service_restart_order": ["math", "cs408", "english", "dashboard"],
            },
            "service_topology": topology,
            "previous_service_topology": topology,
            "service_activation_policy": policy,
            "prepared_at": "2026-08-01T00:00:00+00:00",
            "formal_write_count": 0,
        }
        patched_descriptor = {
            **descriptor,
            "historical_service_topology_sha256": release.sha256_bytes(
                release.canonical_bytes(topology)
            ),
            "historical_service_activation_policy_sha256": release.sha256_bytes(
                release.canonical_bytes(policy)
            ),
        }
        successor_prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "operation": descriptor["successor_operation"],
            "release_id": descriptor["successor_release_id"],
            "expected_current": descriptor[
                "historical_superseded_endpoint_release_id"
            ],
            "observed_current": descriptor[
                "historical_superseded_endpoint_release_id"
            ],
            "prepared_at": "2026-08-01T00:00:02+00:00",
            "formal_write_count": 0,
        }
        proof = {
            "schema_version": release.POST_ACTIVATION_SCHEMA,
            "status": descriptor["successor_post_activation_status"],
            "release_id": descriptor["successor_release_id"],
            "current_path": descriptor.get("successor_proof_current_path"),
            "verified_at": "2026-08-01T00:00:03+00:00",
            "formal_write_count": 0,
        }
        successor_postcommit = {
            "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
            "operation": descriptor["successor_operation"],
            "release_id": descriptor["successor_release_id"],
            "status": "committed",
            "prepare_receipt_sha256": descriptor[
                "successor_prepare_receipt_sha256"
            ],
            "previous_release_id": descriptor[
                "historical_superseded_endpoint_release_id"
            ],
            "post_activation_verification": proof,
            "completed_at": "2026-08-01T00:00:04+00:00",
            "formal_write_count": 0,
        }
        receipts = {
            historical_prepare_sha256: historical_prepare,
            descriptor["successor_prepare_receipt_sha256"]: successor_prepare,
            descriptor["successor_postcommit_receipt_sha256"]: successor_postcommit,
        }
        incomplete = descriptor[
            "historical_incomplete_postcommit_receipt_sha256"
        ]
        if incomplete is not None:
            receipts[incomplete] = {
                "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
                "operation": descriptor["historical_operation"],
                "release_id": descriptor["historical_release_id"],
                "status": "rollback_incomplete_cancel_or_fence_failed",
                "prepare_receipt_sha256": historical_prepare_sha256,
                "completed_at": "2026-08-01T00:00:01+00:00",
                "formal_write_count": 0,
            }
        previous_release_id = descriptor["successor_release_id"]
        for index, continuation in enumerate(
            descriptor.get("continuation_transactions", ()), start=0
        ):
            start_second = 5 + index * 3
            proof = {
                "schema_version": release.THREE_SUBJECT_CANARY_POST_ACTIVATION_SCHEMA,
                "status": "verified_production_canary_active",
                "release_id": continuation["release_id"],
                "verified_at": f"2026-08-01T00:00:{start_second + 1:02d}+00:00",
                "formal_write_count": 0,
                "sol_enabled": False,
                "service_release_ids": {
                    name: continuation["release_id"]
                    for name in ("math", "cs408", "english", "dashboard")
                },
                "subject_controls": [
                    {
                        "subject": subject,
                        "enabled": True,
                        "running": True,
                        "draining": True,
                    }
                    for subject in release.RESUMABLE_SUBJECTS
                ],
            }
            receipts[continuation["prepare_receipt_sha256"]] = {
                "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
                "operation": continuation["operation"],
                "release_id": continuation["release_id"],
                "expected_current": previous_release_id,
                "observed_current": previous_release_id,
                "prepared_at": f"2026-08-01T00:00:{start_second:02d}+00:00",
                "formal_write_count": 0,
            }
            receipts[continuation["postcommit_receipt_sha256"]] = {
                "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
                "operation": continuation["operation"],
                "release_id": continuation["release_id"],
                "status": "committed",
                "prepare_receipt_sha256": continuation["prepare_receipt_sha256"],
                "previous_release_id": previous_release_id,
                "post_activation_verification": proof,
                "completed_at": f"2026-08-01T00:00:{start_second + 2:02d}+00:00",
                "formal_write_count": 0,
            }
            previous_release_id = continuation["release_id"]
        return receipts, proof, release.canonical_bytes(proof), patched_descriptor

    def make_v2_recovery_core(self, historical_prepare_sha256: str) -> dict:
        descriptor = release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS.get(
            historical_prepare_sha256
        )
        if descriptor is None:
            raise release.ReleaseError("deployment_recovery_marker_v2_invalid")
        head = str(descriptor["resolution_head_release_id"])
        current = self.release_base / "releases" / head
        runtime_root = self.runtime_data.resolve()
        states = {}
        for subject in release.RESUMABLE_SUBJECTS:
            value = {
                "schema_version": "study-intake-production-canary-state-v2",
                "subject": subject,
                "release_id": head,
                "formal_write_count": 0,
                "sol_enabled": False,
                "authority": {},
                "activation_id": "b" * 64,
                "terminal_by_outcome": {
                    "cancelled": 0,
                    "failed": 0,
                    "needs_rework": 0,
                    "succeeded": 0,
                    "timed_out": 0,
                },
                "terminal_task_count": 0,
                "active_task_count": 0,
                "activation_receipt_path": str(
                    runtime_root
                    / "dispatch"
                    / "production-canary"
                    / "receipts"
                    / subject
                    / ("b" * 64)
                    / "sha256"
                    / "placeholder.json"
                ),
                "terminal_index_path": str(
                    runtime_root
                    / "dispatch"
                    / "production-canary"
                    / "terminal-indexes"
                    / subject
                    / ("b" * 64)
                    / "sha256"
                    / "placeholder.json"
                ),
            }
            state_snapshot = {
                "path": str(
                    runtime_root
                    / "dispatch"
                    / "state"
                    / "production-canary"
                    / f"{subject}.json"
                ),
                "sha256": release.sha256_bytes(release.canonical_bytes(value)),
                "value": value,
            }
            for member, purpose in (
                ("activation_receipt", "dispatch-production-canary-activation"),
                ("terminal_index", "dispatch-production-canary-terminal-index"),
            ):
                evidence = {
                    "subject": subject,
                    "release_id": head,
                    "activation_id": "b" * 64,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                    "authority": {"purpose": purpose},
                }
                if member == "activation_receipt":
                    evidence.update(
                        {
                            "schema_version": "study-intake-production-canary-activation-receipt-v2",
                            "model_call_count": 0,
                            "provider_request_count": 0,
                            "mcp_tool_call_count": 0,
                        }
                    )
                else:
                    evidence.update(
                        {
                            "schema_version": "study-intake-production-canary-terminal-index-v2",
                            "counter_scope": "current_activation_latest_terminal_per_unit",
                            "model_call_count": 0,
                            "provider_request_count": 0,
                            "terminal_by_outcome": {
                                "cancelled": 0,
                                "failed": 0,
                                "needs_rework": 0,
                                "succeeded": 0,
                                "timed_out": 0,
                            },
                            "terminal_task_count": 0,
                            "units": {},
                        }
                    )
                state_snapshot[member] = {
                    "path": (
                        value["activation_receipt_path"]
                        if member == "activation_receipt"
                        else value["terminal_index_path"]
                    ),
                    "sha256": release.sha256_bytes(
                        release.canonical_bytes(evidence)
                    ),
                    "value": evidence,
                }
            value["activation_receipt_sha256"] = state_snapshot[
                "activation_receipt"
            ]["sha256"]
            value["terminal_index_sha256"] = state_snapshot["terminal_index"][
                "sha256"
            ]
            value["activation_gate_authority_sha256"] = release.sha256_bytes(
                json.dumps(
                    state_snapshot["activation_receipt"]["value"]["authority"],
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
            )
            value["activation_receipt_path"] = str(
                Path(value["activation_receipt_path"]).with_name(
                    f"{value['activation_receipt_sha256']}.json"
                )
            )
            value["terminal_index_path"] = str(
                Path(value["terminal_index_path"]).with_name(
                    f"{value['terminal_index_sha256']}.json"
                )
            )
            state_snapshot["activation_receipt"]["path"] = value[
                "activation_receipt_path"
            ]
            state_snapshot["terminal_index"]["path"] = value["terminal_index_path"]
            state_snapshot["sha256"] = release.sha256_bytes(
                release.canonical_bytes(value)
            )
            states[subject] = state_snapshot
        dashboard_gate = {
            subject: {
                "subject": subject,
                "release_id": head,
                "activation_id": "b" * 64,
                "activation_receipt_sha256": states[subject]["activation_receipt"][
                    "sha256"
                ],
                "terminal_index_sha256": states[subject]["terminal_index"]["sha256"],
                "authority": states[subject]["value"]["authority"],
                "formal_write_count": 0,
                "sol_enabled": False,
            }
            for subject in release.RESUMABLE_SUBJECTS
        }
        telemetry_authority = {
            "schema_version": "study-intake-dispatch-authority-v1",
            "algorithm": "HMAC-SHA256",
            "key_id": "c" * 64,
            "purpose": "dispatch-production-canary-concurrency-telemetry",
            "hmac_sha256": "d" * 64,
        }
        telemetry_authority_sha256 = release.sha256_bytes(
            release.canonical_bytes(telemetry_authority)
        )
        telemetry = {
            "release_id": head,
            "formal_write_count": 0,
            "sol_enabled": False,
            "authority": telemetry_authority,
            "authority_sha256": telemetry_authority_sha256,
            "terminal_index_sha256_by_subject": {
                subject: states[subject]["value"]["terminal_index_sha256"]
                for subject in release.RESUMABLE_SUBJECTS
            },
            "activation_ids": {
                subject: states[subject]["value"]["activation_id"]
                for subject in release.RESUMABLE_SUBJECTS
            },
            "active_by_subject": {
                subject: states[subject]["value"].get("active_task_count", 0)
                for subject in release.RESUMABLE_SUBJECTS
            },
            "terminal_task_count_by_subject": {
                subject: states[subject]["value"]["terminal_task_count"]
                for subject in release.RESUMABLE_SUBJECTS
            },
            "terminal_by_outcome_by_subject": {
                subject: states[subject]["value"]["terminal_by_outcome"]
                for subject in release.RESUMABLE_SUBJECTS
            },
        }
        dashboard_value = {
            "schema_version": "study-intake-dashboard-projection-v3",
            "subjects": {
                subject: {"canary_gate": dashboard_gate[subject]}
                for subject in release.RESUMABLE_SUBJECTS
            },
            "dispatchers": {
                subject: {
                    "canary_gate": dashboard_gate[subject],
                    "release_id": head,
                    "active_release_id": head,
                    "heartbeat_release_id": head,
                }
                for subject in release.RESUMABLE_SUBJECTS
            },
            "global_sol": {"formal_write_count": 0},
            "concurrency": {
                "release_id": head,
                "status": "verified",
                "source_telemetry": telemetry,
                "telemetry_authority_sha256": telemetry_authority_sha256,
            },
        }
        evidence = {
            "current_release_id": head,
            "current_link_target": str(current),
            "states": states,
            "dashboard_projection": {
                "path": str(runtime_root / "state" / "dashboard_projection.json"),
                "sha256": release.sha256_bytes(
                    release.canonical_bytes(dashboard_value)
                ),
                "value": dashboard_value,
            },
            "formal_write_count": 0,
        }
        with (
            mock.patch.object(release, "_current_release", return_value=(head, current)),
            mock.patch.object(release, "_verify_historical_executor_release"),
            mock.patch.object(release, "_verify_historical_manager_script"),
            mock.patch.object(
                release,
                "_capture_recovery_v2_resolution_evidence",
                return_value=evidence,
            ),
        ):
            return release.create_superseded_transaction_recovery_marker_core(
                release_base=self.release_base,
                active_link=self.base / "current",
                historical_prepare_receipt_sha256=historical_prepare_sha256,
                resolved_at="2026-08-01T00:00:12+00:00",
            )

    @staticmethod
    def fixture_v2_public_gate(core: Mapping[str, Any]):
        def rebuild(
            _state: Mapping[str, Any],
            *,
            subject: str,
            release_id: str,
            release_base: Path,
            descriptor: Mapping[str, Any],
        ) -> dict[str, Any]:
            del release_id, release_base, descriptor
            return dict(
                core["resolution_evidence"]["dashboard_projection"]["value"]
                ["subjects"][subject]["canary_gate"]
            )

        return rebuild

    def assert_v2_marker_rejected(
        self,
        historical_prepare_sha256: str,
        mutate: Callable[[dict, dict, dict], None],
    ) -> None:
        receipts, _proof, _proof_bytes, patched_descriptor = (
            self.superseded_recovery_receipts(historical_prepare_sha256)
        )
        core = self.make_v2_recovery_core(historical_prepare_sha256)
        head = core["resolution_evidence"]["current_release_id"]
        head_config = self.release_base / "releases" / head / "config.json"
        head_config.parent.mkdir(parents=True, exist_ok=True)
        head_config.write_text(
            json.dumps({"runtime_root": str(self.runtime_data.resolve())}),
            encoding="utf-8",
        )
        marker = release._seal_deployment_value(
            core,
            release_base=self.release_base,
            purpose=release.DEPLOYMENT_RECOVERY_MARKER_V2_PURPOSE,
        )
        mutate(receipts, marker, patched_descriptor)
        receipts.setdefault("marker", marker)
        original_sha256_bytes = release.sha256_bytes

        def fixture_sha256(raw: bytes) -> str:
            try:
                candidate = json.loads(raw)
                proof_release_id = (
                    candidate.get("release_id")
                    if isinstance(candidate, dict)
                    else None
                )
                proof_schema = (
                    candidate.get("schema_version")
                    if isinstance(candidate, dict)
                    else None
                )
            except (UnicodeError, json.JSONDecodeError):
                proof_release_id = None
                proof_schema = None
            if (
                proof_schema == release.POST_ACTIVATION_SCHEMA
                and proof_release_id == patched_descriptor["successor_release_id"]
            ):
                return patched_descriptor["successor_post_activation_proof_sha256"]
            for continuation in patched_descriptor.get("continuation_transactions", ()):
                if (
                    proof_schema
                    == release.THREE_SUBJECT_CANARY_POST_ACTIVATION_SCHEMA
                    and proof_release_id == continuation["release_id"]
                ):
                    return continuation["post_activation_proof_sha256"]
            return original_sha256_bytes(raw)

        with (
            mock.patch.object(
                release, "_deployment_receipt_objects", return_value=receipts
            ),
            mock.patch.dict(
                release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS,
                {historical_prepare_sha256: patched_descriptor},
            ),
            mock.patch.object(release, "_verify_historical_manager_script"),
            mock.patch.object(release, "_verify_historical_executor_release"),
            mock.patch.object(release, "_verify_recovery_v2_dispatch_authority"),
            mock.patch.object(
                release,
                "_recovery_v2_public_canary_gate",
                side_effect=self.fixture_v2_public_gate(core),
            ),
            mock.patch.object(release, "sha256_bytes", side_effect=fixture_sha256),
            self.assertRaisesRegex(
                release.ReleaseError, "deployment_recovery_marker(?:_v2)?_invalid"
            ),
        ):
            release._assert_no_unresolved_deployment_transaction(self.release_base)

    def write_installed_plist(self, label: str, arguments: list[str]) -> Path:
        path = self.launchagents / f"{label}.plist"
        path.write_bytes(plistlib.dumps({
            "Label": label,
            "ProgramArguments": arguments,
            "RunAtLoad": True,
            "KeepAlive": True,
        }, sort_keys=True))
        path.chmod(0o644)
        return path

    def make_legacy_current(self, release_id: str = "c" * 64) -> tuple[Path, Path]:
        legacy_root = self.base / "legacy-sources" / release_id
        (legacy_root / "bin" / "__pycache__").mkdir(parents=True)
        worker = legacy_root / "bin" / "preprocess_worker.py"
        worker.write_text(
            """#!/usr/bin/env python3
import argparse, json
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--config')
p.add_argument('command', nargs='?')
a=p.parse_args()
if a.command == 'run-once':
    value=json.loads(Path(a.config).read_text())
    root=Path(value['runtime_root'])
    (root/'state').mkdir(parents=True, exist_ok=True)
    (root/'state'/'legacy-smoke.json').write_text('{}\\n')
    print(json.dumps({'status':'ok','model_call_count':0,'formal_write_count':0}))
""",
            encoding="utf-8",
        )
        worker.chmod(0o755)
        (legacy_root / "bin" / "__pycache__" / "preprocess_worker.pyc").write_bytes(
            b"unregistered-bytecode"
        )
        (legacy_root / "dashboard").mkdir()
        (legacy_root / "dashboard" / "server.py").write_text(
            "print('legacy dashboard')\n", encoding="utf-8"
        )
        legacy_template = {
                "runtime_root": "${RUNTIME_DATA_ROOT}",
                "release": {"manifest_path": "${RELEASE_ROOT}/release.json"},
                "model": {
                    "codex_path": "/Applications/ChatGPT.app/codex",
                    "model": "gpt-5.6-luna",
                    "reasoning_effort": "max",
                    "service_tier": "priority",
                },
                "worker": {
                    "log_path": "${RUNTIME_DATA_ROOT}/logs/worker.log",
                    "lock_path": "${RUNTIME_DATA_ROOT}/state/worker.lock",
                },
                "dashboard": {
                    "projection_path": "${RUNTIME_DATA_ROOT}/state/dashboard_projection.json"
                },
                "adapters": {
                    "math": {"enabled": True},
                    "cs408": {"enabled": True},
                    "english": {"enabled": True},
                },
            }
        (legacy_root / "config.example.json").write_text(
            json.dumps(legacy_template),
            encoding="utf-8",
        )
        (legacy_root / "config.json").write_bytes(
            release.canonical_bytes(
                release.substitute(
                    legacy_template,
                    {
                        "${RELEASE_ROOT}": str(legacy_root.resolve()),
                        "${RUNTIME_DATA_ROOT}": str(self.runtime_data.resolve()),
                    },
                )
            )
        )
        (legacy_root / "release.json").write_text(
            json.dumps({"schema_version": "legacy-release-v1", "release_id": release_id}),
            encoding="utf-8",
        )
        current = self.base / "legacy-current"
        current.symlink_to(legacy_root)
        self.write_installed_plist(
            "com.xiazhibin.study-intake-preprocessor",
            ["/usr/bin/true", "legacy-worker"],
        )
        self.write_installed_plist(
            "com.xiazhibin.study-intake-dashboard",
            ["/usr/bin/true", "legacy-dashboard"],
        )
        return current, legacy_root

    def test_build_is_content_addressed_and_excludes_runtime_data(self) -> None:
        validation = self.source / "validation"
        validation.mkdir()
        validation_asset = validation / "golden-replay-modernization-v1.json"
        validation_asset.write_text(
            json.dumps(
                {
                    "schema_version": "fixture-validation-v1",
                    "status": "passed_zero_model_staging",
                    "model_call_count": 0,
                    "formal_write_count": 0,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        first = self.build()
        second = self.build()
        self.assertEqual(first["release_id"], second["release_id"])
        self.assertEqual(second["status"], "already_built")
        root = Path(first["release_dir"])
        self.assertFalse((root / "state").exists())
        self.assertTrue((root / "bin" / "worker.py").is_file())
        self.assertEqual(
            (root / "validation" / validation_asset.name).read_bytes(),
            validation_asset.read_bytes(),
        )
        config = json.loads((root / "config.json").read_text(encoding="utf-8"))
        self.assertEqual(config["runtime_root"], str(self.runtime_data.resolve()))
        self.assertEqual(config["release"]["manifest_path"], str(root / "release.json"))
        manifest = json.loads((root / "release.json").read_text(encoding="utf-8"))
        self.assertEqual(
            manifest["model_contract"],
            release.REQUIRED_MODEL_CONTRACT,
        )
        self.assertNotIn("service_tier", config["model"])
        self.assertEqual(manifest["test_results"]["status"], "not_run")
        self.assertEqual(manifest["test_results"]["real_model_call_count"], 0)
        self.assertEqual(manifest["test_results"]["formal_write_count"], 0)
        self.assertEqual(
            set(manifest["component_inventory"]),
            {
                "programs",
                "configuration",
                "evidence_rules",
                "data_formats",
                "service_configurations",
                "plugins",
            },
        )
        verified = release.verify_release(root)
        self.assertEqual(verified["status"], "verified")
        self.assertIn(
            "validation/golden-replay-modernization-v1.json",
            manifest["source_files"],
        )

    def test_rollback_topology_accepts_only_integrity_verified_pre_priority_release(
        self,
    ) -> None:
        template_path = self.source / "config.example.json"
        template = json.loads(template_path.read_text(encoding="utf-8"))
        template_path.write_text(json.dumps(template), encoding="utf-8")
        historical_contract = {
            "model": "gpt-5.6-luna",
            "reasoning_effort": "max",
        }
        current_component_inventory = release._component_inventory

        def historical_component_inventory(*args, **kwargs):
            value = current_component_inventory(*args, **kwargs)
            value.pop("plugins", None)
            return value

        with (
            mock.patch.object(
                release, "REQUIRED_MODEL_CONTRACT", historical_contract
            ),
            mock.patch.object(
                release,
                "_component_inventory",
                side_effect=historical_component_inventory,
            ),
        ):
            built = self.build()
        release_root = Path(str(built["release_dir"]))

        with self.assertRaisesRegex(
            release.ReleaseError, "release_manifest_invalid"
        ):
            release.verify_release(release_root)
        self.assertEqual(
            release._release_topology(str(release_root)),
            list(release.CONCURRENT_TOPOLOGY),
        )
        rollback_verified = release.verify_rollback_release(release_root)
        self.assertEqual(
            rollback_verified["verification_role"],
            "historical_reopen_or_rollback_only",
        )
        with self.assertRaises(release.ReleaseError):
            release.activate_release(
                release_base=self.release_base,
                release_id=str(built["release_id"]),
                active_link=self.base / "historical-target-current",
                apply=False,
                operation="activate",
                launchagent_dir=self.launchagents,
            )
        with self.assertRaises(release.ReleaseError):
            release.prepare_canary_manifest(
                release_base=self.release_base,
                release_id=str(built["release_id"]),
                output_dir=self.base / "historical-canary-manifests",
                authority_resolver=lambda **_values: {},
            )

        released_worker = release_root / "bin" / "worker.py"
        released_worker.chmod(0o644)
        released_worker.write_text("print('tampered')\n", encoding="utf-8")
        with self.assertRaises(release.ReleaseError):
            release._release_topology(str(release_root))

    def test_pre_target_runtime_release_is_exact_allowlisted_rollback_only(
        self,
    ) -> None:
        built = self.build_pre_target_runtime_release()
        release_root = Path(str(built["release_dir"]))
        release_id = str(built["release_id"])

        with self.assertRaisesRegex(
            release.ReleaseError, "release_target_contract_invalid"
        ):
            release.verify_release(release_root)
        with self.assertRaisesRegex(
            release.ReleaseError, "release_target_contract_invalid"
        ):
            release.verify_rollback_release(release_root)
        with self.assertRaisesRegex(
            release.ReleaseError,
            "historical_target_runtime_release_not_allowlisted",
        ):
            release._verify_release_with_model_contract(
                release_root,
                release.REQUIRED_MODEL_CONTRACT,
                component_inventory_profile="full",
                target_runtime_contract_required=False,
            )

        with mock.patch.object(
            release,
            "HISTORICAL_TARGET_RUNTIME_ROLLBACK_RELEASE_IDS",
            frozenset({release_id}),
        ):
            verified = release.verify_rollback_release(release_root)
            self.assertEqual(
                verified["verification_role"],
                "historical_reopen_or_rollback_only",
            )
            self.assertEqual(
                verified["historical_contract"],
                "historical_target_runtime_v1",
            )
            self.assertEqual(
                release._release_topology(str(release_root)),
                list(release.CONCURRENT_TOPOLOGY),
            )
            with self.assertRaisesRegex(
                release.ReleaseError, "release_target_contract_invalid"
            ):
                release.activate_release(
                    release_base=self.release_base,
                    release_id=release_id,
                    active_link=self.base / "historical-runtime-target-current",
                    apply=False,
                    operation="activate",
                    launchagent_dir=self.launchagents,
                )

            released_worker = release_root / "bin" / "worker.py"
            released_worker.chmod(0o644)
            released_worker.write_text("print('tampered')\n", encoding="utf-8")
            with self.assertRaises(release.ReleaseError):
                release.verify_rollback_release(release_root)

    def test_historical_target_runtime_allowlist_is_exact(self) -> None:
        self.assertEqual(
            release.HISTORICAL_TARGET_RUNTIME_ROLLBACK_RELEASE_IDS,
            frozenset(
                {
                    "a4ff96b8932344211ca51c69edda98a06e95382bcdc4de79e2520fdcf8e343d6",
                    "693450d5e1ccd0814b1a4b0998ed59ec280ad9e6efb7279c5639ef5b6ec6cc65",
                }
            ),
        )

    def test_canary_preview_reopens_exact_allowlisted_previous_runtime(self) -> None:
        target = self.build(passed=True)
        manifest = self.make_canary_manifest(target)
        previous = self.build_pre_target_runtime_release()
        previous_id = str(previous["release_id"])
        active = self.base / "pre-target-runtime-current"
        active.symlink_to(Path(str(previous["release_dir"])))
        observed_surface = {
            "schema_version": "preview-test-v1",
            "active_release_id": previous_id,
            "formal_write_count": 0,
        }

        with mock.patch.object(
            release,
            "HISTORICAL_TARGET_RUNTIME_ROLLBACK_RELEASE_IDS",
            frozenset({previous_id}),
        ):
            planned = release.activate_canary_release(
                release_base=self.release_base,
                release_id=str(target["release_id"]),
                active_link=active,
                canary_manifest=manifest,
                apply=False,
                launchagent_dir=self.launchagents,
                canary_pre_activation_verifier=self.canary_preflight_proof,
                preview_state_inspector=lambda **_values: observed_surface,
            )

        self.assertEqual(planned["status"], "production_canary_planned")
        self.assertTrue(planned["preview_non_mutation_verified"])
        self.assertEqual(planned["previous_release_id"], previous_id)
        self.assertEqual(active.resolve(), Path(str(previous["release_dir"])))

    def test_historical_priority_is_rollback_only_and_cannot_be_target_built(
        self,
    ) -> None:
        template_path = self.source / "config.example.json"
        template = json.loads(template_path.read_text(encoding="utf-8"))
        template["model"]["service_tier"] = "priority"
        template_path.write_text(json.dumps(template), encoding="utf-8")
        with (
            mock.patch.object(
                release,
                "REQUIRED_MODEL_CONTRACT",
                release.HISTORICAL_PRIORITY_MODEL_CONTRACT,
            ),
            mock.patch.object(
                release, "_model_contract_config_matches", return_value=True
            ),
        ):
            built = self.build()
        release_root = Path(str(built["release_dir"]))
        with self.assertRaises(release.ReleaseError):
            release.verify_release(release_root)
        verified = release.verify_rollback_release(release_root)
        self.assertEqual(verified["historical_contract"], "historical_priority_v1")
        with self.assertRaisesRegex(
            release.ReleaseError, "concurrent_target_model_contract_required"
        ):
            release.build_release(
                source_root=self.source,
                release_base=self.release_base,
                runtime_data_root=self.base / "priority-target-runtime",
                skip_tests=True,
                model_contract=release.HISTORICAL_PRIORITY_MODEL_CONTRACT,
            )

    def test_release_identity_and_verification_bind_executable_modes(self) -> None:
        first = self.build()
        worker = self.source / "bin" / "worker.py"
        worker.chmod(0o755)
        second = self.build()
        self.assertNotEqual(first["release_id"], second["release_id"])
        released_worker = Path(second["release_dir"]) / "bin" / "worker.py"
        self.assertTrue(released_worker.stat().st_mode & 0o111)
        released_worker.chmod(0o644)
        with self.assertRaisesRegex(release.ReleaseError, "source_mode_mismatch"):
            release.verify_release(Path(second["release_dir"]))

    def test_runtime_data_root_is_bound_into_release_identity(self) -> None:
        first = self.build()
        second = release.build_release(
            source_root=self.source,
            release_base=self.release_base,
            runtime_data_root=self.base / "other-runtime-data",
            skip_tests=True,
        )
        self.assertNotEqual(first["release_id"], second["release_id"])

    def test_sealed_mcp_binding_verifier_rejects_launcher_semantic_drift(
        self,
    ) -> None:
        mcp_root = Path(
            "/Users/xiazhibin/.codex/local-study-read-mcp/releases/"
            "21d738a1d74586aab72c8a63dc62c680aa10c6ac837c2bd5041e757ba0e63425"
        )
        source_plugin = ROOT / "plugin/kaoyan-study-intake"
        plugin_root = self.base / "candidate/plugin/kaoyan-study-intake"
        plugin_root.parent.mkdir(parents=True)
        import shutil

        shutil.copytree(source_plugin, plugin_root)
        components = json.loads((plugin_root / "components.json").read_text())
        lock_path = plugin_root / "component-lock.json"
        lock = json.loads(lock_path.read_text())
        runtime = {
            key: components["mcp"][key]
            for key in (
                "python_executable",
                "python_flags",
                "sealed_launcher_path",
                "sealed_launcher_sha256",
                "release_root",
                "release_id",
                "release_manifest_sha256",
            )
        }
        manifest = json.loads((mcp_root / "release.json").read_text())
        lock.update({
            "registry_sha256": release.sha256_file(plugin_root / "components.json"),
            "launcher_sha256": release.sha256_file(plugin_root / "bin/kaoyan-read"),
            "mcp_release_root": str(mcp_root),
            "mcp_release_id": mcp_root.name,
            "mcp_release_manifest_sha256": runtime["release_manifest_sha256"],
            "mcp_server_release": manifest["server_release"],
            "mcp_sealed_runtime": runtime,
            "luna_mcp_servers": {
                subject: {
                    "server_name": f"kaoyan_{subject}_read",
                    "launch_mode": "subject-server",
                    "enabled_tools": [
                        "get_task_context",
                        "read_task_artifact",
                        "list_records",
                        "get_records",
                        "search_records",
                        "query_relations",
                    ],
                    "read_session_schema": "study-read-mcp-read-session.v4",
                }
                for subject in ("math", "cs408", "english")
            },
        })
        lock_path.write_bytes(release.canonical_bytes(lock))
        candidate = plugin_root.parents[1]
        config = {
            "processing_plugin": {
                "root": str(plugin_root),
                "component_lock_path": str(lock_path),
                "mcp_client_python": runtime["python_executable"],
                "mcp_project_root": str(mcp_root),
                "profile": "background",
            }
        }
        release._verify_target_sealed_mcp_binding(candidate, config)
        wrapper = plugin_root / "bin/kaoyan-read"
        wrapper.write_text(wrapper.read_text() + "# semantic drift\n")
        lock["launcher_sha256"] = hashlib.sha256(wrapper.read_bytes()).hexdigest()
        lock_path.write_bytes(release.canonical_bytes(lock))
        with self.assertRaisesRegex(
            release.ReleaseError, "release_mcp_sealed_binding_invalid"
        ):
            release._verify_target_sealed_mcp_binding(candidate, config)

    def test_target_build_requires_current_component_generator(self) -> None:
        plugin = self.source / "plugin/kaoyan-study-intake/scripts"
        plugin.mkdir(parents=True)
        generator = plugin / "generate_manifests.py"
        generator.write_text("raise SystemExit(7)\n", encoding="utf-8")
        with self.assertRaisesRegex(
            release.ReleaseError, "historical_test_input_manifest_required"
        ):
            self.build()

    def test_release_tree_is_sealed_read_only(self) -> None:
        built = self.build()
        root = Path(built["release_dir"])
        self.assertEqual(stat.S_IMODE(root.stat().st_mode), 0o555)
        self.assertEqual(stat.S_IMODE((root / "bin").stat().st_mode), 0o555)
        self.assertEqual(stat.S_IMODE((root / "bin" / "worker.py").stat().st_mode), 0o444)
        self.assertEqual(stat.S_IMODE((root / "config.json").stat().st_mode), 0o400)
        self.assertEqual(stat.S_IMODE((root / "release.json").stat().st_mode), 0o444)

    def test_extra_file_directory_bytecode_and_symlink_are_rejected(self) -> None:
        injections = (
            ("rogue_file", "release_tree_file_set_mismatch"),
            ("bytecode", "release_tree_file_set_mismatch"),
            ("empty_directory", "release_tree_directory_set_mismatch"),
            ("symlink", "release_tree_symlink_forbidden"),
        )
        for injection, error in injections:
            with self.subTest(injection=injection):
                runtime = self.base / f"runtime-{injection}"
                built = release.build_release(
                    source_root=self.source,
                    release_base=self.release_base,
                    runtime_data_root=runtime,
                    skip_tests=True,
                )
                root = Path(built["release_dir"])
                if injection == "rogue_file":
                    root.chmod(0o755)
                    path = root / "rogue.py"
                    path.write_text("raise SystemExit(9)\n", encoding="utf-8")
                    path.chmod(0o444)
                    root.chmod(0o555)
                elif injection == "bytecode":
                    parent = root / "bin"
                    parent.chmod(0o755)
                    cache = parent / "__pycache__"
                    cache.mkdir(mode=0o755)
                    path = cache / "worker.cpython-313.pyc"
                    path.write_bytes(b"unmanifested-bytecode")
                    path.chmod(0o444)
                    cache.chmod(0o555)
                    parent.chmod(0o555)
                elif injection == "empty_directory":
                    root.chmod(0o755)
                    (root / "extra").mkdir(mode=0o555)
                    root.chmod(0o555)
                else:
                    root.chmod(0o755)
                    os.symlink("config.json", root / "rogue-link")
                    root.chmod(0o555)
                with self.assertRaisesRegex(release.ReleaseError, error):
                    release.verify_release(root)

    def test_config_cannot_be_rebound_by_updating_generated_hash(self) -> None:
        built = self.build()
        root = Path(built["release_dir"])
        config_path = root / "config.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))
        config["runtime_root"] = str(self.base / "identity-not-bound")
        config_path.chmod(0o600)
        config_path.write_bytes(release.canonical_bytes(config))
        config_path.chmod(0o400)

        manifest_path = root / "release.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["generated_files"]["config.json"] = release.sha256_file(config_path)
        manifest_path.chmod(0o600)
        manifest_path.write_bytes(release.canonical_bytes(manifest))
        manifest_path.chmod(0o444)
        with self.assertRaisesRegex(
            release.ReleaseError,
            "release_config_derivation_mismatch",
        ):
            release.verify_release(root)

    def test_manifest_semantics_and_canonical_encoding_are_enforced(self) -> None:
        built = self.build()
        root = Path(built["release_dir"])
        manifest_path = root / "release.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["formal_write_count"] = 99
        manifest_path.chmod(0o600)
        manifest_path.write_bytes(release.canonical_bytes(manifest))
        manifest_path.chmod(0o444)
        with self.assertRaisesRegex(release.ReleaseError, "release_manifest_invalid"):
            release.verify_release(root)

    def test_owner_xattr_and_acl_metadata_are_verified(self) -> None:
        built = self.build()
        root = Path(built["release_dir"])
        worker = root / "bin" / "worker.py"
        try:
            os.setxattr(worker, "com.xiazhibin.release-test", b"1")
        except (AttributeError, OSError):
            pass
        else:
            with self.assertRaisesRegex(
                release.ReleaseError, "release_node_xattrs_present"
            ):
                release.verify_release(root)
            os.removexattr(worker, "com.xiazhibin.release-test")

        with mock.patch.object(release, "_node_has_acl", return_value=True):
            with self.assertRaisesRegex(
                release.ReleaseError, "release_node_acl_present"
            ):
                release.verify_release(root)

    def test_source_symlink_is_rejected_before_build(self) -> None:
        os.symlink("worker.py", self.source / "bin" / "worker-link.py")
        with self.assertRaisesRegex(
            release.ReleaseError,
            "release_source_symlink_forbidden",
        ):
            self.build()

    def test_activation_is_dry_run_by_default_and_atomic_when_applied(self) -> None:
        built = self.build(passed=True)
        active = self.base / "current"
        planned = release.activate_release(
            release_base=self.release_base,
            release_id=built["release_id"],
            active_link=active,
            apply=False,
            operation="activate",
        )
        self.assertEqual(planned["status"], "planned")
        self.assertFalse(active.exists())
        self.assertEqual(len(planned["service_topology"]), 4)

        runner = self.FakeRunner()
        applied = release.activate_release(
            release_base=self.release_base,
            release_id=built["release_id"],
            active_link=active,
            apply=True,
            operation="activate",
            expected_current="absent",
            command_runner=runner,
            launchagent_dir=self.launchagents,
            post_activation_verifier=self.post_activation_proof,
        )
        self.assertEqual(applied["status"], "activated")
        self.assertTrue(active.is_symlink())
        self.assertEqual(active.resolve(), Path(built["release_dir"]))
        self.assertTrue(applied["launchagent_changed"])
        self.assertEqual(len(runner.commands), 9)
        self.assertTrue(all(command[0] == "launchctl" for command in runner.commands))
        for subject in ("math", "cs408", "english"):
            subject_commands = [
                command
                for command in runner.commands
                if f"com.xiazhibin.study-intake-preprocessor.{subject}"
                in " ".join(command)
            ]
            self.assertEqual(
                [command[1] for command in subject_commands],
                ["disable", "bootout"],
            )
            self.assertEqual(
                applied["service_activation_policy"][subject],
                "disabled_drained",
            )
        subject_labels = {
            f"com.xiazhibin.study-intake-preprocessor.{subject}"
            for subject in ("math", "cs408", "english")
        }
        self.assertFalse(any(
            command[1] in {"enable", "bootstrap"}
            and any(label in " ".join(command) for label in subject_labels)
            for command in runner.commands
        ))
        self.assertTrue(all(
            (self.launchagents / f"{service['label']}.plist").is_file()
            for service in release.CONCURRENT_TOPOLOGY
        ))
        self.assertTrue(
            (
                self.release_base
                / "deployments"
                / f"{applied['prepare_receipt_sha256']}.json"
            ).is_file()
        )
        self.assertTrue(
            (
                self.release_base
                / "deployments"
                / f"{applied['postcommit_receipt_sha256']}.json"
            ).is_file()
        )

    def test_service_deactivation_accepts_only_verified_already_absent_bootout(
        self,
    ) -> None:
        service = release._service_commands(
            [release.CONCURRENT_TOPOLOGY[1]], self.launchagents, uid=501
        )[0]

        class AlreadyAbsentRunner:
            def __init__(inner_self) -> None:
                inner_self.commands: list[list[str]] = []

            def __call__(inner_self, command):
                inner_self.commands.append(list(command))
                if command[1] == "bootout":
                    return {"returncode": 5, "stdout": ""}
                if command[1] == "print":
                    return {
                        "returncode": release.LAUNCHCTL_SERVICE_NOT_FOUND_RETURN_CODE,
                        "stdout": "",
                    }
                return {"returncode": 0, "stdout": ""}

        absent_runner = AlreadyAbsentRunner()
        release._run_service_deactivation([service], absent_runner)
        self.assertEqual(
            [command[1] for command in absent_runner.commands],
            ["disable", "bootout", "print"],
        )

        class StillLoadedRunner(AlreadyAbsentRunner):
            def __call__(inner_self, command):
                result = super().__call__(command)
                if command[1] == "print":
                    return {"returncode": 0, "stdout": "loaded"}
                return result

        with self.assertRaisesRegex(
            release.ReleaseError, "service_bootout_failed:cs408"
        ):
            release._run_service_deactivation([service], StillLoadedRunner())

    def test_previous_canary_deactivation_uses_byte_locked_snapshot_before_status(
        self,
    ) -> None:
        previous_release_id = "6" * 64
        foreign_release_id = "5" * 64
        runtime_root = self.base / "previous-canary-direct-deactivation"
        active_link = runtime_root / "current"
        previous_release = self.base / "previous-release"
        (previous_release / "bin").mkdir(parents=True)
        (previous_release / "bin" / "preprocess_dispatcher.py").write_text(
            "# fixture\n", encoding="utf-8"
        )
        (previous_release / "config.json").write_text(
            "{}\n", encoding="utf-8"
        )
        state_root = (
            runtime_root / "dispatch" / "state" / "production-canary"
        )
        state_root.mkdir(parents=True)
        telemetry_path = (
            runtime_root
            / "dispatch"
            / "state"
            / "production-canary-concurrency-telemetry.json"
        )
        telemetry_path.write_text(
            json.dumps({"release_id": foreign_release_id}) + "\n",
            encoding="utf-8",
        )
        snapshots: dict[str, Mapping[str, Any]] = {}
        for subject in release.RESUMABLE_SUBJECTS:
            gate = {
                "schema_version": "study-intake-production-canary-state-v3",
                "subject": subject,
                "release_id": previous_release_id,
                "status": "production_canary_active",
                "state": "failed_drained" if subject == "english" else "armed",
                "active_task_count": 0,
                "luna_consumer_enabled": subject != "english",
                "formal_write_count": 0,
                "sol_enabled": False,
            }
            path = state_root / f"{subject}.json"
            path.write_bytes(release.canonical_bytes(gate) + b"\n")
            snapshots[subject] = release._snapshot_optional_file_bytes(path)

        class ForeignTelemetryRunner:
            def __init__(inner_self) -> None:
                inner_self.commands: list[list[str]] = []
                inner_self.deactivated: set[str] = set()

            def __call__(inner_self, command):
                inner_self.commands.append(list(command))
                subject = command[command.index("--subject") + 1]
                gate_path = state_root / f"{subject}.json"
                gate = json.loads(gate_path.read_text(encoding="utf-8"))
                if "deactivate-canary" in command:
                    gate.update(
                        {
                            "status": "production_canary_inactive",
                            "state": "inactive_rolled_back",
                            "luna_consumer_enabled": False,
                        }
                    )
                    gate_path.write_bytes(
                        release.canonical_bytes(gate) + b"\n"
                    )
                    telemetry_path.write_text(
                        json.dumps({"release_id": previous_release_id}) + "\n",
                        encoding="utf-8",
                    )
                    inner_self.deactivated.add(subject)
                    payload = gate
                    returncode = 0
                elif command[-1] == "status":
                    telemetry = json.loads(
                        telemetry_path.read_text(encoding="utf-8")
                    )
                    if (
                        subject not in inner_self.deactivated
                        or telemetry.get("release_id") != previous_release_id
                    ):
                        payload = {
                            "error_code": (
                                "production_canary_concurrency_telemetry_binding_invalid"
                            )
                        }
                        returncode = 1
                    else:
                        payload = {
                            "subject": subject,
                            "active_count": 0,
                            "claimed_total": 0,
                            "canary_gate": gate,
                        }
                        returncode = 0
                else:
                    self.fail(f"unexpected command: {command}")
                return subprocess.CompletedProcess(
                    command,
                    returncode,
                    json.dumps(payload),
                    "",
                )

        runner = ForeignTelemetryRunner()
        proof = release._deactivate_previous_canary_states(
            previous_release_id=previous_release_id,
            previous_release=previous_release,
            active_link=active_link,
            runtime_data_root=runtime_root,
            previous_canary_state_snapshots=snapshots,
            runner=runner,
        )
        self.assertEqual(proof["status"], "previous_canary_deactivated")
        self.assertEqual(
            [row["state"] for row in proof["subjects"]],
            ["inactive_rolled_back"] * 3,
        )
        self.assertEqual(
            [
                "deactivate" if "deactivate-canary" in command else "status"
                for command in runner.commands
            ],
            ["deactivate", "status"] * 3,
        )
        self.assertEqual(
            runner.deactivated, set(release.RESUMABLE_SUBJECTS)
        )

    def test_previous_canary_deactivation_preserves_absent_states(self) -> None:
        runtime_root = self.base / "previous-canary-absent"
        active_link = runtime_root / "current"
        state_root = (
            runtime_root / "dispatch" / "state" / "production-canary"
        )
        snapshots = {
            subject: release._snapshot_optional_file_bytes(
                state_root / f"{subject}.json"
            )
            for subject in release.RESUMABLE_SUBJECTS
        }
        runner = mock.Mock(side_effect=AssertionError("runner must not run"))
        proof = release._deactivate_previous_canary_states(
            previous_release_id="6" * 64,
            previous_release=self.base / "previous-release",
            active_link=active_link,
            runtime_data_root=runtime_root,
            previous_canary_state_snapshots=snapshots,
            runner=runner,
        )
        runner.assert_not_called()
        self.assertEqual(
            proof["subjects"],
            [
                {"subject": subject, "state": "not_armed"}
                for subject in release.RESUMABLE_SUBJECTS
            ],
        )

    def test_previous_canary_deactivation_rejects_snapshot_and_gate_drift(
        self,
    ) -> None:
        previous_release_id = "6" * 64

        for case in (
            "snapshot_path",
            "current_bytes",
            "gate_subject",
            "gate_release",
            "active_task_count",
        ):
            with self.subTest(case=case):
                runtime_root = self.base / f"previous-canary-drift-{case}"
                active_link = runtime_root / "current"
                state_root = (
                    runtime_root
                    / "dispatch"
                    / "state"
                    / "production-canary"
                )
                state_root.mkdir(parents=True)
                snapshots: dict[str, Mapping[str, Any]] = {}
                gate = {
                    "schema_version": (
                        "study-intake-production-canary-state-v3"
                    ),
                    "subject": (
                        "cs408" if case == "gate_subject" else "math"
                    ),
                    "release_id": (
                        "7" * 64
                        if case == "gate_release"
                        else previous_release_id
                    ),
                    "status": "production_canary_active",
                    "state": "armed",
                    "active_task_count": (
                        1 if case == "active_task_count" else 0
                    ),
                    "luna_consumer_enabled": True,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                }
                math_path = state_root / "math.json"
                math_path.write_bytes(release.canonical_bytes(gate) + b"\n")
                snapshots["math"] = release._snapshot_optional_file_bytes(
                    math_path
                )
                for subject in ("cs408", "english"):
                    snapshots[subject] = release._snapshot_optional_file_bytes(
                        state_root / f"{subject}.json"
                    )
                if case == "snapshot_path":
                    snapshots["math"] = {
                        **snapshots["math"],
                        "path": str(state_root / "other.json"),
                    }
                elif case == "current_bytes":
                    math_path.write_bytes(math_path.read_bytes() + b" ")

                runner = mock.Mock(
                    side_effect=AssertionError("runner must not run")
                )
                with self.assertRaisesRegex(
                    release.ReleaseError,
                    "previous_canary_deactivation_invalid:math",
                ):
                    release._deactivate_previous_canary_states(
                        previous_release_id=previous_release_id,
                        previous_release=self.base / "previous-release",
                        active_link=active_link,
                        runtime_data_root=runtime_root,
                        previous_canary_state_snapshots=snapshots,
                        runner=runner,
                    )
                runner.assert_not_called()

    def test_expected_current_mismatch_and_service_failure_restore_previous(self) -> None:
        first = self.build(passed=True)
        active = self.base / "current"
        release.activate_release(
            release_base=self.release_base,
            release_id=first["release_id"],
            active_link=active,
            apply=True,
            operation="activate",
            expected_current="absent",
            command_runner=self.FakeRunner(),
            launchagent_dir=self.launchagents,
            post_activation_verifier=self.post_activation_proof,
        )
        (self.source / "bin" / "worker.py").write_text("print('v2')\n", encoding="utf-8")
        second = self.build(passed=True)

        mismatch_runner = self.FakeRunner()
        with self.assertRaisesRegex(release.ReleaseError, "expected_current_mismatch"):
            release.activate_release(
                release_base=self.release_base,
                release_id=second["release_id"],
                active_link=active,
                apply=True,
                operation="activate",
                expected_current="0" * 64,
                command_runner=mismatch_runner,
                launchagent_dir=self.launchagents,
            )
        self.assertEqual(mismatch_runner.commands, [])
        self.assertEqual(active.resolve(), Path(first["release_dir"]))

        no_drain_runner = self.FakeRunner()
        with self.assertRaisesRegex(release.ReleaseError, "drain_receipt_required"):
            release.activate_release(
                release_base=self.release_base,
                release_id=second["release_id"],
                active_link=active,
                apply=True,
                operation="activate",
                expected_current=first["release_id"],
                command_runner=no_drain_runner,
                launchagent_dir=self.launchagents,
            )
        self.assertEqual(no_drain_runner.commands, [])

        installed_math = self.launchagents / "com.xiazhibin.study-intake-preprocessor.math.plist"
        previous_math_plist = installed_math.read_bytes()
        class FailingDashboardBootstrapRunner(self.FakeRunner):
            failed_target_bootstrap = False

            def __call__(inner_self, command):
                if (
                    command[:2] == ["launchctl", "bootstrap"]
                    and "com.xiazhibin.study-intake-dashboard"
                    in " ".join(command)
                    and not inner_self.failed_target_bootstrap
                ):
                    inner_self.failed_target_bootstrap = True
                    inner_self.commands.append(list(command))
                    return {"returncode": 1, "stdout": ""}
                return super().__call__(command)

        failing_runner = FailingDashboardBootstrapRunner()
        with self.assertRaisesRegex(
            release.ReleaseError, "activation_service_failed_rolled_back"
        ):
            release.activate_release(
                release_base=self.release_base,
                release_id=second["release_id"],
                active_link=active,
                apply=True,
                operation="activate",
                expected_current=first["release_id"],
                drain_receipt=self.make_drain_receipt(
                    first["release_id"], second["release_id"], active
                ),
                command_runner=failing_runner,
                launchagent_dir=self.launchagents,
            )
        self.assertEqual(active.resolve(), Path(first["release_dir"]))
        self.assertEqual(installed_math.read_bytes(), previous_math_plist)
        self.assertFalse(any(
            command[:2] == ["launchctl", "enable"]
            and command[-1].endswith(".math")
            for command in failing_runner.commands
        ))
        self.assertTrue(any(
            "bootstrap" in command
            for command in (" ".join(row) for row in failing_runner.commands)
        ))
        receipts = [
            json.loads(receipt.read_text(encoding="utf-8"))
            for receipt in (self.release_base / "deployments").glob("*.json")
        ]
        self.assertTrue(any(row.get("status") == "rolled_back" for row in receipts))
        drain_positions = [
            index
            for index, command in enumerate(failing_runner.commands)
            if command and command[-1] == "drain"
        ]
        self.assertEqual(len(drain_positions), 3)
        failed_bootstrap = next(
            index
            for index, command in enumerate(failing_runner.commands)
            if command[:2] == ["launchctl", "bootstrap"]
            and "com.xiazhibin.study-intake-dashboard"
            in " ".join(command)
        )
        post_failure_disables = [
            index
            for index, command in enumerate(failing_runner.commands)
            if failed_bootstrap < index < min(drain_positions)
            and "disable" in command
        ]
        rollback_activation_start = min(
            index
            for index, command in enumerate(failing_runner.commands)
            if index > max(drain_positions) and "disable" in command
        )
        post_drain_bootouts = [
            index
            for index, command in enumerate(failing_runner.commands)
            if max(drain_positions) < index < rollback_activation_start
            and "bootout" in command
        ]
        self.assertEqual(len(post_failure_disables), 4)
        self.assertEqual(len(post_drain_bootouts), 4)
        self.assertLess(max(post_failure_disables), min(drain_positions))
        self.assertLess(max(drain_positions), min(post_drain_bootouts))

    def test_release_contains_four_launchagent_templates(self) -> None:
        with self.assertRaisesRegex(
            release.ReleaseError, "historical_test_input_manifest_required"
        ):
            release.build_release(
                source_root=ROOT,
                release_base=self.base / "full-release-base",
                runtime_data_root=self.runtime_data,
                skip_tests=True,
            )
        self.assertEqual(len(release.CONCURRENT_TOPOLOGY), 4)
        self.assertTrue(
            all((ROOT / row["template"]).is_file() for row in release.CONCURRENT_TOPOLOGY)
        )
        return
        released_root = Path(full["release_dir"])
        released_config = json.loads(
            (released_root / "config.json").read_text(encoding="utf-8")
        )
        self.assertTrue(released_config["adapters"]["math"]["enabled"])
        subjects = {"math": "math", "cs408": "cs408", "english": "english"}
        for name, subject in subjects.items():
            topology = next(row for row in release.SERVICE_TOPOLOGY if row["name"] == name)
            payload = plistlib.loads((ROOT / topology["template"]).read_bytes())
            self.assertTrue((released_root / topology["template"]).is_file())
            self.assertEqual(payload["Label"], topology["label"])
            self.assertEqual(payload["ProgramArguments"], [
                "/usr/local/bin/python3",
                "${CURRENT_ROOT}/bin/preprocess_dispatcher.py",
                "--config",
                "${CURRENT_ROOT}/config.json",
                "--subject",
                subject,
                "run",
            ])
            self.assertEqual(payload["EnvironmentVariables"]["PYTHONDONTWRITEBYTECODE"], "1")
            self.assertEqual(payload["EnvironmentVariables"]["PYTHONUNBUFFERED"], "1")
            self.assertEqual(payload["WorkingDirectory"], "${CURRENT_ROOT}")

        dashboard = next(
            row for row in release.SERVICE_TOPOLOGY if row["name"] == "dashboard"
        )
        payload = plistlib.loads((ROOT / dashboard["template"]).read_bytes())
        self.assertTrue((released_root / dashboard["template"]).is_file())
        self.assertEqual(payload["Label"], "com.xiazhibin.study-intake-dashboard")
        self.assertEqual(
            payload["ProgramArguments"],
            ["/usr/local/bin/python3", "${CURRENT_ROOT}/dashboard/server.py"],
        )
        rendered = release._render_launchagent_plists(
            released_root,
            full["service_topology"],
            self.base / "rendered-current",
            self.runtime_data.resolve(),
        )
        for service in full["service_topology"]:
            rendered_payload = plistlib.loads(rendered[service["label"]])
            self.assertNotIn("${", rendered[service["label"]].decode("utf-8"))
            self.assertEqual(
                rendered_payload["WorkingDirectory"],
                str(self.base / "rendered-current"),
            )
            self.assertEqual(rendered_payload["ProgramArguments"][0], "/usr/local/bin/python3")
            if service["name"] == "dashboard":
                self.assertEqual(
                    rendered_payload["EnvironmentVariables"]["STUDY_PREPROCESSOR_DASHBOARD_PROJECTION"],
                    str(self.runtime_data.resolve() / "state/dashboard_projection.json"),
                )
            else:
                self.assertEqual(
                    rendered_payload["ProgramArguments"][3],
                    str(self.base / "rendered-current" / "config.json"),
                )

    def test_legacy_rollback_profile_is_closed_smoked_and_receipted(self) -> None:
        expected = "c" * 64
        current, _ = self.make_legacy_current(expected)
        built = release.build_legacy_rollback(
            current_link=current,
            expected_current=expected,
            release_base=self.release_base,
            runtime_data_root=self.runtime_data,
            skip_tests=True,
            launchagent_dir=self.launchagents,
        )
        root = Path(built["release_dir"])
        verified = release.verify_rollback_release(root)
        self.assertEqual(verified["release_profile"], "legacy_monolith")
        self.assertEqual(verified["source_release_id"], expected)
        self.assertEqual(
            [row["label"] for row in verified["service_topology"]],
            [
                "com.xiazhibin.study-intake-preprocessor",
                "com.xiazhibin.study-intake-dashboard",
            ],
        )
        self.assertFalse(any(root.rglob("*.pyc")))
        receipt = built["rollback_verification"]
        self.assertEqual(receipt["status"], "verified")
        self.assertEqual(receipt["model_call_count"], 0)
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertTrue(
            (
                self.release_base
                / "rollback-verifications"
                / f"{receipt['verification_receipt_sha256']}.json"
            ).is_file()
        )

    def test_legacy_snapshot_uses_current_config_and_hermetic_parity_reference(
        self,
    ) -> None:
        expected = "9" * 64
        current, source = self.make_legacy_current(expected)
        stale_test = source / "tests" / "test_release_manager.py"
        stale_test.parent.mkdir(parents=True)
        original_test = '''from pathlib import Path
import json

LIVE_CONFIG = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/config.json"
)
'''
        stale_test.write_text(original_test, encoding="utf-8")
        source_config_before = (source / "config.json").read_bytes()

        built = release.build_legacy_rollback(
            current_link=current,
            expected_current=expected,
            release_base=self.release_base,
            runtime_data_root=self.runtime_data,
            skip_tests=True,
            launchagent_dir=self.launchagents,
        )
        root = Path(built["release_dir"])
        released_test = (root / "tests" / "test_release_manager.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(release.LEGACY_PARITY_CONFIG_ENV, released_test)
        self.assertTrue((root / release.LEGACY_PARITY_FIXTURE).is_file())
        self.assertEqual(stale_test.read_text(encoding="utf-8"), original_test)
        self.assertEqual((source / "config.json").read_bytes(), source_config_before)

        config = json.loads((root / "config.json").read_text(encoding="utf-8"))
        self.assertEqual(
            config["release"]["manifest_path"],
            str(root / release.LEGACY_RUNTIME_MANIFEST),
        )
        identity = json.loads(
            (root / release.LEGACY_RUNTIME_MANIFEST).read_text(encoding="utf-8")
        )
        self.assertEqual(identity["release_id"], built["release_id"])
        self.assertEqual(identity["source_release_id"], expected)
        self.assertTrue(built["legacy_snapshot_proof"]["parity_reference_adjusted"])
        self.assertEqual(
            built["legacy_snapshot_proof"]["source_config_sha256"],
            release.sha256_bytes(source_config_before),
        )

    def test_profile_transition_stops_previous_and_starts_target_topology(self) -> None:
        concurrent = self.build(passed=True)
        expected = "c" * 64
        current_source, _ = self.make_legacy_current(expected)
        legacy = release.build_legacy_rollback(
            current_link=current_source,
            expected_current=expected,
            release_base=self.release_base,
            runtime_data_root=self.runtime_data,
            skip_tests=True,
            launchagent_dir=self.launchagents,
        )
        active = self.base / "profile-current"
        release.activate_release(
            release_base=self.release_base,
            release_id=concurrent["release_id"],
            active_link=active,
            apply=True,
            operation="activate",
            expected_current="absent",
            command_runner=self.FakeRunner(),
            launchagent_dir=self.launchagents,
            post_activation_verifier=self.post_activation_proof,
        )
        runner = self.FakeRunner()
        release.activate_release(
            release_base=self.release_base,
            release_id=legacy["release_id"],
            active_link=active,
            apply=True,
            operation="rollback",
            expected_current=concurrent["release_id"],
            drain_receipt=self.make_drain_receipt(
                concurrent["release_id"],
                legacy["release_id"],
                active,
                operation="rollback",
            ),
            command_runner=runner,
            launchagent_dir=self.launchagents,
        )
        commands = [" ".join(command) for command in runner.commands]
        self.assertTrue(any("disable" in row and ".math" in row for row in commands))
        self.assertTrue(any("bootout" in row and ".math.plist" in row for row in commands))
        self.assertTrue(any(
            "bootstrap" in row
            and row.endswith("com.xiazhibin.study-intake-preprocessor.plist")
            for row in commands
        ))
        self.assertFalse(any(
            "bootstrap" in row and row.endswith(".math.plist") for row in commands
        ))

    def test_concurrent_activation_requires_tests_and_formal_hash_gate(self) -> None:
        unverified = self.build()
        with self.assertRaisesRegex(
            release.ReleaseError, "concurrent_release_gates_not_passed"
        ):
            release.activate_release(
                release_base=self.release_base,
                release_id=unverified["release_id"],
                active_link=self.base / "unverified-current",
                apply=False,
                operation="activate",
            )
        with self.assertRaisesRegex(
            release.ReleaseError, "historical_test_input_manifest_required"
        ):
            release.build_release(
                source_root=self.source,
                release_base=self.base / "ungated-build",
                runtime_data_root=self.runtime_data,
                skip_tests=False,
            )

    def test_forbidden_release_ids_are_rejected_before_any_state_change(self) -> None:
        for release_id in release.FORBIDDEN_ACTIVATION_RELEASE_IDS:
            with self.subTest(release_id=release_id):
                with self.assertRaisesRegex(
                    release.ReleaseError, "release_activation_forbidden"
                ):
                    release.activate_release(
                        release_base=self.release_base,
                        release_id=release_id,
                        active_link=self.base / "forbidden-current",
                        apply=False,
                        operation="activate",
                    )

    def test_drain_receipt_is_hmac_bound_and_live_gate_is_rechecked(self) -> None:
        first = self.build(passed=True)
        active = self.base / "signed-current"
        active.symlink_to(first["release_dir"])
        (self.source / "bin" / "worker.py").write_text("print('next')\n")
        second = self.build(passed=True)
        receipt_path = self.make_drain_receipt(
            first["release_id"], second["release_id"], active
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(receipt["schema_version"], release.DRAIN_RECEIPT_SCHEMA)
        self.assertEqual(receipt["authority"]["algorithm"], "HMAC-SHA256")
        self.assertTrue(receipt["claim_gate"]["held"])
        self.assertIn("process_snapshot", receipt)
        receipt["active_count"] = 1
        receipt_path.write_bytes(release.canonical_bytes(receipt))
        receipt_path.chmod(0o600)
        runner = self.FakeRunner()
        with self.assertRaisesRegex(
            release.ReleaseError, "deployment_authority_invalid"
        ):
            release.activate_release(
                release_base=self.release_base,
                release_id=second["release_id"],
                active_link=active,
                apply=True,
                operation="activate",
                expected_current=first["release_id"],
                drain_receipt=receipt_path,
                command_runner=runner,
                launchagent_dir=self.launchagents,
                post_activation_verifier=self.post_activation_proof,
            )
        self.assertEqual(runner.commands, [])
        self.assertEqual(active.resolve(), Path(first["release_dir"]))

    def test_deployment_scanner_ignores_valid_target_bound_drain_receipt(self) -> None:
        target_release_id = "a" * 64
        path = self.write_scanner_drain_receipt(target_release_id)

        self.assertTrue(path.is_file())
        self.assertEqual(release._deployment_receipt_objects(self.release_base), {})

    def test_valid_drain_receipt_does_not_close_unresolved_prepare(self) -> None:
        built = self.build(passed=True)
        prepare_sha256 = release._write_deployment_receipt(
            self.release_base,
            {
                "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
                "operation": "activate",
                "release_id": str(built["release_id"]),
                "observed_current": None,
                "formal_write_count": 0,
            },
        )
        self.write_scanner_drain_receipt(str(built["release_id"]))

        with self.assertRaisesRegex(
            release.ReleaseError, "deployment_transaction_unresolved"
        ):
            release._assert_no_unresolved_deployment_transaction(self.release_base)
        self.assertTrue(
            (
                self.release_base / "deployments" / f"{prepare_sha256}.json"
            ).is_file()
        )

    def test_deployment_scanner_rejects_invalid_named_drain_receipts(self) -> None:
        target_release_id = "b" * 64
        cases = (
            ("bad_hmac", {"tamper_authority": True}),
            ("wrong_target", {"target_release_id": "c" * 64}),
            ("wrong_schema", {"schema_version": "wrong-schema"}),
            ("formal_write", {"formal_write_count": 1}),
            ("noncanonical", {"noncanonical": True}),
            ("wrong_filename", {"filename_suffix": "-extra"}),
        )
        for name, kwargs in cases:
            with self.subTest(name=name):
                path = self.write_scanner_drain_receipt(target_release_id, **kwargs)
                with self.assertRaisesRegex(
                    release.ReleaseError, "deployment_receipt_invalid"
                ):
                    release._deployment_receipt_objects(self.release_base)
                path.unlink()

    def test_explicit_concurrent_rollback_keeps_all_dispatchers_disabled(self) -> None:
        first = self.build(passed=True)
        active = self.base / "paused-rollback-current"
        release.activate_release(
            release_base=self.release_base,
            release_id=first["release_id"],
            active_link=active,
            apply=True,
            operation="activate",
            expected_current="absent",
            command_runner=self.FakeRunner(),
            launchagent_dir=self.launchagents,
            post_activation_verifier=self.post_activation_proof,
        )
        (self.source / "bin" / "worker.py").write_text(
            "print('second')\n", encoding="utf-8"
        )
        second = self.build(passed=True)
        release.activate_release(
            release_base=self.release_base,
            release_id=second["release_id"],
            active_link=active,
            apply=True,
            operation="activate",
            expected_current=first["release_id"],
            drain_receipt=self.make_drain_receipt(
                first["release_id"], second["release_id"], active
            ),
            command_runner=self.FakeRunner(),
            launchagent_dir=self.launchagents,
            post_activation_verifier=self.post_activation_proof,
        )
        runner = self.FakeRunner()
        rolled_back = release.activate_release(
            release_base=self.release_base,
            release_id=first["release_id"],
            active_link=active,
            apply=True,
            operation="rollback",
            expected_current=second["release_id"],
            drain_receipt=self.make_drain_receipt(
                second["release_id"], first["release_id"], active
            ),
            command_runner=runner,
            launchagent_dir=self.launchagents,
            post_activation_verifier=self.post_rollback_proof,
        )
        self.assertEqual(active.resolve(), Path(first["release_dir"]))
        self.assertEqual(
            rolled_back["service_activation_policy"],
            {
                "math": "disabled_drained",
                "cs408": "disabled_drained",
                "english": "disabled_drained",
                "dashboard": "running",
            },
        )
        rollback_commands = [" ".join(command) for command in runner.commands]
        for subject in ("math", "cs408", "english"):
            self.assertTrue(any(
                "disable" in command and f".{subject}" in command
                for command in rollback_commands
            ))
            self.assertFalse(any(
                "bootstrap" in command and command.endswith(f".{subject}.plist")
                for command in rollback_commands
            ))
        self.assertTrue(any(
            "bootstrap" in command
            and command.endswith("com.xiazhibin.study-intake-dashboard.plist")
            for command in rollback_commands
        ))

    def test_rollback_drain_rejects_stale_claim_and_does_not_switch_back(self) -> None:
        first = self.build(passed=True)
        active = self.base / "stale-rollback-current"
        active.symlink_to(first["release_dir"])
        (self.source / "bin" / "worker.py").write_text(
            "print('stale-rollback-target')\n", encoding="utf-8"
        )
        second = self.build(passed=True)
        receipt_path = self.make_drain_receipt(
            first["release_id"], second["release_id"], active
        )

        def fail_health(**_kwargs):
            raise release.ReleaseError("injected_post_activation_failure")

        def stale_drain(**_kwargs):
            return {
                "schema_version": "study-intake-rollback-drain-v1",
                "status": "drained",
                "release_id": second["release_id"],
                "runtime_data_root": str(self.runtime_data.resolve()),
                "subjects": [
                    {
                        "subject": subject,
                        "active_count": 0,
                        "stale_count": 1 if subject == "cs408" else 0,
                        "claimed_total": 1 if subject == "cs408" else 0,
                        "status_sha256": "d" * 64,
                    }
                    for subject in ("math", "cs408", "english")
                ],
                "formal_write_count": 0,
            }

        runner = self.FakeRunner()
        with self.assertRaisesRegex(
            release.ReleaseError, "activation_failed_rollback_blocked"
        ):
            release.activate_release(
                release_base=self.release_base,
                release_id=second["release_id"],
                active_link=active,
                apply=True,
                operation="activate",
                expected_current=first["release_id"],
                drain_receipt=receipt_path,
                command_runner=runner,
                launchagent_dir=self.launchagents,
                post_activation_verifier=fail_health,
                rollback_drain_controller=stale_drain,
            )
        self.assertEqual(active.resolve(), Path(second["release_dir"]))
        joined = [" ".join(command) for command in runner.commands]
        self.assertFalse(
            any(
                "bootstrap" in command
                and first["release_id"] in command
                for command in joined
            )
        )

    def test_default_rollback_drain_rejects_active_zero_with_stale_claim(self) -> None:
        target = self.base / ("e" * 64)
        (target / "bin").mkdir(parents=True)
        (target / "bin" / "preprocess_dispatcher.py").write_text(
            "# fixture\n", encoding="utf-8"
        )
        (target / "config.json").write_text("{}\n", encoding="utf-8")

        def runner(command):
            subject = command[command.index("--subject") + 1]
            return {
                "returncode": 0,
                "stdout": json.dumps(
                    {
                        "subject": subject,
                        "draining": True,
                        "active_count": 0,
                        "stale_count": 1,
                        "claimed_total": 1,
                    }
                ),
            }

        with self.assertRaisesRegex(
            release.ReleaseError, "rollback_drain_failed:math"
        ):
            release._default_drain_before_rollback(
                target=target,
                runtime_data_root=self.runtime_data,
                runner=runner,
            )

    def test_live_claim_gate_covers_stop_snapshot_and_link_switch(self) -> None:
        first = self.build(passed=True)
        active = self.base / "gate-current"
        active.symlink_to(first["release_dir"])
        (self.source / "bin" / "worker.py").write_text("print('gated-next')\n")
        second = self.build(passed=True)
        receipt_path = self.make_drain_receipt(
            first["release_id"], second["release_id"], active
        )
        events: list[str] = []
        snapshots = 0

        class OrderedRunner(self.FakeRunner):
            def __call__(inner_self, command):
                events.append("command:" + command[1])
                return super().__call__(command)

        def inspector(_previous, _active):
            nonlocal snapshots
            snapshots += 1
            gate = self.runtime_data / "state" / "worker.lock"
            descriptor = os.open(gate, os.O_RDWR)
            try:
                import fcntl

                with self.assertRaises(BlockingIOError):
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(descriptor)
            events.append(f"snapshot:{snapshots}")
            return {
                "schema_version": release.PROCESS_SNAPSHOT_SCHEMA,
                "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "legacy_worker_pids": [777] if snapshots == 1 else [],
                "dispatcher_pids": [],
                "dashboard_pids": [],
                "luna_pids": [],
                "process_set_sha256": "d" * 64,
            }

        result = release.activate_release(
            release_base=self.release_base,
            release_id=second["release_id"],
            active_link=active,
            apply=True,
            operation="activate",
            expected_current=first["release_id"],
            drain_receipt=receipt_path,
            command_runner=OrderedRunner(),
            launchagent_dir=self.launchagents,
            process_inspector=inspector,
            post_activation_verifier=self.post_activation_proof,
        )
        self.assertEqual(snapshots, 2)
        self.assertLess(events.index("snapshot:1"), events.index("command:disable"))
        self.assertLess(events.index("command:bootout"), events.index("snapshot:2"))
        self.assertEqual(
            result["drain_verification"]["authority_status"], "hmac_verified"
        )

    def test_default_post_activation_verifier_keeps_all_dispatchers_disabled_drained(self) -> None:
        built = self.build(passed=True)
        target = Path(built["release_dir"])
        active = self.base / "health-current"
        active.symlink_to(target)
        topology = release._service_commands(
            built["service_topology"], self.launchagents, uid=501
        )
        rendered = release._render_launchagent_plists(
            target,
            built["service_topology"],
            active,
            self.runtime_data.resolve(),
        )
        release._install_launchagent_plists(self.launchagents, rendered)
        activated_at = dt.datetime.now(dt.timezone.utc)

        label_to_pid = {
            service["label"]: 1000 + index
            for index, service in enumerate(topology)
        }

        def launchctl_runner(command):
            if command[:2] == ["launchctl", "print-disabled"]:
                return {
                    "returncode": 0,
                    "stdout": "\n".join(
                        f'"com.xiazhibin.study-intake-preprocessor.{subject}" => disabled'
                        for subject in ("math", "cs408", "english")
                    )
                    + "\n",
                }
            if command[0] != "launchctl":
                subject = command[command.index("--subject") + 1]
                return {
                    "returncode": 0,
                    "stdout": json.dumps({
                        "subject": subject,
                        "draining": True,
                        "active_count": 0,
                        "claimed_total": 0,
                    }),
                }
            label = command[-1].split("/")[-1]
            if label in {
                f"com.xiazhibin.study-intake-preprocessor.{subject}"
                for subject in ("math", "cs408", "english")
            }:
                return {"returncode": 113, "stdout": ""}
            payload = plistlib.loads(
                (self.launchagents / f"{label}.plist").read_bytes()
            )
            output = "\n".join([
                "state = running",
                f"pid = {label_to_pid[label]}",
                f"working directory = {payload['WorkingDirectory']}",
                *payload["ProgramArguments"],
                "",
            ])
            return {"returncode": 0, "stdout": output}

        proof = release._default_post_activation_verifier(
            release_id=built["release_id"],
            target=target,
            active_link=active,
            runtime_data_root=self.runtime_data.resolve(),
            topology=topology,
            rendered_plists=rendered,
            launchagent_dir=self.launchagents,
            runner=launchctl_runner,
            activated_at=activated_at,
            heartbeat_timeout_seconds=1,
        )
        self.assertEqual(proof["service_count"], 4)
        self.assertEqual(proof["status"], "verified_infrastructure_paused")
        self.assertEqual(proof["running_service_count"], 1)
        self.assertEqual(proof["disabled_service_count"], 3)
        self.assertEqual(proof["dispatcher_heartbeat_count"], 0)
        self.assertEqual(
            {row["subject"] for row in proof["subject_controls"]},
            {"math", "cs408", "english"},
        )
        self.assertTrue(all(
            row["enabled"] is False
            and row["running"] is False
            and row["draining"] is True
            for row in proof["subject_controls"]
        ))
        self.assertEqual(proof["model"], "gpt-5.6-luna")
        self.assertEqual(proof["reasoning_effort"], "max")

    def test_canonical_template_preserves_existing_live_semantics(self) -> None:
        if not LIVE_CONFIG.is_file():
            self.skipTest("local live config is unavailable")
        live = json.loads(LIVE_CONFIG.read_text(encoding="utf-8"))
        template = json.loads(
            (ROOT / "config.example.json").read_text(encoding="utf-8")
        )
        self.assertEqual(template["worker"]["poll_interval_seconds"], 1)
        self.assertEqual(template["worker"]["english_poll_interval_seconds"], 1)
        self.assertEqual(template["execution_mode"], "offline")
        self.assertTrue(template["live_execution_gate"]["default_locked"])
        self.assertEqual(
            template["models"]["orchestrator"]["model"],
            "gpt-5.6-terra",
        )
        self.assertEqual(
            template["models"]["reader"]["model"],
            "gpt-5.6-luna",
        )
        # These are the explicitly authorized release/English additions.  All
        # pre-existing non-English settings must remain semantically identical.
        template.pop("release", None)
        template.pop("processing_plugin", None)
        template.pop("english_two_pass_v1", None)
        template.pop("english_legacy_recuration_v1", None)
        template.pop("execution_mode", None)
        template.pop("live_execution_gate", None)
        template.pop("fixture_execution", None)
        template.pop("branch_scheduler", None)
        template.pop("models", None)
        template.get("adapters", {}).pop("english", None)
        template.get("worker", {}).pop("english_poll_interval_seconds", None)
        rendered = json.loads(
            json.dumps(template)
            .replace("${RUNTIME_DATA_ROOT}", str(live["runtime_root"]))
            .replace(
                "${RELEASE_ROOT}",
                "/Users/xiazhibin/.codex/study-intake-preprocessor",
            )
        )
        rendered_flat = flatten(rendered)
        live_flat = flatten(live)
        dispatch = template.get("dispatch")
        if isinstance(dispatch, dict):
            self.assertNotIn("max_jobs_per_scan", template.get("worker", {}))
            self.assertEqual(template["model"]["max_images"], 8)
            self.assertIn("controlled_contract_path", template["cs408_deep_v2"])
            self.assertNotIn("stage_timeout_seconds", template["cs408_deep_v2"])
            self.assertEqual(
                template["cs408_deep_v2"]["soft_runtime_warning_seconds"],
                1800,
            )
            self.assertEqual(dispatch, {
                "authority_required": True,
                "heartbeat_interval_seconds": 15,
                "lease_ttl_seconds": 120,
                "infrastructure_recovery_attempts": 1,
                "production_canary": {
                    "enabled": True,
                    "status": "production_canary_active",
                    "admission": "first_post_activation_producer_capture",
                    "keep_backlog_drained": True,
                    "post_activation_only": True,
                    "initial_canary_inflight_limit": 1,
                    "continuous_concurrency_limit": 20,
                },
            })
        for path in MIGRATION_ALLOWED_PATHS:
            rendered_flat.pop(path, None)
            live_flat.pop(path, None)
        self.assertEqual(rendered_flat, live_flat)

    def test_math_resume_requires_hmac_acceptance_and_zero_backlog(self) -> None:
        built = self.build(passed=True)
        release_id = built["release_id"]
        target = Path(built["release_dir"])
        active = self.base / "current-resume"
        active.symlink_to(target)

        def publish(value: dict, root: Path) -> Path:
            raw = json.dumps(
                value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            digest = release.hashlib.sha256(raw).hexdigest()
            path = root / f"{digest}.json"
            path.write_bytes(raw)
            path.chmod(0o444)
            return path

        report = {
            "schema_version": "study-intake-controlled-replay-report-v1",
            "status": "passed",
            "release_id": release_id,
            "results": [
                {
                    "outcome": "succeeded",
                    "model_call_count": 2,
                    "formal_write_count": 0,
                }
            ],
            "preflight_cases": [
                {
                    "status": "passed",
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
            ],
            "model_call_count": 2,
            "formal_write_count": 0,
        }
        report_path = publish(report, self.base)
        inventory = {}
        for subject, count in {"math": 2, "cs408": 4, "english": 3}.items():
            inventory[subject] = [
                {
                    "capture_id": f"{subject}-{index}",
                    "input_fingerprint": f"{index + 1:064x}",
                    "generation": 1,
                    "status": "succeeded",
                }
                for index in range(count)
            ]
        acceptance = {
            "schema_version": release.MATH_RESUME_ACCEPTANCE_SCHEMA,
            "status": "passed",
            "release_id": release_id,
            "inventory": inventory,
            "unknown_backlog_count": 0,
            "controlled_replay_reports": [
                {
                    "path": str(report_path),
                    "sha256": release.sha256_file(report_path),
                }
            ],
            "business_checks": [
                {
                    "check_id": check_id,
                    "status": "passed",
                    "evidence_refs": [f"test:{check_id}"],
                }
                for check_id in sorted(release.MATH_RESUME_REQUIRED_CHECKS)
            ],
            "formal_surface_guard": {
                "before_sha256": "f" * 64,
                "after_sha256": "f" * 64,
            },
            "formal_write_count": 0,
        }
        acceptance_path = publish(acceptance, self.base)
        receipt_path = self.base / "math-resume-receipt.json"
        sealed = release.seal_math_resume_acceptance(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            acceptance_manifest=acceptance_path,
            output_path=receipt_path,
        )
        self.assertEqual(sealed["status"], "sealed")

        rendered = release._render_launchagent_plists(
            target,
            release.verify_release(target)["service_topology"],
            active,
            self.runtime_data.resolve(),
        )
        for label, raw in rendered.items():
            (self.launchagents / f"{label}.plist").write_bytes(raw)

        def runner(command):
            subject = command[command.index("--subject") + 1]
            if command[-1] == "status":
                payload = {
                    "subject": subject,
                    "draining": True,
                    "active_count": 0,
                    "claimed_total": 0,
                }
            elif command[-1] == "audit":
                payload = {
                    "subject": subject,
                    "eligible_count": 0,
                    "formal_write_count": 0,
                }
            else:
                return {"returncode": 0, "stdout": ""}
            return {"returncode": 0, "stdout": json.dumps(payload)}

        planned = release.resume_math_dispatcher(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            acceptance_receipt=receipt_path,
            launchagent_dir=self.launchagents,
            apply=False,
            command_runner=runner,
        )
        self.assertEqual(planned["status"], "planned")
        self.assertEqual(
            planned["backlog_audits"]["math"]["eligible_count"], 0
        )

        broken = dict(acceptance)
        broken["business_checks"] = broken["business_checks"][:-1]
        broken_path = publish(broken, self.base)
        with self.assertRaisesRegex(
            release.ReleaseError, "math_resume_business_checks_incomplete"
        ):
            release._validate_math_resume_acceptance(
                broken_path, release_id=release_id
            )

    def test_activate_canary_preview_is_zero_model_and_non_mutating(self) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        active = self.base / "canary-preview-current"
        manifest = self.make_canary_manifest(built)
        runner = self.FakeRunner()
        (self.runtime_data / "dispatch" / "state").mkdir(parents=True)
        runtime_marker = self.runtime_data / "dispatch" / "state" / "marker.json"
        runtime_marker.write_text('{"stable":true}\n', encoding="utf-8")
        plist_marker = self.write_installed_plist(
            "com.xiazhibin.study-intake-dashboard",
            ["/usr/bin/true", "preview-dashboard"],
        )
        runtime_before = runtime_marker.read_bytes()
        plist_before = plist_marker.read_bytes()

        with mock.patch.object(
            release, "_port_8767_owner_pids", return_value=[8767]
        ) as port_inspector:
            planned = release.activate_canary_release(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                canary_manifest=manifest,
                apply=False,
                launchagent_dir=self.launchagents,
                command_runner=runner,
            )

        self.assertEqual(planned["status"], "production_canary_planned")
        self.assertFalse(active.exists())
        self.assertEqual(runner.commands, [])
        self.assertEqual(port_inspector.call_count, 2)
        self.assertEqual(runtime_marker.read_bytes(), runtime_before)
        self.assertEqual(plist_marker.read_bytes(), plist_before)
        self.assertTrue(planned["preview_non_mutation_verified"])
        self.assertTrue(planned["canary"]["post_activation_only"])
        self.assertTrue(planned["canary"]["historical_backlog_drained"])
        self.assertEqual(
            planned["canary"]["initial_canary_inflight_limit"], 1
        )
        self.assertEqual(
            planned["canary"]["continuous_concurrency_limit"], 20
        )
        self.assertIsNone(planned["canary"]["requested_service_tier"])
        self.assertFalse(planned["canary"]["fast_mode_requested"])
        self.assertEqual(
            planned["canary"]["fast_mode_effective"], "not_requested"
        )
        self.assertEqual(
            set(planned["canary"]["slots"]), set(release.RESUMABLE_SUBJECTS)
        )
        self.assertTrue(all(
            slot["state"] == "planned"
            and slot["producer_high_watermark_sha256"] is None
            and slot["capture_id"] is None
            and slot["completion_receipt_sha256"] is None
            for slot in planned["canary"]["slots"].values()
        ))
        self.assertEqual(planned["formal_write_count"], 0)
        preflight = planned["pre_activation_verification"]
        self.assertEqual(
            preflight["status"],
            "verified_deployment_controls_and_slots_planned",
        )
        self.assertEqual(preflight["verification_mcp_tool_call_count"], 0)

    def test_default_canary_verifier_accepts_persisted_nonzero_backlog(self) -> None:
        for module_root in (ROOT / "lib", ROOT / "bin"):
            if str(module_root) not in sys.path:
                sys.path.insert(0, str(module_root))
        import hashlib

        import concurrent_dispatch as dispatch
        import preprocess_dispatcher as dispatcher_runtime

        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        planned = json.loads(
            self.make_canary_manifest(built).read_text(encoding="utf-8")
        )
        activated_at = "2026-08-11T00:10:00+00:00"
        context = release._bind_runtime_canary_context(
            planned,
            activated_at=activated_at,
        )
        runtime_root = self.base / "production-shaped-canary-runtime"
        config = {
            "runtime_root": str(runtime_root),
            "worker": {"model_timeout_seconds": 1},
            "math_deep_v2": {
                "soft_runtime_warning_seconds": 3600,
                "stall_timeout_seconds": 1800,
                "stall_probe_interval_seconds": 60,
                "stall_probe_required_consecutive_failures": 2,
            },
            "cs408_deep_v2": {
                "soft_runtime_warning_seconds": 1800,
                "stall_timeout_seconds": 1800,
                "stall_probe_interval_seconds": 60,
                "stall_probe_required_consecutive_failures": 2,
            },
            "english_two_pass_v1": {
                "soft_runtime_warning_seconds": 1800,
                "stall_timeout_seconds": 1800,
                "stall_probe_interval_seconds": 60,
                "stall_probe_required_consecutive_failures": 2,
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
        runtime_release_manifest = self.base / "production-shaped-release.json"
        runtime_release_manifest.write_text(
            json.dumps(
                {
                    "schema_version": "study-intake-preprocessor-release-v2",
                    "release_id": release_id,
                },
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        config["release"] = {
            "manifest_path": str(runtime_release_manifest.resolve())
        }
        config_path = self.base / "production-shaped-canary-config.json"
        config_path.write_text(json.dumps(config) + "\n", encoding="utf-8")
        authorities = {
            subject: self.producer_authority(
                subject=subject,
                release_id=release_id,
                index=index,
            )
            for index, subject in enumerate(
                release.RESUMABLE_SUBJECTS, start=1
            )
        }

        def historical_task(index: int) -> dispatch.FrozenTask:
            subject = "cs408"
            capture_id = f"HISTORICAL-CS408-{index:02d}"
            recorded_at = f"2026-08-11T00:0{index}:00+00:00"
            input_fingerprint = hashlib.sha256(
                f"{subject}:{capture_id}".encode("utf-8")
            ).hexdigest()
            source_event = {
                "event_id": capture_id,
                "recorded_at": recorded_at,
                "source_sha256": hashlib.sha256(
                    f"source:{capture_id}".encode("utf-8")
                ).hexdigest(),
            }
            canonical = lambda value: json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            producer_core = {
                "schema_version": "study-intake-producer-dispatch-input-v2",
                "subject": subject,
                "release_id": release_id,
                "authority_fingerprint": authorities[subject][
                    "authority_fingerprint"
                ],
                "producer_unit_id": capture_id,
                "producer_recorded_at": recorded_at,
                "input_fingerprint": input_fingerprint,
                "source_events": [source_event],
                "earliest_source_recorded_at": recorded_at,
                "latest_source_recorded_at": recorded_at,
                "source_event_set_sha256": hashlib.sha256(
                    canonical([source_event])
                ).hexdigest(),
                "capture_type": None,
                "evidence_contract": None,
                "evidence_contract_sha256": None,
                "formal_write_count": 0,
            }
            producer_contract = {
                **producer_core,
                "producer_input_contract_sha256": hashlib.sha256(
                    canonical(producer_core)
                ).hexdigest(),
            }
            return dispatch.FrozenTask(
                {
                    "subject": subject,
                    "capture_id": capture_id,
                    "study_date": "2026-08-11",
                    "recorded_at": recorded_at,
                    "input_fingerprint": input_fingerprint,
                    "input_binding": {"source": "append-only-producer"},
                    "model_input": {"question": capture_id},
                    "allowed_evidence_refs": [f"capture:{capture_id}"],
                    "image_paths": [],
                    "target_label": capture_id,
                    "canonical_state": "pending",
                    "sol_state": "disabled",
                    "dispatch_contract": {
                        "schema_version": (
                            "study-intake-dispatch-release-binding-v1"
                        ),
                        "release_id": release_id,
                        "dispatch_reason": (
                            "production_canary_pending_queue"
                        ),
                        "requested_service_tier": None,
                        "fast_mode_requested": False,
                        "fast_mode_effective": "not_requested",
                        "producer_input_contract": producer_contract,
                    },
                }
            )

        tasks = {
            "math": [],
            "cs408": [historical_task(index) for index in range(1, 5)],
            "english": [],
        }
        runtimes = {
            subject: dispatcher_runtime.ProductionDispatchRuntime(
                config,
                subject,
                config_path,
            )
            for subject in release.RESUMABLE_SUBJECTS
        }
        store = dispatch.LeaseStore(runtime_root)
        for subject in release.RESUMABLE_SUBJECTS:
            store.begin_subject_drain(subject)

        def scan(_config, subject, **kwargs):
            self.assertFalse(kwargs.get("publish_evidence_readiness", True))
            units = [mock.Mock(task=task) for task in tasks[subject]]
            decisions = [
                {
                    "subject": subject,
                    "capture_id": task.frozen_payload["capture_id"],
                    "study_date": "2026-08-11",
                    "unit_sha256": task.unit_sha256,
                    "eligible": True,
                    "model_enqueue_allowed": True,
                }
                for task in tasks[subject]
            ]
            return units, decisions

        class InProcessRunner:
            def __init__(runner_self) -> None:
                runner_self.commands: list[list[str]] = []
                runner_self.corrupt_cs408_classification = False

            def __call__(runner_self, command):
                runner_self.commands.append(list(command))
                subject = command[command.index("--subject") + 1]
                if command[-1] == "canary-readiness":
                    authority = authorities[subject]
                    payload = {
                        "schema_version": "study-intake-canary-readiness-v1",
                        "subject": subject,
                        "readiness": "ready",
                        "reason": "ready",
                        "authority_generation": f"{subject}-generation",
                        "authority_fingerprint": authority[
                            "authority_fingerprint"
                        ],
                        "batch_id": None,
                        "batch_sha256": None,
                        "writer_revision": 0,
                        "writer_batch_id": None,
                        "source_generation": None,
                        "original_preclaim_failure_receipt_sha256": None,
                        "original_preclaim_failure_receipt_path": None,
                        "preserved_queue_entry_sha256": None,
                        "subsequent_attempt_receipt_sha256s": [],
                        "preserved_task": None,
                        "read_only": True,
                        "authority_snapshot_count": 1,
                        "mcp_tool_call_count": 1,
                        "model_call_count": 0,
                        "provider_request_count": 0,
                        "formal_write_count": 0,
                        "sol_enabled": False,
                    }
                elif command[-1] == "audit":
                    payload = dispatcher_runtime._audit(config, subject)
                    if (
                        runner_self.corrupt_cs408_classification
                        and subject == "cs408"
                    ):
                        payload["excluded_by_high_watermark_count"] = 3
                elif "--deployment-bind-canary-gate" in command:
                    payload = store.activate_production_canary(
                        subject,
                        release_id=release_id,
                        producer_authority=authorities[subject],
                        activated_at=command[command.index("--activated-at") + 1],
                        continuous_concurrency_limit=20,
                    )
                    self.assertEqual(
                        payload["activation_id"],
                        command[command.index("--expected-activation-id") + 1],
                    )
                elif command[-1] == "status":
                    payload = store.subject_status(subject)
                else:
                    raise AssertionError(f"unexpected command: {command}")
                return {"returncode": 0, "stdout": json.dumps(payload)}

        runner = InProcessRunner()
        with (
            mock.patch.object(
                dispatcher_runtime,
                "scan_eligible_candidates",
                side_effect=scan,
            ),
            mock.patch.object(
                dispatcher_runtime,
                "Worker",
                return_value=mock.Mock(release_id=release_id),
            ),
            mock.patch.object(
                dispatcher_runtime,
                "producer_authority_binding",
                side_effect=lambda _config, subject, _release_id: (
                    authorities[subject]
                ),
            ),
        ):
            proof = release._default_canary_pre_activation_verifier(
                release_id=release_id,
                target=target,
                runtime_data_root=runtime_root,
                runner=runner,
                canary_context=context,
                apply=True,
            )

            cs408 = next(
                row for row in proof["subjects"] if row["subject"] == "cs408"
            )
            self.assertNotIn("historical_eligible_count", cs408)
            self.assertEqual(cs408["gate_state"], "armed")
            self.assertEqual(len(runner.commands), 3)
            self.assertTrue(
                all(
                    "--deployment-bind-canary-gate" in command
                    for command in runner.commands
                )
            )
            self.assertFalse(
                any("activate-canary" in command for command in runner.commands)
            )
            self.assertFalse(
                any(
                    command[-1] in {"audit", "status", "canary-readiness"}
                    for command in runner.commands
                )
            )

    def test_post_canary_verifier_reads_private_status_gate(self) -> None:
        release_id = "a" * 64
        target = self.base / release_id
        target.mkdir()
        target = target.resolve()
        active = self.base / "post-canary-current"
        active.symlink_to(target, target_is_directory=True)
        (target / "config.json").write_text(
            json.dumps(
                {
                    "model": {
                        "model": release.REQUIRED_MODEL_CONTRACT["model"],
                        "reasoning_effort": release.REQUIRED_MODEL_CONTRACT[
                            "reasoning_effort"
                        ],
                    }
                }
            ),
            encoding="utf-8",
        )
        launchagent_dir = self.base / "post-canary-launchagents"
        launchagent_dir.mkdir()
        rendered: dict[str, bytes] = {}
        topology: list[dict[str, object]] = []
        for index, name in enumerate(("math", "cs408", "english", "dashboard")):
            label = f"test.post-canary.{name}"
            arguments = ["/usr/bin/true", name]
            raw = plistlib.dumps(
                {
                    "Label": label,
                    "ProgramArguments": arguments,
                    "WorkingDirectory": str(active),
                }
            )
            (launchagent_dir / f"{label}.plist").write_bytes(raw)
            rendered[label] = raw
            topology.append(
                {
                    "name": name,
                    "label": label,
                    "verify": ["launchctl", "print", label],
                    "pid": 3000 + index,
                    "arguments": arguments,
                }
            )
        heartbeat_root = (
            self.base / "post-canary-runtime" / "dispatch" / "state"
            / "subject-projections"
        )
        heartbeat_root.mkdir(parents=True)
        activated_at = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=1)
        for subject in ("math", "cs408", "english"):
            (heartbeat_root / f"{subject}.json").write_text(
                json.dumps(
                    {
                        "subject": subject,
                        "release_id": release_id,
                        "daemon_status": "running",
                        "draining": True,
                        "formal_write_count": 0,
                        "heartbeat_interval_seconds": 15,
                        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "active_count": 0,
                        "claimed_total": 0,
                        "canary_gate": {"view": "public", "subject": subject},
                    }
                ),
                encoding="utf-8",
            )

        status_commands: list[str] = []

        def runner(command: list[str]) -> subprocess.CompletedProcess[str]:
            if command[0] == "launchctl":
                service = next(row for row in topology if row["label"] == command[-1])
                output = "\n".join(
                    [
                        "state = running",
                        f"pid = {service['pid']}",
                        str(active),
                        *service["arguments"],
                    ]
                )
                return subprocess.CompletedProcess(command, 0, output, "")
            subject = command[command.index("--subject") + 1]
            status_commands.append(subject)
            payload = {
                "subject": subject,
                "draining": True,
                "active_count": 0,
                "claimed_total": 0,
                "canary_gate": {"view": "private", "subject": subject},
            }
            return subprocess.CompletedProcess(command, 0, json.dumps(payload), "")

        observed_views: list[tuple[str, str]] = []

        def validate_views(gate: object, status_gate: object, **_: object) -> None:
            assert isinstance(gate, dict)
            assert isinstance(status_gate, dict)
            observed_views.append((gate["view"], status_gate["view"]))

        with mock.patch.object(
            release,
            "_validate_runtime_canary_heartbeat_gate",
            side_effect=validate_views,
        ):
            proof = release._default_post_canary_verifier(
                release_id=release_id,
                target=target,
                active_link=active,
                runtime_data_root=self.base / "post-canary-runtime",
                topology=topology,
                rendered_plists=rendered,
                launchagent_dir=launchagent_dir,
                runner=runner,
                activated_at=activated_at,
                heartbeat_timeout_seconds=1,
                canary_context={
                    "slots": {
                        subject: {"producer_high_watermark_sha256": "b" * 64}
                        for subject in ("math", "cs408", "english")
                    }
                },
            )

        self.assertEqual(status_commands, ["math", "cs408", "english"])
        self.assertEqual(observed_views, [("public", "private")] * 3)
        self.assertEqual(proof["status"], "verified_production_canary_active")

    def test_post_canary_verifier_accepts_public_gate_bound_to_signed_state(
        self,
    ) -> None:
        for module_root in (ROOT / "lib", ROOT / "bin"):
            if str(module_root) not in sys.path:
                sys.path.insert(0, str(module_root))
        import concurrent_dispatch as dispatch
        import dashboard_projection

        subject = "math"
        release_id = "a" * 64
        activated_at = "2026-08-11T13:00:00+00:00"
        authority = self.producer_authority(
            subject=subject,
            release_id=release_id,
            index=1,
        )
        store = dispatch.LeaseStore(self.base / "heartbeat-public-gate-runtime")
        store.begin_subject_drain(subject)
        status_gate = store.activate_production_canary(
            subject,
            release_id=release_id,
            producer_authority=authority,
            activated_at=activated_at,
            continuous_concurrency_limit=20,
        )
        slot = {
            "subject": subject,
            "producer_authority_fingerprint": authority[
                "authority_fingerprint"
            ],
            "producer_high_watermark": status_gate["producer_high_watermark"],
            "producer_high_watermark_sha256": status_gate[
                "producer_high_watermark_sha256"
            ],
            "state": "armed",
            "capture_id": None,
            "completion_receipt_sha256": None,
        }
        public_gate = dashboard_projection._public_canary_gate(
            status_gate,
            subject=subject,
            release_id=release_id,
        )
        self.assertIsNotNone(public_gate)
        assert public_gate is not None
        self.assertNotIn("producer_high_watermark", public_gate)
        self.assertNotIn("queue_classification", public_gate)
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_gate_invalid:math",
        ):
            release._validate_runtime_canary_gate(
                public_gate,
                subject=subject,
                release_id=release_id,
                slot=slot,
            )
        release._validate_runtime_canary_heartbeat_gate(
            public_gate,
            status_gate,
            subject=subject,
            release_id=release_id,
            slot=slot,
        )

        tampered = dict(public_gate)
        tampered["producer_high_watermark_sha256"] = "b" * 64
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_heartbeat_gate_invalid:math",
        ):
            release._validate_runtime_canary_heartbeat_gate(
                tampered,
                status_gate,
                subject=subject,
                release_id=release_id,
                slot=slot,
            )

    def test_prepare_canary_manifest_is_content_addressed_target_v2(self) -> None:
        built = self.build(passed=True)
        first = self.make_canary_manifest(built)
        first_stat = first.stat()
        second = self.make_canary_manifest(built)
        value = json.loads(first.read_text(encoding="utf-8"))

        self.assertEqual(first, second)
        self.assertEqual(first.name, f"{release.sha256_file(first)}.json")
        self.assertEqual(first.stat().st_ino, first_stat.st_ino)
        self.assertEqual(stat.S_IMODE(first.stat().st_mode), 0o400)
        self.assertEqual(
            value["schema_version"],
            "study-intake-three-subject-canary-activation-v2",
        )
        self.assertEqual(value["initial_canary_inflight_limit"], 1)
        self.assertEqual(value["continuous_concurrency_limit"], 20)
        self.assertIsNone(value["requested_service_tier"])
        self.assertFalse(value["fast_mode_requested"])
        self.assertEqual(value["fast_mode_effective"], "not_requested")
        self.assertEqual(value["model_call_count"], 0)
        self.assertEqual(value["provider_request_count"], 0)
        self.assertEqual(value["formal_write_count"], 0)
        self.assertFalse(value["sol_enabled"])

        def invalid_authority(**values):
            subject = values["subject"]
            index = release.RESUMABLE_SUBJECTS.index(subject) + 1
            authority = self.producer_authority(
                subject=subject,
                release_id=values["release_id"],
                index=index,
            )
            authority["service_tier"] = "priority"
            return authority

        with self.assertRaisesRegex(
            release.ReleaseError, "producer_authority_invalid"
        ):
            release.prepare_canary_manifest(
                release_base=self.release_base,
                release_id=str(built["release_id"]),
                output_dir=self.base / "invalid-canary-manifests",
                authority_resolver=invalid_authority,
            )

    def test_canary_v1_schemas_remain_byte_stable_and_preview_flag_is_explicit(
        self,
    ) -> None:
        self.assertEqual(
            release.sha256_file(
                ROOT / "schemas" / "three-subject-canary-activation-v1.json"
            ),
            "0ef8aef7912572bd988d4abd17c51f754c8e2535cccb175c804f2cb699b17eb0",
        )
        self.assertEqual(
            release.sha256_file(
                ROOT
                / "schemas"
                / "three-subject-canary-activation-receipt-v1.json"
            ),
            "e0ed3c7c1a14d6cb75e54a9495c53792ba75369d35204956f27356c822a0b5d8",
        )
        for name in (
            "three-subject-canary-activation-v2.json",
            "three-subject-canary-activation-receipt-v2.json",
            "canary-resume-proof-v2.json",
            "canary-pause-proof-v2.json",
        ):
            schema = json.loads(
                (ROOT / "schemas" / name).read_text(encoding="utf-8")
            )
            self.assertEqual(
                schema["properties"]["initial_canary_inflight_limit"]["const"],
                1,
            )
            self.assertEqual(
                schema["properties"]["continuous_concurrency_limit"]["const"],
                20,
            )
            self.assertEqual(
                schema["properties"]["requested_service_tier"]["type"],
                "null",
            )
            self.assertFalse(
                schema["properties"]["fast_mode_requested"]["const"]
            )
            self.assertEqual(
                schema["properties"]["fast_mode_effective"]["const"],
                "not_requested",
            )
        parsed = release.parser().parse_args(
            [
                "activate-canary",
                "--release-id",
                "a" * 64,
                "--canary-manifest",
                str(self.base / "manifest.json"),
                "--preview",
            ]
        )
        self.assertTrue(parsed.preview)
        self.assertFalse(parsed.apply)

    def test_activate_canary_preview_fails_closed_on_surface_change(self) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        active = self.base / "mutated-preview-current"
        manifest = self.make_canary_manifest(built)
        observations = iter(
            (
                {"runtime": "before", "active": "absent", "port": [1]},
                {"runtime": "after", "active": "absent", "port": [1]},
            )
        )
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_preview_mutated_state",
        ):
            release.activate_canary_release(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                canary_manifest=manifest,
                apply=False,
                launchagent_dir=self.launchagents,
                command_runner=self.FakeRunner(),
                canary_pre_activation_verifier=self.canary_preflight_proof,
                preview_state_inspector=lambda **_values: next(observations),
            )
        self.assertFalse(active.exists())

    def test_preview_runtime_snapshot_ignores_only_live_projection_files(
        self,
    ) -> None:
        dispatch = self.base / "preview-runtime" / "dispatch"
        heartbeat = dispatch / "state" / "subject-projections" / "math.json"
        canary = dispatch / "state" / "production-canary" / "math.json"
        queue = dispatch / "state" / "production-canary-queue" / "math.json"
        receipt = dispatch / "receipts" / "sha256" / "aa" / "receipt.json"
        for path, value in (
            (heartbeat, b"heartbeat-v1\n"),
            (canary, b"canary-age-v1\n"),
            (queue, b"queue-v1\n"),
            (receipt, b"receipt-v1\n"),
        ):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(value)
        before = release._preview_runtime_durable_snapshot(dispatch)
        heartbeat.write_bytes(b"heartbeat-v2\n")
        canary.write_bytes(b"canary-age-v2\n")
        after_live_refresh = release._preview_runtime_durable_snapshot(dispatch)
        self.assertEqual(before, after_live_refresh)
        queue.write_bytes(b"queue-v2\n")
        after_queue_mutation = release._preview_runtime_durable_snapshot(dispatch)
        self.assertNotEqual(before, after_queue_mutation)
        queue.write_bytes(b"queue-v1\n")
        receipt.write_bytes(b"receipt-v2\n")
        after_receipt_mutation = release._preview_runtime_durable_snapshot(
            dispatch
        )
        self.assertNotEqual(before, after_receipt_mutation)

    def test_activate_canary_apply_starts_same_release_and_seals_public_receipt(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "canary-apply-current"
        manifest = self.make_canary_manifest(built)
        runner = self.FakeRunner()
        armed_contexts: list[dict] = []

        def canary_preflight(**values) -> dict:
            if values["apply"]:
                armed_contexts.append(
                    json.loads(json.dumps(values["canary_context"]))
                )
            return self.canary_preflight_proof(**values)

        applied = release.activate_canary_release(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            canary_manifest=manifest,
            apply=True,
            expected_current="absent",
            launchagent_dir=self.launchagents,
            command_runner=runner,
            canary_pre_activation_verifier=canary_preflight,
            post_activation_verifier=self.canary_post_activation_proof,
            canary_rollback_controller=self.canary_rollback_proof,
        )

        self.assertEqual(applied["status"], "production_canary_active")
        self.assertEqual(active.resolve(), target)
        self.assertEqual(len(armed_contexts), 1)
        runtime_context = armed_contexts[0]
        self.assertEqual(
            {
                runtime_context["slots"][subject]["producer_high_watermark"][
                    "recorded_at"
                ]
                for subject in release.RESUMABLE_SUBJECTS
            },
            {applied["canary"]["activated_at"]},
        )
        self.assertEqual(
            applied["activation_id"],
            release._three_subject_canary_activation_id(runtime_context),
        )
        self.assertEqual(
            applied["service_activation_policy"],
            {
                "math": "running",
                "cs408": "running",
                "english": "running",
                "dashboard": "running",
            },
        )
        for phase in ("enable", "bootstrap", "print"):
            self.assertEqual(
                sum(command[:2] == ["launchctl", phase] for command in runner.commands),
                4,
            )
        receipt_sha256 = applied["canary_activation_receipt_sha256"]
        receipt_path = (
            self.release_base / "deployments" / f"{receipt_sha256}.json"
        )
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        self.assertEqual(
            receipt["schema_version"], release.THREE_SUBJECT_CANARY_RECEIPT_SCHEMA
        )
        self.assertEqual(receipt["status"], "production_canary_active")
        self.assertFalse(receipt["production_accepted"])
        self.assertEqual(receipt["real_luna_runs"], 0)
        self.assertEqual(receipt["initial_canary_inflight_limit"], 1)
        self.assertEqual(receipt["continuous_concurrency_limit"], 20)
        self.assertIsNone(receipt["requested_service_tier"])
        self.assertFalse(receipt["fast_mode_requested"])
        self.assertEqual(receipt["fast_mode_effective"], "not_requested")
        self.assertEqual(receipt["provider_request_count"], 0)
        self.assertEqual(receipt["formal_write_count"], 0)
        self.assertFalse(receipt["sol_enabled"])
        self.assertNotIn("subject_batch_recovery_binding", receipt)
        self.assertEqual(set(receipt["service_release_ids"].values()), {release_id})
        self.assertEqual(receipt["activation_id"], applied["activation_id"])
        self.assertEqual(
            receipt["producer_high_watermark_sha256s"],
            {
                subject: runtime_context["slots"][subject][
                    "producer_high_watermark_sha256"
                ]
                for subject in release.RESUMABLE_SUBJECTS
            },
        )
        self.assertNotIn(str(self.base), receipt_path.read_text(encoding="utf-8"))
        release._verify_deployment_authority(
            receipt,
            release_base=self.release_base,
            purpose=release.THREE_SUBJECT_CANARY_RECEIPT_PURPOSE,
        )
        release._validate_three_subject_canary_activation_receipt(
            receipt,
            release_base=self.release_base,
        )
        invalid_v2_core = copy.deepcopy(receipt)
        invalid_v2_core.pop("authority")
        invalid_v2_core["subject_batch_recovery_binding"] = None
        invalid_v2 = release._seal_deployment_value(
            invalid_v2_core,
            release_base=self.release_base,
            purpose=release.THREE_SUBJECT_CANARY_RECEIPT_PURPOSE,
        )
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_activation_receipt_invalid",
        ):
            release._validate_three_subject_canary_activation_receipt(
                invalid_v2,
                release_base=self.release_base,
            )

    def test_canary_english_recovery_stages_finalizes_and_arms_before_services(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        active = self.base / "canary-english-recovery-current"
        manifest = self.make_canary_manifest(built)
        original_receipt_sha256 = "3" * 64
        recovery_receipt_sha256 = "4" * 64
        preserved_queue_entry_sha256 = "5" * 64
        recovery_receipt_path = self.base / "english-recovery-v2.json"
        events: list[str] = []

        def preflight(**values) -> dict:
            proof = self.canary_preflight_proof(**values)
            if values["apply"]:
                events.append("ordinary-apply-verifier")
                return proof
            events.append("readiness-preview")
            english = next(
                row
                for row in proof["subjects"]
                if row["subject"] == "english"
            )
            english.update(
                {
                    "subject_batch_readiness": (
                        "recoverable_terminal_batch"
                    ),
                    "subject_batch_readiness_sha256": "6" * 64,
                    "subject_recovery_required": True,
                    "subject_recovery_already_applied": False,
                    "subject_recovery_original_preclaim_failure_receipt_sha256": (
                        original_receipt_sha256
                    ),
                    "subject_recovery_receipt_sha256": None,
                    "subject_recovery_receipt_path": None,
                    "subject_recovery_rollback_token": None,
                    "subject_readiness_mcp_tool_call_count": 1,
                    "subject_authority_generation": "english-generation-v2",
                    "subject_authority_fingerprint": "7" * 64,
                }
            )
            return proof

        class RecoveryRunner(self.FakeRunner):
            def __call__(inner_self, command):
                inner_self.commands.append(list(command))
                if command and command[0] == "launchctl":
                    events.append(f"launchctl:{command[1]}")
                    return {"returncode": 0, "stdout": ""}
                if "recover-subject-batch" in command:
                    events.append("recover")
                    value = {
                        "schema_version": (
                            "study-intake-subject-batch-recovery-result-v1"
                        ),
                        "subject": "english",
                        "status": "recovered",
                        "source_generation": "english-generation-v1",
                        "next_generation": "english-generation-v2",
                        "next_authority_fingerprint": "7" * 64,
                        "original_preclaim_failure_receipt_sha256": (
                            original_receipt_sha256
                        ),
                        "preserved_queue_entry_sha256": (
                            preserved_queue_entry_sha256
                        ),
                        "subsequent_attempt_receipt_sha256s": ["8" * 64],
                        "preserved_task": {"capture_id": "CAP-English-old"},
                        "recovery_receipt_sha256": recovery_receipt_sha256,
                        "recovery_receipt_path": str(recovery_receipt_path),
                        "rollback_token": "9" * 64,
                        "writer_state_after_sha256": "a" * 64,
                        "consumer_state_unchanged": True,
                        "luna_consumer_enabled": False,
                        "idempotent": False,
                        "authority_snapshot_count": 2,
                        "authority_snapshot_mcp_tool_call_count": 2,
                        "model_mcp_tool_call_count": 0,
                        "mcp_tool_call_count": 2,
                        "model_call_count": 0,
                        "provider_request_count": 0,
                        "formal_write_count": 0,
                        "sol_enabled": False,
                    }
                elif "stage-subject-batch-recovery-activation" in command:
                    events.append("stage")
                    value = {
                        "schema_version": (
                            "study-intake-subject-batch-recovery-staged-activation-result-v1"
                        ),
                        "subject": "english",
                        "status": "staged",
                        "recovery_receipt_sha256": recovery_receipt_sha256,
                        "target_release_id": release_id,
                        "target_activation_id": command[
                            command.index("--expected-activation-id") + 1
                        ],
                        "producer_authority_fingerprint": command[
                            command.index(
                                "--expected-producer-authority-fingerprint"
                            )
                            + 1
                        ],
                        "luna_consumer_enabled": False,
                        "state": "paused_drained",
                        "formal_write_count": 0,
                        "sol_enabled": False,
                    }
                elif "finalize-subject-batch-recovery" in command:
                    events.append("finalize")
                    expected_activation_id = next(
                        value["target_activation_id"]
                        for value in inner_self.payloads
                        if value.get("status") == "staged"
                    )
                    value = {
                        "schema_version": (
                            "study-intake-subject-batch-recovery-finalization-result-v1"
                        ),
                        "subject": "english",
                        "status": "finalized",
                        "recovery_receipt_sha256": recovery_receipt_sha256,
                        "supersede_receipt_sha256": "b" * 64,
                        "supersede_receipt_path": str(
                            self.base / "english-supersede.json"
                        ),
                        "old_activation_id": "c" * 64,
                        "old_queue_entry_sha256": (
                            preserved_queue_entry_sha256
                        ),
                        "target_activation_id": expected_activation_id,
                        "target_release_id": release_id,
                        "replacement_task": {
                            "unit_sha256": "e" * 64,
                            "frozen_payload_sha256": "f" * 64,
                            "task_object_sha256": "1" * 64,
                            "task_object_path": str(
                                self.base / "english-replacement-task.json"
                            ),
                            "producer_input_contract_sha256": "2" * 64,
                            "source_event_set_sha256": "0" * 64,
                        },
                        "replacement_queue_entry_sha256": "d" * 64,
                        "replacement_queue_entry_path": str(
                            self.base / "english-replacement-queue.json"
                        ),
                        "idempotent": False,
                        "model_call_count": 0,
                        "provider_request_count": 0,
                        "formal_write_count": 0,
                        "sol_enabled": False,
                    }
                elif "arm-finalized-subject-batch-recovery" in command:
                    events.append("arm")
                    finalized = next(
                        value
                        for value in inner_self.payloads
                        if value.get("status") == "finalized"
                    )
                    value = {
                        "schema_version": (
                            "study-intake-subject-batch-recovery-arm-result-v1"
                        ),
                        "subject": "english",
                        "status": "armed",
                        "recovery_receipt_sha256": recovery_receipt_sha256,
                        "target_release_id": release_id,
                        "target_activation_id": finalized[
                            "target_activation_id"
                        ],
                        "luna_consumer_enabled": True,
                        "state": "armed",
                        "idempotent": False,
                        "formal_write_count": 0,
                        "sol_enabled": False,
                    }
                else:
                    return {"returncode": 0, "stdout": ""}
                inner_self.payloads.append(value)
                return {"returncode": 0, "stdout": json.dumps(value)}

            def __init__(inner_self) -> None:
                super().__init__()
                inner_self.payloads: list[dict[str, Any]] = []

        runner = RecoveryRunner()
        applied = release.activate_canary_release(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            canary_manifest=manifest,
            apply=True,
            expected_current="absent",
            launchagent_dir=self.launchagents,
            command_runner=runner,
            canary_pre_activation_verifier=preflight,
            post_activation_verifier=self.canary_post_activation_proof,
            canary_rollback_controller=self.canary_rollback_proof,
            subject_recovery_expectations={
                "english": original_receipt_sha256
            },
        )

        ordered = [
            "recover",
            "stage",
            "finalize",
            "arm",
            "ordinary-apply-verifier",
        ]
        self.assertEqual(
            [event for event in events if event in ordered], ordered
        )
        first_service_start = next(
            index
            for index, event in enumerate(events)
            if event == "launchctl:bootstrap"
        )
        self.assertLess(events.index("ordinary-apply-verifier"), first_service_start)
        self.assertFalse(
            any("activate-canary" in command for command in runner.commands)
        )
        self.assertEqual(applied["status"], "production_canary_active")
        self.assertEqual(
            applied["subject_batch_recovery_proof"][
                "recovery_receipt_sha256"
            ],
            recovery_receipt_sha256,
        )
        self.assertEqual(
            applied["subject_batch_recovery_finalization_proof"][
                "supersede_receipt_sha256"
            ],
            "b" * 64,
        )
        receipt = applied["canary_activation_receipt"]
        self.assertEqual(
            receipt["schema_version"],
            release.THREE_SUBJECT_CANARY_RECEIPT_V3_SCHEMA,
        )
        self.assertEqual(
            receipt["subject_batch_recovery_binding"][
                "original_preclaim_failure_receipt_sha256"
            ],
            original_receipt_sha256,
        )
        self.assertNotIn(
            "task_object_path",
            receipt["subject_batch_recovery_binding"]["replacement_task"],
        )
        release._validate_three_subject_canary_activation_receipt(
            receipt,
            release_base=self.release_base,
        )
        tampered_core = copy.deepcopy(receipt)
        tampered_core.pop("authority")
        tampered_core["subject_batch_recovery_binding"][
            "replacement_task"
        ]["unit_sha256"] = "not-a-sha"
        tampered = release._seal_deployment_value(
            tampered_core,
            release_base=self.release_base,
            purpose=release.THREE_SUBJECT_CANARY_RECEIPT_PURPOSE,
        )
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_recovery_binding_invalid",
        ):
            release._validate_three_subject_canary_activation_receipt(
                tampered,
                release_base=self.release_base,
            )

    @staticmethod
    def cs408_terminal_retirement_readiness(
        *, retired: bool = False, drift_batch: bool = False
    ) -> dict[str, Any]:
        descriptor = release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR
        return {
            "schema_version": "study-intake-canary-readiness-v1",
            "subject": "cs408",
            "readiness": "ready" if retired else "recoverable_terminal_batch",
            "reason": (
                "cs408_terminal_batch_retired"
                if retired
                else "explicit_subject_resume_required"
            ),
            "authority_generation": descriptor["target_generation"],
            "authority_fingerprint": descriptor[
                "target_authority_fingerprint"
            ],
            "mode": descriptor["mode"],
            "batch_id": descriptor["batch_id"],
            "batch_sha256": (
                "0" * 64 if drift_batch else descriptor["batch_sha256"]
            ),
            "terminal_receipt_sha256": descriptor[
                "terminal_receipt_sha256"
            ],
            "writer_preimage_sha256": descriptor[
                "writer_preimage_sha256"
            ],
            "batch_pointer_sha256": descriptor["batch_pointer_sha256"],
            "snapshot_sha256": descriptor["snapshot_sha256"],
            "source_generation": descriptor["source_generation"],
            "source_authority_fingerprint": descriptor[
                "source_authority_fingerprint"
            ],
            "writer_revision": 1,
            "writer_batch_id": descriptor["batch_id"],
            "original_preclaim_failure_receipt_sha256": None,
            "original_preclaim_failure_receipt_path": None,
            "preserved_queue_entry_sha256": None,
            "subsequent_attempt_receipt_sha256s": [],
            "preserved_task": None,
            "recovery_receipt_sha256": None,
            "recovery_receipt_path": None,
            "rollback_token": None,
            "cs408_terminal_retirement_receipt_sha256": (
                "4" * 64 if retired else None
            ),
            "cs408_terminal_retirement_receipt_path": (
                "/private/tmp/cs408-retirement.json" if retired else None
            ),
            "cs408_terminal_retirement_rollback_token": (
                "5" * 64 if retired else None
            ),
            "read_only": True,
            "authority_snapshot_count": 1,
            "mcp_tool_call_count": 1,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def test_cs408_terminal_retirement_preview_is_exact_and_read_only(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        manifest = self.make_canary_manifest(built)
        active = self.base / "cs408-retirement-preview-current"
        english_receipt = "3" * 64

        class PreviewRunner(self.FakeRunner):
            def __init__(inner_self, *, drift_batch: bool = False) -> None:
                super().__init__()
                inner_self.drift_batch = drift_batch

            def __call__(inner_self, command):
                if command and command[-1] == "canary-readiness":
                    inner_self.commands.append(list(command))
                    subject = command[command.index("--subject") + 1]
                    if subject == "cs408":
                        value = self.cs408_terminal_retirement_readiness(
                            drift_batch=inner_self.drift_batch
                        )
                    elif subject == "english":
                        value = json.loads(
                            super(PreviewRunner, inner_self).__call__(
                                command
                            )["stdout"]
                        )
                        value.update(
                            {
                                "readiness": "recoverable_terminal_batch",
                                "reason": "explicit_subject_resume_required",
                                "original_preclaim_failure_receipt_sha256": (
                                    english_receipt
                                ),
                                "preserved_queue_entry_sha256": "6" * 64,
                            }
                        )
                        inner_self.commands.pop()
                    else:
                        return super().__call__(command)
                    return {"returncode": 0, "stdout": json.dumps(value)}
                return super().__call__(command)

        stable_surface = {"surface": "unchanged"}
        runner = PreviewRunner()
        with self.assertRaisesRegex(
            release.ReleaseError, "incident_recovery_not_supported_in_deploy_path"
        ):
            release.activate_canary_release(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                canary_manifest=manifest,
                apply=False,
                launchagent_dir=self.launchagents,
                command_runner=runner,
                preview_state_inspector=lambda **_values: stable_surface,
                subject_recovery_expectations={"english": english_receipt},
                cs408_terminal_retirement_terminal_receipt_sha256=(
                    release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                        "terminal_receipt_sha256"
                    ]
                ),
            )
        return
        cs408 = next(
            row
            for row in planned["pre_activation_verification"]["subjects"]
            if row["subject"] == "cs408"
        )
        self.assertTrue(cs408["cs408_terminal_retirement_required"])
        self.assertFalse(
            cs408["cs408_terminal_retirement_already_applied"]
        )
        self.assertTrue(planned["preview_non_mutation_verified"])
        self.assertFalse(
            any(
                "retire-cs408-terminal-batch" in command
                for command in runner.commands
            )
        )
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_subject_readiness_failed:cs408",
        ):
            release.activate_canary_release(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                canary_manifest=manifest,
                apply=False,
                launchagent_dir=self.launchagents,
                command_runner=PreviewRunner(drift_batch=True),
                preview_state_inspector=lambda **_values: stable_surface,
                subject_recovery_expectations={"english": english_receipt},
                cs408_terminal_retirement_terminal_receipt_sha256=(
                    release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                        "terminal_receipt_sha256"
                    ]
                ),
            )

    def test_cs408_retirement_precedes_english_recovery_and_v4_receipt(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        manifest = self.make_canary_manifest(built)
        active = self.base / "cs408-retirement-apply-current"
        english_original = "3" * 64
        recovery_sha = "4" * 64
        retirement_bytes = release.canonical_bytes(
            {"fixture": "cs408-retirement-ca-receipt"}
        )
        retirement_sha = release.sha256_bytes(retirement_bytes)
        retirement_path = (
            self.runtime_data.resolve()
            / "dispatch"
            / "control-receipts"
            / "cs408-terminal-batch-retirements"
            / "sha256"
            / retirement_sha[:2]
            / f"{retirement_sha}.json"
        )
        retirement_path.parent.mkdir(parents=True, exist_ok=True)
        retirement_path.write_bytes(retirement_bytes)
        events: list[str] = []

        def preflight(**values) -> dict:
            proof = self.canary_preflight_proof(**values)
            apply = values["apply"]
            english = next(
                row for row in proof["subjects"] if row["subject"] == "english"
            )
            english.update(
                {
                    "historical_eligible_count": 1,
                    "excluded_by_high_watermark_count": (
                        0 if apply else None
                    ),
                    "canary_queue_count": 1 if apply else None,
                    "subject_batch_readiness": (
                        "ready" if apply else "recoverable_terminal_batch"
                    ),
                    "subject_batch_readiness_sha256": "6" * 64,
                    "subject_recovery_required": True,
                    "subject_recovery_already_applied": apply,
                    "subject_recovery_original_preclaim_failure_receipt_sha256": (
                        english_original
                    ),
                    "subject_recovery_receipt_sha256": (
                        recovery_sha if apply else None
                    ),
                    "subject_recovery_receipt_path": (
                        str(self.base / "english-recovery.json")
                        if apply
                        else None
                    ),
                    "subject_recovery_rollback_token": (
                        "7" * 64 if apply else None
                    ),
                    "subject_readiness_mcp_tool_call_count": 1,
                    "subject_authority_generation": "english-next",
                    "subject_authority_fingerprint": "8" * 64,
                }
            )
            cs408 = next(
                row for row in proof["subjects"] if row["subject"] == "cs408"
            )
            cs408.update(
                {
                    "subject_batch_readiness": (
                        "ready" if apply else "recoverable_terminal_batch"
                    ),
                    "subject_batch_readiness_sha256": "9" * 64,
                    "subject_recovery_required": False,
                    "subject_recovery_already_applied": False,
                    "subject_recovery_original_preclaim_failure_receipt_sha256": None,
                    "subject_recovery_receipt_sha256": None,
                    "subject_recovery_receipt_path": None,
                    "subject_recovery_rollback_token": None,
                    "cs408_terminal_retirement_required": True,
                    "cs408_terminal_retirement_already_applied": apply,
                    "cs408_terminal_retirement_terminal_receipt_sha256": (
                        release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                            "terminal_receipt_sha256"
                        ]
                    ),
                    "cs408_terminal_retirement_receipt_sha256": (
                        retirement_sha if apply else None
                    ),
                    "cs408_terminal_retirement_receipt_path": (
                        retirement["retirement_receipt_path"]
                        if apply
                        else None
                    ),
                    "cs408_terminal_retirement_rollback_token": (
                        "a" * 64 if apply else None
                    ),
                    "subject_readiness_mcp_tool_call_count": 1,
                    "subject_authority_generation": (
                        release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                            "target_generation"
                        ]
                    ),
                    "subject_authority_fingerprint": (
                        release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                            "target_authority_fingerprint"
                        ]
                    ),
                }
            )
            if apply:
                events.append("all-subject-arm")
            return proof

        recovery = {
            "original_preclaim_failure_receipt_sha256": english_original,
            "recovery_receipt_sha256": recovery_sha,
            "recovery_receipt_path": str(self.base / "english-recovery.json"),
            "rollback_token": "7" * 64,
            "preserved_queue_entry_sha256": "b" * 64,
        }
        staged = {
            "target_release_id": release_id,
            "target_activation_id": None,
        }
        finalization = {
            "recovery_receipt_sha256": recovery_sha,
            "target_release_id": release_id,
            "target_activation_id": None,
            "old_queue_entry_sha256": "b" * 64,
            "supersede_receipt_sha256": "c" * 64,
            "replacement_queue_entry_sha256": "d" * 64,
            "replacement_task": {
                "unit_sha256": "e" * 64,
                "frozen_payload_sha256": "f" * 64,
                "task_object_sha256": "1" * 64,
                "producer_input_contract_sha256": "2" * 64,
                "source_event_set_sha256": "0" * 64,
            },
        }
        arm = {
            "recovery_receipt_sha256": recovery_sha,
            "target_release_id": release_id,
            "target_activation_id": None,
        }
        retirement = {
            "schema_version": (
                "study-intake-cs408-terminal-batch-retirement-result-v1"
            ),
            "subject": "cs408",
            "status": "retired",
            "mode": release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR["mode"],
            "authorization_descriptor_sha256": (
                release._authorization_descriptor_sha256(
                    release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR
                )
            ),
            "batch_id": release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                "batch_id"
            ],
            "batch_sha256": release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                "batch_sha256"
            ],
            "terminal_receipt_sha256": (
                release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                    "terminal_receipt_sha256"
                ]
            ),
            "writer_preimage_sha256": (
                release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                    "writer_preimage_sha256"
                ]
            ),
            "batch_pointer_sha256": (
                release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                    "batch_pointer_sha256"
                ]
            ),
            "snapshot_sha256": release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                "snapshot_sha256"
            ],
            "source_generation": (
                release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                    "source_generation"
                ]
            ),
            "source_authority_fingerprint": (
                release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                    "source_authority_fingerprint"
                ]
            ),
            "next_generation": release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                "target_generation"
            ],
            "next_authority_fingerprint": (
                release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                    "target_authority_fingerprint"
                ]
            ),
            "retirement_receipt_sha256": retirement_sha,
            "retirement_receipt_path": str(retirement_path),
            "rollback_token": "a" * 64,
            "writer_postimage_sha256": "b" * 64,
            "postimage_verification_sha256": "c" * 64,
            "authority_snapshot_count": 2,
            "authority_snapshot_mcp_tool_call_count": 2,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 2,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "queue_entry_created_count": 0,
            "replacement_task_created_count": 0,
            "capture_replay_count": 0,
            "idempotent": False,
        }

        def retire(**_values):
            events.append("cs408-retire")
            return retirement

        def recover(**_values):
            events.append("english-recover")
            return recovery

        def stage(**values):
            events.append("english-stage")
            staged["target_activation_id"] = values["expected_activation_id"]
            return staged

        def finalize(**_values):
            events.append("english-finalize")
            finalization["target_activation_id"] = staged[
                "target_activation_id"
            ]
            return finalization

        def arm_recovery(**_values):
            events.append("english-arm")
            arm["target_activation_id"] = staged["target_activation_id"]
            return arm

        with (
            mock.patch.object(
                release,
                "_retire_cs408_terminal_batch_before_canary_arm",
                side_effect=retire,
            ),
            mock.patch.object(
                release,
                "_recover_subject_batch_before_canary_arm",
                side_effect=recover,
            ),
            mock.patch.object(
                release,
                "_stage_subject_batch_recovery_activation",
                side_effect=stage,
            ),
            mock.patch.object(
                release,
                "_finalize_subject_batch_recovery_before_canary_arm",
                side_effect=finalize,
            ),
            mock.patch.object(
                release,
                "_arm_finalized_subject_batch_recovery",
                side_effect=arm_recovery,
            ),
        ):
            applied = release.activate_canary_release(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                canary_manifest=manifest,
                apply=True,
                expected_current="absent",
                launchagent_dir=self.launchagents,
                command_runner=self.FakeRunner(),
                canary_pre_activation_verifier=preflight,
                post_activation_verifier=self.canary_post_activation_proof,
                canary_rollback_controller=self.canary_rollback_proof,
                subject_recovery_expectations={"english": english_original},
                cs408_terminal_retirement_terminal_receipt_sha256=(
                    release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                        "terminal_receipt_sha256"
                    ]
                ),
            )
        self.assertEqual(
            events,
            [
                "cs408-retire",
                "english-recover",
                "english-stage",
                "english-finalize",
                "english-arm",
                "all-subject-arm",
            ],
        )
        receipt = applied["canary_activation_receipt"]
        self.assertEqual(
            receipt["schema_version"],
            release.THREE_SUBJECT_CANARY_RECEIPT_V4_SCHEMA,
        )
        self.assertEqual(
            receipt["cs408_writer_retirement_binding"][
                "authorization_descriptor_sha256"
            ],
            release._authorization_descriptor_sha256(
                release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR
            ),
        )
        release._validate_three_subject_canary_activation_receipt(
            receipt, release_base=self.release_base
        )
        prepare = json.loads(
            (
                self.release_base
                / "deployments"
                / f"{applied['prepare_receipt_sha256']}.json"
            ).read_text(encoding="utf-8")
        )
        postcommit = json.loads(
            (
                self.release_base
                / "deployments"
                / f"{applied['postcommit_receipt_sha256']}.json"
            ).read_text(encoding="utf-8")
        )
        receipts = {
            str(applied["canary_activation_receipt_sha256"]): receipt,
        }
        reopened = {
            **retirement,
            "schema_version": (
                "study-intake-cs408-terminal-batch-retirement-reopen-result-v1"
            ),
            "status": "reopened",
            "idempotent": True,
            "reopen_read_only": True,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
        }
        with mock.patch.object(
            release,
            "_reopen_cs408_terminal_batch_retirement_after_uncertain_attempt",
            return_value=reopened,
        ):
            self.assertTrue(
                release._cs408_retirement_postcommit_closes_prepare(
                    prepare=prepare,
                    postcommit=postcommit,
                    receipts=receipts,
                    release_base=self.release_base,
                )
            )
        retirement_path.unlink()
        with mock.patch.object(
            release,
            "_reopen_cs408_terminal_batch_retirement_after_uncertain_attempt",
            return_value=reopened,
        ):
            self.assertFalse(
                release._cs408_retirement_postcommit_closes_prepare(
                    prepare=prepare,
                    postcommit=postcommit,
                    receipts=receipts,
                    release_base=self.release_base,
                )
            )
        retirement_path.write_bytes(retirement_bytes)
        with mock.patch.object(
            release,
            "_reopen_cs408_terminal_batch_retirement_after_uncertain_attempt",
            side_effect=release.ReleaseError(
                "cs408_terminal_retirement_pointer_missing"
            ),
        ):
            self.assertFalse(
                release._cs408_retirement_postcommit_closes_prepare(
                    prepare=prepare,
                    postcommit=postcommit,
                    receipts=receipts,
                    release_base=self.release_base,
                )
            )

    def test_cs408_retirement_rollback_failure_keeps_target_fenced(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        previous = self.build(passed=True)
        previous_id = str(previous["release_id"])
        previous_target = Path(str(previous["release_dir"]))
        active = self.base / "cs408-retirement-rollback-current"
        active.symlink_to(previous_target)
        self.install_current_plists(previous_target, active)
        manifest = self.make_canary_manifest(built)
        drain = self.make_drain_receipt(previous_id, release_id, active)
        english_original = "3" * 64
        retirement = {
            "retirement_receipt_sha256": "4" * 64,
            "retirement_receipt_path": str(
                self.runtime_data.resolve()
                / "dispatch"
                / "control-receipts"
                / "cs408-terminal-batch-retirements"
                / "sha256"
                / "44"
                / f"{'4' * 64}.json"
            ),
            "rollback_token": "5" * 64,
            "writer_postimage_sha256": "6" * 64,
            "postimage_verification_sha256": "7" * 64,
            "authority_snapshot_count": 2,
            "authority_snapshot_mcp_tool_call_count": 2,
            "model_mcp_tool_call_count": 0,
        }

        def preflight(**values) -> dict:
            proof = self.canary_preflight_proof(**values)
            for subject, readiness_sha in (("english", "8"), ("cs408", "9")):
                row = next(
                    item
                    for item in proof["subjects"]
                    if item["subject"] == subject
                )
                row.update(
                    {
                        "subject_batch_readiness": (
                            "ready"
                            if values["apply"]
                            else "recoverable_terminal_batch"
                        ),
                        "subject_batch_readiness_sha256": readiness_sha * 64,
                        "subject_recovery_required": subject == "english",
                        "subject_recovery_already_applied": (
                            values["apply"] and subject == "english"
                        ),
                        "subject_recovery_original_preclaim_failure_receipt_sha256": (
                            english_original if subject == "english" else None
                        ),
                        "subject_recovery_receipt_sha256": (
                            "a" * 64
                            if values["apply"] and subject == "english"
                            else None
                        ),
                        "subject_recovery_receipt_path": (
                            str(self.base / "english-recovery.json")
                            if values["apply"] and subject == "english"
                            else None
                        ),
                        "subject_recovery_rollback_token": (
                            "b" * 64
                            if values["apply"] and subject == "english"
                            else None
                        ),
                        "subject_readiness_mcp_tool_call_count": 1,
                        "subject_authority_generation": (
                            release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                                "target_generation"
                            ]
                            if subject == "cs408"
                            else "english-next"
                        ),
                        "subject_authority_fingerprint": (
                            release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                                "target_authority_fingerprint"
                            ]
                            if subject == "cs408"
                            else "c" * 64
                        ),
                    }
                )
                if subject == "cs408":
                    row.update(
                        {
                            "cs408_terminal_retirement_required": True,
                            "cs408_terminal_retirement_already_applied": values[
                                "apply"
                            ],
                            "cs408_terminal_retirement_terminal_receipt_sha256": (
                                release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                                    "terminal_receipt_sha256"
                                ]
                            ),
                            "cs408_terminal_retirement_receipt_sha256": (
                                retirement["retirement_receipt_sha256"]
                                if values["apply"]
                                else None
                            ),
                            "cs408_terminal_retirement_receipt_path": (
                                retirement["retirement_receipt_path"]
                                if values["apply"]
                                else None
                            ),
                            "cs408_terminal_retirement_rollback_token": (
                                retirement["rollback_token"]
                                if values["apply"]
                                else None
                            ),
                        }
                    )
            return proof

        recovery = {
            "original_preclaim_failure_receipt_sha256": english_original,
            "recovery_receipt_sha256": "a" * 64,
            "recovery_receipt_path": str(self.base / "english-recovery.json"),
            "rollback_token": "b" * 64,
            "preserved_queue_entry_sha256": "d" * 64,
        }
        staged = {"target_release_id": release_id, "target_activation_id": "e" * 64}
        finalization = {
            "recovery_receipt_sha256": "a" * 64,
            "target_release_id": release_id,
            "target_activation_id": "e" * 64,
            "old_queue_entry_sha256": "d" * 64,
            "supersede_receipt_sha256": "f" * 64,
            "replacement_queue_entry_sha256": "1" * 64,
            "replacement_task": {
                "unit_sha256": "2" * 64,
                "frozen_payload_sha256": "3" * 64,
                "task_object_sha256": "4" * 64,
                "producer_input_contract_sha256": "5" * 64,
                "source_event_set_sha256": "6" * 64,
            },
        }
        arm = {
            "recovery_receipt_sha256": "a" * 64,
            "target_release_id": release_id,
            "target_activation_id": "e" * 64,
        }

        def empty_process(*_values):
            return {
                "schema_version": release.PROCESS_SNAPSHOT_SCHEMA,
                "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "legacy_worker_pids": [],
                "dispatcher_pids": [],
                "dashboard_pids": [],
                "luna_pids": [],
                "process_set_sha256": "7" * 64,
            }

        drained = {
            "schema_version": "study-intake-rollback-drain-v1",
            "status": "drained",
            "release_id": release_id,
            "runtime_data_root": str(self.runtime_data.resolve()),
            "subjects": [
                {
                    "subject": subject,
                    "active_count": 0,
                    "claimed_total": 0,
                    "stale_count": 0,
                    "status_sha256": f"{index}" * 64,
                }
                for index, subject in enumerate(
                    release.RESUMABLE_SUBJECTS, start=1
                )
            ],
            "formal_write_count": 0,
        }
        old_service_restarts: list[list[Mapping[str, Any]]] = []
        with (
            mock.patch.object(
                release,
                "_retire_cs408_terminal_batch_before_canary_arm",
                return_value=retirement,
            ),
            mock.patch.object(
                release,
                "_recover_subject_batch_before_canary_arm",
                return_value=recovery,
            ),
            mock.patch.object(
                release,
                "_stage_subject_batch_recovery_activation",
                return_value=staged,
            ),
            mock.patch.object(
                release,
                "_finalize_subject_batch_recovery_before_canary_arm",
                return_value=finalization,
            ),
            mock.patch.object(
                release,
                "_arm_finalized_subject_batch_recovery",
                return_value=arm,
            ),
            mock.patch.object(
                release,
                "_rollback_subject_batch_recovery",
                return_value={"status": "rolled_back"},
            ),
            mock.patch.object(
                release,
                "_rollback_cs408_terminal_batch_retirement",
                side_effect=release.ReleaseError("injected-cs408-rollback-failure"),
            ),
            mock.patch.object(
                release,
                "_default_canary_shutdown_before_activation_rollback",
                return_value=drained,
            ),
            mock.patch.object(
                release, "_run_service_deactivation", return_value=None
            ),
            mock.patch.object(
                release,
                "_run_service_activation",
                side_effect=lambda topology, *_args, **_kwargs: (
                    old_service_restarts.append(list(topology))
                ),
            ),
        ):
            with self.assertRaisesRegex(
                release.ReleaseError, "activation_failed_rollback_incomplete"
            ):
                release.activate_canary_release(
                    release_base=self.release_base,
                    release_id=release_id,
                    active_link=active,
                    canary_manifest=manifest,
                    apply=True,
                    expected_current=previous_id,
                    drain_receipt=drain,
                    launchagent_dir=self.launchagents,
                    command_runner=self.FakeRunner(),
                    process_inspector=empty_process,
                    canary_pre_activation_verifier=preflight,
                    post_activation_verifier=lambda **_values: (_ for _ in ()).throw(
                        release.ReleaseError("injected-health-failure")
                    ),
                    canary_rollback_controller=self.canary_rollback_proof,
                    subject_recovery_expectations={"english": english_original},
                    cs408_terminal_retirement_terminal_receipt_sha256=(
                        release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR[
                            "terminal_receipt_sha256"
                        ]
                    ),
                )
        self.assertEqual(active.resolve(), target)
        self.assertFalse(any(previous_target == Path(row[0].get("working_directory", "")) for row in old_service_restarts if row))
        postcommits = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in (self.release_base / "deployments").glob("*.json")
        ]
        blocked = next(
            value
            for value in postcommits
            if value.get("status") == "rollback_incomplete"
            and value.get("cs408_terminal_retirement_error_code")
        )
        self.assertTrue(blocked["target_fenced"])
        self.assertFalse(blocked["old_services_restarted"])

    def test_cs408_rolled_back_prepare_requires_both_reopened_chains(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        descriptor = release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR

        def publish(label: str) -> tuple[str, str]:
            raw = release.canonical_bytes({"fixture": label})
            digest = release.sha256_bytes(raw)
            path = self.base / "closure-ca" / digest[:2] / f"{digest}.json"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            return digest, str(path)

        recovery_sha, recovery_path = publish("english-recovery")
        english_rollback_sha, english_rollback_path = publish(
            "english-recovery-rollback"
        )
        retirement_sha, retirement_path = publish("cs408-retirement")
        cs408_rollback_sha, cs408_rollback_path = publish(
            "cs408-retirement-rollback"
        )
        english_original = "3" * 64
        preserved_queue = "4" * 64
        english_token = "5" * 64
        cs408_token = "6" * 64
        writer_restored = "7" * 64
        batch_pointer_restored = "8" * 64
        canary_restored = "9" * 64
        deployment_gate_restored = "b" * 64
        restore_core = {
            "path": str(
                self.base
                / "dispatch"
                / "state"
                / "production-canary"
                / "english.json"
            ),
            "snapshot_state": "present",
            "snapshot_sha256": deployment_gate_restored,
            "restored_state": "present",
            "restored_sha256": deployment_gate_restored,
        }
        english_restore = {
            **restore_core,
            "proof_sha256": release.sha256_bytes(
                release.canonical_bytes(restore_core)
            ),
        }
        prepare = {
            "release_id": release_id,
            "subject_recovery_expectations": {"english": english_original},
            "cs408_terminal_retirement_authorization_descriptor": {
                **descriptor,
                "authorization_descriptor_sha256": (
                    release._authorization_descriptor_sha256(descriptor)
                ),
            },
        }
        retirement = {
            "schema_version": (
                "study-intake-cs408-terminal-batch-retirement-result-v1"
            ),
            "subject": "cs408",
            "status": "retired",
            "authorization_descriptor_sha256": (
                release._authorization_descriptor_sha256(descriptor)
            ),
            "terminal_receipt_sha256": descriptor[
                "terminal_receipt_sha256"
            ],
            "retirement_receipt_sha256": retirement_sha,
            "retirement_receipt_path": retirement_path,
            "rollback_token": cs408_token,
            "authority_snapshot_mcp_tool_call_count": 2,
            "model_mcp_tool_call_count": 0,
            "formal_write_count": 0,
        }
        recovery = {
            "schema_version": "study-intake-subject-batch-recovery-result-v1",
            "subject": "english",
            "status": "recovered",
            "original_preclaim_failure_receipt_sha256": english_original,
            "recovery_receipt_sha256": recovery_sha,
            "recovery_receipt_path": recovery_path,
            "rollback_token": english_token,
            "preserved_queue_entry_sha256": preserved_queue,
            "authority_snapshot_mcp_tool_call_count": 2,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 2,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        english_rollback = {
            "schema_version": (
                "study-intake-subject-batch-recovery-rollback-result-v1"
            ),
            "subject": "english",
            "status": "rolled_back",
            "recovery_receipt_sha256": recovery_sha,
            "rollback_token": english_token,
            "rollback_receipt_sha256": english_rollback_sha,
            "rollback_receipt_path": english_rollback_path,
            "writer_state_restored_sha256": writer_restored,
            "batch_pointer_restored_sha256": batch_pointer_restored,
            "canary_state_restored_sha256": canary_restored,
            "preserved_queue_entry_sha256": preserved_queue,
            "target_queue_withdrawn": True,
            "removed_mutable_pointer_count": 3,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        cs408_rollback = {
            "schema_version": (
                "study-intake-cs408-terminal-batch-retirement-rollback-result-v1"
            ),
            "subject": "cs408",
            "status": "rolled_back",
            "retirement_receipt_sha256": retirement_sha,
            "rollback_receipt_sha256": cs408_rollback_sha,
            "rollback_receipt_path": cs408_rollback_path,
            "rollback_token": cs408_token,
            "writer_state_restored_sha256": descriptor[
                "writer_preimage_sha256"
            ],
            "old_batch_sha256": descriptor["batch_sha256"],
            "batch_pointer_unchanged_sha256": descriptor[
                "batch_pointer_sha256"
            ],
            "batch_snapshot_unchanged_sha256": descriptor[
                "snapshot_sha256"
            ],
            "terminal_receipt_unchanged_sha256": descriptor[
                "terminal_receipt_sha256"
            ],
            "retirement_pointer_withdrawn": True,
            "retirement_intent_withdrawn": True,
            "rollback_postimage_verification_sha256": "a" * 64,
            "rollback_receipt_reopened": True,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        postcommit = {
            "status": "rolled_back",
            "rollback_errors": [],
            "previous_canary_state_restore_proofs": {
                "english": english_restore
            },
            "subject_batch_recovery_proof": recovery,
            "subject_batch_recovery_rollback_proof": english_rollback,
            "cs408_terminal_retirement_proof": retirement,
            "cs408_terminal_retirement_rollback_proof": cs408_rollback,
            "cs408_terminal_retirement_mcp_accounting": (
                release._cs408_retirement_apply_mcp_accounting(
                    retirement_completed=True,
                    english_recovery_completed=True,
                    arm_preflight_completed=False,
                )
            ),
        }
        english_reopened = {
            **english_rollback,
            "schema_version": (
                "study-intake-subject-batch-recovery-rollback-reopen-result-v1"
            ),
            "status": "reopened",
            "rollback_reopen_read_only": True,
            "rollback_receipt_reopened": True,
            "target_queue_withdrawn": True,
            "mutable_recovery_intents_withdrawn": True,
            "deployment_canary_state_postimage_sha256": (
                deployment_gate_restored
            ),
            "finalization_rollback_proof_count": 1,
        }

        def reopen_english(**values: object) -> dict[str, object]:
            expected_count = int(
                values["expected_finalization_rollback_proof_count"]
            )
            self.assertEqual(
                values["deployment_canary_state_sha256"],
                deployment_gate_restored,
            )
            self.assertEqual(values["script_root"], release.SOURCE_ROOT)
            return {
                **english_reopened,
                "finalization_rollback_proof_count": expected_count,
            }
        cs408_reopened = {
            **cs408_rollback,
            "schema_version": (
                "study-intake-cs408-terminal-batch-retirement-rollback-reopen-result-v1"
            ),
            "status": "reopened",
            "rollback_reopen_read_only": True,
            "authorization_descriptor_sha256": (
                release._authorization_descriptor_sha256(descriptor)
            ),
            "retirement_receipt_path": retirement_path,
        }
        with (
            mock.patch.object(
                release,
                "_reopen_subject_batch_recovery_rollback",
                side_effect=reopen_english,
            ),
            mock.patch.object(
                release,
                "_reopen_cs408_terminal_batch_retirement_rollback",
                return_value=cs408_reopened,
            ),
        ):
            self.assertTrue(
                release._cs408_retirement_postcommit_closes_prepare(
                    prepare=prepare,
                    postcommit=postcommit,
                    receipts={},
                    release_base=self.release_base,
                )
            )
            staged_activation_failure = {
                **postcommit,
                "error_code": (
                    "english_subject_batch_recovery_staged_activation_failed"
                ),
                "subject_batch_recovery_staged_activation_proof": None,
                "subject_batch_recovery_finalization_proof": None,
                "subject_batch_recovery_arm_proof": None,
                "subject_batch_recovery_rollback_proof": {
                    **english_rollback,
                    "removed_mutable_pointer_count": 0,
                },
            }
            self.assertTrue(
                release._cs408_retirement_postcommit_closes_prepare(
                    prepare=prepare,
                    postcommit=staged_activation_failure,
                    receipts={},
                    release_base=self.release_base,
                )
            )
            self.assertFalse(
                release._cs408_retirement_postcommit_closes_prepare(
                    prepare=prepare,
                    postcommit={
                        **staged_activation_failure,
                        "error_code": "unbound_early_failure",
                    },
                    receipts={},
                    release_base=self.release_base,
                )
            )
            for field, bad_value in (
                ("rollback_errors", ["english-rollback-incomplete"]),
                ("subject_batch_recovery_rollback_proof", None),
            ):
                with self.subTest(field=field):
                    invalid = {**postcommit, field: bad_value}
                    self.assertFalse(
                        release._cs408_retirement_postcommit_closes_prepare(
                            prepare=prepare,
                            postcommit=invalid,
                            receipts={},
                            release_base=self.release_base,
                        )
                    )

    def test_activate_canary_partial_startup_failure_rolls_back_every_service(
        self,
    ) -> None:
        previous = self.build(passed=True)
        previous_id = str(previous["release_id"])
        previous_target = Path(str(previous["release_dir"]))
        active = self.base / "canary-failure-current"
        active.symlink_to(previous_target)
        self.install_current_plists(previous_target, active)
        previous_plists = {
            path.name: path.read_bytes()
            for path in self.launchagents.glob("*.plist")
        }
        projection_path = (
            self.runtime_data / "state" / "dashboard_projection.json"
        )
        projection_path.parent.mkdir(parents=True, exist_ok=True)
        previous_projection = (
            b'{"schema_version":"study-intake-dashboard-projection-v2",'
            b'"release_id":"fa3b-rollback-fixture"}\n'
        )
        projection_path.write_bytes(previous_projection)
        target_projection = (
            b'{"schema_version":"study-intake-dashboard-projection-v3",'
            b'"release_id":"failed-target"}\n'
        )
        (self.source / "bin" / "worker.py").write_text(
            "print('canary-target')\n", encoding="utf-8"
        )
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        manifest = self.make_canary_manifest(built)
        drain_receipt = self.make_drain_receipt(
            previous_id, release_id, active
        )
        previous_state_root = (
            self.runtime_data
            / "dispatch"
            / "state"
            / "production-canary"
        )
        previous_state_root.mkdir(parents=True, exist_ok=True)
        previous_state_bytes = {}
        previous_gates = {}
        for subject in release.RESUMABLE_SUBJECTS:
            value = (
                json.dumps(
                    {
                        "fixture": "previous-active-canary",
                        "subject": subject,
                        "release_id": previous_id,
                    },
                    sort_keys=True,
                ).encode("utf-8")
                + b"\n"
            )
            path = previous_state_root / f"{subject}.json"
            path.write_bytes(value)
            previous_state_bytes[subject] = value
            previous_gates[subject] = {
                **self.canary_v2_controls(),
                "status": "production_canary_active",
                "state": "armed",
                "subject": subject,
                "release_id": previous_id,
                "activation_id": "9" * 64,
                "activated_at": "2026-08-10T00:00:00+00:00",
                "producer_authority_fingerprint": "8" * 64,
                "producer_high_watermark": {},
                "producer_high_watermark_sha256": "7" * 64,
                "producer_capture_enabled": True,
                "luna_consumer_enabled": True,
                "sol_formal_curation_enabled": False,
                "queue_depth": 0,
                "canary_queue_count": 0,
                "active_task_count": 0,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
                "production_accepted": False,
                "authority": {"method": "hmac-sha256"},
            }
            value = release.canonical_bytes(previous_gates[subject]) + b"\n"
            path.write_bytes(value)
            previous_state_bytes[subject] = value
        gates: dict[str, dict] = {}
        cancel_receipt_sha256: str | None = None
        cancel_receipt_path: Path | None = None
        emit_cancel_proof = True

        def canary_preflight(**values) -> dict:
            proof = self.canary_preflight_proof(**values)
            if not values["apply"]:
                return proof
            context = values["canary_context"]
            activation_id = release._three_subject_canary_activation_id(
                context
            )
            for subject in release.RESUMABLE_SUBJECTS:
                slot = context["slots"][subject]
                gates[subject] = {
                    **self.canary_v2_controls(),
                    "status": "production_canary_active",
                    "state": "armed",
                    "subject": subject,
                    "release_id": release_id,
                    "activation_id": activation_id,
                    "activated_at": slot["producer_high_watermark"][
                        "recorded_at"
                    ],
                    "producer_authority_fingerprint": slot[
                        "producer_authority_fingerprint"
                    ],
                    "producer_high_watermark": slot[
                        "producer_high_watermark"
                    ],
                    "producer_high_watermark_sha256": slot[
                        "producer_high_watermark_sha256"
                    ],
                    "producer_capture_enabled": True,
                    "luna_consumer_enabled": True,
                    "sol_formal_curation_enabled": False,
                    "queue_depth": 1 if subject == "math" else 0,
                    "canary_queue_count": 1 if subject == "math" else 0,
                    "active_task_count": 0,
                    "blocking_reason": None,
                    "next_action": "await_first_post_activation_capture",
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                    "production_accepted": False,
                    "authority": {"method": "hmac-sha256"},
                }
            release._atomic_bytes(projection_path, target_projection)
            return proof

        class FailingCanaryRunner(self.FakeRunner):
            def __call__(inner_self, command):
                nonlocal cancel_receipt_path, cancel_receipt_sha256
                inner_self.commands.append(list(command))
                if "--subject" in command:
                    subject = command[command.index("--subject") + 1]
                    gate = gates.get(subject)
                    if gate is None:
                        gate = previous_gates[subject]
                        if "deactivate-canary" in command:
                            gate.update(
                                {
                                    "status": "production_canary_inactive",
                                    "state": "inactive_rolled_back",
                                    "luna_consumer_enabled": False,
                                    "blocking_reason": "activation_rolled_back",
                                    "next_action": "new_activation_required",
                                }
                            )
                            (
                                previous_state_root / f"{subject}.json"
                            ).write_bytes(
                                release.canonical_bytes(gate) + b"\n"
                            )
                            return {
                                "returncode": 0,
                                "stdout": json.dumps(gate),
                            }
                        return {
                            "returncode": 0,
                            "stdout": json.dumps(
                                {
                                    "subject": subject,
                                    "draining": True,
                                    "active_count": 0,
                                    "claimed_total": 0,
                                    "stale_count": 0,
                                    "canary_gate": gate,
                                }
                            ),
                        }
                    if command[-1] == "pause-canary":
                        gate.update(
                            {
                                "state": "paused_drained",
                                "luna_consumer_enabled": False,
                                "blocking_reason": "paused_by_user",
                                "next_action": (
                                    "explicit_subject_resume_required"
                                ),
                            }
                        )
                        return {
                            "returncode": 0,
                            "stdout": json.dumps(gate),
                        }
                    if "deactivate-canary" in command:
                        gate.update(
                            {
                                "status": "production_canary_inactive",
                                "state": "inactive_rolled_back",
                                "luna_consumer_enabled": False,
                                "blocking_reason": "activation_rolled_back",
                                "next_action": "new_activation_required",
                            }
                        )
                        return {
                            "returncode": 0,
                            "stdout": json.dumps(gate),
                        }
                    return {
                        "returncode": 0,
                        "stdout": json.dumps(
                            {
                                "subject": subject,
                                "release_id": release_id,
                                "draining": True,
                                "active_count": gate["active_task_count"],
                                "claimed_total": gate["active_task_count"],
                                "stale_count": 0,
                                "canary_gate": gate,
                            }
                        ),
                    }
                if (
                    command[:2] == ["launchctl", "bootstrap"]
                    and command[-1].endswith(".math.plist")
                ):
                    gates["math"].update(
                        {
                            "state": "canary_in_flight",
                            "active_task_count": 1,
                            "next_action": "await_terminal",
                        }
                    )
                failed = (
                    command[:2] == ["launchctl", "bootstrap"]
                    and command[-1].endswith(".cs408.plist")
                    and not getattr(inner_self, "cs408_bootstrap_failed", False)
                )
                if failed:
                    inner_self.cs408_bootstrap_failed = True
                    return {"returncode": 1, "stdout": ""}
                if (
                    command[:2] == ["launchctl", "bootout"]
                    and command[-1].endswith(".math.plist")
                    and gates
                    and gates["math"]["active_task_count"] == 1
                ):
                    terminal_fields = {}
                    if emit_cancel_proof:
                        cancel_receipt = {
                            "schema_version": (
                                "study-intake-production-canary-terminal-v1"
                            ),
                            "subject": "math",
                            "release_id": release_id,
                            "activation_id": gates["math"]["activation_id"],
                            "terminal_kind": "emergency_hard_cancel",
                            "late_result_fenced": True,
                            "outcome": "cancelled",
                            "error_code": "daemon_shutdown",
                            "package_path": None,
                            "package_sha256": None,
                            "formal_write_count": 0,
                            "sol_enabled": False,
                            "authority": {"method": "hmac-sha256"},
                        }
                        cancel_receipt_path = self.publish_content_json(
                            cancel_receipt
                        )
                        cancel_receipt_sha256 = release.sha256_file(
                            cancel_receipt_path
                        )
                        terminal_fields = {
                            "last_emergency_cancel_at": dt.datetime.now(
                                dt.timezone.utc
                            ).isoformat(),
                            "last_emergency_cancel_receipt_sha256": (
                                cancel_receipt_sha256
                            ),
                            "last_terminal_receipt_sha256": (
                                cancel_receipt_sha256
                            ),
                            "last_terminal_receipt_path": str(
                                cancel_receipt_path
                            ),
                            "late_result_fence_status": "sealed",
                        }
                    gates["math"].update(
                        {
                            "state": "failed_drained",
                            "active_task_count": 0,
                            "luna_consumer_enabled": False,
                            "blocking_reason": "daemon_shutdown",
                            "next_action": (
                                "explicit_subject_resume_required"
                            ),
                            **terminal_fields,
                        }
                    )
                return {"returncode": 0, "stdout": ""}

        runner = FailingCanaryRunner()
        with self.assertRaisesRegex(
            release.ReleaseError, "activation_service_failed_rolled_back"
        ):
            release.activate_canary_release(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                canary_manifest=manifest,
                apply=True,
                expected_current=previous_id,
                drain_receipt=drain_receipt,
                launchagent_dir=self.launchagents,
                command_runner=runner,
                canary_pre_activation_verifier=canary_preflight,
                post_activation_verifier=self.canary_post_activation_proof,
            )

        self.assertEqual(active.resolve(), previous_target)
        self.assertEqual(
            {
                path.name: path.read_bytes()
                for path in self.launchagents.glob("*.plist")
            },
            previous_plists,
        )
        self.assertEqual(projection_path.read_bytes(), previous_projection)
        self.assertEqual(
            {
                subject: (previous_state_root / f"{subject}.json").read_bytes()
                for subject in release.RESUMABLE_SUBJECTS
            },
            previous_state_bytes,
        )
        self.assertIsNotNone(cancel_receipt_sha256)
        for phase in ("disable", "bootout"):
            labels = {
                command[-1]
                for command in runner.commands
                if command[:2] == ["launchctl", phase]
            }
            self.assertEqual(len(labels), 4)
        failed_bootstrap = next(
            index
            for index, command in enumerate(runner.commands)
            if command[:2] == ["launchctl", "bootstrap"]
            and command[-1].endswith(".cs408.plist")
        )
        topology_names_by_plist = {
            Path(str(service["template"])).name: service["name"]
            for service in release.CONCURRENT_TOPOLOGY
        }
        restored_bootstrap_names = {
            topology_names_by_plist[Path(command[-1]).name]
            for command in runner.commands[failed_bootstrap + 1 :]
            if command[:2] == ["launchctl", "bootstrap"]
        }
        self.assertEqual(
            restored_bootstrap_names,
            {service["name"] for service in release.CONCURRENT_TOPOLOGY},
        )
        shutdown_disable = next(
            index
            for index, command in enumerate(runner.commands)
            if index > failed_bootstrap
            and command[:2] == ["launchctl", "disable"]
            and command[-1].endswith(".math")
        )
        shutdown_bootout = next(
            index
            for index, command in enumerate(runner.commands)
            if index > shutdown_disable
            and command[:2] == ["launchctl", "bootout"]
            and command[-1].endswith(".math.plist")
        )
        shutdown_status = next(
            index
            for index, command in enumerate(runner.commands)
            if index > shutdown_bootout
            and "--subject" in command
            and command[command.index("--subject") + 1] == "math"
            and command[-1] == "status"
        )
        deactivate = next(
            index
            for index, command in enumerate(runner.commands)
            if index > shutdown_status and "deactivate-canary" in command
        )
        self.assertLess(shutdown_disable, shutdown_bootout)
        self.assertLess(shutdown_bootout, shutdown_status)
        self.assertLess(shutdown_status, deactivate)
        receipts = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in (self.release_base / "deployments").glob("*.json")
        ]
        postcommit = next(
            value
            for value in receipts
            if value.get("schema_version")
            == "study-intake-preprocessor-deployment-postcommit-v3"
            and value.get("status") == "rolled_back"
            and value.get("release_id") == release_id
        )
        math_shutdown = next(
            row
            for row in postcommit["rollback_drain_proof"]["subjects"]
            if row["subject"] == "math"
        )
        self.assertEqual(
            math_shutdown["emergency_cancel"]["terminal_receipt_sha256"],
            cancel_receipt_sha256,
        )
        projection_restore = postcommit[
            "dashboard_projection_restore_proof"
        ]
        projection_restore_core = dict(projection_restore)
        projection_restore_sha256 = projection_restore_core.pop(
            "proof_sha256"
        )
        self.assertEqual(
            projection_restore_sha256,
            release.sha256_bytes(
                release.canonical_bytes(projection_restore_core)
            ),
        )
        self.assertEqual(
            postcommit["dashboard_projection_restore_proof_sha256"],
            projection_restore_sha256,
        )
        self.assertEqual(
            projection_restore["snapshot_sha256"],
            release.sha256_bytes(previous_projection),
        )
        self.assertEqual(
            projection_restore["restored_sha256"],
            release.sha256_bytes(previous_projection),
        )

        gates.clear()
        emit_cancel_proof = False
        missing_proof_runner = FailingCanaryRunner()
        missing_proof_drain_receipt = self.make_drain_receipt(
            previous_id, release_id, active
        )
        with self.assertRaisesRegex(
            release.ReleaseError, "activation_failed_rollback_incomplete"
        ):
            release.activate_canary_release(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                canary_manifest=manifest,
                apply=True,
                expected_current=previous_id,
                drain_receipt=missing_proof_drain_receipt,
                launchagent_dir=self.launchagents,
                command_runner=missing_proof_runner,
                canary_pre_activation_verifier=canary_preflight,
                post_activation_verifier=self.canary_post_activation_proof,
                process_exit_timeout_seconds=0.05,
            )
        self.assertEqual(active.resolve(), Path(str(built["release_dir"])))
        incomplete_receipts = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in (self.release_base / "deployments").glob("*.json")
        ]
        self.assertTrue(
            any(
                value.get("status")
                == "rollback_incomplete_cancel_or_fence_failed"
                and value.get("release_id") == release_id
                for value in incomplete_receipts
            )
        )

    def test_three_subject_resume_seal_and_preview_are_zero_model(self) -> None:
        built = self.build_historical_priority(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "current-three-subject-preview"
        active.symlink_to(target)
        self.install_current_plists(target, active)
        receipt_path, subject_paths, _concurrency, _acceptance = (
            self.make_three_subject_resume_receipt(built, active)
        )
        commands: list[list[str]] = []

        def runner(command):
            commands.append(list(command))
            subject = command[command.index("--subject") + 1]
            if command[-1] == "status":
                value = {
                    "subject": subject,
                    "draining": True,
                    "active_count": 0,
                    "claimed_total": 0,
                }
            elif command[-1] == "audit":
                value = {
                    "subject": subject,
                    "eligible_count": 1,
                    "model_call_count": 0,
                    "formal_write_count": 0,
                }
            else:
                self.fail(f"unexpected preview command: {command}")
            return {"returncode": 0, "stdout": json.dumps(value)}

        planned = release.resume_all_dispatchers(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            acceptance_receipt=receipt_path,
            launchagent_dir=self.launchagents,
            apply=False,
            command_runner=runner,
        )
        self.assertEqual(planned["status"], "planned")
        self.assertEqual(planned["resume_model_call_count"], 0)
        self.assertEqual(planned["resume_provider_request_count"], 0)
        self.assertEqual(planned["formal_write_count"], 0)
        self.assertFalse(planned["sol_enabled"])
        self.assertFalse(any(command[0] == "launchctl" for command in commands))
        self.assertEqual(active.resolve(), target)

        math_receipt = subject_paths["math"]
        math_receipt.chmod(0o644)
        value = json.loads(math_receipt.read_text(encoding="utf-8"))
        value["report_sha256"] = "f" * 64
        math_receipt.write_bytes(release.canonical_bytes(value))
        math_receipt.chmod(0o444)
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_resume_subject_receipt_invalid",
        ):
            release.resume_all_dispatchers(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                acceptance_receipt=receipt_path,
                launchagent_dir=self.launchagents,
                apply=False,
                command_runner=runner,
            )

    def test_legacy_priority_resume_chain_rejects_v2_target_release(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "current-v2-resume-forbidden"
        active.symlink_to(target)

        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_resume_legacy_priority_only",
        ):
            self.make_three_subject_resume_receipt(built, active)
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_resume_legacy_priority_only",
        ):
            release.resume_all_dispatchers(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                acceptance_receipt=self.base / "unopened-v1-receipt.json",
                launchagent_dir=self.launchagents,
                apply=False,
                command_runner=lambda _command: {
                    "returncode": 1,
                    "stdout": "",
                },
            )

    def test_resume_all_starts_all_and_accepts_only_fresh_release_heartbeats(
        self,
    ) -> None:
        built = self.build_historical_priority(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "current-three-subject-success"
        active.symlink_to(target)
        self.install_current_plists(target, active)
        receipt_path, _subjects, _concurrency, _acceptance = (
            self.make_three_subject_resume_receipt(built, active)
        )
        heartbeat_root = (
            self.runtime_data
            / "dispatch"
            / "state"
            / "subject-projections"
        )
        heartbeat_root.mkdir(parents=True)
        draining = {subject: True for subject in release.RESUMABLE_SUBJECTS}
        commands: list[list[str]] = []

        def subject_for_plist(path: str) -> str | None:
            return next(
                (
                    subject
                    for subject in release.RESUMABLE_SUBJECTS
                    if path.endswith(f".{subject}.plist")
                ),
                None,
            )

        def runner(command):
            commands.append(list(command))
            if "--subject" in command:
                subject = command[command.index("--subject") + 1]
                if command[-1] == "status":
                    value = {
                        "subject": subject,
                        "draining": draining[subject],
                        "active_count": 0,
                        "claimed_total": 0,
                    }
                elif command[-1] == "audit":
                    value = {
                        "subject": subject,
                        "eligible_count": 1,
                        "model_call_count": 0,
                        "formal_write_count": 0,
                    }
                elif command[-1] == "resume":
                    draining[subject] = False
                    value = {
                        "subject": subject,
                        "draining": False,
                        "active_count": 0,
                        "claimed_total": 0,
                    }
                elif command[-1] == "drain":
                    draining[subject] = True
                    value = {
                        "subject": subject,
                        "draining": True,
                        "active_count": 0,
                        "claimed_total": 0,
                    }
                else:
                    self.fail(f"unexpected dispatcher command: {command}")
                return {"returncode": 0, "stdout": json.dumps(value)}
            if command[:2] == ["launchctl", "bootstrap"]:
                subject = subject_for_plist(command[-1])
                if subject is not None:
                    heartbeat = {
                        "schema_version": "study-intake-subject-projection-v3",
                        "subject": subject,
                        "release_id": release_id,
                        "daemon_status": "running",
                        "draining": False,
                        "heartbeat_interval_seconds": 15,
                        "updated_at": dt.datetime.now(
                            dt.timezone.utc
                        ).isoformat(),
                        "formal_write_count": 0,
                    }
                    (heartbeat_root / f"{subject}.json").write_bytes(
                        release.canonical_bytes(heartbeat)
                    )
            return {"returncode": 0, "stdout": ""}

        resumed = release.resume_all_dispatchers(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            acceptance_receipt=receipt_path,
            launchagent_dir=self.launchagents,
            apply=True,
            command_runner=runner,
            heartbeat_timeout_seconds=1,
        )
        self.assertEqual(resumed["status"], "resumed")
        self.assertEqual(set(resumed["heartbeat_sha256s"]), set(draining))
        self.assertTrue(all(value is False for value in draining.values()))
        for phase in ("enable", "bootstrap", "print"):
            self.assertEqual(
                sum(
                    command[:2] == ["launchctl", phase]
                    for command in commands
                ),
                3,
            )
        self.assertEqual(active.resolve(), target)

        preview_count = len(commands)
        rollback_preview = release.resume_all_dispatchers(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            acceptance_receipt=None,
            launchagent_dir=self.launchagents,
            apply=False,
            rollback=True,
            command_runner=runner,
        )
        self.assertEqual(rollback_preview["status"], "rollback_planned")
        self.assertFalse(
            any(
                command[:2]
                in (["launchctl", "disable"], ["launchctl", "bootout"])
                for command in commands[preview_count:]
            )
        )
        rolled_back = release.resume_all_dispatchers(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            acceptance_receipt=None,
            launchagent_dir=self.launchagents,
            apply=True,
            rollback=True,
            command_runner=runner,
        )
        self.assertEqual(rolled_back["status"], "rolled_back_drained")
        self.assertTrue(all(draining.values()))
        self.assertEqual(active.resolve(), target)

    def test_resume_all_failure_drains_disables_and_boots_out_every_dispatcher(
        self,
    ) -> None:
        built = self.build_historical_priority(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "current-three-subject-failure"
        active.symlink_to(target)
        self.install_current_plists(target, active)
        receipt_path, _subjects, _concurrency, _acceptance = (
            self.make_three_subject_resume_receipt(built, active)
        )
        draining = {subject: True for subject in release.RESUMABLE_SUBJECTS}
        commands: list[list[str]] = []

        def runner(command):
            commands.append(list(command))
            if "--subject" in command:
                subject = command[command.index("--subject") + 1]
                if command[-1] == "resume":
                    draining[subject] = False
                elif command[-1] == "drain":
                    draining[subject] = True
                if command[-1] == "audit":
                    value = {
                        "subject": subject,
                        "eligible_count": 0,
                        "model_call_count": 0,
                        "formal_write_count": 0,
                    }
                else:
                    value = {
                        "subject": subject,
                        "draining": draining[subject],
                        "active_count": 0,
                        "claimed_total": 0,
                    }
                return {"returncode": 0, "stdout": json.dumps(value)}
            failed = (
                command[:2] == ["launchctl", "bootstrap"]
                and command[-1].endswith(".cs408.plist")
            )
            return {"returncode": 1 if failed else 0, "stdout": ""}

        with self.assertRaisesRegex(
            release.ReleaseError, "three_subject_resume_failed_drained"
        ):
            release.resume_all_dispatchers(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                acceptance_receipt=receipt_path,
                launchagent_dir=self.launchagents,
                apply=True,
                command_runner=runner,
                heartbeat_timeout_seconds=1,
            )
        self.assertTrue(all(draining.values()))
        drain_subjects = {
            command[command.index("--subject") + 1]
            for command in commands
            if command[-1] == "drain"
        }
        self.assertEqual(drain_subjects, set(release.RESUMABLE_SUBJECTS))
        for phase in ("disable", "bootout"):
            labels = {
                command[-1]
                for command in commands
                if command[:2] == ["launchctl", phase]
            }
            self.assertEqual(len(labels), 3)
        self.assertEqual(active.resolve(), target)

    def test_global_emergency_stop_preview_and_apply_pause_canary_gates(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "current-global-emergency-stop"
        active.symlink_to(target)
        self.install_current_plists(target, active)
        draining = {subject: False for subject in release.RESUMABLE_SUBJECTS}
        commands: list[list[str]] = []
        gates: dict[str, dict] = {}
        high_watermarks: dict[str, str] = {}
        for index, subject in enumerate(release.RESUMABLE_SUBJECTS, start=1):
            watermark = {
                "schema_version": "study-intake-producer-high-watermark-v1",
                "subject": subject,
                "release_id": release_id,
                "recorded_at": "2026-08-11T00:00:00+00:00",
                "producer_authority_fingerprint": f"{index}" * 64,
                "source_event_ids": [],
                "source_event_set_sha256": release.sha256_bytes(b"[]"),
                "formal_write_count": 0,
            }
            watermark_sha256 = release.sha256_bytes(
                json.dumps(
                    watermark,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            )
            high_watermarks[subject] = watermark_sha256
            gates[subject] = {
                **self.canary_v2_controls(),
                "status": "production_canary_active",
                "state": "armed",
                "subject": subject,
                "release_id": release_id,
                "activation_id": f"{index + 3}" * 64,
                "activated_at": watermark["recorded_at"],
                "producer_authority_fingerprint": f"{index}" * 64,
                "producer_high_watermark": watermark,
                "producer_high_watermark_sha256": watermark_sha256,
                "producer_capture_enabled": True,
                "luna_consumer_enabled": True,
                "sol_formal_curation_enabled": False,
                "queue_depth": 2,
                "canary_queue_count": 2,
                "active_task_count": 0,
                "blocking_reason": None,
                "next_action": "await_first_post_activation_capture",
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
                "production_accepted": False,
                "authority": {"method": "hmac-sha256"},
            }

        def runner(command):
            commands.append(list(command))
            if "--subject" in command:
                subject = command[command.index("--subject") + 1]
                if command[-1] == "drain":
                    draining[subject] = True
                elif command[-1] == "pause-canary":
                    gates[subject].update(
                        {
                            "state": "paused_drained",
                            "luna_consumer_enabled": False,
                            "blocking_reason": "paused_by_user",
                            "next_action": "explicit_subject_resume_required",
                        }
                    )
                    return {
                        "returncode": 0,
                        "stdout": json.dumps(gates[subject]),
                    }
                value = {
                    "subject": subject,
                    "draining": draining[subject],
                    "active_count": 0,
                    "claimed_total": 0,
                    "canary_gate": gates[subject],
                }
                return {"returncode": 0, "stdout": json.dumps(value)}
            return {"returncode": 0, "stdout": ""}

        preview = release.resume_all_dispatchers(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            acceptance_receipt=None,
            launchagent_dir=self.launchagents,
            apply=False,
            rollback=True,
            command_runner=runner,
        )
        self.assertEqual(preview["status"], "rollback_planned")
        self.assertTrue(preview["global_emergency_stop"])
        self.assertFalse(any("pause-canary" in command for command in commands))
        self.assertFalse(any(command[-1] == "drain" for command in commands))

        stopped = release.resume_all_dispatchers(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            acceptance_receipt=None,
            launchagent_dir=self.launchagents,
            apply=True,
            rollback=True,
            command_runner=runner,
        )
        self.assertEqual(stopped["status"], "rolled_back_drained")
        self.assertTrue(stopped["global_emergency_stop"])
        self.assertTrue(stopped["producer_queues_preserved"])
        self.assertEqual(
            set(stopped["canary_gates_after"]),
            set(release.RESUMABLE_SUBJECTS),
        )
        self.assertTrue(all(draining.values()))
        for subject in release.RESUMABLE_SUBJECTS:
            self.assertEqual(gates[subject]["state"], "paused_drained")
            self.assertFalse(gates[subject]["luna_consumer_enabled"])
            self.assertTrue(gates[subject]["producer_capture_enabled"])
            self.assertEqual(
                gates[subject]["producer_high_watermark_sha256"],
                high_watermarks[subject],
            )
        self.assertEqual(
            sum("pause-canary" in command for command in commands), 3
        )
        for phase in ("disable", "bootout"):
            self.assertEqual(
                sum(command[:2] == ["launchctl", phase] for command in commands),
                3,
            )

    def test_global_emergency_stop_partial_pause_failure_still_boots_out_all(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "current-global-emergency-stop-failure"
        active.symlink_to(target)
        self.install_current_plists(target, active)
        commands: list[list[str]] = []
        draining = {subject: False for subject in release.RESUMABLE_SUBJECTS}
        gates: dict[str, dict] = {}
        for index, subject in enumerate(release.RESUMABLE_SUBJECTS, start=1):
            watermark = {
                "schema_version": "study-intake-producer-high-watermark-v1",
                "subject": subject,
                "release_id": release_id,
                "recorded_at": "2026-08-11T00:00:00+00:00",
                "producer_authority_fingerprint": f"{index}" * 64,
                "source_event_ids": [],
                "source_event_set_sha256": release.sha256_bytes(b"[]"),
                "formal_write_count": 0,
            }
            gates[subject] = {
                **self.canary_v2_controls(),
                "status": "production_canary_active",
                "state": "armed",
                "subject": subject,
                "release_id": release_id,
                "activation_id": f"{index + 3}" * 64,
                "activated_at": watermark["recorded_at"],
                "producer_authority_fingerprint": f"{index}" * 64,
                "producer_high_watermark": watermark,
                "producer_high_watermark_sha256": release.sha256_bytes(
                    json.dumps(
                        watermark,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ),
                "producer_capture_enabled": True,
                "luna_consumer_enabled": True,
                "sol_formal_curation_enabled": False,
                "queue_depth": 0,
                "canary_queue_count": 0,
                "active_task_count": 0,
                "blocking_reason": None,
                "next_action": "await_first_post_activation_capture",
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
                "production_accepted": False,
                "authority": {"method": "hmac-sha256"},
            }

        def runner(command):
            commands.append(list(command))
            if "--subject" in command:
                subject = command[command.index("--subject") + 1]
                if command[-1] == "drain":
                    draining[subject] = True
                elif command[-1] == "pause-canary":
                    if subject == "cs408":
                        return {"returncode": 1, "stdout": "{}"}
                    gates[subject].update(
                        {
                            "state": "paused_drained",
                            "luna_consumer_enabled": False,
                            "blocking_reason": "paused_by_user",
                            "next_action": "explicit_subject_resume_required",
                        }
                    )
                    return {
                        "returncode": 0,
                        "stdout": json.dumps(gates[subject]),
                    }
                return {
                    "returncode": 0,
                    "stdout": json.dumps(
                        {
                            "subject": subject,
                            "draining": draining[subject],
                            "active_count": 0,
                            "claimed_total": 0,
                            "canary_gate": gates[subject],
                        }
                    ),
                }
            return {"returncode": 0, "stdout": ""}

        with self.assertRaisesRegex(
            release.ReleaseError, "three_subject_resume_rollback_incomplete"
        ):
            release.resume_all_dispatchers(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                acceptance_receipt=None,
                launchagent_dir=self.launchagents,
                apply=True,
                rollback=True,
                command_runner=runner,
            )
        self.assertEqual(
            sum("pause-canary" in command for command in commands), 3
        )
        for phase in ("disable", "bootout"):
            self.assertEqual(
                sum(command[:2] == ["launchctl", phase] for command in commands),
                3,
            )
        self.assertEqual(gates["cs408"]["state"], "armed")
        self.assertEqual(gates["math"]["state"], "paused_drained")
        self.assertEqual(gates["english"]["state"], "paused_drained")

    def test_global_emergency_stop_active_task_boots_out_before_cancel_proof_pause(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "current-global-emergency-active"
        active.symlink_to(target)
        self.install_current_plists(target, active)
        commands: list[list[str]] = []
        active_counts = {
            subject: 1 if subject == "math" else 0
            for subject in release.RESUMABLE_SUBJECTS
        }
        draining = {subject: False for subject in release.RESUMABLE_SUBJECTS}
        cancel_receipt = {
            "schema_version": "study-intake-production-canary-terminal-v1",
            "subject": "math",
            "release_id": release_id,
            "activation_id": "4" * 64,
            "terminal_kind": "emergency_hard_cancel",
            "late_result_fenced": True,
            "outcome": "cancelled",
            "error_code": "daemon_shutdown",
            "package_path": None,
            "package_sha256": None,
            "formal_write_count": 0,
            "sol_enabled": False,
            "authority": {"method": "hmac-sha256"},
        }
        cancel_path = self.publish_content_json(cancel_receipt)
        cancel_sha256 = release.sha256_file(cancel_path)
        gates: dict[str, dict] = {}
        for index, subject in enumerate(release.RESUMABLE_SUBJECTS, start=1):
            watermark = {
                "schema_version": "study-intake-producer-high-watermark-v1",
                "subject": subject,
                "release_id": release_id,
                "recorded_at": "2026-08-11T00:00:00+00:00",
                "producer_authority_fingerprint": f"{index}" * 64,
                "source_event_ids": [],
                "source_event_set_sha256": release.sha256_bytes(b"[]"),
                "formal_write_count": 0,
            }
            gates[subject] = {
                **self.canary_v2_controls(),
                "status": "production_canary_active",
                "state": "canary_in_flight" if subject == "math" else "armed",
                "subject": subject,
                "release_id": release_id,
                "activation_id": f"{index + 3}" * 64,
                "activated_at": watermark["recorded_at"],
                "producer_authority_fingerprint": f"{index}" * 64,
                "producer_high_watermark": watermark,
                "producer_high_watermark_sha256": release.sha256_bytes(
                    json.dumps(
                        watermark,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ),
                "producer_capture_enabled": True,
                "luna_consumer_enabled": True,
                "sol_formal_curation_enabled": False,
                "queue_depth": 0,
                "canary_queue_count": 1 if subject == "math" else 0,
                "active_task_count": active_counts[subject],
                "blocking_reason": None,
                "next_action": "await_terminal" if subject == "math" else "await_capture",
                "last_emergency_cancel_at": None,
                "last_emergency_cancel_receipt_sha256": None,
                "last_terminal_receipt_sha256": None,
                "last_terminal_receipt_path": None,
                "late_result_fence_status": "not_required",
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
                "production_accepted": False,
                "authority": {"method": "hmac-sha256"},
            }

        def subject_for_plist(path: str) -> str | None:
            return next(
                (
                    subject
                    for subject in release.RESUMABLE_SUBJECTS
                    if path.endswith(f".{subject}.plist")
                ),
                None,
            )

        def runner(command):
            commands.append(list(command))
            if "--subject" in command:
                subject = command[command.index("--subject") + 1]
                if command[-1] == "drain":
                    draining[subject] = True
                elif command[-1] == "pause-canary":
                    gates[subject].update(
                        {
                            "state": "paused_drained",
                            "luna_consumer_enabled": False,
                            "blocking_reason": "paused_by_user",
                            "next_action": "explicit_subject_resume_required",
                        }
                    )
                    return {
                        "returncode": 0,
                        "stdout": json.dumps(gates[subject]),
                    }
                return {
                    "returncode": 0,
                    "stdout": json.dumps(
                        {
                            "subject": subject,
                            "draining": draining[subject],
                            "active_count": active_counts[subject],
                            "claimed_total": active_counts[subject],
                            "canary_gate": gates[subject],
                        }
                    ),
                }
            if command[:2] == ["launchctl", "bootout"]:
                subject = subject_for_plist(command[-1])
                if subject == "math":
                    active_counts[subject] = 0
                    draining[subject] = True
                    gates[subject].update(
                        {
                            "state": "failed_drained",
                            "active_task_count": 0,
                            "luna_consumer_enabled": False,
                            "blocking_reason": "daemon_shutdown",
                            "next_action": "explicit_subject_resume_required",
                            "last_emergency_cancel_at": dt.datetime.now(
                                dt.timezone.utc
                            ).isoformat(),
                            "last_emergency_cancel_receipt_sha256": cancel_sha256,
                            "last_terminal_receipt_sha256": cancel_sha256,
                            "last_terminal_receipt_path": str(cancel_path),
                            "late_result_fence_status": "sealed",
                        }
                    )
            return {"returncode": 0, "stdout": ""}

        stopped = release.resume_all_dispatchers(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            acceptance_receipt=None,
            launchagent_dir=self.launchagents,
            apply=True,
            rollback=True,
            command_runner=runner,
        )
        self.assertEqual(stopped["status"], "rolled_back_drained")
        self.assertEqual(
            stopped["canary_gates_after"]["math"]["emergency_cancel"][
                "terminal_receipt_sha256"
            ],
            cancel_sha256,
        )
        math_disable = next(
            index
            for index, command in enumerate(commands)
            if command[:2] == ["launchctl", "disable"]
            and command[-1].endswith(".math")
        )
        math_bootout = next(
            index
            for index, command in enumerate(commands)
            if command[:2] == ["launchctl", "bootout"]
            and command[-1].endswith(".math.plist")
        )
        math_shutdown_status = next(
            index
            for index, command in enumerate(commands)
            if index > math_bootout
            if "--subject" in command
            and command[command.index("--subject") + 1] == "math"
            and command[-1] == "status"
        )
        math_pause = next(
            index
            for index, command in enumerate(commands)
            if "--subject" in command
            and command[command.index("--subject") + 1] == "math"
            and command[-1] == "pause-canary"
        )
        self.assertLess(math_disable, math_bootout)
        self.assertLess(math_bootout, math_shutdown_status)
        self.assertLess(math_shutdown_status, math_pause)

    def test_resume_canary_subject_preview_apply_preserves_binding_and_heartbeat(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "current-resume-canary-subject"
        active.symlink_to(target)
        self.install_current_plists(target, active)
        subject = "math"
        watermark = {
            "schema_version": "study-intake-producer-high-watermark-v1",
            "subject": subject,
            "release_id": release_id,
            "recorded_at": "2026-08-11T00:00:00+00:00",
            "producer_authority_fingerprint": "1" * 64,
            "source_event_ids": [],
            "source_event_set_sha256": release.sha256_bytes(b"[]"),
            "formal_write_count": 0,
        }
        watermark_sha256 = release.sha256_bytes(
            json.dumps(
                watermark,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        gate = {
            **self.canary_v2_controls(),
            "status": "production_canary_active",
            "state": "paused_drained",
            "subject": subject,
            "release_id": release_id,
            "activation_id": "4" * 64,
            "activated_at": watermark["recorded_at"],
            "producer_authority_fingerprint": "1" * 64,
            "producer_high_watermark": watermark,
            "producer_high_watermark_sha256": watermark_sha256,
            "producer_capture_enabled": True,
            "luna_consumer_enabled": False,
            "sol_formal_curation_enabled": False,
            "queue_depth": 2,
            "canary_queue_count": 2,
            "active_task_count": 0,
            "unlocked_once": False,
            "blocking_reason": "paused_by_user",
            "next_action": "explicit_subject_resume_required",
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "production_accepted": False,
            "authority": {"method": "hmac-sha256"},
        }
        heartbeat_root = (
            self.runtime_data / "dispatch" / "state" / "subject-projections"
        )
        heartbeat_root.mkdir(parents=True)
        commands: list[list[str]] = []

        def runner(command):
            commands.append(list(command))
            if "--subject" in command:
                if command[-1] == "resume-canary":
                    gate.update(
                        {
                            "state": "armed",
                            "luna_consumer_enabled": True,
                            "blocking_reason": None,
                            "next_action": "consume_pending_post_activation_queue",
                        }
                    )
                    return {"returncode": 0, "stdout": json.dumps(gate)}
                return {
                    "returncode": 0,
                    "stdout": json.dumps(
                        {
                            "subject": subject,
                            "draining": True,
                            "active_count": 0,
                            "claimed_total": 0,
                            "canary_gate": gate,
                        }
                    ),
                }
            if command[:2] == ["launchctl", "bootstrap"]:
                heartbeat = {
                    "schema_version": "study-intake-subject-projection-v3",
                    "subject": subject,
                    "release_id": release_id,
                    "daemon_status": "running",
                    "draining": True,
                    "heartbeat_interval_seconds": 15,
                    "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "formal_write_count": 0,
                }
                (heartbeat_root / f"{subject}.json").write_bytes(
                    release.canonical_bytes(heartbeat)
                )
            return {"returncode": 0, "stdout": ""}

        preview = release.resume_canary_dispatchers(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            subject=subject,
            launchagent_dir=self.launchagents,
            apply=False,
            command_runner=runner,
        )
        self.assertEqual(preview["status"], "canary_resume_planned")
        self.assertEqual(preview["initial_canary_inflight_limit"], 1)
        self.assertEqual(preview["continuous_concurrency_limit"], 20)
        self.assertIsNone(preview["requested_service_tier"])
        self.assertFalse(preview["fast_mode_requested"])
        self.assertEqual(preview["fast_mode_effective"], "not_requested")
        self.assertFalse(any("resume-canary" in command for command in commands))
        preview_command_count = len(commands)
        resumed = release.resume_canary_dispatchers(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            subject=subject,
            launchagent_dir=self.launchagents,
            apply=True,
            command_runner=runner,
            heartbeat_timeout_seconds=1,
        )
        self.assertEqual(resumed["status"], "canary_resumed")
        self.assertEqual(gate["state"], "armed")
        self.assertEqual(
            gate["producer_high_watermark_sha256"], watermark_sha256
        )
        self.assertEqual(resumed["target_subjects"], [subject])
        self.assertEqual(
            set(resumed["heartbeat_sha256s"]), {subject}
        )
        self.assertTrue(
            any(
                "resume-canary" in command
                for command in commands[preview_command_count:]
            )
        )
        sealed = dict(resumed)
        sealed.pop("proof_sha256")
        release._verify_deployment_authority(
            sealed,
            release_base=self.release_base,
            purpose=release.CANARY_RESUME_PROOF_PURPOSE,
        )

    def test_pause_canary_active_subject_hard_cancels_then_runs_queue_watcher(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "current-pause-canary-active-subject"
        active.symlink_to(target)
        self.install_current_plists(target, active)
        subject = "math"
        watermark = {
            "schema_version": "study-intake-producer-high-watermark-v1",
            "subject": subject,
            "release_id": release_id,
            "recorded_at": "2026-08-11T00:00:00+00:00",
            "producer_authority_fingerprint": "1" * 64,
            "source_event_ids": [],
            "source_event_set_sha256": release.sha256_bytes(b"[]"),
            "formal_write_count": 0,
        }
        watermark_sha256 = release.sha256_bytes(
            json.dumps(
                watermark,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        cancel_receipt = {
            "schema_version": "study-intake-production-canary-terminal-v1",
            "subject": subject,
            "release_id": release_id,
            "activation_id": "4" * 64,
            "terminal_kind": "emergency_hard_cancel",
            "late_result_fenced": True,
            "outcome": "cancelled",
            "error_code": "daemon_shutdown",
            "package_path": None,
            "package_sha256": None,
            "formal_write_count": 0,
            "sol_enabled": False,
            "authority": {"method": "hmac-sha256"},
        }
        cancel_path = self.publish_content_json(cancel_receipt)
        cancel_sha256 = release.sha256_file(cancel_path)
        gate = {
            **self.canary_v2_controls(),
            "status": "production_canary_active",
            "state": "canary_in_flight",
            "subject": subject,
            "release_id": release_id,
            "activation_id": "4" * 64,
            "activated_at": watermark["recorded_at"],
            "producer_authority_fingerprint": "1" * 64,
            "producer_high_watermark": watermark,
            "producer_high_watermark_sha256": watermark_sha256,
            "producer_capture_enabled": True,
            "luna_consumer_enabled": True,
            "sol_formal_curation_enabled": False,
            "queue_depth": 0,
            "canary_queue_count": 1,
            "active_task_count": 1,
            "blocking_reason": None,
            "next_action": "await_terminal",
            "last_emergency_cancel_at": None,
            "last_emergency_cancel_receipt_sha256": None,
            "last_terminal_receipt_sha256": None,
            "last_terminal_receipt_path": None,
            "late_result_fence_status": "not_required",
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "production_accepted": False,
            "authority": {"method": "hmac-sha256"},
        }
        active_count = 1
        draining = True
        heartbeat_root = (
            self.runtime_data / "dispatch" / "state" / "subject-projections"
        )
        heartbeat_root.mkdir(parents=True)
        commands: list[list[str]] = []

        def runner(command):
            nonlocal active_count, draining
            commands.append(list(command))
            if "--subject" in command:
                self.assertEqual(
                    command[command.index("--subject") + 1], subject
                )
                if command[-1] == "drain":
                    draining = True
                elif command[-1] == "pause-canary":
                    gate.update(
                        {
                            "state": "paused_drained",
                            "luna_consumer_enabled": False,
                            "blocking_reason": "paused_by_user",
                            "next_action": "explicit_subject_resume_required",
                        }
                    )
                    return {"returncode": 0, "stdout": json.dumps(gate)}
                return {
                    "returncode": 0,
                    "stdout": json.dumps(
                        {
                            "subject": subject,
                            "draining": draining,
                            "active_count": active_count,
                            "claimed_total": active_count,
                            "canary_gate": gate,
                        }
                    ),
                }
            if command[:2] == ["launchctl", "bootout"]:
                active_count = 0
                gate.update(
                    {
                        "state": "failed_drained",
                        "active_task_count": 0,
                        "luna_consumer_enabled": False,
                        "blocking_reason": "daemon_shutdown",
                        "next_action": "explicit_subject_resume_required",
                        "last_emergency_cancel_at": dt.datetime.now(
                            dt.timezone.utc
                        ).isoformat(),
                        "last_emergency_cancel_receipt_sha256": cancel_sha256,
                        "last_terminal_receipt_sha256": cancel_sha256,
                        "last_terminal_receipt_path": str(cancel_path),
                        "late_result_fence_status": "sealed",
                    }
                )
            if command[:2] == ["launchctl", "bootstrap"]:
                # The restarted daemon remains consumer-paused, but a producer
                # capture is materialized into the durable post-HWM queue.
                gate.update({"queue_depth": 1, "canary_queue_count": 2})
                heartbeat = {
                    "schema_version": "study-intake-subject-projection-v3",
                    "subject": subject,
                    "release_id": release_id,
                    "daemon_status": "running",
                    "draining": True,
                    "heartbeat_interval_seconds": 15,
                    "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "formal_write_count": 0,
                }
                (heartbeat_root / f"{subject}.json").write_bytes(
                    release.canonical_bytes(heartbeat)
                )
            return {"returncode": 0, "stdout": ""}

        preview = release.pause_canary_dispatchers(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            subject=subject,
            launchagent_dir=self.launchagents,
            apply=False,
            command_runner=runner,
        )
        self.assertEqual(preview["status"], "canary_pause_planned")
        self.assertEqual(preview["initial_canary_inflight_limit"], 1)
        self.assertEqual(preview["continuous_concurrency_limit"], 20)
        self.assertIsNone(preview["requested_service_tier"])
        self.assertFalse(preview["fast_mode_requested"])
        self.assertEqual(preview["fast_mode_effective"], "not_requested")
        preview_count = len(commands)
        self.assertFalse(any("pause-canary" in command for command in commands))
        paused = release.pause_canary_dispatchers(
            release_base=self.release_base,
            release_id=release_id,
            active_link=active,
            subject=subject,
            launchagent_dir=self.launchagents,
            apply=True,
            command_runner=runner,
            heartbeat_timeout_seconds=1,
        )
        self.assertEqual(paused["status"], "canary_paused_watcher_running")
        self.assertEqual(gate["state"], "paused_drained")
        self.assertFalse(gate["luna_consumer_enabled"])
        self.assertTrue(gate["producer_capture_enabled"])
        self.assertEqual(gate["producer_high_watermark_sha256"], watermark_sha256)
        self.assertEqual(gate["canary_queue_count"], 2)
        self.assertEqual(
            paused["canary_gates_after"][subject]["emergency_cancel"][
                "terminal_receipt_sha256"
            ],
            cancel_sha256,
        )
        self.assertFalse(
            any(
                ".cs408" in " ".join(command)
                or ".english" in " ".join(command)
                for command in commands[preview_count:]
            )
        )
        sealed = dict(paused)
        sealed.pop("proof_sha256")
        release._verify_deployment_authority(
            sealed,
            release_base=self.release_base,
            purpose=release.CANARY_PAUSE_PROOF_PURPOSE,
        )

    def test_resume_canary_all_failure_repauses_only_target_set(self) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "current-resume-canary-all-failure"
        active.symlink_to(target)
        self.install_current_plists(target, active)
        gates: dict[str, dict] = {}
        for index, subject in enumerate(release.RESUMABLE_SUBJECTS, start=1):
            watermark = {
                "schema_version": "study-intake-producer-high-watermark-v1",
                "subject": subject,
                "release_id": release_id,
                "recorded_at": "2026-08-11T00:00:00+00:00",
                "producer_authority_fingerprint": f"{index}" * 64,
                "source_event_ids": [],
                "source_event_set_sha256": release.sha256_bytes(b"[]"),
                "formal_write_count": 0,
            }
            gates[subject] = {
                **self.canary_v2_controls(),
                "status": "production_canary_active",
                "state": "paused_drained",
                "subject": subject,
                "release_id": release_id,
                "activation_id": f"{index + 3}" * 64,
                "activated_at": watermark["recorded_at"],
                "producer_authority_fingerprint": f"{index}" * 64,
                "producer_high_watermark": watermark,
                "producer_high_watermark_sha256": release.sha256_bytes(
                    json.dumps(
                        watermark,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ),
                "producer_capture_enabled": True,
                "luna_consumer_enabled": False,
                "sol_formal_curation_enabled": False,
                "queue_depth": 1,
                "canary_queue_count": 1,
                "active_task_count": 0,
                "unlocked_once": False,
                "blocking_reason": "paused_by_user",
                "next_action": "explicit_subject_resume_required",
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
                "production_accepted": False,
                "authority": {"method": "hmac-sha256"},
            }
        cancel_receipt = {
            "schema_version": "study-intake-production-canary-terminal-v1",
            "subject": "math",
            "release_id": release_id,
            "activation_id": gates["math"]["activation_id"],
            "terminal_kind": "emergency_hard_cancel",
            "late_result_fenced": True,
            "outcome": "cancelled",
            "error_code": "daemon_shutdown",
            "package_path": None,
            "package_sha256": None,
            "formal_write_count": 0,
            "sol_enabled": False,
            "authority": {"method": "hmac-sha256"},
        }
        cancel_path = self.publish_content_json(cancel_receipt)
        cancel_sha256 = release.sha256_file(cancel_path)
        commands: list[list[str]] = []

        def runner(command):
            commands.append(list(command))
            if "--subject" in command:
                selected = command[command.index("--subject") + 1]
                if command[-1] == "resume-canary":
                    gates[selected].update(
                        {
                            "state": "armed",
                            "luna_consumer_enabled": True,
                            "blocking_reason": None,
                            "next_action": "consume_pending_post_activation_queue",
                        }
                    )
                    return {
                        "returncode": 0,
                        "stdout": json.dumps(gates[selected]),
                    }
                if command[-1] == "pause-canary":
                    gates[selected].update(
                        {
                            "state": "paused_drained",
                            "luna_consumer_enabled": False,
                            "blocking_reason": "paused_by_user",
                            "next_action": "explicit_subject_resume_required",
                        }
                    )
                    return {
                        "returncode": 0,
                        "stdout": json.dumps(gates[selected]),
                    }
                return {
                    "returncode": 0,
                    "stdout": json.dumps(
                        {
                            "subject": selected,
                            "draining": True,
                            "active_count": gates[selected][
                                "active_task_count"
                            ],
                            "claimed_total": gates[selected][
                                "active_task_count"
                            ],
                            "canary_gate": gates[selected],
                        }
                    ),
                }
            if (
                command[:2] == ["launchctl", "bootout"]
                and command[-1].endswith(".math.plist")
                and gates["math"]["active_task_count"] == 1
            ):
                gates["math"].update(
                    {
                        "state": "failed_drained",
                        "active_task_count": 0,
                        "luna_consumer_enabled": False,
                        "blocking_reason": "daemon_shutdown",
                        "next_action": "explicit_subject_resume_required",
                        "last_emergency_cancel_at": dt.datetime.now(
                            dt.timezone.utc
                        ).isoformat(),
                        "last_emergency_cancel_receipt_sha256": cancel_sha256,
                        "last_terminal_receipt_sha256": cancel_sha256,
                        "last_terminal_receipt_path": str(cancel_path),
                        "late_result_fence_status": "sealed",
                    }
                )
            if (
                command[:2] == ["launchctl", "bootstrap"]
                and command[-1].endswith(".math.plist")
            ):
                gates["math"].update(
                    {
                        "state": "canary_in_flight",
                        "active_task_count": 1,
                    }
                )
            failed = (
                command[:2] == ["launchctl", "bootstrap"]
                and command[-1].endswith(".cs408.plist")
            )
            return {"returncode": 1 if failed else 0, "stdout": ""}

        with self.assertRaisesRegex(
            release.ReleaseError, "canary_resume_failed_paused"
        ):
            release.resume_canary_dispatchers(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                subject="all",
                launchagent_dir=self.launchagents,
                apply=True,
                command_runner=runner,
                heartbeat_timeout_seconds=1,
            )
        self.assertTrue(
            all(
                gate["state"] == "paused_drained"
                and gate["luna_consumer_enabled"] is False
                for gate in gates.values()
            )
        )
        self.assertEqual(
            sum("pause-canary" in command for command in commands), 3
        )
        failure_receipts = []
        for path in (self.release_base / "deployments").glob("*.json"):
            value = json.loads(path.read_text(encoding="utf-8"))
            if (
                value.get("schema_version")
                == release.CANARY_RESUME_PROOF_SCHEMA
                and value.get("status") == "canary_resume_failed_paused"
            ):
                failure_receipts.append(value)
        self.assertEqual(len(failure_receipts), 1)
        self.assertEqual(
            failure_receipts[0]["canary_gates_after"]["math"][
                "emergency_cancel"
            ]["terminal_receipt_sha256"],
            cancel_sha256,
        )

    def test_resume_all_rejects_previous_release_heartbeats_and_drains_all(
        self,
    ) -> None:
        built = self.build_historical_priority(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        active = self.base / "current-three-subject-stale-heartbeat"
        active.symlink_to(target)
        self.install_current_plists(target, active)
        receipt_path, _subjects, _concurrency, _acceptance = (
            self.make_three_subject_resume_receipt(built, active)
        )
        heartbeat_root = (
            self.runtime_data
            / "dispatch"
            / "state"
            / "subject-projections"
        )
        heartbeat_root.mkdir(parents=True)
        draining = {subject: True for subject in release.RESUMABLE_SUBJECTS}

        def runner(command):
            if "--subject" in command:
                subject = command[command.index("--subject") + 1]
                if command[-1] == "resume":
                    draining[subject] = False
                elif command[-1] == "drain":
                    draining[subject] = True
                if command[-1] == "audit":
                    value = {
                        "subject": subject,
                        "eligible_count": 0,
                        "model_call_count": 0,
                        "formal_write_count": 0,
                    }
                else:
                    value = {
                        "subject": subject,
                        "draining": draining[subject],
                        "active_count": 0,
                        "claimed_total": 0,
                    }
                return {"returncode": 0, "stdout": json.dumps(value)}
            if command[:2] == ["launchctl", "bootstrap"]:
                subject = next(
                    (
                        item
                        for item in release.RESUMABLE_SUBJECTS
                        if command[-1].endswith(f".{item}.plist")
                    ),
                    None,
                )
                if subject is not None:
                    heartbeat = {
                        "subject": subject,
                        "release_id": "f" * 64,
                        "daemon_status": "running",
                        "draining": False,
                        "heartbeat_interval_seconds": 15,
                        "updated_at": dt.datetime.now(
                            dt.timezone.utc
                        ).isoformat(),
                        "formal_write_count": 0,
                    }
                    (heartbeat_root / f"{subject}.json").write_bytes(
                        release.canonical_bytes(heartbeat)
                    )
            return {"returncode": 0, "stdout": ""}

        with self.assertRaisesRegex(
            release.ReleaseError, "three_subject_resume_failed_drained"
        ):
            release.resume_all_dispatchers(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                acceptance_receipt=receipt_path,
                launchagent_dir=self.launchagents,
                apply=True,
                command_runner=runner,
                heartbeat_timeout_seconds=0.05,
            )
        self.assertTrue(all(draining.values()))
        self.assertEqual(active.resolve(), target)

    def test_external_profiles_apply_and_restore_exact_bytes_and_tree(self) -> None:
        target = self.base / "external-target"
        plugin = target / "plugin" / "kaoyan-study-intake"
        shutil_source = ROOT / "plugin" / "kaoyan-study-intake"
        import shutil

        shutil.copytree(shutil_source, plugin)
        components_path = plugin / "components.json"
        components = json.loads(components_path.read_text(encoding="utf-8"))
        mcp = components["mcp"]
        runtime = {
            "python_executable": mcp["python_executable"],
            "python_flags": mcp["python_flags"],
            "sealed_launcher_path": mcp["sealed_launcher_path"],
            "sealed_launcher_sha256": mcp["sealed_launcher_sha256"],
            "release_root": mcp["release_root"],
            "release_id": mcp["release_id"],
            "release_manifest_sha256": mcp["release_manifest_sha256"],
        }
        (plugin / "component-lock.json").write_bytes(
            release.canonical_bytes({"mcp_sealed_runtime": runtime})
        )
        config_path = self.base / "external" / "config.toml"
        config_path.parent.mkdir()
        old_config = (
            'model = "fixture"\n\n'
            "[mcp_servers.kaoyan_read]\n"
            'command = "/usr/bin/false"\n'
            'args = ["--old"]\n'
            'cwd = "/private/tmp"\n'
            "env = {}\n"
            "env_vars = []\n"
            "enabled = true\n\n"
            "[unrelated]\n"
            'value = "preserved"\n'
        ).encode("utf-8")
        config_path.write_bytes(old_config)
        pointer_path = self.base / "external" / "morning.json"
        old_pointer = release.canonical_bytes(
            {"schema": "study_read_mcp_morning_preparation_pointer_v1", "old": True}
        )
        pointer_path.write_bytes(old_pointer)
        cache_root = self.base / "external" / "cache"
        cache_path = cache_root / components["plugin"]["version"]
        cache_path.mkdir(parents=True)
        (cache_path / "old.txt").write_text("old-cache\n", encoding="utf-8")
        old_cache = release._directory_inventory(
            cache_path, error_code="fixture_invalid"
        )
        backup_root = self.base / "external" / "backup"

        snapshot = release._backup_external_profiles(
            target,
            codex_config_path=config_path,
            morning_pointer_path=pointer_path,
            plugin_cache_root=cache_root,
            backup_root=backup_root,
        )
        self.assertIsNotNone(snapshot)
        applied = release._apply_external_profiles(
            target, snapshot, token="fixture"
        )
        self.assertEqual(applied["status"], "applied")
        self.assertEqual(applied["mcp_profile_sha256"], release.sha256_bytes(
            release._render_ordinary_mcp_profile(runtime)
        ))
        self.assertEqual(
            release._directory_inventory(cache_path, error_code="fixture_invalid")[
                "tree_sha256"
            ],
            release._directory_inventory(plugin, error_code="fixture_invalid")[
                "tree_sha256"
            ],
        )
        installed_pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        self.assertEqual(installed_pointer["release_id"], runtime["release_id"])
        self.assertEqual(installed_pointer["python_flags"], ["-I", "-S"])
        self.assertEqual(installed_pointer["launcher_mode"], "client")
        self.assertEqual(installed_pointer["profile"], "morning_preparation")
        self.assertEqual(installed_pointer["subject"], "cs408")
        self.assertIn(b'[unrelated]\nvalue = "preserved"', config_path.read_bytes())

        restored = release._restore_external_profiles(
            snapshot, token="fixture"
        )
        self.assertEqual(restored["status"], "restored")
        self.assertEqual(config_path.read_bytes(), old_config)
        self.assertEqual(pointer_path.read_bytes(), old_pointer)
        self.assertEqual(
            release._directory_inventory(cache_path, error_code="fixture_invalid"),
            old_cache,
        )

    def test_atomic_replace_installs_read_only_directory_into_absent_target(
        self,
    ) -> None:
        source = self.base / "sealed-plugin-source"
        (source / "schemas").mkdir(parents=True)
        payload = source / "schemas" / "receipt.json"
        payload.write_text('{"sealed":true}\n', encoding="utf-8")
        payload.chmod(0o444)
        (source / "schemas").chmod(0o555)
        source.chmod(0o555)
        expected = release._directory_inventory(
            source, error_code="fixture_invalid"
        )
        live = self.base / "plugin-cache" / "sealed-version"

        installed = release._atomic_replace_directory(
            live, source, token="sealed-absent"
        )

        self.assertEqual(installed, expected)
        self.assertEqual(
            release._directory_inventory(live, error_code="fixture_invalid"),
            expected,
        )
        self.assertEqual(stat.S_IMODE(live.lstat().st_mode), 0o555)
        self.assertFalse(
            (live.parent / ".sealed-version.sealed-absent.rollback").exists()
        )

    def test_atomic_replace_preserves_read_only_live_mode_on_failed_rollover(
        self,
    ) -> None:
        source = self.base / "sealed-replacement-source"
        source.mkdir()
        (source / "new.txt").write_text("new\n", encoding="utf-8")
        source.chmod(0o555)
        live = self.base / "plugin-cache-present" / "sealed-version"
        live.mkdir(parents=True)
        (live / "old.txt").write_text("old\n", encoding="utf-8")
        live.chmod(0o555)
        original = release._directory_inventory(
            live, error_code="fixture_invalid"
        )

        with mock.patch.object(
            release.os,
            "replace",
            side_effect=PermissionError(13, "fixture rollover denied"),
        ):
            with self.assertRaises(PermissionError):
                release._atomic_replace_directory(
                    live, source, token="sealed-rollover-failure"
                )

        self.assertEqual(
            release._directory_inventory(live, error_code="fixture_invalid"),
            original,
        )
        self.assertEqual(stat.S_IMODE(live.lstat().st_mode), 0o555)
        self.assertFalse(
            (live.parent / ".sealed-version.sealed-rollover-failure.rollback").exists()
        )

    def test_atomic_replace_rolls_over_read_only_live_and_restores_target_mode(
        self,
    ) -> None:
        source = self.base / "sealed-present-replacement-source"
        source.mkdir()
        (source / "new.txt").write_text("new\n", encoding="utf-8")
        source.chmod(0o555)
        expected = release._directory_inventory(
            source, error_code="fixture_invalid"
        )
        live = self.base / "plugin-cache-read-only" / "sealed-version"
        live.mkdir(parents=True)
        (live / "old.txt").write_text("old\n", encoding="utf-8")
        live.chmod(0o555)

        installed = release._atomic_replace_directory(
            live, source, token="sealed-present"
        )

        self.assertEqual(installed, expected)
        self.assertEqual(
            release._directory_inventory(live, error_code="fixture_invalid"),
            expected,
        )
        self.assertEqual(stat.S_IMODE(live.lstat().st_mode), 0o555)
        self.assertFalse(
            (live.parent / ".sealed-version.sealed-present.rollback").exists()
        )

    def test_new_plugin_version_uses_absent_cache_preimage_and_preserves_old_cache(
        self,
    ) -> None:
        target = self.base / "new-version-external-target"
        plugin = target / "plugin" / "kaoyan-study-intake"
        import shutil

        shutil.copytree(ROOT / "plugin" / "kaoyan-study-intake", plugin)
        components = json.loads(
            (plugin / "components.json").read_text(encoding="utf-8")
        )
        mcp = components["mcp"]
        runtime = {
            "python_executable": mcp["python_executable"],
            "python_flags": mcp["python_flags"],
            "sealed_launcher_path": mcp["sealed_launcher_path"],
            "sealed_launcher_sha256": mcp["sealed_launcher_sha256"],
            "release_root": mcp["release_root"],
            "release_id": mcp["release_id"],
            "release_manifest_sha256": mcp["release_manifest_sha256"],
        }
        (plugin / "component-lock.json").write_bytes(
            release.canonical_bytes({"mcp_sealed_runtime": runtime})
        )
        external = self.base / "new-version-external"
        external.mkdir()
        config_path = external / "config.toml"
        old_config = (
            '[mcp_servers.kaoyan_read]\ncommand="/usr/bin/false"\n'
            'args=[]\ncwd="/private/tmp"\nenv={}\nenv_vars=[]\n'
            "enabled=true\n"
        ).encode("utf-8")
        config_path.write_bytes(old_config)
        pointer_path = external / "morning.json"
        old_pointer = release.canonical_bytes({"old": True})
        pointer_path.write_bytes(old_pointer)
        cache_root = external / "cache"
        old_cache_path = cache_root / "0.4.0-canary.9+codex.old-version"
        old_cache_path.mkdir(parents=True)
        (old_cache_path / "sentinel.txt").write_text(
            "immutable-old-cache\n", encoding="utf-8"
        )
        old_cache_before = release._directory_inventory(
            old_cache_path, error_code="fixture_invalid"
        )
        target_cache_path = cache_root / components["plugin"]["version"]
        self.assertNotEqual(target_cache_path, old_cache_path)
        self.assertFalse(target_cache_path.exists())

        plan = release._external_profile_plan(
            target,
            codex_config_path=config_path,
            morning_pointer_path=pointer_path,
            plugin_cache_root=cache_root,
        )
        assert plan is not None
        self.assertEqual(
            plan["preimage_audit"]["plugin_cache_state"], "absent"
        )
        self.assertIsNone(
            plan["preimage_audit"]["plugin_cache_tree_sha256"]
        )
        snapshot = release._backup_external_profiles(
            target,
            codex_config_path=config_path,
            morning_pointer_path=pointer_path,
            plugin_cache_root=cache_root,
            backup_root=external / "backup",
        )
        assert snapshot is not None
        self.assertEqual(snapshot["plugin_cache"]["state"], "absent")
        self.assertIsNone(snapshot["plugin_cache"]["backup_path"])
        target_cache_path.mkdir()
        (target_cache_path / "concurrent.txt").write_text(
            "concurrent-cache-claim\n", encoding="utf-8"
        )
        with self.assertRaisesRegex(
            release.ReleaseError, "plugin_cache_changed_after_backup"
        ):
            release._apply_external_profiles(
                target, snapshot, token="new-version-drift"
            )
        shutil.rmtree(target_cache_path)
        release._apply_external_profiles(target, snapshot, token="new-version")
        self.assertTrue(target_cache_path.is_dir())
        self.assertEqual(
            release._directory_inventory(
                old_cache_path, error_code="fixture_invalid"
            ),
            old_cache_before,
        )
        restored = release._restore_external_profiles(
            snapshot, token="new-version"
        )
        self.assertEqual(restored["plugin_cache_state"], "absent")
        self.assertFalse(
            target_cache_path.exists() or target_cache_path.is_symlink()
        )
        self.assertEqual(config_path.read_bytes(), old_config)
        self.assertEqual(pointer_path.read_bytes(), old_pointer)
        self.assertEqual(
            release._directory_inventory(
                old_cache_path, error_code="fixture_invalid"
            ),
            old_cache_before,
        )

    def test_external_profile_preview_reopens_every_backup_preimage_read_only(
        self,
    ) -> None:
        built = self.build(passed=True)
        target = Path(str(built["release_dir"]))
        verified, components = self.install_external_target_fixture(target)
        external = self.base / "preview-external"
        external.mkdir()
        config_path = external / "config.toml"
        old_config = (
            'model = "fixture"\n\n'
            "[mcp_servers.kaoyan_read]\n"
            'command = "/usr/bin/false"\n'
            'args = ["--old"]\n'
            'cwd = "/private/tmp"\n'
            "env = {}\n"
            "env_vars = []\n"
            "enabled = true\n\n"
            "[unrelated]\n"
            'value = "preserved"\n'
        ).encode("utf-8")
        config_path.write_bytes(old_config)
        pointer_path = external / "morning.json"
        old_pointer = release.canonical_bytes(
            {"schema": "study_read_mcp_morning_preparation_pointer_v1", "old": True}
        )
        pointer_path.write_bytes(old_pointer)
        cache_root = external / "cache"
        cache_path = cache_root / components["plugin"]["version"]
        cache_path.mkdir(parents=True)
        (cache_path / "old.txt").write_text("old-cache\n", encoding="utf-8")
        old_cache = release._directory_inventory(
            cache_path, error_code="fixture_invalid"
        )

        with mock.patch.object(release, "verify_release", return_value=verified):
            planned = release.activate_release(
                release_base=self.release_base,
                release_id=str(built["release_id"]),
                active_link=self.base / "preview-current",
                apply=False,
                operation="activate",
                launchagent_dir=self.launchagents,
                codex_config_path=config_path,
                morning_pointer_path=pointer_path,
                plugin_cache_root=cache_root,
            )
        audit = planned["external_profile_plan"]["preimage_audit"]
        audit_core = dict(audit)
        proof_sha256 = audit_core.pop("proof_sha256")
        self.assertEqual(audit["status"], "verified_backup_ready")
        self.assertEqual(
            proof_sha256,
            release.sha256_bytes(release.canonical_bytes(audit_core)),
        )
        self.assertEqual(audit["ordinary_config_sha256"], release.sha256_bytes(old_config))
        self.assertEqual(audit["morning_pointer_sha256"], release.sha256_bytes(old_pointer))
        self.assertEqual(audit["plugin_cache_tree_sha256"], old_cache["tree_sha256"])
        self.assertEqual(config_path.read_bytes(), old_config)
        self.assertEqual(pointer_path.read_bytes(), old_pointer)
        self.assertEqual(
            release._directory_inventory(cache_path, error_code="fixture_invalid"),
            old_cache,
        )

        pointer_path.write_text("[]\n", encoding="utf-8")
        with (
            mock.patch.object(release, "verify_release", return_value=verified),
            self.assertRaisesRegex(release.ReleaseError, "morning_pointer_invalid"),
        ):
            release.activate_release(
                release_base=self.release_base,
                release_id=str(built["release_id"]),
                active_link=self.base / "preview-current",
                apply=False,
                operation="activate",
                launchagent_dir=self.launchagents,
                codex_config_path=config_path,
                morning_pointer_path=pointer_path,
                plugin_cache_root=cache_root,
            )

    def test_external_profiles_restore_with_current_and_plists_after_start_failure(
        self,
    ) -> None:
        previous = self.build(passed=True)
        previous_target = Path(str(previous["release_dir"]))
        active = self.base / "external-transaction-current"
        active.symlink_to(previous_target)
        self.install_current_plists(previous_target, active)
        previous_plists = {
            path.name: path.read_bytes()
            for path in self.launchagents.glob("*.plist")
        }
        (self.source / "bin" / "worker.py").write_text(
            "print('external-target')\n", encoding="utf-8"
        )
        target_release = self.build(passed=True)
        target = Path(str(target_release["release_dir"]))
        verified, components = self.install_external_target_fixture(target)

        external = self.base / "transaction-external"
        external.mkdir()
        config_path = external / "config.toml"
        old_config = (
            'model = "fixture"\n\n'
            "[mcp_servers.kaoyan_read]\n"
            'command = "/usr/bin/false"\n'
            'args = ["--old"]\n'
            'cwd = "/private/tmp"\n'
            "env = {}\n"
            "env_vars = []\n"
            "enabled = true\n\n"
            "[unrelated]\n"
            'value = "preserved"\n'
        ).encode("utf-8")
        config_path.write_bytes(old_config)
        pointer_path = external / "morning.json"
        old_pointer = release.canonical_bytes(
            {"schema": "study_read_mcp_morning_preparation_pointer_v1", "old": True}
        )
        pointer_path.write_bytes(old_pointer)
        cache_root = external / "cache"
        cache_path = cache_root / components["plugin"]["version"]
        cache_path.mkdir(parents=True)
        (cache_path / "old.txt").write_text("old-cache\n", encoding="utf-8")
        old_cache = release._directory_inventory(
            cache_path, error_code="fixture_invalid"
        )
        target_runtime, _mcp = release._sealed_runtime_from_target(target)
        observed_applied = False

        class FailingDashboardBootstrapRunner(self.FakeRunner):
            failed_target_bootstrap = False

            def __call__(inner_self, command):
                nonlocal observed_applied
                if (
                    command[:2] == ["launchctl", "bootstrap"]
                    and "com.xiazhibin.study-intake-dashboard" in " ".join(command)
                    and not inner_self.failed_target_bootstrap
                ):
                    inner_self.failed_target_bootstrap = True
                    _start, _end, installed = release._ordinary_mcp_profile_span(
                        config_path.read_bytes()
                    )
                    observed_applied = (
                        installed
                        == release._render_ordinary_mcp_profile(target_runtime)
                        and json.loads(pointer_path.read_text(encoding="utf-8"))[
                            "release_id"
                        ]
                        == target_runtime["release_id"]
                        and release._directory_inventory(
                            cache_path, error_code="fixture_invalid"
                        )["tree_sha256"]
                        == release._directory_inventory(
                            target / "plugin" / "kaoyan-study-intake",
                            error_code="fixture_invalid",
                        )["tree_sha256"]
                    )
                    inner_self.commands.append(list(command))
                    return {"returncode": 1, "stdout": ""}
                return super().__call__(command)

        runner = FailingDashboardBootstrapRunner()
        with (
            mock.patch.object(release, "verify_release", return_value=verified),
            self.assertRaisesRegex(
                release.ReleaseError, "activation_service_failed_rolled_back"
            ),
        ):
            release.activate_release(
                release_base=self.release_base,
                release_id=str(target_release["release_id"]),
                active_link=active,
                apply=True,
                operation="activate",
                expected_current=str(previous["release_id"]),
                drain_receipt=self.make_drain_receipt(
                    str(previous["release_id"]),
                    str(target_release["release_id"]),
                    active,
                ),
                launchagent_dir=self.launchagents,
                command_runner=runner,
                codex_config_path=config_path,
                morning_pointer_path=pointer_path,
                plugin_cache_root=cache_root,
            )

        self.assertTrue(observed_applied)
        self.assertEqual(active.resolve(), previous_target)
        self.assertEqual(
            {
                path.name: path.read_bytes()
                for path in self.launchagents.glob("*.plist")
            },
            previous_plists,
        )
        self.assertEqual(config_path.read_bytes(), old_config)
        self.assertEqual(pointer_path.read_bytes(), old_pointer)
        self.assertEqual(
            release._directory_inventory(cache_path, error_code="fixture_invalid"),
            old_cache,
        )
        receipts = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in (self.release_base / "deployments").glob("*.json")
        ]
        postcommit = next(
            value
            for value in receipts
            if value.get("schema_version")
            == "study-intake-preprocessor-deployment-postcommit-v3"
            and value.get("release_id") == str(target_release["release_id"])
            and value.get("status") == "rolled_back"
        )
        self.assertEqual(postcommit["external_profile_apply_proof"]["status"], "applied")
        self.assertEqual(postcommit["external_profile_restore_proof"]["status"], "restored")

    def test_external_profile_backup_detects_post_backup_drift(self) -> None:
        target = self.base / "external-drift-target"
        plugin = target / "plugin" / "kaoyan-study-intake"
        import shutil

        shutil.copytree(ROOT / "plugin" / "kaoyan-study-intake", plugin)
        components = json.loads((plugin / "components.json").read_text(encoding="utf-8"))
        mcp = components["mcp"]
        runtime = {
            "python_executable": mcp["python_executable"],
            "python_flags": mcp["python_flags"],
            "sealed_launcher_path": mcp["sealed_launcher_path"],
            "sealed_launcher_sha256": mcp["sealed_launcher_sha256"],
            "release_root": mcp["release_root"],
            "release_id": mcp["release_id"],
            "release_manifest_sha256": mcp["release_manifest_sha256"],
        }
        (plugin / "component-lock.json").write_bytes(
            release.canonical_bytes({"mcp_sealed_runtime": runtime})
        )
        external = self.base / "external-drift"
        external.mkdir()
        config_path = external / "config.toml"
        config_path.write_text(
            "[mcp_servers.kaoyan_read]\ncommand='old'\n",
            encoding="utf-8",
        )
        pointer_path = external / "morning.json"
        pointer_path.write_text("{}\n", encoding="utf-8")
        cache_root = external / "cache"
        cache_path = cache_root / components["plugin"]["version"]
        cache_path.mkdir(parents=True)
        (cache_path / "old.txt").write_text("old\n", encoding="utf-8")
        snapshot = release._backup_external_profiles(
            target,
            codex_config_path=config_path,
            morning_pointer_path=pointer_path,
            plugin_cache_root=cache_root,
            backup_root=external / "backup",
        )
        config_path.write_text(
            "[mcp_servers.kaoyan_read]\ncommand='concurrent-edit'\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(
            release.ReleaseError, "ordinary_profile_changed_after_backup"
        ):
            release._apply_external_profiles(target, snapshot, token="drift")

    def exact_0adf_early_rollback_receipts(
        self,
    ) -> dict[str, dict[str, Any]]:
        descriptor = release.EXACT_20260813_0ADF_EARLY_ROLLBACK_CLOSURE
        prepare_sha256 = descriptor["prepare_receipt_sha256"]
        postcommit_sha256 = descriptor["postcommit_receipt_sha256"]
        retirement_authorization = {
            **release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR,
            "authorization_descriptor_sha256": (
                release._authorization_descriptor_sha256(
                    release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR
                )
            ),
        }

        def restore_proof(spec: Mapping[str, Any]) -> dict[str, Any]:
            proof = {
                "path": spec["path"],
                "snapshot_state": "present",
                "snapshot_sha256": spec["state_sha256"],
                "restored_state": "present",
                "restored_sha256": spec["state_sha256"],
            }
            proof["proof_sha256"] = release.sha256_bytes(
                release.canonical_bytes(proof)
            )
            self.assertEqual(proof["proof_sha256"], spec["proof_sha256"])
            return proof

        prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "operation": "activate",
            "release_id": descriptor["target_release_id"],
            "active_link": descriptor["active_link"],
            "expected_current": descriptor["previous_release_id"],
            "observed_current": descriptor["previous_release_id"],
            "formal_write_count": 0,
            "cs408_terminal_retirement_authorization_descriptor": (
                retirement_authorization
            ),
            "subject_recovery_expectations": {
                "english": descriptor["english_preclaim_receipt_sha256"]
            },
        }
        null_target_mutation_fields = (
            "previous_canary_deactivation_proof",
            "external_profile_apply_proof",
            "external_profile_restore_proof",
            "cs408_terminal_retirement_proof",
            "cs408_writer_retirement_binding",
            "cs408_terminal_retirement_mcp_accounting",
            "cs408_terminal_retirement_rollback_proof",
            "subject_batch_recovery_proof",
            "subject_batch_recovery_binding",
            "subject_batch_recovery_staged_activation_proof",
            "subject_batch_recovery_finalization_proof",
            "subject_batch_recovery_arm_proof",
            "subject_batch_recovery_rollback_proof",
            "canary_arm_proof",
            "canary_rollback_proof",
            "rollback_drain_proof",
        )
        postcommit: dict[str, Any] = {
            "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
            "operation": "activate",
            "release_id": descriptor["target_release_id"],
            "prepare_receipt_sha256": prepare_sha256,
            "status": "rolled_back",
            "error_code": descriptor["error_code"],
            "restored_release_id": descriptor["previous_release_id"],
            "formal_write_count": 0,
            "rollback_errors": [],
            "previous_canary_state_restore_proofs": {
                subject: restore_proof(spec)
                for subject, spec in descriptor[
                    "canary_restore_proofs"
                ].items()
            },
            "dashboard_projection_restore_proof": restore_proof(
                descriptor["dashboard_restore_proof"]
            ),
            "dashboard_projection_restore_proof_sha256": descriptor[
                "dashboard_restore_proof"
            ]["proof_sha256"],
        }
        postcommit.update(
            {field: None for field in null_target_mutation_fields}
        )
        return {
            prepare_sha256: prepare,
            postcommit_sha256: postcommit,
        }

    def test_exact_0adf_early_rollback_pair_closes_without_live_reads(
        self,
    ) -> None:
        receipts = self.exact_0adf_early_rollback_receipts()
        forbidden_read = AssertionError("exact rollback closure read live state")
        with (
            mock.patch.object(
                release,
                "_deployment_receipt_objects",
                return_value=receipts,
            ),
            mock.patch.object(
                release, "_current_release", side_effect=forbidden_read
            ),
            mock.patch.object(
                release, "verify_release", side_effect=forbidden_read
            ),
            mock.patch.object(
                Path, "read_bytes", side_effect=forbidden_read
            ),
            mock.patch.object(
                Path, "read_text", side_effect=forbidden_read
            ),
            mock.patch.object(Path, "lstat", side_effect=forbidden_read),
            mock.patch.object(Path, "exists", side_effect=forbidden_read),
            mock.patch.object(Path, "is_file", side_effect=forbidden_read),
            mock.patch.object(Path, "resolve", side_effect=forbidden_read),
        ):
            release._assert_no_unresolved_deployment_transaction(
                self.release_base
            )

    def test_exact_0adf_early_rollback_pair_rejects_receipt_drift(
        self,
    ) -> None:
        descriptor = release.EXACT_20260813_0ADF_EARLY_ROLLBACK_CLOSURE
        postcommit_sha256 = descriptor["postcommit_receipt_sha256"]

        def set_postcommit(
            receipts: dict[str, dict[str, Any]], key: str, value: Any
        ) -> None:
            receipts[postcommit_sha256][key] = value

        cases: dict[
            str, Callable[[dict[str, dict[str, Any]]], None]
        ] = {
            "error_code": lambda receipts: set_postcommit(
                receipts, "error_code", "different_failure"
            ),
            "status": lambda receipts: set_postcommit(
                receipts, "status", "committed"
            ),
            "target_mutation_proof": lambda receipts: set_postcommit(
                receipts,
                "cs408_terminal_retirement_proof",
                {"status": "retired"},
            ),
            "rollback_errors": lambda receipts: set_postcommit(
                receipts, "rollback_errors", ["restore_failed"]
            ),
            "math_restore_proof": lambda receipts: receipts[
                postcommit_sha256
            ]["previous_canary_state_restore_proofs"]["math"].__setitem__(
                "proof_sha256", "0" * 64
            ),
            "cs408_restore_proof": lambda receipts: receipts[
                postcommit_sha256
            ]["previous_canary_state_restore_proofs"]["cs408"].__setitem__(
                "restored_sha256", "0" * 64
            ),
            "english_restore_proof": lambda receipts: receipts[
                postcommit_sha256
            ]["previous_canary_state_restore_proofs"]["english"].__setitem__(
                "path", "/wrong/english.json"
            ),
            "dashboard_restore_proof": lambda receipts: receipts[
                postcommit_sha256
            ]["dashboard_projection_restore_proof"].__setitem__(
                "proof_sha256", "0" * 64
            ),
        }
        for name, mutate in cases.items():
            with self.subTest(name=name):
                receipts = self.exact_0adf_early_rollback_receipts()
                mutate(receipts)
                with (
                    mock.patch.object(
                        release,
                        "_deployment_receipt_objects",
                        return_value=receipts,
                    ),
                    self.assertRaisesRegex(
                        release.ReleaseError,
                        "deployment_transaction_unresolved",
                    ),
                ):
                    release._assert_no_unresolved_deployment_transaction(
                        self.release_base
                    )

    def exact_historical_cs408_rolled_back_receipts(
        self, transaction: str
    ) -> dict[str, dict[str, Any]]:
        """Project the two audited CA receipt chains into pure memory.

        The production receipt loader verifies the complete canonical bytes
        before returning these objects.  These fixtures retain every field
        that is security-relevant to closing the two historical prepares,
        while deliberately omitting unrelated timestamps and topology noise.
        """

        previous_release = (
            "693450d5e1ccd0814b1a4b0998ed59ec280ad9e6efb7279c5639ef5b6ec6cc65"
        )
        original_preclaim = (
            "33548b0f2d6a8a2c73fc24b1dc5b2828241aa6baa1ccc29cf758864eed07525a"
        )
        preserved_queue = (
            "b3242a0df80925e4470ea23bfe8100b465c44be56b10e04171ee36c2aa32f8b1"
        )
        authorization = {
            **release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR,
            "authorization_descriptor_sha256": (
                release._authorization_descriptor_sha256(
                    release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR
                )
            ),
        }
        restore_specs = {
            "math": {
                "state_sha256": (
                    "9155a69873676c57e991fdc83961113e4706c03c85ffc61a7eda1a50d03909fb"
                ),
                "proof_sha256": (
                    "f1e3f804ed1779f284283ee21f3fb6e85ecc27bee22b3f1e685ba268a9ffd477"
                ),
            },
            "cs408": {
                "state_sha256": (
                    "897aecae0acaee47f29936105303aefe67bb116268ada641f5468cba9e5f2ab0"
                ),
                "proof_sha256": (
                    "bf7496a48a08e7e4ff9edf8b84d1d9a3678069f8dc78fcc049a4f55b6ca884b0"
                ),
            },
            "english": {
                "state_sha256": (
                    "e35c3919bf40fdf72691cd041c0b0c6e3b47574ee50773e738eef5cda92abe12"
                ),
                "proof_sha256": (
                    "03aa7edf22bffad1a018145522b880e78d8f8c3ebd83241bccf04fa02e7ca9ae"
                ),
            },
        }

        def restore_proof(subject: str) -> dict[str, Any]:
            spec = restore_specs[subject]
            return {
                "path": (
                    "/Users/xiazhibin/.codex/study-intake-preprocessor/"
                    f"dispatch/state/production-canary/{subject}.json"
                ),
                "snapshot_state": "present",
                "snapshot_sha256": spec["state_sha256"],
                "restored_state": "present",
                "restored_sha256": spec["state_sha256"],
                "proof_sha256": spec["proof_sha256"],
            }

        accounting = {
            "schema_version": (
                "study-intake-cs408-terminal-retirement-mcp-accounting-v1"
            ),
            "locked_preflight_authority_snapshot_mcp_tool_call_count": 3,
            "retirement_authority_snapshot_mcp_tool_call_count": 2,
            "english_recovery_authority_snapshot_mcp_tool_call_count": 2,
            "arm_preflight_authority_snapshot_mcp_tool_call_count": 0,
            "apply_authority_snapshot_mcp_tool_call_count": 7,
            "model_mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
        }
        if transaction == "82862":
            prepare_sha256 = (
                "82862fe15d6c80f676292284f1248b2b93ae530ed467273633850a4ce297b801"
            )
            failed_sha256 = None
            postcommit_sha256 = (
                "c5875363f3772f7bd2cf9555e44312ab6f571e6752be66adc970db1bf6909e4e"
            )
            target_release = (
                "0b05b240bbb8985298fbebe1e7352291ff4f2a2f00f7742ea4a5bcb44398a3cf"
            )
            error_code = (
                "english_subject_batch_recovery_staged_activation_failed"
            )
            recovery_receipt = (
                "c7db502e9d4435b939b2565015a4cb6ebb69584ee27789d2011711e4840dc18c"
            )
            recovery_token = (
                "dd2491b059666596940fd33d6706f17aa9b9de6a8fe6ccb9aab54e723df2e4bd"
            )
            recovery_writer = (
                "ba44c9989af370a8fc332be43c5d2158a2231d025b59ace3a26b43ab773c3f0c"
            )
            english_rollback_receipt = (
                "1e76d4f243c11c32f0ad7ba423498cdfe7021915eb07eb9c74983816918bfe0f"
            )
            english_canary_restored = (
                "da2b0fed7b98e6d13510a27cd7f7e48dd72ecfa238ceaa27c013ec5f65ea41d6"
            )
            removed_pointer_count = 0
            retirement_receipt = (
                "1c7340e1d5a49c3c3d4df2fe000c2db9254ff2aa90a4a12a4cd636f3e6ffca19"
            )
            retirement_token = (
                "089977e3e8308c91b13337d450e6a2acad45cc9df013062c99adfd1f2130ee50"
            )
            retirement_authority_snapshot = (
                "d8934b2279d99a0a5a5d1013a1283324fd0fdfe07932b0936b20ba533906dabc"
            )
            writer_postimage = (
                "c6bfa586c9914c8527febc6979583e596fa3e7053c18fef582a3df71f99072aa"
            )
            cs408_rollback_receipt = (
                "0f6220838f6202cd14dfe4f2c6f4c006c54fa02393947d76b2e591ebb5a10967"
            )
            cs408_rollback_postimage = (
                "daece59663d26b32350ced64da485dbf95c53b676ac6262ab9cb304c2ca34a6a"
            )
        elif transaction == "b4d2":
            prepare_sha256 = (
                "b4d2cd209f5b75a70de8a3bd847041faa77c50ab9cffc05ec64c7f7e5aee28ed"
            )
            failed_sha256 = (
                "04e861af34531d6bf3802dda0a696cf6b629be6dcec861bef9c0e4d1ffae5e1f"
            )
            postcommit_sha256 = (
                "2a4074e5bc167f72be82d11e77d545e22db51b5ef05ae3b5144446044d2dfeb2"
            )
            target_release = (
                "50f9570c8e1383d1e5e3392cd57f273fb7ce464c4eed4a248cf89ee897bd6118"
            )
            error_code = "three_subject_canary_preflight_invalid"
            recovery_receipt = (
                "989f01b738a416e572fe5104a3f3de59d3baca854377cb59531d346f95a5e746"
            )
            recovery_token = (
                "995fccfc015a71ceba1ed6cced11bf554aa0bf8ea317e3ce28d6f82a6681c724"
            )
            recovery_writer = (
                "d9ac4dacc4c79d20dc31902ce8f235224408914103f1f20dd31d5e22865ced9c"
            )
            english_rollback_receipt = (
                "eb9958cd3dc53e69a4301c2aa3516f4623c4e4d16c24ef3e4a4610351bf720ee"
            )
            english_canary_restored = (
                "233699fca1719d02d2417dab0fb46e12679fe70ad13e492189336569f6062e04"
            )
            removed_pointer_count = 4
            retirement_receipt = (
                "8812f9d6a42386e0a16f8d10d93a4dae922ecb8a57a7580c7562c56a97e23194"
            )
            retirement_token = (
                "f62d6748a52a3ea4be35688a79e051901d4c4ca2999311acf96aa93f7350bc5d"
            )
            retirement_authority_snapshot = (
                "00b36db668c4d5ec97c72a2df0fdd9b530b1fd7af9223a72118ac505343fcd99"
            )
            writer_postimage = (
                "18cb763c588e2cc4ec2a0afb53fd475afbac8ccbc6f08eb63defe4053da0d023"
            )
            cs408_rollback_receipt = (
                "2f4aa04e6db87fcfbe91db810d0075556060924c9db9f9981c43c45998474c78"
            )
            cs408_rollback_postimage = (
                "85d5838e6f69e264201ede864c34ad2ee2f6349050b711b90f501a3e2d5836c8"
            )
        else:
            raise AssertionError(f"unknown transaction fixture: {transaction}")
        closure_descriptor = (
            release.EXACT_20260813_0B05_ROLLED_BACK_RECEIPT_CLOSURE
            if transaction == "82862"
            else release.EXACT_20260813_50F_ROLLED_BACK_RECEIPT_CLOSURE
        )

        prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "operation": "activate",
            "release_id": target_release,
            "active_link": (
                "/Users/xiazhibin/.codex/study-intake-preprocessor/current"
            ),
            "expected_current": previous_release,
            "observed_current": previous_release,
            "formal_write_count": 0,
            "cs408_terminal_retirement_authorization_descriptor": (
                authorization
            ),
            "subject_recovery_expectations": {
                "english": original_preclaim
            },
        }
        recovery = {
            "schema_version": (
                "study-intake-subject-batch-recovery-result-v1"
            ),
            "subject": "english",
            "status": "recovered",
            "release_id": target_release,
            "source_generation": "english-38139df7439372a24df8",
            "next_generation": "english-317672cabdaba8030c2b",
            "next_authority_fingerprint": (
                "317672cabdaba8030c2b2b36accaf5da35ee2d96efbdc0db48137d83d7b44f13"
            ),
            "original_preclaim_failure_receipt_sha256": original_preclaim,
            "preserved_queue_entry_sha256": preserved_queue,
            "recovery_receipt_sha256": recovery_receipt,
            "recovery_receipt_path": closure_descriptor[
                "english_recovery_receipt_path"
            ],
            "rollback_token": recovery_token,
            "writer_state_after_sha256": recovery_writer,
            "subsequent_attempt_receipt_sha256s": [
                "af2e76dc85b18860caa503a2d021c12698d0871db29c29c82614b7955e0ed39f",
                "ec3bc17f2f2c4c64f876a9b4d44818e6a0c834e5e75733415263854edbb59be0",
            ],
            "preserved_task": {
                "frozen_payload_sha256": (
                    "d0a001606ea84eb72993d84dc01f131f31afb722033cca22226c560c426be0d1"
                ),
                "producer_input_contract_sha256": (
                    "d933ea5f77bcc134e22c25a78ccead1813c697384b5476007c90957b4a8c1e7b"
                ),
                "source_event_set_sha256": (
                    "c28d440eb4fb0b5f981f7121800b9a1001e4f130a9c4b2741d32ce94f2539305"
                ),
                "task_object_path": (
                    "/Users/xiazhibin/.codex/study-intake-preprocessor/"
                    "dispatch/production-canary/tasks/english/sha256/9d/"
                    "9d8f64ca6e9d783db92bcafa445f5105d18a9ae8c9f95d5810a9496af5761183.json"
                ),
                "task_object_sha256": (
                    "9d8f64ca6e9d783db92bcafa445f5105d18a9ae8c9f95d5810a9496af5761183"
                ),
                "unit_sha256": (
                    "c65e25cb0453ca6b96b50c9c7a1ae5952698d03c1505c84228320c6f5804f2ef"
                ),
            },
            "luna_consumer_enabled": False,
            "authority_snapshot_count": 2,
            "authority_snapshot_mcp_tool_call_count": 2,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 2,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        recovery_rollback = {
            "schema_version": (
                "study-intake-subject-batch-recovery-rollback-result-v1"
            ),
            "subject": "english",
            "status": "rolled_back",
            "recovery_receipt_sha256": recovery_receipt,
            "rollback_receipt_sha256": english_rollback_receipt,
            "rollback_receipt_path": closure_descriptor[
                "english_rollback_receipt_path"
            ],
            "rollback_token": recovery_token,
            "writer_state_restored_sha256": (
                "73fe22e9d089be8cc4c7073209667379f9f238d02ed47865313fad192511db57"
            ),
            "batch_pointer_restored_sha256": (
                "cc36d6b44ba4e7eabe2ae5780afd36c914adeb24f41b422d7a2aaa7ff52907de"
            ),
            "canary_state_restored_sha256": english_canary_restored,
            "preserved_queue_entry_sha256": preserved_queue,
            "target_queue_withdrawn": True,
            "removed_mutable_pointer_count": removed_pointer_count,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        descriptor = release.CS408_TERMINAL_RETIREMENT_DESCRIPTOR
        retirement = {
            "schema_version": (
                "study-intake-cs408-terminal-batch-retirement-result-v1"
            ),
            "subject": "cs408",
            "status": "retired",
            "mode": descriptor["mode"],
            "batch_id": descriptor["batch_id"],
            "batch_sha256": descriptor["batch_sha256"],
            "terminal_receipt_sha256": descriptor[
                "terminal_receipt_sha256"
            ],
            "writer_preimage_sha256": descriptor[
                "writer_preimage_sha256"
            ],
            "batch_pointer_sha256": descriptor["batch_pointer_sha256"],
            "snapshot_sha256": descriptor["snapshot_sha256"],
            "source_generation": descriptor["source_generation"],
            "source_authority_fingerprint": descriptor[
                "source_authority_fingerprint"
            ],
            "next_generation": descriptor["target_generation"],
            "next_authority_fingerprint": descriptor[
                "target_authority_fingerprint"
            ],
            "authorization_descriptor_sha256": authorization[
                "authorization_descriptor_sha256"
            ],
            "retirement_receipt_sha256": retirement_receipt,
            "retirement_receipt_path": closure_descriptor[
                "cs408_retirement_receipt_path"
            ],
            "rollback_token": retirement_token,
            "writer_postimage_sha256": writer_postimage,
            "postimage_verification_sha256": (
                "1180b642c6da284a40249cc8333fef5a01537d7d3568ea215f0021b1d56bf9d5"
                if transaction == "82862"
                else "ffd01e1f65d4c2d2fe4d9f0054423c1bc047db39d59f79aa7404c7f80ba8537f"
            ),
            "authority_snapshot_count": 2,
            "authority_snapshot_sha256s": [
                retirement_authority_snapshot,
                retirement_authority_snapshot,
            ],
            "authority_snapshot_mcp_tool_call_count": 2,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 2,
            "model_call_count": 0,
            "provider_request_count": 0,
            "capture_replay_count": 0,
            "replacement_task_created_count": 0,
            "queue_entry_created_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        retirement_rollback = {
            "schema_version": (
                "study-intake-cs408-terminal-batch-retirement-rollback-result-v1"
            ),
            "subject": "cs408",
            "status": "rolled_back",
            "retirement_receipt_sha256": retirement_receipt,
            "rollback_receipt_sha256": cs408_rollback_receipt,
            "rollback_receipt_path": closure_descriptor[
                "cs408_rollback_receipt_path"
            ],
            "rollback_token": retirement_token,
            "writer_state_restored_sha256": descriptor[
                "writer_preimage_sha256"
            ],
            "old_batch_sha256": descriptor["batch_sha256"],
            "batch_pointer_unchanged_sha256": descriptor[
                "batch_pointer_sha256"
            ],
            "batch_snapshot_unchanged_sha256": descriptor[
                "snapshot_sha256"
            ],
            "terminal_receipt_unchanged_sha256": descriptor[
                "terminal_receipt_sha256"
            ],
            "retirement_pointer_withdrawn": True,
            "retirement_intent_withdrawn": True,
            "rollback_postimage_verification_sha256": (
                cs408_rollback_postimage
            ),
            "rollback_receipt_reopened": True,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        postcommit = {
            "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
            "operation": "activate",
            "release_id": target_release,
            "prepare_receipt_sha256": prepare_sha256,
            "status": "rolled_back",
            "error_code": error_code,
            "formal_write_count": 0,
            "rollback_errors": [],
            "cs408_terminal_retirement_mcp_accounting": accounting,
            "subject_batch_recovery_proof": recovery,
            "subject_batch_recovery_rollback_proof": recovery_rollback,
            "cs408_terminal_retirement_proof": retirement,
            "cs408_terminal_retirement_rollback_proof": (
                retirement_rollback
            ),
            "previous_canary_state_restore_proofs": {
                subject: restore_proof(subject)
                for subject in release.RESUMABLE_SUBJECTS
            },
        }
        if transaction == "82862":
            postcommit["restored_release_id"] = previous_release
            postcommit["previous_canary_deactivation_proof"] = {
                "schema_version": (
                    "study-intake-previous-canary-deactivation-v1"
                ),
                "status": "previous_canary_deactivated",
                "previous_release_id": previous_release,
                "subjects": [
                    {
                        "subject": subject,
                        "state": "inactive_rolled_back",
                    }
                    for subject in release.RESUMABLE_SUBJECTS
                ],
                "formal_write_count": 0,
            }
            postcommit["external_profile_apply_proof"] = {
                "schema_version": (
                    "study-intake-external-profile-apply-proof-v1"
                ),
                "status": "applied",
                "proof_sha256": (
                    "215c65fdb44bbf505862d8d806a231c38771b7f998e668089bb9a36f7493880b"
                ),
            }
            dashboard_spec = (
                release.EXACT_20260813_0ADF_EARLY_ROLLBACK_CLOSURE[
                    "dashboard_restore_proof"
                ]
            )
            postcommit["dashboard_projection_restore_proof"] = {
                "path": dashboard_spec["path"],
                "snapshot_state": "present",
                "snapshot_sha256": dashboard_spec["state_sha256"],
                "restored_state": "present",
                "restored_sha256": dashboard_spec["state_sha256"],
                "proof_sha256": dashboard_spec["proof_sha256"],
            }
            postcommit[
                "dashboard_projection_restore_proof_sha256"
            ] = dashboard_spec["proof_sha256"]
        else:
            postcommit.update(
                {
                    "active_link": (
                        "/Users/xiazhibin/.codex/"
                        "study-intake-preprocessor/current"
                    ),
                    "drain_error_code": (
                        "canary_activation_failure_gate_invalid:math"
                    ),
                    "previous_release_id": previous_release,
                    "target_fenced": False,
                    "old_services_restarted": True,
                    "restored_paused_services": [
                        "cs408",
                        "english",
                        "math",
                    ],
                }
            )
        postcommit["external_profile_restore_proof"] = {
            "schema_version": (
                "study-intake-external-profile-restore-proof-v1"
            ),
            "status": "restored",
            "proof_sha256": (
                "75817ec9707cbdad2059503ea40f735fb832c67411720c2ef2b4dbb1f4603629"
            ),
        }

        receipts = {
            prepare_sha256: prepare,
            postcommit_sha256: postcommit,
        }
        if failed_sha256 is not None:
            receipts[failed_sha256] = {
                "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
                "operation": "activate",
                "release_id": target_release,
                "prepare_receipt_sha256": prepare_sha256,
                "status": "rollback_incomplete_cancel_or_fence_failed",
                "error_code": "three_subject_canary_preflight_invalid",
                "drain_error_code": (
                    "canary_activation_failure_gate_invalid:math"
                ),
                "formal_write_count": 0,
                "cs408_terminal_retirement_mcp_accounting": accounting,
                "subject_batch_recovery_proof": None,
                "subject_batch_recovery_rollback_proof": None,
                "cs408_terminal_retirement_proof": retirement,
                "cs408_terminal_retirement_rollback_proof": None,
            }
        return receipts

    def assert_historical_receipts_close_without_runtime_reads(
        self, receipts: dict[str, dict[str, Any]]
    ) -> None:
        forbidden = AssertionError(
            "historical receipt-only closure consulted mutable runtime state"
        )
        for future_live in ("future-live-a", "future-live-b"):
            future_base = self.base / future_live
            with (
                mock.patch.object(
                    release,
                    "_deployment_receipt_objects",
                    return_value=copy.deepcopy(receipts),
                ),
                mock.patch.object(
                    release,
                    "_cs408_retirement_postcommit_closes_prepare",
                    side_effect=forbidden,
                ),
                mock.patch.object(
                    release,
                    "_reopen_subject_batch_recovery_rollback",
                    side_effect=forbidden,
                ),
                mock.patch.object(
                    release,
                    "_reopen_cs408_terminal_batch_retirement_rollback",
                    side_effect=forbidden,
                ),
                mock.patch.object(
                    release, "_current_release", side_effect=forbidden
                ),
                mock.patch.object(
                    release, "verify_release", side_effect=forbidden
                ),
                mock.patch.object(
                    Path, "read_bytes", side_effect=forbidden
                ),
                mock.patch.object(
                    Path, "read_text", side_effect=forbidden
                ),
                mock.patch.object(Path, "lstat", side_effect=forbidden),
                mock.patch.object(Path, "exists", side_effect=forbidden),
                mock.patch.object(Path, "is_file", side_effect=forbidden),
                mock.patch.object(Path, "resolve", side_effect=forbidden),
            ):
                release._assert_no_unresolved_deployment_transaction(
                    future_base
                )

    def assert_historical_receipts_rejected(
        self, receipts: dict[str, dict[str, Any]]
    ) -> None:
        forbidden = AssertionError(
            "invalid historical receipt chain consulted mutable runtime state"
        )
        with (
            mock.patch.object(
                release,
                "_deployment_receipt_objects",
                return_value=receipts,
            ),
            mock.patch.object(
                release,
                "_cs408_retirement_postcommit_closes_prepare",
                side_effect=forbidden,
            ),
            mock.patch.object(
                release,
                "_reopen_subject_batch_recovery_rollback",
                side_effect=forbidden,
            ),
            mock.patch.object(
                release,
                "_reopen_cs408_terminal_batch_retirement_rollback",
                side_effect=forbidden,
            ),
            mock.patch.object(
                release, "_current_release", side_effect=forbidden
            ),
            mock.patch.object(
                release, "verify_release", side_effect=forbidden
            ),
            mock.patch.object(Path, "read_bytes", side_effect=forbidden),
            mock.patch.object(Path, "read_text", side_effect=forbidden),
            mock.patch.object(Path, "lstat", side_effect=forbidden),
            mock.patch.object(Path, "exists", side_effect=forbidden),
            mock.patch.object(Path, "is_file", side_effect=forbidden),
            mock.patch.object(Path, "resolve", side_effect=forbidden),
            self.assertRaisesRegex(
                release.ReleaseError, "deployment_transaction_unresolved"
            ),
        ):
            release._assert_no_unresolved_deployment_transaction(
                self.base / "arbitrary-new-live"
            )

    def test_exact_82862_c587_rolled_back_pair_closes_receipt_only(
        self,
    ) -> None:
        self.assert_historical_receipts_close_without_runtime_reads(
            self.exact_historical_cs408_rolled_back_receipts("82862")
        )

    def test_exact_b4d2_requires_2a407_after_incomplete_04e(
        self,
    ) -> None:
        receipts = self.exact_historical_cs408_rolled_back_receipts(
            "b4d2"
        )
        recovery_sha256 = (
            "2a4074e5bc167f72be82d11e77d545e22db51b5ef05ae3b5144446044d2dfeb2"
        )
        incomplete_only = copy.deepcopy(receipts)
        incomplete_only.pop(recovery_sha256)
        self.assert_historical_receipts_rejected(incomplete_only)
        self.assert_historical_receipts_close_without_runtime_reads(receipts)
        incomplete_sha256 = (
            "04e861af34531d6bf3802dda0a696cf6b629be6dcec861bef9c0e4d1ffae5e1f"
        )
        wrong_key = copy.deepcopy(receipts)
        wrong_key["e" * 64] = wrong_key.pop(incomplete_sha256)
        self.assert_historical_receipts_rejected(wrong_key)
        for field, value in (
            ("status", "rolled_back"),
            ("error_code", "different_failure"),
        ):
            with self.subTest(incomplete_receipt_drift=field):
                drifted = copy.deepcopy(receipts)
                drifted[incomplete_sha256][field] = value
                self.assert_historical_receipts_rejected(drifted)

    def test_exact_historical_rolled_back_pairs_reject_security_drift(
        self,
    ) -> None:
        postcommit_sha256s = {
            "82862": (
                "c5875363f3772f7bd2cf9555e44312ab6f571e6752be66adc970db1bf6909e4e"
            ),
            "b4d2": (
                "2a4074e5bc167f72be82d11e77d545e22db51b5ef05ae3b5144446044d2dfeb2"
            ),
        }
        prepare_sha256s = {
            "82862": (
                "82862fe15d6c80f676292284f1248b2b93ae530ed467273633850a4ce297b801"
            ),
            "b4d2": (
                "b4d2cd209f5b75a70de8a3bd847041faa77c50ab9cffc05ec64c7f7e5aee28ed"
            ),
        }

        def mutate_post(
            receipts: dict[str, dict[str, Any]],
            transaction: str,
            path: tuple[str, ...],
            value: Any,
        ) -> None:
            target: dict[str, Any] = receipts[
                postcommit_sha256s[transaction]
            ]
            for key in path[:-1]:
                target = target[key]
            target[path[-1]] = value

        def move_receipt_key(
            receipts: dict[str, dict[str, Any]], old_key: str
        ) -> None:
            receipts["f" * 64] = receipts.pop(old_key)

        cases: dict[
            str,
            Callable[[dict[str, dict[str, Any]], str], None],
        ] = {
            "wrong_prepare_receipt_key": lambda receipts, tx: (
                move_receipt_key(receipts, prepare_sha256s[tx])
            ),
            "wrong_postcommit_receipt_key": lambda receipts, tx: (
                move_receipt_key(receipts, postcommit_sha256s[tx])
            ),
            "status": lambda receipts, tx: mutate_post(
                receipts, tx, ("status",), "committed"
            ),
            "error_code": lambda receipts, tx: mutate_post(
                receipts, tx, ("error_code",), "different_failure"
            ),
            "authorization": lambda receipts, tx: receipts[
                prepare_sha256s[tx]
            ]["cs408_terminal_retirement_authorization_descriptor"].update(
                {"authorization_descriptor_sha256": "0" * 64}
            ),
            "prepare_33548_binding": lambda receipts, tx: receipts[
                prepare_sha256s[tx]
            ]["subject_recovery_expectations"].update(
                {"english": "0" * 64}
            ),
            "recovery_33548_binding": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "subject_batch_recovery_proof",
                    "original_preclaim_failure_receipt_sha256",
                ),
                "0" * 64,
            ),
            "recovery_b324_binding": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "subject_batch_recovery_proof",
                    "preserved_queue_entry_sha256",
                ),
                "0" * 64,
            ),
            "rollback_b324_binding": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "subject_batch_recovery_rollback_proof",
                    "preserved_queue_entry_sha256",
                ),
                "0" * 64,
            ),
            "rollback_errors": lambda receipts, tx: mutate_post(
                receipts, tx, ("rollback_errors",), ["restore_failed"]
            ),
            "accounting_model_call": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "cs408_terminal_retirement_mcp_accounting",
                    "model_call_count",
                ),
                1,
            ),
            "accounting_provider_call": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "cs408_terminal_retirement_mcp_accounting",
                    "provider_request_count",
                ),
                1,
            ),
            "accounting_authority_call": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "cs408_terminal_retirement_mcp_accounting",
                    "apply_authority_snapshot_mcp_tool_call_count",
                ),
                8,
            ),
            "english_rollback_receipt_binding": lambda receipts, tx: (
                mutate_post(
                    receipts,
                    tx,
                    (
                        "subject_batch_recovery_rollback_proof",
                        "recovery_receipt_sha256",
                    ),
                    "0" * 64,
                )
            ),
            "english_rollback_token_binding": lambda receipts, tx: (
                mutate_post(
                    receipts,
                    tx,
                    (
                        "subject_batch_recovery_rollback_proof",
                        "rollback_token",
                    ),
                    "0" * 64,
                )
            ),
            "english_queue_not_withdrawn": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "subject_batch_recovery_rollback_proof",
                    "target_queue_withdrawn",
                ),
                False,
            ),
            "english_rollback_model_call": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "subject_batch_recovery_rollback_proof",
                    "model_call_count",
                ),
                1,
            ),
            "english_rollback_mcp_call": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "subject_batch_recovery_rollback_proof",
                    "mcp_tool_call_count",
                ),
                1,
            ),
            "english_rollback_writer_binding": lambda receipts, tx: (
                mutate_post(
                    receipts,
                    tx,
                    (
                        "subject_batch_recovery_rollback_proof",
                        "writer_state_restored_sha256",
                    ),
                    "0" * 64,
                )
            ),
            "cs408_rollback_receipt_binding": lambda receipts, tx: (
                mutate_post(
                    receipts,
                    tx,
                    (
                        "cs408_terminal_retirement_rollback_proof",
                        "retirement_receipt_sha256",
                    ),
                    "0" * 64,
                )
            ),
            "cs408_rollback_writer_binding": lambda receipts, tx: (
                mutate_post(
                    receipts,
                    tx,
                    (
                        "cs408_terminal_retirement_rollback_proof",
                        "writer_state_restored_sha256",
                    ),
                    "0" * 64,
                )
            ),
            "cs408_rollback_pointer_binding": lambda receipts, tx: (
                mutate_post(
                    receipts,
                    tx,
                    (
                        "cs408_terminal_retirement_rollback_proof",
                        "batch_pointer_unchanged_sha256",
                    ),
                    "0" * 64,
                )
            ),
            "cs408_rollback_terminal_binding": lambda receipts, tx: (
                mutate_post(
                    receipts,
                    tx,
                    (
                        "cs408_terminal_retirement_rollback_proof",
                        "terminal_receipt_unchanged_sha256",
                    ),
                    "0" * 64,
                )
            ),
            "cs408_rollback_mcp_call": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "cs408_terminal_retirement_rollback_proof",
                    "mcp_tool_call_count",
                ),
                1,
            ),
            "math_restore_proof": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "previous_canary_state_restore_proofs",
                    "math",
                    "restored_sha256",
                ),
                "0" * 64,
            ),
            "cs408_restore_proof": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "previous_canary_state_restore_proofs",
                    "cs408",
                    "restored_sha256",
                ),
                "0" * 64,
            ),
            "english_restore_proof": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "previous_canary_state_restore_proofs",
                    "english",
                    "restored_sha256",
                ),
                "0" * 64,
            ),
            "recovery_model_call": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                ("subject_batch_recovery_proof", "model_call_count"),
                1,
            ),
            "recovery_provider_call": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "subject_batch_recovery_proof",
                    "provider_request_count",
                ),
                1,
            ),
            "cs408_retirement_model_call": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                ("cs408_terminal_retirement_proof", "model_call_count"),
                1,
            ),
            "cs408_retirement_provider_call": lambda receipts, tx: (
                mutate_post(
                    receipts,
                    tx,
                    (
                        "cs408_terminal_retirement_proof",
                        "provider_request_count",
                    ),
                    1,
                )
            ),
            "cs408_capture_replay": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                ("cs408_terminal_retirement_proof", "capture_replay_count"),
                1,
            ),
            "cs408_replacement_task": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "cs408_terminal_retirement_proof",
                    "replacement_task_created_count",
                ),
                1,
            ),
            "cs408_queue_created": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                (
                    "cs408_terminal_retirement_proof",
                    "queue_entry_created_count",
                ),
                1,
            ),
            "formal_write": lambda receipts, tx: mutate_post(
                receipts, tx, ("formal_write_count",), 1
            ),
            "nested_formal_write": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                ("cs408_terminal_retirement_proof", "formal_write_count"),
                1,
            ),
            "sol_enabled": lambda receipts, tx: mutate_post(
                receipts,
                tx,
                ("subject_batch_recovery_rollback_proof", "sol_enabled"),
                True,
            ),
        }
        for transaction in ("82862", "b4d2"):
            for name, mutate in cases.items():
                with self.subTest(transaction=transaction, drift=name):
                    receipts = (
                        self.exact_historical_cs408_rolled_back_receipts(
                            transaction
                        )
                    )
                    mutate(receipts, transaction)
                    self.assert_historical_receipts_rejected(receipts)

    def test_unresolved_deployment_prepare_fails_closed_without_writes(self) -> None:
        built = self.build(passed=True)
        active = self.base / "unresolved-current"
        before = release._preview_tree_snapshot(self.release_base)
        prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "operation": "activate",
            "release_id": str(built["release_id"]),
            "observed_current": None,
            "formal_write_count": 0,
        }
        prepare_sha256 = release._write_deployment_receipt(
            self.release_base, prepare
        )
        lock_path = self.release_base / "deployments" / "activation.lock"
        lock_path.touch(mode=0o600)
        expected = release._preview_tree_snapshot(self.release_base)
        runner = self.FakeRunner()
        with self.assertRaisesRegex(
            release.ReleaseError, "deployment_transaction_unresolved"
        ):
            release.activate_release(
                release_base=self.release_base,
                release_id=str(built["release_id"]),
                active_link=active,
                apply=True,
                operation="activate",
                expected_current="absent",
                launchagent_dir=self.launchagents,
                command_runner=runner,
            )
        self.assertTrue(
            (self.release_base / "deployments" / f"{prepare_sha256}.json").is_file()
        )
        self.assertEqual(runner.commands, [])
        self.assertFalse(active.exists() or active.is_symlink())
        self.assertEqual(release._preview_tree_snapshot(self.release_base), expected)
        self.assertNotEqual(before, expected)

    def test_deployment_recovery_marker_must_be_exact_and_hmac_bound(self) -> None:
        built = self.build(passed=True)
        prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "operation": "activate",
            "release_id": str(built["release_id"]),
            "observed_current": None,
            "formal_write_count": 0,
        }
        prepare_sha256 = release._write_deployment_receipt(
            self.release_base, prepare
        )
        launchagent_inventory_path = (
            self.release_base
            / "deployments"
            / "launchagent-backups"
            / prepare_sha256
            / "inventory.json"
        )
        launchagent_inventory = {
            "com.example.study-intake.math": {
                "existed": False,
                "live_path": str(self.launchagents / "math.plist"),
            }
        }
        release.atomic_json(
            launchagent_inventory_path, launchagent_inventory
        )
        external_inventory_path = (
            self.release_base
            / "deployments"
            / "external-profile-backups"
            / prepare_sha256
            / "inventory.json"
        )
        external_inventory_core = {
            "schema_version": "study-intake-external-profile-backup-v1",
            "ordinary_profile": {},
            "mcp_profile": {},
            "morning_pointer": {},
            "plugin_cache": {},
        }
        external_inventory = {
            **external_inventory_core,
            "inventory_sha256": release.sha256_bytes(
                release.canonical_bytes(external_inventory_core)
            ),
        }
        release.atomic_json(external_inventory_path, external_inventory)
        core = {
            "schema_version": release.DEPLOYMENT_RECOVERY_MARKER_SCHEMA,
            "prepare_receipt_sha256": prepare_sha256,
            "observed_current_release_id": None,
            "launchagent_backup_inventory_sha256": release.sha256_file(
                launchagent_inventory_path
            ),
            "external_profile_backup_inventory_sha256": release.sha256_file(
                external_inventory_path
            ),
            "current_surface_sha256s": {
                "current_symlink": "3" * 64,
                "launchagent_plists": "4" * 64,
                "ordinary_profile": "5" * 64,
                "morning_pointer": "6" * 64,
                "plugin_cache": "7" * 64,
            },
            "outcome": "verified_exact_restore",
            "resolved_at": "2026-08-12T00:00:00+00:00",
            "formal_write_count": 0,
        }
        marker = release._seal_deployment_value(
            core,
            release_base=self.release_base,
            purpose=release.DEPLOYMENT_RECOVERY_MARKER_PURPOSE,
        )
        release._write_deployment_receipt(self.release_base, marker)
        release._assert_no_unresolved_deployment_transaction(self.release_base)

        launchagent_inventory_path.write_bytes(
            release.canonical_bytes({"tampered": True})
        )
        with self.assertRaisesRegex(
            release.ReleaseError, "deployment_recovery_marker_invalid"
        ):
            release._assert_no_unresolved_deployment_transaction(
                self.release_base
            )
        release.atomic_json(
            launchagent_inventory_path, launchagent_inventory
        )

        external_inventory_path.write_bytes(
            release.canonical_bytes(
                {**external_inventory, "schema_version": "tampered"}
            )
        )
        with self.assertRaisesRegex(
            release.ReleaseError, "deployment_recovery_marker_invalid"
        ):
            release._assert_no_unresolved_deployment_transaction(
                self.release_base
            )
        release.atomic_json(external_inventory_path, external_inventory)

        marker_path = next(
            path
            for path in (self.release_base / "deployments").glob("*.json")
            if json.loads(path.read_text(encoding="utf-8")).get("schema_version")
            == release.DEPLOYMENT_RECOVERY_MARKER_SCHEMA
        )
        tampered = json.loads(marker_path.read_text(encoding="utf-8"))
        marker_path.unlink()
        tampered["outcome"] = "verified_no_mutation"
        release._write_deployment_receipt(self.release_base, tampered)
        with self.assertRaisesRegex(
            release.ReleaseError, "deployment_recovery_marker_invalid"
        ):
            release._assert_no_unresolved_deployment_transaction(self.release_base)

    def test_v2_recovery_marker_closes_only_audited_superseded_chains(self) -> None:
        original_sha256_bytes = release.sha256_bytes
        for historical_prepare_sha256 in (
            release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS
        ):
            with self.subTest(historical_prepare_sha256=historical_prepare_sha256):
                receipts, _proof, proof_bytes, patched_descriptor = self.superseded_recovery_receipts(
                    historical_prepare_sha256
                )
                core = self.make_v2_recovery_core(historical_prepare_sha256)
                head = core["resolution_evidence"]["current_release_id"]
                head_config = self.release_base / "releases" / head / "config.json"
                head_config.parent.mkdir(parents=True, exist_ok=True)
                head_config.write_text(
                    json.dumps({"runtime_root": str(self.runtime_data)}),
                    encoding="utf-8",
                )
                marker = release._seal_deployment_value(
                    core,
                    release_base=self.release_base,
                    purpose=release.DEPLOYMENT_RECOVERY_MARKER_V2_PURPOSE,
                )
                receipts[f"marker-{historical_prepare_sha256}"] = marker

                def fixture_sha256(raw: bytes) -> str:
                    try:
                        candidate = json.loads(raw)
                        proof_release_id = (
                            candidate.get("release_id")
                            if isinstance(candidate, dict)
                            else None
                        )
                        proof_schema = (
                            candidate.get("schema_version")
                            if isinstance(candidate, dict)
                            else None
                        )
                    except (UnicodeError, json.JSONDecodeError):
                        proof_release_id = None
                        proof_schema = None
                    descriptor = patched_descriptor
                    if (
                        proof_schema == release.POST_ACTIVATION_SCHEMA
                        and proof_release_id == descriptor["successor_release_id"]
                    ):
                        return descriptor["successor_post_activation_proof_sha256"]
                    for continuation in descriptor.get("continuation_transactions", ()):
                        if (
                            proof_schema
                            == release.THREE_SUBJECT_CANARY_POST_ACTIVATION_SCHEMA
                            and proof_release_id == continuation["release_id"]
                        ):
                            return continuation["post_activation_proof_sha256"]
                    return original_sha256_bytes(raw)

                with (
                    mock.patch.object(
                        release,
                        "_deployment_receipt_objects",
                        return_value=receipts,
                    ),
                    mock.patch.dict(
                        release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS,
                        {historical_prepare_sha256: patched_descriptor},
                    ),
                    mock.patch.object(
                        release,
                        "_verify_historical_manager_script",
                    ),
                    mock.patch.object(
                        release,
                        "_verify_historical_executor_release",
                    ),
                    mock.patch.object(
                        release,
                        "_verify_recovery_v2_dispatch_authority",
                    ),
                    mock.patch.object(
                        release,
                        "_recovery_v2_public_canary_gate",
                        side_effect=self.fixture_v2_public_gate(core),
                    ),
                    mock.patch.object(
                        release, "sha256_bytes", side_effect=fixture_sha256
                    ),
                ):
                    release._assert_no_unresolved_deployment_transaction(
                        self.release_base
                    )

    def test_v2_recovery_marker_rejects_unallowlisted_or_broken_chain(self) -> None:
        with self.assertRaisesRegex(
            release.ReleaseError, "deployment_recovery_marker_v2_invalid"
        ):
            self.make_v2_recovery_core("0" * 64)

        historical_prepare_sha256 = next(
            iter(release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS)
        )
        receipts, _proof, proof_bytes, patched_descriptor = self.superseded_recovery_receipts(
            historical_prepare_sha256
        )
        core = self.make_v2_recovery_core(historical_prepare_sha256)
        marker = release._seal_deployment_value(
            core,
            release_base=self.release_base,
            purpose=release.DEPLOYMENT_RECOVERY_MARKER_V2_PURPOSE,
        )
        marker["historical_manager_mutation_scope"] = "all_deployments"
        marker = release._seal_deployment_value(
            marker,
            release_base=self.release_base,
            purpose=release.DEPLOYMENT_RECOVERY_MARKER_V2_PURPOSE,
        )
        receipts["marker"] = marker
        original_sha256_bytes = release.sha256_bytes

        def fixture_sha256(raw: bytes) -> str:
            try:
                proof_release_id = json.loads(raw).get("release_id")
            except (UnicodeError, json.JSONDecodeError):
                proof_release_id = None
            descriptor = release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS[
                historical_prepare_sha256
            ]
            if proof_release_id == descriptor["successor_release_id"]:
                return descriptor["successor_post_activation_proof_sha256"]
            return original_sha256_bytes(raw)

        with (
            mock.patch.object(
                release, "_deployment_receipt_objects", return_value=receipts
            ),
            mock.patch.dict(
                release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS,
                {historical_prepare_sha256: patched_descriptor},
            ),
            mock.patch.object(release, "_verify_historical_manager_script"),
            mock.patch.object(release, "_verify_historical_executor_release"),
            mock.patch.object(
                release, "_verify_recovery_v2_dispatch_authority"
            ),
            mock.patch.object(
                release,
                "_recovery_v2_public_canary_gate",
                side_effect=self.fixture_v2_public_gate(core),
            ),
            mock.patch.object(release, "sha256_bytes", side_effect=fixture_sha256),
            self.assertRaisesRegex(
                release.ReleaseError, "deployment_recovery_marker_v2_invalid"
            ),
        ):
            release._assert_no_unresolved_deployment_transaction(self.release_base)

    def test_v2_recovery_marker_checks_historical_manager_file_identity(self) -> None:
        release_id = "d" * 64
        manager = (
            self.release_base
            / "releases"
            / release_id
            / "scripts"
            / "release_manager.py"
        )
        manager.parent.mkdir(parents=True)
        manager.write_text("manager\n", encoding="utf-8")
        expected_sha256 = release.sha256_file(manager)
        release._verify_historical_manager_script(
            release_base=self.release_base,
            release_id=release_id,
            expected_sha256=expected_sha256,
        )
        with self.assertRaisesRegex(
            release.ReleaseError, "deployment_recovery_marker_v2_invalid"
        ):
            release._verify_historical_manager_script(
                release_base=self.release_base,
                release_id=release_id,
                expected_sha256="0" * 64,
            )
        manager.unlink()
        manager.symlink_to(self.source / "bin" / "worker.py")
        with self.assertRaisesRegex(
            release.ReleaseError, "deployment_recovery_marker_v2_invalid"
        ):
            release._verify_historical_manager_script(
                release_base=self.release_base,
                release_id=release_id,
                expected_sha256=expected_sha256,
            )

    def test_v2_recovery_marker_rejects_bad_authority_and_missing_incomplete(self) -> None:
        historical_prepare_sha256 = (
            "e462742448a3c538df114ed9b9aeb037cdd2bcff1aa29be6bb3cadbbf0470b55"
        )
        receipts, _proof, proof_bytes, patched_descriptor = self.superseded_recovery_receipts(
            historical_prepare_sha256
        )
        core = self.make_v2_recovery_core(historical_prepare_sha256)
        marker = release._seal_deployment_value(
            core,
            release_base=self.release_base,
            purpose=release.DEPLOYMENT_RECOVERY_MARKER_V2_PURPOSE,
        )
        original_sha256_bytes = release.sha256_bytes

        def fixture_sha256(raw: bytes) -> str:
            if raw == proof_bytes:
                return release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS[
                    historical_prepare_sha256
                ]["successor_post_activation_proof_sha256"]
            return original_sha256_bytes(raw)

        cases = (
            ("bad_authority", {**marker, "authority": {}}),
            ("missing_incomplete", marker),
        )
        incomplete = release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS[
            historical_prepare_sha256
        ]["historical_incomplete_postcommit_receipt_sha256"]
        for name, candidate in cases:
            with self.subTest(name=name):
                fixture_receipts = dict(receipts)
                if name == "missing_incomplete":
                    fixture_receipts.pop(incomplete)
                fixture_receipts["marker"] = candidate
                with (
                    mock.patch.object(
                        release,
                        "_deployment_receipt_objects",
                        return_value=fixture_receipts,
                    ),
                    mock.patch.dict(
                        release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS,
                        {historical_prepare_sha256: patched_descriptor},
                    ),
                    mock.patch.object(release, "_verify_historical_manager_script"),
                    mock.patch.object(release, "_verify_historical_executor_release"),
                    mock.patch.object(
                        release, "_verify_recovery_v2_dispatch_authority"
                    ),
                    mock.patch.object(
                        release,
                        "_recovery_v2_public_canary_gate",
                        side_effect=self.fixture_v2_public_gate(core),
                    ),
                    mock.patch.object(
                        release,
                        "sha256_bytes",
                        side_effect=fixture_sha256,
                    ),
                    self.assertRaisesRegex(
                        release.ReleaseError,
                        "deployment_recovery_marker_v2_invalid",
                    ),
                ):
                    release._assert_no_unresolved_deployment_transaction(
                        self.release_base
                    )

    def test_audited_prepare_cannot_be_closed_by_generic_postcommit(self) -> None:
        historical_prepare_sha256 = next(
            iter(release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS)
        )
        receipts, _proof, _proof_bytes, _patched_descriptor = self.superseded_recovery_receipts(
            historical_prepare_sha256
        )
        receipts["forged-generic-postcommit"] = {
            "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
            "operation": receipts[historical_prepare_sha256]["operation"],
            "release_id": receipts[historical_prepare_sha256]["release_id"],
            "status": "committed",
            "prepare_receipt_sha256": historical_prepare_sha256,
            "formal_write_count": 0,
        }
        with (
            mock.patch.object(
                release, "_deployment_receipt_objects", return_value=receipts
            ),
            self.assertRaisesRegex(
                release.ReleaseError, "deployment_transaction_unresolved"
            ),
        ):
            release._assert_no_unresolved_deployment_transaction(self.release_base)

    def test_v2_recovery_marker_rejects_extra_historical_postcommit(self) -> None:
        historical_prepare_sha256 = (
            "e462742448a3c538df114ed9b9aeb037cdd2bcff1aa29be6bb3cadbbf0470b55"
        )

        def mutate(receipts: dict, _marker: dict, _descriptor: dict) -> None:
            receipts["extra-own-postcommit"] = {
                "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
                "operation": "activate",
                "release_id": "fe61b4f38f7f05678adcbc1ecbebfbe0d9d31a5aa31b8850160bd0c271d0c4df",
                "status": "committed",
                "prepare_receipt_sha256": historical_prepare_sha256,
                "formal_write_count": 0,
            }

        self.assert_v2_marker_rejected(historical_prepare_sha256, mutate)

    def test_v2_recovery_marker_rejects_bad_continuation_prepare_schema(self) -> None:
        historical_prepare_sha256 = (
            "e462742448a3c538df114ed9b9aeb037cdd2bcff1aa29be6bb3cadbbf0470b55"
        )

        def mutate(receipts: dict, _marker: dict, descriptor: dict) -> None:
            receipt_id = descriptor["continuation_transactions"][0][
                "prepare_receipt_sha256"
            ]
            receipts[receipt_id]["schema_version"] = "tampered-prepare-schema"

        self.assert_v2_marker_rejected(historical_prepare_sha256, mutate)

    def test_audited_prepare_rejects_v1_only_v1_plus_v2_and_duplicate_v2(self) -> None:
        historical_prepare_sha256 = next(
            iter(release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS)
        )
        receipts, _proof, _proof_bytes, _descriptor = self.superseded_recovery_receipts(
            historical_prepare_sha256
        )
        receipts["v1-marker"] = {
            "schema_version": release.DEPLOYMENT_RECOVERY_MARKER_SCHEMA,
            "prepare_receipt_sha256": historical_prepare_sha256,
        }
        with (
            mock.patch.object(
                release, "_deployment_receipt_objects", return_value=receipts
            ),
            self.assertRaisesRegex(
                release.ReleaseError, "deployment_recovery_marker_invalid"
            ),
        ):
            release._assert_no_unresolved_deployment_transaction(self.release_base)

        def v1_plus_v2(receipts: dict, _marker: dict, _descriptor: dict) -> None:
            receipts["v1-marker"] = {
                "schema_version": release.DEPLOYMENT_RECOVERY_MARKER_SCHEMA,
                "prepare_receipt_sha256": historical_prepare_sha256,
            }

        self.assert_v2_marker_rejected(historical_prepare_sha256, v1_plus_v2)

        def duplicate_v2(receipts: dict, marker: dict, _descriptor: dict) -> None:
            receipts["marker-two"] = dict(marker)

        self.assert_v2_marker_rejected(historical_prepare_sha256, duplicate_v2)

    def test_v2_recovery_marker_rejects_generic_postcommit_alongside_v2(self) -> None:
        historical_prepare_sha256 = next(
            iter(release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS)
        )

        def mutate(receipts: dict, _marker: dict, descriptor: dict) -> None:
            receipts["generic-postcommit"] = {
                "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
                "operation": descriptor["historical_operation"],
                "release_id": descriptor["historical_release_id"],
                "status": "committed",
                "prepare_receipt_sha256": historical_prepare_sha256,
                "formal_write_count": 0,
            }

        self.assert_v2_marker_rejected(historical_prepare_sha256, mutate)

    def test_v2_recovery_marker_rejects_resolution_evidence_path_and_dashboard_mutations(
        self,
    ) -> None:
        historical_prepare_sha256 = next(
            iter(release.SUPERSEDED_TRANSACTION_RECOVERY_DESCRIPTORS)
        )
        cases = {
            "state_path": lambda marker: marker["resolution_evidence"]["states"][
                "math"
            ].__setitem__("path", "/wrong/state.json"),
            "activation_path": lambda marker: marker["resolution_evidence"]["states"][
                "math"
            ]["activation_receipt"].__setitem__("path", "/wrong/activation.json"),
            "dashboard_schema": lambda marker: marker["resolution_evidence"][
                "dashboard_projection"
            ]["value"].__setitem__("schema_version", "tampered"),
            "concurrency_release": lambda marker: marker["resolution_evidence"][
                "dashboard_projection"
            ]["value"]["concurrency"].__setitem__("release_id", "0" * 64),
            "dispatcher_active_release": lambda marker: marker["resolution_evidence"][
                "dashboard_projection"
            ]["value"]["dispatchers"]["math"].__setitem__(
                "active_release_id", "0" * 64
            ),
            "dashboard_path": lambda marker: marker["resolution_evidence"][
                "dashboard_projection"
            ].__setitem__("path", "/wrong/dashboard.json"),
            "telemetry_terminal_index": lambda marker: marker["resolution_evidence"][
                "dashboard_projection"
            ]["value"]["concurrency"]["source_telemetry"][
                "terminal_index_sha256_by_subject"
            ].__setitem__("math", "0" * 64),
            "state_terminal_count": lambda marker: marker["resolution_evidence"][
                "states"
            ]["math"]["value"].__setitem__("terminal_task_count", 1),
        }
        for name, mutate_marker in cases.items():
            with self.subTest(name=name):
                def mutate(receipts: dict, marker: dict, _descriptor: dict) -> None:
                    del receipts
                    mutate_marker(marker)
                    dashboard = marker["resolution_evidence"]["dashboard_projection"]
                    dashboard["sha256"] = release.sha256_bytes(
                        release.canonical_bytes(dashboard["value"])
                    )
                    resealed = release._seal_deployment_value(
                        marker,
                        release_base=self.release_base,
                        purpose=release.DEPLOYMENT_RECOVERY_MARKER_V2_PURPOSE,
                    )
                    marker.clear()
                    marker.update(resealed)

                self.assert_v2_marker_rejected(historical_prepare_sha256, mutate)

    def test_closed_deployment_history_is_not_misclassified_as_unresolved(self) -> None:
        built = self.build(passed=True)
        prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "operation": "activate",
            "release_id": str(built["release_id"]),
            "observed_current": None,
            "formal_write_count": 0,
        }
        prepare_sha256 = release._write_deployment_receipt(
            self.release_base, prepare
        )
        release._write_deployment_receipt(
            self.release_base,
            {
                "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
                "operation": "activate",
                "release_id": str(built["release_id"]),
                "status": "committed",
                "prepare_receipt_sha256": prepare_sha256,
                "formal_write_count": 0,
            },
        )
        release._assert_no_unresolved_deployment_transaction(self.release_base)

    def test_incomplete_or_misbound_postcommit_does_not_close_prepare(self) -> None:
        built = self.build(passed=True)
        prepare = {
            "schema_version": release.DEPLOYMENT_PREPARE_SCHEMA,
            "operation": "activate",
            "release_id": str(built["release_id"]),
            "observed_current": None,
            "formal_write_count": 0,
        }
        prepare_sha256 = release._write_deployment_receipt(
            self.release_base, prepare
        )
        for value in (
            {
                "status": "rollback_incomplete",
                "operation": "activate",
                "release_id": str(built["release_id"]),
                "formal_write_count": 0,
            },
            {
                "status": "committed",
                "operation": "rollback",
                "release_id": str(built["release_id"]),
                "formal_write_count": 0,
            },
            {
                "status": "rolled_back",
                "operation": "activate",
                "release_id": "f" * 64,
                "formal_write_count": 0,
            },
        ):
            release._write_deployment_receipt(
                self.release_base,
                {
                    "schema_version": release.DEPLOYMENT_POSTCOMMIT_SCHEMA,
                    "prepare_receipt_sha256": prepare_sha256,
                    **value,
                },
            )
        with self.assertRaisesRegex(
            release.ReleaseError, "deployment_transaction_unresolved"
        ):
            release._assert_no_unresolved_deployment_transaction(
                self.release_base
            )

    def test_canary_subject_recovery_expectation_is_exact_and_preview_only(
        self,
    ) -> None:
        original_sha256 = "3" * 64
        generation = "english-next-generation"
        fingerprint = "4" * 64
        authority = {
            subject: self.producer_authority(
                subject=subject,
                release_id="a" * 64,
                index=index,
            )
            for index, subject in enumerate(release.RESUMABLE_SUBJECTS, 1)
        }
        context = {
            "slots": {
                subject: {
                    "subject": subject,
                    "producer_authority_fingerprint": authority[subject][
                        "authority_fingerprint"
                    ],
                    "state": "planned",
                    "capture_id": None,
                    "completion_receipt_sha256": None,
                }
                for subject in release.RESUMABLE_SUBJECTS
            }
        }
        english_readiness = "recoverable_terminal_batch"
        english_readiness_mcp_tool_call_count = 1
        recovery_receipt_path = self.base / ("9" * 64 + ".json")

        def runner(command):
            subject = command[command.index("--subject") + 1]
            subcommand = command[command.index(subject) + 1]
            if subcommand == "canary-readiness":
                recoverable = subject == "english"
                recovered = (
                    recoverable
                    and english_readiness == "ready"
                )
                value = {
                    "schema_version": "study-intake-canary-readiness-v1",
                    "subject": subject,
                    "readiness": (
                        english_readiness if recoverable
                        else "ready"
                    ),
                    "reason": (
                        "recovered_terminal_batch_archived"
                        if recovered
                        else "explicit_subject_resume_required"
                        if recoverable
                        else "ready"
                    ),
                    "authority_generation": (
                        generation if recoverable else f"{subject}-generation"
                    ),
                    "authority_fingerprint": (
                        fingerprint
                        if recoverable
                        else authority[subject]["authority_fingerprint"]
                    ),
                    "original_preclaim_failure_receipt_sha256": (
                        original_sha256 if recoverable else None
                    ),
                    "preserved_queue_entry_sha256": (
                        "5" * 64 if recoverable else None
                    ),
                    "subsequent_attempt_receipt_sha256s": (
                        ["6" * 64, "7" * 64] if recoverable else []
                    ),
                    "recovery_receipt_sha256": (
                        "9" * 64 if recovered else None
                    ),
                    "recovery_receipt_path": (
                        str(recovery_receipt_path) if recovered else None
                    ),
                    "rollback_token": "a" * 64 if recovered else None,
                    "read_only": True,
                    "authority_snapshot_count": 1,
                    "mcp_tool_call_count": (
                        english_readiness_mcp_tool_call_count
                        if recoverable
                        else 1
                    ),
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                }
            elif subcommand == "audit":
                value = {
                    "subject": subject,
                    "read_only": True,
                    "lease_status": {
                        "schema_version": (
                            "study-intake-dispatch-subject-status-read-only-v1"
                        ),
                        "read_only": True,
                        "subject": subject,
                        "draining": True,
                        "active_count": 0,
                        "claimed_total": 0,
                        "model_call_count": 0,
                        "provider_request_count": 0,
                        "formal_write_count": 0,
                        "sol_enabled": False,
                    },
                    "draining": True,
                    "active_count": 0,
                    "claimed_total": 0,
                    "eligible_count": 0,
                    "eligible_units": [],
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                }
            else:
                raise AssertionError(command)
            return {"returncode": 0, "stdout": json.dumps(value)}

        with self.assertRaisesRegex(
            release.ReleaseError, "incident_recovery_not_supported_in_deploy_path"
        ):
            release._default_canary_pre_activation_verifier(
                release_id="a" * 64,
                target=self.source,
                runtime_data_root=self.runtime_data,
                runner=runner,
                canary_context=context,
                apply=False,
                subject_recovery_expectations={"english": original_sha256},
            )
        return
        checked = release._validate_canary_preflight_proof(
            proof, release_id="a" * 64, apply=False
        )
        english = next(
            row for row in checked["subjects"] if row["subject"] == "english"
        )
        self.assertTrue(english["subject_recovery_required"])
        self.assertEqual(
            english[
                "subject_recovery_original_preclaim_failure_receipt_sha256"
            ],
            original_sha256,
        )
        self.assertEqual(english["subject_authority_generation"], generation)
        english_readiness = "ready"
        reopened = release._default_canary_pre_activation_verifier(
            release_id="a" * 64,
            target=self.source,
            runtime_data_root=self.runtime_data,
            runner=runner,
            canary_context=context,
            apply=False,
            subject_recovery_expectations={"english": original_sha256},
        )
        reopened_english = next(
            row
            for row in reopened["subjects"]
            if row["subject"] == "english"
        )
        self.assertEqual(
            reopened_english["subject_batch_readiness"], "ready"
        )
        self.assertTrue(reopened_english["subject_recovery_required"])
        self.assertEqual(
            reopened_english[
                "subject_recovery_original_preclaim_failure_receipt_sha256"
            ],
            original_sha256,
        )
        english_readiness_mcp_tool_call_count = 0
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_subject_readiness_invalid:english",
        ):
            release._default_canary_pre_activation_verifier(
                release_id="a" * 64,
                target=self.source,
                runtime_data_root=self.runtime_data,
                runner=runner,
                canary_context=context,
                apply=False,
                subject_recovery_expectations={"english": original_sha256},
            )
        english_readiness_mcp_tool_call_count = 1
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_subject_readiness_failed:english",
        ):
            release._default_canary_pre_activation_verifier(
                release_id="a" * 64,
                target=self.source,
                runtime_data_root=self.runtime_data,
                runner=runner,
                canary_context=context,
                apply=False,
                subject_recovery_expectations={"english": "8" * 64},
            )
        with self.assertRaisesRegex(
            release.ReleaseError, "three_subject_canary_preflight_invalid"
        ):
            release._validate_canary_preflight_proof(
                proof, release_id="a" * 64, apply=True
            )

    def test_exact_math_migration_readiness_binds_archived_batch(self) -> None:
        release_id = "a" * 64
        archived = {
            "batch_id": "LUNA-MATH-BOUND",
            "batch_sha256": "b" * 64,
            "archive_sha256": "c" * 64,
            "archived_authority_generation": "math-source",
        }
        preview = {
            "schema_version": (
                "study-intake-math-pending-queue-migration-preview-v1"
            ),
            "status": "ready",
            "read_only": True,
            "migration_intent_sha256": "d" * 64,
            "migration_intent": {
                "target_release_id": release_id,
                "gs269_archived_evidence": archived,
            },
            "source_queue_count": 4,
            "target_queue_write_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        readiness = {
            "schema_version": "study-intake-canary-readiness-v1",
            "subject": "math",
            "readiness": "invalid",
            "reason": "subject_luna_batch_already_current",
            "batch_id": "LUNA-MATH-BOUND",
            "batch_sha256": "b" * 64,
            "writer_batch_id": None,
            "source_generation": "math-source",
            "original_preclaim_failure_receipt_sha256": None,
            "original_preclaim_failure_receipt_path": None,
            "preserved_queue_entry_sha256": None,
            "preserved_task": None,
            "subsequent_attempt_receipt_sha256s": [],
            "recovery_receipt_sha256": None,
            "recovery_receipt_path": None,
            "rollback_token": None,
            "authorization_descriptor_sha256": None,
            "terminal_receipt_sha256": None,
            "writer_preimage_sha256": None,
            "batch_pointer_sha256": None,
            "snapshot_sha256": None,
            "source_authority_fingerprint": None,
            "mode": None,
            "cs408_terminal_retirement_receipt_sha256": None,
            "cs408_terminal_retirement_receipt_path": None,
            "cs408_terminal_retirement_rollback_token": None,
            "writer_postimage_sha256": None,
            "postimage_verification_sha256": None,
        }
        self.assertTrue(
            release._math_pending_queue_migration_source_readiness_matches(
                readiness, preview, release_id=release_id
            )
        )
        readiness["batch_sha256"] = "e" * 64
        self.assertFalse(
            release._math_pending_queue_migration_source_readiness_matches(
                readiness, preview, release_id=release_id
            )
        )

    def test_exact_english_review_correction_preactivation_is_explicit_and_narrow(
        self,
    ) -> None:
        release_id = "a" * 64
        preview = self.english_review_repair_preview_fixture()
        context = {
            "slots": {
                subject: {"producer_high_watermark_sha256": f"{index}" * 64}
                for index, subject in enumerate(
                    release.RESUMABLE_SUBJECTS, start=1
                )
            }
        }
        overrides: dict[str, dict[str, object]] = {}
        commands: list[list[str]] = []

        def readiness(subject: str) -> dict:
            if subject == "english":
                descriptor = preview["authorization_descriptor"]
                value = {
                    "schema_version": "study-intake-canary-readiness-v1",
                    "subject": subject,
                    "readiness": "invalid",
                    "reason": "production_canary_preserved_failure_missing",
                    "authority_generation": (
                        release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_AUTHORITY_GENERATION
                    ),
                    "authority_fingerprint": (
                        release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_AUTHORITY_FINGERPRINT
                    ),
                    "batch_id": descriptor["batch_id"],
                    "batch_sha256": descriptor[
                        "subject_batch_preimage_sha256"
                    ],
                    "writer_revision": (
                        release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_WRITER_REVISION
                    ),
                    "writer_batch_id": descriptor["batch_id"],
                    "source_generation": (
                        release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_AUTHORITY_GENERATION
                    ),
                }
            else:
                value = {
                    "schema_version": "study-intake-canary-readiness-v1",
                    "subject": subject,
                    "readiness": "ready",
                    "reason": "ready",
                    "authority_generation": f"{subject}-generation",
                    "authority_fingerprint": "a" * 64,
                    "batch_id": None,
                    "batch_sha256": None,
                    "writer_revision": 0,
                    "writer_batch_id": None,
                    "source_generation": None,
                }
            value.update(
                {
                    "original_preclaim_failure_receipt_sha256": None,
                    "original_preclaim_failure_receipt_path": None,
                    "preserved_queue_entry_sha256": None,
                    "subsequent_attempt_receipt_sha256s": [],
                    "preserved_task": None,
                    "recovery_receipt_sha256": None,
                    "recovery_receipt_path": None,
                    "rollback_token": None,
                    "authorization_descriptor_sha256": None,
                    "terminal_receipt_sha256": None,
                    "writer_preimage_sha256": None,
                    "batch_pointer_sha256": None,
                    "snapshot_sha256": None,
                    "source_authority_fingerprint": None,
                    "mode": None,
                    "cs408_terminal_retirement_receipt_sha256": None,
                    "cs408_terminal_retirement_receipt_path": None,
                    "cs408_terminal_retirement_rollback_token": None,
                    "writer_postimage_sha256": None,
                    "postimage_verification_sha256": None,
                    "read_only": True,
                    "authority_snapshot_count": 1,
                    "mcp_tool_call_count": 1,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                }
            )
            value.update(overrides.get(subject, {}))
            return value

        def runner(command):
            commands.append(list(command))
            subject = command[command.index("--subject") + 1]
            subcommand = command[command.index(subject) + 1]
            if subcommand == "canary-readiness":
                value = readiness(subject)
            elif subcommand == "audit":
                lease_status = {
                    "schema_version": (
                        "study-intake-dispatch-subject-status-read-only-v1"
                    ),
                    "read_only": True,
                    "subject": subject,
                    "draining": True,
                    "active_count": 0,
                    "claimed_total": 0,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                    "sol_enabled": False,
                }
                value = {
                    "subject": subject,
                    "read_only": True,
                    "lease_status": lease_status,
                    "draining": True,
                    "active_count": 0,
                    "claimed_total": 0,
                    "eligible_count": 0,
                    "eligible_units": [],
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "formal_write_count": 0,
                }
            else:
                raise AssertionError(command)
            return {"returncode": 0, "stdout": json.dumps(value)}

        with self.assertRaisesRegex(
            release.ReleaseError, "incident_recovery_not_supported_in_deploy_path"
        ):
            release._default_canary_pre_activation_verifier(
                release_id=release_id,
                target=self.source,
                runtime_data_root=self.runtime_data,
                runner=runner,
                canary_context=context,
                apply=False,
                english_preserved_review_repair_preview=preview,
            )
        return
        checked = release._validate_canary_preflight_proof(
            proof, release_id=release_id, apply=False
        )
        english = next(
            row for row in checked["subjects"] if row["subject"] == "english"
        )
        self.assertEqual(
            english["subject_batch_readiness"],
            "recoverable_review_correction",
        )
        self.assertTrue(english["english_preserved_review_repair_required"])
        self.assertEqual(
            english["english_preserved_review_repair_preview_sha256"],
            preview["preview_sha256"],
        )
        self.assertFalse(english["subject_recovery_required"])
        self.assertEqual(proof["verification_mcp_tool_call_count"], 3)

        commands.clear()
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_subject_readiness_failed:"
            "english:recoverable_review_correction_planning_only",
        ):
            release._default_canary_pre_activation_verifier(
                release_id=release_id,
                target=self.source,
                runtime_data_root=self.runtime_data,
                runner=runner,
                canary_context=context,
                apply=True,
                english_preserved_review_repair_preview=preview,
            )
        self.assertFalse(
            any("audit" in command for command in commands), commands
        )
        self.assertFalse(
            any("activate-canary" in command for command in commands),
            commands,
        )

        for label, subject, field, value, error_subject in (
            (
                "wrong_reason",
                "english",
                "reason",
                "explicit_subject_resume_required",
                "english",
            ),
            (
                "wrong_lineage",
                "english",
                "batch_id",
                "WRONG-BATCH",
                "english",
            ),
            (
                "other_subject",
                "math",
                "readiness",
                "invalid",
                "math",
            ),
        ):
            with self.subTest(label=label):
                overrides.clear()
                overrides[subject] = {field: value}
                if label == "other_subject":
                    overrides[subject]["reason"] = (
                        "production_canary_preserved_failure_missing"
                    )
                with self.assertRaisesRegex(
                    release.ReleaseError,
                    "three_subject_canary_subject_readiness_failed:"
                    + error_subject,
                ):
                    release._default_canary_pre_activation_verifier(
                        release_id=release_id,
                        target=self.source,
                        runtime_data_root=self.runtime_data,
                        runner=runner,
                        canary_context=context,
                        apply=False,
                        english_preserved_review_repair_preview=preview,
                    )
        overrides.clear()
        wrong_gate = copy.deepcopy(preview)
        wrong_gate["source_gate_preimage_sha256"] = "f" * 64
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_subject_readiness_failed:english",
        ):
            release._default_canary_pre_activation_verifier(
                release_id=release_id,
                target=self.source,
                runtime_data_root=self.runtime_data,
                runner=runner,
                canary_context=context,
                apply=False,
                english_preserved_review_repair_preview=wrong_gate,
            )
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_subject_readiness_failed:english",
        ):
            release._default_canary_pre_activation_verifier(
                release_id=release_id,
                target=self.source,
                runtime_data_root=self.runtime_data,
                runner=runner,
                canary_context=context,
                apply=False,
            )

    def test_activate_canary_true_preview_threads_exact_review_correction_proof(
        self,
    ) -> None:
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        manifest = self.make_canary_manifest(built)
        preview = self.english_review_repair_preview_fixture()
        raw_preview = copy.deepcopy(preview)
        raw_preview.pop("preview_sha256")
        source_release_id = release.ENGLISH_PRESERVED_REVIEW_REPAIR_LINEAGE[
            "source_release_id"
        ]
        source = self.base / "review-source" / source_release_id
        source.mkdir(parents=True)
        active = self.base / "review-preview-current"
        active.symlink_to(source)

        class ReviewPreviewRunner(self.FakeRunner):
            def __call__(inner_self, command):
                if "english-preserved-review-repair-preview" in command:
                    inner_self.commands.append(list(command))
                    return {
                        "returncode": 0,
                        "stdout": json.dumps(raw_preview),
                    }
                if command and command[-1] == "canary-readiness":
                    inner_self.commands.append(list(command))
                    subject = command[command.index("--subject") + 1]
                    if subject == "english":
                        descriptor = preview["authorization_descriptor"]
                        value = {
                            "schema_version": (
                                "study-intake-canary-readiness-v1"
                            ),
                            "subject": subject,
                            "readiness": "invalid",
                            "reason": (
                                "production_canary_preserved_failure_missing"
                            ),
                            "authority_generation": (
                                release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_AUTHORITY_GENERATION
                            ),
                            "authority_fingerprint": (
                                release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_AUTHORITY_FINGERPRINT
                            ),
                            "batch_id": descriptor["batch_id"],
                            "batch_sha256": descriptor[
                                "subject_batch_preimage_sha256"
                            ],
                            "writer_revision": (
                                release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_WRITER_REVISION
                            ),
                            "writer_batch_id": descriptor["batch_id"],
                            "source_generation": (
                                release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_AUTHORITY_GENERATION
                            ),
                        }
                    else:
                        value = {
                            "schema_version": (
                                "study-intake-canary-readiness-v1"
                            ),
                            "subject": subject,
                            "readiness": "ready",
                            "reason": "ready",
                            "authority_generation": f"{subject}-generation",
                            "authority_fingerprint": "a" * 64,
                            "batch_id": None,
                            "batch_sha256": None,
                            "writer_revision": 0,
                            "writer_batch_id": None,
                            "source_generation": None,
                        }
                    value.update(
                        {
                            "original_preclaim_failure_receipt_sha256": None,
                            "original_preclaim_failure_receipt_path": None,
                            "preserved_queue_entry_sha256": None,
                            "subsequent_attempt_receipt_sha256s": [],
                            "preserved_task": None,
                            "recovery_receipt_sha256": None,
                            "recovery_receipt_path": None,
                            "rollback_token": None,
                            "authorization_descriptor_sha256": None,
                            "terminal_receipt_sha256": None,
                            "writer_preimage_sha256": None,
                            "batch_pointer_sha256": None,
                            "snapshot_sha256": None,
                            "source_authority_fingerprint": None,
                            "mode": None,
                            "cs408_terminal_retirement_receipt_sha256": None,
                            "cs408_terminal_retirement_receipt_path": None,
                            "cs408_terminal_retirement_rollback_token": None,
                            "writer_postimage_sha256": None,
                            "postimage_verification_sha256": None,
                            "read_only": True,
                            "authority_snapshot_count": 1,
                            "mcp_tool_call_count": 1,
                            "model_call_count": 0,
                            "provider_request_count": 0,
                            "formal_write_count": 0,
                            "sol_enabled": False,
                        }
                    )
                    return {
                        "returncode": 0,
                        "stdout": json.dumps(value),
                    }
                return super().__call__(command)

        runner = ReviewPreviewRunner()
        with mock.patch.object(
            release, "_release_topology", return_value=[]
        ), self.assertRaisesRegex(
            release.ReleaseError, "incident_recovery_not_supported_in_deploy_path"
        ):
            release.activate_canary_release(
                release_base=self.release_base,
                release_id=release_id,
                active_link=active,
                canary_manifest=manifest,
                apply=False,
                expected_current=source_release_id,
                launchagent_dir=self.launchagents,
                command_runner=runner,
                english_preserved_review_repair_descriptor_sha256=(
                    release.ENGLISH_PRESERVED_REVIEW_REPAIR_DESCRIPTOR_SHA256
                ),
                preview_state_inspector=lambda **_values: {
                    "fixture": "unchanged"
                },
            )
        return
        english = next(
            row
            for row in result["pre_activation_verification"]["subjects"]
            if row["subject"] == "english"
        )
        self.assertEqual(result["status"], "production_canary_planned")
        self.assertTrue(result["preview_non_mutation_verified"])
        self.assertEqual(
            english["subject_batch_readiness"],
            "recoverable_review_correction",
        )
        self.assertEqual(
            result["pre_activation_verification"][
                "verification_mcp_tool_call_count"
            ],
            3,
        )

    def test_applied_english_recovery_preflight_accepts_one_replacement_queue(
        self,
    ) -> None:
        release_id = "a" * 64
        context = {
            "slots": {
                subject: {"producer_high_watermark_sha256": "b" * 64}
                for subject in release.RESUMABLE_SUBJECTS
            }
        }
        proof = self.canary_preflight_proof(
            apply=True,
            canary_context=context,
            release_id=release_id,
        )
        english = next(
            row for row in proof["subjects"] if row["subject"] == "english"
        )
        english.update(
            {
                "historical_eligible_count": 3,
                "excluded_by_high_watermark_count": 2,
                "canary_queue_count": 1,
                "subject_batch_readiness": "ready",
                "subject_batch_readiness_sha256": "c" * 64,
                "subject_recovery_required": True,
                "subject_recovery_already_applied": True,
                "subject_recovery_original_preclaim_failure_receipt_sha256": (
                    "d" * 64
                ),
                "subject_recovery_receipt_sha256": "e" * 64,
                "subject_recovery_receipt_path": str(
                    (self.base / "english-recovery.json").resolve()
                ),
                "subject_recovery_rollback_token": "f" * 64,
                "subject_readiness_mcp_tool_call_count": 1,
                "subject_authority_generation": "english-generation-v2",
                "subject_authority_fingerprint": "1" * 64,
            }
        )
        checked = release._validate_canary_preflight_proof(
            proof, release_id=release_id, apply=True
        )
        checked_english = next(
            row
            for row in checked["subjects"]
            if row["subject"] == "english"
        )
        self.assertEqual(
            checked_english["excluded_by_high_watermark_count"] + 1,
            checked_english["historical_eligible_count"],
        )
        invalid = copy.deepcopy(proof)
        invalid_english = next(
            row
            for row in invalid["subjects"]
            if row["subject"] == "english"
        )
        invalid_english["excluded_by_high_watermark_count"] = 3
        with self.assertRaisesRegex(
            release.ReleaseError, "three_subject_canary_preflight_invalid"
        ):
            release._validate_canary_preflight_proof(
                invalid, release_id=release_id, apply=True
            )

    def test_canary_common_controls_accept_v3_for_activation_rollback(
        self,
    ) -> None:
        controls = {
            **self.canary_v2_controls(),
            "schema_version": "study-intake-production-canary-state-v3",
            "production_accepted": False,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        self.assertTrue(release._canary_v2_common_controls_valid(controls))
        controls["schema_version"] = "study-intake-production-canary-state-v4"
        self.assertFalse(release._canary_v2_common_controls_valid(controls))

    def test_activation_rollback_shutdown_accepts_exact_v3_gates(self) -> None:
        release_id = "a" * 64
        gates = {
            subject: {
                **self.canary_v2_controls(),
                "schema_version": "study-intake-production-canary-state-v3",
                "subject": subject,
                "release_id": release_id,
                "active_task_count": 0,
                "production_accepted": False,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            }
            for subject in release.RESUMABLE_SUBJECTS
        }

        def runner(command):
            subject = command[command.index("--subject") + 1]
            return {
                "returncode": 0,
                "stdout": json.dumps(
                    {
                        "subject": subject,
                        "active_count": 0,
                        "claimed_total": 0,
                        "canary_gate": gates[subject],
                    }
                ),
            }

        drained = {
            subject: {
                "subject": subject,
                "active_count": 0,
                "claimed_total": 0,
                "stale_count": 0,
                "status_sha256": str(index) * 64,
            }
            for index, subject in enumerate(release.RESUMABLE_SUBJECTS, 1)
        }
        with mock.patch.object(
            release,
            "_rollback_three_subject_dispatchers",
            return_value=(drained, {}, []),
        ) as rollback:
            proof = release._default_canary_shutdown_before_activation_rollback(
                release_id=release_id,
                target=self.source,
                runtime_data_root=self.runtime_data,
                topology=[],
                runner=runner,
            )
        self.assertEqual(proof["status"], "drained")
        self.assertEqual(
            set(rollback.call_args.kwargs["gate_before"]),
            set(release.RESUMABLE_SUBJECTS),
        )
        tampered = copy.deepcopy(gates["math"])
        tampered["formal_write_count"] = 1
        gates["math"] = tampered
        with self.assertRaisesRegex(
            release.ReleaseError, "canary_activation_failure_gate_invalid:math"
        ):
            release._default_canary_shutdown_before_activation_rollback(
                release_id=release_id,
                target=self.source,
                runtime_data_root=self.runtime_data,
                topology=[],
                runner=runner,
            )

    def test_activation_rollback_shutdown_ignores_untouched_inactive_source_gate(
        self,
    ) -> None:
        target_release_id = "a" * 64
        source_release_id = "b" * 64
        gates = {
            "math": {
                **self.canary_v2_controls(),
                "schema_version": "study-intake-production-canary-state-v3",
                "status": "production_canary_active",
                "state": "armed",
                "subject": "math",
                "release_id": target_release_id,
                "active_task_count": 0,
                "luna_consumer_enabled": True,
                "production_accepted": False,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            },
            "cs408": {
                **self.canary_v2_controls(),
                "schema_version": "study-intake-production-canary-state-v3",
                "status": "production_canary_inactive",
                "state": "inactive_rolled_back",
                "subject": "cs408",
                "release_id": source_release_id,
                "active_task_count": 0,
                "luna_consumer_enabled": False,
                "production_accepted": False,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            },
            "english": {
                **self.canary_v2_controls(),
                "schema_version": "study-intake-production-canary-state-v3",
                "status": "production_canary_inactive",
                "state": "inactive_rolled_back",
                "subject": "english",
                "release_id": source_release_id,
                "active_task_count": 0,
                "luna_consumer_enabled": False,
                "production_accepted": False,
                "model_call_count": 0,
                "provider_request_count": 0,
                "formal_write_count": 0,
                "sol_enabled": False,
            },
        }

        def runner(command):
            subject = command[command.index("--subject") + 1]
            return {
                "returncode": 0,
                "stdout": json.dumps(
                    {
                        "subject": subject,
                        "active_count": 0,
                        "claimed_total": 0,
                        "canary_gate": gates[subject],
                    }
                ),
            }

        drained = {
            subject: {
                "subject": subject,
                "active_count": 0,
                "claimed_total": 0,
                "stale_count": 0,
                "status_sha256": str(index) * 64,
            }
            for index, subject in enumerate(release.RESUMABLE_SUBJECTS, 1)
        }
        with mock.patch.object(
            release,
            "_rollback_three_subject_dispatchers",
            return_value=(drained, {"math": gates["math"]}, []),
        ) as rollback:
            proof = release._default_canary_shutdown_before_activation_rollback(
                release_id=target_release_id,
                target=self.source,
                runtime_data_root=self.runtime_data,
                topology=[],
                runner=runner,
            )

        self.assertEqual(proof["status"], "drained")
        self.assertEqual(
            set(rollback.call_args.kwargs["gate_before"]), {"math"}
        )

    def test_subject_batch_recovery_rollback_result_is_exact(self) -> None:
        recovery_receipt_path = self.base / "english-recovery.json"
        rollback_receipt_bytes = release.canonical_bytes(
            {"fixture": "english-recovery-rollback-ca"}
        )
        rollback_receipt_sha256 = release.sha256_bytes(
            rollback_receipt_bytes
        )
        rollback_receipt_path = (
            self.base
            / "english-recovery-rollbacks"
            / rollback_receipt_sha256[:2]
            / f"{rollback_receipt_sha256}.json"
        )
        rollback_receipt_path.parent.mkdir(parents=True, exist_ok=True)
        rollback_receipt_path.write_bytes(rollback_receipt_bytes)
        recovery = {
            "recovery_receipt_path": str(recovery_receipt_path),
            "recovery_receipt_sha256": "1" * 64,
            "rollback_token": "2" * 64,
            "preserved_queue_entry_sha256": "3" * 64,
        }
        valid = {
            "schema_version": (
                "study-intake-subject-batch-recovery-rollback-result-v1"
            ),
            "subject": "english",
            "status": "rolled_back",
            "recovery_receipt_sha256": recovery[
                "recovery_receipt_sha256"
            ],
            "rollback_token": recovery["rollback_token"],
            "rollback_receipt_sha256": rollback_receipt_sha256,
            "rollback_receipt_path": str(rollback_receipt_path),
            "writer_state_restored_sha256": "4" * 64,
            "batch_pointer_restored_sha256": "6" * 64,
            "canary_state_restored_sha256": "5" * 64,
            "preserved_queue_entry_sha256": recovery[
                "preserved_queue_entry_sha256"
            ],
            "target_queue_withdrawn": True,
            "removed_mutable_pointer_count": 3,
            "idempotent": False,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

        def invoke(payload: Mapping[str, Any]) -> tuple[dict, list[list[str]]]:
            commands: list[list[str]] = []

            def runner(command):
                commands.append(list(command))
                return {"returncode": 0, "stdout": json.dumps(payload)}

            result = release._rollback_subject_batch_recovery(
                target=self.source,
                recovery=recovery,
                runner=runner,
            )
            return result, commands

        result, commands = invoke(valid)
        self.assertEqual(result, valid)
        self.assertEqual(len(commands), 1)
        self.assertIn("rollback-subject-batch-recovery", commands[0])
        self.assertEqual(commands[0][-1], str(recovery_receipt_path))

        required_exact_fields = (
            "rollback_token",
            "rollback_receipt_path",
            "writer_state_restored_sha256",
            "batch_pointer_restored_sha256",
            "canary_state_restored_sha256",
            "preserved_queue_entry_sha256",
            "target_queue_withdrawn",
            "removed_mutable_pointer_count",
            "idempotent",
        )
        for field in required_exact_fields:
            with self.subTest(field=field):
                malformed = dict(valid)
                malformed.pop(field)
                with self.assertRaisesRegex(
                    release.ReleaseError,
                    "english_subject_batch_recovery_rollback_invalid",
                ):
                    invoke(malformed)

        for field, bad_value in (
            ("rollback_token", "f" * 64),
            ("preserved_queue_entry_sha256", "f" * 64),
            ("target_queue_withdrawn", False),
            ("removed_mutable_pointer_count", True),
            ("idempotent", "false"),
        ):
            with self.subTest(field=field, bad_value=bad_value):
                malformed = {**valid, field: bad_value}
                with self.assertRaisesRegex(
                    release.ReleaseError,
                    "english_subject_batch_recovery_rollback_invalid",
                ):
                    invoke(malformed)

    def test_recovery_rollback_failure_keeps_target_fenced_and_does_not_restore_previous(
        self,
    ) -> None:
        previous = self.build(passed=True)
        previous_id = str(previous["release_id"])
        previous_target = Path(str(previous["release_dir"]))
        active = self.base / "canary-recovery-rollback-failure-current"
        active.symlink_to(previous_target)
        self.install_current_plists(previous_target, active)
        (self.source / "bin" / "worker.py").write_text(
            "print('recovery rollback failure target')\n",
            encoding="utf-8",
        )
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        manifest = self.make_canary_manifest(built)
        drain_receipt = self.make_drain_receipt(
            previous_id, release_id, active
        )
        original_receipt_sha256 = "3" * 64
        recovery_receipt_sha256 = "4" * 64
        recovery_receipt_path = self.base / "recovery-rollback-failure.json"
        state_root = (
            self.runtime_data
            / "dispatch"
            / "state"
            / "production-canary"
        )
        state_root.mkdir(parents=True, exist_ok=True)
        previous_state = b'{"fixture":"previous"}\n'
        target_state = b'{"fixture":"target-fenced"}\n'
        state_paths = {
            subject: state_root / f"{subject}.json"
            for subject in release.RESUMABLE_SUBJECTS
        }
        for path in state_paths.values():
            path.write_bytes(previous_state)

        def preflight(**values) -> dict:
            proof = self.canary_preflight_proof(**values)
            if not values["apply"]:
                english = next(
                    row
                    for row in proof["subjects"]
                    if row["subject"] == "english"
                )
                english.update(
                    {
                        "subject_batch_readiness": (
                            "recoverable_terminal_batch"
                        ),
                        "subject_batch_readiness_sha256": "5" * 64,
                        "subject_recovery_required": True,
                        "subject_recovery_already_applied": False,
                        "subject_recovery_original_preclaim_failure_receipt_sha256": (
                            original_receipt_sha256
                        ),
                        "subject_recovery_receipt_sha256": None,
                        "subject_recovery_receipt_path": None,
                        "subject_recovery_rollback_token": None,
                        "subject_readiness_mcp_tool_call_count": 1,
                        "subject_authority_generation": "english-generation-v2",
                        "subject_authority_fingerprint": "6" * 64,
                    }
                )
            return proof

        recovery = {
            "release_id": release_id,
            "original_preclaim_failure_receipt_sha256": (
                original_receipt_sha256
            ),
            "preserved_queue_entry_sha256": "7" * 64,
            "recovery_receipt_sha256": recovery_receipt_sha256,
            "recovery_receipt_path": str(recovery_receipt_path),
            "rollback_token": "8" * 64,
        }
        staged = {
            "schema_version": (
                "study-intake-subject-batch-recovery-staged-activation-result-v1"
            ),
            "subject": "english",
            "status": "staged",
            "recovery_receipt_sha256": recovery_receipt_sha256,
            "target_release_id": release_id,
            "target_activation_id": "9" * 64,
            "producer_authority_fingerprint": "6" * 64,
            "luna_consumer_enabled": False,
            "state": "paused_drained",
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        finalization = {
            "schema_version": (
                "study-intake-subject-batch-recovery-finalization-result-v1"
            ),
            "subject": "english",
            "status": "finalized",
            "recovery_receipt_sha256": recovery_receipt_sha256,
            "supersede_receipt_sha256": "a" * 64,
            "supersede_receipt_path": str(
                self.base / "rollback-failure-supersede.json"
            ),
            "old_queue_entry_sha256": "7" * 64,
            "target_activation_id": "9" * 64,
            "target_release_id": release_id,
            "replacement_task": {
                "unit_sha256": "d" * 64,
                "frozen_payload_sha256": "e" * 64,
                "task_object_sha256": "f" * 64,
                "task_object_path": str(
                    self.base / "rollback-failure-task.json"
                ),
                "producer_input_contract_sha256": "1" * 64,
                "source_event_set_sha256": "2" * 64,
            },
            "replacement_queue_entry_sha256": "b" * 64,
            "replacement_queue_entry_path": str(
                self.base / "rollback-failure-queue.json"
            ),
            "idempotent": False,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        armed = {
            "schema_version": (
                "study-intake-subject-batch-recovery-arm-result-v1"
            ),
            "subject": "english",
            "status": "armed",
            "recovery_receipt_sha256": recovery_receipt_sha256,
            "target_release_id": release_id,
            "target_activation_id": "9" * 64,
            "luna_consumer_enabled": True,
            "state": "armed",
            "idempotent": False,
            "formal_write_count": 0,
            "sol_enabled": False,
        }
        service_activations: list[list[Mapping[str, Any]]] = []

        def record_service_activation(topology, *_args, **_kwargs):
            service_activations.append(list(topology))

        def fail_health(**_values):
            for path in state_paths.values():
                path.write_bytes(target_state)
            raise release.ReleaseError("injected_post_activation_failure")

        def empty_process_snapshot(*_args, **_kwargs):
            return {
                "schema_version": release.PROCESS_SNAPSHOT_SCHEMA,
                "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "legacy_worker_pids": [],
                "dispatcher_pids": [],
                "dashboard_pids": [],
                "luna_pids": [],
                "process_set_sha256": "c" * 64,
            }

        drained = {
            "schema_version": "study-intake-rollback-drain-v1",
            "status": "drained",
            "release_id": release_id,
            "runtime_data_root": str(self.runtime_data.resolve()),
            "subjects": [
                {
                    "subject": subject,
                    "active_count": 0,
                    "claimed_total": 0,
                    "stale_count": 0,
                    "status_sha256": f"{index}" * 64,
                }
                for index, subject in enumerate(
                    release.RESUMABLE_SUBJECTS, start=1
                )
            ],
            "formal_write_count": 0,
        }

        with (
            mock.patch.object(
                release,
                "_recover_subject_batch_before_canary_arm",
                return_value=recovery,
            ),
            mock.patch.object(
                release,
                "_stage_subject_batch_recovery_activation",
                return_value=staged,
            ),
            mock.patch.object(
                release,
                "_finalize_subject_batch_recovery_before_canary_arm",
                return_value=finalization,
            ),
            mock.patch.object(
                release,
                "_arm_finalized_subject_batch_recovery",
                return_value=armed,
            ),
            mock.patch.object(
                release,
                "_rollback_subject_batch_recovery",
                side_effect=release.ReleaseError(
                    "english_subject_batch_recovery_rollback_failed"
                ),
            ),
            mock.patch.object(
                release,
                "_deactivate_previous_canary_states",
                return_value={"status": "deactivated"},
            ),
            mock.patch.object(
                release,
                "_default_canary_shutdown_before_activation_rollback",
                return_value=drained,
            ),
            mock.patch.object(
                release, "_run_service_deactivation", return_value=None
            ),
            mock.patch.object(
                release,
                "_run_service_activation",
                side_effect=record_service_activation,
            ),
        ):
            with self.assertRaisesRegex(
                release.ReleaseError,
                "activation_failed_rollback_incomplete",
            ):
                release.activate_canary_release(
                    release_base=self.release_base,
                    release_id=release_id,
                    active_link=active,
                    canary_manifest=manifest,
                    apply=True,
                    expected_current=previous_id,
                    drain_receipt=drain_receipt,
                    launchagent_dir=self.launchagents,
                    command_runner=self.FakeRunner(),
                    process_inspector=empty_process_snapshot,
                    canary_pre_activation_verifier=preflight,
                    post_activation_verifier=fail_health,
                    canary_rollback_controller=self.canary_rollback_proof,
                    subject_recovery_expectations={
                        "english": original_receipt_sha256
                    },
                )

        self.assertEqual(active.resolve(), target)
        self.assertEqual(len(service_activations), 1)
        self.assertEqual(
            {path.read_bytes() for path in state_paths.values()},
            {target_state},
        )
        receipts = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in (self.release_base / "deployments").glob("*.json")
        ]
        incomplete = next(
            value
            for value in receipts
            if value.get("schema_version") == release.DEPLOYMENT_POSTCOMMIT_SCHEMA
            and value.get("status") == "rollback_incomplete"
            and value.get("release_id") == release_id
        )
        self.assertIsNone(incomplete.get("restored_release_id"))
        self.assertIsNone(
            incomplete.get("previous_canary_state_restore_proofs")
        )

    def test_english_review_repair_stage_splits_subject_and_producer_authority(
        self,
    ) -> None:
        for module_root in (ROOT / "lib", ROOT / "bin"):
            if str(module_root) not in sys.path:
                sys.path.insert(0, str(module_root))
        import concurrent_dispatch as dispatch

        release_id = "a" * 64
        subject_authority = "b" * 64
        producer = self.producer_authority(
            subject="english", release_id=release_id, index=1
        )
        store = dispatch.LeaseStore(self.base / "review-stage-runtime")
        store.begin_subject_drain("english")
        active = store.activate_production_canary(
            "english",
            release_id=release_id,
            producer_authority=producer,
            activated_at="2026-08-13T01:02:03+00:00",
            continuous_concurrency_limit=20,
        )
        paused = store.pause_production_canary("english")
        slot = {
            "subject": "english",
            "producer_authority_fingerprint": producer[
                "authority_fingerprint"
            ],
            "producer_high_watermark": active["producer_high_watermark"],
            "producer_high_watermark_sha256": active[
                "producer_high_watermark_sha256"
            ],
            "state": "armed",
            "capture_id": None,
            "completion_receipt_sha256": None,
        }
        commands: list[list[str]] = []
        responses = iter((active, paused))

        def runner(command):
            commands.append(list(command))
            return {
                "returncode": 0,
                "stdout": json.dumps(next(responses)),
            }

        staged = release._stage_english_preserved_review_repair_target_gate(
            target=self.source,
            release_id=release_id,
            slot=slot,
            target_generation="english-next-generation",
            target_subject_authority_fingerprint=subject_authority,
            runner=runner,
        )
        self.assertEqual(len(commands), 2)
        self.assertIn("activate-canary", commands[0])
        self.assertIn("pause-canary", commands[1])
        self.assertEqual(
            staged["target_subject_authority_fingerprint"],
            subject_authority,
        )
        self.assertEqual(
            staged["target_producer_authority_fingerprint"],
            producer["authority_fingerprint"],
        )
        self.assertNotEqual(
            staged["target_subject_authority_fingerprint"],
            staged["target_producer_authority_fingerprint"],
        )
        self.assertEqual(
            staged["staged_target_canary_state_sha256"],
            release.sha256_bytes(release.canonical_bytes(paused)),
        )
        physical_gate_path = store._production_canary_state_path("english")
        self.assertEqual(
            staged["staged_target_canary_state_sha256"],
            release.sha256_bytes(physical_gate_path.read_bytes()),
        )
        self.assertNotEqual(
            staged["staged_target_canary_state_sha256"],
            release.sha256_bytes(release.canonical_bytes(paused) + b"\n"),
        )

    def test_english_review_repair_exact_cli_results_and_rollback_reopen(
        self,
    ) -> None:
        release_id = "a" * 64
        activation_id = "b" * 64
        subject_authority = "c" * 64
        producer_authority = "d" * 64
        staged_sha = "e" * 64
        absolute = lambda name: str((self.base / name).resolve())
        evidence = {
            key: "1" * 64
            for key in release.ENGLISH_PRESERVED_REVIEW_REPAIR_EVIDENCE_KEYS
        }
        for key in (
            "original_preclaim_failure_receipt_sha256",
            "rollover_receipt_sha256",
            "recovery_supersede_receipt_sha256",
        ):
            evidence[key] = release.ENGLISH_PRESERVED_REVIEW_REPAIR_LINEAGE[
                key
            ]
        preimages = {
            key: "2" * 64
            for key in release.ENGLISH_PRESERVED_REVIEW_REPAIR_PREIMAGE_KEYS
        }
        preimages["gate_preimage_sha256"] = (
            release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_GATE_SHA256
        )
        preimages["staged_gate_preimage_sha256"] = staged_sha
        artifacts = {
            key: (absolute(key) if key.endswith("_path") else "3" * 64)
            for key in release.ENGLISH_PRESERVED_REVIEW_REPAIR_ARTIFACT_KEYS
        }
        gate = {"fixture": "armed-review-gate"}
        receipt = {
            key: "4" * 64
            for key in release.ENGLISH_PRESERVED_REVIEW_REPAIR_RECEIPT_KEYS
        }
        receipt.update(
            {
                "schema_version": (
                    "study-intake-english-preserved-review-repair-receipt-v1"
                ),
                "authorization_descriptor_sha256": (
                    release.ENGLISH_PRESERVED_REVIEW_REPAIR_DESCRIPTOR_SHA256
                ),
                "subject": "english",
                "mode": (
                    "same_queue_review_reclassification_zero_external_calls"
                ),
                "source_release_id": (
                    release.ENGLISH_PRESERVED_REVIEW_REPAIR_LINEAGE[
                        "source_release_id"
                    ]
                ),
                "source_activation_id": (
                    release.ENGLISH_PRESERVED_REVIEW_REPAIR_LINEAGE[
                        "source_activation_id"
                    ]
                ),
                "target_release_id": release_id,
                "target_activation_id": activation_id,
                "target_generation": "english-next-generation",
                "target_subject_authority_fingerprint": subject_authority,
                "target_producer_authority_fingerprint": producer_authority,
                "unit_sha256": (
                    release.ENGLISH_PRESERVED_REVIEW_REPAIR_LINEAGE[
                        "unit_sha256"
                    ]
                ),
                "evidence_bindings": evidence,
                "mutable_preimages": preimages,
                "review_artifacts": artifacts,
                "batch_archive_path": absolute("batch-archive.json"),
                "preimage_capsule_path": absolute("capsule.json"),
                "review_terminal_path": absolute("review-terminal.json"),
                "source_queue_path": absolute("source-queue.json"),
                "target_queue_path": absolute("target-queue.json"),
                "terminal_index_postimage_path": absolute("index.json"),
                "source_gate_preimage_sha256": (
                    release.ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_GATE_SHA256
                ),
                "staged_gate_preimage_sha256": staged_sha,
                "gate_postimage_sha256": release.sha256_bytes(
                    release.canonical_bytes(gate)
                ),
                "queue_postimage_sha256": "5" * 64,
                "report_available": True,
                "report_disposition": "needs_sol_review",
                "sol_review_status": "pending",
                "formal_write_eligible": False,
                "source_queue_absent": True,
                "queue_identity_preserved": True,
                "created_at": "2026-08-13T01:02:03+00:00",
                "authority": {
                    "schema_version": "study-intake-dispatch-authority-v1",
                    "algorithm": "HMAC-SHA256",
                    "key_id": "6" * 64,
                    "purpose": "dispatch-english-preserved-review-repair-v1",
                    "hmac_sha256": "7" * 64,
                },
                **{
                    key: 0
                    for key in (
                        "new_task_count",
                        "new_queue_count",
                        "capture_replay_count",
                        "model_call_count",
                        "provider_request_count",
                        "mcp_tool_call_count",
                        "sol_call_count",
                        "formal_write_count",
                    )
                },
            }
        )
        receipt_sha = release.sha256_bytes(
            release.canonical_bytes(receipt)
        )
        repair_path = absolute("repair.json")
        apply_result = {
            "schema_version": (
                "study-intake-english-preserved-review-repair-result-v1"
            ),
            "status": "applied",
            "repair_receipt": receipt,
            "repair_receipt_sha256": receipt_sha,
            "repair_receipt_path": repair_path,
            "report_sha256": artifacts["report_json_sha256"],
            "source_queue_absent": True,
            "target_queue_path": receipt["target_queue_path"],
            "target_queue_sha256": receipt["queue_postimage_sha256"],
            "queue_identity_preserved": True,
            "gate": gate,
            "reopen_status": "reopened",
            **{
                key: 0
                for key in (
                    "new_task_count",
                    "new_queue_count",
                    "capture_replay_count",
                    "model_call_count",
                    "provider_request_count",
                    "mcp_tool_call_count",
                    "sol_call_count",
                    "formal_write_count",
                )
            },
        }
        reopen = {
            key: apply_result[key]
            for key in (
                "repair_receipt",
                "repair_receipt_sha256",
                "repair_receipt_path",
                "report_sha256",
                "source_queue_absent",
                "target_queue_path",
                "target_queue_sha256",
                "queue_identity_preserved",
                "new_task_count",
                "new_queue_count",
                "capture_replay_count",
                "model_call_count",
                "provider_request_count",
                "mcp_tool_call_count",
                "sol_call_count",
                "formal_write_count",
            )
        }
        reopen.update(
            {
                "schema_version": (
                    "study-intake-english-preserved-review-repair-reopen-v1"
                ),
                "status": "reopened",
                "report_available": True,
                "sol_review_status": "pending",
                "formal_write_eligible": False,
                "restricted_sol_readable": True,
            }
        )
        rollback_receipt = {
            key: "8" * 64
            for key in release.ENGLISH_PRESERVED_REVIEW_REPAIR_ROLLBACK_RECEIPT_KEYS
        }
        rollback_receipt.update(
            {
                "schema_version": (
                    "study-intake-english-preserved-review-repair-rollback-receipt-v1"
                ),
                "repair_receipt_sha256": receipt_sha,
                "subject": "english",
                "status": "rolled_back",
                "source_queue_restored_sha256": preimages[
                    "queue_preimage_sha256"
                ],
                "target_queue_absent": True,
                "historical_evidence_retained": True,
                "rolled_back_at": "2026-08-13T01:03:00+00:00",
                "authority": {"fixture": True},
                **{
                    key: 0
                    for key in (
                        "model_call_count",
                        "provider_request_count",
                        "mcp_tool_call_count",
                        "sol_call_count",
                        "formal_write_count",
                    )
                },
            }
        )
        rollback_sha = release.sha256_bytes(
            release.canonical_bytes(rollback_receipt)
        )
        rollback = {
            "schema_version": (
                "study-intake-english-preserved-review-repair-rollback-result-v1"
            ),
            "status": "rolled_back",
            "rollback_receipt": rollback_receipt,
            "rollback_receipt_sha256": rollback_sha,
            "rollback_receipt_path": absolute("rollback.json"),
            "batch_rollback_receipt_sha256": "9" * 64,
            "batch_rollback_receipt_path": absolute("batch-rollback.json"),
            "source_queue_restored_sha256": preimages[
                "queue_preimage_sha256"
            ],
            "staged_gate_restored_sha256": "a" * 64,
            "target_queue_absent": True,
            "batch_restored_sha256": "b" * 64,
            "batch_pointer_restored_sha256": "c" * 64,
            "writer_restored_sha256": "d" * 64,
            "formal_write_count": 0,
        }
        for key in (
            "staged_gate_restored_sha256",
            "batch_restored_sha256",
            "batch_pointer_restored_sha256",
            "writer_restored_sha256",
        ):
            rollback_receipt[key] = rollback[key]
        rollback_sha = release.sha256_bytes(
            release.canonical_bytes(rollback_receipt)
        )
        rollback["rollback_receipt_sha256"] = rollback_sha
        rollback["rollback_receipt"] = rollback_receipt
        reopen_rollback = {
            "schema_version": (
                "study-intake-english-preserved-review-repair-rollback-reopen-v1"
            ),
            "status": "reopened",
            "repair_receipt_sha256": receipt_sha,
            "rollback_receipt": rollback_receipt,
            "rollback_receipt_sha256": rollback_sha,
            "rollback_receipt_path": rollback["rollback_receipt_path"],
            "source_queue_restored_sha256": rollback[
                "source_queue_restored_sha256"
            ],
            "target_queue_absent": True,
            "batch_restored_sha256": rollback["batch_restored_sha256"],
            "batch_pointer_restored_sha256": rollback[
                "batch_pointer_restored_sha256"
            ],
            "writer_restored_sha256": rollback[
                "writer_restored_sha256"
            ],
            "batch_rollback_status": "reopened",
            "formal_write_count": 0,
        }
        responses = iter((apply_result, reopen, rollback, reopen_rollback))
        commands: list[list[str]] = []

        def runner(command):
            commands.append(list(command))
            return {"returncode": 0, "stdout": json.dumps(next(responses))}

        checked = release._english_preserved_review_repair_apply(
            target=self.source,
            release_id=release_id,
            target_activation_id=activation_id,
            target_generation="english-next-generation",
            target_subject_authority_fingerprint=subject_authority,
            target_producer_authority_fingerprint=producer_authority,
            staged_target_canary_state_sha256=staged_sha,
            runner=runner,
        )
        checked_reopen = release._validate_english_preserved_review_repair_reopen(
            release._reopen_english_preserved_review_repair(
                target=self.source,
                runner=runner,
                repair_receipt_sha256=receipt_sha,
            ),
            repair=checked,
        )
        checked_rollback = release._rollback_english_preserved_review_repair(
            target=self.source, repair=checked, runner=runner
        )
        release._reopen_english_preserved_review_repair_rollback(
            target=self.source,
            repair=checked,
            rollback=checked_rollback,
            runner=runner,
        )
        self.assertEqual(checked_reopen["restricted_sol_readable"], True)
        tails = []
        for command, subcommand in zip(
            commands,
            (
                "english-preserved-review-repair-apply",
                "english-preserved-review-repair-reopen",
                "english-preserved-review-repair-rollback",
                "english-preserved-review-repair-reopen-rollback",
            ),
        ):
            tails.append(command[command.index(subcommand) :])
        self.assertEqual(
            tails[0],
            [
                "english-preserved-review-repair-apply",
                "--target-release-id",
                release_id,
                "--target-activation-id",
                activation_id,
                "--target-generation",
                "english-next-generation",
                "--target-subject-authority-fingerprint",
                subject_authority,
                "--target-producer-authority-fingerprint",
                producer_authority,
                "--staged-target-canary-state-sha256",
                staged_sha,
            ],
        )
        self.assertEqual(
            tails[1],
            [
                "english-preserved-review-repair-reopen",
                "--repair-receipt-sha256",
                receipt_sha,
            ],
        )
        self.assertEqual(tails[2][0], "english-preserved-review-repair-rollback")
        self.assertEqual(
            tails[3][-2:],
            ["--batch-rollback-receipt-sha256", "9" * 64],
        )
        double_newline_receipt = copy.deepcopy(apply_result)
        double_newline_receipt["repair_receipt_sha256"] = (
            release.sha256_bytes(release.canonical_bytes(receipt) + b"\n")
        )
        with self.assertRaisesRegex(
            release.ReleaseError,
            "english_preserved_review_repair_result_invalid",
        ):
            release._validate_english_preserved_review_repair_result(
                double_newline_receipt,
                release_id=release_id,
                activation_id=activation_id,
                target_generation="english-next-generation",
                target_subject_authority_fingerprint=subject_authority,
                target_producer_authority_fingerprint=producer_authority,
                staged_target_canary_state_sha256=staged_sha,
            )
        double_newline_rollback = copy.deepcopy(rollback)
        double_newline_rollback["rollback_receipt_sha256"] = (
            release.sha256_bytes(
                release.canonical_bytes(rollback_receipt) + b"\n"
            )
        )
        with self.assertRaisesRegex(
            release.ReleaseError,
            "english_preserved_review_repair_rollback_invalid",
        ):
            release._rollback_english_preserved_review_repair(
                target=self.source,
                repair=checked,
                runner=lambda _command: {
                    "returncode": 0,
                    "stdout": json.dumps(double_newline_rollback),
                },
            )
        malformed = dict(apply_result)
        malformed["unexpected"] = True
        with self.assertRaisesRegex(
            release.ReleaseError,
            "english_preserved_review_repair_result_invalid",
        ):
            release._validate_english_preserved_review_repair_result(
                malformed,
                release_id=release_id,
                activation_id=activation_id,
                target_generation="english-next-generation",
                target_subject_authority_fingerprint=subject_authority,
                target_producer_authority_fingerprint=producer_authority,
                staged_target_canary_state_sha256=staged_sha,
            )

    def test_generic_canary_gate_excludes_needs_sol_review_from_claim_queue(
        self,
    ) -> None:
        for module_root in (ROOT / "lib", ROOT / "bin"):
            if str(module_root) not in sys.path:
                sys.path.insert(0, str(module_root))
        import concurrent_dispatch as dispatch

        release_id = "a" * 64
        producer = self.producer_authority(
            subject="english", release_id=release_id, index=1
        )
        store = dispatch.LeaseStore(self.base / "review-gate-runtime")
        store.begin_subject_drain("english")
        gate = store.activate_production_canary(
            "english",
            release_id=release_id,
            producer_authority=producer,
            activated_at="2026-08-13T01:02:03+00:00",
            continuous_concurrency_limit=20,
        )
        gate = copy.deepcopy(gate)
        gate.update(
            {
                "last_analysis_status": "completed",
                "last_critical_review_status": "not_started",
                "last_report_status": "reopen_verified",
                "next_action": "await_post_activation_capture",
            }
        )
        slot = {
            "subject": "english",
            "producer_authority_fingerprint": producer[
                "authority_fingerprint"
            ],
            "producer_high_watermark": gate["producer_high_watermark"],
            "producer_high_watermark_sha256": gate[
                "producer_high_watermark_sha256"
            ],
            "state": "armed",
            "capture_id": None,
            "completion_receipt_sha256": None,
        }
        self.assertEqual(gate["canary_queue_count"], 0)
        self.assertEqual(
            gate["queue_classification"],
            {
                "pre_activation_frozen": 0,
                "pending": 0,
                "claimed": 0,
                "succeeded": 0,
                "failed": 0,
            },
        )
        release._validate_runtime_canary_gate(
            gate,
            subject="english",
            release_id=release_id,
            slot=slot,
        )
        malformed = copy.deepcopy(gate)
        malformed["canary_queue_count"] = 1
        with self.assertRaisesRegex(
            release.ReleaseError,
            "three_subject_canary_gate_invalid:english",
        ):
            release._validate_runtime_canary_gate(
                malformed,
                subject="english",
                release_id=release_id,
                slot=slot,
            )

    def test_english_review_repair_pre_service_restore_only_is_exact(
        self,
    ) -> None:
        release_id = "a" * 64
        activation_id = "b" * 64
        gate_path = (
            self.runtime_data
            / "dispatch"
            / "state"
            / "production-canary"
            / "english.json"
        )
        gate_path.parent.mkdir(parents=True, exist_ok=True)
        gate = {
            "schema_version": "study-intake-production-canary-state-v3",
            "subject": "english",
            "state": "paused_drained",
            "release_id": release_id,
            "activation_id": activation_id,
            "luna_consumer_enabled": False,
            "active_task_count": 0,
            "selected": None,
            "active_selections": {},
            "formal_write_count": 0,
        }
        gate_path.write_bytes(release.canonical_bytes(gate))
        staged = {
            "schema_version": (
                "study-intake-english-preserved-review-repair-"
                "target-gate-stage-v1"
            ),
            "subject": "english",
            "status": "staged_paused_drained",
            "target_release_id": release_id,
            "target_activation_id": activation_id,
            "staged_target_canary_state_sha256": release.sha256_file(
                gate_path
            ),
        }
        uncertain = {
            "schema_version": (
                "study-intake-english-preserved-review-repair-"
                "uncertain-reopen-v1"
            ),
            "status": "not_prepared_no_mutation",
            "repair_receipt_sha256": None,
            "repair_receipt_path": None,
            "mutation_observed": False,
            "rollback_complete": True,
            "formal_write_count": 0,
        }

        def snapshot_with(*, dispatcher=None, dashboard=None):
            return {
                "schema_version": release.PROCESS_SNAPSHOT_SCHEMA,
                "observed_at": "2026-08-13T01:02:03+00:00",
                "legacy_worker_pids": [],
                "dispatcher_pids": list(dispatcher or []),
                "dashboard_pids": list(dashboard or []),
                "luna_pids": [],
                "process_set_sha256": "c" * 64,
            }

        checked = release._validate_exact_english_pre_service_restore_only(
            uncertain=uncertain,
            staged_gate=staged,
            release_id=release_id,
            runtime_data_root=self.runtime_data,
            target=self.source,
            active_link=self.base / "current",
            process_inspector=lambda *_args: snapshot_with(),
        )
        self.assertEqual(checked, uncertain)

        drifted = {**staged, "staged_target_canary_state_sha256": "d" * 64}
        with self.assertRaisesRegex(
            release.ReleaseError,
            "english_preserved_review_repair_pre_service_gate_invalid",
        ):
            release._validate_exact_english_pre_service_restore_only(
                uncertain=uncertain,
                staged_gate=drifted,
                release_id=release_id,
                runtime_data_root=self.runtime_data,
                target=self.source,
                active_link=self.base / "current",
                process_inspector=lambda *_args: snapshot_with(),
            )

        with self.assertRaisesRegex(
            release.ReleaseError,
            "english_preserved_review_repair_pre_service_owner_present",
        ):
            release._validate_exact_english_pre_service_restore_only(
                uncertain=uncertain,
                staged_gate=staged,
                release_id=release_id,
                runtime_data_root=self.runtime_data,
                target=self.source,
                active_link=self.base / "current",
                process_inspector=lambda *_args: snapshot_with(
                    dispatcher=[123]
                ),
            )

        receipt_present = {
            **uncertain,
            "repair_receipt_sha256": "e" * 64,
        }
        with self.assertRaisesRegex(
            release.ReleaseError,
            "english_preserved_review_repair_uncertain_reopen_invalid",
        ):
            release._validate_exact_english_pre_service_restore_only(
                uncertain=receipt_present,
                staged_gate=staged,
                release_id=release_id,
                runtime_data_root=self.runtime_data,
                target=self.source,
                active_link=self.base / "current",
                process_inspector=lambda *_args: snapshot_with(),
            )

    def test_english_review_repair_pre_service_failure_restores_snapshots_only(
        self,
    ) -> None:
        previous = self.build(passed=True)
        previous_id = str(previous["release_id"])
        previous_target = Path(str(previous["release_dir"]))
        active = self.base / "review-repair-pre-service-current"
        active.symlink_to(previous_target)
        self.install_current_plists(previous_target, active)
        (self.source / "bin" / "worker.py").write_text(
            "print('review correction pre-service target')\n",
            encoding="utf-8",
        )
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        manifest = self.make_canary_manifest(built)
        drain_receipt = self.make_drain_receipt(
            previous_id, release_id, active
        )
        state_root = (
            self.runtime_data
            / "dispatch"
            / "state"
            / "production-canary"
        )
        state_root.mkdir(parents=True, exist_ok=True)
        source_state = b'{"fixture":"review-source"}\n'
        source_gate_sha = hashlib.sha256(source_state).hexdigest()
        state_paths = {
            subject: state_root / f"{subject}.json"
            for subject in release.RESUMABLE_SUBJECTS
        }
        for path in state_paths.values():
            path.write_bytes(source_state)

        def preflight(**values) -> dict:
            proof = self.canary_preflight_proof(**values)
            for row in proof["subjects"]:
                review_planned = (
                    row["subject"] == "english" and not values["apply"]
                )
                row.update(
                    {
                        "subject_batch_readiness": (
                            "recoverable_review_correction"
                            if review_planned
                            else "ready"
                        ),
                        "subject_batch_readiness_sha256": "4" * 64,
                        "subject_recovery_required": False,
                        "subject_recovery_already_applied": False,
                        "subject_recovery_original_preclaim_failure_receipt_sha256": None,
                        "subject_recovery_receipt_sha256": None,
                        "subject_recovery_receipt_path": None,
                        "subject_recovery_rollback_token": None,
                        "subject_readiness_mcp_tool_call_count": 1,
                        "subject_authority_generation": (
                            f"{row['subject']}-generation"
                        ),
                        "subject_authority_fingerprint": "5" * 64,
                    }
                )
                if review_planned:
                    row.update(
                        {
                            "english_preserved_review_repair_required": True,
                            "english_preserved_review_repair_descriptor_sha256": (
                                release.ENGLISH_PRESERVED_REVIEW_REPAIR_DESCRIPTOR_SHA256
                            ),
                            "english_preserved_review_repair_preview_sha256": (
                                "6" * 64
                            ),
                            "english_preserved_review_repair_source_gate_preimage_sha256": (
                                source_gate_sha
                            ),
                        }
                    )
            return proof

        preview = {
            "preview_sha256": "6" * 64,
            "source_gate_preimage_sha256": source_gate_sha,
        }
        staged_gate = {
            "schema_version": (
                "study-intake-english-preserved-review-repair-"
                "target-gate-stage-v1"
            ),
            "subject": "english",
            "status": "staged_paused_drained",
            "target_release_id": release_id,
            "target_activation_id": "7" * 64,
            "target_generation": "english-generation",
            "target_subject_authority_fingerprint": "5" * 64,
            "target_producer_authority_fingerprint": "8" * 64,
            "luna_consumer_enabled": False,
            "active_task_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "formal_write_count": 0,
        }

        def stage_target_gate(**_values):
            gate = {
                "schema_version": (
                    "study-intake-production-canary-state-v3"
                ),
                "subject": "english",
                "state": "paused_drained",
                "release_id": release_id,
                "activation_id": staged_gate["target_activation_id"],
                "luna_consumer_enabled": False,
                "active_task_count": 0,
                "selected": None,
                "active_selections": {},
                "formal_write_count": 0,
            }
            state_paths["english"].write_bytes(
                release.canonical_bytes(gate)
            )
            staged_gate["staged_target_canary_state_sha256"] = (
                release.sha256_file(state_paths["english"])
            )
            return copy.deepcopy(staged_gate)

        uncertain = {
            "schema_version": (
                "study-intake-english-preserved-review-repair-"
                "uncertain-reopen-v1"
            ),
            "status": "not_prepared_no_mutation",
            "repair_receipt_sha256": None,
            "repair_receipt_path": None,
            "mutation_observed": False,
            "rollback_complete": True,
            "formal_write_count": 0,
        }

        def empty_process_snapshot(*_args, **_kwargs):
            return {
                "schema_version": release.PROCESS_SNAPSHOT_SCHEMA,
                "observed_at": "2026-08-13T01:02:03+00:00",
                "legacy_worker_pids": [],
                "dispatcher_pids": [],
                "dashboard_pids": [],
                "luna_pids": [],
                "process_set_sha256": "c" * 64,
            }

        generic_shutdown = mock.Mock(
            side_effect=AssertionError("generic shutdown must be skipped")
        )
        generic_canary_rollback = mock.Mock(
            side_effect=AssertionError("generic canary rollback must be skipped")
        )
        service_activations: list[list[Mapping[str, Any]]] = []

        with (
            mock.patch.dict(
                release.ENGLISH_PRESERVED_REVIEW_REPAIR_LINEAGE,
                {"source_release_id": previous_id},
            ),
            mock.patch.object(
                release,
                "ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_GATE_SHA256",
                source_gate_sha,
            ),
            mock.patch.object(
                release,
                "_preview_english_preserved_review_repair",
                return_value=preview,
            ),
            mock.patch.object(
                release,
                "_stage_english_preserved_review_repair_target_gate",
                side_effect=stage_target_gate,
            ),
            mock.patch.object(
                release,
                "_english_preserved_review_repair_apply",
                side_effect=release.ReleaseError(
                    "injected_pre_service_review_failure"
                ),
            ),
            mock.patch.object(
                release,
                "_reopen_english_preserved_review_repair",
                return_value=uncertain,
            ),
            mock.patch.object(
                release,
                "_deactivate_previous_canary_states",
                return_value={"status": "deactivated"},
            ),
            mock.patch.object(
                release,
                "_default_canary_shutdown_before_activation_rollback",
                generic_shutdown,
            ),
            mock.patch.object(
                release, "_run_service_deactivation", return_value=None
            ),
            mock.patch.object(
                release,
                "_run_service_activation",
                side_effect=lambda topology, *_args, **_kwargs: (
                    service_activations.append(list(topology))
                ),
            ),
        ):
            with self.assertRaisesRegex(
                release.ReleaseError,
                "activation_service_failed_rolled_back",
            ):
                release.activate_canary_release(
                    release_base=self.release_base,
                    release_id=release_id,
                    active_link=active,
                    canary_manifest=manifest,
                    apply=True,
                    expected_current=previous_id,
                    drain_receipt=drain_receipt,
                    launchagent_dir=self.launchagents,
                    command_runner=self.FakeRunner(),
                    process_inspector=empty_process_snapshot,
                    canary_pre_activation_verifier=preflight,
                    canary_rollback_controller=generic_canary_rollback,
                    english_preserved_review_repair_descriptor_sha256=(
                        release.ENGLISH_PRESERVED_REVIEW_REPAIR_DESCRIPTOR_SHA256
                    ),
                )

        self.assertEqual(generic_shutdown.call_count, 0)
        self.assertEqual(generic_canary_rollback.call_count, 0)
        self.assertEqual(active.resolve(), previous_target)
        self.assertEqual(len(service_activations), 1)
        self.assertEqual(
            {path.read_bytes() for path in state_paths.values()},
            {source_state},
        )
        postcommit = next(
            value
            for value in (
                json.loads(path.read_text(encoding="utf-8"))
                for path in (self.release_base / "deployments").glob("*.json")
            )
            if value.get("schema_version")
            == release.DEPLOYMENT_POSTCOMMIT_SCHEMA
            and value.get("release_id") == release_id
            and value.get("status") == "rolled_back"
        )
        self.assertIsNone(postcommit["rollback_drain_proof"])
        self.assertIsNone(postcommit["canary_rollback_proof"])
        self.assertEqual(
            postcommit["english_preserved_review_repair_reopen_proof"],
            uncertain,
        )

    def test_english_review_repair_rollback_failure_keeps_target_fenced(
        self,
    ) -> None:
        previous = self.build(passed=True)
        previous_id = str(previous["release_id"])
        previous_target = Path(str(previous["release_dir"]))
        active = self.base / "review-repair-rollback-failure-current"
        active.symlink_to(previous_target)
        self.install_current_plists(previous_target, active)
        (self.source / "bin" / "worker.py").write_text(
            "print('review correction target')\n", encoding="utf-8"
        )
        built = self.build(passed=True)
        release_id = str(built["release_id"])
        target = Path(str(built["release_dir"]))
        manifest = self.make_canary_manifest(built)
        drain_receipt = self.make_drain_receipt(
            previous_id, release_id, active
        )
        state_root = (
            self.runtime_data
            / "dispatch"
            / "state"
            / "production-canary"
        )
        state_root.mkdir(parents=True, exist_ok=True)
        source_state = b'{"fixture":"review-source"}\n'
        target_state = b'{"fixture":"review-target-fenced"}\n'
        source_gate_sha = hashlib.sha256(source_state).hexdigest()
        state_paths = {
            subject: state_root / f"{subject}.json"
            for subject in release.RESUMABLE_SUBJECTS
        }
        for path in state_paths.values():
            path.write_bytes(source_state)

        def preflight(**values) -> dict:
            proof = self.canary_preflight_proof(**values)
            for row in proof["subjects"]:
                review_planned = (
                    row["subject"] == "english" and not values["apply"]
                )
                row.update(
                    {
                        "subject_batch_readiness": (
                            "recoverable_review_correction"
                            if review_planned
                            else "ready"
                        ),
                        "subject_batch_readiness_sha256": "4" * 64,
                        "subject_recovery_required": False,
                        "subject_recovery_already_applied": False,
                        "subject_recovery_original_preclaim_failure_receipt_sha256": None,
                        "subject_recovery_receipt_sha256": None,
                        "subject_recovery_receipt_path": None,
                        "subject_recovery_rollback_token": None,
                        "subject_readiness_mcp_tool_call_count": 1,
                        "subject_authority_generation": (
                            f"{row['subject']}-generation"
                        ),
                        "subject_authority_fingerprint": "5" * 64,
                    }
                )
                if review_planned:
                    row.update(
                        {
                            "english_preserved_review_repair_required": True,
                            "english_preserved_review_repair_descriptor_sha256": (
                                release.ENGLISH_PRESERVED_REVIEW_REPAIR_DESCRIPTOR_SHA256
                            ),
                            "english_preserved_review_repair_preview_sha256": (
                                "6" * 64
                            ),
                            "english_preserved_review_repair_source_gate_preimage_sha256": (
                                source_gate_sha
                            ),
                        }
                    )
            return proof

        preview = {
            "preview_sha256": "6" * 64,
            "source_gate_preimage_sha256": source_gate_sha,
        }
        staged = {
            "target_release_id": release_id,
            "target_activation_id": "7" * 64,
            "target_generation": "english-generation",
            "target_subject_authority_fingerprint": "5" * 64,
            "target_producer_authority_fingerprint": "8" * 64,
            "staged_target_canary_state_sha256": "9" * 64,
        }
        repair = {
            "repair_receipt_sha256": "a" * 64,
            "repair_receipt": {"batch_archive_sha256": "b" * 64},
        }
        reopened = {"status": "reopened"}
        service_activations: list[list[Mapping[str, Any]]] = []

        def record_service_activation(topology, *_args, **_kwargs):
            service_activations.append(list(topology))

        def fail_health(**_values):
            for path in state_paths.values():
                path.write_bytes(target_state)
            raise release.ReleaseError("injected_review_post_activation_failure")

        def empty_process_snapshot(*_args, **_kwargs):
            return {
                "schema_version": release.PROCESS_SNAPSHOT_SCHEMA,
                "observed_at": dt.datetime.now(
                    dt.timezone.utc
                ).isoformat(),
                "legacy_worker_pids": [],
                "dispatcher_pids": [],
                "dashboard_pids": [],
                "luna_pids": [],
                "process_set_sha256": "c" * 64,
            }

        drained = {
            "schema_version": "study-intake-rollback-drain-v1",
            "status": "drained",
            "release_id": release_id,
            "runtime_data_root": str(self.runtime_data.resolve()),
            "subjects": [
                {
                    "subject": subject,
                    "active_count": 0,
                    "claimed_total": 0,
                    "stale_count": 0,
                    "status_sha256": f"{index}" * 64,
                }
                for index, subject in enumerate(
                    release.RESUMABLE_SUBJECTS, start=1
                )
            ],
            "formal_write_count": 0,
        }

        def bind(proof, **_values):
            return {
                **copy.deepcopy(dict(proof)),
                "english_preserved_review_repair_binding": {
                    "fixture": "bound"
                },
            }

        generic_shutdown = mock.Mock(return_value=drained)

        with (
            mock.patch.dict(
                release.ENGLISH_PRESERVED_REVIEW_REPAIR_LINEAGE,
                {"source_release_id": previous_id},
            ),
            mock.patch.object(
                release,
                "ENGLISH_PRESERVED_REVIEW_REPAIR_SOURCE_GATE_SHA256",
                source_gate_sha,
            ),
            mock.patch.object(
                release,
                "_preview_english_preserved_review_repair",
                return_value=preview,
            ),
            mock.patch.object(
                release,
                "_stage_english_preserved_review_repair_target_gate",
                return_value=staged,
            ),
            mock.patch.object(
                release,
                "_english_preserved_review_repair_apply",
                return_value=repair,
            ),
            mock.patch.object(
                release,
                "_reopen_english_preserved_review_repair",
                return_value=reopened,
            ),
            mock.patch.object(
                release,
                "_validate_english_preserved_review_repair_reopen",
                return_value=reopened,
            ),
            mock.patch.object(
                release,
                "_bind_english_preserved_review_repair_to_canary_proof",
                side_effect=bind,
            ),
            mock.patch.object(
                release,
                "_rollback_english_preserved_review_repair",
                side_effect=release.ReleaseError(
                    "english_preserved_review_repair_rollback_failed"
                ),
            ),
            mock.patch.object(
                release,
                "_deactivate_previous_canary_states",
                return_value={"status": "deactivated"},
            ),
            mock.patch.object(
                release,
                "_default_canary_shutdown_before_activation_rollback",
                generic_shutdown,
            ),
            mock.patch.object(
                release, "_run_service_deactivation", return_value=None
            ),
            mock.patch.object(
                release,
                "_run_service_activation",
                side_effect=record_service_activation,
            ),
        ):
            with self.assertRaisesRegex(
                release.ReleaseError,
                "activation_failed_rollback_incomplete",
            ):
                release.activate_canary_release(
                    release_base=self.release_base,
                    release_id=release_id,
                    active_link=active,
                    canary_manifest=manifest,
                    apply=True,
                    expected_current=previous_id,
                    drain_receipt=drain_receipt,
                    launchagent_dir=self.launchagents,
                    command_runner=self.FakeRunner(),
                    process_inspector=empty_process_snapshot,
                    canary_pre_activation_verifier=preflight,
                    post_activation_verifier=fail_health,
                    canary_rollback_controller=self.canary_rollback_proof,
                    english_preserved_review_repair_descriptor_sha256=(
                        release.ENGLISH_PRESERVED_REVIEW_REPAIR_DESCRIPTOR_SHA256
                    ),
                )

        self.assertEqual(active.resolve(), target)
        self.assertEqual(len(service_activations), 1)
        self.assertEqual(
            {path.read_bytes() for path in state_paths.values()},
            {target_state},
        )
        receipts = [
            json.loads(path.read_text(encoding="utf-8"))
            for path in (self.release_base / "deployments").glob("*.json")
        ]
        incomplete = next(
            value
            for value in receipts
            if value.get("schema_version")
            == release.DEPLOYMENT_POSTCOMMIT_SCHEMA
            and value.get("status") == "rollback_incomplete"
            and value.get("release_id") == release_id
            and value.get("english_preserved_review_repair_error_code")
        )
        self.assertTrue(incomplete["target_fenced"])
        self.assertFalse(incomplete["old_services_restarted"])
        self.assertIsNone(incomplete["restored_release_id"])
        self.assertEqual(generic_shutdown.call_count, 1)


if __name__ == "__main__":
    unittest.main()
