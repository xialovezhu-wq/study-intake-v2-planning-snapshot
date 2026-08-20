from __future__ import annotations

import copy
import base64
import hashlib
import importlib.util
import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "dashboard"))

import dashboard_projection as dashboard_projection  # noqa: E402
import server as dashboard_server  # noqa: E402
from core_dispatch_bridge import producer_dispatch_input_contract  # noqa: E402
from concurrent_dispatch import (  # noqa: E402
    FrozenTask,
    LeaseStore,
    dispatch_rule_binding,
)
from preprocessor_core import MathAdapter, PreprocessorError, sha256_value  # noqa: E402
from preprocessor_core import CodexRunner  # noqa: E402
from process_identity import kernel_process_start_token  # noqa: E402
from subject_sol_contract import SubjectSolRuntimeStore  # noqa: E402


JSONSCHEMA_PYTHON = Path("/opt/miniconda3/envs/dl/bin/python")
PLUGIN_ROOT = ROOT / "plugin/kaoyan-study-intake"
BASELINE_RELEASE_ROOT = Path(
    "/Users/xiazhibin/.codex/study-intake-preprocessor/releases/"
    "a4ff96b8932344211ca51c69edda98a06e95382bcdc4de79e2520fdcf8e343d6"
)
SUCCESSOR_PLUGIN_VERSION = (
    "0.6.0+codex.20260818-prelive-finalization"
)
SUCCESSOR_MCP_RELEASE_ID = (
    "21d738a1d74586aab72c8a63dc62c680aa10c6ac837c2bd5041e757ba0e63425"
)
SUCCESSOR_MCP_MANIFEST_SHA256 = (
    "3753175de6a66b5cd67efe6b85622a6e8bdb519bee5166834677ee0bad08a5e5"
)
SUCCESSOR_MCP_LAUNCHER_SHA256 = (
    "d0bb1103adab1d606491100f528788cd111deb199db7e63a55109a318656b796"
)
HISTORICAL_SCHEMA_SHA256 = {
    "mcp-authority-snapshot-v1.json": (
        "623564ff69a8f62157aaf74e0bf316923f4840fc9e23bc7f870d56d8004b0762"
    ),
    "mcp-authority-snapshot-receipt-v1.json": (
        "829c51cfd67c957ee8e0abbd55110cca51e0695eb4fd717824b846055b7b9077"
    ),
    "mcp-read-session-v3.json": (
        "0e335fc450f28490b8df179a4a7542688224cbfe6d9bd6dfe531530c5e5d28a8"
    ),
    "three-subject-canary-activation-receipt-v2.json": (
        "58be4202884b1b0cd54609ae8c6b2a3e0c496212c833ffd3496975d482a24b83"
    ),
    "three-subject-canary-activation-receipt-v3.json": (
        "bdf101230906ad2f23909f6ededb605415e7bed361479ec546064cd26e257733"
    ),
    "three-subject-canary-activation-receipt-v4.json": (
        "77c99095729ec9e1a984f410782917bd3c5cfce5037ed5ba4f4084735e1a8003"
    ),
    "dashboard-projection-v4.json": (
        "6e39c1bbbb1bd72998861d252cd40eefe725f7c96d620ddda8d12f2f18ef3c79"
    ),
}
SUCCESSOR_ROOT_SCHEMA_NAMES = frozenset(
    {
        "concurrent-completion-v2.json",
        "cs408-terminal-batch-writer-retirement-pointer-v1.json",
        "cs408-terminal-batch-writer-retirement-receipt-v1.json",
        "cs408-terminal-batch-writer-retirement-rollback-receipt-v1.json",
        "daily-sol-batch-v3.json",
        "dashboard-projection-v4.json",
        "dashboard-projection-v5.json",
        "dashboard-task-detail-v2.json",
        "dispatch-report-v2.json",
        "dispatch-task-detail-v2.json",
        "dispatch-task-event-v2.json",
        "english-quick-flush-intent-v1.json",
        "english-preserved-review-repair-receipt-v1.json",
        "english-preserved-review-repair-rollback-receipt-v1.json",
        "math-capture-evidence-contract-v1.json",
        "math-exact-smoke-capture-apply-receipt-v1.json",
        "math-exact-smoke-execution-authorization-receipt-v1.json",
        "math-exact-smoke-execution-authorization-v1.json",
        "math-exact-smoke-task-binding-v1.json",
        "math-exact-smoke-task-binding-v2.json",
        "mcp-authority-snapshot-receipt-v2.json",
        "mcp-authority-snapshot-v2.json",
        "mcp-read-session-v4.json",
        "model-stage-execution-receipt-v1.json",
        "model-stage-normalization-receipt-v1.json",
        "model-stage-raw-chain-manifest-v1.json",
        "model-stage-raw-chunk-v1.json",
        "model-stage-raw-output-v1.json",
        "preprocess-package-v3.json",
        "producer-dispatch-input-v2.json",
        "production-canary-state-v3.json",
        "production-canary-terminal-index-v3.json",
        "production-canary-terminal-receipt-v3.json",
        "production-canary-queue-entry-v3.json",
        "production-canary-review-terminal-receipt-v1.json",
        "provider-kernel-probe-receipt-v1.json",
        "review-candidate-package-v1.json",
        "review-candidate-terminal-v1.json",
        "sol-commit-receipt-v2.json",
        "sol-review-receipt-v2.json",
        "sol-task-handoff-envelope-v1.json",
        "stage-progress-receipt-v1.json",
        "subject-background-luna-recovery-rollback-receipt-v1.json",
        "subject-background-luna-rollover-receipt-v2.json",
        "subject-batch-recovery-supersede-receipt-v1.json",
        "subject-luna-batch-v2.json",
        "subject-quality-receipt-v2.json",
        "three-subject-canary-activation-receipt-v3.json",
        "three-subject-canary-activation-receipt-v4.json",
        "three-subject-canary-activation-receipt-v5.json",
        "user-sol-authorization-receipt-v2.json",
        "dashboard-multi-agent-v1.json",
        "live-execution-gate-state-v1.json",
        "manual-live-authorization-v1.json",
        "multi-agent-event-chain-receipt-v1.json",
        "multi-agent-model-contract-v1.json",
        "multi-agent-stage-execution-receipt-v1.json",
        "multi-agent-task-receipt-v1.json",
        "orchestration-read-plan-v1.json",
        "processing-binding-v3.json",
        "producer-binding-attestation-v1.json",
        "producer-binding-descriptor-v1.json",
        "read-branch-request-v1.json",
        "read-branch-result-v1.json",
        "read-bundle-v1.json",
        "risk-report-v1.json",
        "sol-handoff-envelope-v1.json",
        "sol-mcp-preflight-session-v1.json",
        "sol-mcp-preflight-call-receipt-v1.json",
        "sol-mcp-preflight-terminal-v1.json",
        "validation-console-state-v1.json",
        "stage-authorization-v1.json",
        "stage-terminal-relock-v1.json",
        "validation-campaign-v1.json",
        "promotion-preview-v1.json",
        "validation-console-technical-status-v1.json",
    }
)


def digest(label: str) -> str:
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def schema_results(
    cases: dict[str, tuple[str, object]],
) -> dict[str, list[dict[str, object]]]:
    """Validate a group of real builder objects with one offline process.

    The application runtime intentionally has no jsonschema dependency.  The
    release verifier's pinned validation interpreter is therefore used here,
    with a local registry containing every root Schema so relative refs never
    fall through to the network.
    """

    if not JSONSCHEMA_PYTHON.is_file():
        raise AssertionError("jsonschema_validation_python_missing")
    request = {
        "schema_root": str(ROOT / "schemas"),
        "cases": {
            label: {"schema_name": schema_name, "instance": instance}
            for label, (schema_name, instance) in cases.items()
        },
    }
    script = r"""
import json
import sys
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

request = json.load(sys.stdin)
schema_root = Path(request["schema_root"])
schemas = {
    path.name: json.loads(path.read_text(encoding="utf-8"))
    for path in schema_root.glob("*.json")
}
registry = Registry()
for schema in schemas.values():
    if isinstance(schema.get("$id"), str):
        registry = registry.with_resource(
            schema["$id"], Resource.from_contents(schema)
        )
results = {}
for label, case in request["cases"].items():
    schema = schemas[case["schema_name"]]
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(
        schema,
        registry=registry,
        format_checker=FormatChecker(),
    )
    errors = sorted(
        validator.iter_errors(case["instance"]),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    results[label] = [
        {
            "path": list(error.absolute_path),
            "validator": error.validator,
            "message": error.message,
        }
        for error in errors
    ]
json.dump(results, sys.stdout, ensure_ascii=False, sort_keys=True)
"""
    completed = subprocess.run(
        [str(JSONSCHEMA_PYTHON), "-c", script],
        input=json.dumps(request, ensure_ascii=False, sort_keys=True),
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(
            completed.stderr or "successor_schema_validation_failed"
        )
    return json.loads(completed.stdout)


def assert_positive_and_negatives(
    test: unittest.TestCase,
    *,
    schema_name: str,
    value: dict[str, object],
    missing_key: str,
    wrong_type_key: str,
    pseudo_sha_path: tuple[str, ...],
    authority_purpose_path: tuple[str, ...] | None = None,
) -> None:
    def replace_at_path(
        document: object, path: tuple[str, ...], replacement: object
    ) -> None:
        cursor = document
        for part in path[:-1]:
            if isinstance(cursor, dict):
                cursor = cursor[part]
            elif isinstance(cursor, list) and part.isdigit():
                cursor = cursor[int(part)]
            else:
                raise AssertionError("schema_mutation_path_invalid")
        final = path[-1]
        if isinstance(cursor, dict):
            cursor[final] = replacement
        elif isinstance(cursor, list) and final.isdigit():
            cursor[int(final)] = replacement
        else:
            raise AssertionError("schema_mutation_path_invalid")

    cases: dict[str, tuple[str, object]] = {
        "positive": (schema_name, value),
    }
    missing = copy.deepcopy(value)
    missing.pop(missing_key)
    cases["missing"] = (schema_name, missing)
    wrong_type = copy.deepcopy(value)
    wrong_type[wrong_type_key] = {"wrong": "type"}
    cases["wrong_type"] = (schema_name, wrong_type)
    pseudo_sha = copy.deepcopy(value)
    replace_at_path(
        pseudo_sha,
        pseudo_sha_path,
        "sha256:not-a-canonical-hex-digest",
    )
    cases["pseudo_sha"] = (schema_name, pseudo_sha)
    if authority_purpose_path is not None:
        bad_purpose = copy.deepcopy(value)
        replace_at_path(
            bad_purpose,
            authority_purpose_path,
            "wrong-authority-purpose",
        )
        cases["bad_purpose"] = (schema_name, bad_purpose)
    results = schema_results(cases)
    test.assertEqual(results["positive"], [])
    for label in cases:
        if label != "positive":
            test.assertTrue(results[label], msg=f"{label} unexpectedly accepted")


class SuccessorReleaseSchemaInventoryTests(unittest.TestCase):
    def test_historical_schema_bytes_are_unchanged(self) -> None:
        for name, expected_sha256 in HISTORICAL_SCHEMA_SHA256.items():
            payload = (ROOT / "schemas" / name).read_bytes()
            self.assertEqual(
                hashlib.sha256(payload).hexdigest(),
                expected_sha256,
                name,
            )

    def test_successor_mcp_schema_versions_are_explicit(self) -> None:
        server_release = "0.4.1+sha256." + ("a" * 64)
        created_at = "2026-08-20T00:00:00+00:00"
        snapshot = {
            "schema_version": "study-read-mcp-authority-snapshot.v2",
            "subject": "math",
            "source_generation": "generation-1",
            "source_authority_fingerprint": "b" * 64,
            "mcp_server_release": server_release,
            "files": [],
            "file_set_sha256": "c" * 64,
            "file_count": 0,
            "total_bytes": 0,
            "created_at": created_at,
            "formal_write_count": 0,
        }
        snapshot_receipt = {
            "schema_version": "mcp_authority_snapshot_receipt_v2",
            "subject": "math",
            "candidate_release_id": "d" * 64,
            "mcp_server_release": server_release,
            "generation": "generation-1",
            "authority_fingerprint": "b" * 64,
            "authority_snapshot_manifest_sha256": "e" * 64,
            "file_count": 0,
            "total_bytes": 0,
            "formal_write_count": 0,
            "created_at": created_at,
            "hmac_key_id": "f" * 64,
            "hmac_sha256": "1" * 64,
        }
        read_session = {
            "schema_version": "study-read-mcp-read-session.v4",
            "read_session_id": "MCPRS-MATH-SUCCESSOR-0001",
            "subject": "math",
            "candidate_release_id": "d" * 64,
            "plugin_version": "0.6.0-test",
            "skill_id": "background-math-processing",
            "skill_version": "4.0.0",
            "mcp_server_release": server_release,
            "generation": "generation-1",
            "authority_fingerprint": "b" * 64,
            "created_at": created_at,
            "formal_write_count": 0,
            "capture_id": "CAPTURE-MATH-SUCCESSOR-0001",
            "capture_manifest_path": "/private/capture.json",
            "capture_manifest_sha256": "2" * 64,
            "artifact_ids": ["capture-facts"],
            "authority_snapshot_manifest_path": "/private/snapshot.json",
            "authority_snapshot_manifest_sha256": "e" * 64,
            "authority_snapshot_root": "/private/snapshot-root",
            "authority_snapshot_receipt_sha256": "3" * 64,
            "manifest_sha256": "4" * 64,
        }
        cases = {
            "snapshot": ("mcp-authority-snapshot-v2.json", snapshot),
            "snapshot_receipt": (
                "mcp-authority-snapshot-receipt-v2.json",
                snapshot_receipt,
            ),
            "read_session": ("mcp-read-session-v4.json", read_session),
        }
        results = schema_results(cases)
        self.assertEqual(results, {key: [] for key in cases})
        for label, (schema_name, value) in cases.items():
            legacy = copy.deepcopy(value)
            legacy["mcp_server_release"] = "0.4.0+sha256." + ("a" * 64)
            rejected = schema_results({label: (schema_name, legacy)})
            self.assertTrue(rejected[label], label)

    def test_new_root_schemas_are_exactly_mirrored_and_component_locked(self) -> None:
        root_schemas = {
            path.name: path
            for path in (ROOT / "schemas").glob("*.json")
        }
        baseline_schemas = {
            path.name: path
            for path in (BASELINE_RELEASE_ROOT / "schemas").glob("*.json")
        }
        self.assertEqual(
            set(root_schemas) - set(baseline_schemas),
            SUCCESSOR_ROOT_SCHEMA_NAMES,
        )
        self.assertEqual(
            {
                name
                for name in set(root_schemas) & set(baseline_schemas)
                if root_schemas[name].read_bytes()
                != baseline_schemas[name].read_bytes()
            },
            set(),
        )
        self.assertEqual(set(baseline_schemas) - set(root_schemas), set())

        lock = json.loads(
            (PLUGIN_ROOT / "component-lock.json").read_text(encoding="utf-8")
        )
        locked_schemas = lock["schemas"]
        generator_path = PLUGIN_ROOT / "scripts/generate_manifests.py"
        spec = importlib.util.spec_from_file_location(
            "successor_schema_generator_contract", generator_path
        )
        if spec is None or spec.loader is None:
            raise AssertionError("plugin_generator_import_failed")
        generator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(generator)
        self.assertTrue(
            SUCCESSOR_ROOT_SCHEMA_NAMES.issubset(
                generator.REQUIRED_RUNTIME_SCHEMAS
            )
        )
        for name in sorted(SUCCESSOR_ROOT_SCHEMA_NAMES):
            root_path = root_schemas[name]
            plugin_path = PLUGIN_ROOT / "schemas" / name
            self.assertEqual(plugin_path.read_bytes(), root_path.read_bytes())
            self.assertEqual(
                locked_schemas[name], hashlib.sha256(root_path.read_bytes()).hexdigest()
            )

    def test_plugin_skills_and_sealed_mcp_runtime_are_exactly_locked(self) -> None:
        components_path = PLUGIN_ROOT / "components.json"
        components = json.loads(components_path.read_text(encoding="utf-8"))
        lock = json.loads(
            (PLUGIN_ROOT / "component-lock.json").read_text(encoding="utf-8")
        )
        # component-lock.json is staging-generated.  Bind registry-derived
        # fields in this private view instead of rewriting the worktree copy.
        lock["registry_sha256"] = hashlib.sha256(
            components_path.read_bytes()
        ).hexdigest()
        lock["external_runtime_sources"] = components[
            "external_runtime_sources"
        ]
        self.assertEqual(
            lock["external_runtime_sources"],
            components["external_runtime_sources"],
        )
        self.assertEqual(components["plugin"]["version"], SUCCESSOR_PLUGIN_VERSION)
        self.assertEqual(lock["plugin_version"], SUCCESSOR_PLUGIN_VERSION)
        for relative in (
            "plugin.json",
            ".codex-plugin/plugin.json",
        ):
            manifest = json.loads(
                (PLUGIN_ROOT / relative).read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["version"], SUCCESSOR_PLUGIN_VERSION)

        expected_skill_versions = {
            "background-math-processing": "4.0.0",
            "background-cs408-processing": "4.0.0",
            "background-english-processing": "4.0.0",
            "multi-agent-read-orchestrate": "1.0.0",
        }
        for skill_name, expected_version in expected_skill_versions.items():
            skill_path = PLUGIN_ROOT / "skills" / skill_name / "SKILL.md"
            self.assertEqual(
                components["skills"][skill_name], expected_version
            )
            self.assertEqual(
                lock["skills"][skill_name]["version"], expected_version
            )
            self.assertEqual(
                lock["skills"][skill_name]["sha256"],
                hashlib.sha256(skill_path.read_bytes()).hexdigest(),
            )

        mcp = components["mcp"]
        runtime = lock["mcp_sealed_runtime"]
        release_root = Path(mcp["release_root"])
        self.assertEqual(mcp["release_id"], SUCCESSOR_MCP_RELEASE_ID)
        self.assertEqual(lock["mcp_release_id"], SUCCESSOR_MCP_RELEASE_ID)
        self.assertEqual(runtime["release_id"], SUCCESSOR_MCP_RELEASE_ID)
        self.assertEqual(
            mcp["release_manifest_sha256"], SUCCESSOR_MCP_MANIFEST_SHA256
        )
        self.assertEqual(
            lock["mcp_release_manifest_sha256"],
            SUCCESSOR_MCP_MANIFEST_SHA256,
        )
        self.assertEqual(
            runtime["release_manifest_sha256"],
            SUCCESSOR_MCP_MANIFEST_SHA256,
        )
        self.assertEqual(runtime["python_flags"], ["-I", "-S"])
        self.assertEqual(mcp["python_flags"], ["-I", "-S"])
        self.assertEqual(runtime["release_root"], str(release_root))
        self.assertEqual(lock["mcp_release_root"], str(release_root))
        manifest_path = release_root / "release.json"
        launcher_path = release_root / "scripts/sealed_launcher.py"
        self.assertEqual(
            hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
            SUCCESSOR_MCP_MANIFEST_SHA256,
        )
        self.assertEqual(
            hashlib.sha256(launcher_path.read_bytes()).hexdigest(),
            SUCCESSOR_MCP_LAUNCHER_SHA256,
        )
        self.assertEqual(
            runtime["sealed_launcher_sha256"], SUCCESSOR_MCP_LAUNCHER_SHA256
        )
        self.assertEqual(
            lock["registry_sha256"],
            hashlib.sha256(components_path.read_bytes()).hexdigest(),
        )


class EnglishQuickFlushSchemaRoundTripTests(unittest.TestCase):
    def test_quick_flush_intent_is_strict_and_authority_bound(self) -> None:
        value = {
            "schema_version": "english_quick_flush_intent_v1",
            "intent_id": digest("quick-flush-intent"),
            "event_id": "EVT-20260813-ABCDEF0123456789",
            "event_sha256": digest("quick-flush-event"),
            "event_path": "/tmp/intake/events/quick-flush-event.json",
            "capture_receipt_id": "RECEIPT-QUICK-FLUSH",
            "capture_receipt_sha256": digest("quick-flush-receipt"),
            "capture_receipt_path": (
                "/tmp/intake/receipts/capture/quick-flush-receipt.json"
            ),
            "idempotency_key": "quick-flush-schema-roundtrip",
            "request_sha256": digest("quick-flush-request"),
            "source_id": "RAW-QUICK-FLUSH-ROUNDTRIP",
            "study_date": "2026-08-13",
            "created_at": "2026-08-13T09:00:00Z",
            "formal_write_count": 0,
            "formal_writeback": "none",
            "authority": {
                "schema_version": "english_quick_flush_authority_v1",
                "algorithm": "HMAC-SHA256",
                "key_id": digest("quick-flush-key"),
                "purpose": "english-quick-flush-intent",
                "hmac_sha256": digest("quick-flush-hmac"),
            },
        }
        assert_positive_and_negatives(
            self,
            schema_name="english-quick-flush-intent-v1.json",
            value=value,
            missing_key="event_sha256",
            wrong_type_key="formal_write_count",
            pseudo_sha_path=("capture_receipt_sha256",),
            authority_purpose_path=("authority", "purpose"),
        )


class ThreeSubjectCanaryActivationReceiptSchemaTests(unittest.TestCase):
    @staticmethod
    def _common_receipt_fields() -> dict[str, object]:
        subject_hashes = {
            subject: digest(f"watermark:{subject}")
            for subject in ("math", "cs408", "english")
        }
        return {
            "status": "production_canary_active",
            "activation_id": digest("global-activation"),
            "release_id": digest("release"),
            "activated_at": "2026-08-13T09:00:00Z",
            "post_activation_only": True,
            "historical_backlog_drained": True,
            "initial_canary_inflight_limit": 1,
            "continuous_concurrency_limit": 20,
            "requested_service_tier": None,
            "fast_mode_requested": False,
            "fast_mode_effective": "not_requested",
            "producer_high_watermark_sha256s": subject_hashes,
            "slots": {
                subject: {
                    "subject": subject,
                    "producer_high_watermark_sha256": subject_hashes[subject],
                    "state": "armed",
                    "capture_id": None,
                    "completion_receipt_sha256": None,
                }
                for subject in ("math", "cs408", "english")
            },
            "canary_manifest_sha256": digest("canary-manifest"),
            "release_manifest_sha256": digest("release-manifest"),
            "pre_activation_verification_sha256": digest("preflight"),
            "post_activation_verification_sha256": digest("postflight"),
            "service_release_ids": {
                subject: digest("release")
                for subject in ("math", "cs408", "english", "dashboard")
            },
            "deployment_prepare_receipt_sha256": digest("prepare"),
            "model_call_count": 0,
            "provider_request_count": 0,
            "real_luna_runs": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "production_accepted": False,
            "authority": {
                "schema_version": "study-intake-deployment-authority-v1",
                "algorithm": "HMAC-SHA256",
                "key_id": digest("deployment-key"),
                "purpose": "three-subject-production-canary-activation",
                "hmac_sha256": digest("deployment-hmac"),
            },
        }

    @staticmethod
    def _english_recovery_binding() -> dict[str, object]:
        return {
            "schema_version": (
                "study-intake-subject-batch-recovery-canary-binding-v1"
            ),
            "subject": "english",
            "original_preclaim_failure_receipt_sha256": digest("original"),
            "recovery_receipt_sha256": digest("recovery"),
            "target_release_id": digest("release"),
            "target_activation_id": digest("english-activation"),
            "staged_activation_proof_sha256": digest("staged"),
            "supersede_receipt_sha256": digest("supersede"),
            "replacement_queue_entry_sha256": digest("replacement-queue"),
            "replacement_task": {
                "unit_sha256": digest("unit"),
                "frozen_payload_sha256": digest("frozen"),
                "task_object_sha256": digest("task"),
                "producer_input_contract_sha256": digest("producer"),
                "source_event_set_sha256": digest("source-set"),
            },
            "arm_proof_sha256": digest("arm"),
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    @staticmethod
    def _cs408_retirement_binding() -> dict[str, object]:
        receipt_path = (
            "/tmp/study-intake/dispatch/control-receipts/"
            f"cs408-terminal-batch-retirements/{digest('retirement-receipt')}.json"
        )
        return {
            "schema_version": (
                "study-intake-cs408-terminal-retirement-canary-binding-v1"
            ),
            "subject": "cs408",
            "mode": "archive_only_zero_replay",
            "authorization_descriptor_sha256": (
                "7539cbcaddfc8e6fa5755473a9e71db8a6916141cc40dee5b594221a2cd0a889"
            ),
            "batch_id": "LUNA-CS408-2026-08-12-6FC5AFB45F313F8A0E4D",
            "batch_sha256": (
                "7b22c081aebb0efa912f981e1d71cbdf8b4501da8634e959aec5108c1125ec39"
            ),
            "terminal_receipt_sha256": (
                "83c000a307f2af2037afae903ec1b791e85c00da75ebb9f5178088c76f11a5ff"
            ),
            "writer_preimage_sha256": (
                "f4dc94f9be21578d37e663e187948af581a4fc0dba633cb2e4fe7f6e12f8a29d"
            ),
            "batch_pointer_sha256": (
                "804f5f213d0a56351bb21179110309943577b453aabe2b5ef71e66640cb91e92"
            ),
            "snapshot_sha256": (
                "96400ebe760cdda21b4fe62f8a3987ef118f217a01289ea0ef8b6f1d8ff541dc"
            ),
            "source_generation": "cs408-26b5390a8f9a2c425e58",
            "source_authority_fingerprint": (
                "26b5390a8f9a2c425e58fbc6bec5b0a450dff85c4e1bd1817d99f133259f35f2"
            ),
            "target_generation": "cs408-e51335bd173df83c1736",
            "target_authority_fingerprint": (
                "e51335bd173df83c173666f4b880bdc91a008055818bd7425650959a8d59b08b"
            ),
            "target_release_id": digest("release"),
            "target_activation_id": digest("global-activation"),
            "retirement_receipt_sha256": digest("retirement-receipt"),
            "retirement_receipt_path": receipt_path,
            "retirement_receipt_path_sha256": hashlib.sha256(
                receipt_path.encode("utf-8")
            ).hexdigest(),
            "retirement_rollback_token": digest("retirement-rollback-token"),
            "writer_postimage_sha256": digest("writer-postimage"),
            "postimage_verification_sha256": digest("postimage-verification"),
            "post_retirement_readiness_sha256": digest("readiness"),
            "locked_preflight_authority_snapshot_mcp_tool_call_count": 3,
            "retirement_authority_snapshot_mcp_tool_call_count": 2,
            "english_recovery_authority_snapshot_mcp_tool_call_count": 2,
            "arm_preflight_authority_snapshot_mcp_tool_call_count": 3,
            "apply_authority_snapshot_mcp_tool_call_count": 10,
            "authority_snapshot_count": 2,
            "authority_snapshot_mcp_tool_call_count": 2,
            "model_mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "queue_entry_created_count": 0,
            "replacement_task_created_count": 0,
            "capture_replay_count": 0,
        }

    @staticmethod
    def _english_review_repair_binding() -> dict[str, object]:
        receipt_sha256 = digest("english-review-repair-receipt")
        receipt_path = (
            "/tmp/study-intake/dispatch/control-receipts/"
            "english-preserved-review-repairs/sha256/"
            f"{receipt_sha256[:2]}/{receipt_sha256}.json"
        )
        return {
            "schema_version": (
                "study-intake-english-preserved-review-repair-canary-binding-v1"
            ),
            "subject": "english",
            "authorization_descriptor_sha256": (
                "74a640975df21f2b267170613931275e668ec4f800bc02e557e9306630da507e"
            ),
            "original_preclaim_failure_receipt_sha256": (
                "33548b0f2d6a8a2c73fc24b1dc5b2828241aa6baa1ccc29cf758864eed07525a"
            ),
            "rollover_receipt_sha256": (
                "847883cae478764305ed4964b5e11324da3abf4fe55154816e78070f4d74c3d4"
            ),
            "recovery_supersede_receipt_sha256": (
                "6e9c67a44e69e7b008bece5b4e32d60233283c8611b193ba7c0b37f1a3b0c024"
            ),
            "source_release_id": (
                "b8cc051cd01b7172b48c92f7cea128cb3fcfadf4552da08fef5c9936e122bd00"
            ),
            "source_activation_id": (
                "f3ca26457aa902bfead0b965d89f1d54fc03c4318108bb7d3c78cbae3e71dc89"
            ),
            "unit_sha256": (
                "3408c5a0dc067a17df0c3dd8c0b1ea171b5d473923df885ee5c7f4e3d85b2cc6"
            ),
            "source_gate_preimage_sha256": (
                "df28fd72271e9f5008285919fada76905ccddf0d6e59ca8b4e330d3c2af69fbe"
            ),
            "source_preview_sha256": digest("source-preview"),
            "target_release_id": digest("release"),
            "target_activation_id": digest("english-activation-v5"),
            "target_generation": "english-generation-v5",
            "target_subject_authority_fingerprint": digest(
                "english-subject-authority-v5"
            ),
            "target_producer_authority_fingerprint": digest(
                "english-producer-authority-v5"
            ),
            "staged_target_canary_state_sha256": digest("staged-gate"),
            "staged_gate_proof_sha256": digest("staged-gate-proof"),
            "repair_receipt_sha256": receipt_sha256,
            "repair_receipt_path": receipt_path,
            "repair_receipt_path_sha256": hashlib.sha256(
                receipt_path.encode("utf-8")
            ).hexdigest(),
            **{
                key: digest(key)
                for key in (
                    "batch_archive_sha256",
                    "batch_retirement_pointer_sha256",
                    "batch_postimage_sha256",
                    "batch_pointer_postimage_sha256",
                    "writer_postimage_sha256",
                    "preimage_capsule_sha256",
                    "frozen_payload_sha256",
                    "producer_input_contract_sha256",
                    "review_package_sha256",
                    "report_json_sha256",
                    "report_markdown_sha256",
                    "review_mcp_transcript_sha256",
                    "normalization_receipt_sha256",
                    "review_terminal_sha256",
                    "queue_postimage_sha256",
                    "gate_postimage_sha256",
                    "terminal_index_postimage_sha256",
                    "reopen_proof_sha256",
                    "arm_proof_sha256",
                )
            },
            "report_available": True,
            "report_disposition": "needs_sol_review",
            "sol_review_status": "pending",
            "formal_write_eligible": False,
            "restricted_sol_readable": True,
            "source_queue_absent": True,
            "queue_identity_preserved": True,
            "new_task_count": 0,
            "new_queue_count": 0,
            "capture_replay_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "mcp_tool_call_count": 0,
            "sol_call_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        }

    def test_v2_is_unchanged_and_recovery_binding_requires_v3(self) -> None:
        common = self._common_receipt_fields()
        binding = self._english_recovery_binding()
        v2 = {
            "schema_version": (
                "study-intake-three-subject-canary-activation-receipt-v2"
            ),
            **copy.deepcopy(common),
        }
        v3 = {
            "schema_version": (
                "study-intake-three-subject-canary-activation-receipt-v3"
            ),
            **copy.deepcopy(common),
            "subject_batch_recovery_binding": binding,
        }
        results = schema_results(
            {
                "v2_positive": (
                    "three-subject-canary-activation-receipt-v2.json",
                    v2,
                ),
                "v2_with_binding": (
                    "three-subject-canary-activation-receipt-v2.json",
                    {**copy.deepcopy(v2), "subject_batch_recovery_binding": binding},
                ),
                "v3_positive": (
                    "three-subject-canary-activation-receipt-v3.json",
                    v3,
                ),
                "v3_missing_binding": (
                    "three-subject-canary-activation-receipt-v3.json",
                    {
                        key: value
                        for key, value in copy.deepcopy(v3).items()
                        if key != "subject_batch_recovery_binding"
                    },
                ),
                "v3_fake_sha": (
                    "three-subject-canary-activation-receipt-v3.json",
                    {
                        **copy.deepcopy(v3),
                        "subject_batch_recovery_binding": {
                            **copy.deepcopy(binding),
                            "recovery_receipt_sha256": "not-a-sha",
                        },
                    },
                ),
            }
        )
        self.assertEqual(results["v2_positive"], [])
        self.assertEqual(results["v3_positive"], [])
        for label in ("v2_with_binding", "v3_missing_binding", "v3_fake_sha"):
            self.assertTrue(results[label], msg=f"{label} unexpectedly accepted")

    def test_v4_requires_exact_english_and_cs408_recovery_bindings(self) -> None:
        common = self._common_receipt_fields()
        english = self._english_recovery_binding()
        retirement = self._cs408_retirement_binding()
        v4 = {
            "schema_version": (
                "study-intake-three-subject-canary-activation-receipt-v4"
            ),
            **copy.deepcopy(common),
            "subject_batch_recovery_binding": english,
            "cs408_writer_retirement_binding": retirement,
        }

        def mutate_retirement(**changes: object) -> dict[str, object]:
            mutated = copy.deepcopy(v4)
            binding = mutated["cs408_writer_retirement_binding"]
            if not isinstance(binding, dict):
                raise AssertionError("v4_retirement_fixture_invalid")
            binding.update(changes)
            return mutated

        missing_english = copy.deepcopy(v4)
        missing_english.pop("subject_batch_recovery_binding")
        missing_retirement = copy.deepcopy(v4)
        missing_retirement.pop("cs408_writer_retirement_binding")
        results = schema_results(
            {
                "positive": (
                    "three-subject-canary-activation-receipt-v4.json",
                    v4,
                ),
                "missing_english": (
                    "three-subject-canary-activation-receipt-v4.json",
                    missing_english,
                ),
                "missing_retirement": (
                    "three-subject-canary-activation-receipt-v4.json",
                    missing_retirement,
                ),
                "wrong_descriptor": (
                    "three-subject-canary-activation-receipt-v4.json",
                    mutate_retirement(
                        batch_sha256=digest("unauthorized-batch")
                    ),
                ),
                "wrong_hash": (
                    "three-subject-canary-activation-receipt-v4.json",
                    mutate_retirement(
                        retirement_receipt_sha256="not-a-sha256"
                    ),
                ),
                "wrong_count": (
                    "three-subject-canary-activation-receipt-v4.json",
                    mutate_retirement(
                        apply_authority_snapshot_mcp_tool_call_count=9
                    ),
                ),
                "wrong_path": (
                    "three-subject-canary-activation-receipt-v4.json",
                    mutate_retirement(
                        retirement_receipt_path="relative/receipt.json"
                    ),
                ),
                "missing_path": (
                    "three-subject-canary-activation-receipt-v4.json",
                    {
                        **copy.deepcopy(v4),
                        "cs408_writer_retirement_binding": {
                            key: value
                            for key, value in copy.deepcopy(retirement).items()
                            if key != "retirement_receipt_path"
                        },
                    },
                ),
            }
        )
        self.assertEqual(results["positive"], [])
        for label, errors in results.items():
            if label != "positive":
                self.assertTrue(errors, msg=f"{label} unexpectedly accepted")

    def test_v5_is_exactly_the_preserved_english_review_repair(self) -> None:
        binding = self._english_review_repair_binding()
        v5 = {
            "schema_version": (
                "study-intake-three-subject-canary-activation-receipt-v5"
            ),
            **copy.deepcopy(self._common_receipt_fields()),
            "english_preserved_review_repair_binding": binding,
        }
        without_binding = copy.deepcopy(v5)
        without_binding.pop("english_preserved_review_repair_binding")
        wrong_lineage = copy.deepcopy(v5)
        wrong_lineage["english_preserved_review_repair_binding"][
            "original_preclaim_failure_receipt_sha256"
        ] = digest("wrong-lineage")
        old_descriptor = copy.deepcopy(v5)
        old_descriptor["english_preserved_review_repair_binding"][
            "authorization_descriptor_sha256"
        ] = (
            "4d684190c661ca66e23d2eb7d821a19d0d503ca9e26561f594af03c1573d5de3"
        )
        v4_with_v5 = {
            **copy.deepcopy(v5),
            "schema_version": (
                "study-intake-three-subject-canary-activation-receipt-v4"
            ),
        }
        results = schema_results(
            {
                "positive": (
                    "three-subject-canary-activation-receipt-v5.json",
                    v5,
                ),
                "missing_binding": (
                    "three-subject-canary-activation-receipt-v5.json",
                    without_binding,
                ),
                "wrong_lineage": (
                    "three-subject-canary-activation-receipt-v5.json",
                    wrong_lineage,
                ),
                "old_descriptor": (
                    "three-subject-canary-activation-receipt-v5.json",
                    old_descriptor,
                ),
                "v4_with_v5_binding": (
                    "three-subject-canary-activation-receipt-v4.json",
                    v4_with_v5,
                ),
            }
        )
        self.assertEqual(results["positive"], [])
        for label in (
            "missing_binding",
            "wrong_lineage",
            "old_descriptor",
            "v4_with_v5_binding",
        ):
            self.assertTrue(results[label], msg=f"{label} unexpectedly accepted")


class Cs408TerminalBatchRetirementSchemaTests(unittest.TestCase):
    @staticmethod
    def _retirement_receipt() -> dict[str, object]:
        return {
            "schema_version": (
                "cs408_terminal_batch_writer_retirement_receipt_v1"
            ),
            "authorization_descriptor_sha256": (
                "7539cbcaddfc8e6fa5755473a9e71db8a6916141cc40dee5b594221a2cd0a889"
            ),
            "retirement_id": digest("retirement-id"),
            "mode": "archive_only_zero_replay",
            "subject": "cs408",
            "old_batch_id": "LUNA-CS408-2026-08-12-6FC5AFB45F313F8A0E4D",
            "old_batch_sha256": (
                "7b22c081aebb0efa912f981e1d71cbdf8b4501da8634e959aec5108c1125ec39"
            ),
            "terminal_receipt_sha256": (
                "83c000a307f2af2037afae903ec1b791e85c00da75ebb9f5178088c76f11a5ff"
            ),
            "terminal_receipt_path": "/tmp/receipts/terminal.json",
            "batch_snapshot_sha256": (
                "96400ebe760cdda21b4fe62f8a3987ef118f217a01289ea0ef8b6f1d8ff541dc"
            ),
            "batch_snapshot_path": "/tmp/snapshots/batch.json",
            "batch_pointer_before": {
                "schema_version": "subject_luna_batch_pointer_v1",
                "subject": "cs408",
            },
            "batch_pointer_before_sha256": (
                "804f5f213d0a56351bb21179110309943577b453aabe2b5ef71e66640cb91e92"
            ),
            "source_generation": "cs408-26b5390a8f9a2c425e58",
            "source_authority_fingerprint": (
                "26b5390a8f9a2c425e58fbc6bec5b0a450dff85c4e1bd1817d99f133259f35f2"
            ),
            "next_generation": "cs408-e51335bd173df83c1736",
            "next_authority_fingerprint": (
                "e51335bd173df83c173666f4b880bdc91a008055818bd7425650959a8d59b08b"
            ),
            "archive_sha256": digest("archive"),
            "archive_path": "/tmp/archive/retired-batch.json",
            "writer_revision_before": 7,
            "writer_revision_after": 8,
            "writer_state_before": {
                "subject": "cs408",
                "batch_id": "LUNA-CS408-2026-08-12-6FC5AFB45F313F8A0E4D",
            },
            "writer_state_before_sha256": (
                "f4dc94f9be21578d37e663e187948af581a4fc0dba633cb2e4fe7f6e12f8a29d"
            ),
            "writer_state_after": {
                "subject": "cs408",
                "batch_id": None,
                "authority_generation": "cs408-e51335bd173df83c1736",
            },
            "writer_state_after_sha256": digest("writer-after"),
            "global_sol_state_before": {
                "sol_enabled": False,
                "formal_write_count": 0,
            },
            "global_sol_state_before_sha256": digest("sol-before"),
            "global_sol_state_after_sha256": digest("sol-before"),
            "rollback_token": digest("rollback-token"),
            "queue_entry_created": False,
            "replacement_task_created": False,
            "capture_replayed": False,
            "sol_called": False,
            "sol_enabled": False,
            "model_call_count": 0,
            "provider_request_count": 0,
            "authority_snapshot_count": 2,
            "authority_snapshot_mcp_tool_call_count": 2,
            "authority_snapshot_sha256s": [
                digest("authority-snapshot-1"),
                digest("authority-snapshot-2"),
            ],
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 2,
            "formal_write_count": 0,
            "created_at": "2026-08-13T09:00:00Z",
            "seal": {
                "algorithm": "HMAC-SHA256",
                "purpose": "cs408-terminal-batch-writer-retirement-v1",
                "hmac_sha256": digest("retirement-hmac"),
            },
        }

    @staticmethod
    def _retirement_pointer() -> dict[str, object]:
        return {
            "schema_version": (
                "cs408_terminal_batch_writer_retirement_pointer_v1"
            ),
            "subject": "cs408",
            "retirement_id": digest("retirement-id"),
            "old_batch_id": "LUNA-CS408-2026-08-12-6FC5AFB45F313F8A0E4D",
            "old_batch_sha256": (
                "7b22c081aebb0efa912f981e1d71cbdf8b4501da8634e959aec5108c1125ec39"
            ),
            "retirement_receipt_sha256": digest("retirement-receipt"),
            "retirement_receipt_path": "/tmp/retirements/receipt.json",
            "writer_revision_after": 8,
            "writer_state_after_sha256": digest("writer-after"),
            "next_generation": "cs408-e51335bd173df83c1736",
            "next_authority_fingerprint": (
                "e51335bd173df83c173666f4b880bdc91a008055818bd7425650959a8d59b08b"
            ),
            "applied_at": "2026-08-13T09:00:01Z",
            "formal_write_count": 0,
            "seal": {
                "algorithm": "HMAC-SHA256",
                "purpose": (
                    "cs408-terminal-batch-writer-retirement-pointer-v1"
                ),
                "hmac_sha256": digest("pointer-hmac"),
            },
        }

    @staticmethod
    def _rollback_receipt() -> dict[str, object]:
        return {
            "schema_version": (
                "cs408_terminal_batch_writer_retirement_rollback_receipt_v1"
            ),
            "subject": "cs408",
            "retirement_receipt_sha256": digest("retirement-receipt"),
            "retirement_receipt_path": "/tmp/retirements/receipt.json",
            "authorization_descriptor_sha256": (
                "7539cbcaddfc8e6fa5755473a9e71db8a6916141cc40dee5b594221a2cd0a889"
            ),
            "rollback_token": digest("rollback-token"),
            "writer_state_after_sha256": digest("writer-after"),
            "writer_state_restored_sha256": (
                "f4dc94f9be21578d37e663e187948af581a4fc0dba633cb2e4fe7f6e12f8a29d"
            ),
            "batch_pointer_unchanged_sha256": (
                "804f5f213d0a56351bb21179110309943577b453aabe2b5ef71e66640cb91e92"
            ),
            "batch_snapshot_unchanged_sha256": (
                "96400ebe760cdda21b4fe62f8a3987ef118f217a01289ea0ef8b6f1d8ff541dc"
            ),
            "terminal_receipt_unchanged_sha256": (
                "83c000a307f2af2037afae903ec1b791e85c00da75ebb9f5178088c76f11a5ff"
            ),
            "retirement_pointer_withdrawn": True,
            "retirement_intent_withdrawn": True,
            "rollback_postimage_verification_sha256": digest(
                "rollback-postimage"
            ),
            "queue_entry_created_count": 0,
            "replacement_task_created_count": 0,
            "capture_replay_count": 0,
            "authority_snapshot_count": 0,
            "authority_snapshot_mcp_tool_call_count": 0,
            "model_mcp_tool_call_count": 0,
            "mcp_tool_call_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
            "rolled_back_at": "2026-08-13T09:00:02Z",
            "seal": {
                "algorithm": "HMAC-SHA256",
                "purpose": (
                    "cs408-terminal-batch-writer-retirement-rollback-v1"
                ),
                "hmac_sha256": digest("rollback-hmac"),
            },
        }

    def test_runtime_retirement_artifacts_round_trip_and_fail_closed(self) -> None:
        receipt = self._retirement_receipt()
        pointer = self._retirement_pointer()
        rollback = self._rollback_receipt()
        schemas = {
            "receipt": "cs408-terminal-batch-writer-retirement-receipt-v1.json",
            "pointer": "cs408-terminal-batch-writer-retirement-pointer-v1.json",
            "rollback": (
                "cs408-terminal-batch-writer-retirement-rollback-receipt-v1.json"
            ),
        }
        fixtures = {
            "receipt": receipt,
            "pointer": pointer,
            "rollback": rollback,
        }
        for label, schema_name in schemas.items():
            schema = json.loads(
                (ROOT / "schemas" / schema_name).read_text(encoding="utf-8")
            )
            self.assertEqual(set(fixtures[label]), set(schema["required"]))

        receipt_replayed = copy.deepcopy(receipt)
        receipt_replayed["capture_replayed"] = True
        receipt_wrong_generation = copy.deepcopy(receipt)
        receipt_wrong_generation["source_generation"] = "cs408-other"
        pointer_relative_path = copy.deepcopy(pointer)
        pointer_relative_path["retirement_receipt_path"] = "relative.json"
        pointer_wrong_generation = copy.deepcopy(pointer)
        pointer_wrong_generation["next_generation"] = "cs408-other"
        rollback_queue_created = copy.deepcopy(rollback)
        rollback_queue_created["queue_entry_created_count"] = 1
        rollback_changed_terminal = copy.deepcopy(rollback)
        rollback_changed_terminal["terminal_receipt_unchanged_sha256"] = digest(
            "changed-terminal"
        )
        results = schema_results(
            {
                "receipt_positive": (schemas["receipt"], receipt),
                "receipt_replayed": (schemas["receipt"], receipt_replayed),
                "receipt_wrong_generation": (
                    schemas["receipt"],
                    receipt_wrong_generation,
                ),
                "pointer_positive": (schemas["pointer"], pointer),
                "pointer_relative_path": (
                    schemas["pointer"],
                    pointer_relative_path,
                ),
                "pointer_wrong_generation": (
                    schemas["pointer"],
                    pointer_wrong_generation,
                ),
                "rollback_positive": (schemas["rollback"], rollback),
                "rollback_queue_created": (
                    schemas["rollback"],
                    rollback_queue_created,
                ),
                "rollback_changed_terminal": (
                    schemas["rollback"],
                    rollback_changed_terminal,
                ),
            }
        )
        for label in (
            "receipt_positive",
            "pointer_positive",
            "rollback_positive",
        ):
            self.assertEqual(results[label], [])
        for label, errors in results.items():
            if not label.endswith("_positive"):
                self.assertTrue(errors, msg=f"{label} unexpectedly accepted")


class ProducerAndMathSchemaRoundTripTests(unittest.TestCase):
    def test_new_source_user_answer_is_verbatim_bound_and_fake_sha_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            adapter = MathAdapter(
                {
                    "repo_root": str(repo),
                    "status_script": str(repo / "unused-status.py"),
                    "adapter_version": "schema-roundtrip-v1",
                    "enabled": False,
                    "max_images": 4,
                },
                {},
            )
            question = repo / "question.png"
            question.write_bytes(
                base64.b64decode(
                    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
                )
            )
            solution = repo / "solution.txt"
            solution.write_text("逐字解析：先固定对象，再检查符号。", encoding="utf-8")
            manifest = {
                "source_locator": "new-source-1",
                "artifacts": [
                    {
                        "path": question.name,
                        "role": "question",
                        "sha256": hashlib.sha256(question.read_bytes()).hexdigest(),
                    },
                    {
                        "path": solution.name,
                        "role": "solution_text",
                        "sha256": hashlib.sha256(solution.read_bytes()).hexdigest(),
                    },
                ],
            }
            manifest_path = repo / "manifest.json"
            manifest_path.write_text(
                json.dumps(manifest, ensure_ascii=False, sort_keys=True),
                encoding="utf-8",
            )
            item = {
                "capture_type": "new_source_problem",
                "source_bundle": {
                    "manifest_path": manifest_path.name,
                    "manifest_hash": hashlib.sha256(
                        manifest_path.read_bytes()
                    ).hexdigest(),
                },
                "episode_evidence": {
                    "user_answer_text": "我先把复合函数的内层导数符号写反了。"
                },
            }
            source_bundle, _images = adapter._source_bundle(item, None)
            assert source_bundle is not None
            contract = source_bundle["math_evidence_contract"]
            self.assertEqual(contract["evidence_status"], "ready")
            self.assertEqual(contract["missing_roles"], [])
            self.assertEqual(
                contract["verbatim_learning_record_sha256"],
                sha256_value(
                    {
                        "user_answer_text": (
                            "我先把复合函数的内层导数符号写反了。"
                        )
                    }
                ),
            )
            assert_positive_and_negatives(
                self,
                schema_name="math-capture-evidence-contract-v1.json",
                value=contract,
                missing_key="verbatim_learning_record_sha256",
                wrong_type_key="question_image_sha256s",
                pseudo_sha_path=("source_fingerprint",),
            )
            invalid = {
                "capture_type": "fact_observation",
                "effective_evidence_hash": "not-a-sha",
                "evidence": {"user_facts": ["逐字学习记录"]},
                "episode_evidence": {
                    "teaching_dialogue": ["逐字教学对话"]
                },
            }
            with self.assertRaisesRegex(
                PreprocessorError,
                "math_capture_effective_evidence_hash_invalid",
            ):
                adapter._source_bundle(invalid, None)

    def test_real_math_source_and_producer_builders_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            repo = Path(temporary)
            adapter = MathAdapter(
                {
                    "repo_root": str(repo),
                    "status_script": str(repo / "unused-status.py"),
                    "adapter_version": "schema-roundtrip-v1",
                    "enabled": False,
                    "max_images": 4,
                },
                {},
            )
            item = {
                "capture_type": "fact_observation",
                "effective_evidence_hash": digest("source-evidence"),
                "evidence": {
                    "user_facts": [
                        "I confused the inner derivative sign while solving."
                    ]
                },
                "episode_evidence": {
                    "dialogue": [
                        {
                            "role": "user",
                            "content": "Why does the sign change here?",
                        },
                        {
                            "role": "assistant",
                            "content": "The inner derivative is negative.",
                        },
                    ]
                },
            }
            source_bundle, _images = adapter._source_bundle(item, None)
            assert source_bundle is not None
            evidence_contract = source_bundle["math_evidence_contract"]
            member = {
                "subject": "math",
                "capture_id": "MATH-SCHEMA-1",
                "recorded_at": "2026-08-12T10:00:00Z",
                "input_fingerprint": digest("input"),
                "input_binding": {
                    "capture_type": "fact_observation",
                    "math_evidence_contract": evidence_contract,
                    "math_evidence_contract_sha256": sha256_value(
                        evidence_contract
                    ),
                },
                "model_input": {"source_bundle": source_bundle},
            }
            producer = producer_dispatch_input_contract(
                config={},
                subject="math",
                release_id=digest("release"),
                producer_unit_id=member["capture_id"],
                producer_recorded_at=member["recorded_at"],
                input_fingerprint=member["input_fingerprint"],
                member_payloads=[member],
                processing_contract_sha256=digest("processing-contract"),
            )

        assert_positive_and_negatives(
            self,
            schema_name="math-capture-evidence-contract-v1.json",
            value=evidence_contract,
            missing_key="full_dialogue_sha256",
            wrong_type_key="question_image_sha256s",
            pseudo_sha_path=("source_fingerprint",),
        )
        assert_positive_and_negatives(
            self,
            schema_name="producer-dispatch-input-v2.json",
            value=producer,
            missing_key="source_events",
            wrong_type_key="formal_write_count",
            pseudo_sha_path=("authority_fingerprint",),
        )


class SubjectSolSchemaRoundTripTests(unittest.TestCase):
    def test_real_subject_batch_builder_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            store = SubjectSolRuntimeStore(Path(temporary))
            value = store.prepare_and_freeze_subject_batch(
                subject="math",
                batch_id="MATH-SCHEMA-BATCH-1",
                study_date="2026-08-12",
                capture_high_watermark="MATH-SCHEMA-CAP-1",
                authority_generation="math-generation-schema-1",
                authority_fingerprint=digest("math-authority"),
                scan_snapshot_sha256=digest("scan-snapshot"),
                tasks=[
                    {
                        "capture_id": "MATH-SCHEMA-CAP-1",
                        "unit_sha256": digest("unit"),
                        "input_fingerprint": digest("input"),
                        "study_date": "2026-08-12",
                        "frozen_payload_sha256": digest("frozen"),
                    }
                ],
            )
        assert_positive_and_negatives(
            self,
            schema_name="subject-luna-batch-v2.json",
            value=value,
            missing_key="tasks",
            wrong_type_key="formal_write_count",
            pseudo_sha_path=("tasks", "0", "unit_sha256"),
        )


class ModelStageSchemaRoundTripTests(unittest.TestCase):
    """Exercise the production LeaseStore/CodexRunner publication path.

    The child below is a local Python process, not a model or Provider request.
    It nevertheless traverses the same process-identity, raw-first chunk chain,
    exit, execution, normalization, and signed progress builders used by the
    production runner.  This prevents a hand-written sample from masking drift
    between the runtime publishers and their root Schemas.
    """

    @staticmethod
    def _read_published(path: str) -> dict[str, object]:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise AssertionError("published_stage_artifact_not_object")
        return value

    def test_real_model_stage_builders_round_trip_and_reject_drift(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            runtime = Path(temporary) / "runtime"
            frozen = FrozenTask(
                {
                    "subject": "math",
                    "capture_id": "MATH-STAGE-SCHEMA-1",
                    "study_date": "2026-08-12",
                    "recorded_at": "2026-08-12T10:00:00Z",
                    "input_fingerprint": digest("stage-input"),
                    "input_binding": {"schema_roundtrip": True},
                    "model_input": {"schema_roundtrip": True},
                    "allowed_evidence_refs": ["capture:MATH-STAGE-SCHEMA-1"],
                    "image_paths": [],
                    "dispatch_contract": {
                        "schema_version": (
                            "study-intake-dispatch-release-binding-v1"
                        ),
                        **dispatch_rule_binding(
                            release_id=digest("stage-release"),
                            subject="math",
                            subject_processing_contract_sha256=None,
                        ),
                    },
                }
            )
            store = LeaseStore(runtime)
            owner_id = f"dispatcher-{os.getpid()}-{'a' * 32}"
            decision = store.claim(
                frozen.unit_sha256,
                owner_id,
                subject="math",
                task=frozen,
            )
            self.assertIsNotNone(decision.lease)
            assert decision.lease is not None
            supervisor = subprocess.Popen(
                [sys.executable, "-c", "import time;time.sleep(60)"],
                start_new_session=True,
            )
            try:
                store.publish_task_process_identity(
                    frozen,
                    decision.lease,
                    child_pid=supervisor.pid,
                    child_pgid=supervisor.pid,
                    process_start_token=kernel_process_start_token(
                        supervisor.pid
                    ),
                    launch_nonce="b" * 32,
                    launched_at="2026-08-12T10:00:00Z",
                    argv=[
                        sys.executable,
                        "-c",
                        "import time;time.sleep(60)",
                    ],
                    executable_path=Path(sys.executable),
                    start_new_session=True,
                )
                context_root = (
                    runtime
                    / "dispatch"
                    / "contexts"
                    / frozen.unit_sha256
                    / "fence-1"
                )
                context_root.mkdir(parents=True, exist_ok=True)
                raw_output_path = runtime / "stage-last-message.json"
                exact_raw = b'{"schema_roundtrip":true}\n'
                command = [
                    sys.executable,
                    "-c",
                    (
                        "from pathlib import Path;import sys;"
                        "Path(sys.argv[1]).write_bytes("
                        "b'{\"schema_roundtrip\":true}\\n');"
                        "sys.stdout.buffer.write(b'provider-stdout');"
                        "sys.stderr.buffer.write(b'provider-stderr')"
                    ),
                    str(raw_output_path),
                ]
                runner = CodexRunner(
                    {
                        "model": "gpt-5.6-luna",
                        "reasoning_effort": "max",
                    },
                    runtime,
                )
                runner.bind_dispatch_process_lifecycle(
                    task=frozen,
                    lease=decision.lease,
                    lease_store=store,
                )
                completed = runner._invoke_subprocess(
                    command,
                    input=b"",
                    timeout=None,
                    cwd=context_root,
                    stage_name="math_analysis",
                    raw_output_path=raw_output_path,
                    provider_schema_sha256=digest("provider-schema"),
                )
                self.assertEqual(completed.returncode, 0)
                self.assertEqual(raw_output_path.read_bytes(), exact_raw)

                raw_refs = runner._provider_raw_refs["math_analysis"]
                closure = runner._provider_closure["math_analysis"]
                execution_refs = store.publish_model_stage_execution_receipt(
                    frozen,
                    decision.lease,
                    stage_name="math_analysis",
                    execution_status="completed",
                    raw_output_object_sha256=str(
                        raw_refs["raw_output_object_sha256"]
                    ),
                    raw_output_object_ref=str(
                        raw_refs["raw_output_object_ref"]
                    ),
                    provider_process_identity_sha256=str(
                        closure["provider_process_identity_sha256"]
                    ),
                    provider_process_exit_sha256=str(
                        closure["provider_process_exit_sha256"]
                    ),
                    authority_snapshot_manifest_sha256=digest(
                        "authority-snapshot-manifest"
                    ),
                    mcp_grounding_manifest_sha256=digest(
                        "mcp-grounding-manifest"
                    ),
                    mcp_transport_sha256=digest("mcp-transport"),
                    mcp_transcript_sha256=digest("mcp-transcript"),
                    attempted_mcp_tool_call_count=2,
                    successful_mcp_tool_call_count=2,
                    grounding_mcp_tool_call_count=2,
                    failed_mcp_tool_call_count=0,
                    last_mcp_error_code=None,
                    provider_returncode=0,
                    duration_ms=25,
                )
                normalization_refs = (
                    store.publish_model_stage_normalization_receipt(
                        frozen,
                        decision.lease,
                        stage_name="math_analysis",
                        execution_receipt_sha256=str(
                            execution_refs[
                                "stage_execution_receipt_sha256"
                            ]
                        ),
                        execution_receipt_ref=str(
                            execution_refs["stage_execution_receipt_ref"]
                        ),
                        raw_output_object_sha256=str(
                            raw_refs["raw_output_object_sha256"]
                        ),
                        raw_output_object_ref=str(
                            raw_refs["raw_output_object_ref"]
                        ),
                        normalization_status="normalized",
                        normalized_payload_sha256=digest(
                            "normalized-payload"
                        ),
                        warnings=[],
                        error_code=None,
                    )
                )

                raw_value = self._read_published(
                    str(raw_refs["raw_output_object_path"])
                )
                execution_value = self._read_published(
                    str(execution_refs["stage_execution_receipt_path"])
                )
                normalization_value = self._read_published(
                    str(
                        normalization_refs[
                            "stage_normalization_receipt_path"
                        ]
                    )
                )
                progress_index = json.loads(
                    (
                        store.stage_progress_latest_root
                        / frozen.unit_sha256
                        / "fence-1"
                        / "math_analysis.json"
                    ).read_text(encoding="utf-8")
                )
                progress_value = self._read_published(
                    str(progress_index["progress_receipt_path"])
                )

                self.assertEqual(
                    raw_value["provider_process_identity_sha256"],
                    raw_value["execution_binding"][
                        "provider_process_identity_sha256"
                    ],
                )
                self.assertEqual(
                    execution_value["provider_process_identity_sha256"],
                    execution_value["execution_binding"][
                        "provider_process_identity_sha256"
                    ],
                )
                self.assertEqual(
                    normalization_value["execution_binding"],
                    execution_value["execution_binding"],
                )
                self.assertEqual(
                    progress_value["provider_process_identity_sha256"],
                    closure["provider_process_identity_sha256"],
                )
            finally:
                if supervisor.poll() is None:
                    os.killpg(supervisor.pid, signal.SIGKILL)
                    supervisor.wait(timeout=2)

        assert_positive_and_negatives(
            self,
            schema_name="model-stage-raw-output-v1.json",
            value=raw_value,
            missing_key="raw_chain_manifest_sha256",
            wrong_type_key="raw_chain_total_chunk_count",
            pseudo_sha_path=("execution_binding", "release_id"),
        )
        assert_positive_and_negatives(
            self,
            schema_name="model-stage-execution-receipt-v1.json",
            value=execution_value,
            missing_key="authority_snapshot_manifest_sha256",
            wrong_type_key="attempted_mcp_tool_call_count",
            pseudo_sha_path=(
                "execution_binding",
                "provider_process_identity_sha256",
            ),
            authority_purpose_path=("authority", "purpose"),
        )
        assert_positive_and_negatives(
            self,
            schema_name="model-stage-normalization-receipt-v1.json",
            value=normalization_value,
            missing_key="execution_receipt_sha256",
            wrong_type_key="warning_count",
            pseudo_sha_path=("execution_binding", "unit_sha256"),
            authority_purpose_path=("authority", "purpose"),
        )
        assert_positive_and_negatives(
            self,
            schema_name="stage-progress-receipt-v1.json",
            value=progress_value,
            missing_key="provider_stage_name",
            wrong_type_key="sequence",
            pseudo_sha_path=("provider_process_identity_sha256",),
            authority_purpose_path=("authority", "purpose"),
        )

        cross_stage_cases: dict[str, tuple[str, object]] = {}
        for label, schema_name, source in (
            (
                "raw_cross_stage",
                "model-stage-raw-output-v1.json",
                raw_value,
            ),
            (
                "execution_cross_stage",
                "model-stage-execution-receipt-v1.json",
                execution_value,
            ),
            (
                "normalization_cross_stage",
                "model-stage-normalization-receipt-v1.json",
                normalization_value,
            ),
            (
                "progress_cross_stage",
                "stage-progress-receipt-v1.json",
                progress_value,
            ),
        ):
            mutated = copy.deepcopy(source)
            mutated["provider_stage_name"] = "cs408_analysis"
            cross_stage_cases[label] = (schema_name, mutated)
        cross_stage_results = schema_results(cross_stage_cases)
        for label, errors in cross_stage_results.items():
            self.assertTrue(errors, msg=f"{label} unexpectedly accepted")


class DashboardSchemaRoundTripTests(unittest.TestCase):
    @staticmethod
    def _public_detail() -> dict[str, object]:
        detail = {
            "schema_version": "study-intake-dispatch-task-detail-v2",
            "unit_sha256": digest("unit"),
            "owner_id": "dispatcher-101-task",
            "fence": 1,
            "attempt": 1,
            "subject": "math",
            "capture_id": "MATH-SCHEMA-CAP-1",
            "release_id": digest("release"),
            "frozen_payload_sha256": digest("frozen"),
            "rule_version": "study-intake-concurrent-dispatch-contract-v2",
            "evidence_integrity": "frozen_and_hmac_bound",
            "phase": "provider_process_started",
            "stage_name": "analysis",
            "latest_event_sha256": digest("event"),
            "server_queue_status": "unknown",
            "artifacts": {},
            "updated_at": "2026-08-12T10:00:00+00:00",
            "formal_write_count": 0,
            "authority": {
                "algorithm": "HMAC-SHA256",
                "purpose": "dispatch-task-detail",
                "hmac_sha256": digest("detail-authority"),
            },
        }
        item = {
            "capture_id": detail["capture_id"],
            "subject": "math",
            "unit_sha256": detail["unit_sha256"],
            "release_id": detail["release_id"],
            "frozen_payload_sha256": detail["frozen_payload_sha256"],
            "input_fingerprint": digest("input"),
            "rule_version": detail["rule_version"],
            "generation": 1,
            "attempt": 1,
            "fence": 1,
            "local_dispatch_status": "running",
            "evidence_access_status": "ready",
            "analysis_execution_status": "running",
            "analysis_report_status": "not_started",
            "review_execution_status": "not_started",
            "review_report_status": "not_started",
            "sol_review_status": "not_eligible",
            "formal_write_status": "not_authorized",
            "server_queue_status": "unknown",
            "server_queue_confirmation": "unconfirmed",
            "warning_codes": [],
            "elapsed_runtime_seconds": 20,
            "last_meaningful_progress_at": "2026-08-12T10:00:00+00:00",
            "soft_timeout_warning": False,
            "stall_probe_status": "healthy",
            "exact_error_code": None,
            "authority_snapshot_sha256": digest("snapshot"),
            "analysis_execution_receipt_sha256": None,
            "analysis_raw_output_sha256": None,
            "analysis_normalization_receipt_sha256": None,
            "analysis_report_sha256": None,
            "critical_review_execution_receipt_sha256": None,
            "critical_review_raw_output_sha256": None,
            "critical_review_normalization_receipt_sha256": None,
            "critical_review_report_sha256": None,
            "sol_handoff_envelope_sha256": None,
            "terminal_receipt_sha256": None,
            "last_state_change_at": "2026-08-12T10:00:00+00:00",
        }
        with (
            mock.patch.object(
                dashboard_server, "_load_dispatch_events", return_value=([], [])
            ),
            mock.patch.object(
                dashboard_server,
                "_verify_completion_authority",
                return_value=None,
            ),
            mock.patch.object(
                dashboard_server,
                "_verify_content_member_authority",
                return_value=None,
            ),
        ):
            return dashboard_server._public_dispatch_task_detail(
                detail,
                item=item,
                runtime_root=ROOT,
                include_raw=False,
                detail_authority_verified=True,
                output_schema_version=dashboard_server.TASK_DETAIL_SCHEMA_VERSION,
            )

    def test_real_dashboard_builders_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            projection = dashboard_projection._main_projection(
                {
                    "dashboard": {
                        "projection_schema_version": (
                            dashboard_projection.MAIN_SCHEMA
                        )
                    },
                    "dispatch": {
                        "production_canary": {
                            "continuous_concurrency_limit": 20
                        }
                    },
                },
                Path(temporary),
                study_date="2026-08-12",
                generated_at="2026-08-12T10:00:00+00:00",
            )
        detail = self._public_detail()
        assert_positive_and_negatives(
            self,
            schema_name="dashboard-projection-v5.json",
            value=projection,
            missing_key="subjects",
            wrong_type_key="configured_global_continuous_concurrency_limit",
            pseudo_sha_path=("dispatchers", "math", "release_id"),
        )
        assert_positive_and_negatives(
            self,
            schema_name="dashboard-task-detail-v2.json",
            value=detail,
            missing_key="analysis",
            wrong_type_key="progress",
            pseudo_sha_path=("identity", "release_id"),
        )


if __name__ == "__main__":
    unittest.main()
