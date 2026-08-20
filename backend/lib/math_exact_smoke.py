#!/usr/bin/env python3
"""Read-only gate and immutable descriptor for the 2026-08-13 math smoke.

This module deliberately describes exactly five already-frozen samples.  It is
not a general capture, replay, or retirement interface.  The preview performs
no locking, staging, ledger append, queue materialization, or external call.
"""

from __future__ import annotations

import copy
import datetime as dt
import fcntl
import hashlib
import hmac
import importlib.util
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Mapping


DESCRIPTOR_SCHEMA = "study-intake-math-exact-smoke-execution-authorization-v1"
PREVIEW_SCHEMA = "study-intake-math-exact-smoke-preview-v1"
AUTHORIZATION_RECEIPT_SCHEMA = (
    "study-intake-math-exact-smoke-execution-authorization-receipt-v1"
)
CAPTURE_APPLY_RECEIPT_SCHEMA = (
    "study-intake-math-exact-smoke-capture-apply-receipt-v1"
)
TASK_BINDING_SCHEMA = (
    "study-intake-math-exact-smoke-task-binding-v1"
)
TASK_BINDING_V2_SCHEMA = (
    "study-intake-math-exact-smoke-task-binding-v2"
)
MIGRATION_INTENT_SCHEMA = (
    "study-intake-math-pending-queue-migration-intent-v1"
)
MIGRATION_DESCRIPTOR_SCHEMA = (
    "study-intake-math-pending-queue-migration-descriptor-v1"
)
MIGRATION_COMMIT_SCHEMA = (
    "study-intake-math-pending-queue-migration-commit-v1"
)
EXACT_SCOPE_ID = "math-smoke-2026-08-13-five-sample-v1"
EXACT_STUDY_DATE = "2026-08-13"

AUTHORIZATION_PURPOSE = "dispatch-math-exact-smoke-execution-authorization"
CAPTURE_APPLY_PURPOSE = "dispatch-math-exact-smoke-capture-apply"
MIGRATION_INTENT_PURPOSE = "dispatch-math-pending-queue-migration-intent"
MIGRATION_DESCRIPTOR_PURPOSE = (
    "dispatch-math-pending-queue-migration-descriptor"
)
MIGRATION_COMMIT_PURPOSE = "dispatch-math-pending-queue-migration-commit"

# The one non-terminal source anomaly admitted by this fixed migration is the
# immutable GS-566 pre-claim admission receipt already present in production.
# It did not claim a lease or submit a model request, so it remains processing
# attempt 1.  Tests replace only these byte identities inside isolated roots.
GS566_SOURCE_QUEUE_SHA256 = (
    "f8e8c6840fc84615ca36b285c0387082121faa86a86baba99cc09e2168fe578d"
)
GS566_PRECLAIM_FAILURE_RECEIPT_SHA256 = (
    "7f1baf972b8b7a93538dd60d24ab177d5dfab591c90793d61ca0138ed68e4563"
)
GS566_PRECLAIM_FAILURE_ERROR = "subject_luna_batch_already_current"

SMOKE_ROOT = Path(
    "/Users/xiazhibin/.codex/kaoyan-math-deferred-intake/2026-08-13/"
    "019fea48-7f82-7352-b652-0479fbc50f71/"
    "快速入库Smoke样例包_2026-08-13"
)
MATH_REPO_ROOT = Path("/Users/xiazhibin/Documents/kaoyan-math")
LEDGER_PATH = MATH_REPO_ROOT / "数学一回滚复习系统/快速入库事件.jsonl"
REVIEW_UNIT_PATH = MATH_REPO_ROOT / "数学一回滚复习系统/复习单元.json"

BATCH_MANIFEST_SHA256 = (
    "af251d73fb772f4d97a628073760bbc048eda8600dab9468be5e694fd920f606"
)
CHECKSUMS_SHA256 = (
    "a1ca7caa3ef44711eb6034b8362eb17bbb47873f535f34c8f28a0a284b06503a"
)
GATES_SHA256 = (
    "d794d0b2f301535a647e0ef96a389cde494ed16aa4a76abd66ff9e36fcbefb50"
)
README_SHA256 = (
    "7ecbbd64c9475f76bea70e613a5c79ea7f3bf63308a5ae4a6380bda53877ce78"
)
INITIAL_LEDGER_SHA256 = (
    "289168feaec0899976d4f05fb1f1a9f753666f455979f21ecc566f8451c5a7e2"
)
REVIEW_UNIT_SHA256 = (
    "95c99baf2f0b713fb4a8683534e47cf9b20bad593a292ad92b9d6d639fe4cee7"
)

EXACT_ORDER = ("GS-269", "GS-566", "GS-225", "LA-050", "LA-016")
MIGRATION_ORDER = EXACT_ORDER[1:]

# Every byte identity below was frozen by the supplied sample package.  The
# capture event/idempotency values are the deterministic identities produced by
# the existing math-fast-intake-v2 normalizer; no event is written by preview.
EXACT_SAMPLES: dict[str, dict[str, Any]] = {
    "GS-269": {
        "sample_id": "MATH-SMOKE-20260813-GS269",
        "sample_sha256": "b2f5c3928f4215bbafbcd40f48fbcd6a689e9487ad35ccf76fef0d0b356b4404",
        "formal_card_path": "错题知识网络/错题卡/GS-269_强化例题9.3（1）.md",
        "formal_card_sha256": "001591649f230245ee61547f1de24eb9228ef847f27f62f73b5192babe227fd5",
        "question_path": "错题知识网络/assets/visual_wrong_questions/GS-269/question_01.png",
        "question_sha256": "1530e2de683df15b23865a10f06d8e9af9ebc190c74b31c23358bbff6aab7e5c",
        "attachment_path": "/Users/xiazhibin/.codex/kaoyan-math-deferred-intake/2026-08-13/019fea48-7f82-7352-b652-0479fbc50f71/GS-269/题目与本轮上下文_01.png",
        "attachment_sha256": "99a008b7be6b3e2db356d7ed535a1cd2c0d962c93066d9ba0719eb10e7b14b2d",
        "stage_required": True,
        "stage_bundle_id": "f6f2857044af6d70561a857c",
        "stage_manifest_sha256": "d85d1a6eb48cb44ccba000d6d66270c8d665b1494caaf5cf14a57f4e7d5185a9",
        "capture_event_id": "MFI-CAP-5d1e87959549b634bdda36b2",
        "capture_idempotency_key": "5d1e87959549b634bdda36b2b96943ef9376a571fec8c3f94bf0aba20626ee59",
    },
    "GS-566": {
        "sample_id": "MATH-SMOKE-20260813-GS566",
        "sample_sha256": "e2e4e70190da2f21a1a7d357011a0b0dea09df2563358d3a9bb169b8789f985a",
        "formal_card_path": "错题知识网络/错题卡/GS-566_138692相邻差望远镜判敛.md",
        "formal_card_sha256": "fdc777d59c6d8c7c36a092c1503362cc979cdfdebd00a64eb5685c39982aafa4",
        "question_path": "错题知识网络/assets/visual_wrong_questions/GS-566/question_01.png",
        "question_sha256": "ca1fea34a735f681db1df6af22b147ea5d9648f1dd1cd1e0a2bd25c0ab8f4fe5",
        "attachment_path": None,
        "attachment_sha256": None,
        "stage_required": False,
        "stage_bundle_id": None,
        "stage_manifest_sha256": None,
        "capture_event_id": "MFI-CAP-5c3eb977aabf72752a6a6048",
        "capture_idempotency_key": "5c3eb977aabf72752a6a60485199718cd393d3a25abb7c00401e664a13755158",
    },
    "GS-225": {
        "sample_id": "MATH-SMOKE-20260813-GS225",
        "sample_sha256": "9e0ea6b056e3acbbfcfe169592142908efc355c95f8cef42181a2e0d1383e8db",
        "formal_card_path": "错题知识网络/错题卡/GS-225_强化例题6.10.md",
        "formal_card_sha256": "5b109f0a7bfed1c4125f091fc0933bfcbbb812b8e987df9562c21488479e3e8e",
        "question_path": "错题知识网络/assets/visual_wrong_questions/GS-225/question_01.png",
        "question_sha256": "e6fcbaeebe81fbbe5c986137f587703356985f7a9593b0d7135dae5a151a7d62",
        "attachment_path": "/Users/xiazhibin/.codex/kaoyan-math-deferred-intake/2026-08-13/019fea48-7f82-7352-b652-0479fbc50f71/GS-225/题目与本轮上下文_01.png",
        "attachment_sha256": "5958504cf5c2f877acd5fc1ef0ebabb08480ef2dad0d5e6172d4c7f9ccf91ff9",
        "stage_required": True,
        "stage_bundle_id": "7b99223b4546f834a17aad66",
        "stage_manifest_sha256": "0da4eb2a05f8a2b6d813a9485a274da3b8d38ab4a09cd8531bacf416dc7af104",
        "capture_event_id": "MFI-CAP-a2c04bdcf4918536d28871c1",
        "capture_idempotency_key": "a2c04bdcf4918536d28871c14a5a59bf68f86fa3f797d489f82aa48b112005ad",
    },
    "LA-050": {
        "sample_id": "MATH-SMOKE-20260813-LA050",
        "sample_sha256": "2ee8be0a7e9eab904a6daf656516ae6627fa4cef5de098b4fb49d56e7b31b67e",
        "formal_card_path": "错题知识网络/错题卡/LA-050_强化例题3.11(171611).md",
        "formal_card_sha256": "bf12612704bdcdb80283c73802c3f8eb5d9978bf2d3ba90130963ce51f9b3b11",
        "question_path": "错题知识网络/assets/visual_wrong_questions/LA-050/question_01.png",
        "question_sha256": "00f7a380e56f096f9a7f56bbe2e069fd9f05ad741fddee851231e5f883513595",
        "attachment_path": "/Users/xiazhibin/.codex/kaoyan-math-deferred-intake/2026-08-13/019fea48-7f82-7352-b652-0479fbc50f71/LA-050/题目与本轮上下文_01.png",
        "attachment_sha256": "a1b667f52643f1300be3c21069ff614bed098f7e5f4de6b049aa55906b157bca",
        "stage_required": True,
        "stage_bundle_id": "c57b9c590cac6a568e3a93eb",
        "stage_manifest_sha256": "8e3b11442c093e373fdb7a64767ba2ffb3f429460700d307fb70357f5bbac142",
        "capture_event_id": "MFI-CAP-42561b07e3f7298558fb736b",
        "capture_idempotency_key": "42561b07e3f7298558fb736b1f7164d9c15349a8919be337539e5e01c023acf1",
    },
    "LA-016": {
        "sample_id": "MATH-SMOKE-20260813-LA016",
        "sample_sha256": "7c833fafbf7833eac71597459c32f5b37702c6f940342d2cc690452b4a835c79",
        "formal_card_path": "错题知识网络/错题卡/LA-016_强化例题1.7.md",
        "formal_card_sha256": "0efe2a9d1eb15b0e6aeb85e8b305f7938d3bc9a423ad4000d10fa76c8341c12c",
        "question_path": "错题知识网络/assets/visual_wrong_questions/LA-016/question_01.png",
        "question_sha256": "6b71471e098bce0e04b469871465c221718ce24beadfbb71739ae23bde679cf4",
        "attachment_path": "/Users/xiazhibin/.codex/kaoyan-math-deferred-intake/2026-08-13/019fea48-7f82-7352-b652-0479fbc50f71/LA-016/题目与本轮上下文_01.png",
        "attachment_sha256": "ae61e75c81545b6fbd1770122ebaa98de2d13c79a4b29ae768869af84e4cc683",
        "stage_required": True,
        "stage_bundle_id": "fef21f7762f5ef5ba131b800",
        "stage_manifest_sha256": "1c98d762aeebbccfdf9163f7311b0b8bd7e48cd075152d0c410617d7a4164179",
        "capture_event_id": "MFI-CAP-eae2494d091f251b0e005333",
        "capture_idempotency_key": "eae2494d091f251b0e00533317f2481142029366a76ed70cce004602e54d71b2",
    },
}


class MathExactSmokeError(RuntimeError):
    """Stable fail-closed error for this one exact smoke authorization."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def file_sha256(path: Path) -> str:
    try:
        return sha256_bytes(path.read_bytes())
    except OSError as exc:
        raise MathExactSmokeError("math_smoke_evidence_unreadable") from exc


def _require_sha256(value: object, code: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise MathExactSmokeError(code)
    return value


def _read_json(path: Path, code: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise MathExactSmokeError(code) from exc
    if not isinstance(value, dict):
        raise MathExactSmokeError(code)
    return value


def _ledger_capture_counts(path: Path) -> dict[str, int]:
    counts = {formal_id: 0 for formal_id in EXACT_ORDER}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise MathExactSmokeError("math_smoke_ledger_unreadable") from exc
    for line in lines:
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise MathExactSmokeError("math_smoke_ledger_invalid") from exc
        if not isinstance(row, dict):
            raise MathExactSmokeError("math_smoke_ledger_invalid")
        target = row.get("target")
        formal_id = target.get("formal_id") if isinstance(target, dict) else None
        if row.get("event_type") == "capture" and formal_id in counts:
            counts[str(formal_id)] += 1
    return counts


def build_authorization_descriptor(
    *,
    target_release_id: str,
    activation_id: str,
    authority_generation: str,
    authority_fingerprint: str,
    producer_authority_fingerprint: str,
) -> dict[str, Any]:
    """Build the unsigned exact descriptor in memory; this performs no write."""

    _require_sha256(target_release_id, "math_smoke_release_invalid")
    _require_sha256(activation_id, "math_smoke_activation_invalid")
    _require_sha256(authority_fingerprint, "math_smoke_authority_invalid")
    _require_sha256(
        producer_authority_fingerprint,
        "math_smoke_producer_authority_invalid",
    )
    if not isinstance(authority_generation, str) or not authority_generation:
        raise MathExactSmokeError("math_smoke_generation_invalid")
    samples = []
    for sequence, formal_id in enumerate(EXACT_ORDER, start=1):
        item = copy.deepcopy(EXACT_SAMPLES[formal_id])
        item.update(
            {
                "sequence": sequence,
                "formal_id": formal_id,
                "capture_count": 0,
                "source_stage_count": 1 if item["stage_required"] else 0,
            }
        )
        samples.append(item)
    descriptor = {
        "schema_version": DESCRIPTOR_SCHEMA,
        "scope_id": EXACT_SCOPE_ID,
        "mode": "exact_five_sample_capture_once_then_successor_attempt_one",
        "target_release_id": target_release_id,
        "activation_id": activation_id,
        "authority_generation": authority_generation,
        "authority_fingerprint": authority_fingerprint,
        "producer_authority_fingerprint": producer_authority_fingerprint,
        "batch_manifest_sha256": BATCH_MANIFEST_SHA256,
        "checksums_sha256": CHECKSUMS_SHA256,
        "gates_sha256": GATES_SHA256,
        "readme_sha256": README_SHA256,
        "initial_ledger_sha256": INITIAL_LEDGER_SHA256,
        "review_unit_sha256": REVIEW_UNIT_SHA256,
        "execution_order": list(EXACT_ORDER),
        "samples": samples,
        "execution_authorized_by_user": True,
        "original_samples_execution_authorized": False,
        "sol_enabled": False,
        "formal_write_count": 0,
        "model_call_count_at_authorization": 0,
        "provider_request_count_at_authorization": 0,
    }
    descriptor["descriptor_sha256"] = sha256_bytes(canonical_bytes(descriptor))
    return descriptor


def preview_exact_smoke(
    descriptor: Mapping[str, Any],
    *,
    smoke_root: Path = SMOKE_ROOT,
    math_repo_root: Path = MATH_REPO_ROOT,
    expected_ledger_sha256: str = INITIAL_LEDGER_SHA256,
    expected_capture_counts: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Reopen all exact inputs without mutating source, runtime, or live state."""

    expected = build_authorization_descriptor(
        target_release_id=str(descriptor.get("target_release_id") or ""),
        activation_id=str(descriptor.get("activation_id") or ""),
        authority_generation=str(descriptor.get("authority_generation") or ""),
        authority_fingerprint=str(descriptor.get("authority_fingerprint") or ""),
        producer_authority_fingerprint=str(
            descriptor.get("producer_authority_fingerprint") or ""
        ),
    )
    if canonical_bytes(dict(descriptor)) != canonical_bytes(expected):
        raise MathExactSmokeError("math_smoke_descriptor_drift")

    expected_package = {
        "README.md": README_SHA256,
        "batch_manifest.json": BATCH_MANIFEST_SHA256,
        "预期验收门禁.json": GATES_SHA256,
        **{
            f"{formal_id}/sample.json": EXACT_SAMPLES[formal_id][
                "sample_sha256"
            ]
            for formal_id in EXACT_ORDER
        },
    }
    checksums_path = smoke_root / "CHECKSUMS.sha256"
    if file_sha256(checksums_path) != CHECKSUMS_SHA256:
        raise MathExactSmokeError("math_smoke_checksums_drift")
    try:
        checksum_rows = {
            line.split(None, 1)[1]: line.split(None, 1)[0]
            for line in checksums_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    except (OSError, UnicodeError, IndexError) as exc:
        raise MathExactSmokeError("math_smoke_checksums_invalid") from exc
    if checksum_rows != expected_package:
        raise MathExactSmokeError("math_smoke_checksums_invalid")
    for relative, digest in expected_package.items():
        if file_sha256(smoke_root / relative) != digest:
            raise MathExactSmokeError("math_smoke_package_hash_drift")

    batch = _read_json(
        smoke_root / "batch_manifest.json", "math_smoke_batch_invalid"
    )
    gates = _read_json(
        smoke_root / "预期验收门禁.json", "math_smoke_gates_invalid"
    )
    if (
        batch.get("execution_authorized") is not False
        or batch.get("state") != "offline_samples_unconsumed"
        or gates.get("execution_authorized") is not False
    ):
        raise MathExactSmokeError("math_smoke_offline_envelope_mutated")

    ledger_path = math_repo_root / "数学一回滚复习系统/快速入库事件.jsonl"
    unit_path = math_repo_root / "数学一回滚复习系统/复习单元.json"
    _require_sha256(expected_ledger_sha256, "math_smoke_ledger_hash_invalid")
    if file_sha256(ledger_path) != expected_ledger_sha256:
        raise MathExactSmokeError("math_smoke_ledger_hash_drift")
    if file_sha256(unit_path) != REVIEW_UNIT_SHA256:
        raise MathExactSmokeError("math_smoke_review_unit_drift")
    counts = _ledger_capture_counts(ledger_path)
    expected_counts = (
        {formal_id: 0 for formal_id in EXACT_ORDER}
        if expected_capture_counts is None
        else dict(expected_capture_counts)
    )
    if set(expected_counts) != set(EXACT_ORDER) or any(
        isinstance(expected_counts[formal_id], bool)
        or not isinstance(expected_counts[formal_id], int)
        or expected_counts[formal_id] not in {0, 1}
        for formal_id in EXACT_ORDER
    ):
        raise MathExactSmokeError("math_smoke_capture_count_expectation_invalid")
    if any(
        counts[formal_id] != expected_counts[formal_id]
        for formal_id in EXACT_ORDER
    ):
        raise MathExactSmokeError("math_smoke_capture_count_drift")

    sample_results: list[dict[str, Any]] = []
    for sequence, formal_id in enumerate(EXACT_ORDER, start=1):
        binding = EXACT_SAMPLES[formal_id]
        sample = _read_json(
            smoke_root / formal_id / "sample.json",
            "math_smoke_sample_invalid",
        )
        if (
            sample.get("execution_authorized") is not False
            or sample.get("state") != "offline_unconsumed"
            or sample.get("capture_count_at_freeze") != 0
        ):
            raise MathExactSmokeError("math_smoke_offline_envelope_mutated")
        formal = sample.get("formal_identity")
        if not isinstance(formal, dict) or (
            formal.get("formal_id") != formal_id
            or formal.get("formal_card_path") != binding["formal_card_path"]
            or formal.get("formal_card_sha256") != binding["formal_card_sha256"]
            or formal.get("canonical_question_path") != binding["question_path"]
            or formal.get("canonical_question_sha256") != binding["question_sha256"]
        ):
            raise MathExactSmokeError("math_smoke_sample_binding_drift")
        if file_sha256(math_repo_root / binding["formal_card_path"]) != binding[
            "formal_card_sha256"
        ]:
            raise MathExactSmokeError("math_smoke_formal_card_drift")
        if file_sha256(math_repo_root / binding["question_path"]) != binding[
            "question_sha256"
        ]:
            raise MathExactSmokeError("math_smoke_question_drift")
        attachment_path = binding["attachment_path"]
        if attachment_path is not None and file_sha256(
            Path(str(attachment_path))
        ) != binding["attachment_sha256"]:
            raise MathExactSmokeError("math_smoke_attachment_drift")
        stage = sample.get("source_stage_payload")
        if binding["stage_required"] is not (stage is not None):
            raise MathExactSmokeError("math_smoke_stage_rule_drift")
        if formal_id == "GS-566":
            episode = sample.get("episode_evidence")
            capture = sample.get("capture_payload_template")
            evidence = capture.get("evidence") if isinstance(capture, dict) else None
            episode = (
                capture.get("episode_evidence")
                if isinstance(capture, dict)
                else None
            )
            if (
                sample.get("supplemental_attachment") is not None
                or stage is not None
                or not isinstance(evidence, dict)
                or evidence.get("result") != "correct"
                or evidence.get("independent_correct_steps") != []
                or not isinstance(episode, dict)
                or episode.get("user_answer_text")
                != "用户明确自述这道题做对了，但本轮未提交具体过程。"
            ):
                raise MathExactSmokeError("math_smoke_gs566_semantics_drift")
        sample_results.append(
            {
                "sequence": sequence,
                "formal_id": formal_id,
                "sample_sha256": binding["sample_sha256"],
                "capture_event_id": binding["capture_event_id"],
                "capture_idempotency_key": binding[
                    "capture_idempotency_key"
                ],
                "capture_count": counts[formal_id],
                "stage_required": binding["stage_required"],
                "stage_write_count": 0,
                "status": (
                    "ready_unconsumed"
                    if counts[formal_id] == 0
                    else "capture_recorded"
                ),
            }
        )

    core = {
        "schema_version": PREVIEW_SCHEMA,
        "scope_id": EXACT_SCOPE_ID,
        "status": "ready",
        "descriptor_sha256": expected["descriptor_sha256"],
        "target_release_id": expected["target_release_id"],
        "activation_id": expected["activation_id"],
        "authority_generation": expected["authority_generation"],
        "authority_fingerprint": expected["authority_fingerprint"],
        "producer_authority_fingerprint": expected[
            "producer_authority_fingerprint"
        ],
        "execution_order": list(EXACT_ORDER),
        "samples": sample_results,
        "read_only": True,
        "source_stage_write_count": 0,
        "capture_write_count": 0,
        "queue_write_count": 0,
        "model_call_count": 0,
        "provider_request_count": 0,
        "sol_enabled": False,
        "formal_write_count": 0,
    }
    core["preview_sha256"] = sha256_bytes(canonical_bytes(core))
    return core


def _utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def _read_authority_key(runtime_root: Path) -> bytes:
    path = runtime_root / "dispatch/state/authority.key"
    try:
        info = path.lstat()
        if (
            path.is_symlink()
            or not path.is_file()
            or (info.st_mode & 0o777) != 0o600
        ):
            raise MathExactSmokeError("math_smoke_authority_key_invalid")
        key = path.read_bytes()
    except OSError as exc:
        raise MathExactSmokeError("math_smoke_authority_key_unreadable") from exc
    if len(key) != 32:
        raise MathExactSmokeError("math_smoke_authority_key_invalid")
    return key


def _seal(value: Mapping[str, Any], *, purpose: str, key: bytes) -> dict[str, Any]:
    core = copy.deepcopy(dict(value))
    core.pop("authority", None)
    mac = hmac.new(
        key,
        canonical_bytes({"purpose": purpose, "payload": core}),
        hashlib.sha256,
    ).hexdigest()
    core["authority"] = {
        "schema_version": "study-intake-dispatch-authority-v1",
        "algorithm": "HMAC-SHA256",
        "key_id": sha256_bytes(key),
        "purpose": purpose,
        "hmac_sha256": mac,
    }
    return core


def _verify_seal(
    value: Mapping[str, Any], *, purpose: str, key: bytes
) -> None:
    authority = value.get("authority")
    if not isinstance(authority, Mapping):
        raise MathExactSmokeError("math_smoke_authority_proof_missing")
    core = copy.deepcopy(dict(value))
    core.pop("authority", None)
    expected = hmac.new(
        key,
        canonical_bytes({"purpose": purpose, "payload": core}),
        hashlib.sha256,
    ).hexdigest()
    if (
        authority.get("schema_version")
        != "study-intake-dispatch-authority-v1"
        or authority.get("algorithm") != "HMAC-SHA256"
        or authority.get("key_id") != sha256_bytes(key)
        or authority.get("purpose") != purpose
        or not hmac.compare_digest(
            str(authority.get("hmac_sha256") or ""), expected
        )
    ):
        raise MathExactSmokeError("math_smoke_authority_hmac_invalid")


def _publish_content_addressed(
    root: Path, value: Mapping[str, Any]
) -> tuple[str, Path]:
    payload = canonical_bytes(value) + b"\n"
    digest = sha256_bytes(payload)
    path = root / "sha256" / digest[:2] / f"{digest}.json"
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(prefix=f".{digest}.", dir=path.parent)
    temporary = Path(name)
    try:
        os.fchmod(fd, 0o400)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != payload:
                raise MathExactSmokeError("math_smoke_content_address_collision")
        os.chmod(path, 0o400)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return digest, path


def _verify_content_addressed_path(
    path: Path, *, root: Path, expected_sha256: str | None = None
) -> tuple[str, dict[str, Any]]:
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root.resolve(strict=True))
        payload = resolved.read_bytes()
        value = json.loads(payload.decode("utf-8"))
    except (OSError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise MathExactSmokeError("math_smoke_receipt_unreadable") from exc
    digest = sha256_bytes(payload)
    if (
        resolved.name != f"{digest}.json"
        or resolved.parent.name != digest[:2]
        or resolved.parent.parent.name != "sha256"
        or (expected_sha256 is not None and digest != expected_sha256)
        or not isinstance(value, dict)
    ):
        raise MathExactSmokeError("math_smoke_receipt_content_address_invalid")
    return digest, value


def _validate_authority_snapshot(
    snapshot: Mapping[str, Any], descriptor: Mapping[str, Any]
) -> None:
    if (
        snapshot.get("schema_version") != "subject_authority_snapshot_v1"
        or snapshot.get("subject") != "math"
        or snapshot.get("generation") != descriptor.get("authority_generation")
        or snapshot.get("authority_fingerprint")
        != descriptor.get("authority_fingerprint")
        or snapshot.get("model_call_count") != 0
        or snapshot.get("formal_write_count") != 0
    ):
        raise MathExactSmokeError("math_smoke_subject_authority_drift")


def _validate_canary_state(
    runtime_root: Path, descriptor: Mapping[str, Any], *, key: bytes
) -> dict[str, Any]:
    state = _read_json(
        runtime_root / "dispatch/state/production-canary/math.json",
        "math_smoke_canary_state_invalid",
    )
    try:
        _verify_seal(state, purpose="dispatch-production-canary-state", key=key)
    except MathExactSmokeError as exc:
        raise MathExactSmokeError("math_smoke_canary_state_invalid") from exc
    if (
        state.get("subject") != "math"
        or state.get("release_id") != descriptor.get("target_release_id")
        or state.get("activation_id") != descriptor.get("activation_id")
        or state.get("producer_authority_fingerprint")
        != descriptor.get("producer_authority_fingerprint")
        or state.get("state") not in {"armed", "continuous_concurrent_unlocked"}
        or state.get("luna_consumer_enabled") is not True
        or state.get("sol_enabled") is not False
        or state.get("formal_write_count") != 0
    ):
        raise MathExactSmokeError("math_smoke_canary_state_drift")
    return state


def authorize_exact_smoke(
    descriptor: Mapping[str, Any],
    *,
    runtime_root: Path,
    authority_snapshot: Mapping[str, Any],
    authorized_at: str | None = None,
    smoke_root: Path = SMOKE_ROOT,
    math_repo_root: Path = MATH_REPO_ROOT,
) -> tuple[str, Path, dict[str, Any]]:
    """Publish the one HMAC/content-addressed authorization after all gates."""

    preview = preview_exact_smoke(
        descriptor, smoke_root=smoke_root, math_repo_root=math_repo_root
    )
    key = _read_authority_key(runtime_root)
    _validate_canary_state(runtime_root, descriptor, key=key)
    _validate_authority_snapshot(authority_snapshot, descriptor)
    snapshot_object = copy.deepcopy(dict(authority_snapshot))
    snapshot_sha256 = sha256_bytes(canonical_bytes(snapshot_object))
    core = {
        "schema_version": AUTHORIZATION_RECEIPT_SCHEMA,
        "scope_id": EXACT_SCOPE_ID,
        "authorization_descriptor": copy.deepcopy(dict(descriptor)),
        "authorization_descriptor_sha256": descriptor["descriptor_sha256"],
        "preview_sha256": preview["preview_sha256"],
        "authority_snapshot": snapshot_object,
        "authority_snapshot_sha256": snapshot_sha256,
        "authority_snapshot_count": 1,
        "authority_snapshot_mcp_tool_call_count": 1,
        "authorized_at": authorized_at or _utc_now(),
        "capture_count": 0,
        "source_stage_count": 0,
        "queue_count": 0,
        "model_call_count": 0,
        "provider_request_count": 0,
        "model_mcp_tool_call_count": 0,
        "sol_enabled": False,
        "formal_write_count": 0,
    }
    receipt = _seal(core, purpose=AUTHORIZATION_PURPOSE, key=key)
    digest, path = _publish_content_addressed(
        runtime_root
        / "dispatch/math-exact-smoke/execution-authorizations",
        receipt,
    )
    return digest, path, receipt


def reopen_execution_authorization(
    receipt_path: Path,
    *,
    runtime_root: Path,
) -> tuple[str, dict[str, Any]]:
    root = runtime_root / "dispatch/math-exact-smoke/execution-authorizations"
    digest, receipt = _verify_content_addressed_path(receipt_path, root=root)
    key = _read_authority_key(runtime_root)
    _verify_seal(receipt, purpose=AUTHORIZATION_PURPOSE, key=key)
    descriptor = receipt.get("authorization_descriptor")
    if (
        receipt.get("schema_version") != AUTHORIZATION_RECEIPT_SCHEMA
        or receipt.get("scope_id") != EXACT_SCOPE_ID
        or not isinstance(descriptor, Mapping)
        or descriptor.get("descriptor_sha256")
        != receipt.get("authorization_descriptor_sha256")
        or receipt.get("capture_count") != 0
        or not isinstance(receipt.get("authority_snapshot"), Mapping)
        or receipt.get("authority_snapshot_sha256")
        != sha256_bytes(canonical_bytes(receipt["authority_snapshot"]))
        or receipt.get("authority_snapshot_count") != 1
        or receipt.get("authority_snapshot_mcp_tool_call_count") != 1
        or receipt.get("source_stage_count") != 0
        or receipt.get("queue_count") != 0
        or receipt.get("model_call_count") != 0
        or receipt.get("provider_request_count") != 0
        or receipt.get("model_mcp_tool_call_count") != 0
        or receipt.get("sol_enabled") is not False
        or receipt.get("formal_write_count") != 0
    ):
        raise MathExactSmokeError("math_smoke_authorization_receipt_invalid")
    expected = build_authorization_descriptor(
        target_release_id=str(descriptor.get("target_release_id") or ""),
        activation_id=str(descriptor.get("activation_id") or ""),
        authority_generation=str(descriptor.get("authority_generation") or ""),
        authority_fingerprint=str(descriptor.get("authority_fingerprint") or ""),
        producer_authority_fingerprint=str(
            descriptor.get("producer_authority_fingerprint") or ""
        ),
    )
    if canonical_bytes(descriptor) != canonical_bytes(expected):
        raise MathExactSmokeError("math_smoke_descriptor_drift")
    return digest, receipt


def _migration_root(runtime_root: Path) -> Path:
    return runtime_root / "dispatch/math-pending-queue-migrations"


def _migration_intent_root(runtime_root: Path) -> Path:
    return _migration_root(runtime_root) / "intents"


def _migration_descriptor_root(runtime_root: Path) -> Path:
    return _migration_root(runtime_root) / "descriptors"


def _migration_commit_root(runtime_root: Path) -> Path:
    return _migration_root(runtime_root) / "commits"


def _migration_state_root(runtime_root: Path) -> Path:
    return runtime_root / "dispatch/state/math-pending-queue-migration"


def _migration_active_pointer_path(runtime_root: Path) -> Path:
    return _migration_state_root(runtime_root) / "active.json"


def _migration_commit_pointer_path(runtime_root: Path) -> Path:
    return _migration_state_root(runtime_root) / "commit.json"


def _publish_migration_commit_pointer(
    *, runtime_root: Path, pointer: Mapping[str, Any], key: bytes
) -> None:
    _verify_seal(
        pointer,
        purpose="dispatch-math-pending-queue-migration-commit-pointer",
        key=key,
    )
    pointer_path = _migration_commit_pointer_path(runtime_root)
    payload = canonical_bytes(pointer) + b"\n"
    if pointer_path.exists() and pointer_path.read_bytes() == payload:
        return
    if pointer_path.exists():
        previous_pointer = _read_json(
            pointer_path, "math_migration_commit_invalid"
        )
        _verify_seal(
            previous_pointer,
            purpose="dispatch-math-pending-queue-migration-commit-pointer",
            key=key,
        )
        previous_descriptor_sha256 = _require_sha256(
            str(
                previous_pointer.get("migration_descriptor_sha256") or ""
            ),
            "math_migration_descriptor_invalid",
        )
        reopen_exact_math_pending_queue_migration_commit(
            runtime_root=runtime_root,
            migration_descriptor_sha256=previous_descriptor_sha256,
            required=True,
        )
    _atomic_restore_bytes(pointer_path, payload)
    os.chmod(pointer_path, 0o400)


def _publish_active_migration_pointer(
    *, runtime_root: Path, pointer: Mapping[str, Any], key: bytes
) -> None:
    _verify_seal(
        pointer,
        purpose="dispatch-math-pending-queue-migration-pointer",
        key=key,
    )
    pointer_path = _migration_active_pointer_path(runtime_root)
    pointer_payload = canonical_bytes(pointer) + b"\n"
    if pointer_path.exists() and pointer_path.read_bytes() == pointer_payload:
        return
    if pointer_path.exists():
        active = _reopen_active_migration_descriptor(
            runtime_root=runtime_root
        )
        committed_descriptor_sha256 = (
            _committed_migration_descriptor_sha256(
                runtime_root=runtime_root
            )
        )
        if (
            active is None
            or committed_descriptor_sha256 is None
            or active[0] != committed_descriptor_sha256
        ):
            raise MathExactSmokeError("math_migration_already_active")
    _atomic_restore_bytes(pointer_path, pointer_payload)
    os.chmod(pointer_path, 0o400)


def _verify_subject_archive_seal(
    value: Mapping[str, Any], *, key: bytes
) -> None:
    seal = value.get("seal")
    core = copy.deepcopy(dict(value))
    core.pop("seal", None)
    expected = hmac.new(
        key,
        canonical_bytes(
            {
                "purpose": "subject-background-luna-batch-archive",
                "payload": core,
            }
        ),
        hashlib.sha256,
    ).hexdigest()
    if (
        not isinstance(seal, Mapping)
        or seal.get("algorithm") != "HMAC-SHA256"
        or seal.get("purpose")
        != "subject-background-luna-batch-archive"
        or not hmac.compare_digest(
            str(seal.get("hmac_sha256") or ""), expected
        )
    ):
        raise MathExactSmokeError("math_migration_gs269_archive_invalid")


def _gs269_terminal_has_completed_execution(
    terminal: Mapping[str, Any],
) -> bool:
    """Distinguish bound historical execution from this reopen's calls."""

    return (
        terminal.get("model_call_count") == 0
        and terminal.get("provider_request_count") == 0
        and terminal.get("mcp_tool_call_count") == 0
        and terminal.get("observed_model_call_count", 0) > 0
        and terminal.get("observed_provider_request_count", 0) > 0
        and terminal.get("observed_mcp_tool_call_count", 0) > 0
        and isinstance(terminal.get("read_session_id"), str)
        and bool(terminal.get("read_session_id"))
    )


def _select_gs269_rollover_archive(
    archive_matches: list[tuple[Path, dict[str, Any]]],
    receipt: Mapping[str, Any],
    *,
    terminal_receipt_sha256: str,
) -> tuple[Path, dict[str, Any]]:
    selected = [
        row
        for row in archive_matches
        if receipt.get("archive_sha256") == file_sha256(row[0])
        and Path(str(receipt.get("archive_path") or "")).resolve()
        == row[0].resolve()
    ]
    if len(selected) != 1:
        raise MathExactSmokeError("math_migration_gs269_archive_duplicate")
    archive_path, archive = selected[0]
    if (
        receipt.get("schema_version")
        != "subject_background_luna_rollover_receipt_v1"
        or receipt.get("subject") != "math"
        or receipt.get("mode") != "explicit_failure_resume"
        or receipt.get("old_batch_id") != archive.get("batch_id")
        or receipt.get("old_batch_sha256") != archive.get("batch_sha256")
        or receipt.get("resume_acceptance_sha256")
        != terminal_receipt_sha256
        or receipt.get("authority_generation")
        != archive.get("authority_generation")
        or receipt.get("authority_fingerprint")
        != archive.get("authority_fingerprint")
        or receipt.get("sol_called") is not False
        or receipt.get("sol_enabled") is not False
        or receipt.get("model_call_count") != 0
        or receipt.get("provider_request_count") != 0
        or receipt.get("formal_write_count") != 0
    ):
        raise MathExactSmokeError("math_migration_gs269_archive_invalid")
    return archive_path, archive


def _completed_gs269_archived_evidence(
    *, runtime_root: Path, key: bytes
) -> dict[str, Any]:
    capture_id = str(EXACT_SAMPLES[EXACT_ORDER[0]]["capture_event_id"])
    queue_root = runtime_root / "dispatch/state/production-canary-queue/math"
    accepted: list[tuple[Path, dict[str, Any]]] = []
    if queue_root.is_dir():
        for path in sorted(queue_root.glob("*/*.json")):
            queue = _read_json(path, "math_migration_gs269_queue_invalid")
            _verify_seal(
                queue, purpose="dispatch-production-canary-queue", key=key
            )
            if (
                queue.get("source_event_ids") == [capture_id]
                and queue.get("queue_status") == "succeeded"
                and queue.get("terminal_outcome") == "succeeded"
            ):
                accepted.append((path.resolve(), queue))
    if len(accepted) != 1:
        raise MathExactSmokeError(
            "math_migration_gs269_success_missing"
            if not accepted
            else "math_migration_gs269_success_duplicate"
        )
    queue_path, queue = accepted[0]
    if (
        queue.get("subject") != "math"
        or queue.get("formal_write_count") != 0
    ):
        raise MathExactSmokeError("math_migration_gs269_queue_invalid")
    task_payload, _ = _read_content_bound_file(
        queue.get("task_object_path"),
        queue.get("task_object_sha256"),
        root=runtime_root / "dispatch/production-canary/tasks/math",
        code="math_migration_gs269_task_invalid",
    )
    terminal_payload, terminal_path = _read_content_bound_file(
        queue.get("terminal_receipt_path"),
        queue.get("terminal_receipt_sha256"),
        root=runtime_root / "dispatch/production-canary/receipts/math",
        code="math_migration_gs269_terminal_invalid",
    )
    try:
        task_object = json.loads(task_payload.decode("utf-8"))
        terminal = json.loads(terminal_payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise MathExactSmokeError(
            "math_migration_gs269_evidence_invalid"
        ) from exc
    frozen_payload = (
        task_object.get("frozen_payload")
        if isinstance(task_object, Mapping)
        else None
    )
    binding = (
        frozen_payload.get("math_exact_smoke_binding")
        if isinstance(frozen_payload, Mapping)
        else None
    )
    if (
        not isinstance(task_object, Mapping)
        or not isinstance(frozen_payload, Mapping)
        or not isinstance(binding, Mapping)
        or binding.get("formal_id") != EXACT_ORDER[0]
        or binding.get("capture_event_id") != capture_id
        or binding.get("processing_attempt_number") not in {1, 2}
        or binding.get("formal_write_count") != 0
        or binding.get("sol_enabled") is not False
        or task_object.get("unit_sha256") != queue.get("unit_sha256")
        or sha256_bytes(canonical_bytes(frozen_payload))
        != queue.get("frozen_payload_sha256")
    ):
        raise MathExactSmokeError("math_migration_gs269_task_invalid")
    if not isinstance(terminal, Mapping):
        raise MathExactSmokeError("math_migration_gs269_terminal_invalid")
    _verify_seal(
        terminal, purpose="dispatch-production-canary-terminal", key=key
    )
    if (
        terminal.get("schema_version")
        != "study-intake-production-canary-terminal-receipt-v3"
        or terminal.get("subject") != "math"
        or terminal.get("release_id") != queue.get("release_id")
        or terminal.get("activation_id") != queue.get("activation_id")
        or terminal.get("outcome") != "succeeded"
        or not _gs269_terminal_has_completed_execution(terminal)
        or terminal.get("report_reopen_status")
        != "json_markdown_package_verified"
        or terminal.get("formal_write_count") != 0
    ):
        raise MathExactSmokeError("math_migration_gs269_terminal_invalid")
    for path_key, sha_key, root in (
        (
            "package_path",
            "package_sha256",
            runtime_root / "dispatch/packages",
        ),
        (
            "completion_receipt_path",
            "completion_receipt_sha256",
            runtime_root / "dispatch/receipts",
        ),
    ):
        _read_content_bound_file(
            terminal.get(path_key),
            terminal.get(sha_key),
            root=root,
            code="math_migration_gs269_report_invalid",
        )
    archive_root = (
        runtime_root
        / "dispatch/subject-luna-batch-archives/background/sha256"
    )
    archive_matches: list[tuple[Path, dict[str, Any]]] = []
    if archive_root.is_dir():
        for path in sorted(archive_root.glob("*/*.json")):
            archive = _read_json(
                path, "math_migration_gs269_archive_invalid"
            )
            batch = archive.get("batch")
            tasks = batch.get("tasks") if isinstance(batch, Mapping) else None
            if not isinstance(tasks, list) or len(tasks) != 1:
                continue
            archived_task = tasks[0]
            if (
                isinstance(archived_task, Mapping)
                and archived_task.get("capture_id") == capture_id
                and archived_task.get("unit_sha256")
                == queue.get("unit_sha256")
                and archived_task.get("package_sha256")
                == terminal.get("package_sha256")
                and archived_task.get("status")
                in {"workflow_complete", "workflow_complete_with_warnings"}
            ):
                archive_matches.append((path.resolve(), archive))
    if not archive_matches:
        raise MathExactSmokeError(
            "math_migration_gs269_archive_missing"
        )
    if len(archive_matches) == 1:
        archive_path, archive = archive_matches[0]
    else:
        batch_identities = {
            (row[1].get("batch_id"), row[1].get("batch_sha256"))
            for row in archive_matches
        }
        if len(batch_identities) != 1:
            raise MathExactSmokeError(
                "math_migration_gs269_archive_duplicate"
            )
        batch_id, batch_sha256 = next(iter(batch_identities))
        intent_path = (
            runtime_root
            / "dispatch/state/subject-background-rollover-intents/math"
            / f"{batch_id}.{batch_sha256}.json"
        )
        if (
            not intent_path.is_file()
            or intent_path.is_symlink()
        ):
            raise MathExactSmokeError(
                "math_migration_gs269_archive_duplicate"
            )
        intent = _read_json(
            intent_path, "math_migration_gs269_archive_invalid"
        )
        receipt_sha256 = file_sha256(intent_path)
        try:
            from subject_sol_contract import (
                SubjectSolContractError,
                SubjectSolRuntimeStore,
            )

            receipt = SubjectSolRuntimeStore(
                runtime_root
            )._read_background_rollover_receipt(receipt_sha256)
        except (ImportError, SubjectSolContractError) as exc:
            raise MathExactSmokeError(
                "math_migration_gs269_archive_invalid"
            ) from exc
        if receipt != intent:
            raise MathExactSmokeError(
                "math_migration_gs269_archive_invalid"
            )
        archive_path, archive = _select_gs269_rollover_archive(
            archive_matches,
            receipt,
            terminal_receipt_sha256=str(queue["terminal_receipt_sha256"]),
        )
    _verify_subject_archive_seal(archive, key=key)
    batch = archive["batch"]
    if (
        archive.get("schema_version")
        != "subject_background_luna_batch_archive_v1"
        or archive.get("subject") != "math"
        or archive.get("batch_id") != batch.get("batch_id")
        or archive.get("batch_sha256")
        != sha256_bytes(canonical_bytes(batch) + b"\n")
        or archive.get("authority_generation")
        != batch.get("authority_generation")
        or archive.get("authority_fingerprint")
        != batch.get("authority_fingerprint")
        or archive.get("sol_called") is not False
        or archive.get("sol_enabled") is not False
        or archive.get("formal_write_count") != 0
        or batch.get("all_terminal") is not True
        or batch.get("status") != "frozen"
        or batch.get("sol_ready") is not False
    ):
        raise MathExactSmokeError("math_migration_gs269_archive_invalid")
    return {
        "capture_event_id": capture_id,
        "accepted_queue_path": str(queue_path),
        "accepted_queue_sha256": file_sha256(queue_path),
        "accepted_task_object_sha256": queue["task_object_sha256"],
        "accepted_unit_sha256": queue["unit_sha256"],
        "terminal_receipt_path": str(terminal_path),
        "terminal_receipt_sha256": queue["terminal_receipt_sha256"],
        "package_sha256": terminal["package_sha256"],
        "report_json_sha256": terminal["report_json_sha256"],
        "report_markdown_sha256": terminal["report_markdown_sha256"],
        "archive_path": str(archive_path),
        "archive_sha256": file_sha256(archive_path),
        "batch_id": archive["batch_id"],
        "batch_sha256": archive["batch_sha256"],
        "archived_authority_generation": archive[
            "authority_generation"
        ],
        "archived_authority_fingerprint": archive[
            "authority_fingerprint"
        ],
        "formal_write_count": 0,
    }


def _gs566_source_preclaim_failure_evidence(
    *,
    runtime_root: Path,
    queue_path: Path,
    queue: Mapping[str, Any],
    source_binding: Mapping[str, Any],
    key: bytes,
) -> dict[str, Any]:
    """Reopen the one frozen pre-claim receipt admitted by this migration."""

    capture_id = str(EXACT_SAMPLES["GS-566"]["capture_event_id"])
    receipt_sha256 = queue.get("terminal_receipt_sha256")
    if (
        file_sha256(queue_path) != GS566_SOURCE_QUEUE_SHA256
        or queue.get("source_event_ids") != [capture_id]
        or queue.get("queue_status") != "pending"
        or queue.get("claimed_at") is not None
        or queue.get("finished_at") is not None
        or queue.get("terminal_outcome") != "failed"
        or queue.get("terminal_error_code")
        != GS566_PRECLAIM_FAILURE_ERROR
        or receipt_sha256 != GS566_PRECLAIM_FAILURE_RECEIPT_SHA256
        or source_binding.get("formal_id") != "GS-566"
        or source_binding.get("processing_attempt_number") != 1
        or source_binding.get("prior_terminal_receipt_sha256") is not None
        or source_binding.get("prior_terminal_receipt_path") is not None
    ):
        raise MathExactSmokeError(
            "math_migration_gs566_preclaim_evidence_invalid"
        )
    expected_receipt_path = (
        runtime_root
        / "dispatch/production-canary/receipts/math"
        / str(queue.get("activation_id") or "")
        / "sha256"
        / str(receipt_sha256)[:2]
        / f"{receipt_sha256}.json"
    ).resolve()
    receipt_payload, receipt_path = _read_content_bound_file(
        queue.get("terminal_receipt_path"),
        receipt_sha256,
        root=runtime_root / "dispatch/production-canary/receipts/math",
        code="math_migration_gs566_preclaim_evidence_invalid",
    )
    if receipt_path != expected_receipt_path:
        raise MathExactSmokeError(
            "math_migration_gs566_preclaim_evidence_invalid"
        )
    try:
        receipt = json.loads(receipt_payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise MathExactSmokeError(
            "math_migration_gs566_preclaim_evidence_invalid"
        ) from exc
    if not isinstance(receipt, Mapping):
        raise MathExactSmokeError(
            "math_migration_gs566_preclaim_evidence_invalid"
        )
    _verify_seal(
        receipt,
        purpose="dispatch-production-canary-preclaim-failure",
        key=key,
    )
    failure = receipt.get("failure_evidence")
    if (
        receipt.get("schema_version")
        != "study-intake-production-canary-preclaim-failure-receipt-v2"
        or receipt.get("subject") != "math"
        or receipt.get("release_id") != queue.get("release_id")
        or receipt.get("activation_id") != queue.get("activation_id")
        or receipt.get("producer_authority_fingerprint")
        != queue.get("producer_authority_fingerprint")
        or receipt.get("failure_stage") != "pre_claim"
        or receipt.get("error_code") != GS566_PRECLAIM_FAILURE_ERROR
        or not isinstance(receipt.get("failed_at"), str)
        or not receipt.get("failed_at")
        or receipt.get("queue_entry_preserved") is not True
        or receipt.get("queue_status_after") != "pending"
        or receipt.get("model_submission_started") is not False
        or receipt.get("model_call_count") != 0
        or receipt.get("provider_request_count") != 0
        or receipt.get("mcp_tool_call_count") != 0
        or receipt.get("formal_write_count") != 0
        or receipt.get("sol_enabled") is not False
        or not isinstance(failure, Mapping)
        or failure.get("queue_status_before") != "pending"
        or failure.get("capture_id") != capture_id
        or failure.get("unit_sha256") != queue.get("unit_sha256")
        or failure.get("frozen_payload_sha256")
        != queue.get("frozen_payload_sha256")
        or failure.get("producer_unit_id")
        != queue.get("producer_unit_id")
        or failure.get("producer_input_contract_sha256")
        != queue.get("producer_input_contract_sha256")
        or failure.get("source_event_set_sha256")
        != queue.get("source_event_set_sha256")
        or failure.get("lease_fence") is not None
    ):
        raise MathExactSmokeError(
            "math_migration_gs566_preclaim_evidence_invalid"
        )
    return {
        "receipt_path": str(receipt_path),
        "receipt_sha256": str(receipt_sha256),
        "failed_at": str(receipt["failed_at"]),
        "failure_stage": "pre_claim",
        "error_code": GS566_PRECLAIM_FAILURE_ERROR,
        "queue_entry_preserved": True,
        "queue_status_after": "pending",
        "model_submission_started": False,
        "model_call_count": 0,
        "provider_request_count": 0,
        "mcp_tool_call_count": 0,
        "formal_write_count": 0,
    }


def _pending_migration_source_mappings(
    *, runtime_root: Path, math_repo_root: Path, key: bytes
) -> list[dict[str, Any]]:
    committed_successor_paths = _committed_migration_target_queue_paths(
        runtime_root=runtime_root
    )
    capture_to_formal = {
        str(EXACT_SAMPLES[item]["capture_event_id"]): item
        for item in MIGRATION_ORDER
    }
    matches: dict[str, list[tuple[Path, dict[str, Any]]]] = {
        formal_id: [] for formal_id in MIGRATION_ORDER
    }
    queue_root = runtime_root / "dispatch/state/production-canary-queue/math"
    if queue_root.is_dir():
        for path in sorted(queue_root.glob("*/*.json")):
            queue = _read_json(path, "math_migration_source_queue_invalid")
            source_ids = queue.get("source_event_ids")
            formal_id = (
                capture_to_formal.get(str(source_ids[0]))
                if isinstance(source_ids, list) and len(source_ids) == 1
                else None
            )
            if formal_id is None:
                continue
            _verify_seal(
                queue, purpose="dispatch-production-canary-queue", key=key
            )
            if path.resolve() in committed_successor_paths:
                continue
            matches[formal_id].append((path.resolve(), queue))
    mappings: list[dict[str, Any]] = []
    common_identity: tuple[str, str, str] | None = None
    for formal_id in MIGRATION_ORDER:
        rows = matches[formal_id]
        if len(rows) != 1:
            raise MathExactSmokeError(
                "math_migration_source_queue_missing"
                if not rows
                else "math_migration_source_queue_duplicate"
            )
        queue_path, queue = rows[0]
        capture_id = str(EXACT_SAMPLES[formal_id]["capture_event_id"])
        if (
            queue.get("schema_version")
            != "study-intake-production-canary-queue-entry-v2"
            or queue.get("subject") != "math"
            or queue.get("source_event_ids") != [capture_id]
            or queue.get("queue_status") != "pending"
            or queue.get("claimed_at") is not None
            or queue.get("finished_at") is not None
            or queue.get("formal_write_count") != 0
        ):
            raise MathExactSmokeError("math_migration_source_queue_invalid")
        identity = (
            str(queue.get("release_id") or ""),
            str(queue.get("activation_id") or ""),
            str(queue.get("producer_authority_fingerprint") or ""),
        )
        for value in identity:
            _require_sha256(value, "math_migration_source_queue_invalid")
        if common_identity is None:
            common_identity = identity
        elif identity != common_identity:
            raise MathExactSmokeError("math_migration_source_authority_mixed")
        task_payload, task_path = _read_content_bound_file(
            queue.get("task_object_path"),
            queue.get("task_object_sha256"),
            root=runtime_root / "dispatch/production-canary/tasks/math",
            code="math_migration_source_task_invalid",
        )
        try:
            task_object = json.loads(task_payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise MathExactSmokeError(
                "math_migration_source_task_invalid"
            ) from exc
        frozen_payload = (
            task_object.get("frozen_payload")
            if isinstance(task_object, Mapping)
            else None
        )
        binding = (
            frozen_payload.get("math_exact_smoke_binding")
            if isinstance(frozen_payload, Mapping)
            else None
        )
        if (
            not isinstance(task_object, Mapping)
            or task_object.get("schema_version")
            != "study-intake-frozen-task-v1"
            or not isinstance(frozen_payload, Mapping)
            or not isinstance(binding, Mapping)
            or task_object.get("unit_sha256") != queue.get("unit_sha256")
            or sha256_bytes(canonical_bytes(frozen_payload))
            != queue.get("frozen_payload_sha256")
            or binding.get("schema_version") != TASK_BINDING_SCHEMA
            or binding.get("scope_id") != EXACT_SCOPE_ID
            or binding.get("formal_id") != formal_id
            or binding.get("capture_event_id") != capture_id
            or binding.get("processing_attempt_number") != 1
            or binding.get("prior_terminal_receipt_sha256") is not None
            or binding.get("prior_terminal_receipt_path") is not None
            or binding.get("target_release_id") != queue.get("release_id")
            or binding.get("activation_id") != queue.get("activation_id")
            or binding.get("producer_authority_fingerprint")
            != queue.get("producer_authority_fingerprint")
            or binding.get("expected_unit_sha256")
            != queue.get("unit_sha256")
            or binding.get("formal_write_count") != 0
            or binding.get("sol_enabled") is not False
        ):
            raise MathExactSmokeError("math_migration_source_task_invalid")
        attempt_id = binding.get("processing_attempt_id")
        attempt_idempotency_key = binding.get("attempt_idempotency_key")
        if (
            not isinstance(attempt_id, str)
            or not attempt_id.startswith("MATH-SMOKE-ATTEMPT-")
            or len(attempt_id) != len("MATH-SMOKE-ATTEMPT-") + 24
        ):
            raise MathExactSmokeError("math_migration_source_task_invalid")
        _require_sha256(
            attempt_idempotency_key, "math_migration_source_task_invalid"
        )
        if formal_id == "GS-566":
            preclaim_evidence: dict[str, Any] | None = (
                _gs566_source_preclaim_failure_evidence(
                    runtime_root=runtime_root,
                    queue_path=queue_path,
                    queue=queue,
                    source_binding=binding,
                    key=key,
                )
            )
        else:
            if (
                queue.get("terminal_outcome") is not None
                or queue.get("terminal_error_code") is not None
                or queue.get("terminal_receipt_sha256") is not None
                or queue.get("terminal_receipt_path") is not None
            ):
                raise MathExactSmokeError(
                    "math_migration_source_queue_invalid"
                )
            preclaim_evidence = None
        _reopen_capture_event(
            math_repo_root=math_repo_root,
            formal_id=formal_id,
            capture_event_id=capture_id,
        )
        apply_sha, apply_path, apply_receipt, auth_sha, auth_path, _ = (
            _exact_apply_receipt_for_capture(
                runtime_root=runtime_root,
                capture_event_id=capture_id,
                key=key,
            )
        )
        if (
            binding.get("capture_apply_receipt_sha256") != apply_sha
            or Path(str(binding.get("capture_apply_receipt_path"))).resolve()
            != apply_path.resolve()
            or binding.get("execution_authorization_sha256") != auth_sha
            or Path(str(binding.get("execution_authorization_path"))).resolve()
            != auth_path.resolve()
            or apply_receipt.get("formal_id") != formal_id
        ):
            raise MathExactSmokeError(
                "math_migration_source_evidence_binding_invalid"
            )
        mappings.append(
            {
                "formal_id": formal_id,
                "capture_event_id": capture_id,
                "source_queue_path": str(queue_path),
                "source_queue_sha256": file_sha256(queue_path),
                "source_task_object_path": str(task_path),
                "source_task_object_sha256": queue["task_object_sha256"],
                "source_unit_sha256": queue["unit_sha256"],
                "source_frozen_payload_sha256": queue[
                    "frozen_payload_sha256"
                ],
                "source_release_id": queue["release_id"],
                "source_activation_id": queue["activation_id"],
                "source_producer_authority_fingerprint": queue[
                    "producer_authority_fingerprint"
                ],
                "source_authority_generation": binding[
                    "authority_generation"
                ],
                "source_authority_fingerprint": binding[
                    "authority_fingerprint"
                ],
                "source_execution_authorization_sha256": auth_sha,
                "source_execution_authorization_path": str(auth_path),
                "source_capture_apply_receipt_sha256": apply_sha,
                "source_capture_apply_receipt_path": str(apply_path),
                "source_task_binding_sha256": sha256_bytes(
                    canonical_bytes(binding)
                ),
                "source_processing_attempt_id": attempt_id,
                "source_attempt_idempotency_key": attempt_idempotency_key,
                "source_preclaim_failure_evidence": preclaim_evidence,
            }
        )
    return mappings


def _committed_migration_target_queue_paths(
    *, runtime_root: Path
) -> set[Path]:
    root = _migration_commit_root(runtime_root)
    content_root = root / "sha256"
    if not content_root.exists():
        return set()
    key = _read_authority_key(runtime_root)
    paths: set[Path] = set()
    for commit_path in sorted(content_root.glob("*/*.json")):
        if commit_path.is_symlink() or not commit_path.is_file():
            raise MathExactSmokeError("math_migration_commit_invalid")
        digest = commit_path.stem
        if (
            len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
            or commit_path.parent.name != digest[:2]
            or file_sha256(commit_path) != digest
        ):
            raise MathExactSmokeError("math_migration_commit_invalid")
        commit = _read_json(
            commit_path, "math_migration_commit_invalid"
        )
        _verify_seal(commit, purpose=MIGRATION_COMMIT_PURPOSE, key=key)
        rows = commit.get("queue_mappings")
        target_release_id = commit.get("target_release_id")
        target_activation_id = commit.get("target_activation_id")
        if (
            commit.get("schema_version") != MIGRATION_COMMIT_SCHEMA
            or commit.get("scope_id") != EXACT_SCOPE_ID
            or len(str(target_release_id or "")) != 64
            or any(
                char not in "0123456789abcdef"
                for char in str(target_release_id or "")
            )
            or len(str(target_activation_id or "")) != 64
            or any(
                char not in "0123456789abcdef"
                for char in str(target_activation_id or "")
            )
            or commit.get("task_count") != 4
            or commit.get("processing_attempt_number") != 1
            or commit.get("capture_write_count") != 0
            or commit.get("model_call_count") != 0
            or commit.get("provider_request_count") != 0
            or commit.get("formal_write_count") != 0
            or commit.get("sol_enabled") is not False
            or not isinstance(rows, list)
            or [row.get("formal_id") for row in rows]
            != list(MIGRATION_ORDER)
        ):
            raise MathExactSmokeError("math_migration_commit_invalid")
        target_root = (
            runtime_root
            / "dispatch/state/production-canary-queue/math"
            / str(target_activation_id)
        ).resolve()
        for row in rows:
            if not isinstance(row, Mapping):
                raise MathExactSmokeError("math_migration_commit_invalid")
            queue_path = Path(str(row.get("target_queue_path") or ""))
            if not queue_path.is_absolute():
                raise MathExactSmokeError("math_migration_commit_invalid")
            try:
                queue_path.resolve().relative_to(target_root)
            except ValueError as exc:
                raise MathExactSmokeError(
                    "math_migration_target_queue_drift"
                ) from exc
            paths.add(queue_path.resolve())
    return paths


def _committed_migration_descriptor_sha256(
    *, runtime_root: Path
) -> str | None:
    pointer_path = _migration_commit_pointer_path(runtime_root)
    if not pointer_path.exists():
        return None
    pointer = _read_json(pointer_path, "math_migration_commit_invalid")
    key = _read_authority_key(runtime_root)
    _verify_seal(
        pointer,
        purpose="dispatch-math-pending-queue-migration-commit-pointer",
        key=key,
    )
    return _require_sha256(
        pointer.get("migration_descriptor_sha256"),
        "math_migration_commit_invalid",
    )


def _active_pending_migration_for_release(
    *, runtime_root: Path, release_id: str
) -> tuple[str, Path, dict[str, Any]] | None:
    active = _reopen_active_migration_descriptor(runtime_root=runtime_root)
    if active is None:
        return None
    if active[2].get("target_release_id") == release_id:
        return active
    if (
        _committed_migration_descriptor_sha256(
            runtime_root=runtime_root
        )
        == active[0]
    ):
        return None
    raise MathExactSmokeError("math_migration_descriptor_invalid")


def preview_exact_math_pending_queue_migration(
    *, runtime_root: Path, math_repo_root: Path, target_release_id: str
) -> dict[str, Any]:
    """Read-only proof for the one fixed four-queue migration."""

    if not runtime_root.is_absolute() or not math_repo_root.is_absolute():
        raise MathExactSmokeError("math_migration_root_invalid")
    _require_sha256(target_release_id, "math_migration_target_release_invalid")
    runtime_root = runtime_root.resolve()
    math_repo_root = math_repo_root.resolve()
    key = _read_authority_key(runtime_root)
    mappings = _pending_migration_source_mappings(
        runtime_root=runtime_root, math_repo_root=math_repo_root, key=key
    )
    if any(row["source_release_id"] == target_release_id for row in mappings):
        raise MathExactSmokeError("math_migration_target_release_not_successor")
    intent = _seal(
        {
            "schema_version": MIGRATION_INTENT_SCHEMA,
            "scope_id": EXACT_SCOPE_ID,
            "target_release_id": target_release_id,
            "math_repo_root": str(math_repo_root),
            "gs269_archived_evidence": _completed_gs269_archived_evidence(
                runtime_root=runtime_root, key=key
            ),
            "source_queue_mappings": mappings,
            "task_count": 4,
            "processing_attempt_number": 1,
            "capture_write_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        },
        purpose=MIGRATION_INTENT_PURPOSE,
        key=key,
    )
    digest = sha256_bytes(canonical_bytes(intent) + b"\n")
    return {
        "schema_version": (
            "study-intake-math-pending-queue-migration-preview-v1"
        ),
        "status": "ready",
        "migration_intent": intent,
        "migration_intent_sha256": digest,
        "source_queue_count": 4,
        "target_queue_write_count": 0,
        "model_call_count": 0,
        "provider_request_count": 0,
        "formal_write_count": 0,
        "sol_enabled": False,
        "read_only": True,
    }


def prepare_exact_math_pending_queue_migration(
    *, runtime_root: Path, math_repo_root: Path, target_release_id: str
) -> dict[str, Any]:
    preview = preview_exact_math_pending_queue_migration(
        runtime_root=runtime_root,
        math_repo_root=math_repo_root,
        target_release_id=target_release_id,
    )
    digest, path = _publish_content_addressed(
        _migration_intent_root(runtime_root), preview["migration_intent"]
    )
    if digest != preview["migration_intent_sha256"]:
        raise MathExactSmokeError("math_migration_intent_digest_drift")
    return {
        "schema_version": (
            "study-intake-math-pending-queue-migration-prepare-v1"
        ),
        "status": "prepared",
        "migration_intent_sha256": digest,
        "migration_intent_path": str(path),
        "source_queue_count": 4,
        "target_queue_write_count": 0,
        "model_call_count": 0,
        "provider_request_count": 0,
        "formal_write_count": 0,
        "sol_enabled": False,
    }


def _reopen_migration_intent(
    *, runtime_root: Path, intent_sha256: str
) -> tuple[Path, dict[str, Any]]:
    _require_sha256(intent_sha256, "math_migration_intent_invalid")
    root = _migration_intent_root(runtime_root)
    path = root / "sha256" / intent_sha256[:2] / f"{intent_sha256}.json"
    digest, intent = _verify_content_addressed_path(
        path, root=root, expected_sha256=intent_sha256
    )
    key = _read_authority_key(runtime_root)
    _verify_seal(intent, purpose=MIGRATION_INTENT_PURPOSE, key=key)
    mappings = intent.get("source_queue_mappings")
    if (
        digest != intent_sha256
        or intent.get("schema_version") != MIGRATION_INTENT_SCHEMA
        or intent.get("scope_id") != EXACT_SCOPE_ID
        or intent.get("task_count") != 4
        or intent.get("processing_attempt_number") != 1
        or intent.get("capture_write_count") != 0
        or intent.get("model_call_count") != 0
        or intent.get("provider_request_count") != 0
        or intent.get("formal_write_count") != 0
        or intent.get("sol_enabled") is not False
        or not isinstance(mappings, list)
        or [row.get("formal_id") for row in mappings]
        != list(MIGRATION_ORDER)
    ):
        raise MathExactSmokeError("math_migration_intent_invalid")
    expected = preview_exact_math_pending_queue_migration(
        runtime_root=runtime_root,
        math_repo_root=Path(str(intent.get("math_repo_root") or "")),
        target_release_id=str(intent.get("target_release_id") or ""),
    )
    if (
        expected["migration_intent_sha256"] != intent_sha256
        or canonical_bytes(expected["migration_intent"])
        != canonical_bytes(intent)
    ):
        raise MathExactSmokeError("math_migration_intent_source_drift")
    return path.resolve(), intent


def stage_exact_math_pending_queue_migration(
    *,
    runtime_root: Path,
    migration_intent_sha256: str,
    state: Mapping[str, Any],
    authority_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind the prepared source intent to the disabled target activation."""

    intent_path, intent = _reopen_migration_intent(
        runtime_root=runtime_root, intent_sha256=migration_intent_sha256
    )
    target_release_id = str(state.get("release_id") or "")
    target_activation_id = str(state.get("activation_id") or "")
    target_producer = str(
        state.get("producer_authority_fingerprint") or ""
    )
    target_generation = str(authority_snapshot.get("generation") or "")
    target_subject_authority = str(
        authority_snapshot.get("authority_fingerprint") or ""
    )
    for value in (
        target_release_id,
        target_activation_id,
        target_producer,
        target_subject_authority,
    ):
        _require_sha256(value, "math_migration_target_authority_invalid")
    if (
        state.get("subject") != "math"
        or target_release_id != intent.get("target_release_id")
        or state.get("luna_consumer_enabled") is not False
        or state.get("active_task_count") != 0
        or state.get("formal_write_count") != 0
        or state.get("sol_enabled") is not False
        or authority_snapshot.get("schema_version")
        != "subject_authority_snapshot_v1"
        or authority_snapshot.get("subject") != "math"
        or not target_generation
        or authority_snapshot.get("model_call_count") != 0
        or authority_snapshot.get("formal_write_count") != 0
    ):
        raise MathExactSmokeError("math_migration_target_authority_invalid")
    descriptor = _seal(
        {
            "schema_version": MIGRATION_DESCRIPTOR_SCHEMA,
            "scope_id": EXACT_SCOPE_ID,
            "migration_intent_sha256": migration_intent_sha256,
            "migration_intent_path": str(intent_path),
            "target_release_id": target_release_id,
            "target_activation_id": target_activation_id,
            "target_authority_generation": target_generation,
            "target_subject_authority_fingerprint": (
                target_subject_authority
            ),
            "target_producer_authority_fingerprint": target_producer,
            "gs269_archived_evidence": copy.deepcopy(
                intent["gs269_archived_evidence"]
            ),
            "source_queue_mappings": copy.deepcopy(
                intent["source_queue_mappings"]
            ),
            "task_count": 4,
            "processing_attempt_number": 1,
            "capture_write_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        },
        purpose=MIGRATION_DESCRIPTOR_PURPOSE,
        key=_read_authority_key(runtime_root),
    )
    digest, path = _publish_content_addressed(
        _migration_descriptor_root(runtime_root), descriptor
    )
    pointer = _seal(
        {
            "schema_version": (
                "study-intake-math-pending-queue-migration-pointer-v1"
            ),
            "scope_id": EXACT_SCOPE_ID,
            "migration_descriptor_sha256": digest,
            "migration_descriptor_path": str(path),
            "target_release_id": target_release_id,
            "target_activation_id": target_activation_id,
            "formal_write_count": 0,
        },
        purpose="dispatch-math-pending-queue-migration-pointer",
        key=_read_authority_key(runtime_root),
    )
    _publish_active_migration_pointer(
        runtime_root=runtime_root,
        pointer=pointer,
        key=_read_authority_key(runtime_root),
    )
    pointer_path = _migration_active_pointer_path(runtime_root)
    return {
        "schema_version": (
            "study-intake-math-pending-queue-migration-stage-v1"
        ),
        "status": "staged",
        "migration_descriptor_sha256": digest,
        "migration_descriptor_path": str(path),
        "active_pointer_path": str(pointer_path),
        "target_release_id": target_release_id,
        "target_activation_id": target_activation_id,
        "target_authority_generation": target_generation,
        "target_subject_authority_fingerprint": target_subject_authority,
        "target_producer_authority_fingerprint": target_producer,
        "task_count": 4,
        "model_call_count": 0,
        "provider_request_count": 0,
        "formal_write_count": 0,
        "sol_enabled": False,
    }


def _reopen_active_migration_descriptor(
    *, runtime_root: Path, expected_release_id: str | None = None
) -> tuple[str, Path, dict[str, Any]] | None:
    pointer_path = _migration_active_pointer_path(runtime_root)
    if not pointer_path.exists():
        return None
    pointer = _read_json(
        pointer_path, "math_migration_active_pointer_invalid"
    )
    key = _read_authority_key(runtime_root)
    _verify_seal(
        pointer,
        purpose="dispatch-math-pending-queue-migration-pointer",
        key=key,
    )
    digest = _require_sha256(
        pointer.get("migration_descriptor_sha256"),
        "math_migration_active_pointer_invalid",
    )
    root = _migration_descriptor_root(runtime_root)
    path = root / "sha256" / digest[:2] / f"{digest}.json"
    reopened_digest, descriptor = _verify_content_addressed_path(
        path, root=root, expected_sha256=digest
    )
    _verify_seal(
        descriptor, purpose=MIGRATION_DESCRIPTOR_PURPOSE, key=key
    )
    if (
        reopened_digest != digest
        or pointer.get("schema_version")
        != "study-intake-math-pending-queue-migration-pointer-v1"
        or pointer.get("migration_descriptor_path") != str(path)
        or pointer.get("target_release_id")
        != descriptor.get("target_release_id")
        or pointer.get("target_activation_id")
        != descriptor.get("target_activation_id")
        or pointer.get("formal_write_count") != 0
        or descriptor.get("schema_version")
        != MIGRATION_DESCRIPTOR_SCHEMA
        or descriptor.get("scope_id") != EXACT_SCOPE_ID
        or descriptor.get("task_count") != 4
        or descriptor.get("processing_attempt_number") != 1
        or descriptor.get("formal_write_count") != 0
        or descriptor.get("sol_enabled") is not False
        or (
            expected_release_id is not None
            and descriptor.get("target_release_id") != expected_release_id
        )
    ):
        raise MathExactSmokeError("math_migration_descriptor_invalid")
    return digest, path.resolve(), descriptor


def _active_migration_mapping_for_capture(
    *, runtime_root: Path, release_id: str, capture_event_id: str
) -> tuple[str, dict[str, Any], dict[str, Any]] | None:
    reopened = _reopen_active_migration_descriptor(
        runtime_root=runtime_root, expected_release_id=release_id
    )
    if reopened is None:
        return None
    digest, _path, descriptor = reopened
    rows = [
        row
        for row in descriptor.get("source_queue_mappings") or []
        if isinstance(row, Mapping)
        and row.get("capture_event_id") == capture_event_id
    ]
    if not rows:
        return None
    if len(rows) != 1:
        raise MathExactSmokeError("math_migration_descriptor_invalid")
    return digest, descriptor, dict(rows[0])


def validate_exact_math_migration_materialization(
    runtime_root: Path,
    tasks: Any,
    state: Mapping[str, Any],
    descriptor: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the exact collision exception used by LeaseStore."""

    if not runtime_root.is_absolute():
        raise MathExactSmokeError("math_migration_root_invalid")
    key = _read_authority_key(runtime_root)
    _verify_seal(
        descriptor, purpose=MIGRATION_DESCRIPTOR_PURPOSE, key=key
    )
    descriptor_sha256 = sha256_bytes(canonical_bytes(descriptor) + b"\n")
    mappings = descriptor.get("source_queue_mappings")
    gs269_evidence = descriptor.get("gs269_archived_evidence")
    task_rows = list(tasks) if not isinstance(tasks, list) else tasks
    if (
        descriptor.get("schema_version") != MIGRATION_DESCRIPTOR_SCHEMA
        or descriptor.get("scope_id") != EXACT_SCOPE_ID
        or descriptor.get("target_release_id") != state.get("release_id")
        or descriptor.get("target_activation_id")
        != state.get("activation_id")
        or descriptor.get("target_producer_authority_fingerprint")
        != state.get("producer_authority_fingerprint")
        or state.get("subject") != "math"
        or state.get("luna_consumer_enabled") is not False
        or state.get("active_task_count") != 0
        or state.get("formal_write_count") != 0
        or state.get("sol_enabled") is not False
        or descriptor.get("task_count") != 4
        or descriptor.get("processing_attempt_number") != 1
        or descriptor.get("capture_write_count") != 0
        or descriptor.get("model_call_count") != 0
        or descriptor.get("provider_request_count") != 0
        or descriptor.get("formal_write_count") != 0
        or descriptor.get("sol_enabled") is not False
        or not isinstance(gs269_evidence, Mapping)
        or gs269_evidence.get("capture_event_id")
        != EXACT_SAMPLES["GS-269"]["capture_event_id"]
        or gs269_evidence.get("formal_write_count") != 0
        or not isinstance(
            gs269_evidence.get("accepted_queue_sha256"), str
        )
        or not isinstance(gs269_evidence.get("archive_sha256"), str)
        or not isinstance(mappings, list)
        or [row.get("formal_id") for row in mappings]
        != list(MIGRATION_ORDER)
        or len(task_rows) != 4
    ):
        raise MathExactSmokeError("math_migration_materialization_invalid")
    _require_sha256(
        gs269_evidence.get("accepted_queue_sha256"),
        "math_migration_materialization_invalid",
    )
    _require_sha256(
        gs269_evidence.get("archive_sha256"),
        "math_migration_materialization_invalid",
    )
    for task, mapping in zip(task_rows, mappings):
        frozen_payload = getattr(task, "frozen_payload", None)
        unit_sha256 = getattr(task, "unit_sha256", None)
        frozen_sha256 = getattr(task, "frozen_payload_sha256", None)
        binding = (
            frozen_payload.get("math_exact_smoke_binding")
            if isinstance(frozen_payload, Mapping)
            else None
        )
        source_queue_path = Path(str(mapping.get("source_queue_path") or ""))
        source_task_path = Path(
            str(mapping.get("source_task_object_path") or "")
        )
        if (
            not source_queue_path.is_absolute()
            or not source_task_path.is_absolute()
        ):
            raise MathExactSmokeError(
                "math_migration_source_mapping_invalid"
            )
        source_queue_payload, resolved_queue_path = _read_content_bound_file(
            str(source_queue_path),
            mapping.get("source_queue_sha256"),
            root=(
                runtime_root
                / "dispatch/state/production-canary-queue/math"
            ),
            code="math_migration_source_queue_drift",
        )
        source_task_payload, resolved_task_path = _read_content_bound_file(
            str(source_task_path),
            mapping.get("source_task_object_sha256"),
            root=runtime_root / "dispatch/production-canary/tasks/math",
            code="math_migration_source_task_drift",
        )
        try:
            source_queue = json.loads(source_queue_payload.decode("utf-8"))
            source_task = json.loads(source_task_payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise MathExactSmokeError(
                "math_migration_source_mapping_invalid"
            ) from exc
        _verify_seal(
            source_queue,
            purpose="dispatch-production-canary-queue",
            key=key,
        )
        source_frozen = (
            source_task.get("frozen_payload")
            if isinstance(source_task, Mapping)
            else None
        )
        source_binding = (
            source_frozen.get("math_exact_smoke_binding")
            if isinstance(source_frozen, Mapping)
            else None
        )
        if not isinstance(source_binding, Mapping):
            raise MathExactSmokeError(
                "math_migration_materialization_invalid"
            )
        if mapping.get("formal_id") == "GS-566":
            source_preclaim_evidence: dict[str, Any] | None = (
                _gs566_source_preclaim_failure_evidence(
                    runtime_root=runtime_root,
                    queue_path=resolved_queue_path,
                    queue=source_queue,
                    source_binding=source_binding,
                    key=key,
                )
            )
        else:
            if (
                source_queue.get("terminal_outcome") is not None
                or source_queue.get("terminal_error_code") is not None
                or source_queue.get("terminal_receipt_sha256") is not None
                or source_queue.get("terminal_receipt_path") is not None
            ):
                raise MathExactSmokeError(
                    "math_migration_materialization_invalid"
                )
            source_preclaim_evidence = None
        if (
            not isinstance(binding, Mapping)
            or binding.get("schema_version") != TASK_BINDING_V2_SCHEMA
            or binding.get("formal_id") != mapping.get("formal_id")
            or binding.get("capture_event_id")
            != mapping.get("capture_event_id")
            or binding.get("processing_attempt_number") != 1
            or binding.get("prior_terminal_receipt_sha256") is not None
            or binding.get("prior_terminal_receipt_path") is not None
            or binding.get("migration_descriptor_sha256")
            != descriptor_sha256
            or binding.get("source_queue_path")
            != str(resolved_queue_path)
            or binding.get("source_queue_sha256")
            != mapping.get("source_queue_sha256")
            or binding.get("source_task_object_path")
            != str(resolved_task_path)
            or binding.get("source_task_object_sha256")
            != mapping.get("source_task_object_sha256")
            or binding.get("source_execution_authorization_sha256")
            != mapping.get("source_execution_authorization_sha256")
            or binding.get("source_capture_apply_receipt_sha256")
            != mapping.get("source_capture_apply_receipt_sha256")
            or binding.get("processing_attempt_id")
            != mapping.get("source_processing_attempt_id")
            or binding.get("attempt_idempotency_key")
            != mapping.get("source_attempt_idempotency_key")
            or canonical_bytes(
                binding.get("source_preclaim_failure_evidence")
            )
            != canonical_bytes(source_preclaim_evidence)
            or canonical_bytes(
                mapping.get("source_preclaim_failure_evidence")
            )
            != canonical_bytes(source_preclaim_evidence)
            or binding.get("target_release_id")
            != descriptor.get("target_release_id")
            or binding.get("activation_id")
            != descriptor.get("target_activation_id")
            or binding.get("authority_generation")
            != descriptor.get("target_authority_generation")
            or binding.get("authority_fingerprint")
            != descriptor.get("target_subject_authority_fingerprint")
            or binding.get("producer_authority_fingerprint")
            != descriptor.get("target_producer_authority_fingerprint")
            or binding.get("expected_unit_sha256") != unit_sha256
            or binding.get("formal_write_count") != 0
            or binding.get("sol_enabled") is not False
            or source_task.get("schema_version")
            != "study-intake-frozen-task-v1"
            or source_task.get("unit_sha256")
            != mapping.get("source_unit_sha256")
            or not isinstance(source_frozen, Mapping)
            or sha256_bytes(canonical_bytes(source_frozen))
            != mapping.get("source_frozen_payload_sha256")
            or source_binding.get("schema_version") != TASK_BINDING_SCHEMA
            or source_binding.get("formal_id") != mapping.get("formal_id")
            or source_binding.get("capture_event_id")
            != mapping.get("capture_event_id")
            or source_binding.get("processing_attempt_number") != 1
            or source_binding.get("prior_terminal_receipt_sha256") is not None
            or source_binding.get("prior_terminal_receipt_path") is not None
            or sha256_bytes(canonical_bytes(source_binding))
            != mapping.get("source_task_binding_sha256")
            or source_binding.get("processing_attempt_id")
            != mapping.get("source_processing_attempt_id")
            or source_binding.get("attempt_idempotency_key")
            != mapping.get("source_attempt_idempotency_key")
            or source_queue.get("queue_status") != "pending"
            or source_queue.get("claimed_at") is not None
            or source_queue.get("finished_at") is not None
            or source_queue.get("unit_sha256")
            != mapping.get("source_unit_sha256")
            or source_queue.get("frozen_payload_sha256")
            != mapping.get("source_frozen_payload_sha256")
            or source_queue.get("formal_write_count") != 0
            or not isinstance(frozen_sha256, str)
            or frozen_sha256
            != sha256_bytes(canonical_bytes(frozen_payload))
        ):
            raise MathExactSmokeError(
                "math_migration_materialization_invalid"
            )
    return {
        "migration_descriptor_sha256": descriptor_sha256,
        "gs269_archived_evidence_sha256": sha256_bytes(
            canonical_bytes(gs269_evidence)
        ),
        "target_release_id": descriptor["target_release_id"],
        "target_activation_id": descriptor["target_activation_id"],
        "target_authority_generation": descriptor[
            "target_authority_generation"
        ],
        "target_subject_authority_fingerprint": descriptor[
            "target_subject_authority_fingerprint"
        ],
        "target_producer_authority_fingerprint": descriptor[
            "target_producer_authority_fingerprint"
        ],
        "source_queue_mappings": copy.deepcopy(mappings),
    }


def publish_exact_math_pending_queue_migration_commit(
    *,
    runtime_root: Path,
    migration_descriptor_sha256: str,
    queue_rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    active = _reopen_active_migration_descriptor(runtime_root=runtime_root)
    if active is None or active[0] != migration_descriptor_sha256:
        raise MathExactSmokeError("math_migration_descriptor_not_active")
    _digest, descriptor_path, descriptor = active
    if len(queue_rows) != 4:
        raise MathExactSmokeError("math_migration_target_queue_count_invalid")
    key = _read_authority_key(runtime_root)
    target_rows: list[dict[str, Any]] = []
    for expected_formal_id, source, returned in zip(
        MIGRATION_ORDER, descriptor["source_queue_mappings"], queue_rows
    ):
        queue_path = Path(str(returned.get("queue_entry_path") or ""))
        queue_sha256 = returned.get("queue_entry_sha256")
        payload, resolved_queue_path = _read_content_bound_file(
            str(queue_path),
            queue_sha256,
            root=(
                runtime_root
                / "dispatch/state/production-canary-queue/math"
                / str(descriptor["target_activation_id"])
            ),
            code="math_migration_target_queue_invalid",
        )
        try:
            queue = json.loads(payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise MathExactSmokeError(
                "math_migration_target_queue_invalid"
            ) from exc
        _verify_seal(
            queue, purpose="dispatch-production-canary-queue", key=key
        )
        task_payload, task_path = _read_content_bound_file(
            queue.get("task_object_path"),
            queue.get("task_object_sha256"),
            root=runtime_root / "dispatch/production-canary/tasks/math",
            code="math_migration_target_task_invalid",
        )
        try:
            task_object = json.loads(task_payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise MathExactSmokeError(
                "math_migration_target_task_invalid"
            ) from exc
        frozen = (
            task_object.get("frozen_payload")
            if isinstance(task_object, Mapping)
            else None
        )
        binding = (
            frozen.get("math_exact_smoke_binding")
            if isinstance(frozen, Mapping)
            else None
        )
        if (
            source.get("formal_id") != expected_formal_id
            or not isinstance(queue, Mapping)
            or queue.get("schema_version")
            != "study-intake-production-canary-queue-entry-v2"
            or queue.get("subject") != "math"
            or queue.get("release_id")
            != descriptor.get("target_release_id")
            or queue.get("activation_id")
            != descriptor.get("target_activation_id")
            or queue.get("producer_authority_fingerprint")
            != descriptor.get("target_producer_authority_fingerprint")
            or queue.get("source_event_ids")
            != [source.get("capture_event_id")]
            or queue.get("queue_status") != "pending"
            or queue.get("terminal_outcome") is not None
            or queue.get("terminal_receipt_sha256") is not None
            or queue.get("formal_write_count") != 0
            or not isinstance(task_object, Mapping)
            or not isinstance(frozen, Mapping)
            or not isinstance(binding, Mapping)
            or task_object.get("unit_sha256") != queue.get("unit_sha256")
            or sha256_bytes(canonical_bytes(frozen))
            != queue.get("frozen_payload_sha256")
            or binding.get("formal_id") != expected_formal_id
            or binding.get("migration_descriptor_sha256")
            != migration_descriptor_sha256
            or binding.get("processing_attempt_number") != 1
            or binding.get("processing_attempt_id")
            != source.get("source_processing_attempt_id")
            or binding.get("attempt_idempotency_key")
            != source.get("source_attempt_idempotency_key")
            or binding.get("prior_terminal_receipt_sha256") is not None
            or binding.get("prior_terminal_receipt_path") is not None
            or binding.get("source_queue_sha256")
            != source.get("source_queue_sha256")
            or binding.get("source_task_object_sha256")
            != source.get("source_task_object_sha256")
            or canonical_bytes(
                binding.get("source_preclaim_failure_evidence")
            )
            != canonical_bytes(
                source.get("source_preclaim_failure_evidence")
            )
        ):
            raise MathExactSmokeError("math_migration_target_queue_invalid")
        target_rows.append(
            {
                "formal_id": expected_formal_id,
                "capture_event_id": source["capture_event_id"],
                "source_queue_path": source["source_queue_path"],
                "source_queue_sha256": source["source_queue_sha256"],
                "source_processing_attempt_id": source[
                    "source_processing_attempt_id"
                ],
                "source_attempt_idempotency_key": source[
                    "source_attempt_idempotency_key"
                ],
                "source_preclaim_failure_evidence": copy.deepcopy(
                    source["source_preclaim_failure_evidence"]
                ),
                "target_queue_path": str(resolved_queue_path),
                "target_queue_sha256": str(queue_sha256),
                "target_task_object_path": str(task_path),
                "target_task_object_sha256": queue[
                    "task_object_sha256"
                ],
                "target_unit_sha256": queue["unit_sha256"],
                "target_frozen_payload_sha256": queue[
                    "frozen_payload_sha256"
                ],
                "producer_input_contract_sha256": queue[
                    "producer_input_contract_sha256"
                ],
            }
        )
    commit = _seal(
        {
            "schema_version": MIGRATION_COMMIT_SCHEMA,
            "scope_id": EXACT_SCOPE_ID,
            "migration_descriptor_sha256": migration_descriptor_sha256,
            "migration_descriptor_path": str(descriptor_path),
            "target_release_id": descriptor["target_release_id"],
            "target_activation_id": descriptor["target_activation_id"],
            "target_authority_generation": descriptor[
                "target_authority_generation"
            ],
            "target_subject_authority_fingerprint": descriptor[
                "target_subject_authority_fingerprint"
            ],
            "target_producer_authority_fingerprint": descriptor[
                "target_producer_authority_fingerprint"
            ],
            "gs269_archived_evidence": copy.deepcopy(
                descriptor["gs269_archived_evidence"]
            ),
            "queue_mappings": target_rows,
            "task_count": 4,
            "processing_attempt_number": 1,
            "capture_write_count": 0,
            "model_call_count": 0,
            "provider_request_count": 0,
            "formal_write_count": 0,
            "sol_enabled": False,
        },
        purpose=MIGRATION_COMMIT_PURPOSE,
        key=key,
    )
    commit_sha256, commit_path = _publish_content_addressed(
        _migration_commit_root(runtime_root), commit
    )
    pointer = _seal(
        {
            "schema_version": (
                "study-intake-math-pending-queue-migration-commit-pointer-v1"
            ),
            "scope_id": EXACT_SCOPE_ID,
            "migration_descriptor_sha256": migration_descriptor_sha256,
            "migration_commit_sha256": commit_sha256,
            "migration_commit_path": str(commit_path),
            "target_release_id": descriptor["target_release_id"],
            "target_activation_id": descriptor["target_activation_id"],
            "formal_write_count": 0,
        },
        purpose="dispatch-math-pending-queue-migration-commit-pointer",
        key=key,
    )
    _publish_migration_commit_pointer(
        runtime_root=runtime_root, pointer=pointer, key=key
    )
    reopened = reopen_exact_math_pending_queue_migration_commit(
        runtime_root=runtime_root,
        migration_descriptor_sha256=migration_descriptor_sha256,
        required=True,
    )
    assert reopened is not None
    return reopened


def reopen_exact_math_pending_queue_migration_commit(
    *,
    runtime_root: Path,
    migration_descriptor_sha256: str,
    required: bool = True,
    validate_target_queues: bool = True,
) -> dict[str, Any] | None:
    _require_sha256(
        migration_descriptor_sha256, "math_migration_descriptor_invalid"
    )
    pointer_path = _migration_commit_pointer_path(runtime_root)
    if not pointer_path.exists():
        if required:
            raise MathExactSmokeError("math_migration_commit_missing")
        return None
    pointer = _read_json(pointer_path, "math_migration_commit_invalid")
    key = _read_authority_key(runtime_root)
    _verify_seal(
        pointer,
        purpose="dispatch-math-pending-queue-migration-commit-pointer",
        key=key,
    )
    commit_sha256 = _require_sha256(
        pointer.get("migration_commit_sha256"),
        "math_migration_commit_invalid",
    )
    root = _migration_commit_root(runtime_root)
    path = root / "sha256" / commit_sha256[:2] / f"{commit_sha256}.json"
    reopened_sha256, commit = _verify_content_addressed_path(
        path, root=root, expected_sha256=commit_sha256
    )
    _verify_seal(commit, purpose=MIGRATION_COMMIT_PURPOSE, key=key)
    rows = commit.get("queue_mappings")
    descriptor_root = _migration_descriptor_root(runtime_root)
    descriptor_path = (
        descriptor_root
        / "sha256"
        / migration_descriptor_sha256[:2]
        / f"{migration_descriptor_sha256}.json"
    )
    descriptor_reopened_sha256, descriptor = (
        _verify_content_addressed_path(
            descriptor_path,
            root=descriptor_root,
            expected_sha256=migration_descriptor_sha256,
        )
    )
    _verify_seal(
        descriptor, purpose=MIGRATION_DESCRIPTOR_PURPOSE, key=key
    )
    descriptor_rows = (
        descriptor.get("source_queue_mappings")
        if isinstance(descriptor, Mapping)
        else None
    )
    if (
        reopened_sha256 != commit_sha256
        or pointer.get("schema_version")
        != "study-intake-math-pending-queue-migration-commit-pointer-v1"
        or pointer.get("migration_descriptor_sha256")
        != migration_descriptor_sha256
        or pointer.get("migration_commit_path") != str(path)
        or pointer.get("formal_write_count") != 0
        or commit.get("schema_version") != MIGRATION_COMMIT_SCHEMA
        or commit.get("scope_id") != EXACT_SCOPE_ID
        or commit.get("migration_descriptor_sha256")
        != migration_descriptor_sha256
        or commit.get("migration_descriptor_path") != str(descriptor_path)
        or descriptor_reopened_sha256 != migration_descriptor_sha256
        or descriptor.get("schema_version") != MIGRATION_DESCRIPTOR_SCHEMA
        or descriptor.get("scope_id") != EXACT_SCOPE_ID
        or commit.get("task_count") != 4
        or commit.get("processing_attempt_number") != 1
        or commit.get("capture_write_count") != 0
        or commit.get("model_call_count") != 0
        or commit.get("provider_request_count") != 0
        or commit.get("formal_write_count") != 0
        or commit.get("sol_enabled") is not False
        or not isinstance(rows, list)
        or [row.get("formal_id") for row in rows]
        != list(MIGRATION_ORDER)
        or not isinstance(descriptor_rows, list)
        or len(descriptor_rows) != 4
    ):
        raise MathExactSmokeError("math_migration_commit_invalid")
    for row, source in zip(rows, descriptor_rows):
        if (
            row.get("formal_id") != source.get("formal_id")
            or row.get("capture_event_id")
            != source.get("capture_event_id")
            or row.get("source_queue_path")
            != source.get("source_queue_path")
            or row.get("source_queue_sha256")
            != source.get("source_queue_sha256")
            or row.get("source_processing_attempt_id")
            != source.get("source_processing_attempt_id")
            or row.get("source_attempt_idempotency_key")
            != source.get("source_attempt_idempotency_key")
            or canonical_bytes(
                row.get("source_preclaim_failure_evidence")
            )
            != canonical_bytes(
                source.get("source_preclaim_failure_evidence")
            )
        ):
            raise MathExactSmokeError("math_migration_commit_invalid")
        _read_content_bound_file(
            row.get("source_queue_path"),
            row.get("source_queue_sha256"),
            root=(
                runtime_root
                / "dispatch/state/production-canary-queue/math"
            ),
            code="math_migration_source_queue_drift",
        )
        preclaim = row.get("source_preclaim_failure_evidence")
        if row.get("formal_id") == "GS-566":
            if (
                not isinstance(preclaim, Mapping)
                or preclaim.get("receipt_sha256")
                != GS566_PRECLAIM_FAILURE_RECEIPT_SHA256
            ):
                raise MathExactSmokeError("math_migration_commit_invalid")
            _read_content_bound_file(
                preclaim.get("receipt_path"),
                preclaim.get("receipt_sha256"),
                root=(
                    runtime_root
                    / "dispatch/production-canary/receipts/math"
                ),
                code="math_migration_gs566_preclaim_evidence_invalid",
            )
        elif preclaim is not None:
            raise MathExactSmokeError("math_migration_commit_invalid")
        if not validate_target_queues:
            continue
        target_queue_path = Path(str(row.get("target_queue_path") or ""))
        target_queue_root = (
            runtime_root
            / "dispatch/state/production-canary-queue/math"
            / str(commit["target_activation_id"])
        )
        try:
            resolved_target_queue_path = target_queue_path.resolve(strict=True)
            resolved_target_queue_path.relative_to(
                target_queue_root.resolve(strict=True)
            )
            queue_payload = resolved_target_queue_path.read_bytes()
            queue = json.loads(queue_payload.decode("utf-8"))
        except (
            OSError,
            ValueError,
            UnicodeError,
            json.JSONDecodeError,
        ) as exc:
            raise MathExactSmokeError(
                "math_migration_target_queue_drift"
            ) from exc
        _verify_seal(
            queue, purpose="dispatch-production-canary-queue", key=key
        )
        if (
            queue.get("queue_status") not in {"pending", "claimed", "succeeded"}
            or queue.get("task_object_sha256")
            != row.get("target_task_object_sha256")
            or queue.get("unit_sha256") != row.get("target_unit_sha256")
            or queue.get("frozen_payload_sha256")
            != row.get("target_frozen_payload_sha256")
            or queue.get("source_event_ids")
            != [row.get("capture_event_id")]
            or queue.get("formal_write_count") != 0
            or (
                queue.get("queue_status") == "pending"
                and queue.get("claimed_at") is None
                and queue.get("terminal_receipt_sha256") is None
                and sha256_bytes(queue_payload)
                != row.get("target_queue_sha256")
            )
        ):
            raise MathExactSmokeError("math_migration_target_queue_drift")
    return {
        "schema_version": (
            "study-intake-math-pending-queue-migration-reopen-v1"
        ),
        "status": "committed",
        "migration_descriptor_sha256": migration_descriptor_sha256,
        "migration_commit_sha256": commit_sha256,
        "migration_commit_path": str(path),
        "target_release_id": commit["target_release_id"],
        "target_activation_id": commit["target_activation_id"],
        "target_authority_generation": commit[
            "target_authority_generation"
        ],
        "target_subject_authority_fingerprint": commit[
            "target_subject_authority_fingerprint"
        ],
        "target_producer_authority_fingerprint": commit[
            "target_producer_authority_fingerprint"
        ],
        "queue_count": 4,
        "queue_mappings": copy.deepcopy(rows),
        "model_call_count": 0,
        "provider_request_count": 0,
        "formal_write_count": 0,
        "sol_enabled": False,
    }


def rollback_uncommitted_exact_math_pending_queue_migration(
    *, runtime_root: Path, migration_descriptor_sha256: str
) -> dict[str, Any]:
    if _migration_commit_pointer_path(runtime_root).exists():
        raise MathExactSmokeError("math_migration_already_committed")
    active = _reopen_active_migration_descriptor(runtime_root=runtime_root)
    removed = False
    if active is not None:
        if active[0] != migration_descriptor_sha256:
            raise MathExactSmokeError("math_migration_descriptor_not_active")
        _migration_active_pointer_path(runtime_root).unlink()
        removed = True
    return {
        "schema_version": (
            "study-intake-math-pending-queue-migration-rollback-v1"
        ),
        "status": "rolled_back" if removed else "already_absent",
        "migration_descriptor_sha256": migration_descriptor_sha256,
        "queue_write_count": 0,
        "model_call_count": 0,
        "provider_request_count": 0,
        "formal_write_count": 0,
        "sol_enabled": False,
    }


def _prepared_migration_intent_for_target(
    *, runtime_root: Path, release_id: str
) -> dict[str, Any] | None:
    root = _migration_intent_root(runtime_root)
    if not root.is_dir():
        return None
    key = _read_authority_key(runtime_root)
    matches: list[str] = []
    for path in sorted((root / "sha256").glob("*/*.json")):
        digest = path.stem
        _require_sha256(digest, "math_migration_intent_invalid")
        _, intent = _verify_content_addressed_path(
            path, root=root, expected_sha256=digest
        )
        _verify_seal(intent, purpose=MIGRATION_INTENT_PURPOSE, key=key)
        if intent.get("target_release_id") == release_id:
            matches.append(digest)
    if not matches:
        return None
    if len(matches) != 1:
        raise MathExactSmokeError("math_migration_intent_duplicate")
    _, intent = _reopen_migration_intent(
        runtime_root=runtime_root, intent_sha256=matches[0]
    )
    return intent


def pending_exact_smoke_dispatch_scope(
    *, runtime_root: Path, math_repo_root: Path, release_id: str
) -> list[dict[str, str]]:
    """Return the contiguous applied suffix still awaiting its first queue.

    This is a read-only production polling exception for the frozen 2026-08-13
    smoke. It reopens the current release/activation HMAC authorization and
    capture-apply chain, then proves that every returned Capture has no queue
    in any activation. GS-269 remains the only canary unlock gate; after that
    terminal is accepted, the four sequentially written tail Captures may be
    returned together so their Luna work can run concurrently. It never widens
    ordinary math discovery to historical dates and never treats an
    unauthorized exact-looking ledger event as in scope.
    """

    if not isinstance(runtime_root, Path) or not runtime_root.is_absolute():
        raise MathExactSmokeError("math_smoke_runtime_root_invalid")
    if not isinstance(math_repo_root, Path) or not math_repo_root.is_absolute():
        raise MathExactSmokeError("math_smoke_math_repo_root_invalid")
    runtime_root = runtime_root.resolve()
    math_repo_root = math_repo_root.resolve()
    _require_sha256(release_id, "math_smoke_release_invalid")
    active_migration = _active_pending_migration_for_release(
        runtime_root=runtime_root, release_id=release_id
    )
    if active_migration is not None:
        descriptor_sha256, _descriptor_path, descriptor = active_migration
        if (
            _committed_migration_descriptor_sha256(
                runtime_root=runtime_root
            )
            == descriptor_sha256
        ):
            committed = reopen_exact_math_pending_queue_migration_commit(
                runtime_root=runtime_root,
                migration_descriptor_sha256=descriptor_sha256,
                required=True,
            )
            if committed is not None:
                return []
        state = _read_json(
            runtime_root / "dispatch/state/production-canary/math.json",
            "math_migration_target_state_invalid",
        )
        key = _read_authority_key(runtime_root)
        _verify_seal(
            state, purpose="dispatch-production-canary-state", key=key
        )
        if (
            state.get("subject") != "math"
            or state.get("release_id") != release_id
            or state.get("activation_id")
            != descriptor.get("target_activation_id")
            or state.get("producer_authority_fingerprint")
            != descriptor.get("target_producer_authority_fingerprint")
            or state.get("luna_consumer_enabled") is not False
            or state.get("active_task_count") != 0
            or state.get("formal_write_count") != 0
            or state.get("sol_enabled") is not False
        ):
            raise MathExactSmokeError("math_migration_target_state_invalid")
        rows: list[dict[str, str]] = []
        for mapping in descriptor["source_queue_mappings"]:
            formal_id = str(mapping["formal_id"])
            capture_id = str(mapping["capture_event_id"])
            _reopen_capture_event(
                math_repo_root=math_repo_root,
                formal_id=formal_id,
                capture_event_id=capture_id,
            )
            rows.append(
                {
                    "scope_id": EXACT_SCOPE_ID,
                    "study_date": EXACT_STUDY_DATE,
                    "formal_id": formal_id,
                    "capture_event_id": capture_id,
                    "capture_apply_receipt_sha256": str(
                        mapping["source_capture_apply_receipt_sha256"]
                    ),
                    "execution_authorization_sha256": str(
                        mapping["source_execution_authorization_sha256"]
                    ),
                    "release_id": release_id,
                    "activation_id": str(
                        descriptor["target_activation_id"]
                    ),
                }
            )
        return rows
    authorization_root = (
        runtime_root / "dispatch/math-exact-smoke/execution-authorizations"
    )
    capture_apply_root = _capture_apply_root(runtime_root)
    if not authorization_root.is_dir() or not capture_apply_root.is_dir():
        return []

    key = _read_authority_key(runtime_root)
    state = _read_json(
        runtime_root / "dispatch/state/production-canary/math.json",
        "math_smoke_canary_state_invalid",
    )
    try:
        _verify_seal(state, purpose="dispatch-production-canary-state", key=key)
    except MathExactSmokeError as exc:
        raise MathExactSmokeError("math_smoke_canary_state_invalid") from exc
    activation_id = state.get("activation_id")
    if (
        state.get("subject") != "math"
        or not isinstance(activation_id, str)
    ):
        raise MathExactSmokeError("math_smoke_canary_state_drift")
    if state.get("release_id") != release_id:
        # Exact-smoke work is scoped to the release that owns the live gate.
        # A successor release auditing historical backlog must not interpret
        # the predecessor's sealed smoke state as its own pending work.
        return []

    matching_authorizations: list[tuple[str, dict[str, Any]]] = []
    for path in sorted((authorization_root / "sha256").glob("*/*.json")):
        digest, receipt = reopen_execution_authorization(
            path, runtime_root=runtime_root
        )
        descriptor = receipt.get("authorization_descriptor")
        if not isinstance(descriptor, Mapping):
            raise MathExactSmokeError("math_smoke_authorization_receipt_invalid")
        if (
            descriptor.get("target_release_id") == release_id
            and descriptor.get("activation_id") == activation_id
        ):
            _validate_canary_state(runtime_root, descriptor, key=key)
            matching_authorizations.append((digest, receipt))
    if not matching_authorizations:
        return []
    if len(matching_authorizations) != 1:
        raise MathExactSmokeError("math_smoke_authorization_receipt_duplicate")

    authorization_sha256, authorization = matching_authorizations[0]
    source_descriptor = authorization.get("authorization_descriptor")
    if not isinstance(source_descriptor, Mapping):
        raise MathExactSmokeError("math_smoke_authorization_receipt_invalid")
    receipts = _load_capture_apply_receipts(
        runtime_root=runtime_root,
        execution_authorization_sha256=authorization_sha256,
        key=key,
    )
    if not receipts:
        return []

    receipt_by_capture = {
        str(receipt["capture_event_id"]): (digest, receipt)
        for digest, _path, receipt in receipts
    }

    exact_capture_ids = {
        str(EXACT_SAMPLES[formal_id]["capture_event_id"])
        for formal_id in EXACT_ORDER
    }
    queue_by_capture: dict[str, list[dict[str, Any]]] = {
        capture_id: [] for capture_id in exact_capture_ids
    }
    queue_root = runtime_root / "dispatch/state/production-canary-queue/math"
    if queue_root.is_dir():
        for path in sorted(queue_root.glob("*/*.json")):
            queue = _read_json(path, "math_smoke_queue_invalid")
            try:
                _verify_seal(
                    queue, purpose="dispatch-production-canary-queue", key=key
                )
            except MathExactSmokeError as exc:
                raise MathExactSmokeError("math_smoke_queue_invalid") from exc
            source_ids = queue.get("source_event_ids")
            matching_ids = (
                exact_capture_ids.intersection(source_ids)
                if isinstance(source_ids, list)
                else set()
            )
            if not matching_ids:
                continue
            queue_schema = queue.get("schema_version")
            queue_status = queue.get("queue_status")
            terminal_outcome = queue.get("terminal_outcome")
            if (
                len(matching_ids) != 1
                or len(source_ids) != 1
                or queue_schema
                not in {
                    "study-intake-production-canary-queue-entry-v2",
                    "study-intake-production-canary-queue-entry-v3",
                }
                or queue_status
                not in {
                    "pending",
                    "claimed",
                    "succeeded",
                    "failed",
                }
                or (
                    queue_schema
                    == "study-intake-production-canary-queue-entry-v3"
                    and not (
                        queue_status == "succeeded"
                        and terminal_outcome == "succeeded"
                        and queue.get("terminal_error_code") is None
                        and queue.get("execution_status") == "succeeded"
                        and queue.get("quality_status") == "issues_found"
                        and queue.get("report_available") is True
                        and queue.get("report_disposition")
                        == "needs_sol_review"
                        and queue.get("sol_review_status") == "pending"
                        and queue.get("formal_write_eligible") is False
                        and queue.get("production_accepted") is False
                        or queue_status == "failed"
                        and terminal_outcome == "failed"
                        and isinstance(
                            queue.get("terminal_error_code"), str
                        )
                        and bool(queue.get("terminal_error_code"))
                        and queue.get("execution_status") == "failed"
                        and queue.get("quality_status") == "unchecked"
                        and queue.get("report_disposition") == "quarantined"
                        and queue.get("sol_review_status") == "not_eligible"
                        and queue.get("formal_write_eligible") is False
                        and queue.get("production_accepted") is False
                    )
                )
                or queue.get("subject") != "math"
                or queue.get("release_id") != release_id
                or queue.get("activation_id") != activation_id
                or queue.get("producer_authority_fingerprint")
                != descriptor["producer_authority_fingerprint"]
                or queue.get("formal_write_count") != 0
            ):
                raise MathExactSmokeError("math_smoke_queue_binding_drift")
            capture_id = next(iter(matching_ids))
            apply_row = receipt_by_capture.get(capture_id)
            if apply_row is None:
                raise MathExactSmokeError("math_smoke_queue_binding_drift")
            task_payload, _task_path = _read_content_bound_file(
                queue.get("task_object_path"),
                queue.get("task_object_sha256"),
                root=runtime_root / "dispatch/production-canary/tasks/math",
                code="math_smoke_queue_task_invalid",
            )
            try:
                task_object = json.loads(task_payload.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise MathExactSmokeError(
                    "math_smoke_queue_task_invalid"
                ) from exc
            frozen_payload = (
                task_object.get("frozen_payload")
                if isinstance(task_object, Mapping)
                else None
            )
            task_binding = (
                frozen_payload.get("math_exact_smoke_binding")
                if isinstance(frozen_payload, Mapping)
                else None
            )
            formal_id = str(apply_row[1]["formal_id"])
            if (
                not isinstance(task_object, Mapping)
                or task_object.get("schema_version")
                != "study-intake-frozen-task-v1"
                or not isinstance(frozen_payload, Mapping)
                or not isinstance(task_binding, Mapping)
                or task_object.get("unit_sha256") != queue.get("unit_sha256")
                or sha256_bytes(canonical_bytes(frozen_payload))
                != queue.get("frozen_payload_sha256")
                or task_binding.get("schema_version") != TASK_BINDING_SCHEMA
                or task_binding.get("scope_id") != EXACT_SCOPE_ID
                or task_binding.get("formal_id") != formal_id
                or task_binding.get("capture_event_id") != capture_id
                or task_binding.get("capture_apply_receipt_sha256")
                != apply_row[0]
                or task_binding.get("target_release_id") != release_id
                or task_binding.get("activation_id") != activation_id
                or task_binding.get("authority_generation")
                != descriptor["authority_generation"]
                or task_binding.get("authority_fingerprint")
                != descriptor["authority_fingerprint"]
                or task_binding.get("producer_authority_fingerprint")
                != descriptor["producer_authority_fingerprint"]
                or task_binding.get("expected_unit_sha256")
                != queue.get("unit_sha256")
                or task_binding.get("formal_write_count") != 0
                or task_binding.get("sol_enabled") is not False
            ):
                raise MathExactSmokeError("math_smoke_queue_binding_drift")
            queue_by_capture[capture_id].append(queue)

    pending: list[tuple[int, str, str, str]] = []
    for digest, _path, receipt in receipts:
        sequence = int(receipt["sequence"])
        formal_id = str(receipt["formal_id"])
        capture_id = str(receipt["capture_event_id"])
        queues = queue_by_capture[capture_id]
        if len(queues) > 1:
            raise MathExactSmokeError("math_smoke_queue_duplicate")
        if queues:
            continue
        capture = _reopen_capture_event(
            math_repo_root=math_repo_root,
            formal_id=formal_id,
            capture_event_id=capture_id,
        )
        if capture.get("study_date") != EXACT_STUDY_DATE:
            raise MathExactSmokeError("math_smoke_capture_study_date_drift")
        pending.append((sequence, formal_id, capture_id, digest))
    if not pending:
        return []
    pending_sequences = [row[0] for row in pending]
    if pending_sequences != list(
        range(pending_sequences[0], len(receipts) + 1)
    ):
        raise MathExactSmokeError("math_smoke_pending_dispatch_scope_invalid")

    return [
        {
            "scope_id": EXACT_SCOPE_ID,
            "study_date": EXACT_STUDY_DATE,
            "formal_id": formal_id,
            "capture_event_id": capture_id,
            "capture_apply_receipt_sha256": apply_sha256,
            "execution_authorization_sha256": authorization_sha256,
            "release_id": release_id,
            "activation_id": str(descriptor["activation_id"]),
        }
        for _sequence, formal_id, capture_id, apply_sha256 in pending
    ]


def _load_quick_intake(math_repo_root: Path) -> Any:
    path = math_repo_root / "数学一回滚复习系统/scripts/quick_intake.py"
    try:
        spec = importlib.util.spec_from_file_location(
            f"math_exact_smoke_quick_intake_{sha256_bytes(str(path).encode())[:12]}",
            path,
        )
        if spec is None or spec.loader is None:
            raise OSError("module spec unavailable")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except Exception as exc:
        raise MathExactSmokeError("math_smoke_quick_intake_unreadable") from exc
    if Path(str(module.REPO_ROOT)).resolve() != math_repo_root.resolve():
        raise MathExactSmokeError("math_smoke_quick_intake_root_drift")
    return module


def _atomic_restore_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.rollback.", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _capture_apply_root(runtime_root: Path) -> Path:
    return runtime_root / "dispatch/math-exact-smoke/capture-applies"


def _load_capture_apply_receipts(
    *, runtime_root: Path, execution_authorization_sha256: str, key: bytes
) -> list[tuple[str, Path, dict[str, Any]]]:
    root = _capture_apply_root(runtime_root)
    rows: list[tuple[str, Path, dict[str, Any]]] = []
    if not root.exists():
        return rows
    for path in sorted((root / "sha256").glob("*/*.json")):
        digest, receipt = _verify_content_addressed_path(path, root=root)
        _verify_seal(receipt, purpose=CAPTURE_APPLY_PURPOSE, key=key)
        if receipt.get("execution_authorization_sha256") != (
            execution_authorization_sha256
        ):
            continue
        sequence = receipt.get("sequence")
        formal_id = receipt.get("formal_id")
        authority_snapshots = receipt.get("authority_snapshots")
        authority_snapshot_sha256s = receipt.get(
            "authority_snapshot_sha256s"
        )
        prior_terminal = receipt.get("prior_sample_terminal")
        prior_terminal_sha = receipt.get("prior_sample_terminal_sha256")
        if (
            receipt.get("schema_version") != CAPTURE_APPLY_RECEIPT_SCHEMA
            or receipt.get("scope_id") != EXACT_SCOPE_ID
            or isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or not 1 <= sequence <= len(EXACT_ORDER)
            or formal_id != EXACT_ORDER[sequence - 1]
            or receipt.get("capture_event_id")
            != EXACT_SAMPLES[str(formal_id)]["capture_event_id"]
            or receipt.get("capture_idempotency_key")
            != EXACT_SAMPLES[str(formal_id)]["capture_idempotency_key"]
            or receipt.get("capture_count_before") != 0
            or receipt.get("capture_count_after") != 1
            or not isinstance(authority_snapshots, list)
            or len(authority_snapshots) != 2
            or not all(isinstance(row, Mapping) for row in authority_snapshots)
            or not isinstance(authority_snapshot_sha256s, list)
            or authority_snapshot_sha256s
            != [
                sha256_bytes(canonical_bytes(row))
                for row in authority_snapshots
            ]
            or receipt.get("authority_snapshot_count") != 2
            or receipt.get("authority_snapshot_mcp_tool_call_count") != 2
            or receipt.get("queue_write_count") != 0
            or receipt.get("model_call_count") != 0
            or receipt.get("provider_request_count") != 0
            or receipt.get("model_mcp_tool_call_count") != 0
            or (
                sequence == 1
                and (prior_terminal is not None or prior_terminal_sha is not None)
            )
            or (
                sequence > 1
                and (
                    not isinstance(prior_terminal, Mapping)
                    or prior_terminal_sha
                    != sha256_bytes(canonical_bytes(prior_terminal))
                    or prior_terminal.get("formal_id")
                    != EXACT_ORDER[0]
                    or prior_terminal.get("terminal_status")
                    not in {"succeeded", "needs_sol_review"}
                    or prior_terminal.get("formal_write_count") != 0
                )
            )
            or receipt.get("sol_enabled") is not False
            or receipt.get("formal_write_count") != 0
        ):
            raise MathExactSmokeError("math_smoke_capture_apply_receipt_invalid")
        rows.append((digest, path, receipt))
    rows.sort(key=lambda row: int(row[2]["sequence"]))
    if [row[2]["sequence"] for row in rows] != list(
        range(1, len(rows) + 1)
    ):
        raise MathExactSmokeError("math_smoke_capture_apply_sequence_invalid")
    for index, (digest, _path, receipt) in enumerate(rows):
        expected_prior = None if index == 0 else rows[index - 1][0]
        if receipt.get("prior_capture_apply_receipt_sha256") != expected_prior:
            raise MathExactSmokeError("math_smoke_capture_apply_lineage_invalid")
        if index > 1 and canonical_bytes(
            receipt.get("prior_sample_terminal")
        ) != canonical_bytes(rows[1][2].get("prior_sample_terminal")):
            raise MathExactSmokeError("math_smoke_concurrency_unlock_drift")
    return rows


def _exact_apply_receipt_for_capture(
    *, runtime_root: Path, capture_event_id: str, key: bytes
) -> tuple[str, Path, dict[str, Any], str, Path, dict[str, Any]]:
    matches: list[
        tuple[str, Path, dict[str, Any], str, Path, dict[str, Any]]
    ] = []
    root = _capture_apply_root(runtime_root)
    if not root.exists():
        raise MathExactSmokeError("math_smoke_capture_apply_receipt_missing")
    for path in sorted((root / "sha256").glob("*/*.json")):
        digest, receipt = _verify_content_addressed_path(path, root=root)
        _verify_seal(receipt, purpose=CAPTURE_APPLY_PURPOSE, key=key)
        if receipt.get("capture_event_id") != capture_event_id:
            continue
        auth_path = Path(str(receipt.get("execution_authorization_path") or ""))
        auth_sha, auth = reopen_execution_authorization(
            auth_path, runtime_root=runtime_root
        )
        if auth_sha != receipt.get("execution_authorization_sha256"):
            raise MathExactSmokeError("math_smoke_capture_apply_binding_invalid")
        validated = _load_capture_apply_receipts(
            runtime_root=runtime_root,
            execution_authorization_sha256=auth_sha,
            key=key,
        )
        if not any(row[0] == digest for row in validated):
            raise MathExactSmokeError("math_smoke_capture_apply_binding_invalid")
        matches.append((digest, path, receipt, auth_sha, auth_path, auth))
    if len(matches) != 1:
        raise MathExactSmokeError(
            "math_smoke_capture_apply_receipt_missing"
            if not matches
            else "math_smoke_capture_apply_receipt_duplicate"
        )
    return matches[0]


def _reopen_capture_event(
    *, math_repo_root: Path, formal_id: str, capture_event_id: str
) -> dict[str, Any]:
    quick = _load_quick_intake(math_repo_root)
    try:
        rows = quick.load_jsonl(quick.EVENTS_PATH)
        capture = quick.replay(rows)["captures"].get(capture_event_id)
    except Exception as exc:
        raise MathExactSmokeError("math_smoke_capture_event_invalid") from exc
    if not isinstance(capture, Mapping):
        raise MathExactSmokeError("math_smoke_capture_event_missing")
    matches = [row for row in rows if row.get("event_id") == capture_event_id]
    if (
        len(matches) != 1
        or capture.get("target", {}).get("formal_id") != formal_id
    ):
        raise MathExactSmokeError("math_smoke_capture_event_duplicate")
    return dict(capture)


def exact_task_binding_for_capture(
    *,
    runtime_root: Path | None = None,
    runtime_root_loader: Callable[[], Path] | None = None,
    math_repo_root: Path,
    capture_event_id: str,
    release_id: str,
    unit_sha256: str,
    content_processing_id: str,
) -> dict[str, Any] | None:
    """Bind the exact Capture apply to its real successor FrozenTask.

    Non-smoke captures return ``None``.  An exact smoke Capture can never fall
    back to an unbound normal task after its immutable apply receipt exists.
    """

    formal_id = next(
        (
            item
            for item in EXACT_ORDER
            if EXACT_SAMPLES[item]["capture_event_id"] == capture_event_id
        ),
        None,
    )
    if formal_id is None:
        return None
    if runtime_root is not None and runtime_root_loader is not None:
        raise MathExactSmokeError("math_smoke_runtime_root_invalid")
    if runtime_root is None:
        if runtime_root_loader is None:
            raise MathExactSmokeError("math_smoke_runtime_root_invalid")
        try:
            runtime_root = runtime_root_loader()
        except (KeyError, OSError, TypeError, ValueError) as exc:
            raise MathExactSmokeError(
                "math_smoke_runtime_root_invalid"
            ) from exc
    if not isinstance(runtime_root, Path) or not runtime_root.is_absolute():
        raise MathExactSmokeError("math_smoke_runtime_root_invalid")
    runtime_root = runtime_root.resolve()
    _require_sha256(release_id, "math_smoke_release_invalid")
    _require_sha256(unit_sha256, "math_smoke_task_unit_invalid")
    _require_sha256(
        content_processing_id, "math_smoke_content_processing_id_invalid"
    )
    key = _read_authority_key(runtime_root)
    (
        apply_sha,
        apply_path,
        apply_receipt,
        authorization_sha,
        authorization_path,
        authorization,
    ) = _exact_apply_receipt_for_capture(
        runtime_root=runtime_root,
        capture_event_id=capture_event_id,
        key=key,
    )
    descriptor = authorization.get("authorization_descriptor")
    if not isinstance(descriptor, Mapping):
        raise MathExactSmokeError("math_smoke_authorization_receipt_invalid")
    source_descriptor = descriptor
    capture = _reopen_capture_event(
        math_repo_root=math_repo_root,
        formal_id=formal_id,
        capture_event_id=capture_event_id,
    )
    binding = EXACT_SAMPLES[formal_id]
    migration = _active_migration_mapping_for_capture(
        runtime_root=runtime_root,
        release_id=release_id,
        capture_event_id=capture_event_id,
    )
    migration_digest: str | None = None
    migration_descriptor: Mapping[str, Any] | None = None
    migration_mapping: Mapping[str, Any] | None = None
    if migration is not None:
        migration_digest, migration_descriptor, migration_mapping = migration
        if (
            formal_id not in MIGRATION_ORDER
            or migration_mapping.get("formal_id") != formal_id
            or migration_mapping.get("source_capture_apply_receipt_sha256")
            != apply_sha
            or migration_mapping.get("source_capture_apply_receipt_path")
            != str(apply_path.resolve())
            or migration_mapping.get(
                "source_execution_authorization_sha256"
            )
            != authorization_sha
            or migration_mapping.get("source_execution_authorization_path")
            != str(authorization_path.resolve())
            or migration_mapping.get("source_release_id")
            != source_descriptor.get("target_release_id")
            or migration_mapping.get("source_activation_id")
            != source_descriptor.get("activation_id")
            or migration_mapping.get("source_authority_generation")
            != source_descriptor.get("authority_generation")
            or migration_mapping.get("source_authority_fingerprint")
            != source_descriptor.get("authority_fingerprint")
            or migration_mapping.get(
                "source_producer_authority_fingerprint"
            )
            != source_descriptor.get("producer_authority_fingerprint")
        ):
            raise MathExactSmokeError(
                "math_migration_task_source_binding_invalid"
            )
        target_activation_id = str(
            migration_descriptor["target_activation_id"]
        )
        target_generation = str(
            migration_descriptor["target_authority_generation"]
        )
        target_authority = str(
            migration_descriptor["target_subject_authority_fingerprint"]
        )
        target_producer = str(
            migration_descriptor["target_producer_authority_fingerprint"]
        )
    else:
        target_activation_id = str(source_descriptor["activation_id"])
        target_generation = str(source_descriptor["authority_generation"])
        target_authority = str(source_descriptor["authority_fingerprint"])
        target_producer = str(
            source_descriptor["producer_authority_fingerprint"]
        )
    if (
        (migration is None and source_descriptor.get("target_release_id") != release_id)
        or apply_receipt.get("formal_id") != formal_id
        or apply_receipt.get("capture_event_sha256")
        != capture.get("content_hash")
        or apply_receipt.get("capture_idempotency_key")
        != capture.get("idempotency_key")
        or capture.get("idempotency_key")
        != binding["capture_idempotency_key"]
    ):
        raise MathExactSmokeError("math_smoke_task_capture_binding_invalid")
    if migration_mapping is not None:
        attempt_identity = _require_sha256(
            migration_mapping.get("source_attempt_idempotency_key"),
            "math_migration_task_source_binding_invalid",
        )
        processing_attempt_id = migration_mapping.get(
            "source_processing_attempt_id"
        )
        if (
            not isinstance(processing_attempt_id, str)
            or not processing_attempt_id.startswith("MATH-SMOKE-ATTEMPT-")
            or len(processing_attempt_id)
            != len("MATH-SMOKE-ATTEMPT-") + 24
        ):
            raise MathExactSmokeError(
                "math_migration_task_source_binding_invalid"
            )
    else:
        attempt_identity = sha256_bytes(
            canonical_bytes(
                {
                    "scope_id": EXACT_SCOPE_ID,
                    "capture_event_id": capture_event_id,
                    "capture_apply_receipt_sha256": apply_sha,
                    "release_id": release_id,
                    "activation_id": target_activation_id,
                    "unit_sha256": unit_sha256,
                    "prior_terminal_receipt_sha256": None,
                }
            )
        )
        processing_attempt_id = (
            f"MATH-SMOKE-ATTEMPT-{attempt_identity[:24]}"
        )
    result = {
        "schema_version": (
            TASK_BINDING_V2_SCHEMA
            if migration_descriptor is not None
            else TASK_BINDING_SCHEMA
        ),
        "scope_id": EXACT_SCOPE_ID,
        "sequence": EXACT_ORDER.index(formal_id) + 1,
        "formal_id": formal_id,
        "execution_authorization_sha256": authorization_sha,
        "execution_authorization_path": str(authorization_path.resolve()),
        "capture_apply_receipt_sha256": apply_sha,
        "capture_apply_receipt_path": str(apply_path.resolve()),
        "capture_event_id": capture_event_id,
        "capture_event_sha256": capture["content_hash"],
        "capture_idempotency_key": capture["idempotency_key"],
        "math_repo_root": str(math_repo_root.resolve()),
        "processing_attempt_number": 1,
        "processing_attempt_id": processing_attempt_id,
        "attempt_idempotency_key": attempt_identity,
        "expected_unit_sha256": unit_sha256,
        "content_processing_id": content_processing_id,
        "prior_terminal_receipt_sha256": None,
        "prior_terminal_receipt_path": None,
        "target_release_id": release_id,
        "activation_id": target_activation_id,
        "authority_generation": target_generation,
        "authority_fingerprint": target_authority,
        "producer_authority_fingerprint": target_producer,
        "single_active_attempt_required": True,
        "single_accepted_package_required": True,
        "capture_write_count": 0,
        "formal_write_count": 0,
        "sol_enabled": False,
    }
    if migration_descriptor is not None and migration_mapping is not None:
        result.update(
            {
                "migration_descriptor_sha256": migration_digest,
                "source_queue_path": migration_mapping[
                    "source_queue_path"
                ],
                "source_queue_sha256": migration_mapping[
                    "source_queue_sha256"
                ],
                "source_task_object_path": migration_mapping[
                    "source_task_object_path"
                ],
                "source_task_object_sha256": migration_mapping[
                    "source_task_object_sha256"
                ],
                "source_unit_sha256": migration_mapping[
                    "source_unit_sha256"
                ],
                "source_frozen_payload_sha256": migration_mapping[
                    "source_frozen_payload_sha256"
                ],
                "source_release_id": migration_mapping[
                    "source_release_id"
                ],
                "source_activation_id": migration_mapping[
                    "source_activation_id"
                ],
                "source_producer_authority_fingerprint": migration_mapping[
                    "source_producer_authority_fingerprint"
                ],
                "source_authority_generation": migration_mapping[
                    "source_authority_generation"
                ],
                "source_authority_fingerprint": migration_mapping[
                    "source_authority_fingerprint"
                ],
                "source_execution_authorization_sha256": migration_mapping[
                    "source_execution_authorization_sha256"
                ],
                "source_execution_authorization_path": migration_mapping[
                    "source_execution_authorization_path"
                ],
                "source_capture_apply_receipt_sha256": migration_mapping[
                    "source_capture_apply_receipt_sha256"
                ],
                "source_capture_apply_receipt_path": migration_mapping[
                    "source_capture_apply_receipt_path"
                ],
                "source_task_binding_sha256": migration_mapping[
                    "source_task_binding_sha256"
                ],
                "source_preclaim_failure_evidence": copy.deepcopy(
                    migration_mapping["source_preclaim_failure_evidence"]
                ),
                "gs269_archived_evidence_sha256": sha256_bytes(
                    canonical_bytes(
                        migration_descriptor["gs269_archived_evidence"]
                    )
                ),
            }
        )
    return result


def verify_exact_task_binding(
    *,
    runtime_root: Path,
    task_payload: Mapping[str, Any],
    unit_sha256: str,
    state: Mapping[str, Any],
) -> dict[str, Any] | None:
    binding = task_payload.get("math_exact_smoke_binding")
    capture_id = task_payload.get("capture_id")
    exact_capture = any(
        EXACT_SAMPLES[item]["capture_event_id"] == capture_id
        for item in EXACT_ORDER
    )
    if binding is None:
        if exact_capture:
            raise MathExactSmokeError("math_smoke_task_binding_missing")
        return None
    if not exact_capture or not isinstance(binding, Mapping):
        raise MathExactSmokeError("math_smoke_task_binding_unexpected")
    raw_math_repo_root = binding.get("math_repo_root")
    if not isinstance(raw_math_repo_root, str) or not raw_math_repo_root.startswith("/"):
        raise MathExactSmokeError("math_smoke_task_binding_drift")
    content_processing_id = task_payload.get("content_processing_id")
    expected = exact_task_binding_for_capture(
        runtime_root=runtime_root,
        math_repo_root=Path(raw_math_repo_root),
        capture_event_id=str(capture_id),
        release_id=str(state.get("release_id") or ""),
        unit_sha256=unit_sha256,
        content_processing_id=str(content_processing_id or ""),
    )
    if (
        expected is None
        or canonical_bytes(binding) != canonical_bytes(expected)
        or binding.get("activation_id") != state.get("activation_id")
        or binding.get("producer_authority_fingerprint")
        != state.get("producer_authority_fingerprint")
        or binding.get("expected_unit_sha256") != unit_sha256
        or binding.get("prior_terminal_receipt_sha256") is not None
    ):
        raise MathExactSmokeError("math_smoke_task_binding_drift")
    return dict(binding)


def verify_committed_exact_math_migration_claim(
    runtime_root: Path,
    task_payload: Mapping[str, Any],
    unit_sha256: str,
    state: Mapping[str, Any],
    producer_contract: Mapping[str, Any],
) -> bool:
    """Authorize only the committed four-task pre-activation claim surface."""

    binding = task_payload.get("math_exact_smoke_binding")
    if not isinstance(binding, Mapping) or binding.get("schema_version") != (
        TASK_BINDING_V2_SCHEMA
    ):
        return False
    code = "math_migration_claim_evidence_invalid"
    try:
        if not runtime_root.is_absolute():
            raise MathExactSmokeError(code)
        _require_sha256(unit_sha256, code)
        descriptor_sha256 = _require_sha256(
            binding.get("migration_descriptor_sha256"), code
        )
        active = _reopen_active_migration_descriptor(
            runtime_root=runtime_root,
            expected_release_id=str(state.get("release_id") or ""),
        )
        if active is None or active[0] != descriptor_sha256:
            raise MathExactSmokeError(code)
        descriptor = active[2]
        commit = reopen_exact_math_pending_queue_migration_commit(
            runtime_root=runtime_root,
            migration_descriptor_sha256=descriptor_sha256,
            required=True,
            validate_target_queues=False,
        )
        if commit is None:
            raise MathExactSmokeError(code)
        formal_id = binding.get("formal_id")
        capture_id = binding.get("capture_event_id")
        descriptor_rows = [
            row
            for row in descriptor.get("source_queue_mappings") or []
            if isinstance(row, Mapping)
            and row.get("formal_id") == formal_id
            and row.get("capture_event_id") == capture_id
        ]
        commit_rows = [
            row
            for row in commit.get("queue_mappings") or []
            if isinstance(row, Mapping)
            and row.get("formal_id") == formal_id
            and row.get("capture_event_id") == capture_id
        ]
        if len(descriptor_rows) != 1 or len(commit_rows) != 1:
            raise MathExactSmokeError(code)
        source = descriptor_rows[0]
        committed = commit_rows[0]
        source_binding_fields = {
            "source_queue_path": "source_queue_path",
            "source_queue_sha256": "source_queue_sha256",
            "source_task_object_path": "source_task_object_path",
            "source_task_object_sha256": "source_task_object_sha256",
            "source_unit_sha256": "source_unit_sha256",
            "source_frozen_payload_sha256": (
                "source_frozen_payload_sha256"
            ),
            "source_release_id": "source_release_id",
            "source_activation_id": "source_activation_id",
            "source_producer_authority_fingerprint": (
                "source_producer_authority_fingerprint"
            ),
            "source_authority_generation": "source_authority_generation",
            "source_authority_fingerprint": (
                "source_authority_fingerprint"
            ),
            "source_execution_authorization_sha256": (
                "source_execution_authorization_sha256"
            ),
            "source_execution_authorization_path": (
                "source_execution_authorization_path"
            ),
            "source_capture_apply_receipt_sha256": (
                "source_capture_apply_receipt_sha256"
            ),
            "source_capture_apply_receipt_path": (
                "source_capture_apply_receipt_path"
            ),
            "source_task_binding_sha256": "source_task_binding_sha256",
        }
        if any(
            binding.get(binding_key) != source.get(source_key)
            for binding_key, source_key in source_binding_fields.items()
        ):
            raise MathExactSmokeError(code)
        gs269_evidence_sha256 = sha256_bytes(
            canonical_bytes(descriptor["gs269_archived_evidence"])
        )
        source_events = producer_contract.get("source_events")
        if (
            formal_id not in MIGRATION_ORDER
            or EXACT_SAMPLES[str(formal_id)]["capture_event_id"] != capture_id
            or binding.get("scope_id") != EXACT_SCOPE_ID
            or binding.get("sequence") != EXACT_ORDER.index(str(formal_id)) + 1
            or binding.get("processing_attempt_number") != 1
            or binding.get("processing_attempt_id")
            != source.get("source_processing_attempt_id")
            or binding.get("attempt_idempotency_key")
            != source.get("source_attempt_idempotency_key")
            or binding.get("prior_terminal_receipt_sha256") is not None
            or binding.get("prior_terminal_receipt_path") is not None
            or binding.get("target_release_id")
            != descriptor.get("target_release_id")
            or binding.get("activation_id")
            != descriptor.get("target_activation_id")
            or binding.get("authority_generation")
            != descriptor.get("target_authority_generation")
            or binding.get("authority_fingerprint")
            != descriptor.get("target_subject_authority_fingerprint")
            or binding.get("producer_authority_fingerprint")
            != descriptor.get("target_producer_authority_fingerprint")
            or binding.get("expected_unit_sha256") != unit_sha256
            or binding.get("gs269_archived_evidence_sha256")
            != gs269_evidence_sha256
            or canonical_bytes(
                binding.get("source_preclaim_failure_evidence")
            )
            != canonical_bytes(
                source.get("source_preclaim_failure_evidence")
            )
            or state.get("subject") != "math"
            or state.get("release_id")
            != descriptor.get("target_release_id")
            or state.get("activation_id")
            != descriptor.get("target_activation_id")
            or state.get("producer_authority_fingerprint")
            != descriptor.get("target_producer_authority_fingerprint")
            or producer_contract.get("subject") != "math"
            or producer_contract.get("release_id") != state.get("release_id")
            or producer_contract.get("authority_fingerprint")
            != state.get("producer_authority_fingerprint")
            or producer_contract.get("producer_unit_id") != capture_id
            or not isinstance(source_events, list)
            or [row.get("event_id") for row in source_events]
            != [capture_id]
            or committed.get("target_unit_sha256") != unit_sha256
            or committed.get("target_frozen_payload_sha256")
            != sha256_bytes(canonical_bytes(task_payload))
            or committed.get("source_queue_sha256")
            != source.get("source_queue_sha256")
            or committed.get("source_processing_attempt_id")
            != source.get("source_processing_attempt_id")
            or committed.get("source_attempt_idempotency_key")
            != source.get("source_attempt_idempotency_key")
            or canonical_bytes(
                committed.get("source_preclaim_failure_evidence")
            )
            != canonical_bytes(
                source.get("source_preclaim_failure_evidence")
            )
            or binding.get("capture_write_count") != 0
            or binding.get("formal_write_count") != 0
            or binding.get("sol_enabled") is not False
        ):
            raise MathExactSmokeError(code)
        task_object_payload, task_object_path = _read_content_bound_file(
            committed.get("target_task_object_path"),
            committed.get("target_task_object_sha256"),
            root=runtime_root / "dispatch/production-canary/tasks/math",
            code=code,
        )
        expected_task_object = {
            "schema_version": "study-intake-frozen-task-v1",
            "frozen_payload": copy.deepcopy(dict(task_payload)),
            "requested_model": "gpt-5.6-luna",
            "requested_reasoning_effort": "max",
            "unit_sha256": unit_sha256,
        }
        if (
            task_object_payload
            != canonical_bytes(expected_task_object) + b"\n"
            or str(task_object_path)
            != committed.get("target_task_object_path")
        ):
            raise MathExactSmokeError(code)
        queue_path = Path(str(committed.get("target_queue_path") or ""))
        try:
            resolved_queue_path = queue_path.resolve(strict=True)
            resolved_queue_path.relative_to(
                (
                    runtime_root
                    / "dispatch/state/production-canary-queue/math"
                    / str(state["activation_id"])
                ).resolve(strict=True)
            )
            queue = json.loads(
                resolved_queue_path.read_text(encoding="utf-8")
            )
        except (
            OSError,
            ValueError,
            UnicodeError,
            json.JSONDecodeError,
        ) as exc:
            raise MathExactSmokeError(code) from exc
        _verify_seal(
            queue,
            purpose="dispatch-production-canary-queue",
            key=_read_authority_key(runtime_root),
        )
        if (
            queue.get("subject") != "math"
            or queue.get("release_id") != state.get("release_id")
            or queue.get("activation_id") != state.get("activation_id")
            or queue.get("producer_authority_fingerprint")
            != state.get("producer_authority_fingerprint")
            or queue.get("producer_input_contract_sha256")
            != producer_contract.get("producer_input_contract_sha256")
            or queue.get("source_event_ids") != [capture_id]
            or queue.get("unit_sha256") != unit_sha256
            or queue.get("frozen_payload_sha256")
            != committed.get("target_frozen_payload_sha256")
            or queue.get("task_object_sha256")
            != committed.get("target_task_object_sha256")
            or queue.get("queue_status")
            not in {"pending", "claimed", "succeeded", "failed"}
            or queue.get("formal_write_count") != 0
        ):
            raise MathExactSmokeError(code)
        return True
    except MathExactSmokeError as exc:
        if exc.code == code:
            raise
        raise MathExactSmokeError(code) from exc


def verify_exact_batch_authority(
    task_payloads: list[Mapping[str, Any]], authority: Mapping[str, Any]
) -> None:
    for payload in task_payloads:
        binding = payload.get("math_exact_smoke_binding")
        if binding is None:
            continue
        if (
            not isinstance(binding, Mapping)
            or authority.get("schema_version")
            != "subject_authority_snapshot_v1"
            or authority.get("subject") != "math"
            or authority.get("generation") != binding.get("authority_generation")
            or authority.get("authority_fingerprint")
            != binding.get("authority_fingerprint")
            or authority.get("model_call_count") != 0
            or authority.get("formal_write_count") != 0
        ):
            raise MathExactSmokeError("math_smoke_batch_authority_drift")


def _read_content_bound_file(
    path_value: object,
    sha_value: object,
    *,
    root: Path,
    code: str,
) -> tuple[bytes, Path]:
    _require_sha256(sha_value, code)
    if not isinstance(path_value, str) or not path_value.startswith("/"):
        raise MathExactSmokeError(code)
    try:
        path = Path(path_value).resolve(strict=True)
        path.relative_to(root.resolve(strict=True))
        payload = path.read_bytes()
    except (OSError, ValueError) as exc:
        raise MathExactSmokeError(code) from exc
    if sha256_bytes(payload) != sha_value:
        raise MathExactSmokeError(code)
    return payload, path


def _prior_terminal_snapshot(
    *,
    runtime_root: Path,
    formal_id: str,
    capture_apply_receipt_sha256: str,
    key: bytes,
) -> dict[str, Any]:
    capture_event_id = str(EXACT_SAMPLES[formal_id]["capture_event_id"])
    queue_root = (
        runtime_root / "dispatch/state/production-canary-queue/math"
    )
    matches: list[tuple[Path, dict[str, Any]]] = []
    if queue_root.exists():
        for path in sorted(queue_root.glob("*/*.json")):
            queue = _read_json(path, "math_smoke_prior_queue_invalid")
            _verify_seal(
                queue, purpose="dispatch-production-canary-queue", key=key
            )
            if capture_event_id in set(queue.get("source_event_ids") or []):
                matches.append((path.resolve(), queue))
    if len(matches) != 1:
        raise MathExactSmokeError(
            "math_smoke_prior_task_missing"
            if not matches
            else "math_smoke_prior_task_duplicate"
        )
    queue_path, queue = matches[0]
    queue_status = queue.get("queue_status")
    quality_success = bool(
        queue_status == "succeeded"
        and queue.get("schema_version")
        == "study-intake-production-canary-queue-entry-v3"
        and queue.get("terminal_outcome") == "succeeded"
        and queue.get("terminal_error_code") is None
        and queue.get("execution_status") == "succeeded"
        and queue.get("quality_status") == "issues_found"
        and queue.get("report_available") is True
        and queue.get("report_disposition") == "needs_sol_review"
        and queue.get("sol_review_status") == "pending"
        and queue.get("formal_write_eligible") is False
        and queue.get("production_accepted") is False
    )
    legacy_quality_terminal = queue_status == "needs_sol_review"
    if queue_status not in {"succeeded", "needs_sol_review"}:
        raise MathExactSmokeError("math_smoke_prior_task_not_accepted_terminal")
    if (
        queue.get("formal_write_count") != 0
        or queue.get("subject") != "math"
        or queue.get("source_event_ids") != [capture_event_id]
        or queue.get("terminal_outcome")
        not in (
            {"succeeded"}
            if queue_status == "succeeded"
            else {"needs_rework"}
        )
    ):
        raise MathExactSmokeError("math_smoke_prior_queue_invalid")

    task_payload, _task_path = _read_content_bound_file(
        queue.get("task_object_path"),
        queue.get("task_object_sha256"),
        root=runtime_root / "dispatch/production-canary/tasks/math",
        code="math_smoke_prior_task_object_invalid",
    )
    try:
        task_object = json.loads(task_payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise MathExactSmokeError("math_smoke_prior_task_object_invalid") from exc
    frozen_payload = (
        task_object.get("frozen_payload")
        if isinstance(task_object, Mapping)
        else None
    )
    task_binding = (
        frozen_payload.get("math_exact_smoke_binding")
        if isinstance(frozen_payload, Mapping)
        else None
    )
    if (
        not isinstance(task_binding, Mapping)
        or task_object.get("unit_sha256") != queue.get("unit_sha256")
        or sha256_bytes(canonical_bytes(frozen_payload))
        != queue.get("frozen_payload_sha256")
        or task_binding.get("formal_id") != formal_id
        or task_binding.get("capture_event_id") != capture_event_id
        or task_binding.get("capture_apply_receipt_sha256")
        != capture_apply_receipt_sha256
        or task_binding.get("processing_attempt_number") != 1
        or task_binding.get("prior_terminal_receipt_sha256") is not None
        or task_binding.get("target_release_id") != queue.get("release_id")
        or task_binding.get("activation_id") != queue.get("activation_id")
        or task_binding.get("expected_unit_sha256") != queue.get("unit_sha256")
    ):
        raise MathExactSmokeError("math_smoke_prior_task_binding_invalid")

    terminal_payload, terminal_path = _read_content_bound_file(
        queue.get("terminal_receipt_path"),
        queue.get("terminal_receipt_sha256"),
        root=runtime_root / "dispatch/production-canary/receipts/math",
        code="math_smoke_prior_terminal_invalid",
    )
    try:
        terminal = json.loads(terminal_payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise MathExactSmokeError("math_smoke_prior_terminal_invalid") from exc
    if not isinstance(terminal, Mapping):
        raise MathExactSmokeError("math_smoke_prior_terminal_invalid")
    _verify_seal(
        terminal, purpose="dispatch-production-canary-terminal", key=key
    )
    expected_schema = (
        "study-intake-production-canary-review-terminal-receipt-v1"
        if quality_success or legacy_quality_terminal
        else "study-intake-production-canary-terminal-receipt-v3"
    )
    selected = terminal.get("selected")
    if (
        terminal.get("schema_version") != expected_schema
        or terminal.get("subject") != "math"
        or terminal.get("release_id") != queue.get("release_id")
        or terminal.get("activation_id") != queue.get("activation_id")
        or terminal.get("formal_write_count") != 0
        or not isinstance(selected, Mapping)
        or selected.get("unit_sha256") != queue.get("unit_sha256")
        or selected.get("frozen_payload_sha256")
        != queue.get("frozen_payload_sha256")
        or terminal.get("report_reopen_status")
        != "json_markdown_package_verified"
    ):
        raise MathExactSmokeError("math_smoke_prior_terminal_invalid")
    if queue_status == "succeeded" and not quality_success:
        if terminal.get("outcome") != "succeeded":
            raise MathExactSmokeError("math_smoke_prior_terminal_invalid")
    elif (
        terminal.get("outcome")
        != ("succeeded" if quality_success else "needs_rework")
        or quality_success
        and (
            terminal.get("error_code") is not None
            or terminal.get("execution_status") != "succeeded"
            or terminal.get("quality_status") != "issues_found"
            or terminal.get("production_accepted") is not False
        )
        or terminal.get("report_available") is not True
        or terminal.get("report_disposition") != "needs_sol_review"
        or terminal.get("sol_review_status") != "pending"
        or terminal.get("formal_write_eligible") is not False
    ):
        raise MathExactSmokeError("math_smoke_prior_terminal_invalid")

    package_payload, _package_path = _read_content_bound_file(
        terminal.get("package_path"),
        terminal.get("package_sha256"),
        root=runtime_root / "dispatch/packages",
        code="math_smoke_prior_package_invalid",
    )
    report_json_sha = terminal.get("report_json_sha256")
    _require_sha256(report_json_sha, "math_smoke_prior_report_invalid")
    report_json_path = (
        runtime_root
        / "dispatch/reports/json/sha256"
        / str(report_json_sha)[:2]
        / f"{report_json_sha}.json"
    )
    report_json_payload, _ = _read_content_bound_file(
        str(report_json_path),
        report_json_sha,
        root=runtime_root / "dispatch/reports/json",
        code="math_smoke_prior_report_invalid",
    )
    report_markdown_sha = terminal.get("report_markdown_sha256")
    _require_sha256(report_markdown_sha, "math_smoke_prior_report_invalid")
    report_markdown_path = (
        runtime_root
        / "dispatch/reports/markdown/sha256"
        / str(report_markdown_sha)[:2]
        / f"{report_markdown_sha}.md"
    )
    report_markdown_payload, _ = _read_content_bound_file(
        str(report_markdown_path),
        report_markdown_sha,
        root=runtime_root / "dispatch/reports/markdown",
        code="math_smoke_prior_report_invalid",
    )
    completion_payload, _ = _read_content_bound_file(
        terminal.get("completion_receipt_path"),
        terminal.get("completion_receipt_sha256"),
        root=runtime_root / "dispatch/receipts",
        code="math_smoke_prior_completion_receipt_invalid",
    )
    try:
        package = json.loads(package_payload.decode("utf-8"))
        report = json.loads(report_json_payload.decode("utf-8"))
        completion = json.loads(completion_payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise MathExactSmokeError("math_smoke_prior_report_invalid") from exc
    if (
        not isinstance(package, Mapping)
        or not isinstance(report, Mapping)
        or not isinstance(completion, Mapping)
        or package.get("formal_write_count") != 0
        or report.get("formal_write_count") != 0
        or completion.get("formal_write_count") != 0
        or report.get("unit_sha256") != queue.get("unit_sha256")
        or report.get("package_sha256") != terminal.get("package_sha256")
        or not report_markdown_payload.strip()
    ):
        raise MathExactSmokeError("math_smoke_prior_report_invalid")
    if (quality_success or legacy_quality_terminal) and (
        report.get("report_available") is not True
        or report.get("sol_review_status") != "pending"
        or report.get("formal_write_eligible") is not False
        or report.get("report_disposition") != "needs_sol_review"
    ):
        raise MathExactSmokeError("math_smoke_prior_report_invalid")
    return {
        "formal_id": formal_id,
        "capture_event_id": capture_event_id,
        "queue_path": str(queue_path),
        "queue_sha256": sha256_bytes(canonical_bytes(queue) + b"\n"),
        "task_object_sha256": queue["task_object_sha256"],
        "unit_sha256": queue["unit_sha256"],
        "terminal_receipt_sha256": queue["terminal_receipt_sha256"],
        "terminal_receipt_path": str(terminal_path),
        "terminal_status": (
            "needs_sol_review"
            if quality_success or legacy_quality_terminal
            else queue_status
        ),
        "report_json_sha256": report_json_sha,
        "report_markdown_sha256": report_markdown_sha,
        "formal_write_count": 0,
    }


def reopen_exact_gs269_review_terminal(
    *,
    runtime_root: Path,
    task_payload: Mapping[str, Any],
    unit_sha256: str,
) -> dict[str, Any]:
    """Reopen the one accepted GS-269 review terminal for batch rollover.

    This is deliberately not a general review-terminal reader.  It reuses the
    exact-smoke queue, task, receipt, report, package, and completion checks
    that gate the four successor Captures, and accepts only the first bound
    task when its disposition is ``needs_sol_review``.
    """

    _require_sha256(unit_sha256, "math_smoke_prior_task_binding_invalid")
    binding = task_payload.get("math_exact_smoke_binding")
    first_id = EXACT_ORDER[0]
    first_capture_id = str(EXACT_SAMPLES[first_id]["capture_event_id"])
    if (
        task_payload.get("subject") != "math"
        or task_payload.get("capture_id") != first_capture_id
        or not isinstance(binding, Mapping)
        or binding.get("scope_id") != EXACT_SCOPE_ID
        or binding.get("sequence") != 1
        or binding.get("formal_id") != first_id
        or binding.get("capture_event_id") != first_capture_id
        or binding.get("expected_unit_sha256") != unit_sha256
        or binding.get("processing_attempt_number") != 1
        or binding.get("prior_terminal_receipt_sha256") is not None
        or binding.get("formal_write_count") != 0
        or binding.get("sol_enabled") is not False
    ):
        raise MathExactSmokeError("math_smoke_prior_task_binding_invalid")
    snapshot = _prior_terminal_snapshot(
        runtime_root=runtime_root,
        formal_id=first_id,
        capture_apply_receipt_sha256=str(
            binding.get("capture_apply_receipt_sha256") or ""
        ),
        key=_read_authority_key(runtime_root),
    )
    if (
        snapshot.get("formal_id") != first_id
        or snapshot.get("capture_event_id") != first_capture_id
        or snapshot.get("unit_sha256") != unit_sha256
        or snapshot.get("terminal_status") != "needs_sol_review"
        or snapshot.get("formal_write_count") != 0
    ):
        raise MathExactSmokeError(
            "math_smoke_prior_task_not_accepted_terminal"
        )
    return snapshot


def _validate_stage_preimages(
    quick: Any, *, completed_count: int
) -> None:
    for index, formal_id in enumerate(EXACT_ORDER):
        binding = EXACT_SAMPLES[formal_id]
        if not binding["stage_required"]:
            continue
        manifest = (
            Path(str(quick.SOURCE_STAGING_ROOT))
            / "2026-08-13"
            / str(binding["stage_bundle_id"])
            / "manifest.json"
        )
        should_exist = index < completed_count
        if should_exist:
            if (
                not manifest.is_file()
                or file_sha256(manifest) != binding["stage_manifest_sha256"]
            ):
                raise MathExactSmokeError("math_smoke_stage_postimage_drift")
        elif manifest.parent.exists():
            raise MathExactSmokeError("math_smoke_stage_preimage_not_empty")


def apply_exact_smoke_capture(
    execution_authorization_path: Path,
    *,
    formal_id: str,
    runtime_root: Path,
    authority_reader: Callable[[], Mapping[str, Any]],
    smoke_root: Path = SMOKE_ROOT,
    math_repo_root: Path = MATH_REPO_ROOT,
    failpoint: str | None = None,
) -> tuple[str, Path, dict[str, Any]]:
    """Apply exactly the next Capture with two CAS checks and rollback.

    ``failpoint`` exists only for deterministic same-process rollback tests.
    No SIGKILL continuation or generic recovery ledger is implemented here.
    """

    if formal_id not in EXACT_ORDER:
        raise MathExactSmokeError("math_smoke_formal_id_invalid")
    authorization_sha256, authorization = reopen_execution_authorization(
        execution_authorization_path, runtime_root=runtime_root
    )
    descriptor = authorization["authorization_descriptor"]
    assert isinstance(descriptor, Mapping)
    key = _read_authority_key(runtime_root)
    receipts = _load_capture_apply_receipts(
        runtime_root=runtime_root,
        execution_authorization_sha256=authorization_sha256,
        key=key,
    )
    completed_count = len(receipts)
    requested_sequence = EXACT_ORDER.index(formal_id) + 1
    if requested_sequence <= completed_count:
        existing = receipts[requested_sequence - 1]
        if existing[2].get("formal_id") != formal_id:
            raise MathExactSmokeError("math_smoke_capture_apply_sequence_invalid")
        return existing
    if requested_sequence != completed_count + 1:
        raise MathExactSmokeError("math_smoke_capture_apply_out_of_order")

    expected_counts = {
        item: int(index < completed_count)
        for index, item in enumerate(EXACT_ORDER)
    }
    expected_ledger_sha256 = (
        INITIAL_LEDGER_SHA256
        if not receipts
        else str(receipts[-1][2]["ledger_post_sha256"])
    )
    preview_exact_smoke(
        descriptor,
        smoke_root=smoke_root,
        math_repo_root=math_repo_root,
        expected_ledger_sha256=expected_ledger_sha256,
        expected_capture_counts=expected_counts,
    )
    prior_terminal_first: dict[str, Any] | None = None
    if receipts:
        prior_terminal_first = _prior_terminal_snapshot(
            runtime_root=runtime_root,
            formal_id=EXACT_ORDER[0],
            capture_apply_receipt_sha256=receipts[0][0],
            key=key,
        )
    first_authority = dict(authority_reader())
    _validate_authority_snapshot(first_authority, descriptor)
    _validate_canary_state(runtime_root, descriptor, key=key)

    quick = _load_quick_intake(math_repo_root)
    _validate_stage_preimages(quick, completed_count=completed_count)
    sample = _read_json(
        smoke_root / formal_id / "sample.json", "math_smoke_sample_invalid"
    )
    ledger_path = Path(str(quick.EVENTS_PATH))
    ledger_preimage = ledger_path.read_bytes()
    stage_dir: Path | None = None
    stage_created = False
    append_completed = False

    with quick.exclusive_lock(quick.SOURCE_LOCK_PATH):
        with quick.exclusive_lock(quick.LOCK_PATH):
            # Second CAS is deliberately in the same source/capture lock window.
            receipts_second = _load_capture_apply_receipts(
                runtime_root=runtime_root,
                execution_authorization_sha256=authorization_sha256,
                key=key,
            )
            if [row[0] for row in receipts_second] != [row[0] for row in receipts]:
                raise MathExactSmokeError("math_smoke_capture_apply_cas_drift")
            preview_exact_smoke(
                descriptor,
                smoke_root=smoke_root,
                math_repo_root=math_repo_root,
                expected_ledger_sha256=expected_ledger_sha256,
                expected_capture_counts=expected_counts,
            )
            prior_terminal_second: dict[str, Any] | None = None
            if receipts:
                prior_terminal_second = _prior_terminal_snapshot(
                    runtime_root=runtime_root,
                    formal_id=EXACT_ORDER[0],
                    capture_apply_receipt_sha256=receipts[0][0],
                    key=key,
                )
                if canonical_bytes(prior_terminal_first) != canonical_bytes(
                    prior_terminal_second
                ):
                    raise MathExactSmokeError(
                        "math_smoke_prior_terminal_cas_drift"
                    )
            second_authority = dict(authority_reader())
            if canonical_bytes(first_authority) != canonical_bytes(second_authority):
                raise MathExactSmokeError("math_smoke_subject_authority_drift")
            _validate_authority_snapshot(second_authority, descriptor)
            _validate_canary_state(runtime_root, descriptor, key=key)
            _validate_stage_preimages(quick, completed_count=completed_count)

            binding = EXACT_SAMPLES[formal_id]
            capture_template = copy.deepcopy(sample["capture_payload_template"])
            stage_manifest_path: str | None = None
            stage_manifest_sha256: str | None = None
            try:
                if binding["stage_required"]:
                    normalized_stage = quick.normalize_source_stage(
                        copy.deepcopy(sample["source_stage_payload"])
                    )
                    if normalized_stage.get("bundle_id") != binding["stage_bundle_id"]:
                        raise MathExactSmokeError("math_smoke_stage_identity_drift")
                    document = quick.source_manifest_document(normalized_stage)
                    if sha256_bytes(quick.canonical_json_bytes(document)) != binding[
                        "stage_manifest_sha256"
                    ]:
                        raise MathExactSmokeError("math_smoke_stage_manifest_drift")
                    stage_dir = (
                        Path(str(quick.SOURCE_STAGING_ROOT))
                        / normalized_stage["study_date"]
                        / normalized_stage["bundle_id"]
                    )
                    status, _document, manifest_path = quick.stage_source_bundle(
                        normalized_stage
                    )
                    if status != "recorded" or file_sha256(manifest_path) != binding[
                        "stage_manifest_sha256"
                    ]:
                        raise MathExactSmokeError("math_smoke_stage_apply_invalid")
                    stage_created = True
                    stage_manifest_path = str(
                        manifest_path.relative_to(Path(str(quick.REPO_ROOT)))
                    )
                    stage_manifest_sha256 = file_sha256(manifest_path)
                    capture_template["source_bundle"] = {
                        "manifest_path": stage_manifest_path,
                        "manifest_hash": stage_manifest_sha256,
                    }
                elif capture_template.get("source_bundle") is not None:
                    raise MathExactSmokeError("math_smoke_gs566_stage_forbidden")

                normalized = quick.normalize_capture(capture_template)
                target_identity = (
                    normalized["target"].get("formal_id")
                    or normalized["target"].get("source_locator")
                )
                idempotency = {
                    "attempt_id": normalized["attempt_id"],
                    "target_identity": target_identity,
                }
                event_id = quick.stable_event_id("MFI-CAP", idempotency)
                idempotency_key = quick.sha256_value(idempotency)
                if (
                    event_id != binding["capture_event_id"]
                    or idempotency_key != binding["capture_idempotency_key"]
                ):
                    raise MathExactSmokeError("math_smoke_capture_identity_drift")
                current_state = quick.replay(quick.load_jsonl(quick.EVENTS_PATH))
                if current_state["captures"].get(event_id) is not None:
                    raise MathExactSmokeError("math_smoke_duplicate_capture")

                if failpoint == "after_stage_before_capture":
                    raise MathExactSmokeError("math_smoke_test_failpoint")

                payload_hash = quick.sha256_value(normalized)
                event_body = {
                    "schema_version": quick.LEDGER_SCHEMA,
                    "event_type": "capture",
                    "event_id": event_id,
                    "idempotency_key": idempotency_key,
                    "payload_hash": payload_hash,
                    "recorded_at": quick.now_local(),
                    "study_date": normalized["study_date"],
                    "attempt_id": normalized["attempt_id"],
                    "target": normalized["target"],
                    "score_ref": normalized["score_ref"],
                    "requested_action": normalized["requested_action"],
                    "thread_ref": normalized["thread_ref"],
                    "source_bundle": normalized["source_bundle"],
                    "evidence": normalized["evidence"],
                    "initial_state": "pending_nightly",
                    "capture_schema_version": quick.CAPTURE_SCHEMA_V2,
                    "episode_evidence": normalized["episode_evidence"],
                }
                event = quick.with_content_hash(event_body)
                quick.append_jsonl(quick.EVENTS_PATH, event)
                append_completed = True
                if failpoint == "after_capture_before_receipt":
                    raise MathExactSmokeError("math_smoke_test_failpoint")
                reopened = quick.replay(quick.load_jsonl(quick.EVENTS_PATH))[
                    "captures"
                ].get(event_id)
                if (
                    reopened is None
                    or reopened.get("content_hash") != event["content_hash"]
                    or _ledger_capture_counts(ledger_path)[formal_id] != 1
                ):
                    raise MathExactSmokeError("math_smoke_capture_postimage_invalid")
                ledger_post_sha256 = file_sha256(ledger_path)
                receipt_core = {
                    "schema_version": CAPTURE_APPLY_RECEIPT_SCHEMA,
                    "scope_id": EXACT_SCOPE_ID,
                    "sequence": requested_sequence,
                    "formal_id": formal_id,
                    "execution_authorization_sha256": authorization_sha256,
                    "execution_authorization_path": str(
                        execution_authorization_path
                    ),
                    "prior_capture_apply_receipt_sha256": (
                        None if not receipts else receipts[-1][0]
                    ),
                    "sample_sha256": binding["sample_sha256"],
                    "formal_card_sha256": binding["formal_card_sha256"],
                    "question_sha256": binding["question_sha256"],
                    "attachment_sha256": binding["attachment_sha256"],
                    "stage_required": binding["stage_required"],
                    "stage_manifest_path": stage_manifest_path,
                    "stage_manifest_sha256": stage_manifest_sha256,
                    "capture_event_id": event_id,
                    "capture_event_sha256": event["content_hash"],
                    "capture_idempotency_key": idempotency_key,
                    "capture_count_before": 0,
                    "capture_count_after": 1,
                    "ledger_pre_sha256": expected_ledger_sha256,
                    "ledger_post_sha256": ledger_post_sha256,
                    "prior_sample_terminal": copy.deepcopy(
                        prior_terminal_second
                    ),
                    "prior_sample_terminal_sha256": (
                        sha256_bytes(canonical_bytes(prior_terminal_second))
                        if prior_terminal_second is not None
                        else None
                    ),
                    "authority_snapshots": [
                        copy.deepcopy(first_authority),
                        copy.deepcopy(second_authority),
                    ],
                    "authority_snapshot_sha256s": [
                        sha256_bytes(canonical_bytes(first_authority)),
                        sha256_bytes(canonical_bytes(second_authority)),
                    ],
                    "authority_snapshot_count": 2,
                    "authority_snapshot_mcp_tool_call_count": 2,
                    "applied_at": _utc_now(),
                    "source_stage_write_count": (
                        1 if binding["stage_required"] else 0
                    ),
                    "capture_write_count": 1,
                    "queue_write_count": 0,
                    "model_call_count": 0,
                    "provider_request_count": 0,
                    "model_mcp_tool_call_count": 0,
                    "sol_enabled": False,
                    "formal_write_count": 0,
                }
                receipt = _seal(
                    receipt_core, purpose=CAPTURE_APPLY_PURPOSE, key=key
                )
                digest, path = _publish_content_addressed(
                    _capture_apply_root(runtime_root), receipt
                )
                return digest, path, receipt
            except Exception:
                if append_completed:
                    _atomic_restore_bytes(ledger_path, ledger_preimage)
                if stage_created and stage_dir is not None:
                    expected_root = Path(str(quick.SOURCE_STAGING_ROOT)).resolve()
                    try:
                        resolved_stage = stage_dir.resolve(strict=True)
                        resolved_stage.relative_to(expected_root)
                    except (OSError, ValueError) as exc:
                        raise MathExactSmokeError(
                            "math_smoke_same_process_rollback_unproven"
                        ) from exc
                    shutil.rmtree(resolved_stage)
                    parent = resolved_stage.parent
                    directory = os.open(parent, os.O_RDONLY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                if ledger_path.read_bytes() != ledger_preimage:
                    raise MathExactSmokeError(
                        "math_smoke_same_process_rollback_unproven"
                    )
                if stage_dir is not None and stage_dir.exists():
                    raise MathExactSmokeError(
                        "math_smoke_same_process_rollback_unproven"
                    )
                raise


__all__ = [
    "DESCRIPTOR_SCHEMA",
    "EXACT_ORDER",
    "EXACT_SAMPLES",
    "EXACT_SCOPE_ID",
    "EXACT_STUDY_DATE",
    "MathExactSmokeError",
    "build_authorization_descriptor",
    "pending_exact_smoke_dispatch_scope",
    "reopen_exact_gs269_review_terminal",
    "preview_exact_smoke",
    "validate_exact_math_migration_materialization",
    "verify_committed_exact_math_migration_claim",
]
