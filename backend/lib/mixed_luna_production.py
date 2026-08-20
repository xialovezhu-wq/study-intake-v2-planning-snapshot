"""Production adapters for the three-subject direct-MCP smoke lane.

The module contains no model stub and no Sol writer.  English delegates to the
ordinary candidate-bound capture lane; EN-P0-006 existing-object inventory is
never selected here.  Math, 408, and English delegate to candidate-bound
``FrozenTask`` objects and real ``ProductionDispatchRuntime`` instances supplied
by the CLI.  Every adapter independently reopens its persisted quality closure.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import copy
import hashlib
import json
from pathlib import Path
from typing import Any, Protocol

if __package__:
    from .concurrent_dispatch import DispatchError, FrozenTask
    from .core_dispatch_bridge import scan_eligible_candidates
    from .english_legacy_recuration import (
        EnglishLegacyRecurationError,
        reopen_work_item_batch,
        run_work_item,
        verify_quality_receipt,
    )
    from .mixed_luna_stress import MixedLunaStressError, RUN_COUNTS
    from .math_live_direct_mcp import (
        DIRECT_MCP_DISPATCH_REASON,
        MathLiveDirectMcpError,
        build_live_math_direct_mcp_execution,
        register_live_math_direct_candidates,
    )
    from .preprocessor_core import (
        Candidate,
        PreprocessorError,
        canonical_subject_package_path,
        current_date,
        json_file_bytes,
        reopen_verified_subject_publication,
        sha256_file,
        sha256_value,
    )
    from .processing_plugin import ProcessingPluginError, SUBJECT_MCP_SERVERS
else:
    from concurrent_dispatch import DispatchError, FrozenTask  # type: ignore[no-redef]
    from core_dispatch_bridge import scan_eligible_candidates  # type: ignore[no-redef]
    from english_legacy_recuration import (  # type: ignore[no-redef]
        EnglishLegacyRecurationError,
        reopen_work_item_batch,
        run_work_item,
        verify_quality_receipt,
    )
    from mixed_luna_stress import MixedLunaStressError, RUN_COUNTS  # type: ignore[no-redef]
    from math_live_direct_mcp import (  # type: ignore[no-redef]
        DIRECT_MCP_DISPATCH_REASON,
        MathLiveDirectMcpError,
        build_live_math_direct_mcp_execution,
        register_live_math_direct_candidates,
    )
    from preprocessor_core import (  # type: ignore[no-redef]
        Candidate,
        PreprocessorError,
        canonical_subject_package_path,
        current_date,
        json_file_bytes,
        reopen_verified_subject_publication,
        sha256_file,
        sha256_value,
    )
    from processing_plugin import (  # type: ignore[no-redef]
        ProcessingPluginError,
        SUBJECT_MCP_SERVERS,
    )


class ProductionMixedAdapter(Protocol):
    subject: str

    def selection_rows(self, run_mode: str) -> list[dict[str, Any]]: ...

    def prepare(self) -> None: ...

    def run(
        self, task: Mapping[str, Any], observe: Callable[..., None]
    ) -> dict[str, Any]: ...

    def verify(self, task: Mapping[str, Any], result: Mapping[str, Any]) -> None: ...

    def close(self) -> None: ...


def _mixed_error(code: str, exc: BaseException | None = None) -> MixedLunaStressError:
    error = MixedLunaStressError(code)
    if exc is not None:
        error.__cause__ = exc
    return error


def _load_object(path: Path, code: str) -> dict[str, Any]:
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 16 * 1024 * 1024:
            raise OSError("unsafe object")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise _mixed_error(code, exc)
    if not isinstance(value, dict):
        raise _mixed_error(code)
    return value


def _binding_sha256(row: Mapping[str, Any], capture_identity: str) -> str:
    return sha256_value(
        {
            "subject": row["subject"],
            "task_id": row["task_id"],
            "unit_sha256": row["unit_sha256"],
            "capture_identity": capture_identity,
            "subject_batch_id": row["subject_batch_id"],
            "subject_batch_sha256": row["subject_batch_sha256"],
            "mcp_namespace": row["mcp_namespace"],
            "generation": row["generation"],
            "authority_fingerprint": row["authority_fingerprint"],
            "purpose": "mixed_luna_read_session_scope_v1",
        }
    )


def _validate_adapter_task(
    expected: Mapping[str, Any], actual: Mapping[str, Any]
) -> None:
    for field in (
        "subject",
        "task_id",
        "unit_sha256",
        "subject_batch_id",
        "dispatcher_id",
        "mcp_namespace",
        "generation",
        "read_session_binding_sha256",
    ):
        if actual.get(field) != expected.get(field):
            raise _mixed_error("mixed_production_task_binding_mismatch")


def load_math_composite_tasks(
    *,
    config: Mapping[str, Any],
    release_id: str,
    live_manifest_path: Path,
    controlled_tasks: Sequence[FrozenTask],
    runtime: object,
    execution_attempt: int,
    execution_runtime_id: str,
) -> tuple[str, tuple[FrozenTask, ...], tuple[Candidate, ...]]:
    """Combine live 001/002/003 with three Golden tasks for mixed stress.

    Live tasks are placed first so even the 3-task smoke executes one real
    direct-MCP math capture.  The 20- and 108-task stages select all six.
    """

    if len(controlled_tasks) != 3:
        raise _mixed_error("mixed_math_controlled_task_set_incomplete")
    try:
        execution = build_live_math_direct_mcp_execution(
            config=config,
            release_id=release_id,
            authority_manifest_path=live_manifest_path,
            execution_attempt=execution_attempt,
            execution_runtime_id=execution_runtime_id,
        )
        register_live_math_direct_candidates(runtime, execution)
    except MathLiveDirectMcpError as exc:
        raise _mixed_error(exc.args[0], exc)
    combined = execution.tasks + tuple(controlled_tasks)
    if len(combined) != 6 or len({row.unit_sha256 for row in combined}) != 6:
        raise _mixed_error("mixed_math_composite_task_set_invalid")
    scope_sha = sha256_value(
        {
            "controlled_unit_sha256s": [row.unit_sha256 for row in controlled_tasks],
            "live_execution_scope_sha256": execution.scope_sha256,
            "live_business_manifest_sha256": execution.authority_manifest_sha256,
            "live_business_unit_sha256s": [row.unit_sha256 for row in execution.tasks],
            "execution_attempt": execution_attempt,
            "execution_runtime_id": execution_runtime_id,
            "purpose": "mixed_math_composite_scope_v2",
            "formal_write_count": 0,
        }
    )
    return scope_sha, combined, execution.candidates


def load_math_new_business_task(
    *,
    config: Mapping[str, Any],
    release_id: str,
    live_manifest_path: Path,
    runtime: object,
    execution_attempt: int,
    execution_runtime_id: str,
) -> tuple[str, tuple[FrozenTask, ...], tuple[Candidate, ...]]:
    """Select the sole not-yet-ingested live math capture for real smoke.

    Existing GS-109 and GS-507 review captures remain available as separate
    regression samples, but they are not counted as new-business smoke work.
    """

    try:
        execution = build_live_math_direct_mcp_execution(
            config=config,
            release_id=release_id,
            authority_manifest_path=live_manifest_path,
            execution_attempt=execution_attempt,
            execution_runtime_id=execution_runtime_id,
        )
        selected = [
            unit
            for unit in execution.units
            if unit.formal_id is None
            and unit.candidate.input_binding.get("source_route") == "new_intake"
        ]
        if len(selected) != 1:
            raise MathLiveDirectMcpError(
                "math_live_new_business_task_set_invalid"
            )
        register = getattr(runtime, "register_controlled_replay_candidate", None)
        if not callable(register):
            raise MathLiveDirectMcpError(
                "math_live_direct_mcp_runtime_registration_missing"
            )
        register(
            selected[0].task,
            selected[0].candidate,
            reason=DIRECT_MCP_DISPATCH_REASON,
        )
    except (MathLiveDirectMcpError, DispatchError) as exc:
        raise _mixed_error(getattr(exc, "code", str(exc)), exc)
    scope_sha = sha256_value(
        {
            "live_execution_scope_sha256": execution.scope_sha256,
            "live_business_manifest_sha256": execution.authority_manifest_sha256,
            "selected_capture_id": selected[0].candidate.capture_id,
            "selected_unit_sha256": selected[0].task.unit_sha256,
            "selection_reason": "not_yet_ingested_new_source",
            "execution_attempt": execution_attempt,
            "execution_runtime_id": execution_runtime_id,
            "purpose": "mixed_math_new_business_scope_v1",
            "formal_write_count": 0,
        }
    )
    return scope_sha, (selected[0].task,), (selected[0].candidate,)


class EnglishLegacyMixedAdapter:
    subject = "english"

    def __init__(
        self,
        *,
        config: Mapping[str, Any],
        runtime_root: Path,
        work_item_batch_path: Path,
        processing_host: object,
    ) -> None:
        self.config = copy.deepcopy(dict(config))
        self.runtime_root = runtime_root.resolve()
        self.processing_host = processing_host
        try:
            batch_sha, batch, items = reopen_work_item_batch(work_item_batch_path)
            snapshot = processing_host.subject_authority_snapshot("english")
        except (EnglishLegacyRecurationError, ProcessingPluginError, AttributeError) as exc:
            raise _mixed_error("mixed_english_batch_reopen_invalid", exc)
        expected_root = (
            self.runtime_root / "dispatch" / "english-legacy-recuration"
        ).resolve()
        if (
            work_item_batch_path.expanduser().absolute().parents[3].resolve()
            != expected_root
            or len(items) != 98
            or batch.get("target_count") != 98
            or batch.get("authority")
            != {
                "generation": snapshot.get("generation"),
                "authority_fingerprint": snapshot.get("authority_fingerprint"),
            }
            or any(item.get("authority") != batch.get("authority") for item in items)
        ):
            raise _mixed_error("mixed_english_candidate_authority_or_path_invalid")
        self.batch_sha256 = batch_sha
        self.batch = batch
        self.items = tuple(items)
        self.snapshot = copy.deepcopy(dict(snapshot))
        self.dispatcher_id = "english-legacy-recuration-v1"
        self._rows: dict[str, dict[str, Any]] = {}
        self._items_by_task: dict[str, dict[str, Any]] = {}

    def selection_rows(self, run_mode: str) -> list[dict[str, Any]]:
        count = RUN_COUNTS[run_mode]["english"]
        expected_attempt = {
            "smoke_3": 1,
            "smoke_20": 2,
            "full_108": 3,
        }[run_mode]
        if any(item.get("attempt") != expected_attempt for item in self.items):
            raise _mixed_error("mixed_english_stage_attempt_binding_invalid")
        rows: list[dict[str, Any]] = []
        self._rows.clear()
        self._items_by_task.clear()
        for item in self.items[:count]:
            work_sha = hashlib.sha256(json_file_bytes(item)).hexdigest()
            task_id = f"english-legacy:{work_sha}"
            row = {
                "task_id": task_id,
                "unit_sha256": work_sha,
                "subject": "english",
                "subject_scope_sha256": self.batch["target_set_sha256"],
                "subject_batch_id": item["remediation_batch_id"],
                "subject_batch_sha256": self.batch_sha256,
                "dispatcher_id": self.dispatcher_id,
                "mcp_namespace": SUBJECT_MCP_SERVERS["english"],
                "generation": self.snapshot["generation"],
                "authority_fingerprint": self.snapshot["authority_fingerprint"],
                "sol_authorized": False,
                "formal_write_count": 0,
            }
            capture_id = (
                f"EN-P0-006-{int(item['ordinal']):03d}-{work_sha[:20].upper()}"
            )
            row["read_session_binding_sha256"] = _binding_sha256(
                row, capture_id
            )
            rows.append(row)
            self._rows[task_id] = copy.deepcopy(row)
            self._items_by_task[task_id] = copy.deepcopy(item)
        return rows

    def prepare(self) -> None:
        try:
            current = self.processing_host.subject_authority_snapshot("english")
        except (ProcessingPluginError, AttributeError) as exc:
            raise _mixed_error("mixed_english_authority_refresh_failed", exc)
        if (
            current.get("generation") != self.snapshot.get("generation")
            or current.get("authority_fingerprint")
            != self.snapshot.get("authority_fingerprint")
        ):
            raise _mixed_error("mixed_english_candidate_authority_drift")

    def _reopen_closure(self, task: Mapping[str, Any], quality_sha: str) -> dict[str, Any]:
        try:
            verified = verify_quality_receipt(
                self.config, self.runtime_root, quality_sha
            )
        except EnglishLegacyRecurationError as exc:
            raise _mixed_error("mixed_english_quality_reopen_invalid", exc)
        base = self.runtime_root / "dispatch" / "english-legacy-recuration"
        package_sha = str(verified["package_sha256"])
        package = _load_object(
            base / "packages" / "sha256" / package_sha[:2] / f"{package_sha}.json",
            "mixed_english_package_reopen_invalid",
        )
        if sha256_file(
            base / "packages" / "sha256" / package_sha[:2] / f"{package_sha}.json"
        ) != package_sha:
            raise _mixed_error("mixed_english_package_reopen_invalid")
        binding = _load_object(
            base / "completion-bindings" / f"{task['unit_sha256']}.json",
            "mixed_english_completion_binding_invalid",
        )
        if (
            binding.get("work_item_sha256") != task["unit_sha256"]
            or binding.get("package_sha256") != package_sha
            or binding.get("quality_receipt_sha256") != quality_sha
            or (base / "needs-rework-bindings" / f"{task['unit_sha256']}.json").exists()
        ):
            raise _mixed_error("mixed_english_completion_binding_invalid")
        try:
            reopened = self.processing_host.reopen_published_read_session(
                subject="english",
                publication=package["processing_publication"],
                stage_receipts=package["stage_receipts"],
            )
            context = reopened["context"]
            call_receipts = {}
            for stage in ("analysis", "critical_review"):
                receipt = package["stage_receipts"][stage]
                call_receipts[stage] = self.processing_host.validate_model_mcp_call_receipt(
                    subject="english",
                    context=context,
                    receipt_sha256=receipt["mcp_call_receipt_sha256"],
                    expected_stage_name=str(receipt["prompt_version"]),
                    expected_transcript_sha256=receipt["mcp_transcript_sha256"],
                    expected_mcp_tool_call_count=receipt["mcp_tool_call_count"],
                    expected_provider_request_count=receipt["provider_request_count"],
                    require_success=True,
                )
        except (ProcessingPluginError, KeyError, TypeError) as exc:
            raise _mixed_error("mixed_english_mcp_closure_invalid", exc)
        session = reopened["context"]["mcp_read_session"]
        binding_mcp = package["processing_publication"]["processing_binding"].get(
            "mcp"
        )
        if (
            session.get("subject") != "english"
            or session.get("generation") != task["generation"]
            or session.get("authority_fingerprint")
            != self.snapshot["authority_fingerprint"]
            or session.get("capture_id")
            != (
                f"EN-P0-006-"
                f"{int(self._items_by_task[task['task_id']]['ordinal']):03d}-"
                f"{str(task['unit_sha256'])[:20].upper()}"
            )
            or not isinstance(binding_mcp, Mapping)
            or binding_mcp.get("id") != task["mcp_namespace"]
        ):
            raise _mixed_error("mixed_english_read_session_binding_invalid")
        result = {
            "status": "quality_passed",
            "quality_outcome": verified["quality_outcome"],
            "proposal_action": verified["proposal_action"],
            "subject": "english",
            "task_id": task["task_id"],
            "unit_sha256": task["unit_sha256"],
            "subject_batch_id": task["subject_batch_id"],
            "dispatcher_id": task["dispatcher_id"],
            "mcp_namespace": task["mcp_namespace"],
            "generation": task["generation"],
            "read_session_binding_sha256": task["read_session_binding_sha256"],
            "read_session_id": session["read_session_id"],
            "read_session_manifest_sha256": package["processing_publication"][
                "read_session_manifest_sha256"
            ],
            "analysis_transcript_sha256": package["stage_receipts"]["analysis"][
                "mcp_transcript_sha256"
            ],
            "analysis_stage_receipt_sha256": package["stage_receipts"]["analysis"][
                "mcp_call_receipt_sha256"
            ],
            "analysis_stage_receipt_hmac_sha256": call_receipts["analysis"][
                "hmac_sha256"
            ],
            "critical_review_transcript_sha256": package["stage_receipts"]
            ["critical_review"]["mcp_transcript_sha256"],
            "critical_review_stage_receipt_sha256": package["stage_receipts"]
            ["critical_review"]["mcp_call_receipt_sha256"],
            "critical_review_stage_receipt_hmac_sha256": call_receipts[
                "critical_review"
            ]["hmac_sha256"],
            "final_read_session_receipt_sha256": package[
                "final_read_session_receipt_sha256"
            ],
            "package_sha256": package_sha,
            "quality_receipt_sha256": quality_sha,
            "model_call_count": 2,
            "formal_write_count": 0,
        }
        _validate_adapter_task(task, result)
        return result

    def run(
        self, task: Mapping[str, Any], observe: Callable[..., None]
    ) -> dict[str, Any]:
        expected = self._rows.get(str(task.get("task_id") or ""))
        item = self._items_by_task.get(str(task.get("task_id") or ""))
        if expected is None or item is None:
            raise _mixed_error("mixed_english_task_not_selected")
        _validate_adapter_task(expected, task)
        try:
            terminal = run_work_item(
                self.config,
                self.runtime_root,
                item,
                stage_observer=observe,
            )
        except EnglishLegacyRecurationError as exc:
            raise _mixed_error(exc.code, exc)
        if terminal.get("status") != "succeeded":
            raise _mixed_error(
                str(terminal.get("error_code") or "mixed_english_quality_not_passed")
            )
        return self._reopen_closure(task, str(terminal["quality_receipt_sha256"]))

    def verify(self, task: Mapping[str, Any], result: Mapping[str, Any]) -> None:
        reopened = self._reopen_closure(task, str(result["quality_receipt_sha256"]))
        if reopened != result:
            raise _mixed_error("mixed_english_independent_reopen_mismatch")

    def close(self) -> None:
        return None


class ControlledReplaySubjectMixedAdapter:
    """Real subject adapter over candidate FrozenTask and Dispatcher paths."""

    def __init__(
        self,
        *,
        subject: str,
        config: Mapping[str, Any],
        config_path: Path,
        release_id: str,
        spec_sha256: str,
        tasks: Sequence[FrozenTask],
        runtime: object,
        processing_host: object,
        task_timeout_seconds: float,
    ) -> None:
        if subject not in {"math", "cs408", "english"}:
            raise _mixed_error("mixed_controlled_subject_invalid")
        self.subject = subject
        self.config = copy.deepcopy(dict(config))
        self.config_path = config_path.resolve()
        self.release_id = release_id
        self.spec_sha256 = spec_sha256
        self.tasks = tuple(tasks)
        self.runtime = runtime
        self.processing_host = processing_host
        self.task_timeout_seconds = float(task_timeout_seconds)
        if self.task_timeout_seconds <= 0:
            raise _mixed_error("mixed_controlled_timeout_invalid")
        try:
            self.snapshot = copy.deepcopy(
                dict(processing_host.subject_authority_snapshot(subject))
            )
        except Exception as exc:
            raise _mixed_error("mixed_controlled_selection_prefreeze_invalid", exc)
        self.batch_sha256 = ""
        self.batch_id = ""
        self.dispatcher_id = str(runtime.dispatcher.owner_id)
        self._rows: dict[str, dict[str, Any]] = {}
        self._tasks_by_id: dict[str, FrozenTask] = {}
        self._prepared = False

    def selection_rows(self, run_mode: str) -> list[dict[str, Any]]:
        count = RUN_COUNTS[run_mode][self.subject]
        if len(self.tasks) != count:
            raise _mixed_error(f"mixed_{self.subject}_golden_task_set_incomplete")
        rows: list[dict[str, Any]] = []
        self._rows.clear()
        self._tasks_by_id.clear()
        selected = self.tasks[:count]
        study_date = current_date(str(self.config.get("timezone") or "UTC"))
        try:
            _snapshot, scan_sha, _high_water, _task_rows = self.runtime._scan_snapshot(
                self.subject, selected, study_date
            )
        except Exception as exc:
            raise _mixed_error("mixed_controlled_selection_prefreeze_invalid", exc)
        self.batch_sha256 = scan_sha
        self.batch_id = (
            f"LUNA-{self.subject.upper()}-{study_date}-{scan_sha[:20].upper()}"
        )
        for frozen in selected:
            payload = frozen.frozen_payload
            capture_id = str(payload.get("capture_id") or "")
            if not capture_id:
                raise _mixed_error("mixed_controlled_capture_identity_missing")
            task_id = f"{self.subject}-controlled:{frozen.unit_sha256}"
            row = {
                "task_id": task_id,
                "unit_sha256": frozen.unit_sha256,
                "subject": self.subject,
                "subject_scope_sha256": self.spec_sha256,
                "subject_batch_id": self.batch_id,
                "subject_batch_sha256": self.batch_sha256,
                "dispatcher_id": self.dispatcher_id,
                "mcp_namespace": SUBJECT_MCP_SERVERS[self.subject],
                "generation": self.snapshot["generation"],
                "authority_fingerprint": self.snapshot["authority_fingerprint"],
                "sol_authorized": False,
                "formal_write_count": 0,
            }
            row["read_session_binding_sha256"] = _binding_sha256(row, capture_id)
            rows.append(row)
            self._rows[task_id] = copy.deepcopy(row)
            self._tasks_by_id[task_id] = frozen
        return rows

    def prepare(self) -> None:
        selected = tuple(self._tasks_by_id.values())
        if not selected:
            raise _mixed_error("mixed_controlled_selection_missing")
        try:
            prepared = self.runtime.prepare_controlled_replay_tasks(
                selected,
                release_id=self.release_id,
            )
        except Exception as exc:
            raise _mixed_error(
                getattr(exc, "code", "mixed_controlled_prefreeze_failed"), exc
            )
        authority = prepared.get("batch_authority")
        if (
            prepared.get("dispatcher_id") != self.dispatcher_id
            or not isinstance(authority, Mapping)
            or authority.get("batch_id") != self.batch_id
            or authority.get("scan_snapshot_sha256") != self.batch_sha256
            or authority.get("generation") != self.snapshot.get("generation")
            or authority.get("authority_fingerprint")
            != self.snapshot.get("authority_fingerprint")
        ):
            raise _mixed_error("mixed_controlled_prefreeze_binding_mismatch")
        self._prepared = True

    def _event_history(self, frozen: FrozenTask) -> dict[str, Any]:
        try:
            return self.runtime.dispatcher.lease_store.verify_task_event_history(
                frozen.unit_sha256,
                expected_release_id=self.release_id,
            )
        except DispatchError as exc:
            raise _mixed_error(exc.code, exc)

    def _reopen_closure(self, task: Mapping[str, Any]) -> dict[str, Any]:
        frozen = self._tasks_by_id.get(str(task.get("task_id") or ""))
        if frozen is None:
            raise _mixed_error("mixed_controlled_task_not_selected")
        payload = frozen.frozen_payload
        capture_id = str(payload.get("capture_id") or "")
        study_date = str(payload.get("study_date") or "")
        input_fingerprint = str(payload.get("input_fingerprint") or "")
        try:
            publication = reopen_verified_subject_publication(
                self.config,
                subject=self.subject,
                capture_id=capture_id,
                study_date=study_date,
                input_fingerprint=input_fingerprint,
            )
            package_sha = str(publication["subject_package_sha256"])
            package_path = canonical_subject_package_path(
                Path(str(self.config["runtime_root"])),
                subject=self.subject,
                study_date=study_date,
                capture_id=capture_id,
                input_fingerprint=input_fingerprint,
                package_sha256=package_sha,
            )
            if package_path.resolve(strict=True) != package_path:
                raise _mixed_error("mixed_controlled_package_invalid")
            package = _load_object(package_path, "mixed_controlled_package_invalid")
            if sha256_file(package_path) != package_sha:
                raise _mixed_error("mixed_controlled_package_invalid")
            reopened = self.processing_host.reopen_published_read_session(
                subject=self.subject,
                publication=package,
                stage_receipts=package["stage_receipts"],
            )
            context = reopened["context"]
            calls = {}
            for stage in ("analysis", "critical_review"):
                receipt = package["stage_receipts"][stage]
                calls[stage] = self.processing_host.validate_model_mcp_call_receipt(
                    subject=self.subject,
                    context=context,
                    receipt_sha256=receipt["mcp_call_receipt_sha256"],
                    expected_stage_name=str(receipt["prompt_version"]),
                    expected_transcript_sha256=receipt["mcp_transcript_sha256"],
                    expected_mcp_tool_call_count=receipt["mcp_tool_call_count"],
                    expected_provider_request_count=receipt["provider_request_count"],
                    require_success=True,
                )
            verified_dispatch = self.runtime.dispatcher.lease_store.verify_authoritative_completion(
                self.subject,
                capture_id,
                expected_release_id=self.release_id,
                expected_unit_sha256=frozen.unit_sha256,
            )
            batch = self.runtime.subject_sol.read_subject_batch(self.subject)
        except (PreprocessorError, ProcessingPluginError, DispatchError, KeyError, TypeError) as exc:
            raise _mixed_error(
                getattr(exc, "code", "mixed_controlled_artifact_reopen_invalid"), exc
            )
        batch_tasks = batch.get("tasks") if isinstance(batch, Mapping) else None
        quality_task = next(
            (
                row
                for row in batch_tasks or []
                if isinstance(row, Mapping)
                and row.get("unit_sha256") == frozen.unit_sha256
                and row.get("capture_id") == capture_id
            ),
            None,
        )
        history = self._event_history(frozen)
        owners = {
            row["event"].get("owner_id")
            for row in history["events"]
            if isinstance(row.get("event"), Mapping)
        }
        session = reopened["context"]["mcp_read_session"]
        binding_mcp = package.get("processing_binding", {}).get("mcp")
        if (
            verified_dispatch.get("completion", {}).get("outcome") != "succeeded"
            or publication.get("critical_review_outcome") not in {"accepted", "corrected"}
            or publication.get("review_status") != "proposal_ready"
            or publication.get("evidence_generation") != task["generation"]
            or publication.get("evidence_authority_fingerprint")
            != task["authority_fingerprint"]
            or not isinstance(quality_task, Mapping)
            or quality_task.get("status") != "quality_passed"
            or quality_task.get("package_sha256") != package_sha
            or owners != {self.dispatcher_id}
            or session.get("subject") != self.subject
            or session.get("capture_id") != capture_id
            or session.get("generation") != task["generation"]
            or session.get("authority_fingerprint")
            != task["authority_fingerprint"]
            or not isinstance(binding_mcp, Mapping)
            or binding_mcp.get("id") != task["mcp_namespace"]
        ):
            raise _mixed_error("mixed_controlled_quality_closure_invalid")
        result = {
            "status": "quality_passed",
            "quality_outcome": publication["critical_review_outcome"],
            "proposal_action": "proposal_ready",
            "subject": self.subject,
            "task_id": task["task_id"],
            "unit_sha256": task["unit_sha256"],
            "subject_batch_id": task["subject_batch_id"],
            "dispatcher_id": task["dispatcher_id"],
            "mcp_namespace": task["mcp_namespace"],
            "generation": task["generation"],
            "read_session_binding_sha256": task["read_session_binding_sha256"],
            "read_session_id": context["mcp_read_session"]["read_session_id"],
            "read_session_manifest_sha256": publication[
                "read_session_manifest_sha256"
            ],
            "analysis_transcript_sha256": package["stage_receipts"]["analysis"][
                "mcp_transcript_sha256"
            ],
            "analysis_stage_receipt_sha256": package["stage_receipts"]["analysis"][
                "mcp_call_receipt_sha256"
            ],
            "analysis_stage_receipt_hmac_sha256": calls["analysis"]["hmac_sha256"],
            "critical_review_transcript_sha256": package["stage_receipts"]
            ["critical_review"]["mcp_transcript_sha256"],
            "critical_review_stage_receipt_sha256": package["stage_receipts"]
            ["critical_review"]["mcp_call_receipt_sha256"],
            "critical_review_stage_receipt_hmac_sha256": calls["critical_review"][
                "hmac_sha256"
            ],
            "final_read_session_receipt_sha256": publication[
                "mcp_read_session_receipt_sha256"
            ],
            "package_sha256": package_sha,
            "quality_receipt_sha256": quality_task["quality_receipt_sha256"],
            "model_call_count": 2,
            "formal_write_count": 0,
        }
        _validate_adapter_task(task, result)
        return result

    def run(
        self, task: Mapping[str, Any], observe: Callable[..., None]
    ) -> dict[str, Any]:
        expected = self._rows.get(str(task.get("task_id") or ""))
        frozen = self._tasks_by_id.get(str(task.get("task_id") or ""))
        if not self._prepared or expected is None or frozen is None:
            raise _mixed_error("mixed_controlled_task_not_prepared")
        _validate_adapter_task(expected, task)
        try:
            handle = self.runtime.submit_prepared_controlled_replay_task(frozen)
        except Exception as exc:
            raise _mixed_error(
                getattr(exc, "code", "mixed_controlled_dispatch_failed"), exc
            )
        try:
            terminal = handle.wait(self.task_timeout_seconds)
        except TimeoutError as exc:
            # A mixed-campaign deadline is local to this subject handle.  Do
            # not return it to the coordinator while its dispatcher lease is
            # still active: request cancellation, then wait for the immutable
            # terminal result that removes only this handle from _active.
            try:
                handle.cancel()
                handle.wait()
            except Exception as terminal_exc:
                raise _mixed_error(
                    getattr(
                        terminal_exc,
                        "code",
                        "mixed_controlled_dispatch_failed",
                    ),
                    terminal_exc,
                )
            raise _mixed_error("mixed_controlled_dispatch_failed", exc)
        except Exception as exc:
            raise _mixed_error(
                getattr(exc, "code", "mixed_controlled_dispatch_failed"), exc
            )
        history = self._event_history(frozen)
        attempt = history["detail"].get("fence")
        event_map = {
            "analysis_submitted": "analysis_submitted",
            "analysis_completed": "analysis_completed",
            "critical_started": "critical_review_submitted",
            "critical_completed": "critical_review_completed",
        }
        stage_rows = [
            row
            for row in history["events"]
            if row.get("attempt") == attempt
            and row.get("event", {}).get("event") in event_map
        ]
        stage_names = [row["event"]["event"] for row in stage_rows]
        expected_names = [
            "analysis_submitted",
            "analysis_completed",
            "critical_started",
            "critical_completed",
        ]
        if len(stage_names) > 4 or stage_names != expected_names[: len(stage_names)]:
            raise _mixed_error("mixed_controlled_stage_history_invalid")
        for row in stage_rows:
            observe(event_map[row["event"]["event"]], row["event"]["occurred_at"])
        if (
            terminal.outcome != "succeeded"
            or history.get("model_call_count") != 2
            or stage_names != expected_names
        ):
            raise _mixed_error(
                str(terminal.error_code or "mixed_controlled_stage_history_invalid")
            )
        try:
            self.runtime.persist_finished_luna([handle])
        except Exception as exc:
            raise _mixed_error(
                getattr(exc, "code", "mixed_controlled_quality_publish_failed"), exc
            )
        return self._reopen_closure(task)

    def verify(self, task: Mapping[str, Any], result: Mapping[str, Any]) -> None:
        reopened = self._reopen_closure(task)
        if reopened != result:
            raise _mixed_error("mixed_controlled_independent_reopen_mismatch")

    def close(self) -> None:
        try:
            if not self.runtime.dispatcher.drain(30):
                raise _mixed_error("mixed_controlled_dispatcher_drain_timeout")
        except DispatchError as exc:
            raise _mixed_error(exc.code, exc)


def build_selection(
    *,
    campaign_id: str,
    candidate_release_id: str,
    execution_runtime_sha256: str,
    run_mode: str,
    execution_attempt: int,
    max_workers: int,
    adapters: Sequence[ProductionMixedAdapter],
) -> tuple[dict[str, Any], dict[str, ProductionMixedAdapter]]:
    if run_mode not in RUN_COUNTS or max_workers != sum(RUN_COUNTS[run_mode].values()):
        raise _mixed_error("mixed_production_worker_count_invalid")
    by_subject = {adapter.subject: adapter for adapter in adapters}
    if set(by_subject) != {"english", "math", "cs408"}:
        raise _mixed_error("mixed_production_adapter_set_invalid")
    tasks: list[dict[str, Any]] = []
    for subject in ("english", "math", "cs408"):
        rows = by_subject[subject].selection_rows(run_mode)
        if len(rows) != RUN_COUNTS[run_mode][subject]:
            raise _mixed_error("mixed_production_subject_count_invalid")
        tasks.extend(rows)
    for ordinal, row in enumerate(tasks, start=1):
        row["ordinal"] = ordinal
    scopes = {
        subject: next(
            row["subject_scope_sha256"] for row in tasks if row["subject"] == subject
        )
        for subject in ("english", "math", "cs408")
    }
    return (
        {
            "schema_version": "mixed_luna_stress_selection_v1",
            "campaign_id": campaign_id,
            "candidate_release_id": candidate_release_id,
            "run_mode": run_mode,
            "execution_attempt": execution_attempt,
            "execution_runtime_sha256": execution_runtime_sha256,
            "max_workers": max_workers,
            "subject_scopes": scopes,
            "tasks": tasks,
            "sol_enabled": False,
            "formal_write_count": 0,
        },
        by_subject,
    )


def prepare_adapters(adapters: Mapping[str, ProductionMixedAdapter]) -> None:
    """Complete every zero-model prefreeze before any common-barrier worker."""

    for subject in ("english", "math", "cs408"):
        adapters[subject].prepare()


def production_runner(
    adapters: Mapping[str, ProductionMixedAdapter]
) -> Callable[[Mapping[str, Any], Callable[..., None]], Mapping[str, Any]]:
    def run(task: Mapping[str, Any], observe: Callable[..., None]) -> Mapping[str, Any]:
        adapter = adapters.get(str(task.get("subject") or ""))
        if adapter is None:
            raise _mixed_error("mixed_production_subject_adapter_missing")
        return adapter.run(task, observe)

    return run


def production_verifier(
    adapters: Mapping[str, ProductionMixedAdapter]
) -> Callable[[Mapping[str, Any], Mapping[str, Any]], None]:
    def verify(task: Mapping[str, Any], result: Mapping[str, Any]) -> None:
        adapter = adapters.get(str(task.get("subject") or ""))
        if adapter is None:
            raise _mixed_error("mixed_production_subject_adapter_missing")
        adapter.verify(task, result)

    return verify


def close_adapters(adapters: Mapping[str, ProductionMixedAdapter]) -> None:
    errors: list[MixedLunaStressError] = []
    for subject in ("english", "math", "cs408"):
        try:
            adapters[subject].close()
        except MixedLunaStressError as exc:
            errors.append(exc)
    if errors:
        raise errors[0]


__all__ = [
    "ControlledReplaySubjectMixedAdapter",
    "EnglishLegacyMixedAdapter",
    "ProductionMixedAdapter",
    "build_selection",
    "close_adapters",
    "load_math_composite_tasks",
    "load_math_new_business_task",
    "prepare_adapters",
    "production_runner",
    "production_verifier",
]
