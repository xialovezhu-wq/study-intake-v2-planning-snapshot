from __future__ import annotations

import copy
import datetime as dt
import hashlib
import hmac
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))
sys.path.insert(0, str(ROOT / "bin"))

from concurrent_dispatch import (  # noqa: E402
    ConcurrentDispatcher,
    DispatchError,
    LeaseStore,
)
from core_dispatch_bridge import (  # noqa: E402
    CoreCandidateSubprocessRunner,
    producer_authority_binding,
    scan_eligible_candidates,
)

from preprocessor_core import (  # noqa: E402
    EnglishAdapter,
    CodexRunner,
    ModelResult,
    PreprocessorError,
    StructuredStageResult,
    Worker,
    atomic_write_json,
    english_candidate_content_id,
    run_json_command,
    run_loop,
    sha256_value,
)
from preprocess_dispatcher import ProductionDispatchRuntime  # noqa: E402


def utc_text(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def normalized_sentence_hash(value: str) -> str:
    return hashlib.sha256(" ".join(value.split()).encode("utf-8")).hexdigest()


class FakeEnglishRunner:
    def __init__(self, *, one_item: bool = False) -> None:
        self.calls = 0
        self.one_item = one_item

    def run(self, candidate):
        self.calls += 1
        items = []
        if self.one_item:
            event = next(
                row
                for row in candidate.model_input["batch_events"]
                if row["event_type"] in {"sentence_captured", "sentence_correction"}
            )
            sentence = event["source"]["source_sentence"]
            items = [
                {
                    "item_id": "ITEM-1",
                    "sequence": 1,
                    "item": "lasting",
                    "candidate_type": "单词",
                    "candidate_status": "familiarity_candidate",
                    "tier": "A",
                    "source_event_id": event["event_id"],
                    "source_article": event["article"]["source_article"],
                    "source_kind": event["source"]["source_kind"],
                    "source_sentence": sentence,
                    "article_source_hash": event["article"]["source_hash"],
                    "sentence_hash": event["source"]["sentence_hash"],
                    "evidence_states": list(
                        event["learning"]["evidence_states"]
                    ),
                    "evidence_origin": event["learning"]["evidence_origin"],
                    "user_evidence": list(event["learning"]["user_evidence"]),
                    "bank_status": "new_candidate",
                    "bank_match_ids": [],
                    "mastered_status": "clear",
                    "mastery_proposal": None,
                    "grounding": {
                        "status": "not_requested",
                        "user_evidence_ref": event["event_id"],
                        "writing_pattern": {"status": "not_requested"},
                        "writing_vocabulary": {"status": "not_requested"},
                        "syllabus_occurrence": {"status": "not_requested"},
                        "sentence_pattern": {"status": "not_requested"},
                        "old_word_sources": [],
                        "naturalness_check": "not_requested",
                    },
                    "card": {
                        "meaning": "持久的",
                        "source_translation": "持久的回报",
                        "usage": "形容词",
                        "review_note": "不是最后的",
                    },
                }
            ]
        document = {
            "schema_version": "english_luna_candidate_v1",
            "candidate_id": candidate.input_binding["candidate_document_id"],
            "study_date": candidate.study_date,
            "source_id": candidate.input_binding["source_id"],
            "article_id": candidate.input_binding["article_id"],
            "article_source_hash": candidate.input_binding[
                "article_source_hash"
            ],
            "created_at": "2026-08-05T00:00:00Z",
            "producer": {
                "role": "luna_candidate_consumer",
                "model": "gpt-5.6-luna",
                "prompt_version": "english-test-v1",
            },
            "runtime_identity": {
                "requested": {
                    "model": "gpt-5.6-luna",
                    "reasoning_effort": "max",
                },
                "observed": {
                    "model": "gpt-5.6-luna",
                    "reasoning_effort": "max",
                },
                "provenance": {
                    "source": "runtime_attestation",
                    "reference": "test-attestation",
                    "observed_at": "2026-08-05T00:00:00Z",
                },
                "status": "confirmed",
            },
            "capture_event_ids": list(
                candidate.input_binding["capture_event_ids"]
            ),
            "capture_event_sha256": dict(
                candidate.input_binding["capture_event_sha256"]
            ),
            "formal_write_count": 0,
            "formal_writeback": "none",
            "validation": {"status": "PASS", "blockers": []},
            "items": items,
        }
        stage = StructuredStageResult(
            payload={"items": copy.deepcopy(items)},
            duration_ms=3,
            runtime_model="gpt-5.6-luna",
            runtime_reasoning_effort="max",
            runtime_metadata_provenance="codex_json_attestation_v1",
            runtime_identity_status="confirmed",
            output_sha256="1" * 64,
            schema_sha256="2" * 64,
        )
        host_runner = CodexRunner(
            {
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
            Path(tempfile.gettempdir()),
        )
        document = host_runner._finalize_english_document(
            candidate,
            {"items": copy.deepcopy(items)},
            stage=stage,
            prompt_version="english-test-v1",
        )
        return ModelResult(
            analysis=document,
            duration_ms=7,
            runtime_model="gpt-5.6-luna",
            runtime_reasoning_effort="max",
            runtime_metadata_provenance="codex_json_attestation_v1",
            pipeline_status="two_pass_ready",
            draft_analysis=copy.deepcopy(document),
            critical_review={
                "schema_version": "study-intake-english-critical-review-v1",
                "verdict": "confirmed",
                "draft_analysis_sha256": sha256_value(document),
                "findings": [],
                "correction_resolutions": [],
                "revised_items": copy.deepcopy(items),
            },
            stage_receipts={
                stage_name: {
                    "status": "ready",
                    "requested_model": "gpt-5.6-luna",
                    "requested_reasoning_effort": "max",
                    "runtime_model": "gpt-5.6-luna",
                    "runtime_reasoning_effort": "max",
                    "runtime_metadata_provenance": (
                        "codex_json_attestation_v1"
                    ),
                    "duration_ms": 3,
                }
                for stage_name in ("analysis", "critical_review")
            },
        )


class EnglishAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.repo = self.base / "english"
        self.state = self.base / "state"
        self.runtime = self.base / "runtime"
        self.repo.mkdir(parents=True)
        for relative in (
            "english_pipeline/candidates.py",
            "english_pipeline/cli.py",
            "english_pipeline/constants.py",
            "english_pipeline/errors.py",
            "english_pipeline/events.py",
            "english_pipeline/formal.py",
            "english_pipeline/migrations.py",
            "english_pipeline/nightly.py",
            "english_pipeline/quick_flush.py",
            "english_pipeline/review_status.py",
            "english_pipeline/util.py",
            "english_pipeline/views.py",
            "english_pipeline/writer.py",
            "scripts/build_old_word_memory_curve_index.py",
            "scripts/build_review_status_proposals.py",
            "scripts/english_learning_pipeline.py",
            "scripts/select_bbdc_foundation.py",
            "schema/english_pipeline/capture-event-v2.schema.json",
            "schema/english_pipeline/luna-candidate-v2.schema.json",
        ):
            source_path = self.repo / relative
            source_path.parent.mkdir(parents=True, exist_ok=True)
            source_path.write_text(
                "{}\n" if source_path.suffix == ".json" else "# fixture\n",
                encoding="utf-8",
            )
        self.pipeline = self.repo / "pipeline.py"
        self.pipeline.write_text(
            """#!/usr/bin/env python3
import json, pathlib, sys
args = sys.argv[1:]
command = args[0]
if command == 'status':
    print(json.dumps({'schema_version':'english_pipeline_status_v1','event_total':0}))
elif command == 'validate-candidate':
    value = json.loads(pathlib.Path(args[1]).read_text())
    assert value['formal_write_count'] == 0 and value['formal_writeback'] == 'none'
    print(json.dumps({'schema_version':'english_luna_candidate_v1','status':'PASS','item_count':len(value['items'])}))
elif command == 'render-candidate':
    output = pathlib.Path(args[args.index('--output') + 1])
    value = json.loads(pathlib.Path(args[1]).read_text())
    output.write_text('Luna candidate: ' + value['candidate_id'] + '\\n')
    print(json.dumps({'schema_version':'english_candidate_render_receipt_v1','status':'rendered'}))
else:
    raise SystemExit(2)
""",
            encoding="utf-8",
        )
        self.schema = self.repo / "candidate.schema.json"
        atomic_write_json(self.schema, {"type": "object"})
        self.foundation = self.repo / "foundation.py"
        self.foundation.write_text(
            """#!/usr/bin/env python3
import hashlib, json, sys
args = sys.argv[1:]
def arg(name, default=''):
    return args[args.index(name) + 1] if name in args else default
item = arg('--item')
digest = lambda name: hashlib.sha256(name.encode()).hexdigest()
packet = {
 'schema':'bbdc_foundation_packet_v1','read_only':True,
 'eligibility':{'bank_match':'existing_bank','bank_ids':['BANK-' + item[:8]],'mastered_check':'excluded' if item.endswith('0') else 'clear'},
 'relationship_graph':{'status':'loaded'},
 'foundation':{
   'current_user_foundation':[{'wording':item,'evidence_kind':'unknown'}],
   'user_old_word_candidates':[],
   'writing_pattern_candidates':[{'id':'WP-1','status':'approved'}],
   'writing_vocab_candidates':[{'id':'WV-1','status':'approved'}],
   'syllabus_vocab_candidates':[{'id':'SYL-1','status':'verified'}],
   'double_hits':[],
   'optional_sp_candidates':[{'id':'SP-001','title':'acknowledge that'}],
 },
 'validation':{'degradation_path':[],'reference_gap':[],'required_final_checks':['source_separation']},
 'formal_input_hashes_before':{
   'bank/master_bank.csv':digest('master'),
   'bank/mastered_items.csv':digest('mastered'),
   'bank/sentence_patterns.md':digest('patterns'),
 },
 'formal_sources_unchanged':True,'formal_writeback':'none'
}
print(json.dumps(packet))
""",
            encoding="utf-8",
        )
        self.review_status = (
            self.repo / "scripts" / "build_review_status_proposals.py"
        )
        self.review_status.write_text(
            """#!/usr/bin/env python3
import argparse, hashlib, json
parser = argparse.ArgumentParser()
parser.add_argument('--repo-root')
parser.add_argument('--state-dir')
parser.add_argument('--study-date', required=True)
args = parser.parse_args()
print(json.dumps({
  'schema_version':'english_review_status_proposals_v2',
  'study_date':args.study_date,
  'capture_event_ids':[],
  'daily_explicit_unknown_terms':[],
  'review_exclusion_proposals':[],
  'reactivation_proposals':[],
  'needs_user_decision':[],
  'source_hashes':{'fixture':hashlib.sha256(b'fixture').hexdigest()},
  'formal_write_count':0,
}))
""",
            encoding="utf-8",
        )
        self.config = {
            "schema_version": "study-intake-preprocessor-config-v1",
            "runtime_root": str(self.runtime),
            "timezone": "Asia/Shanghai",
            "dispatch": {
                "authority_required": True,
                "heartbeat_interval_seconds": 15,
                "lease_ttl_seconds": 120,
                "infrastructure_recovery_attempts": 1,
            },
            "worker": {
                "enabled": True,
                "poll_interval_seconds": 60,
                "english_poll_interval_seconds": 5,
                "debounce_seconds": 0,
                "status_timeout_seconds": 5,
                "model_timeout_seconds": 5,
                "max_attempts": 3,
                "retry_base_seconds": 0,
                "max_jobs_per_scan": 10,
                "log_path": str(self.runtime / "logs/worker.log"),
                "lock_path": str(self.runtime / "state/worker.lock"),
            },
            "model": {
                "codex_path": str(self.pipeline),
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
                "output_schema": str(self.schema),
                "prompt_version": "test-v1",
                "max_prompt_bytes": 1048576,
                "max_images": 8,
            },
            "english_two_pass_v1": {
                "enabled": True,
                "analysis_output_schema": str(
                    ROOT / "schemas" / "luna-english-candidate-draft-v1.json"
                ),
                "critical_review_output_schema": str(
                    ROOT / "schemas" / "luna-english-critical-review-v1.json"
                ),
                "controlled_contract_path": str(
                    ROOT
                    / "schemas"
                    / "luna-english-controlled-contract-v1.json"
                ),
                "analysis_prompt_version": "english-analysis-test-v1",
                "critical_review_prompt_version": (
                    "english-critical-review-test-v1"
                ),
                "stage_timeout_seconds": 60,
                "max_prompt_bytes": 1048576,
                "max_output_bytes": 524288,
            },
            "dashboard": {
                "projection_path": str(
                    self.runtime / "state" / "dashboard_projection.json"
                ),
                "max_items_per_subject": 200,
            },
            "adapters": {
                "math": {
                    "enabled": False,
                    "adapter_version": "math-test-v1",
                    "python_path": sys.executable,
                    "repo_root": str(self.base / "math"),
                    "status_script": str(self.base / "math.py"),
                },
                "cs408": {
                    "enabled": False,
                    "adapter_version": "cs408-test-v1",
                    "python_path": sys.executable,
                    "repo_root": str(self.base / "cs408"),
                    "status_script": str(self.base / "cs408.py"),
                },
                "english": {
                    "enabled": True,
                    "adapter_version": "english-test-v1",
                    "python_path": sys.executable,
                    "repo_root": str(self.repo),
                    "status_script": str(self.pipeline),
                    "state_dir": str(self.state),
                    "candidate_root": str(self.state / "candidates"),
                    "candidate_schema": str(self.schema),
                    "foundation_script": str(self.foundation),
                    "review_status_script": str(self.review_status),
                    "microbatch_capture_count": 5,
                    "quiet_seconds": 180,
                    "max_events_per_scan": 100,
                    "max_event_bytes": 262144,
                    "max_batch_bytes": 1048576,
                    "max_foundation_items": 24,
                    "max_mastered_matches": 24,
                    "max_master_bank_matches": 24,
                    "max_sentence_pattern_matches": 24,
                    "max_formal_prefetch_bytes": 262144,
                    "max_candidate_documents": 100,
                },
            },
        }
        self.now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
        self.study_date = (
            self.now.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def write_sentence(
        self,
        index: int,
        *,
        article_id: str = "ARTICLE-1",
        source_id: str | None = None,
        age_seconds: int = 0,
        candidate: bool = True,
        evidence_origin: str = "synthetic_fixture",
    ) -> dict:
        occurred = self.now - dt.timedelta(seconds=age_seconds)
        day = occurred.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
        source_id = source_id or article_id
        event_id = f"EVT-{day.replace('-', '')}-{index:016X}"
        sentence = f"Sentence {index} brings lasting reward."
        event = {
            "schema_version": "english_capture_event_v1",
            "event_id": event_id,
            "event_type": "sentence_captured",
            "idempotency_key": f"capture-{article_id}-{index}",
            "request_sha256": hashlib.sha256(
                f"request-{article_id}-{index}".encode()
            ).hexdigest(),
            "occurred_at": utc_text(occurred),
            "article": {
                "article_id": article_id,
                "source_article": f"articles/{article_id}.md",
                "source_id": source_id,
                "source_hash": hashlib.sha256(article_id.encode()).hexdigest(),
            },
            "source": {
                "sentence_id": f"S-{index:03d}",
                "source_sentence": sentence,
                "source_kind": "article",
                "sentence_hash": normalized_sentence_hash(sentence),
            },
            "learning": {
                "first_translation": None,
                "user_evidence_verbatim": "lasting 不会",
                "user_evidence": ["unknown"],
                "evidence_states": ["unknown_observed"],
                "evidence_origin": evidence_origin,
                "answer_protection": "practice_safe",
                "hint_level": 0,
                "translation": "",
                "explanation": "",
                "first_breakpoint": "",
                "restatement": "",
            },
            "candidates": (
                [
                    {
                        "item": "lasting",
                        "candidate_type": "单词",
                        "meaning": "持久的",
                        "decision": "long_term_candidate",
                    }
                ]
                if candidate
                else []
            ),
            "producer": {
                "role": "foreground_producer",
                "name": "test",
                "version": "1",
            },
            "formal_write_count": 0,
            "formal_writeback": "none",
        }
        atomic_write_json(
            self.state / "events" / day / f"{event_id}.json", event
        )
        return event

    def write_production_source_object(
        self, article_id: str = "ARTICLE-1"
    ) -> tuple[Path, str]:
        source_text = "First source line.  \r\nSecond source line.\r\n"
        canonical_text = "First source line.\nSecond source line."
        source_hash = hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()
        payload_path = self.repo / "sources" / f"{article_id}.txt"
        payload_path.parent.mkdir(parents=True, exist_ok=True)
        payload_path.write_text(source_text, encoding="utf-8", newline="")
        handoff_path = self.repo / "handoffs" / f"{article_id}.json"
        atomic_write_json(
            handoff_path,
            {
                "schema_version": "english-learning-pipeline-handoff-v1",
                "source_id": article_id,
                "source_hash": f"sha256:{source_hash}",
                "canonical_payload": {
                    "path": f"sources/{article_id}.txt",
                    "visibility": "practice_safe",
                    "normalization": (
                        "UTF-8; LF line endings; trailing whitespace removed "
                        "per line; no final LF"
                    ),
                },
            },
        )
        article_path = self.repo / "articles" / f"{article_id}.md"
        article_path.parent.mkdir(parents=True, exist_ok=True)
        article_path.write_text(
            "\n".join(
                (
                    f"- source_id: `{article_id}`",
                    f"- source_hash: `sha256:{source_hash}`",
                    f"- pipeline_handoff: `handoffs/{article_id}.json`",
                )
            )
            + "\n",
            encoding="utf-8",
        )
        return payload_path, source_hash

    def test_production_v2_reopens_canonical_article_as_task_artifact(self) -> None:
        payload_path, source_hash = self.write_production_source_object()
        for index in range(1, 6):
            event = self.write_sentence(index, candidate=False)
            event.update(
                {
                    "schema_version": "english_capture_event_v2",
                    "observed_signals": [],
                    "source_signal_ids": [],
                    "capture_coverage": {
                        "schema_version": "english_capture_coverage_v2",
                        "declared_signal_count": 0,
                        "covered_signal_count": 0,
                        "signals": [],
                    },
                    "producer": {
                        "role": "foreground_producer",
                        "name": "english_learning_pipeline",
                        "version": "0.1.0",
                    },
                }
            )
            event["article"]["source_hash"] = source_hash
            atomic_write_json(
                self.state
                / "events"
                / self.study_date
                / f"{event['event_id']}.json",
                event,
            )

        adapter = self.adapter()
        candidate = adapter.candidates(adapter.status(self.study_date))[0]
        source = candidate.model_input["article_source_artifact"]
        self.assertEqual(source["source_hash"], source_hash)
        self.assertEqual(
            source["canonical_text"],
            "First source line.\nSecond source line.",
        )

        runner = CodexRunner(
            {"model": "gpt-5.6-luna", "reasoning_effort": "max"},
            self.runtime,
        )
        artifacts = runner._capture_artifacts(candidate)
        article_artifacts = [
            row for row in artifacts if row["artifact_kind"] == "source_text"
        ]
        self.assertEqual(len(article_artifacts), 1)
        self.assertNotIn("path", article_artifacts[0])
        self.assertEqual(
            article_artifacts[0]["content"]["canonical_text"],
            source["canonical_text"],
        )
        facts = runner._capture_mcp_facts(candidate)["facts"][
            "article_source_artifact"
        ]
        self.assertNotIn("canonical_text", facts)
        self.assertEqual(facts["content_route"], "mcp_read_task_artifact")
        self.assertRegex(facts["artifact_sha256"], r"^[0-9a-f]{64}$")

        payload_path.write_text("drifted source", encoding="utf-8")
        drifted_adapter = self.adapter()
        self.assertEqual(
            drifted_adapter.candidates(
                drifted_adapter.status(self.study_date)
            ),
            [],
        )
        self.assertEqual(
            set(drifted_adapter.candidate_errors.values()),
            {"english_source_object_invalid"},
        )

    def write_correction(
        self, event: dict, *, index: int, age_seconds: int = 0
    ) -> dict:
        corrected = copy.deepcopy(event)
        occurred = self.now - dt.timedelta(seconds=age_seconds)
        day = occurred.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
        corrected.update(
            {
                "event_id": f"EVT-{day.replace('-', '')}-{index:016X}",
                "event_type": "sentence_correction",
                "idempotency_key": f"correction-{index}",
                "request_sha256": hashlib.sha256(
                    f"correction-{index}".encode()
                ).hexdigest(),
                "occurred_at": utc_text(occurred),
                "supersedes_event_id": event["event_id"],
                "correction_reason": "evidence-backed correction",
            }
        )
        atomic_write_json(
            self.state / "events" / day / f"{corrected['event_id']}.json",
            corrected,
        )
        return corrected

    def write_completion(self, sentences: list[dict], index: int = 999) -> dict:
        article = sentences[-1]["article"]
        occurred = self.now
        day = occurred.astimezone(ZoneInfo("Asia/Shanghai")).date().isoformat()
        event_id = f"EVT-{day.replace('-', '')}-{index:016X}"
        hashes = {event["event_id"]: sha256_value(event) for event in sentences}
        event = {
            "schema_version": "english_capture_event_v1",
            "event_id": event_id,
            "event_type": "article_completed",
            "idempotency_key": f"complete-{article['article_id']}",
            "request_sha256": hashlib.sha256(event_id.encode()).hexdigest(),
            "occurred_at": utc_text(occurred),
            "article": copy.deepcopy(article),
            "completion": {
                "study_date": day,
                "effective_capture_event_ids": [
                    event["event_id"] for event in sentences
                ],
                "capture_event_sha256": hashes,
            },
            "producer": {
                "role": "foreground_producer",
                "name": "test",
                "version": "1",
            },
            "formal_write_count": 0,
            "formal_writeback": "none",
        }
        atomic_write_json(
            self.state / "events" / day / f"{event_id}.json", event
        )
        return event

    def write_quick_flush_intent(self, event: dict) -> dict:
        event_path = (
            self.state
            / "events"
            / self.study_date
            / f"{event['event_id']}.json"
        )
        receipt_path = (
            self.state
            / "receipts"
            / "capture"
            / self.study_date
            / f"CAPTURE-{event['event_id'][4:]}.json"
        )
        receipt_identity = {
            "schema_version": "english_capture_receipt_v2",
            "receipt_id": f"CAPTURE-{event['event_id'][4:]}",
            "capture_id": event["event_id"],
            "event_path": str(event_path),
            "event_sha256": sha256_value(event),
            "request_sha256": event["request_sha256"],
            "formal_write_count": 0,
            "formal_writeback": "none",
            "supersedes_event_id": event.get("supersedes_event_id"),
            "stages": {
                "event_written": True,
                "schema_validated": True,
                "dispatcher_accepted": False,
                "package_visible": False,
            },
        }
        receipt = {
            **receipt_identity,
            "status": "created",
            "receipt_path": str(receipt_path),
            "original_status": "created",
            "replayed": False,
        }
        atomic_write_json(receipt_path, receipt)
        identity = {
            "event_id": event["event_id"],
            "event_sha256": sha256_value(event),
            "capture_receipt_id": receipt_identity["receipt_id"],
            "capture_receipt_sha256": sha256_value(receipt_identity),
            "idempotency_key": event["idempotency_key"],
            "request_sha256": event["request_sha256"],
            "source_id": event["article"]["source_id"],
            "study_date": self.study_date,
        }
        core = {
            "schema_version": "english_quick_flush_intent_v1",
            "intent_id": sha256_value(identity),
            "event_id": event["event_id"],
            "event_sha256": sha256_value(event),
            "event_path": str(event_path),
            "capture_receipt_id": receipt_identity["receipt_id"],
            "capture_receipt_sha256": sha256_value(receipt_identity),
            "capture_receipt_path": str(receipt_path),
            "idempotency_key": event["idempotency_key"],
            "request_sha256": event["request_sha256"],
            "source_id": event["article"]["source_id"],
            "study_date": self.study_date,
            "created_at": utc_text(self.now),
            "formal_write_count": 0,
            "formal_writeback": "none",
        }
        key_path = self.state / "quick-flush" / "authority.key"
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key = b"q" * 32
        key_path.write_bytes(key)
        os.chmod(key_path, 0o600)
        mac = hmac.new(
            key,
            json.dumps(
                {"purpose": "english-quick-flush-intent", "payload": core},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        intent = {
            **core,
            "authority": {
                "schema_version": "english_quick_flush_authority_v1",
                "algorithm": "HMAC-SHA256",
                "key_id": hashlib.sha256(key).hexdigest(),
                "purpose": "english-quick-flush-intent",
                "hmac_sha256": mac,
            },
        }
        payload = (
            json.dumps(
                intent,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        digest = hashlib.sha256(payload).hexdigest()
        path = (
            self.state
            / "quick-flush"
            / "intents"
            / "sha256"
            / digest[:2]
            / f"{digest}.json"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        os.chmod(path, 0o600)
        return intent

    def adapter(self) -> EnglishAdapter:
        adapter = Worker(
            self.config, model_runner=FakeEnglishRunner()
        ).adapters["english"]
        self.assertIsInstance(adapter, EnglishAdapter)
        return adapter

    def test_candidate_root_outside_state_remains_rejected_by_default(self) -> None:
        config = copy.deepcopy(self.config)
        config["adapters"]["english"]["candidate_root"] = str(
            self.runtime / "outside-english-candidates"
        )
        with self.assertRaises(PreprocessorError) as raised:
            Worker(config, model_runner=FakeEnglishRunner())
        self.assertEqual(
            raised.exception.code,
            "english_candidate_root_outside_state",
        )

    def test_adapter_subsection_cannot_forge_private_candidate_authority(self) -> None:
        config = copy.deepcopy(self.config)
        candidate_root = self.runtime / "private" / "english-candidates"
        config["adapters"]["english"].update(
            {
                "candidate_root": str(candidate_root),
                "_verification_private_candidate_publication_root": str(
                    candidate_root
                ),
            }
        )
        with self.assertRaises(PreprocessorError) as raised:
            Worker(config, model_runner=FakeEnglishRunner())
        self.assertEqual(
            raised.exception.code,
            "english_candidate_root_outside_state",
        )

    def test_microbatch_trigger_is_five_unconsumed_captures(self) -> None:
        for index in range(1, 5):
            self.write_sentence(index)
        adapter = self.adapter()
        status = adapter.status(self.study_date)
        self.assertEqual(adapter.candidates(status), [])
        captured = adapter.captured_rows(status, self.study_date)
        self.assertEqual(captured[0]["english_status"], "captured")
        self.assertEqual(captured[0]["capture_count"], 4)

        self.write_sentence(5)
        status = adapter.status(self.study_date)
        rows = adapter.candidates(status)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].input_binding["batch_trigger"], "capture_threshold")
        self.assertEqual(rows[0].input_binding["capture_count"], 5)

    def test_malformed_event_envelope_roles_and_source_kind_fail_closed(self) -> None:
        event = self.write_sentence(777)
        mutations = {
            "producer_role": lambda value: value["producer"].update(
                {"role": "luna_candidate_consumer"}
            ),
            "article_extra": lambda value: value["article"].update(
                {"private_path": "/tmp/forbidden"}
            ),
            "source_kind": lambda value: value["source"].update(
                {"source_kind": "answer_key"}
            ),
        }
        adapter = self.adapter()
        for label, mutate in mutations.items():
            with self.subTest(label=label):
                malformed = copy.deepcopy(event)
                mutate(malformed)
                with self.assertRaises(PreprocessorError):
                    adapter._validate_event(malformed)

    def test_microbatch_trigger_is_180_seconds_of_article_silence(self) -> None:
        self.write_sentence(1, age_seconds=181)
        adapter = self.adapter()
        rows = adapter.candidates(adapter.status(self.study_date))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].input_binding["batch_trigger"], "article_silence")

    def test_explicit_quick_flush_triggers_one_bound_event_immediately(self) -> None:
        event = self.write_sentence(1)
        self.write_quick_flush_intent(event)
        adapter = self.adapter()
        rows = adapter.candidates(adapter.status(self.study_date))
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0].input_binding["batch_trigger"],
            "explicit_quick_intake",
        )
        self.assertEqual(
            rows[0].input_binding["capture_event_ids"],
            [event["event_id"]],
        )
        self.assertEqual(
            rows[0].input_binding["quick_flush_intent_id"],
            sha256_value(
                {
                    "event_id": event["event_id"],
                    "event_sha256": sha256_value(event),
                    "capture_receipt_id": f"CAPTURE-{event['event_id'][4:]}",
                    "capture_receipt_sha256": rows[0].input_binding[
                        "quick_flush_capture_receipt_sha256"
                    ],
                    "idempotency_key": event["idempotency_key"],
                    "request_sha256": event["request_sha256"],
                    "source_id": event["article"]["source_id"],
                    "study_date": self.study_date,
                }
            ),
        )
        self.assertRegex(
            rows[0].input_binding["quick_flush_intent_sha256"],
            r"^[0-9a-f]{64}$",
        )
        self.assertRegex(
            rows[0].input_binding["quick_flush_authority_sha256"],
            r"^[0-9a-f]{64}$",
        )

    def test_quick_flush_files_must_be_owned_regular_and_not_writable_by_peers(
        self,
    ) -> None:
        event = self.write_sentence(1)
        self.write_quick_flush_intent(event)
        intent_path = next(
            (self.state / "quick-flush" / "intents").glob(
                "sha256/*/*.json"
            )
        )
        intent = json.loads(intent_path.read_text(encoding="utf-8"))
        targets = (
            (
                intent_path,
                "english_quick_flush_intent_invalid",
            ),
            (
                Path(intent["event_path"]),
                "english_quick_flush_source_binding_invalid",
            ),
            (
                Path(intent["capture_receipt_path"]),
                "english_quick_flush_source_binding_invalid",
            ),
        )
        adapter = self.adapter()
        for target, error_code in targets:
            original = target.read_bytes()
            with self.subTest(target=target.name, condition="peer_writable"):
                os.chmod(target, 0o622)
                with self.assertRaises(PreprocessorError) as raised:
                    adapter._quick_flush_intents([event])
                self.assertEqual(raised.exception.code, error_code)
                os.chmod(target, 0o600)

            with self.subTest(target=target.name, condition="symlink"):
                backing = target.with_name(target.name + ".backing")
                backing.write_bytes(original)
                os.chmod(backing, 0o600)
                target.unlink()
                target.symlink_to(backing)
                with self.assertRaises(PreprocessorError) as raised:
                    adapter._quick_flush_intents([event])
                self.assertEqual(raised.exception.code, error_code)
                target.unlink()
                target.write_bytes(original)
                os.chmod(target, 0o600)
                backing.unlink()

    def test_quick_flush_isolated_from_ordinary_pending_and_tamper_fails_closed(
        self,
    ) -> None:
        first = self.write_sentence(1)
        self.write_quick_flush_intent(first)
        self.write_sentence(2)
        adapter = self.adapter()
        rows = adapter.candidates(adapter.status(self.study_date))
        self.assertEqual(
            [row.input_binding["capture_event_ids"] for row in rows],
            [[first["event_id"]]],
        )
        intent_path = next(
            (self.state / "quick-flush" / "intents").glob("sha256/*/*.json")
        )
        intent = json.loads(intent_path.read_text(encoding="utf-8"))
        intent["source_id"] = "TAMPERED"
        intent_path.write_text(json.dumps(intent) + "\n", encoding="utf-8")
        with self.assertRaises(PreprocessorError):
            adapter.candidates(adapter.status(self.study_date))

    def test_each_quick_flush_intent_forms_one_independent_candidate(self) -> None:
        events = [self.write_sentence(index) for index in (1, 2)]
        for event in events:
            self.write_quick_flush_intent(event)
        adapter = self.adapter()
        first = adapter.candidates(adapter.status(self.study_date))
        second = adapter.candidates(adapter.status(self.study_date))
        expected = [[event["event_id"]] for event in events]
        self.assertEqual(
            [row.input_binding["capture_event_ids"] for row in first],
            expected,
        )
        self.assertEqual(
            [row.capture_id for row in first],
            [row.capture_id for row in second],
        )
        self.assertEqual(
            [row.input_fingerprint for row in first],
            [row.input_fingerprint for row in second],
        )

    def test_quick_flush_leaves_five_event_and_completion_paths_independent(
        self,
    ) -> None:
        explicit = self.write_sentence(1)
        self.write_quick_flush_intent(explicit)
        ordinary = [self.write_sentence(index) for index in range(2, 7)]
        adapter = self.adapter()
        rows = adapter.candidates(adapter.status(self.study_date))
        by_trigger = {
            row.input_binding["batch_trigger"]: row for row in rows
        }
        self.assertEqual(set(by_trigger), {"explicit_quick_intake", "capture_threshold"})
        self.assertEqual(
            by_trigger["explicit_quick_intake"].input_binding[
                "capture_event_ids"
            ],
            [explicit["event_id"]],
        )
        self.assertEqual(
            by_trigger["capture_threshold"].input_binding[
                "capture_event_ids"
            ],
            [event["event_id"] for event in ordinary],
        )

        only = EnglishAdapterTests(methodName="runTest")
        only.setUp()
        try:
            event = only.write_sentence(1)
            only.write_quick_flush_intent(event)
            only.write_completion([event])
            only_adapter = only.adapter()
            only_rows = only_adapter.candidates(
                only_adapter.status(only.study_date)
            )
            self.assertEqual(len(only_rows), 1)
            self.assertEqual(
                only_rows[0].input_binding["batch_trigger"],
                "explicit_quick_intake",
            )
        finally:
            only.tearDown()

    def test_published_quick_flush_is_consumed_once_but_fence_stays_valid(
        self,
    ) -> None:
        event = self.write_sentence(1)
        self.write_quick_flush_intent(event)
        worker = Worker(
            self.config, model_runner=FakeEnglishRunner(one_item=True)
        )
        adapter = worker.adapters["english"]
        candidate = adapter.candidates(adapter.status(self.study_date))[0]
        worker._assert_current_candidate_generation(candidate)
        document = FakeEnglishRunner(one_item=True).run(candidate).analysis
        adapter.publish_candidate(
            candidate, document, runtime_root=self.runtime
        )

        worker._assert_current_candidate_generation(candidate)
        reopened = self.adapter()
        self.assertEqual(
            reopened.candidates(reopened.status(self.study_date)), []
        )

    def test_superseded_quick_flush_is_fenced_before_provider(self) -> None:
        event = self.write_sentence(1)
        self.write_quick_flush_intent(event)
        runner = FakeEnglishRunner(one_item=True)
        worker = Worker(self.config, model_runner=runner)
        adapter = worker.adapters["english"]
        candidate = adapter.candidates(adapter.status(self.study_date))[0]
        self.write_correction(event, index=2)

        with self.assertRaises(PreprocessorError) as raised:
            worker.process_claimed_candidate(candidate, "eligible")
        self.assertEqual(raised.exception.code, "stale_input_superseded")
        self.assertEqual(runner.calls, 0)
        self.assertEqual(list((self.state / "candidates").glob("*/*.json")), [])

    def test_correction_during_quick_flush_blocks_old_publication(self) -> None:
        event = self.write_sentence(1)
        self.write_quick_flush_intent(event)
        case = self

        class CorrectingRunner(FakeEnglishRunner):
            def run(inner_self, candidate):
                result = super().run(candidate)
                case.write_correction(event, index=2)
                return result

        runner = CorrectingRunner(one_item=True)
        worker = Worker(self.config, model_runner=runner)
        adapter = worker.adapters["english"]
        candidate = adapter.candidates(adapter.status(self.study_date))[0]
        result = worker.process_claimed_candidate(candidate, "eligible")

        self.assertEqual(result["status"], "retrying")
        self.assertEqual(result["last_error_code"], "stale_input_superseded")
        self.assertEqual(runner.calls, 1)
        self.assertEqual(list((self.state / "candidates").glob("*/*.json")), [])
        self.assertFalse(
            worker.store.latest_path("english", candidate.capture_id).exists()
        )

    def test_queued_quick_flush_correction_seals_non_business_supersede(
        self,
    ) -> None:
        event = self.write_sentence(1, evidence_origin="live_user")
        self.write_quick_flush_intent(event)
        frozen, decisions = scan_eligible_candidates(self.config, "english")
        self.assertEqual(len(frozen), 1, decisions)
        selected = frozen[0]
        task = selected.task
        contract = task.frozen_payload["dispatch_contract"][
            "producer_input_contract"
        ]
        release_id = contract["release_id"]
        store = LeaseStore(self.runtime)
        activated_at = utc_text(
            dt.datetime.fromisoformat(
                str(contract["producer_recorded_at"]).replace("Z", "+00:00")
            )
            - dt.timedelta(seconds=1)
        )
        store.begin_subject_drain("english")
        store.activate_production_canary(
            "english",
            release_id=release_id,
            producer_authority=producer_authority_binding(
                self.config, "english", release_id
            ),
            activated_at=activated_at,
            continuous_concurrency_limit=20,
        )
        queue = store.materialize_production_canary_task(task)
        queue_entry_path = store._production_canary_queue_path(
            "english", contract["producer_input_contract_sha256"]
        )
        queue_before = queue_entry_path.read_bytes()

        correction = self.write_correction(event, index=2)
        adapter = self.adapter()
        evidence = adapter.quick_flush_supersession_evidence(
            selected.candidate
        )
        runtime = object.__new__(ProductionDispatchRuntime)
        runtime.config = self.config
        runtime.subject = "english"
        runtime.production_canary = True
        runtime.dispatcher = SimpleNamespace(lease_store=store)
        supersede_decisions = (
            runtime._reconcile_english_quick_flush_supersessions()
        )
        self.assertEqual(len(supersede_decisions), 1)
        self.assertEqual(
            supersede_decisions[0]["reason"],
            "english_quick_flush_source_superseded",
        )
        result = store.supersede_stale_english_quick_flush_task(
            task, supersession_evidence=evidence
        )
        self.assertEqual(result["status"], "superseded")
        self.assertTrue(result["idempotent"])
        self.assertEqual(queue_entry_path.read_bytes(), queue_before)
        self.assertEqual(store.pending_production_canary_tasks("english"), [])
        reopened = LeaseStore(self.runtime).production_canary_status(
            "english"
        )
        self.assertEqual(reopened["state"], "armed")
        self.assertTrue(reopened["luna_consumer_enabled"])
        self.assertEqual(reopened["queue_depth"], 0)
        self.assertEqual(reopened["terminal_task_count"], 0)
        self.assertEqual(sum(reopened["terminal_by_outcome"].values()), 0)
        self.assertEqual(reopened["formal_write_count"], 0)
        self.assertFalse(reopened["sol_enabled"])
        receipt = json.loads(
            Path(result["terminal_receipt_path"]).read_text(encoding="utf-8")
        )
        self.assertEqual(receipt["terminal_outcome"], "superseded")
        self.assertEqual(receipt["model_call_count"], 0)
        self.assertEqual(receipt["provider_request_count"], 0)
        self.assertEqual(receipt["mcp_tool_call_count"], 0)
        self.assertEqual(
            store.supersede_stale_english_quick_flush_task(
                task, supersession_evidence=evidence
            )["terminal_receipt_sha256"],
            result["terminal_receipt_sha256"],
        )

        self.write_quick_flush_intent(correction)
        replacement_rows, replacement_decisions = scan_eligible_candidates(
            self.config, "english"
        )
        self.assertEqual(len(replacement_rows), 1, replacement_decisions)
        replacement = replacement_rows[0].task
        store.materialize_production_canary_task(replacement)
        store.materialize_production_canary_task(replacement)
        pending = store.pending_production_canary_tasks("english")
        self.assertEqual(
            [row.unit_sha256 for row in pending],
            [replacement.unit_sha256],
        )
        self.assertNotEqual(replacement.unit_sha256, task.unit_sha256)

        tampered = copy.deepcopy(evidence)
        tampered["effective_event_sha256"] = "f" * 64
        with self.assertRaises(DispatchError):
            store.supersede_stale_english_quick_flush_task(
                task, supersession_evidence=tampered
            )

    def test_correction_during_candidate_staging_blocks_artifact_write(
        self,
    ) -> None:
        event = self.write_sentence(1)
        self.write_quick_flush_intent(event)
        adapter = self.adapter()
        candidate = adapter.candidates(adapter.status(self.study_date))[0]
        document = FakeEnglishRunner(one_item=True).run(candidate).analysis
        calls = 0

        def run_then_correct(*args, **kwargs):
            nonlocal calls
            result = run_json_command(*args, **kwargs)
            calls += 1
            if calls == 2:
                self.write_correction(event, index=2)
            return result

        with mock.patch(
            "preprocessor_core.run_json_command",
            side_effect=run_then_correct,
        ):
            with self.assertRaises(PreprocessorError) as raised:
                adapter.publish_candidate(
                    candidate, document, runtime_root=self.runtime
                )
        self.assertEqual(raised.exception.code, "stale_input_superseded")
        self.assertEqual(list((self.state / "candidates").glob("*/*")), [])

    def test_microbatch_trigger_is_article_completed(self) -> None:
        sentence = self.write_sentence(1)
        completion = self.write_completion([sentence])
        adapter = self.adapter()
        rows = adapter.candidates(adapter.status(self.study_date))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].input_binding["batch_trigger"], "article_completed")
        self.assertIn(completion["event_id"], rows[0].input_binding["capture_event_ids"])

    def test_controlled_replay_rebuilds_only_exact_consumed_events(self) -> None:
        selected = [
            self.write_sentence(index, age_seconds=181)
            for index in (1, 2, 3)
        ]
        unrelated = self.write_sentence(99, age_seconds=181)
        adapter = self.adapter()
        allowlist = frozenset(event["event_id"] for event in selected)
        status = adapter.status(self.study_date)
        adapter._candidate_ids_by_event = {
            event_id: {"OLD-CANDIDATE"}
            for event_id in allowlist | {unrelated["event_id"]}
        }
        self.assertEqual(adapter.candidates(status), [])
        rows = adapter.candidates(
            status,
            capture_allowlist=allowlist,
            controlled_replay=True,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            set(rows[0].input_binding["capture_event_ids"]), allowlist
        )
        self.assertEqual(rows[0].input_binding["capture_count"], 3)

    def legacy_contract_model_input_is_bounded_prefetch_with_hashes_and_limits(self) -> None:
        for index in range(1, 6):
            self.write_sentence(index)
        adapter = self.adapter()
        candidate = adapter.candidates(
            adapter.status(self.study_date)
        )[0]
        self.assertEqual(
            set(candidate.model_input),
            {
                "batch",
                "batch_events",
                "mastered_matches",
                "master_bank_matches",
                "sentence_pattern_matches",
                "four_layer_matches",
                "review_status_proposals",
            },
        )
        self.assertEqual(len(candidate.model_input["batch_events"]), 5)
        self.assertLessEqual(len(candidate.model_input["four_layer_matches"]), 24)
        self.assertLessEqual(len(candidate.model_input["mastered_matches"]), 24)
        self.assertLessEqual(len(candidate.model_input["master_bank_matches"]), 24)
        self.assertLessEqual(len(candidate.model_input["sentence_pattern_matches"]), 24)
        binding = candidate.input_binding
        self.assertLessEqual(binding["formal_prefetch_bytes"], 262144)
        self.assertEqual(binding["formal_prefetch_limits"]["bytes"], 262144)
        self.assertEqual(
            set(binding["formal_source_hashes"]),
            {
                "bank/master_bank.csv",
                "bank/mastered_items.csv",
                "bank/sentence_patterns.md",
            },
        )
        self.assertTrue(
            all(len(value) == 64 for value in binding["formal_source_hashes"].values())
        )
        self.assertEqual(len(binding["article_source_hash"]), 64)
        self.assertEqual(set(binding["capture_event_sha256"]), set(binding["capture_event_ids"]))

    def test_english_model_draft_schema_uses_supported_strict_subset(self) -> None:
        schema_path = ROOT / "schemas/luna-english-candidate-draft-v1.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        unsupported = {
            "allOf", "anyOf", "oneOf", "not", "if", "then", "else",
            "dependentRequired", "dependentSchemas", "patternProperties",
            "unevaluatedProperties", "uniqueItems", "format",
        }

        def walk(value):
            if isinstance(value, dict):
                self.assertFalse(unsupported.intersection(value))
                if "const" in value:
                    self.assertIn("type", value)
                if value.get("type") == "object":
                    self.assertIs(value.get("additionalProperties"), False)
                    properties = value.get("properties")
                    self.assertIsInstance(properties, dict)
                    self.assertEqual(set(value.get("required") or []), set(properties))
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)

        walk(schema)
        template = json.loads((ROOT / "config.example.json").read_text(encoding="utf-8"))
        self.assertEqual(
            template["english_two_pass_v1"]["analysis_output_schema"],
            "${RELEASE_ROOT}/schemas/luna-english-candidate-draft-v1.json",
        )

    def test_schema_valid_model_output_is_persisted_before_semantic_gate(self) -> None:
        runner = CodexRunner(
            {"model": "gpt-5.6-luna", "reasoning_effort": "max"},
            self.runtime,
        )
        object_sha256, object_ref = runner._persist_model_stage_output(
            stage_name="english_critical_review",
            payload={"verdict": "reject", "correction_resolutions": []},
            output_sha256="1" * 64,
            schema_sha256="2" * 64,
            duration_ms=17,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
        )
        path = (
            self.runtime
            / "private/reports/model-stage-outputs/objects"
            / f"{object_sha256}.json"
        )
        self.assertTrue(path.is_file())
        self.assertEqual(
            hashlib.sha256(path.read_bytes()).hexdigest(), object_sha256
        )
        artifact = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            object_ref,
            f"study-intake-model-stage-output://sha256/{object_sha256}",
        )
        self.assertEqual(artifact["stage_name"], "english_critical_review")
        self.assertEqual(artifact["formal_write_count"], 0)

    def test_requested_unverified_document_uses_host_bound_capture_evidence(self) -> None:
        for index in range(1, 6):
            self.write_sentence(index)
        adapter = self.adapter()
        candidate = adapter.candidates(
            adapter.status(self.study_date)
        )[0]
        source_event = candidate.model_input["batch_events"][0]
        payload = {
            "items": [
                {
                    "item_id": "ITEM-1",
                    "sequence": 1,
                    "item": "lasting",
                    "candidate_type": "单词",
                    "candidate_status": "familiarity_candidate",
                    "tier": "A",
                    "source_event_id": source_event["event_id"],
                    "source_article": "MODEL-MUST-NOT-BIND",
                    "source_kind": "explanation",
                    "source_sentence": "MODEL-MUST-NOT-BIND",
                    "article_source_hash": "0" * 64,
                    "sentence_hash": "0" * 64,
                    "evidence_states": ["independent_correct_use"],
                    "evidence_origin": "live_user",
                    "user_evidence": ["other"],
                    "bank_status": "new_candidate",
                    "bank_match_ids": [],
                    "mastered_status": "clear",
                    "mastery_proposal": None,
                    "grounding": {
                        "status": "not_requested",
                        "user_evidence_ref": source_event["event_id"],
                        "writing_pattern": {"status": "not_requested"},
                        "writing_vocabulary": {"status": "not_requested"},
                        "syllabus_occurrence": {"status": "not_requested"},
                        "sentence_pattern": {"status": "not_requested"},
                        "old_word_sources": [],
                        "naturalness_check": "not_requested",
                    },
                    "card": {
                        "meaning": "持久的",
                        "source_translation": "持久的回报",
                        "usage": "形容词",
                        "review_note": "不是最后的",
                    },
                }
            ]
        }
        runner = CodexRunner(
            {
                "model": "gpt-5.6-luna",
                "reasoning_effort": "max",
            },
            self.runtime,
        )
        analysis_stage = StructuredStageResult(
            payload=payload,
            duration_ms=3,
            runtime_model=None,
            runtime_reasoning_effort=None,
            runtime_metadata_provenance="unavailable",
            runtime_identity_status="requested_unverified",
            output_sha256="1" * 64,
            schema_sha256="2" * 64,
        )
        document = runner._finalize_english_document(
            candidate,
            payload,
            stage=analysis_stage,
            prompt_version="english-analysis-test-v1",
        )
        item = document["items"][0]
        self.assertEqual(document["runtime_identity"]["status"], "requested_unverified")
        self.assertEqual(document["runtime_identity"]["provenance"]["source"], "request_only")
        self.assertEqual(document["validation"], {"status": "PASS", "blockers": []})
        self.assertEqual(document["source_id"], source_event["article"]["source_id"])
        sentence_record = document["sentence_records"][0]
        self.assertEqual(item["sentence_record_id"], sentence_record["sentence_record_id"])
        self.assertEqual(
            sentence_record["source_sentence"],
            source_event["source"]["source_sentence"],
        )
        for repeated_field in (
            "source_article", "source_kind", "source_sentence",
            "article_source_hash", "sentence_hash",
        ):
            self.assertNotIn(repeated_field, item)
        self.assertEqual(item["evidence_states"], source_event["learning"]["evidence_states"])
        self.assertEqual(item["evidence_origin"], source_event["learning"]["evidence_origin"])
        self.assertEqual(item["user_evidence"], source_event["learning"]["user_evidence"])

    def legacy_contract_critical_review_failure_keeps_non_consumable_checkpoint(self) -> None:
        for index in range(1, 6):
            self.write_sentence(index)
        worker = Worker(self.config)
        analysis_stage = StructuredStageResult(
            payload={"items": []},
            duration_ms=3,
            runtime_model="gpt-5.6-luna",
            runtime_reasoning_effort="max",
            runtime_metadata_provenance="codex_json_attestation_v1",
            runtime_identity_status="confirmed",
            output_sha256="5" * 64,
            schema_sha256="6" * 64,
        )
        calls = []

        def execute(**kwargs):
            calls.append(kwargs["stage_name"])
            if kwargs["stage_name"] == "english_analysis":
                return analysis_stage
            raise PreprocessorError("english_critical_review_timeout")

        with mock.patch.object(
            worker.runner, "_execute_prompt", side_effect=execute
        ):
            result = worker.run_once(
                subject="english", study_date=self.study_date
            )
        self.assertEqual(
            calls, ["english_analysis", "english_critical_review"]
        )
        self.assertEqual(result["receipt"]["status"], "failed")
        job = worker.store.all_jobs("english")[0]
        self.assertIn(job["status"], {"retrying", "failed"})
        self.assertEqual(
            job["last_error_code"], "english_critical_review_timeout"
        )
        self.assertFalse(job["checkpoint_consumable"])
        self.assertEqual(
            job["critical_resume_status"],
            "checkpoint_only_not_consumable",
        )
        checkpoint = Path(
            self.runtime
            / "private"
            / "reports"
            / "analysis-checkpoints"
            / "objects"
            / f"{job['analysis_checkpoint_sha256']}.json"
        )
        self.assertTrue(checkpoint.is_file())
        self.assertEqual(json.loads(checkpoint.read_text())["formal_write_count"], 0)
        self.assertFalse(worker.store.latest_path("english", job["capture_id"]).exists())
        self.assertEqual(list((self.state / "candidates").glob("*/*.json")), [])

    def legacy_contract_production_subprocess_crash_resumes_critical_without_reanalysis(
        self,
    ) -> None:
        for index in range(1, 6):
            self.write_sentence(index)
        counter_path = self.base / "model-stage-counts.json"
        fake_codex = self.base / "fake-codex-recovery"
        fake_codex.write_text(
            """#!/usr/bin/env python3
import json, os, pathlib, re, signal, sys, time
counter_path = pathlib.Path(%r)
try:
    counts = json.loads(counter_path.read_text())
except FileNotFoundError:
    counts = {"analysis": 0, "critical_review": 0}
args = sys.argv[1:]
schema_path = pathlib.Path(args[args.index("--output-schema") + 1])
output_path = pathlib.Path(args[args.index("--output-last-message") + 1])
prompt = sys.stdin.read()
is_review = "english_critical_review" in schema_path.name
stage = "critical_review" if is_review else "analysis"
counts[stage] += 1
counter_path.write_text(json.dumps(counts, sort_keys=True))
if is_review and counts[stage] == 1:
    os.kill(os.getppid(), signal.SIGKILL)
    time.sleep(0.1)
    raise SystemExit(73)
if is_review:
    match = re.search(r'"draft_analysis_sha256"\\s*:\\s*"([0-9a-f]{64})"', prompt)
    if match is None:
        raise SystemExit(74)
    payload = {
        "schema_version": "study-intake-english-critical-review-v1",
        "verdict": "confirmed",
        "draft_analysis_sha256": match.group(1),
        "findings": [],
        "correction_resolutions": [],
        "revised_items": [],
    }
else:
    payload = {"items": []}
output_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True))
"""
            % str(counter_path),
            encoding="utf-8",
        )
        fake_codex.chmod(0o700)
        config = copy.deepcopy(self.config)
        config["model"]["codex_path"] = str(fake_codex)
        config_path = self.base / "production-recovery-config.json"
        atomic_write_json(config_path, config)
        study_date = self.study_date
        frozen, decisions = scan_eligible_candidates(config, "english")
        self.assertEqual(len(frozen), 1, decisions)
        selected = frozen[0]
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: CoreCandidateSubprocessRunner(
                config_path, selected.reason
            ),
            stage_timeout_seconds=20,
        )
        result = dispatcher.submit(selected.task).wait(30)
        self.assertEqual(result.outcome, "succeeded", result)
        self.assertEqual(result.completion["lease_fence"], 2)
        self.assertEqual(
            json.loads(counter_path.read_text()),
            {"analysis": 1, "critical_review": 2},
        )
        job = json.loads(
            (
                self.runtime
                / "state"
                / "jobs"
                / "english"
                / f"{selected.candidate.capture_id}.json"
            ).read_text()
        )
        self.assertTrue(job["analysis_checkpoint_reused"])
        self.assertEqual(job["critical_resume_status"], "two_pass_ready")
        fence_two_root = (
            self.runtime
            / "dispatch"
            / "state"
            / "task-events"
            / selected.task.unit_sha256
            / "fence-2"
        )
        fence_two_events = [
            json.loads(path.read_text())["event"]
            for path in sorted(fence_two_root.glob("*.json"))
        ]
        self.assertNotIn("analysis_submitted", fence_two_events)
        self.assertEqual(fence_two_events.count("critical_started"), 1)
        self.assertEqual(fence_two_events.count("critical_completed"), 1)

    def legacy_contract_production_crash_after_checkpoint_before_review_resumes_only_review(
        self,
    ) -> None:
        for index in range(1, 6):
            self.write_sentence(index)
        counter_path = self.base / "checkpoint-window-stage-counts.json"
        fake_codex = self.base / "fake-codex-checkpoint-window"
        fake_codex.write_text(
            """#!/usr/bin/env python3
import json, pathlib, re, sys
counter_path = pathlib.Path(%r)
try:
    counts = json.loads(counter_path.read_text())
except FileNotFoundError:
    counts = {"analysis": 0, "critical_review": 0}
args = sys.argv[1:]
schema_path = pathlib.Path(args[args.index("--output-schema") + 1])
output_path = pathlib.Path(args[args.index("--output-last-message") + 1])
prompt = sys.stdin.read()
is_review = "english_critical_review" in schema_path.name
stage = "critical_review" if is_review else "analysis"
counts[stage] += 1
counter_path.write_text(json.dumps(counts, sort_keys=True))
if is_review:
    match = re.search(r'"draft_analysis_sha256"\\s*:\\s*"([0-9a-f]{64})"', prompt)
    if match is None:
        raise SystemExit(74)
    payload = {
        "schema_version": "study-intake-english-critical-review-v1",
        "verdict": "confirmed",
        "draft_analysis_sha256": match.group(1),
        "findings": [],
        "correction_resolutions": [],
        "revised_items": [],
    }
else:
    payload = {"items": []}
output_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True))
"""
            % str(counter_path),
            encoding="utf-8",
        )
        fake_codex.chmod(0o700)
        config = copy.deepcopy(self.config)
        config["model"]["codex_path"] = str(fake_codex)
        config_path = self.base / "checkpoint-window-config.json"
        atomic_write_json(config_path, config)
        frozen, decisions = scan_eligible_candidates(config, "english")
        self.assertEqual(len(frozen), 1, decisions)
        selected = frozen[0]
        command = [
            sys.executable,
            str(
                ROOT
                / "tests"
                / "fixtures"
                / "checkpoint_crash_task_runner.py"
            ),
        ]
        dispatcher = ConcurrentDispatcher(
            self.runtime,
            lambda _task, _context: CoreCandidateSubprocessRunner(
                config_path, selected.reason, command=command
            ),
            stage_timeout_seconds=20,
        )
        result = dispatcher.submit(selected.task).wait(30)
        self.assertEqual(result.outcome, "succeeded", result)
        self.assertEqual(result.completion["lease_fence"], 2)
        self.assertEqual(
            json.loads(counter_path.read_text()),
            {"analysis": 1, "critical_review": 1},
        )
        event_base = (
            self.runtime
            / "dispatch"
            / "state"
            / "task-events"
            / selected.task.unit_sha256
        )
        fence_one_events = [
            json.loads(path.read_text())["event"]
            for path in sorted((event_base / "fence-1").glob("*.json"))
        ]
        fence_two_events = [
            json.loads(path.read_text())["event"]
            for path in sorted((event_base / "fence-2").glob("*.json"))
        ]
        self.assertIn("analysis_completed", fence_one_events)
        self.assertNotIn("critical_started", fence_one_events)
        self.assertNotIn("analysis_submitted", fence_two_events)
        self.assertIn("analysis_checkpoint_reused", fence_two_events)
        self.assertEqual(fence_two_events.count("critical_started"), 1)
        self.assertEqual(fence_two_events.count("critical_completed"), 1)

    def test_candidate_validation_and_render_failures_keep_stage_specific_codes(self) -> None:
        for index in range(1, 6):
            self.write_sentence(index)
        adapter = self.adapter()
        candidate = adapter.candidates(
            adapter.status(self.study_date)
        )[0]
        document = FakeEnglishRunner(one_item=True).run(candidate).analysis
        with mock.patch(
            "preprocessor_core.run_json_command",
            side_effect=PreprocessorError("canonical_status_failed"),
        ):
            with self.assertRaisesRegex(
                PreprocessorError, "english_candidate_validation_failed"
            ):
                adapter.publish_candidate(
                    candidate, document, runtime_root=self.runtime
                )
        with mock.patch(
            "preprocessor_core.run_json_command",
            side_effect=[
                {"status": "PASS"},
                PreprocessorError("canonical_status_failed"),
            ],
        ):
            with self.assertRaisesRegex(
                PreprocessorError, "english_candidate_render_failed"
            ):
                adapter.publish_candidate(
                    candidate, document, runtime_root=self.runtime
                )

    def test_candidate_publication_is_content_addressed_across_revisions(self) -> None:
        for index in range(1, 6):
            self.write_sentence(index)
        adapter = self.adapter()
        candidate = adapter.candidates(
            adapter.status(self.study_date)
        )[0]
        first = FakeEnglishRunner(one_item=True).run(candidate).analysis
        first_publication = adapter.publish_candidate(
            candidate, first, runtime_root=self.runtime
        )
        first_id = first_publication["candidate_document_id"]
        first_path = (
            self.state
            / "candidates"
            / candidate.study_date
            / f"{first_id}.json"
        )
        first_bytes = first_path.read_bytes()

        second = copy.deepcopy(first)
        second["items"][0]["card"]["review_note"] = "content revision two"
        second["candidate_id"] = english_candidate_content_id(
            candidate.input_binding["candidate_document_id"], second
        )
        second_publication = adapter.publish_candidate(
            candidate, second, runtime_root=self.runtime
        )
        second_id = second_publication["candidate_document_id"]

        self.assertNotEqual(first_id, second_id)
        self.assertEqual(first_path.read_bytes(), first_bytes)
        self.assertTrue(
            (
                self.state
                / "candidates"
                / candidate.study_date
                / f"{second_id}.json"
            ).is_file()
        )
        self.assertFalse(first_publication["candidate_reused"])
        repeated = adapter.publish_candidate(
            candidate, second, runtime_root=self.runtime
        )
        self.assertEqual(repeated["candidate_document_id"], second_id)
        self.assertTrue(repeated["candidate_reused"])
        status = adapter.status(candidate.study_date)
        self.assertEqual(status["candidate_document_count"], 2)

    def legacy_contract_worker_offline_projection_keeps_canonical_queue_and_recovers_once(self) -> None:
        for index in range(1, 6):
            self.write_sentence(index, candidate=False)
        paused_config = copy.deepcopy(self.config)
        paused_config["worker"]["enabled"] = False
        paused = Worker(paused_config, model_runner=FakeEnglishRunner())
        paused_result = paused.run_once(
            subject="english", study_date=self.study_date
        )
        self.assertEqual(paused_result["status"], "paused")
        projection = json.loads(
            Path(paused_config["dashboard"]["projection_path"]).read_text()
        )
        item = projection["subjects"]["english"]["items"][0]
        self.assertEqual(item["english_status"], "queued")
        self.assertEqual(item["display_status"], "queued_worker_offline")

        runner = FakeEnglishRunner()
        worker = Worker(self.config, model_runner=runner)
        result = worker.run_once(
            subject="english", study_date=self.study_date
        )
        self.assertEqual(result["receipt"]["status"], "processed")
        self.assertEqual(runner.calls, 1)
        candidate_files = sorted(
            (self.state / "candidates" / self.study_date).glob(
                "*.json"
            )
        )
        self.assertEqual(len(candidate_files), 1)
        actual_candidate_id = json.loads(
            candidate_files[0].read_text()
        )["candidate_id"]
        self.assertNotEqual(
            actual_candidate_id,
            candidate_files[0].stem.rsplit("-", 1)[0],
        )
        latest_files = sorted(
            (self.runtime / "state" / "latest" / "english").glob("*.json")
        )
        self.assertEqual(len(latest_files), 1)
        latest = json.loads(latest_files[0].read_text())
        self.assertEqual(latest["candidate_document_id"], actual_candidate_id)
        self.assertEqual(
            latest["candidate_document_sha256"],
            hashlib.sha256(candidate_files[0].read_bytes()).hexdigest(),
        )
        self.assertFalse(latest["candidate_reused"])
        package = json.loads(Path(latest["package_path"]).read_text())
        self.assertEqual(
            package["analysis"]["candidate_id"], actual_candidate_id
        )
        second = worker.run_once(
            subject="english", study_date=self.study_date
        )
        self.assertEqual(second["receipt"]["status"], "no_eligible_batch")
        self.assertEqual(runner.calls, 1)

    def legacy_contract_article_completion_can_publish_zero_item_skipped_package(self) -> None:
        sentence = self.write_sentence(1, candidate=False)
        self.write_completion([sentence])
        runner = FakeEnglishRunner()
        worker = Worker(self.config, model_runner=runner)
        result = worker.run_once(
            subject="english", study_date=self.study_date
        )
        self.assertEqual(result["receipt"]["status"], "processed")
        projection = json.loads(
            Path(self.config["dashboard"]["projection_path"]).read_text()
        )
        item = projection["subjects"]["english"]["items"][0]
        self.assertEqual(item["english_status"], "skipped")
        self.assertEqual(item["candidate_item_count"], 0)
        self.assertEqual(runner.calls, 1)

    def test_shadow_fixture_has_three_sources_and_twenty_isolated_events(self) -> None:
        for index in range(1, 21):
            group = (index - 1) % 3
            self.write_sentence(
                index,
                article_id=f"SOURCE-{group + 1}",
                source_id=f"SOURCE-{group + 1}",
                candidate=False,
            )
        runner = FakeEnglishRunner()
        integration_config = copy.deepcopy(self.config)
        real_repo = Path("/Users/xiazhibin/Documents/kaoyan-english")
        real_script = real_repo / "scripts/english_learning_pipeline.py"
        real_schema = (
            real_repo
            / "schema/english_pipeline/luna-candidate-v1.schema.json"
        )
        if not real_script.is_file() or not real_schema.is_file():
            self.skipTest("canonical English integration CLI is unavailable")
        integration_config["adapters"]["english"].update(
            {
                "repo_root": str(real_repo),
                "status_script": str(real_script),
                "candidate_schema": str(real_schema),
            }
        )
        worker = Worker(integration_config, model_runner=runner)
        result = worker.run_once(
            subject="english", study_date=self.study_date
        )
        self.assertEqual(result["receipt"]["candidate_count"], 3)
        self.assertEqual(result["receipt"]["processed_count"], 3)
        self.assertEqual(runner.calls, 3)
        events = list((self.state / "events").glob("*/*.json"))
        self.assertEqual(len(events), 20)
        source_ids = {
            json.loads(path.read_text())["article"]["source_id"] for path in events
        }
        self.assertEqual(source_ids, {"SOURCE-1", "SOURCE-2", "SOURCE-3"})
        validation = json.loads(
            subprocess.run(
                [
                    sys.executable,
                    str(real_script),
                    "validate-events",
                    "--repo-root",
                    str(real_repo),
                    "--state-dir",
                    str(self.state),
                    "--date",
                    self.study_date,
                ],
                check=True,
                stdout=subprocess.PIPE,
                text=True,
            ).stdout
        )
        self.assertEqual(validation["status"], "PASS")
        self.assertEqual(validation["validated_event_count"], 20)

    def test_exact_english_scan_does_not_call_other_adapters(self) -> None:
        worker = Worker(self.config, model_runner=FakeEnglishRunner())
        calls = {"math": 0, "cs408": 0, "english": 0}

        class SpyAdapter:
            enabled = True

            def __init__(inner_self, name):
                inner_self.name = name

            def status(inner_self, _date=None):
                calls[inner_self.name] += 1
                return {"schema_version": "spy"}

        worker.adapters = {
            "math": SpyAdapter("math"),
            "cs408": SpyAdapter("cs408"),
            "english": SpyAdapter("english"),
        }
        worker.scan_statuses(self.study_date, only_subject="english")
        self.assertEqual(calls, {"math": 0, "cs408": 0, "english": 1})

    def test_malformed_model_output_fails_closed_without_publication(self) -> None:
        for index in range(1, 6):
            self.write_sentence(index)

        class MalformedRunner:
            calls = 0

            def run(inner_self, _candidate):
                inner_self.calls += 1
                return ModelResult(
                    analysis={"items": "not-an-array"},
                    duration_ms=1,
                    runtime_model="gpt-5.6-luna",
                    runtime_reasoning_effort="max",
                    runtime_metadata_provenance="codex_json_attestation_v1",
                )

        runner = MalformedRunner()
        worker = Worker(self.config, model_runner=runner)
        result = worker.run_once(
            subject="english", study_date=self.study_date
        )
        self.assertEqual(result["receipt"]["status"], "failed")
        self.assertEqual(runner.calls, 1)
        job = worker.store.all_jobs("english")[0]
        self.assertEqual(job["status"], "retrying")
        self.assertEqual(
            job["last_error_code"], "english_two_pass_receipt_invalid"
        )
        self.assertEqual(list((self.state / "candidates").glob("*/*.json")), [])

    def legacy_contract_rate_limit_is_retryable_and_model_is_once_per_attempt(self) -> None:
        for index in range(1, 6):
            self.write_sentence(index)

        class RateLimitedOnceRunner(FakeEnglishRunner):
            def run(inner_self, candidate):
                if inner_self.calls == 0:
                    inner_self.calls += 1
                    raise PreprocessorError("english_candidate_rate_limited")
                return super().run(candidate)

        runner = RateLimitedOnceRunner()
        worker = Worker(self.config, model_runner=runner)
        first = worker.run_once(
            subject="english", study_date=self.study_date
        )
        self.assertEqual(first["receipt"]["status"], "failed")
        self.assertEqual(worker.store.all_jobs("english")[0]["status"], "retrying")
        second = worker.run_once(
            subject="english", study_date=self.study_date
        )
        self.assertEqual(second["receipt"]["status"], "processed")
        self.assertEqual(runner.calls, 2)
        self.assertEqual(worker.store.all_jobs("english")[0]["attempts"], 2)

    def test_source_drift_after_selection_blocks_publication(self) -> None:
        events = [self.write_sentence(index) for index in range(1, 6)]
        event_id = events[0]["event_id"]
        event_path = next((self.state / "events").glob(f"*/{event_id}.json"))
        adapter = self.adapter()
        candidate = adapter.candidates(adapter.status(self.study_date))[0]
        document = FakeEnglishRunner(one_item=True).run(candidate).analysis
        current = json.loads(event_path.read_text(encoding="utf-8"))
        current["article"]["source_hash"] = hashlib.sha256(
            b"changed-source"
        ).hexdigest()
        atomic_write_json(event_path, current)
        with self.assertRaisesRegex(
            PreprocessorError, "english_candidate_source_drift"
        ):
            adapter.publish_candidate(
                candidate, document, runtime_root=self.runtime
            )
        self.assertEqual(list((self.state / "candidates").glob("*/*.json")), [])

    def test_answer_material_key_is_rejected_recursively(self) -> None:
        for index in range(1, 6):
            self.write_sentence(index)
        adapter = self.adapter()
        candidate = adapter.candidates(adapter.status(self.study_date))[0]
        document = FakeEnglishRunner(one_item=True).run(candidate).analysis
        document["items"][0]["card"]["correct_answer"] = "D"
        with self.assertRaisesRegex(
            PreprocessorError, "english_candidate_answer_leak"
        ):
            adapter.publish_candidate(
                candidate, document, runtime_root=self.runtime
            )
        self.assertEqual(list((self.state / "candidates").glob("*/*.json")), [])

    def test_utc_1600_is_next_asia_shanghai_study_date(self) -> None:
        before = self.write_sentence(
            901,
            age_seconds=int(
                (
                    self.now
                    - dt.datetime(2026, 8, 4, 15, 59, 59, tzinfo=dt.timezone.utc)
                ).total_seconds()
            ),
            article_id="BOUNDARY-BEFORE",
        )
        after = self.write_sentence(
            902,
            age_seconds=int(
                (
                    self.now
                    - dt.datetime(2026, 8, 4, 16, 0, 0, tzinfo=dt.timezone.utc)
                ).total_seconds()
            ),
            article_id="BOUNDARY-AFTER",
        )
        self.assertEqual(self.adapter()._event_study_date(before), "2026-08-04")
        self.assertEqual(self.adapter()._event_study_date(after), "2026-08-05")

    def test_independent_english_watcher_refreshes_while_full_run_is_blocked(self) -> None:
        handlers = {}
        full_started = threading.Event()
        projection_seen = threading.Event()

        class LoopWorker:
            def __init__(inner_self):
                inner_self.worker_config = {
                    "poll_interval_seconds": 60,
                    "english_poll_interval_seconds": 1,
                }
                inner_self.adapters = {
                    "english": SimpleNamespace(enabled=True)
                }
                inner_self.runner = SimpleNamespace(cancel_active=lambda: None)
                inner_self.logger = mock.Mock()
                inner_self.calls = []

            def run_once(inner_self):
                inner_self.calls.append("full")
                full_started.set()
                # Represents a full-subject model call whose configured bound
                # can be 180 seconds.  The watcher must make progress without
                # waiting for this operation to release its lock.
                if not projection_seen.wait(timeout=180):
                    raise AssertionError("projection watcher was blocked")

            def run_subject_once(inner_self, subject):
                inner_self.calls.append(subject)

            def refresh_english_projection(inner_self):
                self.assertTrue(full_started.wait(timeout=2))
                inner_self.calls.append("english_projection")
                projection_seen.set()
                handlers[signal.SIGTERM](signal.SIGTERM, None)

        worker = LoopWorker()

        def fake_signal(signum, handler):
            handlers[signum] = handler

        with mock.patch("preprocessor_core.signal.signal", side_effect=fake_signal):
            self.assertEqual(run_loop(worker), 0)
        self.assertEqual(worker.calls[0], "full")
        self.assertIn("english_projection", worker.calls)
        self.assertTrue(projection_seen.is_set())


if __name__ == "__main__":
    unittest.main()
