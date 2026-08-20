from __future__ import annotations

import copy
import hashlib
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from english_pipeline.errors import ValidationError
from english_pipeline.events import append_event
from english_pipeline.quick_flush import (
    publish_quick_flush_intent,
    verify_quick_flush_intent,
)
from english_pipeline.util import load_json


def request(
    *,
    key: str = "quick-flush-1",
    source_article: str = "articles/quick-flush.md",
    source_hash: str = "a" * 64,
) -> dict:
    sentence = "A lasting reward can change how people think."
    return {
        "event_type": "sentence_captured",
        "idempotency_key": key,
        "occurred_at": "2026-08-13T09:00:00Z",
        "article": {
            "article_id": "RAW-QUICK-FLUSH-001",
            "source_id": "RAW-QUICK-FLUSH-001",
            "source_article": source_article,
            "source_hash": source_hash,
        },
        "source": {
            "sentence_id": "S01",
            "source_sentence": sentence,
            "source_kind": "article",
        },
        "learning": {
            "first_translation": None,
            "user_evidence_verbatim": "lasting 不会",
            "evidence_states": ["unknown_observed"],
            "evidence_origin": "synthetic_fixture",
            "user_evidence": ["unknown"],
            "answer_protection": "practice_safe",
            "hint_level": 0,
            "translation": "持久的回报会改变人们的想法。",
            "explanation": "lasting 表示持久的。",
            "first_breakpoint": "lasting",
            "restatement": "lasting 是持久的。",
        },
        "candidates": [],
    }


class QuickFlushIntentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        base = Path(self.temp.name)
        self.repo = base / "repo"
        self.state = base / "intake"
        self.article_locator = "articles/quick-flush.md"
        handoff_locator = "raw/fixtures/quick-flush/pipeline_handoff.json"
        payload_locator = "source/practice-safe.md"
        payload_path = self.repo / "raw/fixtures/quick-flush/source/practice-safe.md"
        payload_path.parent.mkdir(parents=True, exist_ok=True)
        raw_payload = (
            "# Quick Flush Practice Safe\r\n\r\n"
            "A lasting reward can change how people think.\t\r\n\r\n"
        ).encode("utf-8")
        payload_path.write_bytes(raw_payload)
        normalized = raw_payload.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
        normalized = "\n".join(line.rstrip() for line in normalized.split("\n")).rstrip("\n")
        self.source_hash = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        handoff_path = self.repo / handoff_locator
        handoff_path.parent.mkdir(parents=True, exist_ok=True)
        handoff_path.write_text(
            json.dumps(
                {
                    "schema_version": "english-learning-pipeline-handoff-v1",
                    "source_id": "RAW-QUICK-FLUSH-001",
                    "source_hash": f"sha256:{self.source_hash}",
                    "canonical_payload": {
                        "path": payload_locator,
                        "normalization": "UTF-8; LF line endings; trailing whitespace removed per line; no final LF",
                        "visibility": "practice_safe",
                    },
                    "units": [],
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        article_path = self.repo / self.article_locator
        article_path.parent.mkdir(parents=True, exist_ok=True)
        article_path.write_text(
            "\n".join(
                [
                    "# Quick Flush Fixture",
                    "",
                    "## 基本信息",
                    "",
                    "- source_id：RAW-QUICK-FLUSH-001",
                    f"- source_hash：sha256:{self.source_hash}",
                    f"- pipeline_handoff：`{handoff_locator}`",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _request(self, *, key: str = "quick-flush-1") -> dict:
        return request(
            key=key,
            source_article=self.article_locator,
            source_hash=self.source_hash,
        )

    def _publish(self) -> tuple[dict, dict, dict]:
        capture = append_event(self.state, self._request())
        event = load_json(Path(capture["event_path"]))
        receipt = load_json(Path(capture["receipt_path"]))
        result = publish_quick_flush_intent(
            self.state,
            event=event,
            capture_receipt=receipt,
        )
        return event, receipt, result

    def test_intent_is_hmac_sealed_content_addressed_and_idempotent(self) -> None:
        event, receipt, first = self._publish()
        path = Path(first["intent_path"])
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), path.stem)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        verified = verify_quick_flush_intent(
            json.loads(path.read_bytes()), state_dir=self.state
        )
        self.assertEqual(verified["event_id"], event["event_id"])
        self.assertEqual(first["formal_write_count"], 0)
        again = publish_quick_flush_intent(
            self.state,
            event=event,
            capture_receipt=receipt,
        )
        self.assertEqual(again["status"], "idempotent_noop")
        self.assertEqual(again["intent_sha256"], first["intent_sha256"])
        key_path = self.state / "quick-flush" / "authority.key"
        self.assertEqual(stat.S_IMODE(key_path.stat().st_mode), 0o600)
        self.assertEqual(len(key_path.read_bytes()), 32)

    def test_tamper_wrong_receipt_and_duplicate_event_fail_closed(self) -> None:
        event, receipt, first = self._publish()
        intent = load_json(Path(first["intent_path"]))
        tampered = copy.deepcopy(intent)
        tampered["source_id"] = "RAW-TAMPERED"
        with self.assertRaises(ValidationError):
            verify_quick_flush_intent(tampered, state_dir=self.state)

        bad_receipt = copy.deepcopy(receipt)
        bad_receipt["event_sha256"] = "f" * 64
        with self.assertRaises(ValidationError):
            publish_quick_flush_intent(
                self.state,
                event=event,
                capture_receipt=bad_receipt,
            )

        original_path = Path(first["intent_path"])
        duplicate = copy.deepcopy(intent)
        duplicate["created_at"] = "2026-08-13T10:00:00Z"
        duplicate["authority"]["hmac_sha256"] = "0" * 64
        raw = json.dumps(duplicate, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        digest = hashlib.sha256(raw).hexdigest()
        duplicate_path = original_path.parents[1] / digest[:2] / f"{digest}.json"
        duplicate_path.parent.mkdir(parents=True, exist_ok=True)
        duplicate_path.write_bytes(raw)
        os.chmod(duplicate_path, 0o600)
        with self.assertRaises(ValidationError):
            publish_quick_flush_intent(
                self.state,
                event=event,
                capture_receipt=receipt,
            )

    def test_cli_quick_flush_returns_event_and_intent_receipts(self) -> None:
        request_path = Path(self.temp.name) / "request.json"
        request_path.write_text(
            json.dumps(self._request(key="quick-flush-cli"), ensure_ascii=False),
            encoding="utf-8",
        )
        script = Path(__file__).resolve().parents[2] / "scripts" / "english_learning_pipeline.py"
        completed = subprocess.run(
            [
                sys.executable,
                str(script),
                "capture",
                "--repo-root",
                str(self.repo),
                "--state-dir",
                str(self.state),
                "--input-json",
                str(request_path),
                "--quick-flush",
            ],
            cwd=script.parents[1],
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        result = json.loads(completed.stdout)
        self.assertEqual(result["status"], "created")
        self.assertEqual(result["quick_flush"]["status"], "created")
        self.assertEqual(result["quick_flush"]["event_id"], result["capture_id"])
        self.assertEqual(result["formal_write_count"], 0)


if __name__ == "__main__":
    unittest.main()
