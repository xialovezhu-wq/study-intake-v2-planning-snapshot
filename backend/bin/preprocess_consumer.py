#!/usr/bin/env python3
"""Fail-open package consumer and append-only Sol adoption receipt CLI."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lib"))

from preprocessor_core import (  # noqa: E402
    ADOPTION_OUTCOMES,
    PreprocessorError,
    consume_package,
    load_config,
    record_adoption,
)


DEFAULT_CONFIG = ROOT / "config.json"


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Consume validated Luna preprocessing packages")
    root.add_argument("--config", default=str(DEFAULT_CONFIG))
    sub = root.add_subparsers(dest="command", required=True)

    consume = sub.add_parser("consume")
    consume.add_argument("--subject", choices=("math", "cs408", "english"), required=True)
    consume.add_argument("--capture-id", required=True)
    consume.add_argument("--study-date", required=True)
    consume.add_argument("--freeze-id")
    consume.add_argument("--effective-evidence-hash")
    consume.add_argument("--batch-id")
    consume.add_argument("--capture-set-hash")

    adoption = sub.add_parser("record-adoption")
    adoption.add_argument("--adoption-token", required=True)
    adoption.add_argument("--outcome", choices=sorted(ADOPTION_OUTCOMES), required=True)
    adoption.add_argument("--reason-code")
    adoption.add_argument("--sol-decision-sha256")
    adoption.add_argument("--canonical-package-sha256")
    adoption.add_argument("--item-result-event-id")
    adoption.add_argument("--item-result-event-sha256")
    adoption.add_argument(
        "--terminal-outcome",
        choices=("curated", "already_current", "needs_user", "failed"),
    )
    adoption.add_argument("--formal-id")
    adoption.add_argument("--normal-receipt-sha256")
    adoption.add_argument("--verification-sha256")
    return root


def main() -> int:
    os.umask(0o077)
    args = parser().parse_args()
    try:
        config = load_config(Path(args.config).expanduser().resolve())
        if args.command == "consume":
            value = consume_package(
                config,
                subject=args.subject,
                capture_id=args.capture_id,
                study_date=args.study_date,
                freeze_id=args.freeze_id,
                effective_evidence_hash=args.effective_evidence_hash,
                batch_id=args.batch_id,
                capture_set_hash=args.capture_set_hash,
            )
        else:
            value = record_adoption(
                config,
                adoption_token=args.adoption_token,
                outcome=args.outcome,
                reason_code=args.reason_code,
                sol_decision_sha256=args.sol_decision_sha256,
                canonical_package_sha256=args.canonical_package_sha256,
                item_result_event_id=args.item_result_event_id,
                item_result_event_sha256=args.item_result_event_sha256,
                terminal_outcome=args.terminal_outcome,
                formal_id=args.formal_id,
                normal_receipt_sha256=args.normal_receipt_sha256,
                verification_sha256=args.verification_sha256,
            )
        emit(value)
        return 0
    except PreprocessorError as exc:
        emit(
            {
                "schema_version": "study-intake-preprocess-cli-error-v1",
                "status": "error",
                "error_code": exc.code,
                "formal_write_count": 0,
            }
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
