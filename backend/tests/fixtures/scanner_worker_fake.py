"""Small, test-only scanner Worker contract fake.

The production scanner owns candidate construction.  Tests that exercise the
dispatch bridge only need to provide already-built rows and, for producer
watermark cases, a status projection.  Keeping those inputs outside this
fixture makes it harder for a business payload fake to accidentally become a
second producer implementation.
"""

from __future__ import annotations

import copy
from collections.abc import Callable, Mapping, Sequence
from typing import Any


DEFAULT_RELEASE_ID = "a" * 64

RowsSource = (
    Sequence[tuple[Any, str]]
    | Callable[[str, str, Mapping[str, Any]], Sequence[tuple[Any, str]]]
)
StatusesSource = Mapping[str, Any] | Callable[..., Mapping[str, Any]] | None


class ScannerWorkerFake:
    """A read-only scanner surface with explicit call recording."""

    def __init__(
        self,
        config: Mapping[str, Any],
        *,
        release_id: str = DEFAULT_RELEASE_ID,
        eligible_candidates: RowsSource = (),
        scan_statuses: StatusesSource = None,
        adapters: Mapping[str, Any] | None = None,
        calls: list[dict[str, Any]] | None = None,
    ) -> None:
        self.config = dict(config)
        self.release_id = release_id
        self._eligible_source = eligible_candidates
        self._statuses_source = scan_statuses
        self.adapters = dict(adapters or {})
        self.calls = calls if calls is not None else []

    def scan_statuses(
        self,
        study_date: str | None = None,
        *,
        only_subject: str | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "method": "scan_statuses",
                "study_date": study_date,
                "only_subject": only_subject,
            }
        )
        source = self._statuses_source
        if callable(source):
            try:
                value = source(study_date, only_subject=only_subject)
            except TypeError:
                value = source(study_date, only_subject)
        elif source is None:
            value = {}
        else:
            value = source
        if not isinstance(value, Mapping):
            raise AssertionError("scanner fake statuses must be a mapping")
        if (
            only_subject is not None
            and "pending" in value
            and only_subject not in value
        ):
            value = {only_subject: value}
        return copy.deepcopy(dict(value))

    def eligible_candidates(
        self,
        subject: str,
        study_date: str,
        *,
        mutate_recovery_state: bool = False,
        scan_all_study_dates: bool = False,
        capture_allowlist: frozenset[str] | None = None,
        controlled_replay: bool = False,
    ) -> list[tuple[Any, str]]:
        kwargs = {
            "mutate_recovery_state": mutate_recovery_state,
            "scan_all_study_dates": scan_all_study_dates,
            "capture_allowlist": capture_allowlist,
            "controlled_replay": controlled_replay,
        }
        self.calls.append(
            {
                "method": "eligible_candidates",
                "subject": subject,
                "study_date": study_date,
                "kwargs": copy.deepcopy(kwargs),
            }
        )
        if mutate_recovery_state:
            raise AssertionError("scanner fake must remain read-only")
        source = self._eligible_source
        if callable(source):
            try:
                rows = source(subject, study_date, **kwargs)
            except TypeError:
                rows = source(subject, study_date, kwargs)
        else:
            rows = source
        return list(rows)


class ScannerWorkerFactory:
    """Callable factory suitable for ``scan_eligible_candidates``."""

    def __init__(
        self,
        *,
        release_id: str = DEFAULT_RELEASE_ID,
        eligible_candidates: RowsSource = (),
        scan_statuses: StatusesSource = None,
        adapters: Mapping[str, Any] | None = None,
    ) -> None:
        self.release_id = release_id
        self.eligible_rows = eligible_candidates
        self.status_rows = scan_statuses
        self.adapters = dict(adapters or {})
        self.calls: list[dict[str, Any]] = []

    @property
    def eligible_candidates(self) -> RowsSource:
        return self.eligible_rows

    @eligible_candidates.setter
    def eligible_candidates(self, rows: RowsSource) -> None:
        self.eligible_rows = rows

    @property
    def scan_statuses(self) -> StatusesSource:
        return self.status_rows

    @scan_statuses.setter
    def scan_statuses(self, statuses: StatusesSource) -> None:
        self.status_rows = statuses

    def __call__(self, config: Mapping[str, Any]) -> ScannerWorkerFake:
        return ScannerWorkerFake(
            config,
            release_id=self.release_id,
            eligible_candidates=self.eligible_rows,
            scan_statuses=self.status_rows,
            adapters=self.adapters,
            calls=self.calls,
        )

    def set_candidates(self, rows: RowsSource) -> None:
        self.eligible_rows = rows

    def set_statuses(self, statuses: StatusesSource) -> None:
        self.status_rows = statuses

    def reset_calls(self) -> None:
        self.calls.clear()


def make_scanner_worker_factory(
    *,
    release_id: str = DEFAULT_RELEASE_ID,
    eligible_candidates: RowsSource = (),
    scan_statuses: StatusesSource = None,
    adapters: Mapping[str, Any] | None = None,
) -> ScannerWorkerFactory:
    """Build a configured worker factory and its shared call ledger."""

    return ScannerWorkerFactory(
        release_id=release_id,
        eligible_candidates=eligible_candidates,
        scan_statuses=scan_statuses,
        adapters=adapters,
    )


__all__ = [
    "DEFAULT_RELEASE_ID",
    "ScannerWorkerFake",
    "ScannerWorkerFactory",
    "make_scanner_worker_factory",
]
