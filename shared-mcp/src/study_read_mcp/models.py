from __future__ import annotations

from datetime import date
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


Subject = Literal["math", "cs408", "english"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class EvidenceItem(StrictModel):
    subject: Subject
    artifact_kind: str = Field(min_length=1, max_length=48)
    stable_id: str = Field(min_length=1, max_length=160)
    expected_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class RouteContext(StrictModel):
    caller_skill_id: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9][a-z0-9-]*$")
    caller_skill_version: str = Field(min_length=1, max_length=80, pattern=r"^[A-Za-z0-9][A-Za-z0-9.+-]*$")
    plugin_version: str = Field(min_length=1, max_length=80, pattern=r"^[A-Za-z0-9][A-Za-z0-9.+-]*$")
    route_request_id: str = Field(min_length=8, max_length=80, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
    evidence_scope_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    read_route: Literal["mcp", "mcp_chunked"] = "mcp"
    chunk_index: int = Field(default=1, ge=1, le=256)
    chunk_count: int = Field(default=1, ge=1, le=256)
    consumed_duplicate_read_count: Literal[0] = 0

    @model_validator(mode="after")
    def validate_chunks(self) -> "RouteContext":
        if self.chunk_index > self.chunk_count:
            raise ValueError("chunk_index must not exceed chunk_count")
        if self.read_route == "mcp" and (self.chunk_index != 1 or self.chunk_count != 1):
            raise ValueError("unchunked routes must use chunk 1 of 1")
        if self.read_route == "mcp_chunked" and self.chunk_count < 2:
            raise ValueError("chunked routes require at least two chunks")
        return self


class MathQuery(StrictModel):
    op: Literal["formal_cards", "activity_window", "direct_relations", "review_snapshot"]
    ids: list[str] = Field(default_factory=list, max_length=24)
    date_from: date | None = None
    date_to: date | None = None
    fields: list[str] = Field(default_factory=list, max_length=16)

    @model_validator(mode="after")
    def validate_shape(self) -> "MathQuery":
        if self.op in {"formal_cards", "direct_relations", "review_snapshot"} and not self.ids:
            raise ValueError("ids are required for this operation")
        if self.op == "activity_window" and not (self.date_from and self.date_to):
            raise ValueError("date_from and date_to are required")
        if self.fields and self.op not in {"formal_cards", "review_snapshot"}:
            raise ValueError("fields are supported only for card-bearing operations")
        if self.date_from and self.date_to and self.date_from > self.date_to:
            raise ValueError("date_from must not exceed date_to")
        return self


class CS408Query(StrictModel):
    op: Literal["curation_inventory", "knowledge_nodes", "direct_edges", "morning_state", "review_identity"]
    ids: list[str] = Field(default_factory=list, max_length=24)
    study_date: date | None = None
    fields: list[str] = Field(default_factory=list, max_length=16)

    @model_validator(mode="after")
    def validate_shape(self) -> "CS408Query":
        if self.op in {"knowledge_nodes", "direct_edges", "review_identity"} and not self.ids:
            raise ValueError("ids are required for this operation")
        if self.fields:
            raise ValueError("408 field projections are not enabled in schema v1")
        return self


class CS408MorningPreparationRequest(StrictModel):
    review_date: date
    queue_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    item_ids: list[str] = Field(min_length=1, max_length=4)

    @field_validator("item_ids")
    @classmethod
    def validate_item_ids(cls, values: list[str]) -> list[str]:
        if len(values) != len(set(values)):
            raise ValueError("item_ids must be unique")
        if any(
            not re.fullmatch(r"(?:MQ|DQ|OQ|AOQ)-[A-Za-z0-9_.:-]{1,80}", value)
            for value in values
        ):
            raise ValueError("item_id is invalid")
        return values


class EnglishRequest(StrictModel):
    article_id: str | None = Field(default=None, max_length=160)
    sentence_ids: list[str] = Field(default_factory=list, max_length=60)
    terms: list[str] = Field(default_factory=list, max_length=60)
    study_date: date | None = None
    include: list[Literal["article", "sentences", "vocab_status", "day_events", "patterns", "coverage"]] = Field(min_length=1, max_length=6)
    expected_generation: str | None = None
    expected_release: str | None = None
    expected_fingerprint: str | None = None

    @field_validator("terms")
    @classmethod
    def bound_terms(cls, values: list[str]) -> list[str]:
        if any(len(v) > 100 for v in values):
            raise ValueError("term exceeds 100 characters")
        return values

    @model_validator(mode="after")
    def validate_shape(self) -> "EnglishRequest":
        if any(x in self.include for x in ("article", "sentences", "coverage")) and not self.article_id:
            raise ValueError("article_id is required")
        if "day_events" in self.include and not self.study_date:
            raise ValueError("study_date is required")
        if len(self.sentence_ids) + len(self.terms) > 80:
            raise ValueError("combined item limit is 80")
        return self


class PagedReadRequest(StrictModel):
    collection: str = Field(
        min_length=1, max_length=48, pattern=r"^[a-z][a-z0-9_]*$"
    )
    cursor: str | None = Field(
        default=None, max_length=512, pattern=r"^[A-Za-z0-9_-]+$"
    )
    page_size: int = Field(default=24, ge=1, le=48)
    query: str | None = Field(default=None, max_length=240)
    ids: list[str] = Field(default_factory=list, max_length=48)
    article_id: str | None = Field(default=None, max_length=160)
    study_date: date | None = None

    @field_validator("ids")
    @classmethod
    def bound_ids(cls, values: list[str]) -> list[str]:
        if any(not value or len(value) > 160 for value in values):
            raise ValueError("id exceeds its stable bound")
        if len(values) != len(set(values)):
            raise ValueError("ids must be unique")
        return values


JsonObject = dict[str, Any]
