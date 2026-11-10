"""Dead-letter queue contract: ``{original_event, error_type, error_detail, failed_at}``."""

from __future__ import annotations

import base64
from datetime import UTC, datetime
from typing import Literal

from pydantic import AwareDatetime, BaseModel

from freightpulse.domain.enums import DlqErrorType

Encoding = Literal["utf-8", "base64"]


class DlqRecord(BaseModel):
    original_event: str
    original_encoding: Encoding = "utf-8"
    original_topic: str
    original_key: str | None = None
    original_partition: int | None = None
    original_offset: int | None = None
    error_type: DlqErrorType
    error_detail: str
    failed_at: AwareDatetime

    @classmethod
    def build(
        cls,
        raw: bytes,
        *,
        topic: str,
        key: str | None,
        error_type: DlqErrorType,
        detail: str,
        partition: int | None = None,
        offset: int | None = None,
    ) -> DlqRecord:
        enc: Encoding
        try:
            text, enc = raw.decode("utf-8"), "utf-8"
        except UnicodeDecodeError:
            text, enc = base64.b64encode(raw).decode("ascii"), "base64"
        return cls(
            original_event=text,
            original_encoding=enc,
            original_topic=topic,
            original_key=key,
            original_partition=partition,
            original_offset=offset,
            error_type=error_type,
            error_detail=detail[:2000],
            failed_at=datetime.now(UTC),
        )

    def original_bytes(self) -> bytes:
        if self.original_encoding == "base64":
            return base64.b64decode(self.original_event)
        return self.original_event.encode("utf-8")
