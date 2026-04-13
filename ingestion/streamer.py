"""
Streaming generator — yields one ST-SE transaction block at a time.

Memory contract: only one ST-SE block is held in memory at once.
Caller controls buffering; large files remain < 200 MB RSS.

Usage:
    content = normalize_file_content(path)
    delimiters = detect_delimiters(content)
    for block in stream_transactions(content, delimiters):
        process(block)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Generator

from .detector import DelimiterSet, extract_isa_fields
from .integrity import (
    EnvelopeError,
    TruncatedFileError,
    validate_envelope,
    validate_transaction_counts,
)

log = logging.getLogger(__name__)


@dataclass
class TransactionBlock:
    """One ST-SE transaction with its envelope metadata."""

    # ISA-level
    sender_id: str
    receiver_id: str
    isa_control_number: str
    isa_version: str
    usage_indicator: str

    # GS-level
    gs_sender: str
    gs_receiver: str
    gs_date: str
    gs_time: str
    gs_control_number: str
    functional_id: str

    # ST-SE content
    segments: list[str] = field(default_factory=list)
    """All segments inclusive of ST and SE."""

    # Envelope errors discovered for this transaction
    envelope_errors: list[EnvelopeError] = field(default_factory=list)

    @property
    def st_control_number(self) -> str:
        if self.segments:
            parts = self.segments[0].split("*")
            return parts[2] if len(parts) > 2 else ""
        return ""


def stream_transactions(
    content: str,
    delimiters: DelimiterSet,
    allow_truncated: bool = False,
) -> Generator[TransactionBlock, None, None]:
    """
    Yield one :class:`TransactionBlock` per ST-SE pair found in *content*.

    File-level envelope validation (IEA missing → TruncatedFileError) is
    performed eagerly on all segments before streaming begins, but individual
    ST-SE blocks are yielded lazily so memory stays bounded.

    Parameters
    ----------
    content:
        Normalized file content (BOM/CRLF already processed).
    delimiters:
        Delimiter set from :func:`detect_delimiters`.
    allow_truncated:
        When ``True``, a missing IEA segment is treated as a warning rather
        than a fatal error.  Useful for files from vendors that omit the
        interchange-closing segment.  Defaults to ``False`` (strict mode).

    Raises
    ------
    TruncatedFileError
        If the IEA segment is absent **and** ``allow_truncated`` is ``False``.
    """
    seg_term = delimiters.segment
    ed = delimiters.element

    # Split into segments once — this is an O(N) operation on the full content.
    # Blank entries (e.g. trailing newlines after segment terminator) are filtered.
    all_segments: list[str] = [
        s.strip() for s in content.split(seg_term) if s.strip()
    ]

    # File-level integrity check.
    # TruncatedFileError (missing IEA) is re-raised unless lenient mode is on.
    try:
        file_errors = validate_envelope(all_segments, delimiters)
    except TruncatedFileError:
        if allow_truncated:
            log.warning(
                "IEA segment not found — processing in lenient mode. "
                "Claims extracted from ST-SE blocks will still be validated."
            )
            file_errors = []
        else:
            raise

    if file_errors:
        for err in file_errors:
            log.warning("Envelope error: %s", str(err))

    # Extract ISA header fields.
    isa_fields = extract_isa_fields(content, delimiters)

    # Stream ST-SE blocks.
    current_isa: dict = {}
    current_gs: dict = {}
    in_transaction = False
    transaction_segments: list[str] = []

    for seg in all_segments:
        els = seg.split(ed)
        seg_id = els[0]

        if seg_id == "ISA":
            current_isa = {
                "sender_id": els[6].strip() if len(els) > 6 else "",
                "receiver_id": els[8].strip() if len(els) > 8 else "",
                "control_number": els[13] if len(els) > 13 else "",
                "version": els[12] if len(els) > 12 else "",
                "usage": els[15] if len(els) > 15 else "",
            }

        elif seg_id == "GS":
            current_gs = {
                "functional_id": els[1] if len(els) > 1 else "",
                "sender": els[2] if len(els) > 2 else "",
                "receiver": els[3] if len(els) > 3 else "",
                "date": els[4] if len(els) > 4 else "",
                "time": els[5] if len(els) > 5 else "",
                "control_number": els[6] if len(els) > 6 else "",
            }

        elif seg_id == "ST":
            in_transaction = True
            transaction_segments = [seg]

        elif seg_id == "SE":
            transaction_segments.append(seg)
            in_transaction = False

            tx_errors = validate_transaction_counts(transaction_segments, delimiters)

            block = TransactionBlock(
                sender_id=current_isa.get("sender_id", ""),
                receiver_id=current_isa.get("receiver_id", ""),
                isa_control_number=current_isa.get("control_number", ""),
                isa_version=current_isa.get("version", ""),
                usage_indicator=current_isa.get("usage", ""),
                gs_sender=current_gs.get("sender", ""),
                gs_receiver=current_gs.get("receiver", ""),
                gs_date=current_gs.get("date", ""),
                gs_time=current_gs.get("time", ""),
                gs_control_number=current_gs.get("control_number", ""),
                functional_id=current_gs.get("functional_id", ""),
                segments=transaction_segments,
                envelope_errors=tx_errors,
            )
            log.debug(
                "Yielding ST=%s with %d segments.",
                block.st_control_number,
                len(transaction_segments),
            )
            yield block
            transaction_segments = []

        elif in_transaction:
            transaction_segments.append(seg)

        # ISA, GS, GE, IEA are consumed but not appended to transaction_segments.
