"""
Envelope integrity validation — ISA/GS/ST control numbers and counts.

Rules enforced per ASC X12 spec:
  - ISA13 == IEA02  (interchange control number match)
  - IEA01 == number of GS groups present
  - GS06  == GE02   (group control number match)
  - GE01  == number of ST transactions in that GS group
  - ST02  == SE02   (transaction set control number match)
  - SE01  (segment count) == actual segment count in ST..SE (inclusive)
  - Missing IEA → TruncatedFileError
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Generator

from .detector import DelimiterSet

log = logging.getLogger(__name__)


class EnvelopeError(Exception):
    """Raised for recoverable envelope integrity violations."""

    def __init__(self, message: str, segment: str = "", position: int = -1) -> None:
        super().__init__(message)
        self.segment = segment
        self.position = position


class TruncatedFileError(Exception):
    """Raised when IEA segment is missing — file is truncated."""


@dataclass
class _InterchangeCtx:
    control_number: str = ""
    gs_expected: int = 0
    gs_count: int = 0


@dataclass
class _GroupCtx:
    control_number: str = ""
    st_expected: int = 0
    st_count: int = 0


def validate_envelope(
    segments: list[str],
    delimiters: DelimiterSet,
) -> list[EnvelopeError]:
    """
    Validate the ISA/GS/ST/SE/GE/IEA envelope of a normalized segment list.

    Parameters
    ----------
    segments:
        All segments from the file (split by segment terminator, blank entries removed).
    delimiters:
        Delimiter set extracted by detect_delimiters().

    Returns
    -------
    list[EnvelopeError]
        Empty list means the envelope is intact.

    Raises
    ------
    TruncatedFileError
        If no IEA segment is found at all.
    """
    ed = delimiters.element
    errors: list[EnvelopeError] = []

    iea_found = False
    interchange: _InterchangeCtx | None = None
    group: _GroupCtx | None = None

    for pos, seg in enumerate(segments):
        els = seg.split(ed)
        seg_id = els[0]

        if seg_id == "ISA":
            interchange = _InterchangeCtx(
                control_number=_get(els, 13),
            )

        elif seg_id == "IEA":
            iea_found = True
            if interchange is None:
                errors.append(EnvelopeError("IEA without matching ISA.", seg, pos))
                continue
            iea_gs_count = _int(els, 1)
            iea_ctrl = _get(els, 2)
            if iea_ctrl != interchange.control_number:
                errors.append(EnvelopeError(
                    f"ISA13/IEA02 mismatch: ISA13={interchange.control_number!r}, "
                    f"IEA02={iea_ctrl!r}",
                    seg, pos,
                ))
            if iea_gs_count != interchange.gs_count:
                errors.append(EnvelopeError(
                    f"IEA01 says {iea_gs_count} GS groups, but found {interchange.gs_count}.",
                    seg, pos,
                ))
            interchange = None

        elif seg_id == "GS":
            group = _GroupCtx(control_number=_get(els, 6))
            if interchange:
                interchange.gs_count += 1

        elif seg_id == "GE":
            if group is None:
                errors.append(EnvelopeError("GE without matching GS.", seg, pos))
                continue
            ge_st_count = _int(els, 1)
            ge_ctrl = _get(els, 2)
            if ge_ctrl != group.control_number:
                errors.append(EnvelopeError(
                    f"GS06/GE02 mismatch: GS06={group.control_number!r}, "
                    f"GE02={ge_ctrl!r}",
                    seg, pos,
                ))
            if ge_st_count != group.st_count:
                errors.append(EnvelopeError(
                    f"GE01 says {ge_st_count} ST transactions, but found {group.st_count}.",
                    seg, pos,
                ))
            group = None

        elif seg_id == "ST":
            if group:
                group.st_count += 1

    if not iea_found:
        raise TruncatedFileError(
            "File is truncated: IEA segment not found. "
            "The interchange envelope was never closed."
        )

    return errors


def validate_transaction_counts(
    st_segments: list[str],
    delimiters: DelimiterSet,
) -> list[EnvelopeError]:
    """
    Validate a single ST..SE transaction block.

    Rules:
      - ST02 == SE02
      - SE01 == actual segment count (ST and SE inclusive)

    Parameters
    ----------
    st_segments:
        All segments within (and including) ST..SE.
    delimiters:
        Delimiter set.
    """
    ed = delimiters.element
    errors: list[EnvelopeError] = []

    if not st_segments:
        return errors

    st_els = st_segments[0].split(ed)
    se_els = st_segments[-1].split(ed)

    st_ctrl = _get(st_els, 2)
    se_ctrl = _get(se_els, 2)
    se_count = _int(se_els, 1)
    actual_count = len(st_segments)

    if st_els[0] != "ST":
        errors.append(EnvelopeError(
            f"Transaction block does not begin with ST: {st_segments[0][:40]!r}",
            st_segments[0], 0,
        ))

    if se_els[0] != "SE":
        errors.append(EnvelopeError(
            f"Transaction block does not end with SE: {st_segments[-1][:40]!r}",
            st_segments[-1], len(st_segments) - 1,
        ))
        return errors

    if st_ctrl != se_ctrl:
        errors.append(EnvelopeError(
            f"ST02/SE02 mismatch: ST02={st_ctrl!r}, SE02={se_ctrl!r}",
            st_segments[-1], len(st_segments) - 1,
        ))

    if se_count != actual_count:
        errors.append(EnvelopeError(
            f"SE01 segment count mismatch: SE01={se_count}, actual={actual_count}",
            st_segments[-1], len(st_segments) - 1,
        ))

    return errors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(elements: list[str], index: int, default: str = "") -> str:
    try:
        return elements[index]
    except IndexError:
        return default


def _int(elements: list[str], index: int, default: int = 0) -> int:
    try:
        return int(elements[index])
    except (IndexError, ValueError):
        return default
