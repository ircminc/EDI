"""
Delimiter detection from the ISA segment.

The ISA segment is exactly 106 characters (including the segment terminator).
All positions are fixed per ASC X12 005010 spec:

  Position 3   → element delimiter    (e.g. '*')
  Position 104  → component separator  (e.g. ':')
  Position 105  → segment terminator   (e.g. '~')

ISA06 (sender ID)   → positions 35-49  (15 chars, right-padded with spaces)
ISA08 (receiver ID) → positions 54-68  (15 chars, right-padded with spaces)
ISA13 (control num) → positions 90-98  (9 chars)
ISA15 (usage ind.)  → position 102     ('P'=Production, 'T'=Test)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)

ISA_LENGTH = 106
ISA_SEGMENT_ID = "ISA"


@dataclass(frozen=True)
class DelimiterSet:
    element: str       # e.g. '*'
    component: str     # e.g. ':'
    segment: str       # e.g. '~'
    repetition: str    # ISA11 in 00501 is the repetition separator


def detect_delimiters(raw: str) -> DelimiterSet:
    """
    Extract delimiters from the first 106 characters of the ISA segment.

    Parameters
    ----------
    raw:
        Normalized file content (BOM stripped, CRLF→LF already applied).
        Must start with 'ISA'.

    Raises
    ------
    ValueError
        If the content does not begin with 'ISA' or is shorter than 106 chars.
    """
    if len(raw) < ISA_LENGTH:
        raise ValueError(
            f"Content too short to be a valid ISA segment: got {len(raw)} chars, "
            f"expected at least {ISA_LENGTH}."
        )

    if not raw.startswith(ISA_SEGMENT_ID):
        raise ValueError(
            f"Content does not begin with 'ISA'. Got: {raw[:3]!r}"
        )

    element_delim = raw[3]
    repetition_sep = raw[82]   # ISA11 — repetition separator in 005010
    component_sep = raw[104]   # ISA16
    segment_term = raw[105]    # segment terminator

    delimiters = DelimiterSet(
        element=element_delim,
        component=component_sep,
        segment=segment_term,
        repetition=repetition_sep,
    )

    log.debug(
        "Detected delimiters — element=%r component=%r segment=%r repetition=%r",
        element_delim, component_sep, segment_term, repetition_sep,
    )

    return delimiters


def extract_isa_fields(raw: str, delimiters: DelimiterSet) -> dict[str, str]:
    """
    Extract key ISA header fields into a dict.

    Returns keys: isa06_sender, isa08_receiver, isa13_control_number,
                  isa15_usage_indicator, isa12_version.
    """
    if len(raw) < ISA_LENGTH:
        raise ValueError("Content too short for ISA field extraction.")

    return {
        "isa06_sender": raw[35:50].strip(),
        "isa08_receiver": raw[54:69].strip(),
        "isa12_version": raw[84:89].strip(),
        "isa13_control_number": raw[90:99].strip(),
        "isa15_usage_indicator": raw[102].strip(),
    }
