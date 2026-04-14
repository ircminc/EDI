"""
Delimiter detection from the ISA segment.

Per ASC X12 005010 spec the ISA segment is exactly 106 characters when all
fields are padded to their fixed widths.  In practice many trading partners
omit the padding, so we derive delimiters two ways:

Primary (split-based, works for padded AND unpadded ISA):
  - Element delimiter  → raw[3]  (always position 3 — immediately after "ISA")
  - Split raw on element delimiter with a limit of 16 to get 17 parts.
    parts[16] = ISA16_char + segment_terminator + rest_of_file
    → component separator = parts[16][0]
    → segment terminator  = parts[16][1]
  - Repetition separator → parts[11][0]  (ISA11 field)

Fallback for field extraction (uses fixed positions — only valid for padded ISA):
  ISA06 (sender ID)   → positions 35-49  (15 chars)
  ISA08 (receiver ID) → positions 54-68  (15 chars)
  ISA13 (control num) → positions 90-98  (9 chars)
  ISA15 (usage ind.)  → position 102
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
    Extract delimiters from the ISA segment using element-delimiter splitting.

    Works correctly for both spec-compliant (padded) ISA segments and
    non-padded ISA segments produced by some trading partners.

    Parameters
    ----------
    raw:
        Normalized file content (BOM stripped, CRLF→LF already applied).
        Must start with 'ISA'.

    Raises
    ------
    ValueError
        If the content does not begin with 'ISA', is too short, or has
        fewer than 16 element delimiters in the ISA segment.
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

    # Position 3 is always the element delimiter — "ISA" is fixed at positions 0-2.
    element_delim = raw[3]

    # Split on element delimiter exactly 16 times to isolate the 17 ISA fields.
    # parts[0]  = "ISA"
    # parts[1]  = ISA01 (auth info qualifier)
    # ...
    # parts[11] = ISA11 (repetition separator — one character)
    # ...
    # parts[16] = ISA16_char + segment_terminator + rest_of_file
    parts = raw.split(element_delim, 16)
    if len(parts) < 17:
        raise ValueError(
            f"ISA segment appears malformed: fewer than 16 '{element_delim}' "
            f"element delimiters found in the first segment."
        )

    # ISA11 — repetition separator (single-character field)
    repetition_sep = parts[11][0] if parts[11] else "^"

    # ISA16 (component separator) is the first character of parts[16].
    # The segment terminator immediately follows it.
    isa16_tail = parts[16]
    component_sep = isa16_tail[0] if isa16_tail else ":"
    segment_term = isa16_tail[1] if len(isa16_tail) > 1 else "~"

    if segment_term.isalnum() or segment_term == " ":
        log.warning(
            "Segment terminator resolved to %r (hex %02X) — "
            "this is unusual and may indicate a malformed ISA segment.",
            segment_term, ord(segment_term),
        )

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
