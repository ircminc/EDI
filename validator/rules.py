"""
Individual SNIP validation rules for levels 1, 2, and 3.

Each rule function returns a list of error dicts or an empty list.

Error object schema:
  {
    "level": int,
    "severity": "error"|"warning",
    "code": str,
    "message": str,
    "loop": str,
    "segment": str,
    "raw_segment": str,
    "claim_id": str,
    "position": int,
  }
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

from parser.models import CanonicalClaim

# ---------------------------------------------------------------------------
# SNIP LEVEL 1 — Syntactic integrity
# ---------------------------------------------------------------------------

_ILLEGAL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

# Segments that are valid in an 837P transaction (non-exhaustive list of
# standard segment IDs; unknown IDs trigger L1-INVALID-SEG)
_VALID_SEGMENT_IDS = {
    "ST", "BHT", "NM1", "PER", "HL", "PRV", "N3", "N4", "REF",
    "SBR", "PAT", "DMG", "CLM", "DTP", "HI", "LX", "SV1", "SV2",
    "SV3", "SV4", "SV5", "TOO", "CR1", "CR2", "CR3", "CR4", "CR5",
    "CR6", "CR8", "CRC", "NTE", "AMT", "MOA", "PWK", "CN1", "DN1",
    "DN2", "K3", "OI", "MIA", "MOA", "QTY", "HCP", "LIN", "CTP",
    "CL1", "ACB", "MEA", "SE", "GS", "GE", "ISA", "IEA", "TA1",
}


def check_illegal_characters(
    segments: list[str], claim_id: str
) -> list[dict[str, Any]]:
    """L1: Any segment containing control characters is invalid."""
    errors = []
    for pos, seg in enumerate(segments):
        bad = _ILLEGAL_RE.findall(seg)
        if bad:
            errors.append(_err(
                level=1,
                severity="error",
                code="L1-ILLEGAL-CHAR",
                message=f"Segment contains illegal characters: {bad!r}",
                loop="",
                segment=seg[:40],
                raw_segment=seg,
                claim_id=claim_id,
                position=pos,
            ))
    return errors


def check_invalid_segments(
    segments: list[str], claim_id: str, element_delimiter: str
) -> list[dict[str, Any]]:
    """L1: Segment ID not in the 837P standard set."""
    errors = []
    for pos, seg in enumerate(segments):
        seg_id = seg.split(element_delimiter)[0]
        if seg_id and seg_id not in _VALID_SEGMENT_IDS:
            errors.append(_err(
                level=1,
                severity="error",
                code="L1-INVALID-SEG",
                message=f"Unrecognised segment ID: {seg_id!r}",
                loop="",
                segment=seg[:40],
                raw_segment=seg,
                claim_id=claim_id,
                position=pos,
            ))
    return errors


# ---------------------------------------------------------------------------
# SNIP LEVEL 2 — Required segments
# ---------------------------------------------------------------------------

def check_billing_provider_nm1(
    claim: CanonicalClaim, raw_segments: list[dict]
) -> list[dict[str, Any]]:
    """L2: NM1*85 (Billing Provider Name) must be present with a valid NPI."""
    errors = []
    claim_id = claim.claim.claim_id

    if not claim.claim.billing_provider.npi:
        # Find the first raw segment in 2010AA to attach
        raw = _find_raw(raw_segments, "2010AA") or ""
        errors.append(_err(
            level=2,
            severity="error",
            code="L2-MISSING-NM185",
            message="NM1*85 (Billing Provider) is missing or has no NPI (NM109).",
            loop="2010AA",
            segment="NM1",
            raw_segment=raw,
            claim_id=claim_id,
            position=-1,
        ))
    return errors


def check_hl_hierarchy(hl_errors: list[dict], claim_id: str) -> list[dict[str, Any]]:
    """L2: HL parent-child violations discovered during parsing."""
    result = []
    for e in hl_errors:
        result.append(_err(
            level=2,
            severity="error",
            code="L2-HL-HIERARCHY",
            message=e.get("message", "HL hierarchy violation"),
            loop=e.get("loop", "2000x"),
            segment=e.get("segment", "HL"),
            raw_segment=e.get("raw_segment", e.get("segment", "")),
            claim_id=claim_id,
            position=e.get("position", -1),
        ))
    return result


def check_subscriber_name(
    claim: CanonicalClaim, raw_segments: list[dict]
) -> list[dict[str, Any]]:
    """L2: NM1*IL (Subscriber Name) must have a member ID."""
    errors = []
    if not claim.claim.subscriber.member_id:
        raw = _find_raw(raw_segments, "2010BA") or ""
        errors.append(_err(
            level=2,
            severity="warning",
            code="L2-MISSING-NM1IL",
            message="NM1*IL subscriber member ID (NM109) is absent.",
            loop="2010BA",
            segment="NM1",
            raw_segment=raw,
            claim_id=claim.claim.claim_id,
            position=-1,
        ))
    return errors


# ---------------------------------------------------------------------------
# SNIP LEVEL 3 — Claim balance
# ---------------------------------------------------------------------------

def check_claim_balance(claim: CanonicalClaim) -> list[dict[str, Any]]:
    """
    L3: CLM02 (total charge) must equal the sum of SV1*02 (line charges).

    If the claim has no service lines, skip this check.
    """
    errors = []
    sls = claim.claim.service_lines
    if not sls:
        return errors

    sv1_total = sum(sl.charge for sl in sls)
    clm_total = claim.claim.total_charge

    if sv1_total != clm_total:
        errors.append(_err(
            level=3,
            severity="error",
            code="L3-BALANCE-MISMATCH",
            message=(
                f"Balance Mismatch: CLM02={clm_total} != "
                f"sum(SV102)={sv1_total} (difference={clm_total - sv1_total})"
            ),
            loop="2300",
            segment="CLM",
            raw_segment="",
            claim_id=claim.claim.claim_id,
            position=-1,
        ))
    return errors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _err(
    level: int,
    severity: str,
    code: str,
    message: str,
    loop: str,
    segment: str,
    raw_segment: str,
    claim_id: str,
    position: int,
) -> dict[str, Any]:
    return {
        "level": level,
        "severity": severity,
        "code": code,
        "message": message,
        "loop": loop,
        "segment": segment,
        "raw_segment": raw_segment,
        "claim_id": claim_id,
        "position": position,
    }


def _find_raw(raw_segments: list[dict], loop: str) -> str:
    for rs in raw_segments:
        if rs.get("loop") == loop:
            return rs.get("segment", "")
    return ""
