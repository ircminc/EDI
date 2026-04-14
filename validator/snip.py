"""
SNIP validation orchestrator — levels 1 through 3.

Status rules:
  - ANY error  → status = 'Fail'
  - only warnings → status = 'Pass'
  - no issues      → status = 'Pass'

One failed claim MUST NOT stop processing of remaining claims.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from parser.models import CanonicalClaim
from .rules import (
    check_billing_provider_nm1,
    check_claim_balance,
    check_diagnosis_pointers,
    check_diagnosis_present,
    check_dos_present,
    check_dos_valid,
    check_hl_hierarchy,
    check_illegal_characters,
    check_invalid_segments,
    check_npi_format,
    check_service_lines_present,
    check_subscriber_name,
    check_total_charge_nonzero,
)

log = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Single validation finding."""
    level: int
    severity: str          # 'error' | 'warning'
    code: str
    message: str
    loop: str = ""
    segment: str = ""
    raw_segment: str = ""
    claim_id: str = ""
    position: int = -1

    def to_dict(self) -> dict[str, Any]:
        return {
            "level": self.level,
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "loop": self.loop,
            "segment": self.segment,
            "raw_segment": self.raw_segment,
            "claim_id": self.claim_id,
            "position": self.position,
        }


@dataclass
class ValidationResult:
    claim_id: str
    status: str                              # 'Pass' | 'Fail'
    errors: list[ValidationError] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        return any(e.severity == "error" for e in self.errors)

    def to_dict(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "status": self.status,
            "errors": [e.to_dict() for e in self.errors],
        }


class SNIPValidator:
    """
    Runs SNIP Level 1–3 validation against a :class:`CanonicalClaim`.

    Parameters
    ----------
    parse_errors:
        Errors already discovered by the state machine (L1 illegal chars,
        L2 HL hierarchy). These are merged into the result.
    element_delimiter:
        Used for L1 invalid-segment checks.
    """

    def __init__(
        self,
        parse_errors: list[dict] | None = None,
        element_delimiter: str = "*",
    ) -> None:
        self._parse_errors = parse_errors or []
        self._ed = element_delimiter

    def validate(self, claim: CanonicalClaim) -> ValidationResult:
        """
        Run all three SNIP levels against *claim*.

        Returns a :class:`ValidationResult` whose status is derived from
        whether any finding has severity='error'.
        """
        findings: list[ValidationError] = []
        claim_id = claim.claim.claim_id
        raw_segs_dicts = [
            {"segment": rs.segment, "loop": rs.loop, "position": rs.position}
            for rs in claim.claim.raw_segments
        ]
        raw_text_list = [rs.segment for rs in claim.claim.raw_segments]

        # ---- Level 1 -------------------------------------------------------
        for e in check_illegal_characters(raw_text_list, claim_id):
            findings.append(_to_ve(e))

        for e in check_invalid_segments(raw_text_list, claim_id, self._ed):
            findings.append(_to_ve(e))

        # Merge parse-time errors (already classified by level)
        for pe in self._parse_errors:
            if pe.get("claim_id", claim_id) == claim_id or not pe.get("claim_id"):
                findings.append(_to_ve({**pe, "claim_id": claim_id}))

        # ---- Level 2 -------------------------------------------------------
        for e in check_billing_provider_nm1(claim, raw_segs_dicts):
            findings.append(_to_ve(e))

        for e in check_subscriber_name(claim, raw_segs_dicts):
            findings.append(_to_ve(e))

        for e in check_diagnosis_present(claim, raw_segs_dicts):
            findings.append(_to_ve(e))

        for e in check_dos_present(claim, raw_segs_dicts):
            findings.append(_to_ve(e))

        for e in check_service_lines_present(claim, raw_segs_dicts):
            findings.append(_to_ve(e))

        # HL hierarchy errors from parse_errors (already labelled L2)
        hl_errors = [p for p in self._parse_errors if p.get("code") == "L2-HL-HIERARCHY"]
        for e in check_hl_hierarchy(hl_errors, claim_id):
            findings.append(_to_ve(e))

        # ---- Level 3 -------------------------------------------------------
        for e in check_claim_balance(claim):
            findings.append(_to_ve(e))

        for e in check_npi_format(claim):
            findings.append(_to_ve(e))

        for e in check_total_charge_nonzero(claim):
            findings.append(_to_ve(e))

        for e in check_dos_valid(claim):
            findings.append(_to_ve(e))

        for e in check_diagnosis_pointers(claim):
            findings.append(_to_ve(e))

        # Deduplicate by code + position + message so that the same rule firing
        # on two different service lines (both using position=-1) produces two
        # distinct findings rather than silently suppressing the second one.
        seen: set[tuple] = set()
        unique: list[ValidationError] = []
        for f in findings:
            key = (f.code, f.position, f.claim_id, f.message)
            if key not in seen:
                seen.add(key)
                unique.append(f)

        has_error = any(f.severity == "error" for f in unique)
        status = "Fail" if has_error else "Pass"

        log.debug("Claim %s validated: status=%s findings=%d", claim_id, status, len(unique))

        return ValidationResult(claim_id=claim_id, status=status, errors=unique)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _to_ve(d: dict[str, Any]) -> ValidationError:
    return ValidationError(
        level=d.get("level", 0),
        severity=d.get("severity", "error"),
        code=d.get("code", "UNKNOWN"),
        message=d.get("message", ""),
        loop=d.get("loop", ""),
        segment=d.get("segment", ""),
        raw_segment=d.get("raw_segment", ""),
        claim_id=d.get("claim_id", ""),
        position=d.get("position", -1),
    )
