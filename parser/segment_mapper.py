"""
Segment mapper — converts raw segment strings into structured dicts.

Dates with format qualifier D8 → ISO-8601 (YYYY-MM-DD).
Monetary values → Decimal.
All IDs remain strings.
"""

from __future__ import annotations

import logging
import re
from decimal import Decimal, InvalidOperation
from typing import Any

log = logging.getLogger(__name__)

# Illegal characters per X12 Level 1 SNIP validation
_ILLEGAL_CHAR_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def to_date(value: str) -> str:
    """
    Convert D8 (YYYYMMDD) or RD8 (YYYYMMDD-YYYYMMDD) to ISO-8601.

    D8  → "YYYY-MM-DD"
    RD8 → "YYYY-MM-DD to YYYY-MM-DD"
    Anything else is returned unchanged.
    """
    v = value.strip()
    # D8 — single date
    if len(v) == 8 and v.isdigit():
        return f"{v[:4]}-{v[4:6]}-{v[6:8]}"
    # RD8 — date range  "YYYYMMDD-YYYYMMDD"
    if len(v) == 17 and v[8] == "-" and v[:8].isdigit() and v[9:].isdigit():
        s, e = v[:8], v[9:]
        return f"{s[:4]}-{s[4:6]}-{s[6:8]} to {e[:4]}-{e[4:6]}-{e[6:8]}"
    return v


def to_decimal(value: str) -> Decimal:
    """Convert string to Decimal; return Decimal(0) on failure."""
    try:
        return Decimal(value.strip())
    except (InvalidOperation, ValueError):
        return Decimal("0")


def check_illegal_chars(segment: str) -> list[str]:
    """Return list of illegal characters found in *segment*."""
    return _ILLEGAL_CHAR_RE.findall(segment)


def map_nm1(els: list[str], ed: str, comp: str) -> dict[str, Any]:
    """NM1 — Name segment."""
    return {
        "entity_id": _e(els, 1),
        "entity_type": _e(els, 2),
        "last_org_name": _e(els, 3),
        "first_name": _e(els, 4),
        "middle_name": _e(els, 5),
        "prefix": _e(els, 6),
        "suffix": _e(els, 7),
        "id_qualifier": _e(els, 8),
        "id_code": _e(els, 9),
    }


def map_n3(els: list[str]) -> dict[str, str]:
    return {"address1": _e(els, 1), "address2": _e(els, 2)}


def map_n4(els: list[str]) -> dict[str, str]:
    return {"city": _e(els, 1), "state": _e(els, 2), "zip_code": _e(els, 3)}


def map_clm(els: list[str], comp: str) -> dict[str, Any]:
    """CLM — Claim Information (elements 1–11)."""
    pos_info = _e(els, 5).split(comp)
    return {
        "claim_id": _e(els, 1),
        "total_charge": to_decimal(_e(els, 2)),
        "facility_code": pos_info[0] if pos_info else "",
        "claim_frequency": pos_info[2] if len(pos_info) > 2 else "",
        "provider_accept_assignment": _e(els, 6),
        "benefit_assignment": _e(els, 7),
        "release_info_code": _e(els, 8),
        "patient_signature_source": _e(els, 9),
        "special_program_indicator": _e(els, 10),   # CLM10: 05=EPSDT, 09=CLIA
        "delay_reason_code": _e(els, 11),            # CLM11
    }


def map_sv1(els: list[str], comp: str) -> dict[str, Any]:
    """SV1 — Professional Service.

    SV101 = procedure composite, SV102 = charge, SV103 = unit basis,
    SV104 = quantity, SV105 = facility type / place of service (optional),
    SV106 = service type code (optional), SV107 = diagnosis pointer composite.
    """
    proc_info = _e(els, 1).split(comp)
    diag_raw = _e(els, 7)
    diag_pointers = [d for d in diag_raw.split(comp) if d] if diag_raw else []
    return {
        "procedure_qualifier": proc_info[0] if proc_info else "",
        "procedure_code": proc_info[1] if len(proc_info) > 1 else "",
        "modifier1": proc_info[2] if len(proc_info) > 2 else "",
        "modifier2": proc_info[3] if len(proc_info) > 3 else "",
        "modifier3": proc_info[4] if len(proc_info) > 4 else "",
        "modifier4": proc_info[5] if len(proc_info) > 5 else "",
        "charge": to_decimal(_e(els, 2)),
        "unit_basis": _e(els, 3),
        "quantity": _e(els, 4),
        "place_of_service": _e(els, 5),   # SV105 — facility type code
        "diagnosis_pointers": diag_pointers,
    }


def map_dtp(els: list[str]) -> dict[str, str]:
    """DTP — Date/Time Reference."""
    qualifier = _e(els, 1)
    fmt = _e(els, 2)
    value = _e(els, 3)
    # Both D8 and RD8 are handled by to_date()
    date_value = to_date(value) if fmt in ("D8", "RD8") else value
    return {"qualifier": qualifier, "format": fmt, "date": date_value}


def map_hi(els: list[str], comp: str) -> list[dict]:
    """HI — Health Care Diagnosis Code.

    Returns list of {"qualifier": str, "code": str} dicts.
    Qualifier "BK" = principal diagnosis; "BF" = other diagnosis.
    """
    codes = []
    for i in range(1, len(els)):
        parts = els[i].split(comp)
        if len(parts) >= 2 and parts[1]:
            codes.append({"qualifier": parts[0], "code": parts[1]})
    return codes


def map_ref(els: list[str]) -> dict[str, str]:
    return {"qualifier": _e(els, 1), "value": _e(els, 2)}


def map_sbr(els: list[str]) -> dict[str, str]:
    return {
        "payer_responsibility": _e(els, 1),
        "individual_relationship": _e(els, 2),
        "group_number": _e(els, 3),
        "insurance_type": _e(els, 8),
        "claim_filing_indicator": _e(els, 9),
    }


def map_dmg(els: list[str]) -> dict[str, str]:
    fmt = _e(els, 1)
    dob_raw = _e(els, 2)
    return {
        "dob": to_date(dob_raw) if fmt == "D8" else dob_raw,
        "gender": _e(els, 3),
    }


def map_lx(els: list[str]) -> dict[str, str]:
    return {"line_number": _e(els, 1)}


def map_prv(els: list[str]) -> dict[str, str]:
    return {
        "provider_code": _e(els, 1),
        "qualifier": _e(els, 2),
        "taxonomy_code": _e(els, 3),
    }


def map_pat(els: list[str]) -> dict[str, str]:
    return {"relationship_code": _e(els, 1)}


def map_lin(els: list[str]) -> dict[str, str]:
    """LIN — Drug Identification (2410).

    LIN01 = assigned number, LIN02 = product/service ID qualifier (N4=NDC),
    LIN03 = product/service ID.
    """
    return {
        "assigned_number": _e(els, 1),
        "qualifier": _e(els, 2),
        "product_id": _e(els, 3),
    }


def map_ctp(els: list[str]) -> dict[str, Any]:
    """CTP — Drug Pricing (2410).

    CTP03 = unit price, CTP04 = quantity, CTP05 = unit of measure.
    """
    return {
        "unit_price": to_decimal(_e(els, 3)),
        "quantity": _e(els, 4),
        "unit": _e(els, 5),
    }


def map_svd(els: list[str], comp: str) -> dict[str, Any]:
    """SVD — Service Line Adjudication (2430).

    SVD01 = payer ID, SVD02 = paid amount, SVD03 = procedure composite,
    SVD05 = paid units.
    """
    proc_parts = _e(els, 3).split(comp)
    return {
        "payer_id": _e(els, 1),
        "paid_amount": to_decimal(_e(els, 2)),
        "procedure_code": proc_parts[1] if len(proc_parts) > 1 else "",
        "paid_units": _e(els, 5),
    }


def map_cas(els: list[str]) -> list[dict[str, Any]]:
    """CAS — Claim Adjustment (2430).

    CAS01 = group code; then up to 6 triplets of (reason_code, amount, qty)
    starting at element 2.
    """
    group_code = _e(els, 1)
    adjustments = []
    for offset in range(6):
        base = 2 + offset * 3
        reason = _e(els, base)
        if not reason:
            break
        adjustments.append({
            "group_code": group_code,
            "reason_code": reason,
            "amount": to_decimal(_e(els, base + 1)),
            "quantity": _e(els, base + 2),
        })
    return adjustments


def map_amt(els: list[str]) -> dict[str, Any]:
    """AMT — Monetary Amount (2300 or 2400).

    AMT01 = qualifier, AMT02 = amount.
    """
    return {
        "qualifier": _e(els, 1),
        "amount": to_decimal(_e(els, 2)),
    }


def map_nte(els: list[str]) -> dict[str, str]:
    """NTE — Note/Special Instruction (2300 claim or 2400 service-line).

    NTE01 = note reference code (ADD = additional information, TPO = third-party org).
    NTE02 = free-form text description.
    """
    return {
        "note_reference_code": _e(els, 1),
        "description": _e(els, 2),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _e(els: list[str], index: int, default: str = "") -> str:
    try:
        return els[index]
    except IndexError:
        return default
