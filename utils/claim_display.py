"""
Pure-Python display helpers for CanonicalClaim objects.

Extracted from ui/app.py so they can be reused in export.py and other
non-Streamlit contexts without triggering Streamlit's import-time side effects.
"""

from __future__ import annotations

from parser.models import CanonicalClaim
from utils.dates import service_date_display


def patient_name(canonical: CanonicalClaim) -> str:
    """
    Return a formatted patient name string: "Last, First Middle".

    Falls back to subscriber when no patient block is present.
    Returns "-" when neither has a name.
    """
    p = canonical.claim.patient
    s = canonical.claim.subscriber

    def _fmt(last: str, first: str, middle: str) -> str:
        last   = last.strip().title()
        first  = first.strip().title()
        mid    = middle.strip().title()
        name   = f"{last}, {first}".strip(", ").strip()
        return f"{name} {mid}".strip() if mid else name or "-"

    if p and (p.last_name or p.first_name):
        return _fmt(p.last_name, p.first_name, p.middle_name)
    if s.last_name or s.first_name:
        return _fmt(s.last_name, s.first_name, s.middle_name)
    return "-"


def dos(canonical: CanonicalClaim) -> str:
    """
    Return a human-readable Date of Service string.

    Prefers claim-level service_date_from/to; falls back to the min/max of
    service-line dates when the claim-level fields are absent.
    """
    c = canonical.claim
    line_dates = [sl.date for sl in c.service_lines if sl.date]
    return service_date_display(c.service_date_from, c.service_date_to, line_dates)
