"""
Date formatting utilities for EDI 837P display.

All functions are pure Python — no Streamlit dependency — so they are
directly unit-testable.

Stored formats after parsing:
  "YYYY-MM-DD"              single ISO date (D8)
  "YYYY-MM-DD to YYYY-MM-DD" ISO range (RD8)

Legacy raw formats that may appear in real-world files:
  "YYYYMMDD"                raw D8 (before to_date() conversion)
  "YYYYMMDD-YYYYMMDD"       raw RD8

Output format:
  Single date  → "Feb 12, 2026"
  Same month   → "Feb 12–15, 2026"
  Diff months  → "Dec 30, 2025 – Jan 2, 2026"
  Missing      → "-"
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

# ---- Patterns ----------------------------------------------------------------
_D8_RAW   = re.compile(r"^\d{8}$")                          # YYYYMMDD
_RD8_RAW  = re.compile(r"^\d{8}-\d{8}$")                   # YYYYMMDD-YYYYMMDD
_ISO      = re.compile(r"^\d{4}-\d{2}-\d{2}$")             # YYYY-MM-DD
_ISO_RANGE = re.compile(r"^\d{4}-\d{2}-\d{2} to \d{4}-\d{2}-\d{2}$")


# ---- Low-level helpers -------------------------------------------------------

def _parse(iso: str) -> Optional[datetime]:
    """Parse an ISO date string; return None on failure."""
    try:
        return datetime.strptime(iso.strip(), "%Y-%m-%d")
    except (ValueError, AttributeError):
        return None


def normalize_date(value: str) -> str:
    """
    Normalize any supported date string to ISO-8601 (single or range).

    Handles D8, RD8, ISO, and ISO-range inputs.
    Returns the original value unchanged if it cannot be recognized
    (callers treat that as an unformattable value → "-").
    """
    v = value.strip()
    if not v:
        return ""
    if _ISO.match(v) or _ISO_RANGE.match(v):
        return v
    if _D8_RAW.match(v):
        return f"{v[:4]}-{v[4:6]}-{v[6:8]}"
    if _RD8_RAW.match(v):
        s, e = v[:8], v[9:]
        return f"{s[:4]}-{s[4:6]}-{s[6:8]} to {e[:4]}-{e[4:6]}-{e[6:8]}"
    return v  # unrecognized — pass through; fmt_* will return "-"


def fmt_human(iso: str) -> str:
    """
    Convert a single ISO date to human-readable form.

    "2026-02-12" → "Feb 12, 2026"
    Returns "-" for missing or unparseable input.
    """
    if not iso or iso == "-":
        return "-"
    iso = normalize_date(iso)
    # If normalization produced a range, take the start date
    if " to " in iso:
        iso = iso.split(" to ")[0]
    dt = _parse(iso)
    return dt.strftime("%b %d, %Y") if dt else "-"


def fmt_range(start_iso: str, end_iso: str) -> str:
    """
    Format a date range with smart collapsing:

    Same date         → "Feb 12, 2026"
    Same month/year   → "Feb 12–15, 2026"
    Different months  → "Dec 30, 2025 – Jan 2, 2026"
    Any parse failure → "-"
    """
    s = _parse(normalize_date(start_iso))
    e = _parse(normalize_date(end_iso))

    if not s or not e:
        return "-"
    if s == e:
        return s.strftime("%b %d, %Y")
    if s.year == e.year and s.month == e.month:
        # "Feb 12–15, 2026"
        return f"{s.strftime('%b %d')}–{e.strftime('%d, %Y')}"
    # "Dec 30, 2025 – Jan 2, 2026"
    return f"{s.strftime('%b %d, %Y')} – {e.strftime('%b %d, %Y')}"


def service_date_display(
    service_date_from: str,
    service_date_to: str,
    line_dates: list[str],
) -> str:
    """
    Produce a human-readable Date of Service string for one claim.

    Priority:
      1. Claim-level DTP*472 stored as split ISO dates (service_date_from / _to)
      2. Range derived from service-line dates (earliest → latest)

    service_date_from / service_date_to: ISO "YYYY-MM-DD" strings.
      • For a single date both fields are equal (or _to may be empty).
      • For a range they are the start and end of the DTP*472 RD8 value.

    line_dates: ISO single dates OR ISO range strings
      ("YYYY-MM-DD to YYYY-MM-DD") from service-line DTP*472 segments.

    Returns "-" if no usable date is found.
    """
    from_v = service_date_from.strip() if service_date_from else ""
    to_v   = service_date_to.strip()   if service_date_to   else ""

    if from_v:
        # Claim-level date available — use it (to_v may equal from_v for singles)
        effective_to = to_v if to_v else from_v
        return fmt_range(from_v, effective_to)

    # Fall back to service-line dates — collect all individual ISO endpoints,
    # expanding any stored RD8 ranges into their two endpoints.
    all_iso: list[str] = []
    for d in line_dates:
        if not d or d.strip() == "-":
            continue
        normalized = normalize_date(d.strip())
        if " to " in normalized:
            for part in normalized.split(" to "):
                part = part.strip()
                if _ISO.match(part):
                    all_iso.append(part)
        elif _ISO.match(normalized):
            all_iso.append(normalized)

    if not all_iso:
        return "-"
    all_iso.sort()
    return fmt_range(all_iso[0], all_iso[-1])
