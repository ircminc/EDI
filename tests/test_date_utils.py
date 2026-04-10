"""
Unit tests for utils.dates — date formatting utilities.
No Streamlit dependency.
"""

from __future__ import annotations

import pytest
from utils.dates import fmt_human, fmt_range, normalize_date, service_date_display


# ---------------------------------------------------------------------------
# normalize_date
# ---------------------------------------------------------------------------

class TestNormalizeDate:
    def test_d8_raw(self):
        assert normalize_date("20260212") == "2026-02-12"

    def test_rd8_raw(self):
        assert normalize_date("20260212-20260215") == "2026-02-12 to 2026-02-15"

    def test_iso_passthrough(self):
        assert normalize_date("2026-02-12") == "2026-02-12"

    def test_iso_range_passthrough(self):
        v = "2026-02-12 to 2026-02-15"
        assert normalize_date(v) == v

    def test_empty_string(self):
        assert normalize_date("") == ""

    def test_whitespace_stripped(self):
        assert normalize_date("  20260212  ") == "2026-02-12"

    def test_unrecognized_passthrough(self):
        assert normalize_date("N/A") == "N/A"


# ---------------------------------------------------------------------------
# fmt_human
# ---------------------------------------------------------------------------

class TestFmtHuman:
    def test_iso_date(self):
        assert fmt_human("2026-02-12") == "Feb 12, 2026"

    def test_leading_zeros_day(self):
        assert fmt_human("2026-01-05") == "Jan 05, 2026"

    def test_december(self):
        assert fmt_human("2025-12-31") == "Dec 31, 2025"

    def test_raw_d8(self):
        # normalize_date is called internally
        assert fmt_human("20260212") == "Feb 12, 2026"

    def test_empty_returns_dash(self):
        assert fmt_human("") == "-"

    def test_dash_returns_dash(self):
        assert fmt_human("-") == "-"

    def test_malformed_returns_dash(self):
        assert fmt_human("not-a-date") == "-"

    def test_partial_date_returns_dash(self):
        assert fmt_human("2026-02") == "-"

    def test_iso_range_returns_start(self):
        # If a range accidentally passed in, use start
        assert fmt_human("2026-02-12 to 2026-02-15") == "Feb 12, 2026"


# ---------------------------------------------------------------------------
# fmt_range
# ---------------------------------------------------------------------------

class TestFmtRange:
    def test_same_day(self):
        assert fmt_range("2026-02-12", "2026-02-12") == "Feb 12, 2026"

    def test_same_month(self):
        result = fmt_range("2026-02-12", "2026-02-15")
        assert result == "Feb 12–15, 2026"

    def test_different_months_same_year(self):
        result = fmt_range("2026-01-28", "2026-02-03")
        assert result == "Jan 28, 2026 – Feb 03, 2026"

    def test_different_years(self):
        result = fmt_range("2025-12-30", "2026-01-02")
        assert result == "Dec 30, 2025 – Jan 02, 2026"

    def test_raw_d8_inputs(self):
        # normalize_date applied internally
        result = fmt_range("20260212", "20260215")
        assert result == "Feb 12–15, 2026"

    def test_empty_start_returns_dash(self):
        assert fmt_range("", "2026-02-15") == "-"

    def test_empty_end_returns_dash(self):
        assert fmt_range("2026-02-12", "") == "-"

    def test_malformed_start_returns_dash(self):
        assert fmt_range("BADDATE", "2026-02-15") == "-"

    def test_malformed_end_returns_dash(self):
        assert fmt_range("2026-02-12", "BADDATE") == "-"


# ---------------------------------------------------------------------------
# service_date_display
# Signature: service_date_display(service_date_from, service_date_to, line_dates)
#
# service_date_from / _to are pre-split ISO "YYYY-MM-DD" strings produced by
# the state machine. Raw D8/RD8 normalization happens at parse time, not here.
# ---------------------------------------------------------------------------

class TestServiceDateDisplay:
    def test_claim_level_single_iso(self):
        # Same from and to → single date display
        assert service_date_display("2026-02-12", "2026-02-12", []) == "Feb 12, 2026"

    def test_claim_level_single_from_only(self):
        # to absent → treat as single date
        assert service_date_display("2026-02-12", "", []) == "Feb 12, 2026"

    def test_claim_level_iso_range(self):
        result = service_date_display("2026-02-12", "2026-02-15", [])
        assert result == "Feb 12–15, 2026"

    def test_claim_level_range_different_months(self):
        result = service_date_display("2025-12-30", "2026-01-02", [])
        assert result == "Dec 30, 2025 – Jan 02, 2026"

    def test_falls_back_to_line_dates(self):
        result = service_date_display("", "", ["2026-02-12", "2026-02-14", "2026-02-15"])
        assert result == "Feb 12–15, 2026"

    def test_single_line_date(self):
        result = service_date_display("", "", ["2026-02-12"])
        assert result == "Feb 12, 2026"

    def test_missing_everything_returns_dash(self):
        assert service_date_display("", "", []) == "-"

    def test_missing_claim_date_empty_lines_returns_dash(self):
        assert service_date_display("", "", ["-", "", "  "]) == "-"

    def test_claim_date_takes_priority_over_lines(self):
        result = service_date_display("2026-01-01", "2026-01-01", ["2026-03-01", "2026-03-15"])
        assert result == "Jan 01, 2026"

    def test_malformed_from_returns_dash(self):
        # Unrecognised format → _parse returns None → fmt_range → "-"
        result = service_date_display("BAD-DATE", "BAD-DATE", [])
        assert result == "-"

    def test_line_dates_sorted_correctly(self):
        # Out-of-order input — earliest and latest must be correct
        result = service_date_display("", "", ["2026-02-15", "2026-02-03", "2026-02-09"])
        assert result == "Feb 03–15, 2026"

    def test_rd8_line_dates_same_day(self):
        # ISO range stored in service line — both endpoints equal
        result = service_date_display("", "", ["2005-03-21 to 2005-03-21"])
        assert result == "Mar 21, 2005"

    def test_rd8_line_dates_range(self):
        result = service_date_display("", "", ["2004-02-01 to 2004-02-07"])
        assert result == "Feb 01–07, 2004"

    def test_rd8_multiple_lines_full_range(self):
        # Multiple identical RD8 lines — min of all starts to max of all ends
        result = service_date_display("", "", [
            "2004-02-01 to 2004-02-07",
            "2004-02-01 to 2004-02-07",
            "2004-02-01 to 2004-02-07",
        ])
        assert result == "Feb 01–07, 2004"

    def test_mixed_single_and_range_line_dates(self):
        result = service_date_display("", "", [
            "2006-10-03",
            "2006-10-03",
            "2006-10-10",
            "2006-10-10",
        ])
        assert result == "Oct 03–10, 2006"

    def test_all_line_dates_same(self):
        result = service_date_display("", "", [
            "2005-01-19", "2005-01-19",
        ])
        assert result == "Jan 19, 2005"
