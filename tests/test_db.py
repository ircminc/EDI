"""
Tests for the DB repository layer.

These tests use an in-memory mock rather than a live PostgreSQL connection
so they can run in CI without a database.
"""

from __future__ import annotations

import json
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

from parser.models import (
    BillingProvider, CanonicalClaim, Claim, FileEnvelope,
    RawSegment, ServiceLine, Subscriber, TransactionEnvelope,
)
from validator.snip import SNIPValidator, ValidationResult, ValidationError
from db.repository import ClaimRepository, _dumps


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_canonical(
    claim_id="CLM001",
    total=Decimal("150"),
    npi="1234567890",
    sender="SENDER",
    receiver="RECEIVER",
) -> CanonicalClaim:
    return CanonicalClaim(
        file=FileEnvelope(
            file_name="test.edi",
            sender_id=sender,
            receiver_id=receiver,
        ),
        transaction=TransactionEnvelope(st_control_number="0001"),
        claim=Claim(
            claim_id=claim_id,
            total_charge=total,
            billing_provider=BillingProvider(npi=npi),
            subscriber=Subscriber(member_id="MEM001"),
            service_lines=[
                ServiceLine(line_number=1, charge=total, date="2024-01-01"),
            ],
            raw_segments=[
                RawSegment(segment="CLM*CLM001*150", position=0, loop="2300"),
            ],
        ),
    )


def _make_result(status="Pass", errors=None) -> ValidationResult:
    return ValidationResult(
        claim_id="CLM001",
        status=status,
        errors=errors or [],
    )


def _mock_conn(fetchone_return=(42,)):
    conn = MagicMock()
    cur = MagicMock()
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchone.return_value = fetchone_return
    cur.description = [
        ("id",), ("file_name",), ("sender_id",), ("receiver_id",),
        ("claim_id",), ("billing_npi",), ("total_charge",), ("status",),
        ("dos_from",), ("dos_to",), ("prior_auth_number",),
        ("rendering_npi",), ("payer_id",),
        ("raw_payload",), ("validation_log",), ("created_at",),
    ]
    cur.fetchall.return_value = [
        (
            1, "test.edi", "S", "R", "CLM001", "1234567890",
            150, "Pass",
            "2024-01-01", "2024-01-01", "",
            "9876543210", "BCBS01",
            {}, [], None,
        )
    ]
    conn.cursor.return_value = cur
    return conn, cur


# ---------------------------------------------------------------------------
# JSON encoder
# ---------------------------------------------------------------------------

class TestDecimalEncoder:
    def test_decimal_serialized_as_string(self):
        data = {"charge": Decimal("99.99")}
        out = json.loads(_dumps(data))
        assert out["charge"] == "99.99"

    def test_nested_decimal(self):
        data = {"a": {"b": Decimal("0.01")}}
        out = json.loads(_dumps(data))
        assert out["a"]["b"] == "0.01"


# ---------------------------------------------------------------------------
# Insert
# ---------------------------------------------------------------------------

class TestInsert:
    def test_insert_returns_id(self):
        conn, cur = _mock_conn(fetchone_return=(42,))
        repo = ClaimRepository(conn)
        row_id = repo.insert_claim(_make_canonical(), _make_result(), "test.edi")
        assert row_id == 42

    def test_insert_calls_execute(self):
        conn, cur = _mock_conn(fetchone_return=(1,))
        repo = ClaimRepository(conn)
        repo.insert_claim(_make_canonical(), _make_result(), "test.edi")
        assert cur.execute.called

    def test_insert_failed_claim(self):
        conn, cur = _mock_conn(fetchone_return=(7,))
        repo = ClaimRepository(conn)
        err = ValidationError(
            level=3, severity="error", code="L3-BALANCE-MISMATCH",
            message="Balance Mismatch", loop="2300", segment="CLM",
            raw_segment="", claim_id="CLM001", position=-1,
        )
        result = ValidationResult(claim_id="CLM001", status="Fail", errors=[err])
        row_id = repo.insert_claim(_make_canonical(), result, "test.edi")
        assert row_id == 7

    def test_insert_many_returns_ids(self):
        conn, cur = _mock_conn(fetchone_return=(1,))
        # Make fetchone return different IDs sequentially
        cur.fetchone.side_effect = [(1,), (2,)]
        repo = ClaimRepository(conn)
        pairs = [
            (_make_canonical("CLM001"), _make_result()),
            (_make_canonical("CLM002"), _make_result()),
        ]
        ids = repo.insert_many(pairs, "test.edi")
        assert ids == [1, 2]

    def test_insert_uses_claim_id(self):
        conn, cur = _mock_conn(fetchone_return=(5,))
        repo = ClaimRepository(conn)
        canonical = _make_canonical(claim_id="MY_CLAIM_99")
        repo.insert_claim(canonical, _make_result(), "f.edi")
        # Check that "MY_CLAIM_99" was passed to execute
        call_args = cur.execute.call_args
        params = call_args[0][1]
        assert "MY_CLAIM_99" in params

    def test_insert_uses_npi(self):
        conn, cur = _mock_conn(fetchone_return=(5,))
        repo = ClaimRepository(conn)
        canonical = _make_canonical(npi="9999999999")
        repo.insert_claim(canonical, _make_result(), "f.edi")
        call_args = cur.execute.call_args
        params = call_args[0][1]
        assert "9999999999" in params


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

class TestSearch:
    def test_search_returns_list(self):
        conn, cur = _mock_conn()
        repo = ClaimRepository(conn)
        rows = repo.search()
        assert isinstance(rows, list)

    def test_search_by_claim_id(self):
        conn, cur = _mock_conn()
        repo = ClaimRepository(conn)
        repo.search(claim_id="CLM001")
        sql = cur.execute.call_args[0][0]
        assert "claim_id" in sql

    def test_search_by_npi(self):
        conn, cur = _mock_conn()
        repo = ClaimRepository(conn)
        repo.search(billing_npi="1234567890")
        sql = cur.execute.call_args[0][0]
        assert "billing_npi" in sql

    def test_search_by_status(self):
        conn, cur = _mock_conn()
        repo = ClaimRepository(conn)
        repo.search(status="Fail")
        sql = cur.execute.call_args[0][0]
        assert "status" in sql

    def test_count_method(self):
        conn, cur = _mock_conn()
        cur.fetchone.return_value = (5,)
        repo = ClaimRepository(conn)
        n = repo.count()
        assert n == 5

    def test_search_by_dos_from(self):
        conn, cur = _mock_conn()
        repo = ClaimRepository(conn)
        repo.search(dos_from="2024-01-01")
        sql = cur.execute.call_args[0][0]
        assert "dos_from" in sql

    def test_search_by_dos_to(self):
        conn, cur = _mock_conn()
        repo = ClaimRepository(conn)
        repo.search(dos_to="2024-12-31")
        sql = cur.execute.call_args[0][0]
        assert "dos_to" in sql

    def test_search_dos_range_both(self):
        """Both dos_from and dos_to appear together in the WHERE clause."""
        conn, cur = _mock_conn()
        repo = ClaimRepository(conn)
        repo.search(dos_from="2024-01-01", dos_to="2024-12-31")
        call_args = cur.execute.call_args[0]
        sql, params = call_args[0], call_args[1]
        assert "dos_from" in sql
        assert "dos_to" in sql
        assert "2024-01-01" in params
        assert "2024-12-31" in params

    def test_search_dos_open_ended_from_only(self):
        """Omitting dos_to still inserts dos_from condition only (no dos_to WHERE clause)."""
        conn, cur = _mock_conn()
        repo = ClaimRepository(conn)
        repo.search(dos_from="2024-06-01")
        call_args = cur.execute.call_args[0]
        sql, params = call_args[0], call_args[1]
        assert "dos_from >= %s" in sql
        assert "dos_to <= %s" not in sql   # <= %s only; dos_to also in SELECT so check full clause
        assert "2024-06-01" in params

    def test_search_no_dos_no_conditions(self):
        """No DOS args → no dos_from / dos_to WHERE conditions in generated SQL."""
        conn, cur = _mock_conn()
        repo = ClaimRepository(conn)
        repo.search()
        sql = cur.execute.call_args[0][0]
        assert "dos_from >= %s" not in sql
        assert "dos_to <= %s" not in sql


# ---------------------------------------------------------------------------
# New columns (rendering_npi / payer_id) in INSERT
# ---------------------------------------------------------------------------

class TestInsertNewColumns:
    def test_rendering_npi_included_in_params(self):
        from parser.models import Provider
        conn, cur = _mock_conn(fetchone_return=(10,))
        repo = ClaimRepository(conn)
        canonical = _make_canonical()
        # Attach a rendering provider
        canonical.claim.rendering_provider = Provider(
            qualifier="82", npi="9876543210"
        )
        repo.insert_claim(canonical, _make_result(), "f.edi")
        params = cur.execute.call_args[0][1]
        assert "9876543210" in params

    def test_payer_id_included_in_params(self):
        conn, cur = _mock_conn(fetchone_return=(11,))
        repo = ClaimRepository(conn)
        canonical = _make_canonical()
        canonical.claim.subscriber.payer_id = "BCBS001"
        repo.insert_claim(canonical, _make_result(), "f.edi")
        params = cur.execute.call_args[0][1]
        assert "BCBS001" in params

    def test_rendering_npi_empty_when_no_provider(self):
        """When rendering_provider is None, rendering_npi must be empty string."""
        conn, cur = _mock_conn(fetchone_return=(12,))
        repo = ClaimRepository(conn)
        canonical = _make_canonical()
        canonical.claim.rendering_provider = None
        repo.insert_claim(canonical, _make_result(), "f.edi")
        params = cur.execute.call_args[0][1]
        # rendering_npi is the 11th positional param (0-indexed: 10)
        assert params[10] == ""


# ---------------------------------------------------------------------------
# B3 — DOS fallback handles range-format service-line dates
# ---------------------------------------------------------------------------

class TestDOSRangeFallback:
    """B3 — When all service lines have range dates ("YYYY-MM-DD to YYYY-MM-DD"),
    dos_from and dos_to must still be extracted from those ranges."""

    def _canonical_with_line_dates(self, *dates: str) -> "CanonicalClaim":
        """Build a canonical claim whose service lines use the given date strings."""
        from parser.models import Subscriber
        canonical = _make_canonical()
        canonical.claim.service_date_from = ""
        canonical.claim.service_date_to   = ""
        canonical.claim.service_lines = [
            ServiceLine(line_number=i + 1, charge=Decimal("100"), date=d)
            for i, d in enumerate(dates)
        ]
        canonical.claim.subscriber = Subscriber(member_id="M001")
        return canonical

    def test_single_range_date_extracts_bounds(self):
        conn, cur = _mock_conn(fetchone_return=(1,))
        repo = ClaimRepository(conn)
        canonical = self._canonical_with_line_dates("2024-01-01 to 2024-01-05")
        repo.insert_claim(canonical, _make_result(), "f.edi")
        params = cur.execute.call_args[0][1]
        # dos_from is at index 7, dos_to at index 8
        assert params[7] == "2024-01-01"
        assert params[8] == "2024-01-05"

    def test_multiple_range_dates_picks_earliest_and_latest(self):
        conn, cur = _mock_conn(fetchone_return=(2,))
        repo = ClaimRepository(conn)
        canonical = self._canonical_with_line_dates(
            "2024-03-01 to 2024-03-05",
            "2024-01-10 to 2024-01-15",
        )
        repo.insert_claim(canonical, _make_result(), "f.edi")
        params = cur.execute.call_args[0][1]
        assert params[7] == "2024-01-10"   # earliest start
        assert params[8] == "2024-03-05"   # latest end

    def test_mixed_single_and_range_dates(self):
        conn, cur = _mock_conn(fetchone_return=(3,))
        repo = ClaimRepository(conn)
        canonical = self._canonical_with_line_dates(
            "2024-06-01 to 2024-06-10",
            "2024-05-20",               # single date
        )
        repo.insert_claim(canonical, _make_result(), "f.edi")
        params = cur.execute.call_args[0][1]
        assert params[7] == "2024-05-20"   # earliest overall
        assert params[8] == "2024-06-10"   # latest overall

    def test_all_single_dates_still_work(self):
        """Regression: existing single-date logic must not regress."""
        conn, cur = _mock_conn(fetchone_return=(4,))
        repo = ClaimRepository(conn)
        canonical = self._canonical_with_line_dates("2024-02-01", "2024-02-15")
        repo.insert_claim(canonical, _make_result(), "f.edi")
        params = cur.execute.call_args[0][1]
        assert params[7] == "2024-02-01"
        assert params[8] == "2024-02-15"

    def test_no_dates_produces_none(self):
        conn, cur = _mock_conn(fetchone_return=(5,))
        repo = ClaimRepository(conn)
        canonical = self._canonical_with_line_dates("")   # empty date
        repo.insert_claim(canonical, _make_result(), "f.edi")
        params = cur.execute.call_args[0][1]
        assert params[7] is None
        assert params[8] is None


# ---------------------------------------------------------------------------
# get_stats
# ---------------------------------------------------------------------------

class TestGetStats:
    def _mock_stats_conn(self, row):
        conn = MagicMock()
        cur = MagicMock()
        cur.__enter__ = MagicMock(return_value=cur)
        cur.__exit__ = MagicMock(return_value=False)
        cur.fetchone.return_value = row
        conn.cursor.return_value = cur
        return conn, cur

    def test_get_stats_returns_dict(self):
        conn, cur = self._mock_stats_conn((10, 8, 2, "1500.00", 3, 5))
        repo = ClaimRepository(conn)
        stats = repo.get_stats()
        assert isinstance(stats, dict)

    def test_get_stats_keys_present(self):
        conn, cur = self._mock_stats_conn((10, 8, 2, "1500.00", 3, 5))
        repo = ClaimRepository(conn)
        stats = repo.get_stats()
        for key in (
            "total_claim_count", "pass_count", "fail_count",
            "pass_rate", "total_charge_sum",
            "warning_count_sum", "error_count_sum",
        ):
            assert key in stats, f"Missing key: {key}"

    def test_get_stats_pass_rate_calculation(self):
        conn, cur = self._mock_stats_conn((10, 8, 2, "1500.00", 3, 5))
        repo = ClaimRepository(conn)
        stats = repo.get_stats()
        assert stats["total_claim_count"] == 10
        assert stats["pass_count"] == 8
        assert stats["fail_count"] == 2
        assert abs(stats["pass_rate"] - 0.8) < 1e-9

    def test_get_stats_charge_sum_is_decimal(self):
        conn, cur = self._mock_stats_conn((5, 5, 0, "999.50", 0, 0))
        repo = ClaimRepository(conn)
        stats = repo.get_stats()
        from decimal import Decimal
        assert stats["total_charge_sum"] == Decimal("999.50")

    def test_get_stats_zero_claims_pass_rate(self):
        """Pass rate must be 0.0 (not ZeroDivisionError) when table is empty."""
        conn, cur = self._mock_stats_conn((0, 0, 0, "0", 0, 0))
        repo = ClaimRepository(conn)
        stats = repo.get_stats()
        assert stats["pass_rate"] == 0.0

    def test_get_stats_warning_and_error_counts(self):
        conn, cur = self._mock_stats_conn((4, 2, 2, "400.00", 7, 3))
        repo = ClaimRepository(conn)
        stats = repo.get_stats()
        assert stats["warning_count_sum"] == 7
        assert stats["error_count_sum"] == 3


# ---------------------------------------------------------------------------
# delete_by_file
# ---------------------------------------------------------------------------

class TestDeleteByFile:
    def _mock_delete_conn(self, rowcount: int):
        conn = MagicMock()
        cur = MagicMock()
        cur.__enter__ = MagicMock(return_value=cur)
        cur.__exit__ = MagicMock(return_value=False)
        cur.rowcount = rowcount
        conn.cursor.return_value = cur
        return conn, cur

    def test_delete_returns_rowcount(self):
        conn, cur = self._mock_delete_conn(3)
        repo = ClaimRepository(conn)
        n = repo.delete_by_file("old_batch.edi")
        assert n == 3

    def test_delete_zero_when_not_found(self):
        conn, cur = self._mock_delete_conn(0)
        repo = ClaimRepository(conn)
        n = repo.delete_by_file("nonexistent.edi")
        assert n == 0

    def test_delete_sql_uses_file_name(self):
        conn, cur = self._mock_delete_conn(5)
        repo = ClaimRepository(conn)
        repo.delete_by_file("batch_2024.edi")
        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "file_name" in sql
        assert "batch_2024.edi" in params

    def test_delete_calls_execute_once(self):
        conn, cur = self._mock_delete_conn(1)
        repo = ClaimRepository(conn)
        repo.delete_by_file("x.edi")
        assert cur.execute.call_count == 1
