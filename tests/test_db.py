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
        ("raw_payload",), ("validation_log",), ("created_at",),
    ]
    cur.fetchall.return_value = [
        (1, "test.edi", "S", "R", "CLM001", "1234567890", 150, "Pass", {}, [], None)
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
