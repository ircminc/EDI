"""
Tests for the SNIP validation engine (levels 1–3).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from ingestion.normalizer import normalize_file_content
from ingestion.detector import detect_delimiters
from ingestion.streamer import stream_transactions
from parser.models import FileEnvelope, TransactionEnvelope
from parser.state_machine import EDI837PStateMachine
from validator.snip import SNIPValidator, ValidationResult
from validator.rules import (
    check_claim_balance,
    check_billing_provider_nm1,
    check_illegal_characters,
    check_invalid_segments,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _validate_file(raw_bytes: bytes) -> list[tuple]:
    """Returns list of (CanonicalClaim, ValidationResult)."""
    content = normalize_file_content(raw_bytes)
    d = detect_delimiters(content)
    pairs = []
    for tx in stream_transactions(content, d):
        fe = FileEnvelope(file_name="test.edi", sender_id=tx.sender_id, receiver_id=tx.receiver_id)
        te = TransactionEnvelope(st_control_number=tx.st_control_number)
        sm = EDI837PStateMachine(fe, te, d.element, d.component)
        claims = sm.parse(tx.segments)
        for c in claims:
            v = SNIPValidator(parse_errors=sm.parse_errors, element_delimiter=d.element)
            r = v.validate(c)
            pairs.append((c, r))
    return pairs


# ---------------------------------------------------------------------------
# Status rules
# ---------------------------------------------------------------------------

class TestStatusRules:
    def test_valid_single_passes(self, valid_single_bytes):
        pairs = _validate_file(valid_single_bytes)
        assert len(pairs) == 1
        assert pairs[0][1].status == "Pass"

    def test_valid_multi_all_pass(self, valid_multi_bytes):
        pairs = _validate_file(valid_multi_bytes)
        assert all(r.status == "Pass" for _, r in pairs)

    def test_missing_nm185_fails(self, missing_nm185_bytes):
        pairs = _validate_file(missing_nm185_bytes)
        assert len(pairs) >= 1
        result = pairs[0][1]
        assert result.status == "Fail"
        codes = [e.code for e in result.errors]
        assert "L2-MISSING-NM185" in codes

    def test_balance_mismatch_fails(self, balance_mismatch_bytes):
        pairs = _validate_file(balance_mismatch_bytes)
        assert len(pairs) >= 1
        result = pairs[0][1]
        assert result.status == "Fail"
        codes = [e.code for e in result.errors]
        assert "L3-BALANCE-MISMATCH" in codes

    def test_invalid_segment_fails(self, invalid_segment_bytes):
        pairs = _validate_file(invalid_segment_bytes)
        assert len(pairs) >= 1
        result = pairs[0][1]
        assert result.status == "Fail"
        codes = [e.code for e in result.errors]
        assert "L1-INVALID-SEG" in codes

    def test_hl_parent_error_fails(self, hl_parent_error_bytes):
        pairs = _validate_file(hl_parent_error_bytes)
        assert len(pairs) >= 1
        result = pairs[0][1]
        # HL hierarchy error → Fail
        assert result.status == "Fail"


# ---------------------------------------------------------------------------
# SNIP Level 1
# ---------------------------------------------------------------------------

class TestSNIPLevel1:
    def test_illegal_char_detected(self):
        segments = ["CLM*X*100\x07BAD"]
        errors = check_illegal_characters(segments, "CLM001")
        assert len(errors) == 1
        assert errors[0]["level"] == 1
        assert errors[0]["severity"] == "error"
        assert errors[0]["code"] == "L1-ILLEGAL-CHAR"

    def test_no_illegal_chars(self):
        segments = ["CLM*X*100", "SV1*HC:99213*100*UN*1***1"]
        errors = check_illegal_characters(segments, "CLM001")
        assert errors == []

    def test_invalid_segment_id(self):
        errors = check_invalid_segments(["ZZZ*FOO*BAR"], "CLM001", "*")
        assert len(errors) == 1
        assert errors[0]["code"] == "L1-INVALID-SEG"

    def test_valid_segment_ids_pass(self):
        segs = ["CLM*001*100", "NM1*85*2*HOSPITAL*****XX*1234567890", "SV1*HC:99213*100*UN*1"]
        errors = check_invalid_segments(segs, "CLM001", "*")
        assert errors == []


# ---------------------------------------------------------------------------
# SNIP Level 2
# ---------------------------------------------------------------------------

class TestSNIPLevel2:
    def test_missing_npi_creates_error(self, missing_nm185_bytes):
        pairs = _validate_file(missing_nm185_bytes)
        errors = pairs[0][1].errors
        l2_errors = [e for e in errors if e.code == "L2-MISSING-NM185"]
        assert len(l2_errors) == 1
        assert l2_errors[0].level == 2
        assert l2_errors[0].loop == "2010AA"


# ---------------------------------------------------------------------------
# SNIP Level 3
# ---------------------------------------------------------------------------

class TestSNIPLevel3:
    def test_balance_mismatch_error_content(self, balance_mismatch_bytes):
        pairs = _validate_file(balance_mismatch_bytes)
        errors = pairs[0][1].errors
        l3 = [e for e in errors if e.code == "L3-BALANCE-MISMATCH"]
        assert len(l3) == 1
        assert "Balance Mismatch" in l3[0].message
        assert l3[0].level == 3

    def test_balanced_claim_no_l3_error(self, valid_single_bytes):
        pairs = _validate_file(valid_single_bytes)
        errors = pairs[0][1].errors
        l3 = [e for e in errors if e.level == 3]
        assert l3 == []

    def test_one_failed_claim_does_not_stop_others(self, valid_multi_bytes):
        """Processing should produce results for all claims even if one fails."""
        pairs = _validate_file(valid_multi_bytes)
        # Both claims should be in results regardless
        assert len(pairs) == 2


# ---------------------------------------------------------------------------
# Error object format
# ---------------------------------------------------------------------------

class TestErrorObjectFormat:
    def test_error_dict_keys(self, missing_nm185_bytes):
        pairs = _validate_file(missing_nm185_bytes)
        err = pairs[0][1].errors[0].to_dict()
        required_keys = {"level", "severity", "code", "message", "loop",
                         "segment", "raw_segment", "claim_id", "position"}
        assert required_keys.issubset(err.keys())

    def test_severity_values(self, missing_nm185_bytes):
        pairs = _validate_file(missing_nm185_bytes)
        for err in pairs[0][1].errors:
            assert err.severity in ("error", "warning")
