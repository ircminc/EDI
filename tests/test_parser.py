"""
Tests for the parser layer:
  - HL tracker
  - Segment mapper
  - State machine (canonical output)
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from ingestion.normalizer import normalize_file_content
from ingestion.detector import detect_delimiters
from ingestion.streamer import stream_transactions
from parser.models import FileEnvelope, TransactionEnvelope, CanonicalClaim
from parser.state_machine import EDI837PStateMachine
from parser.hl_tracker import HLTracker
from parser.segment_mapper import to_date, to_decimal, map_sv1, map_clm, map_nm1


# ---------------------------------------------------------------------------
# Segment mapper unit tests
# ---------------------------------------------------------------------------

class TestSegmentMapper:
    def test_to_date_d8(self):
        assert to_date("20240101") == "2024-01-01"

    def test_to_date_passthrough(self):
        assert to_date("RD8-stuff") == "RD8-stuff"

    def test_to_decimal_valid(self):
        assert to_decimal("150.75") == Decimal("150.75")

    def test_to_decimal_invalid(self):
        assert to_decimal("N/A") == Decimal("0")

    def test_map_sv1_charge(self):
        els = "SV1*HC:99213*150.00*UN*1***1".split("*")
        result = map_sv1(els, ":")
        assert result["charge"] == Decimal("150.00")
        assert result["procedure_code"] == "99213"

    def test_map_clm(self):
        els = "CLM*CLM001*250***11:B:1*Y*A*Y*I".split("*")
        result = map_clm(els, ":")
        assert result["claim_id"] == "CLM001"
        assert result["total_charge"] == Decimal("250")

    def test_map_nm1_billing_provider(self):
        els = "NM1*85*2*ACME HOSPITAL*****XX*1234567890".split("*")
        result = map_nm1(els, "*", ":")
        assert result["entity_id"] == "85"
        assert result["id_code"] == "1234567890"


# ---------------------------------------------------------------------------
# HL Tracker
# ---------------------------------------------------------------------------

class TestHLTracker:
    def _make_hl(self, hl_id, parent, code, child="0"):
        return f"HL*{hl_id}*{parent}*{code}*{child}"

    def test_valid_hierarchy(self):
        tracker = HLTracker()
        tracker.process(self._make_hl("1", "", "20", "1"), "*", 0)
        tracker.process(self._make_hl("2", "1", "22", "0"), "*", 1)
        assert len(tracker.errors) == 0

    def test_valid_three_level(self):
        tracker = HLTracker()
        tracker.process(self._make_hl("1", "", "20", "1"), "*", 0)
        tracker.process(self._make_hl("2", "1", "22", "1"), "*", 1)
        tracker.process(self._make_hl("3", "2", "23", "0"), "*", 2)
        assert len(tracker.errors) == 0

    def test_invalid_parent_missing(self):
        tracker = HLTracker()
        tracker.process(self._make_hl("1", "", "20", "1"), "*", 0)
        # Parent 99 does not exist
        tracker.process(self._make_hl("2", "99", "22", "0"), "*", 1)
        assert len(tracker.errors) == 1
        assert "99" in tracker.errors[0].message

    def test_invalid_parent_wrong_level(self):
        tracker = HLTracker()
        # 23 (patient) cannot be child of 20 (billing provider)
        tracker.process(self._make_hl("1", "", "20", "1"), "*", 0)
        tracker.process(self._make_hl("2", "1", "23", "0"), "*", 1)
        assert len(tracker.errors) == 1

    def test_reset_clears_state(self):
        tracker = HLTracker()
        tracker.process(self._make_hl("1", "", "20", "1"), "*", 0)
        tracker.reset()
        assert tracker.current is None
        assert tracker.errors == []


# ---------------------------------------------------------------------------
# State machine — end-to-end claim extraction
# ---------------------------------------------------------------------------

def _parse_file(raw_bytes: bytes) -> list[CanonicalClaim]:
    content = normalize_file_content(raw_bytes)
    d = detect_delimiters(content)
    all_claims = []
    for tx in stream_transactions(content, d):
        fe = FileEnvelope(
            file_name="test.edi",
            sender_id=tx.sender_id,
            receiver_id=tx.receiver_id,
            isa_control_number=tx.isa_control_number,
        )
        te = TransactionEnvelope(
            st_control_number=tx.st_control_number,
            gs_control_number=tx.gs_control_number,
        )
        sm = EDI837PStateMachine(fe, te, d.element, d.component)
        all_claims.extend(sm.parse(tx.segments))
    return all_claims


class TestStateMachine:
    def test_valid_single_produces_one_claim(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        assert len(claims) == 1

    def test_claim_id_extracted(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        assert claims[0].claim.claim_id == "CLM001"

    def test_total_charge_decimal(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        assert claims[0].claim.total_charge == Decimal("150")

    def test_billing_npi_extracted(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        assert claims[0].claim.billing_provider.npi == "1234567890"

    def test_subscriber_extracted(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        sub = claims[0].claim.subscriber
        assert sub.last_name == "DOE"
        assert sub.first_name == "JOHN"

    def test_patient_extracted(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        pat = claims[0].claim.patient
        assert pat is not None
        assert pat.last_name == "DOE"
        assert pat.first_name == "JANE"

    def test_service_lines_extracted(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        sls = claims[0].claim.service_lines
        assert len(sls) == 2
        assert sls[0].charge == Decimal("100")
        assert sls[1].charge == Decimal("50")

    def test_service_line_date_iso(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        assert claims[0].claim.service_lines[0].date == "2024-01-01"

    def test_valid_multi_produces_two_claims(self, valid_multi_bytes):
        claims = _parse_file(valid_multi_bytes)
        assert len(claims) == 2

    def test_raw_segments_populated(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        assert len(claims[0].claim.raw_segments) > 0

    def test_raw_segment_has_loop(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        loops = {rs.loop for rs in claims[0].claim.raw_segments}
        assert "2300" in loops
        assert "2400" in loops

    def test_to_dict_structure(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        d = claims[0].to_dict()
        assert "file" in d
        assert "transaction" in d
        assert "claim" in d
        assert "billing_provider" in d["claim"]
        assert "service_lines" in d["claim"]
        assert "raw_segments" in d["claim"]

    def test_hl_parent_error_still_extracts_claim(self, hl_parent_error_bytes):
        """A bad HL should not prevent claim extraction."""
        claims = _parse_file(hl_parent_error_bytes)
        assert len(claims) >= 1
