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

    def test_diagnosis_codes_are_dicts(self, valid_single_bytes):
        """HI now returns list of {qualifier, code} dicts."""
        claims = _parse_file(valid_single_bytes)
        codes = claims[0].claim.diagnosis_codes
        assert len(codes) > 0
        assert "qualifier" in codes[0]
        assert "code" in codes[0]

    def test_diagnosis_bk_qualifier(self, valid_single_bytes):
        claims = _parse_file(valid_single_bytes)
        assert claims[0].claim.diagnosis_codes[0]["qualifier"] == "BK"

    def test_to_dict_includes_new_fields(self, valid_single_bytes):
        d = _parse_file(valid_single_bytes)[0].to_dict()["claim"]
        assert "service_date_from" in d
        assert "service_date_to" in d
        assert "special_program_indicator" in d
        assert "delay_reason_code" in d
        assert "prior_auth_number" in d
        assert "ref_extras" in d
        bp = d["billing_provider"]
        assert "taxonomy" in bp
        sub = d["subscriber"]
        assert "insurance_type" in sub
        assert "claim_filing_indicator" in sub
        for sl in d["service_lines"]:
            assert "modifier2" in sl
            assert "modifier3" in sl
            assert "modifier4" in sl


# ---------------------------------------------------------------------------
# Batch 1 — remediation feature tests
# ---------------------------------------------------------------------------

class TestBatch1Features:
    """Tests for Batch 1 remediation: PRV taxonomy, SBR completeness,
    CLM10-11, REF claim-level, DTP named dates, SV1 modifiers 2-4,
    HI qualifier tagging."""

    @pytest.fixture(autouse=True)
    def _claims(self, batch1_features_bytes):
        self._all = _parse_file(batch1_features_bytes)
        assert len(self._all) == 1
        self._c = self._all[0].claim

    # --- PRV fix: taxonomy must NOT overwrite tax_id ---
    def test_prv_taxonomy_stored_correctly(self):
        assert self._c.billing_provider.taxonomy == "207Q00000X"

    def test_prv_does_not_pollute_tax_id(self):
        # tax_id comes from REF*EI only
        assert self._c.billing_provider.tax_id == "123456789"

    # --- SBR completeness ---
    def test_sbr_insurance_type(self):
        assert self._c.subscriber.insurance_type == "MB"

    def test_sbr_claim_filing_indicator(self):
        assert self._c.subscriber.claim_filing_indicator == "12"

    # --- CLM extensions ---
    def test_clm_special_program_indicator(self):
        assert self._c.special_program_indicator == "09"

    def test_clm_delay_reason_code(self):
        assert self._c.delay_reason_code == "3"

    # --- REF claim-level handlers ---
    def test_ref_prior_auth_number(self):
        assert self._c.prior_auth_number == "PA12345"

    def test_ref_referral_number(self):
        assert self._c.referral_number == "REF98765"

    def test_ref_payer_claim_ctrl_number(self):
        assert self._c.payer_claim_ctrl_number == "PAYERCTRL001"

    def test_ref_medical_record_number(self):
        assert self._c.medical_record_number == "MED100"

    def test_ref_patient_control_number(self):
        assert self._c.patient_control_number == "PATCTRL001"

    def test_ref_extras_captures_unknown_qualifier(self):
        assert "ZZ" in self._c.ref_extras
        assert self._c.ref_extras["ZZ"] == "UNKNOWN_VALUE"

    # --- DTP named dates ---
    def test_dtp_onset_date(self):
        assert self._c.onset_date == "2023-12-15"

    def test_dtp_accident_date(self):
        assert self._c.accident_date == "2023-12-10"

    # --- SV1 modifiers 2-4 ---
    def test_sv1_modifier1(self):
        assert self._c.service_lines[0].modifier == "25"

    def test_sv1_modifier2(self):
        assert self._c.service_lines[0].modifier2 == "52"

    def test_sv1_modifier3(self):
        assert self._c.service_lines[0].modifier3 == "GT"

    def test_sv1_modifier4(self):
        assert self._c.service_lines[0].modifier4 == ""

    # --- HI qualifier tagging ---
    def test_hi_bk_qualifier(self):
        codes = self._c.diagnosis_codes
        bk = [d for d in codes if d["qualifier"] == "BK"]
        assert len(bk) == 1
        assert bk[0]["code"] == "J06.9"

    def test_hi_bf_qualifier(self):
        codes = self._c.diagnosis_codes
        bf = [d for d in codes if d["qualifier"] == "BF"]
        assert len(bf) == 1
        assert bf[0]["code"] == "M79.3"

    # --- service_date split ---
    def test_service_date_from_populated_from_line(self):
        # No claim-level DTP*472; date comes from service line
        # service_date_from should be empty (populated from line dates in UI)
        assert self._c.service_date_from == ""
        assert self._c.service_lines[0].date == "2024-01-01"

    # --- subscriber middle_name propagated ---
    def test_subscriber_middle_name_propagated(self):
        # valid_single has no middle name; batch1 fixture has none either — verify no crash
        assert self._c.subscriber.middle_name == ""


# ---------------------------------------------------------------------------
# Batch 2 — Provider completeness + address completeness tests
# ---------------------------------------------------------------------------

class TestBatch2Features:
    """Tests for Batch 2: pay-to provider, 2310 providers, subscriber address,
    patient address2, PRV taxonomy in 2310 context."""

    @pytest.fixture(autouse=True)
    def _claims(self, batch2_features_bytes):
        self._all = _parse_file(batch2_features_bytes)
        assert len(self._all) == 1
        self._c = self._all[0].claim

    # ── Pay-to Provider (2010AB NM1*87) ─────────────────────────────────
    def test_pay_to_provider_extracted(self):
        assert self._c.pay_to_provider is not None

    def test_pay_to_provider_name(self):
        assert self._c.pay_to_provider.last_name == "BILLING SOLUTIONS INC"

    def test_pay_to_provider_npi(self):
        assert self._c.pay_to_provider.npi == "9876543210"

    def test_pay_to_provider_qualifier(self):
        assert self._c.pay_to_provider.qualifier == "87"

    def test_pay_to_provider_address1(self):
        assert self._c.pay_to_provider.address1 == "200 BILLING AVE"

    def test_pay_to_provider_address2(self):
        assert self._c.pay_to_provider.address2 == "SUITE 300"

    def test_pay_to_provider_city(self):
        assert self._c.pay_to_provider.city == "BILLCITY"

    def test_pay_to_provider_state(self):
        assert self._c.pay_to_provider.state == "CA"

    def test_pay_to_provider_zip(self):
        assert self._c.pay_to_provider.zip_code == "90210"

    # ── Rendering Provider (2310D NM1*82) ───────────────────────────────
    def test_rendering_provider_extracted(self):
        assert self._c.rendering_provider is not None

    def test_rendering_provider_last_name(self):
        assert self._c.rendering_provider.last_name == "JONES"

    def test_rendering_provider_first_name(self):
        assert self._c.rendering_provider.first_name == "ALICE"

    def test_rendering_provider_middle_name(self):
        assert self._c.rendering_provider.middle_name == "M"

    def test_rendering_provider_npi(self):
        assert self._c.rendering_provider.npi == "5678901234"

    def test_rendering_provider_taxonomy(self):
        # PRV*PE*ZZ*208000000X follows NM1*82
        assert self._c.rendering_provider.taxonomy == "208000000X"

    def test_rendering_provider_qualifier(self):
        assert self._c.rendering_provider.qualifier == "82"

    # ── Service Facility (2310E NM1*77) ─────────────────────────────────
    def test_service_facility_extracted(self):
        assert self._c.service_facility is not None

    def test_service_facility_name(self):
        assert self._c.service_facility.last_name == "MAIN CLINIC"

    def test_service_facility_npi(self):
        assert self._c.service_facility.npi == "1122334455"

    def test_service_facility_address1(self):
        assert self._c.service_facility.address1 == "500 CLINIC BLVD"

    def test_service_facility_city(self):
        assert self._c.service_facility.city == "MEDCITY"

    def test_service_facility_state(self):
        assert self._c.service_facility.state == "FL"

    def test_service_facility_zip(self):
        assert self._c.service_facility.zip_code == "33101"

    # ── Referring Provider (2310A NM1*DN) ───────────────────────────────
    def test_referring_provider_extracted(self):
        assert self._c.referring_provider is not None

    def test_referring_provider_last_name(self):
        assert self._c.referring_provider.last_name == "BROWN"

    def test_referring_provider_first_name(self):
        assert self._c.referring_provider.first_name == "WILLIAM"

    def test_referring_provider_npi(self):
        assert self._c.referring_provider.npi == "9988776655"

    def test_referring_provider_qualifier(self):
        assert self._c.referring_provider.qualifier == "DN"

    # ── Subscriber Address (2010BA N3/N4) ───────────────────────────────
    def test_subscriber_address1(self):
        assert self._c.subscriber.address1 == "301 SUBSCRIBER ST"

    def test_subscriber_city(self):
        assert self._c.subscriber.city == "SUBTOWN"

    def test_subscriber_state(self):
        assert self._c.subscriber.state == "NY"

    def test_subscriber_zip(self):
        assert self._c.subscriber.zip_code == "10001"

    def test_subscriber_middle_name(self):
        assert self._c.subscriber.middle_name == "J"

    # ── to_dict includes all new provider fields ─────────────────────────
    def test_to_dict_has_pay_to_provider(self):
        d = self._all[0].to_dict()["claim"]
        assert d["pay_to_provider"] is not None
        assert d["pay_to_provider"]["npi"] == "9876543210"

    def test_to_dict_has_rendering_provider(self):
        d = self._all[0].to_dict()["claim"]
        assert d["rendering_provider"] is not None
        assert d["rendering_provider"]["taxonomy"] == "208000000X"

    def test_to_dict_has_service_facility(self):
        d = self._all[0].to_dict()["claim"]
        assert d["service_facility"] is not None
        assert d["service_facility"]["city"] == "MEDCITY"

    def test_to_dict_has_referring_provider(self):
        d = self._all[0].to_dict()["claim"]
        assert d["referring_provider"] is not None
        assert d["referring_provider"]["last_name"] == "BROWN"

    def test_to_dict_has_subscriber_address(self):
        d = self._all[0].to_dict()["claim"]
        assert d["subscriber"]["address1"] == "301 SUBSCRIBER ST"
        assert d["subscriber"]["state"] == "NY"

    def test_to_dict_absent_providers_are_none(self):
        d = self._all[0].to_dict()["claim"]
        assert d["supervising_provider"] is None
        assert d["ordered_provider"] is None
        assert d["purchased_service_provider"] is None

    # ── No regression: Batch 1 fields still work ────────────────────────
    def test_billing_provider_npi_unchanged(self):
        assert self._c.billing_provider.npi == "1234567890"

    def test_billing_provider_tax_id_unchanged(self):
        assert self._c.billing_provider.tax_id == "123456789"

    def test_service_line_date_unchanged(self):
        assert self._c.service_lines[0].date == "2024-02-01"
