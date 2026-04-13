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
from parser.segment_mapper import (
    to_date, to_decimal, map_sv1, map_clm, map_nm1,
    map_lin, map_ctp, map_svd, map_cas, map_amt, map_nte,
)


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


# ---------------------------------------------------------------------------
# Batch 3 — NDC, service-line providers, adjudication, AMT, REF routing
# ---------------------------------------------------------------------------

def _parse_b3(raw: bytes):
    """Parse batch3_features.edi and return (claims list, first claim)."""
    all_claims = _parse_file(raw)
    return all_claims, all_claims[0].claim


class TestBatch3SegmentMappers:
    """Unit tests for Batch 3 segment mapper functions."""

    def test_map_lin_ndc(self):
        els = "LIN*1*N4*12345678901".split("*")
        result = map_lin(els)
        assert result["qualifier"] == "N4"
        assert result["product_id"] == "12345678901"

    def test_map_lin_assigned_number(self):
        els = "LIN*2*N4*99887766554".split("*")
        result = map_lin(els)
        assert result["assigned_number"] == "2"

    def test_map_ctp_unit_price(self):
        els = "CTP***0*3*ML".split("*")
        result = map_ctp(els)
        assert result["unit_price"] == Decimal("0")   # CTP03 = 0
        assert result["quantity"] == "3"
        assert result["unit"] == "ML"

    def test_map_ctp_with_price(self):
        els = "CTP***5.50*10*ML".split("*")
        result = map_ctp(els)
        assert result["unit_price"] == Decimal("5.50")
        assert result["quantity"] == "10"
        assert result["unit"] == "ML"

    def test_map_svd_basic(self):
        els = "SVD*BC001*350*HC:99213**1".split("*")
        result = map_svd(els, ":")
        assert result["payer_id"] == "BC001"
        assert result["paid_amount"] == Decimal("350")
        assert result["procedure_code"] == "99213"
        assert result["paid_units"] == "1"

    def test_map_cas_single(self):
        els = "CAS*CO*45*150".split("*")
        result = map_cas(els)
        assert len(result) == 1
        assert result[0]["group_code"] == "CO"
        assert result[0]["reason_code"] == "45"
        assert result[0]["amount"] == Decimal("150")

    def test_map_cas_multiple_triplets(self):
        els = "CAS*PR*1*50**2*75".split("*")
        result = map_cas(els)
        assert len(result) == 2
        assert result[0]["reason_code"] == "1"
        assert result[0]["amount"] == Decimal("50")
        assert result[1]["reason_code"] == "2"
        assert result[1]["amount"] == Decimal("75")

    def test_map_cas_empty_stops(self):
        els = "CAS*CO*45*100".split("*")
        result = map_cas(els)
        assert len(result) == 1

    def test_map_amt_basic(self):
        els = "AMT*F3*50".split("*")
        result = map_amt(els)
        assert result["qualifier"] == "F3"
        assert result["amount"] == Decimal("50")

    def test_map_amt_zero(self):
        els = "AMT*A8*0".split("*")
        result = map_amt(els)
        assert result["amount"] == Decimal("0")


class TestBatch3Features:
    """Integration tests using batch3_features.edi."""

    @pytest.fixture(autouse=True)
    def _setup(self, batch3_features_bytes):
        self._all, self._c = _parse_b3(batch3_features_bytes)

    # ── Claim-level AMT ─────────────────────────────────────────────────

    def test_claim_amt_f3_patient_paid(self):
        assert "F3" in self._c.amounts
        assert self._c.amounts["F3"] == Decimal("50")

    def test_claim_amt_not_on_service_line(self):
        sl = self._c.service_lines[0]
        assert "F3" not in sl.amounts

    # ── NDC (2410 LIN/CTP) ──────────────────────────────────────────────

    def test_ndc_populated(self):
        sl = self._c.service_lines[0]
        assert sl.ndc == "12345678901"

    def test_ndc_quantity(self):
        sl = self._c.service_lines[0]
        assert sl.ndc_quantity == "3"

    def test_ndc_unit(self):
        sl = self._c.service_lines[0]
        assert sl.ndc_unit == "ML"

    # ── Service-line REF routing (2400 scope) ───────────────────────────

    def test_sl_ref_stored_on_service_line(self):
        sl = self._c.service_lines[0]
        assert sl.line_refs.get("6R") == "SL-REF-001"

    def test_sl_ref_not_on_claim(self):
        assert self._c.referral_number == ""
        assert "6R" not in self._c.ref_extras

    # ── Service-line providers (2420) ───────────────────────────────────

    def test_line_provider_present(self):
        sl = self._c.service_lines[0]
        assert len(sl.line_providers) == 1

    def test_line_provider_qualifier(self):
        sl = self._c.service_lines[0]
        assert sl.line_providers[0].qualifier == "82"

    def test_line_provider_npi(self):
        sl = self._c.service_lines[0]
        assert sl.line_providers[0].npi == "5678901234"

    def test_line_provider_last_name(self):
        sl = self._c.service_lines[0]
        assert sl.line_providers[0].last_name == "JONES"

    def test_claim_rendering_not_overwritten(self):
        # NM1*82 after LX must NOT overwrite claim.rendering_provider
        assert self._c.rendering_provider is None

    # ── Adjudication (2430 SVD/CAS/DTP) ─────────────────────────────────

    def test_adjudication_count(self):
        sl = self._c.service_lines[0]
        assert len(sl.adjudications) == 1

    def test_adjudication_payer_id(self):
        adj = self._c.service_lines[0].adjudications[0]
        assert adj.payer_id == "BC001"

    def test_adjudication_paid_amount(self):
        adj = self._c.service_lines[0].adjudications[0]
        assert adj.paid_amount == Decimal("350")

    def test_adjudication_procedure_code(self):
        adj = self._c.service_lines[0].adjudications[0]
        assert adj.procedure_code == "99213"

    def test_adjudication_paid_units(self):
        adj = self._c.service_lines[0].adjudications[0]
        assert adj.paid_units == "1"

    def test_adjudication_paid_date(self):
        adj = self._c.service_lines[0].adjudications[0]
        assert adj.paid_date == "2024-03-15"

    def test_adjudication_adjustment_count(self):
        adj = self._c.service_lines[0].adjudications[0]
        assert len(adj.adjustments) == 1

    def test_adjustment_group_code(self):
        a = self._c.service_lines[0].adjudications[0].adjustments[0]
        assert a.group_code == "CO"

    def test_adjustment_reason_code(self):
        a = self._c.service_lines[0].adjudications[0].adjustments[0]
        assert a.reason_code == "45"

    def test_adjustment_amount(self):
        a = self._c.service_lines[0].adjudications[0].adjustments[0]
        assert a.amount == Decimal("150")

    # ── to_dict completeness ─────────────────────────────────────────────

    def test_to_dict_amounts_in_claim(self):
        d = self._all[0].to_dict()["claim"]
        assert d["amounts"]["F3"] == Decimal("50")

    def test_to_dict_ndc_in_service_line(self):
        d = self._all[0].to_dict()["claim"]
        sl = d["service_lines"][0]
        assert sl["ndc"] == "12345678901"
        assert sl["ndc_unit"] == "ML"

    def test_to_dict_line_refs(self):
        d = self._all[0].to_dict()["claim"]
        assert d["service_lines"][0]["line_refs"]["6R"] == "SL-REF-001"

    def test_to_dict_line_providers(self):
        d = self._all[0].to_dict()["claim"]
        assert len(d["service_lines"][0]["line_providers"]) == 1
        assert d["service_lines"][0]["line_providers"][0]["npi"] == "5678901234"

    def test_to_dict_adjudications(self):
        d = self._all[0].to_dict()["claim"]
        adjs = d["service_lines"][0]["adjudications"]
        assert len(adjs) == 1
        assert adjs[0]["paid_amount"] == Decimal("350")
        assert adjs[0]["adjustments"][0]["group_code"] == "CO"

    # ── No regression: Batch 1 + 2 fields still work ────────────────────

    def test_diagnosis_codes_present(self):
        assert any(d["code"] == "J06.9" for d in self._c.diagnosis_codes)

    def test_service_line_procedure_code(self):
        assert self._c.service_lines[0].procedure_code == "99213"

    def test_service_line_charge(self):
        assert self._c.service_lines[0].charge == Decimal("600")


# ---------------------------------------------------------------------------
# Batch 4.1 — SV105 place_of_service + NTE notes
# ---------------------------------------------------------------------------

class TestBatch4SegmentMappers:
    """Unit tests for Batch 4.1 segment mapper additions."""

    def test_map_sv1_place_of_service_extracted(self):
        els = "SV1*HC:99213*200*UN*1*11**1".split("*")
        result = map_sv1(els, ":")
        assert result["place_of_service"] == "11"

    def test_map_sv1_place_of_service_empty(self):
        # Original format without SV105
        els = "SV1*HC:99213*100*UN*1***1".split("*")
        result = map_sv1(els, ":")
        assert result["place_of_service"] == ""

    def test_map_sv1_procedure_code_unchanged(self):
        els = "SV1*HC:99213*200*UN*1*11**1".split("*")
        result = map_sv1(els, ":")
        assert result["procedure_code"] == "99213"
        assert result["charge"] == Decimal("200")

    def test_map_sv1_diag_pointer_still_extracted(self):
        els = "SV1*HC:99213*200*UN*1*11**1".split("*")
        result = map_sv1(els, ":")
        assert result["diagnosis_pointers"] == ["1"]

    def test_map_nte_basic(self):
        els = "NTE*ADD*PRIOR AUTH ON FILE".split("*")
        result = map_nte(els)
        assert result["note_reference_code"] == "ADD"
        assert result["description"] == "PRIOR AUTH ON FILE"

    def test_map_nte_tpo_code(self):
        els = "NTE*TPO*THIRD PARTY ORG NOTE".split("*")
        result = map_nte(els)
        assert result["note_reference_code"] == "TPO"
        assert result["description"] == "THIRD PARTY ORG NOTE"

    def test_map_nte_empty_description(self):
        els = "NTE*ADD".split("*")
        result = map_nte(els)
        assert result["description"] == ""


class TestBatch4Features:
    """Integration tests for NTE notes and SV105 place_of_service."""

    @pytest.fixture(autouse=True)
    def _setup(self, batch4_features_bytes):
        self._all = _parse_file(batch4_features_bytes)
        self._c = self._all[0].claim

    # ── SV105 place_of_service ───────────────────────────────────────────

    def test_sl_place_of_service_populated(self):
        assert self._c.service_lines[0].place_of_service == "11"

    def test_sl_place_of_service_in_to_dict(self):
        d = self._all[0].to_dict()["claim"]
        assert d["service_lines"][0]["place_of_service"] == "11"

    # ── Claim-level NTE notes ────────────────────────────────────────────

    def test_claim_notes_present(self):
        assert len(self._c.notes) == 1

    def test_claim_note_text(self):
        assert self._c.notes[0] == "PRIOR AUTH ON FILE"

    def test_claim_notes_in_to_dict(self):
        d = self._all[0].to_dict()["claim"]
        assert d["notes"] == ["PRIOR AUTH ON FILE"]

    # ── Service-line NTE notes ───────────────────────────────────────────

    def test_sl_notes_present(self):
        sl = self._c.service_lines[0]
        assert len(sl.notes) == 1

    def test_sl_note_text(self):
        sl = self._c.service_lines[0]
        assert sl.notes[0] == "OFFICE VISIT WITH PNEUMONIA"

    def test_sl_notes_in_to_dict(self):
        d = self._all[0].to_dict()["claim"]
        assert d["service_lines"][0]["notes"] == ["OFFICE VISIT WITH PNEUMONIA"]

    # ── Claim note does NOT land in service line and vice versa ──────────

    def test_claim_note_not_on_service_line(self):
        sl = self._c.service_lines[0]
        assert "PRIOR AUTH ON FILE" not in sl.notes

    def test_sl_note_not_on_claim(self):
        assert "OFFICE VISIT WITH PNEUMONIA" not in self._c.notes

    # ── Regression: existing fields still work ───────────────────────────

    def test_diagnosis_codes_present(self):
        codes = [d["code"] for d in self._c.diagnosis_codes]
        assert "J18.9" in codes

    def test_service_line_charge(self):
        assert self._c.service_lines[0].charge == Decimal("200")

    def test_billing_npi_present(self):
        assert self._c.billing_provider.npi == "1234567890"
