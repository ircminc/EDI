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
from decimal import Decimal

from parser.models import (
    BillingProvider, CanonicalClaim, Claim, FileEnvelope,
    ServiceLine, TransactionEnvelope,
)
from validator.rules import (
    check_billing_provider_nm1,
    check_claim_balance,
    check_diagnosis_pointers,
    check_diagnosis_present,
    check_dos_present,
    check_dos_valid,
    check_illegal_characters,
    check_invalid_segments,
    check_npi_format,
    check_service_lines_present,
    check_total_charge_nonzero,
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


# ---------------------------------------------------------------------------
# Batch 4.1 — Helper to build minimal CanonicalClaim from dataclasses
# ---------------------------------------------------------------------------

def _make_canonical(
    claim_id: str = "TEST001",
    total_charge: Decimal = Decimal("100"),
    service_date_from: str = "2024-01-01",
    service_date_to: str = "2024-01-01",
    npi: str = "1234567890",
    diagnosis_codes=None,
    service_lines=None,
) -> CanonicalClaim:
    """Build a minimal valid CanonicalClaim with optional overrides."""
    if diagnosis_codes is None:
        diagnosis_codes = [{"qualifier": "BK", "code": "Z0000"}]
    if service_lines is None:
        service_lines = [ServiceLine(
            line_number=1,
            procedure_code="99213",
            charge=total_charge,
            date=service_date_from,
            diagnosis_pointers=["1"],
        )]
    claim = Claim(
        claim_id=claim_id,
        total_charge=total_charge,
        service_date_from=service_date_from,
        service_date_to=service_date_to,
        billing_provider=BillingProvider(npi=npi),
        diagnosis_codes=diagnosis_codes,
        service_lines=service_lines,
    )
    return CanonicalClaim(
        file=FileEnvelope(file_name="test.edi"),
        transaction=TransactionEnvelope(),
        claim=claim,
    )


# ---------------------------------------------------------------------------
# Batch 4.1 — L2 new rule unit tests
# ---------------------------------------------------------------------------

class TestL2DiagnosisPresent:
    def test_no_diagnosis_fires(self):
        canon = _make_canonical(diagnosis_codes=[])
        errors = check_diagnosis_present(canon, [])
        assert len(errors) == 1
        assert errors[0]["code"] == "L2-MISSING-HI"
        assert errors[0]["level"] == 2
        assert errors[0]["severity"] == "error"

    def test_with_diagnosis_passes(self):
        canon = _make_canonical()
        errors = check_diagnosis_present(canon, [])
        assert errors == []

    def test_integration_missing_hi_fails(self, missing_hi_bytes):
        pairs = _validate_file(missing_hi_bytes)
        codes = [e.code for e in pairs[0][1].errors]
        assert "L2-MISSING-HI" in codes
        assert pairs[0][1].status == "Fail"

    def test_integration_valid_has_no_missing_hi(self, valid_single_bytes):
        pairs = _validate_file(valid_single_bytes)
        codes = [e.code for e in pairs[0][1].errors]
        assert "L2-MISSING-HI" not in codes


class TestL2DOSPresent:
    def test_no_dos_fires_when_both_empty(self):
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="",
                         diagnosis_pointers=["1"])
        canon = _make_canonical(service_date_from="", service_date_to="",
                                service_lines=[sl])
        errors = check_dos_present(canon, [])
        assert len(errors) == 1
        assert errors[0]["code"] == "L2-MISSING-DOS"
        assert errors[0]["level"] == 2

    def test_claim_level_dos_passes(self):
        canon = _make_canonical(service_date_from="2024-01-01")
        errors = check_dos_present(canon, [])
        assert errors == []

    def test_line_level_dos_passes(self):
        # service_date_from empty but line has date
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="2024-01-01",
                         diagnosis_pointers=["1"])
        canon = _make_canonical(service_date_from="", service_lines=[sl])
        errors = check_dos_present(canon, [])
        assert errors == []

    def test_integration_missing_dos_fails(self, missing_dos_bytes):
        pairs = _validate_file(missing_dos_bytes)
        codes = [e.code for e in pairs[0][1].errors]
        assert "L2-MISSING-DOS" in codes
        assert pairs[0][1].status == "Fail"

    def test_integration_valid_has_no_missing_dos(self, valid_single_bytes):
        pairs = _validate_file(valid_single_bytes)
        codes = [e.code for e in pairs[0][1].errors]
        assert "L2-MISSING-DOS" not in codes


class TestL2ServiceLinesPresent:
    def test_no_service_lines_fires(self):
        canon = _make_canonical(service_lines=[])
        errors = check_service_lines_present(canon, [])
        assert len(errors) == 1
        assert errors[0]["code"] == "L2-MISSING-SV1"
        assert errors[0]["level"] == 2

    def test_with_service_lines_passes(self):
        canon = _make_canonical()
        errors = check_service_lines_present(canon, [])
        assert errors == []

    def test_integration_missing_sv1_fails(self, missing_sv1_bytes):
        pairs = _validate_file(missing_sv1_bytes)
        codes = [e.code for e in pairs[0][1].errors]
        assert "L2-MISSING-SV1" in codes
        assert pairs[0][1].status == "Fail"

    def test_integration_valid_has_no_missing_sv1(self, valid_single_bytes):
        pairs = _validate_file(valid_single_bytes)
        codes = [e.code for e in pairs[0][1].errors]
        assert "L2-MISSING-SV1" not in codes


# ---------------------------------------------------------------------------
# Batch 4.1 — L3 new rule unit tests
# ---------------------------------------------------------------------------

class TestL3NPIFormat:
    def test_valid_10_digit_npi_passes(self):
        canon = _make_canonical(npi="1234567890")
        errors = check_npi_format(canon)
        assert errors == []

    def test_short_npi_fires(self):
        canon = _make_canonical(npi="12345")
        errors = check_npi_format(canon)
        assert len(errors) == 1
        assert errors[0]["code"] == "L3-NPI-FORMAT"
        assert errors[0]["level"] == 3

    def test_11_digit_npi_fires(self):
        canon = _make_canonical(npi="12345678901")
        errors = check_npi_format(canon)
        assert len(errors) == 1
        assert errors[0]["code"] == "L3-NPI-FORMAT"

    def test_alpha_npi_fires(self):
        canon = _make_canonical(npi="ABCD123456")
        errors = check_npi_format(canon)
        assert len(errors) == 1
        assert errors[0]["code"] == "L3-NPI-FORMAT"

    def test_empty_npi_skipped(self):
        # Empty NPI → already caught by L2-MISSING-NM185; L3 must not double-fire
        canon = _make_canonical(npi="")
        errors = check_npi_format(canon)
        assert errors == []

    def test_integration_valid_npi_no_error(self, valid_single_bytes):
        pairs = _validate_file(valid_single_bytes)
        codes = [e.code for e in pairs[0][1].errors]
        assert "L3-NPI-FORMAT" not in codes


class TestL3ZeroCharge:
    def test_positive_charge_passes(self):
        canon = _make_canonical(total_charge=Decimal("150"))
        errors = check_total_charge_nonzero(canon)
        assert errors == []

    def test_zero_charge_fires(self):
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("0"), date="2024-01-01",
                         diagnosis_pointers=["1"])
        canon = _make_canonical(total_charge=Decimal("0"), service_lines=[sl])
        errors = check_total_charge_nonzero(canon)
        assert len(errors) == 1
        assert errors[0]["code"] == "L3-ZERO-CHARGE"
        assert errors[0]["level"] == 3

    def test_negative_charge_fires(self):
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("-50"), date="2024-01-01",
                         diagnosis_pointers=["1"])
        canon = _make_canonical(total_charge=Decimal("-50"), service_lines=[sl])
        errors = check_total_charge_nonzero(canon)
        assert len(errors) == 1
        assert errors[0]["code"] == "L3-ZERO-CHARGE"

    def test_integration_valid_charge_no_error(self, valid_single_bytes):
        pairs = _validate_file(valid_single_bytes)
        codes = [e.code for e in pairs[0][1].errors]
        assert "L3-ZERO-CHARGE" not in codes


class TestL3DOSValid:
    def test_valid_date_passes(self):
        canon = _make_canonical(service_date_from="2024-01-15")
        errors = check_dos_valid(canon)
        assert errors == []

    def test_invalid_month_fires(self):
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="2024-13-01",
                         diagnosis_pointers=["1"])
        canon = _make_canonical(service_date_from="", service_lines=[sl])
        errors = check_dos_valid(canon)
        assert len(errors) == 1
        assert errors[0]["code"] == "L3-DOS-INVALID"
        assert errors[0]["level"] == 3

    def test_invalid_day_fires(self):
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="2024-02-30",
                         diagnosis_pointers=["1"])
        canon = _make_canonical(service_date_from="", service_lines=[sl])
        errors = check_dos_valid(canon)
        assert len(errors) == 1
        assert errors[0]["code"] == "L3-DOS-INVALID"

    def test_bad_format_fires(self):
        # Claim-level DOS is a non-ISO string; service line has a valid date
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="2024-01-01",
                         diagnosis_pointers=["1"])
        canon = _make_canonical(service_date_from="20240101", service_lines=[sl])
        errors = check_dos_valid(canon)
        assert len(errors) == 1
        assert errors[0]["code"] == "L3-DOS-INVALID"

    def test_empty_dos_skipped(self):
        # No DOS set at claim level and no line dates — already caught by L2
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="",
                         diagnosis_pointers=["1"])
        canon = _make_canonical(service_date_from="", service_lines=[sl])
        errors = check_dos_valid(canon)
        assert errors == []

    def test_range_date_skipped(self):
        # "2024-01-01 to 2024-01-31" contains " to " — skip format check
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="2024-01-01 to 2024-01-31",
                         diagnosis_pointers=["1"])
        canon = _make_canonical(service_date_from="", service_lines=[sl])
        errors = check_dos_valid(canon)
        assert errors == []

    def test_integration_valid_dos_no_error(self, valid_single_bytes):
        pairs = _validate_file(valid_single_bytes)
        codes = [e.code for e in pairs[0][1].errors]
        assert "L3-DOS-INVALID" not in codes


class TestL3DiagPointers:
    def test_valid_pointer_passes(self):
        # 1 diagnosis, pointer "1" → valid
        canon = _make_canonical()
        errors = check_diagnosis_pointers(canon)
        assert errors == []

    def test_pointer_in_range_passes(self):
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="2024-01-01",
                         diagnosis_pointers=["1", "2"])
        canon = _make_canonical(
            diagnosis_codes=[
                {"qualifier": "BK", "code": "Z0000"},
                {"qualifier": "BF", "code": "J06.9"},
            ],
            service_lines=[sl],
        )
        errors = check_diagnosis_pointers(canon)
        assert errors == []

    def test_pointer_out_of_range_fires(self):
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="2024-01-01",
                         diagnosis_pointers=["5"])  # only 1 diagnosis
        canon = _make_canonical(service_lines=[sl])
        errors = check_diagnosis_pointers(canon)
        assert len(errors) == 1
        assert errors[0]["code"] == "L3-DIAG-PTR"
        assert errors[0]["level"] == 3
        assert "5" in errors[0]["message"]

    def test_pointer_zero_fires(self):
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="2024-01-01",
                         diagnosis_pointers=["0"])  # 0 is invalid (1-based)
        canon = _make_canonical(service_lines=[sl])
        errors = check_diagnosis_pointers(canon)
        assert len(errors) == 1
        assert errors[0]["code"] == "L3-DIAG-PTR"

    def test_non_numeric_pointer_skipped(self):
        # Alphabetic pointers (some trading partners) are not validated
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="2024-01-01",
                         diagnosis_pointers=["A"])
        canon = _make_canonical(service_lines=[sl])
        errors = check_diagnosis_pointers(canon)
        assert errors == []

    def test_no_pointers_passes(self):
        sl = ServiceLine(line_number=1, procedure_code="99213",
                         charge=Decimal("100"), date="2024-01-01",
                         diagnosis_pointers=[])
        canon = _make_canonical(service_lines=[sl])
        errors = check_diagnosis_pointers(canon)
        assert errors == []

    def test_integration_valid_pointers_no_error(self, valid_single_bytes):
        pairs = _validate_file(valid_single_bytes)
        codes = [e.code for e in pairs[0][1].errors]
        assert "L3-DIAG-PTR" not in codes


# ---------------------------------------------------------------------------
# Batch 4.1 — Integration: all 7 rules pass on valid claims
# ---------------------------------------------------------------------------

class TestBatch41AllRulesPass:
    def test_valid_single_passes_all_new_rules(self, valid_single_bytes):
        pairs = _validate_file(valid_single_bytes)
        result = pairs[0][1]
        new_codes = {
            "L2-MISSING-HI", "L2-MISSING-DOS", "L2-MISSING-SV1",
            "L3-NPI-FORMAT", "L3-ZERO-CHARGE", "L3-DOS-INVALID", "L3-DIAG-PTR",
        }
        fired = {e.code for e in result.errors}
        assert fired.isdisjoint(new_codes), f"Unexpected new-rule errors: {fired & new_codes}"

    def test_batch4_features_passes_all_rules(self, batch4_features_bytes):
        pairs = _validate_file(batch4_features_bytes)
        result = pairs[0][1]
        assert result.status == "Pass", f"Unexpected failures: {[e.code for e in result.errors]}"

    def test_batch4_features_no_new_rule_errors(self, batch4_features_bytes):
        pairs = _validate_file(batch4_features_bytes)
        new_codes = {
            "L2-MISSING-HI", "L2-MISSING-DOS", "L2-MISSING-SV1",
            "L3-NPI-FORMAT", "L3-ZERO-CHARGE", "L3-DOS-INVALID", "L3-DIAG-PTR",
        }
        fired = {e.code for e in pairs[0][1].errors}
        assert fired.isdisjoint(new_codes)
