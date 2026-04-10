"""
Canonical JSON contract dataclasses for EDI 837P claims.
All monetary values use Decimal. Dates are ISO-8601 strings.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Optional


@dataclass
class RawSegment:
    segment: str
    position: int
    loop: str


@dataclass
class Provider:
    """
    Generic provider used for 2010AB (pay-to) and 2310A-E sub-loops.
    Covers rendering, referring, service facility, ordered, purchased-service.
    """
    qualifier: str = ""       # NM101 entity qualifier e.g. "82", "77", "DN"
    entity_type: str = ""     # NM102: "1" = person, "2" = org
    last_name: str = ""       # NM103 — org name when entity_type == "2"
    first_name: str = ""      # NM104
    middle_name: str = ""     # NM105
    npi: str = ""             # NM109 when NM108 == "XX"
    id_qualifier: str = ""    # NM108 (XX = NPI, ZZ = taxonomy, etc.)
    id_code: str = ""         # NM109 (raw; npi is pre-extracted when XX)
    taxonomy: str = ""        # PRV segment taxonomy code
    address1: str = ""        # N3 element 1
    address2: str = ""        # N3 element 2
    city: str = ""            # N4 element 1
    state: str = ""           # N4 element 2
    zip_code: str = ""        # N4 element 3


@dataclass
class BillingProvider:
    npi: str = ""
    entity_type: str = ""
    last_name: str = ""
    first_name: str = ""
    org_name: str = ""
    address1: str = ""
    address2: str = ""
    city: str = ""
    state: str = ""
    zip_code: str = ""
    tax_id: str = ""
    taxonomy: str = ""          # PRV*PE*ZZ*<taxonomy_code>


@dataclass
class Subscriber:
    member_id: str = ""
    last_name: str = ""
    first_name: str = ""
    middle_name: str = ""
    dob: str = ""
    gender: str = ""
    group_number: str = ""
    payer_name: str = ""
    payer_id: str = ""
    relationship_code: str = ""
    insurance_type: str = ""            # SBR08
    claim_filing_indicator: str = ""    # SBR09
    address1: str = ""                  # N3 element 1 (2010BA)
    address2: str = ""                  # N3 element 2 (2010BA)
    city: str = ""                      # N4 element 1 (2010BA)
    state: str = ""                     # N4 element 2 (2010BA)
    zip_code: str = ""                  # N4 element 3 (2010BA)


@dataclass
class Patient:
    last_name: str = ""
    first_name: str = ""
    middle_name: str = ""
    dob: str = ""
    gender: str = ""
    address1: str = ""
    address2: str = ""        # N3 element 2 (was never captured — fixed)
    city: str = ""
    state: str = ""
    zip_code: str = ""
    relationship_code: str = ""


@dataclass
class ServiceLine:
    line_number: int = 0
    procedure_code: str = ""
    modifier: str = ""          # SV1 modifier 1
    modifier2: str = ""         # SV1 modifier 2
    modifier3: str = ""         # SV1 modifier 3
    modifier4: str = ""         # SV1 modifier 4
    charge: Decimal = field(default_factory=lambda: Decimal("0"))
    units: str = ""
    quantity: str = ""
    diagnosis_pointers: list[str] = field(default_factory=list)
    date: str = ""
    place_of_service: str = ""
    ndc: str = ""


@dataclass
class Claim:
    claim_id: str = ""
    total_charge: Decimal = field(default_factory=lambda: Decimal("0"))
    place_of_service: str = ""
    frequency_code: str = ""
    provider_accept_assignment: str = ""
    benefit_assignment: str = ""
    release_info_code: str = ""
    special_program_indicator: str = ""     # CLM10
    delay_reason_code: str = ""             # CLM11

    # Date of service (split ISO dates)
    service_date_from: str = ""
    service_date_to: str = ""

    # Named clinical dates
    onset_date: str = ""                    # DTP*431
    accident_date: str = ""                 # DTP*439

    # Reference numbers
    prior_auth_number: str = ""             # REF*G1
    referral_number: str = ""               # REF*9F
    payer_claim_ctrl_number: str = ""       # REF*F8
    medical_record_number: str = ""         # REF*EA
    patient_control_number: str = ""        # REF*EJ
    ref_extras: dict = field(default_factory=dict)

    # Diagnosis codes: [{"qualifier": "BK"|"BF", "code": "J06.9"}]
    diagnosis_codes: list[dict] = field(default_factory=list)

    # Providers
    billing_provider: BillingProvider = field(default_factory=BillingProvider)
    pay_to_provider: Optional[Provider] = None          # 2010AB NM1*87
    rendering_provider: Optional[Provider] = None       # 2310D NM1*82
    referring_provider: Optional[Provider] = None       # 2310A NM1*DN
    service_facility: Optional[Provider] = None         # 2310E NM1*77
    supervising_provider: Optional[Provider] = None     # 2310F NM1*DQ
    ordered_provider: Optional[Provider] = None         # 2310C NM1*DK
    purchased_service_provider: Optional[Provider] = None  # 2310B NM1*P3

    subscriber: Subscriber = field(default_factory=Subscriber)
    patient: Optional[Patient] = None
    service_lines: list[ServiceLine] = field(default_factory=list)
    raw_segments: list[RawSegment] = field(default_factory=list)


@dataclass
class FileEnvelope:
    file_name: str = ""
    sender_id: str = ""
    receiver_id: str = ""
    isa_control_number: str = ""
    isa_version: str = ""
    usage_indicator: str = ""


@dataclass
class TransactionEnvelope:
    st_control_number: str = ""
    gs_control_number: str = ""
    gs_date: str = ""
    gs_time: str = ""
    functional_id: str = ""


# ---------------------------------------------------------------------------
# Serialization helper
# ---------------------------------------------------------------------------

def _provider_dict(p: Optional[Provider]) -> Optional[dict]:
    if p is None:
        return None
    return {
        "qualifier":    p.qualifier,
        "entity_type":  p.entity_type,
        "last_name":    p.last_name,
        "first_name":   p.first_name,
        "middle_name":  p.middle_name,
        "npi":          p.npi,
        "id_qualifier": p.id_qualifier,
        "id_code":      p.id_code,
        "taxonomy":     p.taxonomy,
        "address1":     p.address1,
        "address2":     p.address2,
        "city":         p.city,
        "state":        p.state,
        "zip_code":     p.zip_code,
    }


@dataclass
class CanonicalClaim:
    """Top-level canonical output for one claim."""
    file: FileEnvelope = field(default_factory=FileEnvelope)
    transaction: TransactionEnvelope = field(default_factory=TransactionEnvelope)
    claim: Claim = field(default_factory=Claim)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a plain dict (Decimal values preserved)."""
        c = self.claim
        return {
            "file": {
                "file_name":          self.file.file_name,
                "sender_id":          self.file.sender_id,
                "receiver_id":        self.file.receiver_id,
                "isa_control_number": self.file.isa_control_number,
                "isa_version":        self.file.isa_version,
                "usage_indicator":    self.file.usage_indicator,
            },
            "transaction": {
                "st_control_number": self.transaction.st_control_number,
                "gs_control_number": self.transaction.gs_control_number,
                "gs_date":           self.transaction.gs_date,
                "gs_time":           self.transaction.gs_time,
                "functional_id":     self.transaction.functional_id,
            },
            "claim": {
                "claim_id":                   c.claim_id,
                "total_charge":               c.total_charge,
                "place_of_service":           c.place_of_service,
                "frequency_code":             c.frequency_code,
                "provider_accept_assignment": c.provider_accept_assignment,
                "benefit_assignment":          c.benefit_assignment,
                "release_info_code":          c.release_info_code,
                "special_program_indicator":  c.special_program_indicator,
                "delay_reason_code":          c.delay_reason_code,
                "service_date_from":          c.service_date_from,
                "service_date_to":            c.service_date_to,
                "onset_date":                 c.onset_date,
                "accident_date":              c.accident_date,
                "prior_auth_number":          c.prior_auth_number,
                "referral_number":            c.referral_number,
                "payer_claim_ctrl_number":    c.payer_claim_ctrl_number,
                "medical_record_number":      c.medical_record_number,
                "patient_control_number":     c.patient_control_number,
                "ref_extras":                 c.ref_extras,
                "diagnosis_codes":            c.diagnosis_codes,
                "billing_provider": {
                    "npi":         c.billing_provider.npi,
                    "entity_type": c.billing_provider.entity_type,
                    "last_name":   c.billing_provider.last_name,
                    "first_name":  c.billing_provider.first_name,
                    "org_name":    c.billing_provider.org_name,
                    "address1":    c.billing_provider.address1,
                    "address2":    c.billing_provider.address2,
                    "city":        c.billing_provider.city,
                    "state":       c.billing_provider.state,
                    "zip_code":    c.billing_provider.zip_code,
                    "tax_id":      c.billing_provider.tax_id,
                    "taxonomy":    c.billing_provider.taxonomy,
                },
                "pay_to_provider":            _provider_dict(c.pay_to_provider),
                "rendering_provider":         _provider_dict(c.rendering_provider),
                "referring_provider":         _provider_dict(c.referring_provider),
                "service_facility":           _provider_dict(c.service_facility),
                "supervising_provider":       _provider_dict(c.supervising_provider),
                "ordered_provider":           _provider_dict(c.ordered_provider),
                "purchased_service_provider": _provider_dict(c.purchased_service_provider),
                "subscriber": {
                    "member_id":             c.subscriber.member_id,
                    "last_name":             c.subscriber.last_name,
                    "first_name":            c.subscriber.first_name,
                    "middle_name":           c.subscriber.middle_name,
                    "dob":                   c.subscriber.dob,
                    "gender":                c.subscriber.gender,
                    "group_number":          c.subscriber.group_number,
                    "payer_name":            c.subscriber.payer_name,
                    "payer_id":              c.subscriber.payer_id,
                    "relationship_code":     c.subscriber.relationship_code,
                    "insurance_type":        c.subscriber.insurance_type,
                    "claim_filing_indicator": c.subscriber.claim_filing_indicator,
                    "address1":              c.subscriber.address1,
                    "address2":              c.subscriber.address2,
                    "city":                  c.subscriber.city,
                    "state":                 c.subscriber.state,
                    "zip_code":              c.subscriber.zip_code,
                },
                "patient": {
                    "last_name":         c.patient.last_name,
                    "first_name":        c.patient.first_name,
                    "middle_name":       c.patient.middle_name,
                    "dob":               c.patient.dob,
                    "gender":            c.patient.gender,
                    "address1":          c.patient.address1,
                    "address2":          c.patient.address2,
                    "city":              c.patient.city,
                    "state":             c.patient.state,
                    "zip_code":          c.patient.zip_code,
                    "relationship_code": c.patient.relationship_code,
                } if c.patient else None,
                "service_lines": [
                    {
                        "line_number":       sl.line_number,
                        "procedure_code":    sl.procedure_code,
                        "modifier":          sl.modifier,
                        "modifier2":         sl.modifier2,
                        "modifier3":         sl.modifier3,
                        "modifier4":         sl.modifier4,
                        "charge":            sl.charge,
                        "units":             sl.units,
                        "quantity":          sl.quantity,
                        "diagnosis_pointers": sl.diagnosis_pointers,
                        "date":              sl.date,
                        "place_of_service":  sl.place_of_service,
                        "ndc":               sl.ndc,
                    }
                    for sl in c.service_lines
                ],
                "raw_segments": [
                    {
                        "segment":  rs.segment,
                        "position": rs.position,
                        "loop":     rs.loop,
                    }
                    for rs in c.raw_segments
                ],
            },
        }
