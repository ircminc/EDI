"""
EDI 837P state machine parser.

Processes a single ST-SE transaction block and produces one or more
:class:`CanonicalClaim` objects — one per CLM segment found.

Loop detection is segment-sequence driven:
  BHT        → HEADER
  NM1*41     → 1000A (Submitter)
  NM1*40     → 1000B (Receiver)
  HL + code20→ 2000A (Billing Provider HL)
  NM1*85     → 2010AA (Billing Provider Name)
  NM1*87     → 2010AB (Pay-to Provider)         [Batch 2: now extracts data]
  HL + code22→ 2000B (Subscriber HL)
  NM1*IL     → 2010BA (Subscriber Name)          [Batch 2: N3/N4 now captured]
  NM1*PR     → 2010BB (Payer Name)
  HL + code23→ 2000C (Patient HL)
  NM1*QC     → 2010CA (Patient Name)
  CLM        → 2300 (Claim)
  NM1*82     → 2310D (Rendering Provider)        [Batch 2: new]
  NM1*77     → 2310E (Service Facility)          [Batch 2: new]
  NM1*DN     → 2310A (Referring Provider)        [Batch 2: new]
  NM1*P3     → 2310B (Purchased Service Prov.)   [Batch 2: new]
  NM1*DK     → 2310C (Ordered Provider)          [Batch 2: new]
  NM1*DQ     → 2310F (Supervising Provider)      [Batch 2: new]
  LX         → 2400 (Service Line)
"""

from __future__ import annotations

import logging
from decimal import Decimal
from enum import Enum
from typing import Optional

from .hl_tracker import HLTracker
from .models import (
    Adjudication,
    Adjustment,
    BillingProvider,
    CanonicalClaim,
    Claim,
    FileEnvelope,
    Patient,
    Provider,
    RawSegment,
    ServiceLine,
    Subscriber,
    TransactionEnvelope,
)
from .segment_mapper import (
    check_illegal_chars,
    map_amt,
    map_cas,
    map_clm,
    map_ctp,
    map_dmg,
    map_dtp,
    map_hi,
    map_lin,
    map_lx,
    map_n3,
    map_n4,
    map_nm1,
    map_nte,
    map_pat,
    map_prv,
    map_ref,
    map_sbr,
    map_sv1,
    map_svd,
)

log = logging.getLogger(__name__)


class Loop(str, Enum):
    HEADER  = "HEADER"
    L1000A  = "1000A"
    L1000B  = "1000B"
    L2000A  = "2000A"
    L2010AA = "2010AA"
    L2010AB = "2010AB"   # Pay-to Provider
    L2000B  = "2000B"
    L2010BA = "2010BA"   # Subscriber Name
    L2010BB = "2010BB"   # Payer Name
    L2000C  = "2000C"
    L2010CA = "2010CA"   # Patient Name
    L2300   = "2300"
    L2310A  = "2310A"    # Referring Provider (DN)
    L2310B  = "2310B"    # Purchased Service Provider (P3)
    L2310C  = "2310C"    # Ordered Provider (DK)
    L2310D  = "2310D"    # Rendering Provider (82)
    L2310E  = "2310E"    # Service Facility (77)
    L2310F  = "2310F"    # Supervising Provider (DQ)
    L2310   = "2310"     # Unknown 2310 sub-loop
    L2400   = "2400"
    L2410   = "2410"     # Drug Identification (LIN/CTP)
    L2420   = "2420"     # Service Line Provider (NM1 in service-line scope)
    L2430   = "2430"     # Line Adjudication (SVD/CAS)
    UNKNOWN = "UNKNOWN"


# Map NM1 qualifier → (loop, Claim attribute name)
_NM1_PROVIDER_MAP: dict[str, tuple[Loop, str]] = {
    "82": (Loop.L2310D, "rendering_provider"),
    "77": (Loop.L2310E, "service_facility"),
    "DN": (Loop.L2310A, "referring_provider"),
    "P3": (Loop.L2310B, "purchased_service_provider"),
    "DK": (Loop.L2310C, "ordered_provider"),
    "DQ": (Loop.L2310F, "supervising_provider"),
}


class EDI837PStateMachine:
    """
    Parse a list of segments from one ST-SE block into canonical claims.

    Usage::

        sm = EDI837PStateMachine(file_envelope, transaction_envelope, delimiters)
        claims = sm.parse(segments)
    """

    def __init__(
        self,
        file_env: FileEnvelope,
        tx_env: TransactionEnvelope,
        element_delimiter: str,
        component_delimiter: str,
    ) -> None:
        self._fe = file_env
        self._te = tx_env
        self._ed = element_delimiter
        self._cd = component_delimiter

        self._loop = Loop.HEADER
        self._hl = HLTracker()

        # ── Billing-provider context (2000A scope — shared across claims) ──
        self._billing_provider = BillingProvider()
        self._pay_to_provider: Optional[Provider] = None   # 2010AB

        # ── Active generic provider pointer ──────────────────────────────
        # Set whenever we enter 2010AB or any 2310 sub-loop; cleared on
        # NM1 qualifiers that have their own dedicated handlers.
        self._active_provider: Optional[Provider] = None

        # ── Per-subscriber context ────────────────────────────────────────
        self._subscriber = Subscriber()

        # ── Buffered patient (2000C data arrives before CLM) ─────────────
        self._pending_patient: Optional[Patient] = None

        # ── Per-claim context ─────────────────────────────────────────────
        self._current_claim: Optional[Claim] = None
        self._current_lx: int = 0
        self._current_sl: Optional[ServiceLine] = None
        self._current_adj: Optional[Adjudication] = None

        # ── Output ────────────────────────────────────────────────────────
        self._claims: list[CanonicalClaim] = []

        # Parse errors accumulated (SNIP picks them up separately)
        self.parse_errors: list[dict] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse(self, segments: list[str]) -> list[CanonicalClaim]:
        """Parse all segments and return list of canonical claims."""
        for pos, seg in enumerate(segments):
            seg = seg.strip()
            if not seg:
                continue
            bad = check_illegal_chars(seg)
            if bad:
                self.parse_errors.append({
                    "level": 1, "severity": "error",
                    "code": "L1-ILLEGAL-CHAR",
                    "message": f"Illegal characters {bad!r} in segment.",
                    "segment": seg[:40], "position": pos,
                })

            els = seg.split(self._ed)
            seg_id = els[0]
            self._dispatch(seg_id, els, seg, pos)

        self._finalize_claim()

        for err in self._hl.errors:
            self.parse_errors.append({
                "level": 2, "severity": "error",
                "code": "L2-HL-HIERARCHY",
                "message": err.message,
                "segment": err.segment[:40],
                "position": err.position,
                "loop": HL_LOOP_MAP.get(err.level_code, "2000x"),
            })

        return self._claims

    # ------------------------------------------------------------------
    # Segment dispatcher
    # ------------------------------------------------------------------

    def _dispatch(self, seg_id: str, els: list[str], raw: str, pos: int) -> None:
        ed, cd = self._ed, self._cd

        if seg_id == "ST":
            self._te.st_control_number = els[2] if len(els) > 2 else ""

        elif seg_id == "BHT":
            self._loop = Loop.HEADER

        elif seg_id == "HL":
            node = self._hl.process(raw, ed, pos)
            lc = node.level_code
            if lc == "20":
                self._finalize_claim()
                self._billing_provider  = BillingProvider()
                self._pay_to_provider   = None
                self._active_provider   = None
                self._loop = Loop.L2000A
            elif lc == "22":
                self._finalize_claim()
                self._subscriber        = Subscriber()
                self._pending_patient   = None
                self._active_provider   = None
                self._loop = Loop.L2000B
            elif lc == "23":
                self._finalize_claim()
                self._pending_patient   = Patient()
                self._active_provider   = None
                self._loop = Loop.L2000C
            else:
                self._loop = Loop.UNKNOWN

        elif seg_id == "PRV":
            data = map_prv(els)
            taxonomy = data.get("taxonomy_code", "")
            if self._loop == Loop.L2000A:
                self._billing_provider.taxonomy = taxonomy
            elif self._active_provider is not None:
                # 2310 provider taxonomy (rendering, referring, etc.)
                self._active_provider.taxonomy = taxonomy

        elif seg_id == "NM1":
            self._handle_nm1(els, raw, pos)

        elif seg_id == "N3":
            data = map_n3(els)
            self._apply_n3(data)

        elif seg_id == "N4":
            data = map_n4(els)
            self._apply_n4(data)

        elif seg_id == "REF":
            data = map_ref(els)
            self._apply_ref(data)

        elif seg_id == "SBR":
            data = map_sbr(els)
            self._subscriber.relationship_code     = data.get("payer_responsibility", "")
            self._subscriber.group_number          = data.get("group_number", "")
            self._subscriber.insurance_type        = data.get("insurance_type", "")
            self._subscriber.claim_filing_indicator = data.get("claim_filing_indicator", "")

        elif seg_id == "DMG":
            data = map_dmg(els)
            self._apply_dmg(data)

        elif seg_id == "PAT":
            data = map_pat(els)
            if self._loop == Loop.L2000C:
                if self._pending_patient is None:
                    self._pending_patient = Patient()
                self._pending_patient.relationship_code = data.get("relationship_code", "")
                if self._current_claim is not None and self._current_claim.patient is None:
                    self._current_claim.patient = self._pending_patient

        elif seg_id == "CLM":
            self._handle_clm(els, raw, pos)

        elif seg_id == "HI":
            if self._current_claim is not None:
                codes = map_hi(els, cd)
                self._current_claim.diagnosis_codes.extend(codes)
                self._record_raw(raw, pos, Loop.L2300)

        elif seg_id == "LX":
            self._finalize_service_line()
            data = map_lx(els)
            self._current_lx = int(data["line_number"]) if data["line_number"].isdigit() else 0
            self._current_sl = ServiceLine(line_number=self._current_lx)
            self._active_provider = None   # service-line scope: clear 2310 context
            self._loop = Loop.L2400
            self._record_raw(raw, pos, Loop.L2400)

        elif seg_id == "LIN":
            if self._current_sl is not None:
                data = map_lin(els)
                if data["qualifier"] == "N4":
                    self._current_sl.ndc = data["product_id"]
                self._loop = Loop.L2410
                self._record_raw(raw, pos, Loop.L2410)

        elif seg_id == "CTP":
            if self._current_sl is not None:
                data = map_ctp(els)
                self._current_sl.ndc_unit_price = data["unit_price"]
                self._current_sl.ndc_quantity   = data["quantity"]
                self._current_sl.ndc_unit       = data["unit"]
                self._record_raw(raw, pos, Loop.L2410)

        elif seg_id == "SV1":
            if self._current_sl is not None:
                data = map_sv1(els, cd)
                self._current_sl.procedure_code    = data["procedure_code"]
                self._current_sl.modifier          = data["modifier1"]
                self._current_sl.modifier2         = data["modifier2"]
                self._current_sl.modifier3         = data["modifier3"]
                self._current_sl.modifier4         = data["modifier4"]
                self._current_sl.charge            = data["charge"]
                self._current_sl.units             = data["unit_basis"]
                self._current_sl.quantity          = data["quantity"]
                self._current_sl.place_of_service  = data["place_of_service"]  # SV105
                self._current_sl.diagnosis_pointers = data["diagnosis_pointers"]
                self._record_raw(raw, pos, Loop.L2400)

        elif seg_id == "NTE":
            data = map_nte(els)
            note = data.get("description", "")
            if note:
                if self._current_sl is not None:
                    self._current_sl.notes.append(note)
                elif self._current_claim is not None:
                    self._current_claim.notes.append(note)
            self._record_raw(raw, pos, self._loop)

        elif seg_id == "SVD":
            if self._current_sl is not None:
                self._finalize_adjudication()
                data = map_svd(els, cd)
                self._current_adj = Adjudication(
                    payer_id       = data["payer_id"],
                    paid_amount    = data["paid_amount"],
                    procedure_code = data["procedure_code"],
                    paid_units     = data["paid_units"],
                )
                self._loop = Loop.L2430
                self._record_raw(raw, pos, Loop.L2430)

        elif seg_id == "CAS":
            if self._current_adj is not None:
                for adj_data in map_cas(els):
                    self._current_adj.adjustments.append(Adjustment(
                        group_code  = adj_data["group_code"],
                        reason_code = adj_data["reason_code"],
                        amount      = adj_data["amount"],
                        quantity    = adj_data["quantity"],
                    ))
                self._record_raw(raw, pos, Loop.L2430)

        elif seg_id == "AMT":
            data = map_amt(els)
            qualifier = data["qualifier"]
            amount    = data["amount"]
            if self._current_sl is not None:
                self._current_sl.amounts[qualifier] = amount
            elif self._current_claim is not None:
                self._current_claim.amounts[qualifier] = amount
            self._record_raw(raw, pos, self._loop)

        elif seg_id == "DTP":
            data      = map_dtp(els)
            qualifier = data["qualifier"]
            date_val  = data["date"]
            if self._current_adj is not None and qualifier == "573":
                # 2430 adjudication payment date
                self._current_adj.paid_date = date_val
            elif self._current_sl is not None and qualifier == "472":
                self._current_sl.date = date_val
            elif self._current_claim is not None and self._current_sl is None:
                if qualifier == "472":
                    if " to " in date_val:
                        start, end = date_val.split(" to ", 1)
                        self._current_claim.service_date_from = start.strip()
                        self._current_claim.service_date_to   = end.strip()
                    elif not self._current_claim.service_date_from:
                        self._current_claim.service_date_from = date_val
                        self._current_claim.service_date_to   = date_val
                elif qualifier == "431":
                    self._current_claim.onset_date = date_val
                elif qualifier == "439":
                    self._current_claim.accident_date = date_val
            self._record_raw(raw, pos, self._loop)

        elif seg_id in ("SE", "GE", "IEA"):
            pass

        else:
            self._record_raw(raw, pos, self._loop)

    # ------------------------------------------------------------------
    # NM1 handler
    # ------------------------------------------------------------------

    def _handle_nm1(self, els: list[str], raw: str, pos: int) -> None:
        qualifier = els[1] if len(els) > 1 else ""
        data = map_nm1(els, self._ed, self._cd)

        if qualifier == "41":
            self._loop = Loop.L1000A
            self._active_provider = None

        elif qualifier == "40":
            self._loop = Loop.L1000B
            self._active_provider = None

        elif qualifier == "85":
            self._loop = Loop.L2010AA
            self._active_provider = None
            self._billing_provider.entity_type = data["entity_type"]
            self._billing_provider.last_name   = data["last_org_name"]
            self._billing_provider.first_name  = data["first_name"]
            self._billing_provider.org_name    = data["last_org_name"]
            self._billing_provider.npi         = data["id_code"]

        elif qualifier == "87":
            # Pay-to Provider (2010AB) — within 2000A scope
            self._loop = Loop.L2010AB
            prov = Provider(
                qualifier    = "87",
                entity_type  = data["entity_type"],
                last_name    = data["last_org_name"],
                first_name   = data["first_name"],
                middle_name  = data["middle_name"],
                id_qualifier = data["id_qualifier"],
                id_code      = data["id_code"],
                npi          = data["id_code"] if data["id_qualifier"] == "XX" else "",
            )
            self._pay_to_provider = prov
            self._active_provider = prov

        elif qualifier == "IL":
            self._loop = Loop.L2010BA
            self._active_provider = None
            self._subscriber.last_name  = data["last_org_name"]
            self._subscriber.first_name = data["first_name"]
            self._subscriber.middle_name = data["middle_name"]
            self._subscriber.member_id  = data["id_code"]

        elif qualifier == "PR":
            self._loop = Loop.L2010BB
            self._active_provider = None
            self._subscriber.payer_name = data["last_org_name"]
            self._subscriber.payer_id   = data["id_code"]

        elif qualifier == "QC":
            self._loop = Loop.L2010CA
            self._active_provider = None
            if self._pending_patient is None:
                self._pending_patient = Patient()
            self._pending_patient.last_name   = data["last_org_name"]
            self._pending_patient.first_name  = data["first_name"]
            self._pending_patient.middle_name = data["middle_name"]
            if self._current_claim is not None:
                if self._current_claim.patient is None:
                    self._current_claim.patient = self._pending_patient
                else:
                    self._current_claim.patient.last_name   = data["last_org_name"]
                    self._current_claim.patient.first_name  = data["first_name"]
                    self._current_claim.patient.middle_name = data["middle_name"]

        elif qualifier in _NM1_PROVIDER_MAP:
            loop_enum, claim_attr = _NM1_PROVIDER_MAP[qualifier]
            prov = Provider(
                qualifier    = qualifier,
                entity_type  = data["entity_type"],
                last_name    = data["last_org_name"],
                first_name   = data["first_name"],
                middle_name  = data["middle_name"],
                id_qualifier = data["id_qualifier"],
                id_code      = data["id_code"],
                npi          = data["id_code"] if data["id_qualifier"] == "XX" else "",
            )
            self._active_provider = prov
            if self._current_sl is not None:
                # 2420 scope: append to service-line providers, not claim-level
                self._current_sl.line_providers.append(prov)
                self._loop = Loop.L2420
            elif self._current_claim is not None:
                # 2310 scope: set on claim
                setattr(self._current_claim, claim_attr, prov)
                self._loop = loop_enum

        else:
            self._loop = Loop.L2310
            self._active_provider = None

        self._record_raw(raw, pos, self._loop)

    # ------------------------------------------------------------------
    # CLM handler
    # ------------------------------------------------------------------

    def _handle_clm(self, els: list[str], raw: str, pos: int) -> None:
        self._finalize_claim()
        data = map_clm(els, self._cd)

        claim = Claim(
            claim_id                  = data["claim_id"],
            total_charge              = data["total_charge"],
            place_of_service          = data["facility_code"],
            frequency_code            = data["claim_frequency"],
            provider_accept_assignment = data["provider_accept_assignment"],
            benefit_assignment        = data["benefit_assignment"],
            release_info_code         = data["release_info_code"],
            special_program_indicator = data.get("special_program_indicator", ""),
            delay_reason_code         = data.get("delay_reason_code", ""),
            billing_provider = BillingProvider(
                npi         = self._billing_provider.npi,
                entity_type = self._billing_provider.entity_type,
                last_name   = self._billing_provider.last_name,
                first_name  = self._billing_provider.first_name,
                org_name    = self._billing_provider.org_name,
                address1    = self._billing_provider.address1,
                address2    = self._billing_provider.address2,
                city        = self._billing_provider.city,
                state       = self._billing_provider.state,
                zip_code    = self._billing_provider.zip_code,
                tax_id      = self._billing_provider.tax_id,
                taxonomy    = self._billing_provider.taxonomy,
            ),
            pay_to_provider = _copy_provider(self._pay_to_provider),
            subscriber = Subscriber(
                member_id              = self._subscriber.member_id,
                last_name              = self._subscriber.last_name,
                first_name             = self._subscriber.first_name,
                middle_name            = self._subscriber.middle_name,
                dob                    = self._subscriber.dob,
                gender                 = self._subscriber.gender,
                group_number           = self._subscriber.group_number,
                payer_name             = self._subscriber.payer_name,
                payer_id               = self._subscriber.payer_id,
                relationship_code      = self._subscriber.relationship_code,
                insurance_type         = self._subscriber.insurance_type,
                claim_filing_indicator = self._subscriber.claim_filing_indicator,
                address1               = self._subscriber.address1,
                address2               = self._subscriber.address2,
                city                   = self._subscriber.city,
                state                  = self._subscriber.state,
                zip_code               = self._subscriber.zip_code,
            ),
        )

        # Attach buffered patient.
        # Do NOT clear _pending_patient here — the 837P spec allows multiple CLM
        # segments within the same HL*23 (patient) scope.  Clearing it here would
        # cause the second and subsequent CLMs in the same scope to lose patient
        # data.  _pending_patient is reset by the HL dispatcher when a new HL*22
        # or HL*23 is encountered.
        if self._pending_patient is not None:
            claim.patient = self._pending_patient

        self._current_claim = claim
        self._active_provider = None   # reset; 2310 providers arrive after CLM
        self._loop = Loop.L2300
        self._record_raw(raw, pos, Loop.L2300)

    # ------------------------------------------------------------------
    # N3 / N4 context appliers
    # ------------------------------------------------------------------

    def _apply_n3(self, data: dict) -> None:
        if self._loop == Loop.L2010AA:
            self._billing_provider.address1 = data.get("address1", "")
            self._billing_provider.address2 = data.get("address2", "")
        elif self._loop == Loop.L2010BA:
            self._subscriber.address1 = data.get("address1", "")
            self._subscriber.address2 = data.get("address2", "")
        elif self._loop == Loop.L2010CA:
            target = (
                self._current_claim.patient
                if self._current_claim and self._current_claim.patient
                else self._pending_patient
            )
            if target:
                target.address1 = data.get("address1", "")
                target.address2 = data.get("address2", "")
        elif self._active_provider is not None:
            # 2010AB pay-to or any 2310 provider sub-loop
            self._active_provider.address1 = data.get("address1", "")
            self._active_provider.address2 = data.get("address2", "")

    def _apply_n4(self, data: dict) -> None:
        if self._loop == Loop.L2010AA:
            self._billing_provider.city    = data.get("city", "")
            self._billing_provider.state   = data.get("state", "")
            self._billing_provider.zip_code = data.get("zip_code", "")
        elif self._loop == Loop.L2010BA:
            self._subscriber.city    = data.get("city", "")
            self._subscriber.state   = data.get("state", "")
            self._subscriber.zip_code = data.get("zip_code", "")
        elif self._loop == Loop.L2010CA:
            # Mirror the same fallback logic as _apply_n3: prefer the claim's
            # patient if already attached, otherwise write to _pending_patient so
            # the data survives until CLM is parsed.
            target = (
                self._current_claim.patient
                if self._current_claim and self._current_claim.patient
                else self._pending_patient
            )
            if target:
                target.city    = data.get("city", "")
                target.state   = data.get("state", "")
                target.zip_code = data.get("zip_code", "")
        elif self._active_provider is not None:
            self._active_provider.city    = data.get("city", "")
            self._active_provider.state   = data.get("state", "")
            self._active_provider.zip_code = data.get("zip_code", "")

    # ------------------------------------------------------------------
    # REF handler
    # ------------------------------------------------------------------

    _CLAIM_REF_MAP = {
        "G1": "prior_auth_number",
        "9F": "referral_number",
        "F8": "payer_claim_ctrl_number",
        "EA": "medical_record_number",
        "EJ": "patient_control_number",
    }

    def _apply_ref(self, data: dict) -> None:
        qualifier = data.get("qualifier", "")
        value     = data.get("value", "")

        if qualifier == "EI" and self._loop == Loop.L2010AA:
            self._billing_provider.tax_id = value
            return

        if qualifier == "G2" and self._loop == Loop.L2010BB:
            self._subscriber.group_number = value
            return

        # 2400 scope: REF belongs to the service line, not the claim
        if self._current_sl is not None:
            self._current_sl.line_refs[qualifier] = value
            return

        if self._current_claim is not None:
            attr = self._CLAIM_REF_MAP.get(qualifier)
            if attr:
                setattr(self._current_claim, attr, value)
            else:
                self._current_claim.ref_extras[qualifier] = value

    # ------------------------------------------------------------------
    # DMG handler
    # ------------------------------------------------------------------

    def _apply_dmg(self, data: dict) -> None:
        if self._loop == Loop.L2010BA:
            self._subscriber.dob    = data.get("dob", "")
            self._subscriber.gender = data.get("gender", "")
        elif self._loop == Loop.L2010CA:
            target = (
                self._current_claim.patient
                if self._current_claim and self._current_claim.patient
                else self._pending_patient
            )
            if target:
                target.dob    = data.get("dob", "")
                target.gender = data.get("gender", "")

    # ------------------------------------------------------------------
    # Service line finalization
    # ------------------------------------------------------------------

    def _finalize_adjudication(self) -> None:
        if self._current_adj is not None and self._current_sl is not None:
            self._current_sl.adjudications.append(self._current_adj)
            self._current_adj = None

    def _finalize_service_line(self) -> None:
        self._finalize_adjudication()
        if self._current_sl is not None and self._current_claim is not None:
            self._current_claim.service_lines.append(self._current_sl)
            self._current_sl = None

    def _finalize_claim(self) -> None:
        self._finalize_service_line()
        if self._current_claim is not None:
            canonical = CanonicalClaim(
                file=self._fe,
                transaction=self._te,
                claim=self._current_claim,
            )
            self._claims.append(canonical)
            self._current_claim = None

    # ------------------------------------------------------------------
    # Raw segment recorder
    # ------------------------------------------------------------------

    def _record_raw(self, raw: str, pos: int, loop: Loop) -> None:
        if self._current_claim is not None:
            self._current_claim.raw_segments.append(
                RawSegment(segment=raw, position=pos, loop=loop.value)
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _copy_provider(src: Optional[Provider]) -> Optional[Provider]:
    """Shallow-copy a Provider dataclass (all fields are str — safe)."""
    if src is None:
        return None
    return Provider(
        qualifier    = src.qualifier,
        entity_type  = src.entity_type,
        last_name    = src.last_name,
        first_name   = src.first_name,
        middle_name  = src.middle_name,
        npi          = src.npi,
        id_qualifier = src.id_qualifier,
        id_code      = src.id_code,
        taxonomy     = src.taxonomy,
        address1     = src.address1,
        address2     = src.address2,
        city         = src.city,
        state        = src.state,
        zip_code     = src.zip_code,
    )


HL_LOOP_MAP = {"20": "2000A", "22": "2000B", "23": "2000C"}
