"""
EDI 837P Ingestion System — Streamlit UI

Features:
  1. File upload (.edi / .txt)
  2. Real-time progress bar (per claim processed)
  3. Validation viewer — color-coded table of failed claims
  4. Row click → raw segment + error detail
  5. Search by claim_id, billing_npi, status filter
"""

from __future__ import annotations

import json
import os
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import streamlit as st

# ---------------------------------------------------------------------------
# Path setup — allow running from repo root or ui/ directory
# ---------------------------------------------------------------------------
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.dates import service_date_display
from ingestion.normalizer import normalize_file_content
from ingestion.detector import detect_delimiters
from ingestion.streamer import stream_transactions
from ingestion.integrity import TruncatedFileError, EnvelopeError
from parser.models import FileEnvelope, TransactionEnvelope
from parser.state_machine import EDI837PStateMachine
from validator.snip import SNIPValidator, ValidationResult
from parser.models import CanonicalClaim

# DB import is optional — UI works without a DB connection
try:
    from db.connection import managed_connection, apply_schema
    from db.repository import ClaimRepository
    _DB_AVAILABLE = True
except Exception:
    _DB_AVAILABLE = False

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="EDI 837P Ingestion System",
    page_icon="🏥",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------
if "results" not in st.session_state:
    st.session_state.results = []          # list of (CanonicalClaim, ValidationResult)
if "selected_row" not in st.session_state:
    st.session_state.selected_row = None
if "file_name" not in st.session_state:
    st.session_state.file_name = ""

# ---------------------------------------------------------------------------
# Sidebar — Upload + DB
# ---------------------------------------------------------------------------
st.sidebar.title("EDI 837P System")
st.sidebar.markdown("---")

uploaded = st.sidebar.file_uploader(
    "Upload EDI File",
    type=["edi", "txt"],
    help="ASC X12N 837P (005010X222A1)",
)

persist_to_db = st.sidebar.checkbox(
    "Persist to PostgreSQL",
    value=False,
    disabled=not _DB_AVAILABLE,
    help="Requires PGHOST / PGUSER / PGPASSWORD / PGDATABASE env vars.",
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Search**")
search_claim = st.sidebar.text_input("Claim ID", key="search_claim")
search_npi = st.sidebar.text_input("Billing NPI", key="search_npi")
search_status = st.sidebar.selectbox(
    "Status", ["All", "Pass", "Fail"], key="search_status"
)

# ---------------------------------------------------------------------------
# Main area — title
# ---------------------------------------------------------------------------
st.title("EDI 837P Ingestion & Validation")

# ---------------------------------------------------------------------------
# Processing pipeline
# ---------------------------------------------------------------------------

def _decimal_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def process_file(raw_bytes: bytes, file_name: str) -> list[tuple[CanonicalClaim, ValidationResult]]:
    """Full ingestion → parse → validate pipeline. Returns list of (claim, result)."""
    pairs: list[tuple[CanonicalClaim, ValidationResult]] = []

    content = normalize_file_content(raw_bytes)
    delimiters = detect_delimiters(content)

    transactions = list(stream_transactions(content, delimiters))
    total_tx = len(transactions)

    progress = st.progress(0, text="Starting…")
    status_box = st.empty()
    processed = 0

    for tx in transactions:
        status_box.info(f"Processing transaction ST={tx.st_control_number}…")

        file_env = FileEnvelope(
            file_name=file_name,
            sender_id=tx.sender_id,
            receiver_id=tx.receiver_id,
            isa_control_number=tx.isa_control_number,
            isa_version=tx.isa_version,
            usage_indicator=tx.usage_indicator,
        )
        tx_env = TransactionEnvelope(
            st_control_number=tx.st_control_number,
            gs_control_number=tx.gs_control_number,
            gs_date=tx.gs_date,
            gs_time=tx.gs_time,
            functional_id=tx.functional_id,
        )

        sm = EDI837PStateMachine(
            file_env=file_env,
            tx_env=tx_env,
            element_delimiter=delimiters.element,
            component_delimiter=delimiters.component,
        )
        try:
            claims = sm.parse(tx.segments)
        except Exception as e:
            st.warning(f"ST={tx.st_control_number} parse error: {e} — skipping transaction.")
            continue

        for canonical in claims:
            validator = SNIPValidator(
                parse_errors=sm.parse_errors,
                element_delimiter=delimiters.element,
            )
            result = validator.validate(canonical)
            pairs.append((canonical, result))

        processed += 1
        pct = int(processed / total_tx * 100) if total_tx else 100
        progress.progress(pct, text=f"Processed {processed}/{total_tx} transactions")

    progress.progress(100, text="Complete")
    status_box.success(f"Done — {len(pairs)} claim(s) extracted.")
    return pairs


if uploaded is not None and st.sidebar.button("▶  Process File", type="primary"):
    st.session_state.results = []
    st.session_state.selected_row = None
    st.session_state.file_name = uploaded.name

    raw = uploaded.read()
    try:
        with st.spinner("Ingesting…"):
            pairs = process_file(raw, uploaded.name)
        st.session_state.results = pairs

        if persist_to_db and _DB_AVAILABLE and pairs:
            try:
                with managed_connection() as conn:
                    apply_schema(conn)
                    repo = ClaimRepository(conn)
                    ids = repo.insert_many(pairs, file_name=uploaded.name)
                st.success(f"Persisted {len(ids)} claim(s) to PostgreSQL.")
            except Exception as db_err:
                st.error(f"DB persist failed: {db_err}")

    except TruncatedFileError as e:
        st.error(f"File truncated (missing IEA): {e}")
    except ValueError as e:
        st.error(f"Invalid file format: {e}")
    except Exception as e:
        st.error(f"Unexpected error: {e}")
        raise

# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def _patient_name(canonical: CanonicalClaim) -> str:
    """Last, First [Middle] — falls back to subscriber; '-' if unavailable."""
    p = canonical.claim.patient
    s = canonical.claim.subscriber

    def _fmt(last: str, first: str, middle: str) -> str:
        last  = last.strip().title()
        first = first.strip().title()
        mid   = middle.strip().title()
        name  = f"{last}, {first}".strip(", ").strip()
        return f"{name} {mid}".strip() if mid else name or "-"

    if p and (p.last_name or p.first_name):
        return _fmt(p.last_name, p.first_name, p.middle_name)
    if s.last_name or s.first_name:
        return _fmt(s.last_name, s.first_name, s.middle_name)
    return "-"


def _dos(canonical: CanonicalClaim) -> str:
    """Human-readable Date of Service via utils.dates.service_date_display."""
    c = canonical.claim
    line_dates = [sl.date for sl in c.service_lines if sl.date]
    return service_date_display(c.service_date_from, c.service_date_to, line_dates)


def _mono(val: str) -> str:
    """Render val in a subtle monospace code chip."""
    safe = val.replace("<", "&lt;").replace(">", "&gt;")
    return (
        f'<code style="font-size:0.82rem;background:rgba(255,255,255,0.07);'
        f'padding:2px 6px;border-radius:4px;border:1px solid rgba(255,255,255,0.12)">'
        f'{safe}</code>'
    )


def _status_badge(status: str) -> str:
    if status == "Pass":
        return (
            '<span style="color:#4ade80;font-weight:600;font-size:0.88rem;'
            'letter-spacing:0.02em">✓ Pass</span>'
        )
    return (
        '<span style="color:#f87171;font-weight:600;font-size:0.88rem;'
        'letter-spacing:0.02em">✗ Fail</span>'
    )


def _error_pills(errors: list) -> str:
    err_n  = sum(1 for e in errors if e.severity == "error")
    warn_n = sum(1 for e in errors if e.severity == "warning")
    parts  = []
    if err_n:
        parts.append(
            f'<span style="background:#7f1d1d;color:#fca5a5;border-radius:10px;'
            f'padding:1px 8px;font-size:0.8rem;font-weight:600">{err_n} err</span>'
        )
    if warn_n:
        parts.append(
            f'<span style="background:#78350f;color:#fcd34d;border-radius:10px;'
            f'padding:1px 8px;font-size:0.8rem;font-weight:600">{warn_n} warn</span>'
        )
    return "&nbsp;".join(parts) if parts else '<span style="color:#6b7280;font-size:0.8rem">—</span>'


def _kv(label: str, value: str) -> None:
    """Render a single label: value row in the detail panel."""
    if value:
        st.markdown(
            f'<div style="display:flex;gap:8px;margin:2px 0">'
            f'<span style="color:#9ca3af;font-size:0.82rem;min-width:160px">{label}</span>'
            f'<span style="font-size:0.85rem">{value}</span></div>',
            unsafe_allow_html=True,
        )


def _render_detail(canonical: CanonicalClaim, result: ValidationResult) -> None:
    """Inline detail panel rendered below a row."""
    c = canonical.claim
    tab0, tab1, tab2, tab3 = st.tabs(
        ["Claim Info", "Validation Errors", "Raw Segments", "Full Payload"]
    )

    # ── Tab 0: Claim Info ───────────────────────────────────────────────────
    with tab0:
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("**Claim**")
            _kv("Claim ID", c.claim_id)
            _kv("Total Charge", f"${c.total_charge:,.2f}")
            _kv("Place of Service", c.place_of_service)
            _kv("Frequency Code", c.frequency_code)
            _kv("Release of Info", c.release_info_code)
            _kv("Special Program", c.special_program_indicator)
            _kv("Delay Reason", c.delay_reason_code)

            st.markdown("**Clinical Dates**")
            _kv("Service Date From", c.service_date_from)
            _kv("Service Date To", c.service_date_to)
            _kv("Onset Date", c.onset_date)
            _kv("Accident Date", c.accident_date)

        with col_right:
            st.markdown("**Reference Numbers**")
            _kv("Prior Auth Number", c.prior_auth_number)
            _kv("Referral Number", c.referral_number)
            _kv("Payer Claim Ctrl #", c.payer_claim_ctrl_number)
            _kv("Medical Record #", c.medical_record_number)
            _kv("Patient Control #", c.patient_control_number)
            if c.ref_extras:
                st.markdown("**Other REF Segments**")
                for qual, val in c.ref_extras.items():
                    _kv(f"REF*{qual}", val)

            st.markdown("**Provider**")
            _kv("Billing NPI", c.billing_provider.npi)
            _kv("Tax ID (EIN)", c.billing_provider.tax_id)
            _kv("Taxonomy", c.billing_provider.taxonomy)

            st.markdown("**Coverage**")
            _kv("Insurance Type", c.subscriber.insurance_type)
            _kv("Claim Filing", c.subscriber.claim_filing_indicator)
            _kv("Group Number", c.subscriber.group_number)
            _kv("Payer", c.subscriber.payer_name)

        # Provider section (pay-to, rendering, referring, service facility)
        def _prov_block(label: str, p) -> None:
            if p is None:
                return
            name = " ".join(filter(None, [p.last_name, p.first_name, p.middle_name])).title()
            st.markdown(f"**{label}**")
            _kv("Name", name or "—")
            _kv("NPI", p.npi)
            _kv("Taxonomy", p.taxonomy)
            _kv("Address", " ".join(filter(None, [p.address1, p.address2])))
            _kv("City / State / ZIP", " ".join(filter(None, [p.city, p.state, p.zip_code])))

        prov_cols = st.columns(2)
        with prov_cols[0]:
            _prov_block("Rendering Provider", c.rendering_provider)
            _prov_block("Referring Provider", c.referring_provider)
            _prov_block("Supervising Provider", c.supervising_provider)
        with prov_cols[1]:
            _prov_block("Service Facility", c.service_facility)
            _prov_block("Pay-to Provider", c.pay_to_provider)
            _prov_block("Ordered Provider", c.ordered_provider)
            _prov_block("Purchased Svc Provider", c.purchased_service_provider)

        # Subscriber address (Batch 2)
        if any([c.subscriber.address1, c.subscriber.city]):
            st.markdown("**Subscriber Address**")
            _kv("Address", " ".join(filter(None, [c.subscriber.address1, c.subscriber.address2])))
            _kv("City / State / ZIP", " ".join(filter(None, [c.subscriber.city, c.subscriber.state, c.subscriber.zip_code])))

        # Diagnosis codes
        if c.diagnosis_codes:
            st.markdown("**Diagnosis Codes**")
            diag_cols = st.columns(min(len(c.diagnosis_codes), 5))
            for i, dx in enumerate(c.diagnosis_codes):
                qual = dx.get("qualifier", "")
                code = dx.get("code", "")
                label = "Principal" if qual == "BK" else ("Other" if qual == "BF" else qual)
                diag_cols[i % 5].markdown(
                    f'<div style="text-align:center;padding:4px 8px;'
                    f'background:rgba(255,255,255,0.06);border-radius:6px;margin:2px">'
                    f'<div style="font-size:0.7rem;color:#9ca3af">{label}</div>'
                    f'<div style="font-size:0.9rem;font-weight:600">{code}</div></div>',
                    unsafe_allow_html=True,
                )

        # Service lines summary
        if c.service_lines:
            st.markdown("**Service Lines**")
            for sl in c.service_lines:
                mods = " ".join(m for m in [sl.modifier, sl.modifier2, sl.modifier3, sl.modifier4] if m)
                st.markdown(
                    f'<div style="font-size:0.82rem;margin:2px 0;padding:3px 8px;'
                    f'background:rgba(255,255,255,0.04);border-radius:4px">'
                    f'Line {sl.line_number} &nbsp;·&nbsp; '
                    f'<code>{sl.procedure_code}</code>'
                    + (f' &nbsp;<span style="color:#93c5fd">{mods}</span>' if mods else "") +
                    f' &nbsp;·&nbsp; ${sl.charge:,.2f} &nbsp;·&nbsp; {sl.date or "—"}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    # ── Tab 1: Validation Errors ────────────────────────────────────────────
    with tab1:
        if not result.errors:
            st.success("No validation errors — claim passed all SNIP checks.")
        for err in result.errors:
            icon = "🔴" if err.severity == "error" else "🟡"
            label = f"{icon} L{err.level} &nbsp;·&nbsp; `{err.code}` &nbsp;·&nbsp; {err.message[:90]}"
            with st.expander(label):
                col_a, col_b = st.columns(2)
                col_a.markdown(f"**Loop:** `{err.loop or '—'}`")
                col_b.markdown(f"**Position:** `{err.position}`")
                if err.raw_segment:
                    st.code(err.raw_segment, language="text")

    # ── Tab 2: Raw Segments ─────────────────────────────────────────────────
    with tab2:
        if not c.raw_segments:
            st.info("No raw segments captured.")
        for rs in c.raw_segments:
            st.code(
                f"[loop={rs.loop:<8s}  pos={rs.position:>4d}]  {rs.segment}",
                language="text",
            )

    # ── Tab 3: Full Payload ─────────────────────────────────────────────────
    with tab3:
        payload = canonical.to_dict()
        st.json(json.dumps(payload, default=_decimal_default, indent=2))


# ---------------------------------------------------------------------------
# Results display
# ---------------------------------------------------------------------------

results: list[tuple[CanonicalClaim, ValidationResult]] = st.session_state.results

if results:
    # Apply search filters
    filtered = results
    if search_claim:
        filtered = [(c, r) for c, r in filtered
                    if search_claim.lower() in c.claim.claim_id.lower()]
    if search_npi:
        filtered = [(c, r) for c, r in filtered
                    if search_npi in c.claim.billing_provider.npi]
    if search_status != "All":
        filtered = [(c, r) for c, r in filtered if r.status == search_status]

    st.markdown(f"### Results — {len(filtered)} claim(s)")

    # ClaimID | NPI | PatientName | DateOfService | Charge | Status | Errors | Toggle
    COL_W = [1.5, 1.3, 2.0, 2.0, 1.2, 0.85, 1.4, 0.55]

    # ---- Header ----
    hdr = st.columns(COL_W)
    _HDR = "font-size:0.78rem;font-weight:700;text-transform:uppercase;letter-spacing:0.06em;color:#9ca3af"
    for col, label in zip(hdr, [
        "Claim ID", "Billing NPI", "Patient Name",
        "Date of Service", "Total Charge", "Status", "Errors", "",
    ]):
        col.markdown(f'<span style="{_HDR}">{label}</span>', unsafe_allow_html=True)

    st.markdown(
        '<hr style="margin:4px 0 8px 0;border:none;border-top:1px solid rgba(255,255,255,0.1)">',
        unsafe_allow_html=True,
    )

    # ---- Rows ----
    for idx, (canonical, result) in enumerate(filtered):
        c   = canonical.claim
        row = st.columns(COL_W)

        row[0].markdown(_mono(c.claim_id or "—"), unsafe_allow_html=True)
        row[1].markdown(_mono(c.billing_provider.npi or "—"), unsafe_allow_html=True)
        row[2].markdown(
            f'<span style="font-size:0.9rem">{_patient_name(canonical)}</span>',
            unsafe_allow_html=True,
        )
        row[3].markdown(
            f'<span style="font-size:0.88rem;white-space:nowrap">{_dos(canonical)}</span>',
            unsafe_allow_html=True,
        )
        row[4].markdown(
            f'<span style="font-size:0.9rem;font-variant-numeric:tabular-nums">'
            f'${c.total_charge:,.2f}</span>',
            unsafe_allow_html=True,
        )
        row[5].markdown(_status_badge(result.status), unsafe_allow_html=True)
        row[6].markdown(_error_pills(result.errors), unsafe_allow_html=True)

        # Toggle button — ▶ closed / ▼ open
        is_open = st.session_state.get("selected_row") == idx
        if row[7].button("▼" if is_open else "▶", key=f"toggle_{idx}",
                         help="Open / close claim detail"):
            st.session_state.selected_row = None if is_open else idx

        # ---- Inline detail panel (directly below this row) ----
        if st.session_state.get("selected_row") == idx:
            with st.container():
                st.markdown(
                    '<div style="background:rgba(255,255,255,0.03);border:1px solid '
                    'rgba(255,255,255,0.1);border-radius:8px;padding:12px 16px;margin:6px 0 10px 0">',
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f'<p style="font-size:0.8rem;color:#9ca3af;margin:0 0 8px 0">'
                    f'Claim Detail — <code>{c.claim_id}</code></p>',
                    unsafe_allow_html=True,
                )
                _render_detail(canonical, result)
                st.markdown("</div>", unsafe_allow_html=True)

        st.markdown(
            '<hr style="margin:2px 0;border:none;border-top:1px solid rgba(255,255,255,0.05)">',
            unsafe_allow_html=True,
        )

# ---------------------------------------------------------------------------
# DB search tab (separate from file results)
# ---------------------------------------------------------------------------
if _DB_AVAILABLE and not results:
    st.markdown("### Search Database")
    if st.button("Search"):
        try:
            with managed_connection() as conn:
                repo = ClaimRepository(conn)
                rows = repo.search(
                    claim_id=search_claim or None,
                    billing_npi=search_npi or None,
                    status=search_status if search_status != "All" else None,
                )
            if rows:
                st.dataframe(
                    [
                        {
                            "ID": r["id"],
                            "Claim ID": r["claim_id"],
                            "NPI": r["billing_npi"],
                            "Charge": r["total_charge"],
                            "Status": r["status"],
                            "File": r["file_name"],
                            "Created": str(r["created_at"]),
                        }
                        for r in rows
                    ]
                )
            else:
                st.info("No records found.")
        except Exception as e:
            st.error(f"DB query error: {e}")

elif not _DB_AVAILABLE:
    st.sidebar.caption("⚠️ DB not available — install psycopg2 and set PG* env vars.")
