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

import hashlib
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
# Authentication gate
# ---------------------------------------------------------------------------
# SHA-256 of the access password — never store the plaintext.
_ACCESS_HASH = "659439c7eb3369a7c308dfdca809f08c5aea6b44e12ac508f4660ad3c39ff648"


def _verify_password(pwd: str) -> bool:
    return hashlib.sha256(pwd.encode()).hexdigest() == _ACCESS_HASH


if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.markdown(
        """
        <style>
        /* Hide the default sidebar and header while on the login screen */
        [data-testid="stSidebar"] { display: none; }
        [data-testid="stHeader"]  { display: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    _col_l, _col_c, _col_r = st.columns([1, 1.4, 1])
    with _col_c:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown(
            '<div style="text-align:center;font-size:2.2rem;margin-bottom:4px">🏥</div>'
            '<h2 style="text-align:center;margin-bottom:2px">EDI 837P System</h2>'
            '<p style="text-align:center;color:#9ca3af;font-size:0.9rem;margin-bottom:24px">'
            'Enter your access password to continue</p>',
            unsafe_allow_html=True,
        )
        _pwd_input = st.text_input(
            "Password",
            type="password",
            placeholder="Enter password…",
            label_visibility="collapsed",
        )
        _login_btn = st.button("Unlock", type="primary", use_container_width=True)

        if _login_btn or (_pwd_input and st.session_state.get("_pwd_enter")):
            if _verify_password(_pwd_input):
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Incorrect password. Please try again.")

        # Allow Enter key to submit by tracking input changes
        st.session_state["_pwd_enter"] = bool(_pwd_input)

    st.stop()

# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------
if "results" not in st.session_state:
    st.session_state.results = []          # list of (CanonicalClaim, ValidationResult)
if "selected_row" not in st.session_state:
    st.session_state.selected_row = None
if "file_name" not in st.session_state:
    st.session_state.file_name = ""
if "last_diag" not in st.session_state:
    st.session_state.last_diag = None      # _PipelineDiag from last processed file
if "file_diags" not in st.session_state:
    st.session_state.file_diags = {}       # file_name → _PipelineDiag
if "batch_log" not in st.session_state:
    st.session_state.batch_log = []        # list of log-entry dicts from last batch run

# ---------------------------------------------------------------------------
# Sidebar — tabbed layout
# ---------------------------------------------------------------------------
st.sidebar.title("EDI 837P System")
if st.sidebar.button("🔒 Sign Out", key="sign_out"):
    st.session_state.authenticated = False
    st.rerun()
st.sidebar.markdown("---")

_upload_tab, _options_tab, _log_tab = st.sidebar.tabs(["📁 Upload & Search", "⚙ Processing Options", "📋 Batch Logs"])

# ── Tab 1: Upload & Search ──────────────────────────────────────────────────
with _upload_tab:
    uploaded_files = st.file_uploader(
        "Upload EDI File(s)",
        type=["edi", "txt"],
        accept_multiple_files=True,
        help="ASC X12N 837P (005010X222A1) — select one or more files.",
    )

    persist_to_db = st.checkbox(
        "Persist to PostgreSQL",
        value=False,
        disabled=not _DB_AVAILABLE,
        help="Requires PGHOST / PGUSER / PGPASSWORD / PGDATABASE env vars.",
    )

    _process_clicked = st.button(
        "▶  Process Files",
        type="primary",
        disabled=not uploaded_files,
    )

    st.markdown("---")
    st.markdown("**Search**")
    search_claim = st.text_input("Claim ID", key="search_claim")
    search_npi = st.text_input("Billing NPI", key="search_npi")
    search_status = st.selectbox(
        "Status", ["All", "Pass", "Fail"], key="search_status"
    )

    st.markdown("**Date of Service Range**")
    search_dos_from = st.date_input(
        "DOS From",
        value=None,
        key="search_dos_from",
        help="Filter claims with dos_from on or after this date.",
    )
    search_dos_to = st.date_input(
        "DOS To",
        value=None,
        key="search_dos_to",
        help="Filter claims with dos_to on or before this date.",
    )

# ── Tab 2: Processing Options ───────────────────────────────────────────────
with _options_tab:
    st.markdown("#### Envelope Processing")
    st.info(
        "**Truncated Interchange Envelope**  \n"
        "Files missing the IEA closing segment are processed automatically. "
        "A warning is logged and claims inside any ST–SE blocks are fully "
        "extracted and validated as normal.  \n\n"
        "**Segment Terminator Detection**  \n"
        "The terminator is derived from the ISA element structure, not a fixed "
        "byte offset. This handles ISA headers with non-standard field padding.",
    )

    # Show live diagnostics from the last processed file (if any)
    _last_diag = st.session_state.get("last_diag")
    if _last_diag is not None:
        st.markdown("---")
        st.markdown("#### Last File — Envelope Diagnostics")
        st.markdown(
            f"**Segment terminator:** `{_last_diag.seg_term!r}` "
            f"(hex `{_last_diag.seg_term_hex}`)  \n"
            f"**Element delimiter:** `{_last_diag.element_delim!r}` "
            f"(hex `{_last_diag.element_delim_hex}`)  \n"
            f"**Total segments:** {_last_diag.total_segments}  \n"
            f"**ST–SE transactions:** {_last_diag.total_transactions}  \n"
            f"**IEA present in file:** "
            f"{'✅ Yes' if _last_diag.iea_in_raw else '❌ No (envelope unclosed)'}  \n"
            f"**IEA found as clean segment ID:** "
            f"{'✅ Yes' if _last_diag.iea_as_seg_id else '❌ No'}",
        )
        if _last_diag.last_segments:
            with st.expander("Last 5 segments"):
                for _s in _last_diag.last_segments:
                    st.code(_s[:200])

    st.markdown("---")
    st.markdown("#### Additional Options")
    st.caption("Future processing controls will appear here (e.g. SNIP validation level).")

# ── Tab 3: Batch Logs ───────────────────────────────────────────────────────
with _log_tab:
    _batch_log = st.session_state.get("batch_log", [])
    if not _batch_log:
        st.caption("No batch processed yet. Upload files and click ▶ Process Files.")
    else:
        for _entry in _batch_log:
            _icon = "✅" if _entry["success"] else "❌"
            _fname_short = _entry["file"]
            if len(_fname_short) > 28:
                _fname_short = "…" + _fname_short[-25:]
            st.markdown(
                f'<div style="font-size:0.78rem;padding:4px 6px;margin:2px 0;'
                f'background:rgba(255,255,255,0.04);border-radius:4px;'
                f'border-left:3px solid {"#4ade80" if _entry["success"] else "#f87171"}">'
                f'<span>{_icon} <b title="{_entry["file"]}">{_fname_short}</b></span><br>'
                f'<span style="color:#9ca3af">'
                + (f'{_entry["claims"]} claim(s)' if _entry["success"] else "")
                + (f' &nbsp;·&nbsp; ⚠️ envelope' if _entry.get("env_warning") else "")
                + (f' &nbsp;·&nbsp; {_entry["parse_warnings"]} parse warn' if _entry.get("parse_warnings") else "")
                + (f' &nbsp;·&nbsp; 💾 {_entry["db_persisted"]}' if _entry.get("db_persisted") else "")
                + (f'<br><span style="color:#f87171">{_entry["error"]}</span>' if _entry.get("error") else "")
                + (f'<br><span style="color:#fcd34d">DB: {_entry["db_error"]}</span>' if _entry.get("db_error") else "")
                + f'</span></div>',
                unsafe_allow_html=True,
            )
        if st.button("🗑 Clear Logs", key="clear_batch_log"):
            st.session_state.batch_log = []
            st.rerun()

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


import dataclasses as _dc


@_dc.dataclass
class _PipelineDiag:
    """Diagnostic metadata from a processing run."""
    seg_term: str = ""
    seg_term_hex: str = ""
    element_delim: str = ""
    element_delim_hex: str = ""
    total_segments: int = 0
    total_transactions: int = 0
    last_segments: list = _dc.field(default_factory=list)
    iea_in_raw: bool = False
    iea_as_seg_id: bool = False
    tx_segment_counts: list = _dc.field(default_factory=list)
    """List of (st_control_number, segment_count) for every yielded transaction."""
    truncation_error: str = ""
    """Non-empty if a TruncatedFileError was caught during streaming."""
    parse_warnings: list = _dc.field(default_factory=list)
    """Per-transaction parse warnings collected during processing."""


def process_file(
    raw_bytes: bytes,
    file_name: str,
) -> tuple[list[tuple[CanonicalClaim, ValidationResult]], _PipelineDiag]:
    """Full ingestion → parse → validate pipeline.

    Returns
    -------
    (pairs, diag)
        pairs — list of (CanonicalClaim, ValidationResult)
        diag  — diagnostic metadata for debug display
    """
    pairs: list[tuple[CanonicalClaim, ValidationResult]] = []
    diag = _PipelineDiag()

    content = normalize_file_content(raw_bytes)
    delimiters = detect_delimiters(content)

    # Populate delimiter diagnostics immediately so they're available even if
    # an exception occurs later.
    diag.seg_term = delimiters.segment
    diag.seg_term_hex = f"{ord(delimiters.segment):02X}"
    diag.element_delim = delimiters.element
    diag.element_delim_hex = f"{ord(delimiters.element):02X}"

    all_segs_raw = [s.strip() for s in content.split(delimiters.segment) if s.strip()]
    diag.total_segments = len(all_segs_raw)
    diag.last_segments = all_segs_raw[-5:] if all_segs_raw else []
    diag.iea_in_raw = "IEA" in content
    diag.iea_as_seg_id = any(
        s.split(delimiters.element)[0] == "IEA" for s in all_segs_raw
    )

    try:
        transactions = list(stream_transactions(content, delimiters))
    except TruncatedFileError as _te:
        # Bypass: log the error into diagnostics, continue with whatever
        # ST-SE blocks were yielded before the error (may be empty list).
        diag.truncation_error = str(_te)
        transactions = []
    diag.total_transactions = len(transactions)

    for tx in transactions:
        diag.tx_segment_counts.append((tx.st_control_number, len(tx.segments)))

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
            diag.parse_warnings.append(
                f"ST={tx.st_control_number} parse error: {e} — transaction skipped."
            )
            continue

        for canonical in claims:
            validator = SNIPValidator(
                parse_errors=sm.parse_errors,
                element_delimiter=delimiters.element,
            )
            result = validator.validate(canonical)
            pairs.append((canonical, result))

    return pairs, diag


if uploaded_files and _process_clicked:
    st.session_state.results = []
    st.session_state.selected_row = None
    st.session_state.file_name = ""
    st.session_state.file_diags = {}
    st.session_state.batch_log = []

    _all_pairs: list = []
    _total_files = len(uploaded_files)

    for _uploaded in uploaded_files:
        _fname = _uploaded.name
        _log_entry: dict = {
            "file": _fname,
            "claims": 0,
            "success": False,
            "error": None,
            "env_warning": False,
            "parse_warnings": 0,
            "db_persisted": None,
            "db_error": None,
        }
        raw = _uploaded.read()
        try:
            pairs, _diag = process_file(raw, _fname)
            _all_pairs.extend(pairs)
            st.session_state.file_diags[_fname] = _diag
            st.session_state.last_diag = _diag

            _log_entry["success"] = True
            _log_entry["claims"] = len(pairs)
            _log_entry["env_warning"] = bool(_diag.truncation_error or not _diag.iea_in_raw)
            _log_entry["parse_warnings"] = len(_diag.parse_warnings)

            if persist_to_db and _DB_AVAILABLE and pairs:
                try:
                    with managed_connection() as conn:
                        apply_schema(conn)
                        repo = ClaimRepository(conn)
                        ids = repo.insert_many(pairs, file_name=_fname)
                    _log_entry["db_persisted"] = len(ids)
                except Exception as db_err:
                    _log_entry["db_error"] = str(db_err)

        except ValueError as e:
            _log_entry["error"] = f"Invalid file format: {e}"
        except Exception as e:
            _log_entry["error"] = f"Unexpected error: {e}"

        st.session_state.batch_log.append(_log_entry)

    st.session_state.results = _all_pairs
    st.session_state.file_name = ", ".join(f.name for f in uploaded_files)

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
    tab0, tab1, tab2, tab3, tab4 = st.tabs(
        ["Claim Info", "Validation Errors", "Raw Segments", "Full Payload", "Processing Review"]
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

            st.markdown("**Patient Demographics**")
            # Patient-level DOB/gender preferred; fall back to subscriber
            _pat = canonical.claim.patient
            _sub = canonical.claim.subscriber
            _dob    = (_pat.dob    if _pat and _pat.dob    else _sub.dob)
            _gender = (_pat.gender if _pat and _pat.gender else _sub.gender)
            _gender_map = {"M": "Male", "F": "Female", "U": "Unknown"}
            _kv("Date of Birth", _dob)
            _kv("Gender", _gender_map.get(_gender, _gender))

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

        # Claim-level notes (Batch 4.1)
        if c.notes:
            st.markdown("**Clinical Notes**")
            for note in c.notes:
                st.markdown(
                    f'<div style="font-size:0.82rem;padding:4px 8px;margin:2px 0;'
                    f'background:rgba(250,204,21,0.08);border-left:3px solid #fbbf24;'
                    f'border-radius:0 4px 4px 0">{note}</div>',
                    unsafe_allow_html=True,
                )

        # Service lines summary
        if c.service_lines:
            st.markdown("**Service Lines**")
            for sl in c.service_lines:
                mods = " ".join(m for m in [sl.modifier, sl.modifier2, sl.modifier3, sl.modifier4] if m)
                pos_str = f" &nbsp;·&nbsp; POS: {sl.place_of_service}" if sl.place_of_service else ""
                st.markdown(
                    f'<div style="font-size:0.82rem;margin:2px 0;padding:3px 8px;'
                    f'background:rgba(255,255,255,0.04);border-radius:4px">'
                    f'Line {sl.line_number} &nbsp;·&nbsp; '
                    f'<code>{sl.procedure_code}</code>'
                    + (f' &nbsp;<span style="color:#93c5fd">{mods}</span>' if mods else "") +
                    f' &nbsp;·&nbsp; ${sl.charge:,.2f} &nbsp;·&nbsp; {sl.date or "—"}'
                    f'{pos_str}</div>',
                    unsafe_allow_html=True,
                )
                # NDC detail (Batch 3)
                if sl.ndc:
                    st.markdown(
                        f'<div style="font-size:0.78rem;margin:0 0 2px 12px;color:#a5b4fc">'
                        f'NDC: <code>{sl.ndc}</code>'
                        + (f' &nbsp;·&nbsp; {sl.ndc_quantity} {sl.ndc_unit}' if sl.ndc_quantity else "") +
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                # Line-level REF (Batch 3)
                if sl.line_refs:
                    refs_str = " &nbsp;|&nbsp; ".join(
                        f'<code>{q}</code>: {v}' for q, v in sl.line_refs.items()
                    )
                    st.markdown(
                        f'<div style="font-size:0.78rem;margin:0 0 2px 12px;color:#9ca3af">'
                        f'REF: {refs_str}</div>',
                        unsafe_allow_html=True,
                    )
                # Line-level AMT (Batch 3)
                if sl.amounts:
                    amts_str = " &nbsp;|&nbsp; ".join(
                        f'<code>{q}</code>: ${v:,.2f}' for q, v in sl.amounts.items()
                    )
                    st.markdown(
                        f'<div style="font-size:0.78rem;margin:0 0 2px 12px;color:#9ca3af">'
                        f'AMT: {amts_str}</div>',
                        unsafe_allow_html=True,
                    )
                # Service-line providers (2420, Batch 3)
                for lp in sl.line_providers:
                    lp_name = " ".join(filter(None, [lp.last_name, lp.first_name])).title()
                    st.markdown(
                        f'<div style="font-size:0.78rem;margin:0 0 2px 12px;color:#6ee7b7">'
                        f'Provider ({lp.qualifier}): {lp_name or "—"} &nbsp;·&nbsp; NPI: {lp.npi or "—"}'
                        f'</div>',
                        unsafe_allow_html=True,
                    )
                # Service-line notes (Batch 4.1)
                for note in sl.notes:
                    st.markdown(
                        f'<div style="font-size:0.78rem;margin:0 0 2px 12px;'
                        f'padding:2px 6px;background:rgba(250,204,21,0.06);'
                        f'border-left:2px solid #fbbf24;border-radius:0 3px 3px 0">'
                        f'{note}</div>',
                        unsafe_allow_html=True,
                    )

                # Adjudication (2430, Batch 3)
                for adj in sl.adjudications:
                    adj_line = (
                        f'Adj [{adj.payer_id}]: paid ${adj.paid_amount:,.2f}'
                        + (f' on {adj.paid_date}' if adj.paid_date else "")
                    )
                    if adj.adjustments:
                        adj_parts = ", ".join(
                            f'{a.group_code}/{a.reason_code} ${a.amount:,.2f}'
                            for a in adj.adjustments
                        )
                        adj_line += f' &nbsp;·&nbsp; CAS: {adj_parts}'
                    st.markdown(
                        f'<div style="font-size:0.78rem;margin:0 0 2px 12px;color:#fbbf24">'
                        f'{adj_line}</div>',
                        unsafe_allow_html=True,
                    )

        # Claim-level AMT (Batch 3)
        if c.amounts:
            st.markdown("**Financial Amounts**")
            for qual, amt in c.amounts.items():
                _kv(f"AMT*{qual}", f"${amt:,.2f}")

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

    # ── Tab 4: Processing Review ────────────────────────────────────────────
    with tab4:
        # Resolve the diag for this specific claim's source file
        _file_diags = st.session_state.get("file_diags", {})
        _claim_file = canonical.file.file_name if canonical.file else ""
        _diag = _file_diags.get(_claim_file) or st.session_state.get("last_diag")

        # ── Accordion 1: Envelope Status (IEA / Truncation) ───────────────
        _iea_missing   = _diag is not None and not _diag.iea_in_raw
        _iea_malformed = (
            _diag is not None
            and _diag.iea_in_raw
            and not _diag.iea_as_seg_id
        )
        _envelope_ok   = _diag is not None and _diag.iea_as_seg_id
        _trunc_caught  = _diag is not None and bool(_diag.truncation_error)

        if _trunc_caught or _iea_missing:
            _env_label = "⚠️  Truncated Envelope — IEA Segment Absent"
        elif _iea_malformed:
            _env_label = "⚠️  Envelope Warning — IEA Detected but Not Parsed"
        else:
            _env_label = "✅  Envelope Intact — IEA Segment Found"

        _expand_env = _trunc_caught or _iea_missing or _iea_malformed
        with st.expander(_env_label, expanded=_expand_env):
            if _diag is None:
                st.info("No file has been processed yet in this session.")
            else:
                col_a, col_b = st.columns(2)
                col_a.markdown(
                    f"**Segment terminator:** `{_diag.seg_term!r}` "
                    f"(hex `{_diag.seg_term_hex}`)"
                )
                col_b.markdown(
                    f"**Element delimiter:** `{_diag.element_delim!r}` "
                    f"(hex `{_diag.element_delim_hex}`)"
                )
                st.markdown(
                    f"**Total segments:** {_diag.total_segments}  \n"
                    f"**ST–SE transactions:** {_diag.total_transactions}"
                )
                st.markdown("---")
                if _trunc_caught:
                    st.warning(
                        f"**Truncated envelope detected and bypassed.**  \n"
                        f"The IEA interchange-closing segment was not found. "
                        f"Processing continued and any valid claims inside "
                        f"ST–SE blocks were extracted normally.  \n\n"
                        f"**Detail:** `{_diag.truncation_error}`"
                    )
                elif _envelope_ok:
                    st.success(
                        "Envelope is intact. IEA segment was found and parsed "
                        "correctly as the interchange closing segment."
                    )
                elif _iea_malformed:
                    st.warning(
                        "IEA text is present in the raw file but was not found "
                        "as a clean segment ID after splitting. The ISA header "
                        "may use non-standard field padding."
                    )
                else:
                    st.warning(
                        "IEA segment is absent. The interchange envelope was not "
                        "closed. Claims were still extracted and validated normally."
                    )
                if _diag.last_segments:
                    with st.expander("Last 5 segments from file"):
                        for _s in _diag.last_segments:
                            st.code(_s[:200], language="text")

        st.markdown("")

        # ── Accordion 2: Lenient Mode ──────────────────────────────────────
        with st.expander("✅  Lenient Mode — Always Active", expanded=False):
            st.success(
                "**Lenient Mode is permanently active.**  \n"
                "Files with a missing or malformed IEA interchange-closing segment "
                "are always accepted. Claims inside ST–SE blocks are fully extracted "
                "and validated regardless of envelope status. This behaviour cannot "
                "be disabled — envelope issues are informational only."
            )
            st.markdown(
                "**What this means:**\n"
                "- Missing IEA → warning logged, claims still extracted\n"
                "- Malformed ISA header → segment terminator auto-detected from "
                "element structure, not fixed byte offset\n"
                "- Envelope integrity errors → shown here, never block processing"
            )


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

    # ── Summary metrics bar ──────────────────────────────────────────────────
    total_claims    = len(filtered)
    pass_count      = sum(1 for _, r in filtered if r.status == "Pass")
    fail_count      = total_claims - pass_count
    total_charge    = sum(c.claim.total_charge for c, _ in filtered)
    pass_rate_pct   = (pass_count / total_claims * 100) if total_claims else 0.0

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Claims",  total_claims)
    m2.metric("Pass Rate",     f"{pass_rate_pct:.1f}%")
    m3.metric("Failed Claims", fail_count)
    m4.metric("Total Charges", f"${total_charge:,.2f}")
    st.markdown(
        '<hr style="margin:4px 0 12px 0;border:none;border-top:1px solid rgba(255,255,255,0.1)">',
        unsafe_allow_html=True,
    )
    # ── End metrics bar ──────────────────────────────────────────────────────

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
                    dos_from=search_dos_from.isoformat() if search_dos_from else None,
                    dos_to=search_dos_to.isoformat() if search_dos_to else None,
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
