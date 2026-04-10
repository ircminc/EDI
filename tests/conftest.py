"""
pytest configuration and shared fixtures.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Ensure the repo root is on sys.path so `import ingestion` etc. works
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helper to read a fixture as bytes
# ---------------------------------------------------------------------------

def fixture_bytes(name: str) -> bytes:
    return (FIXTURES_DIR / name).read_bytes()


def fixture_text(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def valid_single_bytes():
    return fixture_bytes("valid_single.edi")


@pytest.fixture
def valid_multi_bytes():
    return fixture_bytes("valid_multi.edi")


@pytest.fixture
def missing_iea_bytes():
    return fixture_bytes("missing_iea.edi")


@pytest.fixture
def hl_parent_error_bytes():
    return fixture_bytes("hl_parent_error.edi")


@pytest.fixture
def missing_nm185_bytes():
    return fixture_bytes("missing_nm185.edi")


@pytest.fixture
def balance_mismatch_bytes():
    return fixture_bytes("balance_mismatch.edi")


@pytest.fixture
def invalid_segment_bytes():
    return fixture_bytes("invalid_segment.edi")


@pytest.fixture
def batch1_features_bytes():
    return fixture_bytes("batch1_features.edi")


@pytest.fixture
def large_file_bytes():
    """Generate a ~5MB valid EDI file programmatically for streaming tests."""
    base = fixture_bytes("valid_single.edi").decode("utf-8")
    # Extract the ISA through BHT header and the ST..SE body
    seg_term = "~"
    segs = [s for s in base.split(seg_term) if s.strip()]

    header_segs = segs[:3]   # ISA, GS, ST
    body_segs = segs[3:-4]   # BHT through last DTP
    footer_segs = segs[-4:]  # SE, GE, IEA (we rebuild these)

    # Build a large file by repeating the claim body 300 times inside one ST
    # Adjust SE01 segment count accordingly
    body_part = seg_term.join(body_segs) + seg_term
    big_body = body_part * 300

    # Count segments
    all_inner = body_segs * 300
    seg_count = len(header_segs[2:]) + len(all_inner) + 1  # +1 for SE

    rebuilt = (
        segs[0] + seg_term          # ISA
        + segs[1] + seg_term        # GS
        + segs[2] + seg_term        # ST
        + big_body
        + f"SE*{seg_count}*0001" + seg_term
        + "GE*1*1" + seg_term
        + segs[-1] + seg_term       # IEA
    )
    return rebuilt.encode("utf-8")
