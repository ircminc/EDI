"""
Tests for the ingestion layer:
  - Delimiter detection
  - File normalization
  - Envelope integrity validation
  - Streaming generator
"""

from __future__ import annotations

import pytest

from ingestion.normalizer import normalize_file_content
from ingestion.detector import detect_delimiters, extract_isa_fields, DelimiterSet
from ingestion.integrity import (
    validate_envelope,
    validate_transaction_counts,
    TruncatedFileError,   # still used by TestEnvelopeIntegrity.test_missing_iea_raises_truncated
    EnvelopeError,
)
from ingestion.streamer import stream_transactions, TransactionBlock


# ---------------------------------------------------------------------------
# Delimiter detection
# ---------------------------------------------------------------------------

class TestDelimiterDetection:
    def test_standard_delimiters(self, valid_single_bytes):
        content = normalize_file_content(valid_single_bytes)
        d = detect_delimiters(content)
        assert d.element == "*"
        assert d.component == ":"
        assert d.segment == "~"

    def test_too_short_raises(self):
        with pytest.raises(ValueError, match="too short"):
            detect_delimiters("ISA*00*")

    def test_no_isa_prefix_raises(self):
        with pytest.raises(ValueError, match="does not begin"):
            detect_delimiters("X" * 106)

    def test_extract_isa_fields(self, valid_single_bytes):
        content = normalize_file_content(valid_single_bytes)
        d = detect_delimiters(content)
        fields = extract_isa_fields(content, d)
        assert fields["isa06_sender"] == "SENDER01"
        assert fields["isa08_receiver"] == "RECEIVER01"
        assert fields["isa15_usage_indicator"] == "T"

    def test_bom_stripped(self):
        # UTF-8 BOM + valid ISA
        raw = b"\xef\xbb\xbf" + b"ISA*00*          *00*          *ZZ*SENDER01       *ZZ*RECEIVER01     *240101*1200*^*00501*000000001*0*T*:~"
        content = normalize_file_content(raw)
        assert content.startswith("ISA")
        d = detect_delimiters(content)
        assert d.element == "*"

    def test_crlf_normalized(self, valid_single_bytes):
        crlf = valid_single_bytes.replace(b"\n", b"\r\n")
        content = normalize_file_content(crlf)
        assert "\r" not in content

    def test_unpadded_isa_detects_correct_segment_terminator(self):
        """Segment terminator must be detected correctly even when ISA fields
        are not padded to their fixed widths (non-standard but real-world)."""
        # Build an ISA where sender/receiver IDs are NOT padded to 15 chars.
        # Fixed-position read (raw[105]) would land on a wrong character;
        # split-based detection must still resolve '~' correctly.
        isa = (
            "ISA*00*          *00*          "
            "*ZZ*SHORT"           # ISA05+ISA06 — sender ID only 5 chars, not padded
            "*ZZ*ALSOSHORT"       # ISA07+ISA08 — receiver ID only 8 chars
            "*240101*1200*^*00501*000000001*0*T*:~"
        )
        # Pad to minimum length requirement with a dummy GS segment after ISA
        content = isa + "GS*HC*SHORT*ALSOSHORT*240101*1200*1*X*005010X222A1~"
        d = detect_delimiters(content)
        assert d.element == "*"
        assert d.segment == "~"
        assert d.component == ":"
        assert d.repetition == "^"

    def test_fewer_than_16_element_delimiters_raises(self):
        """ISA with too few element delimiters must raise ValueError."""
        bad = "ISA*00*only*three*fields" + "X" * 82
        with pytest.raises(ValueError, match="malformed|fewer than 16"):
            detect_delimiters(bad)


# ---------------------------------------------------------------------------
# Envelope integrity
# ---------------------------------------------------------------------------

class TestEnvelopeIntegrity:
    def _segments(self, content: str, seg_term: str) -> list[str]:
        return [s.strip() for s in content.split(seg_term) if s.strip()]

    def test_valid_single_no_errors(self, valid_single_bytes):
        content = normalize_file_content(valid_single_bytes)
        d = detect_delimiters(content)
        segs = self._segments(content, d.segment)
        errors = validate_envelope(segs, d)
        assert errors == []

    def test_missing_iea_raises_truncated(self, missing_iea_bytes):
        content = normalize_file_content(missing_iea_bytes)
        d = detect_delimiters(content)
        segs = self._segments(content, d.segment)
        with pytest.raises(TruncatedFileError):
            validate_envelope(segs, d)

    def test_valid_multi_no_errors(self, valid_multi_bytes):
        content = normalize_file_content(valid_multi_bytes)
        d = detect_delimiters(content)
        segs = self._segments(content, d.segment)
        errors = validate_envelope(segs, d)
        assert errors == []

    def test_st_se_count_mismatch(self):
        # SE01 says 5 segments but only 3 are present
        segs = ["ST*837*0001", "CLM*X*100", "SE*5*0001"]
        from ingestion.detector import DelimiterSet
        d = DelimiterSet(element="*", component=":", segment="~", repetition="^")
        errors = validate_transaction_counts(segs, d)
        assert any("mismatch" in str(e).lower() for e in errors)

    def test_st_se_ctrl_mismatch(self):
        segs = ["ST*837*0001", "CLM*X*100", "HI*BK:Z0000", "SE*4*9999"]
        from ingestion.detector import DelimiterSet
        d = DelimiterSet(element="*", component=":", segment="~", repetition="^")
        errors = validate_transaction_counts(segs, d)
        assert any("0001" in str(e) or "9999" in str(e) for e in errors)


# ---------------------------------------------------------------------------
# Streaming
# ---------------------------------------------------------------------------

class TestStreaming:
    def test_valid_single_yields_one_block(self, valid_single_bytes):
        content = normalize_file_content(valid_single_bytes)
        d = detect_delimiters(content)
        blocks = list(stream_transactions(content, d))
        assert len(blocks) == 1
        assert isinstance(blocks[0], TransactionBlock)

    def test_valid_multi_yields_two_blocks(self, valid_multi_bytes):
        content = normalize_file_content(valid_multi_bytes)
        d = detect_delimiters(content)
        blocks = list(stream_transactions(content, d))
        assert len(blocks) == 2

    def test_missing_iea_does_not_raise(self, missing_iea_bytes):
        """Missing IEA is now a warning, not a fatal error."""
        content = normalize_file_content(missing_iea_bytes)
        d = detect_delimiters(content)
        # Must complete without raising
        blocks = list(stream_transactions(content, d))
        assert len(blocks) >= 1

    def test_block_has_sender_receiver(self, valid_single_bytes):
        content = normalize_file_content(valid_single_bytes)
        d = detect_delimiters(content)
        block = list(stream_transactions(content, d))[0]
        assert block.sender_id == "SENDER01"
        assert block.receiver_id == "RECEIVER01"

    def test_large_file_streaming_memory(self, large_file_bytes):
        """Streaming a large file should stay well under 200MB."""
        import tracemalloc
        tracemalloc.start()
        content = normalize_file_content(large_file_bytes)
        d = detect_delimiters(content)
        count = 0
        for block in stream_transactions(content, d):
            count += 1
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        # Peak must stay under 200 MB
        assert peak < 200 * 1024 * 1024, f"Peak memory {peak/1024/1024:.1f} MB exceeded 200 MB"
        assert count >= 1

    def test_missing_iea_yields_valid_transaction_blocks(self, missing_iea_bytes):
        """ST-SE blocks must be yielded as valid TransactionBlocks even without IEA."""
        content = normalize_file_content(missing_iea_bytes)
        d = detect_delimiters(content)
        blocks = list(stream_transactions(content, d))
        assert all(isinstance(b, TransactionBlock) for b in blocks)
        assert all(len(b.segments) > 0 for b in blocks)

    def test_complete_file_still_streams_normally(self, valid_single_bytes):
        """A file with a proper IEA must stream identically to before."""
        content = normalize_file_content(valid_single_bytes)
        d = detect_delimiters(content)
        blocks = list(stream_transactions(content, d))
        assert len(blocks) == 1
        assert blocks[0].st_control_number != ""
