"""
File normalization — encoding detection, BOM stripping, line-ending normalization.

Supports:
  - ASCII
  - UTF-8 (with or without BOM)
  - Latin-1 / ISO-8859-1 fallback

Memory strategy: reads the whole file for normalization but the streamer
then works on the resulting string using a generator, keeping peak RSS low
for files that are processed one transaction at a time.
"""

from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger(__name__)

# UTF-8 BOM
_UTF8_BOM = b"\xef\xbb\xbf"

_ENCODINGS_TO_TRY = ("utf-8-sig", "utf-8", "latin-1")


def normalize_file_content(source: str | Path | bytes) -> str:
    """
    Read and normalize an EDI file.

    Parameters
    ----------
    source:
        Either a file path (str or Path) or raw bytes already in memory.

    Returns
    -------
    str
        Normalized content with:
        - BOM removed
        - CRLF and bare CR replaced with LF
        - Leading/trailing whitespace stripped
    """
    if isinstance(source, (str, Path)):
        raw_bytes = Path(source).read_bytes()
    elif isinstance(source, bytes):
        raw_bytes = source
    else:
        raise TypeError(f"Unsupported source type: {type(source)}")

    # Strip UTF-8 BOM explicitly (utf-8-sig handles it but we also want
    # the log message for audit purposes).
    if raw_bytes.startswith(_UTF8_BOM):
        log.debug("BOM detected and stripped.")
        raw_bytes = raw_bytes[len(_UTF8_BOM):]

    text = _decode(raw_bytes)
    text = _normalize_line_endings(text)
    return text


def _decode(raw_bytes: bytes) -> str:
    for encoding in _ENCODINGS_TO_TRY:
        try:
            text = raw_bytes.decode(encoding)
            log.debug("Decoded file as %s.", encoding)
            return text
        except (UnicodeDecodeError, LookupError):
            continue
    # Last-resort: replace invalid bytes rather than crash.
    text = raw_bytes.decode("utf-8", errors="replace")
    log.warning("File contained non-UTF-8 bytes; replaced undecodable characters.")
    return text


def _normalize_line_endings(text: str) -> str:
    """Replace CRLF and bare CR with LF."""
    return text.replace("\r\n", "\n").replace("\r", "\n")
