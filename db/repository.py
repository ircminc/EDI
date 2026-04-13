"""
ClaimRepository — insert and query operations for edi_claims.

All monetary values are stored as NUMERIC(12,2) via str(Decimal).
JSONB payloads are serialized with a custom encoder that handles Decimal.
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import Any, Optional

from parser.models import CanonicalClaim
from validator.snip import ValidationResult

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JSON encoder that handles Decimal
# ---------------------------------------------------------------------------

class _DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


def _dumps(obj: Any) -> str:
    return json.dumps(obj, cls=_DecimalEncoder)


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class ClaimRepository:
    """
    Data access object for the ``edi_claims`` table.

    Parameters
    ----------
    conn:
        An open psycopg2 connection.  The caller is responsible for
        committing / rolling back / returning it to the pool.
    """

    def __init__(self, conn) -> None:
        self._conn = conn

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def insert_claim(
        self,
        canonical: CanonicalClaim,
        result: ValidationResult,
        file_name: str = "",
    ) -> int:
        """
        Insert one claim (Pass or Fail) into edi_claims.

        Returns the new row's ``id``.
        """
        payload_dict = canonical.to_dict()
        validation_log = [e.to_dict() for e in result.errors]

        c = canonical.claim

        # Resolve dos_from / dos_to — prefer claim-level dates, fall back to
        # earliest/latest service-line dates (matching the UI display logic).
        def _iso_or_none(val: str):
            """Return val if it looks like YYYY-MM-DD, else None."""
            v = (val or "").strip()
            return v if len(v) == 10 and v[4] == "-" and v[7] == "-" else None

        dos_from = _iso_or_none(c.service_date_from)
        dos_to   = _iso_or_none(c.service_date_to)
        if not dos_from:
            line_dates = sorted(
                sl.date for sl in c.service_lines
                if sl.date and not " to " in sl.date and len(sl.date) == 10
            )
            if line_dates:
                dos_from = line_dates[0]
                dos_to   = line_dates[-1]

        rendering_npi = (
            c.rendering_provider.npi if c.rendering_provider else ""
        )
        payer_id = c.subscriber.payer_id

        sql = """
            INSERT INTO edi_claims
                (file_name, sender_id, receiver_id, claim_id,
                 billing_npi, total_charge, status,
                 dos_from, dos_to, prior_auth_number,
                 rendering_npi, payer_id,
                 raw_payload, validation_log)
            VALUES
                (%s, %s, %s, %s,
                 %s, %s, %s,
                 %s, %s, %s,
                 %s, %s,
                 %s::jsonb, %s::jsonb)
            RETURNING id
        """
        params = (
            file_name,
            canonical.file.sender_id,
            canonical.file.receiver_id,
            c.claim_id,
            c.billing_provider.npi,
            str(c.total_charge),
            result.status,
            dos_from,
            dos_to,
            c.prior_auth_number,
            rendering_npi,
            payer_id,
            _dumps(payload_dict),
            _dumps(validation_log),
        )

        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            row_id: int = cur.fetchone()[0]

        log.debug(
            "Inserted claim %s (status=%s) → id=%d",
            canonical.claim.claim_id, result.status, row_id,
        )
        return row_id

    def insert_many(
        self,
        pairs: list[tuple[CanonicalClaim, ValidationResult]],
        file_name: str = "",
    ) -> list[int]:
        """Bulk insert; returns list of inserted row IDs."""
        return [self.insert_claim(c, r, file_name) for c, r in pairs]

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def search(
        self,
        claim_id: Optional[str] = None,
        billing_npi: Optional[str] = None,
        status: Optional[str] = None,
        dos_from: Optional[str] = None,
        dos_to: Optional[str] = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Search edi_claims with optional filters.

        All text filters use exact match for indexed performance.
        ``dos_from`` / ``dos_to`` are ISO-8601 date strings (YYYY-MM-DD);
        either may be omitted for an open-ended range query.
        Returns plain dicts (no psycopg2 row objects).
        """
        conditions = []
        params: list[Any] = []

        if claim_id:
            conditions.append("claim_id = %s")
            params.append(claim_id)
        if billing_npi:
            conditions.append("billing_npi = %s")
            params.append(billing_npi)
        if status:
            conditions.append("status = %s")
            params.append(status)
        if dos_from:
            conditions.append("dos_from >= %s")
            params.append(dos_from)
        if dos_to:
            conditions.append("dos_to <= %s")
            params.append(dos_to)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"""
            SELECT id, file_name, sender_id, receiver_id,
                   claim_id, billing_npi, total_charge, status,
                   dos_from, dos_to, prior_auth_number,
                   rendering_npi, payer_id,
                   raw_payload, validation_log, created_at
            FROM edi_claims
            {where}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, offset])

        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [desc[0] for desc in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_by_id(self, row_id: int) -> Optional[dict[str, Any]]:
        sql = """
            SELECT id, file_name, sender_id, receiver_id,
                   claim_id, billing_npi, total_charge, status,
                   dos_from, dos_to, prior_auth_number,
                   rendering_npi, payer_id,
                   raw_payload, validation_log, created_at
            FROM edi_claims WHERE id = %s
        """
        with self._conn.cursor() as cur:
            cur.execute(sql, (row_id,))
            row = cur.fetchone()
            if row is None:
                return None
            cols = [desc[0] for desc in cur.description]
            return dict(zip(cols, row))

    def count(
        self,
        claim_id: Optional[str] = None,
        billing_npi: Optional[str] = None,
        status: Optional[str] = None,
    ) -> int:
        conditions = []
        params: list[Any] = []
        if claim_id:
            conditions.append("claim_id = %s")
            params.append(claim_id)
        if billing_npi:
            conditions.append("billing_npi = %s")
            params.append(billing_npi)
        if status:
            conditions.append("status = %s")
            params.append(status)
        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"SELECT COUNT(*) FROM edi_claims {where}"
        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]

    def get_stats(self) -> dict[str, Any]:
        """
        Return aggregate statistics across all claims in the table.

        Keys returned:
          total_claim_count  int
          pass_count         int
          fail_count         int
          pass_rate          float   (0.0 – 1.0; 0.0 when no claims exist)
          total_charge_sum   Decimal
          warning_count_sum  int     (count of validation_log entries with severity='warning')
          error_count_sum    int     (count of validation_log entries with severity='error')
        """
        sql = """
            SELECT
                COUNT(*)                                                    AS total_claim_count,
                COUNT(*) FILTER (WHERE status = 'Pass')                    AS pass_count,
                COUNT(*) FILTER (WHERE status = 'Fail')                    AS fail_count,
                COALESCE(SUM(total_charge), 0)                             AS total_charge_sum,
                COALESCE(
                    SUM(
                        (SELECT COUNT(*) FROM jsonb_array_elements(validation_log) AS e
                         WHERE e->>'severity' = 'warning')
                    ), 0
                )                                                           AS warning_count_sum,
                COALESCE(
                    SUM(
                        (SELECT COUNT(*) FROM jsonb_array_elements(validation_log) AS e
                         WHERE e->>'severity' = 'error')
                    ), 0
                )                                                           AS error_count_sum
            FROM edi_claims
        """
        with self._conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()

        total        = int(row[0])
        pass_count   = int(row[1])
        fail_count   = int(row[2])
        charge_sum   = Decimal(str(row[3])) if row[3] is not None else Decimal("0")
        warn_sum     = int(row[4])
        error_sum    = int(row[5])
        pass_rate    = (pass_count / total) if total > 0 else 0.0

        return {
            "total_claim_count": total,
            "pass_count":        pass_count,
            "fail_count":        fail_count,
            "pass_rate":         pass_rate,
            "total_charge_sum":  charge_sum,
            "warning_count_sum": warn_sum,
            "error_count_sum":   error_sum,
        }

    def delete_by_file(self, file_name: str) -> int:
        """
        Delete all claims belonging to ``file_name``.

        Returns the number of rows deleted.
        """
        sql = "DELETE FROM edi_claims WHERE file_name = %s"
        with self._conn.cursor() as cur:
            cur.execute(sql, (file_name,))
            return cur.rowcount
