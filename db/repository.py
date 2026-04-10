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

        sql = """
            INSERT INTO edi_claims
                (file_name, sender_id, receiver_id, claim_id,
                 billing_npi, total_charge, status,
                 raw_payload, validation_log)
            VALUES
                (%s, %s, %s, %s,
                 %s, %s, %s,
                 %s::jsonb, %s::jsonb)
            RETURNING id
        """
        params = (
            file_name,
            canonical.file.sender_id,
            canonical.file.receiver_id,
            canonical.claim.claim_id,
            canonical.claim.billing_provider.npi,
            str(canonical.claim.total_charge),
            result.status,
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
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """
        Search edi_claims with optional filters.

        All text filters use exact match for indexed performance.
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

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"""
            SELECT id, file_name, sender_id, receiver_id,
                   claim_id, billing_npi, total_charge, status,
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
