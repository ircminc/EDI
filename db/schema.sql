-- EDI 837P Claims schema
-- PostgreSQL 14+

CREATE TABLE IF NOT EXISTS edi_claims (
    id               BIGSERIAL PRIMARY KEY,
    file_name        TEXT        NOT NULL DEFAULT '',
    sender_id        TEXT        NOT NULL DEFAULT '',
    receiver_id      TEXT        NOT NULL DEFAULT '',
    claim_id         TEXT        NOT NULL,
    billing_npi      TEXT        NOT NULL DEFAULT '',
    total_charge     NUMERIC(12, 2) NOT NULL DEFAULT 0,
    status           TEXT        NOT NULL CHECK (status IN ('Pass', 'Fail')),
    raw_payload      JSONB       NOT NULL DEFAULT '{}',
    validation_log   JSONB       NOT NULL DEFAULT '[]',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for fast lookups (< 2 seconds per spec)
CREATE INDEX IF NOT EXISTS idx_edi_claims_claim_id
    ON edi_claims (claim_id);

CREATE INDEX IF NOT EXISTS idx_edi_claims_billing_npi
    ON edi_claims (billing_npi);

CREATE INDEX IF NOT EXISTS idx_edi_claims_file_name
    ON edi_claims (file_name);

CREATE INDEX IF NOT EXISTS idx_edi_claims_status
    ON edi_claims (status);

-- GIN index on JSONB payload for key-based searches
CREATE INDEX IF NOT EXISTS idx_edi_claims_raw_payload
    ON edi_claims USING GIN (raw_payload);
