# EDI 837P Ingestion System (iRCM)

Production-grade ASC X12N 837P (Professional) ingestion pipeline with streaming parsing, SNIP validation (Levels 1–3), PostgreSQL persistence, and a Streamlit UI.

---

## Architecture

```
iRCM/
├── ingestion/        Streaming + envelope integrity
│   ├── detector.py   ISA delimiter extraction
│   ├── normalizer.py BOM / CRLF / encoding normalization
│   ├── integrity.py  ISA/GS/ST envelope validation
│   └── streamer.py   ST-SE streaming generator
├── parser/           Defensive state machine
│   ├── models.py     Canonical JSON dataclasses
│   ├── hl_tracker.py HL parent-child hierarchy
│   ├── segment_mapper.py  Raw segment → structured data
│   └── state_machine.py   Loop-aware claim parser
├── validator/        SNIP engine
│   ├── rules.py      Individual rules (L1–L3)
│   └── snip.py       Orchestrator + error objects
├── db/               PostgreSQL persistence
│   ├── schema.sql    DDL with indexes
│   ├── connection.py Connection pool (psycopg2)
│   └── repository.py Insert + search queries
├── ui/
│   └── app.py        Streamlit application
└── tests/            pytest suite (≥85% coverage)
```

---

## Requirements

- Python 3.11+
- PostgreSQL 14+ (optional for UI demo)

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure PostgreSQL (optional)

Create a `.env` file in the project root:

```env
PGHOST=localhost
PGPORT=5432
PGDATABASE=ircm
PGUSER=postgres
PGPASSWORD=yourpassword
```

### 3. Create the database schema

```bash
python - <<'EOF'
from db.connection import managed_connection, apply_schema
with managed_connection() as conn:
    apply_schema(conn)
EOF
```

---

## Running the Streamlit UI

```bash
# From the project root
streamlit run ui/app.py
```

The UI will be available at http://localhost:8501.

### UI Features

| Feature | Details |
|---|---|
| File Upload | `.edi` and `.txt` files accepted |
| Real-Time Progress | Progress bar per transaction processed |
| Validation Table | Color-coded: red=error, yellow=warning |
| Row Detail | Click Detail → raw segment + error drill-down |
| Search | By Claim ID, Billing NPI, Status filter |
| DB Search | Query persisted claims when PostgreSQL is connected |

---

## Running Tests

```bash
# From the project root
pytest
```

Expected output: ≥85% coverage across all modules.

To run without coverage (faster):

```bash
pytest --no-cov
```

---

## SNIP Validation Rules

| Level | Code | Severity | Description |
|---|---|---|---|
| 1 | L1-ILLEGAL-CHAR | error | Segment contains control characters |
| 1 | L1-INVALID-SEG | error | Unknown segment ID not in 837P standard set |
| 2 | L2-MISSING-NM185 | error | Billing Provider NM1*85 missing or has no NPI |
| 2 | L2-HL-HIERARCHY | error | HL parent-child relationship violation |
| 2 | L2-MISSING-NM1IL | warning | Subscriber NM1*IL has no member ID |
| 3 | L3-BALANCE-MISMATCH | error | CLM02 ≠ sum(SV102) across all service lines |

**Status rules:**
- Any `error` → `Fail`
- Only `warning` (or none) → `Pass`
- Failed claims are always persisted (never dropped)

---

## Envelope Integrity Rules

| Rule | Description |
|---|---|
| ISA13 == IEA02 | Interchange control number must match |
| IEA01 == GS count | IEA must declare correct number of GS groups |
| GS06 == GE02 | Group control number must match |
| GE01 == ST count | GE must declare correct number of ST transactions |
| ST02 == SE02 | Transaction control number must match |
| SE01 == segment count | SE must declare correct number of segments |
| IEA missing | Raises `TruncatedFileError` — entire file rejected |

---

## Canonical Claim Contract

```json
{
  "file": {
    "file_name": "...",
    "sender_id": "ISA06",
    "receiver_id": "ISA08",
    "isa_control_number": "ISA13",
    "isa_version": "ISA12",
    "usage_indicator": "ISA15"
  },
  "transaction": {
    "st_control_number": "ST02",
    "gs_control_number": "GS06",
    "gs_date": "GS04",
    "gs_time": "GS05",
    "functional_id": "GS01"
  },
  "claim": {
    "claim_id": "CLM01",
    "total_charge": "Decimal",
    "billing_provider": { "npi": "NM109", "..." : "..." },
    "subscriber": { "member_id": "NM109", "..." : "..." },
    "patient": null,
    "service_lines": [
      { "line_number": 1, "charge": "Decimal", "date": "YYYY-MM-DD" }
    ],
    "raw_segments": [
      { "segment": "SV1*...", "position": 0, "loop": "2400" }
    ]
  }
}
```

---

## Database Schema

Table: `edi_claims`

| Column | Type | Notes |
|---|---|---|
| id | BIGSERIAL PK | Auto |
| file_name | TEXT | Indexed |
| sender_id | TEXT | ISA06 |
| receiver_id | TEXT | ISA08 |
| claim_id | TEXT NOT NULL | Indexed |
| billing_npi | TEXT | Indexed |
| total_charge | NUMERIC(12,2) | CLM02 |
| status | TEXT | 'Pass' or 'Fail' |
| raw_payload | JSONB | Full canonical claim |
| validation_log | JSONB | Array of error objects |
| created_at | TIMESTAMPTZ | Auto |

---

## Error Recovery

| Scenario | Behavior |
|---|---|
| Bad claim (parse/SNIP error) | Mark Fail, continue processing |
| Broken ST block | Reject that ST only, continue |
| Missing IEA | Raise `TruncatedFileError`, fail entire file |
| Parsing exception in ST | Log warning, attempt recovery at next ST |
