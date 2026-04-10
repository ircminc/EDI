"""
PostgreSQL connection management using psycopg2 connection pooling.

Configuration is read from environment variables (or a .env file):
  PGHOST     default: localhost
  PGPORT     default: 5432
  PGDATABASE default: ircm
  PGUSER     default: postgres
  PGPASSWORD required

Usage:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(...)
        conn.commit()
    finally:
        conn.close()          # returns to pool

Or use the context manager:
    with get_connection() as conn:
        ...
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from typing import Generator

try:
    import psycopg2
    from psycopg2 import pool as pg_pool
    from psycopg2.extras import Json, RealDictCursor
    _PSYCOPG2_AVAILABLE = True
except ImportError:
    _PSYCOPG2_AVAILABLE = False

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

log = logging.getLogger(__name__)

_pool: "pg_pool.ThreadedConnectionPool | None" = None


def _dsn() -> str:
    return (
        f"host={os.getenv('PGHOST', 'localhost')} "
        f"port={os.getenv('PGPORT', '5432')} "
        f"dbname={os.getenv('PGDATABASE', 'ircm')} "
        f"user={os.getenv('PGUSER', 'postgres')} "
        f"password={os.getenv('PGPASSWORD', '')} "
        f"connect_timeout=10"
    )


def get_pool() -> "pg_pool.ThreadedConnectionPool":
    """Return the global connection pool, creating it on first call."""
    global _pool
    if not _PSYCOPG2_AVAILABLE:
        raise RuntimeError(
            "psycopg2 is not installed. Run: pip install psycopg2-binary"
        )
    if _pool is None:
        _pool = pg_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=_dsn(),
        )
        log.info("Connection pool created (min=1, max=10).")
    return _pool


def get_connection():
    """Get a connection from the pool."""
    return get_pool().getconn()


def release_connection(conn) -> None:
    """Return a connection to the pool."""
    pool = get_pool()
    pool.putconn(conn)


@contextmanager
def managed_connection() -> Generator:
    """Context manager that auto-returns the connection to the pool."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_connection(conn)


def apply_schema(conn) -> None:
    """
    Execute schema.sql against the connected database.
    Safe to run multiple times (uses IF NOT EXISTS).
    """
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        ddl = f.read()
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    log.info("Schema applied successfully.")
