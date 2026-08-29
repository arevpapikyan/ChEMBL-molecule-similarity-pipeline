from contextlib import contextmanager

import psycopg2
import psycopg2.extras

from .config import Settings

_CONNECT_TIMEOUT_SECONDS = 10
_STATEMENT_TIMEOUT_MS = 10 * 60 * 1000


@contextmanager
def get_connection(settings: Settings):
    conn = psycopg2.connect(
        settings.dwh_url,
        connect_timeout=_CONNECT_TIMEOUT_SECONDS,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {_STATEMENT_TIMEOUT_MS}")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_rows(conn, table: str, columns: list[str], rows: list[tuple], conflict_cols: list[str]) -> int:
    if not rows:
        return 0
    cols_sql = ", ".join(columns)
    update_sql = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in conflict_cols)
    conflict_sql = ", ".join(conflict_cols)
    query = (
        f"INSERT INTO {table} ({cols_sql}) VALUES %s "
        f"ON CONFLICT ({conflict_sql}) DO UPDATE SET {update_sql}"
    )
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, query, rows)
    return len(rows)
