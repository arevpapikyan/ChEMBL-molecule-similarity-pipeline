"""Ingest the four required ChEMBL tables into Bronze."""

import csv
import io
import logging
import os
import shutil
import sqlite3
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

import chembl_downloader
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2.extras import Json

from .config import Settings, get_settings
from .db_utils import get_connection
from .s3_utils import get_s3_client

logger = logging.getLogger(__name__)

REQUIRED_TABLES = [
    "chembl_id_lookup",
    "molecule_dictionary",
    "compound_properties",
    "compound_structures",
]

TABLE_COLUMNS = {
    "chembl_id_lookup": ["chembl_id", "entity_type", "entity_id", "status"],
    "molecule_dictionary": [
        "chembl_id", "molecule_type", "pref_name",
        "max_phase", "therapeutic_flag", "withdrawn_flag",
    ],
    "compound_properties": [
        "chembl_id", "mw_freebase", "alogp", "psa", "cx_logp",
        "molecular_species", "full_mwt", "aromatic_rings", "heavy_atoms",
    ],
    "compound_structures": [
        "chembl_id", "canonical_smiles", "standard_inchi", "standard_inchi_key",
    ],
}

TABLE_QUERIES = {
    "chembl_id_lookup": """
        SELECT chembl_id, entity_type, entity_id, status
        FROM chembl_id_lookup
        WHERE entity_type = 'COMPOUND'
    """,
    "molecule_dictionary": """
        SELECT chembl_id, molecule_type, pref_name,
               max_phase, therapeutic_flag, withdrawn_flag
        FROM molecule_dictionary
    """,
    "compound_properties": """
        SELECT md.chembl_id, cp.mw_freebase, cp.alogp, cp.psa,
               NULL AS cx_logp,
               NULL AS molecular_species,
               cp.full_mwt, cp.aromatic_rings, cp.heavy_atoms
        FROM compound_properties cp
        JOIN molecule_dictionary md ON md.molregno = cp.molregno
    """,
    "compound_structures": """
        SELECT md.chembl_id, cs.canonical_smiles, cs.standard_inchi, cs.standard_inchi_key
        FROM compound_structures cs
        JOIN molecule_dictionary md ON md.molregno = cs.molregno
    """,
}

TABLE_SCHEMAS = {
    "chembl_id_lookup": pa.schema([
        ("chembl_id", pa.string()),
        ("entity_type", pa.string()),
        ("entity_id", pa.int64()),
        ("status", pa.string()),
    ]),
    "molecule_dictionary": pa.schema([
        ("chembl_id", pa.string()),
        ("molecule_type", pa.string()),
        ("pref_name", pa.string()),
        ("max_phase", pa.float64()),
        ("therapeutic_flag", pa.int64()),
        ("withdrawn_flag", pa.int64()),
    ]),
    "compound_properties": pa.schema([
        ("chembl_id", pa.string()),
        ("mw_freebase", pa.float64()),
        ("alogp", pa.float64()),
        ("psa", pa.float64()),
        ("cx_logp", pa.float64()),
        ("molecular_species", pa.string()),
        ("full_mwt", pa.float64()),
        ("aromatic_rings", pa.int64()),
        ("heavy_atoms", pa.int64()),
    ]),
    "compound_structures": pa.schema([
        ("chembl_id", pa.string()),
        ("canonical_smiles", pa.string()),
        ("standard_inchi", pa.string()),
        ("standard_inchi_key", pa.string()),
    ]),
}

DOWNLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB per read
DOWNLOAD_PROGRESS_LOG_INTERVAL_SECONDS = 15  # log at most this often, not every chunk
CHEMBL_BASE = "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBLdb/releases"

# Large downloads may fail mid-transfer, so resume with HTTP Range requests and retry.
DOWNLOAD_MAX_ATTEMPTS = int(os.environ.get("INGEST_DOWNLOAD_MAX_ATTEMPTS", "10"))
DOWNLOAD_RETRY_DELAY_SECONDS = int(os.environ.get("INGEST_DOWNLOAD_RETRY_DELAY", "15"))
DOWNLOAD_SOCKET_TIMEOUT_SECONDS = int(os.environ.get("INGEST_DOWNLOAD_SOCKET_TIMEOUT", "120"))

PYSTOW_HOME = Path(os.environ.get("PYSTOW_HOME", os.path.expanduser("~/.data")))

# Fail fast if free disk space is insufficient for extraction.
# Override the required space via env for different ChEMBL releases.
MIN_FREE_BYTES_FOR_EXTRACTION = int(
    os.environ.get("INGEST_MIN_FREE_BYTES", str(50 * 1024**3))  # 50 GiB
)

# Maximum extraction time; fail clearly instead of hanging indefinitely.
EXTRACTION_TIMEOUT_SECONDS = int(
    os.environ.get("INGEST_EXTRACTION_TIMEOUT_SECONDS", str(2 * 60 * 60))  # 2h
)

# Written only after successful extraction; its presence marks the extraction
# as complete. Interrupted extractions lack the marker and are re-extracted.
EXTRACTION_MARKER_NAME = ".extraction_complete"

EXTRACTION_VALIDATION_MAX_ATTEMPTS = 2

DEFAULT_BATCH_SIZE = 25_000

DB_STEP_MAX_ATTEMPTS = 3
DB_STEP_RETRY_DELAY_SECONDS = 5

# Age threshold for treating a backend transaction as a zombie, set well above
# the expected batch duration to avoid killing legitimate long-running work.
STALE_BACKEND_MAX_AGE_SECONDS = 600

_TRANSIENT_DB_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)

EXTRACTION_PROGRESS_LOG_INTERVAL_SECONDS = 15


def _assert_enough_disk(target_dir: Path, min_free_bytes: int) -> None:
    """Refuse to start extraction if free space is below min_free_bytes."""
    probe = target_dir
    while not probe.exists():
        parent = probe.parent
        if parent == probe: # reached the filesystem root
            break
        probe = parent

    usage = shutil.disk_usage(probe)
    if usage.free < min_free_bytes:
        raise OSError(
            "Not enough free disk to extract the ChEMBL SQLite dump at "
            f"{target_dir}: need >= {min_free_bytes / 1024**3:.1f} GiB free, "
            f"have {usage.free / 1024**3:.1f} GiB (of {usage.total / 1024**3:.1f} "
            "GiB total). Free up space on the Docker volume / host disk, point "
            "PYSTOW_HOME at a larger disk, or lower INGEST_MIN_FREE_BYTES."
        )
    logger.info(
        "Disk preflight OK for %s: %.1f GiB free (need >= %.1f GiB)",
        target_dir, usage.free / 1024**3, min_free_bytes / 1024**3,
    )


def _is_valid_chembl_tarball(path: Path) -> bool:
    """
    Checks that the tar.gz archive is complete and contains a .db file.

    A truncated or incorrect download may still open as a valid tar archive, so
    checking for the expected SQLite database catches the problem early.
    """
    logger.info(
        "Validating cached ChEMBL tarball at %s -- this decompresses the whole "
        "archive and typically takes several minutes, with no output until done",
        path,
    )
    started = time.monotonic()
    try:
        with tarfile.open(path, "r:gz") as tar:
            names = tar.getnames()
    except (tarfile.TarError, EOFError, OSError) as exc:
        logger.warning("ChEMBL dump at %s failed validation: %s", path, exc)
        return False

    if not any(name.endswith(".db") for name in names):
        logger.warning(
            "ChEMBL dump at %s is a readable archive but contains no .db file "
            "(%s members); treating as invalid",
            path, len(names),
        )
        return False

    logger.info(
        "Validated ChEMBL tarball at %s in %.1fs (%s members, .db present)",
        path, time.monotonic() - started, len(names),
    )
    return True


def _remote_size(url: str) -> int | None:
    """Content-Length of url, or None if the server won't say."""
    request = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=DOWNLOAD_SOCKET_TIMEOUT_SECONDS) as response:
            length = response.headers.get("Content-Length")
            return int(length) if length is not None else None
    except (urllib.error.URLError, OSError, ValueError) as exc:
        logger.warning("Could not determine remote size for %s: %s", url, exc)
        return None


def _download_once_resuming(url: str, partial_path: Path, total_size: int | None) -> int:
    """
    Stream the URL into partial_path, resuming any existing partial download.

    Returns the current file size. Network errors are raised to the caller, while
    the partial file is preserved for subsequent retries.
    """
    resume_from = partial_path.stat().st_size if partial_path.exists() else 0

    if total_size is not None and resume_from >= total_size:
        return resume_from

    request = urllib.request.Request(url)
    if resume_from:
        request.add_header("Range", f"bytes={resume_from}-")
        logger.info(
            "Resuming ChEMBL download at %.1f MB / %.1f MB",
            resume_from / 1_048_576,
            (total_size or 0) / 1_048_576,
        )

    started = time.monotonic()
    last_logged = started
    bytes_this_attempt = 0

    with urllib.request.urlopen(request, timeout=DOWNLOAD_SOCKET_TIMEOUT_SECONDS) as response:
        # 206 means Range was honoured, so append. Otherwise, restart to avoid
        # corrupting the file by appending a duplicate full download.
        if resume_from and response.status != 206:
            logger.warning(
                "Server ignored Range request (HTTP %s); restarting download from scratch",
                response.status,
            )
            resume_from = 0
            mode = "wb"
        else:
            mode = "ab" if resume_from else "wb"

        with open(partial_path, mode) as out_file:
            while True:
                chunk = response.read(DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                out_file.write(chunk)
                bytes_this_attempt += len(chunk)

                now = time.monotonic()
                if now - last_logged >= DOWNLOAD_PROGRESS_LOG_INTERVAL_SECONDS:
                    elapsed = now - started
                    speed_kb_s = (bytes_this_attempt / 1024) / elapsed if elapsed > 0 else 0
                    have = resume_from + bytes_this_attempt
                    if total_size:
                        logger.info(
                            "Downloading ChEMBL dump: %.1f MB / %.1f MB (%.1f%%) at %.0f KB/s",
                            have / 1_048_576, total_size / 1_048_576,
                            100 * have / total_size, speed_kb_s,
                        )
                    else:
                        logger.info(
                            "Downloading ChEMBL dump: %.1f MB so far at %.0f KB/s",
                            have / 1_048_576, speed_kb_s,
                        )
                    last_logged = now

    return resume_from + bytes_this_attempt


def _download_chembl_dump_with_progress(version: str) -> None:
    target_dir = PYSTOW_HOME / "chembl" / version
    target_dir.mkdir(parents=True, exist_ok=True)
    filename = f"chembl_{version}_sqlite.tar.gz"
    target_path = target_dir / filename
    partial_path = target_dir / f"{filename}.part"

    url = f"{CHEMBL_BASE}/chembl_{version}/{filename}"
    total_size = _remote_size(url)

    if target_path.exists():
        # Skip expensive validation when the cached file matches the server-reported size.
        # Validate fully on size mismatch or when the remote size is unknown.
        local_size = target_path.stat().st_size
        if total_size is not None and local_size == total_size:
            logger.info(
                "ChEMBL dump already cached at %s with expected size (%.1f MB), skipping download",
                target_path, local_size / 1_048_576,
            )
            return
        if total_size is None and _is_valid_chembl_tarball(target_path):
            logger.info("ChEMBL dump already cached and verified at %s, skipping download", target_path)
            return
        logger.warning(
            "Cached ChEMBL dump at %s is %.1f MB but server reports %.1f MB; re-downloading",
            target_path, local_size / 1_048_576, (total_size or 0) / 1_048_576,
        )
        target_path.unlink()

    logger.info(
        "Downloading %s (%.1f MB)", url, (total_size or 0) / 1_048_576,
    )

    started = time.monotonic()
    have = 0
    last_exc: Exception | None = None

    for attempt in range(1, DOWNLOAD_MAX_ATTEMPTS + 1):
        try:
            have = _download_once_resuming(url, partial_path, total_size)
            if total_size is None or have >= total_size:
                break
            # Connection closed early without raising: treat as retryable.
            raise OSError(
                f"Download truncated: have {have} bytes, expected {total_size}"
            )
        except Exception as exc:
            last_exc = exc
            have = partial_path.stat().st_size if partial_path.exists() else 0
            if attempt < DOWNLOAD_MAX_ATTEMPTS:
                logger.warning(
                    "ChEMBL download attempt %s/%s failed at %.1f MB (%s); "
                    "retrying in %ss and resuming from where it stopped",
                    attempt, DOWNLOAD_MAX_ATTEMPTS, have / 1_048_576,
                    exc, DOWNLOAD_RETRY_DELAY_SECONDS,
                )
                time.sleep(DOWNLOAD_RETRY_DELAY_SECONDS)
            else:
                logger.error(
                    "ChEMBL download failed after %s attempts, last error: %s",
                    DOWNLOAD_MAX_ATTEMPTS, exc,
                )
    else:
        # Loop exhausted without a successful break.
        raise OSError(
            f"Could not download the ChEMBL dump after {DOWNLOAD_MAX_ATTEMPTS} "
            f"attempts (got {have} of {total_size} bytes). The partial file has "
            f"been kept at {partial_path}, so re-running resumes rather than "
            "starting over."
        ) from last_exc

    if total_size is not None and have != total_size:
        raise OSError(
            f"Download incomplete: got {have} bytes, server reported {total_size} bytes"
        )

    if not _is_valid_chembl_tarball(partial_path):
        # A complete-but-invalid archive is not resumable; drop it so the next
        # run starts clean rather than resuming onto garbage.
        partial_path.unlink(missing_ok=True)
        raise OSError("Downloaded file failed archive validation (corrupt or wrong contents)")

    partial_path.rename(target_path)
    total_elapsed = time.monotonic() - started
    avg_speed = (have / 1024) / total_elapsed if total_elapsed > 0 else 0
    logger.info(
        "Finished downloading and verified ChEMBL dump: %.1f MB in %.1fs (%.0f KB/s average)",
        have / 1_048_576, total_elapsed, avg_speed,
    )


def _dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except OSError:
                pass
    return total


def _run_extraction_with_progress(version: str, extract_dir: Path) -> str:
    result: dict = {}
    error: dict = {}

    def _run() -> None:
        try:
            result["path"] = chembl_downloader.download_extract_sqlite(version=version)
        except Exception as exc:
            error["exc"] = exc

    thread = threading.Thread(target=_run, daemon=True)
    started = time.monotonic()
    thread.start()

    last_logged = started
    while thread.is_alive():
        thread.join(timeout=2)
        now = time.monotonic()

        elapsed = now - started
        if elapsed > EXTRACTION_TIMEOUT_SECONDS:
            raise TimeoutError(
                f"ChEMBL extraction exceeded {EXTRACTION_TIMEOUT_SECONDS}s "
                f"(reached {_dir_size_bytes(extract_dir) / 1_048_576:.1f} MB). "
                "Check disk space / host I/O, or raise "
                "INGEST_EXTRACTION_TIMEOUT_SECONDS."
            )

        if now - last_logged >= EXTRACTION_PROGRESS_LOG_INTERVAL_SECONDS:
            extracted_mb = _dir_size_bytes(extract_dir) / 1_048_576
            logger.info(
                "Extracting ChEMBL dump: %.1f MB written so far (%.0fs elapsed)",
                extracted_mb, now - started,
            )
            last_logged = now

    thread.join()

    if "exc" in error:
        raise error["exc"]

    logger.info("Finished extracting ChEMBL dump in %.1fs", time.monotonic() - started)
    return result["path"]


def _is_valid_sqlite_db(path: str) -> bool:
    """Perform a cheap structural sanity check on the extracted SQLite database."""
    try:
        conn = sqlite3.connect(path)
        try:
            conn.execute("SELECT count(*) FROM sqlite_master;").fetchone()
            # Verify a required table is present and non-empty before COPY begins.
            conn.execute("SELECT chembl_id FROM chembl_id_lookup LIMIT 1;").fetchone()
        finally:
            conn.close()
        return True
    except sqlite3.DatabaseError as exc:
        logger.warning("Extracted ChEMBL database at %s failed validation: %s", path, exc)
        return False


def _extract_chembl_sqlite_with_progress(version: str) -> str:
    extract_dir = PYSTOW_HOME / "chembl" / version / "data"
    marker = extract_dir / EXTRACTION_MARKER_NAME

    _assert_enough_disk(extract_dir, MIN_FREE_BYTES_FOR_EXTRACTION)

    # chembl_downloader skips existing destinations, so use the marker to
    # distinguish completed extractions from interrupted ones.
    if extract_dir.exists() and not marker.exists():
        logger.warning(
            "Extraction directory %s exists but has no completion marker -- it is "
            "a leftover from an interrupted run; wiping it and re-extracting",
            extract_dir,
        )
        shutil.rmtree(extract_dir, ignore_errors=True)

    for attempt in range(1, EXTRACTION_VALIDATION_MAX_ATTEMPTS + 1):
        sqlite_path = _run_extraction_with_progress(version, extract_dir)

        if _is_valid_sqlite_db(sqlite_path):
            # Mark successful extraction so future runs can trust the directory directly.
            try:
                extract_dir.mkdir(parents=True, exist_ok=True)
                marker.write_text(
                    f"chembl {version} extracted and validated at "
                    f"{datetime.now(UTC).isoformat()}\n"
                )
            except OSError as exc:
                logger.warning("Could not write extraction marker %s: %s", marker, exc)
            return sqlite_path

        logger.warning(
            "Extracted ChEMBL database failed validation (attempt %s/%s) -- deleting "
            "%s and re-extracting from the (already-validated) tarball",
            attempt, EXTRACTION_VALIDATION_MAX_ATTEMPTS, extract_dir,
        )
        shutil.rmtree(extract_dir, ignore_errors=True)

    raise RuntimeError(
        f"Extracted ChEMBL database still failed validation after "
        f"{EXTRACTION_VALIDATION_MAX_ATTEMPTS} attempts"
    )


def _terminate_stale_backends(settings: Settings, table: str) -> None:
    query = """
        SELECT pid, state, wait_event_type, now() - xact_start AS xact_age, query
        FROM pg_stat_activity
        WHERE pid <> pg_backend_pid()
          AND datname = current_database()
          AND query ILIKE %s
          AND (
              wait_event_type = 'Client'
              OR EXTRACT(EPOCH FROM (now() - xact_start)) > %s
          )
    """
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (f"%raw.{table}%", STALE_BACKEND_MAX_AGE_SECONDS))
            stale = cur.fetchall()
            for pid, state, wait_event_type, xact_age, stale_query in stale:
                logger.warning(
                    "Terminating stale backend pid=%s (state=%s, wait_event_type=%s, "
                    "xact_age=%s) blocking raw.%s: %s",
                    pid, state, wait_event_type, xact_age, table, (stale_query or "").strip()[:200],
                )
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))

    if stale:
        time.sleep(2)


def _fetch_table_batches(
        sqlite_path: str,
        table: str,
        batch_size: int = DEFAULT_BATCH_SIZE
        ) -> Iterator[pa.Table]:
    """
    Yields the table as pa.Table batches using its declared schema.

    A fixed schema keeps all batches type-identical and prevents per-batch type
    inference from causing ParquetWriter failures.
    """
    cols = TABLE_COLUMNS[table]
    schema = TABLE_SCHEMAS[table]
    conn = sqlite3.connect(sqlite_path)
    try:
        cursor = conn.execute(TABLE_QUERIES[table])
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            arrays = [
                pa.array([row[i] for row in rows], type=schema.field(i).type)
                for i in range(len(cols))
            ]
            yield pa.table(arrays, schema=schema)
    finally:
        conn.close()


def _batch_to_csv_text(arrow_table: pa.Table, cols: list[str]) -> tuple[str, int]:
    columns_pylist = [arrow_table.column(c).to_pylist() for c in cols]
    batch_len = len(columns_pylist[0]) if columns_pylist and columns_pylist[0] else 0
    if batch_len == 0:
        return "", 0

    buf = io.StringIO()
    writer = csv.writer(buf)
    for row in zip(*columns_pylist, strict=True):
        writer.writerow(["\\N" if v is None else v for v in row])
    return buf.getvalue(), batch_len


def _copy_batch_with_retry(settings: Settings, table: str, cols: list[str], csv_text: str) -> None:
    cols_sql = ", ".join(cols)
    last_exc: Exception | None = None
    for attempt in range(1, DB_STEP_MAX_ATTEMPTS + 1):
        try:
            with get_connection(settings) as conn:
                with conn.cursor() as cur:
                    cur.copy_expert(
                        f"COPY raw.{table} ({cols_sql}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')",
                        io.StringIO(csv_text),
                    )
            return
        except _TRANSIENT_DB_ERRORS as exc:
            last_exc = exc
            logger.warning(
                "DB connection dropped mid-COPY for raw.%s (attempt %s/%s): %s",
                table, attempt, DB_STEP_MAX_ATTEMPTS, exc,
            )
            if attempt < DB_STEP_MAX_ATTEMPTS:
                time.sleep(DB_STEP_RETRY_DELAY_SECONDS)
    raise RuntimeError(
        f"Failed to COPY a batch into raw.{table} after {DB_STEP_MAX_ATTEMPTS} attempts"
    ) from last_exc


def _delete_table(settings: Settings, table: str) -> None:
    _terminate_stale_backends(settings, table)
    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM raw.{table}")


def _ingest_table(settings: Settings, sqlite_path: str, table: str) -> int:
    cols = TABLE_COLUMNS[table]
    _delete_table(settings, table)

    n_rows = 0
    batch_num = 0
    tmp_path = None
    writer: pq.ParquetWriter | None = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_path = tmp.name

        for arrow_table in _fetch_table_batches(sqlite_path, table):
            if arrow_table.num_rows == 0:
                continue

            if writer is None:
                writer = pq.ParquetWriter(tmp_path, arrow_table.schema)
            writer.write_table(arrow_table)

            csv_text, batch_len = _batch_to_csv_text(arrow_table, cols)
            if batch_len == 0:
                continue

            batch_num += 1
            batch_started = time.monotonic()
            _copy_batch_with_retry(settings, table, cols, csv_text)
            batch_seconds = time.monotonic() - batch_started
            n_rows += batch_len
            logger.info(
                "raw.%s: batch %s COPYed %s rows in %.1fs (%s rows so far)",
                table, batch_num, batch_len, batch_seconds, n_rows,
            )
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        pq.write_table(pa.table({c: pa.array([]) for c in cols}), tmp_path)

    s3_key = f"{settings.bronze_prefix}/{table}.parquet"
    client = get_s3_client(settings)
    client.upload_file(tmp_path, settings.s3_bucket, s3_key)
    os.remove(tmp_path)

    return n_rows


def ingest_bronze(settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    started_at = datetime.now(UTC)
    version = chembl_downloader.latest()

    with get_connection(settings) as conn:
        logger.info("DWH connection OK, downloading ChEMBL SQLite dump (version %s)", version)
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO raw.ingestion_runs (chembl_release, s3_bronze_prefix, started_at) "
                "VALUES (%s, %s, %s) RETURNING run_id",
                (version, settings.bronze_prefix, started_at),
            )
            run_id = cur.fetchone()[0]

    _download_chembl_dump_with_progress(version)
    sqlite_path = _extract_chembl_sqlite_with_progress(version)
    logger.info("ChEMBL SQLite dump ready, starting per-table ingest")
    row_counts: dict[str, int] = {}

    for table in REQUIRED_TABLES:
        logger.info("Ingesting %s", table)
        n_rows = _ingest_table(settings, sqlite_path, table)
        row_counts[table] = n_rows
        logger.info("Loaded %s rows into raw.%s (also wrote to S3)", n_rows, table)

    with get_connection(settings) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE raw.ingestion_runs SET finished_at = %s, status = 'success', row_counts = %s "
                "WHERE run_id = %s",
                (datetime.now(UTC), Json(row_counts), run_id),
            )

    return row_counts
