"""Tests for the Bronze ingestion stage."""

import contextlib
import sqlite3
import tarfile
import threading
from collections import namedtuple

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from workers.pipeline import ingest
from workers.pipeline.config import Settings
from workers.pipeline.ingest import (
    REQUIRED_TABLES,
    TABLE_COLUMNS,
    TABLE_QUERIES,
    TABLE_SCHEMAS,
    _assert_enough_disk,
    _batch_to_csv_text,
    _copy_batch_with_retry,
    _delete_table,
    _dir_size_bytes,
    _download_chembl_dump_with_progress,
    _download_once_resuming,
    _extract_chembl_sqlite_with_progress,
    _fetch_table_batches,
    _ingest_table,
    _is_valid_chembl_tarball,
    _is_valid_sqlite_db,
    _remote_size,
    _run_extraction_with_progress,
    _terminate_stale_backends,
    ingest_bronze,
)

MODULE = "workers.pipeline.ingest"

DiskUsage = namedtuple("DiskUsage", "total used free")
GIB = 1024**3


@pytest.fixture
def settings():
    return Settings(s3_bucket="test-bucket", dwh_url="postgresql://x", s3_prefix="test/prefix")


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    """Retry delays are behaviour under test; waiting them out is not."""
    monkeypatch.setattr(ingest.time, "sleep", lambda _seconds: None)


def test_every_required_table_is_fully_described():
    assert set(TABLE_COLUMNS) == set(REQUIRED_TABLES)
    assert set(TABLE_QUERIES) == set(REQUIRED_TABLES)
    assert set(TABLE_SCHEMAS) == set(REQUIRED_TABLES)


@pytest.mark.parametrize("table", REQUIRED_TABLES)
def test_declared_columns_match_the_arrow_schema(table):
    assert TABLE_COLUMNS[table] == [field.name for field in TABLE_SCHEMAS[table]]


@pytest.mark.parametrize("table", REQUIRED_TABLES)
def test_every_table_is_keyed_by_chembl_id(table):
    # The DDL makes chembl_id the primary key of all four raw tables, and the
    # downstream joins are on chembl_id rather than molregno.
    assert TABLE_COLUMNS[table][0] == "chembl_id"


def test_dimension_properties_survive_ingestion():
    # Deliverable 6a names these columns; they can only reach the mart if
    # Bronze carries them.
    required = {
        "chembl_id", "mw_freebase", "alogp", "psa", "cx_logp",
        "molecular_species", "full_mwt", "aromatic_rings", "heavy_atoms",
    }
    assert required.issubset(set(TABLE_COLUMNS["compound_properties"]))
    assert "molecule_type" in TABLE_COLUMNS["molecule_dictionary"]


@pytest.fixture
def chembl_sqlite(tmp_path):
    """A miniature ChEMBL dump with the source schema the queries expect."""
    path = tmp_path / "chembl.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE chembl_id_lookup (
            chembl_id TEXT, entity_type TEXT, entity_id INTEGER, status TEXT);
        CREATE TABLE molecule_dictionary (
            molregno INTEGER, chembl_id TEXT, molecule_type TEXT, pref_name TEXT,
            max_phase REAL, therapeutic_flag INTEGER, withdrawn_flag INTEGER);
        CREATE TABLE compound_properties (
            molregno INTEGER, mw_freebase REAL, alogp REAL, psa REAL,
            full_mwt REAL, aromatic_rings INTEGER, heavy_atoms INTEGER);
        CREATE TABLE compound_structures (
            molregno INTEGER, canonical_smiles TEXT, standard_inchi TEXT,
            standard_inchi_key TEXT);

        INSERT INTO chembl_id_lookup VALUES
            ('CHEMBL1', 'COMPOUND', 1, 'ACTIVE'),
            ('CHEMBL2', 'COMPOUND', 2, 'ACTIVE'),
            ('CHEMBL3', 'COMPOUND', 3, 'OBS'),
            ('CHEMBL1000', 'ASSAY', 10, 'ACTIVE'),
            ('CHEMBL2000', 'TARGET', 20, 'ACTIVE');

        INSERT INTO molecule_dictionary VALUES
            (1, 'CHEMBL1', 'Small molecule', 'ASPIRIN', 4.0, 1, 0),
            (2, 'CHEMBL2', 'Small molecule', NULL, NULL, 0, 0),
            (3, 'CHEMBL3', 'Protein', NULL, NULL, 0, 0);

        INSERT INTO compound_properties VALUES
            (1, 180.16, 1.31, 63.6, 180.16, 1, 13),
            (2, NULL, NULL, NULL, NULL, NULL, NULL);

        INSERT INTO compound_structures VALUES
            (1, 'CC(=O)Oc1ccccc1C(=O)O', 'InChI=1S/C9', 'BSYNRYMUTXBXSQ'),
            (2, 'CCO', NULL, NULL),
            (99, 'CCC', NULL, NULL);
        """
    )
    conn.commit()
    conn.close()
    return str(path)


@pytest.fixture
def empty_sqlite(tmp_path):
    """A dump whose tables exist but hold no rows."""
    path = tmp_path / "empty.db"
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE chembl_id_lookup "
        "(chembl_id TEXT, entity_type TEXT, entity_id INTEGER, status TEXT)"
    )
    conn.commit()
    conn.close()
    return str(path)


def _all_rows(sqlite_path, table, batch_size=1000):
    batches = list(_fetch_table_batches(sqlite_path, table, batch_size))
    return pa.concat_tables(batches).to_pylist() if batches else []


@pytest.mark.parametrize("table", REQUIRED_TABLES)
def test_batches_carry_the_declared_schema(chembl_sqlite, table):
    # A fixed schema per batch is what stops per-batch type inference from
    # breaking the ParquetWriter partway through a table.
    for batch in _fetch_table_batches(chembl_sqlite, table):
        assert batch.schema == TABLE_SCHEMAS[table]


def test_id_lookup_keeps_only_compound_entities(chembl_sqlite):
    rows = _all_rows(chembl_sqlite, "chembl_id_lookup")

    assert {row["chembl_id"] for row in rows} == {"CHEMBL1", "CHEMBL2", "CHEMBL3"}
    assert {row["entity_type"] for row in rows} == {"COMPOUND"}


def test_obsolete_compound_ids_are_still_ingested(chembl_sqlite):
    # The filter is on entity_type, not status: CHEMBL3 is an OBS compound and
    # is expected to survive.
    statuses = {row["chembl_id"]: row["status"] for row in _all_rows(chembl_sqlite, "chembl_id_lookup")}

    assert statuses["CHEMBL3"] == "OBS"


def test_properties_are_resolved_to_chembl_id(chembl_sqlite):
    rows = {row["chembl_id"]: row for row in _all_rows(chembl_sqlite, "compound_properties")}

    assert set(rows) == {"CHEMBL1", "CHEMBL2"}
    assert rows["CHEMBL1"]["alogp"] == pytest.approx(1.31)
    assert rows["CHEMBL2"]["alogp"] is None


def test_cx_logp_and_molecular_species_are_null_by_design(chembl_sqlite):
    # ChEMBL stopped shipping the ChemAxon properties after release 35, so the
    # columns are kept for shape and selected as NULL.
    for row in _all_rows(chembl_sqlite, "compound_properties"):
        assert row["cx_logp"] is None
        assert row["molecular_species"] is None


def test_structures_without_a_molecule_row_are_dropped(chembl_sqlite):
    # molregno 99 has a structure but no molecule_dictionary entry, so it has
    # no chembl_id to key on.
    rows = _all_rows(chembl_sqlite, "compound_structures")

    assert {row["chembl_id"] for row in rows} == {"CHEMBL1", "CHEMBL2"}


def test_rows_are_yielded_in_batches_of_the_requested_size(chembl_sqlite):
    batches = list(_fetch_table_batches(chembl_sqlite, "chembl_id_lookup", batch_size=2))

    assert [batch.num_rows for batch in batches] == [2, 1]


def test_batching_does_not_change_the_result(chembl_sqlite):
    one_at_a_time = _all_rows(chembl_sqlite, "chembl_id_lookup", 1)

    assert one_at_a_time == _all_rows(chembl_sqlite, "chembl_id_lookup", 1000)


def test_an_empty_table_yields_no_batches(empty_sqlite):
    assert list(_fetch_table_batches(empty_sqlite, "chembl_id_lookup")) == []


def test_the_sqlite_connection_is_closed_even_if_the_caller_stops_early(chembl_sqlite):
    batches = _fetch_table_batches(chembl_sqlite, "chembl_id_lookup", batch_size=1)
    next(batches)
    batches.close() # generator close runs the finally block

    # If the connection had leaked, this exclusive lock would fail.
    conn = sqlite3.connect(chembl_sqlite)
    conn.execute("BEGIN EXCLUSIVE")
    conn.close()

def test_null_becomes_the_copy_null_marker():
    table = pa.table({"chembl_id": ["CHEMBL1"], "alogp": [None]})

    text, n_rows = _batch_to_csv_text(table, ["chembl_id", "alogp"])

    assert text == "CHEMBL1,\\N\r\n"
    assert n_rows == 1


def test_values_containing_the_delimiter_are_quoted():
    table = pa.table({"chembl_id": ["CHEMBL1"], "pref_name": ["ASPIRIN, ACETYLSALICYLIC"]})

    text, _ = _batch_to_csv_text(table, ["chembl_id", "pref_name"])

    assert text == 'CHEMBL1,"ASPIRIN, ACETYLSALICYLIC"\r\n'


def test_embedded_quotes_are_doubled():
    table = pa.table({"chembl_id": ["CHEMBL1"], "pref_name": ['SAID "HI"']})

    text, _ = _batch_to_csv_text(table, ["chembl_id", "pref_name"])

    assert text == 'CHEMBL1,"SAID ""HI"""\r\n'


def test_embedded_newlines_are_quoted_not_treated_as_row_breaks():
    table = pa.table({"chembl_id": ["CHEMBL1"], "pref_name": ["TWO\nLINES"]})

    text, n_rows = _batch_to_csv_text(table, ["chembl_id", "pref_name"])

    assert text == 'CHEMBL1,"TWO\nLINES"\r\n'
    assert n_rows == 1


def test_backslashes_in_smiles_survive_unescaped():
    # Stereochemistry uses backslashes. CSV mode gives backslash no special
    # meaning, so C/C=C\\N must reach Postgres intact rather than as a NULL.
    table = pa.table({"chembl_id": ["CHEMBL1"], "canonical_smiles": ["C/C=C\\N"]})

    text, _ = _batch_to_csv_text(table, ["chembl_id", "canonical_smiles"])

    assert text == "CHEMBL1,C/C=C\\N\r\n"


def test_a_value_that_is_exactly_the_null_marker_is_indistinguishable_from_null():
    # Documents a real ambiguity: an unquoted field equal to \\N is read back
    # as NULL. No ChEMBL column is known to hold that literal, but the encoding
    # cannot tell the two apart, so a future column that can must not use it.
    literal = pa.table({"chembl_id": ["CHEMBL1"], "pref_name": ["\\N"]})
    actual_null = pa.table({"chembl_id": ["CHEMBL1"], "pref_name": [None]})

    assert _batch_to_csv_text(literal, ["chembl_id", "pref_name"])[0] == \
        _batch_to_csv_text(actual_null, ["chembl_id", "pref_name"])[0]


def test_columns_are_written_in_the_order_requested_not_table_order():
    table = pa.table({"b": ["second"], "a": ["first"]})

    text, _ = _batch_to_csv_text(table, ["a", "b"])

    assert text == "first,second\r\n"


def test_an_empty_batch_produces_no_payload():
    table = pa.table({"chembl_id": pa.array([], type=pa.string())})

    assert _batch_to_csv_text(table, ["chembl_id"]) == ("", 0)


def test_row_count_matches_the_lines_written():
    table = pa.table({"chembl_id": ["A", "B", "C"], "status": ["x", None, "z"]})

    text, n_rows = _batch_to_csv_text(table, ["chembl_id", "status"])

    assert n_rows == 3
    assert text.count("\r\n") == 3


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params=None):
        self._conn.executed.append((" ".join(str(query).split()), params))

    def copy_expert(self, statement, stream):
        self._conn.copies.append((statement, stream.read()))
        if self._conn.fail_copies > 0:
            self._conn.fail_copies -= 1
            raise psycopg2.OperationalError("server closed the connection unexpectedly")

    def fetchall(self):
        return self._conn.rows

    def fetchone(self):
        return self._conn.rows[0] if self._conn.rows else None


class _FakeConn:
    def __init__(self, rows=(), fail_copies=0):
        self.rows = list(rows)
        self.fail_copies = fail_copies
        self.executed = []
        self.copies = []

    def cursor(self):
        return _FakeCursor(self)


def _patch_connection(monkeypatch, conn):
    @contextlib.contextmanager
    def fake_get_connection(_settings):
        yield conn

    monkeypatch.setattr(f"{MODULE}.get_connection", fake_get_connection)
    return conn


def test_copy_statement_names_the_table_columns_and_null_marker(monkeypatch, settings):
    conn = _patch_connection(monkeypatch, _FakeConn())

    _copy_batch_with_retry(settings, "compound_structures", ["chembl_id", "canonical_smiles"], "A,B\r\n")

    statement, payload = conn.copies[0]
    assert "COPY raw.compound_structures (chembl_id, canonical_smiles)" in statement
    assert "FORMAT CSV, NULL '\\N'" in statement
    assert payload == "A,B\r\n"


def test_a_dropped_connection_mid_copy_is_retried(monkeypatch, settings):
    conn = _patch_connection(monkeypatch, _FakeConn(fail_copies=2))

    _copy_batch_with_retry(settings, "molecule_dictionary", ["chembl_id"], "A\r\n")

    assert len(conn.copies) == 3 # two failures, then success


def test_copy_gives_up_after_the_attempt_limit(monkeypatch, settings):
    conn = _patch_connection(monkeypatch, _FakeConn(fail_copies=99))

    with pytest.raises(RuntimeError, match="after 3 attempts"):
        _copy_batch_with_retry(settings, "molecule_dictionary", ["chembl_id"], "A\r\n")

    assert len(conn.copies) == ingest.DB_STEP_MAX_ATTEMPTS


def test_a_non_transient_error_is_not_retried(monkeypatch, settings):
    class _Boom(_FakeConn):
        def cursor(self):
            raise psycopg2.ProgrammingError("relation raw.x does not exist")

    _patch_connection(monkeypatch, _Boom())

    with pytest.raises(psycopg2.ProgrammingError):
        _copy_batch_with_retry(settings, "molecule_dictionary", ["chembl_id"], "A\r\n")


def test_stale_backends_holding_the_table_are_terminated(monkeypatch, settings):
    conn = _patch_connection(
        monkeypatch,
        _FakeConn(rows=[(101, "idle in transaction", "Client", "00:20:00", "COPY raw.compound_structures")]),
    )

    _terminate_stale_backends(settings, "compound_structures")

    terminations = [params for query, params in conn.executed if "pg_terminate_backend" in query]
    assert terminations == [(101,)]


def test_nothing_is_terminated_when_no_backend_is_stale(monkeypatch, settings):
    conn = _patch_connection(monkeypatch, _FakeConn(rows=[]))

    _terminate_stale_backends(settings, "compound_structures")

    assert not [query for query, _ in conn.executed if "pg_terminate_backend" in query]


def test_delete_clears_the_table_after_clearing_blockers(monkeypatch, settings):
    conn = _patch_connection(monkeypatch, _FakeConn())
    calls = []
    monkeypatch.setattr(f"{MODULE}._terminate_stale_backends", lambda _s, table: calls.append(table))

    _delete_table(settings, "compound_properties")

    assert calls == ["compound_properties"]
    assert ("DELETE FROM raw.compound_properties", None) in conn.executed


class _FakeS3:
    def __init__(self):
        self.uploads = []

    def upload_file(self, filename, bucket, key):
        # Read the parquet now: _ingest_table deletes the temp file straight after.
        self.uploads.append((bucket, key, pq.read_table(filename)))


@pytest.fixture
def ingest_table_harness(monkeypatch):
    """Stubs out the database and S3 so _ingest_table's own logic is visible."""
    s3 = _FakeS3()
    copied = []
    deleted = []

    monkeypatch.setattr(f"{MODULE}.get_s3_client", lambda _settings: s3)
    monkeypatch.setattr(f"{MODULE}._delete_table", lambda _s, table: deleted.append(table))
    monkeypatch.setattr(
        f"{MODULE}._copy_batch_with_retry",
        lambda _s, table, cols, csv_text: copied.append((table, csv_text)),
    )
    return s3, copied, deleted


def test_ingest_table_reports_the_rows_it_loaded(settings, chembl_sqlite, ingest_table_harness):
    _, copied, deleted = ingest_table_harness

    n_rows = _ingest_table(settings, chembl_sqlite, "chembl_id_lookup")

    assert n_rows == 3
    assert deleted == ["chembl_id_lookup"]  # existing rows cleared before loading
    assert len(copied) == 1


@pytest.fixture
def small_batches(monkeypatch):
    """
    Forces multi-batch behaviour.

    _ingest_table calls _fetch_table_batches with no batch size, and that
    parameter binds DEFAULT_BATCH_SIZE as a default at import time, so patching
    the constant has no effect. Wrapping the generator is the only seam.
    """
    real = ingest._fetch_table_batches
    monkeypatch.setattr(
        f"{MODULE}._fetch_table_batches",
        lambda path, table, batch_size=2: real(path, table, batch_size),
    )


def test_batch_size_is_fixed_at_import_time(chembl_sqlite, monkeypatch):
    # Documents why small_batches exists, and a real limitation: the batch size
    # is bound as a function default when the module loads, so rebinding the
    # constant later cannot change it. Batching is not tunable at runtime.
    monkeypatch.setattr(f"{MODULE}.DEFAULT_BATCH_SIZE", 1)

    batches = list(_fetch_table_batches(chembl_sqlite, "chembl_id_lookup"))

    assert [batch.num_rows for batch in batches] == [3]  # not [1, 1, 1]


def test_ingest_table_copies_once_per_batch(settings, chembl_sqlite, ingest_table_harness, small_batches):
    _, copied, _ = ingest_table_harness

    n_rows = _ingest_table(settings, chembl_sqlite, "chembl_id_lookup")

    assert n_rows == 3
    assert len(copied) == 2


def test_ingest_table_uploads_the_whole_table_to_the_bronze_prefix(
    settings, chembl_sqlite, ingest_table_harness, small_batches
):
    s3, _, _ = ingest_table_harness

    _ingest_table(settings, chembl_sqlite, "chembl_id_lookup")

    bucket, key, table = s3.uploads[0]
    assert bucket == "test-bucket"
    assert key == "test/prefix/bronze/chembl_id_lookup.parquet"
    # Every batch reaches the parquet, not just the last one.
    assert table.num_rows == 3
    assert table.schema == TABLE_SCHEMAS["chembl_id_lookup"]


def test_parquet_and_postgres_receive_the_same_rows(settings, chembl_sqlite, ingest_table_harness):
    s3, copied, _ = ingest_table_harness

    _ingest_table(settings, chembl_sqlite, "compound_structures")

    _, _, table = s3.uploads[0]
    copied_lines = sum(text.count("\r\n") for _, text in copied)
    assert table.num_rows == copied_lines


def test_an_empty_source_table_still_produces_an_object(settings, empty_sqlite, ingest_table_harness):
    s3, copied, _ = ingest_table_harness

    n_rows = _ingest_table(settings, empty_sqlite, "chembl_id_lookup")

    assert n_rows == 0
    assert copied == []
    assert s3.uploads[0][2].num_rows == 0


def test_empty_table_parquet_keeps_the_declared_column_names(settings, empty_sqlite, ingest_table_harness):
    # Known wart: the empty-table fallback builds its own table, so the column
    # types are inferred rather than taken from TABLE_SCHEMAS. The names must at
    # least match, or a reader union-ing Bronze objects breaks.
    s3, _, _ = ingest_table_harness

    _ingest_table(settings, empty_sqlite, "chembl_id_lookup")

    assert s3.uploads[0][2].column_names == TABLE_COLUMNS["chembl_id_lookup"]


def test_the_temp_parquet_is_not_left_behind(
    settings, chembl_sqlite, ingest_table_harness, tmp_path, monkeypatch
):
    monkeypatch.setattr(ingest.tempfile, "tempdir", str(tmp_path))

    _ingest_table(settings, chembl_sqlite, "chembl_id_lookup")

    assert list(tmp_path.glob("*.parquet")) == []


@pytest.fixture
def bronze_run(monkeypatch):
    conn = _FakeConn(rows=[(7,)])
    _patch_connection(monkeypatch, conn)
    monkeypatch.setattr(ingest.chembl_downloader, "latest", lambda: "36")
    monkeypatch.setattr(f"{MODULE}._download_chembl_dump_with_progress", lambda _v: None)
    monkeypatch.setattr(f"{MODULE}._extract_chembl_sqlite_with_progress", lambda _v: "/tmp/chembl.db")
    return conn


def test_ingest_bronze_loads_every_required_table(settings, bronze_run, monkeypatch):
    ingested = []
    monkeypatch.setattr(
        f"{MODULE}._ingest_table",
        lambda _s, _path, table: ingested.append(table) or 10,
    )

    counts = ingest_bronze(settings)

    assert ingested == REQUIRED_TABLES
    assert counts == dict.fromkeys(REQUIRED_TABLES, 10)


def test_ingest_bronze_records_the_release_and_the_row_counts(settings, bronze_run, monkeypatch):
    monkeypatch.setattr(f"{MODULE}._ingest_table", lambda _s, _path, _table: 5)

    ingest_bronze(settings)

    executed = bronze_run.executed
    insert = next(p for q, p in executed if "INSERT INTO raw.ingestion_runs" in q)
    update = next(p for q, p in executed if "UPDATE raw.ingestion_runs" in q)
    assert insert[0] == "36"
    assert insert[1] == settings.bronze_prefix
    assert update[-1] == 7  # the run_id returned by the INSERT
    assert update[1].adapted == dict.fromkeys(REQUIRED_TABLES, 5)


def test_a_failed_table_leaves_the_run_unfinished(settings, bronze_run, monkeypatch):
    def explode(_s, _path, table):
        if table == "compound_properties":
            raise RuntimeError("COPY failed")
        return 1

    monkeypatch.setattr(f"{MODULE}._ingest_table", explode)

    with pytest.raises(RuntimeError, match="COPY failed"):
        ingest_bronze(settings)

    # No success marker: the run row stays 'running' for an operator to find.
    assert not [query for query, _ in bronze_run.executed if "UPDATE raw.ingestion_runs" in query]


def test_the_dump_is_fetched_before_any_table_is_touched(settings, monkeypatch):
    order = []
    conn = _FakeConn(rows=[(1,)])
    _patch_connection(monkeypatch, conn)
    monkeypatch.setattr(ingest.chembl_downloader, "latest", lambda: "36")
    monkeypatch.setattr(f"{MODULE}._download_chembl_dump_with_progress", lambda _v: order.append("download"))
    monkeypatch.setattr(f"{MODULE}._extract_chembl_sqlite_with_progress",
                        lambda _v: order.append("extract") or "/tmp/chembl.db")
    monkeypatch.setattr(f"{MODULE}._ingest_table", lambda _s, _p, table: order.append(table) or 1)

    ingest_bronze(settings)

    assert order[:2] == ["download", "extract"]


def test_extraction_is_refused_when_the_disk_is_too_small(tmp_path, monkeypatch):
    monkeypatch.setattr(ingest.shutil, "disk_usage", lambda _p: DiskUsage(100 * GIB, 95 * GIB, 5 * GIB))

    with pytest.raises(OSError, match="Not enough free disk"):
        _assert_enough_disk(tmp_path, 50 * GIB)


def test_extraction_proceeds_when_there_is_room(tmp_path, monkeypatch):
    monkeypatch.setattr(ingest.shutil, "disk_usage", lambda _p: DiskUsage(100 * GIB, 10 * GIB, 90 * GIB))

    _assert_enough_disk(tmp_path, 50 * GIB)  # no exception


def test_disk_is_measured_on_the_nearest_existing_parent(tmp_path, monkeypatch):
    probed = []

    def fake_usage(path):
        probed.append(path)
        return DiskUsage(100 * GIB, 10 * GIB, 90 * GIB)

    monkeypatch.setattr(ingest.shutil, "disk_usage", fake_usage)

    # The extraction directory does not exist yet; its ancestor does.
    _assert_enough_disk(tmp_path / "chembl" / "36" / "data", 1)

    assert probed == [tmp_path]


def _make_tarball(path, members):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as tar:
        for name, body in members.items():
            source = path.parent / name.replace("/", "_")
            source.write_bytes(body)
            tar.add(source, arcname=name)


def test_a_tarball_containing_the_database_is_valid(tmp_path):
    archive = tmp_path / "chembl_36_sqlite.tar.gz"
    _make_tarball(archive, {"chembl_36/chembl_36_sqlite/chembl_36.db": b"sqlite bytes"})

    assert _is_valid_chembl_tarball(archive) is True


def test_a_readable_tarball_without_a_database_is_rejected(tmp_path):
    archive = tmp_path / "wrong.tar.gz"
    _make_tarball(archive, {"chembl_36/README.txt": b"not the dump"})

    assert _is_valid_chembl_tarball(archive) is False


def test_a_truncated_download_is_rejected(tmp_path):
    archive = tmp_path / "truncated.tar.gz"
    archive.write_bytes(b"\x1f\x8b\x08\x00 truncated gzip stream")

    assert _is_valid_chembl_tarball(archive) is False


def test_a_missing_archive_is_rejected(tmp_path):
    assert _is_valid_chembl_tarball(tmp_path / "absent.tar.gz") is False


def test_a_database_with_the_expected_table_is_valid(chembl_sqlite):
    assert _is_valid_sqlite_db(chembl_sqlite) is True


def test_a_database_missing_the_expected_table_is_rejected(tmp_path):
    path = tmp_path / "other.db"
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE something_else (x INTEGER)")
    conn.commit()
    conn.close()

    assert _is_valid_sqlite_db(str(path)) is False


def test_a_file_that_is_not_a_database_is_rejected(tmp_path):
    path = tmp_path / "notadb.db"
    path.write_text("this is plain text, not SQLite")

    assert _is_valid_sqlite_db(str(path)) is False


class _FakeResponse:
    def __init__(self, body=b"", status=200, headers=None):
        self._body = body
        self._offset = 0
        self.status = status
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self, size):
        chunk = self._body[self._offset:self._offset + size]
        self._offset += len(chunk)
        return chunk


def _patch_urlopen(monkeypatch, responses):
    """Serve the given responses in order, recording each Request."""
    requests = []
    queue = list(responses)

    def fake_urlopen(request, timeout=None):
        requests.append(request)
        result = queue.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(ingest.urllib.request, "urlopen", fake_urlopen)
    return requests


def test_remote_size_reads_the_content_length(monkeypatch):
    _patch_urlopen(monkeypatch, [_FakeResponse(headers={"Content-Length": "1234"})])

    assert _remote_size("https://example.invalid/chembl.tar.gz") == 1234


def test_remote_size_is_unknown_when_the_server_omits_it(monkeypatch):
    _patch_urlopen(monkeypatch, [_FakeResponse(headers={})])

    assert _remote_size("https://example.invalid/chembl.tar.gz") is None


def test_remote_size_is_unknown_when_the_head_request_fails(monkeypatch):
    _patch_urlopen(monkeypatch, [OSError("connection reset")])

    assert _remote_size("https://example.invalid/chembl.tar.gz") is None


def test_a_fresh_download_writes_the_whole_body(tmp_path, monkeypatch):
    partial = tmp_path / "chembl.tar.gz.part"
    _patch_urlopen(monkeypatch, [_FakeResponse(b"0123456789")])

    written = _download_once_resuming("https://example.invalid/f", partial, 10)

    assert written == 10
    assert partial.read_bytes() == b"0123456789"


def test_a_complete_file_is_not_downloaded_again(tmp_path, monkeypatch):
    partial = tmp_path / "chembl.tar.gz.part"
    partial.write_bytes(b"0123456789")
    requests = _patch_urlopen(monkeypatch, [])

    written = _download_once_resuming("https://example.invalid/f", partial, 10)

    assert written == 10
    assert requests == [] # the network was never touched


def test_an_interrupted_download_resumes_with_a_range_request(tmp_path, monkeypatch):
    partial = tmp_path / "chembl.tar.gz.part"
    partial.write_bytes(b"01234")
    requests = _patch_urlopen(monkeypatch, [_FakeResponse(b"56789", status=206)])

    written = _download_once_resuming("https://example.invalid/f", partial, 10)

    assert requests[0].get_header("Range") == "bytes=5-"
    assert written == 10
    assert partial.read_bytes() == b"0123456789"


def test_a_server_that_ignores_range_restarts_instead_of_appending(tmp_path, monkeypatch):
    # The regression this guards: appending a full body onto a partial file
    # yields a corrupt archive that still looks complete by size.
    partial = tmp_path / "chembl.tar.gz.part"
    partial.write_bytes(b"01234")
    _patch_urlopen(monkeypatch, [_FakeResponse(b"0123456789", status=200)])

    written = _download_once_resuming("https://example.invalid/f", partial, 10)

    assert written == 10
    assert partial.read_bytes() == b"0123456789"


@pytest.fixture
def download_harness(tmp_path, monkeypatch):
    """Isolates the cache directory and stubs the transfer itself."""
    monkeypatch.setattr(f"{MODULE}.PYSTOW_HOME", tmp_path)
    monkeypatch.setattr(f"{MODULE}._is_valid_chembl_tarball", lambda _path: True)
    return tmp_path / "chembl" / "36"


def test_a_cached_dump_of_the_right_size_is_not_re_downloaded(download_harness, monkeypatch):
    download_harness.mkdir(parents=True)
    (download_harness / "chembl_36_sqlite.tar.gz").write_bytes(b"cached")
    monkeypatch.setattr(f"{MODULE}._remote_size", lambda _url: 6)
    monkeypatch.setattr(f"{MODULE}._download_once_resuming", lambda *a: pytest.fail("downloaded anyway"))

    _download_chembl_dump_with_progress("36")


def test_a_cached_dump_of_the_wrong_size_is_replaced(download_harness, monkeypatch):
    download_harness.mkdir(parents=True)
    target = download_harness / "chembl_36_sqlite.tar.gz"
    target.write_bytes(b"stale")
    monkeypatch.setattr(f"{MODULE}._remote_size", lambda _url: 6)

    def fake_download(_url, partial_path, _total):
        partial_path.write_bytes(b"fresh!")
        return 6

    monkeypatch.setattr(f"{MODULE}._download_once_resuming", fake_download)

    _download_chembl_dump_with_progress("36")

    assert target.read_bytes() == b"fresh!"


def test_the_release_specific_url_is_requested(download_harness, monkeypatch):
    urls = []
    monkeypatch.setattr(f"{MODULE}._remote_size", lambda url: urls.append(url) or 6)
    monkeypatch.setattr(f"{MODULE}._download_once_resuming",
                        lambda _u, partial, _t: (partial.write_bytes(b"fresh!"), 6)[1])

    _download_chembl_dump_with_progress("36")

    assert urls == [f"{ingest.CHEMBL_BASE}/chembl_36/chembl_36_sqlite.tar.gz"]


def test_a_flaky_transfer_is_retried_and_the_partial_file_is_kept(download_harness, monkeypatch):
    monkeypatch.setattr(f"{MODULE}._remote_size", lambda _url: 6)
    attempts = []

    def flaky(_url, partial_path, _total):
        attempts.append(1)
        if len(attempts) < 3:
            partial_path.write_bytes(b"par")  # progress survives the failure
            raise OSError("connection reset by peer")
        partial_path.write_bytes(b"fresh!")
        return 6

    monkeypatch.setattr(f"{MODULE}._download_once_resuming", flaky)

    _download_chembl_dump_with_progress("36")

    assert len(attempts) == 3
    assert (download_harness / "chembl_36_sqlite.tar.gz").read_bytes() == b"fresh!"


def test_download_gives_up_after_the_attempt_limit(download_harness, monkeypatch):
    monkeypatch.setattr(f"{MODULE}.DOWNLOAD_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(f"{MODULE}._remote_size", lambda _url: 6)
    attempts = []

    def always_fails(_url, _partial, _total):
        attempts.append(1)
        raise OSError("connection reset by peer")

    monkeypatch.setattr(f"{MODULE}._download_once_resuming", always_fails)

    with pytest.raises(OSError, match="after 3 attempts"):
        _download_chembl_dump_with_progress("36")

    assert len(attempts) == 3


def test_a_complete_but_corrupt_download_is_discarded(download_harness, monkeypatch):
    monkeypatch.setattr(f"{MODULE}._remote_size", lambda _url: 6)
    monkeypatch.setattr(f"{MODULE}._is_valid_chembl_tarball", lambda _path: False)
    monkeypatch.setattr(f"{MODULE}._download_once_resuming",
                        lambda _u, partial, _t: (partial.write_bytes(b"junk!!"), 6)[1])

    with pytest.raises(OSError, match="failed archive validation"):
        _download_chembl_dump_with_progress("36")

    # Not resumable, so the partial must be gone rather than resumed onto.
    assert not (download_harness / "chembl_36_sqlite.tar.gz.part").exists()


@pytest.fixture
def extract_harness(tmp_path, monkeypatch):
    monkeypatch.setattr(f"{MODULE}.PYSTOW_HOME", tmp_path)
    monkeypatch.setattr(f"{MODULE}._assert_enough_disk", lambda *_a: None)
    return tmp_path / "chembl" / "36" / "data"


def test_a_successful_extraction_is_marked_complete(extract_harness, monkeypatch):
    monkeypatch.setattr(f"{MODULE}._run_extraction_with_progress", lambda _v, _d: "/tmp/chembl_36.db")
    monkeypatch.setattr(f"{MODULE}._is_valid_sqlite_db", lambda _p: True)

    path = _extract_chembl_sqlite_with_progress("36")

    assert path == "/tmp/chembl_36.db"
    assert (extract_harness / ingest.EXTRACTION_MARKER_NAME).exists()


def test_an_unmarked_leftover_directory_is_wiped_first(extract_harness, monkeypatch):
    extract_harness.mkdir(parents=True)
    (extract_harness / "half_written.db").write_bytes(b"partial")
    monkeypatch.setattr(f"{MODULE}._run_extraction_with_progress", lambda _v, _d: "/tmp/chembl_36.db")
    monkeypatch.setattr(f"{MODULE}._is_valid_sqlite_db", lambda _p: True)

    _extract_chembl_sqlite_with_progress("36")

    assert not (extract_harness / "half_written.db").exists()


def test_a_marked_directory_is_reused_rather_than_wiped(extract_harness, monkeypatch):
    extract_harness.mkdir(parents=True)
    (extract_harness / ingest.EXTRACTION_MARKER_NAME).write_text("done")
    (extract_harness / "chembl_36.db").write_bytes(b"good")
    monkeypatch.setattr(f"{MODULE}._run_extraction_with_progress", lambda _v, _d: "/tmp/chembl_36.db")
    monkeypatch.setattr(f"{MODULE}._is_valid_sqlite_db", lambda _p: True)

    _extract_chembl_sqlite_with_progress("36")

    assert (extract_harness / "chembl_36.db").read_bytes() == b"good"


def test_a_corrupt_extraction_is_retried_then_abandoned(extract_harness, monkeypatch):
    attempts = []
    monkeypatch.setattr(f"{MODULE}._run_extraction_with_progress",
                        lambda _v, _d: attempts.append(1) or "/tmp/chembl_36.db")
    monkeypatch.setattr(f"{MODULE}._is_valid_sqlite_db", lambda _p: False)

    with pytest.raises(RuntimeError, match="failed validation"):
        _extract_chembl_sqlite_with_progress("36")

    assert len(attempts) == ingest.EXTRACTION_VALIDATION_MAX_ATTEMPTS
    assert not (extract_harness / ingest.EXTRACTION_MARKER_NAME).exists()


def test_extraction_returns_the_path_the_worker_produced(tmp_path, monkeypatch):
    monkeypatch.setattr(ingest.chembl_downloader, "download_extract_sqlite",
                        lambda version: f"/data/chembl_{version}.db")

    assert _run_extraction_with_progress("36", tmp_path) == "/data/chembl_36.db"


def test_an_error_inside_the_extraction_thread_reaches_the_caller(tmp_path, monkeypatch):
    monkeypatch.setattr(ingest.chembl_downloader, "download_extract_sqlite",
                        lambda version: (_ for _ in ()).throw(OSError("no space left on device")))

    with pytest.raises(OSError, match="no space left on device"):
        _run_extraction_with_progress("36", tmp_path)


def test_a_hung_extraction_is_abandoned_rather_than_waited_on(tmp_path, monkeypatch):
    release = threading.Event()
    monkeypatch.setattr(f"{MODULE}.EXTRACTION_TIMEOUT_SECONDS", 0)
    monkeypatch.setattr(ingest.chembl_downloader, "download_extract_sqlite",
                        lambda version: release.wait(timeout=30) or "/tmp/chembl.db")

    try:
        with pytest.raises(TimeoutError, match="exceeded"):
            _run_extraction_with_progress("36", tmp_path)
    finally:
        release.set()


def test_extraction_progress_is_measured_from_the_bytes_on_disk(tmp_path):
    (tmp_path / "nested").mkdir()
    (tmp_path / "nested" / "a.db").write_bytes(b"x" * 100)
    (tmp_path / "b.db").write_bytes(b"y" * 23)

    assert _dir_size_bytes(tmp_path) == 123


def test_a_directory_that_does_not_exist_yet_measures_zero(tmp_path):
    assert _dir_size_bytes(tmp_path / "absent") == 0
