import contextlib

import pyarrow as pa
import pytest

from workers.pipeline.config import Settings
from workers.pipeline.mart_load import load_fact_similarity, load_mart_batch

MODULE = "workers.pipeline.mart_load"


class _FakeCursor:
    def __init__(self, rows=()):
        self.executed = []
        self._rows = list(rows)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, *args):
        self.executed.append(args)

    def fetchall(self):
        return self._rows


class _FakeConn:
    def __init__(self, rows=()):
        self.cursor_obj = _FakeCursor(rows)

    def cursor(self):
        return self.cursor_obj


@pytest.fixture
def settings():
    return Settings(s3_bucket="b", dwh_url="postgresql://x", s3_prefix="p")


@pytest.fixture
def top_k_table():
    return pa.table({
        "source_chembl_id": ["CHEMBL1", "CHEMBL1"],
        "target_chembl_id": ["CHEMBL2", "CHEMBL3"],
        "similarity_score": [0.9, 0.8],
        "rank_within_source": [1, 2],
        "has_duplicates_of_last_largest_score": [False, False],
    })


def test_load_fact_similarity_row_shape(monkeypatch, top_k_table):
    captured = {}

    def fake_upsert_rows(conn, table, columns, rows, conflict_cols):
        captured["table"] = table
        captured["columns"] = columns
        captured["rows"] = rows
        captured["conflict_cols"] = conflict_cols
        return len(rows)

    monkeypatch.setattr(f"{MODULE}.upsert_rows", fake_upsert_rows)

    n = load_fact_similarity(_FakeConn(), top_k_table)

    assert n == 2
    assert captured["table"] == "mart.fact_similarity"
    assert captured["conflict_cols"] == ["source_chembl_id", "target_chembl_id"]
    assert captured["rows"][0] == ("CHEMBL1", "CHEMBL2", 0.9, 1, False)


def test_fact_carries_every_column(monkeypatch, top_k_table):
    captured = {}
    monkeypatch.setattr(
        f"{MODULE}.upsert_rows",
        lambda conn, table, columns, rows, conflict_cols: captured.update(columns=columns) or len(rows),
    )

    load_fact_similarity(_FakeConn(), top_k_table)

    for required in (
        "source_chembl_id", "target_chembl_id",
        "similarity_score", "has_duplicates_of_last_largest_score",
    ):
        assert required in captured["columns"]


def test_duplicate_flag_is_preserved_per_row(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        f"{MODULE}.upsert_rows",
        lambda conn, table, columns, rows, conflict_cols: captured.update(rows=rows) or len(rows),
    )

    table = pa.table({
        "source_chembl_id": ["CHEMBL1", "CHEMBL1"],
        "target_chembl_id": ["CHEMBL2", "CHEMBL3"],
        "similarity_score": [0.5, 0.5],
        "rank_within_source": [9, 10],
        "has_duplicates_of_last_largest_score": [True, True],
    })
    load_fact_similarity(_FakeConn(), table)

    assert [row[-1] for row in captured["rows"]] == [True, True]


def test_empty_top_k_table_loads_zero_rows(monkeypatch):
    monkeypatch.setattr(
        f"{MODULE}.upsert_rows",
        lambda conn, table, columns, rows, conflict_cols: len(rows),
    )

    empty = pa.table({
        "source_chembl_id": pa.array([], type=pa.string()),
        "target_chembl_id": pa.array([], type=pa.string()),
        "similarity_score": pa.array([], type=pa.float64()),
        "rank_within_source": pa.array([], type=pa.int64()),
        "has_duplicates_of_last_largest_score": pa.array([], type=pa.bool_()),
    })

    assert load_fact_similarity(_FakeConn(), empty) == 0


def test_dim_is_restricted_to_molecules_referenced_by_the_facts(monkeypatch, settings, top_k_table):
    requested_ids = {}

    def fake_load_dim(conn, chembl_ids):
        requested_ids["ids"] = set(chembl_ids)
        return len(chembl_ids)

    monkeypatch.setattr(f"{MODULE}.load_dim_molecule", fake_load_dim)
    monkeypatch.setattr(f"{MODULE}.load_fact_similarity", lambda conn, t: t.num_rows)

    @contextlib.contextmanager
    def fake_get_connection(_settings):
        yield _FakeConn()

    monkeypatch.setattr(f"{MODULE}.get_connection", fake_get_connection)

    load_mart_batch([top_k_table], settings)

    # sources and targets, and nothing else
    assert requested_ids["ids"] == {"CHEMBL1", "CHEMBL2", "CHEMBL3"}


def test_dim_ids_are_deduplicated_across_sources(monkeypatch, settings):
    requested_ids = {}
    monkeypatch.setattr(
        f"{MODULE}.load_dim_molecule",
        lambda conn, ids: requested_ids.update(ids=set(ids)) or len(ids),
    )
    monkeypatch.setattr(f"{MODULE}.load_fact_similarity", lambda conn, t: t.num_rows)

    @contextlib.contextmanager
    def fake_get_connection(_settings):
        yield _FakeConn()

    monkeypatch.setattr(f"{MODULE}.get_connection", fake_get_connection)

    shared_target = pa.table({
        "source_chembl_id": ["CHEMBL_A"],
        "target_chembl_id": ["CHEMBL_SHARED"],
        "similarity_score": [0.7],
        "rank_within_source": [1],
        "has_duplicates_of_last_largest_score": [False],
    })
    other = pa.table({
        "source_chembl_id": ["CHEMBL_B"],
        "target_chembl_id": ["CHEMBL_SHARED"],
        "similarity_score": [0.6],
        "rank_within_source": [1],
        "has_duplicates_of_last_largest_score": [False],
    })

    load_mart_batch([shared_target, other], settings)

    assert requested_ids["ids"] == {"CHEMBL_A", "CHEMBL_B", "CHEMBL_SHARED"}


def test_batch_load_reports_row_counts(monkeypatch, settings, top_k_table):
    monkeypatch.setattr(f"{MODULE}.load_dim_molecule", lambda conn, ids: len(ids))
    monkeypatch.setattr(f"{MODULE}.load_fact_similarity", lambda conn, t: t.num_rows)

    @contextlib.contextmanager
    def fake_get_connection(_settings):
        yield _FakeConn()

    monkeypatch.setattr(f"{MODULE}.get_connection", fake_get_connection)

    stats = load_mart_batch([top_k_table, top_k_table], settings)

    assert stats == {"dim_molecule_rows": 3, "fact_similarity_rows": 4}
