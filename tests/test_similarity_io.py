"""Tests for the orchestration layer of workers.pipeline.similarity."""

import numpy as np
import pyarrow as pa
import pytest

from workers.pipeline.config import Settings
from workers.pipeline.similarity import (
    TOPK_PREFIX,
    _prune_orphaned_outputs,
    _score_one_source,
    _source_id_from_key,
    compute_similarity_for_all_sources,
    select_top_k,
)

MODULE = "workers.pipeline.similarity"

TEST_PREFIX = "test/prefix"


@pytest.fixture
def settings():
    return Settings(
        s3_bucket="test-bucket",
        dwh_url="postgresql://test:test@localhost:5432/testdb",
        s3_prefix=TEST_PREFIX,
    )


@pytest.fixture
def captured_writes(monkeypatch):
    """Captures every write_parquet call as (key, table)."""
    writes = []

    def fake_write_parquet(settings, table, key):
        writes.append((key, table))
        return f"s3://{settings.s3_bucket}/{key}"

    monkeypatch.setattr(f"{MODULE}.write_parquet", fake_write_parquet)
    return writes


def _packed(rows: list[list[int]]) -> np.ndarray:
    return np.packbits(np.array(rows, dtype=np.uint8), axis=1)


def test_source_id_parsed_from_hive_style_key():
    key = "any/prefix/source_chembl_id=CHEMBL25/full.parquet"
    assert _source_id_from_key(key) == "CHEMBL25"


def test_source_id_is_none_when_partition_segment_absent():
    assert _source_id_from_key("any/prefix/full.parquet") is None


def test_source_id_parsing_tolerates_prefix_depth():
    assert _source_id_from_key("source_chembl_id=CHEMBL1/top10.parquet") == "CHEMBL1"


def test_prune_deletes_only_sources_not_in_keep_set(monkeypatch, settings):
    keys = [
        "any/source_chembl_id=CHEMBL1/full.parquet",
        "any/source_chembl_id=CHEMBL2/full.parquet",
        "any/source_chembl_id=CHEMBL3/full.parquet",
    ]
    deleted = []

    monkeypatch.setattr(f"{MODULE}.list_keys", lambda s, prefix: keys)
    monkeypatch.setattr(f"{MODULE}.delete_keys", lambda s, ks: deleted.extend(ks) or len(ks))

    n = _prune_orphaned_outputs(settings, "any/", keep={"CHEMBL1", "CHEMBL3"})

    assert n == 1
    assert deleted == ["any/source_chembl_id=CHEMBL2/full.parquet"]


def test_prune_is_a_noop_when_nothing_is_orphaned(monkeypatch, settings):
    monkeypatch.setattr(f"{MODULE}.list_keys", lambda s, prefix: ["any/source_chembl_id=CHEMBL1/f.parquet"])

    def explode(*args, **kwargs):
        raise AssertionError("delete_keys must not be called when there are no orphans")

    monkeypatch.setattr(f"{MODULE}.delete_keys", explode)

    assert _prune_orphaned_outputs(settings, "any/", keep={"CHEMBL1"}) == 0


def test_prune_ignores_keys_without_a_source_partition(monkeypatch, settings):
    monkeypatch.setattr(f"{MODULE}.list_keys", lambda s, prefix: ["any/_manifest.json"])
    monkeypatch.setattr(f"{MODULE}.delete_keys", lambda s, ks: len(ks))

    assert _prune_orphaned_outputs(settings, "any/", keep=set()) == 0


def test_score_one_source_excludes_self_from_the_output(settings, captured_writes):
    chembl_ids = ["CHEMBL1", "CHEMBL2", "CHEMBL3"]
    packed = _packed([[1, 1, 0, 0], [1, 0, 0, 0], [0, 0, 1, 1]])

    _score_one_source("CHEMBL1", chembl_ids, packed, 4, settings)

    (key, table), = captured_writes
    assert table.num_rows == 2
    assert table.column("target_chembl_id").to_pylist() == ["CHEMBL2", "CHEMBL3"]
    assert "CHEMBL1" not in table.column("target_chembl_id").to_pylist()


def test_score_one_source_writes_to_the_hive_partitioned_silver_key(settings, captured_writes):
    chembl_ids = ["CHEMBL1", "CHEMBL2"]
    packed = _packed([[1, 1, 0, 0], [1, 0, 0, 0]])

    uri = _score_one_source("CHEMBL1", chembl_ids, packed, 4, settings)

    (key, _), = captured_writes
    assert key == (
        f"{TEST_PREFIX}/silver/similarity/"
        "source_chembl_id=CHEMBL1/full.parquet"
    )
    assert uri == f"s3://test-bucket/{key}"


def test_score_one_source_produces_the_documented_three_column_schema(settings, captured_writes):
    chembl_ids = ["CHEMBL1", "CHEMBL2"]
    packed = _packed([[1, 1, 0, 0], [1, 0, 0, 0]])

    _score_one_source("CHEMBL1", chembl_ids, packed, 4, settings)

    (_, table), = captured_writes
    assert table.column_names == ["source_chembl_id", "target_chembl_id", "similarity_score"]
    assert table.column("source_chembl_id").to_pylist() == ["CHEMBL1"]
    assert table.column("similarity_score")[0].as_py() == pytest.approx(0.5)


def test_score_one_source_raises_a_clear_error_for_an_unknown_source(settings, captured_writes):
    with pytest.raises(ValueError, match="no fingerprint in Silver"):
        _score_one_source("CHEMBL_MISSING", ["CHEMBL1"], _packed([[1, 0, 0, 0]]), 4, settings)


def test_already_present_sources_are_skipped(monkeypatch, settings, captured_writes):
    monkeypatch.setattr(
        f"{MODULE}.list_keys",
        lambda s, prefix: ["any/source_chembl_id=CHEMBL1/full.parquet"],
    )
    monkeypatch.setattr(f"{MODULE}.delete_keys", lambda s, ks: len(ks))

    def explode(*args, **kwargs):
        raise AssertionError("the fingerprint matrix must not be loaded when there is nothing to do")

    monkeypatch.setattr(f"{MODULE}._load_packed_matrix", explode)

    assert compute_similarity_for_all_sources(["CHEMBL1"], settings) == []
    assert captured_writes == []


def test_skip_existing_disabled_recomputes_everything(monkeypatch, settings, captured_writes):
    monkeypatch.setattr(
        f"{MODULE}.list_keys",
        lambda s, prefix: ["any/source_chembl_id=CHEMBL1/full.parquet"],
    )
    monkeypatch.setattr(f"{MODULE}.delete_keys", lambda s, ks: len(ks))
    monkeypatch.setattr(
        f"{MODULE}._load_packed_matrix",
        lambda s: (["CHEMBL1", "CHEMBL2"], _packed([[1, 1, 0, 0], [1, 0, 0, 0]]), 4),
    )

    uris = compute_similarity_for_all_sources(["CHEMBL1"], settings, skip_existing=False)

    assert len(uris) == 1
    assert len(captured_writes) == 1


def test_batch_scoring_matches_single_source_scoring(monkeypatch, settings, captured_writes):
    """The batch path must produce byte-identical output to the single-source path."""
    chembl_ids = ["CHEMBL1", "CHEMBL2", "CHEMBL3"]
    packed = _packed([[1, 1, 0, 0], [1, 0, 0, 0], [0, 0, 1, 1]])

    monkeypatch.setattr(f"{MODULE}.list_keys", lambda s, prefix: [])
    monkeypatch.setattr(f"{MODULE}.delete_keys", lambda s, ks: len(ks))
    monkeypatch.setattr(f"{MODULE}._load_packed_matrix", lambda s: (chembl_ids, packed, 4))

    compute_similarity_for_all_sources(["CHEMBL2"], settings)
    batch_key, batch_table = captured_writes.pop()

    _score_one_source("CHEMBL2", chembl_ids, packed, 4, settings)
    single_key, single_table = captured_writes.pop()

    assert batch_key == single_key
    assert batch_table.equals(single_table)


def _full_similarity_table(n: int) -> pa.Table:
    return pa.table({
        "source_chembl_id": ["CHEMBL1"] * n,
        "target_chembl_id": [f"CHEMBL_T{i}" for i in range(n)],
        "similarity_score": [1.0 - i / 100 for i in range(n)],
    })


def test_select_top_k_shapes_the_fact_row_contract(monkeypatch, settings, captured_writes):
    monkeypatch.setattr(f"{MODULE}.read_parquet", lambda s, key: _full_similarity_table(25))

    result = select_top_k("CHEMBL1", k=10, settings=settings)

    assert result.column_names == [
        "source_chembl_id", "target_chembl_id", "similarity_score",
        "rank_within_source", "has_duplicates_of_last_largest_score",
    ]
    assert result.num_rows == 10
    assert result.column("rank_within_source").to_pylist() == list(range(1, 11))
    assert result.column("source_chembl_id").to_pylist() == ["CHEMBL1"] * 10


def test_select_top_k_writes_under_the_topk_prefix(monkeypatch, settings, captured_writes):
    monkeypatch.setattr(f"{MODULE}.read_parquet", lambda s, key: _full_similarity_table(25))

    select_top_k("CHEMBL1", k=10, settings=settings)

    (key, _), = captured_writes
    assert key == (
        f"{TEST_PREFIX}/{TOPK_PREFIX}/"
        "source_chembl_id=CHEMBL1/top10.parquet"
    )


def test_select_top_k_returns_scores_in_descending_order(monkeypatch, settings, captured_writes):
    monkeypatch.setattr(f"{MODULE}.read_parquet", lambda s, key: _full_similarity_table(25))

    scores = select_top_k("CHEMBL1", k=10, settings=settings).column("similarity_score").to_pylist()

    assert scores == sorted(scores, reverse=True)


def test_select_top_k_handles_fewer_targets_than_k(monkeypatch, settings, captured_writes):
    monkeypatch.setattr(f"{MODULE}.read_parquet", lambda s, key: _full_similarity_table(3))

    result = select_top_k("CHEMBL1", k=10, settings=settings)

    assert result.num_rows == 3
    assert result.column("rank_within_source").to_pylist() == [1, 2, 3]


def test_select_top_k_similarity_scores_are_plain_floats(monkeypatch, settings, captured_writes):
    """psycopg2 cannot adapt numpy scalars, so the fact rows must carry Python floats."""
    monkeypatch.setattr(f"{MODULE}.read_parquet", lambda s, key: _full_similarity_table(25))

    result = select_top_k("CHEMBL1", k=10, settings=settings)

    assert all(isinstance(v, float) for v in result.column("similarity_score").to_pylist())
