"""Tests for the Tanimoto kernel in workers.pipeline.similarity."""

import numpy as np
import pytest

from workers.pipeline.similarity import _tail_mask, tanimoto_packed


def _pack(bits: list[int]) -> np.ndarray:
    """One fingerprint -> packed uint8 row."""
    return np.packbits(np.array(bits, dtype=np.uint8))


def _pack_many(rows: list[list[int]]) -> np.ndarray:
    """Many fingerprints -> packed uint8 matrix, one row per fingerprint."""
    return np.packbits(np.array(rows, dtype=np.uint8), axis=1)


def _tanimoto_reference(query: np.ndarray, other: np.ndarray) -> float:
    """Brute-force Tanimoto on unpacked bits, used to cross-check the packed kernel."""
    intersection = np.count_nonzero(query & other)
    union = np.count_nonzero(query | other)
    return intersection / union if union else 0.0


def test_tanimoto_identical_vectors_is_one():
    query = _pack([1, 0, 1, 0])
    others = _pack_many([[1, 0, 1, 0]])
    assert tanimoto_packed(query, others, n_bits=4)[0] == pytest.approx(1.0)


def test_tanimoto_disjoint_vectors_is_zero():
    query = _pack([1, 1, 0, 0])
    others = _pack_many([[0, 0, 1, 1]])
    assert tanimoto_packed(query, others, n_bits=4)[0] == pytest.approx(0.0)


def test_tanimoto_known_value():
    # intersection = 1, union = 3 -> 1/3
    query = _pack([1, 1, 0])
    others = _pack_many([[1, 0, 1]])
    assert tanimoto_packed(query, others, n_bits=3)[0] == pytest.approx(1 / 3)


def test_tanimoto_all_zero_vectors_returns_zero_not_nan():
    query = _pack([0, 0, 0])
    others = _pack_many([[0, 0, 0]])
    result = tanimoto_packed(query, others, n_bits=3)[0]
    assert result == 0.0
    assert not np.isnan(result)


def test_tanimoto_empty_query_against_nonempty_target_is_zero():
    # union > 0 but intersection == 0, so this exercises the divide, not the guard
    query = _pack([0, 0, 0, 0])
    others = _pack_many([[1, 1, 0, 0]])
    assert tanimoto_packed(query, others, n_bits=4)[0] == 0.0


def test_tanimoto_subset_is_ratio_of_popcounts():
    # query is a strict subset of other: 2 bits of 4 -> 2/4
    query = _pack([1, 1, 0, 0])
    others = _pack_many([[1, 1, 1, 1]])
    assert tanimoto_packed(query, others, n_bits=4)[0] == pytest.approx(0.5)


def test_returns_one_score_per_row_in_order():
    query = _pack([1, 1, 0, 0])
    others = _pack_many([
        [1, 1, 0, 0], # identical -> 1.0
        [0, 0, 1, 1], # disjoint -> 0.0
        [1, 0, 0, 0], # half overlap -> 1/2
    ])
    scores = tanimoto_packed(query, others, n_bits=4)
    assert scores.shape == (3,)
    assert scores.dtype == np.float64
    assert scores == pytest.approx([1.0, 0.0, 0.5])


def test_scores_are_bounded_to_unit_interval():
    rng = np.random.default_rng(0)
    bits = rng.integers(0, 2, size=(50, 64), dtype=np.uint8)
    packed = np.packbits(bits, axis=1)
    scores = tanimoto_packed(packed[0], packed, n_bits=64)
    assert scores.min() >= 0.0
    assert scores.max() <= 1.0


def test_chunking_does_not_change_results():
    rng = np.random.default_rng(1)
    bits = rng.integers(0, 2, size=(37, 32), dtype=np.uint8)
    packed = np.packbits(bits, axis=1)

    one_shot = tanimoto_packed(packed[0], packed, n_bits=32, chunk=10**9)
    tiny_chunks = tanimoto_packed(packed[0], packed, n_bits=32, chunk=5)
    single_row = tanimoto_packed(packed[0], packed, n_bits=32, chunk=1)

    assert np.array_equal(one_shot, tiny_chunks)
    assert np.array_equal(one_shot, single_row)


def test_chunk_larger_than_corpus_is_handled():
    query = _pack([1, 1, 0, 0])
    others = _pack_many([[1, 1, 0, 0]])
    assert tanimoto_packed(query, others, n_bits=4, chunk=10_000)[0] == pytest.approx(1.0)


def test_tail_mask_is_none_when_bit_width_is_byte_aligned():
    assert _tail_mask(2048) is None
    assert _tail_mask(8) is None


def test_tail_mask_clears_only_trailing_padding_bits():
    mask = _tail_mask(5)
    assert mask is not None
    assert mask.tolist() == [0b11111000]


def test_padding_bits_are_ignored_at_non_byte_aligned_widths():
    # 5 logical bits stored in one byte; the 3 padding bits must not count.
    query = _pack([1, 1, 0, 1, 0])
    others = _pack_many([[1, 0, 0, 1, 0]])
    # intersection = {bit0, bit3} = 2, union = {bit0, bit1, bit3} = 3
    assert tanimoto_packed(query, others, n_bits=5)[0] == pytest.approx(2 / 3)


def test_dirty_padding_bits_do_not_affect_the_score():
    # Same 5 logical bits, but with junk left in the padding region. The mask
    # must make this indistinguishable from clean input.
    clean_q, clean_o = _pack([1, 1, 0, 1, 0]), _pack_many([[1, 0, 0, 1, 0]])
    dirty_q = (clean_q | 0b00000111).astype(np.uint8)
    dirty_o = (clean_o | 0b00000111).astype(np.uint8)

    expected = tanimoto_packed(clean_q, clean_o, n_bits=5)
    assert tanimoto_packed(dirty_q, dirty_o, n_bits=5) == pytest.approx(expected)


def test_matches_bruteforce_reference_on_random_fingerprints():
    rng = np.random.default_rng(42)
    bits = rng.integers(0, 2, size=(40, 128), dtype=np.uint8)
    packed = np.packbits(bits, axis=1)

    for i in range(0, 40, 7):
        scores = tanimoto_packed(packed[i], packed, n_bits=128)
        expected = [_tanimoto_reference(bits[i], bits[j]) for j in range(40)]
        assert scores == pytest.approx(expected)


def test_similarity_is_symmetric():
    rng = np.random.default_rng(7)
    bits = rng.integers(0, 2, size=(12, 64), dtype=np.uint8)
    packed = np.packbits(bits, axis=1)

    full = np.vstack([tanimoto_packed(packed[i], packed, n_bits=64) for i in range(12)])
    assert full == pytest.approx(full.T)


def test_self_similarity_is_exactly_one_at_production_width():
    rng = np.random.default_rng(3)
    bits = rng.integers(0, 2, size=(5, 2048), dtype=np.uint8)
    packed = np.packbits(bits, axis=1)
    for i in range(5):
        assert tanimoto_packed(packed[i], packed, n_bits=2048)[i] == 1.0
