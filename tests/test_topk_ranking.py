import numpy as np
import pytest

from workers.pipeline.similarity import rank_top_k


def test_no_ties_at_boundary_no_flags():
    scores = np.array([0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.05, 0.01])
    targets = [f"T{i}" for i in range(len(scores))]

    top_scores, top_targets, flag = rank_top_k(scores, targets, k=10)

    assert len(top_targets) == 10
    assert not any(flag)
    assert top_targets[0] == "T0" # highest score first


def test_results_are_sorted_descending():
    scores = np.array([0.1, 0.9, 0.5, 0.7, 0.3])
    targets = [f"T{i}" for i in range(len(scores))]

    top_scores, top_targets, _ = rank_top_k(scores, targets, k=5)

    assert list(top_scores) == sorted(top_scores, reverse=True)
    assert top_targets == ["T1", "T3", "T2", "T4", "T0"]


def test_scores_and_targets_stay_aligned():
    scores = np.array([0.2, 0.8, 0.4])
    targets = ["low", "high", "mid"]

    top_scores, top_targets, _ = rank_top_k(scores, targets, k=3)

    assert dict(zip(top_targets, top_scores, strict=True)) == pytest.approx(
        {"high": 0.8, "mid": 0.4, "low": 0.2}
    )


def test_stable_order_preserves_relative_order_within_ties():
    scores = np.array([0.5, 0.5, 0.9])
    targets = ["first_tie", "second_tie", "best"]
    _, top_targets, _ = rank_top_k(scores, targets, k=3)
    assert top_targets == ["best", "first_tie", "second_tie"]


def test_input_array_is_not_mutated():
    scores = np.array([0.1, 0.9, 0.5])
    original = scores.copy()
    targets = ["a", "b", "c"]

    rank_top_k(scores, targets, k=2)

    assert np.array_equal(scores, original)
    assert targets == ["a", "b", "c"]


def test_ties_beyond_k_are_flagged():
    # rank 10 and rank 11 share the same score (0.5) -> both flagged
    scores = np.array([0.9, 0.8, 0.7, 0.6, 0.55, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5])
    targets = [f"T{i}" for i in range(len(scores))]

    top_scores, top_targets, flag = rank_top_k(scores, targets, k=5)

    # k=5 -> boundary score is scores[4]=0.55, no ties there
    assert not any(flag)

    top_scores10, top_targets10, flag10 = rank_top_k(scores, targets, k=10)
    # boundary at rank 10 (index 9) has value 0.5, but there are 6 molecules
    # with 0.5 (indices 5..10) and only 5 spots among the top-10 -> flagged
    assert any(flag10)
    assert list(flag10).count(True) == 5  # the five 0.5-scored rows kept in top-10


def test_only_boundary_score_rows_are_flagged_not_higher_ranked_ones():
    # 0.9 appears twice and both fit comfortably -> must not be flagged.
    # 0.4 is the boundary score and is over-subscribed -> flagged.
    scores = np.array([0.9, 0.9, 0.4, 0.4, 0.4])
    targets = [f"T{i}" for i in range(len(scores))]

    _, top_targets, flag = rank_top_k(scores, targets, k=3)

    assert top_targets == ["T0", "T1", "T2"]
    assert list(flag) == [False, False, True]


def test_tie_entirely_inside_k_is_not_flagged():
    # Every 0.5 fits within k, so nothing was excluded -> no flag.
    scores = np.array([0.9, 0.5, 0.5, 0.5, 0.1])
    targets = [f"T{i}" for i in range(len(scores))]

    _, _, flag = rank_top_k(scores, targets, k=4)

    assert not any(flag)


def test_tie_ending_exactly_at_k_is_not_flagged():
    # The last tied row lands exactly on the boundary; none were pushed out.
    scores = np.array([0.9, 0.5, 0.5])
    targets = ["a", "b", "c"]

    _, _, flag = rank_top_k(scores, targets, k=3)

    assert not any(flag)


def test_all_scores_equal_and_over_subscribed_flags_every_kept_row():
    scores = np.array([0.5] * 5)
    targets = list("ABCDE")

    _, top_targets, flag = rank_top_k(scores, targets, k=2)

    assert len(top_targets) == 2
    assert all(flag)


def test_all_scores_equal_and_all_fit_flags_nothing():
    scores = np.array([0.5] * 5)
    targets = list("ABCDE")

    _, _, flag = rank_top_k(scores, targets, k=5)

    assert not any(flag)


def test_zero_scores_can_still_be_flagged():
    # A boundary score of 0.0 is a legitimate tie, not a falsy "no score".
    scores = np.array([0.0] * 4)
    targets = list("ABCD")

    _, top_targets, flag = rank_top_k(scores, targets, k=2)

    assert len(top_targets) == 2
    assert all(flag)


def test_fewer_candidates_than_k_no_error():
    scores = np.array([0.9, 0.5])
    targets = ["A", "B"]
    top_scores, top_targets, flag = rank_top_k(scores, targets, k=10)
    assert len(top_targets) == 2
    assert not any(flag)


def test_empty_input_returns_empty_results():
    top_scores, top_targets, flag = rank_top_k(np.array([]), [], k=10)
    assert len(top_scores) == 0
    assert top_targets == []
    assert flag == []


def test_single_candidate():
    top_scores, top_targets, flag = rank_top_k(np.array([0.42]), ["only"], k=10)
    assert top_targets == ["only"]
    assert top_scores[0] == pytest.approx(0.42)
    assert flag == [False]


def test_k_of_one_returns_single_best():
    scores = np.array([0.3, 0.99, 0.7])
    targets = ["a", "b", "c"]

    top_scores, top_targets, flag = rank_top_k(scores, targets, k=1)

    assert top_targets == ["b"]
    assert top_scores[0] == pytest.approx(0.99)
    assert flag == [False]


def test_all_three_outputs_have_matching_length():
    scores = np.array([0.9, 0.8, 0.7, 0.6])
    targets = [f"T{i}" for i in range(4)]

    for k in (1, 2, 4, 99):
        top_scores, top_targets, flag = rank_top_k(scores, targets, k=k)
        assert len(top_scores) == len(top_targets) == len(flag) == min(k, 4)


def test_flags_are_boolean_valued():
    scores = np.array([0.5, 0.5, 0.5])
    targets = list("abc")
    _, _, flag = rank_top_k(scores, targets, k=2)
    assert all(isinstance(bool(f), bool) for f in flag)
    assert all(f in (True, False) for f in flag)
