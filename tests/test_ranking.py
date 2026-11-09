import math
import pytest

from dse.ranking import bm25_term, combine_scores, idf


def test_idf_exact_and_positive():
    assert idf(4, 1) == pytest.approx(math.log(1 + 3.5 / 1.5))
    assert idf(4, 4) == pytest.approx(math.log(1 + 0.5 / 4.5))
    assert idf(4, 4) > 0.0
    assert idf(1, 1) > 0.0
    assert idf(100, 100) > 0.0


def test_idf_decreases_with_df():
    assert idf(10, 1) > idf(10, 5) > idf(10, 10) > 0.0


def test_bm25_term_exact():
    expected = math.log(10 / 3) * 4.4 / 3.2
    assert bm25_term(2, 10, 10, math.log(10 / 3)) == pytest.approx(expected)


def test_bm25_term_longer_document_scores_lower():
    assert bm25_term(2, 20, 10, 1.0) < bm25_term(2, 10, 10, 1.0)


def test_bm25_term_zero_tf():
    assert bm25_term(0, 10, 10, 1.0) == 0.0


def test_combine_scores_exact():
    scores = combine_scores({"a": 2.0, "b": 1.0}, {"a": 0.1, "b": 0.4}, 0.4)
    assert scores["a"] == pytest.approx(0.775)
    assert scores["b"] == pytest.approx(0.65)


def test_combine_scores_doc_absent_from_pagerank():
    scores = combine_scores({"a": 2.0, "c": 1.0}, {"a": 0.4}, 0.4)
    assert scores["a"] == pytest.approx(0.7 * 1.0 + 0.3 * 1.0)
    assert scores["c"] == pytest.approx(0.7 * 0.5 + 0.3 * 0.0)


def test_combine_scores_doc_only_in_pagerank_not_in_bm25():
    scores = combine_scores({"a": 2.0}, {"a": 0.2, "other": 0.4}, 0.4)
    assert set(scores.keys()) == {"a"}


def test_combine_scores_empty_input():
    assert combine_scores({}, {"a": 0.4}, 0.4) == {}
    assert combine_scores({}, {}, 0.0) == {}


def test_combine_scores_zero_max_bm25():
    scores = combine_scores({"a": 0.0, "b": 0.0}, {"a": 0.1, "b": 0.4}, 0.4)
    assert scores["a"] == pytest.approx(0.3 * (0.1 / 0.4))
    assert scores["b"] == pytest.approx(0.3 * (0.4 / 0.4))


def test_combine_scores_zero_max_pagerank():
    scores = combine_scores({"a": 2.0, "b": 1.0}, {"a": 0.0, "b": 0.0}, 0.0)
    assert scores["a"] == pytest.approx(0.7 * (2.0 / 2.0))
    assert scores["b"] == pytest.approx(0.7 * (1.0 / 2.0))


def test_combine_scores_both_zero_max():
    scores = combine_scores({"a": 0.0}, {"a": 0.0}, 0.0)
    assert scores["a"] == 0.0
