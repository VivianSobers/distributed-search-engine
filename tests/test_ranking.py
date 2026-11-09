import math
import pytest

from dse.ranking import bm25_term, idf


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
