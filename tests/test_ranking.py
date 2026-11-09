import math
import pytest

from dse.ranking import idf


def test_idf_exact_and_positive():
    assert idf(4, 1) == pytest.approx(math.log(1 + 3.5 / 1.5))
    assert idf(4, 4) == pytest.approx(math.log(1 + 0.5 / 4.5))
    assert idf(4, 4) > 0.0
    assert idf(1, 1) > 0.0
    assert idf(100, 100) > 0.0


def test_idf_decreases_with_df():
    assert idf(10, 1) > idf(10, 5) > idf(10, 10) > 0.0
