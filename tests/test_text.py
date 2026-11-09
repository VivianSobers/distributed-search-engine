from dse.text import tokenize


def test_tokenize_splits_and_lowercases():
    assert tokenize("Don't STOP, 42!") == ["don", "t", "stop", "42"]


def test_tokenize_empty():
    assert tokenize("") == []
