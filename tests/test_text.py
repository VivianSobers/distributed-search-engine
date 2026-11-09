from dse.text import content_hash, tokenize


def test_tokenize_splits_and_lowercases():
    assert tokenize("Don't STOP, 42!") == ["don", "t", "stop", "42"]


def test_tokenize_empty():
    assert tokenize("") == []


def test_content_hash_deterministic():
    first = content_hash(
        url="https://example.com/a",
        title="A",
        text="Body",
        links=["https://example.com/b"],
        is_deleted=False,
    )
    second = content_hash(
        url="https://example.com/a",
        title="A",
        text="Body",
        links=["https://example.com/b"],
        is_deleted=False,
    )
    assert first == second


def test_content_hash_changes_when_any_value_changes():
    base = content_hash(
        url="https://example.com/a",
        title="A",
        text="Body",
        links=["https://example.com/b"],
        is_deleted=False,
    )
    assert (
        content_hash(
            url="https://example.com/x",
            title="A",
            text="Body",
            links=["https://example.com/b"],
            is_deleted=False,
        )
        != base
    )
    assert (
        content_hash(
            url="https://example.com/a",
            title="X",
            text="Body",
            links=["https://example.com/b"],
            is_deleted=False,
        )
        != base
    )
    assert (
        content_hash(
            url="https://example.com/a",
            title="A",
            text="Other",
            links=["https://example.com/b"],
            is_deleted=False,
        )
        != base
    )
    assert (
        content_hash(
            url="https://example.com/a",
            title="A",
            text="Body",
            links=["https://example.com/c"],
            is_deleted=False,
        )
        != base
    )
    assert (
        content_hash(
            url="https://example.com/a",
            title="A",
            text="Body",
            links=["https://example.com/b"],
            is_deleted=True,
        )
        != base
    )
