from dataclasses import FrozenInstanceError

from dse.records import Accepted, Rejected, parse_line


def test_parse_line_blank_line_returns_none():
    assert parse_line("   \n", line_no=1, phase=0) is None


def test_parse_line_invalid_json_rejected():
    rec = parse_line("{", line_no=2, phase=3)
    assert isinstance(rec, Rejected)
    assert rec.reason == "malformed_json"
    assert rec.raw == "{"


def test_parse_line_json_non_object_rejected():
    rec = parse_line('["x"]', line_no=3, phase=4)
    assert isinstance(rec, Rejected)
    assert rec.reason == "not_object"


def test_parse_line_bad_op_rejected():
    rec = parse_line('{"op":"merge","doc_id":"d1"}', line_no=4, phase=4)
    assert isinstance(rec, Rejected)
    assert rec.reason == "bad_op"


def test_parse_line_op_defaults_to_upsert():
    rec = parse_line(
        '{"doc_id":"d1","url":"u","title":"t","text":"x","version":1}',
        line_no=5,
        phase=7,
    )
    assert isinstance(rec, Accepted)
    assert rec.op == "UPSERT"


def test_parse_line_missing_doc_id_rejected():
    rec = parse_line('{"url":"u","title":"t","text":"x"}', line_no=6, phase=1)
    assert isinstance(rec, Rejected)
    assert rec.reason == "missing_field:doc_id"


def test_parse_line_all_null_row_rejected_missing_doc_id():
    rec = parse_line(
        '{"doc_id":null,"url":null,"title":null,"text":null,"links":null}',
        line_no=7,
        phase=1,
    )
    assert isinstance(rec, Rejected)
    assert rec.reason == "missing_field:doc_id"


def test_parse_line_empty_doc_id_rejected_missing_doc_id():
    rec = parse_line('{"doc_id":"","url":"","title":"","text":""}', line_no=8, phase=1)
    assert isinstance(rec, Rejected)
    assert rec.reason == "missing_field:doc_id"


def test_parse_line_upsert_requires_url_title_text_in_order():
    rec_url = parse_line('{"doc_id":"d","title":"t","text":"x"}', line_no=9, phase=1)
    rec_title = parse_line('{"doc_id":"d","url":"u","text":"x"}', line_no=10, phase=1)
    rec_text = parse_line('{"doc_id":"d","url":"u","title":"t"}', line_no=11, phase=1)
    assert isinstance(rec_url, Rejected) and rec_url.reason == "missing_field:url"
    assert isinstance(rec_title, Rejected) and rec_title.reason == "missing_field:title"
    assert isinstance(rec_text, Rejected) and rec_text.reason == "missing_field:text"


def test_parse_line_missing_links_defaults_to_empty_tuple():
    rec = parse_line(
        '{"doc_id":"d1","url":"u","title":"t","text":"x"}',
        line_no=12,
        phase=2,
    )
    assert isinstance(rec, Accepted)
    assert rec.links == ()


def test_parse_line_links_null_defaults_to_empty_tuple():
    rec = parse_line(
        '{"doc_id":"d1","url":"u","title":"t","text":"x","links":null}',
        line_no=13,
        phase=2,
    )
    assert isinstance(rec, Accepted)
    assert rec.links == ()


def test_parse_line_bad_links_rejected():
    rec_not_list = parse_line(
        '{"doc_id":"d1","url":"u","title":"t","text":"x","links":"x"}',
        line_no=14,
        phase=2,
    )
    rec_not_str = parse_line(
        '{"doc_id":"d1","url":"u","title":"t","text":"x","links":["ok",1]}',
        line_no=15,
        phase=2,
    )
    assert isinstance(rec_not_list, Rejected) and rec_not_list.reason == "bad_links"
    assert isinstance(rec_not_str, Rejected) and rec_not_str.reason == "bad_links"


def test_parse_line_links_drop_blank_and_dedupe_preserve_order():
    rec = parse_line(
        '{"doc_id":"d1","url":"u","title":"t","text":"x","links":["a","","a","b","  ","b","c"]}',
        line_no=16,
        phase=2,
    )
    assert isinstance(rec, Accepted)
    assert rec.links == ("a", "b", "c")


def test_parse_line_bad_version_rejected():
    rec_negative = parse_line(
        '{"doc_id":"d1","url":"u","title":"t","text":"x","version":-1}',
        line_no=17,
        phase=2,
    )
    rec_bool = parse_line(
        '{"doc_id":"d1","url":"u","title":"t","text":"x","version":true}',
        line_no=18,
        phase=2,
    )
    rec_str = parse_line(
        '{"doc_id":"d1","url":"u","title":"t","text":"x","version":"1"}',
        line_no=19,
        phase=2,
    )
    assert isinstance(rec_negative, Rejected) and rec_negative.reason == "bad_version"
    assert isinstance(rec_bool, Rejected) and rec_bool.reason == "bad_version"
    assert isinstance(rec_str, Rejected) and rec_str.reason == "bad_version"


def test_parse_line_version_defaults_to_phase():
    rec = parse_line(
        '{"doc_id":"d1","url":"u","title":"t","text":"x"}',
        line_no=20,
        phase=9,
    )
    assert isinstance(rec, Accepted)
    assert rec.version == 9


def test_parse_line_delete_needs_only_doc_id_and_blanks_content_fields():
    rec = parse_line(
        '{"op":"delete","doc_id":"d1","url":"u","title":"t","text":"x","links":["a"],"version":5}',
        line_no=21,
        phase=3,
    )
    assert isinstance(rec, Accepted)
    assert rec.op == "DELETE"
    assert rec.url == ""
    assert rec.title == ""
    assert rec.text == ""
    assert rec.links == ()
    assert rec.version == 5


def test_rejected_raw_is_truncated_to_2000_chars():
    long_line = "{" + ("x" * 2100)
    rec = parse_line(long_line, line_no=22, phase=1)
    assert isinstance(rec, Rejected)
    assert len(rec.raw) == 2000
    assert rec.raw == long_line[:2000]


def test_accepted_and_rejected_are_frozen():
    accepted = Accepted(
        doc_id="d1",
        url="u",
        title="t",
        text="x",
        links=(),
        op="UPSERT",
        version=0,
        line_no=1,
    )
    rejected = Rejected(line_no=1, reason="r", raw="x")
    try:
        accepted.doc_id = "new"  # type: ignore[misc]
    except FrozenInstanceError:
        pass
    else:
        raise AssertionError("Accepted must be frozen")
    try:
        rejected.reason = "new"  # type: ignore[misc]
    except FrozenInstanceError:
        pass
    else:
        raise AssertionError("Rejected must be frozen")


def test_parse_line_version_beyond_int_column_rejected():
    rec = parse_line(
        '{"doc_id":"d1","url":"u","title":"t","text":"x","version":2147483648}',
        line_no=23,
        phase=1,
    )
    assert isinstance(rec, Rejected) and rec.reason == "bad_version"


def test_parse_line_extremely_nested_json_rejected():
    rec = parse_line("[" * 200000, line_no=24, phase=1)
    assert isinstance(rec, Rejected) and rec.reason == "malformed_json"
