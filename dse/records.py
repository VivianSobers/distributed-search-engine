import json
from dataclasses import dataclass


MAX_VERSION = 2**31 - 1  # the Iceberg column is a 32-bit INT


@dataclass(frozen=True)
class Accepted:
    doc_id: str
    url: str
    title: str
    text: str
    links: tuple[str, ...]
    op: str
    version: int
    line_no: int


@dataclass(frozen=True)
class Rejected:
    line_no: int
    reason: str
    raw: str


def _reject(line_no: int, reason: str, line: str) -> Rejected:
    return Rejected(line_no=line_no, reason=reason, raw=line[:2000])


def _is_non_empty_string(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""


def parse_line(line: str, line_no: int, phase: int) -> Accepted | Rejected | None:
    if line.strip() == "":
        return None

    try:
        data = json.loads(line)
    except (json.JSONDecodeError, RecursionError):
        return _reject(line_no=line_no, reason="malformed_json", line=line)

    if not isinstance(data, dict):
        return _reject(line_no=line_no, reason="not_object", line=line)

    op_value = data.get("op")
    if op_value is None:
        op = "UPSERT"
    elif isinstance(op_value, str):
        op = op_value.upper()
        if op not in {"UPSERT", "DELETE"}:
            return _reject(line_no=line_no, reason="bad_op", line=line)
    else:
        return _reject(line_no=line_no, reason="bad_op", line=line)

    doc_id = data.get("doc_id")
    if not _is_non_empty_string(doc_id):
        return _reject(line_no=line_no, reason="missing_field:doc_id", line=line)

    version = data.get("version", phase)
    if isinstance(version, bool) or not isinstance(version, int) or not 0 <= version <= MAX_VERSION:
        return _reject(line_no=line_no, reason="bad_version", line=line)

    if op == "DELETE":
        return Accepted(
            doc_id=doc_id,
            url="",
            title="",
            text="",
            links=(),
            op=op,
            version=version,
            line_no=line_no,
        )

    for field_name in ("url", "title", "text"):
        if not _is_non_empty_string(data.get(field_name)):
            return _reject(
                line_no=line_no,
                reason=f"missing_field:{field_name}",
                line=line,
            )

    links_value = data.get("links")
    if links_value is None:
        links: tuple[str, ...] = ()
    else:
        if not isinstance(links_value, list):
            return _reject(line_no=line_no, reason="bad_links", line=line)
        cleaned_links: list[str] = []
        seen: set[str] = set()
        for item in links_value:
            if not isinstance(item, str):
                return _reject(line_no=line_no, reason="bad_links", line=line)
            if item.strip() == "":
                continue
            if item not in seen:
                seen.add(item)
                cleaned_links.append(item)
        links = tuple(cleaned_links)

    return Accepted(
        doc_id=doc_id,
        url=data["url"],
        title=data["title"],
        text=data["text"],
        links=links,
        op=op,
        version=version,
        line_no=line_no,
    )
