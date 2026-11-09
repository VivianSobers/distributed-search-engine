import hashlib
import json
import re


def tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def content_hash(
    url: str,
    title: str,
    text: str,
    links: list[str],
    is_deleted: bool,
) -> str:
    payload = {
        "url": url,
        "title": title,
        "text": text,
        "links": links,
        "is_deleted": is_deleted,
    }
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
