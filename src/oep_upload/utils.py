from __future__ import annotations

import re


def is_blank(v) -> bool:
    return not isinstance(v, str) or v.strip() == ""


def slugify(s: str, fallback: str = "resource") -> str:
    s = s.strip().lower().replace(" ", "-")
    s = re.sub(r"[^\w\-]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or fallback
