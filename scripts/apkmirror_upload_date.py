"""
Resolve APKMirror ``Uploaded`` timestamps for release pages.

CSV listings sometimes omit ``release_date``; detail pages carry ``metaSlide``
upload metadata. Results persist under ``data/cache/apkmirror_upload_dates.json``
so later pipeline / workbook-only runs avoid repeated HTTP.

Optional env:
  APKMIRROR_UPLOAD_FETCH_MAX — max **new** HTTP fetches per process (default 800).
  Use ``0`` to disable fetching (fully offline/reproducible).
  Use ``unlimited`` for no cap (slow on large caches).
"""

from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

ROOT = Path(__file__).resolve().parents[1]
_CACHE_PATH = ROOT / "data" / "cache" / "apkmirror_upload_dates.json"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

_UPLOADED_RE = re.compile(r"Uploaded\s*[:\s]+\s*(.+?)(?:\s{2,}|\||$)", re.I)

_budget_state: dict[str, Any] = {"remaining": None, "initialized": False}


def _fetch_budget_remaining() -> int | None:
    if not _budget_state["initialized"]:
        _budget_state["initialized"] = True
        raw = os.environ.get("APKMIRROR_UPLOAD_FETCH_MAX", "800").strip()
        if raw == "" or raw.lower() in ("none", "unlimited"):
            _budget_state["remaining"] = None
        else:
            try:
                n = int(raw, 10)
                # 0 means "no new fetches" (offline); negatives treated as 0 as well.
                _budget_state["remaining"] = max(0, n)
            except ValueError:
                _budget_state["remaining"] = 800
    return _budget_state["remaining"]


def _consume_fetch_slot() -> bool:
    cap = _fetch_budget_remaining()
    if cap is None:
        return True
    if cap <= 0:
        return False
    _budget_state["remaining"] = cap - 1
    return True


def _is_cloudflare_interstitial(html: str) -> bool:
    if len(html) < 200:
        return False
    head = html[:12000]
    return "cdn-cgi/challenge-platform" in head or (
        "Just a moment" in head and "challenge-error-text" in head
    )


def _iso_from_raw(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    try:
        return date_parser.parse(s, fuzzy=True).date().isoformat()
    except (ValueError, OverflowError, TypeError):
        return ""


def parse_upload_date_from_apkmirror_html(html: str) -> str:
    if _is_cloudflare_interstitial(html):
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for slide in soup.select("div.metaSlide"):
        lab = slide.select_one(".metaSlide-label")
        val = slide.select_one(".metaSlide-value")
        if not lab or not val:
            continue
        if "upload" not in lab.get_text(strip=True).lower():
            continue
        iso = _iso_from_raw(val.get_text(strip=True))
        if iso:
            return iso
    m = _UPLOADED_RE.search(soup.get_text(" ", strip=True))
    if m:
        return _iso_from_raw(m.group(1).strip())
    return ""


def _load_disk_cache() -> dict[str, str]:
    if not _CACHE_PATH.is_file():
        return {}
    try:
        raw = json.loads(_CACHE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in raw.items():
        if not isinstance(k, str):
            continue
        if isinstance(v, str) and len(v) >= 10 and v[4] == "-" and v[7] == "-":
            out[k] = v[:10]
    return out


def _save_disk_cache(store: dict[str, str]) -> None:
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _CACHE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(store, indent=0, sort_keys=True), encoding="utf-8")
    tmp.replace(_CACHE_PATH)


def resolve_apk_upload_date(url: str, *, sleep_s: float = 0.22) -> str:
    """
    Return ``YYYY-MM-DD`` from JSON cache or fetch the APKMirror release page once.
    Returns ``\"\"`` if unavailable (never writes empty strings into cache).
    """
    u = (url or "").strip()
    if not u.startswith(("http://", "https://")) or "apkmirror.com" not in u.lower():
        return ""

    store = _load_disk_cache()
    hit = store.get(u)
    if hit:
        return hit

    if not _consume_fetch_slot():
        return ""

    try:
        r = requests.get(u, headers=_HEADERS, timeout=50)
    except requests.RequestException:
        return ""
    if r.status_code != 200:
        return ""
    iso = parse_upload_date_from_apkmirror_html(r.text)
    if iso:
        store[u] = iso
        try:
            _save_disk_cache(store)
        except OSError:
            pass
    time.sleep(sleep_s)
    return iso


def fill_missing_apk_mirror_release_dates(version_df):  # pandas.DataFrame
    """Backfill ``release_date`` for Android ``apkmirror_cache`` rows using release URLs."""
    import pandas as pd

    if version_df.empty or "platform" not in version_df.columns:
        return version_df
    need_cols = {"source_type", "release_date", "history_source_url"}
    if not need_cols.issubset(version_df.columns):
        return version_df

    out = version_df.copy()
    plat = out["platform"].astype(str).str.strip().eq("Android")
    src = out["source_type"].astype(str).str.strip().eq("apkmirror_cache")
    rd = out["release_date"].astype(str).str.strip()
    blank_rd = rd.eq("") | rd.str.lower().isin(("nan", "nat", "none"))
    m = plat & src & blank_rd
    if not bool(m.any()):
        return out

    for idx in out.index[m]:
        url = str(out.at[idx, "history_source_url"] or "").strip()
        iso = resolve_apk_upload_date(url)
        if iso:
            out.at[idx, "release_date"] = iso
    return out
