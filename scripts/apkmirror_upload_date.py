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
_FAIL_CACHE_PATH = ROOT / "data" / "cache" / "apkmirror_upload_failures.json"

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


def parse_upload_date_from_apkmirror_html(html: str, *, debug: bool = False) -> str:
    if _is_cloudflare_interstitial(html):
        return ""
    soup = BeautifulSoup(html, "html.parser")

    def _scan_uploaded_near(hit) -> str:
        """Given a BeautifulSoup text node containing 'Uploaded', scan nearby text for a date."""
        try:
            s_hit = str(hit).strip()
        except Exception:
            return ""
        if not s_hit:
            return ""
        # (a) "Uploaded: May 6, 2026 at 5:28PM GMT+0000" in same node.
        m_inline = _UPLOADED_RE.search(s_hit)
        if m_inline:
            iso = _iso_from_raw(m_inline.group(1).strip())
            if iso:
                return iso
        # (b) Look at parent text (often contains both label + value).
        parent = getattr(hit, "parent", None)
        if parent is not None:
            ptxt = parent.get_text(" ", strip=True)
            m_parent = _UPLOADED_RE.search(ptxt)
            if m_parent:
                iso = _iso_from_raw(m_parent.group(1).strip())
                if iso:
                    return iso
        # (c) Grab next text node(s) after the "Uploaded" token and parse that.
        nxt = hit
        for _ in range(8):
            try:
                nxt = nxt.find_next(string=True)  # type: ignore[attr-defined]
            except Exception:
                nxt = None
            if nxt is None:
                break
            cand = str(nxt).strip()
            if not cand or re.search(r"\buploaded\b", cand, re.I):
                continue
            iso = _iso_from_raw(cand)
            if iso:
                return iso
        return ""

    # --- Best-effort "this release" match (most precise) ---
    # Release pages include the release title in H1; the "All Releases" list also repeats it as a link.
    # We match that link and extract Uploaded from the same row container to avoid unrelated sections.
    h1 = soup.select_one("h1")
    release_title = h1.get_text(" ", strip=True) if h1 is not None else ""
    if release_title:
        title_hits = soup.find_all(string=re.compile(rf"^{re.escape(release_title)}$", re.I))
        for th in title_hits[:25]:
            parent = getattr(th, "parent", None)
            if parent is None or getattr(parent, "name", "") != "a":
                continue
            row = parent.find_parent(class_="appRow")
            container = row.parent if row is not None and getattr(row, "parent", None) is not None else row
            if container is None:
                continue
            up = container.find(string=re.compile(r"\buploaded\b", re.I))
            if up is not None:
                iso = _scan_uploaded_near(up)
                if iso:
                    return iso

    # --- Structured layouts (fast path) ---
    # Newer APKMirror layout uses infoSlide/meta blocks (infoSlide-name/value).
    for p in soup.select("p"):
        lab = p.select_one(".infoSlide-name")
        val = p.select_one(".infoSlide-value")
        if not lab or not val:
            continue
        if "upload" not in lab.get_text(strip=True).lower():
            continue
        iso = _iso_from_raw(val.get_text(" ", strip=True))
        if iso:
            return iso
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

    # Prefer the "Uploaded" that belongs to THIS release version (avoid unrelated "Apps related to…" blocks).
    ver = ""
    if release_title:
        mver = re.search(r"(\d+\.\d+(?:\.\d+)*)", release_title)
        if mver:
            ver = mver.group(1)
    if ver:
        # APKMirror release pages usually include "Version:{ver}" in the "All Releases" block.
        v_hits = soup.find_all(string=re.compile(rf"\bVersion\s*:\s*{re.escape(ver)}\b", re.I))
        for vh in v_hits[:20]:
            # Scan forward in document order from this version label until we hit:
            # - an "Uploaded" field (use it), or
            # - the next "Version:" label (stop; we're in another release block), or
            # - a small step cap.
            cur = vh
            for _ in range(260):
                try:
                    cur = cur.find_next(string=True)  # type: ignore[attr-defined]
                except Exception:
                    cur = None
                if cur is None:
                    break
                txt = str(cur).strip()
                if not txt:
                    continue
                if re.search(r"\bVersion\s*:\s*\d", txt, re.I):
                    break
                if re.search(r"\buploaded\b", txt, re.I):
                    iso = _scan_uploaded_near(cur)
                    if iso:
                        return iso

    # Resilient fallback: look for any "Uploaded" label and grab the nearest date-like text.
    # APKMirror has changed markup multiple times; the word still appears even when metaSlide blocks do not.
    uploaded_hits = soup.find_all(string=re.compile(r"\buploaded\b", re.I))
    for hit in uploaded_hits[:60]:
        try:
            iso = _scan_uploaded_near(hit)
        except Exception:
            continue
        if iso:
            return iso

    # Last resort: regex against full page text.
    full_txt = soup.get_text(" ", strip=True)
    m = _UPLOADED_RE.search(full_txt)
    if m:
        return _iso_from_raw(m.group(1).strip())

    if debug and uploaded_hits:
        # Print context around every "Uploaded" match to help tune parser without guessing markup.
        # (200 chars around each match, as requested).
        raw_txt = soup.get_text(" ", strip=True)
        low = raw_txt.lower()
        needle = "uploaded"
        idx = 0
        seen = 0
        while True:
            j = low.find(needle, idx)
            if j < 0:
                break
            a = max(0, j - 100)
            b = min(len(raw_txt), j + 100)
            snippet = raw_txt[a:b].replace("\n", " ")
            print(f"[apkmirror_upload_date][debug] Uploaded context {seen+1}: …{snippet}…")
            seen += 1
            if seen >= 20:
                break
            idx = j + len(needle)
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


def _load_failure_cache() -> dict[str, int]:
    """
    Negative cache for URLs that consistently fail (e.g., 403).
    Maps url -> http_status_code.
    """
    if not _FAIL_CACHE_PATH.is_file():
        return {}
    try:
        raw = json.loads(_FAIL_CACHE_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(raw, dict):
        return {}
    out: dict[str, int] = {}
    for k, v in raw.items():
        if not isinstance(k, str):
            continue
        try:
            code = int(v)
        except Exception:
            continue
        if 100 <= code <= 599:
            out[k] = code
    return out


def _save_failure_cache(store: dict[str, int]) -> None:
    _FAIL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _FAIL_CACHE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(store, indent=0, sort_keys=True), encoding="utf-8")
    tmp.replace(_FAIL_CACHE_PATH)


def _save_disk_cache(store: dict[str, str]) -> None:
    _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = _CACHE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(store, indent=0, sort_keys=True), encoding="utf-8")
    tmp.replace(_CACHE_PATH)


def resolve_apk_upload_date_detailed(url: str, *, sleep_s: float = 0.22) -> tuple[str, str]:
    """
    Like ``resolve_apk_upload_date`` but also returns a status code:
    - cache_hit | fetch_disabled | request_exception | http_non_200 | cloudflare | parse_empty | fetched_ok
    """
    u = (url or "").strip()
    if not u.startswith(("http://", "https://")) or "apkmirror.com" not in u.lower():
        return "", "parse_empty"

    store = _load_disk_cache()
    hit = store.get(u)
    if hit:
        return hit, "cache_hit"

    fail = _load_failure_cache()
    if fail.get(u) == 403:
        return "", "http_403_skipped"

    if not _consume_fetch_slot():
        return "", "fetch_disabled"

    try:
        r = requests.get(u, headers=_HEADERS, timeout=50)
    except requests.RequestException:
        return "", "request_exception"
    if r.status_code != 200:
        if int(r.status_code) == 403:
            fail[u] = 403
            try:
                _save_failure_cache(fail)
            except OSError:
                pass
        return "", f"http_{int(r.status_code)}"
    if _is_cloudflare_interstitial(r.text):
        time.sleep(sleep_s)
        return "", "cloudflare"

    debug = os.environ.get("APKMIRROR_UPLOAD_DEBUG", "").strip().lower() in ("1", "true", "yes", "y")
    iso = parse_upload_date_from_apkmirror_html(r.text, debug=debug)
    if iso:
        store[u] = iso
        try:
            _save_disk_cache(store)
        except OSError:
            pass
        time.sleep(sleep_s)
        return iso, "fetched_ok"

    time.sleep(sleep_s)
    return "", "parse_empty"


def resolve_apk_upload_date(url: str, *, sleep_s: float = 0.22) -> str:
    """
    Return ``YYYY-MM-DD`` from JSON cache or fetch the APKMirror release page once.
    Returns ``\"\"`` if unavailable (never writes empty strings into cache).
    """
    iso, _status = resolve_apk_upload_date_detailed(url, sleep_s=sleep_s)
    return iso


def fill_missing_apk_mirror_release_dates(
    version_df,  # pandas.DataFrame
    *,
    verbose: bool = False,
    save_every: int = 25,
):  # pandas.DataFrame
    """
    Backfill ``release_date`` for Android ``apkmirror_cache`` rows using release URLs.

    Notes:
    - Fetching is controlled by env ``APKMIRROR_UPLOAD_FETCH_MAX`` (0 disables fetching).
    - Results persist in ``data/cache/apkmirror_upload_dates.json``.
    """
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

    idxs = list(out.index[m])
    total = len(idxs)
    filled = 0
    status_counts: dict[str, int] = {}
    fetched_attempts = 0
    for i, idx in enumerate(idxs, start=1):
        url = str(out.at[idx, "history_source_url"] or "").strip()
        iso, status = resolve_apk_upload_date_detailed(url)
        status_counts[status] = status_counts.get(status, 0) + 1
        if status in {"fetched_ok", "parse_empty", "cloudflare", "request_exception"} or status.startswith("http_"):
            fetched_attempts += 1
        if iso:
            out.at[idx, "release_date"] = iso
            filled += 1
        if verbose and (i == 1 or i % 25 == 0 or i == total):
            print(f"[apkmirror] backfill progress: {i}/{total} rows processed; {filled} filled")
    if verbose:
        ordered = sorted(status_counts.items(), key=lambda kv: (-kv[1], kv[0]))
        top = ", ".join([f"{k}={v}" for k, v in ordered[:8]])
        print(f"[apkmirror] summary: processed={total} filled={filled} fetched_attempts≈{fetched_attempts} ({top})")
    return out
