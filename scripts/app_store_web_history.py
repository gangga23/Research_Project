"""
Fetch App Store product page HTML and extract embedded versionHistory JSON.

Apple's iTunes Lookup API does not expose multi-version history; the consumer
web page embeds a versionHistory flow payload (often under pageData.shelves).

This module does not call amp-api-edge (typically 401 without browser session);
it parses server-rendered / hydration JSON present in the HTML document.
"""

from __future__ import annotations

import json
import re
from typing import Any

import requests

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
)


def app_store_product_url(track_id: int, country: str = "us") -> str:
    return f"https://apps.apple.com/{country}/app/id{track_id}"


def fetch_app_store_html(track_id: int, country: str = "us", timeout: int = 60) -> str:
    url = app_store_product_url(track_id, country)
    r = requests.get(
        url,
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        },
        timeout=timeout,
    )
    r.raise_for_status()
    r.encoding = r.apparent_encoding or "utf-8"
    return r.text


def wayback_list_timestamps(url: str, *, max_results: int = 70, timeout: int = 45) -> list[str]:
    """
    Return Wayback timestamps for a URL (newest first).
    Uses CDX API; does not guarantee that each timestamp fetches usable HTML.
    """
    r = requests.get(
        "https://web.archive.org/cdx/search/cdx",
        params={
            "url": url,
            "output": "json",
            "filter": "statuscode:200",
            "collapse": "digest",
            "fl": "timestamp",
            "limit": str(max_results),
        },
        headers={"User-Agent": UA},
        timeout=timeout,
    )
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or len(data) < 2:
        return []
    out: list[str] = []
    for row in data[1:]:
        if isinstance(row, list) and row:
            ts = str(row[0]).strip()
        else:
            ts = str(row).strip()
        if ts.isdigit() and len(ts) >= 8:
            out.append(ts)
    return out


def wayback_fetch_html(timestamp: str, url: str, *, timeout: int = 60) -> str | None:
    """
    Fetch archived HTML for URL at a timestamp.
    Returns None if response isn't usable HTML.
    """
    ts = str(timestamp).strip()
    if not ts:
        return None
    wb = f"https://web.archive.org/web/{ts}id_/{url}"
    try:
        r = requests.get(
            wb,
            headers={
                "User-Agent": UA,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=timeout,
        )
        if r.status_code != 200:
            return None
        ct = (r.headers.get("content-type") or "").lower()
        if "text/html" not in ct and "application/xhtml" not in ct:
            return None
        r.encoding = r.apparent_encoding or "utf-8"
        return r.text
    except Exception:
        return None


def _extract_json_object_from(html: str, start_brace: int) -> tuple[dict[str, Any] | None, int | None]:
    """Return (obj, end_index) by brace-matching from start_brace pointing at '{'."""
    if start_brace < 0 or start_brace >= len(html) or html[start_brace] != "{":
        return None, None
    depth = 0
    in_str = False
    esc = False
    quote = ""
    for i in range(start_brace, len(html)):
        ch = html[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == quote:
                in_str = False
            continue
        if ch in '"\'':
            in_str = True
            quote = ch
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                blob = html[start_brace : i + 1]
                try:
                    return json.loads(blob), i + 1
                except json.JSONDecodeError:
                    return None, None
    return None, None


def parse_version_history_items(html: str) -> list[dict[str, str]]:
    """
    Returns list of {version_number, release_date_raw, release_notes_raw}
    from embedded versionHistory pageData (detail paragraphs).
    """
    marker = '"page":"versionHistory"'
    positions = [m.start() for m in re.finditer(re.escape(marker), html)]
    best: list[dict[str, str]] = []

    for pos in positions:
        pd = html.find('"pageData":', pos)
        if pd == -1:
            continue
        brace = html.find("{", pd + len('"pageData":'))
        obj, _ = _extract_json_object_from(html, brace)
        if not obj:
            continue
        rows: list[dict[str, str]] = []
        for shelf in obj.get("shelves") or []:
            if not isinstance(shelf, dict):
                continue
            for item in shelf.get("items") or []:
                if not isinstance(item, dict):
                    continue
                if item.get("$kind") != "TitledParagraph":
                    continue
                if item.get("style") != "detail":
                    continue
                ver = (item.get("primarySubtitle") or "").strip()
                if ver.lower().startswith("version "):
                    ver = ver[8:].strip()
                date_raw = (item.get("secondarySubtitle") or "").strip()
                notes = (item.get("text") or "").strip()
                if ver and (notes or date_raw):
                    rows.append(
                        {
                            "version_number": ver,
                            "release_date_raw": date_raw,
                            "release_notes_raw": notes,
                        }
                    )
        if len(rows) > len(best):
            best = rows

    return best
