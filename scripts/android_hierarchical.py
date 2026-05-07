"""
Android version history: hierarchical sources

Play snapshot (high) → merged Wayback CDX (medium) → optional developer
RSS/Atom (`android_changelog_feed_url`): auto-classified feeds; only
`release_feed` entries with semver or explicit in-text date become
`developer_changelog`; others become `feature_signal`.

If ``data/cache/apkmirror_{app_id}.csv`` exists with usable rows (version + APKMirror
/apk/ URL), those rows are ingested as ``apkmirror_cache`` (medium) and count as
structured coverage — skipping the review inferred fallback.
Blank CSV ``release_date`` cells trigger a one-time APKMirror release-page fetch for an
``Uploaded`` timestamp (cached in ``data/cache/apkmirror_upload_dates.json``) so dated
cadence charts can include Android APKMirror rows.

Review inferred (low) runs only when snapshot, Wayback, feed strict changelog,
and APKMirror cache together provide no structured signal.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from feed_validator import (
    SEMVER_LIKE,
    classify_feed_sample,
    entry_qualifies_for_version_row,
)
from urllib.parse import urlparse

import requests
from google_play_scraper import Sort, reviews
from google_play_scraper.constants.regex import Regex
from google_play_scraper.constants.request import Formats
from google_play_scraper.utils.request import get

HEUR_EXCLUDE = re.compile(
    r"top charts|similar apps|you might also|data safety|privacy policy|"
    r"see more|developer|contains ads|everyone|teen|mature|"
    r"all rights reserved|©",
    re.I,
)
HEUR_INCLUDE = re.compile(
    r"(bug|fix|update|improv|new feature|performance|security|"
    r"release|changelog|what's new|whats new|version)",
    re.I,
)


def fetch_play_detail_html(package_id: str, lang: str = "en", country: str = "us") -> str:
    url = Formats.Detail.build(app_id=package_id, lang=lang, country=country)
    try:
        return get(url)
    except Exception:
        return get(Formats.Detail.fallback_build(app_id=package_id, lang=lang))


def _clean_notes(raw: str | None, max_len: int = 4000) -> str:
    from html import unescape

    if not raw or not str(raw).strip():
        return "Not available"
    t = unescape(str(raw))
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    # Fix common mojibake punctuation (UTF-8 decoded as cp1252), e.g. â€™ -> ’.
    if "â" in t or "Ã" in t:
        t = (
            t.replace("â€™", "’")
            .replace("â€˜", "‘")
            .replace("â€œ", "“")
            .replace("â€�", "”")
            .replace("â€“", "–")
            .replace("â€”", "—")
            .replace("â€¦", "…")
            .replace("Â ", " ")
            .replace("Â", "")
        )
    if not t:
        return "Not available"
    return t[:max_len]


def _walk_collect_strings(obj: Any, out: list[str], depth: int = 0) -> None:
    if depth > 26:
        return
    if isinstance(obj, str) and 45 <= len(obj) <= 6000:
        if HEUR_INCLUDE.search(obj) and not HEUR_EXCLUDE.search(obj):
            out.append(obj)
    elif isinstance(obj, list):
        for x in obj[:600]:
            _walk_collect_strings(x, out, depth + 1)
    elif isinstance(obj, dict):
        for v in obj.values():
            _walk_collect_strings(v, out, depth + 1)


def heuristic_whatsnew_from_html(html: str, description_prefix: str | None) -> str | None:
    candidates: list[str] = []
    for m in Regex.SCRIPT.findall(html):
        if not Regex.KEY.findall(m):
            continue
        try:
            root = json.loads(Regex.VALUE.findall(m)[0])
        except (json.JSONDecodeError, IndexError):
            continue
        _walk_collect_strings(root, candidates)
    if not candidates:
        return None
    seen: set[str] = set()
    uniq: list[str] = []
    for s in candidates:
        h = hashlib.sha256(s[:400].encode("utf-8", errors="ignore")).hexdigest()[:16]
        if h in seen:
            continue
        seen.add(h)
        uniq.append(s)
    if not uniq:
        return None
    best = max(uniq, key=len)
    if description_prefix and best.strip().startswith(description_prefix.strip()[:200]):
        rest = [x for x in uniq if x != best]
        if not rest:
            return None
        best = max(rest, key=len)
    cleaned = re.sub(r"\s+", " ", best).strip()
    return cleaned[:4000] if cleaned else None


def _norm_note(s: str) -> str:
    return re.sub(r"\s+", " ", s.lower())[:500]


def wayback_list_timestamps_for_url(cdx_host_path: str, *, max_results: int = 40) -> list[str]:
    for attempt in range(3):
        try:
            r = requests.get(
                "https://web.archive.org/cdx/search/cdx",
                params={
                    "url": cdx_host_path,
                    "output": "json",
                    "filter": "statuscode:200",
                    "collapse": "digest",
                    "limit": str(max_results * 4),
                },
                headers={"User-Agent": "ResearchProject/1.0 (educational)"},
                timeout=50,
            )
            if r.status_code != 200:
                raise RuntimeError(f"cdx_{r.status_code}")
            data = r.json()
            if not data or len(data) < 2:
                return []
            out: list[str] = []
            for row in data[1:]:
                if isinstance(row, list) and len(row) > 1:
                    ts = row[1]
                    if isinstance(ts, str) and len(ts) >= 8 and ts.isdigit():
                        out.append(ts)
            dedup: list[str] = []
            s2 = set()
            for ts in out:
                if ts not in s2:
                    s2.add(ts)
                    dedup.append(ts)
            return dedup[:max_results]
        except Exception:
            time.sleep(1.2 * (attempt + 1))
    return []


def wayback_timestamps_merged(package_id: str, *, max_per_pattern: int = 45) -> list[str]:
    """Merge CDX hits for common Play listing URL variants (more snapshots)."""
    patterns = [
        f"play.google.com/store/apps/details?id={package_id}&hl=en&gl=us",
        f"play.google.com/store/apps/details?id={package_id}&hl=en",
        f"play.google.com/store/apps/details?id={package_id}",
    ]
    all_ts: list[str] = []
    for p in patterns:
        all_ts.extend(wayback_list_timestamps_for_url(p, max_results=max_per_pattern))
    uniq = sorted(set(all_ts), reverse=True)
    return uniq


def play_store_listing_url(package_id: str) -> str:
    return f"https://play.google.com/store/apps/details?id={package_id}&hl=en&gl=us"


def android_wayback_capture_url(timestamp: str, package_id: str) -> str:
    return f"https://web.archive.org/web/{timestamp}id_/{play_store_listing_url(package_id)}"


def wayback_fetch_html(timestamp: str, package_id: str) -> str | None:
    url = android_wayback_capture_url(timestamp, package_id)
    try:
        r = requests.get(
            url,
            headers={"User-Agent": "ResearchProject/1.0 (educational)"},
            timeout=50,
        )
        if r.status_code != 200 or len(r.text) < 4000:
            return None
        return r.text
    except Exception:
        return None


def _ts_to_date(ts: str) -> str:
    if len(ts) >= 8 and ts[:8].isdigit():
        y, m, d = int(ts[:4]), int(ts[4:6]), int(ts[6:8])
        return f"{y:04d}-{m:02d}-{d:02d}"
    return ""


def _feed_entry_history_url(ent: dict[str, Any], fallback_feed: str) -> str:
    link = (ent.get("link") or "").strip()
    if link.startswith(("http://", "https://")):
        return link
    fb = (fallback_feed or "").strip()
    return fb if fb.startswith(("http://", "https://")) else ""


def _registrable_host(url: str | None) -> str | None:
    if not url or not isinstance(url, str):
        return None
    try:
        h = (urlparse(url).hostname or "").lower()
        if not h:
            return None
        parts = h.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
    except Exception:
        return None


def developer_feed_rows_from_url(
    feed_url: str,
    play: dict,
    package_id: str,
    app_id: str,
    name: str,
    categorize_fn: Callable[[str], str],
    *,
    max_entries: int = 100,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    """
    Ingest RSS/Atom: classify feed from a sample, then emit ``developer_changelog``
    only for ``release_feed`` items that contain a semver or explicit in-text
    date; all other non-reject items emit ``feature_signal`` (version_number
    cleared for those).
    """
    import feedparser

    parsed = feedparser.parse(feed_url)
    entries = list(getattr(parsed, "entries", []))
    feed_type, _flags, sample_n, counts = classify_feed_sample(entries, sample_max=5, feed_url=feed_url)

    meta: dict[str, Any] = {
        "app_name": name,
        "android_package": package_id,
        "feed_url": feed_url,
        "feed_type": feed_type,
        "feed_sample_n": sample_n,
        "count_version_like": counts["version_like"],
        "count_timestamp": counts["timestamp_present"],
        "count_changelog_style": counts["changelog_style"],
        "count_explicit_date_in_text": counts["explicit_date_in_text"],
        "parser_bozo": bool(getattr(parsed, "bozo", 0)),
    }

    if feed_type == "reject" or not entries:
        return [], meta

    dev_site = play.get("developerWebsite") or ""
    feed_host = _registrable_host(feed_url)
    dev_host = _registrable_host(dev_site)
    conf_ver = (
        "high"
        if (
            feed_host
            and dev_host
            and (feed_host == dev_host or feed_host.endswith(dev_host) or dev_host.endswith(feed_host))
        )
        else "medium"
    )

    rows: list[dict[str, str]] = []
    for ent in entries[:max_entries]:
        title = (ent.get("title") or "").strip()
        summary = (ent.get("summary") or ent.get("description") or "").strip()
        text = _clean_notes(f"{title}\n{summary}" if summary else title)
        if text == "Not available":
            continue
        published = ent.get("published_parsed") or ent.get("updated_parsed")
        rdate = ""
        if published:
            try:
                rdate = datetime(
                    published.tm_year,
                    published.tm_mon,
                    published.tm_mday,
                    tzinfo=timezone.utc,
                ).date().isoformat()
            except Exception:
                rdate = ""
        mver = SEMVER_LIKE.search(title) or SEMVER_LIKE.search(summary)
        ver = ""
        if mver:
            ver = re.sub(r"^v\s*", "", mver.group(0), flags=re.I).strip()

        ent_d: dict[str, Any] = ent if isinstance(ent, dict) else dict(ent)
        is_version_row = feed_type == "release_feed" and entry_qualifies_for_version_row(feed_type, ent_d)
        hist_url = _feed_entry_history_url(ent_d, feed_url)
        if is_version_row:
            rows.append(
                {
                    "app_id": app_id,
                    "app_name": name,
                    "platform": "Android",
                    "version_number": ver,
                    "release_date": rdate,
                    "release_notes": text,
                    "source_type": "developer_changelog",
                    "confidence_level": conf_ver,
                    "update_category": categorize_fn(text),
                    "history_source_url": hist_url,
                }
            )
        else:
            rows.append(
                {
                    "app_id": app_id,
                    "app_name": name,
                    "platform": "Android",
                    "version_number": "",
                    "release_date": rdate,
                    "release_notes": text,
                    "source_type": "feature_signal",
                    "confidence_level": "low",
                    "update_category": categorize_fn(text),
                    "history_source_url": hist_url,
                }
            )

    return rows, meta


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _apkmirror_cache_csv_path(app_id: str) -> Path:
    return _project_root() / "data" / "cache" / f"apkmirror_{app_id}.csv"


def _android_row_version_keys(rows: list[dict[str, str]]) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for r in rows:
        v = (r.get("version_number") or "").strip()
        d = (r.get("release_date") or "").strip()
        keys.add((v, d))
    return keys


def _usable_apkmirror_row_url(url: str) -> bool:
    u = (url or "").strip().lower()
    if not u.startswith(("http://", "https://")):
        return False
    if "apkmirror.com" not in u:
        return False
    return "/apk/" in u


def load_apkmirror_cache_rows(
    app_name: str,
    app_id: str,
    categorize_fn: Callable[[str], str],
    existing_rows: list[dict[str, str]],
) -> list[dict[str, str]]:
    """
    Load ``data/cache/apkmirror_{app_id}.csv`` release rows (non-empty version + /apk/ URL).

    Dedupes against ``existing_rows`` by (version_number, release_date).
    """
    path = _apkmirror_cache_csv_path(app_id)
    if not path.is_file():
        return []

    keys = _android_row_version_keys(existing_rows)
    seen_apk: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []

    try:
        with path.open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                return []
            cols = {h.strip() for h in reader.fieldnames}
            if not {"app_id", "version_number", "apkmirror_url"}.issubset(cols):
                return []
            for row in reader:
                aid = (row.get("app_id") or "").strip()
                if aid != app_id:
                    continue
                ver = (row.get("version_number") or "").strip()
                if not ver:
                    continue
                url = (row.get("apkmirror_url") or "").strip()
                if not _usable_apkmirror_row_url(url):
                    continue
                rdate = (row.get("release_date") or "").strip()
                if not rdate:
                    try:
                        from apkmirror_upload_date import resolve_apk_upload_date

                        rdate = resolve_apk_upload_date(url) or ""
                    except ImportError:
                        pass
                key = (ver, rdate)
                if key in keys or key in seen_apk:
                    continue
                seen_apk.add(key)
                keys.add(key)

                ucat = categorize_fn("Not available")
                out.append(
                    {
                        "app_id": app_id,
                        "app_name": app_name,
                        "platform": "Android",
                        "version_number": ver,
                        "release_date": rdate,
                        "release_notes": "Not available",
                        "source_type": "apkmirror_cache",
                        "confidence_level": "medium",
                        "update_category": ucat,
                        "history_source_url": url,
                    }
                )
    except OSError:
        return []

    return out


def fetch_review_fallback_rows(
    package_id: str,
    *,
    max_reviews: int = 2000,
    max_versions: int = 50,
) -> list[dict[str, Any]]:
    batch, _ = reviews(package_id, sort=Sort.NEWEST, count=max_reviews)
    by_ver: dict[str, list[datetime]] = defaultdict(list)
    for row in batch:
        ver = row.get("appVersion") or row.get("reviewCreatedVersion")
        if not ver or not isinstance(ver, str):
            continue
        ver = ver.strip()
        if not ver or ver.lower() in ("nan", "varies with device", "null"):
            continue
        at = row.get("at")
        if isinstance(at, datetime):
            by_ver[ver].append(at.astimezone(timezone.utc))
    out: list[dict[str, Any]] = []
    for ver, dates in by_ver.items():
        if dates:
            out.append({"version_number": ver, "earliest_review_at": min(dates)})
    out.sort(key=lambda x: x["earliest_review_at"], reverse=True)
    return out[:max_versions]


def build_android_history_rows(
    cfg: dict,
    play: dict,
    package_id: str,
    app_id: str,
    categorize_fn: Callable[[str], str],
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    name = cfg["app_name"]
    rows: list[dict[str, str]] = []

    desc = (play.get("description") or "")[:400]
    html_live = fetch_play_detail_html(package_id)
    whats_raw = heuristic_whatsnew_from_html(html_live, desc[:280] if desc else None)
    notes_primary = _clean_notes(whats_raw)
    # Defensive guard: observed cross-app contamination where Spotify marketing copy appeared
    # as Android release_notes for multiple unrelated packages. Treat as unavailable.
    if (
        notes_primary != "Not available"
        and name.strip().lower() != "spotify"
        and re.search(r"(With the Spotify music and podcast app|WHY SPOTIFY FOR MUSIC AND PODCASTS\?)", notes_primary, re.I)
    ):
        notes_primary = "Not available"

    ver_play = (play.get("version") or "").strip()
    if ver_play.lower() in ("varies with device", ""):
        ver_play = ""
    ts_play = play.get("updated")
    rdate_play = ""
    if isinstance(ts_play, (int, float)) and ts_play > 1_000_000_000:
        rdate_play = datetime.fromtimestamp(int(ts_play), tz=timezone.utc).date().isoformat()
    else:
        lu = play.get("lastUpdatedOn")
        if isinstance(lu, str) and lu.strip():
            try:
                from dateutil import parser as dp

                rdate_play = dp.parse(lu, fuzzy=True).date().isoformat()
            except Exception:
                rdate_play = ""

    concrete_ver = bool(ver_play and re.match(r"^\d+(\.\d+)+", ver_play))
    structured = (notes_primary != "Not available") or concrete_ver

    listing_u = play_store_listing_url(package_id)
    rows.append(
        {
            "app_id": app_id,
            "app_name": name,
            "platform": "Android",
            "version_number": ver_play,
            "release_date": rdate_play,
            "release_notes": notes_primary,
            "source_type": "play_store_snapshot",
            "confidence_level": "high",
            "update_category": categorize_fn(notes_primary),
            "history_source_url": listing_u,
        }
    )

    seen_norm = {_norm_note(notes_primary)} if notes_primary != "Not available" else set()
    wayback_hits = 0
    wb_cap = 0
    for ts in wayback_timestamps_merged(package_id, max_per_pattern=50):
        if wb_cap >= 18:
            break
        wh = wayback_fetch_html(ts, package_id)
        if not wh:
            continue
        wraw = heuristic_whatsnew_from_html(wh, None)
        wnotes = _clean_notes(wraw)
        if wnotes == "Not available":
            continue
        nh = _norm_note(wnotes)
        if nh in seen_norm:
            continue
        seen_norm.add(nh)
        wayback_hits += 1
        wb_cap += 1
        rows.append(
            {
                "app_id": app_id,
                "app_name": name,
                "platform": "Android",
                "version_number": "",
                "release_date": _ts_to_date(ts),
                "release_notes": wnotes,
                "source_type": "wayback_snapshot",
                "confidence_level": "medium",
                "update_category": categorize_fn(wnotes),
                "history_source_url": android_wayback_capture_url(ts, package_id),
            }
        )
        time.sleep(0.22)

    feed_url = (cfg.get("android_changelog_feed_url") or "").strip()
    feed_rows: list[dict[str, str]] = []
    feed_validations: list[dict[str, Any]] = []
    changelog_feed_structured = False
    if feed_url:
        try:
            feed_rows, meta = developer_feed_rows_from_url(
                feed_url, play, package_id, app_id, name, categorize_fn, max_entries=100
            )
            feed_validations.append(meta)
        except Exception as ex:
            feed_rows = []
            feed_validations.append(
                {
                    "app_name": name,
                    "android_package": package_id,
                    "feed_url": feed_url,
                    "feed_type": "reject",
                    "feed_sample_n": 0,
                    "count_version_like": 0,
                    "count_timestamp": 0,
                    "count_changelog_style": 0,
                    "count_explicit_date_in_text": 0,
                    "parser_bozo": False,
                    "fetch_error": str(ex)[:240],
                }
            )
        for fr in feed_rows:
            if fr.get("release_notes") and fr["release_notes"] != "Not available":
                seen_norm.add(_norm_note(fr["release_notes"]))
        rows.extend(feed_rows)
        changelog_feed_structured = any(
            r.get("source_type") == "developer_changelog"
            and r.get("release_notes") not in (None, "", "Not available")
            for r in feed_rows
        )

    apk_cache_rows = load_apkmirror_cache_rows(name, app_id, categorize_fn, rows)
    rows.extend(apk_cache_rows)
    apk_mirror_nonempty = len(apk_cache_rows) > 0

    structured = structured or wayback_hits > 0 or changelog_feed_structured or apk_mirror_nonempty

    if not structured and wayback_hits == 0 and not changelog_feed_structured:
        for item in fetch_review_fallback_rows(package_id):
            at: datetime = item["earliest_review_at"]
            rows.append(
                {
                    "app_id": app_id,
                    "app_name": name,
                    "platform": "Android",
                    "version_number": str(item["version_number"]),
                    "release_date": at.date().isoformat(),
                    "release_notes": "Not available",
                    "source_type": "review_inferred",
                    "confidence_level": "low",
                    "update_category": "Other",
                    "history_source_url": listing_u,
                }
            )

    return rows, feed_validations
