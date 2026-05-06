"""
Automatic classification of RSS/Atom feeds for Android changelog ingestion.

Samples the first N entries and assigns feed_type:
  release_feed | product_blog | reject
"""

from __future__ import annotations

import re
from typing import Any, Literal

FeedType = Literal["release_feed", "product_blog", "reject"]

SEMVER_LIKE = re.compile(r"\b(?:v\s*)?\d+\.\d+(?:\.\d+){0,3}\b", re.I)

EXPLICIT_DATE_IN_TEXT = re.compile(
    r"(?i)"
    r"\b(19|20)\d{2}-\d{2}-\d{2}\b"
    r"|\b(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+\d{1,2}(st|nd|rd|th)?,?\s+(19|20)\d{2}\b"
    r"|\b\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+(19|20)\d{2}\b"
    r"|\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+(19|20)\d{2}\b",
)

FEED_CHANGELOG_STYLE = re.compile(
    r"(?i)(what(\s|'|’|s\s+)?s?\s*new|whats\s+new|changelog|release\s*notes|this\s+(release|update|version)|"
    r"rolling\s*out|now\s*available|update\s*includes|bug\s*fix|performance|"
    r"improvements?\s+to|security\s*patch|version\s+\d|introducing|announcing|"
    r"new\s+features|product\s+update)",
)


def _entry_combined_text(entry: dict[str, Any]) -> str:
    title = (entry.get("title") or "").strip()
    summary = (entry.get("summary") or entry.get("description") or "").strip()
    if summary:
        return f"{title}\n{summary}".strip()
    return title


def entry_signals(entry: dict[str, Any]) -> dict[str, bool]:
    """Per-entry checks used for feed classification and row routing."""
    text = _entry_combined_text(entry)
    ts = bool(entry.get("published_parsed") or entry.get("updated_parsed"))
    return {
        "version_like": bool(text and SEMVER_LIKE.search(text)),
        "timestamp_present": ts,
        "changelog_style": bool(
            text and len(text) >= 36 and FEED_CHANGELOG_STYLE.search(text)
        ),
        "explicit_date_in_text": bool(text and EXPLICIT_DATE_IN_TEXT.search(text)),
        "nonempty": bool(text and len(text) >= 12),
    }


def _summarize_flags(flags: list[dict[str, bool]]) -> dict[str, int]:
    keys = ("version_like", "timestamp_present", "changelog_style", "explicit_date_in_text")
    return {k: sum(1 for f in flags if f.get(k)) for k in keys}


def _feed_url_suggests_release_channel(feed_url: str | None) -> bool:
    """URL path often indicates a release-notes feed (vs general blog)."""
    if not feed_url:
        return False
    u = feed_url.lower()
    return any(
        s in u
        for s in (
            "/releases",
            "releases.xml",
            "releases.atom",
            "release-notes",
            "releasenotes",
            "changelog",
            "whats-new",
            "what-s-new",
            "/updates/",
            "github.com/",
        )
    )


def classify_feed_sample(
    entries: list[Any],
    *,
    sample_max: int = 5,
    feed_url: str | None = None,
) -> tuple[FeedType, list[dict[str, bool]], int, dict[str, int]]:
    """
    Classify feed from the first up to ``sample_max`` entries (fewer if the
    feed is short — typical audit window is 3–5 items). Returns
    (feed_type, per-entry signal dicts for the sample, sample_n, aggregate counts).
    """
    n_feed = len(entries)
    if n_feed == 0:
        return "reject", [], 0, {k: 0 for k in ("version_like", "timestamp_present", "changelog_style", "explicit_date_in_text")}

    k = min(sample_max, n_feed)
    if k < 1:
        z = {kk: 0 for kk in ("version_like", "timestamp_present", "changelog_style", "explicit_date_in_text")}
        return "reject", [], 0, z
    sample = entries[:k]

    flags: list[dict[str, bool]] = []
    for ent in sample:
        e = ent if isinstance(ent, dict) else {}
        flags.append(entry_signals(e))

    counts = _summarize_flags(flags)

    if not any(f["nonempty"] for f in flags):
        return "reject", flags, k, counts

    n = len(flags)
    sv = counts["version_like"]
    sd = counts["explicit_date_in_text"]
    sch = counts["changelog_style"]
    st = counts["timestamp_present"]

    # release_feed: version-heavy, date-in-text-heavy, or strong release-note shape
    if sv >= 2 or sd >= 2:
        return "release_feed", flags, k, counts
    if sch >= 3 and (sv >= 1 or sd >= 1):
        return "release_feed", flags, k, counts
    if sch >= 4 and st >= max(3, n - 1):
        return "release_feed", flags, k, counts
    if sv >= 1 and sch >= 2 and st >= 2:
        return "release_feed", flags, k, counts

    url_release = _feed_url_suggests_release_channel(feed_url)
    if url_release and st >= max(3, n - 1) and any(f["nonempty"] for f in flags):
        return "release_feed", flags, k, counts

    # reject: very thin or no timestamps and no product signals (broken / non-feed)
    if st == 0 and sv == 0 and sd == 0 and sch <= 1:
        return "reject", flags, k, counts

    return "product_blog", flags, k, counts


def entry_qualifies_for_version_row(feed_type: FeedType, entry: dict[str, Any]) -> bool:
    """developer_changelog row only if release_feed AND (semver OR explicit date in text)."""
    if feed_type != "release_feed":
        return False
    text = _entry_combined_text(entry)
    if not text:
        return False
    return bool(SEMVER_LIKE.search(text) or EXPLICIT_DATE_IN_TEXT.search(text))
