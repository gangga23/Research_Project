"""
Normalized mobile app version dataset for time-series–ready storage.

iOS: App Store web embedded versionHistory (+ Lookup for app_master only).
Android: hierarchical Play snapshot (high) → Wayback (medium) → optional
RSS/Atom (auto-classified; strict ``developer_changelog`` rows only from
``release_feed`` items with semver or in-text date; else ``feature_signal``)
→ review inferred (low, only if no structured signal from snapshot+Wayback
+ strict changelog rows).
"""

from __future__ import annotations

import html
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from dateutil import parser as date_parser
from google_play_scraper import app

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from android_hierarchical import build_android_history_rows
from app_store_web_history import (
    app_store_product_url,
    fetch_app_store_html,
    parse_version_history_items,
    wayback_fetch_html,
    wayback_list_timestamps,
)
from export_workbook_bundle import export_workbook_bundle

CONFIG_PATH = ROOT / "config" / "apps.json"
OUTPUT_DIR = ROOT / "output"

ITUNES_LOOKUP = "https://itunes.apple.com/lookup"

ALLOWED_SOURCE_TYPES = frozenset(
    {
        "play_store_snapshot",
        "wayback_snapshot",
        "developer_changelog",
        "feature_signal",
        "apkmirror_cache",
        "review_inferred",
        "app_store_web",
    }
)
ALLOWED_CONFIDENCE = frozenset({"high", "medium", "low"})

UPDATE_CATEGORIES: tuple[str, ...] = (
    "Bug fixes / performance improvements",
    "UI / design changes",
    "Creator tools / content features",
    "Localization / languages",
    "Enterprise / admin features",
    "Privacy / data policy changes",
    "AI-related features",
    "Payments / monetization",
    "Personalization / recommendations",
    "Security / account safety",
    "SDK / API / developer integration",
    "New product feature",
    "Other",
)

# Priority order: first match wins (single strict label).
CATEGORY_RULES: list[tuple[str, tuple[re.Pattern[str], ...]]] = [
    ("Security / account safety", (re.compile(r"\b(security|password|2fa|two-factor|fraud|verify account|login|auth)\b", re.I),)),
    # Avoid over-labeling from "privacy statement" footer links; require a policy/update/tracking/data cue.
    ("Privacy / data policy changes", (re.compile(r"\b(privacy policy|data policy|tracking|gdpr|personal data|privacy (update|updates|changes?|settings?))\b", re.I),)),
    (
        "Enterprise / admin features",
        (
            re.compile(
                r"\b(admin|admins|enterprise|org(?:anization)?|workspace admin|controls? for admins|permissions?|"
                r"roles?|audit|compliance|sso|scim|governance)\b",
                re.I,
            ),
        ),
    ),
    ("Payments / monetization", (
        re.compile(
            r"\b(subscribe|subscription|billing|wallet|iap|in-app purchase|checkout|purchase|pricing plan|"
            r"premium\b|paywall|recurring payment|renewal|\btrial\b|upgrade to premium|cash back|cashback|"
            r"rewards?|offers?)\b",
            re.I,
        ),
    )),
    (
        "AI-related features",
        (
            re.compile(
                r"\b(ai|a\.i\.|artificial intelligence|gpt|openai|chatgpt|generative|llm|copilot|claude|gemini|neural|"
                r"machine learning)\b|\bml\b",
                re.I,
            ),
            re.compile(r"\b(agent|agents)\b", re.I),
            re.compile(r"\b(opus)\b", re.I),
        ),
    ),
    (
        "SDK / API / developer integration",
        (
            re.compile(r"\b(sdk|api|integration|developer|oauth|webhook)\b", re.I),
            # Common enterprise integrations referenced in release notes
            re.compile(r"\b(slack|salesforce|asana|github|jira|zapier)\b", re.I),
        ),
    ),
    # Before generic marketing ("introducing", broad availability copy) so fix/crash language wins.
    (
        "Bug fixes / performance improvements",
        (
            re.compile(
                r"\b(bug|bugs|fix|fixes|fixed|fixing|crash(?:ed|es)?|stability|performance|slow|freeze|hotfix|patch|"
                r"resolved|mitigate|optimized|optimizations?|rollback|faster|speed|latency)\b",
                re.I,
            ),
            # Common vague-but-non-feature boilerplate used by some apps (e.g. Spotify).
            re.compile(r"\b(changes and improvements|improvements and fixes)\b", re.I),
            # Netflix frequently uses "player improvements" phrasing.
            re.compile(r"\bplayer improvements?\b", re.I),
            # Netflix also uses "playback improvements" wording.
            re.compile(r"\bplayback improvements?\b", re.I),
            # Notion-style minimal changelog phrasing.
            re.compile(r"\bimproving the basics\b", re.I),
        ),
    ),
    (
        "Localization / languages",
        (
            re.compile(
                r"\b(new languages?|in \d+ new languages?|locali[sz]ation|translated|translation|"
                r"language support|internationali[sz]ation|i18n)\b",
                re.I,
            ),
        ),
    ),
    (
        "Creator tools / content features",
        (
            re.compile(
                r"\b(sticker|stickers|filter|filters|effect|effects|lens|lenses|template|templates|"
                r"caption|captions|subtitle|subtitles|edit(?:ing)?|editor|camera|video|reels|story|stories|"
                r"upload|post|posting|share|sharing|clip|clips|shoot videos?)\b",
                re.I,
            ),
            # Lightweight social creation interactions
            re.compile(r"\b(tag your friends?|tag friends?)\b", re.I),
            re.compile(r"\bcomments?\b", re.I),
        ),
    ),
    (
        "UI / design changes",
        (
            re.compile(r"\b(ui|design|layout|dark mode|theme|interface|visual)\b", re.I),
            # Netflix gallery UI note
            re.compile(r"\bgallery improvements?\b", re.I),
        ),
    ),
    ("Personalization / recommendations", (re.compile(r"\b(recommend|recommending|personali[sz]e|for you|discover feed|suggested|profiles?)\b", re.I),)),
    # Narrow vs older rule: drop easy false positives ("now available", "profiles") that stole labels from Bug/UI.
    (
        "New product feature",
        (
            re.compile(
                r"\b(new feature|introducing|now you can|added support|launches|new:|download|offline|rent|"
                r"\bmail\b|\bcalendar\b|forms?\b|rollups?\b|dashboard views?\b|tabs?\b|discussions?\b|library\b|"
                r"teamspaces?\b|comment reactions?\b|progress bars?\b|simple tables?\b|automate workflows?\b|"
                r"automations?\b)\b",
                re.I,
            ),
        ),
    ),
]


def load_apps() -> list[dict]:
    with CONFIG_PATH.open(encoding="utf-8") as f:
        return json.load(f)


def itunes_lookup(app_id: int, country: str = "us") -> dict | None:
    r = requests.get(ITUNES_LOOKUP, params={"id": app_id, "country": country}, timeout=60)
    r.raise_for_status()
    data = r.json().get("results") or []
    return data[0] if data else None


def stable_app_id(app_name: str, platform: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "_", app_name.lower()).strip("_")[:48]
    plat = "ios" if platform.lower() == "ios" else "android"
    return f"{base}_{plat}"


def _fix_mojibake_punct(text: str) -> str:
    """
    Repair common Windows-1252/UTF-8 mojibake seen in store release notes, e.g.:
    "Weâ€™re" -> "We’re".
    """
    if not text:
        return ""
    # Fast path: only touch strings that show telltale sequences.
    if "â" not in text and "Ã" not in text:
        return text
    repl = {
        "â€™": "’",
        "â€˜": "‘",
        "â€œ": "“",
        "â€�": "”",
        "â€“": "–",
        "â€”": "—",
        "â€¦": "…",
        "Â ": " ",
        "Â": "",
    }
    out = text
    for a, b in repl.items():
        out = out.replace(a, b)
    return out


def strip_html(text: str) -> str:
    if not text:
        return ""
    t = html.unescape(text)
    t = re.sub(r"<[^>]+>", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return _fix_mojibake_punct(t)


def strip_marketing_lines(text: str) -> str:
    if not text:
        return ""
    skip = re.compile(
        r"follow us on|subscribe to|rate this app|http://|https://|www\.|#\w+\b|instagram\.com|tiktok\.com|twitter\.com|facebook\.com",
        re.I,
    )
    out_lines = []
    for line in text.splitlines():
        line = line.strip()
        if not line or skip.search(line):
            continue
        out_lines.append(line)
    return " ".join(re.sub(r"\s+", " ", " ".join(out_lines)).split())


def clean_release_notes(raw: str | None, max_chars: int = 4000) -> tuple[str, str | None]:
    """
    Returns (cleaned_notes, short_flag_if_truncated_or_empty_logic).
    """
    if not raw or not str(raw).strip():
        return "Not available", None
    t = strip_html(raw)
    t = strip_marketing_lines(t)
    t = t.strip()
    if not t:
        return "Not available", None
    flag = None
    if len(t) > max_chars:
        t = t[:max_chars].rsplit(" ", 1)[0] + " …"
        flag = "truncated_after_clean"
    return t, flag


def pick_update_category(text: str) -> str:
    if not text or text == "Not available":
        return "Other"
    for label, patterns in CATEGORY_RULES:
        if label not in UPDATE_CATEGORIES:
            continue
        for p in patterns:
            if p.search(text):
                return label
    return "Other"


def to_iso_datetime(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        try:
            dt = date_parser.parse(s, fuzzy=True)
        except (ValueError, TypeError, OverflowError):
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def to_iso_date_only(s: str | None) -> str | None:
    iso = to_iso_datetime(s)
    if not iso:
        return None
    return iso[:10]


def play_listing_date_iso(play: dict) -> str | None:
    ts = play.get("updated")
    if isinstance(ts, (int, float)) and ts > 1_000_000_000:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat()
    lu = play.get("lastUpdatedOn")
    if isinstance(lu, str) and lu.strip():
        d = to_iso_date_only(lu)
        return d
    return None


def parse_play_initial_released(play: dict) -> str | None:
    rel = play.get("released")
    if not isinstance(rel, str) or not rel.strip():
        return None
    return to_iso_date_only(rel)


# Android app_master: standardized missing-Play-released text (see build_master_android / iTunes proxy below).
_ANDROID_MASTER_MISSING_RELEASED = (
    "missing initial_release_date: google-play-scraper returned no parseable Play `released` field"
)
# Keep initial_release_date blank when Play omits `released` (document inference in notes instead).
_SKIP_IOS_INITIAL_PROXY_ANDROID_KEYS = frozenset({"paypal"})


def build_master_ios(cfg: dict, ios: dict, country: str = "us") -> dict:
    ios_id = int(cfg["ios_app_id"])
    # Canonical web URL (version history is parsed from this page, not Lookup).
    url = f"https://apps.apple.com/{country}/app/id{ios_id}"
    init = to_iso_date_only(ios.get("releaseDate"))
    notes = ""
    if not init:
        notes = "initial_release_date missing: iTunes Lookup did not return a parseable releaseDate."
    return {
        "app_id": stable_app_id(cfg["app_name"], "iOS"),
        "app_name": cfg["app_name"],
        "platform": "iOS",
        "developer": (ios.get("sellerName") or ios.get("artistName") or "").strip(),
        "category": (ios.get("primaryGenreName") or "").strip(),
        "initial_release_date": init,
        "source_url": url,
        "current_version": (ios.get("version") or "").strip(),
        "current_version_release_date": to_iso_date_only(ios.get("currentVersionReleaseDate")),
        "notes": notes,
    }


def build_master_android(cfg: dict, play: dict) -> dict:
    pkg = cfg["android_package"]
    url = play.get("url") or f"https://play.google.com/store/apps/details?id={pkg}"
    cur_date = play_listing_date_iso(play)
    init = parse_play_initial_released(play)
    notes_parts: list[str] = []
    cur_ver = (play.get("version") or "").strip()
    if not cur_ver:
        notes_parts.append(
            "store current_version missing on google-play-scraper listing snapshot (Play listing)"
        )
    elif cur_ver.lower() == "varies with device":
        notes_parts.append(
            "store current_version is 'Varies with device' on Play (multiple APK variants; "
            "no single semantic-version string comparable to iOS)"
        )
    if not init:
        miss = _ANDROID_MASTER_MISSING_RELEASED
        if (cfg.get("app_key") or "").strip().lower() == "paypal":
            miss += " (can be substituted from iOS data as inference)"
        notes_parts.append(miss)
    return {
        "app_id": stable_app_id(cfg["app_name"], "Android"),
        "app_name": cfg["app_name"],
        "platform": "Android",
        "developer": (play.get("developer") or "").strip(),
        "category": (play.get("genre") or "").strip(),
        "initial_release_date": init,
        "source_url": url,
        "current_version": cur_ver,
        "current_version_release_date": cur_date,
        "notes": "; ".join(notes_parts),
    }


def parse_app_store_display_date(raw: str | None) -> str | None:
    if not raw or not str(raw).strip():
        return None
    try:
        dt = date_parser.parse(str(raw), fuzzy=True)
        return dt.date().isoformat()
    except (ValueError, TypeError, OverflowError):
        return None


def build_ios_version_rows(cfg: dict, ios: dict, country: str = "us") -> list[dict]:
    """
    Prefer multi-version rows from App Store web embedded JSON; if none,
    one row from iTunes Lookup current build only.
    """
    app_id = stable_app_id(cfg["app_name"], "iOS")
    name = cfg["app_name"]
    ios_id = int(cfg["ios_app_id"])
    rows: list[dict] = []
    product_url = app_store_product_url(ios_id, country=country)

    MIN_LIVE_ROWS_FOR_WAYBACK_FALLBACK = 15

    items_live: list[dict[str, str]] = []
    try:
        html_doc = fetch_app_store_html(ios_id, country=country)
        items_live = parse_version_history_items(html_doc) or []
    except Exception:
        items_live = []

    # Primary path: live embed has enough depth.
    if len(items_live) >= MIN_LIVE_ROWS_FOR_WAYBACK_FALLBACK:
        for it in items_live:
            notes, trunc = clean_release_notes(it.get("release_notes_raw"))
            cat = pick_update_category(notes)
            rdate = parse_app_store_display_date(it.get("release_date_raw")) or ""
            ver = (it.get("version_number") or "").strip()
            rows.append(
                {
                    "app_id": app_id,
                    "app_name": name,
                    "platform": "iOS",
                    "version_number": ver or "",
                    "release_date": rdate,
                    "release_notes": notes,
                    "source_type": "app_store_web",
                    "confidence_level": "high",
                    "update_category": cat,
                    "history_source_url": product_url,
                }
            )
        return rows

    # Fallback: if live embed exists but is shallow (< threshold), augment using Wayback snapshots.
    # Only accept versionHistory items actually embedded in archived HTML (not archive timestamp as release date).
    if items_live:
        seen: set[tuple[str, str]] = set()
        for it in items_live:
            v = (it.get("version_number") or "").strip()
            d = (it.get("release_date_raw") or "").strip()
            if v and d:
                seen.add((v, d))

        added = 0
        for ts in wayback_list_timestamps(product_url, max_results=60):
            wh = wayback_fetch_html(ts, product_url)
            if not wh:
                continue
            try:
                items_wb = parse_version_history_items(wh) or []
            except Exception:
                items_wb = []
            if not items_wb:
                continue
            for it in items_wb:
                ver = (it.get("version_number") or "").strip()
                date_raw = (it.get("release_date_raw") or "").strip()
                if not ver or not date_raw or (ver, date_raw) in seen:
                    continue
                seen.add((ver, date_raw))
                notes, _trunc = clean_release_notes(it.get("release_notes_raw"))
                cat = pick_update_category(notes)
                rdate = parse_app_store_display_date(date_raw) or ""
                rows.append(
                    {
                        "app_id": app_id,
                        "app_name": name,
                        "platform": "iOS",
                        "version_number": ver,
                        "release_date": rdate,
                        "release_notes": notes,
                        "source_type": "wayback_snapshot",
                        "confidence_level": "medium",
                        "update_category": cat,
                    }
                )
                added += 1
                if len(items_live) + added >= MIN_LIVE_ROWS_FOR_WAYBACK_FALLBACK:
                    break
            if len(items_live) + added >= MIN_LIVE_ROWS_FOR_WAYBACK_FALLBACK:
                break

    # Shallow live rows (if any) are still kept.
    if items_live:
        for it in items_live:
            notes, trunc = clean_release_notes(it.get("release_notes_raw"))
            cat = pick_update_category(notes)
            rdate = parse_app_store_display_date(it.get("release_date_raw")) or ""
            ver = (it.get("version_number") or "").strip()
            rows.append(
                {
                    "app_id": app_id,
                    "app_name": name,
                    "platform": "iOS",
                    "version_number": ver or "",
                    "release_date": rdate,
                    "release_notes": notes,
                    "source_type": "app_store_web",
                    "confidence_level": "high",
                    "update_category": cat,
                    "history_source_url": product_url,
                }
            )
        return rows

    # Fallback: iTunes Lookup current build only (not multi-version web payload).
    notes, _trunc = clean_release_notes(ios.get("releaseNotes"))
    cat = pick_update_category(notes)
    rows.append(
        {
            "app_id": app_id,
            "app_name": name,
            "platform": "iOS",
            "version_number": (ios.get("version") or "").strip(),
            "release_date": to_iso_date_only(ios.get("currentVersionReleaseDate")) or "",
            "release_notes": notes,
            "source_type": "app_store_web",
            "confidence_level": "medium",
            "update_category": cat,
            "history_source_url": product_url,
        }
    )
    return rows


def schema_text() -> str:
    return """TABLE submission_observations (assignment-aligned denormalized export)
  app_id, app_name, platform, developer, category, initial_release_date,
  version_number, release_date, is_current_version (Yes | No | Unknown),
  store_current_version, store_current_version_release_date,
  history_source_url (single clickable source URL: Wayback/feed/APKMirror row URL when https; else store listing),
  release_notes, update_category, update_summary, source_type, confidence_level, notes

TABLE app_master
  app_id, app_name, platform, developer, category, initial_release_date,
  source_url, current_version, current_version_release_date, notes

TABLE app_version_history
  app_id, app_name, platform
  version_number (empty string when unknown; never fabricated)
  release_date (ISO YYYY-MM-DD or empty)
  release_notes (cleaned; Not available when absent)
  history_source_url (listing/Wayback/feed scrape URL; APKMirror permalink when matched from data/cache)
  source_type (strict):
    - play_store_snapshot | wayback_snapshot | developer_changelog |
      feature_signal | apkmirror_cache | review_inferred (Android)
    - app_store_web (iOS web / lookup fallback)
  confidence_level: high | medium | low
  update_category: one of ten fixed labels

Android: Play live HTML+app() snapshot (high) → Wayback archived Play pages
(medium) → optional feed URL: validator classifies feed; matching items become
developer_changelog, non-matching items feature_signal → optional
data/cache/apkmirror_{app_id}.csv rows as apkmirror_cache (medium) → review_inferred (low)
only if none of the above yield structured signal (including APKMirror cache).
iOS: app_store_web embedded versionHistory (high) or lookup-only fallback (medium).
"""


def format_feed_validation_report(feed_rows: list[dict]) -> str:
    lines = ["feed_validation_report", ""]
    for d in feed_rows:
        lines.append(
            f"app={d.get('app_name')} package={d.get('android_package')} "
            f"feed_type={d.get('feed_type')} sample_n={d.get('feed_sample_n')} "
            f"count_version_like={d.get('count_version_like')} "
            f"count_timestamp={d.get('count_timestamp')} "
            f"count_changelog_style={d.get('count_changelog_style')} "
            f"count_explicit_date_in_text={d.get('count_explicit_date_in_text')} "
            f"parser_bozo={d.get('parser_bozo')} url={d.get('feed_url')}"
        )
        if d.get("fetch_error"):
            lines.append(f"  fetch_error={d['fetch_error']}")
    return "\n".join(lines)


def data_quality_report(version_df: pd.DataFrame, n_config_apps: int, master_rows: int) -> str:
    n = len(version_df)
    ad = version_df[version_df["platform"] == "Android"]
    na = max(len(ad), 1)

    def _pct(mask: pd.Series) -> float:
        return 100.0 * float(mask.sum()) / na

    p_play = _pct(ad["source_type"] == "play_store_snapshot")
    p_way = _pct(ad["source_type"] == "wayback_snapshot")
    p_dev = _pct(ad["source_type"] == "developer_changelog")
    p_feat = _pct(ad["source_type"] == "feature_signal")
    p_apk = _pct(ad["source_type"] == "apkmirror_cache")
    p_rev = _pct(ad["source_type"] == "review_inferred")
    mv = ad["version_number"].fillna("").astype(str).str.strip() == ""
    miss_ver = 100.0 * float(mv.sum()) / na

    ios = version_df[version_df["platform"] == "iOS"]
    lines = [
        "data_quality_report",
        f"config_apps: {n_config_apps}",
        f"app_master_rows: {master_rows}",
        f"app_version_history_rows: {n}",
        f"ios_rows: {len(ios)}",
        f"android_rows: {len(ad)}",
        f"pct_all_rows_app_store_web: {100.0 * len(ios) / max(n, 1):.1f}%",
        f"pct_android_play_store_snapshot_rows: {p_play:.1f}%",
        f"pct_android_wayback_snapshot_rows: {p_way:.1f}%",
        f"pct_android_developer_changelog_rows: {p_dev:.1f}%",
        f"pct_android_feature_signal_rows: {p_feat:.1f}%",
        f"pct_android_apkmirror_cache_rows: {p_apk:.1f}%",
        f"pct_android_review_inferred_rows: {p_rev:.1f}%",
        f"pct_android_missing_version_number: {miss_ver:.1f}%",
        f"pct_all_rows_release_notes_not_available: {100.0 * float((version_df['release_notes'] == 'Not available').sum()) / max(n, 1):.1f}%",
    ]
    return "\n".join(lines)


def validation_report(
    n_config_apps: int,
    master_rows: int,
    version_rows: int,
    version_df: pd.DataFrame,
    n_apps_both_platforms: int,
) -> str:
    """Legacy summary line block + data_quality_report."""
    ios_rows = len(version_df[version_df["platform"] == "iOS"])
    and_rows = len(version_df[version_df["platform"] == "Android"])
    dq = data_quality_report(version_df, n_config_apps, master_rows)
    lines = [
        "validation_report",
        f"config_apps: {n_config_apps}",
        f"app_master_rows: {master_rows}",
        f"app_version_history_rows: {version_rows}",
        f"ios_version_history_rows: {ios_rows}",
        f"android_version_history_rows: {and_rows}",
        f"apps_with_ios_and_android_rows: {n_apps_both_platforms}",
        "",
        dq,
    ]
    return "\n".join(lines)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    apps = load_apps()
    master: list[dict] = []
    versions: list[dict] = []
    feed_validations: list[dict] = []
    both_platforms = 0

    for cfg in apps:
        name = cfg["app_name"]
        ios_id = int(cfg["ios_app_id"])
        pkg = cfg["android_package"]

        ios = itunes_lookup(ios_id)
        if not ios:
            print(f"[warn] iTunes Lookup empty: {name} id={ios_id}", file=sys.stderr)
            continue

        try:
            play = app(pkg, lang="en", country="us")
        except Exception as e:
            print(f"[warn] Play app() failed {name}: {e}", file=sys.stderr)
            play = None

        master.append(build_master_ios(cfg, ios))
        versions.extend(build_ios_version_rows(cfg, ios))

        if play:
            row_android = build_master_android(cfg, play)
            # google-play-scraper often omits ``released`` (initial ship date). Proxy from iTunes releaseDate
            # when both platforms exist — clearer app_master than a blank Android cell (still approximate).
            if not row_android.get("initial_release_date"):
                fb = to_iso_date_only(ios.get("releaseDate"))
                if fb and (cfg.get("app_key") or "").strip().lower() not in _SKIP_IOS_INITIAL_PROXY_ANDROID_KEYS:
                    row_android["initial_release_date"] = fb
                    prior = str(row_android.get("notes") or "").strip()
                    if _ANDROID_MASTER_MISSING_RELEASED in prior:
                        prior = prior.replace(_ANDROID_MASTER_MISSING_RELEASED, "").strip()
                        prior = re.sub(r"^[;\s]+|[;\s]+$", "", prior)
                    extra = (
                        "initial_release_date proxied from iTunes Lookup releaseDate because google-play-scraper "
                        "returned no parseable Play `released` field"
                    )
                    row_android["notes"] = (prior + ("; " if prior else "") + extra).strip()
            master.append(row_android)
            aid = stable_app_id(cfg["app_name"], "Android")
            and_rows, feed_meta = build_android_history_rows(cfg, play, pkg, aid, pick_update_category)
            versions.extend(and_rows)
            feed_validations.extend(feed_meta)
            both_platforms += 1

    master_cols = [
        "app_id",
        "app_name",
        "platform",
        "developer",
        "category",
        "initial_release_date",
        "source_url",
        "current_version",
        "current_version_release_date",
        "notes",
    ]
    version_cols = [
        "app_id",
        "app_name",
        "platform",
        "version_number",
        "release_date",
        "release_notes",
        "history_source_url",
        "source_type",
        "confidence_level",
        "update_category",
    ]

    master_df = pd.DataFrame(master)[master_cols]
    version_df = pd.DataFrame(versions)[version_cols]

    bad = set(version_df["update_category"].unique()) - set(UPDATE_CATEGORIES)
    if bad:
        raise RuntimeError(f"Invalid update_category values: {bad}")
    bad_s = set(version_df["source_type"].unique()) - ALLOWED_SOURCE_TYPES
    if bad_s:
        raise RuntimeError(f"Invalid source_type values: {bad_s}")
    bad_c = set(version_df["confidence_level"].unique()) - ALLOWED_CONFIDENCE
    if bad_c:
        raise RuntimeError(f"Invalid confidence_level values: {bad_c}")

    rep = export_workbook_bundle(
        master_df,
        version_df,
        n_config_apps=len(apps),
        both_platforms=both_platforms,
        feed_validations=feed_validations,
        output_dir=OUTPUT_DIR,
        repo_root=ROOT,
        script_dir=SCRIPT_DIR,
        rewrite_master_version_csv=True,
        rewrite_feed_validation_report=True,
    )

    xlsx = OUTPUT_DIR / "normalized_dataset.xlsx"
    print(
        f"Wrote {OUTPUT_DIR / 'app_master.csv'}, {OUTPUT_DIR / 'app_version_history.csv'}, "
        f"{OUTPUT_DIR / 'submission_observations.csv'}, {OUTPUT_DIR / 'feed_validation_report.txt'}, {xlsx}"
    )
    print(rep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
