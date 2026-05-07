"""
APKMirror targets from ``config/apps.json``.

Scrape progress is persisted to ``data/cache/apkmirror_status.json``.
On ``--scrape``, apps with status ``complete`` or ``blocked`` are skipped;
``partial`` and ``failed`` apps are re-scraped.

Amazon Shopping uses ``apkmirror_category`` only for uploads pagination (no
listing fetch) so pagination stays on ``appcategory=amazon-shopping``.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Literal
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "apps.json"
CACHE_DIR = ROOT / "data" / "cache"
STATUS_FILENAME = "apkmirror_status.json"
APKMIRROR_APK_PREFIX = "https://www.apkmirror.com/apk/"
APKMIRROR_ORIGIN = "https://www.apkmirror.com"
MAX_UPLOAD_PAGES = 20

TerminalStatus = Literal["complete", "partial", "blocked", "failed"]

SCRAPER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

SKIP_TOKEN_RE = re.compile(r"\b(alpha|beta|stub)\b", re.I)
UPLOADED_RE = re.compile(r"Uploaded\s*[:\s]+\s*(.+?)(?:\s{2,}|$)", re.I)


def status_path() -> Path:
    return CACHE_DIR / STATUS_FILENAME


def load_apkmirror_status(path: Path | None = None) -> dict[str, Any]:
    p = path or status_path()
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return raw if isinstance(raw, dict) else {}


def save_apkmirror_status(data: dict[str, Any], path: Path | None = None) -> None:
    p = path or status_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp.replace(p)


def record_app_status(
    store: dict[str, Any],
    *,
    app_key: str,
    terminal_status: TerminalStatus,
    detail: str,
    versions_count: int,
    pages_fetched: int,
) -> None:
    store[app_key] = {
        "status": terminal_status,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "detail": detail,
        "versions_count": versions_count,
        "pages_fetched": pages_fetched,
    }
    save_apkmirror_status(store)


def _is_cloudflare_interstitial(html: str) -> bool:
    if len(html) < 200:
        return False
    head = html[:12000]
    return "cdn-cgi/challenge-platform" in head or (
        "Just a moment" in head and "challenge-error-text" in head
    )


def load_apps_json(config_path: Path) -> list[dict[str, Any]]:
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError(f"Expected JSON array in {config_path}")
    return raw


def normalize_slug(slug: str) -> str:
    return (slug or "").strip().strip("/")


def apk_mirror_listing_url(slug: str) -> str:
    """HTTPS URL for the app's APKMirror product listing (apk/developer/app/)."""
    s = normalize_slug(slug)
    if not s:
        return ""
    parts = [quote(p, safe="") for p in s.split("/") if p]
    return APKMIRROR_APK_PREFIX + "/".join(parts) + "/"


def uploads_page_url(appcategory: str, page_num: int) -> str:
    """
    Paginated uploads listing for one app category.

    Page 1: /uploads/?appcategory={slug}
    Page N: /uploads/page/{N}/?appcategory={slug}
    """
    q = urlencode({"appcategory": appcategory})
    if page_num <= 1:
        return f"{APKMIRROR_ORIGIN}/uploads/?{q}"
    return f"{APKMIRROR_ORIGIN}/uploads/page/{page_num}/?{q}"


def pipeline_android_app_id(app_name: str) -> str:
    """Matches ``stable_app_id(..., \"Android\")`` in ``run_pipeline``."""
    base = re.sub(r"[^a-z0-9]+", "_", app_name.lower()).strip("_")[:48]
    return f"{base}_android"


def iter_android_apkmirror_apps(
    config_path: Path | None = None,
) -> Iterator[dict[str, Any]]:
    path = config_path or DEFAULT_CONFIG
    for cfg in load_apps_json(path):
        pkg = (cfg.get("android_package") or "").strip()
        if not pkg:
            continue
        slug = normalize_slug(str(cfg.get("apkmirror_slug") or ""))
        if not slug:
            continue
        app_key = (cfg.get("app_key") or "").strip()
        app_name = (cfg.get("app_name") or "").strip()
        yield {
            "app_key": app_key,
            "app_name": app_name,
            "android_package": pkg,
            "apkmirror_slug": slug,
            "apk_mirror_listing_url": apk_mirror_listing_url(slug),
            "apkmirror_category": (cfg.get("apkmirror_category") or "").strip(),
        }


def extract_appcategory_from_listing_page(html: str, page_url: str) -> str:
    """Parse ``appcategory`` from a ``See more uploads`` link on the app listing page."""
    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        label = re.sub(r"\s+", " ", (a.get_text() or "").strip().lower())
        if "see more uploads" not in label:
            continue
        full = urljoin(page_url, a["href"].strip())
        qs = parse_qs(urlparse(full).query)
        vals = qs.get("appcategory") or []
        if vals and vals[0].strip():
            return vals[0].strip()
    return ""


def _should_skip_entry(version_number: str, title: str, page_url: str) -> bool:
    blob = f"{version_number} {title} {page_url}".lower()
    return bool(SKIP_TOKEN_RE.search(blob))


def _absolute_apkmirror_url(href: str | None) -> str:
    if not href:
        return ""
    href = href.strip()
    if href.startswith(("http://", "https://")):
        return href
    return urljoin(APKMIRROR_ORIGIN + "/", href.lstrip("/"))


def _normalize_release_date(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    try:
        dt = date_parser.parse(s, fuzzy=True)
        return dt.date().isoformat()
    except (ValueError, OverflowError, TypeError):
        return s


def _extract_version_and_date(app_row: Any) -> tuple[str, str]:
    ver_spans = app_row.select("span.infoSlide-value")
    version_number = ""
    if ver_spans:
        version_number = ver_spans[0].get_text(strip=True)

    uploaded = ""
    # Newer APKMirror uploads layout uses infoSlide-name/value pairs.
    if not uploaded:
        for p in app_row.select("p"):
            lab = p.select_one(".infoSlide-name")
            val = p.select_one(".infoSlide-value")
            if not lab or not val:
                continue
            if "upload" not in lab.get_text(strip=True).lower():
                continue
            uploaded = val.get_text(" ", strip=True)
            if uploaded:
                break

    for slide in app_row.select("div.metaSlide"):
        lab = slide.select_one(".metaSlide-label")
        val = slide.select_one(".metaSlide-value")
        if lab and val and "upload" in lab.get_text(strip=True).lower():
            uploaded = val.get_text(strip=True)
            break

    if not uploaded:
        m = UPLOADED_RE.search(app_row.get_text(" ", strip=True))
        if m:
            uploaded = m.group(1).strip()

    return version_number, _normalize_release_date(uploaded)


def _parse_listing_rows(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    widget = soup.select_one("#content div.listWidget") or soup.select_one("div.listWidget")
    if widget:
        candidates = widget.select("div.appRow")
    else:
        candidates = soup.select("#content div.appRow")

    out: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for app_row in candidates:
        link_el = app_row.select_one("h5 a") or app_row.select_one("a.fontBlack") or app_row.find(
            "a", href=True
        )
        if not link_el or not link_el.get("href"):
            continue

        title = link_el.get_text(strip=True)
        page_url = _absolute_apkmirror_url(link_el["href"])

        version_number, release_date = _extract_version_and_date(app_row)
        if not version_number:
            vgrep = re.search(
                r"\b(\d+(?:\.\d+){1,6})\b",
                title,
            )
            if vgrep:
                version_number = vgrep.group(1)

        if _should_skip_entry(version_number, title, page_url):
            continue

        if page_url in seen_urls:
            continue
        seen_urls.add(page_url)

        out.append(
            {
                "version_number": version_number,
                "release_date": release_date,
                "apkmirror_url": page_url,
            }
        )

    return out


def _fetch_once(session: requests.Session, url: str) -> tuple[int, str]:
    """Single GET — no retries (403/429 are terminal for rate-limit / block detection)."""
    try:
        r = session.get(url, timeout=60)
        return r.status_code, r.text
    except requests.RequestException as e:
        return 0, str(e)


def _resolve_uploads_appcategory(
    session: requests.Session,
    target: dict[str, Any],
    request_gate: dict[str, int],
) -> tuple[str | None, str | None]:
    """
    Resolve uploads ``appcategory`` slug.

    ``amazon_shopping``: skip listing fetch; use ``apkmirror_category`` so uploads
    URLs always use ``appcategory=amazon-shopping`` (listing pagination can differ).

    Others: GET listing; parse ``See more uploads``, else config fallback.
    """
    fallback = (target.get("apkmirror_category") or "").strip()

    if target.get("app_key") == "amazon_shopping":
        if fallback:
            return fallback, None
        return None, "amazon_shopping missing apkmirror_category"

    listing_url = target["apk_mirror_listing_url"]

    if request_gate["n"] > 0:
        time.sleep(3.0)
    request_gate["n"] += 1
    status, html = _fetch_once(session, listing_url)

    if status == 404 and not fallback:
        return None, "app listing HTTP 404 (no apkmirror_category fallback)"
    if status == 403 and not fallback:
        return None, "app listing HTTP 403 (no apkmirror_category fallback)"

    listing_ok = status == 200 and not _is_cloudflare_interstitial(html)
    if listing_ok:
        cat = extract_appcategory_from_listing_page(html, listing_url)
        if cat:
            return cat, None

    if fallback:
        return fallback, None

    if status != 200:
        detail = f"HTTP {status}" if status else "network error"
        return None, f"app listing {detail} (no apkmirror_category fallback)"
    if _is_cloudflare_interstitial(html):
        return None, "Cloudflare on app listing (no apkmirror_category fallback)"
    return None, "See more uploads / appcategory not found (no apkmirror_category fallback)"


def scrape_one_app(target: dict[str, Any], request_gate: dict[str, int]) -> tuple[list[dict[str, str]], TerminalStatus, str, int]:
    """
    Returns (rows, terminal_status, detail, pages_fetched).
    """
    session = requests.Session()
    session.headers.update(SCRAPER_HEADERS)

    appcategory, resolve_err = _resolve_uploads_appcategory(session, target, request_gate)
    if not appcategory:
        return [], "failed", resolve_err or "unknown resolve error", 0

    aggregated: list[dict[str, str]] = []
    seen_release_urls: set[str] = set()
    pages_fetched = 0
    terminal_status: TerminalStatus = "complete"
    detail = ""

    for page_no in range(1, MAX_UPLOAD_PAGES + 1):
        uploads_url = uploads_page_url(appcategory, page_no)

        if request_gate["n"] > 0:
            time.sleep(3.0)
        request_gate["n"] += 1
        status, html = _fetch_once(session, uploads_url)

        if status == 404:
            terminal_status = "partial"
            detail = f"HTTP 404 on uploads page {page_no} — stopping with rows collected so far"
            break

        if status == 403:
            if page_no == 1:
                terminal_status = "blocked"
                detail = "HTTP 403 on uploads page 1 (treated as blocked / Cloudflare)"
            elif aggregated:
                terminal_status = "partial"
                detail = f"HTTP 403 on uploads page {page_no}"
            else:
                terminal_status = "failed"
                detail = f"HTTP 403 on uploads page {page_no}"
            break

        if status != 200:
            msg = f"HTTP {status} on uploads page {page_no}" if status else f"network error on uploads page {page_no}"
            if not aggregated:
                terminal_status = "failed"
            else:
                terminal_status = "partial"
            detail = msg
            break

        if _is_cloudflare_interstitial(html):
            if page_no == 1:
                terminal_status = "blocked"
                detail = "Cloudflare challenge page on uploads page 1 (blocked)"
            elif aggregated:
                terminal_status = "partial"
                detail = f"Cloudflare challenge on uploads page {page_no}"
            else:
                terminal_status = "failed"
                detail = f"Cloudflare challenge on uploads page {page_no}"
            break

        pages_fetched = page_no
        rows = _parse_listing_rows(html)
        new_rows: list[dict[str, str]] = []
        for row in rows:
            u = row["apkmirror_url"]
            if u not in seen_release_urls:
                seen_release_urls.add(u)
                new_rows.append(row)

        aggregated.extend(new_rows)
        print(
            f"  [{target['app_name']}] uploads page {page_no}/{MAX_UPLOAD_PAGES}: +{len(new_rows)} entries "
            f"(total versions kept: {len(aggregated)})",
            flush=True,
        )

        if not new_rows:
            terminal_status = "complete"
            detail = "no new rows (listing exhausted)"
            break

    if terminal_status == "complete" and not detail:
        detail = f"fetched up to {pages_fetched} page(s)"

    clean = [
        {
            "version_number": r["version_number"],
            "release_date": r["release_date"],
            "apkmirror_url": r["apkmirror_url"],
        }
        for r in aggregated
    ]
    return clean, terminal_status, detail, pages_fetched


def run_scrape(targets: list[dict[str, Any]]) -> int:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    status_store = load_apkmirror_status()

    skip_status = frozenset({"complete", "blocked"})
    to_run: list[dict[str, Any]] = []
    skipped_note: list[str] = []

    for t in targets:
        key = t["app_key"]
        prev = status_store.get(key)
        if isinstance(prev, dict):
            st = prev.get("status")
            if st in skip_status:
                skipped_note.append(f"{t['app_name']} ({key}): skip — status={st}")
                continue
        to_run.append(t)

    if skipped_note:
        print("Skipping (apkmirror_status.json):", flush=True)
        for line in skipped_note:
            print(f"  - {line}", flush=True)
        print(flush=True)

    per_app_counts: dict[str, int] = {}
    failed_or_partial: list[str] = []
    hard_failed: list[str] = []
    skipped_empty: list[str] = []
    request_gate = {"n": 0}

    for idx, t in enumerate(to_run, start=1):
        app_key = t["app_key"]
        app_id = pipeline_android_app_id(t["app_name"])
        csv_path = CACHE_DIR / f"apkmirror_{app_id}.csv"
        print(f"[{idx}/{len(to_run)}] Scraping {t['app_name']} ({app_key}, app_id={app_id}) …", flush=True)

        rows, terminal_status, detail, pages_fetched = scrape_one_app(t, request_gate)

        record_app_status(
            status_store,
            app_key=app_key,
            terminal_status=terminal_status,
            detail=detail,
            versions_count=len(rows),
            pages_fetched=pages_fetched,
        )

        print(f"  → status={terminal_status}: {detail}", flush=True)

        if terminal_status == "failed" and not rows:
            print(f"  skipped / failed: {detail}", flush=True)
            msg = f"{t['app_name']} ({app_key}): {detail}"
            failed_or_partial.append(msg)
            hard_failed.append(msg)
            per_app_counts[app_key] = 0
            with csv_path.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(
                    f,
                    fieldnames=["app_id", "version_number", "release_date", "apkmirror_url"],
                )
                w.writeheader()
            continue

        if terminal_status in ("partial", "blocked") or detail:
            if terminal_status != "failed":
                failed_or_partial.append(f"{t['app_name']} ({app_key}): {terminal_status} — {detail}")

        per_app_counts[app_key] = len(rows)
        if not rows and terminal_status != "blocked":
            skipped_empty.append(f"{t['app_name']} ({app_key})")

        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["app_id", "version_number", "release_date", "apkmirror_url"],
            )
            w.writeheader()
            for r in rows:
                w.writerow(
                    {
                        "app_id": app_id,
                        "version_number": r["version_number"],
                        "release_date": r["release_date"],
                        "apkmirror_url": r["apkmirror_url"],
                    }
                )

        print(f"  wrote {len(rows)} rows → {csv_path}", flush=True)

    total = sum(per_app_counts.values())
    print("\n=== scrape summary ===", flush=True)
    print("Versions per app (this run):", flush=True)
    for t in targets:
        k = t["app_key"]
        aid = pipeline_android_app_id(t["app_name"])
        prev = status_store.get(k)
        pst = prev.get("status") if isinstance(prev, dict) else None
        cnt = per_app_counts.get(k)
        if cnt is None:
            cnt = prev.get("versions_count") if isinstance(prev, dict) else 0
        print(f"  - {t['app_name']} ({k}, {aid}): {cnt}  [last_status={pst}]", flush=True)
    print(f"Total versions (summed this run’s scraped apps): {total}", flush=True)
    if skipped_empty:
        print("Apps with zero parsed rows (after filters):", flush=True)
        for line in skipped_empty:
            print(f"  - {line}", flush=True)
    if failed_or_partial:
        print("Failures / partial / blocked notes:", flush=True)
        for line in failed_or_partial:
            print(f"  - {line}", flush=True)

    return 1 if hard_failed else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Resolve APKMirror listing URLs from config/apps.json.")
    p.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG,
        help=f"path to apps.json (default: {DEFAULT_CONFIG})",
    )
    mode = p.add_mutually_exclusive_group()
    mode.add_argument(
        "--list",
        action="store_true",
        help="print app_key, slug, and listing URL for each included Android app",
    )
    mode.add_argument(
        "--scrape",
        action="store_true",
        help="fetch uploads listings and save CSVs + apkmirror_status.json under data/cache/",
    )
    args = p.parse_args(argv)

    if not args.config.is_file():
        print(f"Missing config: {args.config}", file=sys.stderr)
        return 1

    targets = list(iter_android_apkmirror_apps(args.config))

    if args.scrape:
        return run_scrape(targets)

    if args.list:
        for t in targets:
            print(f"{t['app_key']}\t{t['apkmirror_slug']}\t{t['apk_mirror_listing_url']}")
        return 0

    print(f"{len(targets)} Android app(s) with apkmirror_slug from {args.config}")
    for t in targets:
        print(f"  - {t['app_name']} ({t['app_key']}): {t['apk_mirror_listing_url']}")
    sp = status_path()
    if sp.is_file():
        print(f"\nStatus file: {sp}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
