"""
Overlay APKMirror release URLs onto ``app_version_history.history_source_url``.

Reads ``data/cache/apkmirror_{app_id}.csv`` (from ``apkmirror_scraper.py``). When an
Android observation matches by ``app_id`` + ``version_number`` (or by
``release_date`` if version is empty), ``history_source_url`` is set to the
APKMirror permalink so Excel/export keeps a clickable third-party source across
full rescrapes and ``build_workbook_only`` rebuilds.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

try:
    from dateutil import parser as date_parser
except ImportError:
    date_parser = None  # type: ignore[misc, assignment]


def _norm_ver(v: object) -> str:
    s = (str(v) if v is not None else "").strip()
    if s.casefold() == "unknown":
        return ""
    return s


def _norm_date_iso(d: object) -> str:
    s = (str(d) if d is not None else "").strip()
    if not s:
        return ""
    if date_parser is None:
        return s[:10] if len(s) >= 10 and s[4] == "-" else s
    try:
        return date_parser.parse(s, fuzzy=True).date().isoformat()
    except (ValueError, OverflowError, TypeError):
        return s[:10] if len(s) >= 10 and s[4] == "-" else s


def _load_apkmirror_lookups(cache_dir: Path) -> tuple[dict[tuple[str, str], str], dict[tuple[str, str], str]]:
    """Build (by_app_version, by_app_date) -> apkmirror_url."""
    by_ver: dict[tuple[str, str], str] = {}
    by_date: dict[tuple[str, str], str] = {}
    if not cache_dir.is_dir():
        return by_ver, by_date

    for path in sorted(cache_dir.glob("apkmirror_*.csv")):
        try:
            df = pd.read_csv(path, dtype=str, keep_default_na=False)
        except (OSError, pd.errors.ParserError, UnicodeDecodeError):
            continue
        need = {"app_id", "apkmirror_url"}
        if not need.issubset(df.columns):
            continue
        for _, row in df.iterrows():
            aid = _norm_ver(row.get("app_id"))
            url = _norm_ver(row.get("apkmirror_url"))
            if not aid or not url.startswith(("http://", "https://")):
                continue
            ver = _norm_ver(row.get("version_number"))
            rdate = _norm_date_iso(row.get("release_date"))
            if ver:
                by_ver[(aid, ver)] = url
            if rdate:
                by_date.setdefault((aid, rdate), url)

    return by_ver, by_date


def merge_apkmirror_history_urls(version_df: pd.DataFrame, cache_dir: Path) -> pd.DataFrame:
    """
    For Android rows, replace ``history_source_url`` with APKMirror URL when a cache
    row matches (version preferred, else release date).
    """
    if version_df.empty or "platform" not in version_df.columns:
        return version_df
    if "history_source_url" not in version_df.columns:
        return version_df

    by_ver, by_date = _load_apkmirror_lookups(cache_dir)
    if not by_ver and not by_date:
        return version_df

    out = version_df.copy()
    mask = out["platform"].astype(str).str.strip().eq("Android")
    if not mask.any():
        return out

    for idx in out.loc[mask].index:
        aid = _norm_ver(out.at[idx, "app_id"])
        ver = _norm_ver(out.at[idx, "version_number"])
        rdt = _norm_date_iso(out.at[idx, "release_date"])
        url = ""
        if ver and (aid, ver) in by_ver:
            url = by_ver[(aid, ver)]
        elif rdt and (aid, rdt) in by_date:
            url = by_date[(aid, rdt)]
        if url:
            out.at[idx, "history_source_url"] = url

    return out
