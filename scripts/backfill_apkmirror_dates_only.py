"""
Backfill Android APKMirror release_date values into cached CSVs (no scraping).

Goal: make dated-only charts/narratives reproducible for submission by writing the
backfilled dates directly into ``output/app_version_history.csv`` and persisting the
URL->date cache under ``data/cache/apkmirror_upload_dates.json``.

Usage (PowerShell examples):
  # Budgeted run (recommended; resumable)
  $env:APKMIRROR_UPLOAD_FETCH_MAX="200"
  python scripts/backfill_apkmirror_dates_only.py

  # Repeat until unique_urls_missing_from_cache reaches ~0, then freeze:
  $env:APKMIRROR_UPLOAD_FETCH_MAX="0"
  python scripts/build_workbook_only.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "output"


def _blank_date_mask(s: pd.Series) -> pd.Series:
    rd = s.fillna("").astype(str).str.strip()
    return rd.eq("") | rd.str.lower().isin(["nan", "nat", "none"])


def main() -> int:
    try:
        from apkmirror_upload_date import fill_missing_apk_mirror_release_dates
    except ImportError as e:
        print(f"Missing apkmirror_upload_date dependency: {e}", file=sys.stderr)
        return 2

    vcsv = OUTPUT_DIR / "app_version_history.csv"
    if not vcsv.is_file():
        print(f"Missing {vcsv}. Run pipeline once to generate cached CSVs.", file=sys.stderr)
        return 1

    df = pd.read_csv(vcsv)
    if df.empty:
        print("app_version_history.csv is empty; nothing to backfill.")
        return 0

    need = {"platform", "source_type", "release_date", "history_source_url"}
    if not need.issubset(set(df.columns)):
        print(f"app_version_history.csv missing required columns: {sorted(need - set(df.columns))}", file=sys.stderr)
        return 1

    ad = df[df["platform"].astype(str).eq("Android")]
    apk = ad[ad["source_type"].astype(str).eq("apkmirror_cache")]
    blank_before = int(_blank_date_mask(apk["release_date"]).sum()) if len(apk) else 0

    out = fill_missing_apk_mirror_release_dates(df, verbose=True)

    ad2 = out[out["platform"].astype(str).eq("Android")]
    apk2 = ad2[ad2["source_type"].astype(str).eq("apkmirror_cache")]
    blank_after = int(_blank_date_mask(apk2["release_date"]).sum()) if len(apk2) else 0

    if blank_after < blank_before:
        out.to_csv(vcsv, index=False, encoding="utf-8")
        print(f"Wrote updated CSV: {vcsv}")

    print(f"APKMirror cache rows (Android): {len(apk2)}")
    print(f"Blank release_date before: {blank_before}")
    print(f"Blank release_date after:  {blank_after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

