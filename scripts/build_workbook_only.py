"""
Rebuild Excel + derived CSV/text outputs from cached ``app_master.csv`` and
``app_version_history.csv`` — **no** Play / Apple / Wayback scraping.

Use after a full ``run_pipeline.py`` run when you only changed summary logic
or formatting — finishes in seconds.

Requires: ``output/app_master.csv`` and ``output/app_version_history.csv``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "output"
CONFIG_PATH = ROOT / "config" / "apps.json"


def main() -> int:
    from export_workbook_bundle import export_workbook_bundle, infer_both_platform_app_count

    mcsv = OUTPUT_DIR / "app_master.csv"
    vcsv = OUTPUT_DIR / "app_version_history.csv"
    if not mcsv.is_file() or not vcsv.is_file():
        print(
            f"Missing {mcsv.name} and/or {vcsv.name}. Run scripts/run_pipeline.py once to fetch data.",
            file=sys.stderr,
        )
        return 1

    apps = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    master_df = pd.read_csv(mcsv)
    version_df = pd.read_csv(vcsv)
    if "notes" not in master_df.columns:
        master_df = master_df.copy()
        master_df["notes"] = ""
    both_platforms = infer_both_platform_app_count(master_df)

    # Prefer the canonical filename; if Windows temporarily locks it, fall back to a new name.
    out_name = "normalized_dataset.xlsx"
    try:
        rep = export_workbook_bundle(
            master_df,
            version_df,
            n_config_apps=len(apps),
            both_platforms=both_platforms,
            feed_validations=[],
            output_dir=OUTPUT_DIR,
            repo_root=ROOT,
            script_dir=SCRIPT_DIR,
            rewrite_master_version_csv=False,
            rewrite_feed_validation_report=False,
            xlsx_name=out_name,
        )
    except PermissionError:
        out_name = "normalized_dataset_rebuilt.xlsx"
        rep = export_workbook_bundle(
            master_df,
            version_df,
            n_config_apps=len(apps),
            both_platforms=both_platforms,
            feed_validations=[],
            output_dir=OUTPUT_DIR,
            repo_root=ROOT,
            script_dir=SCRIPT_DIR,
            rewrite_master_version_csv=False,
            rewrite_feed_validation_report=False,
            xlsx_name=out_name,
        )

    print(f"Rebuilt deliverables from CSV cache (no scrape): {OUTPUT_DIR / out_name}")
    locked = (OUTPUT_DIR / out_name).with_name(
        f"{Path(out_name).stem}_locked{Path(out_name).suffix}"
    )
    if locked.is_file():
        print(f"Read-only/Final copy (lightweight): {locked}")
    print(rep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
