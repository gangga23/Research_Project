"""
Re-apply Excel formatting to an existing workbook without re-running the data pipeline.

Uses openpyxl inside ``submission_summary.apply_submission_sheet_style`` to load
``output/normalized_dataset.xlsx``, style ``submission_summary``, and save.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_XLSX = ROOT / "output" / "normalized_dataset.xlsx"


def _load_submission_summary():
    sd = str(SCRIPT_DIR.resolve())
    if sd not in sys.path:
        sys.path.insert(0, sd)
    path = SCRIPT_DIR / "submission_summary.py"
    if not path.is_file():
        raise ImportError(f"Missing required file: {path}")
    spec = importlib.util.spec_from_file_location("submission_summary", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module spec for {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    if not OUTPUT_XLSX.is_file():
        print(f"Missing workbook: {OUTPUT_XLSX}", file=sys.stderr)
        print("Run scripts/run_pipeline.py first to generate it.", file=sys.stderr)
        return 1

    ss = _load_submission_summary()
    ss.apply_submission_sheet_style(OUTPUT_XLSX, ("summary",))
    print(f"Updated formatting: {OUTPUT_XLSX}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
