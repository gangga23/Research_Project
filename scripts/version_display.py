"""
Display sentinel for missing semantic ``version_number`` in exported CSV/Excel.

Pipeline internals may still use empty strings; exports normalize to ``Unknown`` so
tables look complete. Use ``version_string_missing()`` anywhere logic treats empty/Unknown
as no version string.
"""

from __future__ import annotations

import pandas as pd

MISSING_VERSION_DISPLAY = "Unknown"


def version_string_missing(x: object) -> bool:
    if x is None:
        return True
    try:
        if pd.isna(x):
            return True
    except (ValueError, TypeError):
        pass
    s = str(x).strip()
    if not s:
        return True
    return s.casefold() == MISSING_VERSION_DISPLAY.casefold()


def format_version_number_for_export(x: object) -> str:
    """String for CSV/Excel: empty / NaN -> ``Unknown``."""
    if version_string_missing(x):
        return MISSING_VERSION_DISPLAY
    return str(x).strip()


def apply_version_number_export_column(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with ``version_number`` formatted for deliverables."""
    out = df.copy()
    if "version_number" in out.columns:
        out["version_number"] = out["version_number"].map(format_version_number_for_export)
    return out
