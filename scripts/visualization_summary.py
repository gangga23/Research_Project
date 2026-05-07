"""
Fast-scan visuals aligned with ``timeseries_insights_core`` (same facts as
``submission_summary.build_timeseries_insights``):

This sheet is intentionally small and rubric-aligned:
- update frequency over time by app (cadence heatmaps — iOS / Android)
- iOS vs Android observation depth per app (dated span)
- category share shift (oldest vs newest quartile; slope chart)

Uses Matplotlib (Agg) + openpyxl images; no network.
"""

from __future__ import annotations

import functools
import io
import math
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd

from timeseries_insights_core import (
    build_automated_trend_synopsis_bullets,
    dated_subset,
    parse_release_dates,
)

_SHEET = "viz_fast_scan"

# Cadence heatmaps (iOS/Android): cap color normalization so 0–20 uses full colormap;
# counts above the cap share the top color (colorbar ``extend='max'``).
CADENCE_HEATMAP_VMAX_CAP = 20.0


@functools.lru_cache(maxsize=1)
def _cadence_heatmap_colormap():
    """White at zero, then yellow → blue (no green band). Matplotlib only when charts run."""
    from matplotlib.colors import LinearSegmentedColormap

    return LinearSegmentedColormap.from_list(
        "cadence_yb",
        [
            "#ffffff",
            "#fffef7",
            "#fff9c4",
            "#ffeb3b",
            "#c5e1f5",
            "#64b5f6",
            "#1976d2",
            "#0d47a1",
        ],
        N=256,
    )


def build_automated_trend_synopsis(version_df: pd.DataFrame) -> list[str]:
    """Bullets from shared core (coverage → cadence → quartiles → strategy read → epistemic)."""
    # Keep the viz sheet synopsis short/scannable.
    return build_automated_trend_synopsis_bullets(version_df)[:6]


def _save_png_bytes(buf: io.BytesIO, out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(buf.getvalue())
    return out_path


def _short_quarter_tick_label(col: str) -> str:
    """Human-readable quarter ticks from pandas Period strings like ``2024Q1``."""
    s = str(col).strip()
    m = re.match(r"^(\d{4})Q([1-4])$", s)
    if m:
        return f"Q{m.group(2)} '{m.group(1)[2:]}"
    return s


def _compact_month_tick_label(ym: str) -> str:
    """Turn ``YYYY-MM`` period strings into short ticks like ``May '23``."""
    s = str(ym).strip()
    try:
        ts = pd.Timestamp(s)
        return f"{ts.strftime('%b')} '{ts.strftime('%y')}"
    except (ValueError, TypeError):
        return s


def _heatmap_xtick_positions(n: int, *, target_labels: int = 13) -> list[int]:
    """Sparse tick indices so month axes stay readable when many columns are drawn."""
    if n <= 0:
        return []
    if n <= target_labels:
        return list(range(n))
    stride = max(1, math.ceil(n / target_labels))
    out = list(range(0, n, stride))
    if out[-1] != n - 1:
        out.append(n - 1)
    return sorted(set(out))


def _cadence_heatmap_prepare(
    version_df: pd.DataFrame,
    *,
    platform: str,
    app_order: list[str],
    max_bins: int | None,
    bin_period: str,
    since: pd.Timestamp | None = None,
    fill_period_bins_since: pd.Timestamp | None = None,
    fill_end: pd.Timestamp | None = None,
) -> tuple[Any, list[str], list[str]] | None:
    """
    Build cadence matrix for ``imshow``: rows = apps (shared order), cols = time bins.
    Returns ``(mat, x_tick_labels, row_labels)`` or ``None``.

    ``since`` drops dated rows before that timestamp (platform-specific windows).

    ``fill_period_bins_since`` reindexes to every calendar bin from that anchor through
    the latest dated row (zeros where nothing was captured). Supports ``bin_period``
    ``M`` or ``Q`` (and ``Y``).

    When ``fill_end`` is set with ``fill_period_bins_since``, the reindex uses that end
    (e.g. global max date) so iOS/Android monthly matrices share the same column range.
    """
    sub = dated_subset(version_df)
    if sub is None:
        return None
    sub = sub[sub["platform"].astype(str) == platform]
    if len(sub) < 1:
        return None

    sub = sub.copy()
    if since is not None:
        sub = sub[sub["_dt"] >= since]
    if len(sub) < 1:
        return None

    sub["_bin"] = sub["_dt"].dt.to_period(bin_period).astype(str)
    piv = sub.pivot_table(index="app_name", columns="_bin", values="app_id", aggfunc="count", fill_value=0)
    if piv.empty:
        return None

    try:
        ordered_cols = sorted(piv.columns, key=lambda c: pd.Period(str(c), freq=bin_period))
    except (ValueError, TypeError):
        ordered_cols = list(piv.columns)
    piv = piv.reindex(columns=ordered_cols)

    if fill_period_bins_since is not None:
        anchor = pd.Timestamp(fill_period_bins_since)
        end_dt = sub["_dt"].max()
        if pd.isna(end_dt):
            return None
        end_ts = pd.Timestamp(fill_end) if fill_end is not None else pd.Timestamp(end_dt)
        freq_map = {"M": "M", "Q": "Q", "Y": "Y"}
        freq = freq_map.get(bin_period)
        if freq is None:
            pass
        else:
            start_p = anchor.to_period(freq)
            end_p = end_ts.to_period(freq)
            if end_p >= start_p:
                full_cols = pd.period_range(start_p, end_p, freq=freq).astype(str)
                piv = piv.reindex(columns=list(full_cols), fill_value=0)

    cols = list(piv.columns)
    if max_bins is not None and len(cols) > max_bins:
        piv = piv[cols[-max_bins:]]

    piv = piv.reindex(app_order).fillna(0)

    apps = list(piv.index.astype(str))
    raw_cols = list(piv.columns.astype(str))
    if bin_period == "Q":
        xlabels = [_short_quarter_tick_label(c) for c in raw_cols]
    elif bin_period == "M":
        xlabels = [_compact_month_tick_label(c) for c in raw_cols]
    else:
        xlabels = [str(c) for c in raw_cols]
    mat = piv.values.astype(float)
    return mat, xlabels, apps


def _chart_update_frequency_heatmap_platform(
    version_df: pd.DataFrame,
    *,
    platform: str,
    app_order: list[str],
    max_bins: int | None,
    bin_period: str,
    title: str,
    subtitle: str,
    vmax: float | None = None,
    prepared: tuple[Any, list[str], list[str]] | None = None,
    since: pd.Timestamp | None = None,
    fill_period_bins_since: pd.Timestamp | None = None,
    fill_end: pd.Timestamp | None = None,
    cmap=None,
) -> io.BytesIO | None:
    """
    Cadence heatmap for a single platform (dated rows).
    Rows: app_name; columns: YYYY-MM or YYYYQn; values: observation counts.
    Use the same ``vmax`` and default colormap for iOS and Android so numeric and hue scaling match.
    ``vmax`` is normally capped (see ``CADENCE_HEATMAP_VMAX_CAP``) so low/mid counts show more hue spread.
    """
    import matplotlib.pyplot as plt
    import numpy as np

    block = prepared
    if block is None:
        block = _cadence_heatmap_prepare(
            version_df,
            platform=platform,
            app_order=app_order,
            max_bins=max_bins,
            bin_period=bin_period,
            since=since,
            fill_period_bins_since=fill_period_bins_since,
            fill_end=fill_end,
        )
    if block is None:
        return None
    mat, xlabels, apps = block

    if cmap is None:
        cmap = _cadence_heatmap_colormap()

    # Larger fonts for heatmap readability in Excel.
    base_fs = 13
    fs_plot_title = int(base_fs * 1.72)
    fs_sub = int(base_fs * 1.12)
    fs_axis = int(base_fs * 1.15)
    fs_tick_y = int(base_fs * 1.08)
    fs_tick_x = int(base_fs * 1.02)
    fs_cbar = int(base_fs * 1.18)

    # Fixed requested geometry so heatmaps match across exports.
    fig, ax = plt.subplots(figsize=(16, 7))
    fig.patch.set_facecolor("#ffffff")
    ax.set_facecolor("#ffffff")

    vm = float(vmax) if vmax is not None else min(
        CADENCE_HEATMAP_VMAX_CAP,
        max(1.0, float(np.ceil(float(mat.max())))),
    )
    im = ax.imshow(mat, aspect="auto", cmap=cmap, vmin=0, vmax=vm)
    cbar_extend = "max" if float(mat.max()) > vm + 1e-9 else "neither"
    # Keep titles aligned so tight-cropping doesn't produce different PNG geometry per platform.
    ax.set_title(title, fontsize=fs_plot_title, pad=14)
    ax.text(
        0.0,
        1.01,
        subtitle,
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=fs_sub,
        color="#444444",
    )

    ax.set_xlabel("Quarter" if bin_period == "Q" else "Year" if bin_period == "Y" else "Month", fontsize=fs_axis)
    ax.set_ylabel("App", fontsize=fs_axis)
    ax.set_yticks(np.arange(len(apps)))
    ax.set_yticklabels(apps, fontsize=fs_tick_y)
    nxb = len(xlabels)
    xi = _heatmap_xtick_positions(nxb) if nxb else []
    tick_labs = [xlabels[i] for i in xi]
    ax.set_xticks(xi)
    ax.set_xticklabels(tick_labs, rotation=45, ha="right", fontsize=fs_tick_x)
    cbar = fig.colorbar(im, ax=ax, fraction=0.03, pad=0.02, extend=cbar_extend)
    cbar.set_label("Observation count (per bin)", fontsize=fs_cbar)
    nticks = min(5, max(2, int(math.ceil(vm)) + 1))
    cbar.set_ticks(np.linspace(0.0, float(vm), num=nticks))
    cbar.ax.tick_params(labelsize=int(fs_cbar * 0.9))
    # Fixed margins + no bbox_inches='tight' => consistent PNG geometry for iOS/Android.
    fig.subplots_adjust(left=0.24, right=0.985, top=0.88, bottom=0.18)
    buf = io.BytesIO()
    fig.savefig(
        buf,
        format="png",
        dpi=150,
        bbox_inches=None,
        facecolor="#ffffff",
        edgecolor="none",
        pad_inches=0.0,
    )
    plt.close(fig)
    buf.seek(0)
    return buf


def _chart_update_frequency_heatmap(version_df: pd.DataFrame) -> io.BytesIO | None:
    """
    (1) Update frequency over time by app: monthly heatmap (dated rows).
    Rows: app_name (pooled across platforms); Columns: YYYY-MM; Values: count of observations.
    """
    import matplotlib.pyplot as plt
    import numpy as np

    sub = dated_subset(version_df)
    if sub is None:
        return None
    sub = sub.copy()
    sub["_ym"] = sub["_dt"].dt.to_period("M").astype(str)
    # Pool across platforms; goal is cadence by app overall.
    piv = sub.pivot_table(index="app_name", columns="_ym", values="app_id", aggfunc="count", fill_value=0)
    if piv.empty:
        return None

    # Keep the chart readable: show at most last 30 months if very wide.
    cols = list(piv.columns)
    if len(cols) > 30:
        piv = piv[cols[-30:]]

    apps = list(piv.index.astype(str))
    xlabels = list(piv.columns.astype(str))
    mat = piv.values.astype(float)

    # Scale figure height to number of apps (cap).
    h = min(0.32 * len(apps) + 2.4, 10.5)
    w = min(0.35 * len(xlabels) + 4.8, 12.5)
    fig, ax = plt.subplots(figsize=(w, h))
    fig.patch.set_facecolor("#ffffff")
    ax.set_facecolor("#ffffff")
    vm = min(CADENCE_HEATMAP_VMAX_CAP, max(1.0, float(np.ceil(float(mat.max())))))
    im = ax.imshow(mat, aspect="auto", cmap=_cadence_heatmap_colormap(), vmin=0, vmax=vm)
    ax.set_title("1. Update frequency over time by app (monthly heatmap; dated rows)")
    ax.set_xlabel("Month")
    ax.set_ylabel("App")
    ax.set_yticks(np.arange(len(apps)))
    ax.set_yticklabels(apps, fontsize=8)
    ax.set_xticks(np.arange(len(xlabels)))
    ax.set_xticklabels(xlabels, rotation=45, ha="right", fontsize=7)
    cbar = fig.colorbar(
        im,
        ax=ax,
        fraction=0.03,
        pad=0.02,
        extend="max" if float(mat.max()) > vm + 1e-9 else "neither",
    )
    cbar.set_label("Observation count (per bin; cap 20)", fontsize=8)
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(
        buf,
        format="png",
        dpi=150,
        bbox_inches="tight",
        facecolor="#ffffff",
        edgecolor="none",
        pad_inches=0.12,
    )
    plt.close(fig)
    buf.seek(0)
    return buf

def _chart_category_evolution_quartile_buckets(version_df: pd.DataFrame) -> io.BytesIO | None:
    """
    Category share shift over time (monthly, 2025–2026) for top ``update_category`` labels.

    Replaces the old 2-point quartile slope chart so volatility / intermediate fluctuations
    are visible rather than implied by straight endpoints.
    """
    import matplotlib.pyplot as plt
    import matplotlib as mpl
    import numpy as np

    import run_pipeline as rp

    sub = dated_subset(version_df)
    if sub is None or len(sub) < 8:
        return None
    # Focus window: Jul 2025 — May 2026 (no future months).
    start = pd.Timestamp("2025-07-01")
    end = pd.Timestamp("2026-05-31")
    s = sub[(sub["_dt"] >= start) & (sub["_dt"] <= end)].copy()
    if len(s) < 6:
        return None

    cat_allowed = set(rp.UPDATE_CATEGORIES)

    def _norm_cat(x: object) -> str:
        c = str(x).strip()
        return c if c in cat_allowed else "Other"

    s["_bk"] = s["update_category"].map(_norm_cat)
    s["_ym"] = s["_dt"].dt.to_period("M").astype(str)

    # Pick top-K categories within this window by overall share.
    K = 6
    overall = s["_bk"].value_counts(normalize=True)
    keys = [k for k in overall.index.tolist() if k in cat_allowed][:K]
    if not keys:
        return None

    # Monthly counts (fill missing months with 0 so volatility is visible).
    months = pd.period_range(start.to_period("M"), end.to_period("M"), freq="M").astype(str).tolist()
    counts = (
        s.pivot_table(index="_ym", columns="_bk", values="app_id", aggfunc="count", fill_value=0)
        .reindex(months, fill_value=0)
    )

    # Sparse-month gating: only plot months with ≥ 5 total observations.
    SPARSE_MIN = 5
    totals_per_month = counts.sum(axis=1)
    valid_mask = (totals_per_month >= SPARSE_MIN).values

    denom = totals_per_month.replace(0, np.nan)
    shares = (100.0 * counts.div(denom, axis=0)).fillna(0.0)
    shares = shares.reindex(columns=keys, fill_value=0.0)

    def _legend_label(full: str) -> str:
        t = full.replace(" / ", "/")
        return t if len(t) <= 44 else t[:41] + "…"

    short = [_legend_label(k) for k in keys]
    BUGFIX_KEY = "Bug fixes / performance improvements"
    BUGFIX_COLOR = "#0d3b66"  # darker blue
    tab = mpl.colormaps["tab10"]
    palette: list = []
    for i, k in enumerate(keys):
        palette.append(BUGFIX_COLOR if k == BUGFIX_KEY else tab(i % 10))

    # Smooth shares with a 3-month moving average to reduce spike noise.
    shares_sm = shares.copy()
    for k in keys:
        shares_sm[k] = shares_sm[k].astype(float).rolling(window=3, min_periods=1).mean()

    xs = np.arange(len(months))
    masked_xs = np.where(valid_mask, xs, np.nan)

    # Faceted small-multiples: one mini chart per update_category (more readable than a crowded legend).
    nkeys = len(keys)
    # Keep a 3×2 dashboard-friendly facet grid, but make it taller so panels are readable.
    ncols = 3 if nkeys >= 4 else nkeys
    nrows = int(math.ceil(nkeys / ncols))
    fig_w = 12.8 if ncols == 3 else 8.6
    fig_h = 3.6 + 3.3 * nrows
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(fig_w, fig_h), sharex=True, sharey=True)
    fig.patch.set_facecolor("#ffffff")
    axes_arr = np.array(axes).reshape(-1)
    fs_axis = 10

    # Y zoom based on plotted (valid) months only.
    plotted_vals = shares_sm[keys].values[valid_mask]
    if plotted_vals.size:
        vmin = float(np.nanmin(plotted_vals))
        vmax = float(np.nanmax(plotted_vals))
    else:
        vmin, vmax = 0.0, 1.0
    span = max(1e-6, vmax - vmin)
    pad = max(2.0, 0.12 * span)
    y_lo = max(-0.5, vmin - pad)
    y_hi = min(100.0, vmax + pad)

    # X ticks: quarterly labels for readability.
    tick_idx = [i for i, m in enumerate(months) if m.endswith(("-01", "-04", "-07", "-10"))]
    if not tick_idx:
        tick_idx = list(range(0, len(months), 3))
    tick_lbl = [_compact_month_tick_label(months[i]) for i in tick_idx]

    # Policy / event markers (reference only; helps interpret visible shifts).
    month_to_idx = {m: i for i, m in enumerate(months)}
    refs = [
        ("2025-09", "Policy / iOS release"),
        ("2025-11", "Holiday season"),
        ("2026-01", "TikTok deadline"),
    ]
    shade_windows = [
        ("2026-01", "2026-04", "Jan–Apr 2026 window"),
    ]

    for ax_i, (k, color) in enumerate(zip(keys, palette)):
        ax = axes_arr[ax_i]
        ax.set_facecolor("#ffffff")
        ys_full = shares_sm[k].astype(float).values
        ys_masked = np.where(valid_mask, ys_full, np.nan)
        ax.plot(
            masked_xs,
            ys_masked,
            marker="o" if k == BUGFIX_KEY else None,
            markersize=3.4 if k == BUGFIX_KEY else 0,
            linewidth=2.0 if k == BUGFIX_KEY else 1.4,
            color=color,
            solid_capstyle="round",
            zorder=3,
        )
        ax.set_title(_legend_label(k), fontsize=10, fontweight="600", color="#111827", pad=6)
        ax.set_ylim(y_lo, y_hi)
        ax.set_xlim(-0.5, len(months) - 0.5)
        ax.grid(axis="y", color="#eef2f7", linestyle="-", linewidth=0.9, zorder=0)
        ax.grid(axis="x", color="#f6f8fb", linestyle="-", linewidth=0.7, zorder=0)
        for ym, _lbl in refs:
            if ym in month_to_idx:
                ax.axvline(
                    month_to_idx[ym],
                    color="#c0392b",
                    linestyle="--",
                    linewidth=1.0,
                    alpha=0.35,
                    zorder=1,
                )
        for a_ym, b_ym, _lbl in shade_windows:
            if a_ym in month_to_idx and b_ym in month_to_idx:
                a = month_to_idx[a_ym] - 0.5
                b = month_to_idx[b_ym] + 0.5
                ax.axvspan(a, b, color="#fff7ed", alpha=0.35, zorder=0)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        ax.spines["left"].set_color("#c7d0df")
        ax.spines["bottom"].set_color("#c7d0df")
        ax.tick_params(axis="both", colors="#4b5563", labelsize=fs_axis - 1, length=3, width=0.8)

    # Hide unused axes.
    for j in range(nkeys, len(axes_arr)):
        axes_arr[j].axis("off")

    # Only label x ticks on bottom row.
    for ax in axes_arr[max(0, nkeys - ncols) : nkeys]:
        ax.set_xticks(tick_idx)
        ax.set_xticklabels(tick_lbl, rotation=0, ha="center", fontsize=fs_axis - 1, color="#374151")

    fig.text(
        0.10,
        0.97,
        "Category share over time (monthly; 3‑month moving average) — Jan 2025 to May 2026",
        fontsize=14,
        fontweight="600",
        color="#1f2937",
        va="top",
        ha="left",
    )
    fig.text(
        0.10,
        0.915,
        "Small multiples: each panel is one update_category; excludes months with <5 observations. "
        "Dashed lines / shading are reference-only event windows.",
        fontsize=11,
        color="#5c6575",
        va="top",
        ha="left",
    )
    fig.text(
        0.10,
        0.885,
        "Date markers: 2025-09 ≈ policy/iOS-release window; 2025-11 ≈ holiday season; 2026-01 ≈ TikTok deadline. "
        "Shading highlights Jan–Apr 2026.",
        fontsize=10,
        color="#5c6575",
        va="top",
        ha="left",
    )
    fig.text(
        0.10,
        0.03,
        "Interpret platform differences cautiously (Android dated coverage is thinner).",
        fontsize=9,
        color="#6b7280",
        va="bottom",
        ha="left",
    )

    fig.subplots_adjust(left=0.07, right=0.985, top=0.86, bottom=0.10, wspace=0.22, hspace=0.45)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="#ffffff", edgecolor="none", pad_inches=0.18)
    plt.close(fig)
    buf.seek(0)
    return buf


def _classify_history_source_url(url: object) -> str:
    """Bucket ``history_source_url`` host for visualization (matches rubric single URL column)."""
    s = str(url or "").strip().lower()
    if not s:
        return "Other"
    if "web.archive.org" in s:
        return "Wayback"
    if "apkmirror.com" in s:
        return "APKMirror"
    if any(h in s for h in ("play.google.com", "apps.apple.com", "itunes.apple.com")):
        return "Store listing"
    return "Other"


def _chart_history_url_class_by_platform(version_df: pd.DataFrame) -> io.BytesIO | None:
    """Stacked 100% horizontal bars: URL host class within each platform (all observation rows)."""
    import matplotlib.pyplot as plt
    import numpy as np

    if "history_source_url" not in version_df.columns or len(version_df) < 1:
        return None

    df = version_df.copy()
    df["_ucls"] = df["history_source_url"].map(_classify_history_source_url)
    classes = ("Wayback", "APKMirror", "Store listing", "Other")
    colors = {
        "Wayback": "#5c6bc0",
        "APKMirror": "#ef6c00",
        "Store listing": "#43a047",
        "Other": "#9e9e9e",
    }

    platforms: list[str] = []
    mat: list[list[float]] = []
    for plat in ("iOS", "Android"):
        p = df[df["platform"].astype(str) == plat]
        if len(p) == 0:
            continue
        platforms.append(plat)
        n = len(p)
        mat.append([100.0 * float((p["_ucls"] == c).sum()) / float(n) for c in classes])

    if not platforms:
        return None

    mat_arr = np.array(mat, dtype=float)
    fig_h = max(2.95, 0.52 * len(platforms) + 2.05)
    fig, ax = plt.subplots(figsize=(8.0, fig_h))
    fig.patch.set_facecolor("#ffffff")
    ax.set_facecolor("#ffffff")

    y = np.arange(len(platforms))
    left = np.zeros(len(platforms))
    bar_h = 0.52
    for j, cls in enumerate(classes):
        vals = mat_arr[:, j]
        ax.barh(
            y,
            vals,
            height=bar_h,
            left=left,
            label=cls,
            color=colors[cls],
            edgecolor="#ffffff",
            linewidth=0.85,
        )
        left = left + vals

    ax.set_yticks(y)
    ax.set_yticklabels(platforms, fontsize=11, fontweight="600")
    ax.set_xlabel("Share of observations within platform (%)", fontsize=11)
    ax.set_xlim(0, 100)
    ax.set_title(
        "1C. Observation URL profile by platform (history_source_url)",
        fontsize=13,
        fontweight="600",
    )
    ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.14),
        ncol=4,
        fontsize=9,
        frameon=True,
        fancybox=True,
        edgecolor="#cdd6e4",
    )
    ax.tick_params(axis="x", labelsize=10)
    fig.subplots_adjust(left=0.14, right=0.98, top=0.88, bottom=0.22)
    buf = io.BytesIO()
    fig.savefig(
        buf,
        format="png",
        dpi=150,
        bbox_inches=None,
        facecolor="#ffffff",
        edgecolor="none",
        pad_inches=0.08,
    )
    plt.close(fig)
    buf.seek(0)
    return buf


def _chart_observation_depth_by_app_platform(version_df: pd.DataFrame) -> io.BytesIO | None:
    """
    (3) iOS vs Android observation depth: dated span in days per app per platform.
    """
    import matplotlib.pyplot as plt
    import numpy as np

    sub = dated_subset(version_df)
    if sub is None or len(sub) < 2:
        return None
    g = (
        sub.groupby(["app_name", "platform"])["_dt"]
        .agg(["min", "max", "count"])
        .reset_index()
        .rename(columns={"min": "dmin", "max": "dmax", "count": "n"})
    )
    if g.empty:
        return None
    g["span_days"] = (g["dmax"] - g["dmin"]).dt.days.astype(int)
    piv = g.pivot(index="app_name", columns="platform", values="span_days").fillna(0)
    for col in ("iOS", "Android"):
        if col not in piv.columns:
            piv[col] = 0
    piv = piv[["iOS", "Android"]]

    apps = list(piv.index.astype(str))
    ios_vals = piv["iOS"].astype(float).values
    and_vals = piv["Android"].astype(float).values

    y = np.arange(len(apps))
    n_apps = len(apps)
    bar_h = 0.34
    # Cap height so ~10 apps don’t get an oversized vertical canvas vs bar thickness.
    fig, ax = plt.subplots(figsize=(14, 6))
    fig.patch.set_facecolor("#ffffff")
    ax.set_facecolor("#ffffff")
    ax.barh(y - bar_h / 2, ios_vals, height=bar_h, label="iOS", color="#5b9bd5")
    ax.barh(y + bar_h / 2, and_vals, height=bar_h, label="Android", color="#ed7d31")
    ax.set_yticks(y)
    ax.set_yticklabels(apps, fontsize=9.0, fontweight="bold")
    ax.set_ylim(-0.62, max(n_apps - 1, 0) + 0.62)
    ax.set_xlabel("Dated span (days) = max(release_date) − min(release_date)")
    ax.set_title("iOS vs Android observation depth by app (dated span)", fontsize=14)
    ax.axvline(730, color="#666666", linestyle="--", linewidth=1.2)
    ymin, ymax = ax.get_ylim()
    ax.text(730, ymin + max(0.22, 0.04 * (ymax - ymin)), "2 years", ha="center", va="bottom", fontsize=10, color="#444444")
    ax.legend(loc="lower right", fontsize=9)
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(
        buf,
        format="png",
        dpi=150,
        bbox_inches="tight",
        facecolor="#ffffff",
        edgecolor="none",
        pad_inches=0.12,
    )
    plt.close(fig)
    buf.seek(0)
    return buf


def build_explanatory_questions(version_df: pd.DataFrame) -> list[str]:
    """Mix static prompts and simple data-triggered prompts."""
    static = [
        "Which apps show accelerating vs decelerating update cadence (heatmap), and does that coincide with major platform policy events?",
        "Is Android depth shallow for specific apps (span chart) — suggesting missing captures rather than true inactivity?",
        "In the quartile category chart, do headline bucket shifts reflect disclosure strategy (templated copy) or real content change? Validate using high-confidence provenance subsets.",
    ]
    dynamic: list[str] = []
    n = len(version_df)
    if n == 0:
        return static[:3]

    ios_share = len(version_df[version_df["platform"] == "iOS"]) / n
    if ios_share > 0.72:
        dynamic.append(
            "iOS rows outweigh Android—beyond disclosure asymmetry, could scraping windows explain residual imbalance?"
        )
    elif ios_share < 0.45:
        dynamic.append(
            "Android share is high versus typical embed-vs-Play asymmetry—audit source_type per observation."
        )

    missing_ver = version_df["version_number"].fillna("").astype(str).str.strip().eq("").mean()
    if missing_ver > 0.45:
        dynamic.append(
            "Sparse version_number weakens semver cadence reads—rely on dated proxies and provenance filters."
        )

    sub = dated_subset(version_df)
    if sub is not None and len(sub) >= 5:
        yrs = sub["_dt"].dt.year.astype(int)
        if int(yrs.max()) == int(yrs.min()):
            dynamic.append(
                "Single-calendar-year dated span limits multi-year trend claims—expand historical captures if needed."
            )

    low_share = (version_df["confidence_level"].astype(str).str.lower() == "low").mean()
    if low_share > 0.35:
        dynamic.append(
            "Many low-confidence rows—define an analysis subset excluding feature_signal / review_inferred where "
            "appropriate."
        )

    out = static + dynamic
    seen: set[str] = set()
    uniq: list[str] = []
    for q in out:
        if q not in seen:
            seen.add(q)
            uniq.append(q)
    return uniq[:14]


def append_visualization_sheet(xlsx_path: Path, version_df: pd.DataFrame) -> None:
    """Insert / replace ``viz_fast_scan`` worksheet."""
    try:
        import matplotlib

        matplotlib.use("Agg")
        from openpyxl.drawing.image import Image as XLImage
        from openpyxl import load_workbook
        from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    except ImportError as e:
        raise ImportError(
            "visualization_summary requires matplotlib and openpyxl. Install: pip install matplotlib openpyxl"
        ) from e

    charts_dir = xlsx_path.parent / "charts"
    wb = load_workbook(xlsx_path)
    if _SHEET in wb.sheetnames:
        wb.remove(wb[_SHEET])
    ws = wb.create_sheet(_SHEET)

    # Dashboard-like grid: left card (A–E), spacer (F), right card (G–J).
    for col in "ABCDE":
        ws.column_dimensions[col].width = 12.5
    ws.column_dimensions["A"].width = 14.0
    ws.column_dimensions["F"].width = 3.0
    for col in "GHIJ":
        ws.column_dimensions[col].width = 12.5
    ws.column_dimensions["G"].width = 14.0
    ws.freeze_panes = None

    fill_hdr = PatternFill(fill_type="solid", start_color="111827", end_color="111827")
    fill_subhdr = PatternFill(fill_type="solid", start_color="EEF2FF", end_color="EEF2FF")
    fill_card = PatternFill(fill_type="solid", start_color="FFFFFF", end_color="FFFFFF")
    fill_canvas = PatternFill(fill_type="solid", start_color="F8FAFC", end_color="F8FAFC")
    edge = Side(style="thin", color="D0D7E2")
    border_card = Border(top=edge, bottom=edge, left=edge, right=edge)

    for r in range(1, 220):
        for c in range(1, 11):
            ws.cell(r, c).fill = fill_canvas

    ws["A1"] = "Visualization (Quick Scans)"
    ws["A1"].font = Font(bold=True, size=24)
    ws["A1"].fill = fill_hdr
    ws["A1"].font = Font(bold=True, size=24, color="FFFFFF")
    ws["A1"].alignment = Alignment(vertical="center")
    ws.merge_cells("A1:J1")
    ws.row_dimensions[1].height = 36

    n = len(version_df)
    ios_n = len(version_df[version_df["platform"] == "iOS"])
    and_n = len(version_df[version_df["platform"] == "Android"])
    dated_n = int(parse_release_dates(version_df).notna().sum())
    ws["A3"] = (
        "Same underlying rows as app_version_history / submission_observations\n"
        f"({n} observations; {ios_n} iOS / {and_n} Android; {dated_n} dated)."
    )
    ws["A3"].alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells("A3:J4")
    ws.row_dimensions[3].height = 40

    ws["A5"] = "Automated trend synopsis (quick read)"
    ws["A5"].font = Font(bold=True, size=11, color="1F2937")
    ws["A5"].fill = fill_subhdr
    ws.merge_cells("A5:J5")
    bullets = build_automated_trend_synopsis(version_df)
    syn_parts: list[str] = []
    for b in bullets:
        raw = str(b).strip()
        if not raw:
            continue
        one_line = re.sub(r"\s+", " ", raw.replace("\n", " ").strip())
        syn_parts.append(one_line.lstrip("•").strip())

    # Combined synopsis: one merged wrapped row (A6:J6).
    synopsis = " ".join("• " + p for p in syn_parts if p).strip()
    ws["A6"] = synopsis if synopsis else "(No synopsis — insufficient dated rows.)"
    ws["A6"].font = Font(size=11)
    ws["A6"].alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells("A6:J6")
    ws.row_dimensions[6].height = 288
    synopsis_bot_row = 6
    for c in range(1, 11):
        ws.cell(6, c).fill = fill_card
        ws.cell(6, c).border = border_card

    app_order = sorted(version_df["app_name"].fillna("").astype(str).unique().tolist())
    app_order = [a for a in app_order if a]

    row_anchor = 8
    # Dashboard layout:
    # Row 8: two heatmap cards side-by-side (A–E) and (G–J)
    # Then full-width cards below.

    heatmap_since = pd.Timestamp("2020-01-01")
    ds = dated_subset(version_df)
    fill_end: pd.Timestamp | None = None
    if ds is not None and len(ds) and "_dt" in ds.columns:
        mx_dt = ds["_dt"].max()
        if pd.notna(mx_dt):
            fill_end = pd.Timestamp(mx_dt)

    ios_p = _cadence_heatmap_prepare(
        version_df,
        platform="iOS",
        app_order=app_order,
        max_bins=30,
        bin_period="M",
        since=heatmap_since,
        fill_period_bins_since=heatmap_since,
        fill_end=fill_end,
    )
    and_p = _cadence_heatmap_prepare(
        version_df,
        platform="Android",
        app_order=app_order,
        max_bins=30,
        bin_period="M",
        since=heatmap_since,
        fill_period_bins_since=heatmap_since,
        fill_end=fill_end,
    )
    mx = 1.0
    if ios_p is not None:
        mx = max(mx, float(ios_p[0].max()))
    if and_p is not None:
        mx = max(mx, float(and_p[0].max()))
    shared_vmax = min(CADENCE_HEATMAP_VMAX_CAP, max(1.0, float(math.ceil(mx))))

    ios_buf = (
        _chart_update_frequency_heatmap_platform(
            version_df,
            platform="iOS",
            app_order=app_order,
            max_bins=30,
            bin_period="M",
            title="Cadence heatmap (monthly) — iOS",
            subtitle=(
                "iOS | monthly since 2020 (last 30 bins) | per-bin counts | shared palette | shared count cap 20"
            ),
            vmax=shared_vmax,
            prepared=ios_p,
            since=heatmap_since,
            fill_period_bins_since=heatmap_since,
            fill_end=fill_end,
        )
        if ios_p is not None
        else None
    )
    ios_png = _save_png_bytes(ios_buf, charts_dir / "heatmap_ios.png") if ios_buf else None

    and_buf = (
        _chart_update_frequency_heatmap_platform(
            version_df,
            platform="Android",
            app_order=app_order,
            max_bins=30,
            bin_period="M",
            title="Cadence heatmap (monthly) — Android",
            subtitle=(
                "Android | monthly since 2020 (last 30 bins) | per-bin counts | shared palette | shared count cap 20"
            ),
            vmax=shared_vmax,
            prepared=and_p,
            since=heatmap_since,
            fill_period_bins_since=heatmap_since,
            fill_end=fill_end,
        )
        if and_p is not None
        else None
    )
    and_png = _save_png_bytes(and_buf, charts_dir / "heatmap_android.png") if and_buf else None
    # --- Heatmap cards (row_anchor) ---
    card_h_title = row_anchor
    card_h_img = row_anchor + 1

    def _card_title(cell_addr: str, title: str) -> None:
        ws[cell_addr] = title
        ws[cell_addr].font = Font(bold=True, size=11, color="111827")
        ws[cell_addr].fill = fill_card
        ws[cell_addr].alignment = Alignment(vertical="center")

    # Left: iOS (A–E)
    ws.merge_cells(start_row=card_h_title, start_column=1, end_row=card_h_title, end_column=5)
    _card_title(f"A{card_h_title}", "iOS cadence heatmap")
    for c in range(1, 6):
        ws.cell(card_h_title, c).border = border_card
        ws.cell(card_h_img, c).border = border_card
        ws.cell(card_h_img, c).fill = fill_card

    if ios_png and ios_png.is_file():
        img = XLImage(str(ios_png))
        img.width = 470
        img.height = 300
        ws.add_image(img, f"A{card_h_img}")
    else:
        ws["A9"] = "(Not enough data — see synopsis.)"

    # Right: Android (G–J) (start at column 7)
    ws.merge_cells(start_row=card_h_title, start_column=7, end_row=card_h_title, end_column=10)
    _card_title(f"G{card_h_title}", "Android cadence heatmap")
    for c in range(7, 11):
        ws.cell(card_h_title, c).border = border_card
        ws.cell(card_h_img, c).border = border_card
        ws.cell(card_h_img, c).fill = fill_card

    if and_png and and_png.is_file():
        img = XLImage(str(and_png))
        img.width = 470
        img.height = 300
        ws.add_image(img, f"G{card_h_img}")
    else:
        ws["G9"] = "(Not enough data — see synopsis.)"

    # Advance below the two-row heatmap card block (plus a spacer).
    row_anchor = card_h_img + 18

    # --- Full-width cards ---
    def _add_fullwidth_card(title: str, png_path: Path | None) -> None:
        nonlocal row_anchor
        title_row = row_anchor
        img_row = row_anchor + 1
        ws.merge_cells(start_row=title_row, start_column=1, end_row=title_row, end_column=10)
        _card_title(f"A{title_row}", title)
        for c in range(1, 11):
            ws.cell(title_row, c).border = border_card
            ws.cell(img_row, c).border = border_card
            ws.cell(img_row, c).fill = fill_card
        if png_path and png_path.is_file():
            img = XLImage(str(png_path))
            img.width = 950
            img.height = 330
            ws.add_image(img, f"A{img_row}")
            row_anchor = img_row + 18
        else:
            ws["A" + str(img_row)] = "(Not enough data — see synopsis.)"
            row_anchor = img_row + 3

    depth_buf = _chart_observation_depth_by_app_platform(version_df)
    depth_png = _save_png_bytes(depth_buf, charts_dir / "depth_by_app_platform.png") if depth_buf else None
    _add_fullwidth_card("iOS vs Android observation depth (dated span) by app", depth_png)

    cat_buf = _chart_category_evolution_quartile_buckets(version_df)
    cat_png = _save_png_bytes(cat_buf, charts_dir / "category_evolution_quartiles.png") if cat_buf else None
    _add_fullwidth_card("Category share shift (oldest vs newest quartile; slope chart)", cat_png)

    wb.save(xlsx_path)


def try_append_visualization_sheet(xlsx_path: Path, version_df: pd.DataFrame) -> None:
    try:
        append_visualization_sheet(xlsx_path, version_df)
    except Exception as e:
        print(f"[warn] viz_fast_scan sheet skipped: {e}", file=sys.stderr)
