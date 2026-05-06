"""
Fast-scan visuals aligned with ``timeseries_insights_core`` (same facts as
``submission_summary.build_timeseries_insights``):

This sheet is intentionally small and rubric-aligned:
- update frequency over time by app (cadence heatmap)
- category share shift (oldest vs newest quartile; slope chart)
- iOS vs Android observation depth per app (dated span)

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
    return build_automated_trend_synopsis_bullets(version_df)


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
) -> io.BytesIO | None:
    """
    Cadence heatmap for a single platform (dated rows).
    Rows: app_name; columns: YYYY-MM or YYYYQn; values: observation counts.
    Use the same ``vmax`` for iOS and Android so color means the same count on both charts.
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

    # +20% font sizes globally (Excel zoom variability).
    base_fs = 10
    fs_title = int(base_fs * 1.44)
    fs_sub = int(base_fs * 1.15)
    fs_axis = int(base_fs * 1.2)
    fs_tick_y = int(base_fs * 1.05)
    fs_tick_x = int(base_fs * 0.95)
    fs_cbar = int(base_fs * 1.25)

    # Same canvas geometry for iOS and Android so paired heatmaps match in Excel.
    row_px = 0.32
    extra_pad = 2.6
    h = min(row_px * len(apps) + extra_pad, 10.8)
    w = min(0.35 * len(xlabels) + 4.8, 12.5)
    fig, ax = plt.subplots(figsize=(w, h))
    fig.patch.set_facecolor("#ffffff")
    ax.set_facecolor("#ffffff")

    vm = float(vmax) if vmax is not None else min(
        CADENCE_HEATMAP_VMAX_CAP,
        max(1.0, float(np.ceil(float(mat.max())))),
    )
    im = ax.imshow(mat, aspect="auto", cmap=_cadence_heatmap_colormap(), vmin=0, vmax=vm)
    cbar_extend = "max" if float(mat.max()) > vm + 1e-9 else "neither"
    ax.set_title(title, fontsize=fs_title, pad=18)
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
    ax.set_ylabel("App")
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
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(
        buf,
        format="png",
        dpi=115,
        bbox_inches="tight",
        facecolor="#ffffff",
        edgecolor="none",
        pad_inches=0.12,
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
        dpi=115,
        bbox_inches="tight",
        facecolor="#ffffff",
        edgecolor="none",
        pad_inches=0.12,
    )
    plt.close(fig)
    buf.seek(0)
    return buf

def _chart_category_evolution_quartile_buckets(version_df: pd.DataFrame) -> io.BytesIO | None:
    """Oldest vs newest dated quartile — slope chart for Bug / AI / Payments (Other in caption only)."""
    import matplotlib.pyplot as plt
    import numpy as np

    BUG = "Bug fixes / performance improvements"
    AI = "AI-related features"
    PAY = "Payments / monetization"

    sub = dated_subset(version_df)
    if sub is None or len(sub) < 8:
        return None
    s = sub.sort_values("_dt").reset_index(drop=True)
    q = len(s) // 4
    oldest = s.iloc[:q].copy()
    newest = s.iloc[-q:].copy()

    def _fmt_rng(frame: pd.DataFrame) -> str:
        a = frame["_dt"].min()
        b = frame["_dt"].max()
        if pd.isna(a) or pd.isna(b):
            return "n/a"
        # Hyphen (not en-dash) + explicit strftime args avoid odd tick rendering;
        # mpl tick Text can treat '%' specially unless parse_math=False.
        left = a.strftime("%b %Y")
        right = b.strftime("%b %Y")
        return f"{left}-{right}"

    o_rng = _fmt_rng(oldest)
    n_rng = _fmt_rng(newest)

    def bucket(arr):
        out: list[str] = []
        for c in arr:
            cs = str(c)
            if cs == BUG:
                out.append(BUG)
            elif cs == AI:
                out.append(AI)
            elif cs == PAY:
                out.append(PAY)
            else:
                out.append("Other")
        return out

    oldest["_bk"] = bucket(oldest["update_category"])
    newest["_bk"] = bucket(newest["update_category"])
    keys = [BUG, AI, PAY]
    short = ["Bug fixes / performance", "AI-related", "Payments"]
    palette = ["#c0504d", "#4f81bd", "#9bbb59"]
    other_old = 100.0 * float((oldest["_bk"] == "Other").sum()) / float(max(len(oldest), 1))
    other_new = 100.0 * float((newest["_bk"] == "Other").sum()) / float(max(len(newest), 1))

    def share_bk(frame: pd.DataFrame, key: str) -> float:
        return 100.0 * float((frame["_bk"] == key).sum()) / float(max(len(frame), 1))

    o_share = [share_bk(oldest, k) for k in keys]
    n_share = [share_bk(newest, k) for k in keys]

    def _slope_endpoint_dy_pt(vals: list[float], *, spread: float = 28.0) -> list[float]:
        """Spread endpoint labels vertically (points); widening avoids collisions on dense slopes."""
        order = sorted(range(len(vals)), key=lambda i: vals[i])
        mid = (len(order) - 1) / 2.0
        out = [0.0] * len(vals)
        for rank, i in enumerate(order):
            out[i] = (rank - mid) * spread
        return out

    def _slope_bump_close_pairs_dy_pt(vals: list[float], base_dy: list[float], *, min_sep_pt: float = 26.0) -> list[float]:
        """Force larger vertical separation (points) when two endpoints sit close in % terms."""
        out = list(base_dy)
        order = sorted(range(len(vals)), key=lambda i: float(vals[i]))
        for k in range(1, len(order)):
            lo, hi = order[k - 1], order[k]
            gap_pct = float(vals[hi]) - float(vals[lo])
            if gap_pct >= 9.0:
                continue
            if abs(out[hi] - out[lo]) >= min_sep_pt:
                continue
            mid = (out[lo] + out[hi]) / 2.0
            out[lo] = mid - min_sep_pt / 2.0 - 3.0
            out[hi] = mid + min_sep_pt / 2.0 + 3.0
        return out

    fo = [float(v) for v in o_share]
    fn = [float(v) for v in n_share]
    dy_left = _slope_bump_close_pairs_dy_pt(fo, _slope_endpoint_dy_pt(fo, spread=32.0), min_sep_pt=30.0)
    dy_right = _slope_endpoint_dy_pt(fn, spread=32.0)
    dy_right = _slope_bump_close_pairs_dy_pt(fn, dy_right, min_sep_pt=30.0)
    # Low % endpoints sit on/near y=0; negative offset points pull labels under the axis — bump upward.
    low_pct_bump_pt = 26.0
    for i in range(len(fo)):
        if fo[i] < 12.0:
            dy_left[i] = max(float(dy_left[i]), low_pct_bump_pt)
        if fn[i] < 12.0:
            dy_right[i] = max(float(dy_right[i]), low_pct_bump_pt)

    fig, ax = plt.subplots(figsize=(8.2, 5.45))
    fig.patch.set_facecolor("#ffffff")
    ax.set_facecolor("#ffffff")
    fig.subplots_adjust(left=0.13, right=0.94, top=0.72, bottom=0.26)

    import matplotlib.patheffects as pe

    fs_ann = 11  # ~9 * 1.2
    fs_axis = 13  # tick & axis labels ~10–11 * 1.2
    fs_leg = 11

    xs = np.array([0.0, 1.0])
    for idx, (label, color) in enumerate(zip(short, palette)):
        yo, yn = float(o_share[idx]), float(n_share[idx])
        lx_pad = -52 - idx * 12
        rx_pad = 20 + idx * 12
        ax.plot(
            xs,
            [yo, yn],
            marker="o",
            markersize=9,
            markeredgecolor="#ffffff",
            markeredgewidth=1.85,
            linewidth=2.9,
            color=color,
            label=label,
            solid_capstyle="round",
            clip_on=False,
            zorder=4,
        )
        ta = ax.annotate(
            f"{yo:.0f}%",
            (0.0, yo),
            xytext=(lx_pad, dy_left[idx]),
            textcoords="offset points",
            ha="right",
            va="center",
            fontsize=fs_ann,
            fontweight="600",
            color=color,
            clip_on=False,
            zorder=6,
        )
        ta.set_path_effects([pe.withStroke(linewidth=4.0, foreground="#ffffff")])
        tb = ax.annotate(
            f"{yn:.0f}%",
            (1.0, yn),
            xytext=(rx_pad, dy_right[idx]),
            textcoords="offset points",
            ha="left",
            va="center",
            fontsize=fs_ann,
            fontweight="600",
            color=color,
            clip_on=False,
            zorder=6,
        )
        tb.set_path_effects([pe.withStroke(linewidth=4.0, foreground="#ffffff")])

    ax.set_xticks([0.0, 1.0])
    xt1 = f"Oldest quartile\n({o_rng})"
    xt2 = f"Newest quartile\n({n_rng})"
    xlabels_obj = ax.set_xticklabels([xt1, xt2], fontsize=fs_axis, color="#374151")
    for xl in xlabels_obj:
        if hasattr(xl, "set_parse_math"):
            xl.set_parse_math(False)
    ax.set_xlim(-0.52, 1.48)
    peak = max(max(o_share, default=0.0), max(n_share, default=0.0))
    ylim_top = min(100.0, peak + 24.0)
    ax.set_ylim(-3.0, ylim_top)
    ax.set_yticks([t for t in (0, 20, 40, 60, 80, 100) if t <= ylim_top + 1e-6])
    ax.set_ylabel("Share within quartile (%)", fontsize=fs_axis, color="#374151", labelpad=10)

    ax.set_axisbelow(True)
    ax.axvline(0.0, color="#d8dee8", linewidth=1.45, zorder=1)
    ax.axvline(1.0, color="#d8dee8", linewidth=1.45, zorder=1)
    ax.grid(axis="y", color="#e8ecf2", linestyle="-", linewidth=1.0, zorder=0)
    ax.tick_params(axis="both", colors="#4b5563", labelsize=fs_axis, length=4, width=0.9)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.spines["left"].set_color("#aeb9c9")
    ax.spines["bottom"].set_color("#aeb9c9")
    ax.spines["left"].set_linewidth(1.15)
    ax.spines["bottom"].set_linewidth(1.15)

    ax.legend(
        loc="upper right",
        bbox_to_anchor=(0.99, 0.99),
        ncol=1,
        fontsize=fs_leg,
        frameon=True,
        fancybox=True,
        framealpha=1.0,
        edgecolor="#cdd6e4",
        facecolor="#ffffff",
    )

    fig.text(
        0.13,
        0.97,
        "Category share shift — oldest vs newest quartile",
        fontsize=14,
        fontweight="600",
        color="#1f2937",
        va="top",
        ha="left",
    )
    fig.text(
        0.13,
        0.895,
        f"Lines omit Other — Other share: {other_old:.0f}% (oldest), {other_new:.0f}% (newest).",
        fontsize=12,
        color="#5c6575",
        va="top",
        ha="left",
    )

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=115, bbox_inches="tight", facecolor="#ffffff", edgecolor="none", pad_inches=0.18)
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
    fig_h = min(4.35, max(2.55, 0.19 * n_apps + 1.28))
    fig, ax = plt.subplots(figsize=(7.6, fig_h))
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
        dpi=115,
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
        "In the quartile category chart, do bug-fix / AI / payments shifts reflect disclosure strategy (templated copy) or real content change? Validate using high-confidence provenance subsets.",
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
        from openpyxl.styles import Alignment, Font
    except ImportError as e:
        raise ImportError(
            "visualization_summary requires matplotlib and openpyxl. Install: pip install matplotlib openpyxl"
        ) from e

    charts_dir = xlsx_path.parent / "charts"
    wb = load_workbook(xlsx_path)
    if _SHEET in wb.sheetnames:
        wb.remove(wb[_SHEET])
    ws = wb.create_sheet(_SHEET)

    ws["A1"] = "Visualization (Quick Scans)"
    ws["A1"].font = Font(bold=True, size=14)
    ws.merge_cells("A1:J1")

    n = len(version_df)
    ios_n = len(version_df[version_df["platform"] == "iOS"])
    and_n = len(version_df[version_df["platform"] == "Android"])
    dated_n = int(parse_release_dates(version_df).notna().sum())
    ws["A3"] = (
        f"Same underlying rows as app_version_history / submission_observations ({n} observations; {ios_n} iOS / "
        f"{and_n} Android; {dated_n} dated). Charts below focus on cadence by app, category evolution, and per-app "
        "iOS/Android depth (dated span)."
    )
    ws["A3"].alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells("A3:J4")
    ws.row_dimensions[3].height = 42

    ws["A5"] = "Automated trend synopsis (quick read)"
    ws["A5"].font = Font(bold=True, size=11)
    synopsis = "\n".join(build_automated_trend_synopsis(version_df))
    ws["A6"] = synopsis
    ws["A6"].alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells("A6:J13")
    ws.row_dimensions[6].height = 180

    app_order = sorted(version_df["app_name"].fillna("").astype(str).unique().tolist())
    app_order = [a for a in app_order if a]

    row_anchor = 15
    images: list[tuple[str, Path | None]] = []

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
            title="iOS update frequency over time by app (heatmap)",
            subtitle=(
                "iOS only | app_store_web source | high confidence | monthly from 2020, last 30 months shown | "
                "per-bin counts; cap 20 (triangle if above)"
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
    images.append(("1A. iOS cadence heatmap", ios_png))

    and_buf = (
        _chart_update_frequency_heatmap_platform(
            version_df,
            platform="Android",
            app_order=app_order,
            max_bins=30,
            bin_period="M",
            title="Android update frequency over time by app (heatmap)",
            subtitle=(
                "Android only | monthly from 2020 (empty months = no dated capture in slice) | mixed sources — "
                "same month axis & color scale as iOS (per-bin counts; cap 20, triangle = values above cap)"
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
    images.append(("1B. Android cadence heatmap", and_png))

    depth_buf = _chart_observation_depth_by_app_platform(version_df)
    depth_png = _save_png_bytes(depth_buf, charts_dir / "depth_by_app_platform.png") if depth_buf else None
    images.append(("iOS vs Android observation depth (dated span) by app", depth_png))

    cat_buf = _chart_category_evolution_quartile_buckets(version_df)
    cat_png = _save_png_bytes(cat_buf, charts_dir / "category_evolution_quartiles.png") if cat_buf else None
    images.append(("Category share shift (oldest vs newest quartile; slope chart)", cat_png))

    for title, p in images:
        ws.cell(row=row_anchor, column=1, value=title)
        ws.cell(row=row_anchor, column=1).font = Font(bold=True, size=10)
        row_anchor += 1
        if p is None or not p.is_file():
            ws.cell(row=row_anchor, column=1, value="(Not enough data for this chart — see synopsis.)")
            row_anchor += 3
            continue
        img = XLImage(str(p))
        img.width = 640
        img.height = 330
        ws.add_image(img, f"A{row_anchor}")
        row_anchor += 21

    ws.cell(row=row_anchor, column=1, value="Reading note")
    ws.cell(row=row_anchor, column=1).font = Font(bold=True, size=11)
    row_anchor += 1
    note_cell = ws.cell(
        row=row_anchor,
        column=1,
        value=(
            "Lower AI-related share can reflect template dilution as bug-fix framing rises, "
            "not necessarily less AI work."
        ),
    )
    note_cell.alignment = Alignment(wrap_text=True, vertical="top")
    ws.merge_cells(start_row=row_anchor, start_column=1, end_row=row_anchor + 1, end_column=10)
    ws.row_dimensions[row_anchor].height = 28
    ws.row_dimensions[row_anchor + 1].height = 28

    ws.column_dimensions["A"].width = 108
    wb.save(xlsx_path)


def try_append_visualization_sheet(xlsx_path: Path, version_df: pd.DataFrame) -> None:
    try:
        append_visualization_sheet(xlsx_path, version_df)
    except Exception as e:
        print(f"[warn] viz_fast_scan sheet skipped: {e}", file=sys.stderr)
