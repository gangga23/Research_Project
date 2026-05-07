"""
Shared facts for automated time-series commentary (submission_summary + viz_fast_scan).

Single source of truth for coverage, cadence, quartile category evolution, and
latest-year vs prior strategy-label contrasts — formatters add section headers / bullets.
"""

from __future__ import annotations

import pandas as pd

from version_display import version_string_missing

# Categories highlighted for quartile deltas (rule-based labels).
STRATEGY_CATS_QUARTILE: tuple[str, ...] = (
    "AI-related features",
    "Privacy / data policy changes",
    "New product feature",
    "Payments / monetization",
    "Bug fixes / performance improvements",
    "Security / account safety",
)

# Latest calendar year vs all prior years — headline labels (+ bug-fix share).
STRATEGY_CATS_RECENT_VS_PRIOR: tuple[str, ...] = (
    "AI-related features",
    "Privacy / data policy changes",
    "New product feature",
    "Bug fixes / performance improvements",
)

NO_PARSEABLE_DATES_SUBMISSION = (
    "No parseable release_date values in this export — cadence, quartile category evolution, and "
    "platform contrasts cannot be computed. Re-run the pipeline; confirm Android Wayback rows carry dates."
)

NO_PARSEABLE_DATES_SYNOPSIS_BULLET = (
    "No parseable release_date — cadence, quartile, and calendar-strategy visuals match "
    "submission_summary caveat (insufficient dated timeline)."
)

EPISTEMIC_NOTE_SUBMISSION = (
    "Epistemic note: row density reflects disclosure + scraper coverage, not compile frequency. "
    "Cross-check headline claims against raw release_notes and source_type."
)

EPISTEMIC_NOTE_SYNOPSIS_BULLET = (
    "Epistemic: row density reflects disclosure + scraper coverage, not compile-only cadence "
    "(same caveat as submission_summary)."
)


def parse_release_dates(version_df: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(version_df["release_date"].astype(str).str.strip(), errors="coerce")


def dated_subset(version_df: pd.DataFrame) -> pd.DataFrame | None:
    dt = parse_release_dates(version_df)
    mask = dt.notna()
    if int(mask.sum()) < 1:
        return None
    sub = version_df.loc[mask].copy()
    sub["_dt"] = dt[mask]
    return sub


def cat_share(frame: pd.DataFrame, cat: str) -> float:
    if len(frame) == 0:
        return 0.0
    return float((frame["update_category"] == cat).sum()) / float(len(frame))


def top_cat_shares(frame: pd.DataFrame, k: int = 4) -> str:
    if len(frame) == 0:
        return "(no rows)"
    vc = frame["update_category"].value_counts(normalize=True).head(k)
    return "; ".join(f"{idx} {100 * val:.0f}%" for idx, val in vc.items())


def median_gap_days(sorted_df: pd.DataFrame) -> float | None:
    if len(sorted_df) < 2:
        return None
    d = sorted_df["_dt"].diff().dt.days.dropna()
    if len(d) == 0:
        return None
    return float(d.median())


def platform_full_timeline_median_gap(sub: pd.DataFrame, plat: str) -> float | None:
    """Median days between consecutive dated rows for one platform (full sorted timeline)."""
    p = sub[sub["platform"] == plat].sort_values("_dt")
    if len(p) < 2:
        return None
    return median_gap_days(p)


def build_quick_scan_insights_text(version_df: pd.DataFrame) -> str:
    """
    Short bullet block for submission_summary (max 8 lines, leading •).
    Compresses density, cadence, reliability/provenance, and quartile category deltas.
    """
    bullets: list[str] = []
    ios = version_df[version_df["platform"] == "iOS"]
    andf = version_df[version_df["platform"] == "Android"]
    n_i, n_a = len(ios), len(andf)
    if n_i + n_a == 0:
        return "• No observations in this export."

    d_i = int(parse_release_dates(ios).notna().sum()) if n_i else 0
    d_a = int(parse_release_dates(andf).notna().sum()) if n_a else 0
    ri = n_i / max(n_i + n_a, 1)
    balance = (
        "iOS-heavy panel"
        if ri > 0.58
        else "Android-heavy panel"
        if ri < 0.42
        else "roughly balanced row counts"
    )
    pct_i = 100.0 * d_i / max(n_i, 1)
    pct_a = 100.0 * d_a / max(n_a, 1)
    bullets.append(
        f"• Data density: iOS {n_i} rows ({d_i} dated, {pct_i:.0f}% dated) vs Android {n_a} ({d_a} dated, "
        f"{pct_a:.0f}% dated) — {balance}."
    )

    sub = dated_subset(version_df)

    def hi_share(frame: pd.DataFrame) -> float:
        if len(frame) == 0:
            return 0.0
        return 100.0 * float((frame["confidence_level"].astype(str).str.lower() == "high").sum()) / float(len(frame))

    def src_share(frame: pd.DataFrame, types: set[str]) -> float:
        if len(frame) == 0:
            return 0.0
        return 100.0 * float(frame["source_type"].isin(types).sum()) / float(len(frame))

    if sub is None:
        bullets.append(
            "• Cadence / quartile dashboard reads need parseable release_date — disproportionately affects Android "
            "archive paths."
        )
        bullets.append(
            f"• Confidence mix (all rows): iOS high ≈{hi_share(ios):.0f}% vs Android high ≈{hi_share(andf):.0f}%."
        )
        bullets.append(
            "• Provenance: contrast app_store_web (iOS) vs Android wayback_snapshot / feature_signal / "
            "developer_changelog mix — see cadence heatmaps (1A–1B) and URL-class chart (1C)."
        )
        return "\n".join(bullets[:8])

    gi = platform_full_timeline_median_gap(sub, "iOS")
    ga = platform_full_timeline_median_gap(sub, "Android")
    if gi is not None and ga is not None:
        if gi < ga * 0.85:
            bullets.append(
                f"• Cadence proxy: iOS median gap ≈{gi:.0f} d vs Android ≈{ga:.0f} d between consecutive dated rows "
                "— iOS reads more compressed; Android sparser / archive-fragmented."
            )
        elif ga < gi * 0.85:
            bullets.append(
                f"• Cadence proxy: Android median gap ≈{ga:.0f} d vs iOS ≈{gi:.0f} d — tighter spacing on this panel "
                "(subset-sensitive; filter by source_type)."
            )
        else:
            bullets.append(
                f"• Cadence proxy: similar median spacing once dated (iOS ≈{gi:.0f} d, Android ≈{ga:.0f} d)."
            )
    elif gi is not None or ga is not None:
        gi_s = f"≈{gi:.0f} d" if gi is not None else "n/a"
        ga_s = f"≈{ga:.0f} d" if ga is not None else "n/a"
        bullets.append(f"• Cadence proxy: partial coverage — iOS {gi_s} vs Android {ga_s} median gap.")

    bullets.append(
        f"• Source reliability (confidence_level): iOS high ≈{hi_share(ios):.0f}% vs Android ≈{hi_share(andf):.0f}% "
        "(Android stacks Wayback/feeds → expect more medium/low)."
    )
    bullets.append(
        "• Provenance skew: iOS app_store_web ≈"
        f"{src_share(ios, {'app_store_web'}):.0f}% of rows; Android developer_changelog + play snapshot ≈"
        f"{src_share(andf, {'developer_changelog', 'play_store_snapshot'}):.0f}% vs "
        f"wayback_snapshot + feature_signal ≈{src_share(andf, {'wayback_snapshot', 'feature_signal'}):.0f}% "
        "(cadence heatmaps 1A–1B; URL-class mix 1C)."
    )

    split = _quartile_oldest_newest(sub)
    if split:
        oldest, newest = split

        def fmt_arrow(label: str, cat: str) -> str | None:
            pp = 100.0 * (cat_share(newest, cat) - cat_share(oldest, cat))
            if pp >= 3.0:
                return f"{label} ↑ (+{pp:.0f} pp)"
            if pp <= -3.0:
                return f"{label} ↓ ({pp:.0f} pp)"
            return None

        bits = [
            fmt_arrow("Bug fixes", "Bug fixes / performance improvements"),
            fmt_arrow("AI", "AI-related features"),
            fmt_arrow("Monetization", "Payments / monetization"),
        ]
        bits = [b for b in bits if b]
        if bits:
            bullets.append(
                "• Category quartiles (oldest → newest dated quartile): "
                + "; ".join(bits)
                + " — category quartile slope chart."
            )
        else:
            bullets.append(
                "• Category quartiles: bug-fix / AI / monetization stable (<3 pp swing) on this timeline — "
                "category quartile slope chart."
            )
    else:
        bullets.append(
            "• Category quartiles: insufficient dated depth for oldest/newest split — expand captures or filter subset."
        )

    return "\n".join(bullets[:8])


def coverage_lines(version_df: pd.DataFrame, sub: pd.DataFrame) -> list[str]:
    """Platform coverage strings (no leading dash or bullet)."""
    lines: list[str] = []
    for plat in ("iOS", "Android"):
        p_all = version_df[version_df["platform"] == plat]
        p_d = sub[sub["platform"] == plat]
        if len(p_all) == 0:
            continue
        ver_nonempty = (~p_all["version_number"].map(version_string_missing)).mean() * 100.0
        top_src = p_all["source_type"].value_counts().head(2)
        src_hint = ", ".join(f"{k} ({int(v)})" for k, v in top_src.items())
        lines.append(
            f"{plat}: {len(p_all)} observations, {len(p_d)} with dates; "
            f"{ver_nonempty:.0f}% carry a non-empty version_number. Dominant source_type: {src_hint}."
        )
    ios_d = sub[sub["platform"] == "iOS"]
    and_d = sub[sub["platform"] == "Android"]
    if len(ios_d) and len(and_d):
        span_i = (ios_d["_dt"].max() - ios_d["_dt"].min()).days
        span_a = (and_d["_dt"].max() - and_d["_dt"].min()).days
        lines.append(
            f"Historical depth (dated span): iOS ≈ {span_i} d vs Android ≈ {span_a} d — Android density is "
            "often archive- or feed-driven, not a continuous vendor changelog."
        )
    return lines


def cadence_lines(sub: pd.DataFrame) -> list[str]:
    """Early vs late window median inter-release spacing, by platform."""
    chunks: list[str] = []
    for plat in ("iOS", "Android"):
        p = sub[sub["platform"] == plat].sort_values("_dt")
        if len(p) < 6:
            chunks.append(f"{plat}: insufficient dated rows for an early/late cadence split (n={len(p)}).")
            continue
        mid = len(p) // 2
        early, late = p.iloc[:mid], p.iloc[mid:]
        ge = median_gap_days(early)
        gl = median_gap_days(late)
        if ge is None or gl is None:
            chunks.append(f"{plat}: cadence split undefined.")
            continue
        direction = "tighter" if gl < ge * 0.85 else "looser" if gl > ge * 1.15 else "similar"
        chunks.append(
            f"{plat}: median gap between consecutive dated rows — early window ≈ {ge:.0f} d vs late ≈ {gl:.0f} d "
            f"({direction} cadence in the late window). Proxy for release rhythm, not store approval latency."
        )
    return chunks


def _quartile_oldest_newest(sub: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    s = sub.sort_values("_dt").reset_index(drop=True)
    n = len(s)
    if n < 8:
        return None
    q = n // 4
    if q < 1:
        return None
    return s.iloc[:q], s.iloc[-q:]


def quartile_delta_parts(
    oldest: pd.DataFrame, newest: pd.DataFrame
) -> tuple[list[str], bool]:
    """Returns (delta_fragments for notable shifts, stable_band)."""
    deltas: list[str] = []
    for cat in STRATEGY_CATS_QUARTILE:
        o, ne = cat_share(oldest, cat), cat_share(newest, cat)
        pp = 100.0 * (ne - o)
        if abs(pp) >= 3.0:
            deltas.append(f"{cat}: {pp:+.0f} pp (newest − oldest quartile)")
    stable = len(deltas) == 0
    return deltas, stable


def quartile_lines_for_submission(sub: pd.DataFrame) -> list[str]:
    """Three or four lines for submission_summary (no leading dash)."""
    split = _quartile_oldest_newest(sub)
    if split is None:
        return ["Too few dated rows for a stable oldest-vs-newest quartile category comparison."]
    oldest, newest = split
    o_txt, n_txt = top_cat_shares(oldest, 3), top_cat_shares(newest, 3)
    lines = [
        f"Oldest dated quartile (n={len(oldest)}): {o_txt}.",
        f"Newest dated quartile (n={len(newest)}): {n_txt}.",
    ]
    deltas, stable = quartile_delta_parts(oldest, newest)
    if not stable:
        lines.append("Notable shifts (rule-based labels): " + "; ".join(deltas) + ".")
    else:
        lines.append("Category mix shifts across quartiles are within ±3 pp for tracked strategy labels.")
    return lines


def quartile_bullet_for_synopsis(sub: pd.DataFrame) -> str:
    """Single bullet body (no •) for viz_fast_scan."""
    split = _quartile_oldest_newest(sub)
    if split is None:
        return "Category quartiles: too few dated rows for stable oldest/newest quartile comparison."
    oldest, newest = split
    deltas, stable = quartile_delta_parts(oldest, newest)
    if stable:
        return "Category quartiles: headline strategy-label shifts within ±3 pp (newest vs oldest quartile)."
    short = []
    for cat in STRATEGY_CATS_QUARTILE:
        o, ne = cat_share(oldest, cat), cat_share(newest, cat)
        pp = 100.0 * (ne - o)
        if abs(pp) >= 3.0:
            short.append(f"{cat.split('/')[0].strip()}: {pp:+.0f} pp")
    return "Category quartiles (newest − oldest dated quartile): " + "; ".join(short) + "."


def strategy_line_for_submission(sub: pd.DataFrame) -> str:
    """One line, no leading dash — submission prefixes with em dash."""
    sub = sub.copy()
    sub["year"] = sub["_dt"].dt.year
    y_max = int(sub["year"].max())
    recent = sub[sub["year"] == y_max]
    prior = sub[sub["year"] < y_max]
    if len(prior) < 5 or len(recent) < 3:
        return (
            "Strategy read: insufficient split between latest year and prior years for a robust vendor-messaging "
            "contrast; rely on quartile comparison above."
        )
    bits: list[str] = []
    for cat in STRATEGY_CATS_RECENT_VS_PRIOR:
        pr, pp = cat_share(recent, cat), cat_share(prior, cat)
        d = 100.0 * (pr - pp)
        if abs(d) >= 2.0:
            bits.append(f"{cat} {d:+.0f} pp vs all prior years")
    if not bits:
        return "Strategy read: latest-year vs prior mix is stable on headline categories (±2 pp)."
    return "Strategy read (heuristic labels): " + "; ".join(bits) + " — validate in release_notes text."


def strategy_bullet_for_synopsis(sub: pd.DataFrame) -> str:
    """Single bullet body (no •) for viz; shortened category tokens where helpful."""
    sub = sub.copy()
    sub["year"] = sub["_dt"].dt.year
    y_max = int(sub["year"].max())
    recent = sub[sub["year"] == y_max]
    prior = sub[sub["year"] < y_max]
    if len(prior) < 5 or len(recent) < 3:
        return (
            "Latest-year vs prior strategy contrast: insufficient dated split — rely on quartile chart below."
        )
    bits: list[str] = []
    short_map = {
        "AI-related features": "AI",
        "Privacy / data policy changes": "Privacy",
        "New product feature": "New feature",
        "Bug fixes / performance improvements": "Bug-fix label",
    }
    for cat in ("AI-related features", "Privacy / data policy changes", "New product feature"):
        d = 100.0 * (cat_share(recent, cat) - cat_share(prior, cat))
        if abs(d) >= 2.0:
            bits.append(f"{short_map[cat]} {d:+.0f} pp ({y_max} vs prior)")
    bf_d = 100.0 * (
        cat_share(recent, "Bug fixes / performance improvements")
        - cat_share(prior, "Bug fixes / performance improvements")
    )
    if abs(bf_d) >= 2.0:
        bits.append(f"Bug-fix label {bf_d:+.0f} pp ({y_max} vs prior)")
    if bits:
        return "Latest-year vs all prior (dated): " + "; ".join(bits) + "."
    return (
        f"Latest-year ({y_max}) vs prior: headline category mix stable within ±2 pp on tracked labels."
    )


def cadence_lines_synopsis(sub: pd.DataFrame) -> list[str]:
    """Shorter cadence lines for viz bullets (no •)."""
    lines: list[str] = []
    for plat in ("iOS", "Android"):
        p = sub[sub["platform"] == plat].sort_values("_dt")
        if len(p) < 6:
            lines.append(f"Cadence ({plat}): insufficient dated rows for early/late split (n={len(p)}).")
            continue
        mid = len(p) // 2
        early, late = p.iloc[:mid], p.iloc[mid:]
        ge, gl = median_gap_days(early), median_gap_days(late)
        if ge is None or gl is None:
            lines.append(f"Cadence ({plat}): undefined median spacing.")
            continue
        direction = "tighter" if gl < ge * 0.85 else "looser" if gl > ge * 1.15 else "similar"
        lines.append(
            f"Cadence ({plat}): median days between consecutive dated rows — early window ≈ {ge:.0f} d, "
            f"late ≈ {gl:.0f} d ({direction} late-window rhythm)."
        )
    return lines


def coverage_lines_synopsis(sub: pd.DataFrame, version_df: pd.DataFrame) -> list[str]:
    """Coverage bullets for viz (no •) — compressed wording."""
    lines: list[str] = []
    for plat in ("iOS", "Android"):
        p_all = version_df[version_df["platform"] == plat]
        p_d = sub[sub["platform"] == plat]
        if len(p_all) == 0:
            continue
        ver_nonempty = (~p_all["version_number"].map(version_string_missing)).mean() * 100.0
        top_src = p_all["source_type"].value_counts().head(2)
        src_hint = ", ".join(f"{k} ({int(v)})" for k, v in top_src.items())
        lines.append(
            f"{plat}: {len(p_all)} observations / {len(p_d)} dated; {ver_nonempty:.0f}% with "
            f"version_number; dominant source_type: {src_hint}."
        )
    ios_d = sub[sub["platform"] == "iOS"]
    and_d = sub[sub["platform"] == "Android"]
    if len(ios_d) and len(and_d):
        span_i = (ios_d["_dt"].max() - ios_d["_dt"].min()).days
        span_a = (and_d["_dt"].max() - and_d["_dt"].min()).days
        lines.append(
            f"Dated span (coverage depth proxy): iOS ≈ {span_i} d vs Android ≈ {span_a} d "
            "(archive/listing-driven gaps expected on Android)."
        )
    return lines


def build_timeseries_insights_text(version_df: pd.DataFrame) -> str:
    """Full narrative for submission_summary » Time-series insights."""
    dt = parse_release_dates(version_df)
    mask = dt.notna()
    if not mask.any():
        return NO_PARSEABLE_DATES_SUBMISSION

    sub = version_df.loc[mask].copy()
    sub["_dt"] = dt[mask]

    cov = coverage_lines(version_df, sub)
    cadence_hdr = "Cadence (dated rows, consecutive spacing)"
    cadence_body = "\n".join("— " + c for c in cadence_lines(sub))
    cat_hdr = "update_category evolution (dated)"
    cat_body = "\n".join("— " + x for x in quartile_lines_for_submission(sub))
    strat = "— " + strategy_line_for_submission(sub)

    parts = [
        "COVERAGE (iOS vs Android)",
        "\n".join("— " + x for x in cov) if cov else "— No platform split available.",
        "",
        cadence_hdr,
        cadence_body,
        "",
        cat_hdr,
        cat_body,
        "",
        strat,
        "",
        EPISTEMIC_NOTE_SUBMISSION,
    ]
    return "\n".join(parts)


def build_automated_trend_synopsis_bullets(version_df: pd.DataFrame) -> list[str]:
    """Prefixed bullets for viz_fast_scan."""
    sub = dated_subset(version_df)
    if sub is None:
        return ["• " + NO_PARSEABLE_DATES_SYNOPSIS_BULLET]
    out: list[str] = []
    for line in coverage_lines_synopsis(sub, version_df):
        out.append("• " + line)
    for line in cadence_lines_synopsis(sub):
        out.append("• " + line)
    out.append("• " + quartile_bullet_for_synopsis(sub))
    out.append("• " + strategy_bullet_for_synopsis(sub))
    out.append("• " + EPISTEMIC_NOTE_SYNOPSIS_BULLET)
    return out
