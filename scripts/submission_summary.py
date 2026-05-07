"""
Human-readable submission narrative for Excel: methodology, time-series
insights derived from ``app_version_history``, and data-collection challenges.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import pandas as pd

from timeseries_insights_core import (
    build_quick_scan_insights_text,
    build_timeseries_insights_text,
    cat_share,
    dated_subset,
    parse_release_dates,
)

META_FILENAME = "project_meta.json"


def load_repository_url(project_root: Path) -> str:
    meta = project_root / "config" / META_FILENAME
    if meta.exists():
        try:
            data = json.loads(meta.read_text(encoding="utf-8"))
            u = (data.get("repository_url") or "").strip()
            if u:
                return u
        except (json.JSONDecodeError, OSError):
            pass
    return (os.environ.get("SUBMISSION_GITHUB_REPO_URL") or "").strip()


def methodology_block(repo_url: str) -> str:
    repo_line = repo_url if repo_url else "https://github.com/gangga23/Research_Project"
    return (
        "Automated Python pipelines (no LLM-generated store text).\n\n"
        "• iOS — iTunes Lookup for app_master; multi-version history from App Store product-page HTML "
        "(embedded versionHistory / hydration JSON; scripts/app_store_web_history.py). Lookup-only single row "
        "if web parse returns nothing.\n\n"
        "• Android — Play listing HTML + google-play-scraper; heuristic changelog strings from embedded JSON; "
        "Internet Archive CDX + archived Play pages (same heuristic). Optional RSS: feed_validator classifies "
        "release_feed vs product_blog; strict developer_changelog requires semver or explicit in-text date; "
        "else feature_signal. review_inferred only when no higher-confidence structured signal.\n\n"
        "• Deliverables — CSV + this workbook; every observation carries source_type, confidence_level, "
        "update_category (rule-based single label).\n\n"
        "• Confidence_level (read with source_type) — High: structured live listing capture (Play snapshot row or "
        "App Store web embedded versionHistory / primary Lookup metadata). Medium: Wayback-archived Play HTML or "
        "iOS Lookup-only fallback when embedded history is thin. Low: vendor RSS/marketing lines classified as "
        "feature_signal (no strict semver/date gate) or review-inferred timing when no stronger structured signal "
        "exists.\n\n"
        f"Repository:\n{repo_line}"
    )


def _confidence_lines(version_df: pd.DataFrame) -> str:
    lines: list[str] = []
    for plat in ("iOS", "Android"):
        p = version_df[version_df["platform"] == plat]
        if len(p) == 0:
            continue
        parts = []
        for lev in ("high", "medium", "low"):
            pct = 100.0 * float((p["confidence_level"].astype(str).str.lower() == lev).sum()) / max(len(p), 1)
            parts.append(f"{lev} {pct:.0f}%")
        lines.append(f"{plat}: " + ", ".join(parts) + ".")
    if not lines:
        return "No rows to summarize confidence."
    return (
        "Confidence mix (this export):\n"
        + "\n".join("— " + x for x in lines)
        + '\n— Definitions: see "Data collection approach" → Confidence_level. '
        "Do not equate label frequency with ground-truth release quality."
    )


def challenges_block(version_df: pd.DataFrame) -> str:
    andf = version_df[version_df["platform"] == "Android"]
    miss_ver = 0.0
    if len(andf):
        miss_ver = 100.0 * float(andf["version_number"].fillna("").astype(str).str.strip().eq("").mean())
    tech = (
        "• Coverage asymmetry: Android lacks a public multi-version changelog API comparable to the App Store web "
        "embed; longitudinal depth depends on Wayback, feeds, and bounded review inference.\n\n"
        f"• Android version_number is blank for {miss_ver:.0f}% of observations — genuine disclosure limits "
        "(Wayback snapshots, Play variant listings), not fabricated fillers. submission_observations » notes flags "
        "fabrication policy, Wayback date caveat, Play variant listings, and scrape anomalies only; exclude sparse-version "
        "rows from strict semver sequencing but retain for cadence/category reads.\n\n"
        "• Layout drift: Play/App Store HTML and JSON shapes change; heuristics need maintenance.\n\n"
        "• Wayback: uneven capture by app/period; CDX/network noise reduces snapshots without failing the run.\n\n"
        "• RSS gates: semver/date rules limit false version rows but split vendor narrative across "
        "developer_changelog vs feature_signal.\n\n"
        "• update_category: regex-first single label — useful for aggregate trends, weak for fine-grained product "
        "taxonomy."
    )
    conf = _confidence_lines(version_df)
    return tech + "\n\n" + conf


def build_timeseries_metrics(version_df: pd.DataFrame) -> pd.DataFrame:
    """Compact key/value metrics for the timeseries_metrics sheet."""
    rows: list[tuple[str, str]] = []
    n = len(version_df)
    rows.append(("Total observation rows", str(n)))

    dt = parse_release_dates(version_df)
    mask = dt.notna()
    dated_n = int(mask.sum())
    rows.append(("Rows with parseable release_date", str(dated_n)))

    if dated_n == 0:
        rows.append(("Dated span", "n/a"))
        rows.append(("Note", "Re-run pipeline when sources return dates for cadence metrics."))
        return pd.DataFrame(rows, columns=["metric", "value"])

    sub = version_df.loc[mask].copy()
    sub["_dt"] = dt[mask]
    dmin, dmax = sub["_dt"].min(), sub["_dt"].max()
    rows.append(("Earliest observation date", dmin.date().isoformat()))
    rows.append(("Latest observation date", dmax.date().isoformat()))

    for plat in ["iOS", "Android"]:
        psub = sub[sub["platform"] == plat]
        rows.append((f"Dated rows ({plat})", str(len(psub))))
        if len(psub) > 0:
            yy = psub["_dt"].dt.year.value_counts().sort_index()
            peak_year = int(yy.idxmax())
            rows.append((f"{plat}: peak year by row count", f"{peak_year} ({int(yy.loc[peak_year])} rows)"))

    vc = sub.groupby(["platform", "source_type"]).size().reset_index(name="n")
    for _, r in vc.iterrows():
        rows.append((f"Dated rows — {r['platform']} / {r['source_type']}", str(int(r["n"]))))

    return pd.DataFrame(rows, columns=["metric", "value"])


def _metrics_block_for_summary(version_df: pd.DataFrame) -> str:
    """Tight, scannable key counts block (grouped)."""
    n = len(version_df)
    ios = version_df[version_df["platform"] == "iOS"]
    andf = version_df[version_df["platform"] == "Android"]
    n_i, n_a = len(ios), len(andf)

    dt = parse_release_dates(version_df)
    mask = dt.notna()
    if mask.any():
        dmin = dt[mask].min().date().isoformat()
        dmax = dt[mask].max().date().isoformat()
        date_cov = f"{dmin} \u2192 {dmax}"
    else:
        date_cov = "n/a"

    def pct_ver(frame: pd.DataFrame) -> float:
        if len(frame) == 0:
            return 0.0
        return 100.0 * float(frame["version_number"].fillna("").astype(str).str.strip().ne("").sum()) / float(len(frame))

    ver_line = f"{pct_ver(ios):.0f}% iOS / {pct_ver(andf):.0f}% Android"

    # Peak year (dated rows) with platform breakdown.
    peak_line = "n/a"
    if mask.any():
        sub = version_df.loc[mask].copy()
        sub["_dt"] = dt[mask]
        sub["year"] = sub["_dt"].dt.year.astype(int)
        yc = sub.groupby(["year", "platform"]).size().unstack(fill_value=0)
        if not yc.empty:
            yc["total"] = yc.sum(axis=1)
            peak_year = int(yc["total"].idxmax())
            peak_line = f"{peak_year} ({int(yc.loc[peak_year].get('iOS', 0))} iOS rows, {int(yc.loc[peak_year].get('Android', 0))} Android rows)"

    # Android source mix (all rows).
    src = (
        andf["source_type"]
        .fillna("")
        .astype(str)
        .value_counts()
    )
    src_lines = []
    for k in (
        "developer_changelog",
        "feature_signal",
        "wayback_snapshot",
        "play_store_snapshot",
        "apkmirror_cache",
        "review_inferred",
    ):
        if k in src.index:
            src_lines.append(f"  {k:<19} {int(src.loc[k])}")

    out = [
        f"Panel size:        {n} total rows ({n_i} iOS / {n_a} Android)",
        f"Date coverage:     {date_cov}",
        f"Version number:    {ver_line}",
        f"Peak year:         {peak_line}",
    ]
    if src_lines:
        out.append("")
        out.append("Android source mix:")
        out.extend(src_lines)
    return "\n".join(out)


def _timeseries_insights_lede(version_df: pd.DataFrame) -> str:
    """Lead with a concrete finding; keep it cautious and testable."""
    sub = dated_subset(version_df)
    if sub is None:
        return "No parseable release_date values — time-series findings below are unavailable for this export."

    s = sub.sort_values("_dt").reset_index(drop=True)
    q = len(s) // 4
    bug_pp = None
    if q >= 1:
        oldest = s.iloc[:q]
        newest = s.iloc[-q:]
        bug_pp = 100.0 * (cat_share(newest, "Bug fixes / performance improvements") - cat_share(oldest, "Bug fixes / performance improvements"))

    parts: list[str] = []
    if bug_pp is not None and abs(bug_pp) >= 3.0:
        parts.append(
            f"Most salient shift is a {bug_pp:+.0f} pp change toward Bug fixes / performance improvements in the newest quartile, "
            "consistent with platform maturity or strategic disclosure framing."
        )
    else:
        parts.append(
            "Category evolution is present but modest on headline buckets; interpret update_category as disclosure, not ground-truth engineering work."
        )

    return " ".join(parts)


def build_timeseries_insights(version_df: pd.DataFrame) -> str:
    """Research-oriented narrative from dated rows (shared core with viz_fast_scan synopsis)."""
    base = build_timeseries_insights_text(version_df)
    lede = _timeseries_insights_lede(version_df)
    return f"{lede}\n\n{base}"


def finance_hypothesis_block() -> str:
    """
    Concrete, finance-relevant hypothesis that turns descriptive trends into a testable claim.
    Keep it short (2–3 sentences) and explicitly caution against naive use of labels.
    """
    return (
        "Hypothesis (finance-relevant): shifts in update labeling over time (e.g., newer-period increases in "
        "Bug fixes / performance improvements or decreases in AI-related features) partly reflect "
        "strategic disclosure and compliance framing—especially around platform policy/regulatory events "
        "(GDPR, App Tracking Transparency, payment policy changes)—rather than purely underlying engineering work. "
        "For example, an increase in bug-fix framing post-2021 would be consistent with disclosure incentives around "
        "Apple’s App Tracking Transparency rollout.\n\n"
        "If true, the update_category series behaves like a noisy disclosure proxy: it should co-move with "
        "independent policy-event timelines and platform enforcement intensity, and any empirical model linking "
        "update labeling to firm outcomes should validate against those exogenous events and restrict to higher-confidence "
        "source_type rows."
    )


def recommended_analysis_subset_block() -> str:
    """
    Explicit defaults for empirical use vs robustness checks.
    This does not change the data; it only clarifies recommended filters.
    """
    return (
        "Recommended empirical subset (default):\n"
        "• Keep rows with source_type in {app_store_web, developer_changelog}.\n"
        "• Keep release_notes != \"Not available\" and release_date present when doing cadence/time-series models.\n"
        "• Optionally require confidence_level == high for strictest estimates.\n\n"
        "Robustness / sensitivity:\n"
        "• Add wayback_snapshot (medium) to test dependence on archive coverage.\n"
        "• Treat feature_signal as weak disclosure evidence; include only in robustness and report separately.\n"
        "• Exclude review_inferred unless you are explicitly studying missingness / inference bias."
    )


def validation_data_summary_block(validation_text: str, data_quality_text: str) -> str:
    """
    Compact, evaluator-facing discipline block. Keeps only high-signal lines.
    """
    def _pick(lines: list[str], keys: tuple[str, ...]) -> list[str]:
        out: list[str] = []
        for ln in lines:
            s = ln.strip()
            if not s or s.lower().endswith("_report"):
                continue
            if any(s.startswith(k) for k in keys):
                out.append(s)
        return out

    v_lines = [x for x in (validation_text or "").splitlines()]
    d_lines = [x for x in (data_quality_text or "").splitlines()]

    v_keep = _pick(
        v_lines,
        (
            "config_apps:",
            "app_master_rows:",
            "app_version_history_rows:",
            "ios_version_history_rows:",
            "android_version_history_rows:",
            "apps_with_ios_and_android_rows:",
        ),
    )
    d_keep = _pick(
        d_lines,
        (
            "pct_all_rows_app_store_web:",
            "pct_android_play_store_snapshot_rows:",
            "pct_android_wayback_snapshot_rows:",
            "pct_android_developer_changelog_rows:",
            "pct_android_feature_signal_rows:",
            "pct_android_review_inferred_rows:",
            "pct_android_missing_version_number:",
            "pct_all_rows_release_notes_not_available:",
        ),
    )

    out: list[str] = []
    if v_keep:
        out.append("Validation summary:")
        out.extend(f"• {x}" for x in v_keep)
    if d_keep:
        if out:
            out.append("")
        out.append("Data quality diagnostics:")
        out.extend(f"• {x}" for x in d_keep)
    return "\n".join(out) if out else "No validation/data quality text available."

def build_submission_summary_dataframe(
    version_df: pd.DataFrame,
    *,
    n_config_apps: int,
    repo_url: str,
    validation_text: str = "",
    data_quality_text: str = "",
) -> pd.DataFrame:
    dt = parse_release_dates(version_df)
    mask = dt.notna()
    if mask.any():
        dmin = dt[mask].min().date().isoformat()
        dmax = dt[mask].max().date().isoformat()
        span = f"{dmin}\u2013{dmax}"
    else:
        span = "n/a"
    overview = (
        f"Cross-platform panel covering {n_config_apps} matched iOS/Android app pairs, {len(version_df)} "
        f"version-history observations spanning {span}. Each row is tied to a verifiable source and confidence level, "
        "enabling evaluators to subset to high-credibility paths before drawing product or policy inferences."
    )
    rows = [
        ("Overview", overview),
        ("Key counts (auto)", _metrics_block_for_summary(version_df)),
        ("Data collection approach", methodology_block(repo_url)),
        ("Validation & data quality (auto)", validation_data_summary_block(validation_text, data_quality_text)),
        ("Recommended analysis subset (for empirical models)", recommended_analysis_subset_block()),
        ("Time-series insights (automated)", build_timeseries_insights(version_df)),
        ("Finance-relevant hypothesis (testable)", finance_hypothesis_block()),
        ("Challenges and limitations", challenges_block(version_df)),
    ]
    return pd.DataFrame(rows, columns=["Section", "Details"])


def _version_int_parts(s: str) -> list[int]:
    s = (s or "").strip()
    if not s:
        return []
    return [int(x) for x in re.findall(r"\d+", s)]


def _versions_equivalent_for_current(obs_ver: str, store_ver: str) -> bool:
    """Loose equality for marketing semver variants (e.g. 426 vs 426.0.0)."""
    obs_ver = (obs_ver or "").strip()
    store_ver = (store_ver or "").strip()
    if not obs_ver or not store_ver:
        return False
    if obs_ver.lower() == store_ver.lower():
        return True
    a, b = _version_int_parts(obs_ver), _version_int_parts(store_ver)
    if not a or not b:
        return False
    length = max(len(a), len(b))
    ta = a + [0] * (length - len(a))
    tb = b + [0] * (length - len(b))
    return ta == tb


def _is_current_cell(row: pd.Series) -> str:
    vn = row.get("version_number")
    cv = row.get("store_current_version")
    vn_s = "" if vn is None or (isinstance(vn, float) and pd.isna(vn)) else str(vn).strip()
    cv_s = "" if cv is None or (isinstance(cv, float) and pd.isna(cv)) else str(cv).strip()
    if not vn_s:
        return "Unknown"
    if not cv_s or cv_s.lower() == "varies with device":
        return "Unknown"
    return "Yes" if _versions_equivalent_for_current(vn_s, cv_s) else "No"


def _observation_notes(row: pd.Series) -> str:
    """DQ-only notes (pipe-separated). Does not duplicate release_notes or source_type columns."""
    parts: list[str] = []

    vn = row.get("version_number")
    vn_s = "" if vn is None or (isinstance(vn, float) and pd.isna(vn)) else str(vn).strip()
    if not vn_s:
        parts.append("version_number missing (not fabricated)")

    cv = row.get("store_current_version")
    cv_s = "" if cv is None or (isinstance(cv, float) and pd.isna(cv)) else str(cv).strip()
    if cv_s.lower() == "varies with device":
        parts.append("Play Store serves device-dependent variants")

    st = str(row.get("source_type") or "").strip()
    if st == "wayback_snapshot":
        parts.append("archive date; not verified ship date")

    dq = str(row.get("_dq_pipeline_note") or "").strip()
    if dq:
        parts.append(dq)

    return " | ".join(parts)


def _standardized_update_summary(row: pd.Series) -> str:
    """
    Format: {short_code}:{primary_descriptor}
    Examples: bugfix:stability, ai:search_feature, feature:darkmode, privacy:att_compliance, no_release_notes
    """
    cat = str(row.get("update_category") or "").strip()
    rn = str(row.get("release_notes") or "").strip()
    if not rn or rn == "Not available":
        return "no_release_notes"

    cat_l = cat.lower()
    code = "other"
    if "bug fixes" in cat_l or "performance" in cat_l:
        code = "bugfix"
    elif cat_l.startswith("ui"):
        code = "ui"
    elif "creator tools" in cat_l or "content" in cat_l:
        code = "content"
    elif "localization" in cat_l or "languages" in cat_l:
        code = "localization"
    elif "enterprise" in cat_l or "admin" in cat_l:
        code = "enterprise"
    elif cat_l.startswith("privacy"):
        code = "privacy"
    elif cat_l.startswith("ai"):
        code = "ai"
    elif cat_l.startswith("payments"):
        code = "payments"
    elif cat_l.startswith("personalization"):
        code = "recs"
    elif cat_l.startswith("security"):
        code = "security"
    elif cat_l.startswith("sdk"):
        code = "sdk"
    elif cat_l.startswith("new product feature"):
        code = "feature"

    # One best descriptor from release_notes (single string).
    desc_rules: list[tuple[str, re.Pattern[str]]] = [
        # Privacy / compliance
        ("att_compliance", re.compile(r"\batt\b|app tracking transparency|tracking transparency", re.I)),
        ("gdpr_compliance", re.compile(r"\bgdpr\b", re.I)),
        ("tracking_controls", re.compile(r"\btracking\b|track(?:er|ers)?", re.I)),
        ("privacy_policy", re.compile(r"privacy policy|data policy|privacy update|privacy changes?", re.I)),
        # Security
        ("two_factor_auth", re.compile(r"\b2fa\b|two-factor|two factor", re.I)),
        ("password_security", re.compile(r"\bpassword\b", re.I)),
        ("login_auth", re.compile(r"\blogin\b|\bauth\b|authentication", re.I)),
        ("fraud_protection", re.compile(r"\bfraud\b|phishing|scam", re.I)),
        # AI
        ("agent_feature", re.compile(r"\bagents?\b", re.I)),
        ("gpt_feature", re.compile(r"\bgpt\b|chatgpt|openai", re.I)),
        ("genai_feature", re.compile(r"\bgenerative\b|\bllm\b", re.I)),
        ("ai_search", re.compile(r"\bai\b.*\bsearch\b|\bsearch\b.*\bai\b", re.I)),
        # Payments
        ("cashback_offers", re.compile(r"cash ?back|rewards?|offers?", re.I)),
        ("subscription", re.compile(r"subscribe|subscription|renewal|trial", re.I)),
        ("checkout_payment", re.compile(r"checkout|purchase|billing|wallet|paywall|premium", re.I)),
        # Performance / bugfix
        ("stability", re.compile(r"\bstability\b|reliab", re.I)),
        ("crash_fix", re.compile(r"\bcrash\b", re.I)),
        ("performance", re.compile(r"\bperformance\b|faster|speed|latency|optimized?", re.I)),
        ("playback", re.compile(r"\bplayback\b|\bplayer\b", re.I)),
        ("bug_fix", re.compile(r"\bbug\b|\bfix(?:ed|es|ing)?\b", re.I)),
        # UI
        ("darkmode", re.compile(r"dark mode|dark theme", re.I)),
        ("gallery_ui", re.compile(r"\bgallery\b", re.I)),
        ("navigation_ui", re.compile(r"\bnavigation\b|tabs?\b|sidebar\b|home\b", re.I)),
        ("design_refresh", re.compile(r"\bdesign\b|\blayout\b|\binterface\b|\bui\b", re.I)),
        # Content creation
        ("stickers", re.compile(r"\bstickers?\b", re.I)),
        ("filters_effects", re.compile(r"\bfilters?\b|\beffects?\b|\blenses?\b", re.I)),
        ("video_editing", re.compile(r"\bedit(?:ing)?\b|editor|shoot videos?|camera", re.I)),
        ("sharing_comments", re.compile(r"\bshare\b|comments?\b|tag (?:your )?friends?\b", re.I)),
        # Enterprise/admin
        ("admin_controls", re.compile(r"controls? for admins?|workspace admin|admin", re.I)),
        ("permissions_roles", re.compile(r"permissions?|roles?", re.I)),
        ("sso_scim", re.compile(r"\bsso\b|\bscim\b", re.I)),
        # Localization
        ("new_languages", re.compile(r"new languages?|in \d+ new languages?", re.I)),
        ("translation", re.compile(r"translated|translation|locali[sz]ation|i18n", re.I)),
        # Product features
        ("automations", re.compile(r"automations?|automate workflows?", re.I)),
        ("mail_calendar", re.compile(r"\bmail\b|\bcalendar\b|schedule meetings", re.I)),
        ("forms", re.compile(r"\bforms?\b", re.I)),
        ("tables", re.compile(r"\btables?\b|simple tables", re.I)),
        ("teamspaces", re.compile(r"\bteamspaces?\b", re.I)),
        ("progress_bars", re.compile(r"\bprogress bars?\b", re.I)),
        ("integrations", re.compile(r"\bintegration\b|slack|salesforce|asana|github|jira|zapier", re.I)),
        ("new_feature", re.compile(r"\bnew feature\b|introducing|now you can|added support|launch", re.I)),
    ]

    descriptor = "other"
    for d, p in desc_rules:
        if p.search(rn):
            descriptor = d
            break

    return f"{code}:{descriptor}"


SUBMISSION_OBSERVATION_COLUMN_ORDER: tuple[str, ...] = (
    "app_id",
    "app_name",
    "platform",
    "developer",
    "category",
    "initial_release_date",
    "version_number",
    "release_date",
    "is_current_version",
    "store_current_version",
    "store_current_version_release_date",
    "history_source_url",
    "release_notes",
    "update_category",
    "update_summary",
    "source_type",
    "confidence_level",
    "notes",
)


def _submission_observations_history_source_url(row: pd.Series) -> str:
    """
    One rubric URL per row: prefer pipeline ``history_source_url`` (Wayback, feed item,
    APKMirror release page, etc.) when https; otherwise fall back to store listing.
    """
    h = str(row.get("history_source_url") or "").strip()
    if h.startswith(("http://", "https://")):
        return h
    l = str(row.get("listing_source_url") or "").strip()
    if l.startswith(("http://", "https://")):
        return l
    return h or l


def build_submission_observations(version_df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    """
    One row per app-platform-version observation with listing metadata joined from ``app_master``.
    Suitable for assignment grading without manual VLOOKUP.
    """
    mcols = [
        "app_id",
        "developer",
        "category",
        "initial_release_date",
        "source_url",
        "current_version",
        "current_version_release_date",
    ]
    merged = version_df.merge(master_df[mcols], on="app_id", how="left", validate="many_to_one")
    merged = merged.rename(
        columns={
            "source_url": "listing_source_url",
            "current_version": "store_current_version",
            "current_version_release_date": "store_current_version_release_date",
        }
    )
    merged["is_current_version"] = merged.apply(_is_current_cell, axis=1)
    merged["notes"] = merged.apply(_observation_notes, axis=1)
    merged["update_summary"] = merged.apply(_standardized_update_summary, axis=1)
    merged["history_source_url"] = merged.apply(_submission_observations_history_source_url, axis=1)
    if "_dq_pipeline_note" in merged.columns:
        merged = merged.drop(columns=["_dq_pipeline_note"])
    return merged[list(SUBMISSION_OBSERVATION_COLUMN_ORDER)]


def build_cover_sheet_dataframe(repo_url: str, *, n_apps: int, n_obs: int) -> pd.DataFrame:
    """Evaluator-facing guide: sheet map + rubric column mapping + repo link."""
    repo_line = (
        repo_url
        if repo_url
        else "Set config/project_meta.json » repository_url (or env SUBMISSION_GITHUB_REPO_URL)."
    )
    sheets = (
        "COVER — this guide.\n\n"
        "submission_observations — primary submission table (denormalized). One row per observation; aligns with "
        "the assignment rubric columns.\n\n"
        "app_master — per-platform listing snapshot (developer, category, initial release, store current version, "
        "listing URL; optional notes when the store scrape omits initial_release_date).\n\n"
        "app_version_history — same observations as submission_observations without duplicated master fields; "
        "useful for joins or audits.\n\n"
        "submission_summary — methodology, automated time-series commentary, quick-scan dashboard bullets "
        "(density / cadence / provenance / quartile trends), challenges + confidence interpretation.\n\n"
        "timeseries_metrics — key counts derived from version history.\n\n"
        "viz_fast_scan — synopsis bullets mirroring Time-series insights; monthly cadence heatmaps (iOS/Android); "
        "URL-class profile from history_source_url (Wayback vs APKMirror vs store listing); observation depth by "
        "app/platform (dated span); quartile category-share slope chart — requires matplotlib.\n\n"
        "validation / data_quality / field_schema — pipeline diagnostics and schema reference."
    )
    mapping = (
        "App name → app_name\n"
        "Platform → platform\n"
        "Developer / company → developer\n"
        "App category → category\n"
        "Version number → version_number\n"
        "Version release/update date → release_date\n"
        "Whether this is the current version → is_current_version (Yes | No | Unknown)\n"
        "Initial app release date → initial_release_date\n"
        "Update description / release notes → release_notes\n"
        "Standardized update category → update_category (single best-fit label per rubric; multi-signal updates default to primary cue)\n"
        "Brief standardized summary for variables → update_summary (short_code:primary_descriptor; plain text for coding)\n"
        "Source of update history (clickable URL in Excel) → history_source_url "
        "(Wayback capture, feed/APKMirror permalink when present for that row; else store listing URL)\n"
        "Listing snapshot: store-reported current version + date → store_current_version, "
        "store_current_version_release_date\n"
        "Observation provenance → source_type, confidence_level\n"
        "Data quality flags only (pipe-separated) → notes"
    )
    join_txt = (
        "submission_observations is produced by merging app_version_history with app_master on app_id (stable per "
        "app_name + platform). No additional manual join is required for grading."
    )
    caveats = (
        "Android observation rows may lack version_number or use archive-backed dates (see source_type). "
        "submission_observations » notes (pipe-separated): blank version → not fabricated; wayback_snapshot → archive "
        "date caveat; Varies with device listing → Play variant caveat; plus contamination/anomaly text when present."
    )
    rows = [
        ("Title", "Matched iOS / Android app update history — submission workbook"),
        ("Panel size", f"{n_apps} apps × 2 platforms; {n_obs} version-history observations in this export."),
        ("Start here", "Filter and analyze submission_observations first; read submission_summary for methodology."),
        ("Sheet guide", sheets),
        ("Rubric → columns (submission_observations)", mapping),
        ("Join logic", join_txt),
        ("Interpretation caveats", caveats),
        ("Code repository", repo_line),
    ]
    return pd.DataFrame(rows, columns=["Section", "Details"])


def apply_submission_sheet_style(path: Path, sheet_names: tuple[str, ...]) -> None:
    """Fixed column widths (``ColumnDimension.width`` only), wrap column B, top alignment; clear fixed row heights."""
    try:
        from openpyxl import load_workbook
        from openpyxl.styles import Alignment, Font
    except ImportError:
        return

    wb = load_workbook(path)
    bold_header = Font(bold=True)
    align_top_left = Alignment(
        horizontal="left",
        vertical="top",
        wrap_text=False,
        shrink_to_fit=False,
    )
    align_top_wrap_b = Alignment(
        horizontal="left",
        vertical="top",
        wrap_text=True,
        shrink_to_fit=False,
    )

    def _lock_col_width(dim, width: float) -> None:
        dim.width = width

    def _clear_fixed_row_heights(worksheet) -> None:
        for rd in worksheet.row_dimensions.values():
            rd.height = None

    for name in sheet_names:
        if name not in wb.sheetnames:
            continue
        ws = wb[name]
        if name in ("COVER", "submission_summary"):
            _lock_col_width(ws.column_dimensions["A"], 28)
            _lock_col_width(ws.column_dimensions["B"], 80)
            _clear_fixed_row_heights(ws)
            for r_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=ws.max_row), start=1):
                for cell in row:
                    col_idx = cell.column
                    if col_idx == 2:
                        cell.alignment = align_top_wrap_b
                    else:
                        cell.alignment = align_top_left
                    if r_idx == 1 or col_idx == 1:
                        cell.font = bold_header
        elif name == "submission_observations":
            ws.freeze_panes = "A2"
        elif name == "timeseries_metrics":
            _lock_col_width(ws.column_dimensions["A"], 52)
            _lock_col_width(ws.column_dimensions["B"], 36)
            _clear_fixed_row_heights(ws)
            for r_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=ws.max_row), start=1):
                for cell in row:
                    cell.alignment = Alignment(
                        horizontal="left",
                        vertical="top",
                        wrap_text=True,
                        shrink_to_fit=False,
                    )
                    if r_idx == 1:
                        cell.font = bold_header

    wb.save(path)
