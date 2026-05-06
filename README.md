# Research_Project

Normalized **`app_master`** + **`app_version_history`** with explicit **provenance** (`source_type`, `confidence_level`). No fabricated version numbers.

## Commands (quick reference)

| Goal | Command | Notes |
|------|---------|--------|
| **FULL REFRESH** — scrape stores/archives, rewrite cached CSVs + full workbook | `python scripts/run_pipeline.py` | Slow (often tens of minutes). Run after changing apps in `config/apps.json` or when data must be fresh. |
| **REBUILD REPORTS** — rebuild Excel + derived CSV/text from existing `app_master` / `app_version_history` CSVs | `python scripts/build_workbook_only.py` | Fast (seconds). Use after editing `submission_summary.py`, COVER text, or merge logic — **no** network scrape. Requires prior full refresh CSVs. |
| **FORMAT ONLY** — restyle COVER + submission_summary columns/wrap | `python scripts/reformat_workbook.py` | Fast. Does not change cell contents. |

Optional **Make** shortcuts (from repo root, if `make` is installed): `make full`, `make reports`, `make format`, `make help`.

Install **`matplotlib`** for the **`viz_fast_scan`** worksheet (`pip install -r requirements.txt`). If Matplotlib is missing, the workbook still builds; that sheet is skipped with a stderr warning.

## iOS

- **`app_master`:** iTunes Lookup (metadata only).
- **`app_version_history`:** App Store **web** HTML — embedded `versionHistory` JSON (`source_type` = **`app_store_web`**, **`high`** confidence). If the embed is missing, one **Lookup** fallback row (**`medium`**).

## Android (hierarchical)

Implemented in **`scripts/android_hierarchical.py`**:

1. **`play_store_snapshot` (`high`)** — Live Play detail page: `google-play-scraper` transport (`get` + `app()` metadata) + **heuristic** extraction of changelog-like strings from embedded `AF_initDataCallback` JSON (not review text). Listing `version` / `updated` / `lastUpdatedOn` when present. **Never** treats reviews as primary.
2. **`wayback_snapshot` (`medium`)** — [Wayback CDX](https://web.archive.org/) for the same `play.google.com/store/apps/details?...` URL, then archived HTML parsed with the **same** heuristic (capped fetches per app). `version_number` is left **empty** when the archive page does not expose a reliable semver (no fabrication).
3. **Feed ingest (`scripts/feed_validator.py` + `android_hierarchical.py`)** — Optional **`android_changelog_feed_url`**: the validator samples up to **five** recent entries and classifies the feed as **`release_feed`**, **`product_blog`**, or **`reject`** (counts: version-like text, timestamps, changelog-style wording, explicit in-text dates). **`developer_changelog` (`high` / `medium`)** rows are emitted **only** when `feed_type == release_feed` **and** the item contains a **semver** or an **explicit calendar date in the title/summary** (not RSS `pubDate` alone). Other parsed items (and all `product_blog` items) become **`feature_signal` (`low`)** with **`version_number` forced empty** so they do not masquerade as releases. **`reject`** yields no rows. Summary is written to **`output/feed_validation_report.txt`**. Only **`developer_changelog`** rows count as structured signal for step 4.
4. **`review_inferred` (`low`)** — **Only if** there is **no** structured signal from steps 1–3 (including no strict **`developer_changelog`** rows from the feed). Then bounded `reviews()` cohort by `appVersion` + earliest review date; **`release_notes`** = `Not available`.

### Android: research-grade feed discovery

Prefer URLs that are **official**, **machine-readable**, and **product- or release-scoped** (less noise than a general marketing blog).

Common patterns to try (verify in a browser or with `curl` / `feedparser` before adding to config):

| Pattern | Example shape |
|--------|------------------|
| Corporate blog RSS | `https://example.com/blog/rss` or `/blog/feed/` |
| Releases / “what’s new” RSS | `https://example.com/releases.xml`, `/releases/rss.xml` |
| GitHub Releases (open-source apps) | `https://github.com/org/repo/releases.atom` |
| Paths worth probing | `/changelog`, `/release-notes`, `/updates` (often HTML only; look for `<link rel="alternate" type="application/rss+xml" …>` in page source) |

**Tip:** On many sites the feed URL is not linked in the footer; search the HTML of the blog or releases index for `application/rss+xml` or `application/atom+xml`.

**Caveat:** A broad company blog RSS mixes product, culture, and SEO posts—still usable with your `update_category` rules, but a **dedicated releases** feed (when it exists) is stronger provenance for version-adjacent research.

## Outputs (`output/`)

| File | Description |
|------|-------------|
| `app_master.csv` / `app_version_history.csv` | Core normalized tables |
| `submission_observations.csv` | Rubric-ready merge (master fields + each version row + `is_current_version`, `notes`) |
| `data_quality_report.txt` | `%` by Android `source_type`, missing Android `version_number` rate, etc. |
| `feed_validation_report.txt` | Per configured Android feed: auto `feed_type` + signal counts from the sample |
| `validation_report.txt` | Row counts + embedded data quality block |
| `normalized_dataset.xlsx` | Sheets: **`COVER`**, **`submission_observations`**, `app_master`, `app_version_history`, `validation`, **`data_quality`**, `field_schema`, **`timeseries_metrics`**, **`submission_summary`**, **`viz_fast_scan`** (synopsis + charts aligned with automated Time-series insights; needs `matplotlib`) |
| `schema_tables.txt` | Table definitions |

## Run

### 1. Fetch data (slow — networks / Wayback)

```bash
pip install -r requirements.txt
python scripts/run_pipeline.py
```

Writes **`output/app_master.csv`** and **`output/app_version_history.csv`** plus Excel/text exports.

Wayback calls are rate-friendly (small cap per app); CDX/network failures degrade to fewer `wayback_snapshot` rows without failing the run.

### 2. Rebuild workbook only (fast — no scrape)

After the CSVs exist, regenerate **`submission_observations`**, **`normalized_dataset.xlsx`**, reports, and summary sheets whenever you change `submission_summary.py` or Cover copy:

```bash
python scripts/build_workbook_only.py
```

Does **not** overwrite **`feed_validation_report.txt`** from the last full scrape if it already exists; leaves **`app_master.csv`** / **`app_version_history.csv`** untouched.

### 3. Excel formatting only

```bash
python scripts/reformat_workbook.py
```

Close open `output/*.csv` / `.xlsx` on Windows before re-running pipelines or workbook rebuilds.
