# Research_Project

Normalized **`app_master`** + **`app_version_history`** with explicit **provenance** (`source_type`, `confidence_level`). No fabricated version numbers.

## Commands (quick reference)

| Goal | Command | Notes |
|------|---------|--------|
| **FULL REFRESH** — scrape stores/archives, rewrite cached CSVs + full workbook | `python scripts/run_pipeline.py` | Slow (often tens of minutes). Run after changing apps in `config/apps.json` or when data must be fresh. |
| **APKMIRROR LISTINGS** (optional) — refresh `data/cache/apkmirror_*.csv` | `python scripts/apkmirror_scraper.py --scrape` | Run before **`run_pipeline.py`** when you want mirror-derived **`apkmirror_cache`** rows; respects **`data/cache/apkmirror_status.json`** (skip complete/blocked). |
| **REBUILD REPORTS** — rebuild Excel + derived CSV/text from existing `app_master` / `app_version_history` CSVs | `python scripts/build_workbook_only.py` | Fast (seconds). Use after editing `submission_summary.py` or merge logic — **no** network scrape. Requires prior full refresh CSVs. |
| **FORMAT ONLY** — restyle `submission_summary` columns/wrap | `python scripts/reformat_workbook.py` | Fast. Does not change cell contents. |

Optional **Make** shortcuts (from repo root, if `make` is installed): `make full`, `make reports`, `make format`, `make help`.

Install **`matplotlib`** for the **`viz_fast_scan`** worksheet (`pip install -r requirements.txt`). If Matplotlib is missing, the workbook still builds; that sheet is skipped with a stderr warning.

## iOS

- **`app_master`:** iTunes Lookup (metadata only).
- **`app_version_history`:** App Store **web** HTML — embedded `versionHistory` JSON (`source_type` = **`app_store_web`**, **`high`** confidence). If the embed is missing, one **Lookup** fallback row (**`medium`**).

## Android (hierarchical)

Implemented in **`scripts/android_hierarchical.py`**:

1. **`play_store_snapshot` (`high`)** — Live Play detail page: `google-play-scraper` transport (`get` + `app()` metadata) + **heuristic** extraction of changelog-like strings from embedded `AF_initDataCallback` JSON (not review text). Listing `version` / `updated` / `lastUpdatedOn` when present. **Never** treats reviews as primary.
2. **`wayback_snapshot` (`medium`)** — [Wayback CDX](https://web.archive.org/) for the same `play.google.com/store/apps/details?...` URL, then archived HTML parsed with the **same** heuristic (capped fetches per app). `version_number` is left **empty** when the archive page does not expose a reliable semver (no fabrication).
3. **Feed ingest (`scripts/feed_validator.py` + `android_hierarchical.py`)** — Optional **`android_changelog_feed_url`**: the validator samples up to **five** recent entries and classifies the feed as **`release_feed`**, **`product_blog`**, or **`reject`** (counts: version-like text, timestamps, changelog-style wording, explicit in-text dates). **`developer_changelog` (`high` / `medium`)** rows are emitted **only** when `feed_type == release_feed` **and** the item contains a **semver** or an **explicit calendar date in the title/summary** (not RSS `pubDate` alone). Other parsed items (and all `product_blog` items) become **`feature_signal` (`low`)** with **`version_number` forced empty** so they do not masquerade as releases. **`reject`** yields no rows. Summary is written to **`output/feed_validation_report.txt`**. Only **`developer_changelog`** rows count as structured signal from the feed when deciding whether **`review_inferred`** is needed (together with steps 1–2 and step 4).
4. **APKMirror listing CSV (`scripts/apkmirror_scraper.py`)** — Optional offline-first refresh: run **`python scripts/apkmirror_scraper.py --scrape`** to populate **`data/cache/apkmirror_{app_id}.csv`** per configured Android app (see **`config/apps.json`**: `apkmirror_slug`, `apkmirror_category`; progress in **`data/cache/apkmirror_status.json`**). During **`run_pipeline.py`**, if a CSV exists with usable rows (APKMirror **`/apk/`** URL per version), those observations are ingested as **`apkmirror_cache` (`low`)** with **`release_notes`** typically **`Not available`**. **`release_date`** prefers the listing CSV **Uploaded** field when parsed; otherwise the pipeline may resolve **Uploaded** from the release detail page via **`scripts/apkmirror_upload_date.py`** (cached under **`data/cache/apkmirror_upload_dates.json`**). Bulk detail fetch is often throttled (**HTTP 403**); listing scrape + pipeline is usually higher ROI than hammering detail URLs. Optional CSV-only repair pass: **`scripts/backfill_apkmirror_dates_only.py`** with **`APKMIRROR_UPLOAD_FETCH_MAX`** (use **`0`** for offline workbook rebuilds). **`apkmirror_cache`** counts as structured coverage so **`review_inferred`** is skipped when mirror rows exist together with steps 1–3 as implemented in **`scripts/android_hierarchical.py`**.
5. **`review_inferred` (`low`)** — **Only if** steps **1–4** together provide **no** usable structured signal (Play/Wayback/feed **`developer_changelog`** path **and** no qualifying APKMirror CSV rows). Then bounded `reviews()` cohort by `appVersion` + earliest review date; **`release_notes`** = `Not available`.

## Manual overrides / provenance exceptions

- **PayPal Android `initial_release_date`**: when the Play Store scraper does not expose a reliable “released” date, we set a **latest-by bound** using PayPal’s corporate press release date (**2010-10-26**) and store the provenance in `app_index.source_url` (press release URL) with a plain-text note in `app_index.notes`. This ensures the override is reproducible and auditable across pipeline runs.

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
| `submission_observations.csv` | Rubric-ready merge (master fields + each version row + `is_current_version`, `has_release_notes`, `notes`, `update_summary`) |
| `data_quality_report.txt` | `%` by Android `source_type`, missing Android `version_number`, parseable `release_date` rates (all platforms + APKMirror subset), `release_notes` availability, etc. |
| `feed_validation_report.txt` | Per configured Android feed: auto `feed_type` + signal counts from the sample |
| `validation_report.txt` | Row counts + embedded data quality block |
| `normalized_dataset.xlsx` | Sheets include **`submission_observations`**, `app_master`, **`submission_summary`**, **`viz_fast_scan`** (synopsis + charts; needs `matplotlib`). Post-processing styling may touch additional tabs when present. |
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

After the CSVs exist, regenerate **`submission_observations`**, **`normalized_dataset.xlsx`**, reports, and summary sheets whenever you change `submission_summary.py` or merge/export logic:

```bash
python scripts/build_workbook_only.py
```

Does **not** overwrite **`feed_validation_report.txt`** from the last full scrape if it already exists; leaves **`app_master.csv`** / **`app_version_history.csv`** untouched.

### 3. Excel formatting only

```bash
python scripts/reformat_workbook.py
```

Close open `output/*.csv` / `.xlsx` on Windows before re-running pipelines or workbook rebuilds.
