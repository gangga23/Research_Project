"""
Sanitize user-review-like text mistakenly captured as release_notes on Android Wayback snapshots.

Edits the cached CSV outputs (no network):
- output/app_version_history.csv
- output/submission_observations.csv

Rule: only rows with source_type == "wayback_snapshot" and release_notes that look like user reviews.
Action:
- release_notes -> "Not available"
- has_release_notes -> False
- submission_observations.notes -> prefix DQ note (keeps existing notes)
"""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output"

DQ_NOTE = "Unverified user review / marketing copy (not official release notes); excluded from analysis."

REVIEW_LIKE = re.compile(
    r"\b(i\s+love|i\s+hate|please\s+fix|doesn'?t\s+work|crash(?:es|ing)?|"
    r"wifi|pen\s*test|scam(?:mer|my)?|shady|track(?:ing)?|location|"
    r"dm|dms|nude|traffick|podcast|interface|user\s*friendly|"
    r"shuffle|playlist|song|premium|ads?\b|ban(?:ned)?|appeal|stars?\b|rating\b)\b",
    re.I,
)
# Guardrail to avoid sanitizing genuine changelog snippets.
# Note: do NOT match generic "release" because reviews often say "new release".
CHANGELOG_LIKE = re.compile(r"\bwhat'?s\s+new\b|\bbug\s*fix(?:es)?\b|\bimprov(?:e|ement|ements)?\b|\bstability\b", re.I)
REVIEW_PUNCT = re.compile(r"[!?]{2,}|[\U0001F300-\U0001FAFF]")  # repeated punctuation or emojis
APP_UX = re.compile(
    r"\b(app|android|ios|update|interface|ui|feature|broken|workaround|keyboard|"
    r"performance|sluggish|table|enter|column|dm|refund|driver|restaurant|order)\b",
    re.I,
)
MARKETING_LIKE = re.compile(
    r"(?:\bwanna\b|exactly what you need|efficient and simple|any issues|mail us|"
    r"free\s+note|notepad\s+free|sticky\s+notes\s+widget|sync\s+(?:and|&)\s*backup|"
    r"export\s+to\s+pdf|customize|\btheme\b|color\s+note|locked\s+notes|private\s+notes)",
    re.I,
)


def looks_like_user_review(text: str) -> bool:
    s = (text or "").strip()
    if not s or s == "Not available":
        return False
    # Promotional / app-description copy (not release notes). Check before changelog guard:
    # marketing text often contains words like "improve" or "stability" in feature lists.
    if len(s) >= 260 and MARKETING_LIKE.search(s):
        return True
    if CHANGELOG_LIKE.search(s):
        return False
    # First-person complaint prose (common in Play listing snapshots).
    pronouns = len(re.findall(r"\b(i|i'm|ive|i've|my|me)\b", s, flags=re.I))
    if len(s) >= 180 and pronouns >= 3:
        return True
    # Very long multi-paragraph prose about UX/issues is almost certainly a review.
    if len(s) >= 420 and (pronouns >= 2 or APP_UX.search(s)):
        return True
    # Medium-length first-person UX complaint (common review format).
    if len(s) >= 200 and pronouns >= 1 and APP_UX.search(s):
        return True
    # Emoji / rant punctuation is a strong review marker.
    if len(s) >= 160 and REVIEW_PUNCT.search(s) and (pronouns >= 1 or APP_UX.search(s)):
        return True
    if len(s) >= 220 and REVIEW_LIKE.search(s):
        return True
    if REVIEW_LIKE.search(s):
        return True
    return False


def sanitize_app_version_history(path: Path) -> int:
    df = pd.read_csv(path)
    if df.empty or "source_type" not in df.columns or "release_notes" not in df.columns:
        return 0
    mask = (
        df["source_type"].astype(str).eq("wayback_snapshot")
        & df["release_notes"].fillna("").astype(str).apply(looks_like_user_review)
    )
    n = int(mask.sum())
    if not n:
        return 0
    df.loc[mask, "release_notes"] = "Not available"
    if "has_release_notes" in df.columns:
        df.loc[mask, "has_release_notes"] = False
    df.to_csv(path, index=False, encoding="utf-8")
    return n


def sanitize_submission_observations(path: Path) -> int:
    df = pd.read_csv(path)
    if df.empty or "source_type" not in df.columns or "release_notes" not in df.columns:
        return 0
    mask = (
        df["source_type"].astype(str).eq("wayback_snapshot")
        & df["release_notes"].fillna("").astype(str).apply(looks_like_user_review)
    )
    n = int(mask.sum())
    if not n:
        return 0
    df.loc[mask, "release_notes"] = "Not available"
    if "has_release_notes" in df.columns:
        df.loc[mask, "has_release_notes"] = False
    if "notes" in df.columns:
        prior = df.loc[mask, "notes"].fillna("").astype(str)
        # Prefix once; avoid duplicating the DQ note.
        df.loc[mask, "notes"] = prior.apply(
            lambda s: s
            if s.strip().startswith(DQ_NOTE)
            else (DQ_NOTE + (" | " + s.strip() if s.strip() else ""))
        )
    df.to_csv(path, index=False, encoding="utf-8")
    return n


def main() -> int:
    avh = OUT / "app_version_history.csv"
    sub = OUT / "submission_observations.csv"
    n1 = sanitize_app_version_history(avh) if avh.is_file() else 0
    n2 = sanitize_submission_observations(sub) if sub.is_file() else 0
    print(f"[sanitize_wayback_release_notes] app_version_history sanitized: {n1}")
    print(f"[sanitize_wayback_release_notes] submission_observations sanitized: {n2}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

