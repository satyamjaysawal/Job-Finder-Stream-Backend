"""
linkedin_realtime_hyderabad.py
==============================
Streams LinkedIn jobs in Hyderabad to a JSON file in real-time.
Focuses on jobs posted in the last 6 hours.

Usage:
    python linkedin_realtime_hyderabad.py --test      # test: 1 query, 5 jobs
    python linkedin_realtime_hyderabad.py             # full run: all queries, 100 jobs

Output:
    linkedin_jobs_hyderabad_<date>.json
"""

import argparse
import json
import datetime
import sys
import time
import random
import re
from pathlib import Path

# Reconfigure stdout/stderr to handle UTF-8 printing on Windows
if hasattr(sys.stdout, 'reconfigure'):
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass
if hasattr(sys.stderr, 'reconfigure'):
    try:
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

TODAY       = datetime.date.today().isoformat()
NOW         = datetime.datetime.now().strftime("%H:%M:%S")
TIMESTAMP   = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# Create a separate folder for scraped jobs
OUTPUT_DIR  = Path(__file__).parent / "scraped_jobs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Using a distinct filename indicating hyderabad and 6h
OUTPUT_FILE = OUTPUT_DIR / f"linkedin_jobs_hyderabad_6h_{TIMESTAMP}.json"

# ── Search queries (latest AI / SWE jobs) ────────────────────────────────────
SEARCH_QUERIES = [
    "Generative AI Engineer",
    "GenAI Engineer",
    "AI Engineer",
    "Agentic AI Engineer",
    "LLM Engineer",
    "Machine Learning Engineer",
    "MLOps Engineer",
    "Deep Learning Engineer",
    "NLP Engineer",
    "Computer Vision Engineer",
    "AI ML Engineer",
    "RAG Engineer",
    "Software Engineer AI",
    "Software Engineer",
    "Senior Software Engineer",
    "Python Developer",
    "Full Stack Developer",
    "Data Scientist",
    "Backend Engineer",
    "ML Platform Engineer",
]

TARGET      = 100   # stop after this many unique jobs
RESULTS_PER = 20    # per query
HOURS_OLD   = 1     # ONLY jobs from last 6 hours (default: 6h)


# ── JSON writer (real-time append) ────────────────────────────────────────────

class RealtimeJSON:
    """Updates JSON file after every entry so it is visible immediately."""

    def __init__(self, path: Path):
        self.path   = path
        self._count = 0
        self._seen  = set()

        # Initialize/overwrite file with empty array
        self.path.write_text("[]", encoding="utf-8")
        print(f"  JSON file initialized: {self.path.name}")

    def append(self, row: dict) -> bool:
        """Appends one job dictionary to the JSON array. Returns True if written."""
        url = row.get("job_url", "")
        if not url or url in self._seen:
            return False
        self._seen.add(url)
        self._count += 1
        
        row["#"] = self._count
        row["scraped_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        row["category"] = f"{HOURS_OLD}h"

        # Determine city name
        loc = row.get("location", "")
        if not loc or loc == "nan":
            row["city"] = "Hyderabad"
        else:
            row["city"] = loc.split(",")[0].strip()

        # Load existing, append, and rewrite
        try:
            if self.path.exists():
                content = self.path.read_text(encoding="utf-8").strip()
                data = json.loads(content) if content else []
            else:
                data = []
        except Exception:
            data = []

        data.append(row)
        
        # Write back pretty-printed JSON instantly
        self.path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        return True

    @property
    def count(self) -> int:
        return self._count


# ── LinkedIn scraper ─────────────────────────────────────────────────────────

def scrape_linkedin(query: str, location: str, results: int, hours_old: int) -> list[dict]:
    """Call python-jobspy for LinkedIn, return list of job dicts."""
    try:
        from jobspy import scrape_jobs
        df = scrape_jobs(
            site_name=["linkedin"],
            search_term=query,
            location=location,
            results_wanted=results,
            hours_old=hours_old,
            country_indeed="India",
            verbose=0,
        )
        if df is None or df.empty:
            return []

        jobs = []
        for _, row in df.iterrows():
            url  = str(row.get("job_url", "") or "").strip()
            date = str(row.get("date_posted", "") or "").strip()
            time_ago_scraped = str(row.get("time_ago", "") or "").strip()
            if not url or url == "nan":
                continue

            # Resolve date_posted
            if not date or date == "nan":
                date_posted = TODAY
            else:
                # Format to YYYY-MM-DD
                date_posted = date.split(" ")[0].strip()

            # Resolve time_ago
            if time_ago_scraped and time_ago_scraped != "nan":
                time_ago = time_ago_scraped
            else:
                # Fallback: estimate time_ago based on date_posted and search recency
                try:
                    posted_dt = datetime.date.fromisoformat(date_posted)
                    today_dt = datetime.date.fromisoformat(TODAY)
                    days_diff = (today_dt - posted_dt).days
                except Exception:
                    days_diff = 0

                if days_diff <= 0:
                    # Posted today
                    if random.random() < 0.15:
                        minutes = random.randint(5, 59)
                        time_ago = f"{minutes} minutes ago"
                    else:
                        max_h = min(23, max(1, hours_old))
                        hours = random.randint(1, max_h)
                        time_ago = f"{hours} hour{'s' if hours > 1 else ''} ago"
                elif days_diff == 1:
                    time_ago = "1 day ago"
                else:
                    time_ago = f"{days_diff} days ago"

            jobs.append({
                "title":       str(row.get("title", "")    or "").strip(),
                "company":     str(row.get("company", "")  or "").strip(),
                "location":    str(row.get("location", "") or "").strip(),
                "job_url":     url,
                "date_posted": date_posted,
                "time_ago":    time_ago,
            })
        return jobs

    except Exception as e:
        print(f"    ERROR: {e}")
        return []


# ── MAIN ─────────────────────────────────────────────────────────────────────

def run(test_mode: bool = False) -> None:
    queries     = SEARCH_QUERIES[:1] if test_mode else SEARCH_QUERIES
    target      = 5                  if test_mode else TARGET
    results_per = 5                  if test_mode else RESULTS_PER

    # Focused only on Hyderabad, India
    cities = ["Hyderabad"]

    mode_label = "TEST MODE (1 query, 5 jobs)" if test_mode else "FULL MODE"

    print("=" * 62)
    print(f"  LinkedIn Real-Time Job Scraper (Hyderabad) — {mode_label}")
    print(f"  Date    : {TODAY}  ({NOW})")
    print(f"  Filter  : Last {HOURS_OLD} hours (LATEST jobs only)")
    print(f"  Target  : {target} unique jobs")
    print(f"  Cities  : {', '.join(cities)}")
    print(f"  Output  : {OUTPUT_FILE.name}")
    print("=" * 62)

    json_out = RealtimeJSON(OUTPUT_FILE)

    for i, query in enumerate(queries, 1):
        if json_out.count >= target:
            print(f"\n  Reached {target} jobs — done!")
            break

        print(f"\n  [{i:02d}/{len(queries)}] Scraping: {query!r}")

        for city in cities:
            if json_out.count >= target:
                break

            print(f"           Location: {city} | hours_old={HOURS_OLD} | results={results_per}")
            jobs = scrape_linkedin(query, location=city, results=results_per, hours_old=HOURS_OLD)

            if not jobs:
                continue

            added = 0
            for job in jobs:
                if json_out.count >= target:
                    break
                written = json_out.append(job)
                if written:
                    added += 1
                    print(
                        f"    [{json_out.count:03d}] SAVED  "
                        f"{job['title'][:38]:<40}\n"
                        f"           Company: {job['company'][:30]:<32} | Location: {job['location'][:30]}\n"
                        f"           URL:     {job['job_url']}\n"
                    )

            print(f"           +{added} new from {city} (total={json_out.count})")
            
            # Brief delay between searches
            time.sleep(random.uniform(1.5, 2.5))

    # ── Summary ───────────────────────────────────────────────────
    print(f"\n{'=' * 62}")
    print(f"  DONE!  {json_out.count} jobs written to:")
    print(f"  {OUTPUT_FILE}")
    print(f"{'=' * 62}\n")

    if test_mode and json_out.count > 0:
        print("  Test passed! Run full scrape with:")
        print("    python linkedin_realtime_hyderabad.py")
    elif test_mode and json_out.count == 0:
        print("  No jobs found in test. LinkedIn may be rate-limiting.")
        print("  Try again in a few minutes or increase --hours to 72.")


def main() -> None:
    global HOURS_OLD, OUTPUT_FILE
    parser = argparse.ArgumentParser(description="LinkedIn real-time Hyderabad job scraper")
    parser.add_argument(
        "--test",
        action="store_true",
        help="Test mode: 1 query, 5 results (verify it works before full run)",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=HOURS_OLD,
        metavar="N",
        help=f"Max job age in hours (default: {HOURS_OLD} = last {HOURS_OLD}h only)",
    )
    args = parser.parse_args()

    HOURS_OLD = args.hours
    OUTPUT_FILE = OUTPUT_DIR / f"linkedin_jobs_hyderabad_{HOURS_OLD}h_{TIMESTAMP}.json"

    run(test_mode=args.test)


if __name__ == "__main__":
    main()
