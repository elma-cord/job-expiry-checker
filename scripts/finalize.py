"""Merge chunk results, remove EXPIRED listings in Cord, build the CSVs.

Inputs (current directory):
  parts/**/results_part_*.csv   checker output per chunk
  listings.json                 listings fetched from Cord at the start of the run
  morning/run_results.csv       (afternoon only, optional) the morning run's results

Env:
  SLOT              morning | afternoon | now
  RUN_DATE          YYYY-MM-DD (UK)
  REMOVE_EXPIRED    "true" to actually remove; anything else = report-only
  MAX_REMOVE        safety cap per run (default 2000)

Outputs:
  results.csv          same columns as before (for the Google Sheet)
  run_results.csv      one row per listing, with listing id, company, action
  daily_summary.csv    afternoon only: removed today + still to check manually
"""
import csv
import glob
import json
import os
import sys
from collections import defaultdict
from urllib.parse import urlparse

import cord_api

SLOT = os.environ.get("SLOT", "now")
RUN_DATE = os.environ.get("RUN_DATE", "")
REMOVE = os.environ.get("REMOVE_EXPIRED", "").strip().lower() == "true"
MAX_REMOVE = int(os.environ.get("MAX_REMOVE") or 2000)
BATCH = 200

RESULT_FIELDS = ["url", "final_url", "domain", "status_code", "status", "evidence", "reason", "checked_at"]
RUN_FIELDS = ["listing_id", "company_id", "company_name", "position", "url", "domain",
              "status", "action", "evidence", "reason", "checked_at", "run"]
SUMMARY_FIELDS = ["listing_id", "company_name", "position", "url", "domain", "action",
                  "run", "times_flagged", "evidence", "reason", "checked_at"]


def read_results():
    rows = []
    for fn in sorted(glob.glob("parts/**/results_part_*.csv", recursive=True)):
        with open(fn, newline="", encoding="utf-8") as f:
            rows.extend(csv.DictReader(f))
    return rows


def write_csv(path, fields, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def remove_listings(ids):
    """Returns the set of ids Cord confirmed removed."""
    session = cord_api.login()
    removed = set()
    for i in range(0, len(ids), BATCH):
        batch = ids[i:i + BATCH]
        status = cord_api.bulk_delete(session, batch)
        if 200 <= status < 300:
            removed.update(batch)
        else:
            print(f"WARNING: bulk-delete batch {i // BATCH + 1} failed with HTTP {status}")
    return removed


def build_daily_summary(today_rows):
    morning = []
    if os.path.exists("morning/run_results.csv"):
        with open("morning/run_results.csv", newline="", encoding="utf-8") as f:
            morning = list(csv.DictReader(f))
    else:
        print("WARNING: morning results for today not found; summary covers this run only.")

    removed_actions = {"REMOVED", "WOULD_REMOVE"}
    removed, seen = [], set()
    for r in morning + today_rows:
        if r["action"] in removed_actions and r["listing_id"] not in seen:
            seen.add(r["listing_id"])
            removed.append({**r, "times_flagged": ""})

    flagged = defaultdict(int)
    for r in morning + today_rows:
        if r["status"] == "CHECK_MANUALLY":
            flagged[r["listing_id"]] += 1

    manual = [
        {**r, "times_flagged": flagged[r["listing_id"]]}
        for r in today_rows
        if r["status"] == "CHECK_MANUALLY" and r["listing_id"] not in seen
    ]
    manual.sort(key=lambda r: (r["domain"], r["company_name"]))

    write_csv("daily_summary.csv", SUMMARY_FIELDS, removed + manual)
    return len(removed), len(manual), bool(morning)


def main():
    with open("listings.json", encoding="utf-8") as f:
        listings = json.load(f)
    by_url = defaultdict(list)
    for l in listings:
        by_url[l["url"]].append(l)

    results = read_results()
    if not results:
        sys.exit("No chunk results found.")
    write_csv("results.csv", RESULT_FIELDS, results)

    run_rows = []
    for r in results:
        for l in by_url.get((r.get("url") or "").strip(), []):
            run_rows.append({
                "listing_id": str(l["listing_id"]),
                "company_id": l["company_id"],
                "company_name": l["company_name"],
                "position": l["position"],
                "url": l["url"],
                "domain": r.get("domain") or urlparse(l["url"]).netloc,
                "status": r.get("status", ""),
                "action": "",
                "evidence": r.get("evidence", ""),
                "reason": r.get("reason", ""),
                "checked_at": r.get("checked_at", ""),
                "run": SLOT,
            })

    checked_ids = {r["listing_id"] for r in run_rows}
    not_checked = len(listings) - len(checked_ids)

    expired_ids = sorted({int(r["listing_id"]) for r in run_rows if r["status"] == "EXPIRED"})
    failed = False
    removed_ids = set()

    if len(expired_ids) > MAX_REMOVE:
        print(f"SAFETY STOP: {len(expired_ids)} EXPIRED is above the cap of {MAX_REMOVE}. Nothing removed.")
        expired_action, failed = "NOT_REMOVED_CAP", True
    elif REMOVE and expired_ids:
        removed_ids = {str(i) for i in remove_listings(expired_ids)}
        expired_action = None
        failed = len(removed_ids) < len(expired_ids)
    else:
        expired_action = "WOULD_REMOVE"

    for r in run_rows:
        if r["status"] == "EXPIRED":
            if expired_action:
                r["action"] = expired_action
            else:
                r["action"] = "REMOVED" if r["listing_id"] in removed_ids else "REMOVE_FAILED"
        elif r["status"] == "CHECK_MANUALLY":
            r["action"] = "CHECK_MANUALLY"

    write_csv("run_results.csv", RUN_FIELDS, run_rows)

    counts = defaultdict(int)
    for r in run_rows:
        counts[r["status"]] += 1

    lines = [
        f"## {RUN_DATE} {SLOT} run ({'REMOVING' if REMOVE else 'REPORT-ONLY'})",
        f"- Listings from Cord: {len(listings)}",
        f"- ACTIVE: {counts['ACTIVE']} | EXPIRED: {counts['EXPIRED']} | CHECK_MANUALLY: {counts['CHECK_MANUALLY']}",
        f"- Removed in Cord: {len(removed_ids)}",
    ]
    if not_checked:
        lines.append(f"- WARNING: {not_checked} listings had no result (a chunk may have failed)")

    if SLOT == "afternoon":
        n_removed, n_manual, had_morning = build_daily_summary(run_rows)
        lines.append(f"- Daily summary: {n_removed} removed today, {n_manual} to check manually"
                     + ("" if had_morning else " (morning results missing)"))

    report = "\n".join(lines)
    print(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(report + "\n")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
