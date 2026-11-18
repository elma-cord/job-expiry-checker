"""Merge chunk results, remove EXPIRED listings in Cord, build the CSVs.

Inputs (current directory):
  parts/**/results_part_*.csv   checker output per chunk
  listings.json                 listings fetched from Cord at the start of the run
  morning/run_results.csv       (midday only, optional) the morning run's results

Env:
  SLOT              morning | midday | now
  RUN_DATE          YYYY-MM-DD (UK)
  REMOVE_EXPIRED    "true" to actually remove; anything else = report-only
  MAX_REMOVE        safety cap per run (default 2000)
  CLAUDE_REMOVE     "true" to also remove the jobs the Claude check found expired (default: shadow mode, nothing)
  CLAUDE_MAX_REMOVE safety cap for Claude removals per run (default 600; above it Claude removes nothing that run)
  SHEET_WEBHOOK_URL / SHEET_WEBHOOK_TOKEN  Apps Script (apps_script/expiry_sheets.gs) that, after the midday run,
                    adds the jobs still to check to "Manual check" and the day's removed count to the data table
                    (Expired roles, row 16). Optional.

Claude check (Elma, 16 Nov 2026; scripts/claude_check.py): claude/**/claude_part_*.json holds Claude's answer for each
CHECK_MANUALLY link; memory.json the answers of the last days. Hard proof (404 / gone from the job system) is removed
at once; Claude's EXPIRED from the morning is removed at midday after a second EXPIRED (CONFIRMED); a job first seen
at midday is removed on its one EXPIRED (Elma 18 Nov 2026); LINK_BROKEN is never removed.

Outputs:
  results.csv          same columns as before (for the Google Sheet)
  run_results.csv      one row per listing, with listing id, company, action
  daily_summary.csv    midday only: removed today + still to check manually
"""
import csv
import glob
import requests
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
CLAUDE_REMOVE = os.environ.get("CLAUDE_REMOVE", "").strip().lower() == "true"
CLAUDE_MAX_REMOVE = int(os.environ.get("CLAUDE_MAX_REMOVE") or 600)
# Elma 17 Nov 2026: press admin "Refresh last checked" after every finished check.
# Repo variable REFRESH_LAST_CHECKED=off turns it off.
REFRESH_LAST_CHECKED = os.environ.get("REFRESH_LAST_CHECKED", "").strip().lower() != "off"
MEMORY_DAYS = 7
BATCH = 200

RESULT_FIELDS = ["url", "final_url", "domain", "status_code", "status", "evidence", "reason", "checked_at"]
RUN_FIELDS = ["listing_id", "company_id", "company_name", "position", "url", "domain",
              "status", "action", "evidence", "reason", "checked_at", "run",
              "claude_status", "claude_reason", "claude_source", "claude_action"]
CLAUDE_FIELDS = ["date", "run", "listing_id", "company_name", "position", "url", "claude_status", "claude_reason",
                 "claude_source", "claude_action", "your_check"]
MANUAL_FIELDS = ["listing_id", "position", "url", "claude_status", "claude_reason", "claude_source"]
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


def read_claude():
    out = {}
    for fn in sorted(glob.glob("claude/**/claude_part_*.json", recursive=True)):
        with open(fn, encoding="utf-8") as f:
            out.update(json.load(f))
    return out


def claude_action(c):
    """What happens to a job because of Claude's answer (before shadow mode / caps are applied)."""
    st = c.get("status")
    if st == "EXPIRED" and c.get("hard"):
        return "REMOVE"
    if st == "EXPIRED":
        if c.get("confirm") == "CONFIRMED":
            return "REMOVE"
        if SLOT == "morning":
            return "WAIT_MIDDAY"  # looked at again at midday; a second EXPIRED confirms it
        if SLOT == "midday" and not c.get("confirm"):
            # Elma 18 Nov 2026: a job Claude sees for the first time at midday has no later check, so Claude's
            # EXPIRED is trusted and removed (before, these ~90 a day went to Manual check as "unsure").
            return "REMOVE"
        return "CHECK_MANUALLY"  # a manual run has no second check
    if st == "LINK_BROKEN":
        return "LINK_BROKEN"
    if st == "ACTIVE":
        return "KEEP"
    return "CHECK_MANUALLY"


def save_memory(claude):
    old = {}
    if os.path.exists("memory.json"):
        with open("memory.json", encoding="utf-8") as f:
            old = json.load(f)
    keep = {}
    for u, m in {**old, **claude}.items():
        try:
            from datetime import date
            if (date.fromisoformat(RUN_DATE) - date.fromisoformat(m.get("date", ""))).days <= MEMORY_DAYS:
                keep[u] = m
        except (ValueError, TypeError):
            pass
    with open("claude_memory.json", "w", encoding="utf-8") as f:
        json.dump(keep, f)
    return len(keep)


def manual_check_rows(today_rows):
    """Elma 18 Nov 2026: after the midday run, every job still to check by hand goes to the "Manual check" tab:
    Claude unsure (or no Claude answer) and LINK BROKEN. Claude's ACTIVE (keep) is trusted when CLAUDE_REMOVE is on,
    and removed jobs are already gone."""
    out, seen = [], set()
    for r in sorted(today_rows, key=lambda r: (r["domain"], r["company_name"])):
        if r["status"] != "CHECK_MANUALLY" or r["action"] == "REMOVED" or r["listing_id"] in seen:
            continue
        if CLAUDE_REMOVE and r.get("claude_action") == "KEEP":
            continue
        seen.add(r["listing_id"])
        out.append({"listing_id": r["listing_id"], "position": r["position"], "url": r["url"],
                    "claude_status": "link broken" if r.get("claude_action") == "LINK_BROKEN" else "unsure",
                    "claude_reason": r.get("claude_reason", ""), "claude_source": r.get("claude_source", "")})
    return out


def push_to_sheets(n_removed, manual):
    """Midday only: Manual check rows + the day's removed count for the data table (Apps Script expiry_sheets.gs)."""
    url = os.environ.get("SHEET_WEBHOOK_URL", "").strip()
    if not url:
        return "not set"
    try:
        body = {"token": os.environ.get("SHEET_WEBHOOK_TOKEN", ""), "action": "daily", "date": RUN_DATE,
                "removed": n_removed, "manual_rows": [[r[k] for k in MANUAL_FIELDS] for r in manual]}
        resp = requests.post(url, json=body, timeout=180)
        return f"HTTP {resp.status_code}: {resp.text[:200]}"
    except Exception as e:  # noqa: BLE001
        return f"failed: {type(e).__name__}"


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
        and not (CLAUDE_REMOVE and r.get("claude_action") in ("KEEP", "REMOVE"))  # Claude decided these
    ]
    manual.sort(key=lambda r: (r["domain"], r["company_name"]))

    write_csv("daily_summary.csv", SUMMARY_FIELDS, removed + manual)
    return len(removed), len(manual), bool(morning)


def refresh_last_checked(not_checked, failed):
    """Refresh "last checked" on all external positions, only when the whole check finished."""
    if not REFRESH_LAST_CHECKED:
        return "refresh turned off (REFRESH_LAST_CHECKED=off)"
    if not_checked:
        return f"NOT refreshed, {not_checked} listings were not checked"
    if failed:
        return "NOT refreshed, removing expired jobs did not fully work"
    try:
        status = cord_api.refresh_all_last_checked(cord_api.login())
    except SystemExit as e:
        return f"NOT refreshed, {e}"
    except Exception as e:
        return f"NOT refreshed, {type(e).__name__}"
    if status < 400:
        return "refreshed for all external positions"
    return f"NOT refreshed, Cord answered HTTP {status}"


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

    claude = read_claude()
    for r in run_rows:
        c = claude.get(r["url"].strip())
        if c and r["status"] == "CHECK_MANUALLY":
            r["claude_status"], r["claude_reason"] = c.get("status", ""), c.get("reason", "")
            r["claude_source"] = c.get("how", "") + (f" (reused: {c['reused']})" if c.get("reused") else "") \
                + (f" (midday check: {c['confirm']})" if c.get("confirm") else "")
            r["claude_action"] = claude_action(c)

    checked_ids = {r["listing_id"] for r in run_rows}
    not_checked = len(listings) - len(checked_ids)

    expired_ids = sorted({int(r["listing_id"]) for r in run_rows if r["status"] == "EXPIRED"})
    claude_ids = sorted({int(r["listing_id"]) for r in run_rows if r.get("claude_action") == "REMOVE"})
    claude_note = "shadow mode, nothing removed" if not CLAUDE_REMOVE else ""
    if CLAUDE_REMOVE and len(claude_ids) > CLAUDE_MAX_REMOVE:
        print(f"SAFETY STOP: Claude found {len(claude_ids)} expired, above the cap of {CLAUDE_MAX_REMOVE}. None of them removed.")
        claude_note, claude_ids = "above the Claude cap, nothing removed", []
    elif not CLAUDE_REMOVE:
        claude_ids = []
    expired_ids = sorted(set(expired_ids) | set(claude_ids))
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
        elif r["status"] == "CHECK_MANUALLY" and r.get("claude_action") == "REMOVE" and int(r["listing_id"]) in claude_ids:
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
    manual_rows = [r for r in run_rows if r["status"] == "CHECK_MANUALLY"]
    with_claude = [r for r in manual_rows if r.get("claude_status")]
    if claude or os.path.exists("memory.json"):
        cc = defaultdict(int)
        for r in with_claude:
            cc[r["claude_action"]] += 1
        lines.append(f"- Claude check ({'REMOVING' if CLAUDE_REMOVE else 'SHADOW MODE'}): answered {len(with_claude)} of "
                     f"{len(manual_rows)} CHECK_MANUALLY | remove {cc['REMOVE']} | wait for midday {cc['WAIT_MIDDAY']} | "
                     f"link broken {cc['LINK_BROKEN']} | keep {cc['KEEP']} | still to check manually {cc['CHECK_MANUALLY']}"
                     + (f" ({claude_note})" if claude_note else ""))
        sheet_rows = [{"date": RUN_DATE, "run": SLOT, "listing_id": r["listing_id"], "company_name": r["company_name"],
                       "position": r["position"], "url": r["url"], "claude_status": r["claude_status"],
                       "claude_reason": r["claude_reason"], "claude_source": r["claude_source"],
                       "claude_action": ("WOULD " + r["claude_action"]) if (not CLAUDE_REMOVE and r["claude_action"] == "REMOVE")
                       else r["claude_action"], "your_check": ""}
                      for r in with_claude if not (SLOT == "midday" and "reused: this morning" in r["claude_source"])]
        write_csv("claude_check.csv", CLAUDE_FIELDS, sheet_rows)  # kept as a run file; no longer sent to the sheet
        lines.append(f"- Claude memory: {save_memory(claude)} links")

    if SLOT == "midday":
        n_removed, n_manual, had_morning = build_daily_summary(run_rows)
        lines.append(f"- Daily summary: {n_removed} {'removed' if REMOVE else 'would be removed'} today, {n_manual} to check manually"
                     + ("" if had_morning else " (morning results missing)"))
        manual = manual_check_rows(run_rows)
        write_csv("manual_check.csv", MANUAL_FIELDS, manual)
        sheet_removed = n_removed if REMOVE else 0
        lines.append(f"- Sheets: {len(manual)} jobs for Manual check, {sheet_removed} removed for the data table | "
                     f"{push_to_sheets(sheet_removed, manual)}")

    lines.append(f"- Last checked: {refresh_last_checked(not_checked, failed)}")

    report = "\n".join(lines)
    print(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(report + "\n")

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
