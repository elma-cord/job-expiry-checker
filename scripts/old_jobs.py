"""Remove external jobs posted on cord more than 3 months ago (Elma, 19 Nov 2026).

Runs first in the MORNING run, before the expiry check (so the check does not spend time on them).
Rule: every external job that is not deleted and whose "date posted" (the day it was uploaded to cord, admin
external-listings `datePosted`, read as a UK date) is BEFORE today minus 3 calendar months.
  19/11 -> removes jobs posted before 19/08;  30/11 -> before 30/08;  31/05 -> before 28/02 (end of month).
Safety: if more than OLD_JOBS_MAX (default 2000) jobs match, NOTHING is removed and the run says why (a number that high
means something is wrong, e.g. a bad date); Elma normally removes 100-200 a day.
Switched off with the repo variable OLD_JOBS=off.

Writes old_jobs.json ({date, cutoff, found, removed, failed, note}) and old_jobs_removed.csv (the removed jobs).
The midday run sends `removed` to the sheet: "Expired roles" row 20 "Positions posted more than 3 months ago".
Never exits with an error: the expiry check always goes on.
"""
import calendar
import csv
import json
import os
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import cord_api

UK = ZoneInfo("Europe/London")
MAX = int(os.environ.get("OLD_JOBS_MAX") or 2000)
BATCH = 200


def minus_months(d, n):
    y, m = d.year, d.month - n
    while m < 1:
        m += 12
        y -= 1
    return date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def posted_uk_date(value):
    s = str(value or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        try:
            return date.fromisoformat(s[:10])
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(UK).date()


def save(result, removed_rows):
    with open("old_jobs.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=1)
    with open("old_jobs_removed.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["listing_id", "company_id", "company_name", "position", "date_posted", "url"])
        w.writerows(removed_rows)
    print(json.dumps(result))


def main():
    today = date.fromisoformat(os.environ.get("RUN_DATE") or datetime.now(UK).date().isoformat())
    cutoff = minus_months(today, 3)
    result = {"date": today.isoformat(), "cutoff": cutoff.isoformat(), "found": 0, "removed": 0, "failed": 0, "note": ""}
    if (os.environ.get("OLD_JOBS") or "").strip().lower() == "off":
        result.update(removed=None, note="switched off (OLD_JOBS=off)")
        return save(result, [])
    try:
        session = cord_api.login()
        listings = cord_api.get_external_listings(session)
    except SystemExit as e:
        result.update(removed=None, note=f"not run: {e}")
        return save(result, [])
    no_date = 0
    old = []
    for l in listings:
        d = posted_uk_date(l.get("datePosted"))
        if d is None:
            no_date += 1
        elif d < cutoff:
            old.append(l)
    result["found"] = len(old)
    if no_date:
        result["note"] = f"{no_date} jobs have no date posted (left alone). "
    if not listings or len(old) > MAX:
        result.update(removed=None, note=result["note"] + (
            f"NOTHING removed: {len(old)} jobs posted before {cutoff} is more than the limit of {MAX}"
            if listings else "NOTHING removed: cord returned no listings"))
        return save(result, [])
    old.sort(key=lambda l: str(l.get("datePosted")))
    removed_rows = []
    for i in range(0, len(old), BATCH):
        batch = old[i:i + BATCH]
        status = cord_api.bulk_delete(session, [l["listingID"] for l in batch])
        if 200 <= status < 300:
            removed_rows += [[l.get("listingID"), l.get("companyID"), l.get("companyName"), l.get("positionName"),
                              str(posted_uk_date(l.get("datePosted"))), l.get("externalURL")] for l in batch]
        else:
            result["failed"] += len(batch)
            print(f"WARNING: bulk-delete batch {i // BATCH + 1} failed with HTTP {status}")
    result["removed"] = len(removed_rows)
    result["note"] += f"removed every job posted before {cutoff}"
    save(result, removed_rows)


if __name__ == "__main__":
    main()
