"""Wait until the slot's start time (UK time), then print the run date.

GitHub starts scheduled runs late (up to ~5.5h seen), so the crons fire
~4h early and this script holds the run until the window opens. If GitHub
was so late that the window has already opened, it starts straight away.

Usage: python wait_for_slot.py <morning|afternoon|now>
Writes run_date=YYYY-MM-DD and slot=<slot> to $GITHUB_OUTPUT.
"""
import os
import sys
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")
SLOT_START_HOUR = {"morning": 3, "afternoon": 15}
MAX_WAIT = timedelta(hours=5, minutes=30)


def target_for(slot, now):
    hour = SLOT_START_HOUR[slot]
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    # The morning cron fires the evening before, so its window is tomorrow.
    if slot == "morning" and now.hour >= 12:
        target += timedelta(days=1)
    return target


def main():
    slot = (sys.argv[1] if len(sys.argv) > 1 else "now").strip() or "now"
    now = datetime.now(LONDON)

    if slot in SLOT_START_HOUR:
        target = target_for(slot, now)
        wait = min(target - now, MAX_WAIT)
        if wait.total_seconds() > 0:
            print(f"Now {now:%Y-%m-%d %H:%M} UK. Waiting until {target:%Y-%m-%d %H:%M} UK ({wait}).")
            time.sleep(wait.total_seconds())
        else:
            print(f"Now {now:%Y-%m-%d %H:%M} UK. Window opened at {target:%H:%M}, starting immediately.")
    else:
        slot = "now"
        print(f"Manual run at {now:%Y-%m-%d %H:%M} UK, no waiting.")

    run_date = datetime.now(LONDON).strftime("%Y-%m-%d")
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as gh:
        gh.write(f"run_date={run_date}\nslot={slot}\n")


if __name__ == "__main__":
    main()
