"""Second expiry agent (Elma, 16 Nov 2026): candidates' "expired job" reports in Slack #position-ops-reports.

Every ~10 minutes this reads the channel's new report messages, for example
  "2026-11-16 17:00:43 | production | Full Stack Developer (497808) by LCP reported by Bogdan (...)"
and checks the reported job's link with the same Claude check as the expiry checker (scripts/claude_check.py):
  * EXPIRED (404 / gone from the job system / a clear closed notice): the job is removed in cord admin
    (External positions, bulk delete) and the message gets :white_check_mark:
  * ACTIVE (the advert is shown at our link): not removed, the message gets :a:
  * not an external job (a company's own cord job, or no longer listed): not touched, the message gets :eyes:
  * LINK_BROKEN / UNSURE: no reaction (Elma's choice); checked again on a later run, at most MAX_TRIES times,
    at least an hour apart, then left for a person.
A message that already has any reaction (from a person or from this agent) is never touched.
State (tries per message) is kept in state.json, carried between runs as a GitHub artifact.

Env: SLACK_BOT_TOKEN, ANTHROPIC_KEY, CONTEXTDEV_KEY, CORD_ADMIN_EMAIL, CORD_ADMIN_PASSWORD,
     DRY_RUN=true (check and report only: no removal, no reactions), LOOKBACK_HOURS (default 48).
"""
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import cord_api  # noqa: E402

CHANNEL = os.environ.get("SLACK_CHANNEL", "C07T1UFM4BF")  # #position-ops-reports
DRY = os.environ.get("DRY_RUN", "").strip().lower() == "true"
LOOKBACK_H = float(os.environ.get("LOOKBACK_HOURS") or 48)
MAX_TRIES = 3
RETRY_GAP_S = 3600
MAX_REMOVE = 20  # per run, safety cap
REPORT = re.compile(r"\|\s*production\s*\|\s*(?P<title>.+?)\s*\((?P<id>\d+)\)\s*by\s+(?P<company>.+?)\s+reported by", re.S)


def slack(method, http="get", **params):
    token = os.environ["SLACK_BOT_TOKEN"].strip()
    url = f"https://slack.com/api/{method}"
    hdr = {"Authorization": f"Bearer {token}"}
    for _ in range(4):
        r = requests.post(url, headers=hdr, json=params, timeout=60) if http == "post" else requests.get(url, headers=hdr, params=params, timeout=60)
        if r.status_code == 429:  # Slack limits new apps to about 1 history read a minute
            time.sleep(int(r.headers.get("Retry-After", "60")) + 1)
            continue
        break
    d = r.json()
    if not d.get("ok") and d.get("error") != "already_reacted":
        raise RuntimeError(f"Slack {method}: {d.get('error')}")
    return d


def new_reports():
    oldest = str(time.time() - LOOKBACK_H * 3600)
    msgs, cursor = [], None
    while True:
        d = slack("conversations.history", channel=CHANNEL, oldest=oldest, limit=15, **({"cursor": cursor} if cursor else {}))
        msgs += d.get("messages", [])
        print(f"    Slack returned {len(d.get('messages', []))} messages | "
              + json.dumps({k: v for k, v in d.items() if k != "messages"})[:300], flush=True)
        cursor = (d.get("response_metadata") or {}).get("next_cursor")
        if not cursor:
            break
    out = []
    for m in msgs:
        if m.get("reactions"):
            continue  # someone (a person or this agent) already handled it
        text = " ".join([m.get("text", ""), json.dumps(m.get("blocks", []), ensure_ascii=False),
                         json.dumps(m.get("attachments", []), ensure_ascii=False)])
        g = REPORT.search(m.get("text", "")) or REPORT.search(text)
        if not g and DRY and m.get("subtype") != "channel_join":
            print("    (dry run) message not read as a report:", repr(text[:400]))
        if g:
            out.append({"ts": m["ts"], "id": g.group("id"), "title": html.unescape(g.group("title").strip()), "company": html.unescape(g.group("company").strip())})
    return sorted(out, key=lambda x: float(x["ts"]))


def react(ts, name):
    if DRY:
        print(f"    (dry run) would react :{name}:")
        return
    slack("reactions.add", http="post", channel=CHANNEL, timestamp=ts, name=name)


def main():
    if not os.environ.get("SLACK_BOT_TOKEN", "").strip():
        print("SLACK_BOT_TOKEN is not set yet: nothing to do.")
        return
    state = json.load(open("state.json")) if os.path.exists("state.json") else {}
    if "--count" not in sys.argv and os.path.exists("reports.json"):
        found = json.load(open("reports.json"))  # read by the "Any new reports?" step of this run
    else:
        found = new_reports()
        json.dump(found, open("reports.json", "w"))
    reports = [r for r in found
               if state.get(r["ts"], {}).get("tries", 0) < MAX_TRIES
               and time.time() - state.get(r["ts"], {}).get("last", 0) >= RETRY_GAP_S]
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC | reports to check: {len(reports)}{' | DRY RUN' if DRY else ''}")
    if "--count" in sys.argv:  # first step of the workflow: is there anything to check? (skips the browser install)
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"reports={len(reports)}\n")
        return
    if not reports:
        json.dump(state, open("state.json", "w"))
        return

    import claude_check as CC  # loads Claude + the browser only when there is work

    session = cord_api.login()
    external = {str(l.get("listingID")): l for l in cord_api.get_external_listings(session)}
    lines, removed = [], 0
    for r in reports:
        st = state.setdefault(r["ts"], {"tries": 0})
        listing = external.get(r["id"])
        label = f"{r['title']} ({r['id']}) by {r['company']}"
        if not listing or not (listing.get("externalURL") or "").strip():
            print(f"- {label}: not a live external job -> :eyes:")
            react(r["ts"], "eyes")
            st.update(tries=MAX_TRIES, last=time.time(), result="not external")
            lines.append(f"- {label}: not a live external job, :eyes:")
            continue
        url = listing["externalURL"].strip()
        res = CC.decide(url, listing.get("positionName") or r["title"])
        st.update(tries=st["tries"] + 1, last=time.time(), result=res["status"], reason=res["reason"])
        print(f"- {label}: {res['status']} - {res['reason']} ({res['how']}) | {url}")
        if res["status"] == "EXPIRED":
            if removed >= MAX_REMOVE:
                lines.append(f"- {label}: EXPIRED but the cap of {MAX_REMOVE} removals per run was reached; next run")
                st["tries"] -= 1
                continue
            if DRY:
                print("    (dry run) would remove it in cord")
                ok = True
            else:
                code = cord_api.bulk_delete(session, [int(r["id"])])
                ok = 200 <= code < 300
                print(f"    removed in cord: HTTP {code}")
            if ok:
                removed += 1
                react(r["ts"], "white_check_mark")
                lines.append(f"- {label}: expired, removed, :white_check_mark: ({res['reason']})")
            else:
                st["tries"] -= 1  # try again next run
                lines.append(f"- {label}: expired but the removal failed; next run tries again")
        elif res["status"] == "ACTIVE":
            react(r["ts"], "a")
            lines.append(f"- {label}: still active, :a: ({res['reason']})")
        else:
            lines.append(f"- {label}: {res['status'].lower()}, no reaction, try {st['tries']} of {MAX_TRIES} ({res['reason']})")

    # forget messages older than the lookback window
    cut = time.time() - (LOOKBACK_H + 24) * 3600
    state = {k: v for k, v in state.items() if float(k) >= cut}
    json.dump(state, open("state.json", "w"))
    report = "## Slack expiry reports" + (" (DRY RUN)" if DRY else "") + "\n" + "\n".join(lines)
    print(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(report + "\n")


if __name__ == "__main__":
    main()
