"""Second expiry agent (Elma, 16 Nov 2026): candidates' "expired job" reports in Slack #position-ops-reports.

Every ~10 minutes this reads the channel's new report messages, for example
  "2026-11-16 17:00:43 | production | Full Stack Developer (497808) by LCP reported by Bogdan (...)"
and checks the reported job's link the same way as the twice-daily expiry run (Elma, 26 Nov 2026: "it needs to use the
same rules"): first the rule checker (check_jobs.py: 404/410, redirected to the careers list, closed wording, apply-by
date in the past, apply + job body), then, only when the rules say CHECK_MANUALLY, the Claude check (claude_check.py):
  * EXPIRED (404 / gone from the job system / a clear closed notice): the job is removed in cord admin
    (External positions, bulk delete) and the message gets :white_check_mark:
  * ACTIVE (the advert is shown at our link): not removed, the message gets :a:
  * not an external job (a company's own cord job, or no longer listed): not touched, the message gets :eyes:
  * LINK_BROKEN / UNSURE: no reaction (Elma's choice); checked again on a later run, at most MAX_TRIES times,
    at least an hour apart, then left for a person.
A message that already has any reaction (from a person or from this agent) is never checked again.
TWO REPORTS (Elma, Ben, KP, standup 20 Nov 2026): when two or more DIFFERENT candidates (by email) report the same
job within 24 hours of each other, the job is removed in cord even if it still looks live (recruiters may tell
candidates it is closed). Every report of that job gets the number of reports (:two:, :three:, ...) and
:white_check_mark:; an earlier :a: stays. Messages a person reacted to are counted but not changed. A job that is
not one of our live external jobs is never removed this way (same as :eyes:).
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
WHO = re.compile(r"reported by\s+(?P<name>[^(<\n|]+?)\s*(?:\(\s*<?(?:mailto:)?(?P<email>[^|>\s)]+@[^|>\s)]+)|$|\n)", re.S)
NUMBERS = {2: "two", 3: "three", 4: "four", 5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine"}
WINDOW_S = 24 * 3600
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
    oldest = f"{time.time() - LOOKBACK_H * 3600:.6f}"  # Slack wants exactly 6 decimals
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
        text = " ".join([m.get("text", ""), json.dumps(m.get("blocks", []), ensure_ascii=False),
                         json.dumps(m.get("attachments", []), ensure_ascii=False)])
        g = REPORT.search(m.get("text", "")) or REPORT.search(text)
        if not g and DRY and m.get("subtype") != "channel_join" and not m.get("reactions"):
            print("    (dry run) message not read as a report:", repr(text[:400]))
        if g:
            raw = m.get("text", "") or text
            who = WHO.search(raw)
            reporter = (who.group("email") or who.group("name") or "").strip().lower() if who else ""
            out.append({"ts": m["ts"], "id": g.group("id"), "title": html.unescape(g.group("title").strip()),
                        "company": html.unescape(g.group("company").strip()), "reporter": reporter or m["ts"],
                        "reactions": {x["name"]: x.get("users", []) for x in m.get("reactions", [])}})
    return sorted(out, key=lambda x: float(x["ts"]))


def react(ts, name):
    if DRY:
        print(f"    (dry run) would react :{name}:")
        return
    slack("reactions.add", http="post", channel=CHANNEL, timestamp=ts, name=name)


def unreact(ts, name):
    if DRY:
        print(f"    (dry run) would remove reaction :{name}:")
        return
    try:
        slack("reactions.remove", http="post", channel=CHANNEL, timestamp=ts, name=name)
    except RuntimeError as e:
        print(f"    could not remove :{name}: ({e})")


def rule_check(url):
    """Step 1 of the twice-daily run for one link: check_jobs.check_one, same browser rules and headers.
    Returns (EXPIRED | ACTIVE | CHECK_MANUALLY, evidence)."""
    import asyncio
    import aiohttp
    from concurrent.futures import ThreadPoolExecutor
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # check_jobs.py is at the repo root
    import check_jobs as R

    async def run():
        ex = ThreadPoolExecutor(max_workers=1)
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                                 "Chrome/124.0.0.0 Safari/537.36",
                   "Accept-Language": "en-US,en;q=0.9",
                   "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False, force_close=True),
                                             headers=headers) as session:
                return await R.check_one(url, session, ex, 25, {"done": 0, "total": 1})
        finally:
            try:
                ex.submit(R._close_thread_browser).result(timeout=10)
            except Exception:
                pass
            ex.shutdown(wait=False)
    try:
        res = asyncio.run(run())
        return res["status"], f"{res['evidence']}: {res['reason']}"
    except Exception as e:  # the rule step failing must not stop the Claude step
        return "CHECK_MANUALLY", f"rule check failed ({type(e).__name__})"


def multi_groups(found):
    """job id -> the reports of that job when 2+ different candidates reported it within 24 hours of each other."""
    by_id = {}
    for r in found:
        by_id.setdefault(r["id"], []).append(r)
    groups = {}
    for jid, rs in by_id.items():
        rs.sort(key=lambda x: float(x["ts"]))
        hit = any(len({o["reporter"] for o in rs if abs(float(o["ts"]) - float(r["ts"])) <= WINDOW_S}) >= 2 for r in rs)
        if hit:
            groups[jid] = rs
    return groups


def pending_multi(found, state):
    """groups whose reactions are not up to date yet (new group, or a new report since the last run)."""
    done = state.get("_multi", {})
    return {jid: rs for jid, rs in multi_groups(found).items()
            if done.get(jid, {}).get("count") != len({r["reporter"] for r in rs})}


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
    multi = pending_multi(found, state)
    all_multi = multi_groups(found)
    reports = [r for r in found
               if not r["reactions"] and r["id"] not in all_multi
               and state.get(r["ts"], {}).get("tries", 0) < MAX_TRIES
               and time.time() - state.get(r["ts"], {}).get("last", 0) >= RETRY_GAP_S]
    print(f"{datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC | reports to check: {len(reports)} | jobs with 2+ reports to act on: {len(multi)}{' | DRY RUN' if DRY else ''}")
    if "--count" in sys.argv:  # first step of the workflow: is there anything to check? (skips the browser install)
        if os.environ.get("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a") as f:
                f.write(f"reports={len(reports) + len(multi)}\n")
        return
    if not reports and not multi:
        json.dump(state, open("state.json", "w"))
        return

    session = cord_api.login()
    external = {str(l.get("listingID")): l for l in cord_api.get_external_listings(session)}
    lines, removed = [], 0
    bot = slack("auth.test").get("user_id") if not DRY else "BOT"

    # 20 Nov: 2+ different candidates within 24 hours -> remove, number + green tick on every report of the job
    done = state.setdefault("_multi", {})
    for jid, rs in multi.items():
        n = len({r["reporter"] for r in rs})
        label = f"{rs[-1]['title']} ({jid}) by {rs[-1]['company']}"
        was_removed = done.get(jid, {}).get("removed")
        if not was_removed:
            listing = external.get(jid)
            if not listing or not (listing.get("externalURL") or "").strip():
                done[jid] = {"count": n, "removed": False, "note": "not a live external job"}
                lines.append(f"- {label}: {n} reports, but not a live external job: not removed")
                continue
            if removed >= MAX_REMOVE:
                lines.append(f"- {label}: {n} reports, cap of {MAX_REMOVE} removals reached; next run")
                continue
            if DRY:
                print(f"    (dry run) would remove {jid} in cord ({n} reports)")
                ok = True
            else:
                code = cord_api.bulk_delete(session, [int(jid)])
                ok = 200 <= code < 300
                print(f"    {jid}: {n} reports, removed in cord: HTTP {code}")
            if not ok:
                lines.append(f"- {label}: {n} reports, removal failed; next run tries again")
                continue
            removed += 1
        num = NUMBERS.get(n, "nine")
        for r in rs:
            people = {u for users in r["reactions"].values() for u in users if u != bot}
            if people:
                continue  # a person handled this message: counted, not changed
            for old_n in NUMBERS.values():
                if old_n != num and old_n in r["reactions"]:
                    unreact(r["ts"], old_n)
            if num not in r["reactions"]:
                react(r["ts"], num)
            if "white_check_mark" not in r["reactions"]:
                react(r["ts"], "white_check_mark")
            state.setdefault(r["ts"], {}).update(tries=MAX_TRIES, last=time.time(), result=f"{n} reports")
        done[jid] = {"count": n, "removed": True, "last": time.time()}
        lines.append(f"- {label}: reported by {n} different candidates within 24 hours: removed, :{num}: :white_check_mark:")

    if reports:
        import claude_check as CC  # loads Claude + the browser only when there is single reports to check
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
            rule, evidence = rule_check(url)
            if rule == "EXPIRED":
                res = {"status": "EXPIRED", "reason": evidence, "how": "rules, as in the twice-daily run"}
            elif rule == "ACTIVE":
                res = {"status": "ACTIVE", "reason": evidence, "how": "rules, as in the twice-daily run"}
            else:
                res = CC.decide(url, listing.get("positionName") or r["title"])
                res["how"] = f"{res['how']}; rules said: {evidence}"
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
    multi_keep = {k: v for k, v in state.get("_multi", {}).items() if time.time() - v.get("last", time.time()) < 3 * 86400}
    state = {k: v for k, v in state.items() if k != "_multi" and float(k) >= cut}
    state["_multi"] = multi_keep
    json.dump(state, open("state.json", "w"))
    report = "## Slack expiry reports" + (" (DRY RUN)" if DRY else "") + "\n" + "\n".join(lines)
    print(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write(report + "\n")


if __name__ == "__main__":
    main()
