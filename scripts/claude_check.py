"""Claude check for the jobs the rule checker marks CHECK_MANUALLY (Elma, 16 Nov 2026).

Tested on the 2,307 jobs of the "Manual check" tab on 16 Nov 2026 (Claude agreed with the manual check on 174 of
202 expired jobs; ~7% left unsure). The rules, all agreed with Elma:
  * Judge ONLY what our link shows a candidate: a real browser, visible text only (hidden "job filled" templates
    don't count), embedded Greenhouse/Ashby/Lever... frames included, menus / cookie banners / dropdown lists left out,
    page scrolled, PDFs read.
  * Hard proof, no Claude: HTTP 404/410, or the job system (Greenhouse, Ashby, Lever, Workday, SmartRecruiters) says
    the job is gone -> EXPIRED.
  * Claude (Haiku) answers ACTIVE / EXPIRED (a clear closed / not found / deadline notice) / LINK_BROKEN (our link
    shows only an application form, a careers list or home page, or another page: never removed, Elma fixes the
    link) / UNSURE. Our job titles are standardised, so a different title on the same job page is the same job.
  * A page with only menus is UNSURE. If the job system still lists the job, Claude's EXPIRED becomes UNSURE.
  * Context Dev only when GitHub's browser cannot read the page, under a credit cap.
  * Memory: a page that has not changed since an earlier answer (same visible text) reuses that answer.
  * Midday: jobs answered this morning reuse the morning answer, except Claude's EXPIRED (not hard proof), which is
    checked again; only a second EXPIRED confirms it.

Usage: python scripts/claude_check.py <results_part.csv> <listings.json> <memory.json or -> <out.json>
Env: ANTHROPIC_KEY, CONTEXTDEV_KEY, SLOT (morning|midday|now), RUN_DATE (YYYY-MM-DD), CTX_CAP (credits for this
chunk, default 150), CLAUDE_LIMIT (max jobs, for tests), THREADS (default 4).
"""
import csv
import hashlib
import io
import json
import os
import re
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import anthropic
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

SLOT = os.environ.get("SLOT", "now")
RUN_DATE = os.environ.get("RUN_DATE") or date.today().isoformat()
CTX_CAP = int(os.environ.get("CTX_CAP") or 150)
LIMIT = int(os.environ.get("CLAUDE_LIMIT") or 0)
THREADS = int(os.environ.get("THREADS") or 4)
MODEL = "claude-haiku-5-5"
MEMORY_DAYS = 7
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36"
JOBSYS = re.compile(r"greenhouse\.io|ashbyhq\.com|lever\.co|workable\.com|smartrecruiters|teamtailor|recruitee|personio|"
                    r"bamboohr|pinpoint|hibob|myworkdayjobs|icims|jobvite|breezy", re.I)
MENU_ONLY = re.compile(r"menu|navigation|cookie|boilerplate|footer|only site|corporate site|no job (title|content)", re.I)

client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_KEY"].strip(), max_retries=4)
lock = threading.Lock()
stats = {"claude_in": 0, "claude_out": 0, "ctx_credits": 0, "ctx_refused": 0, "reused": 0, "checked": 0}
tls = threading.local()
RETRY = threading.Semaphore(2)


# ---------------------------------------------------------------- reading the page as a candidate sees it

def text_of(html, clean=True):
    soup = BeautifulSoup(html or "", "lxml")
    for t in soup(["script", "style", "noscript", "svg", "select", "option", "datalist"]):
        t.decompose()
    if clean:
        for t in soup(["nav", "header", "footer"]):
            t.decompose()
        for t in soup.find_all(attrs={"id": re.compile(r"cookie|consent|onetrust|gdpr|cky-|cmp", re.I)}):
            t.decompose()
        for t in soup.find_all(attrs={"class": re.compile(r"^(nav|navbar|menu|footer|site-nav|site-header|site-footer)([-_]|$)|"
                                                          r"[-_](nav|navbar|menu|footer)([-_]|$)|cookie|consent|onetrust|gdpr|^cky-|^cmp-", re.I)}):
            if len(t.get_text(" ", strip=True).split()) < 400:  # never drop a big block (it could hold the advert)
                t.decompose()
    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    return title, re.sub(r"\s+", " ", soup.get_text(" ", strip=True))


HIDE_JS = """() => { for (const el of Array.from(document.querySelectorAll('body *'))) {
  const cs = getComputedStyle(el);
  if (cs.display === 'none' || cs.visibility === 'hidden' || el.hidden) el.remove(); } }"""


def visible_html(fr):
    try:
        fr.evaluate(HIDE_JS)
    except Exception:  # noqa: BLE001
        pass
    return fr.content()


def browser():
    if not getattr(tls, "b", None):
        tls.p = sync_playwright().start()
        tls.b = tls.p.chromium.launch(headless=True)
    return tls.b


def render(url, wait_ms):
    """(status, final_url, title, text) as a candidate sees it, or status 0 when the page could not be opened."""
    ctx = None
    try:
        ctx = browser().new_context(user_agent=UA, locale="en-GB")
        pg = ctx.new_page()
        resp = pg.goto(url, timeout=35000, wait_until="domcontentloaded")
        pg.wait_for_timeout(wait_ms)
        try:
            for _ in range(4):
                pg.mouse.wheel(0, 2500)
                pg.wait_for_timeout(700)
        except Exception:  # noqa: BLE001
            pass
        if re.search(r"[?&](gh_jid|ashby_jid)=", url) or JOBSYS.search(pg.content()[:400000]):
            for _ in range(10):  # an embedded job frame can appear late
                if any(JOBSYS.search(f.url or "") for f in pg.frames[1:]):
                    break
                pg.wait_for_timeout(1000)
            if any(JOBSYS.search(f.url or "") for f in pg.frames[1:]):
                pg.wait_for_timeout(4000)
        extra = ""
        for f in pg.frames[1:]:
            try:
                if JOBSYS.search(f.url or ""):
                    _, ft = text_of(visible_html(f))
                    extra += ft + " "
            except Exception:  # noqa: BLE001
                pass
        title, text = text_of(visible_html(pg))
        if extra.strip():
            text = "[EMBEDDED JOB FRAME] " + extra + " [PAGE] " + text
        return (resp.status if resp else 0), pg.url, title, text, bool(extra.strip())
    except Exception:  # noqa: BLE001
        return 0, url, "", "", False
    finally:
        if ctx:
            try:
                ctx.close()
            except Exception:  # noqa: BLE001
                pass


def read_page(url):
    st, fin, title, text, framed = render(url, 6000)
    jid = re.search(r"[?&](gh_jid|ashby_jid)=", url)
    if st == 200 and (len(text.split()) < 150 or (jid and not framed)):  # slow page: one more try, longer, fewer at once
        with RETRY:
            r2 = render(url, 9000)
        if (jid and r2[4] and not framed) or len(r2[3].split()) > len(text.split()):
            st, fin, title, text, framed = r2
    return st, fin, title, text


def get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, b""
    except Exception:  # noqa: BLE001
        return 0, b""


def final_url(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.geturl()
    except Exception:  # noqa: BLE001
        return ""


def pdf_text(url):
    st, data = get(url, 40)
    if st != 200 or not data:
        return st, ""
    try:
        import pypdf
        return st, re.sub(r"\s+", " ", " ".join((p.extract_text() or "") for p in pypdf.PdfReader(io.BytesIO(data)).pages))
    except Exception:  # noqa: BLE001
        return st, ""


def context_dev(url):
    with lock:
        if stats["ctx_credits"] >= CTX_CAP:
            stats["ctx_refused"] += 1
            return "", ""
    body = json.dumps({"url": url, "formats": {"markdown": True}, "sharedParams": {"waitFor": 3000}, "maxAgeMs": 0,
                       "timeoutOpts": {"milliseconds": 60000, "behavior": "return-partial"},
                       "tags": ["expiry-checker"]}).encode()
    req = urllib.request.Request("https://api.context.dev/v1/web/scrape", data=body, method="POST",
                                 headers={"Authorization": "Bearer " + os.environ["CONTEXTDEV_KEY"].strip(),
                                          "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            d = json.loads(r.read() or b"{}")
    except Exception:  # noqa: BLE001
        return "", ""
    with lock:
        stats["ctx_credits"] += ((d.get("key_metadata") or {}).get("credits_consumed") or 0)
    return ((d.get("markdown") or {}).get("data") or ""), ((d.get("metadata") or {}).get("title") or "")


# ---------------------------------------------------------------- the job system's own data

def job_system(url):
    """("EXPIRED"|"LISTED"|"OWN_PAGE", reason) from the job system's data, or None. LISTED never makes a job active by itself:
    it only stops Claude's EXPIRED (safety net)."""
    u = url.strip()
    m = re.match(r"https?://([^.]+)\.(wd\d+)\.myworkdayjobs\.com/(?:[a-z]{2}-[A-Z]{2}/)?([^/]+)/job/(.+?)(?:\?.*)?$", u)
    if m:
        tenant, wd, site, path = m.groups()
        st, body = get(f"https://{tenant}.{wd}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/job/{path}")
        if st == 200 and b'"jobPostingInfo"' in body:
            info = json.loads(body).get("jobPostingInfo", {})
            if info.get("canApply") is False or info.get("jobPostingEndDate", "9999") < RUN_DATE:
                return "EXPIRED", "Workday says the job no longer accepts applications"
            return "LISTED", "Workday lists the job"
        if st in (404, 410):
            return "EXPIRED", f"Workday job data not found (HTTP {st})"
        return None
    m = re.match(r"https?://jobs\.(?:eu\.)?lever\.co/([^/]+)/([0-9a-f-]{36})", u)
    if m:
        st, _ = get(f"https://api.lever.co/v0/postings/{m.group(1)}/{m.group(2)}")
        return ("LISTED", "Lever lists the job") if st == 200 else ("EXPIRED", f"Lever job data not found (HTTP {st})") if st in (404, 410) else None
    m = re.match(r"https?://jobs\.ashbyhq\.com/([^/]+)/([0-9a-f-]{36})", u)
    if m:
        st, body = get(f"https://api.ashbyhq.com/posting-api/job-board/{m.group(1)}")
        if st == 200:
            ids = {j.get("id") for j in json.loads(body).get("jobs", [])}
            return ("LISTED", "listed on the Ashby job board") if m.group(2) in ids else ("EXPIRED", "not on the company's Ashby job board any more")
        return None
    m = re.match(r"https?://(?:jobs|careers)\.smartrecruiters\.com/([^/]+)/(\d+)", u)
    if m:
        st, _ = get(f"https://api.smartrecruiters.com/v1/companies/{m.group(1)}/postings/{m.group(2)}")
        return ("LISTED", "SmartRecruiters lists the job") if st == 200 else ("EXPIRED", f"SmartRecruiters job not found (HTTP {st})") if st in (404, 410) else None
    m = re.match(r"https?://(?:job-boards|boards)\.(?:eu\.)?greenhouse\.io/([^/]+)/jobs/(\d+)", u)
    gh = [(m.group(1), m.group(2))] if m else []
    if not gh:
        m = re.search(r"[?&]gh_jid=(\d+)", u)
        if m:
            st, html = get(u)
            html = html.decode("utf-8", "ignore")
            toks = list(dict.fromkeys(re.findall(r"(?:boards(?:-api)?\.greenhouse\.io/(?:v1/boards/|embed/job_board(?:/js)?\?for=)|"
                                                 r"job-boards\.(?:eu\.)?greenhouse\.io/(?:embed/job_app\?for=)?)([A-Za-z0-9_-]+)", html)))
            bm = re.search(r"[?&](?:board|for)=([A-Za-z0-9_-]+)", u)
            if bm:
                toks = [bm.group(1)] + [t for t in toks if t != bm.group(1)]
            gh = [(t, m.group(1)) for t in toks if t not in ("embed", "v1")]
    for board, jid in gh:
        for host in ("boards-api.greenhouse.io", "boards-api.eu.greenhouse.io"):
            s2, _ = get(f"https://{host}/v1/boards/{board}/jobs/{jid}")
            if s2 == 200:
                return "LISTED", "Greenhouse lists the job"
            if s2 == 404:
                s3, _ = get(f"https://{host}/v1/boards/{board}/jobs")
                if s3 == 200:
                    return "EXPIRED", "not on the company's Greenhouse board any more"
    # 26 Nov 2026: Workable and Pinpoint. The Slack agent left Bionic (Workable, job gone: link redirects to
    # ?not_found=true) and ParentPay (Pinpoint, job gone from the board) as "link broken", and Nasstar (Pinpoint,
    # still on the board and its advert shown at our link) as "link broken" too.
    m = re.match(r"https?://apply\.workable\.com/(?:([^/]+)/)?j/([0-9A-Fa-f]{6,12})", u)
    if m:
        acct, code = m.group(1), m.group(2).upper()
        if not acct:  # short link: the redirect names the account (or says not_found)
            fin = final_url(u)
            if "not_found=true" in fin:
                return "EXPIRED", "Workable says the job was not found (the link goes to the careers page with not_found)"
            am = re.match(r"https?://apply\.workable\.com/([^/?]+)/", fin)
            acct = am.group(1) if am else None
        if acct:
            st, _ = get(f"https://apply.workable.com/api/v2/accounts/{acct}/jobs/{code}")
            if st == 200:
                return "OWN_PAGE", "Workable lists the job and our link is its own page"
            if st == 404:
                return "EXPIRED", "not on the company's Workable job board any more"
        return None
    m = re.match(r"https?://([a-z0-9-]+)\.pinpointhq\.com/(?:[a-z]{2}(?:-[A-Z]{2})?/)?postings/([0-9a-f-]{36})", u)
    if m:
        st, body = get(f"https://{m.group(1)}.pinpointhq.com/postings.json")
        if st == 200:
            try:
                posts = json.loads(body).get("data", [])
            except ValueError:
                return None
            if posts and any(m.group(2) in (p.get("url") or p.get("path") or "") for p in posts):
                return "OWN_PAGE", "Pinpoint lists the job and our link is its own page"
            if posts:
                return "EXPIRED", "not on the company's Pinpoint job board any more"
        return None
    m = re.search(r"[?&]ashby_jid=([0-9a-f-]{36})", u)
    if m:
        st, html = get(u)
        for o in dict.fromkeys(re.findall(r"(?:jobs\.ashbyhq\.com/|organizationHostedJobsPageName[\\\"': ]+)([A-Za-z0-9_.-]+)", html.decode("utf-8", "ignore"))):
            s2, body = get(f"https://api.ashbyhq.com/posting-api/job-board/{o}")
            if s2 == 200:
                ids = {j.get("id") for j in json.loads(body).get("jobs", [])}
                return ("LISTED", "listed on the Ashby job board") if m.group(1) in ids else ("EXPIRED", "not on the company's Ashby job board any more")
    return None


# ---------------------------------------------------------------- Claude

SYS = """You check whether ONE job advert is still usable, for a UK jobs site. Candidates click OUR link, so judge ONLY
what the page at our link shows them.
You get the job title we listed, the link, and the visible text of the page at that link (rendered in a browser,
including any embedded job-system frame; menus and cookie text mostly removed).
Our job titles are STANDARDISED by us, and companies rename roles, so the page's title often differs from ours
("Account Manager" vs "Account Executive - Merlin ERP"; "Senior Counsel" vs "Associate General Counsel"). If the link is
one job's page (job id / slug) and the page shows a job advert there, it is THIS job even when the title differs.
Answer one of:
- "active": the page shows the job's advert: a job title AND a real job description (duties / requirements / about
  the role), with no closed notice. An application form alone is NOT an advert.
- "expired": the page clearly shows a closed / filled / no longer available / not found / 404 / deadline-passed notice.
- "link_broken": the page does not show a job advert to a candidate, but nothing says the job closed: only an
  application form (no description), a careers home, a search or list of jobs (even if this title is listed there),
  a department page, or an unrelated page.
- "unsure": blocked, login wall, captcha, the page is still loading or nearly empty (only menus / cookie text), the
  advert clearly sits in a widget that did not load, or the text is unreadable.
Reply JSON only: {"status": "active|expired|link_broken|unsure", "reason": "max 15 words"}"""


def ask(title, url, text, page_title):
    err = ""
    for _ in range(3):
        try:
            m = client.messages.create(model=MODEL, max_tokens=300, output_config={"effort": "low"}, system=SYS,
                                       messages=[{"role": "user", "content": f"Job title we listed: {title}\nLink: {url}\n"
                                                  f"Page title: {page_title}\n\nPAGE TEXT:\n{text[:15000]}"}])
            with lock:
                stats["claude_in"] += m.usage.input_tokens
                stats["claude_out"] += m.usage.output_tokens
            mm = re.search(r"\{[\s\S]*\}", "".join(b.text for b in m.content if b.type == "text"))
            if not mm:
                err = "no JSON in reply"
                continue
            d = json.loads(mm.group(0))
            if d.get("status") in ("active", "expired", "link_broken", "unsure"):
                return d
            err = "unknown status"
        except Exception as e:  # noqa: BLE001
            err = str(e)[:80]
    return {"status": "unsure", "reason": "check failed: " + err}


# ---------------------------------------------------------------- one job

def age_days(m):
    try:
        return (date.fromisoformat(RUN_DATE) - date.fromisoformat(m.get("date", ""))).days
    except ValueError:
        return 999


def reuse(m, why):
    with lock:
        stats["reused"] += 1
    return {k: m[k] for k in ("status", "reason", "how", "hard", "hash") if k in m} | {"reused": why}


def decide(url, title, mem=None):
    """{status, reason, how, hard, hash}. status: EXPIRED | ACTIVE | LINK_BROKEN | UNSURE.
    mem: an earlier answer for this link (memory between days), reused when the page has not changed."""
    mem = mem if (mem and mem.get("status") != "EXPIRED" and age_days(mem) <= MEMORY_DAYS) else None
    js = job_system(url)
    if js and js[0] == "EXPIRED":
        return {"status": "EXPIRED", "reason": js[1], "how": "job system data", "hard": True, "hash": ""}
    if re.search(r"\.pdf($|\?)", url, re.I):
        st, txt = pdf_text(url)
        if st in (404, 410):
            return {"status": "EXPIRED", "reason": f"PDF link returns HTTP {st}", "how": "page", "hard": True, "hash": ""}
        if len(txt.split()) >= 80:
            r = ask(title, url, "[a PDF job spec at our link]\n" + txt, "")
            return finish(r, "PDF + Claude", js, hashlib.sha1(txt.encode()).hexdigest())
    st, fin, ptitle, text = read_page(url)
    if st in (404, 410):
        return {"status": "EXPIRED", "reason": f"page returns HTTP {st}", "how": "page", "hard": True, "hash": ""}
    h = hashlib.sha1(text.encode()).hexdigest() if st == 200 else ""
    if mem and h and mem.get("hash") == h:
        return reuse(mem, "same page as on " + mem.get("date", ""))
    if st == 200 and len(text.split()) >= 150:
        r = ask(title, url, f"[final link: {fin}]\n" + text, ptitle)
        return finish(r, "browser + Claude", js, h)
    if mem and mem.get("how") == "Context Dev + Claude" and age_days(mem) <= 2:
        return reuse(mem, "blocked page, Context Dev answer from " + mem.get("date", ""))
    md, mtitle = context_dev(url)
    if not md.strip():
        return {"status": "UNSURE", "reason": "page could not be read", "how": "page", "hard": False, "hash": h}
    r = ask(title, url, md, mtitle)
    return finish(r, "Context Dev + Claude", js, h)


def finish(r, how, js, h):
    status, reason = r.get("status", "unsure").upper(), r.get("reason", "")
    if status == "EXPIRED" and MENU_ONLY.search(reason):  # a page with only menus: the advert may not have loaded
        status, reason = "UNSURE", "page showed only site menus: " + reason
    if status == "EXPIRED" and js and js[0] in ("LISTED", "OWN_PAGE"):  # safety net
        status, reason = "UNSURE", f"{js[1]}, but the page did not show it: " + reason
    elif status in ("LINK_BROKEN", "UNSURE") and js and js[0] == "OWN_PAGE":
        # the job system itself says our link is this open job's own page, so a candidate sees the advert there
        # (Nasstar 26 Nov: Claude read Pinpoint's careers list around the advert as "link broken")
        status, reason = "ACTIVE", f"{js[1]} ({reason})"
    return {"status": status, "reason": reason, "how": how, "hard": False, "hash": h}


# ---------------------------------------------------------------- main

def main():
    results_csv, listings_json, memory_json, out_json = sys.argv[1:5]
    with open(results_csv, newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r.get("status") == "CHECK_MANUALLY" and (r.get("url") or "").strip()]
    titles = {}
    for l in json.load(open(listings_json, encoding="utf-8")):
        titles.setdefault(l["url"].strip(), l.get("position") or "")
    memory = json.load(open(memory_json, encoding="utf-8")) if memory_json != "-" and os.path.exists(memory_json) else {}
    urls = list(dict.fromkeys(r["url"].strip() for r in rows))
    if LIMIT:
        urls = urls[:LIMIT]
    print(f"CHECK_MANUALLY URLs in this chunk: {len(urls)} | memory entries: {len(memory)} | slot {SLOT}", flush=True)

    out = {}

    def one(u):
        m = memory.get(u)
        # midday: this morning's answer stands, except Claude's EXPIRED (not hard proof), which is checked again
        if m and SLOT == "midday" and m.get("date") == RUN_DATE and m.get("slot") == "morning":
            if not (m["status"] == "EXPIRED" and not m.get("hard")):
                with lock:
                    stats["reused"] += 1
                return u, {**m, "reused": "this morning"}
            r = decide(u, titles.get(u, ""))
            r["confirm"] = "CONFIRMED" if r["status"] == "EXPIRED" else "NOT_CONFIRMED"
            return u, {**r, "date": RUN_DATE, "slot": SLOT, "morning": m.get("reason", "")}
        r = decide(u, titles.get(u, ""), m)
        with lock:
            stats["checked"] += 1
        return u, {**r, "date": RUN_DATE, "slot": SLOT}

    t0 = time.time()
    with ThreadPoolExecutor(THREADS) as ex:
        for i, (u, r) in enumerate(ex.map(one, urls), 1):
            out[u] = r
            if i % 50 == 0:
                print(f"  {i}/{len(urls)} done, {round((time.time() - t0) / 60, 1)} min", flush=True)
                json.dump(out, open(out_json, "w"))
    json.dump(out, open(out_json, "w"))
    cost = stats["claude_in"] / 1e6 * 1.0 + stats["claude_out"] / 1e6 * 5.0
    counts = {}
    for r in out.values():
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print(json.dumps({"jobs": len(out), "answers": counts, "reused": stats["reused"], "claude_cost_usd": round(cost, 2),
                      "context_dev_credits": stats["ctx_credits"], "context_dev_refused_by_cap": stats["ctx_refused"],
                      "minutes": round((time.time() - t0) / 60, 1)}))


if __name__ == "__main__":
    main()
