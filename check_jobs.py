import argparse
import asyncio
import csv
import functools
import logging
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import requests


# Suppress noisy asyncio warnings on Windows
logging.getLogger("asyncio").setLevel(logging.CRITICAL)


DEFAULT_SHEET_WEBHOOK = "https://script.google.com/macros/s/AKfycbyc9vzS3o-X0QAgjJy0wG5e2l5sNWYStpZd0B-S39DqIlTkX4nVy0gRDLeW0oLI9LHp/exec"
DEFAULT_SHEET_NAME = "Results"


# =======================================================================
# EXPIRED patterns — used for VISIBLE TEXT and PAGE TITLE only.
# =======================================================================
EXPIRED_STRONG_PATTERNS = [
    # "position/job ... no longer available/closed/expired" — WITHOUT "filled"
    # "filled" is handled separately below to avoid "until the position is filled"
    r"\b(job|position|vacanc(?:y|ies)|posting)\b.{0,40}\b(no longer available|closed|expired)\b",
    # "filled" requires definitive past tense: "has been filled" or "was filled"
    r"\b(job|position|vacanc(?:y|ies)|posting)\b.{0,20}\b(has been|was)\s+filled\b",
    r"\b(no longer available|position filled|posting closed|job closed)\b",
    r"\bthis (job|position|vacancy|posting) (is )?(no longer available|closed|filled|expired)\b",
    r"\bapplications?\s+(are\s+)?closed\b",
    r"\bjob not found\b",
    r"\bposition not found\b",
    r"\bposting not found\b",
    r"\bthis posting does not exist\b",
    r"\bwe couldn'?t find\b.{0,30}\b(job|position|posting)\b",
    r"\bpage not found\b",
    r"\b404\s+(error|not found|page)\b",
    r"\berror\s+404\b",
    r"\bhttp\s+404\b",
    r"\bthis (role|opportunity) (is )?(no longer|not) (available|open|active)\b",
    r"\bsorry.{0,30}(job|position|posting|role|vacancy).{0,30}(not found|no longer|doesn'?t exist|does not exist|removed|expired|been closed)\b",
    r"\bthis (job|position|posting|role) has been (closed|filled|removed|archived)\b",
    r"\b(job|position|posting|role|vacancy) (has been|was) (removed|deleted|archived|withdrawn)\b",
    r"\bno (open )?(positions?|jobs?|roles?|vacancies) (found|available|match)\b",
    r"\bthis opportunity is closed\b",
    r"\bthis vacancy has (been )?(closed|expired)\b",
    r"\bapplication deadline has passed\b",
    r"\bno results found\b.{0,20}\b(job|position|role)\b",
    r"\brole (is )?(no longer|not) (available|open|active|accepting)\b",
    r"\bthe link you followed may be (broken|expired|outdated)\b",
    r"\bthis page (doesn'?t|does not) exist\b",
    r"\bopportunity (is )?(no longer|not) (available|open)\b",
    r"\bwe (are|'re) (sorry|unable).{0,30}(find|locate).{0,30}(job|position|posting|role)\b",
    r"\bthis listing (is )?(no longer|not) (available|active)\b",
    r"\bjob (has been|is) (expired|closed|removed)\b",
    r"\bposition (has been|is) (expired|closed|removed|filled)\b",
    r"\bthis requisition (is )?(no longer|not) (available|open|active)\b",
    r"\bapplications?\s+(for\s+this\s+(job|role|position|vacancy)\s+)?(are\s+)?no longer (accepted|being accepted|open)\b",
    r"\bno longer accepting applications\b",
    r"\bthis (job|role|position|vacancy) is no longer accepting applications\b",
    r"\bapplications?\s+closed\b",
    r"\bapplication\s+period\s+(has\s+)?(ended|closed|expired)\b",
    r"\bsubmissions?\s+(are\s+)?(now\s+)?closed\b",
    r"\bthis role has been filled\b",
    r"\bwe are no longer accepting\b",
    r"\brecruiting for this (job|role|position) (is|has) (closed|ended|been completed)\b",
]

# Phrases that should PREVENT an expired match when they appear nearby.
EXPIRED_FALSE_POSITIVE_GUARDS = [
    r"\buntil\b.{0,30}\bfilled\b",
    r"\bwhen\b.{0,30}\bfilled\b",
    r"\bonce\b.{0,30}\bfilled\b",
    r"\bopen\s+until\b",
    r"\baccepted\s+.{0,20}until\b",
    r"\bwill remain open\b",
    r"\buntil\s+(the\s+)?(position|role|job)\s+is\s+filled\b",
]

# =======================================================================
# Separate, STRICTER patterns for scanning API/JSON response bodies.
# =======================================================================
API_EXPIRED_PATTERNS = [
    r'"(job|position|posting|requisition|vacancy)[_-]?(status|state)"\s*:\s*"(expired|closed|inactive|not[_ ]found|gone|removed|archived|filled)"',
    r'"(status|state)"\s*:\s*"(expired|not[_ ]found|gone|removed|archived)"',
    r'"(status|state|job_status|jobStatus|positionStatus|posting_status)"\s*:\s*"closed"',
    r'"(message|error_message|detail|msg)"\s*:\s*"[^"]{0,50}(no longer available|position.{0,10}(closed|filled|expired)|job.{0,10}(closed|expired|removed)|posting.{0,10}(closed|expired|removed))',
    r'"(is_active|isActive|job_active|jobActive|position_active)"\s*:\s*false\b',
    r'"(is_expired|isExpired|job_expired|jobExpired|job_closed|isClosed|position_closed)"\s*:\s*true\b',
    r'\bthis (job|position|posting|role) (is )?(no longer available|closed|filled|expired)\b',
    r'\bapplications?\s+(for\s+this\s+(job|role|position)\s+)?(are\s+)?no longer (accepted|being accepted)\b',
    r'\bposition not found\b',
    r'\bjob not found\b',
    r'\bposting not found\b',
]

EXPIRED_WEAK_PATTERNS = [
    r"\bno longer available\b",
    r"\bnot available\b",
    r"\bnot found\b",
    r"\bno longer accepting\b",
]

APPLY_PATTERNS = [
    r"\bapply\b",
    r"apply now",
    r"submit application",
    r"start application",
]

JOB_BODY_CUES = [
    r"\bresponsibilit(?:y|ies)\b",
    r"\brequirements?\b",
    r"\bqualifications?\b",
    r"\babout (the )?role\b",
    r"\bjob description\b",
    r"\bthe role\b",
    r"\bwhat you(?:'|')ll do\b",
    r"\bwhat we(?:'|')re looking for\b",
    r"\bkey responsibilities\b",
    r"\bessential (skills|experience)\b",
]

BOILERPLATE_CUES = [
    r"\bcookies?\b",
    r"\bprivacy\b",
    r"\bterms\b",
    r"\baccessibility\b",
    r"\baccept all\b",
    r"\bmanage preferences\b",
    r"\blog in\b",
    r"\bsign in\b",
    r"\bregister\b",
    r"\bcontact us\b",
    r"\bskip to\b",
    r"\bpowered by\b",
    r"\ball rights reserved\b",
    r"\bloading\b",
    r"\benable javascript\b",
]

JS_SHELL_HINTS_IN_HTML = [
    "__NEXT_DATA__",
    "webpack",
    "data-reactroot",
    "id=\"root\"",
    "id=\"app\"",
]

FORCE_PLAYWRIGHT_DOMAINS = [
    "pinpointhq.com",
    "careers.hsbc.com",
    "portal.careers.hsbc.com",
    "eightfold.ai",
    "jobs.elastic.co",
    "griddynamics.com",
    "careers.dazn.com",
    "movar.group",
    "myworkdayjobs.com",
    "wd3.myworkdayjobs.com",
    "wd5.myworkdayjobs.com",
    "taleo.net",
    "oracle.com",
    "icims.com",
    "lever.co",
    "jobs.lever.co",
    "greenhouse.io",
    "boards.greenhouse.io",
    "smartrecruiters.com",
    "jobs.smartrecruiters.com",
    "ashbyhq.com",
    "jobs.ashbyhq.com",
    "breezy.hr",
    "recruitee.com",
    "workable.com",
    "apply.workable.com",
    "bamboohr.com",
    "jazz.co",
    "applytojob.com",
    "jobvite.com",
    "successfactors.com",
    "phenom.com",
    "avature.net",
    "ultipro.com",
    "cornerstoneondemand.com",
]

LISTING_PAGE_TITLE_PATTERNS = [
    r"^search results\b",
    r"^job search\b",
    r"^careers?\b.*search",
    r"^open (positions|roles|jobs|vacancies)\b",
    r"^all (jobs|positions|roles|vacancies)\b",
    r"^job (listings?|openings?|board)\b",
    r"^current (openings?|vacancies|opportunities)\b",
    r"^(find|browse) (jobs|careers|opportunities)\b",
    r"\bsearch results\b",
]

APPLY_BY_DATE_PATTERNS = [
    r"(?:apply\s+by|closing\s+date|application\s+deadline|deadline|closes?(?:\s+on)?|end\s+date|expiry\s+date|expires?\s+on|applications?\s+close)\s*[:\-]?\s*(\d{1,2}\s+\w+\s+\d{4})",
    r"(?:apply\s+by|closing\s+date|application\s+deadline|deadline|closes?(?:\s+on)?|end\s+date|expiry\s+date|expires?\s+on|applications?\s+close)\s*[:\-]?\s*(\w+\s+\d{1,2},?\s+\d{4})",
    r"(?:apply\s+by|closing\s+date|application\s+deadline|deadline|closes?(?:\s+on)?|end\s+date|expiry\s+date|expires?\s+on|applications?\s+close)\s*[:\-]?\s*(\d{4}-\d{2}-\d{2})",
    r"(?:apply\s+by|closing\s+date|application\s+deadline|deadline|closes?(?:\s+on)?|end\s+date|expiry\s+date|expires?\s+on|applications?\s+close)\s*[:\-]?\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})",
]

DATE_PARSE_FORMATS = [
    "%d %B %Y", "%d %b %Y", "%B %d, %Y", "%B %d %Y",
    "%b %d, %Y", "%b %d %Y", "%Y-%m-%d", "%d/%m/%Y",
    "%m/%d/%Y", "%d-%m-%Y",
]

API_SCAN_SKIP_EXTENSIONS = (
    ".js", ".css", ".woff", ".woff2", ".ttf", ".eot",
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".webp",
    ".map",
)
API_SCAN_SKIP_PATHS = (
    "/locales/", "/locale/", "/i18n/", "/lang/",
    "/config", "/appconfig", "/app-config",
    "/widget", "/chatbot", "/chat/",
    "/builds/meta/", "/_nuxt/",
    "/analytics", "/tracking", "/telemetry",
    "/cdn-cgi/",
    "/consent", "/cookie", "/onetrust", "/osano", "/cookiebot",
    "/gdpr", "/privacy", "/optanon",
    "/theme", "/settings/", "/preferences",
    "/embed/", "/badge", "/beacon", "/livechat", "/intercom",
    "/hubspot", "/drift", "/zendesk", "/freshchat",
    "/recaptcha", "/captcha", "/turnstile",
)

# Performance-only PW blocking configuration
PW_BLOCK_RESOURCE_TYPES = {"image", "font", "media"}
PW_BLOCK_URL_SUBSTRINGS = [
    "google-analytics.com", "googletagmanager.com", "doubleclick.net",
    "segment.com", "cdn.segment.com", "hotjar.com", "fullstory.com",
    "intercom.io", "sentry.io", "rollbar.com", "datadoghq.com",
    "newrelic.com", "snap.licdn.com", "connect.facebook.net",
    "bat.bing.com", "clarity.ms", "optimizely.com"
]


# =====================================================================
# Helper functions
# =====================================================================

def _domain_needs_playwright(url):
    try:
        netloc = urlparse(url).netloc.lower()
    except Exception:
        return False
    for d in FORCE_PLAYWRIGHT_DOMAINS:
        if netloc == d or netloc.endswith("." + d):
            return True
    return False


def normalize_space(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def word_count(s):
    return len(re.findall(r"[A-Za-z]{2,}", s or ""))


def _find_first_match(patterns, haystack):
    for pat in patterns:
        m = re.search(pat, haystack, flags=re.IGNORECASE)
        if m:
            return m.group(0)
    return None


def _find_expired_match_with_guard(text):
    lower = (text or "").lower()
    for pat in EXPIRED_STRONG_PATTERNS:
        m = re.search(pat, lower, flags=re.IGNORECASE)
        if m:
            start = max(0, m.start() - 120)
            end = min(len(lower), m.end() + 120)
            context = lower[start:end]
            for guard in EXPIRED_FALSE_POSITIVE_GUARDS:
                if re.search(guard, context, flags=re.IGNORECASE):
                    return None
            return m.group(0)
    return None


def cleaned_job_text(text):
    lower = (text or "").lower()
    cleaned = lower
    for pat in BOILERPLATE_CUES:
        cleaned = re.sub(pat, " ", cleaned)
    return normalize_space(cleaned)


def has_jobposting_structured_data(raw_html):
    if not raw_html:
        return False
    h = raw_html.lower()
    return ("jobposting" in h) and ("application/ld+json" in h or "schema.org" in h)


def content_looks_real_job(text, title, raw_html):
    lower = (text or "").lower()
    lower_title = (title or "").lower()
    if _find_first_match(JOB_BODY_CUES, lower) or _find_first_match(JOB_BODY_CUES, lower_title):
        return True
    if has_jobposting_structured_data(raw_html):
        return True
    cleaned = cleaned_job_text(text)
    return word_count(cleaned) >= 220


def looks_like_shell_http(raw_html, text):
    t = normalize_space(text or "")
    h = (raw_html or "")
    if len(t) < 300:
        return True
    if h.count("<script") >= 6:
        return True
    if any(x in h for x in JS_SHELL_HINTS_IN_HTML):
        return True
    if re.search(r"\bapply\b", (text or ""), flags=re.IGNORECASE) and not _find_first_match(JOB_BODY_CUES, (text or "").lower()):
        return True
    return False


def _detect_redirect_to_listing(original_url, final_url):
    if not original_url or not final_url:
        return False
    try:
        orig_parsed = urlparse(original_url.strip().rstrip("/"))
        final_parsed = urlparse(final_url.strip().rstrip("/"))
        if orig_parsed.netloc.lower() != final_parsed.netloc.lower():
            return False
        orig_path = orig_parsed.path.rstrip("/").lower()
        final_path = final_parsed.path.rstrip("/").lower()
        if orig_path == final_path:
            return False
        if len(final_path) < len(orig_path) and orig_path.startswith(final_path):
            return True
        redirect_indicators = [
            "/search", "/results", "/all-jobs", "/open-positions",
            "/job-search", "/opportunities", "/listings", "/search-results",
        ]
        for indicator in redirect_indicators:
            if indicator in final_path and indicator not in orig_path:
                return True
        orig_segments = [s for s in orig_path.split("/") if s]
        final_segments = [s for s in final_path.split("/") if s]
        if len(orig_segments) > len(final_segments) + 1:
            return True
    except Exception:
        return False
    return False


def _title_looks_like_listing_page(title, original_url):
    if not title or not original_url:
        return False
    lower_title = title.lower().strip()
    is_listing_title = False
    for pat in LISTING_PAGE_TITLE_PATTERNS:
        if re.search(pat, lower_title, flags=re.IGNORECASE):
            is_listing_title = True
            break
    if not is_listing_title:
        return False
    path = urlparse(original_url).path
    has_job_id = bool(re.search(r"/(\d{3,}|[0-9a-f]{8}-[0-9a-f]{4})", path, flags=re.IGNORECASE))
    return has_job_id


def _parse_date_flexible(date_str):
    date_str = date_str.strip().rstrip(".")
    for fmt in DATE_PARSE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def _check_apply_by_date(text):
    if not text:
        return False, None
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    for pat in APPLY_BY_DATE_PATTERNS:
        for m in re.finditer(pat, text, flags=re.IGNORECASE):
            raw_date = m.group(1).strip()
            parsed = _parse_date_flexible(raw_date)
            if parsed and parsed < today:
                return True, m.group(0).strip()
    return False, None


def _should_scan_api_body(url, content_type):
    rurl = (url or "").lower()
    ct = (content_type or "").lower()
    if "json" not in ct:
        return False
    if any(rurl.endswith(ext) for ext in API_SCAN_SKIP_EXTENSIONS):
        return False
    if any(skip in rurl for skip in API_SCAN_SKIP_PATHS):
        return False
    if "cdn." in rurl or ".cdn." in rurl:
        return False
    if "/embed/" in rurl and ("/js" in rurl or "job_board" in rurl):
        return False
    return True


def extract_visible_text(raw_html):
    soup = BeautifulSoup(raw_html or "", "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    title = soup.title.string.strip() if soup.title and soup.title.string else ""
    text = soup.get_text(" ", strip=True)
    return title, text


def classify(status_code, title, text, final_url, raw_html,
             render_meta=None, original_url=None):

    lower = (text or "").lower()
    lower_title = (title or "").lower()

    if status_code in (404, 410):
        return ("EXPIRED", f"HTTP {status_code}", "Page not found (expired/removed).")
    if status_code in (401, 403):
        return ("CHECK_MANUALLY", f"HTTP {status_code}", "Access blocked/restricted.")
    if status_code and status_code >= 500:
        return ("CHECK_MANUALLY", f"HTTP {status_code}", "Server error; retry later/manual check.")

    strong = _find_expired_match_with_guard(text) or _find_expired_match_with_guard(title)
    if strong:
        return ("EXPIRED", f"Expired phrase found: {strong}", "Page indicates job is closed/expired.")

    if render_meta:
        if render_meta.get("main_status") in (404, 410):
            return ("EXPIRED", f"Rendered main HTTP {render_meta['main_status']}", "Rendered page not found (expired/removed).")

        api_hits = render_meta.get("api_expired_hits") or []
        if api_hits:
            return ("EXPIRED", f"Expired phrase found in API: {api_hits[0]}", "Underlying job API indicates closed/expired.")

        if render_meta.get("redirect_detected"):
            return ("EXPIRED", "Redirected away from job page",
                    "Page redirected to job listing/search — job likely removed.")

    if original_url and final_url and _detect_redirect_to_listing(original_url, final_url):
        return ("EXPIRED", "Redirected away from job page",
                "Page redirected to job listing/search — job likely removed.")

    if _title_looks_like_listing_page(title, original_url or ""):
        return ("EXPIRED", f"Page title is a listing page: '{title.strip()}'",
                "URL was for a specific job but page shows search/listing view — job likely removed.")

    date_expired, date_match = _check_apply_by_date(text or "")
    if date_expired:
        return ("EXPIRED", f"Apply-by date in the past: {date_match}",
                "Application deadline has passed.")

    if word_count(lower) < 120:
        return ("CHECK_MANUALLY", "Page text very short / empty", "No reliable job content detected.")

    apply_hit = any(re.search(p, lower) for p in APPLY_PATTERNS)
    real_job = content_looks_real_job(text, title, raw_html or "")

    if apply_hit and not real_job:
        weak = _find_first_match(EXPIRED_WEAK_PATTERNS, lower) or _find_first_match(EXPIRED_WEAK_PATTERNS, lower_title)
        if weak:
            return ("CHECK_MANUALLY", f"Apply seen but job body missing; weak expired wording: {weak}",
                    "Likely closed/empty page; confirm manually.")
        return ("CHECK_MANUALLY", "Apply seen but job body missing", "Template/apply text present but no reliable job body found.")

    if apply_hit and real_job:
        return ("ACTIVE", "Apply + job body", "Job body appears present and apply action exists.")

    if not apply_hit and not real_job:
        weak = _find_first_match(EXPIRED_WEAK_PATTERNS, lower) or _find_first_match(EXPIRED_WEAK_PATTERNS, lower_title)
        if weak:
            return ("CHECK_MANUALLY", f"Weak expired wording found: {weak}", "Wording suggests closed but needs confirmation.")
        return ("CHECK_MANUALLY", "No apply + no job body", "No reliable job content detected.")

    return ("CHECK_MANUALLY", "Insufficient evidence", "Could not confidently determine status.")


# =====================================================================
# Playwright rendering — SYNCHRONOUS, runs in a thread pool
# =====================================================================

_thread_local = threading.local()


def _get_thread_browser():
    if not hasattr(_thread_local, "pw"):
        _thread_local.pw = sync_playwright().start()
        _thread_local.browser = _thread_local.pw.chromium.launch(headless=True)
    return _thread_local.browser


def _close_thread_browser():
    if hasattr(_thread_local, "browser"):
        try:
            _thread_local.browser.close()
        except Exception:
            pass
    if hasattr(_thread_local, "pw"):
        try:
            _thread_local.pw.stop()
        except Exception:
            pass
        try:
            del _thread_local.pw
            del _thread_local.browser
        except Exception:
            pass


def playwright_render_sync(url, timeout_s=25, extra_wait_ms=3000):
    browser = _get_thread_browser()

    api_expired_hits = []
    main_status = None

    page = browser.new_page()
    try:
        # Performance-only: block images/fonts/media + common trackers
        def route_handler(route, request):
            try:
                rtype = (request.resource_type or "").lower()
                rurl = (request.url or "").lower()

                if rtype in PW_BLOCK_RESOURCE_TYPES:
                    route.abort()
                    return

                if any(s in rurl for s in PW_BLOCK_URL_SUBSTRINGS):
                    route.abort()
                    return

                route.continue_()
            except Exception:
                # safest fallback: try to continue
                try:
                    route.continue_()
                except Exception:
                    pass

        try:
            page.route("**/*", route_handler)
        except Exception:
            pass

        def on_response(resp):
            nonlocal api_expired_hits
            try:
                ct = (resp.headers.get("content-type") or "").lower()
                rurl = resp.url or ""

                if _should_scan_api_body(rurl, ct):
                    try:
                        body = resp.text()
                        if body and len(body) <= 5000:
                            hit = _find_first_match(API_EXPIRED_PATTERNS, body.lower())
                            if hit:
                                api_expired_hits.append(hit)
                    except Exception:
                        pass
            except Exception:
                return

        page.on("response", on_response)

        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
            if resp:
                main_status = resp.status
        except Exception:
            pass

        page.wait_for_timeout(extra_wait_ms)

        try:
            page.wait_for_load_state("networkidle", timeout=5000)
        except Exception:
            pass

        page.wait_for_timeout(1000)

        title = ""
        try:
            title = page.title()
        except Exception:
            pass

        text = ""
        for selector in ["main", "[role='main']", "#content", ".content", "#app", "#root", "body"]:
            try:
                el = page.query_selector(selector)
                if el:
                    text = el.inner_text()
                    text = normalize_space(text)
                    if word_count(text) > 30:
                        break
            except Exception:
                continue

        if not text or word_count(text) < 30:
            try:
                text = page.inner_text("body")
            except Exception:
                text = ""

        text = normalize_space(text)[:25000]
        final_url = page.url
        redirect_detected = _detect_redirect_to_listing(url, final_url)

        meta = {
            "api_expired_hits": api_expired_hits[:5],
            "main_status": main_status,
            "redirect_detected": redirect_detected,
        }
        return {
            "main_status": main_status,
            "final_url": final_url,
            "title": title,
            "text": text,
            "meta": meta,
        }
    finally:
        try:
            page.close()
        except Exception:
            pass


# =====================================================================
# Async HTTP + main logic
# =====================================================================

async def fetch_http(session, url, timeout_s):
    async with session.get(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
        status_code = resp.status
        final_url = str(resp.url)
        content_type = (resp.headers.get("Content-Type") or "").lower()
        raw = await resp.text(errors="ignore") if ("text" in content_type or "html" in content_type or content_type == "") else ""
        title, text = "", ""
        if raw:
            title, text = extract_visible_text(raw)
            text = text[:20000]
        return status_code, final_url, title, text, raw


async def run_playwright_in_thread(executor, url, timeout_s, extra_wait_ms=3000):
    loop = asyncio.get_running_loop()
    result = await asyncio.wait_for(
        loop.run_in_executor(
            executor,
            functools.partial(playwright_render_sync, url, timeout_s, extra_wait_ms)
        ),
        timeout=timeout_s + 20
    )
    return result


async def check_one(url, session, executor, timeout_s, counter):
    checked_at = datetime.now(timezone.utc).isoformat()
    url = (url or "").strip()
    domain = urlparse(url).netloc if url else ""

    counter["done"] += 1
    idx = counter["done"]
    total = counter["total"]

    if not url:
        print(f"  [{idx}/{total}] (empty URL) -> CHECK_MANUALLY")
        return {"url": url, "final_url": "", "domain": domain, "status_code": "",
                "status": "CHECK_MANUALLY", "evidence": "Empty URL",
                "reason": "No URL provided.", "checked_at": checked_at}

    force_pw = _domain_needs_playwright(url)

    try:
        if force_pw:
            try:
                pw = await run_playwright_in_thread(executor, url, timeout_s, extra_wait_ms=3000)
                status, evidence, reason = classify(
                    pw["main_status"], pw["title"], pw["text"], pw["final_url"], None,
                    render_meta=pw["meta"], original_url=url
                )
                print(f"  [{idx}/{total}] {domain} -> {status} (PW forced)")
                return {
                    "url": url, "final_url": pw["final_url"],
                    "domain": urlparse(pw["final_url"]).netloc or domain,
                    "status_code": pw["main_status"] or "", "status": status,
                    "evidence": f"Forced-render ({domain}): {evidence}",
                    "reason": reason, "checked_at": checked_at
                }
            except Exception as pw_err:
                print(f"  [{idx}/{total}] {domain} -> PW failed ({type(pw_err).__name__}), trying HTTP.")

        status_code, final_url, title, text, raw_html = await asyncio.wait_for(
            fetch_http(session, url, timeout_s),
            timeout=timeout_s + 5
        )

        if raw_html and looks_like_shell_http(raw_html, text):
            try:
                pw = await run_playwright_in_thread(executor, url, timeout_s)
                status, evidence, reason = classify(
                    pw["main_status"], pw["title"], pw["text"], pw["final_url"], None,
                    render_meta=pw["meta"], original_url=url
                )
                print(f"  [{idx}/{total}] {domain} -> {status} (PW rendered)")
                return {
                    "url": url, "final_url": pw["final_url"],
                    "domain": urlparse(pw["final_url"]).netloc or domain,
                    "status_code": pw["main_status"] or "", "status": status,
                    "evidence": f"Rendered: {evidence}", "reason": reason, "checked_at": checked_at
                }
            except Exception:
                pass

        status, evidence, reason = classify(
            status_code, title, text, final_url, raw_html,
            render_meta=None, original_url=url
        )
        print(f"  [{idx}/{total}] {domain} -> {status} (HTTP)")
        return {
            "url": url, "final_url": final_url, "domain": urlparse(final_url).netloc or domain,
            "status_code": status_code or "", "status": status,
            "evidence": evidence, "reason": reason, "checked_at": checked_at
        }

    except asyncio.TimeoutError:
        print(f"  [{idx}/{total}] {domain} -> CHECK_MANUALLY (Timeout)")
        return {"url": url, "final_url": "", "domain": domain, "status_code": "",
                "status": "CHECK_MANUALLY", "evidence": "Timeout",
                "reason": "Request timed out.", "checked_at": checked_at}
    except Exception as e:
        print(f"  [{idx}/{total}] {domain} -> CHECK_MANUALLY ({type(e).__name__})")
        return {"url": url, "final_url": "", "domain": domain, "status_code": "",
                "status": "CHECK_MANUALLY", "evidence": "Fetch error",
                "reason": f"{type(e).__name__}: {e}", "checked_at": checked_at}


# =====================================================================
# CSV / Sheets helpers
# =====================================================================

def read_urls_from_csv(path):
    if str(path).lower().endswith(".txt"):
        with open(path, "r", encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip()]

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        col = None
        for c in reader.fieldnames or []:
            if c and c.lower() == "url":
                col = c
                break
        if not col:
            raise ValueError("CSV must have a column named url (case-insensitive).")
        return [((row.get(col) or "").strip()) for row in reader if (row.get(col) or "").strip()]


def load_done_urls(out_csv):
    done = set()
    if not os.path.exists(out_csv) or os.path.getsize(out_csv) == 0:
        return done
    try:
        with open(out_csv, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                u = (row.get("url") or "").strip()
                if u:
                    done.add(u)
    except Exception:
        return set()
    return done


def append_rows(out_csv, rows, fieldnames):
    file_exists = os.path.exists(out_csv) and os.path.getsize(out_csv) > 0
    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            w.writeheader()
        w.writerows(rows)


def push_rows_to_sheets(webhook_url, sheet_name, results):
    rows = [[r.get("url", ""), r.get("final_url", ""), r.get("domain", ""), r.get("status_code", ""),
             r.get("status", ""), r.get("evidence", ""), r.get("reason", ""), r.get("checked_at", "")]
            for r in results]
    payload = {"sheet": sheet_name, "rows": rows}
    resp = requests.post(webhook_url, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()


# =====================================================================
# Batch processing and main
# =====================================================================

async def process_batch(urls, concurrency, timeout_s, pw_threads):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    connector = aiohttp.TCPConnector(limit_per_host=concurrency, ssl=False, force_close=True)
    sem = asyncio.Semaphore(concurrency)
    counter = {"done": 0, "total": len(urls)}

    executor = ThreadPoolExecutor(max_workers=pw_threads)

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        async def bound(u):
            async with sem:
                return await check_one(u, session, executor, timeout_s, counter)

        tasks = [bound(u) for u in urls]
        results = []
        for coro in asyncio.as_completed(tasks):
            results.append(await coro)

    futs = []
    for _ in range(pw_threads):
        futs.append(executor.submit(_close_thread_browser))
    for f in futs:
        try:
            f.result(timeout=10)
        except Exception:
            pass
    executor.shutdown(wait=False)

    return results


async def main_async(input_csv, out_csv, concurrency, timeout_s,
                     batch_size, resume, webhook_url, sheet_name, pw_threads):
    urls_all = read_urls_from_csv(input_csv)
    done = load_done_urls(out_csv) if resume else set()
    urls = [u for u in urls_all if u not in done]

    print(f"Total URLs in input: {len(urls_all)}")
    if resume:
        print(f"Already in output (skipped): {len(done)}")
    print(f"Remaining to process: {len(urls)}")
    print(f"Batch size: {batch_size} | Concurrency: {concurrency} | Timeout: {timeout_s}s | PW threads: {pw_threads}")

    fieldnames = ["url", "final_url", "domain", "status_code", "status", "evidence", "reason", "checked_at"]

    total = len(urls)
    processed = 0
    batch_num = 0

    for start in range(0, total, batch_size):
        batch_num += 1
        batch = urls[start:start + batch_size]
        print(f"\n--- Batch {batch_num} ({len(batch)} URLs) ---")

        results = await process_batch(batch, concurrency, timeout_s, pw_threads)

        append_rows(out_csv, results, fieldnames)
        processed += len(batch)
        print(f"\nSaved batch {batch_num}. Progress: {processed}/{total}. Output: {out_csv}")

        if webhook_url:
            try:
                js = push_rows_to_sheets(webhook_url, sheet_name, results)
                appended = js.get("appended", "unknown")
                print(f"Appended to Google Sheet '{sheet_name}': {appended} rows")
            except Exception as e:
                print(f"WARNING: Failed to append to Google Sheets: {type(e).__name__}: {e}")
                print("Continuing; CSV is saved locally.")


def main():
    ap = argparse.ArgumentParser(description="Job URL Checker — detects expired/active job postings")
    ap.add_argument("input_csv")
    ap.add_argument("-o", "--out", default="results.csv")
    ap.add_argument("-c", "--concurrency", type=int, default=3)
    ap.add_argument("-t", "--timeout", type=int, default=25)
    ap.add_argument("-b", "--batch-size", type=int, default=300)
    ap.add_argument("--pw-threads", type=int, default=2,
                    help="Number of Playwright browser threads (default: 2)")
    ap.add_argument("--resume", action="store_true")
    ap.add_argument("--sheet-webhook", default=DEFAULT_SHEET_WEBHOOK)
    ap.add_argument("--sheet-name", default=DEFAULT_SHEET_NAME)

    args = ap.parse_args()
    webhook = (args.sheet_webhook or "").strip()
    if webhook == "":
        webhook = None

    asyncio.run(main_async(args.input_csv, args.out, args.concurrency, args.timeout, args.batch_size,
                           args.resume, webhook, args.sheet_name, args.pw_threads))


if __name__ == "__main__":
    main()
