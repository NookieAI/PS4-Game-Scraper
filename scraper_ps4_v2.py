"""
scraper_ps4_v2.py — dlpsgame.com PS4 scraper (fast edition)
============================================================

DROP-IN REPLACEMENT for scraper.py. Output schema is 100% identical —
games_cache.json produced here loads cleanly in any code that reads the old one.

WHERE THE SPEED COMES FROM
──────────────────────────
Old scraper bottlenecks and exactly how each is eliminated:

  1. _wait_for_secure_data() polled JS for up to 6 s per page.
     NEW: .secure-data divs contain data-payload="BASE64" in the *static*
     server-rendered HTML. driver.page_source has it before JS even runs.
     Python decodes base64 in ~1 ms. No JS wait at all.
     Fallback: if JS already cleared the attribute, read innerHTML via JS
     (same as old scraper, but only triggered ~20 % of the time).

  2. fetch_filehosts_from_intermediary() navigated the BROWSER to each
     downloadgameps3.net/archives/XXXXX page, waited CF (up to 40 s),
     then slept SLEEP_INTERMEDIARY=15 s. Sequential, in browser.
     NEW: downloadgameps3.net has no Cloudflare. A requests.Session with
     Chrome headers fetches each page in ~0.5–2 s. All intermediary pages
     for one game are fetched in parallel via ThreadPoolExecutor. Total: ~2–4 s.

  3. Image downloads were sequential via the browser. 5 screenshots × 4 s = 20 s.
     NEW: blogspot/blogger images downloaded in parallel via requests threads.
     wp-content images fetched in a single browser Promise.all() JS call (batch).
     Total: ~3–6 s regardless of count.

  4. SLEEP_BETWEEN_GAMES was 45 s. Code comments say "CF detects via fingerprints
     not session duration". undetected_chromedriver handles fingerprinting.
     NEW: 8 s. Enough for human-like pacing without dominating runtime.

  5. Intermediary fetching was synchronous — browser blocked until all resolved.
     NEW: Pipeline. After parsing page_source, all I/O work is submitted to
     thread pools and runs DURING the 8-s sleep. Collection is near-instant.

  Estimated per-game time:
    Old: ~80–150 s (45 s sleep + sequential intermediaries in browser)
    New: ~15–25 s  (8 s sleep + parallel I/O in background)
    Speedup: ~5–8×
    6 000 games: ~28–42 hours vs ~7–10 days

ARCHITECTURE
────────────
  Browser thread (main):
    ① driver.get(game_url)           ← only dlpsgame.com, CF handled by uc
    ② wait_for_dlpsgame()
    ③ jitter(2, 0.3)                 ← human pause (was 3 s)
    ④ page_src = driver.page_source
    ⑤ decode payloads (Python)       ← instant, replaces JS wait
    ⑥ extract_metadata(soup)         ← pure HTML parse
    ⑦ submit intermediary jobs → _inter_pool  (requests, parallel)
    ⑧ batch-download wp-content images via browser Promise.all()
    ⑨ submit blogspot image jobs → _img_pool  (requests, parallel)
    ⑩ jitter(8, 0.3)                 ← inter + images finish here
    ⑪ collect Future results
    ⑫ save cache

  _inter_pool (daemon threads):
    requests.get(downloadgameps3.net/...) → parse HTML → filehost links

  _img_pool (daemon threads):
    requests.get(blogspot_url) → write PNG/JPG to disk

USAGE
─────
  python scraper_ps4_v2.py

  Uses the same games.json / games_cache.json / screenshots/ as scraper.py.
"""

# ── IMPORTS ───────────────────────────────────────────────────────────────────
import json
import time
import base64
import re
import os
import traceback
import html as _html
from concurrent.futures import ThreadPoolExecutor, Future, as_completed, wait as fw_wait, ALL_COMPLETED
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from bs4 import BeautifulSoup
import requests
import undetected_chromedriver as uc

# ── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_JSON      = "games.json"
OUTPUT_JSON     = "games_cache.json"
SCREENSHOTS_DIR = "screenshots"

# ── TIMING ────────────────────────────────────────────────────────────────────
# CF detects bots via request fingerprints (TLS, JS behaviour, UA), not timing.
# undetected_chromedriver handles fingerprinting. 8 s is enough human pacing.
SLEEP_BETWEEN_GAMES = 8     # ← was 45 s — the biggest single speedup
SLEEP_AFTER_LOAD    = 2     # brief pause after page loads (was 3 s)
SLEEP_REVEAL        = 2     # after clicking a reveal button (was 5 s)
CF_TIMEOUT          = 40    # max seconds to wait for CF challenge to clear

# ── THREAD POOLS ─────────────────────────────────────────────────────────────
# Both are long-lived module-level pools, created once and shared across all games.
# Intermediary fetches: 8 workers — one game has at most ~6–8 inter links
# Image downloads:      6 workers — cover + up to 5 screenshots in parallel
_INTER_WORKERS = 2      # ← was 8. downloadgameps3.net rate-limits hard; 2 is safe
_IMG_WORKERS   = 6

# ── HTTP CONFIG FOR REQUESTS (intermediary + images) ─────────────────────────
FETCH_TIMEOUT = 25   # seconds, per request via requests library
IMG_TIMEOUT   = 30

import random as _random

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
]
SESSION_UA    = _random.choice(_USER_AGENTS)
FETCH_HEADERS = {"User-Agent": SESSION_UA}

# Shared requests.Session for intermediary + image fetches.
# Created once so connections are reused (HTTP keep-alive).
_req_session = requests.Session()
_req_session.headers.update({
    "User-Agent":      SESSION_UA,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
})

# ── INTERMEDIARY RATE LIMITER ─────────────────────────────────────────────────
# downloadgameps3.net enforces strict rate limits (CF Error 1015 = temp IP ban).
# This lock + timestamp ensures at least _INTER_MIN_GAP seconds between any two
# requests to that domain, regardless of how many threads are running.
# Combined with _INTER_WORKERS=2 this keeps us well within their limits.
import threading as _threading
_inter_lock      = _threading.Lock()
_inter_last_req  = [0.0]          # mutable container so threads share it
_INTER_MIN_GAP   = 2.5            # minimum seconds between intermediary fetches
_1015_BACKOFF    = 120            # seconds to sleep when Error 1015 is detected

def _inter_rate_limit():
    """Block until it's safe to make another intermediary request."""
    with _inter_lock:
        gap  = time.time() - _inter_last_req[0]
        wait = _INTER_MIN_GAP - gap
        if wait > 0:
            time.sleep(wait)
        _inter_last_req[0] = time.time()

def _is_1015(text: str) -> bool:
    """Return True if the response body/title contains a CF Error 1015 page."""
    t = text.lower()
    return ("error 1015" in t or
            "rate limited" in t and "cloudflare" in t or
            "you are being rate limited" in t)

def _is_cf_challenge(text: str) -> bool:
    """Return True if the response body is a Cloudflare JS/Turnstile challenge page
    (i.e. requests got a 200 but the actual content was not delivered).
    CF serves these as HTTP 200 so raise_for_status() doesn't catch them.

    Uses only high-specificity signals that are absent on real CF-protected pages:
    - Title "Just a moment..." is only set on challenge pages
    - challenge-form / cf-browser-verification only appear in challenge flows
    Note: cdn-cgi/challenge-platform and __CF$cv$params appear on ALL CF sites.
    """
    t = text.lower()
    # "Just a moment..." title — the single most reliable CF challenge signal
    if "<title>just a moment" in t:
        return True
    # CF challenge form present (not the beacon script)
    if 'id="challenge-form"' in t or 'class="cf-browser-verification"' in t:
        return True
    # Turnstile / JS challenge markers
    if "cf_chl_opt" in t or "cf_chl_prog" in t:
        return True
    return False


CATEGORY_URLS       = ["https://dlpsgame.com/category/ps4/"]
MAX_DISCOVERY_PAGES = 400   # site has ~313 PS4 pages; 400 gives safe headroom

# ── DOMAIN LISTS ──────────────────────────────────────────────────────────────
FILEHOST_DOMAINS = [
    "mediafire.com", "pixeldrain.com",
    "mega.nz", "mega.co.nz",
    "1fichier.com", "gofile.io", "uptobox.com",
    "krakenfiles.com", "letsupload.io", "anonfiles.com",
    "mixdrop.co", "rapidgator.net", "zippyshare.com",
    "bayfiles.com", "racaty.net",
    "vikingfile.com", "viki.gg",
    "akirabox.com", "akirabox.to",
    "send.cm", "drive.google.com", "sbfull.com",
    "katfile.com", "filecrypt.cc", "filecrypt.co",
    "usersdrive.com", "dropapk.to", "workupload.com",
    "filerio.in", "terabox.com", "fenixx.org",
    "rootz.so", "rootz.to", "dropbox.com",
    "ranoz.gg", "ranoz.to",
    "transfer.it",
    "1file.io", "1cloudfile.com",
    "buzzheavier.com",
    "qiwi.gg",
]
INTERMEDIARY_DOMAINS = ["downloadgameps3.net/archives"]
SHORTENER_DOMAINS    = ["shrinkearn.com", "shrinkme.io", "ouo.io", "clk.sh",
                        "fc.lc", "short2win.com", "adfly"]
GUIDE_DOMAINS        = ["downloadgameps3.com"]

# Domains that consistently block plain requests (CF challenges the requests
# session but passes the UC browser which has solved CF already).
# URLs on these domains go straight to browser, skipping requests entirely.
BROWSER_ONLY_INTER_DOMAINS = ["downloadgameps3.net"]

_URL_SCAN_RE = re.compile(r'https?://\S+')

# ── BASIC URL HELPERS ─────────────────────────────────────────────────────────
def is_filehost_url(href):   return any(d in href.lower() for d in FILEHOST_DOMAINS)
def is_intermediary_url(h):  return any(d in h.lower()    for d in INTERMEDIARY_DOMAINS)
def is_shortener_url(href):  return any(d in href.lower() for d in SHORTENER_DOMAINS)
def is_guide_url(href):      return any(d in href.lower() for d in GUIDE_DOMAINS)
def is_browser_only_inter(url: str) -> bool:
    """True for inter domains that always block requests — use browser directly."""
    return any(d in url.lower() for d in BROWSER_ONLY_INTER_DOMAINS)

def decode_shortener_url(href):
    try:
        params = parse_qs(urlparse(href).query)
        for key in ["url", "link", "target", "dest", "redirect"]:
            if key in params:
                b64 = params[key][0]
                b64 += "=" * (4 - len(b64) % 4)
                decoded = base64.b64decode(b64).decode("utf-8", errors="replace")
                if decoded.startswith("http"):
                    return decoded
    except Exception:
        pass
    return None

def resolve_href(href):
    if is_shortener_url(href):
        real = decode_shortener_url(href)
        if real:
            return real
    return href

# ── TIMING ────────────────────────────────────────────────────────────────────
def jitter(base, variance=0.4, minimum=1.0):
    lo = max(minimum, base * (1.0 - variance))
    hi = base * (1.0 + variance)
    time.sleep(_random.uniform(lo, hi))

# ── AD-TEXT SCRUBBING ─────────────────────────────────────────────────────────
_AD_RE = re.compile(
    r"uploaded\s+by\s+dlpsgame\.com"
    r"|for\s+the\s+latest\s+updates[^\n]*"
    r"|(?:www\.)?dlpsgame\.com"
    r"|have\s+fun\s*!?"
    r"|_{3,}"          # separator lines like _________________________
    r"|-{3,}"          # separator lines like -------------------------
    r"|\*{3,}",        # separator lines like *************************
    re.I,
)

def _strip_ad_text(text):
    if not text:
        return text
    cleaned = _AD_RE.sub("", text)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned if cleaned else None

# ── BROWSER WAIT HELPERS ──────────────────────────────────────────────────────
def wait_for_cf(driver, timeout=CF_TIMEOUT, require_selector=None):
    """Wait for Cloudflare challenge to clear. Returns True if cleared."""
    for _ in range(timeout):
        t = driver.title.lower()
        if "just a moment" not in t and "cloudflare" not in t:
            if require_selector:
                if driver.find_elements("css selector", require_selector):
                    return True
            else:
                return True
        time.sleep(1)
    print("    [WARN] CF timeout")
    return False

def wait_for_dlpsgame(driver):
    return wait_for_cf(driver,
                       require_selector=".post-body.entry-content, h1.post-title, .post")

# ── ★ FAST PAYLOAD DECODER ────────────────────────────────────────────────────
def decode_payloads_from_page_source(page_src: str) -> list[str]:
    """
    Extract and decode all .secure-data payloads from the raw static HTML.

    The site JS does:
        div.innerHTML = decodeURIComponent(atob(data-payload))
    which is just base64 UTF-8. We replicate this in Python in ~1 ms,
    eliminating the old _wait_for_secure_data() JS polling loop (up to 6 s).

    The data-payload attribute lives in the server-rendered HTML. It's present
    in driver.page_source unless JS already ran and removed it. Fallback path
    (get_payload_htmls) handles that case.
    """
    results = []
    for m in re.finditer(r'data-payload="([^"]+)"', page_src):
        encoded = m.group(1)
        pad = (4 - len(encoded) % 4) % 4
        try:
            decoded = base64.b64decode(encoded + "=" * pad).decode("utf-8", errors="replace")
            if len(decoded) > 10:
                results.append(decoded)
        except Exception:
            pass
    return results

def expand_su_spoilers(driver) -> int:
    """
    Open any closed Shortcodes Ultimate spoilers that contain a .secure-data
    div, then force-decode any .secure-data elements whose data-payload is
    still present (i.e. the site's own decode script skipped them because they
    were hidden).

    Recent dlpsgame.com posts wrap the download section in a collapsible
    su-spoiler block.  The site's secure-data decode script only runs on
    *visible* elements, so the payload inside a closed spoiler is never
    decoded before Selenium reads page_source.  This function:

      1. Clicks the title of every closed su-spoiler that has a .secure-data
         child, making the content visible.
      2. Waits up to 1 s for the site's own decode script to fire.
      3. Falls back to a direct JS atob() decode for any still-encoded divs
         (in case the site script still doesn't fire).

    Returns the number of spoilers that were opened.
    """
    try:
        opened = driver.execute_script("""
            var opened = 0;
            // Find every closed spoiler whose content contains a secure-data div
            var spoilers = document.querySelectorAll('.su-spoiler-closed');
            for (var i = 0; i < spoilers.length; i++) {
                var sp = spoilers[i];
                if (!sp.querySelector('.secure-data')) continue;
                // Click the title to open it
                var title = sp.querySelector('.su-spoiler-title');
                if (title) { title.click(); opened++; }
            }
            return opened;
        """) or 0

        if opened:
            print(f"    su-spoiler: opened {opened} closed spoiler(s) — waiting for decode")
            import time; time.sleep(1)  # give the site's decode script time to run

        # Force-decode any .secure-data divs that still have data-payload
        # (covers the case where the site script still skips them)
        force_decoded = driver.execute_script("""
            var count = 0;
            var divs = document.querySelectorAll('.secure-data[data-payload]');
            for (var i = 0; i < divs.length; i++) {
                try {
                    divs[i].innerHTML = decodeURIComponent(atob(divs[i].getAttribute('data-payload')));
                    divs[i].removeAttribute('data-payload');
                    count++;
                } catch(e) {}
            }
            return count;
        """) or 0

        if force_decoded:
            print(f"    su-spoiler: force-decoded {force_decoded} payload(s) via JS atob()")

        return opened

    except Exception as e:
        print(f"    [WARN] expand_su_spoilers: {e}")
        return 0


# Placeholder text the site shows while decode is pending — never a real payload
_SECURE_DATA_PLACEHOLDER = "đang giải mã nội dung"


# ── ★ CDP PAYLOAD INTERCEPTOR ────────────────────────────────────────────────
# clk.sh/full-page-script.js is a BLOCKING (no async/defer) script that runs
# during HTML parsing — before Selenium gets control.  It:
#   1. Decodes data-payload via atob()
#   2. Sets element.innerHTML to the decoded HTML
#   3. Wraps every href matching adlinkfly_domains (mediafire, downloadgameps3.net,
#      filecrypt, mega, 1fichier, akirabox, vikingfile, rootz, etc.) with a
#      clk.sh redirect URL
#   4. Removes the data-payload attribute
#
# By the time wait_for_dlpsgame returns, data-payload is gone and all links
# point to clk.sh instead of the real hosts.
#
# Fix: use CDP Page.addScriptToEvaluateOnNewDocument to inject a hook that runs
# BEFORE any page script.  It intercepts innerHTML assignments on elements that
# still have data-payload, capturing the raw base64 before clk.sh removes it.
# The captured values are stored in window.__dlps_raw_payloads.

_CDP_PAYLOAD_HOOK = """
window.__dlps_raw_payloads = [];
(function() {
    var _desc = Object.getOwnPropertyDescriptor(Element.prototype, 'innerHTML');
    if (!_desc || !_desc.set) return;
    Object.defineProperty(Element.prototype, 'innerHTML', {
        set: function(val) {
            try {
                var p = this.getAttribute && this.getAttribute('data-payload');
                if (p && window.__dlps_raw_payloads.indexOf(p) === -1) {
                    window.__dlps_raw_payloads.push(p);
                }
            } catch(e) {}
            _desc.set.call(this, val);
        },
        get: _desc.get,
        configurable: true,
        enumerable: true
    });
})();
"""

def install_payload_interceptor(driver) -> bool:
    """
    Inject the CDP payload hook so it runs before any page script on every
    subsequent navigation.  Call once after driver creation.

    Returns True on success, False if CDP is unavailable (non-Chrome driver).
    """
    try:
        driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": _CDP_PAYLOAD_HOOK},
        )
        print("[cdp] Payload interceptor installed (captures data-payload before clk.sh)")
        return True
    except Exception as e:
        print(f"[cdp] WARNING: could not install payload interceptor: {e}")
        return False


def get_payload_htmls(driver, page_src: str) -> list[str]:
    """
    Get decoded secure-data payload HTML strings.

    Strategy 1 (fast, ~1 ms): read data-payload from static page_source, decode Python.
    Strategy 2 (fallback, JS): read innerHTML via JS if data-payload already removed
                                by the client-side decode script.

    Note: expand_su_spoilers() must be called BEFORE this function when the
    page may have closed su-spoiler elements (see scrape_page / patch_entry).

    Returns a list of decoded HTML strings — same as old extract_intermediary_urls
    received from its JS call, so all downstream parsing is identical.

    Priority order:
      1. CDP interceptor (window.__dlps_raw_payloads) — captures raw base64
         before clk.sh wraps all hrefs with redirect URLs.  Most reliable.
      2. Static page_source scan — works on the rare page where JS hasn't run.
      3. JS innerHTML fallback — last resort; hrefs may be clk.sh-wrapped.
    """
    # ── Strategy 1: CDP-captured raw payloads (pre-clk.sh) ───────────────────
    try:
        captured = driver.execute_script(
            "return window.__dlps_raw_payloads || [];"
        ) or []
        if captured:
            decoded_list = []
            for enc in captured:
                pad = (4 - len(enc) % 4) % 4
                try:
                    decoded = base64.b64decode(
                        enc + "=" * pad
                    ).decode("utf-8", errors="replace")
                    if len(decoded) > 10:
                        decoded_list.append(decoded)
                except Exception:
                    pass
            if decoded_list:
                print(f"    secure-data: {len(decoded_list)} payload(s) decoded"
                      f" via CDP interceptor (pre-clk.sh)")
                return decoded_list
    except Exception:
        pass  # CDP not available or page hasn't navigated yet

    # ── Strategy 2: data-payload in static page_source ────────────────────────
    payloads = decode_payloads_from_page_source(page_src)
    if payloads:
        print(f"    secure-data: {len(payloads)} payload(s) decoded from static HTML")
        return payloads

    # ── Strategy 3: JS innerHTML fallback ─────────────────────────────────────
    # clk.sh already ran and replaced data-payload with decoded+wrapped HTML.
    # The hrefs in innerHTML may point to clk.sh redirects, not original hosts.
    try:
        htmls = driver.execute_script("""
            var divs = document.querySelectorAll('.secure-data');
            var out = [];
            for (var i = 0; i < divs.length; i++) out.push(divs[i].innerHTML);
            return out;
        """) or []
        # Filter out the Vietnamese "decoding…" placeholder
        valid = [
            h for h in htmls
            if h
            and len(h) > 10
            and _SECURE_DATA_PLACEHOLDER not in h.lower()
        ]
        if valid:
            print(f"    secure-data: {len(valid)} div(s) read via JS fallback"
                  f" (clk.sh-wrapped hrefs possible)")
        else:
            print("    secure-data: 0 payloads found")
        return valid
    except Exception as e:
        print(f"    [WARN] secure-data JS exec: {e}")
        return []

# ── PARAGRAPH CLASSIFIER ──────────────────────────────────────────────────────
def classify_paragraph(text):
    """
    Classify a <p> tag's text into link roles: game / update / dlc / backport.
    Identical to scraper.py — no changes.
    """
    if ":" not in text:
        return []

    header     = text.split(":", 1)[0].strip()
    header_low = header.lower()
    roles      = []

    # Combo: "Game + Update 1.55 + DLC : links"
    if "+" in header and any(kw in header_low for kw in ("game", "update", "backport", "dlc")):
        tokens = [t.strip() for t in header.split("+")]
        for token in tokens:
            tlow = token.lower()
            if tlow.startswith("game"):
                roles.append({"role": "game", "version": "", "label": token})
            elif tlow.startswith("update"):
                vm = re.search(r'update\s+([\d\.]+)(.*)', token, re.I)
                if vm:
                    ver   = vm.group(1)
                    extra = vm.group(2).strip().strip("()")
                    label = f"v{ver}" + (f" {extra}" if extra else "")
                    roles.append({"role": "update", "version": ver, "label": label})
            elif "dlc" in tlow:
                roles.append({"role": "dlc", "version": "DLC", "label": token})
            elif tlow.startswith("backport"):
                vm = re.search(r'backport\s+([\d\.]+xx|[\d\.]+)', token, re.I)
                ver = vm.group(1) if vm else ""
                lbl = re.sub(r'\s*\(@[^)]+\)', '', token).strip()
                roles.append({"role": "backport", "version": ver, "label": lbl})
        if roles:
            return roles

    if header_low.startswith("update"):
        vm = re.search(r'update\s+([\d\.]+)(.*)', header, re.I)
        if vm:
            ver   = vm.group(1)
            extra = vm.group(2).strip().strip("()")
            label = f"v{ver}" + (f" {extra}" if extra else "")
            return [{"role": "update", "version": ver, "label": label}]
        return [{"role": "update", "version": "unknown", "label": header}]

    if "dlc" in header_low:
        vm = re.search(r'dlc\s+(v[\w\.]+)', header, re.I) or \
             re.search(r'dlc\s+([\w\.]+)', header_low)
        ver = ("DLC-" + vm.group(1)) if vm else "DLC"
        return [{"role": "dlc", "version": ver, "label": header}]

    if header_low.startswith("backport"):
        vm = re.search(r'backport\s+([\d\.]+xx|[\d\.]+)', header, re.I)
        ver = vm.group(1) if vm else ""
        lbl = re.sub(r'\s*\(@[^)]+\)', '', header).strip()
        return [{"role": "backport", "version": ver, "label": lbl}]

    if "game" in header_low:
        return [{"role": "game", "version": "", "label": header}]

    return []

# ── ★ RELEASE EXTRACTOR (HTML-ONLY, NO DRIVER) ───────────────────────────────
def extract_releases_from_htmls(payload_htmls: list[str],
                                 page_src: str) -> tuple[list, dict]:
    """
    Parse decoded payload HTML strings → structured releases + global metadata.
    Replaces extract_intermediary_urls(driver) — takes already-decoded HTML.
    Logic is identical; the only change is the input (pre-decoded HTML vs live DOM).
    """
    releases     = []
    seen_urls    = set()
    global_extra = {"cusa_ids": []}

    # ── Comprehensive game-ID patterns ────────────────────────────────────────
    # PS4/PS5 native  : CUSA, PPSA, LAPY, LBXP, STRN → cusa_ids / cusa_id
    # Legacy platform : SCES/SLES/SCUS/SLUS (PS1), BLES/BLUS/BCES/BCUS/BCAS/BCJS,
    #                   NPUB/NPEB/NPJA/NPJB (PS3/PSP) → _ps_legacy_id
    #
    # \d{3,6}: pages sometimes show TRUNCATED IDs (e.g. "CUSA6929" when the
    # actual PKG file is "CUSA46929"). The PKG-filename scan below is the
    # authoritative override for the correct digit count.
    _GAMEID_RE = re.compile(
        r"\b((?:CUSA|PPSA|LAPY|LBXP|STRN|"
        r"SCES|SLES|SCUS|SLUS|BLES|BLUS|BCES|BCUS|BCAS|BCJS|"
        r"NPUB|NPEB|NPJA|NPJB)\d{3,6})\b",
        re.IGNORECASE,
    )
    # Optional region that follows an ID  e.g. "– EUR" / "- USA" / "– HKG"
    _REGION_AFTER_RE = re.compile(
        r"[\u2013\u2014\-]\s*(USA|EUR|JPN|JAP|JP|ASIA|UK|HKG|HK|KOR|KR|CHN|AU|AUS|INT)",
        re.IGNORECASE,
    )
    # PKG / RAR / ZIP filenames embed the REAL authoritative ID:
    #   EP8709-CUSA46929_00-A0100-V0100.pkg  →  CUSA46929
    #   Base-CUSA12345-game.rar              →  CUSA12345
    #   LAPY00123-game.pkg                   →  LAPY00123
    _PKG_FILENAME_ID_RE = re.compile(
        r"(?:\b[A-Z]{2}\d{4}[-_])?(CUSA\d{4,6}|PPSA\d{4,6}|LAPY\d{3,6}|LBXP\d{3,6}|STRN\d{3,6})"
        r"(?:[_\-\.]|\b)",
        re.IGNORECASE,
    )
    # PS4/PS5 prefixes (native content IDs → stored in cusa_ids)
    _PS4PS5_PREFIXES = frozenset({"CUSA", "PPSA", "LAPY", "LBXP", "STRN"})

    def _register_id(raw_id: str, region: str = ""):
        """Add a game ID to global_extra, routing PS4/PS5 vs legacy."""
        raw_id  = raw_id.upper().strip()
        region  = region.upper().strip()
        prefix  = re.match(r"[A-Z]+", raw_id)
        if not prefix:
            return
        pfx = prefix.group()
        if pfx in _PS4PS5_PREFIXES:
            gentry = {"cusa": raw_id, "region": region}
            if gentry not in global_extra["cusa_ids"]:
                global_extra["cusa_ids"].append(gentry)
            if "cusa_id" not in global_extra:
                global_extra["cusa_id"] = raw_id
                global_extra["region"]  = region
        else:
            # Legacy platform ID — only record the first one found
            if "_ps_legacy_id" not in global_extra:
                global_extra["_ps_legacy_id"] = raw_id
                print(f"    [legacy-ps-id] {raw_id}")

    def _apply_game_ids(text: str):
        """
        Extract ALL game IDs from raw text or HTML.

        Works on both plain text and raw HTML (strips tags first).
        Uses d{3,6} to catch truncated page IDs and all known prefixes.
        """
        # Strip HTML tags while preserving their text content so that
        # span-obfuscated IDs like CU<span>SA</span>6929 are collapsed
        # into CUSA6929 (NOT "CU SA 6929" which the old get_text(" ") produced)
        clean = _html.unescape(re.sub(r"<[^>]+>", "", text))
        for m in _GAMEID_RE.finditer(clean):
            raw_id = m.group(1)
            # Try to grab the optional region that immediately follows
            tail   = clean[m.end(): m.end() + 25]
            rm     = _REGION_AFTER_RE.match(tail)
            region = rm.group(1) if rm else ""
            _register_id(raw_id, region)

    def _apply_pkg_filename_ids(text: str):
        """
        Scan raw href / URL text for PKG-filename embedded IDs.

        These are the AUTHORITATIVE IDs — e.g. EP8709-CUSA46929_00 tells us
        the real ID is CUSA46929, even if the page text shows CUSA6929.
        Upgrades an already-found truncated text ID to the full one if found.
        """
        for m in _PKG_FILENAME_ID_RE.finditer(text):
            raw_id = m.group(1).upper()
            existing = [e["cusa"] for e in global_extra["cusa_ids"]]
            # If a shorter version of this ID is already stored, replace it
            for i, eid in enumerate(existing):
                if raw_id.endswith(eid[len(re.match(r'[A-Z]+', eid).group()):]):
                    # e.g. stored "CUSA6929", found "CUSA46929" — upgrade
                    if len(raw_id) > len(eid):
                        global_extra["cusa_ids"][i]["cusa"] = raw_id
                        if global_extra.get("cusa_id") == eid:
                            global_extra["cusa_id"] = raw_id
                        return
            _register_id(raw_id)

    # ── Global ID scan ────────────────────────────────────────────────────────
    # 1. Decoded payload HTML (inside encrypted divs — most reliable text source)
    _apply_game_ids(" ".join(payload_htmls))
    # 2. PKG filenames in ALL hrefs across payload HTML (authoritative IDs)
    _apply_pkg_filename_ids(" ".join(payload_htmls))
    # 3. Raw page source as fallback (catches IDs outside encrypted divs)
    if not global_extra["cusa_ids"]:
        _apply_game_ids(page_src)
    if not global_extra["cusa_ids"]:
        _apply_pkg_filename_ids(page_src)
    # 4. If still nothing PS4/PS5, set _no_cusa only if no legacy ID found either
    if not global_extra.get("cusa_id") and not global_extra.get("_ps_legacy_id"):
        # Don't set _no_cusa yet — the per-payload per-paragraph scan below
        # and the URL-filename scan may still find an ID.
        pass

    if global_extra["cusa_ids"]:
        print(f"    [game-ids] {[c['cusa']+(('-'+c['region']) if c['region'] else '') for c in global_extra['cusa_ids']]}")

    _STRUCTURED_STARTS = (
        "thank", "password", "voice", "audio",
        "subtitle", "screen lang", "language", "languages",
        "game size", "backport", "game", "update", "dlc",
    )

    for html in payload_htmls:
        if not html or len(html) < 10:
            continue

        sub = BeautifulSoup(html, "html.parser")
        contributor = ""; cusa = ""; region = ""; password = ""
        release_notes = []

        for p in sub.find_all("p"):
            text = p.get_text(" ", strip=True)
            low  = text.lower()

            if low.startswith("thank") and not contributor:
                contributor = re.sub(r"(?i)^thank\s+", "", text).strip()

            # ── Per-paragraph ID scan ─────────────────────────────────────────
            # IMPORTANT: use tag-stripped raw HTML (not get_text(" ")) to
            # collapse span-obfuscated IDs like CU<span>SA</span>6929 → CUSA6929.
            # get_text(" ") would produce "CU SA 6929" which breaks all regexes.
            p_raw = str(p)
            _apply_game_ids(p_raw)
            _apply_pkg_filename_ids(p_raw)
            # Also scan all hrefs inside this paragraph for PKG filenames
            for pa in p.find_all("a", href=True):
                _apply_pkg_filename_ids(pa["href"])
            # Update per-release cusa/region from first match found in this payload
            if not cusa and global_extra.get("cusa_id"):
                cusa   = global_extra["cusa_id"]
                region = global_extra.get("region", "")

            if "password" in low and len(text) < 80 and not password:
                password = re.split(r"password\s*:?\s*", text, flags=re.I, maxsplit=1)[-1].strip()

            # Global shared metadata — first seen wins
            if (low.startswith("voice") or low.startswith("audio")) \
                    and "voice" not in global_extra:
                global_extra["voice"] = text.split(":", 1)[-1].strip()
            if (low.startswith("subtitle") or low.startswith("screen lang")) \
                    and "subtitles" not in global_extra:
                global_extra["subtitles"] = text.split(":", 1)[-1].strip()
            if (low.startswith("language") or low.startswith("languages")) \
                    and len(text) < 200 and "language" not in global_extra:
                global_extra["language"] = text.split(":", 1)[-1].strip()
            if (low.startswith("note") or low.startswith("notes")) \
                    and "note" not in global_extra and len(text) < 300:
                body = _strip_ad_text(text.split(":", 1)[-1].strip())
                if body and body.lower() not in ("here", ""):
                    global_extra["note"] = body
            if low.startswith("game size") and "game_size" not in global_extra:
                gm = re.search(r'[\u2013\u2014\-:]+\s*(.+)$', text)
                if gm:
                    global_extra["game_size"] = gm.group(1).strip()
            if ("dlc" in low or "addon" in low or "add-on" in low) \
                    and ":" not in text and len(text) > 15 \
                    and not text.startswith("Thank") \
                    and "dlc_note" not in global_extra:
                global_extra["dlc_note"] = text.strip()

            # Per-release notes
            if len(release_notes) < 5:
                if (low.startswith("note") or low.startswith("notes")) and len(text) < 400:
                    body = _strip_ad_text(text.split(":", 1)[-1].strip())
                    if body and body.lower() not in ("here", ""):
                        release_notes.append(body)
                elif (
                    15 < len(text) < 400
                    and not any(low.startswith(p) for p in _STRUCTURED_STARTS)
                    and not text.startswith("(")
                    and not _URL_SCAN_RE.search(text)
                    and ":" not in text[:40]
                    and "guide" not in low
                    and not re.match(r'(?:CUSA|PPSA|LAPY|LBXP|STRN|SCES|SLES|SCUS|SLUS|BLES|NPUB)\d{3,6}', text, re.I)
                ):
                    cleaned = _strip_ad_text(text)
                    if cleaned:
                        release_notes.append(cleaned)

        # ── Per-payload links ─────────────────────────────────────────────────
        game_inter    = []
        game_direct   = []
        update_inter  = []
        update_direct = {}

        for a in sub.find_all("a", href=True):
            raw   = a["href"].strip()
            label = a.get_text(strip=True)
            real  = resolve_href(raw)

            # Every link href is a potential source of authoritative PKG IDs
            _apply_pkg_filename_ids(raw)
            if real != raw:
                _apply_pkg_filename_ids(real)

            if is_guide_url(real) or not real or real in seen_urls:
                continue

            parent_p    = a.find_parent("p")
            parent_text = parent_p.get_text(" ", strip=True) if parent_p else ""
            roles       = classify_paragraph(parent_text)
            if not roles:
                roles = [{"role": "game", "version": "", "label": label}]
            r = roles[0]

            if is_intermediary_url(real):
                seen_urls.add(real)
                if r["role"] == "game":
                    game_inter.append(real)
                    print(f"    ✓ Game inter [{label}]: {real}")
                else:
                    update_inter.append({
                        "version": r["version"],
                        "type":    r["role"],
                        "label":   r["label"],
                        "url":     real,
                    })
                    print(f"    ✓ {r['role'].upper()} {r['label']} inter: {real}")

            elif is_filehost_url(real):
                seen_urls.add(real)
                if r["role"] == "game":
                    game_direct.append({"label": label, "url": real})
                    print(f"    ✓ Game direct [{label}]: {real}")
                else:
                    vk = r["version"]
                    if vk not in update_direct:
                        update_direct[vk] = {
                            "version":   r["version"],
                            "type":      r["role"],
                            "label":     r["label"],
                            "filehosts": [],
                        }
                    update_direct[vk]["filehosts"].append({"label": label, "url": real})
                    print(f"    ✓ {r['role'].upper()} {r['label']} direct [{label}]")

        if game_inter or game_direct or update_inter or update_direct:
            releases.append({
                "cusa":          cusa,
                "region":        region,
                "contributor":   contributor,
                "password":      password,
                "note":          "\n".join(release_notes) if release_notes else "",
                "game_inter":    game_inter,
                "game_direct":   game_direct,
                "update_inter":  update_inter,
                "update_direct": list(update_direct.values()),
            })
            n_game = len(game_inter) + len(game_direct)
            n_upd  = len(update_inter) + len(update_direct)
            print(f"    Release: {cusa or '?'} {region} "
                  f"[{contributor[:30] or 'unknown'}] "
                  f"— {n_game} game, {n_upd} update/dlc")

    if not global_extra.get("cusa_id") and releases:
        for rel in releases:
            if rel["cusa"]:
                global_extra["cusa_id"] = rel["cusa"]
                global_extra["region"]  = rel["region"]
                break

    # If we have a legacy PS ID but no PS4/PS5 ID, mark _no_cusa
    # (but ONLY after all scans are done — don't mark it prematurely)
    if not global_extra.get("cusa_id"):
        if global_extra.get("_ps_legacy_id"):
            global_extra.setdefault("_no_cusa", True)
        elif releases:
            # Releases found but truly no ID of any kind
            global_extra.setdefault("_no_cusa", True)

    ids_summary = [c["cusa"] + ("-" + c["region"] if c["region"] else "")
                   for c in global_extra["cusa_ids"]]
    legacy_summary = global_extra.get("_ps_legacy_id", "")
    print(f"    → {len(releases)} release(s) | "
          f"IDs: {ids_summary or ('legacy:'+legacy_summary if legacy_summary else 'none')}") 
    return releases, global_extra

# ── Intermediary page section parser ─────────────────────────────────────────
# Many intermediary pages have labelled link groups like:
#   <p>Game (Fix 5.05/6.72/7.xx/9.00/11.00/12.00) : <a>Mediafire</a> – <a>Akia</a></p>
#   <p>Update v1.08 : <a>Pikcloud</a> – <a>1file</a></p>
# This parser extracts those groups so they can be labelled correctly in the UI
# (Game row shows "Game (Fix 5.05/6.72/7.xx)" as the label, Update gets its own row)
# and the redundant notes text isn't shown as a yellow callout.

_INTER_SECTION_HEAD_RE = re.compile(
    r'^(game|update|upd\b|dlc|backport|patch|fix\b|addon)\b',
    re.IGNORECASE,
)
_INTER_SECTION_TYPE_MAP = {
    'game': 'game',  'fix': 'game', 'patch': 'game', 'backport': 'game',
    'update': 'update', 'upd': 'update',
    'dlc': 'dlc', 'addon': 'dlc',
}


def _parse_inter_sections(soup: "BeautifulSoup") -> list | None:
    """
    Walk the DOM of an intermediary page looking for labelled link groups.
    Returns list of {type, label, links} or None if no structure found.
    """
    sections: list = []
    seen: set = set()

    for el in soup.find_all(['p', 'div', 'li', 'h2', 'h3', 'h4', 'span']):
        text = el.get_text(' ', strip=True)
        if not text or len(text) > 400:
            continue
        m = _INTER_SECTION_HEAD_RE.match(text.strip())
        if not m:
            continue

        # Collect filehost links directly inside this element
        el_links: list = []
        for a in el.find_all('a', href=True):
            href = resolve_href(a['href'].strip())
            if is_filehost_url(href) and not is_guide_url(href) and href not in seen:
                lbl = urlparse(href).netloc.replace('www.', '').split('.')[0].capitalize()
                el_links.append({'label': lbl, 'url': href})
                seen.add(href)

        # No links in this element? Check immediately following siblings
        if not el_links:
            for sib in el.find_next_siblings(limit=3):
                if _INTER_SECTION_HEAD_RE.match(sib.get_text(strip=True) or ''):
                    break  # hit next section header — stop
                for a in sib.find_all('a', href=True):
                    href = resolve_href(a['href'].strip())
                    if is_filehost_url(href) and not is_guide_url(href) and href not in seen:
                        lbl = urlparse(href).netloc.replace('www.', '').split('.')[0].capitalize()
                        el_links.append({'label': lbl, 'url': href})
                        seen.add(href)
                if el_links:
                    break

        if not el_links:
            continue

        # Build clean label: take text before the colon (strip trailing filehost names)
        colon = text.find(':')
        label = text[:colon].strip() if colon != -1 else text
        label = label[:120].strip()

        word = m.group(1).lower().rstrip()
        sections.append({
            'type':  _INTER_SECTION_TYPE_MAP.get(word, 'game'),
            'label': label,
            'links': el_links,
        })

    return sections if sections else None


def _apply_game_inter_results(fh: list, rel: dict, game_inter_upd: list) -> None:
    """
    Route results from a game intermediary page to the correct lists.

    Grouped results (_group=True) are split by type:
      • 'game'   → stored as group item in rel["game_direct"]  (main.js handles labels)
      • 'update' → appended to game_inter_upd for later merge into update_direct
      • 'dlc'    → same

    Flat results (old format, or pages with no detectable section structure) all
    go into rel["game_direct"] unchanged — backwards-compatible.
    """
    if any(item.get('_group') for item in fh):
        for grp in fh:
            if not grp.get('_group'):
                continue
            if grp['type'] == 'game':
                rel['game_direct'].append(grp)
            elif grp['type'] in ('update', 'upd'):
                game_inter_upd.append({
                    'version':   '',
                    'type':      'update',
                    'label':     grp['label'],
                    'filehosts': grp['filehosts'],
                })
            elif grp['type'] == 'dlc':
                game_inter_upd.append({
                    'version':   '',
                    'type':      'dlc',
                    'label':     grp['label'],
                    'filehosts': grp['filehosts'],
                })
    else:
        rel['game_direct'].extend(fh)


# ── ★ INTERMEDIARY FETCHER (requests, not browser) ────────────────────────────
def fetch_filehosts_via_requests(url: str) -> tuple[list, str, bool]:
    """
    Fetch a downloadgameps3.net intermediary page via requests (no browser).

    Returns (links, notes, cf_blocked) where cf_blocked=True means the server
    returned a 403/CF challenge and the caller should retry via the browser.
    On plain network errors or timeouts, cf_blocked=False (no point retrying).
    """
    # Skip requests entirely for domains that always CF-block plain HTTP clients
    if is_browser_only_inter(url):
        print(f"      → [req] {url}")
        print(f"        [browser-only] skipping requests — CF blocks this domain")
        return [], "", True   # cf_blocked=True → caller retries via UC browser

    print(f"      → [req] {url}")
    _inter_rate_limit()   # enforce minimum gap between inter requests
    try:
        hdrs = {
            **FETCH_HEADERS,
            "Referer": "https://dlpsgame.com/",
            "Accept":  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        r = _req_session.get(url, headers=hdrs, timeout=FETCH_TIMEOUT,
                              allow_redirects=True)
        r.raise_for_status()

        # CF Error 1015 can arrive as a 200 with HTML body
        if _is_1015(r.text):
            print(f"        [1015] rate limit page in response body — backing off {_1015_BACKOFF}s")
            time.sleep(_1015_BACKOFF)
            return [], "", True   # flag for browser retry after backoff

        # Cloudflare JS/Turnstile challenge — also served as HTTP 200.
        # requests has no cookies/JS so CF delivers a challenge page instead of
        # real content.  Flag cf_blocked so the caller retries via the real browser.
        if _is_cf_challenge(r.text):
            print(f"        [CF-challenge] JS/Turnstile challenge in 200 response — will retry via browser")
            return [], "", True

        # If the final URL after redirects is itself a filehost, return it
        if is_filehost_url(r.url) and r.url != url:
            label = urlparse(r.url).netloc.replace("www.", "").split(".")[0].capitalize()
            print(f"        → redirect to filehost: {r.url[:60]}")
            return [{"label": label, "url": r.url}], "", False

    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else 0
        body   = e.response.text if e.response is not None else ""
        # 403/503 = Cloudflare blocking us
        # 429     = Too Many Requests (rate limit)
        # Any of these with a 1015 body = hard rate-limit ban → long backoff
        if _is_1015(body):
            print(f"        [1015] rate limit ban — backing off {_1015_BACKOFF}s")
            time.sleep(_1015_BACKOFF)
            return [], "", True
        if status in (403, 429, 503):
            print(f"        [CF-block] {status} — will retry via browser")
            return [], "", True
        print(f"        [WARN] requests fetch failed: {e}")
        return [], "", False
    except Exception as e:
        print(f"        [WARN] requests fetch failed: {e}")
        return [], "", False

    soup      = BeautifulSoup(r.text, "html.parser")
    links     = []
    seen      = set()

    # ── Step 0: structured section parsing ────────────────────────────────────
    # Try to find Game / Update / DLC sections with proper labels.
    # If found, return grouped items — the caller routes them to the right rows
    # and the UI uses the section label (e.g. "Game (Fix 5.05/6.72/7.xx)") as
    # the row label instead of the generic "Game".  No notes text needed.
    sections = _parse_inter_sections(soup)
    if sections:
        groups = [
            {'_group': True, 'type': s['type'], 'label': s['label'], 'filehosts': s['links']}
            for s in sections
        ]
        total = sum(len(s['links']) for s in sections)
        print(f"        → {total} link(s) in {len(sections)} labelled section(s)")
        return groups, '', False

    # ── Step 1: all <a href> anchors ──────────────────────────────────────────
    for a in soup.find_all("a", href=True):
        href = resolve_href(a["href"].strip())
        if is_guide_url(href) or not href or href in seen:
            continue
        if is_filehost_url(href):
            domain_lbl = urlparse(href).netloc.replace("www.", "").split(".")[0].capitalize()
            links.append({"label": domain_lbl, "url": href})
            seen.add(href)

    # ── Step 2: data-href / data-url / data-link attributes ──────────────────
    for el in soup.find_all(attrs={"data-href": True}):
        href = resolve_href(el["data-href"].strip())
        if is_filehost_url(href) and href not in seen:
            domain_lbl = urlparse(href).netloc.replace("www.", "").split(".")[0].capitalize()
            links.append({"label": domain_lbl, "url": href})
            seen.add(href)

    # ── Step 3: plain-text URLs in page body ─────────────────────────────────
    body_text = r.text
    for raw_url in _URL_SCAN_RE.findall(body_text):
        raw_url = raw_url.rstrip('/.,;)"\'')
        raw_url = resolve_href(raw_url)
        if is_filehost_url(raw_url) and raw_url not in seen:
            lbl = urlparse(raw_url).netloc.replace("www.", "").split(".")[0].capitalize()
            links.append({"label": lbl, "url": raw_url})
            seen.add(raw_url)

    # ── Step 4: redirect target became a filehost ─────────────────────────────
    cur = r.url
    if not links and is_filehost_url(cur) and cur not in seen:
        label = urlparse(cur).netloc.split(".")[-2].capitalize()
        links.append({"label": label, "url": cur})

    # ── Step 5: extract notes text ────────────────────────────────────────────
    _SKIP_LINES = {
        'skip to content', 'link download free', 'download', 'enjoy',
        'check all link befor download', 'check all links before download',
        'link download', 'guide download', 'tool download',
    }
    text_content = soup.get_text("\n", strip=True)
    note_lines = []
    for ln in text_content.splitlines():
        ln = ln.strip()
        if not ln or ln.lower() in _SKIP_LINES:
            continue
        if _URL_SCAN_RE.search(ln):
            continue
        if re.match(r'^[A-Za-z0-9]{1,12}$', ln):
            continue
        note_lines.append(ln)
    notes = _strip_ad_text("\n".join(note_lines)) or ""

    print(f"        → {len(links)} link(s)")
    return links, notes, False

# ── ★ INTERMEDIARY FETCHER (browser fallback for CF-blocked pages) ────────────
def fetch_filehosts_via_browser(url: str, driver) -> tuple[list, str]:
    """
    Navigate to an intermediary page using the real browser (already has CF
    clearance from the main dlpsgame.com session). Used only when
    fetch_filehosts_via_requests gets a 403/429/503 CF block.

    KEY FIX vs previous version:
    - wait_for_cf return value is checked — if False (timed out) we don't try
      to parse a CF challenge page (which always yields 0 links).
    - Uses a longer CF timeout (90 s) because downloadgameps3.net is a separate
      CF domain the browser hasn't pre-solved; it needs its own clearance time.
    - One retry: if CF clears but we get 0 links, waits 3 s and tries once more
      (handles pages where links are injected slightly after CF clears).
    """
    _CF_BROWSER_TIMEOUT = 90   # longer than main-site timeout — fresh CF domain

    print(f"      → [browser-fallback] {url}")
    _inter_rate_limit()   # same gate as requests path — prevents 1015 on sequential browser retries
    origin_host = urlparse(url).netloc

    try:
        driver.set_page_load_timeout(120)
        try:
            driver.get(url)
        except Exception as _pg_e:
            print(f"        [WARN] page load timed out: {_pg_e}")
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
    except Exception as e:
        print(f"        [ERROR loading] {e}")
        return [], ""
    finally:
        try:
            driver.set_page_load_timeout(300)
        except Exception:
            pass

    # Wait for CF to clear — check return value, don't parse a challenge page
    cf_ok = wait_for_cf(driver, timeout=_CF_BROWSER_TIMEOUT)
    if not cf_ok:
        # One more attempt: wait 15 s and check if the title has cleared
        print(f"        [CF] still challenged — waiting 15 s more...")
        time.sleep(15)
        cf_ok = wait_for_cf(driver, timeout=15)
        if not cf_ok:
            print(f"        [WARN] CF did not clear — skipping link extraction")
            return [], ""

    # Check for Error 1015 (rate limit ban) — back off hard and return empty
    try:
        page_text = driver.execute_script(
            "return document.title + ' ' + (document.body ? document.body.innerText : '')"
        ) or ""
        if _is_1015(page_text):
            print(f"        [1015] rate limit ban in browser — backing off {_1015_BACKOFF}s")
            time.sleep(_1015_BACKOFF)
            return [], ""
    except Exception:
        pass

    jitter(SLEEP_REVEAL, 0.3)

    # Try clicking a reveal/download button
    REVEAL_TEXTS     = ["get links", "show links", "click here to download",
                        "download links", "show download", "get download links",
                        "reveal links", "unlock links"]
    REVEAL_BTN_ONLY  = ["download"]
    try:
        clicked = driver.execute_script("""
            var exact = arguments[0], btnOnly = arguments[1];
            function inNav(el) {
                var p = el.parentElement;
                while (p) {
                    var tag = (p.tagName||'').toLowerCase();
                    var cls = (p.className||'').toLowerCase();
                    var id  = (p.id||'').toLowerCase();
                    if (tag==='nav'||tag==='header'||tag==='footer') return true;
                    if (cls.indexOf('nav')!==-1||cls.indexOf('menu')!==-1) return true;
                    if (id.indexOf('nav')!==-1||id.indexOf('menu')!==-1)  return true;
                    p = p.parentElement;
                }
                return false;
            }
            var els = document.querySelectorAll(
                'button, input[type=button], input[type=submit], a[href="#"], a:not([href])');
            for (var i=0; i<els.length; i++) {
                var el = els[i];
                if (inNav(el)) continue;
                var t = (el.innerText||el.value||el.textContent||'').toLowerCase().trim();
                if (!t || t.length>60) continue;
                for (var j=0; j<exact.length; j++) {
                    if (t===exact[j]||t.indexOf(exact[j])!==-1) { el.click(); return t; }
                }
                var tag = el.tagName.toLowerCase();
                if (tag==='button'||tag==='input') {
                    for (var j=0; j<btnOnly.length; j++) {
                        if (t.indexOf(btnOnly[j])!==-1) { el.click(); return t; }
                    }
                }
            }
            return null;
        """, REVEAL_TEXTS, REVEAL_BTN_ONLY)
        if clicked:
            print(f"        Clicked reveal: '{clicked}'")
            jitter(SLEEP_REVEAL, 0.3)
            if origin_host not in driver.current_url:
                driver.back()
                wait_for_cf(driver, timeout=_CF_BROWSER_TIMEOUT)
    except Exception:
        pass

    def _collect_links():
        """Extract all filehost links from the current page DOM."""
        # Direct redirect to a filehost?
        if is_filehost_url(driver.current_url):
            lbl = urlparse(driver.current_url).netloc.split(".")[-2].capitalize()
            return [{"label": lbl, "url": driver.current_url}]

        all_hrefs = driver.execute_script("""
            var results=[], seen={};
            document.querySelectorAll('a[href]').forEach(function(a){
                var h=a.href||'', t=(a.innerText||a.textContent||'').trim();
                if(h&&!seen[h]){seen[h]=1;results.push([h,t]);}
            });
            document.querySelectorAll('[data-href],[data-url],[data-link]').forEach(function(el){
                var h=el.getAttribute('data-href')||el.getAttribute('data-url')||
                      el.getAttribute('data-link')||'';
                var t=(el.innerText||el.textContent||'').trim();
                if(h&&!seen[h]){seen[h]=1;results.push([h,t]);}
            });
            document.querySelectorAll('[onclick]').forEach(function(el){
                var oc=el.getAttribute('onclick')||'';
                var m=oc.match(/https?:[/][/][^'" ]+/g);
                if(m) m.forEach(function(u){
                    if(!seen[u]){seen[u]=1;results.push([u,(el.innerText||'').trim()]);}
                });
            });
            return results;
        """) or []

        found = []
        seen_links = set()
        for href, _label in all_hrefs:
            href = resolve_href(href.strip())
            if not href or href in seen_links or is_guide_url(href):
                continue
            if is_filehost_url(href):
                domain_lbl = urlparse(href).netloc.replace("www.", "").split(".")[0].capitalize()
                found.append({"label": domain_lbl, "url": href})
                seen_links.add(href)

        # Plain-text URL scan as last resort
        try:
            body = driver.execute_script(
                "return document.body ? document.body.innerText : ''") or ""
            for raw_url in _URL_SCAN_RE.findall(body):
                raw_url = raw_url.rstrip('/.,;)"\'')
                raw_url = resolve_href(raw_url)
                if is_filehost_url(raw_url) and raw_url not in seen_links:
                    lbl = urlparse(raw_url).netloc.replace("www.", "").split(".")[0].capitalize()
                    found.append({"label": lbl, "url": raw_url})
                    seen_links.add(raw_url)
        except Exception:
            pass

        return found

    links = _collect_links()

    # Retry once if 0 links — page may need a moment after CF clears
    if not links:
        print(f"        [retry] 0 links — waiting 3 s and trying once more")
        time.sleep(3)
        links = _collect_links()

    # ── Structured section parsing on live page source ─────────────────────────
    # Try to identify Game / Update / DLC sections with proper labels.
    # Run after _collect_links so the page has fully rendered.
    try:
        browser_soup = BeautifulSoup(driver.page_source, "html.parser")
        sections = _parse_inter_sections(browser_soup)
        if sections:
            groups = [
                {'_group': True, 'type': s['type'], 'label': s['label'], 'filehosts': s['links']}
                for s in sections
            ]
            total = sum(len(s['links']) for s in sections)
            print(f"        → [browser] {total} link(s) in {len(sections)} labelled section(s)")
            return groups, ''
    except Exception:
        pass

    # Notes extraction
    _SKIP_LINES = {'skip to content','link download free','download','enjoy',
                   'check all link befor download','check all links before download',
                   'link download','guide download','tool download'}
    notes = ""
    try:
        body_text = driver.execute_script(
            "return document.body ? document.body.innerText : ''") or ""
        note_lines = []
        for ln in body_text.splitlines():
            ln = ln.strip()
            if not ln or ln.lower() in _SKIP_LINES: continue
            if _URL_SCAN_RE.search(ln): continue
            if re.match(r'^[A-Za-z0-9]{1,12}$', ln): continue
            note_lines.append(ln)
        notes = _strip_ad_text("\n".join(note_lines)) or ""
    except Exception:
        pass

    print(f"        → [browser] {len(links)} link(s)")
    return links, notes

# ── METADATA EXTRACTOR ────────────────────────────────────────────────────────
def extract_metadata(soup):
    """
    Extract title, description, cover URL, screenshot URLs, info table, youtube_id.
    Identical to scraper.py — no changes.
    """
    def full_size(url):
        return re.sub(r'/s\d{2,4}(-c)?/', '/s1600/', url)

    def is_blogspot(url):
        return ("blogger.googleusercontent.com" in url
                or "bp.blogspot.com" in url)

    def is_junk(url):
        return any(x in url for x in [
            "emoji", ".svg", "wp-includes", "/icon", "/logo",
            "avatar", "bar-bg2.png", "youtube.png", "wpfront",
            "wpdiscuz", "jetpack", "sharing",
        ])

    # ── Title ─────────────────────────────────────────────────────────────────
    title    = ""
    title_el = soup.select_one("h1.post-title.entry-title, h1.entry-title")
    if title_el:
        title = title_el.get_text(strip=True)
    if not title:
        og_t = soup.find("meta", property="og:title")
        if og_t:
            title = og_t.get("content", "").strip()
    if not title:
        pt = soup.find("title")
        if pt:
            title = re.split(r'\s*[-|]\s*Download', pt.get_text(strip=True))[0].strip()

    # ── Description ───────────────────────────────────────────────────────────
    desc    = ""
    content = soup.select_one(".post-body.entry-content") or soup
    bq      = content.select_one("blockquote")
    if bq:
        desc = _strip_ad_text(bq.get_text("\n", strip=True)) or ""
    if not desc:
        dd = content.select_one(".game_desc .desc, .desc")
        if dd:
            desc = dd.get_text("\n", strip=True)
    if not desc:
        og_d = soup.find("meta", property="og:description")
        if og_d:
            desc = _strip_ad_text(og_d.get("content", "").strip()) or ""
    if not desc:
        for p in content.find_all("p"):
            t = p.get_text(" ", strip=True)
            if len(t) > 80 and not re.search(r'(?:CUSA|PPSA|LAPY|LBXP|STRN|SCES|SLES|SLUS)\d{3,6}', t):
                desc = t; break

    # ── Cover + Screenshots ───────────────────────────────────────────────────
    cover = None
    shots = []

    def head_cover():
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            return og["content"].strip()
        for script in soup.find_all("script", type="application/ld+json"):
            m = re.search(r'"thumbnailUrl"\s*:\s*"([^"]+)"', script.string or "")
            if m:
                return m.group(1).strip()
        return None

    for img in content.find_all("img", src=True):
        src = (img.get("data-lazy-src") or img.get("src") or "").strip()
        if not src or is_junk(src):
            continue

        wrap_a     = img.find_parent("a")
        in_sep     = img.find_parent("div", class_="separator") is not None
        in_td      = img.find_parent("td") is not None
        in_rowspan = False
        if in_td:
            td = img.find_parent("td")
            in_rowspan = td.has_attr("rowspan")

        is_wp   = "dlpsgame.com/wp-content" in src
        is_blog = is_blogspot(src)

        if wrap_a and is_wp:
            href = wrap_a.get("href", "")
            if "dlpsgame.com/" in href and "/wp-content/" not in href:
                continue

        if is_blog and in_sep and len(shots) < 5:
            fa = img.find_parent("a", class_="ari-fancybox")
            shot_url = (fa["href"].strip() if fa and fa.get("href") else src)
            shots.append(full_size(shot_url))
            continue

        if cover is None:
            if   is_wp   and in_rowspan:   cover = src
            elif is_wp   and in_td:        cover = src
            elif is_wp   and not in_sep:   cover = src
            elif is_blog and in_rowspan:   cover = full_size(src)
            elif is_blog and in_td:        cover = full_size(src)
            elif is_blog and not in_sep:   cover = full_size(src)

    if not cover:
        hc = head_cover()
        if hc:
            cover = re.sub(r'/s\d{2,4}(-c)?/', '/s1600/', hc)
            print(f"    cover: og:image fallback → {cover[:60]}")

    seen_s = set()
    if cover:
        seen_s.add(re.sub(r'/s\d{2,4}(-c)?/', '/s1600/', cover))
    shots = [s for s in shots if not (s in seen_s or seen_s.add(s))]

    # ── Info table ────────────────────────────────────────────────────────────
    info_table = {}
    for table in content.find_all("table"):
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                nc = [c for c in cells if not c.find("img")]
                if len(nc) >= 2:
                    key = nc[0].get_text(strip=True).upper()
                    val = nc[1].get_text(strip=True)
                    if key and val and len(key) < 30:
                        info_table[key] = val

    # ── YouTube embed ─────────────────────────────────────────────────────────
    youtube_id = None
    for iframe in content.find_all("iframe"):
        isrc = iframe.get("src", "") or iframe.get("data-lazy-src", "")
        ym = re.search(r"youtube\.com/embed/([\w\-]+)", isrc)
        if ym:
            youtube_id = ym.group(1); break
    if not youtube_id:
        for div in content.find_all("div", class_="rll-youtube-player"):
            did = div.get("data-id", "") or ""
            if did:
                youtube_id = did; break
            ym = re.search(r"youtube\.com/embed/([\w\-]+)",
                           div.get("data-src", "") or "")
            if ym:
                youtube_id = ym.group(1); break

    print(f"    metadata: title={bool(title)} cover={bool(cover)} "
          f"shots={len(shots)} desc={len(desc)}ch")
    return title, desc, cover, shots, info_table, youtube_id

# ── ★ SLUG + IMAGE DOWNLOADER ─────────────────────────────────────────────────
def game_slug(title: str, url: str, cusa_id: str = "") -> str:
    path = urlparse(url).path.strip("/")
    slug = path.split("/")[-1] if path else ""
    if not slug:
        slug = re.sub(r"[^\w\-]", "-", title.lower())[:60]
    if cusa_id:
        slug = f"{cusa_id}-{slug}"
    return slug

def _download_one_via_requests(url: str, local_path: Path,
                                role: str, referer: str) -> bool:
    """Download a single image via requests. Returns True on success."""
    if local_path.exists() and local_path.stat().st_size > 500:
        return True  # already on disk
    try:
        hdrs = {
            **FETCH_HEADERS,
            "Referer": referer,
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        }
        r = _req_session.get(url, headers=hdrs, timeout=IMG_TIMEOUT, stream=True)
        r.raise_for_status()
        data = b"".join(r.iter_content(8192))
        if len(data) > 500:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, "wb") as f:
                f.write(data)
            return True
        return False
    except Exception:
        return False

def download_screenshots(urls: list, labels: list, slug: str,
                          cusa_id: str = "", driver=None,
                          page_url: str = "",
                          img_pool: ThreadPoolExecutor = None) -> list:
    """
    Download cover + screenshots.

    FAST VERSION vs old scraper:
    • All blogspot/blogger images downloaded in parallel via img_pool threads.
    • All wp-content images batched into ONE browser Promise.all() JS call,
      so N wp-content images take the same time as 1 (was N × 4 s sequential).
    • On any failure, falls back to requests (same as before).

    Returns the same [{"role", "url", "local"}, ...] structure.
    """
    if not urls:
        return []

    out_dir     = Path(SCREENSHOTS_DIR) / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    cusa_suffix = f"_{cusa_id}" if cusa_id else ""
    results     = {}   # role → result dict

    # Split into groups
    wp_jobs     = []   # (url, role, local_path, relative)
    blog_jobs   = []   # (url, role, local_path, relative)

    for url, role in zip(urls, labels):
        ext = Path(urlparse(url).path).suffix.lower()
        if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
            ext = ".jpg"
        local_name = f"{role}{cusa_suffix}{ext}"
        local_path = out_dir / local_name
        relative   = f"{SCREENSHOTS_DIR}/{slug}/{local_name}"

        if local_path.exists() and local_path.stat().st_size > 500:
            results[role] = {"role": role, "url": url, "local": relative}
            print(f"      {role}: cached on disk")
            continue

        if "dlpsgame.com" in url:
            wp_jobs.append((url, role, local_path, relative))
        else:
            blog_jobs.append((url, role, local_path, relative))

    # ── Blogspot images: parallel via requests threads ────────────────────────
    blog_futures = {}
    for url, role, local_path, relative in blog_jobs:
        referer = urlparse(url).scheme + "://" + urlparse(url).netloc + "/"
        if img_pool:
            fut = img_pool.submit(_download_one_via_requests,
                                  url, local_path, role, referer)
        else:
            # No pool provided — run inline
            ok = _download_one_via_requests(url, local_path, role, referer)
            saved = {"role": role, "url": url, "local": relative if ok else None}
            results[role] = saved
            print(f"      {role}: {'saved (requests)' if ok else 'FAILED'} → {relative}")
            continue
        blog_futures[fut] = (role, url, relative)

    # ── wp-content images: batch browser fetch (Promise.all) ─────────────────
    if wp_jobs and driver:
        target_referer = page_url or "https://dlpsgame.com/"
        # Ensure browser is on dlpsgame.com for the right cookie context
        cur = driver.current_url or ""
        if "dlpsgame.com" not in cur:
            try:
                driver.get(target_referer)
                wait_for_dlpsgame(driver)
                jitter(2, 0.3)
            except Exception:
                pass

        wp_url_list = [j[0] for j in wp_jobs]
        try:
            # Single async JS call downloading all wp-content images in parallel
            b64_results = driver.execute_async_script("""
                var urls    = arguments[0];
                var referer = arguments[1];
                var done    = arguments[2];
                Promise.all(urls.map(function(url) {
                    return fetch(url, {
                        method: 'GET',
                        credentials: 'include',
                        headers: {
                            'Accept':  'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
                            'Referer': referer
                        }
                    })
                    .then(function(r) { return r.ok ? r.blob() : null; })
                    .then(function(blob) {
                        if (!blob) return {ok:false};
                        return new Promise(function(resolve) {
                            var reader = new FileReader();
                            reader.onloadend = function() {
                                resolve({ok: true,
                                         b64: reader.result ? reader.result.split(',')[1] : null});
                            };
                            reader.readAsDataURL(blob);
                        });
                    })
                    .catch(function() { return {ok:false}; });
                })).then(done).catch(function() { done([]); });
            """, wp_url_list, target_referer)

            if not b64_results:
                b64_results = [{"ok": False}] * len(wp_jobs)

            for (url, role, local_path, relative), res in zip(wp_jobs, b64_results):
                saved = False
                if res and res.get("ok") and res.get("b64"):
                    try:
                        data = base64.b64decode(res["b64"])
                        if len(data) > 500:
                            out_dir.mkdir(parents=True, exist_ok=True)
                            with open(local_path, "wb") as f:
                                f.write(data)
                            results[role] = {"role": role, "url": url, "local": relative}
                            print(f"      {role}: saved (browser-batch) → {relative}")
                            saved = True
                    except Exception:
                        pass

                if not saved:
                    # Browser fetch failed — try requests as fallback
                    ok = _download_one_via_requests(url, local_path, role,
                                                    "https://dlpsgame.com/")
                    status = "saved (req-fallback)" if ok else "FAILED"
                    results[role] = {"role": role, "url": url,
                                     "local": relative if ok else None}
                    print(f"      {role}: {status}")

        except Exception as e:
            print(f"      [WARN] batch browser fetch error: {e!r}")
            # Fall back to sequential requests for wp-content
            for url, role, local_path, relative in wp_jobs:
                ok = _download_one_via_requests(url, local_path, role,
                                                "https://dlpsgame.com/")
                results[role] = {"role": role, "url": url,
                                 "local": relative if ok else None}
                print(f"      {role}: {'saved (req-fallback)' if ok else 'FAILED'}")

    elif wp_jobs:
        # No driver — try requests only
        for url, role, local_path, relative in wp_jobs:
            ok = _download_one_via_requests(url, local_path, role,
                                            "https://dlpsgame.com/")
            results[role] = {"role": role, "url": url,
                             "local": relative if ok else None}
            print(f"      {role}: {'saved (requests)' if ok else 'FAILED'}")

    # ── Collect blogspot future results ───────────────────────────────────────
    for fut, (role, url, relative) in blog_futures.items():
        try:
            ok = fut.result(timeout=60)
            results[role] = {"role": role, "url": url,
                             "local": relative if ok else None}
            print(f"      {role}: {'saved (req-parallel)' if ok else 'FAILED'} → {relative}")
        except Exception as e:
            print(f"      {role}: thread error ({e!r})")
            results[role] = {"role": role, "url": url, "local": None}

    # Preserve order: cover first, then screenshots in label order
    ordered = []
    for lbl in labels:
        if lbl in results:
            ordered.append(results.pop(lbl))
    ordered.extend(results.values())   # any remaining (shouldn't happen)
    return ordered

# ── ENTRY COMPLETENESS ────────────────────────────────────────────────────────
def entry_missing(entry):
    """Identical to scraper.py — returns set of missing field names."""
    missing = set()
    if entry.get("error"):
        missing.add("error")

    releases   = entry.get("releases") or []
    extra      = entry.get("extra", {})
    has_game   = any(r.get("game") for r in releases)
    has_legacy = bool(entry.get("filehosts"))
    if not has_game and not has_legacy and not extra.get("_no_game"):
        missing.add("releases")

    shots = entry.get("screenshots", [])

    def file_ok(local):
        if not local or local == "dead":
            return False
        p = Path(local)
        if not p.exists():
            p = Path(__file__).parent / local
        return p.exists() and p.stat().st_size > 500

    def img_settled(s):
        return s.get("local") is not None

    cover_e = next((s for s in shots if s.get("role") == "cover"), None)
    if not cover_e or not img_settled(cover_e):
        missing.add("cover")

    screen_shots  = [s for s in shots if s.get("role", "").startswith("screenshot_")]
    _no_shots_flg = entry.get("extra", {}).get("_no_screenshots", False)
    if not screen_shots:
        if not _no_shots_flg:
            missing.add("screenshots")
    else:
        any_unsettled = any(not img_settled(s) for s in screen_shots)
        if any_unsettled:
            missing.add("screenshots")

    # extra already bound above (line 1508); reassignment kept for clarity
    extra       = entry.get("extra", {})
    # Satisfied by: any PS4/PS5 native ID (cusa_id field or per-release cusa),
    # OR a legacy platform ID (_ps_legacy_id), OR the _no_cusa flag meaning
    # "we've already confirmed no ID exists on this page".
    has_any_id = (
        extra.get("cusa_id") or
        extra.get("_ps_legacy_id") or
        any(r.get("cusa") for r in entry.get("releases", []))
    )
    if not has_any_id and entry.get("releases") and not extra.get("_no_cusa"):
        missing.add("cusa_id")

    return missing

# ── ★ RESOLVE INTERMEDIARIES (parallel requests → browser fallback for CF) ────
def _resolve_releases(releases: list,
                      inter_pool: ThreadPoolExecutor,
                      driver,
                      soup: BeautifulSoup) -> list:
    """
    Resolve all intermediary URLs → real filehost links.

    Strategy:
      1. Submit all inter URLs to thread pool (requests, all in parallel).
      2. Collect results. Any URL that got CF-blocked (403/503) is queued.
      3. Browser-fallback CF-blocked URLs sequentially on the main thread —
         the browser already has CF clearance from the main dlpsgame.com visit.

    This keeps the fast path fast (most URLs work via requests) while
    correctly handling pages where CF decides to challenge the plain request.
    """
    for rel in releases:
        # ── Game intermediaries ───────────────────────────────────────────────
        game_inter_futures: dict[Future, str] = {}
        for _i, iurl in enumerate(rel.get("game_inter", [])):
            if _i > 0:
                time.sleep(_random.uniform(0.3, 0.7))  # stagger submissions; real throttle is _inter_rate_limit()
            fut = inter_pool.submit(fetch_filehosts_via_requests, iurl)
            game_inter_futures[fut] = iurl

        cf_retry_game = []   # URLs that need browser fallback
        game_inter_upd: list = []   # Update/DLC groups found inside game_inter pages
        for fut, iurl in game_inter_futures.items():
            try:
                fh, _inotes, cf_blocked = fut.result(timeout=60)
                if fh:
                    _apply_game_inter_results(fh, rel, game_inter_upd)
                    is_grouped = any(item.get('_group') for item in fh)
                    n_links = (sum(len(g.get('filehosts', [])) for g in fh if g.get('_group'))
                               if is_grouped else len(fh))
                    print(f"    ✓ game inter → {n_links} link(s)")
                    # Only keep plain notes text when we couldn't parse section structure
                    if _inotes and not is_grouped and not rel.get("notes"):
                        rel["notes"] = _inotes
                elif cf_blocked:
                    cf_retry_game.append(iurl)
                else:
                    print(f"    [WARN] 0 links from {iurl[:50]} (requests)")
            except Exception as e:
                print(f"    [WARN] intermediary future error: {e}")

        # Browser fallback for CF-blocked game inter URLs
        for iurl in cf_retry_game:
            fh, _inotes = fetch_filehosts_via_browser(iurl, driver)
            if fh:
                _apply_game_inter_results(fh, rel, game_inter_upd)
                is_grouped = any(item.get('_group') for item in fh)
                if _inotes and not is_grouped and not rel.get("notes"):
                    rel["notes"] = _inotes
            else:
                print(f"    [WARN] browser fallback also got 0 links: {iurl[:50]}")
        rel.pop("game_inter", None)

        # Merge any Update/DLC groups found inside game_inter pages into update_direct
        if game_inter_upd:
            rel.setdefault("update_direct", []).extend(game_inter_upd)

        # ── Update/DLC/backport intermediaries ───────────────────────────────
        new_upd = list(rel.get("update_direct", []))
        upd_inter_futures: dict[Future, dict] = {}
        for _i, ui in enumerate(rel.get("update_inter", [])):
            if _i > 0:
                time.sleep(_random.uniform(0.3, 0.7))  # stagger submissions; real throttle is _inter_rate_limit()
            fut = inter_pool.submit(fetch_filehosts_via_requests, ui["url"])
            upd_inter_futures[fut] = ui

        cf_retry_upd = []    # (ui dict) that need browser fallback
        for fut, ui in upd_inter_futures.items():
            try:
                fh, _inotes, cf_blocked = fut.result(timeout=60)
                if fh:
                    # Flatten grouped results (update_inter pages rarely have sub-sections,
                    # but _parse_inter_sections may trigger — handle gracefully)
                    flat_fh = []
                    for item in fh:
                        if item.get('_group'):
                            flat_fh.extend(item.get('filehosts', []))
                        else:
                            flat_fh.append(item)
                    fh = flat_fh or fh
                    upd_entry = {
                        "version":   ui["version"],
                        "type":      ui["type"],
                        "label":     ui["label"],
                        "filehosts": fh,
                    }
                    if _inotes:
                        upd_entry["notes"] = _inotes
                    new_upd.append(upd_entry)
                    print(f"    ✓ {ui['type']} {ui['label']} → {len(fh)} link(s)")
                elif cf_blocked:
                    cf_retry_upd.append(ui)
                else:
                    print(f"    [WARN] 0 links for {ui['label']} (requests)")
            except Exception as e:
                print(f"    [WARN] update inter error: {e}")

        # Browser fallback for CF-blocked update/DLC inter URLs
        for ui in cf_retry_upd:
            fh, _inotes = fetch_filehosts_via_browser(ui["url"], driver)
            if fh:
                flat_fh = []
                for item in fh:
                    if item.get('_group'):
                        flat_fh.extend(item.get('filehosts', []))
                    else:
                        flat_fh.append(item)
                fh = flat_fh or fh
                upd_entry = {
                    "version":   ui["version"],
                    "type":      ui["type"],
                    "label":     ui["label"],
                    "filehosts": fh,
                }
                if _inotes:
                    upd_entry["notes"] = _inotes
                new_upd.append(upd_entry)
            else:
                print(f"    [WARN] browser fallback also got 0 links: {ui['url'][:50]}")
        rel.pop("update_inter", None)
        rel["update_direct"] = new_upd

    return releases

# ── ★ SCRAPE PAGE (main per-game function) ────────────────────────────────────
def scrape_page(url: str, title_hint: str, driver,
                inter_pool: ThreadPoolExecutor,
                img_pool: ThreadPoolExecutor) -> dict:
    """
    Full scrape of one game page. Fastest possible path:
      1. Browser loads page (CF handled by uc)
      2. Python decodes payloads from static HTML (no JS wait)
      3. Intermediary fetches submitted to thread pool (parallel, requests)
      4. Images downloaded: blogspot parallel via thread pool, wp-content batch JS
      5. Sleep while background I/O finishes
    """
    # ── Load page ─────────────────────────────────────────────────────────────
    for _attempt in range(2):
        try:
            driver.get(url)
            break
        except Exception as _e:
            print(f"  [WARN] page load error (attempt {_attempt+1}): {_e}")
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            if _attempt == 0:
                jitter(2, 0.3)
            else:
                break

    wait_for_dlpsgame(driver)
    jitter(SLEEP_AFTER_LOAD, 0.3)   # brief human-like pause

    # ── Open closed su-spoilers so secure-data payloads become decodable ──────
    # Recent posts wrap the download section in a collapsed Shortcodes Ultimate
    # spoiler.  The site's decode script skips hidden elements, leaving the
    # payload encrypted.  expand_su_spoilers() clicks them open and force-decodes
    # any remaining data-payload attributes before we read page_source.
    expand_su_spoilers(driver)

    # ── Read page source + decode payloads (instant) ──────────────────────────
    page_src = driver.page_source
    soup     = BeautifulSoup(page_src, "html.parser")

    # get_payload_htmls tries Python-decode first (no JS wait), JS fallback
    payload_htmls = get_payload_htmls(driver, page_src)

    title, desc, cover_url, screenshot_urls, info_table, youtube_id = \
        extract_metadata(soup)
    print(f"  Title: {title or '(not found)'}")

    releases, global_extra = extract_releases_from_htmls(payload_htmls, page_src)

    # ── Submit intermediary fetches to thread pool (all in parallel) ──────────
    # Pre-collect all inter URLs so we can submit and then move on
    releases = _resolve_releases(releases, inter_pool, driver, soup)

    # ── Fallback: if no releases, scan page HTML directly ─────────────────────
    if not releases:
        print("  No payload links — scanning page directly for filehost links")
        content = soup.select_one(".post-body.entry-content")
        fallback_fh = []
        if content:
            seen_fb = set()
            for a in content.find_all("a", href=True):
                href  = resolve_href(a["href"].strip())
                label = a.get_text(strip=True)
                if is_filehost_url(href) and not is_guide_url(href) and href not in seen_fb:
                    fallback_fh.append({"label": label, "url": href})
                    seen_fb.add(href)
        if fallback_fh:
            releases.append({
                "cusa":          global_extra.get("cusa_id", ""),
                "region":        global_extra.get("region", ""),
                "contributor":   "",
                "password":      global_extra.get("password", ""),
                "game_direct":   fallback_fh,
                "update_direct": [],
            })

        # ── Cross-reference detection ─────────────────────────────────────────
        # Some pages (e.g. "The Sly Collection") contain no download links of
        # their own — they just link to individual game pages on dlpsgame.com.
        # Detect those internal game-page links and store them as _cross_refs
        # in extra so that resolve_cross_refs() can populate releases later.
        if not releases and content:
            _DLPS_GAME_RE = re.compile(
                r"https?://dlpsgame\.com/([a-z0-9][a-z0-9\-]+)/",
                re.IGNORECASE,
            )
            _SKIP_SLUGS = ("category/", "list-", "guide-", "daily-update",
                           "author/", "wp-", "tag/", "page/", "feed/",
                           "search", "contact", "about", "privacy")
            cross_refs  = []
            seen_crefs  = set()
            for a in content.find_all("a", href=True):
                href = a["href"].strip()
                if "dlpsgame.com" not in href.lower():
                    continue
                m = _DLPS_GAME_RE.search(href)
                if not m:
                    continue
                slug = m.group(1).lower()
                if any(skip in slug for skip in _SKIP_SLUGS):
                    continue
                # Normalise URL: always trailing-slash, no query/fragment
                norm = f"https://dlpsgame.com/{slug}/"
                if norm in seen_crefs:
                    continue
                seen_crefs.add(norm)
                label = a.get_text(strip=True) or slug
                cross_refs.append({"title": label, "url": norm})

            if cross_refs:
                print(f"  [cross-ref] {len(cross_refs)} internal game link(s) detected"
                      f" — will resolve after full scrape")
                global_extra["_cross_refs"] = cross_refs

    # ── ID fallback scans (page_src level) ───────────────────────────────────
    # extract_releases_from_htmls already ran the same scans, but it operates
    # on the page_source captured right after load.  Re-run here in case the
    # DOM was modified after that snapshot (e.g. lazy-loaded content).
    if not global_extra.get("cusa_id"):
        # Strip span obfuscation then search for ALL known ID prefixes
        page_clean = _html.unescape(re.sub(r"<[^>]+>", "", page_src))
        _FALLBACK_ID_RE = re.compile(
            r"\b((?:CUSA|PPSA|LAPY|LBXP|STRN|"
            r"SCES|SLES|SCUS|SLUS|BLES|BLUS|BCES|BCUS|BCAS|BCJS|"
            r"NPUB|NPEB|NPJA|NPJB)\d{3,6})\b",
            re.IGNORECASE,
        )
        _FALLBACK_REGION_RE = re.compile(
            r"[\u2013\u2014\-]\s*(USA|EUR|JPN|JAP|JP|ASIA|UK|HKG|HK|KOR|CHN|AU|INT)",
            re.IGNORECASE,
        )
        _PS4PS5_PFX = frozenset({"CUSA", "PPSA", "LAPY", "LBXP", "STRN"})
        fallback_found = []
        for m in _FALLBACK_ID_RE.finditer(page_clean):
            raw_id = m.group(1).upper()
            pfx    = re.match(r"[A-Z]+", raw_id).group()
            tail   = page_clean[m.end(): m.end() + 25]
            rm     = _FALLBACK_REGION_RE.match(tail)
            region = rm.group(1).upper() if rm else ""
            if pfx in _PS4PS5_PFX:
                entry_c = {"cusa": raw_id, "region": region}
                if entry_c not in global_extra["cusa_ids"]:
                    global_extra["cusa_ids"].append(entry_c)
                if "cusa_id" not in global_extra:
                    global_extra["cusa_id"] = raw_id
                    global_extra["region"]  = region
                fallback_found.append(raw_id)
            elif pfx not in _PS4PS5_PFX and "_ps_legacy_id" not in global_extra:
                global_extra["_ps_legacy_id"] = raw_id
        # Also scan PKG filenames embedded in the raw page source
        _FALLBACK_PKG_RE = re.compile(
            r"(?:\b[A-Z]{2}\d{4}[-_])?(CUSA\d{4,6}|PPSA\d{4,6}|LAPY\d{3,6}|LBXP\d{3,6}|STRN\d{3,6})"
            r"(?:[_\-\.]|\b)",
            re.IGNORECASE,
        )
        for m in _FALLBACK_PKG_RE.finditer(page_src):
            raw_id = m.group(1).upper()
            entry_c = {"cusa": raw_id, "region": ""}
            if entry_c not in global_extra["cusa_ids"]:
                global_extra["cusa_ids"].append(entry_c)
            if "cusa_id" not in global_extra:
                global_extra["cusa_id"] = raw_id
            fallback_found.append(raw_id)
        if fallback_found:
            print(f"    [page-scan] IDs: {list(dict.fromkeys(fallback_found))}")
        # Back-fill releases that have no cusa
        all_found_tuples = [(e["cusa"], e["region"]) for e in global_extra["cusa_ids"]]
        unset = [r for r in releases if not r.get("cusa")]
        for i, rel in enumerate(unset):
            if i < len(all_found_tuples):
                rel["cusa"]   = all_found_tuples[i][0]
                rel["region"] = all_found_tuples[i][1]
            elif len(all_found_tuples) == 1:
                rel["cusa"]   = all_found_tuples[0][0]
                rel["region"] = all_found_tuples[0][1]

    cusa_id = global_extra.get("cusa_id", "")

    # ── Rename keys for output ────────────────────────────────────────────────
    for rel in releases:
        rel["game"]    = rel.pop("game_direct",   [])
        rel["updates"] = rel.pop("update_direct", [])

    # ── Images (parallel) ─────────────────────────────────────────────────────
    slug     = game_slug(title or title_hint, url, cusa_id)
    all_imgs = ([cover_url] if cover_url else []) + screenshot_urls
    img_lbls = (["cover"]   if cover_url else []) + \
               [f"screenshot_{i+1}" for i in range(len(screenshot_urls))]

    print(f"  Downloading {len(all_imgs)} image(s){f' [{cusa_id}]' if cusa_id else ''}...")
    screenshots = download_screenshots(all_imgs, img_lbls, slug, cusa_id,
                                       driver=driver, page_url=url,
                                       img_pool=img_pool)

    # ── Build extra ───────────────────────────────────────────────────────────
    extra_out = {k: v for k, v in global_extra.items()
                 if k in ("cusa_id", "cusa_ids",
                           "voice", "subtitles", "language", "note",
                           "game_size", "dlc_note",
                           "_no_cusa", "_ps_legacy_id", "_no_screenshots", "_no_game",
                           "_cross_refs")}

    if info_table:
        if "GENRE" in info_table and "genre" not in extra_out:
            extra_out["genre"] = info_table["GENRE"]
        if "RELEASE" in info_table and "release_year" not in extra_out:
            extra_out["release_year"] = info_table["RELEASE"]
        lv = (info_table.get("LANGUAGE") or info_table.get("LANGUAGES")
              or info_table.get("LANG"))
        if lv and "table_language" not in extra_out:
            extra_out["table_language"] = lv
        extra_out["info_table"] = info_table
    if youtube_id:
        extra_out["youtube_id"] = youtube_id

    if (not extra_out.get("cusa_id") and not extra_out.get("_ps_legacy_id")
            and releases and not extra_out.get("_no_cusa")):
        print("  [no-id] full scrape found no game ID — marking _no_cusa=True")
        extra_out["_no_cusa"] = True

    shot_results = [s for s in screenshots if s.get("role", "").startswith("screenshot_")]
    if not shot_results and not extra_out.get("_no_screenshots"):
        print("  [no-screenshots] full scrape found no screenshots")
        extra_out["_no_screenshots"] = True

    has_game_links = any(r.get("game") for r in releases)
    if not has_game_links and releases and not extra_out.get("_no_game"):
        if extra_out.get("_cross_refs"):
            # Cross-ref page — links live on other pages, not here. Don't flag.
            pass
        else:
            print("  [no-game] full scrape found no game links — marking _no_game=True")
            extra_out["_no_game"] = True

    n_game   = sum(len(r["game"])    for r in releases)
    n_upd    = sum(len(r["updates"]) for r in releases)
    print(f"  ✓ {len(releases)} release(s), {n_game} game link(s), "
          f"{n_upd} update/dlc group(s), {len(screenshots)} screenshot(s)")

    return {
        "url":         url,
        "title":       title or title_hint,
        "description": desc,
        "screenshots": screenshots,
        "releases":    releases,
        "extra":       extra_out,
        "scraped_at":  __import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

# ── ★ PATCH ENTRY ─────────────────────────────────────────────────────────────
def patch_entry(entry: dict, driver,
                inter_pool: ThreadPoolExecutor,
                img_pool: ThreadPoolExecutor) -> dict:
    """
    Re-visit a cached entry and fill in only what is missing.
    Fast path: if only images are missing and URLs are already stored, download
    without reloading the page. Full path: re-scrape as needed.
    """
    url        = entry["url"]
    title_hint = entry.get("title", url)
    missing    = entry_missing(entry)
    print(f"  Patching: {', '.join(sorted(missing))}")

    existing_extra = entry.get("extra") or {}
    cusa_id        = existing_extra.get("cusa_id", "")
    slug           = game_slug(entry.get("title") or title_hint, url, cusa_id)

    # ── Fast path: image URLs already stored, just download ──────────────────
    image_only_missing = missing - {"cover", "screenshots"}
    if not image_only_missing:
        shots      = entry.get("screenshots") or []
        cached_urls = {s["role"]: s["url"] for s in shots
                       if isinstance(s, dict) and s.get("url")}
        fast_urls, fast_labels = [], []
        if "cover" in missing:
            cu = cached_urls.get("cover")
            if cu:
                fast_urls.append(cu); fast_labels.append("cover")
        if "screenshots" in missing:
            for s in shots:
                if isinstance(s, dict) and s.get("role", "").startswith("screenshot_"):
                    if s.get("url"):
                        fast_urls.append(s["url"]); fast_labels.append(s["role"])

        if fast_urls:
            print(f"  Fast path: downloading {len(fast_urls)} image(s) from cached URLs")
            fresh    = download_screenshots(fast_urls, fast_labels, slug, cusa_id,
                                            driver=driver, page_url=url,
                                            img_pool=img_pool)
            role_map = {s["role"]: s for s in shots}
            for s in fresh:
                if s.get("local"):
                    role_map[s["role"]] = s
            entry["screenshots"] = list(role_map.values())

            # Check whether all targeted downloads actually succeeded.
            # If any are still null, the cached URLs may be stale/dead (e.g. the
            # site admin updated the page with new image URLs since we last scraped).
            # Fall through to a full re-scrape so we pick up the current live URLs
            # instead of looping forever retrying the same dead ones.
            still_null = [
                s for s in entry["screenshots"]
                if "screenshots" in missing
                and isinstance(s, dict)
                and s.get("role", "").startswith("screenshot_")
                and s.get("local") is None
            ]
            cover_still_null = (
                "cover" in missing
                and not any(s.get("local") for s in entry["screenshots"]
                            if isinstance(s, dict) and s.get("role") == "cover")
            )
            if not still_null and not cover_still_null:
                entry.pop("error", None)
                driver._last_was_page_load = False
                return entry
            # Cached URLs are stale — fall through to full page re-scrape
            print(f"  Fast path: {len(still_null)} screenshot(s) still null after download "
                  f"— cached URLs likely dead, falling through to full re-scrape")

    # ── Full path: reload the page ────────────────────────────────────────────
    for _attempt in range(2):
        try:
            driver.get(url)
            break
        except Exception as _e:
            print(f"  [WARN] page load error (attempt {_attempt+1}): {_e}")
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            if _attempt == 0:
                jitter(2, 0.3)
            else:
                break

    wait_for_dlpsgame(driver)
    jitter(SLEEP_AFTER_LOAD, 0.3)

    # Open collapsed su-spoilers before reading page_source (same reason as scrape_page)
    expand_su_spoilers(driver)

    page_src      = driver.page_source
    soup          = BeautifulSoup(page_src, "html.parser")
    payload_htmls = get_payload_htmls(driver, page_src)

    title, desc, cover_url, screenshot_urls, info_table, youtube_id = \
        extract_metadata(soup)
    releases, global_extra = extract_releases_from_htmls(payload_htmls, page_src)

    # ── Merge info_table + youtube_id into existing_extra ────────────────────
    if info_table:
        if "GENRE" in info_table and "genre" not in existing_extra:
            existing_extra["genre"] = info_table["GENRE"]
        if "RELEASE" in info_table and "release_year" not in existing_extra:
            existing_extra["release_year"] = info_table["RELEASE"]
        lv = (info_table.get("LANGUAGE") or info_table.get("LANGUAGES")
              or info_table.get("LANG"))
        if lv and "table_language" not in existing_extra:
            existing_extra["table_language"] = lv
        if "info_table" not in existing_extra:
            existing_extra["info_table"] = info_table
    if youtube_id and "youtube_id" not in existing_extra:
        existing_extra["youtube_id"] = youtube_id

    # ── ID resolution ─────────────────────────────────────────────────────────
    cusa_id = global_extra.get("cusa_id", "")
    if not cusa_id:
        # Comprehensive scan of raw page source if extract_releases didn't find it
        _PE_ID_RE = re.compile(
            r"\b((?:CUSA|PPSA|LAPY|LBXP|STRN)\d{3,6})\b", re.IGNORECASE)
        _PE_PKG_RE = re.compile(
            r"(?:\b[A-Z]{2}\d{4}[-_])?(CUSA\d{4,6}|PPSA\d{4,6}|LAPY\d{3,6}|LBXP\d{3,6}|STRN\d{3,6})"
            r"(?:[_\-\.]|\b)", re.IGNORECASE)
        page_clean = _html.unescape(re.sub(r"<[^>]+>", "", page_src))
        m = _PE_ID_RE.search(page_clean) or _PE_PKG_RE.search(page_src)
        if m:
            cusa_id = m.group(1).upper()
            global_extra["cusa_id"] = cusa_id

    for k, v in global_extra.items():
        if k in ("region", "password"):
            continue
        if not existing_extra.get(k):
            existing_extra[k] = v
    entry["extra"] = existing_extra

    cusa_found = (existing_extra.get("cusa_id") or
                  existing_extra.get("_ps_legacy_id") or
                  any(r.get("cusa") for r in entry.get("releases", [])))
    if not cusa_found and entry.get("releases") and not existing_extra.get("_no_cusa"):
        print("  [no-id] patch re-scrape found no game ID — marking _no_cusa=True")
        existing_extra["_no_cusa"] = True

    new_shots = [s for s in entry.get("screenshots", [])
                 if s.get("role", "").startswith("screenshot_")]
    if not new_shots and not existing_extra.get("_no_screenshots"):
        print("  [no-screenshots] patch re-scrape found no screenshots")
        existing_extra["_no_screenshots"] = True

    cusa_id = existing_extra.get("cusa_id", cusa_id)

    if not entry.get("title") or entry["title"] == url:
        entry["title"] = title or title_hint
    if not entry.get("description") and desc:
        entry["description"] = desc

    slug = game_slug(entry.get("title") or title_hint, url, cusa_id)

    # ── Rename screenshot folder if CUSA was just discovered ─────────────────
    if cusa_id and not existing_extra.get("_had_cusa_before"):
        old_slug = game_slug(entry.get("title") or title_hint, url, "")
        if old_slug != slug:
            old_dir = Path(SCREENSHOTS_DIR) / old_slug
            new_dir = Path(SCREENSHOTS_DIR) / slug
            if old_dir.exists() and not new_dir.exists():
                old_dir.rename(new_dir)
                existing_extra["_had_cusa_before"] = True
                print(f"  Renamed screenshots folder: {old_slug} → {slug}")
                updated = []
                for s in entry.get("screenshots", []):
                    if isinstance(s, dict) and s.get("local"):
                        new_local = s["local"].replace(
                            f"{SCREENSHOTS_DIR}/{old_slug}/",
                            f"{SCREENSHOTS_DIR}/{slug}/", 1)
                        updated.append({**s, "local": new_local})
                    else:
                        updated.append(s)
                entry["screenshots"] = updated

    # ── Cover + screenshots ───────────────────────────────────────────────────
    need_cover = "cover" in missing
    need_shots = "screenshots" in missing
    new_urls, new_labels = [], []
    if need_cover and cover_url:
        new_urls.append(cover_url); new_labels.append("cover")
    if need_shots:
        for i, su in enumerate(screenshot_urls):
            new_urls.append(su); new_labels.append(f"screenshot_{i+1}")

    if new_urls:
        fresh    = download_screenshots(new_urls, new_labels, slug, cusa_id,
                                        driver=driver, page_url=url,
                                        img_pool=img_pool)
        role_map = {s["role"]: s for s in entry.get("screenshots", [])}
        for s in fresh:
            if s.get("local"):
                role_map[s["role"]] = s
        entry["screenshots"] = list(role_map.values())

    # ── Releases ─────────────────────────────────────────────────────────────
    has_stubs = any(not r.get("game") for r in entry.get("releases", []))
    if "releases" not in entry or not entry.get("releases") or has_stubs:
        if releases:
            # Fresh scrape found release structure — resolve intermediaries
            releases = _resolve_releases(releases, inter_pool, driver, soup)
            for rel in releases:
                rel["game"]    = rel.pop("game_direct",   [])
                rel["updates"] = rel.pop("update_direct", [])
            entry["releases"] = releases
        elif entry.get("releases"):
            # Fresh scrape returned empty releases but cache has stubs —
            # preserve the existing stubs (they contain contributor/CUSA/password)
            # rather than wiping them with an empty list.
            print("  [patch] Fresh scrape found no releases — preserving cached stubs")
        else:
            entry["releases"] = []
        entry.pop("filehosts", None)
        entry.pop("updates",   None)

    # Set _no_game flag if after all attempts we still have no downloadable game links.
    # Mirrors the _no_screenshots / _no_cusa pattern to stop infinite re-patching.
    has_game_after = any(r.get("game") for r in entry.get("releases", []))
    if not has_game_after and not existing_extra.get("_no_game"):
        if existing_extra.get("_cross_refs"):
            pass  # Cross-ref page — game links come from referenced entries
        else:
            print("  [no-game] patch found no game links — marking _no_game=True")
            existing_extra["_no_game"] = True

    entry.pop("error", None)
    entry["scraped_at"] = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return entry

# ── NEW-GAME DISCOVERY ────────────────────────────────────────────────────────
def _discover_page_via_requests(page_url: str) -> list:
    """Fetch a category page with requests and extract game links via BS4.
    Returns list of (href, title) tuples, or empty list on failure."""
    selectors = [
        ".blog-posts .post-title a",
        "h2.post-title.entry-title a",
        "h2.post-title a",
        ".entry-title a",
        ".post.bar.hentry h2 a",
    ]
    selectors_str = ", ".join(selectors)
    try:
        hdrs = {**FETCH_HEADERS,
                "Referer": "https://dlpsgame.com/",
                "Accept":  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        r = _req_session.get(page_url, headers=hdrs, timeout=FETCH_TIMEOUT,
                             allow_redirects=True)
        r.raise_for_status()
        if _is_cf_challenge(r.text):
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        return [(a["href"], a.get_text(strip=True))
                for a in soup.select(selectors_str)
                if a.get("href") and a.get_text(strip=True)]
    except Exception:
        return []


def discover_new_games(driver, known_urls: set) -> list:
    """
    Scrape PS4 category pages and collect new game URLs/titles,
    stop at first known (already in JSON).

    Strategy: try requests first (fast, no CF issue on dlpsgame category pages).
    Fall back to UC browser if requests returns 0 results (e.g. CF blocked).
    """
    new_games = []
    print("\n── New-game discovery (PS4) ──────────────────────────────────────────")

    selectors_string = ", ".join([
        ".blog-posts .post-title a",
        "h2.post-title.entry-title a",
        "h2.post-title a",
        "h1.post-title a",
        ".entry-title a",
        "article.post h2 a",
        ".post-title a",
        ".post.bar.hentry h2 a",
    ])

    _JS_EXTRACT = """
        var seen = {}, out = [];
        var selectors = [
            '.blog-posts .post-title a',
            'h2.post-title.entry-title a',
            'h2.post-title a',
            'h1.post-title a',
            '.entry-title a',
            'article.post h2 a',
            '.post-title a',
            '.post.bar.hentry h2 a'
        ];
        selectors.forEach(function(sel) {
            document.querySelectorAll(sel).forEach(function(a) {
                var h = a.href || '';
                var t = (a.innerText || a.textContent || '').trim();
                if (h && t && !seen[h]) { seen[h] = 1; out.push([h, t]); }
            });
        });
        return out;
    """

    for cat_url in CATEGORY_URLS:
        print(f"  Scanning: {cat_url}")
        for page_num in range(1, MAX_DISCOVERY_PAGES + 1):
            page_url = cat_url if page_num == 1 else f"{cat_url}page/{page_num}/"
            try:
                # ── Primary: requests + BS4 (fast, no browser CF issues) ──────
                raw_links = _discover_page_via_requests(page_url)
                used_browser = False

                # ── Fallback: UC browser (when requests is CF-blocked) ────────
                if not raw_links:
                    print(f"    Page {page_num}: requests got 0 — retrying via browser")
                    driver.get(page_url)
                    wait_for_cf(driver, require_selector="h2.post-title")
                    jitter(1, 0.4)
                    raw_links = driver.execute_script(_JS_EXTRACT) or []
                    if not raw_links:
                        time.sleep(3)
                        raw_links = driver.execute_script(_JS_EXTRACT) or []
                    if not raw_links:
                        soup = BeautifulSoup(driver.page_source, "html.parser")
                        raw_links = [
                            (a["href"], a.get_text(strip=True))
                            for a in soup.select(selectors_string)
                            if a.get("href")
                        ]
                    used_browser = True

                if not raw_links:
                    print(f"    Page {page_num}: no entries found — stopping")
                    break

                source_label = "browser" if used_browser else "requests"
                hit_known = False
                page_new   = 0

                for href, title in raw_links:
                    href_norm = href.split("?")[0].split("#")[0].rstrip("/")
                    if not href_norm.endswith("/"):
                        href_norm += "/"
                    if any(x in href_norm for x in ["/category/", "/tag/", "/page/",
                                                    "/author/", "?", "#"]):
                        continue
                    if "dlpsgame.com/" not in href_norm:
                        continue
                    if href_norm in known_urls:
                        print(f"    Page {page_num}: hit known '{title}' — stopping")
                        hit_known = True
                        break
                    new_games.append({"title": title, "url": href_norm})
                    known_urls.add(href_norm)
                    page_new += 1
                    print(f"    + {title}")

                print(f"    Page {page_num} [{source_label}]: {page_new} new game(s)")
                if hit_known:
                    break

            except Exception as e:
                print(f"    [ERROR] page {page_num}: {e}")
                break

    print(f"  Total new: {len(new_games)}")
    print("─────────────────────────────────────────────────────────────────────\n")
    return new_games

# ── MAIN ──────────────────────────────────────────────────────────────────────
def resolve_cross_refs(cache: dict) -> int:
    """
    Resolve cross-reference entries: populate their releases by copying game
    links from the entries they reference.

    Some pages (e.g. "The Sly Collection") have no download links of their own
    — they just point readers to the individual game pages.  scrape_page stores
    those internal dlpsgame.com links as extra["_cross_refs"].  This function
    runs over the entire cache after every save and fills in releases from the
    referenced entries, so the collection page ends up with the same download
    links as the individual games it covers.

    Each inherited release gets an "_inherited_from" tag so consumers can
    label/group them appropriately.  The source entry's releases are NOT
    modified.

    Returns the number of entries that were resolved.
    """
    resolved = 0
    for entry in cache.values():
        cross_refs = entry.get("extra", {}).get("_cross_refs")
        if not cross_refs:
            continue

        inherited = []
        for ref in cross_refs:
            ref_url   = ref.get("url") if isinstance(ref, dict) else str(ref)
            ref_title = ref.get("title", "") if isinstance(ref, dict) else ""
            # Try exact match, then with/without trailing slash
            src = (cache.get(ref_url)
                   or cache.get(ref_url.rstrip("/") + "/")
                   or cache.get(ref_url.rstrip("/")))
            if not src:
                print(f"  [cross-ref] WARNING: referenced entry not found: {ref_url}")
                continue
            for rel in src.get("releases", []):
                if not rel.get("game"):
                    continue
                # Deep-copy the release so modifications don't affect the source
                import copy
                rel_copy = copy.deepcopy(rel)
                rel_copy["_inherited_from"] = src.get("title") or ref_title or ref_url
                inherited.append(rel_copy)

        if inherited:
            entry["releases"] = inherited
            # Clear _no_game flag — we now have real game links
            entry.get("extra", {}).pop("_no_game", None)
            resolved += 1
            src_titles = list({r["_inherited_from"] for r in inherited})
            print(f"  [cross-ref] '{entry.get('title')}' resolved:"
                  f" {len(inherited)} release(s) from {src_titles}")

    return resolved


def main():
    # ── Load inputs ───────────────────────────────────────────────────────────
    if not Path(INPUT_JSON).is_file():
        # No games.json — full rebuild mode: discovery will scan all category
        # pages and build the list from scratch.  This collects ~6000+ games
        # across 313 pages before the main scraping loop starts.
        print(f"[info] '{INPUT_JSON}' not found — running full discovery to rebuild it.")
        games = []
    else:
        with open(INPUT_JSON, "r", encoding="utf-8") as f:
            games = json.load(f)

    known_urls = set((g.get("url") or "").strip().rstrip("/") + "/"
                     for g in games if g.get("url"))

    # ── Load cache ────────────────────────────────────────────────────────────
    cache      = {}
    cache_file = Path(OUTPUT_JSON)
    if cache_file.is_file():
        cache_size = cache_file.stat().st_size
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            cache = {e["url"]: e for e in loaded if e.get("url")}
        except Exception as e:
            print(f"FATAL: '{OUTPUT_JSON}' exists ({cache_size:,} bytes) but failed to parse: {e}")
            return

        if cache_size > 10_000 and len(cache) == 0:
            print(f"FATAL: '{OUTPUT_JSON}' is {cache_size:,} bytes but 0 valid entries.")
            return

        if len(cache) == 0 and cache_size > 100:
            print(f"WARNING: '{OUTPUT_JSON}' loaded 0 entries ({cache_size:,} bytes).")
            confirm = input("         Continue (re-scrape everything)? [y/N] ").strip().lower()
            if confirm != "y":
                print("Aborted."); return

    statuses   = {url: entry_missing(e) for url, e in cache.items()}
    fully_done = sum(1 for m in statuses.values() if not m)
    need_patch = sum(1 for m in statuses.values() if m)
    print(f"Loaded {len(games)} games — {len(cache)} cached "
          f"({fully_done} complete, {need_patch} need patching).")

    # ── Browser setup ─────────────────────────────────────────────────────────
    _WIN_SIZES = [
        (1920, 1080), (1920, 1080), (1920, 1080),
        (1440, 900),  (1536, 864),  (1366, 768),
        (2560, 1440), (1280, 800),  (1600, 900),
    ]
    win_w, win_h = _random.choice(_WIN_SIZES)

    options = uc.ChromeOptions()
    options.headless = False
    options.add_argument(f"--window-size={win_w},{win_h}")
    print(f"[browser] Window: {win_w}×{win_h} | Between-game sleep: {SLEEP_BETWEEN_GAMES}s")

    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(300)
    driver.set_script_timeout(120)   # 120 s for batch image Promise.all()

    # Install CDP hook to capture data-payload BEFORE clk.sh wraps the hrefs.
    # Must be called once after driver creation; applies to all subsequent loads.
    install_payload_interceptor(driver)
    try:
        driver.command_executor._client_config.timeout = 300
    except Exception:
        try:
            driver.command_executor.set_timeout(300)
        except Exception:
            pass

    # ── Shared thread pools ───────────────────────────────────────────────────
    inter_pool = ThreadPoolExecutor(max_workers=_INTER_WORKERS,
                                    thread_name_prefix="inter")
    img_pool   = ThreadPoolExecutor(max_workers=_IMG_WORKERS,
                                    thread_name_prefix="img")

    def atomic_write(path, data):
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

    def save_games():
        atomic_write(INPUT_JSON, games)

    def save_cache():
        # Resolve cross-reference entries (e.g. collection pages that link to
        # individual game pages) before writing, so they always reflect the
        # latest state of their referenced entries.
        resolve_cross_refs(cache)
        # Write in games[] order (newest first) but NEVER drop cache entries
        # that aren't in games[] yet — that would silently nuke scraped data.
        games_order = {(g.get("url") or "").rstrip("/") + "/": i
                       for i, g in enumerate(games)}
        def _sort_key(entry):
            u = (entry.get("url") or "").rstrip("/") + "/"
            return games_order.get(u, len(games))   # unknown → end of list
        ordered = sorted(cache.values(), key=_sort_key)
        atomic_write(OUTPUT_JSON, ordered)

    try:
        # ── Discovery ─────────────────────────────────────────────────────────
        new_games = discover_new_games(driver, known_urls)
        if new_games:
            games = new_games + games
            save_games()
            print(f"Added {len(new_games)} new game(s) to {INPUT_JSON}")

        # ── Main loop ─────────────────────────────────────────────────────────
        for idx, game in enumerate(games):
            url        = (game.get("url") or "").strip()
            title_hint = game.get("title") or f"Game #{idx+1}"
            if not url:
                continue

            cached = cache.get(url)

            if cached and not statuses.get(url):
                print(f"[{idx+1}/{len(games)}] SKIP: {title_hint}")
                continue

            print(f"\n[{idx+1}/{len(games)}] {title_hint}")
            print(f"  URL: {url}")

            try:
                if cached:
                    cache[url] = patch_entry(cached, driver, inter_pool, img_pool)
                else:
                    cache[url] = scrape_page(url, title_hint, driver,
                                             inter_pool, img_pool)

            except Exception as e:
                print(f"  [ERROR] {e}")
                traceback.print_exc()
                if not cached:
                    cache[url] = {
                        "url": url, "title": title_hint,
                        "description": "", "screenshots": [],
                        "releases": [], "extra": {},
                        "error": str(e),
                    }
                else:
                    cached["error"] = str(e)
                try:
                    driver.execute_script("window.stop();")
                    driver.get("about:blank")
                except Exception:
                    pass

            save_cache()

            # Only sleep after a real page load — image-only fast-path skips it
            did_page_load = getattr(driver, '_last_was_page_load', True)
            if did_page_load:
                jitter(SLEEP_BETWEEN_GAMES, 0.3)   # 8 s ± 30 % (was 45 s)
            driver._last_was_page_load = True

    finally:
        save_cache()
        inter_pool.shutdown(wait=False)
        img_pool.shutdown(wait=False)

        # ── Guaranteed Chrome shutdown ────────────────────────────────────────
        # undetected_chromedriver sometimes leaves Chrome alive after quit().
        # Grab the chromedriver PID before quitting so we can kill its process
        # tree if Chrome survives.
        chromedriver_pid = None
        try:
            chromedriver_pid = driver.service.process.pid
        except Exception:
            pass

        try:
            driver.quit()
        except Exception:
            pass

        # Give Chrome 3 s to exit cleanly after driver.quit()
        time.sleep(3)

        # Force-kill by PID tree — chromedriver is the parent, Chrome is its child.
        # /T kills the full process tree, /F forces termination.
        import subprocess
        if chromedriver_pid:
            try:
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(chromedriver_pid)],
                    capture_output=True,
                )
            except Exception:
                pass

        # Belt-and-braces: kill any leftover chromedriver.exe by name.
        # Safe to do — chromedriver.exe only exists when Selenium spawned it.
        # We do NOT kill chrome.exe globally to avoid closing the user's own browser.
        try:
            subprocess.run(
                ["taskkill", "/F", "/IM", "chromedriver.exe", "/T"],
                capture_output=True,
            )
        except Exception:
            pass

        print("[browser] Chrome shutdown complete.")

    print(f"\nDone! {len(cache)} entries in '{OUTPUT_JSON}'")


if __name__ == "__main__":
    main()
