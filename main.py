"""Notebook Warehouse-deal sniper.

Watches Amazon Warehouse across DE/IT/ES for laptops and fires a
Telegram alert the moment a listing appears or its price moves, so a good deal
can be bought before someone else takes it.

The one thing that makes this work: Amazon hides prices from a session with no
deliverable address. Every driver sets a local delivery postcode at startup —
without it the search page returns ~1 priced card out of 24, with it ~23.
"""

import base64
import json
import os
import re
import signal
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ── SETTINGS ──────────────────────────────────────────────────────────────────

# Every marketplace needs its own Warehouse node (srs/bbn) and its own
# Notebooks category (rh) — reusing one marketplace's path elsewhere makes
# Amazon silently ignore the category filter and serve the wrong department.
#
#   srs / bbn = that marketplace's Warehouse Deals node
#   rh=n:...  = its "Notebooks" category, plus a p_n_g-... RAM-capacity facet
#               so Amazon itself pre-filters to ~16GB+ before we ever see a
#               card — DE additionally stacks a storage-capacity facet.
# DE deliberately uses i=computers, not i=warehouse-deals like the other four:
# that's what Amazon's own UI produced for this exact node/facet combination,
# and srs/bbn (the Warehouse node) still constrain it the same way — verified
# live, prices and stock behave identically to the other marketplaces.
SEARCH_PATHS = {
    "DE": ("/s?i=computers&srs=3581963031&bbn=3581963031"
           "&rh=n%3A427957031%2Cp_n_g-101014849667111%3A88253294031"
           "%2Cp_n_g-1003119721111%3A100549564031%257C27399048031"
           "%257C27399051031%257C27399052031&s=price-asc-rank"),
    "IT": ("/s?i=warehouse-deals&srs=3581999031&bbn=3581999031"
           "&rh=n%3A460158031%2Cp_n_g-1003119721111%3A27399062031"
           "%257C27399065031%257C27399066031&s=price-asc-rank"),
    "ES": ("/s?i=warehouse-deals&srs=3582001031&bbn=3582001031"
           "&rh=n%3A938008031%2Cp_n_g-1003119721111%3A100549558031"
           "%257C27399055031%257C27399058031%257C27399059031&s=price-asc-rank"),
}

DOMAINS = {
    "DE": "https://www.amazon.de",
    "IT": "https://www.amazon.it",
    "ES": "https://www.amazon.es",
}

# What a card on this marketplace should be priced in. Amazon occasionally
# shows a card priced in a different currency (a third-party offer, seemingly),
# and taking that at face value once poisoned a market's stored price — a
# later genuine drop compared a real £ price against a stale € one and
# produced a nonsense "-14%". A price in the wrong currency for its own
# marketplace is treated the same as no price at all.
EXPECTED_CURRENCY = {"DE": "€", "IT": "€", "ES": "€"}

# A deliverable address per marketplace. This is what unlocks the prices.
ZIPS = {
    "DE": "10115",      # Berlin
    "IT": "00100",      # Roma
    "ES": "28001",      # Madrid
}

# Windows dev box and the Linux server disagree on where Chrome lives, and the
# container ships Chromium rather than Chrome. Both are overridable so the same
# file runs unchanged in either place.
CHROME_PATH       = os.environ.get(
    "CHROME_PATH",
    r"C:\Program Files\Google\Chrome\Application\chrome.exe" if os.name == "nt"
    else "/usr/bin/chromium",
)
# Empty means "let Selenium Manager find a driver". The image pins a chromedriver
# built against its own Chromium, so the container sets this and never downloads.
CHROMEDRIVER_PATH = os.environ.get("CHROMEDRIVER_PATH", "")

# One-off experiment: route the listed domains' Chrome traffic through a mobile
# proxy instead of the VPS's own IP, to see whether that holds up against
# Amazon's blocking better than backing off request volume alone. Comma-
# separated tags, e.g. "DE" — empty (the default) means every domain uses the
# VPS's direct connection, unchanged. Credentials live only in the environment,
# same as the Telegram token, never hardcoded here.
PROXY_DOMAINS = {t.strip() for t in os.environ.get("PROXY_DOMAINS", "").split(",") if t.strip()}
PROXY_HOST    = os.environ.get("PROXY_HOST", "")
PROXY_PORT    = os.environ.get("PROXY_PORT", "")
PROXY_USER    = os.environ.get("PROXY_USER", "")
PROXY_PASS    = os.environ.get("PROXY_PASS", "")

# 5s meant near-continuous polling: five browsers overlapped, pages timed out,
# and every timeout used to wipe the database. 10s ran stably for a day, but a
# fixed 10s cadence across 5 Amazon sites, 24/7, is also an easy bot signature
# on its own — and on 2026-09-24 it drew a ~4.5h total block across all five
# domains at once (0 cards, not just 0 priced) that self-lifted on its own.
# Dropping FR/UK helped (3 sites instead of 5), but all 3 now share a single
# mobile-proxy IP (PROXY_DOMAINS below) instead of the VPS's own — fetching
# them in parallel would put 3x the request rate through that one IP at once,
# exactly what a proxy is supposed to avoid. Domains take turns instead: one
# fetch, then a fixed pause, then the next domain in rotation, so the shared
# IP only ever has one request in flight. This is the experiment itself —
# watching how long a single mobile IP holds up at this rate is the point.
ROTATION_INTERVAL = 60
PAGE_TIMEOUT      = 20
DE_TOLERANCE      = 0.05   # prefer DE if within 5% of the lowest price

# Consecutive confirmed absences before a listing is dropped.
GONE_STRIKES      = 2

# Consecutive cycles with literally 0 product cards (not 0 priced — 0 present at
# all) before a domain is treated as blocked rather than a one-off slow page load.
# One Telegram alert fires per block, not one per cycle it stays down, and the
# flag resets the moment the domain reports cards again so a later block still
# alerts.
BLOCK_STRIKES     = 2

# A domain that fails rests, doubling per strike, and is rebuilt in the
# background so one sick marketplace never stalls the healthy ones.
BACKOFF_BASE      = 60
BACKOFF_MAX       = 900
REBUILD_INTERVAL  = 180

MAX_FAILURES      = 3
LONG_COOLDOWN     = 120

# Set to a number to only alert below a given price. None = alert on every
# notebook found, and let the price in the message do the talking.
MAX_PRICE: Optional[float] = None

# A price drop only alerts once it clears this fraction — a €0.01 wobble on a
# €600 laptop is not worth a notification. Relists (a delisted item coming
# back cheaper) use the same bar. Rises never alert, at any size.
PRICE_DROP_THRESHOLD = 0.05

# Adds the "N% off | was X" line by loading each product page. Costs 5-12s per
# alert, paid at the worst possible moment, so it is off by default.
LIST_PRICE_LOOKUP = False

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
# The container mounts a host directory for state so the price history survives
# an image rebuild; everywhere else state sits next to the script as before.
STATE_DIR  = os.environ.get("STATE_DIR", BASE_DIR)
KNOWN_FILE = os.path.join(STATE_DIR, "known_listings.json")
LOCK_FILE  = os.path.join(STATE_DIR, "watcher.lock")

# ── TELEGRAM ──────────────────────────────────────────────────────────────────

# Read from the environment, not hardcoded, so the real token never ends up in
# git history. Set both in a local shell / .env for dev, or telegram.env for
# the deployed container (see DEPLOY.md).
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
TELEGRAM_API       = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

# ── PATTERNS ──────────────────────────────────────────────────────────────────

PRICE_RE = re.compile(r"^(?:[€£]|(?:EUR|GBP)\xa0)[\d.,]+$")

# The Notebooks category node is far leakier than a keyword search would be —
# mechanical keyboards, monitors, a robot vacuum and a software licence have
# all shown up warehouse-priced under it. A title has to actually say
# it is a laptop before it can raise an alert; category membership alone is
# not enough to trust.
NOTEBOOK_WORDS = (
    "laptop", "notebook", "chromebook", "macbook", "ultrabook", "netbook",
    # Bare "portable" is deliberately absent — it is French for laptop, but
    # also plain English for "portable [anything]", which is exactly how a
    # "Portable Gaming Console" (a handheld, not a laptop) slipped past this
    # gate once already. The fuller phrases below aren't ambiguous that way.
    "portátil", "portatil", "ordinateur portable", "pc portable",
    "computer portatile", "portatile",
    "surface pro", "surface laptop", "surface go", "surface book",
    # Premium/business lines whose titles often skip the word "laptop"
    # entirely and rely on the line name alone. "legion" and "rog" are
    # deliberately absent — Legion Go / ROG Ally are handheld gaming
    # consoles, not laptops, and a real Legion/ROG *laptop* listing says
    # "laptop" anyway, so nothing real is missed by leaving them out.
    "vivobook", "ideapad", "thinkpad", "zenbook", "aspire", "swift",
    "spectre", "envy", "pavilion", "inspiron", "latitude", "precision",
    "xps", "probook", "elitebook", "omnibook", "travelmate", "extensa",
    "predator", "nitro", "galaxy book",
)

# Minimum spec bar — anything under either number is filtered out even if it
# is a genuine laptop, so most Chromebooks and entry Windows laptops would
# already fail this on their own; "chromebook" is excluded outright below
# regardless of spec, since some newer ones do clear 16GB/512GB.
MIN_RAM_GB     = 16
MIN_STORAGE_GB = 512

_CAP = r"(\d+)\s*(GB|Go|TB|To)"
# "4GB RAM", "16GB DDR4", "8GB LPDDR5", "16GB Memory", "16GB Unified Memory"
RAM_PATTERNS = (
    re.compile(rf"{_CAP}\s*(?:RAM|DDR\d|LPDDR\d|Unified Memory|Memory)", re.I),
    re.compile(rf"(?:RAM|Memory)\s*[:\-]?\s*{_CAP}", re.I),
)
# "128GB SSD", "64GB eMMC", "1TB Storage", "512GB Flash"
STORAGE_PATTERNS = (
    re.compile(rf"{_CAP}\s*(?:SSD|eMMC|HDD|Flash|Storage|MMC|Disk)", re.I),
    re.compile(rf"(?:SSD|eMMC|HDD|Flash|Storage|Disk)\s*[:\-]?\s*{_CAP}", re.I),
)

# Rejected even when a notebook word is present — a laptop bag or a
# replacement charger can still say "for Notebook" in its own title.
ACCESSORY_WORDS = (
    # bags / cases / sleeves
    "case", "sleeve", "cover", "skin", "bag", "backpack", "pouch",
    "housse", "sacoche", "custodia", "borsa", "funda", "bolsa", "mochila",
    "tasche", "hülle", "huelle", "rucksack",
    # chargers / power
    "charger", "power supply", "power adapter", "ac adapter", "adapter", "adaptor",
    "netzteil", "ladegerät", "ladegeraet", "ladekabel",
    "chargeur", "adaptateur secteur",
    "cargador", "adaptador de corriente",
    "caricabatterie", "alimentatore", "adattatore",
    # batteries / spare parts / repairs
    "battery", "replacement battery", "spare battery",
    "batteria", "batterie", "akku", "bateria",
    "screen replacement", "display replacement", "lcd replacement",
    "hinge", "keyboard replacement", "trackpad replacement", "motherboard",
    "fan replacement", "cooling fan",
    # peripherals / other accessories
    "screen protector", "protector de pantalla", "pellicola protettiva",
    "displayschutzfolie", "protection écran", "protection ecran",
    "stand", "riser", "cooling pad", "lapdesk",
    "docking station", "dock", "usb hub", "hub usb",
    "sticker", "skin decal", "decal",
    "external webcam", "usb webcam", "mouse", "ratón", "raton", "topo", "maus",
    "keyboard cover", "keyboard skin",
    "cable", "kabel", "câble", "cavo",
    "ram module", "memory module", "ram upgrade",
    "stylus", "pen only", "touchscreen pen",
    "learning computer", "kids laptop", "toy laptop",
)

# Handheld gaming PCs — not laptops, but they carry real RAM/storage specs
# and warehouse pricing just like one, so they clear every other check.
# Named explicitly rather than relying only on the absence of a notebook
# word, since a title wording quirk (e.g. "Portable Gaming Console") can
# defeat that gate on its own.
HANDHELD_WORDS = (
    "legion go", "rog ally", "steam deck", "gaming console", "gaming konsole",
    "handheld gaming", "portable gaming console",
)

NL = chr(10)


# ── PRICE HELPERS ─────────────────────────────────────────────────────────────

def normalize_price(raw: str) -> str:
    return raw.replace("EUR\xa0", "€").replace("GBP\xa0", "£")


def parse_price(price_str: str) -> float:
    try:
        return float(re.sub(r"[€£,\s]", "", price_str))
    except ValueError:
        return float("inf")


def currency_of(price_str: str) -> str:
    return "£" if "£" in price_str else "€"


def _cap_gb(patterns: Tuple[re.Pattern, ...], title: str) -> Optional[int]:
    for pat in patterns:
        m = pat.search(title)
        if m:
            value, unit = int(m.group(1)), m.group(2).lower()
            return value * 1024 if unit in ("tb", "to") else value
    return None


def ram_gb(title: str) -> Optional[int]:
    return _cap_gb(RAM_PATTERNS, title)


def storage_gb(title: str) -> Optional[int]:
    return _cap_gb(STORAGE_PATTERNS, title)


def is_notebook(title: str) -> bool:
    low = title.lower()
    if not low:
        return False
    if not any(word in low for word in NOTEBOOK_WORDS):
        return False
    if any(bad in low for bad in ACCESSORY_WORDS):
        return False
    if "chromebook" in low:
        return False
    if any(bad in low for bad in HANDHELD_WORDS):
        return False

    ram = ram_gb(title)
    if ram is None or ram < MIN_RAM_GB:
        return False
    storage = storage_gb(title)
    if storage is None or storage < MIN_STORAGE_GB:
        return False
    return True


def good_enough(price_str: str) -> bool:
    if MAX_PRICE is None:
        return True
    return parse_price(price_str) <= MAX_PRICE


def price_dropped_enough(old_price: str, new_price: str) -> bool:
    """True only for a real drop of at least PRICE_DROP_THRESHOLD — a rise,
    an unparseable price, or a wobble too small to matter all return False.

    Also guards against comparing a price stuck in the wrong currency from
    before EXPECTED_CURRENCY filtering existed — €984.39 to £846.14 is not
    a 14% drop, it is two different currencies, and treating it as a real
    move is how that exact nonsense alert happened."""
    if currency_of(old_price) != currency_of(new_price):
        return False
    old_v, new_v = parse_price(old_price), parse_price(new_price)
    if old_v in (0.0, float("inf")) or new_v == float("inf"):
        return False
    if new_v >= old_v:
        return False
    return (old_v - new_v) / old_v >= PRICE_DROP_THRESHOLD


# ── TYPES ─────────────────────────────────────────────────────────────────────

DomainMap = Dict[str, Tuple[str, str]]              # asin -> (title, price)
AllMap    = Dict[str, Dict[str, Tuple[str, str]]]   # asin -> {tag: (title, price)}
KnownMap  = Dict[str, dict]   # asin -> {"title": str, "prices": {tag: price}}


# ── PERSISTENCE ───────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def human_age(iso: Optional[str]) -> Optional[str]:
    """'3d', '5h', '12m' — how long ago, or None if we cannot tell."""
    if not iso:
        return None
    try:
        delta = datetime.now() - datetime.fromisoformat(iso)
    except ValueError:
        return None
    mins = int(delta.total_seconds() // 60)
    if mins < 1:
        return "just now"
    if mins < 60:
        return f"{mins}m"
    if mins < 60 * 24:
        return f"{mins // 60}h"
    return f"{mins // (60 * 24)}d"


def new_market(price: str, when: str) -> dict:
    return {"price": price, "lowest": price, "lowest_at": when,
            "first_seen": when, "last_seen": when}


def new_entry(title: str, when: str) -> dict:
    return {"title": title, "first_seen": when, "last_seen": when,
            "active": True, "markets": {}}


def load_known() -> KnownMap:
    if not os.path.exists(KNOWN_FILE):
        return {}
    try:
        with open(KNOWN_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return {}

    when = now_iso()
    known: KnownMap = {}
    for asin, entry in raw.items():
        if isinstance(entry, dict) and "markets" in entry:
            known[asin] = entry                         # current schema
            continue

        # migrate: legacy [title, price], or {"title", "prices"}
        if isinstance(entry, dict):
            title  = entry.get("title", "Unknown product")
            prices = dict(entry.get("prices", {}))
        else:
            title  = entry[0] if entry else "Unknown product"
            prices = {}

        migrated = new_entry(title, when)
        # A migrated price is the only datapoint we have, so it seeds the low.
        migrated["markets"] = {t: new_market(p, when) for t, p in prices.items()}
        known[asin] = migrated
    return known


def save_known(known: KnownMap) -> None:
    with open(KNOWN_FILE, "w", encoding="utf-8") as f:
        json.dump(known, f, ensure_ascii=False, indent=2)


# ── LOCK ──────────────────────────────────────────────────────────────────────

def acquire_lock() -> None:
    if os.path.exists(LOCK_FILE):
        with open(LOCK_FILE) as f:
            pid = f.read().strip()
        try:
            # In a container the script is always PID 1, so a lock left behind by
            # a killed run names a PID that os.kill() happily confirms — it is
            # ours. Without this check no restart would ever get past the lock.
            if int(pid) == os.getpid():
                raise OSError
            os.kill(int(pid), 0)
            print(f"ERROR: another instance is already running (PID {pid}). Exiting.")
            raise SystemExit(1)
        except (OSError, ValueError):
            pass  # stale lock — process is dead
    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))


def release_lock() -> None:
    try:
        os.remove(LOCK_FILE)
    except OSError:
        pass


# ── HELPERS ───────────────────────────────────────────────────────────────────

def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


# ── BROWSER ───────────────────────────────────────────────────────────────────

DRIVERS: Dict[str, webdriver.Chrome] = {}


def _relay_pipe(a: socket.socket, b: socket.socket) -> None:
    """Shuttle bytes both ways between two already-connected sockets until
    both directions have hit EOF."""
    def forward(src: socket.socket, dst: socket.socket) -> None:
        try:
            while True:
                data = src.recv(8192)
                if not data:
                    break
                dst.sendall(data)
        except OSError:
            pass
        finally:
            try:
                dst.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    t1 = threading.Thread(target=forward, args=(a, b), daemon=True)
    t2 = threading.Thread(target=forward, args=(b, a), daemon=True)
    t1.start()
    t2.start()
    t1.join()
    t2.join()


def start_local_proxy(upstream_host: str, upstream_port: int,
                       user: str, password: str) -> int:
    """A tiny local, unauthenticated HTTP proxy that forwards everything to an
    authenticated upstream proxy, injecting its Proxy-Authorization header on
    the way through. Chrome's own --proxy-server flag has no way to carry a
    username/password, and the Manifest-V3 extension trick for that turned out
    to fail silently on current Chrome — this sidesteps both.

    Returns the local port; the caller points Chrome's --proxy-server at
    127.0.0.1:<that port> and never needs to know real credentials exist.
    """
    auth_header = "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()

    def handle_client(client_sock: socket.socket) -> None:
        upstream: Optional[socket.socket] = None
        try:
            request = b""
            while b"\r\n\r\n" not in request:
                chunk = client_sock.recv(4096)
                if not chunk:
                    return
                request += chunk
            head, _, rest = request.partition(b"\r\n\r\n")
            lines = head.split(b"\r\n")
            method, target, version = lines[0].decode().split(" ")

            upstream = socket.create_connection((upstream_host, upstream_port), timeout=15)

            if method.upper() == "CONNECT":
                upstream.sendall(
                    f"CONNECT {target} {version}\r\n"
                    f"Proxy-Authorization: {auth_header}\r\n"
                    f"Host: {target}\r\n\r\n".encode())
                resp = b""
                while b"\r\n\r\n" not in resp:
                    chunk = upstream.recv(4096)
                    if not chunk:
                        break
                    resp += chunk
                client_sock.sendall(resp)
                if b" 200 " not in resp.split(b"\r\n", 1)[0]:
                    return
            else:
                header_lines = [l for l in lines[1:]
                                 if not l.lower().startswith(b"proxy-authorization")]
                header_lines.append(f"Proxy-Authorization: {auth_header}".encode())
                upstream.sendall(lines[0] + b"\r\n" +
                                  b"\r\n".join(header_lines) + b"\r\n\r\n" + rest)

            _relay_pipe(client_sock, upstream)
        except OSError:
            pass
        finally:
            client_sock.close()
            if upstream:
                upstream.close()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(50)
    local_port = server.getsockname()[1]

    def accept_loop() -> None:
        while True:
            client, _ = server.accept()
            threading.Thread(target=handle_client, args=(client,), daemon=True).start()

    threading.Thread(target=accept_loop, daemon=True).start()
    return local_port


_local_proxy_port: Optional[int] = None
_local_proxy_lock = threading.Lock()


def ensure_local_proxy() -> int:
    """Starts the local relay at most once and hands back its port every time
    after — every proxied domain shares the one relay, since it is generic."""
    global _local_proxy_port
    with _local_proxy_lock:
        if _local_proxy_port is None:
            _local_proxy_port = start_local_proxy(
                PROXY_HOST, int(PROXY_PORT), PROXY_USER, PROXY_PASS)
            log(f"  local proxy relay on 127.0.0.1:{_local_proxy_port} "
                f"-> {PROXY_HOST}:{PROXY_PORT}")
        return _local_proxy_port


def _log_egress_ip(driver: webdriver.Chrome, tag: str) -> None:
    """Only called while a proxy test is active — confirms which IP a domain's
    traffic is actually leaving from, since a misconfigured proxy extension
    fails silently back to the direct connection rather than erroring."""
    try:
        driver.get("https://api.ipify.org")
        ip = driver.find_element(By.TAG_NAME, "body").text.strip()
        log(f"  [{tag}] egress IP: {ip}")
    except Exception as exc:
        log(f"  [{tag}] egress IP check failed: {type(exc).__name__}")


def build_driver(tag: str) -> webdriver.Chrome:
    opts = Options()
    opts.binary_location = CHROME_PATH
    opts.page_load_strategy = "eager"        # DOM is enough; subresources are noise
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument(f"--user-agent={USER_AGENT}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_experimental_option(
        "prefs", {"profile.managed_default_content_settings.images": 2})

    if tag in PROXY_DOMAINS and PROXY_HOST and PROXY_PORT:
        local_port = ensure_local_proxy()
        opts.add_argument(f"--proxy-server=127.0.0.1:{local_port}")
        log(f"  [{tag}] routing through proxy {PROXY_HOST}:{PROXY_PORT}")

    service = Service(executable_path=CHROMEDRIVER_PATH) if CHROMEDRIVER_PATH else None
    driver = webdriver.Chrome(options=opts, service=service) if service \
        else webdriver.Chrome(options=opts)
    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"},
    )
    driver.set_page_load_timeout(PAGE_TIMEOUT + 10)
    return driver


def _click(driver: webdriver.Chrome, selector: str) -> bool:
    try:
        driver.execute_script(
            "arguments[0].click();", driver.find_element(By.CSS_SELECTOR, selector))
        return True
    except Exception:
        return False


def set_location(driver: webdriver.Chrome, tag: str, attempts: int = 3) -> bool:
    """Pin a delivery postcode. Without this Amazon serves pages with no prices."""
    base = DOMAINS[tag]
    for attempt in range(1, attempts + 1):
        try:
            driver.get(base + "/")
            WebDriverWait(driver, 25).until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#nav-global-location-popover-link, #glow-ingress-block")))

            _click(driver, "#sp-cc-rejectall-link")      # decline non-essential cookies
            time.sleep(1)

            _click(driver, "#nav-global-location-popover-link")
            WebDriverWait(driver, 25).until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#GLUXZipUpdateInput")))

            box = driver.find_element(By.CSS_SELECTOR, "#GLUXZipUpdateInput")
            box.clear()
            box.send_keys(ZIPS[tag])
            time.sleep(0.5)
            _click(driver, "#GLUXZipUpdate input, #GLUXZipUpdate-announce")
            time.sleep(4)
            _click(driver, "button[name='glowDoneButton'], .a-popover-footer input[type='submit']")
            time.sleep(3)

            driver.get(base + "/")
            time.sleep(2)
            line = driver.find_element(By.CSS_SELECTOR, "#glow-ingress-line2").text.strip()
            if any(ch.isdigit() for ch in line):
                log(f"  [{tag}] delivering to {line[:24]}")
                return True
        except Exception as exc:
            log(f"  [{tag}] location attempt {attempt}/{attempts} failed ({type(exc).__name__})")
    log(f"  [{tag}] WARNING: no delivery location — prices will be missing")
    return False


def build_and_bootstrap(tag: str) -> Tuple[webdriver.Chrome, bool]:
    """Returns the driver and whether it has a delivery address.

    Without an address Amazon serves priceless pages, which look exactly like
    "no stock" — so a failure here must not be reported as a healthy domain.
    """
    driver = build_driver(tag)
    if PROXY_DOMAINS:
        _log_egress_ip(driver, tag)
    return driver, set_location(driver, tag)


def quit_driver(driver: Optional[webdriver.Chrome]) -> None:
    if driver:
        try:
            driver.quit()
        except Exception:
            pass


# ── SCRAPING ──────────────────────────────────────────────────────────────────

class Challenged(Exception):
    """Cards loaded but not one had a price — the delivery address was lost."""


def card_price(card) -> Optional[str]:
    for span in card.find_all("span", class_="a-color-base"):
        raw = span.get_text(strip=True)
        if PRICE_RE.match(raw):
            return normalize_price(raw)

    offscreen = card.select_one(".a-price:not(.a-text-price) .a-offscreen")
    if offscreen:
        raw = offscreen.get_text(strip=True)
        if PRICE_RE.match(raw):
            return normalize_price(raw)
    return None


def parse_results(html: str, tag: str) -> Tuple[DomainMap, int, int]:
    """Returns (notebook results, cards on page, cards carrying any price)."""
    soup = BeautifulSoup(html, "html.parser")
    results: DomainMap = {}
    cards = priced = 0
    expected = EXPECTED_CURRENCY[tag]

    for card in soup.select("[data-component-type='s-search-result']"):
        cards += 1
        raw_price = card_price(card)
        if raw_price:
            priced += 1
        asin = card.get("data-asin", "")
        if not asin:
            anchor = card.select_one("a[href*='/dp/']")
            if anchor:
                m = re.search(r"/dp/([A-Z0-9]{10})", anchor.get("href", ""))
                if m:
                    asin = m.group(1)
        if not asin:
            continue

        title_tag = card.select_one("h2 span")
        title     = title_tag.get_text(strip=True) if title_tag else ""
        if not is_notebook(title):
            continue

        if raw_price and currency_of(raw_price) == expected:
            results[asin] = (title, raw_price)

    return results, cards, priced


def list_price(tag: str, asin: str) -> Optional[str]:
    """The "was" price, for the `N% off | was X` line.

    It is not in the search results at all — only on the product page, which
    measured 4.7-12.4s to load. That delay lands on the alert itself, which is
    the one place latency actually costs you a deal, so this is off by default.
    Turn on LIST_PRICE_LOOKUP if you would rather have the discount figure.
    """
    if not LIST_PRICE_LOOKUP:
        return None

    driver = DRIVERS.get(tag)
    if driver is None:
        return None
    try:
        driver.get(f"{DOMAINS[tag]}/dp/{asin}")
        soup   = BeautifulSoup(driver.page_source, "html.parser")
        prices = [p.get_text(strip=True)
                  for p in soup.select("#corePrice_feature_div .a-offscreen")
                  if PRICE_RE.match(p.get_text(strip=True))]
        # Amazon lists the struck-through reference price first when there is one.
        return max(prices, key=parse_price) if len(prices) > 1 else None
    except Exception as exc:
        log(f"  [{tag}] list price lookup failed for {asin}: {type(exc).__name__}")
        return None


def fetch_domain(tag: str) -> DomainMap:
    driver = DRIVERS[tag]
    driver.get(DOMAINS[tag] + SEARCH_PATHS[tag])
    try:
        WebDriverWait(driver, PAGE_TIMEOUT).until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, "[data-component-type='s-search-result']")))
    except Exception:
        pass

    results, cards, priced = parse_results(driver.page_source, tag)

    # Results but not one price on the whole page means the delivery address
    # was lost, not that stock ran out. Treating that as "no stock" is what
    # retired every listing and made them all relist minutes later.
    if cards and not priced:
        raise Challenged(f"{cards} cards, 0 prices — delivery address lost")

    if cards:
        block_strikes.pop(tag, None)
        blocked_alerted.discard(tag)
    else:
        block_strikes[tag] = block_strikes.get(tag, 0) + 1
        if block_strikes[tag] >= BLOCK_STRIKES and tag not in blocked_alerted:
            blocked_alerted.add(tag)
            _post(f"🚫 [{tag}] looks blocked — 0 product cards for "
                  f"{block_strikes[tag]} cycles running. Staying quiet on "
                  f"this domain until it recovers.")

    log(f"  [{tag}] {len(results)} notebook(s)  ({priced}/{cards} cards priced)")
    return results


# ── DEAL SELECTION ────────────────────────────────────────────────────────────

def best_deal(asin: str, by_domain: Dict[str, Tuple[str, str]]) -> Tuple[str, str, str, str]:
    """Return (tag, title, price_str, url) — prefer DE if within DE_TOLERANCE of cheapest."""
    cheapest_tag   = min(by_domain, key=lambda t: parse_price(by_domain[t][1]))
    cheapest_price = parse_price(by_domain[cheapest_tag][1])

    if "DE" in by_domain and parse_price(by_domain["DE"][1]) <= cheapest_price * (1 + DE_TOLERANCE):
        tag = "DE"
    else:
        tag = cheapest_tag

    title, price_str = by_domain[tag]
    return tag, title, price_str, f"{DOMAINS[tag]}/dp/{asin}"


# ── TELEGRAM ──────────────────────────────────────────────────────────────────

def _post(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("  Telegram not configured — set TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID")
        return False
    try:
        resp = requests.post(
            TELEGRAM_API,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=10,
        )
        if not resp.ok:
            log(f"  Telegram error {resp.status_code}: {resp.text[:120]}")
            return False
        return True
    except requests.RequestException as exc:
        log(f"  Telegram failed: {exc}")
        return False


def _links(asin: str, tag: str, url: str) -> str:
    links = f"🔗 {url}"
    if tag != "DE":
        links += f"{NL}🔗 https://www.amazon.de/dp/{asin}"
    return links


def lowest_line(price: str, lowest: Optional[str]) -> Optional[str]:
    """The line that turns 'this exists' into 'this is worth buying'."""
    if not lowest:
        return None
    if currency_of(lowest) != currency_of(price):
        return None
    low_v, now_v = parse_price(lowest), parse_price(price)
    if low_v in (0.0, float("inf")) or now_v == float("inf"):
        return None
    if now_v < low_v:
        return f"⭐ lowest ever seen  (previous best {lowest})"
    if now_v == low_v:
        return "matches lowest ever seen"
    return f"lowest ever seen {lowest}  (+{(now_v - low_v) / low_v * 100:.1f}%)"


def build_message(marker: str, asin: str, tag: str, title: str, price: str,
                  url: str, prev: Optional[str] = None,
                  was: Optional[str] = None,
                  lowest: Optional[str] = None,
                  since: Optional[str] = None) -> str:
    """Price first, then title, then a bare URL so Telegram renders a preview."""
    lines = [f"{marker} {price}  {title}", url]

    if was:
        was_v, now_v = parse_price(was), parse_price(price)
        if was_v not in (0.0, float("inf")) and now_v != float("inf") and was_v > now_v:
            lines.append(f"{(was_v - now_v) / was_v * 100:.1f}% off  |  was {was}")

    low = lowest_line(price, lowest)
    if low:
        lines.append(low)

    meta = f"Amazon Warehouse  |  {tag}"
    age = human_age(since)
    if age and age != "just now":
        meta += f"  |  tracked {age}"
    lines.append(meta)

    if prev:
        prev_v, now_v = parse_price(prev), parse_price(price)
        if prev_v not in (0.0, float("inf")) and now_v != float("inf"):
            move = (now_v - prev_v) / prev_v * 100
            lines.append(f"Old price: {prev}  →  New price: {price}  ({move:+.1f}%)")
        else:
            lines.append(f"Old price: {prev}  →  New price: {price}")

    if tag != "DE":
        lines.append(f"DE: https://www.amazon.de/dp/{asin}")
    return NL.join(lines)


def send_new(asin: str, tag: str, title: str, price: str, url: str,
             lowest: Optional[str] = None, since: Optional[str] = None,
             relisted: bool = False) -> None:
    _post(build_message("🔁" if relisted else "🆕", asin, tag, title, price, url,
                        was=list_price(tag, asin), lowest=lowest, since=since))


def send_price_change(asin: str, tag: str, title: str, old: str, new: str,
                      url: str, lowest: Optional[str] = None,
                      since: Optional[str] = None) -> None:
    marker = "📉" if parse_price(new) < parse_price(old) else "📈"
    _post(build_message(marker, asin, tag, title, new, url, prev=old,
                        was=list_price(tag, asin), lowest=lowest, since=since))


def send_digest(lines: List[str]) -> None:
    """One Telegram message per listing, so each is its own tappable card
    instead of a wall of text lumping dozens of products together."""
    if not lines:
        _post("Notebook watcher started — no notebooks listed right now.")
        return

    _post(f"Notebook watcher started — {len(lines)} listing(s) live:")
    for block in lines:
        _post(block)
        time.sleep(0.3)   # stay clear of Telegram's per-chat flood limit


# ── DOMAIN HEALTH ─────────────────────────────────────────────────────────────

cooldown_until: Dict[str, float]           = {}
strikes:        Dict[str, int]             = {}
_rebuild:       Optional[threading.Thread] = None
_last_rebuild:  Dict[str, float]           = {}

block_strikes:   Dict[str, int] = {}   # consecutive 0-card cycles, per domain
blocked_alerted: Set[str]       = set()  # domains already notified for the current block


def active_domains() -> List[str]:
    now = time.time()
    return [t for t in DOMAINS if t in DRIVERS and cooldown_until.get(t, 0) <= now]


def note_failure(tag: str, reason: str) -> None:
    strikes[tag] = strikes.get(tag, 0) + 1
    wait = min(BACKOFF_BASE * (2 ** (strikes[tag] - 1)), BACKOFF_MAX)
    cooldown_until[tag] = time.time() + wait
    log(f"  [{tag}] {reason} — backing off {wait}s")


def note_success(tag: str) -> None:
    strikes.pop(tag, None)
    cooldown_until.pop(tag, None)


def _rebuild_worker(tags: List[str]) -> None:
    for tag in tags:
        log(f"  [{tag}] rebuilding driver ...")
        quit_driver(DRIVERS.pop(tag, None))
        try:
            driver, located = build_and_bootstrap(tag)
            DRIVERS[tag] = driver
            if located:
                note_success(tag)
                log(f"  [{tag}] driver back up")
            else:
                note_failure(tag, "rebuilt but no delivery location")
        except Exception as exc:
            log(f"  [{tag}] rebuild failed: {exc}")


def maybe_rebuild(tags: List[str]) -> None:
    """Re-create sick drivers off the hot path."""
    global _rebuild
    if _rebuild is not None and _rebuild.is_alive():
        return

    now = time.time()
    due = [t for t in tags if now - _last_rebuild.get(t, 0.0) > REBUILD_INTERVAL]
    if not due:
        return
    for tag in due:
        _last_rebuild[tag] = now

    _rebuild = threading.Thread(target=_rebuild_worker, args=(due,), daemon=True)
    _rebuild.start()


# ── CYCLE ─────────────────────────────────────────────────────────────────────

def run_cycle(known: KnownMap, first_cycle: bool,
              tags: List[str]) -> Tuple[AllMap, List[str], Set[str]]:
    """Fetches exactly the given domains — the caller decides which (and how
    many) run this turn, so a single-domain rotation and an all-at-once cycle
    both go through the same machinery."""
    combined: AllMap    = {}
    sick:     List[str] = []
    alerted:  Set[str]  = set()
    healthy:  Set[str]  = set()     # domains that actually answered this cycle
    if not tags:
        return combined, sick, healthy

    with ThreadPoolExecutor(max_workers=len(tags)) as pool:
        futures = {pool.submit(fetch_domain, tag): tag for tag in tags}

        for future in as_completed(futures):
            tag = futures[future]
            try:
                results = future.result()
            except Exception as exc:
                note_failure(tag, f"fetch failed ({type(exc).__name__})")
                sick.append(tag)
                continue

            note_success(tag)
            healthy.add(tag)
            for asin, (title, price) in results.items():
                combined.setdefault(asin, {})[tag] = (title, price)

            if first_cycle:
                continue

            # Alert off this domain the instant it reports rather than waiting
            # for the slowest marketplace — those seconds are the whole point.
            # Cheapest first, so the best deal reaches the phone first.
            for asin, (title, price) in sorted(
                    results.items(), key=lambda kv: parse_price(kv[1][1])):
                if asin in alerted or not good_enough(price):
                    continue
                entry  = known.get(asin)
                market = (entry or {}).get("markets", {}).get(tag, {})
                # Read the low BEFORE reconciliation writes this sighting into it.
                low    = market.get("lowest")
                since  = (entry or {}).get("first_seen")

                if entry is None:
                    btag, btitle, bprice, url = best_deal(asin, combined[asin])
                    log(f"  *** NEW [{btag}] ***  {bprice}  |  {btitle[:58]}")
                    send_new(asin, btag, btitle, bprice, url, since=since)
                    alerted.add(asin)
                    continue

                old = market.get("price")

                if not entry.get("active", True):
                    # A relist alerts regardless of price direction — it was
                    # truly gone (collect_gone() only retires after
                    # GONE_STRIKES confirmed-absent cycles) and worth knowing
                    # about again either way, with `lowest` carrying the price
                    # history into the message rather than gating it.
                    #
                    # But a listing sitting right at the page-1 cutoff can
                    # flicker gone/back every cycle at an unchanged price as
                    # other listings shuffle past it — that isn't news, it's
                    # pagination noise, so an unchanged price stays silent.
                    if old and parse_price(price) == parse_price(old):
                        continue
                    btag, btitle, bprice, url = best_deal(asin, combined[asin])
                    blow = entry.get("markets", {}).get(btag, {}).get("lowest")
                    log(f"  *** RELIST [{btag}] ***  {old} → {bprice}  |  {btitle[:52]}")
                    send_new(asin, btag, btitle, bprice, url, lowest=blow,
                             since=since, relisted=True)
                    alerted.add(asin)
                    continue

                # Only drops of at least PRICE_DROP_THRESHOLD. A rise, or a
                # drop too small to matter, is not something you act on.
                if old and price_dropped_enough(old, price):
                    log(f"  *** DROP [{tag}] ***  {old} → {price}  |  {title[:55]}")
                    send_price_change(asin, tag, title, old, price,
                                      f"{DOMAINS[tag]}/dp/{asin}",
                                      lowest=low, since=since)
                    alerted.add(asin)

    when = now_iso()
    for asin, by_domain in combined.items():
        _, btitle, _, _ = best_deal(asin, by_domain)
        entry = known.get(asin)
        if entry is None:
            entry = known[asin] = new_entry(btitle, when)

        entry["title"]     = btitle
        entry["last_seen"] = when
        entry["active"]    = True

        for tag, (_, price) in by_domain.items():
            market = entry["markets"].get(tag)
            if market is None:
                entry["markets"][tag] = new_market(price, when)
                continue
            market["price"]     = price
            market["last_seen"] = when
            stored_lowest = market.get("lowest")
            # A "lowest" left over from before EXPECTED_CURRENCY filtering
            # existed can be stuck in the wrong currency; self-heal it onto
            # the current (now currency-verified) price rather than compare
            # numbers across currencies forever.
            if (not stored_lowest or currency_of(stored_lowest) != currency_of(price)
                    or parse_price(price) < parse_price(stored_lowest)):
                market["lowest"]    = price
                market["lowest_at"] = when

    return combined, sick, healthy


misses: Dict[str, int] = {}


def collect_gone(known: KnownMap, current: AllMap, healthy: Set[str]) -> List[str]:
    """Listings that really are gone.

    A fetch failure is not a disappearance. An ASIN only counts as gone once
    every marketplace that was carrying it has answered us successfully and
    still does not list it, GONE_STRIKES cycles running. Without this a single
    timeout wipes the database and the whole catalogue re-alerts as new.
    """
    gone: List[str] = []
    for asin, entry in known.items():
        if asin in current:
            misses.pop(asin, None)
            continue
        if not entry.get("active", True):
            continue              # already retired, do not report it again

        stored = set(entry.get("markets", {}))
        if stored and not stored.issubset(healthy):
            continue              # a domain that had it is down — trust nothing
        if not stored and not healthy:
            continue

        misses[asin] = misses.get(asin, 0) + 1
        if misses[asin] >= GONE_STRIKES:
            gone.append(asin)
    return gone


# ── MAIN ──────────────────────────────────────────────────────────────────────

def _sigterm(signum, frame) -> None:
    """`kill <pid>` and systemd's default stop signal are both SIGTERM, which
    Python does not turn into a catchable exception on its own — without this
    handler it skips straight past the `finally` below and every Chrome
    process this run opened is orphaned. Left running, those add up across
    restarts until the box runs out of RAM."""
    raise KeyboardInterrupt


def main() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    signal.signal(signal.SIGTERM, _sigterm)
    acquire_lock()

    known:          KnownMap = load_known()
    is_first_cycle: bool     = len(known) == 0
    failures:       int      = 0

    log(f"Notebook sniper starting  |  domains={list(DOMAINS)}  |  "
        f"rotation: 1 domain every {ROTATION_INTERVAL}s")
    if is_first_cycle:
        log("No saved data — first cycle sends a digest of everything live now.")
    else:
        live = sum(1 for e in known.values() if e.get("active", True))
        log(f"Loaded {len(known)} known ASIN(s) — {live} live, "
            f"{len(known) - live} retired (price history kept).")

    try:
        # Sequential on purpose: the location popover is unreliable when five
        # browsers race it, and this is a one-time cost.
        log("Bootstrapping browsers and delivery addresses (one-time, ~2-3 min) ...")
        for tag in DOMAINS:
            try:
                driver, located = build_and_bootstrap(tag)
                DRIVERS[tag] = driver
                if not located:
                    note_failure(tag, "no delivery location at startup")
            except Exception as exc:
                log(f"  [{tag}] startup failed: {exc}")
        if not DRIVERS:
            log("No usable browsers — exiting.")
            return
        log(f"Ready  |  {len(DRIVERS)}/{len(DOMAINS)} domains live")

        domain_order = list(DOMAINS)   # fixed rotation order
        turn = 0

        while True:
            try:
                started = time.time()
                tag  = domain_order[turn % len(domain_order)]
                turn += 1
                tags = [tag] if tag in active_domains() else []

                log(f"--- Fetching [{tag}] ---" if tags else
                    f"--- Skipping [{tag}] (cooling down) ---")
                current, sick, healthy = run_cycle(known, is_first_cycle, tags)

                gone = collect_gone(known, current, healthy)

                if is_first_cycle:
                    if current:
                        log(f"Fresh start — digesting {len(current)} "
                            f"notebook listing(s) from [{tag}].")
                        blocks = []
                        deals  = [best_deal(a, current[a]) for a in current]
                        for btag, title, price, url in sorted(
                                deals, key=lambda d: parse_price(d[2])):
                            log(f"  [{btag}] {price}  |  {title[:60]}")
                            asin = url.rsplit("/", 1)[-1]
                            blocks.append(build_message("•", asin, btag, title, price, url))
                        send_digest(blocks)
                    is_first_cycle = False
                else:
                    for asin in gone:
                        # Retired, not deleted — the price history is the whole
                        # point, and a return is a relist rather than a new find.
                        entry = known[asin]
                        entry["active"] = False
                        misses.pop(asin, None)
                        lows = [m.get("lowest", "") for m in entry["markets"].values()]
                        best = min([l for l in lows if l], key=parse_price, default="")
                        log(f"  --- GONE ---  low {best}  |  {entry['title'][:56]}")

                save_known(known)
                log(f"Done in {time.time() - started:.1f}s  |  "
                    f"{len(current)} active  |  {len(gone)} removed")
                failures = 0

                if sick:
                    maybe_rebuild(sick)

                time.sleep(max(0.0, ROTATION_INTERVAL - (time.time() - started)))

            except KeyboardInterrupt:
                log("Stopped by user.")
                break

            except Exception as exc:
                failures += 1
                log(f"ERROR [{failures}/{MAX_FAILURES}]: {exc}")
                if failures >= MAX_FAILURES:
                    log(f"Too many failures — cooling down {LONG_COOLDOWN}s ...")
                    failures = 0
                    time.sleep(LONG_COOLDOWN)
                else:
                    time.sleep(ROTATION_INTERVAL)
    finally:
        for tag in list(DRIVERS):
            quit_driver(DRIVERS.pop(tag, None))
        release_lock()


if __name__ == "__main__":
    main()
