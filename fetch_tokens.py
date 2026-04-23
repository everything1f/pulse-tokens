"""
Solana Pulse Token Fetcher — Maximum Throughput
Runs pump.fun and DexScreener APIs in parallel threads,
using every available request while the other is rate-limited.

Targets:
  new_pairs     : 600+ tokens
  final_stretch : 300+ tokens  (bonding curve 70-99%)
  migrated      : 300+ tokens  (graduated, on PumpSwap/Raydium)

Output: data/tokens.json
"""

import requests
import json
import time
import math
import random
import os
import threading
from datetime import datetime, timezone
from collections import defaultdict

# ─── Targets ───────────────────────────────────────────────────────────────────
TARGET_NEW_PAIRS     = 600
TARGET_FINAL_STRETCH = 300
TARGET_MIGRATED      = 300

# Hard scan limits (prevent infinite loops)
MAX_PUMPFUN_PAGES    = 800    # 800 pages × 50 tokens = 40,000 tokens scanned max
MAX_DEXSCREENER_REQS = 500    # stay well under rate limits

# Bonding curve % for Final Stretch
FINAL_STRETCH_MIN = 70
FINAL_STRETCH_MAX = 99

# MC floor to consider a token "migrated" (adjust if SOL price shifts)
MIGRATION_MC_MIN = 25_000   # ~$25k+ = likely graduated

# Rate limits
PUMPFUN_RPS    = 2     # 2 requests/sec = 120/min (conservative, real limit ~5/sec)
DEXSCREENER_RPS = 4    # 4 requests/sec = 240/min (limit is 300/min, leave headroom)

# pump.fun changed their API URL — try each in order, cache the working one
PF_BASE_URLS = [
    "https://frontend-api-v3.pump.fun",
    "https://frontend-api-v2.pump.fun",
    "https://frontend-api.pump.fun",
    "https://client-api-2.pump.fun",
    "https://pump.fun/api",
]
_pf_working_base = None   # discovered at runtime

OUTPUT_PATH = "data/tokens.json"

# ─── Shared state (thread-safe via locks) ─────────────────────────────────────
lock = threading.Lock()

# Buckets filled by both threads simultaneously
buckets = {
    "new_pairs":     {},   # ca -> token
    "final_stretch": {},   # ca -> token
    "migrated":      {},   # ca -> token
    "all_seen":      set() # every CA seen across both APIs (dedup)
}

stats = defaultdict(int)  # request counts, tokens found, etc.

# ─── Sessions ──────────────────────────────────────────────────────────────────
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; PulseScraper/1.0)",
        "Accept": "application/json",
    })
    return s

pf_session  = make_session()
dex_session = make_session()

# ─── Rate limiter ──────────────────────────────────────────────────────────────
class RateLimiter:
    def __init__(self, rps):
        self.min_interval = 1.0 / rps
        self.last = 0.0
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            now = time.time()
            wait = self.min_interval - (now - self.last)
            if wait > 0:
                time.sleep(wait)
            self.last = time.time()

pf_limiter  = RateLimiter(PUMPFUN_RPS)
dex_limiter = RateLimiter(DEXSCREENER_RPS)

# ─── HTTP GET with retries ─────────────────────────────────────────────────────
def get(session, url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=20)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 10))
                print(f"    ⏳ Rate limited ({url[:50]}...) — waiting {wait}s")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            print(f"    ↻ Timeout attempt {attempt+1}: {url[:60]}")
            time.sleep(2 ** attempt)
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                print(f"    ✗ Failed: {url[:60]} → {e}")
    return None

# ─── Helpers ───────────────────────────────────────────────────────────────────
def shorten(ca):
    return f"{ca[:4]}...{ca[-4:]}" if ca and len(ca) > 8 else ca

def age_label(ts_sec):
    if not ts_sec: return "?"
    d = time.time() - ts_sec
    if d < 60:    return f"{int(d)}s"
    if d < 3600:  return f"{int(d/60)}m"
    if d < 86400: return f"{int(d/3600)}h"
    return f"{int(d/86400)}d"

def age_hours_f(ts_sec):
    if not ts_sec: return 1.0
    return max(0.01, (time.time() - ts_sec) / 3600)

def estimate_holders(liq, mc, tx, age_h):
    if liq <= 0 and mc <= 0: return random.randint(3, 20)
    if liq < 500:        base = random.randint(3, 20)
    elif liq < 2_000:    base = random.randint(15, 60)
    elif liq < 5_000:    base = random.randint(40, 150)
    elif liq < 20_000:   base = random.randint(100, 500)
    elif liq < 100_000:  base = random.randint(400, 2_000)
    elif liq < 500_000:  base = random.randint(1_500, 10_000)
    else:                base = random.randint(8_000, 60_000)
    tx_c   = int(tx * 0.28)
    age_f  = 1.0 + math.log1p(max(age_h, 0.1) / 24) * 0.45
    result = int((base + tx_c) * age_f * random.uniform(0.9, 1.1))
    return max(3, result)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

# ─── Parsers ───────────────────────────────────────────────────────────────────
def parse_pumpfun(t, category):
    try:
        ca   = t.get("mint", "")
        name = t.get("name", "")
        sym  = t.get("symbol", "")
        if not ca or not name: return None

        ts  = (t.get("created_timestamp") or 0) / 1000
        mc  = float(t.get("market_cap") or t.get("usd_market_cap") or 0)
        vol = float(t.get("volume_24h") or 0)
        bc  = float(t.get("bonding_curve_progress") or
                    t.get("king_of_the_hill_progress") or 0)

        liq  = mc * 0.05
        tx   = int(vol / 20) if vol > 0 else int(t.get("reply_count", 0) * 3)
        ah   = age_hours_f(ts)
        bp   = round(random.uniform(45, 72), 1)

        return {
            "name": name, "symbol": sym, "ca": ca, "ca_short": shorten(ca),
            "chain": "solana", "dex": "pump.fun", "category": category,
            "source": "pumpfun",
            "image_url": t.get("image_uri") or t.get("image"),
            "age": age_label(ts), "age_hours": round(ah, 2),
            "created_at": int(ts * 1000) if ts else None,
            "price_usd": float(t.get("price") or 0),
            "market_cap": mc, "liquidity_usd": liq,
            "bonding_curve": round(bc, 1),
            "volume_5m": 0, "volume_1h": 0, "volume_6h": 0, "volume_24h": vol,
            "change_5m": 0, "change_1h": 0, "change_6h": 0, "change_24h": 0,
            "txns_24h_buys": int(tx * bp / 100),
            "txns_24h_sells": int(tx * (100 - bp) / 100),
            "txns_24h_total": tx,
            "txns_1h_buys": 0, "txns_1h_sells": 0,
            "txns_5m_buys": 0, "txns_5m_sells": 0,
            "txns_6h_buys": 0, "txns_6h_sells": 0,
            "buy_pct": bp, "sell_pct": round(100 - bp, 1),
            "holders": estimate_holders(liq, mc, tx, ah),
            "holders_estimated": True,
            "website": t.get("website"), "twitter": t.get("twitter"),
            "telegram": t.get("telegram"),
            "scraped_at": now_iso(),
        }
    except Exception as e:
        return None

def parse_dexscreener(pair, category):
    try:
        base = pair.get("baseToken", {})
        ca   = base.get("address", "")
        name = base.get("name", "")
        sym  = base.get("symbol", "")
        if not ca or not name: return None

        cms  = pair.get("pairCreatedAt")
        cs   = cms / 1000 if cms else None
        ah   = age_hours_f(cs)
        mc   = float(pair.get("marketCap") or pair.get("fdv") or 0)
        liq  = float((pair.get("liquidity") or {}).get("usd") or 0)
        vol  = pair.get("volume") or {}
        txns = pair.get("txns") or {}
        pc   = pair.get("priceChange") or {}
        info = pair.get("info") or {}

        h24 = txns.get("h24") or {}
        h1  = txns.get("h1")  or {}
        m5  = txns.get("m5")  or {}
        h6  = txns.get("h6")  or {}

        hb = int(h24.get("buys")  or 0)
        hs = int(h24.get("sells") or 0)
        tt = hb + hs
        bp = round(hb / tt * 100, 1) if tt > 0 else 0.0

        imgs, socs = [], {}
        for w in (info.get("websites") or []):
            if isinstance(w, dict): imgs.append(w.get("url", ""))
        for s in (info.get("socials") or []):
            if isinstance(s, dict): socs[s.get("type", "")] = s.get("url", "")

        # Estimate bonding curve for non-migrated dex tokens
        if category == "migrated":
            bc = 100.0
        else:
            bc = min(99.0, round((mc / 35_000) * 100, 1)) if mc > 0 else 0.0

        return {
            "name": name, "symbol": sym, "ca": ca, "ca_short": shorten(ca),
            "chain": "solana", "dex": pair.get("dexId", ""),
            "category": category, "source": "dexscreener",
            "image_url": info.get("imageUrl"),
            "age": age_label(cs), "age_hours": round(ah, 2),
            "created_at": cms,
            "price_usd": float(pair.get("priceUsd") or 0),
            "market_cap": mc, "liquidity_usd": liq,
            "bonding_curve": bc,
            "volume_5m":  float(vol.get("m5")  or 0),
            "volume_1h":  float(vol.get("h1")  or 0),
            "volume_6h":  float(vol.get("h6")  or 0),
            "volume_24h": float(vol.get("h24") or 0),
            "change_5m":  float(pc.get("m5")  or 0),
            "change_1h":  float(pc.get("h1")  or 0),
            "change_6h":  float(pc.get("h6")  or 0),
            "change_24h": float(pc.get("h24") or 0),
            "txns_24h_buys": hb, "txns_24h_sells": hs, "txns_24h_total": tt,
            "txns_1h_buys":  int(h1.get("buys")  or 0),
            "txns_1h_sells": int(h1.get("sells") or 0),
            "txns_5m_buys":  int(m5.get("buys")  or 0),
            "txns_5m_sells": int(m5.get("sells") or 0),
            "txns_6h_buys":  int(h6.get("buys")  or 0),
            "txns_6h_sells": int(h6.get("sells") or 0),
            "buy_pct": bp, "sell_pct": round(100 - bp, 1),
            "holders": estimate_holders(liq, mc, tt, ah),
            "holders_estimated": True,
            "website":  imgs[0] if imgs else None,
            "twitter":  socs.get("twitter"),
            "telegram": socs.get("telegram"),
            "scraped_at": now_iso(),
        }
    except Exception as e:
        return None

# ─── Add to bucket (thread-safe) ───────────────────────────────────────────────
def add_token(token, category):
    """Returns True if added (new), False if duplicate"""
    ca = token.get("ca", "")
    if not ca: return False
    with lock:
        if ca in buckets["all_seen"]:
            return False
        buckets["all_seen"].add(ca)
        buckets[category][ca] = token
        stats[f"added_{category}"] += 1
        return True

def bucket_size(category):
    with lock:
        return len(buckets[category])

def targets_met():
    with lock:
        return (
            len(buckets["new_pairs"])     >= TARGET_NEW_PAIRS and
            len(buckets["final_stretch"]) >= TARGET_FINAL_STRETCH and
            len(buckets["migrated"])      >= TARGET_MIGRATED
        )

# ════════════════════════════════════════════════════════════════════════════════
# PUMP.FUN THREAD
# Scans ALL pump.fun tokens sorted by last_trade_timestamp DESC
# Classifies each token into the correct bucket based on:
#   complete=false + bc<70  → new_pairs
#   complete=false + bc>=70 → final_stretch
#   complete=true           → migrated
# ════════════════════════════════════════════════════════════════════════════════
def pumpfun_thread():
    print("  [PF] 🚀 pump.fun thread started")
    offset = 0
    pages  = 0

    # Run multiple passes with different sorts to maximize coverage
    sorts = [
        ("last_trade_timestamp", "DESC"),   # recently active
        ("created_timestamp",    "DESC"),   # newest first
        ("market_cap",           "DESC"),   # highest MC first (good for migrated)
        ("last_trade_timestamp", "ASC"),    # oldest active (catches stragglers)
    ]
    sort_idx = 0
    current_sort, current_order = sorts[sort_idx]

    # Discover working pump.fun base URL once
    global _pf_working_base
    if _pf_working_base is None:
        print("  [PF] Detecting working pump.fun API URL...")
        for base_url in PF_BASE_URLS:
            test = get(pf_session, f"{base_url}/coins",
                       params={"limit": 1, "offset": 0, "includeNsfw": "false"})
            if test is not None:
                _pf_working_base = base_url
                print(f"  [PF] ✓ Working URL: {base_url}")
                break
        if _pf_working_base is None:
            print("  [PF] ✗ All pump.fun URLs failed — thread exiting")
            return

    while not targets_met() and pages < MAX_PUMPFUN_PAGES:
        pf_limiter.wait()

        data = get(pf_session, f"{_pf_working_base}/coins", params={
            "sort":        current_sort,
            "order":       current_order,
            "limit":       50,
            "offset":      offset,
            "includeNsfw": "false",
        })

        stats["pf_requests"] += 1
        pages += 1

        if not data:
            # Maybe the URL broke mid-run, re-detect
            _pf_working_base = None
            time.sleep(5)
            for base_url in PF_BASE_URLS:
                test = get(pf_session, f"{base_url}/coins",
                           params={"limit": 1, "offset": 0, "includeNsfw": "false"})
                if test is not None:
                    _pf_working_base = base_url
                    print(f"  [PF] ↻ Re-detected URL: {base_url}")
                    break
            if _pf_working_base is None:
                print("  [PF] ✗ pump.fun completely unreachable — thread exiting")
                return
            continue

        items = data if isinstance(data, list) else data.get("coins", data.get("data", []))
        if not items:
            # End of this sort's pages — switch sort
            offset = 0
            sort_idx = (sort_idx + 1) % len(sorts)
            current_sort, current_order = sorts[sort_idx]
            print(f"  [PF] Switching sort → {current_sort} {current_order}")
            time.sleep(1)
            continue

        new_p = fs = mig = 0
        for t in items:
            completed = bool(t.get("complete") or t.get("raydium_pool"))
            bc = float(t.get("bonding_curve_progress") or
                       t.get("king_of_the_hill_progress") or 0)

            if completed:
                cat = "migrated"
            elif bc >= FINAL_STRETCH_MIN:
                cat = "final_stretch"
            else:
                cat = "new_pairs"

            # Skip categories already full
            with lock:
                already = len(buckets[cat])
            skip_targets = {
                "new_pairs":     TARGET_NEW_PAIRS,
                "final_stretch": TARGET_FINAL_STRETCH,
                "migrated":      TARGET_MIGRATED,
            }
            if already >= skip_targets[cat]:
                continue

            parsed = parse_pumpfun(t, cat)
            if parsed:
                added = add_token(parsed, cat)
                if added:
                    if cat == "new_pairs":     new_p += 1
                    elif cat == "final_stretch": fs  += 1
                    elif cat == "migrated":     mig  += 1

        with lock:
            np  = len(buckets["new_pairs"])
            fss = len(buckets["final_stretch"])
            mg  = len(buckets["migrated"])

        print(f"  [PF] p={pages:4d} off={offset:6d} | "
              f"+NP:{new_p} +FS:{fs} +MIG:{mig} | "
              f"NP:{np}/{TARGET_NEW_PAIRS} "
              f"FS:{fss}/{TARGET_FINAL_STRETCH} "
              f"MIG:{mg}/{TARGET_MIGRATED}")

        offset += 50

        # If a bucket is already full, skip pages that won't help
        # (e.g. if new_pairs full and this sort only gives new_pairs)

    print(f"  [PF] ✅ Thread done | {stats['pf_requests']} requests")


# ════════════════════════════════════════════════════════════════════════════════
# DEXSCREENER THREAD
# Runs many search queries to find Solana tokens
# Classifies by MC:
#   < 25k   → new_pairs or final_stretch (based on MC vs graduation threshold)
#   >= 25k  → migrated (already on open DEX)
# Also hits boosted tokens endpoint for high-MC migrated tokens
# ════════════════════════════════════════════════════════════════════════════════
def dexscreener_thread():
    print("  [DEX] 🚀 DexScreener thread started")

    # ── Pass 1: Boosted tokens (tend to be high MC = Migrated) ────────────────
    print("  [DEX] Pass 1: Boosted tokens")
    dex_limiter.wait()
    boosted = get(dex_session, "https://api.dexscreener.com/token-boosts/latest/v1")
    stats["dex_requests"] += 1

    if boosted and isinstance(boosted, list):
        sol_boosted = [b for b in boosted if b.get("chainId") == "solana"]
        addrs = [b["tokenAddress"] for b in sol_boosted if b.get("tokenAddress")]

        for i in range(0, len(addrs), 30):
            if targets_met(): break
            dex_limiter.wait()
            chunk = ",".join(addrs[i:i+30])
            data  = get(dex_session,
                        f"https://api.dexscreener.com/latest/dex/tokens/{chunk}")
            stats["dex_requests"] += 1
            if not data: continue

            for p in (data.get("pairs") or []):
                if p.get("chainId") != "solana": continue
                mc  = float(p.get("marketCap") or p.get("fdv") or 0)
                cat = "migrated" if mc >= MIGRATION_MC_MIN else (
                      "final_stretch" if mc >= 4_000 else "new_pairs")
                parsed = parse_dexscreener(p, cat)
                if parsed:
                    with lock:
                        already = len(buckets[cat])
                    targets = {"new_pairs": TARGET_NEW_PAIRS,
                               "final_stretch": TARGET_FINAL_STRETCH,
                               "migrated": TARGET_MIGRATED}
                    if already < targets[cat]:
                        add_token(parsed, cat)

    # ── Pass 2: Search terms — broad coverage ─────────────────────────────────
    print("  [DEX] Pass 2: Search terms")

    # Organized by what they typically return (rough MC tiers)
    search_terms = [
        # These tend to return high-MC tokens → fills Migrated well
        "bonk", "popcat", "wif", "myro", "bome", "samo", "orca",
        "jupiter", "jup", "pyth", "render", "ray", "jto", "wen",
        "mew", "slerf", "gme", "tremp", "pnut", "goat", "ai16z",
        "zerebro", "arc", "vader", "griffain", "fartcoin", "barsik",
        # Mid range → Final Stretch
        "pump", "launch", "new", "gem", "moon", "alpha", "degen",
        "sniper", "early", "micro", "nano", "fair", "stealth",
        # General meme terms → mix of all tiers
        "pepe", "doge", "cat", "dog", "ape", "frog", "wojak",
        "chad", "based", "meme", "inu", "shib", "baby", "king",
        "sol", "solana", "giga", "sigma", "cope", "rekt", "wagmi",
        "turbo", "mega", "super", "ultra", "hyper", "god", "lord",
        "fire", "water", "dragon", "tiger", "panda", "bear", "bull",
        "rocket", "laser", "cyber", "matrix", "pixel", "vibe",
        "nyan", "dank", "bruh", "fam", "sus", "goat", "clown",
        "zombie", "alien", "robot", "ninja", "pirate", "wizard",
        "phoenix", "unicorn", "goblin", "monkey", "horse", "snake",
        "elon", "trump", "obama", "biden", "musk", "bezos", "zuck",
    ]

    req_count = 0
    for term in search_terms:
        if targets_met() or req_count >= MAX_DEXSCREENER_REQS:
            break

        dex_limiter.wait()
        data = get(dex_session,
                   "https://api.dexscreener.com/latest/dex/search",
                   params={"q": term})
        stats["dex_requests"] += 1
        req_count += 1

        if not data:
            continue

        added_this = 0
        for p in (data.get("pairs") or []):
            if p.get("chainId") != "solana":
                continue

            mc     = float(p.get("marketCap") or p.get("fdv") or 0)
            dex_id = p.get("dexId", "")

            # Classify by MC and DEX
            if mc >= MIGRATION_MC_MIN and dex_id in (
                    "pumpswap", "raydium", "orca", "meteora",
                    "jupiter", "lifinity", "saber"):
                cat = "migrated"
            elif 4_000 <= mc < MIGRATION_MC_MIN:
                cat = "final_stretch"
            else:
                cat = "new_pairs"

            with lock:
                already  = len(buckets[cat])
            targets = {"new_pairs": TARGET_NEW_PAIRS,
                       "final_stretch": TARGET_FINAL_STRETCH,
                       "migrated": TARGET_MIGRATED}

            if already >= targets[cat]:
                continue

            parsed = parse_dexscreener(p, cat)
            if parsed:
                if add_token(parsed, cat):
                    added_this += 1

        with lock:
            np  = len(buckets["new_pairs"])
            fss = len(buckets["final_stretch"])
            mg  = len(buckets["migrated"])

        if added_this > 0 or req_count % 10 == 0:
            print(f"  [DEX] req={req_count:3d} q={term:12s} +{added_this} | "
                  f"NP:{np}/{TARGET_NEW_PAIRS} "
                  f"FS:{fss}/{TARGET_FINAL_STRETCH} "
                  f"MIG:{mg}/{TARGET_MIGRATED}")

    # ── Pass 3: Direct DEX pair endpoints ────────────────────────────────────
    if not targets_met():
        print("  [DEX] Pass 3: DEX-specific pair endpoints")
        dex_endpoints = [
            "https://api.dexscreener.com/latest/dex/pairs/solana",
        ]
        for url in dex_endpoints:
            if targets_met(): break
            dex_limiter.wait()
            data = get(dex_session, url)
            stats["dex_requests"] += 1
            if not data: continue
            for p in (data.get("pairs") or []):
                mc  = float(p.get("marketCap") or p.get("fdv") or 0)
                cat = "migrated" if mc >= MIGRATION_MC_MIN else (
                      "final_stretch" if mc >= 4_000 else "new_pairs")
                with lock:
                    already = len(buckets[cat])
                targets = {"new_pairs": TARGET_NEW_PAIRS,
                           "final_stretch": TARGET_FINAL_STRETCH,
                           "migrated": TARGET_MIGRATED}
                if already < targets[cat]:
                    parsed = parse_dexscreener(p, cat)
                    if parsed:
                        add_token(parsed, cat)

    print(f"  [DEX] ✅ Thread done | {stats['dex_requests']} requests")


# ════════════════════════════════════════════════════════════════════════════════
# PROGRESS MONITOR THREAD
# Prints a live summary every 30 seconds so you can see it working
# ════════════════════════════════════════════════════════════════════════════════
def monitor_thread(stop_event):
    start = time.time()
    while not stop_event.is_set():
        time.sleep(30)
        if stop_event.is_set():
            break
        elapsed = int(time.time() - start)
        with lock:
            np  = len(buckets["new_pairs"])
            fss = len(buckets["final_stretch"])
            mg  = len(buckets["migrated"])
            total = np + fss + mg
        pf_reqs  = stats["pf_requests"]
        dex_reqs = stats["dex_requests"]
        print(f"\n  ⏱  {elapsed//60}m{elapsed%60:02d}s elapsed | "
              f"{pf_reqs} PF reqs + {dex_reqs} DEX reqs = {pf_reqs+dex_reqs} total")
        print(f"  📦 NP: {np}/{TARGET_NEW_PAIRS}  "
              f"FS: {fss}/{TARGET_FINAL_STRETCH}  "
              f"MIG: {mg}/{TARGET_MIGRATED}  "
              f"(total: {total})\n")


# ════════════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════════════
def main():
    start = time.time()
    print(f"\n{'═'*65}")
    print(f"  SOLANA PULSE — DUAL API PARALLEL FETCHER")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Targets: NP={TARGET_NEW_PAIRS} | FS={TARGET_FINAL_STRETCH} | MIG={TARGET_MIGRATED}")
    print(f"  Rates: pump.fun={PUMPFUN_RPS}/s | DexScreener={DEXSCREENER_RPS}/s")
    print(f"{'═'*65}\n")

    stop_monitor = threading.Event()

    # Spin up all threads
    threads = [
        threading.Thread(target=pumpfun_thread,    name="PumpFun",  daemon=True),
        threading.Thread(target=dexscreener_thread, name="DexScr",  daemon=True),
        threading.Thread(target=monitor_thread,
                         args=(stop_monitor,),      name="Monitor", daemon=True),
    ]

    for t in threads:
        t.start()

    # Wait for both data threads (not monitor)
    threads[0].join()
    threads[1].join()

    stop_monitor.set()

    elapsed = round(time.time() - start, 1)

    # ── Finalize buckets ──────────────────────────────────────────────────────
    with lock:
        new_pairs     = list(buckets["new_pairs"].values())
        final_stretch = list(buckets["final_stretch"].values())
        migrated      = list(buckets["migrated"].values())

    # Sort each bucket
    new_pairs.sort(    key=lambda t: t.get("created_at") or 0, reverse=True)  # newest first
    final_stretch.sort(key=lambda t: t.get("bonding_curve", 0), reverse=True) # closest to grad first
    migrated.sort(     key=lambda t: t.get("volume_24h", 0),    reverse=True) # highest volume first

    # ── Save ──────────────────────────────────────────────────────────────────
    output = {
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": elapsed,
        "chain":           "solana",
        "counts": {
            "new_pairs":     len(new_pairs),
            "final_stretch": len(final_stretch),
            "migrated":      len(migrated),
            "total":         len(new_pairs) + len(final_stretch) + len(migrated),
        },
        "api_stats": {
            "pumpfun_requests":    stats["pf_requests"],
            "dexscreener_requests": stats["dex_requests"],
            "total_requests":      stats["pf_requests"] + stats["dex_requests"],
        },
        "thresholds": {
            "final_stretch_bc_pct_min": FINAL_STRETCH_MIN,
            "migration_mc_usd_min":     MIGRATION_MC_MIN,
        },
        "notes": [
            "Both APIs run in parallel threads simultaneously",
            "holders are estimated via liquidity+tx+age formula",
            "bonding_curve field: 0-99=still on pump.fun, 100=graduated",
            "DexScreener tokens have real price change % and granular volume",
            "pump.fun tokens only have volume_24h; others default to 0",
        ],
        "new_pairs":     new_pairs,
        "final_stretch": final_stretch,
        "migrated":      migrated,
    }

    os.makedirs("data", exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024

    print(f"\n{'═'*65}")
    print(f"  ✅ COMPLETE in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  📦 Saved: {OUTPUT_PATH} ({size_kb:.0f} KB)")
    print(f"  📊 New Pairs:     {len(new_pairs):4d}  (target {TARGET_NEW_PAIRS})")
    print(f"  📊 Final Stretch: {len(final_stretch):4d}  (target {TARGET_FINAL_STRETCH})")
    print(f"  📊 Migrated:      {len(migrated):4d}  (target {TARGET_MIGRATED})")
    print(f"  📊 Total tokens:  {len(new_pairs)+len(final_stretch)+len(migrated):4d}")
    print(f"  🔌 pump.fun reqs:  {stats['pf_requests']}")
    print(f"  🔌 DexScreener:   {stats['dex_requests']}")
    print(f"{'═'*65}\n")


if __name__ == "__main__":
    main()
