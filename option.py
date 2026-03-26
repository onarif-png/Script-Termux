#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║   SMART MONEY SCANNER  —  OI + IV + Price Direction                 ║
║                                                                      ║
║   Usage:   ivr spy  /  ivr qqq  /  ivr xly                         ║
║                                                                      ║
║   Signal logic (per your framework):                                 ║
║   🟢🔥 Price↑ + OI↑ + IV↑ + Calls dominant  = Explosive Bull       ║
║   🟢   Price↑ + OI↑ + Calls dominant         = Strong Bull          ║
║   🔴🔥 Price↓ + OI↑ + IV↑ + Puts dominant   = Panic Drop           ║
║   🔴   Price↓ + OI↑ + Puts dominant          = Strong Bear          ║
║   ⚠️   Price↑ + OI↓                          = Short Covering        ║
║   ⚠️   Price↓ + OI↓                          = Long Liquidation      ║
║   ↔️   Call↑ + Put↑ + IV flat                = Straddle/Noise        ║
║   🎯   Price near gamma wall (max OI strike)  = Squeeze Risk         ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import sys, json, math, time, glob, warnings, csv, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
import requests

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent
COOKIE_PATH  = SCRIPT_DIR / "cookies.json"
API_KEY_PATH = SCRIPT_DIR / "Massive_key.txt"
BASE_URL     = "https://api.massive.com"

TOP_N        = 20
MAX_HOLDINGS = 1000
MAX_WORKERS  = 8
CALL_DELAY   = 0.15

# Expiry windows
SHORT_MIN = 5    # days — short-dated for volume/OI data
SHORT_MAX = 30
MID_MIN   = 45   # days — mid-term for IV + Greeks
MID_MAX   = 65


# ─────────────────────────────────────────────────────────────────
#  MASSIVE API
# ─────────────────────────────────────────────────────────────────
class MassiveAPI:
    def __init__(self, key: str):
        self._key = key

    def _get(self, path, params=None):
        time.sleep(CALL_DELAY)
        s = requests.Session()
        s.headers["Authorization"] = f"Bearer {self._key}"
        try:
            r = s.get(BASE_URL + path, params=params or {}, timeout=20)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 15))
                time.sleep(wait)
                return self._get(path, params)
            if r.status_code in (403, 404): return {}
            r.raise_for_status()
            return r.json()
        except: return {}

    def get_options(self, ticker, exp_min=None, exp_max=None, as_of=None):
        """Fetch options snapshot. Without expiry filter → day data populated."""
        params = {"limit": 250}
        if exp_min: params["expiration_date.gte"] = exp_min
        if exp_max: params["expiration_date.lte"] = exp_max
        if as_of:   params["as_of"] = as_of
        return self._get(f"/v3/snapshot/options/{ticker}", params).get("results", [])

    def ping(self):
        return bool(self._get("/v3/snapshot/options/AAPL", {"limit": 1}))


# ─────────────────────────────────────────────────────────────────
#  STOCKANALYSIS.COM — price, price change, industry (cookie)
# ─────────────────────────────────────────────────────────────────
def _sa_session() -> requests.Session:
    raw        = json.loads(COOKIE_PATH.read_text())
    cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in raw if "name" in c)
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
        "Accept":     "application/json, text/plain, */*",
        "Referer":    "https://stockanalysis.com/",
        "Cookie":     cookie_str,
    })
    return s


def _resolve(struct, data, out, prefix=""):
    if not isinstance(struct, dict): return
    for k, idx in struct.items():
        full = f"{prefix}.{k}" if prefix else k
        if not isinstance(idx, int) or idx >= len(data): continue
        val = data[idx]
        if isinstance(val, dict): _resolve(val, data, out, full)
        else: out[full] = val


def _parse_flat(json_data):
    result = {}
    for node in json_data.get("nodes", []):
        if not isinstance(node, dict) or node.get("type") != "data": continue
        data = node.get("data", [])
        if not isinstance(data, list) or not data: continue
        if isinstance(data[0], dict):
            _resolve(data[0], data, result)
    return result


def fetch_stock_data(ticker: str, session: requests.Session) -> dict:
    """
    Get stock price, price change %, and industry from stockanalysis.com.
    Returns dict with price, price_chg_pct, industry.
    """
    sym = ticker.lower().replace("-", ".")
    result = {"price": None, "price_chg_pct": None, "stock_vol": None, "industry": ""}

    # Price + change from main stock page
    try:
        r = session.get(f"https://stockanalysis.com/stocks/{sym}/__data.json", timeout=10)
        if r.status_code == 200:
            flat = _parse_flat(r.json())
            p    = flat.get("info.quote.p") or flat.get("price")
            cp   = flat.get("info.quote.cp")   # price change %
            if p:  result["price"]         = float(p)
            if cp: result["price_chg_pct"] = float(cp)
            v = flat.get("info.quote.v") or flat.get("info.quote.volume")
            if v:  result["stock_vol"]     = int(float(v))
    except: pass

    # Industry from company page
    try:
        r2 = session.get(f"https://stockanalysis.com/stocks/{sym}/company/__data.json", timeout=8)
        if r2.status_code == 200:
            flat2 = _parse_flat(r2.json())
            for key in ("company.industry", "info.industry", "industry"):
                val = flat2.get(key)
                if val and isinstance(val, str) and val.strip():
                    result["industry"] = val.strip()
                    break
            if not result["industry"]:
                for k, v in flat2.items():
                    if "industry" in k.lower() and v and isinstance(v, str):
                        result["industry"] = v.strip(); break
    except: pass

    return result


# ─────────────────────────────────────────────────────────────────
#  OPTIONS DATA PROCESSING
# ─────────────────────────────────────────────────────────────────
def last_trading_day(today: datetime) -> str:
    """Return last weekday (skip weekends)."""
    d = today - timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d.strftime("%Y-%m-%d")


def process_options_chain(contracts: list, atm_price: float = None) -> dict:
    """
    Process a list of option contracts.
    Separates calls vs puts, computes OI, volume, finds ATM metrics.
    atm_price: if known, used to find ATM zone (±5%). Otherwise uses delta.
    """
    call_oi = put_oi = call_vol = put_vol = 0
    atm_iv  = atm_delta = atm_strike = None
    best_atm_dist = 9.0

    # For gamma wall: OI by strike
    strike_oi: dict = {}

    for c in contracts:
        ct      = (c.get("details") or {}).get("contract_type", "").lower()
        strike  = float((c.get("details") or {}).get("strike_price") or
                        c.get("strike_price") or 0)
        oi      = int(c.get("open_interest") or 0)
        day     = c.get("day") or {}
        vol     = int(day.get("volume") or 0)
        iv      = c.get("implied_volatility")
        greeks  = c.get("greeks") or {}
        delta   = greeks.get("delta")

        if ct == "call":
            call_oi  += oi
            call_vol += vol
        elif ct == "put":
            put_oi  += oi
            put_vol += vol

        # Track OI per strike for gamma wall
        if strike > 0:
            strike_oi[strike] = strike_oi.get(strike, 0) + oi

        # Find ATM contract (delta-based, calls only)
        if ct == "call" and delta is not None and iv is not None:
            iv_f  = float(iv)
            dlt_f = float(delta)
            if 0.01 < iv_f < 5.0 and dlt_f > 0:
                dist = abs(dlt_f - 0.50)
                if dist < best_atm_dist:
                    best_atm_dist = dist
                    atm_iv    = iv_f
                    atm_delta = dlt_f
                    atm_strike = strike

    # Gamma wall: strike with highest OI within ±10% of ATM
    gamma_wall = None
    if atm_strike and strike_oi:
        lo, hi = atm_strike * 0.90, atm_strike * 1.10
        nearby = {s: o for s, o in strike_oi.items() if lo <= s <= hi}
        if nearby:
            gamma_wall = max(nearby, key=nearby.get)

    return {
        "call_oi":    call_oi,
        "put_oi":     put_oi,
        "total_oi":   call_oi + put_oi,
        "call_vol":   call_vol,
        "put_vol":    put_vol,
        "total_vol":  call_vol + put_vol,
        "pcr":        round(put_oi / call_oi, 2) if call_oi > 0 else None,
        "atm_iv":     atm_iv,
        "atm_delta":  atm_delta,
        "atm_strike": atm_strike,
        "gamma_wall": gamma_wall,
    }


def get_signal(price_chg: float, oi_chg: float, iv_chg: float,
               call_oi: int, put_oi: int,
               call_vol: int, put_vol: int,
               cvol_chg, pvol_chg,
               atm_strike: float, gamma_wall: float) -> tuple:
    """
    Signal matrix combining price + OI + IV + volume direction.

    OI change proxy: when OI delta unavailable (=0), use call/put VOLUME
    ratio as the directional proxy — volume spikes reveal where money flows.

    PCR (put/call volume): <0.7 = bullish flow, >1.3 = bearish flow
    """
    price_up   = price_chg is not None and price_chg > 0.3
    price_down = price_chg is not None and price_chg < -0.3
    price_flat = not price_up and not price_down
    iv_up      = iv_chg is not None and iv_chg > 0.5
    iv_down    = iv_chg is not None and iv_chg < -0.5

    # Money flow direction — priority: OI delta → volume delta → volume ratio
    oi_up = oi_down = False
    if oi_chg is not None and abs(oi_chg) > 100:
        oi_up   = oi_chg > 0
        oi_down = oi_chg < 0
    elif cvol_chg is not None and pvol_chg is not None:
        # Volume CHANGE shows where money is flowing vs yesterday
        if cvol_chg > pvol_chg and cvol_chg > 0:
            oi_up = True    # call volume growing faster = bullish flow building
        elif pvol_chg > cvol_chg and pvol_chg > 0:
            oi_down = True  # put volume growing faster = bearish flow building
    else:
        # Fallback: today's call/put volume ratio
        total_vol = call_vol + put_vol
        if total_vol > 50:
            cvr   = call_vol / total_vol
            oi_up   = cvr > 0.55
            oi_down = cvr < 0.45

    # Call/put OI positioning bias
    call_dom = call_oi > put_oi * 1.2
    put_dom  = put_oi  > call_oi * 1.2

    # Volume flow bias (today's actual activity)
    call_vol_dom = call_vol > put_vol * 1.3
    put_vol_dom  = put_vol  > call_vol * 1.3

    # Combined positioning signal
    bullish_pos = call_dom or call_vol_dom
    bearish_pos = put_dom  or put_vol_dom

    # Gamma squeeze proximity
    gamma_sq = (atm_strike and gamma_wall and
                abs(atm_strike - gamma_wall) / gamma_wall < 0.02)

    # ── Signal matrix ────────────────────────────────────────────
    # Priority: most conviction first
    if price_up and oi_up and iv_up and bullish_pos:
        sig, bonus = "🟢🔥 EXPL BULL ", 35
    elif price_up and oi_up and bullish_pos:
        sig, bonus = "🟢  STRONG BULL", 25
    elif price_up and oi_up:
        sig, bonus = "🟢  BULL BUILD ", 15
    elif price_up and not oi_up and not oi_down:
        sig, bonus = "🟡  PRICE RALLY", 8
    elif price_down and oi_up and iv_up and bearish_pos:
        sig, bonus = "🔴🔥 PANIC DROP", 35
    elif price_down and oi_up and bearish_pos:
        sig, bonus = "🔴  STRONG BEAR", 25
    elif price_down and oi_up:
        sig, bonus = "🔴  BEAR BUILD ", 15
    elif price_down and not oi_up and not oi_down:
        sig, bonus = "🟠  PRICE SLIP ", 5
    elif price_up and oi_down:
        sig, bonus = "⚠️  SHORT CVR  ", 5
    elif price_down and oi_down:
        sig, bonus = "⚠️  LONG LIQ   ", 0
    elif price_flat and oi_up and bullish_pos and iv_up:
        sig, bonus = "🟡  CALL ACCUM ", 12
    elif price_flat and oi_up and bearish_pos and iv_up:
        sig, bonus = "🟠  PUT ACCUM  ", 12
    elif price_flat and oi_up:
        sig, bonus = "↔️  STRADDLE   ", 8
    else:
        sig, bonus = "➡️  NEUTRAL    ", 0

    if gamma_sq:
        sig   = "🎯" + sig[1:]
        bonus += 15

    return sig, bonus


# ─────────────────────────────────────────────────────────────────
#  IV INDEX  (30-day constant maturity)
# ─────────────────────────────────────────────────────────────────
def calc_iv_index(api: MassiveAPI, ticker: str, today: datetime) -> float:
    lo_min = (today + timedelta(days=20)).strftime("%Y-%m-%d")
    lo_max = (today + timedelta(days=30)).strftime("%Y-%m-%d")
    hi_min = (today + timedelta(days=30)).strftime("%Y-%m-%d")
    hi_max = (today + timedelta(days=50)).strftime("%Y-%m-%d")

    lo_c = api.get_options(ticker, lo_min, lo_max)
    hi_c = api.get_options(ticker, hi_min, hi_max)

    def atm_iv(contracts):
        best, dist = None, 9.0
        for c in contracts:
            g = c.get("greeks") or {}
            d = g.get("delta"); iv = c.get("implied_volatility")
            if d is None or iv is None: continue
            df, ivf = float(d), float(iv)
            if df <= 0 or not (0.01 < ivf < 5.0): continue
            dd = abs(df - 0.50)
            if dd < dist:
                dist, best = dd, ivf
        return best

    def get_dte(contracts):
        for c in contracts:
            exp = (c.get("details") or {}).get("expiration_date")
            if exp:
                try: return (datetime.strptime(exp, "%Y-%m-%d") - today).days
                except: pass
        return None

    lo_iv = atm_iv(lo_c); hi_iv = atm_iv(hi_c)
    if not lo_iv and not hi_iv: return None
    if not lo_iv: return hi_iv
    if not hi_iv: return lo_iv

    T_lo = get_dte(lo_c) or 25
    T_hi = get_dte(hi_c) or 40
    if T_hi == T_lo: return lo_iv
    iv_30 = lo_iv + (hi_iv - lo_iv) * (30 - T_lo) / (T_hi - T_lo)
    return round(iv_30, 6)


# ─────────────────────────────────────────────────────────────────
#  PER-TICKER SCAN  (Pass 1 — all holdings, parallel)
# ─────────────────────────────────────────────────────────────────
def scan_ticker(ticker: str, api: MassiveAPI, today: datetime,
                short_min: str, short_max: str,
                mid_min: str, mid_max: str) -> dict:
    """
    Two fetches per ticker:
    1. Short-dated (5-30d), NO expiry filter for today → day.volume populated
       → call/put OI split, volume data
    2. Mid-term (45-65d) → ATM IV + Greeks
    """
    # Fetch 1: short-dated for OI, volume (day data)
    short_c = api.get_options(ticker, short_min, short_max)

    # Fetch 2: mid-term for IV, Greeks
    mid_c   = api.get_options(ticker, mid_min, mid_max)

    if not short_c and not mid_c:
        return None

    # Process short-dated chain
    short_d = process_options_chain(short_c) if short_c else {}
    # Process mid-term for ATM IV
    mid_d   = process_options_chain(mid_c)   if mid_c   else {}

    atm_iv    = mid_d.get("atm_iv") or short_d.get("atm_iv")
    atm_strike= mid_d.get("atm_strike") or short_d.get("atm_strike")
    gamma_wall= short_d.get("gamma_wall") or mid_d.get("gamma_wall")

    if not atm_iv:
        return None

    wk = round(atm_iv * math.sqrt(7 / 365) * 100, 2)

    # IVR≈ — near/mid IV ratio
    near_iv = short_d.get("atm_iv") or atm_iv
    mid_iv  = mid_d.get("atm_iv")   or atm_iv
    ivr_a   = round(near_iv / mid_iv, 3) if mid_iv else 1.0
    slope   = round((near_iv - mid_iv) * 100, 2)

    call_oi  = short_d.get("call_oi", 0)
    put_oi   = short_d.get("put_oi",  0)
    total_oi = short_d.get("total_oi", 0)
    call_vol = short_d.get("call_vol", 0)
    put_vol  = short_d.get("put_vol",  0)
    total_vol= short_d.get("total_vol", 0)
    pcr      = short_d.get("pcr")

    return {
        "ticker":      ticker,
        "atm_strike":  atm_strike,
        "iv_pct":      round(atm_iv * 100, 1),
        "weekly_move": wk,
        "ivr_approx":  ivr_a,
        "slope_pp":    slope,
        "call_oi":     call_oi,
        "put_oi":      put_oi,
        "total_oi":    total_oi,
        "call_vol":    call_vol,
        "put_vol":     put_vol,
        "total_vol":   total_vol,
        "pcr":         pcr,
        "gamma_wall":  gamma_wall,
        # filled in pass 2:
        "price":       None,
        "stock_vol":   None,
        "price_chg":   None,
        "oi_yest":     None,
        "oi_chg":      None,
        "iv_yest":     None,
        "iv_chg":      None,
        "cvol_chg":    None,   # filled in pass 2
        "pvol_chg":    None,   # filled in pass 2
        "iv_index":    None,
        "industry":    "",
        "signal":      "—",
        "sig_bonus":   0,
        "score":       0.0,
    }


# ─────────────────────────────────────────────────────────────────
#  SCORING
# ─────────────────────────────────────────────────────────────────
def oi_score(oi: int) -> float:
    if   oi >= 10000: return 40.0
    elif oi >= 5000:  return 32.0
    elif oi >= 2000:  return 24.0
    elif oi >= 1000:  return 18.0
    elif oi >= 500:   return 12.0
    elif oi >= 100:   return  6.0
    elif oi >= 10:    return  2.0
    return 0.0


def compute_score(r: dict) -> float:
    s  = r["weekly_move"] * 3.0
    s += oi_score(r["total_oi"])
    s += max(r["ivr_approx"] - 1.0, 0) * 15
    s += max(r["slope_pp"], 0) * 0.8
    s += r.get("sig_bonus", 0)
    return round(s, 2)


# ─────────────────────────────────────────────────────────────────
#  ETF HOLDINGS LOADER
# ─────────────────────────────────────────────────────────────────
def _parse_holdings_from_nodes(nodes: list) -> list:
    best = []
    for node in nodes:
        if not isinstance(node, dict) or node.get("type") != "data": continue
        data = node.get("data", [])
        if not isinstance(data, list) or len(data) < 5: continue
        for i, val in enumerate(data):
            if not isinstance(val, list) or len(val) < 3: continue
            if not all(isinstance(x, int) for x in val[:5]): continue
            tickers = []
            for ri in val:
                if not isinstance(ri, int) or ri >= len(data): continue
                row = data[ri]
                if not isinstance(row, dict) or "s" not in row: continue
                si = row["s"]
                if not isinstance(si, int) or si >= len(data): continue
                sym = str(data[si]).lstrip("$").strip().upper().replace(".", "-")
                if not sym or len(sym) > 6 or not sym.replace("-","").isalpha(): continue
                wt = 0.0
                for wk in ("as", "w"):
                    wi = row.get(wk)
                    if isinstance(wi, int) and wi < len(data):
                        try: wt = float(str(data[wi]).rstrip("%")); break
                        except: pass
                tickers.append((sym, wt))
            if len(tickers) > len(best):
                best = tickers
    if best:
        best.sort(key=lambda x: x[1], reverse=True)
    return best


def load_etf_holdings(etf_symbol: str) -> list:
    sym = etf_symbol.lower()
    print(f"  🌐  Fetching {etf_symbol} holdings from stockanalysis.com …")
    try:
        sa = _sa_session()
        for url in [
            f"https://stockanalysis.com/etf/{sym}/holdings/__data.json",
            f"https://stockanalysis.com/etf/{sym}/__data.json",
        ]:
            r = sa.get(url, timeout=15)
            if r.status_code == 403: print("  ⚠️  Cookie expired"); break
            if r.status_code == 404: raise SystemExit(f"\n❌  {etf_symbol} not found\n")
            if r.status_code != 200: continue
            tickers = _parse_holdings_from_nodes(r.json().get("nodes", []))
            if tickers:
                result = [t[0] for t in tickers[:MAX_HOLDINGS]]
                print(f"  ✅  {len(result)} holdings  (top 5: {', '.join(result[:5])})")
                return result
    except SystemExit: raise
    except Exception as e: print(f"  ⚠️  {e}")

    # CSV fallback
    files = sorted(glob.glob(str(SCRIPT_DIR / "etf_holdings_map_*.csv")), reverse=True)
    if files:
        rows = []
        with open(files[0], newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                etf = (row.get("ETF") or "").strip().upper()
                s2  = (row.get("Symbol") or row.get("Ticker") or "").strip().upper()
                if etf == etf_symbol.upper() and s2:
                    try:    wt = float(str(row.get("Weight") or "0").rstrip("%"))
                    except: wt = 0.0
                    rows.append((s2, wt))
        if rows:
            rows.sort(key=lambda x: x[1], reverse=True)
            result = [r[0] for r in rows[:MAX_HOLDINGS]]
            print(f"  ✅  {len(result)} holdings from CSV")
            return result

    raise SystemExit(f"\n❌  No holdings found for {etf_symbol}\n")


# ─────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────
def scan(etf_symbol: str):
    try:
        api_key = API_KEY_PATH.read_text().strip()
    except FileNotFoundError:
        raise SystemExit(f"\n❌  Massive key not found: {API_KEY_PATH}\n")

    api   = MassiveAPI(api_key)
    today = datetime.now()
    ts    = today.strftime("%Y-%m-%d")
    yest  = last_trading_day(today)

    short_min = (today + timedelta(days=SHORT_MIN)).strftime("%Y-%m-%d")
    short_max = (today + timedelta(days=SHORT_MAX)).strftime("%Y-%m-%d")
    mid_min   = (today + timedelta(days=MID_MIN)).strftime("%Y-%m-%d")
    mid_max   = (today + timedelta(days=MID_MAX)).strftime("%Y-%m-%d")

    print(f"\n{'═'*72}")
    print(f"  🔍  {etf_symbol.upper()} — SMART MONEY SCANNER")
    print(f"{'─'*72}")
    print(f"  Date       : {ts}  (yesterday: {yest})")
    print(f"  Data       : Massive.com + stockanalysis.com")
    print(f"  Signal     : OI direction + IV + Price = market conviction")
    print(f"{'═'*72}\n")

    holdings = load_etf_holdings(etf_symbol)

    print(f"\n  🔌  Massive … ", end="", flush=True)
    if not api.ping():
        raise SystemExit("❌  Cannot connect.")
    print(f"✅  ({len(holdings)} holdings, {MAX_WORKERS} threads)\n")

    # ─────────────────────────────────────────────────────────────
    #  PASS 1: Scan all holdings in parallel
    # ─────────────────────────────────────────────────────────────
    results  = []
    skipped  = 0
    done     = 0
    lock     = threading.Lock()

    def process(ticker):
        return ticker, scan_ticker(ticker, api, today,
                                   short_min, short_max, mid_min, mid_max)

    print(f"  Scanning {len(holdings)} tickers …")
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(process, t): t for t in holdings}
        for fut in as_completed(futures):
            ticker, res = fut.result()
            with lock:
                done += 1
                if res:
                    results.append(res)
                else:
                    skipped += 1
                if done % 25 == 0 or done == len(holdings):
                    print(f"  [{done:>3}/{len(holdings)}]  "
                          f"found {len(results)}  {time.time()-t0:.0f}s")

    print(f"\n  ✅  Pass 1: {len(results)} results, {skipped} skipped, "
          f"{time.time()-t0:.0f}s\n")

    # Base score (no signal bonus yet)
    for r in results:
        r["score"] = compute_score(r)
    results.sort(key=lambda x: x["score"], reverse=True)
    top_candidates = results[:TOP_N]

    # ─────────────────────────────────────────────────────────────
    #  PASS 2: Top 20 — price, OI change, IV change, signal, industry
    # ─────────────────────────────────────────────────────────────
    print(f"  📊  Pass 2: price + OI direction + signal for top {len(top_candidates)} …")
    sa_sess = _sa_session()

    def enrich(r):
        t = r["ticker"]

        # Stock price + price change + industry (stockanalysis)
        sd      = fetch_stock_data(t, sa_sess)
        price   = sd["price"] or r["atm_strike"]
        p_chg   = sd["price_chg_pct"]
        stk_vol = sd.get("stock_vol")
        ind     = sd["industry"]

        # IV index (30-day constant maturity)
        iv_idx  = calc_iv_index(api, t, today)

        # Yesterday snapshot for OI + volume change
        yest_c    = api.get_options(t, short_min, short_max, as_of=yest)
        yest_d    = process_options_chain(yest_c) if yest_c else {}
        oi_yest   = yest_d.get("total_oi", 0)
        iv_yest   = yest_d.get("atm_iv")
        cvol_yest = yest_d.get("call_vol", 0)
        pvol_yest = yest_d.get("put_vol",  0)

        oi_chg    = r["total_oi"] - oi_yest if oi_yest is not None else None
        iv_chg    = ((r["iv_pct"] / 100) - iv_yest) * 100 if iv_yest else None
        cvol_chg  = r["call_vol"] - cvol_yest if cvol_yest is not None else None
        pvol_chg  = r["put_vol"]  - pvol_yest if pvol_yest is not None else None

        # Signal — uses volume change as primary OI proxy
        sig, bonus = get_signal(
            p_chg, oi_chg, iv_chg,
            r["call_oi"], r["put_oi"],
            r["call_vol"], r["put_vol"],
            cvol_chg, pvol_chg,
            r["atm_strike"], r["gamma_wall"]
        )

        return t, price, p_chg, stk_vol, oi_yest, oi_chg, iv_yest, iv_chg, \
               cvol_chg, pvol_chg, iv_idx, ind, sig, bonus

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(enrich, r): r for r in top_candidates}
        for fut in as_completed(futs):
            (ticker, price, p_chg, stk_vol, oi_yest, oi_chg,
             iv_yest, iv_chg, cvol_chg, pvol_chg, iv_idx, ind, sig, bonus) = fut.result()
            for r in top_candidates:
                if r["ticker"] == ticker:
                    r["price"]     = price
                    r["price_chg"] = p_chg
                    r["stock_vol"] = stk_vol
                    r["oi_yest"]   = oi_yest
                    r["oi_chg"]    = oi_chg
                    r["iv_yest"]   = iv_yest
                    r["iv_chg"]    = iv_chg
                    r["cvol_chg"]  = cvol_chg
                    r["pvol_chg"]  = pvol_chg
                    r["iv_index"]  = round(iv_idx * 100, 1) if iv_idx else None
                    r["industry"]  = ind
                    r["signal"]    = sig
                    r["sig_bonus"] = bonus
                    r["score"]     = compute_score(r)
                    break

    print(f"  ✅  Pass 2 done")

    top_candidates.sort(key=lambda x: x["score"], reverse=True)
    top20 = top_candidates[:TOP_N]

    # ─────────────────────────────────────────────────────────────
    #  TABLE
    # ─────────────────────────────────────────────────────────────
    W = 158
    print(f"\n\n{'═'*W}")
    print(f"  🏆  {etf_symbol.upper()} TOP {TOP_N}  —  SMART MONEY SCANNER  ·  {ts}")
    print(f"{'─'*W}")
    print(f"  {'#':>2}  {'TICKER':<6}  {'PRICE':>8}  {'P%':>5}  "
          f"{'IVΔ':>6}  {'WK±%':>5}  "
          f"{'C-OI':>7}  {'P-OI':>7}  {'PCR':>5}  "
          f"{'OI':>8}  "
          f"{'C-VOL':>7}  {'ΔC-VOL':>7}  {'P-VOL':>7}  {'ΔP-VOL':>7}  "
          f"{'S-VOL':>9}  "
          f"{'IVR≈':>5}  {'SLOPE':>7}  "
          f"{'SIGNAL':<15}  {'INDUSTRY':<24}  SCORE")
    print(f"  {'─'*2}  {'─'*6}  {'─'*8}  {'─'*5}  "
          f"{'─'*6}  {'─'*5}  "
          f"{'─'*7}  {'─'*7}  {'─'*5}  "
          f"{'─'*8}  "
          f"{'─'*7}  {'─'*7}  {'─'*7}  {'─'*7}  "
          f"{'─'*9}  "
          f"{'─'*5}  {'─'*7}  "
          f"{'─'*15}  {'─'*24}  {'─'*5}")

    for i, r in enumerate(top20, 1):
        price_s  = f"${r['price']:>7,.2f}"           if r["price"]      else "       —"
        pchg_s   = f"{r['price_chg']:>+4.1f}%"       if r["price_chg"]  is not None else "    — "
        ivchg_s  = f"{r['iv_chg']:>+5.1f}pp"         if r["iv_chg"]     is not None else "     — "
        pcr_s    = f"{r['pcr']:>5.2f}"               if r["pcr"]        else "    — "
        cvchg_s  = f"{r['cvol_chg']:>+7,}"           if r.get("cvol_chg") is not None else "      — "
        pvchg_s  = f"{r['pvol_chg']:>+7,}"           if r.get("pvol_chg") is not None else "      — "
        svol_s   = f"{r['stock_vol']:>9,}"            if r.get("stock_vol") else "        — "
        arr      = "▲" if r["slope_pp"] >= 2 else ("=" if r["slope_pp"] >= 0 else "▼")
        ind      = (r["industry"] or "—")[:24]
        print(f"  {i:>2}. {r['ticker']:<6}  {price_s}  {pchg_s}  "
              f"{ivchg_s}  {r['weekly_move']:>4.1f}%  "
              f"{r['call_oi']:>7,}  {r['put_oi']:>7,}  {pcr_s}  "
              f"{r['total_oi']:>8,}  "
              f"{r['call_vol']:>7,}  {cvchg_s}  {r['put_vol']:>7,}  {pvchg_s}  "
              f"{svol_s}  "
              f"{r['ivr_approx']:>5.3f}  {arr}{r['slope_pp']:>+5.1f}pp  "
              f"{r['signal']:<15}  {ind:<24}  {r['score']:>5.1f}")

    print(f"\n{'─'*W}")
    print(f"  P%      → Stock price change today")
    print(f"  IVΔ     → IV change vs yesterday  (↑ = volatility expanding = move building)")
    print(f"  WK±%    → Expected 1-sigma move next 7 days  [IV × √(7/365)]")
    print(f"  C-OI    → Call open interest  |  P-OI → Put open interest")
    print(f"  PCR     → Put/Call OI ratio  (<0.7 bullish  >1.3 bearish)")
    print(f"  OI      → Total near-term open interest")
    print(f"  C-VOL   → Call volume today  |  ΔC-VOL → vs yesterday  (↑ = call flow building)")
    print(f"  P-VOL   → Put volume today   |  ΔP-VOL → vs yesterday  (↑ = put flow building)")
    print(f"  S-VOL   → Stock share volume today  (stockanalysis.com)")
    print(f"  IVR≈    → Near/mid IV ratio  (>1.0 = near-term elevated)")
    print(f"  SLOPE   → Near IV minus mid IV  (▲ rising = event risk building)")
    print(f"  SIGNAL  → 🟢🔥 Expl Bull | 🟢 Bull | 🔴🔥 Panic | 🔴 Bear | ⚠️ Cover/Liq | 🎯 Gamma")
    print(f"  SCORE   → Weekly move × OI conviction × signal bonus")
    print(f"{'═'*W}")
    print(f"\n  Holdings: {len(holdings)}  ·  Scanned: {len(results)+skipped}  ·  "
          f"Results: {len(results)}  ·  Total time: {time.time()-t0:.0f}s")

    out = SCRIPT_DIR / f"option_{etf_symbol.lower()}_results.json"
    with open(out, "w") as f:
        json.dump({"etf": etf_symbol, "run_date": ts,
                   "top_20": top20, "all": results}, f, indent=2)
    print(f"  💾  Saved → {out}\n")


def main():
    args = sys.argv[1:]
    if not args:
        print("\n  Usage:  option spy  /  option qqq  /  option xly\n")
        return
    scan(args[0].upper())

if __name__ == "__main__":
    main()
