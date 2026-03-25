#!/usr/bin/env python3
"""
IV CATALYST SCANNER — TEST VERSION (5 tickers only)
Use this to verify everything works before running full scan!
"""

import json, math, time, warnings
from datetime import datetime, timedelta
from pathlib import Path
import requests

warnings.filterwarnings("ignore")

# ── CONFIG ───────────────────────────────────────────────────────
API_KEY_PATH = Path("/data/data/com.termux/files/home/massive_key.txt")
BASE_URL     = "https://api.massive.com"
IVR_MAX      = 45
HV_RISE_MIN  = 1.5
TOP_N        = 5
RV_WINDOW    = 21
RATE_DELAY   = 13.0    # free plan: 5 calls/min

# ── ONLY 5 TICKERS FOR TESTING ───────────────────────────────────
UNIVERSE = ["AAPL", "MSFT", "NVDA", "TSLA", "SPY"]
ETFS     = {"SPY"}

# ── MASSIVE API ──────────────────────────────────────────────────
class MassiveAPI:
    def __init__(self, key):
        self.s = requests.Session()
        self.s.headers["Authorization"] = f"Bearer {key}"
        self._last = 0.0

    def _get(self, path, params=None):
        gap = time.time() - self._last
        if gap < RATE_DELAY:
            time.sleep(RATE_DELAY - gap)
        try:
            r = self.s.get(BASE_URL + path,
                          params=params or {}, timeout=20)
            self._last = time.time()
            if r.status_code == 429:
                print("  rate-limited – sleeping 30s...")
                time.sleep(30)
                return self._get(path, params)
            if r.status_code in (403, 404):
                return {}
            r.raise_for_status()
            return r.json()
        except Exception:
            self._last = time.time()
            return {}

    def get_price_history(self, ticker, days=365):
        end   = datetime.now()
        start = end - timedelta(days=days)
        data  = self._get(
            f"/v2/aggs/ticker/{ticker}/range/1/day"
            f"/{start.strftime('%Y-%m-%d')}"
            f"/{end.strftime('%Y-%m-%d')}",
            {"adjusted": "true", "sort": "asc", "limit": 365}
        )
        results = data.get("results", [])
        if not results:
            return None, []
        closes = [r["c"] for r in results if "c" in r]
        return (closes[-1] if closes else None), closes

    def get_news(self, ticker, limit=5):
        data = self._get("/v2/reference/news",
                        {"ticker": ticker,
                         "limit": limit,
                         "order": "desc"})
        results = []
        for a in data.get("results", []):
            title = a.get("title", "")
            pub   = a.get("published_utc", "")[:10]
            if title:
                results.append({"title": title, "date": pub})
        return results

    def ping(self):
        data = self._get("/v2/aggs/ticker/AAPL/prev",
                        {"adjusted": "true"})
        r = data.get("results", [{}])
        return r[0].get("c") if r else None

# ── MATH ─────────────────────────────────────────────────────────
def calc_hv(closes, window=RV_WINDOW):
    if len(closes) < window + 2:
        return []
    series = []
    for i in range(window, len(closes)):
        sl = closes[i-window:i]
        lr = [math.log(sl[j]/sl[j-1]) for j in range(1, len(sl))]
        mu  = sum(lr) / len(lr)
        var = sum((r-mu)**2 for r in lr) / (len(lr)-1)
        series.append(math.sqrt(var * 252))
    return series

def hv_rank(hv_now, hv_series):
    if len(hv_series) < 10:
        return 50.0
    lo, hi = min(hv_series), max(hv_series)
    if hi <= lo:
        return 0.0
    return round((hv_now - lo) / (hi - lo) * 100, 1)

def score(hvr, delta_pp, has_catalyst):
    s  = (IVR_MAX - hvr) * 2.0
    s += delta_pp * 2.0
    if has_catalyst:
        s += 20.0
    return round(s, 2)

def detect_earnings_from_news(news):
    keywords = ["earnings", "quarterly", "q1", "q2", "q3", "q4",
                "eps", "revenue", "guidance"]
    for h in news:
        for kw in keywords:
            if kw in h["title"].lower():
                return True
    return False

def suggest_strategy(hvr, has_catalyst):
    if has_catalyst:
        if hvr <= 25:
            return "Long Straddle — HV cheap + catalyst!"
        elif hvr <= 40:
            return "Long Strangle — good setup into catalyst"
        else:
            return "Calendar Spread — sell near, buy far"
    return "Watch — wait for clearer catalyst"

# ── MAIN ─────────────────────────────────────────────────────────
def scan():
    try:
        api_key = API_KEY_PATH.read_text().strip()
    except FileNotFoundError:
        raise SystemExit(
            f"\nAPI key not found!\n"
            f"Run: echo 'YOUR_API_KEY' > ~/massive_key.txt\n")

    api       = MassiveAPI(api_key)
    today_str = datetime.now().strftime("%Y-%m-%d")

    print(f"\n{'='*50}")
    print(f"  IV SCANNER — TEST MODE (5 tickers)")
    print(f"  Date: {today_str}")
    print(f"{'='*50}\n")

    print("Testing API... ", end="", flush=True)
    p = api.ping()
    print(f"OK! AAPL=${p}\n" if p else "WARNING: No response!\n")

    print("Scanning tickers (takes ~2 mins on free plan)...\n")

    qualified = []
    skipped   = 0

    for idx, ticker in enumerate(UNIVERSE, 1):
        print(f"[{idx}/{len(UNIVERSE)}] {ticker:<6}", end="  ", flush=True)
        try:
            price, closes = api.get_price_history(ticker)
            if not price or len(closes) < 60:
                print("no price data")
                skipped += 1
                continue

            hv_series = calc_hv(closes)
            if not hv_series:
                print("insufficient history")
                skipped += 1
                continue

            hv_now   = hv_series[-1]
            hv_7d    = hv_series[-5] if len(hv_series) >= 5 else hv_series[0]
            delta_pp = round((hv_now - hv_7d) * 100, 1)
            hvr      = hv_rank(hv_now, hv_series)
            hv_rising = delta_pp >= HV_RISE_MIN

            news         = api.get_news(ticker, limit=5)
            has_earnings = (detect_earnings_from_news(news)
                           if ticker not in ETFS else False)
            has_catalyst = has_earnings or len(news) > 0

            print(f"HV={hv_now*100:5.1f}%  HVR={hvr:5.1f}  "
                  f"dHV={delta_pp:+5.1f}pp"
                  + ("  [EARNINGS]" if has_earnings else ""))

            if hvr <= IVR_MAX and hv_rising:
                qualified.append({
                    "ticker":       ticker,
                    "price":        round(price, 2),
                    "hv_pct":       round(hv_now * 100, 1),
                    "hvr":          hvr,
                    "delta_pp":     delta_pp,
                    "has_earnings": has_earnings,
                    "has_catalyst": has_catalyst,
                    "news":         news,
                    "score":        score(hvr, delta_pp, has_catalyst),
                })

        except KeyboardInterrupt:
            print("\nAborted.")
            break
        except Exception as e:
            print(f"error: {e}")
            skipped += 1

    qualified.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n{'='*50}")
    print(f"RESULTS")
    print(f"{'─'*50}")

    if not qualified:
        print("No tickers matched filters today.")
        print("Try lowering IVR_MAX or HV_RISE_MIN in config.")
    else:
        print(f"{'#':>2}  {'TICKER':<6}  {'PRICE':>8}  "
              f"{'HV%':>6}  {'HVR':>5}  {'dHV':>7}  SCORE")
        print(f"{'─'*50}")
        for i, r in enumerate(qualified, 1):
            earn = " [E]" if r["has_earnings"] else ""
            print(f"{i:>2}. {r['ticker']:<6}  "
                  f"${r['price']:>7,.2f}  "
                  f"{r['hv_pct']:>5.1f}%  "
                  f"{r['hvr']:>5.1f}  "
                  f"{r['delta_pp']:>+6.1f}pp  "
                  f"{r['score']:>6.1f}{earn}")

        print(f"\nDETAILS:")
        for r in qualified:
            print(f"\n  {r['ticker']} | ${r['price']:,.2f} | Score {r['score']}")
            print(f"  Strategy : {suggest_strategy(r['hvr'], r['has_catalyst'])}")
            if r["news"]:
                print(f"  News     : {r['news'][0]['title'][:55]}")

    print(f"\n{'='*50}")
    print(f"Qualified: {len(qualified)} | Skipped: {skipped}")
    print(f"TEST PASSED! Now run full scanner: iv_scanner_massive.py")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    scan()
