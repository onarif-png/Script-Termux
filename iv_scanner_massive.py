#!/usr/bin/env python3
"""
IV CATALYST SCANNER — Massive.com Only Version
Free Plan: Uses Historical Volatility (HV) instead of real IV
Paid Plan: Set USE_REAL_IV = True (when you upgrade)
"""

import json, math, time, warnings
from datetime import datetime, timedelta
from pathlib import Path
import requests

warnings.filterwarnings("ignore")

# ── CONFIG ───────────────────────────────────────────────────────
API_KEY_PATH = Path("/data/data/com.termux/files/home/massive_key.txt")
BASE_URL     = "https://api.massive.com"
USE_REAL_IV  = False   # Set True when you upgrade to paid plan
IVR_MAX      = 45
HV_RISE_MIN  = 1.5
TOP_N        = 20
RV_WINDOW    = 21
RATE_DELAY   = 13.0    # free plan: 5 calls/min

UNIVERSE = [
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","AVGO","AMD",
    "INTC","MU","QCOM","AMAT","MRVL","SMCI","ARM",
    "JPM","BAC","GS","MS","V","MA","COF",
    "JNJ","UNH","LLY","MRNA","ABBV","AMGN","REGN",
    "XOM","CVX","OXY",
    "COST","WMT","HD","NKE","SBUX",
    "BA","CAT","GE","LMT",
    "PLTR","COIN","SNAP","UBER","SHOP","CRWD","PANW","NET","DDOG",
    "SPY","QQQ","IWM","XBI","ARKK",
]
UNIVERSE = list(dict.fromkeys(UNIVERSE))
ETFS = {"SPY","QQQ","IWM","GLD","SLV","XLF","XLE","XBI","ARKK",
        "SQQQ","TQQQ","DIA","VXX","UVXY"}

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
        except Exception as e:
            self._last = time.time()
            return {}

    def get_price_history(self, ticker, days=365):
        """Get daily closing prices for the last N days."""
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
        """Get recent news headlines."""
        data = self._get("/v2/reference/news",
                        {"ticker": ticker,
                         "limit": limit,
                         "order": "desc"})
        results = []
        for a in data.get("results", []):
            title = a.get("title", "")
            pub   = a.get("published_utc", "")[:10]
            url   = a.get("article_url", "")
            if title:
                results.append({
                    "title": title,
                    "date":  pub,
                    "url":   url
                })
        return results

    def ping(self):
        data = self._get("/v2/aggs/ticker/AAPL/prev",
                        {"adjusted": "true"})
        r = data.get("results", [{}])
        return r[0].get("c") if r else None

# ── MATH ─────────────────────────────────────────────────────────
def calc_hv(closes, window=RV_WINDOW):
    """Calculate Historical Volatility series (proxy for IV)."""
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
    """Calculate HV Rank (0-100), proxy for IV Rank."""
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

# ── CATALYST DETECTION ───────────────────────────────────────────
def detect_earnings_from_news(news):
    """Detect upcoming earnings from news headlines."""
    keywords = ["earnings", "quarterly results",
                "q1", "q2", "q3", "q4", "eps",
                "revenue forecast", "guidance"]
    for h in news:
        title = h["title"].lower()
        for kw in keywords:
            if kw in title:
                return True
    return False

def catalyst_type(news, delta_pp, has_earnings):
    catalysts = []
    if has_earnings:
        catalysts.append("EARNINGS NEWS — options may be pricing in move")
    if delta_pp >= 20:
        catalysts.append("MAJOR HV SPIKE — unusual volatility detected")
    elif delta_pp >= 10:
        catalysts.append("HV BUILDING — steady volatility increase")
    elif delta_pp >= 3:
        catalysts.append("HV RISING — early catalyst positioning")
    keywords = {
        "merger":      "M&A RUMOR",
        "acquisition": "M&A RUMOR",
        "takeover":    "M&A RUMOR",
        "fda":         "FDA EVENT",
        "trial":       "CLINICAL TRIAL",
        "lawsuit":     "LEGAL EVENT",
        "sec":         "REGULATORY",
        "guidance":    "GUIDANCE UPDATE",
        "layoff":      "RESTRUCTURING",
        "ceo":         "LEADERSHIP CHANGE",
        "dividend":    "DIVIDEND",
        "buyback":     "BUYBACK",
        "contract":    "CONTRACT NEWS",
        "tariff":      "MACRO/TARIFF",
        "rate":        "MACRO/RATES",
    }
    found = set()
    for h in news:
        for kw, label in keywords.items():
            if kw in h["title"].lower() and label not in found:
                catalysts.append(label)
                found.add(label)
    if not catalysts:
        catalysts.append("NO CLEAR CATALYST — watch carefully")
    return catalysts

def suggest_strategy(hvr, delta_pp, has_catalyst):
    """Suggest options strategy based on HV setup."""
    strategies = []
    if has_catalyst:
        if hvr <= 25:
            strategies.append(
                "Long Straddle — HV very cheap + catalyst = great setup!")
        elif hvr <= 40:
            strategies.append(
                "Long Strangle — good risk/reward into catalyst")
        else:
            strategies.append(
                "Calendar Spread — sell near expiry, buy far")
    elif delta_pp >= 5:
        strategies.append(
            "Long Call/Put — momentum play on rising volatility")
    else:
        strategies.append(
            "Watch — wait for clearer catalyst before entering")
    return strategies

# ── MAIN ─────────────────────────────────────────────────────────
def scan():
    try:
        api_key = API_KEY_PATH.read_text().strip()
    except FileNotFoundError:
        raise SystemExit(
            f"\nAPI key not found!\nPlease create file: {API_KEY_PATH}\n"
            f"Run this in Termux: echo 'YOUR_API_KEY' > ~/massive_key.txt\n")

    api       = MassiveAPI(api_key)
    today_str = datetime.now().strftime("%Y-%m-%d")

    print(f"\nIV CATALYST SCANNER (Massive.com Free Plan)")
    print(f"Date     : {today_str}")
    print(f"Universe : {len(UNIVERSE)} tickers")
    print(f"Mode     : Historical Volatility (HV) proxy for IV")
    print(f"{'─'*55}")

    print("\nTesting Massive API... ", end="", flush=True)
    p = api.ping()
    print(f"OK! (AAPL=${p})\n" if p else "WARNING: No response!\n")

    qualified = []
    skipped   = 0

    for idx, ticker in enumerate(UNIVERSE, 1):
        print(f"[{idx:>2}/{len(UNIVERSE)}] {ticker:<6}", end="  ", flush=True)
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

            hv_now    = hv_series[-1]
            hv_7d     = hv_series[-5] if len(hv_series) >= 5 else hv_series[0]
            delta_pp  = round((hv_now - hv_7d) * 100, 1)
            hvr       = hv_rank(hv_now, hv_series)
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
    top20 = qualified[:TOP_N]

    print(f"\n{'='*60}")
    print(f"TOP {TOP_N} — LOW HVR + RISING HV + CATALYST")
    print(f"{'─'*60}")
    print(f"{'#':>2}  {'TICKER':<6}  {'PRICE':>8}  "
          f"{'HV%':>6}  {'HVR':>5}  {'dHV':>7}  SCORE")
    print(f"{'─'*60}")

    for i, r in enumerate(top20, 1):
        earn = " [E]" if r["has_earnings"] else ""
        print(f"{i:>2}. {r['ticker']:<6}  "
              f"${r['price']:>7,.2f}  "
              f"{r['hv_pct']:>5.1f}%  "
              f"{r['hvr']:>5.1f}  "
              f"{r['delta_pp']:>+6.1f}pp  "
              f"{r['score']:>6.1f}{earn}")

    print(f"\n{'='*60}")
    print("CATALYST BRIEFING")
    print(f"{'='*60}")

    for i, r in enumerate(top20, 1):
        print(f"\n#{i} {r['ticker']} | "
              f"${r['price']:,.2f} | "
              f"HV {r['hv_pct']}% | "
              f"HVR {r['hvr']} | "
              f"Score {r['score']}")
        print(f"{'─'*55}")
        cats = catalyst_type(r["news"], r["delta_pp"], r["has_earnings"])
        print(f"  Why HV rising : {', '.join(cats)}")
        strats = suggest_strategy(r["hvr"], r["delta_pp"], r["has_catalyst"])
        print(f"  Strategy      : {', '.join(strats)}")
        if r["news"]:
            print(f"  Recent news   :")
            for n in r["news"][:3]:
                print(f"    [{n['date']}] {n['title'][:60]}")
        else:
            print(f"  Recent news   : No headlines found")

    print(f"\n{'='*60}")
    print(f"Qualified : {len(qualified)}")
    print(f"Skipped   : {skipped}")
    print(f"Universe  : {len(UNIVERSE)}")
    print(f"NOTE: Using HV as IV proxy (free plan)")
    print(f"      Set USE_REAL_IV=True after upgrading to paid plan!")
    print(f"{'='*60}\n")

    out = Path("/data/data/com.termux/files/home/iv_scan_results.json")
    with open(out, "w") as f:
        json.dump({
            "run_date": today_str,
            "mode":     "HV_proxy_free_plan",
            "top_20":   top20,
            "all":      qualified
        }, f, indent=2)
    print(f"Saved to: {out}\n")

if __name__ == "__main__":
    scan()
