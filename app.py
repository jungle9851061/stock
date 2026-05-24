# -*- coding: utf-8 -*-
from flask import Flask, jsonify, request
from flask_cors import CORS
import yfinance as yf
import requests
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, time as dtime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pytz
import re
import io
import json
import os
import threading
import time as _time
import uuid

try:
    import psutil as _psutil
    _psutil.Process().nice(_psutil.BELOW_NORMAL_PRIORITY_CLASS)
except Exception:
    pass

app = Flask(__name__)
CORS(app)

_DIR = os.path.dirname(os.path.abspath(__file__))
_HOLDINGS_DIR = os.path.join(_DIR, "holdings")

# ── Taiwan OTC stock codes ─────────────────────────────────────────────────────
TWO_CODES_988 = {"5274", "6223", "6274"}
TWO_CODES_990 = {"5274", "5347", "6274"}
TWO_CODES_981 = {
    "5274", "6223", "6274", "5347",  # 已知 TPEX（與 988/990 共用）
    "1815", "3217", "3264", "4966", "5439",
    "6147", "6187", "6510", "8358",
}
TWO_CODES_403 = {
    "5274", "6223", "6274", "5347", "4966", "6147", "8358",  # 與其他 ETF 共用
    "8299", "3529", "8996", "3081", "3211", "4979", "3105",  # 00403A 特有
}

SUFFIX_MAP_988 = {"JP": ".T", "KS": ".KS", "KQ": ".KQ", "HK": ".HK", "GY": ".DE", "FP": ".PA"}
SUFFIX_MAP_990 = {"JP": ".T", "KP": ".KS", "KQ": ".KQ", "HK": ".HK", "GY": ".DE", "GR": ".DE", "FP": ".PA"}
SUFFIX_MAP_403 = {}  # 00403A 全部為台股，無其他交易所

NON_US_SUFFIXES = (".TW", ".TWO", ".T", ".KS", ".KQ", ".DE", ".PA", ".HK", ".SS", ".SZ")

# ── Holdings globals ──────────────────────────────────────────────────────────
STOCKS_988: list = []
PREV_STOCKS_988: list = []
CURRENT_STOCKS_988: list = []

STOCKS_990: list = []
PREV_STOCKS_990: list = []
CURRENT_STOCKS_990: list = []

STOCKS_981: list = []
PREV_STOCKS_981: list = []
CURRENT_STOCKS_981: list = []

STOCKS_403: list = []
PREV_STOCKS_403: list = []
CURRENT_STOCKS_403: list = []

# ── Result caches ─────────────────────────────────────────────────────────────
_stocks_cache_988: list = []
_stocks_cache_990: list = []
_stocks_cache_981: list = []
_stocks_cache_403: list = []
_indices_cache: list = []
_nav_cache: dict = {}       # {"00988A": {...}, "00990A": {...}, "00981A": {...}}

# ── Slow cache (history + metadata, every 10 min) ─────────────────────────────
_hist_cache: dict = {}
_meta_cache: dict = {}
_slow_lock  = threading.Lock()
_slow_ready = threading.Event()

_cache_lock  = threading.Lock()
_cache_ready = threading.Event()

_indices_lock  = threading.Lock()
_indices_ready = threading.Event()


# ─────────────────────────────────────────────────────────────────────────────
# Ticker conversion
# ─────────────────────────────────────────────────────────────────────────────

def convert_ticker_988(raw_code, exchange):
    code = str(raw_code).strip()
    exch = str(exchange).strip().upper() if exchange else ""
    if exch in SUFFIX_MAP_988:
        return code + SUFFIX_MAP_988[exch]
    if exch == "CH":
        return code + (".SS" if code.startswith("6") else ".SZ")
    if exch in ("", "TW"):
        return code + (".TWO" if code in TWO_CODES_988 else ".TW")
    return code


def convert_ticker_990(raw_code, exchange):
    code = str(raw_code).strip()
    exch = str(exchange).strip().upper() if exchange else ""
    if exch in SUFFIX_MAP_990:
        return code + SUFFIX_MAP_990[exch]
    if exch in ("", "TW"):
        return code + (".TWO" if code in TWO_CODES_990 else ".TW")
    return code


def convert_ticker_981(raw_code, exchange):
    code = str(raw_code).strip()
    return code + (".TWO" if code in TWO_CODES_981 else ".TW")


def convert_ticker_403(raw_code, exchange):
    code = str(raw_code).strip()
    return code + (".TWO" if code in TWO_CODES_403 else ".TW")


def parse_weight(raw):
    try:
        s = str(raw).strip()
        if s in ("nan", "None", ""):
            return None
        if "%" in s:
            return float(s.replace("%", ""))
        w = float(s)
        return w * 100 if w < 1.0 else w
    except (ValueError, TypeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# 00988A: ezmoney XLSX
# ─────────────────────────────────────────────────────────────────────────────

def parse_xlsx_bytes(content: bytes, converter=None) -> list:
    df = pd.read_excel(io.BytesIO(content), sheet_name=0, header=None)
    header_row = None
    for i, row in df.iterrows():
        if "股票代號" in [str(v).strip() for v in row.values]:
            header_row = i
            break
    if header_row is None:
        raise ValueError("找不到「股票代號」標題列")
    df.columns = df.iloc[header_row].astype(str).str.strip()
    df = df.iloc[header_row + 1:].reset_index(drop=True)
    _conv = converter if converter else convert_ticker_988
    stocks = []
    for _, row in df.iterrows():
        raw = str(row.get("股票代號", "")).strip()
        if not raw or raw in ("nan", "None", ""):
            continue
        parts = raw.split()
        code, exch = parts[0], (parts[1] if len(parts) > 1 else "")
        name = str(row.get("股票名稱", code)).strip()
        w = parse_weight(row.get("持股權重", ""))
        if w is None:
            continue
        stocks.append({"id": _conv(code, exch), "name": name, "weight": f"{w:.2f}%"})
    stocks.sort(key=lambda x: float(x["weight"].replace("%", "")), reverse=True)
    return stocks


def fetch_etf_holdings_988() -> list:
    url = "https://www.ezmoney.com.tw/ETF/Fund/AssetExcelNPOI?fundCode=61YTW"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=61YTW",
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return parse_xlsx_bytes(resp.content)


def fetch_etf_holdings_981() -> list:
    url = "https://www.ezmoney.com.tw/ETF/Fund/AssetExcelNPOI?fundCode=49YTW"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=49YTW",
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return parse_xlsx_bytes(resp.content, converter=convert_ticker_981)


def fetch_etf_holdings_403() -> list:
    url = "https://www.ezmoney.com.tw/ETF/Fund/AssetExcelNPOI?fundCode=63YTW"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Referer": "https://www.ezmoney.com.tw/ETF/Fund/Info?fundCode=63YTW",
    }
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    return parse_xlsx_bytes(resp.content, converter=convert_ticker_403)


# ─────────────────────────────────────────────────────────────────────────────
# 00990A: Yuanta SSR
# ─────────────────────────────────────────────────────────────────────────────

def _decode_js_escapes(s: str) -> str:
    return re.sub(r'\\u([0-9a-fA-F]{4})', lambda m: chr(int(m.group(1), 16)), s)


def parse_yuanta_holdings(html_text: str) -> list:
    start_tag = 'StockWeights:['
    start = html_text.find(start_tag)
    if start == -1:
        raise ValueError("StockWeights not found in page")
    start += len(start_tag)
    depth, i = 1, start
    while i < len(html_text) and depth > 0:
        if html_text[i] == '[':
            depth += 1
        elif html_text[i] == ']':
            depth -= 1
        i += 1
    raw = html_text[start:i - 1]
    stocks = []
    for block in re.findall(r'\{[^{}]+\}', raw):
        m_code    = re.search(r'code:"([^"]+)"', block)
        m_ename   = re.search(r'ename:"([^"]*)"', block)
        m_name    = re.search(r'(?:^|,)name:"([^"]*)"', block)
        m_weights = re.search(r'weights:([\d.]+)', block)
        if not m_code or not m_weights:
            continue
        code_raw = m_code.group(1).strip()
        ename    = _decode_js_escapes(m_ename.group(1).strip()) if m_ename else ''
        name_cn  = _decode_js_escapes(m_name.group(1).strip()) if m_name else ''
        weight   = float(m_weights.group(1))
        parts    = code_raw.split()
        ticker   = parts[0]
        exchange = parts[1] if len(parts) > 1 else ''
        display  = (name_cn or ename or ticker) if not exchange else (ename or name_cn or ticker)
        yf_id    = convert_ticker_990(ticker, exchange)
        stocks.append({"id": yf_id, "name": display, "weight": f"{weight:.2f}%"})
    return sorted(stocks, key=lambda x: float(x["weight"].replace('%', '')), reverse=True)


def fetch_etf_holdings_990() -> list:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.yuantaetfs.com/",
    }
    resp = requests.get(
        "https://www.yuantaetfs.com/product/detail/00990A/ratio",
        headers=headers, timeout=25
    )
    resp.raise_for_status()
    return parse_yuanta_holdings(resp.text)


# ─────────────────────────────────────────────────────────────────────────────
# Market helpers
# ─────────────────────────────────────────────────────────────────────────────

def check_market_status(ticker: str) -> bool:
    now_utc = datetime.now(pytz.utc)
    try:
        if ".TW" in ticker or ".TWO" in ticker:
            tz, open_t, close_t = pytz.timezone("Asia/Taipei"),    dtime(9, 0),  dtime(13, 35)
        elif ".KS" in ticker or ".KQ" in ticker:
            tz, open_t, close_t = pytz.timezone("Asia/Seoul"),     dtime(9, 0),  dtime(15, 30)
        elif ".T" in ticker:
            tz, open_t, close_t = pytz.timezone("Asia/Tokyo"),     dtime(9, 0),  dtime(15, 30)
        elif ".HK" in ticker:
            tz, open_t, close_t = pytz.timezone("Asia/Hong_Kong"), dtime(9, 30), dtime(16, 0)
        elif ".DE" in ticker or ".PA" in ticker:
            tz, open_t, close_t = pytz.timezone("Europe/Berlin"),  dtime(9, 0),  dtime(17, 30)
        elif ".SS" in ticker or ".SZ" in ticker:
            tz, open_t, close_t = pytz.timezone("Asia/Shanghai"),  dtime(9, 30), dtime(15, 0)
        else:
            tz, open_t, close_t = pytz.timezone("America/New_York"), dtime(9, 30), dtime(16, 0)
        m_now = now_utc.astimezone(tz)
        if m_now.weekday() >= 5:
            return False
        return open_t <= m_now.time() <= close_t
    except Exception:
        return False


def _get_us_market_state() -> str:
    now_et = datetime.now(pytz.timezone("America/New_York"))
    if now_et.weekday() >= 5:
        return "CLOSED"
    t = now_et.time()
    if dtime(4, 0) <= t < dtime(9, 30):    return "PRE"
    if dtime(9, 30) <= t <= dtime(16, 0):  return "REGULAR"
    if dtime(16, 0) < t <= dtime(20, 0):   return "POST"
    return "CLOSED"


def get_pct_change(series, periods):
    if series is None or len(series) < periods + 1:
        return 0.0
    try:
        curr = series.iloc[-1]
        prev = series.iloc[-(periods + 1)]
        return float(((curr - prev) / prev) * 100) if prev != 0 else 0.0
    except Exception:
        return 0.0


def _get_ticker_meta(stocks: list) -> dict:
    market_config = {
        ".TW":  {"flag": "🇹🇼", "region": "TW"},
        ".TWO": {"flag": "🇹🇼", "region": "TW"},
        ".T":   {"flag": "🇯🇵", "region": "JP"},
        ".KS":  {"flag": "🇰🇷", "region": "KR"},
        ".KQ":  {"flag": "🇰🇷", "region": "KR"},
        ".DE":  {"flag": "🇩🇪", "region": "DE"},
        ".PA":  {"flag": "🇫🇷", "region": "FR"},
        ".HK":  {"flag": "🇭🇰", "region": "HK"},
        ".SS":  {"flag": "🇨🇳", "region": "CN"},
        ".SZ":  {"flag": "🇨🇳", "region": "CN"},
    }
    result = {}
    for s in stocks:
        tk = s["id"]
        flag, region = "🇺🇸", "US"
        for suffix, conf in market_config.items():
            if tk.endswith(suffix):
                flag, region = conf["flag"], conf["region"]
                break
        result[tk] = {"flag": flag, "region": region}
    return result


def _weight_change(tk: str, current_weight_str: str, prev_stocks: list):
    if not prev_stocks:
        return None
    prev_map = {s["id"]: float(s["weight"].replace("%", "")) for s in prev_stocks}
    if tk not in prev_map:
        return "new"
    diff = round(float(current_weight_str.replace("%", "")) - prev_map[tk], 2)
    return diff if abs(diff) >= 0.01 else None


def get_all_tickers() -> list:
    seen, result = set(), []
    for s in STOCKS_988 + STOCKS_990 + STOCKS_981 + STOCKS_403:
        if s["id"] not in seen:
            seen.add(s["id"])
            result.append(s["id"])
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Data fetching
# ─────────────────────────────────────────────────────────────────────────────

def _batch_download_history(tickers: list) -> dict:
    now_utc = datetime.now(pytz.utc)
    start   = (now_utc - timedelta(days=35)).strftime("%Y-%m-%d")
    end     = (now_utc + timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        raw = yf.download(
            tickers, start=start, end=end,
            auto_adjust=True, progress=False,
            group_by="ticker", threads=True,
        )
    except Exception:
        return {tk: pd.Series([], dtype=float) for tk in tickers}
    result = {}
    for tk in tickers:
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                series = raw[tk]["Close"].dropna()
            else:
                series = raw["Close"].dropna()
            result[tk] = series if len(series) > 0 else pd.Series([], dtype=float)
        except (KeyError, TypeError):
            result[tk] = pd.Series([], dtype=float)
    return result


def _fetch_meta(tk: str) -> tuple:
    info = {"rmt": None, "tz_name": None}
    try:
        meta = yf.Ticker(tk).history_metadata
        info["rmt"]     = meta.get("regularMarketTime")
        info["tz_name"] = meta.get("exchangeTimezoneName")
    except Exception:
        pass
    return tk, info


def _fetch_all_metadata(non_us_tks: list) -> dict:
    if not non_us_tks:
        return {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        return dict(ex.map(_fetch_meta, non_us_tks))


_V8_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


async def _fetch_spark_batch_async(session: aiohttp.ClientSession, chunk: list) -> dict:
    """Spark 批次端點：一次最多 20 個 ticker，range=5d 取得 prev_close"""
    try:
        async with session.get(
            "https://query1.finance.yahoo.com/v8/finance/spark",
            params={"symbols": ",".join(chunk), "range": "5d", "interval": "1d"},
        ) as r:
            if r.status != 200:
                return {}
            data = await r.json(content_type=None)
    except Exception:
        return {}
    if not isinstance(data, dict) or "spark" in data:
        return {}
    ms_us = _get_us_market_state()
    out = {}
    for tk, info in data.items():
        if not isinstance(info, dict):
            continue
        closes = [c for c in (info.get("close") or []) if c is not None]
        if not closes:
            continue
        price      = float(closes[-1])
        prev_close = float(closes[-2]) if len(closes) >= 2 else None
        is_us = not any(tk.endswith(s) for s in NON_US_SUFFIXES)
        ms    = ms_us if is_us else ("REGULAR" if check_market_status(tk) else "CLOSED")
        out[tk] = {
            "price":        price,
            "prev_close":   prev_close,
            "pre_price":    None,
            "post_price":   None,
            "market_state": ms,
        }
    return out


async def _batch_quote_async(tickers: list) -> dict:
    """Spark 批次取得報價：每批 20 個，101 個 ticker 約 6 個並行請求"""
    chunks    = [tickers[i:i+20] for i in range(0, len(tickers), 20)]
    connector = aiohttp.TCPConnector(limit=6)
    timeout   = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(headers=_V8_HEADERS, connector=connector, timeout=timeout) as session:
        tasks   = [_fetch_spark_batch_async(session, chunk) for chunk in chunks]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    out = {}
    for item in results:
        if isinstance(item, dict):
            out.update(item)
    return out


def _batch_quote(tickers: list) -> dict:
    if not tickers:
        return {}
    return asyncio.run(_batch_quote_async(tickers))


# ─────────────────────────────────────────────────────────────────────────────
# Results assembly
# ─────────────────────────────────────────────────────────────────────────────

def _assemble_results(stocks: list, prev_stocks: list,
                      hist_map: dict, quote_map: dict,
                      meta_map: dict, ticker_meta: dict) -> list:
    results = []
    for s in stocks:
        tk     = s["id"]
        meta   = ticker_meta.get(tk, {"flag": "🇺🇸", "region": "US"})
        flag   = meta["flag"]
        region = meta["region"]
        cp     = hist_map.get(tk, pd.Series([], dtype=float))
        q      = quote_map.get(tk, {})
        cp_last = float(cp.iloc[-1]) if len(cp) > 0 else 0.0

        try:
            # Session state from market_state
            ms = (q.get("market_state") or "CLOSED").upper()
            if ms == "REGULAR":
                session, is_open = "regular", True
            elif ms in ("PRE", "PREPRE"):
                session, is_open = "pre", False
            elif ms in ("POST", "POSTPOST"):
                session, is_open = "post", False
            else:
                session, is_open = "closed", False

            # Non-US: validate against metadata (public holiday guard)
            if region != "US" and is_open:
                tmeta = meta_map.get(tk, {})
                rmt, tz_name = tmeta.get("rmt"), tmeta.get("tz_name")
                if rmt and tz_name:
                    mkt_tz = pytz.timezone(tz_name)
                    if datetime.fromtimestamp(rmt, tz=mkt_tz).date() < datetime.now(mkt_tz).date():
                        is_open, session = False, "closed"

            reg_price  = q.get("price")
            prev_close = q.get("prev_close")
            pre_price  = q.get("pre_price")
            post_price = q.get("post_price")

            # Latest price
            # v8 regularMarketPrice 對所有市場都回傳本地貨幣正確價格，開收盤皆適用
            latest_price = reg_price or cp_last

            # Day change
            if reg_price and prev_close and prev_close > 0:
                day_change = (reg_price - prev_close) / prev_close * 100
            else:
                day_change = get_pct_change(cp, 1)

            # Extended hours (US only)
            ext_price = ext_change = None
            if region == "US" and reg_price and reg_price > 0:
                if session == "pre" and pre_price:
                    diff = pre_price - reg_price
                    if abs(diff) / reg_price > 0.0001:
                        ext_price, ext_change = pre_price, diff / reg_price * 100
                elif session in ("post", "closed") and post_price:
                    diff = post_price - reg_price
                    if abs(diff) / reg_price > 0.0001:
                        ext_price, ext_change = post_price, diff / reg_price * 100

            results.append({
                "id": tk, "name": s["name"], "weight": s["weight"],
                "weight_change": _weight_change(tk, s["weight"], prev_stocks),
                "price": latest_price or 0, "flag": flag, "region": region,
                "is_open": is_open, "session": session,
                "day_change": day_change,
                "ext_price":  ext_price,
                "ext_change": ext_change,
                "change_3d":  get_pct_change(cp, 3),
                "change_1w":  get_pct_change(cp, 5),
                "change_1m":  get_pct_change(cp, 20),
            })
        except Exception:
            results.append({
                "id": tk, "name": s["name"], "weight": s["weight"],
                "weight_change": _weight_change(tk, s["weight"], prev_stocks),
                "price": 0, "flag": flag, "region": region,
                "is_open": False, "session": "closed",
                "day_change": 0, "ext_price": None, "ext_change": None,
                "change_3d": 0, "change_1w": 0, "change_1m": 0,
            })
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Background refresh
# ─────────────────────────────────────────────────────────────────────────────

def _do_slow_refresh():
    global _hist_cache, _meta_cache
    try:
        all_tks    = get_all_tickers()
        all_stocks = STOCKS_988 + STOCKS_990 + STOCKS_981
        all_meta   = _get_ticker_meta(all_stocks)
        non_us_tks = [tk for tk in all_tks if all_meta.get(tk, {}).get("region") != "US"]
        with ThreadPoolExecutor(max_workers=2) as ex:
            fut_hist = ex.submit(_batch_download_history, all_tks)
            fut_meta = ex.submit(_fetch_all_metadata, non_us_tks)
            new_hist, new_meta = fut_hist.result(), fut_meta.result()
        with _slow_lock:
            _hist_cache = new_hist
            _meta_cache = new_meta
        _slow_ready.set()
        print(f"[SLOW] 歷史+metadata 刷新完成，{len(all_tks)} 支")
    except Exception as e:
        print(f"[SLOW] 失敗: {e}")
        _slow_ready.set()


def _slow_refresh_loop():
    while True:
        _do_slow_refresh()
        _time.sleep(600)


def _fast_refresh_loop():
    global _stocks_cache_988, _stocks_cache_990, _stocks_cache_981, _stocks_cache_403
    _slow_ready.wait(timeout=120)
    while True:
        try:
            t0              = _time.monotonic()
            all_tks         = get_all_tickers()
            all_ticker_meta = _get_ticker_meta(STOCKS_988 + STOCKS_990 + STOCKS_981 + STOCKS_403)
            meta_988 = meta_990 = meta_981 = meta_403 = all_ticker_meta

            quote_map = _batch_quote(all_tks)

            with _slow_lock:
                hist_map = dict(_hist_cache)
                meta_map = dict(_meta_cache)

            data_988 = _assemble_results(STOCKS_988, PREV_STOCKS_988,
                                         hist_map, quote_map, meta_map, meta_988)
            data_990 = _assemble_results(STOCKS_990, PREV_STOCKS_990,
                                         hist_map, quote_map, meta_map, meta_990)
            data_981 = _assemble_results(STOCKS_981, PREV_STOCKS_981,
                                         hist_map, quote_map, meta_map, meta_981)
            data_403 = _assemble_results(STOCKS_403, PREV_STOCKS_403,
                                         hist_map, quote_map, meta_map, meta_403)

            with _cache_lock:
                if any(r["price"] > 0 for r in data_988):
                    _stocks_cache_988 = data_988
                if any(r["price"] > 0 for r in data_990):
                    _stocks_cache_990 = data_990
                if any(r["price"] > 0 for r in data_981):
                    _stocks_cache_981 = data_981
                if any(r["price"] > 0 for r in data_403):
                    _stocks_cache_403 = data_403
            _cache_ready.set()
            print(f"[CACHE] 988A:{len(data_988)} 990A:{len(data_990)} 981A:{len(data_981)} 403A:{len(data_403)} 耗時 {_time.monotonic()-t0:.1f}s")
        except Exception as e:
            print(f"[CACHE] refresh 失敗: {e}")
        _time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# Holdings persistence
# ─────────────────────────────────────────────────────────────────────────────

def _load_holdings_file(path: str) -> list:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        print(f"[WARN] 無法讀取 {os.path.basename(path)}: {e}")
    return []


def _save_holdings_file(path: str, data: list):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[WARN] 無法儲存 {os.path.basename(path)}: {e}")


def _holdings_equal(a: list, b: list) -> bool:
    if len(a) != len(b):
        return False
    return {s["id"]: s["weight"] for s in a} == {s["id"]: s["weight"] for s in b}


_ETF_HOLDINGS_CFG = {
    "00988A": ("STOCKS_988", "PREV_STOCKS_988", "CURRENT_STOCKS_988",
               "prev_holdings.json", "current_holdings.json"),
    "00990A": ("STOCKS_990", "PREV_STOCKS_990", "CURRENT_STOCKS_990",
               "prev_holdings_990.json", "current_holdings_990.json"),
    "00981A": ("STOCKS_981", "PREV_STOCKS_981", "CURRENT_STOCKS_981",
               "prev_holdings_981.json", "current_holdings_981.json"),
    "00403A": ("STOCKS_403", "PREV_STOCKS_403", "CURRENT_STOCKS_403",
               "prev_holdings_403.json", "current_holdings_403.json"),
}


def _apply_holdings_update(etf_label: str, new_holdings: list):
    if not new_holdings:
        return
    g = globals()
    stocks_var, prev_var, current_var, prev_file, curr_file = _ETF_HOLDINGS_CFG[etf_label]
    current = g[current_var]
    if not _holdings_equal(new_holdings, current):
        g[prev_var]    = list(current) if current else list(new_holdings)
        g[stocks_var]  = new_holdings
        g[current_var] = new_holdings
        _save_holdings_file(os.path.join(_HOLDINGS_DIR, prev_file), g[prev_var])
        _save_holdings_file(os.path.join(_HOLDINGS_DIR, curr_file), g[stocks_var])
        print(f"[INFO] {etf_label} 持股更新 {len(g[prev_var])}→{len(g[stocks_var])}")
        _slow_ready.clear()
        threading.Thread(target=_do_slow_refresh, daemon=True).start()
    else:
        g[stocks_var] = new_holdings
        if not g[prev_var] and current:
            g[prev_var] = list(current)
            _save_holdings_file(os.path.join(_HOLDINGS_DIR, prev_file), g[prev_var])
        print(f"[INFO] {etf_label} 持股未變化（{len(g[stocks_var])} 檔）")


def _apply_holdings_update_988(new_holdings: list):
    _apply_holdings_update("00988A", new_holdings)

def _apply_holdings_update_990(new_holdings: list):
    _apply_holdings_update("00990A", new_holdings)

def _apply_holdings_update_981(new_holdings: list):
    _apply_holdings_update("00981A", new_holdings)

def _apply_holdings_update_403(new_holdings: list):
    _apply_holdings_update("00403A", new_holdings)


# ─────────────────────────────────────────────────────────────────────────────
# Startup: load holdings
# ─────────────────────────────────────────────────────────────────────────────

PREV_STOCKS_988    = _load_holdings_file(os.path.join(_HOLDINGS_DIR, "prev_holdings.json"))
CURRENT_STOCKS_988 = _load_holdings_file(os.path.join(_HOLDINGS_DIR, "current_holdings.json"))
print(f"[OK] 00988A prev={len(PREV_STOCKS_988)} current={len(CURRENT_STOCKS_988)}")

try:
    _new988 = fetch_etf_holdings_988()
    _apply_holdings_update_988(_new988)
except Exception as e:
    print(f"[ERROR] 00988A 啟動載入失敗: {e}")
    STOCKS_988 = CURRENT_STOCKS_988 or PREV_STOCKS_988
    print(f"[INFO] 00988A fallback {len(STOCKS_988)} 檔")

PREV_STOCKS_990    = _load_holdings_file(os.path.join(_HOLDINGS_DIR, "prev_holdings_990.json"))
CURRENT_STOCKS_990 = _load_holdings_file(os.path.join(_HOLDINGS_DIR, "current_holdings_990.json"))
print(f"[OK] 00990A prev={len(PREV_STOCKS_990)} current={len(CURRENT_STOCKS_990)}")

try:
    _new990 = fetch_etf_holdings_990()
    _apply_holdings_update_990(_new990)
except Exception as e:
    print(f"[ERROR] 00990A 啟動載入失敗: {e}")
    STOCKS_990 = CURRENT_STOCKS_990 or PREV_STOCKS_990
    print(f"[INFO] 00990A fallback {len(STOCKS_990)} 檔")

PREV_STOCKS_981    = _load_holdings_file(os.path.join(_HOLDINGS_DIR, "prev_holdings_981.json"))
CURRENT_STOCKS_981 = _load_holdings_file(os.path.join(_HOLDINGS_DIR, "current_holdings_981.json"))
print(f"[OK] 00981A prev={len(PREV_STOCKS_981)} current={len(CURRENT_STOCKS_981)}")

try:
    _new981 = fetch_etf_holdings_981()
    _apply_holdings_update_981(_new981)
except Exception as e:
    print(f"[ERROR] 00981A 啟動載入失敗: {e}")
    STOCKS_981 = CURRENT_STOCKS_981 or PREV_STOCKS_981
    print(f"[INFO] 00981A fallback {len(STOCKS_981)} 檔")

PREV_STOCKS_403    = _load_holdings_file(os.path.join(_HOLDINGS_DIR, "prev_holdings_403.json"))
CURRENT_STOCKS_403 = _load_holdings_file(os.path.join(_HOLDINGS_DIR, "current_holdings_403.json"))
print(f"[OK] 00403A prev={len(PREV_STOCKS_403)} current={len(CURRENT_STOCKS_403)}")

try:
    _new403 = fetch_etf_holdings_403()
    _apply_holdings_update_403(_new403)
except Exception as e:
    print(f"[ERROR] 00403A 啟動載入失敗: {e}")
    STOCKS_403 = CURRENT_STOCKS_403 or PREV_STOCKS_403
    print(f"[INFO] 00403A fallback {len(STOCKS_403)} 檔")

# ─────────────────────────────────────────────────────────────────────────────
# Start background threads
# ─────────────────────────────────────────────────────────────────────────────

threading.Thread(target=_slow_refresh_loop, daemon=True).start()
threading.Thread(target=_fast_refresh_loop, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# Indices helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_tw_future_scraper():
    try:
        r = requests.get("https://tw.stock.yahoo.com/future/WTX&",
                         headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")
        price_tag = soup.find("span", class_=re.compile(r"Fz\(32px\)"))
        if not price_tag:
            return {"name": "台指期夜盤", "price": 0, "change_pts": 0, "change_pct": 0}
        price = float(price_tag.text.replace(",", ""))
        trend_tags = soup.find_all("span", class_=re.compile(r"Fz\(20px\)"))
        pts, pct = 0.0, 0.0
        if len(trend_tags) >= 2:
            style   = str(trend_tags[0].get("class", []))
            is_down = "down" in style or "trend-down" in style
            pts = float(trend_tags[0].text.strip().replace(",", "").replace("+", "").replace("-", ""))
            pct = float(trend_tags[1].text.strip().replace("(", "").replace(")", "").replace("%", "").replace("+", "").replace("-", ""))
            if is_down:
                pts, pct = -pts, -pct
        return {"name": "台指期夜盤", "price": price, "change_pts": pts, "change_pct": pct}
    except Exception:
        return {"name": "台指期夜盤", "price": 0, "change_pts": 0, "change_pct": 0}



_INDICES_TARGETS = [
    ("^DJI",    "道瓊工業"),
    ("^SOX",    "費半"),
    ("^N225",   "日經225"),
    ("^KS11",   "韓國KOSPI"),
    ("BTC-USD", "BTC 比特幣"),
    ("BZ=F",    "布蘭特原油"),
    ("TSM",     "TSM ADR"),
]


async def _fetch_v8_quote_async(session: aiohttp.ClientSession, sid: str) -> tuple:
    try:
        async with session.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{sid}",
            params={"interval": "1d", "range": "5d"},
        ) as r:
            data = await r.json(content_type=None)
        res = data.get("chart", {}).get("result", [])
        if not res:
            return sid, None
        meta   = res[0].get("meta", {})
        curr   = meta.get("regularMarketPrice")
        closes = [float(c) for c in res[0].get("indicators", {}).get("quote", [{}])[0].get("close", []) if c is not None]
        if not curr or len(closes) < 2:
            return sid, None
        curr       = float(curr)
        last_close = closes[-1]
        prev = closes[-2] if abs(curr - last_close) / last_close < 0.001 else last_close
        return sid, {"curr": curr, "prev": prev}
    except Exception:
        return sid, None


async def _fetch_indices_async() -> list:
    connector = aiohttp.TCPConnector(limit=10)
    timeout   = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(headers=_V8_HEADERS, connector=connector, timeout=timeout) as session:
        tasks   = [_fetch_v8_quote_async(session, sid) for sid, _ in _INDICES_TARGETS]
        raw     = await asyncio.gather(*tasks, return_exceptions=True)
    quote_map = {sid: info for sid, info in raw if isinstance(info, dict)}
    out = []
    for sid, name in _INDICES_TARGETS:
        q = quote_map.get(sid)
        if q and q["curr"] and q["prev"]:
            pts = q["curr"] - q["prev"]
            pct = pts / q["prev"] * 100
            out.append({"name": name, "price": q["curr"], "change_pts": pts, "change_pct": pct})
        else:
            out.append({"name": name, "price": 0, "change_pts": 0, "change_pct": 0})
    out.append(get_tw_future_scraper())
    return out


def _indices_refresh_loop():
    global _indices_cache
    while True:
        try:
            results = asyncio.run(_fetch_indices_async())
            if sum(1 for r in results if r["price"] > 0 and r["name"] != "台指期夜盤") >= 1:
                with _indices_lock:
                    _indices_cache = results
                _indices_ready.set()
                print(f"[INDICES] 刷新完成")
        except Exception as e:
            print(f"[INDICES] 失敗: {e}")
        _time.sleep(30)

threading.Thread(target=_indices_refresh_loop, daemon=True).start()


# ─────────────────────────────────────────────────────────────────────────────
# API routes
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/stocks")
def get_stocks():
    etf = request.args.get("etf", "00988A")
    _cache_ready.wait(timeout=120)
    with _cache_lock:
        if etf == "00990A":
            return jsonify(_stocks_cache_990)
        elif etf == "00981A":
            return jsonify(_stocks_cache_981)
        elif etf == "00403A":
            return jsonify(_stocks_cache_403)
        else:
            return jsonify(_stocks_cache_988)


@app.route("/api/reload", methods=["POST"])
def reload_stocks():
    etf = request.args.get("etf", "00988A")
    try:
        if etf == "00990A":
            _apply_holdings_update_990(fetch_etf_holdings_990())
            return jsonify({"status": "ok", "etf": etf, "count": len(STOCKS_990)})
        elif etf == "00981A":
            _apply_holdings_update_981(fetch_etf_holdings_981())
            return jsonify({"status": "ok", "etf": etf, "count": len(STOCKS_981)})
        elif etf == "00403A":
            _apply_holdings_update_403(fetch_etf_holdings_403())
            return jsonify({"status": "ok", "etf": etf, "count": len(STOCKS_403)})
        else:
            _apply_holdings_update_988(fetch_etf_holdings_988())
            return jsonify({"status": "ok", "etf": etf, "count": len(STOCKS_988)})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/indices")
def get_indices():
    _indices_ready.wait(timeout=35)
    with _indices_lock:
        return jsonify(list(_indices_cache) if _indices_cache else [])


_YUANTA_DEVICE_ID = str(uuid.uuid4())
_YUANTA_API_URL   = "https://etfapi.yuantaetfs.com/ectranslation/api/trans"
_YUANTA_SITE      = "https://www.yuantaetfs.com"
_YUANTA_UA        = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"


def _yuanta_get(func_id: str, extra: dict) -> dict | None:
    """呼叫 Yuanta ETFBackstage API，回傳 Data 欄位；失敗回傳 None。"""
    params = {
        "APIType":     "ETFBackstage",
        "CompanyName": "YUANTAFUNDS",
        "PageName":    "/tradeInfo/comparison/00990A/realtime",
        "DeviceId":    _YUANTA_DEVICE_ID,
        "FuncId":      func_id,
        "AppName":     "ETF",
        "Device":      "4",
        "Platform":    "ETF",
        **extra,
    }
    headers = {
        "User-Agent": _YUANTA_UA,
        "Accept":     "application/json, text/plain, */*",
        "Origin":     _YUANTA_SITE,
        "Referer":    f"{_YUANTA_SITE}/tradeInfo/comparison/00990A/realtime",
    }
    r = requests.get(_YUANTA_API_URL, params=params, headers=headers, timeout=12)
    d = r.json()
    return d.get("Data") if d.get("ResultCode") == 0 else None


def fetch_etf_nav_990_yuanta() -> dict | None:
    """從 Yuanta API 取得 00990A 預估淨值、市價、折溢價。"""
    try:
        # 取得當日 intraday NAV/Price 時間序列
        today_data = _yuanta_get("ETFNAV/GetTodayNav", {"stk_cd": "00990A"})
        if not today_data:
            return None
        detail_list = today_data.get("Comparison_DetailList") or []
        if not detail_list:
            return None

        latest    = detail_list[-1]
        est_nav   = latest.get("NOW_NAV")
        mkt_price = latest.get("NOW_PRICE")
        update_t  = latest.get("UPDATE_T", "")

        # 取得近期每日比較資料（取前一交易日的收盤值）
        now_tw   = datetime.now(pytz.timezone("Asia/Taipei"))
        end_d    = now_tw.strftime("%Y%m%d")
        start_d  = (now_tw - timedelta(days=10)).strftime("%Y%m%d")
        comp_data = _yuanta_get("ETFNAV/GetComparison",
                                {"stk_cd": "00990A", "SDATE": start_d, "EDATE": end_d})

        prev_nav = prev_price = None
        if isinstance(comp_data, list) and len(comp_data) >= 2:
            prev_entry = comp_data[1]
            prev_nav   = prev_entry.get("NOW_NAV")
            prev_price = prev_entry.get("NOW_PRICE")
        elif isinstance(comp_data, list) and len(comp_data) == 1:
            prev_nav   = comp_data[0].get("NOW_NAV")
            prev_price = comp_data[0].get("NOW_PRICE")

        nav_chg_pct = mkt_chg_pct = premium = premium_pct = None
        if est_nav and prev_nav and prev_nav > 0:
            nav_chg_pct = round((est_nav - prev_nav) / prev_nav * 100, 2)
        if mkt_price and prev_price and prev_price > 0:
            mkt_chg_pct = round((mkt_price - prev_price) / prev_price * 100, 2)
        if est_nav and mkt_price and est_nav > 0:
            premium     = round(mkt_price - est_nav, 4)
            premium_pct = round(premium / est_nav * 100, 2)

        edit_str = None
        try:
            tw_tz   = pytz.timezone("Asia/Taipei")
            edit_dt = datetime.fromisoformat(update_t).replace(tzinfo=None)
            edit_dt = tw_tz.localize(edit_dt)
            edit_str = edit_dt.strftime("%m/%d %H:%M")
        except Exception:
            pass

        navdate = today_data.get("NAVDATE", "").strip()

        return {
            "ticker":         "00990A",
            "usd_twd":        None,
            "prev_nav":       prev_nav,
            "est_nav":        est_nav,
            "nav_chg_pct":    nav_chg_pct,
            "prev_price":     prev_price,
            "market_price":   mkt_price,
            "market_chg_pct": mkt_chg_pct,
            "premium":        premium,
            "premium_pct":    premium_pct,
            "update_time":    edit_str or navdate,
        }
    except Exception as e:
        print(f"[NAV-990] Yuanta exception: {e}")
        return None


@app.route("/api/etf_nav")
def get_etf_nav():
    global _nav_cache
    etf = request.args.get("etf", "00988A")

    # 00990A → 元大 API
    if etf == "00990A":
        try:
            result = fetch_etf_nav_990_yuanta()
            if result:
                if not isinstance(_nav_cache, dict):
                    _nav_cache = {}
                _nav_cache[etf] = result
                return jsonify(result)
        except Exception as e:
            print(f"[NAV-990] exception: {e}")
        cached = _nav_cache.get(etf) if isinstance(_nav_cache, dict) else None
        return jsonify(cached if cached else {"error": "00990A NAV unavailable"})

    # 00988A（及其他）→ ezmoney
    BASE = "https://www.ezmoney.com.tw"
    UA   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/147.0.0.0 Safari/537.36"
    try:
        with requests.Session() as sess:
            sess.cookies.set("agree", "y", domain="www.ezmoney.com.tw", path="/")
            sess.get(f"{BASE}/ETF/Transaction/Estimate", params={"agree": "y"},
                     headers={"User-Agent": UA, "Accept": "text/html,*/*"}, timeout=12)
            resp = sess.post(
                f"{BASE}/ETF/Transaction/GetInTimeEstimation", json={},
                headers={
                    "User-Agent": UA,
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Content-Type": "application/json; charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"{BASE}/ETF/Transaction/Estimate?agree=y",
                    "Origin": BASE,
                }, timeout=12,
            )
            data = resp.json()
        etf_data = next((x for x in data.get("inTimeEstimation", [])
                         if x.get("StockNo") == etf), None)
        usd_twd  = None
        for c in data.get("currency", []):
            if c.get("Name") == "NTD":
                usd_twd = round(float(c["RateToUSD"]), 3)
                break
        if not etf_data:
            cached = _nav_cache.get(etf)
            return jsonify(cached if cached else {"error": f"{etf} not found"})
        edit_str = None
        m = re.search(r'\d+', etf_data.get("EditTime", ""))
        if m:
            try:
                tw_tz   = pytz.timezone("Asia/Taipei")
                edit_dt = datetime.fromtimestamp(int(m.group()) / 1000, tz=pytz.utc).astimezone(tw_tz)
                edit_str = edit_dt.strftime("%m/%d %H:%M")
            except Exception:
                pass
        result = {
            "ticker":         etf,
            "usd_twd":        usd_twd,
            "prev_nav":       etf_data.get("PerUnitYesterday"),
            "est_nav":        etf_data.get("PerUnitInTime"),
            "nav_chg_pct":    etf_data.get("PerUnitRate"),
            "prev_price":     etf_data.get("ClosePriceYesterday"),
            "market_price":   etf_data.get("ClosePriceInTime"),
            "market_chg_pct": etf_data.get("ClosePriceRate"),
            "premium":        etf_data.get("Discount"),
            "premium_pct":    etf_data.get("DiscountRate"),
            "update_time":    edit_str or etf_data.get("TranDateYesterday"),
        }
        if not isinstance(_nav_cache, dict):
            _nav_cache = {}
        _nav_cache[etf] = result
        return jsonify(result)
    except Exception as e:
        print(f"[NAV] exception: {e}")
        cached = _nav_cache.get(etf) if isinstance(_nav_cache, dict) else None
        if cached:
            return jsonify(cached)
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(debug=True, use_reloader=False, port=5000)
