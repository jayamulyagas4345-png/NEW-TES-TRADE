#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
backtest.py — Backtest strategi bot (main.py) pada data historis Binance Futures.

PRINSIP
  • Memakai analyze_pair() ASLI dari main.py (bukan salinan) — jadi yang dites
    = logika sinyal yang sama dengan bot live. Data live di-"palsukan" lewat
    penggantian fetch_ohlcv_realdata() agar hanya melihat candle yang sudah
    CLOSE sampai waktu simulasi (tidak ada intip masa depan).
  • TIDAK menyentuh trade_log.json / bot_state.json / Binance order apa pun.
    Hasil ditulis ke file TERPISAH:  <out>/backtest_log_<run>.csv
                                      <out>/backtest_summary_<run>.txt/.json

CARA PAKAI (taruh sejajar main.py)
    python backtest.py --days 7 --pairs 15
    python backtest.py --days 14 --pairs all --balance 1000 --risk 1.0
    python backtest.py --days 7 --pairs BTCUSDT,ETHUSDT,SOLUSDT
    python backtest.py --days 30 --end 2026-10-12 --pairs 15   # 30 hari SEBELUM 12 Okt 2026 (UTC)
    python backtest.py --selftest            # tes alur pakai data sintetis (tanpa internet)

ASUMSI EKSEKUSI (disamakan dengan bot live bila memungkinkan)
  • Entry = LIMIT di harga `entry` sinyal; batal jika harga menyentuh TP1 dulu,
    atau lewat LIMIT_ORDER_TIMEOUT_MINUTES.
  • TP1 menutup TP1_PARTIAL (50%), sisa: SL digeser ke entry (BE), target TP2.
  • Satu candle menyentuh SL & TP sekaligus → SL dianggap kena duluan (pesimis).
  • Candle tempat entry terisi hanya dicek SL-nya (TP baru dicek candle berikutnya).
  • Ukuran posisi = risk% x balance / jarak SL (tanpa compounding), fee per sisi.
  • TIDAK disimulasikan: minNotional/leverage/margin, batas trade per hari,
    structure-reversal close, slippage, funding fee.
"""
import argparse
import random
import contextlib
import copy
import csv
import io
import json
import math
import os
import sys
import time as _real_time
from collections import Counter
from datetime import datetime as _RealDT, timedelta, timezone

import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# ═════════════════════════════════════════════════════════════════════════════
#  ⚙️  SETTING — EDIT DI SINI, LALU TINGGAL RUN FILE INI (tanpa perlu argumen)
#  (Kalau tetap mau pakai command line, argumen CLI akan menimpa setting ini.)
# ═════════════════════════════════════════════════════════════════════════════
PAIRS      = "15"      # "all"  |  10 (10 pair pertama)  |  ["BTCUSDT", "ETHUSDT", "SOL"]
                        # nulis "SOL" otomatis jadi "SOLUSDT"
DAYS       = 30          # berapa hari ke belakang (maks 90)
BALANCE    = 1000.0     # modal awal / base uang (USDT)
MAX_OPEN   = 1          # maks posisi bersamaan. 1 = hanya 1 posisi sekaligus (None = otomatis sesuai DYNAMIC_TRADE_TIERS, saldo 1000 → 4 posisi)
RISK_PCT   = None       # risk % per trade dari BALANCE. None = ikut bot (MAX_SL_LOSS_PCT)
FEE_PCT    = 0.04       # fee % per sisi
SCAN_EVERY = 15         # menit, kelipatan 15
OUT_DIR    = None       # None = folder "backtests" di sebelah backtest.py
OPEN_FOLDER_AT_END = True    # buka folder hasil otomatis setelah selesai
PAUSE_AT_END       = True    # tahan window biar gak langsung nutup (tekan ENTER)
SINGLE_FILE        = True    # True = hasil cuma 1 file (ringkasan + daftar trade, kolom penting saja)
                             # (run dari Telegram /backtest tetap menghasilkan 3 file seperti dulu)

# ── Opsi uji (A/B) ───────────────────────────────────────────────────────────
DIRECTION  = "both"     # "both" | "long" | "short"  — batasi arah trade
EXP_BE_AT_R = None      # EKSPERIMEN (belum ada di bot live): geser SL ke entry saat profit
                        # menyentuh X × R sebelum TP1. Contoh 1.0. None = mati.
FLIP       = False      # EKSPERIMEN: balik arah sinyal (SHORT→LONG), SL & TP ditukar (TP asli jadi SL, SL asli jadi TP)
EXP_SINGLE_TP = False      # EKSPERIMEN: satu TP saja (tanpa partial/BE/trailing): keluar penuh di TP1 atau SL
BOT_OVERRIDES = {       # timpa konstanta main.py khusus run ini (tanpa edit main.py)
    # "TREND_GATE_ON": False,            # matikan trend gate (bandingkan sebelum/sesudah)
    # "TREND_GATE_ADX_MIN": 25,          # ketatkan ADX minimum (0 = tanpa syarat ADX)
    # "TREND_GATE_DI_ALIGN": False,      # matikan syarat arah +DI/-DI
    # "TREND_GATE_ADX_MAX": 0,           # v70: matikan cap ADX maksimum (0 = nonaktif)
    # "ENTRY_BLOCK_FVG_ONLY": False,     # v70: izinkan lagi entry zona FVG-saja
    # "MIN_SCORE_RELAXED_CUSTOM": 40,    # kembalikan floor skor lama
    # "SHORT_SCORE_PENALTY_RELAXED": 10, # kembalikan penalti SHORT lama
    # "RR_GRADE_B": 1.5,                 # kembalikan TP1 lama (1.5R); default sekarang 0.7R
    # "SLOW_TREND_GATE_ON": False,       # matikan filter EMA50/EMA200 HTF
    # "HTF_STRUCT_GATE_ON": False,       # v76: matikan gate struktur HTF (breakout/HH-HL, anti-sideways)
    # "EDGE_VOL_CONFIRM_ON": True,       # tes konfirmasi volume (mati = normal)
    # "EDGE_REGIME_GATE_ON": True,       # v78: blok SHORT saat BULL_REGIME & LONG saat BEAR_REGIME
}
# ═════════════════════════════════════════════════════════════════════════════

import main as bot  # noqa: E402  (import aman: tidak menjalankan bot)

for _k, _v in BOT_OVERRIDES.items():
    if hasattr(bot, _k):
        setattr(bot, _k, _v)
    else:
        print(f"⚠️  BOT_OVERRIDES: '{_k}' tidak ada di main.py — diabaikan", flush=True)

# Ikuti mode TP tunggal di main.py (v75): keluar penuh di TP, tanpa partial/BE.
if getattr(bot, "SINGLE_TP_MODE", False):
    EXP_SINGLE_TP = True

TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000, "1w": 604_800_000,
}
WARMUP_CANDLES = 320


# ═════════════════════════════════════════════════════════════════════════════
#  JAM SIMULASI (menggantikan time.time / datetime.now / sleep di main.py)
# ═════════════════════════════════════════════════════════════════════════════
class _Clock:
    ts = 0.0

    @classmethod
    def set_ms(cls, ms):
        cls.ts = ms / 1000.0

    @classmethod
    def dt(cls):
        return _RealDT.fromtimestamp(cls.ts, timezone.utc)

    @classmethod
    def ms(cls):
        return int(cls.ts * 1000)


class _FakeTime:
    def time(self):
        return _Clock.ts

    def sleep(self, _s):
        return None

    def __getattr__(self, name):
        return getattr(_real_time, name)


class _FakeDT(_RealDT):
    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return cls.fromtimestamp(_Clock.ts, timezone.utc).replace(tzinfo=None)
        return cls.fromtimestamp(_Clock.ts, tz)

    @classmethod
    def utcnow(cls):
        return cls.fromtimestamp(_Clock.ts, timezone.utc).replace(tzinfo=None)


# ═════════════════════════════════════════════════════════════════════════════
#  DATA HISTORIS
# ═════════════════════════════════════════════════════════════════════════════
class Store:
    """Unduh klines Binance Futures sekali per (symbol, tf), lalu layani irisan waktu."""

    def __init__(self, start_ms, end_ms, cache_dir):
        self.start_ms, self.end_ms = start_ms, end_ms
        self.cache_dir = cache_dir
        self.mem = {}
        self.warned = set()
        os.makedirs(cache_dir, exist_ok=True)

    def _download(self, symbol, tf, from_ms, to_ms):
        url = f"{bot.MARKET_DATA_BASE_URL}/fapi/v1/klines"
        rows, cur, fails = [], from_ms, 0
        while cur < to_ms:
            try:
                r = requests.get(
                    url,
                    params={"symbol": symbol, "interval": tf, "startTime": cur,
                            "endTime": to_ms, "limit": 1500},
                    headers=bot.get_headers(), timeout=20,
                )
                if r.status_code in (418, 429):
                    _real_time.sleep(min(60, 10 * (fails + 1)))
                    fails += 1
                    if fails > 4:
                        raise RuntimeError(f"HTTP {r.status_code} rate limit")
                    continue
                r.raise_for_status()
                data = r.json()
            except Exception as e:  # noqa: BLE001
                raise RuntimeError(f"{symbol}@{tf}: {e}")
            if not data:
                break
            rows.extend(data)
            cur = int(data[-1][0]) + 1
            if len(data) < 1500:
                break
            _real_time.sleep(0.12)
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=[
            "open_ms", "open", "high", "low", "close", "volume", "close_time", "qav",
            "num_trades", "tbb", "tbq", "ignore"])
        return df[["open_ms", "open", "high", "low", "close", "volume"]]

    def _prepare(self, df, tf):
        df = df.copy()
        for c in ("open", "high", "low", "close", "volume"):
            df[c] = df[c].astype(float)
        df["open_ms"] = df["open_ms"].astype("int64")
        df = df.drop_duplicates("open_ms").sort_values("open_ms").reset_index(drop=True)
        df["end_ms"] = df["open_ms"] + TF_MS[tf]
        df["timestamp"] = pd.to_datetime(df["open_ms"], unit="ms")
        return df

    def _load(self, symbol, tf):
        from_ms = self.start_ms - WARMUP_CANDLES * TF_MS[tf]
        day = _RealDT.fromtimestamp(from_ms / 1000, timezone.utc).strftime("%Y%m%d")
        path = os.path.join(self.cache_dir, f"{symbol}_{tf}_{day}.csv")
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                if len(df) and df["open_ms"].iloc[-1] + 2 * TF_MS[tf] >= self.end_ms:
                    return df
            except Exception:  # noqa: BLE001
                pass
        df = self._download(symbol, tf, from_ms, self.end_ms)
        if df is not None:
            df.to_csv(path, index=False)
        return df

    def get(self, symbol, tf):
        key = (symbol, tf)
        if key in self.mem:
            return self.mem[key]
        df = None
        if tf in TF_MS:
            try:
                raw = self._load(symbol, tf)
                df = self._prepare(raw, tf) if raw is not None and len(raw) else None
            except Exception as e:  # noqa: BLE001
                if key not in self.warned:
                    print(f"  ⚠️  data gagal: {e}", flush=True)
                    self.warned.add(key)
        self.mem[key] = df
        return df

    def funding(self, symbol):
        """Riwayat funding rate (time_ms, rate), di-cache. Dipakai strategi --strategy fund."""
        key = (symbol, "funding")
        if key in self.mem:
            return self.mem[key]
        df = None
        try:
            df = self._load_funding(symbol)
        except Exception as e:  # noqa: BLE001
            if key not in self.warned:
                print(f"  ⚠️  funding gagal: {e}", flush=True)
                self.warned.add(key)
        self.mem[key] = df
        return df

    def _load_funding(self, symbol):
        from_ms = self.start_ms - 3 * 86_400_000
        day = _RealDT.fromtimestamp(from_ms / 1000, timezone.utc).strftime("%Y%m%d")
        path = os.path.join(self.cache_dir, f"{symbol}_fund_{day}.csv")
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                if len(df) and df["time_ms"].iloc[-1] + 12 * 3_600_000 >= self.end_ms:
                    return df
            except Exception:  # noqa: BLE001
                pass
        url = f"{bot.MARKET_DATA_BASE_URL}/fapi/v1/fundingRate"
        rows, cur, fails = [], from_ms, 0
        while cur < self.end_ms:
            r = requests.get(url, params={"symbol": symbol, "startTime": cur,
                                          "endTime": self.end_ms, "limit": 1000},
                             headers=bot.get_headers(), timeout=20)
            if r.status_code in (418, 429):
                _real_time.sleep(min(60, 10 * (fails + 1)))
                fails += 1
                if fails > 4:
                    raise RuntimeError(f"{symbol} funding: HTTP {r.status_code} rate limit")
                continue
            r.raise_for_status()
            data = r.json()
            if not data:
                break
            rows.extend(data)
            cur = int(data[-1]["fundingTime"]) + 1
            if len(data) < 1000:
                break
            _real_time.sleep(0.12)
        if not rows:
            return None
        df = pd.DataFrame({"time_ms": [int(x["fundingTime"]) for x in rows],
                           "rate": [float(x["fundingRate"]) for x in rows]})
        df = df.drop_duplicates("time_ms").sort_values("time_ms").reset_index(drop=True)
        df.to_csv(path, index=False)
        return df


class SynthStore(Store):
    """Data acak (random walk bertren) untuk --selftest tanpa internet."""

    def __init__(self, start_ms, end_ms, cache_dir):
        super().__init__(start_ms, end_ms, cache_dir)
        self.base = {}

    def funding(self, symbol):
        import numpy as np
        key = (symbol, "funding")
        if key not in self.mem:
            rng = np.random.default_rng(abs(hash(symbol + "f")) % (2 ** 32))
            t0 = (self.start_ms - 3 * 86_400_000) // 28_800_000 * 28_800_000
            ms = np.arange(t0, self.end_ms, 28_800_000)
            self.mem[key] = pd.DataFrame({"time_ms": ms, "rate": rng.normal(0.0001, 0.0002, len(ms))})
        return self.mem[key]

    def _base(self, symbol):
        if symbol in self.base:
            return self.base[symbol]
        import numpy as np
        seed = abs(hash(symbol)) % (2 ** 32)
        rng = np.random.default_rng(seed)
        step = TF_MS["15m"]
        from_ms = self.start_ms - 130 * 86_400_000
        n = int((self.end_ms - from_ms) // step) + 1
        drift = np.repeat(rng.normal(0, 0.0006, n // 96 + 2), 96)[:n]
        ret = drift + rng.normal(0, 0.0045, n)
        close = 100 * np.exp(np.cumsum(ret))
        open_ = np.concatenate([[100.0], close[:-1]])
        wick = np.abs(rng.normal(0, 0.0025, n))
        high = np.maximum(open_, close) * (1 + wick)
        low = np.minimum(open_, close) * (1 - wick)
        vol = rng.uniform(100, 1000, n) * (1 + 3 * (np.abs(ret) > 0.008))
        ms = from_ms + np.arange(n) * step
        df = pd.DataFrame({"open_ms": ms, "open": open_, "high": high, "low": low,
                           "close": close, "volume": vol})
        self.base[symbol] = df
        return df

    def get(self, symbol, tf):
        key = (symbol, tf)
        if key in self.mem:
            return self.mem[key]
        base = self._base(symbol)
        if tf == "15m":
            raw = base
        else:
            g = base.copy()
            g["bucket"] = (g["open_ms"] // TF_MS[tf]) * TF_MS[tf]
            raw = g.groupby("bucket").agg(
                open=("open", "first"), high=("high", "max"), low=("low", "min"),
                close=("close", "last"), volume=("volume", "sum")).reset_index()
            raw = raw.rename(columns={"bucket": "open_ms"})
            raw = raw[raw["open_ms"] + TF_MS[tf] <= base["open_ms"].iloc[-1] + TF_MS["15m"]]
        df = self._prepare(raw, tf)
        self.mem[key] = df
        return df


STORE = None


def bt_funding(symbol):
    """Funding settlement yang SUDAH terjadi pada waktu simulasi (10 terakhir)."""
    df = STORE.funding(symbol)
    if df is None or not len(df):
        return None
    n = int(df["time_ms"].searchsorted(_Clock.ms(), side="right"))
    if n < 1:
        return None
    return df.iloc[max(0, n - 10):n]


def bt_fetch(symbol, interval, limit=200):
    """Pengganti fetch_ohlcv_realdata: hanya candle yang SUDAH close pada waktu simulasi."""
    tf = bot._INTERVAL_MAP.get(interval, interval)
    df = STORE.get(symbol, tf)
    if df is None:
        return None
    n = int(df["end_ms"].searchsorted(_Clock.ms(), side="right"))
    if n < 50:
        return None
    out = df.iloc[max(0, n - int(limit)):n][
        ["timestamp", "open", "high", "low", "close", "volume"]]
    return out.reset_index(drop=True)


def candle_at(symbol, open_ms):
    df = STORE.get(symbol, "15m")
    if df is None:
        return None
    i = int(df["open_ms"].searchsorted(open_ms))
    if i >= len(df) or int(df["open_ms"].iloc[i]) != open_ms:
        return None
    r = df.iloc[i]
    return float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])


# ═════════════════════════════════════════════════════════════════════════════
#  STATE SIMULASI
# ═════════════════════════════════════════════════════════════════════════════
class Sim:
    def __init__(self, args):
        self.a = args
        self.pending = {}      # pair -> order
        self.open = {}         # pair -> position
        self.cooldown = {}     # (pair, BULLISH/BEARISH) -> until (datetime)
        self.seen = set()
        self.rows = []
        self.day_pnl = Counter()      # tanggal UTC -> PnL terealisasi hari itu
        self.day_opened = Counter()   # tanggal UTC -> jumlah order yang dibuka hari itu
        self.week_pnl = Counter()     # (tahun ISO, minggu) -> PnL terealisasi
        self.month_pnl = Counter()    # (tahun, bulan) -> PnL terealisasi
        self.errs = Counter()
        self.fee = args.fee / 100.0
        self.risk_usdt = args.balance * args.risk / 100.0
        self.rng = random.Random(getattr(args, "seed", 1))   # untuk --random-entry


SIM = None


def _bt_is_on_cooldown(pair, direction):
    until = SIM.cooldown.get((pair, direction))
    now = _Clock.dt()
    if until and now < until:
        rem = (until - now).total_seconds() / 60
        return True, rem, f"Cooldown — sisa {rem:.0f} menit"
    return False, 0.0, ""


def _bt_has_active_position(pair, trade_direction):
    return pair in SIM.open or pair in SIM.pending


def _bt_is_dup(pair, direction, entry, sl):
    return (pair, direction, round(float(entry), 6), round(float(sl), 6)) in SIM.seen


_CACHE_INIT = {}


def _snapshot_caches():
    for name in dir(bot):
        if name in ("_exchange_info_cache",) or name.startswith("_leverage"):
            continue
        if not name.lower().endswith("cache") and "_cache" not in name.lower():
            continue
        val = getattr(bot, name)
        if isinstance(val, (dict, list)):
            _CACHE_INIT[name] = copy.deepcopy(val)


def _reset_caches():
    for name, val in _CACHE_INIT.items():
        setattr(bot, name, copy.deepcopy(val))


def install_patches():
    bot.time = _FakeTime()
    bot.datetime = _FakeDT
    bot.fetch_ohlcv_realdata = bt_fetch
    bot.is_on_cooldown = _bt_is_on_cooldown
    bot.has_active_position = _bt_has_active_position
    bot._is_duplicate_signal = _bt_is_dup
    _snapshot_caches()


# ═════════════════════════════════════════════════════════════════════════════
#  EKSEKUSI & MANAJEMEN POSISI
# ═════════════════════════════════════════════════════════════════════════════
def _csv_fields():
    base = list(bot._TRADE_LOG_CSV_FIELDS)          # sudah termasuk kolom versi bot
    extra = ["bt_run_id", "bt_signal_ts", "bt_fill_ts", "adx_htf", "di_gap_htf", "ema_gap_htf", "htf_struct_ok", "htf_er"]
    return base + [c for c in extra if c not in base]


def _finish(pos, close_dt, close_price, close_type):
    c = pos["cand"]
    direction = pos["dir"]
    entry = pos["entry"]
    sign = 1 if direction == "LONG" else -1
    pnl = pos["realized"]
    if pos["qty_left"] > 0:
        pnl += (close_price - entry) * sign * pos["qty_left"]
        pos["fees"] += close_price * pos["qty_left"] * SIM.fee
        pos["qty_left"] = 0.0
    pnl -= pos["fees"]
    risk_dist = abs(entry - pos["sl0"]) or 1e-9
    leg_close = pos["last_exit"] if pos["last_exit"] else close_price
    row = {k: "" for k in FIELDS}
    row.update({
        "ts": close_dt.isoformat(), "symbol": pos["pair"], "direction": direction,
        "entry": entry, "close_price": round(leg_close, 8), "sl": pos["sl0"],
        "tp1": pos["tp1"], "tp2": pos["tp2"], "lot": round(pos["qty"], 6),
        "pnl_usdt": round(pnl, 4), "close_type": close_type,
        "mode": c["mode"]["label"], "entry_tf": c["mode"]["entry_tf"],
        "score": c["score"], "rr_plan": round(float(c["rr"]), 2),
        "rr_actual": round(abs(leg_close - entry) / risk_dist, 2),
        "is_win": pnl > 0, "score_bd": c["score_bd"],
        "atr_pct": c.get("atr_pct", ""), "sl_atr_mult": c.get("sl_atr_mult", ""),
        "vol_rat": c.get("vol_rat", ""), "htf_bias": c.get("htf_bias", ""),
        "bos_strength": c.get("bos_strength", ""), "sweep_strength": c.get("sweep_strength", ""),
        "engulf_break": c.get("engulf_break", ""), "engulf_entry": c.get("engulf_entry", ""),
        "engulf_note": c.get("engulf_note", ""),
        "fvg_retest": c.get("fvg_retest", ""), "fvg_retest_note": c.get("fvg_retest_note", ""),
        "stoch_div": c.get("stoch_div", ""), "stoch_weak": c.get("stoch_weak", ""),
        "stoch_note": c.get("stoch_note", ""),
        "sl_source": c.get("sl_source", ""), "rsi_entry": round(float(c.get("rsi", 0)), 2),
        "mae_pct": round(pos["mae"], 4), "mfe_pct": round(pos["mfe"], 4),
        "duration_sec": int((close_dt - pos["fill_dt"]).total_seconds()),
        "btc_bias": c.get("btc_bias", ""), "btcd_trend": c.get("btcd_trend", ""),
        "btc_ltf_dir": c.get("btc_ltf_dir", ""), "btcd_ltf_dir": c.get("btcd_ltf_dir", ""),
        "mkt_regime": c.get("mkt_regime", ""), "alt_bull_pct": c.get("alt_bull_pct", ""), "alt_bear_pct": c.get("alt_bear_pct", ""),
        "btc_bias_at_close": "-", "btcd_trend_at_close": "-",
        "btc_ltf_dir_at_close": "-", "btcd_ltf_dir_at_close": "-",
        "bt_run_id": SIM.a.run_id,
        "bt_signal_ts": pos["signal_dt"].isoformat(),
        "bt_fill_ts": pos["fill_dt"].isoformat(),
        "adx_htf": c.get("adx_htf", ""), "di_gap_htf": c.get("di_gap_htf", ""), "ema_gap_htf": c.get("ema_gap_htf", ""),
        "htf_struct_ok": c.get("htf_struct_ok", ""), "htf_er": c.get("htf_er", ""),
    })
    row.update(bot._bot_version_fields())
    SIM.rows.append(row)
    SIM.day_pnl[close_dt.date()] += pnl
    _cd = close_dt.date(); _iso = _cd.isocalendar()
    SIM.week_pnl[(_iso[0], _iso[1])] += pnl
    SIM.month_pnl[(_cd.year, _cd.month)] += pnl
    SIM.writer.writerow(row)
    SIM.fh.flush()
    SIM.open.pop(pos["pair"], None)


def _part_exit(pos, price, frac_qty):
    sign = 1 if pos["dir"] == "LONG" else -1
    q = min(frac_qty, pos["qty_left"])
    pos["realized"] += (price - pos["entry"]) * sign * q
    pos["fees"] += price * q * SIM.fee
    pos["qty_left"] -= q
    pos["last_exit"] = price


def manage_step(t_ms):
    """Proses candle 15m yang baru close (open_ms = t_ms - 15m) untuk order & posisi."""
    open_ms = t_ms - TF_MS["15m"]
    close_dt = _RealDT.fromtimestamp(t_ms / 1000, timezone.utc)

    # 1) Pending limit order
    for pair in list(SIM.pending):
        o = SIM.pending[pair]
        ca = candle_at(pair, open_ms)
        if ca is None:
            continue
        _, hi, lo, _c = ca
        age_min = (close_dt - o["placed_dt"]).total_seconds() / 60
        long_ = o.get("fill_dir", o["dir"]) == "LONG"   # arah ASLI sinyal (untuk logika fill/batal)
        pos_long = o["dir"] == "LONG"
        filled = (lo <= o["entry"]) if long_ else (hi >= o["entry"])
        if filled:
            SIM.pending.pop(pair)
            pos = {
                "pair": pair, "dir": o["dir"], "entry": o["entry"], "sl": o["sl"], "sl0": o["sl"],
                "tp1": o["tp1"], "tp2": o["tp2"], "qty": o["qty"], "qty_left": o["qty"],
                "realized": 0.0, "fees": o["entry"] * o["qty"] * SIM.fee, "tp1_hit": False,
                "last_exit": None, "cand": o["cand"], "signal_dt": o["placed_dt"],
                "fill_dt": _RealDT.fromtimestamp(open_ms / 1000, timezone.utc),
                "mae": 0.0, "mfe": 0.0,
            }
            SIM.open[pair] = pos
            # candle fill: hanya cek SL (pesimis)
            _track_excursion(pos, hi, lo)
            if (lo <= pos["sl"]) if pos_long else (hi >= pos["sl"]):
                _part_exit(pos, pos["sl"], pos["qty_left"])
                _finish(pos, close_dt, pos["sl"], "SL")
            continue
        _otp = o.get("orig_tp", o["tp1"])
        touched_tp1 = (hi >= _otp) if long_ else (lo <= _otp)
        _tmo = o.get("expire_min") or bot.LIMIT_ORDER_TIMEOUT_MINUTES
        _inv = o.get("invalid_px")
        _invalid = bool(_inv) and ((lo <= _inv) if long_ else (hi >= _inv))   # struktur patah sebelum fill
        if touched_tp1 or _invalid or age_min > _tmo:
            SIM.pending.pop(pair)

    # 2) Posisi terbuka (bukan yang baru terisi di candle ini)
    for pair in list(SIM.open):
        pos = SIM.open[pair]
        if pos["fill_dt"] >= _RealDT.fromtimestamp(open_ms / 1000, timezone.utc):
            continue
        ca = candle_at(pair, open_ms)
        if ca is None:
            continue
        _, hi, lo, _c = ca
        long_ = pos["dir"] == "LONG"
        _track_excursion(pos, hi, lo)
        sl_hit = (lo <= pos["sl"]) if long_ else (hi >= pos["sl"])
        if sl_hit:
            _part_exit(pos, pos["sl"], pos["qty_left"])
            _finish(pos, close_dt, pos["sl"], "BE" if (pos["tp1_hit"] or pos.get("be_early")) else "SL")
            continue
        if EXP_BE_AT_R and not pos["tp1_hit"] and not pos.get("be_early"):
            _r = abs(pos["entry"] - pos["sl0"])
            _lvl = pos["entry"] + _r * EXP_BE_AT_R if long_ else pos["entry"] - _r * EXP_BE_AT_R
            if (hi >= _lvl) if long_ else (lo <= _lvl):
                try:
                    pos["sl"] = bot.be_target_price(pos["entry"], pos["dir"])   # BE + buffer fee (sama dgn bot)
                except Exception:  # noqa: BLE001
                    pos["sl"] = pos["entry"]
                pos["be_early"] = True
        if not pos["tp1_hit"]:
            if (hi >= pos["tp1"]) if long_ else (lo <= pos["tp1"]):
                if EXP_SINGLE_TP:
                    _part_exit(pos, pos["tp1"], pos["qty_left"])
                    _finish(pos, close_dt, pos["tp1"], "TP2")
                    continue
                _part_exit(pos, pos["tp1"], pos["qty"] * bot.TP1_PARTIAL)
                pos["tp1_hit"] = True
                # BE + buffer fee (sama dengan bot live: main.be_target_price)
                try:
                    pos["sl"] = bot.be_target_price(pos["entry"], pos["dir"])
                except Exception:  # noqa: BLE001
                    pos["sl"] = pos["entry"]   # trailing BE setelah TP1
        if pos["tp1_hit"] and ((hi >= pos["tp2"]) if long_ else (lo <= pos["tp2"])):
            _part_exit(pos, pos["tp2"], pos["qty_left"])
            _finish(pos, close_dt, pos["tp2"], "TP2")


def _track_excursion(pos, hi, lo):
    e = pos["entry"]
    if pos["dir"] == "LONG":
        pos["mfe"] = max(pos["mfe"], (hi - e) / e * 100)
        pos["mae"] = max(pos["mae"], (e - lo) / e * 100)
    else:
        pos["mfe"] = max(pos["mfe"], (e - lo) / e * 100)
        pos["mae"] = max(pos["mae"], (hi - e) / e * 100)


def place_order(cand, now_dt):
    sig = cand["signal"]
    pair = cand["pair"]
    direction = sig["direction"]
    entry, sl, tp1, tp2 = float(cand["entry"]), float(cand["sl"]), float(cand["tp1"]), float(cand["tp2"])
    if direction == "LONG":
        if sl >= entry:
            return False
        if tp1 <= entry or tp2 <= entry or (tp2 <= tp1 and not EXP_SINGLE_TP):
            mm = entry * 0.003
            tp1, tp2 = entry + mm * bot.RR_GRADE_B, entry + mm * (bot.RR_GRADE_A + 0.5)
    else:
        if sl <= entry:
            return False
        if tp1 >= entry or tp2 >= entry or (tp2 >= tp1 and not EXP_SINGLE_TP):
            mm = entry * 0.003
            tp1, tp2 = entry - mm * bot.RR_GRADE_B, entry - mm * (bot.RR_GRADE_A + 0.5)
    try:
        cap = bot.SL_TP_CAPS.get(cand["mode"].get("label"), (bot.MAX_SL_DISTANCE_PCT, 0.09))[0]
        # strategi sd punya batas SL sendiri (sd.P max_sl_pct); cap LOW_TF bot (1.2% x3) tidak berlaku
        if not cand.get("skip_cap") and abs(entry - sl) / entry > cap * 3:
            return False
    except Exception:  # noqa: BLE001
        pass
    fill_dir, orig_tp = direction, tp1
    if FLIP:
        # Balik arah: entry sama, SL baru = TP asli, TP baru = SL asli (RR jadi kebalikan).
        # Order fill tetap memakai arah ASLI (entry di zona = stop-entry arah berlawanan,
        # bukan limit yang langsung terisi di harga pasar).
        direction = "SHORT" if direction == "LONG" else "LONG"
        sl, tp1 = tp1, sl
        tp2 = tp1
    dist = abs(entry - sl)
    if dist <= 0:
        return False
    qty = SIM.risk_usdt / dist
    SIM.pending[pair] = {"pair": pair, "dir": direction, "entry": entry, "sl": sl,
                         "fill_dir": fill_dir, "orig_tp": orig_tp,
                         "tp1": tp1, "tp2": tp2, "qty": qty, "cand": cand,
                         "placed_dt": now_dt,
                         "expire_min": cand.get("expire_min"), "invalid_px": cand.get("invalid_px")}
    return True


def scan(pairs, modes, now_dt):
    btc_bias = bot.get_btc_bias()
    btcd_trend = bot.get_btcd_bias()
    session = bot.get_session()
    cands = []
    for pair in pairs:
        for mode in modes:
            df_htf = bt_fetch(pair, mode["htf_tf"], 250)
            if df_htf is None:
                continue
            df_ref = bt_fetch(pair, mode["ref_tf"], 250) if mode.get("ref_tf") else None
            local = []
            try:
                bot.analyze_pair(pair=pair, mode=mode, df_htf=df_htf, df_ref=df_ref,
                                 btc_bias=btc_bias, btcd_trend=btcd_trend,
                                 session=session, signal_candidates=local)
            except Exception as e:  # noqa: BLE001
                SIM.errs[f"{type(e).__name__}: {str(e)[:70]}"] += 1
                continue
            cands.extend(local)
    if not cands:
        return 0
    seen = {}
    for c in cands:
        k = f"{c['pair']}|{c['direction']}"
        ex = seen.get(k)
        if not ex or c["score"] > ex["score"] or (c["score"] == ex["score"] and c["rr"] > ex["rr"]):
            seen[k] = c
    cands = list(seen.values())
    if str(DIRECTION).lower() in ("long", "short"):
        _want = "BULLISH" if str(DIRECTION).lower() == "long" else "BEARISH"
        cands = [c for c in cands if c["direction"] == _want]
    try:
        cands = bot.apply_correlation_filter(cands)
    except Exception:  # noqa: BLE001
        pass
    cands = [c for c in cands if c["pair"] not in SIM.pending and c["pair"] not in SIM.open
             and not _bt_is_dup(c["pair"], c["direction"], c["entry"], c["sl"])]
    cands.sort(key=lambda c: (c["score"], c["rr"]), reverse=True)
    slots = max(0, SIM.a.max_open - len(SIM.open) - len(SIM.pending))
    n = 0
    for c in cands[:slots]:
        c["mode"] = c["mode"]
        if place_order(c, now_dt):
            n += 1
            SIM.seen.add((c["pair"], c["direction"], round(float(c["entry"]), 6),
                          round(float(c["sl"]), 6)))
            SIM.cooldown[(c["pair"], c["direction"])] = now_dt + timedelta(
                hours=bot.SIGNAL_COOLDOWN_HOURS)
    return n


_place_order_orig = place_order


def place_order(cand, now_dt):  # noqa: F811 — bungkus: hitung order yang dibuka per hari (untuk --max-trades-day)
    ok = _place_order_orig(cand, now_dt)
    if ok:
        SIM.day_opened[now_dt.date()] += 1
    return ok


def _day_ok():
    """Batas harian ala bot live (DAILY_LOSS_LIMIT_PCT / DAILY_WIN_LIMIT_PCT / MAX_TRADES_PER_DAY).
    Persen dihitung dari modal awal backtest (risiko per trade tetap, tanpa compounding).
    Saat batas kena: scan baru dilewati sampai 00:00 UTC; posisi yang sudah ada tetap dikelola."""
    a = SIM.a
    d = _Clock.dt().date()
    base = a.balance
    if a.daily_loss_pct > 0 and SIM.day_pnl[d] <= -base * a.daily_loss_pct / 100.0:
        SIM.errs["limit harian: loss tercapai (scan dilewati)"] += 1
        return False
    if a.daily_win_pct > 0 and SIM.day_pnl[d] >= base * a.daily_win_pct / 100.0:
        SIM.errs["limit harian: win tercapai (scan dilewati)"] += 1
        return False
    _iso = d.isocalendar(); _wk = (_iso[0], _iso[1]); _mo = (d.year, d.month)
    if getattr(a, "weekly_loss_pct", 0) > 0 and SIM.week_pnl[_wk] <= -base * a.weekly_loss_pct / 100.0:
        SIM.errs["limit mingguan: loss tercapai (scan dilewati)"] += 1
        return False
    if getattr(a, "monthly_loss_pct", 0) > 0 and SIM.month_pnl[_mo] <= -base * a.monthly_loss_pct / 100.0:
        SIM.errs["limit bulanan: loss tercapai (scan dilewati)"] += 1
        return False
    if getattr(a, "monthly_win_pct", 0) > 0 and SIM.month_pnl[_mo] >= base * a.monthly_win_pct / 100.0:
        SIM.errs["limit bulanan: target profit tercapai (scan dilewati)"] += 1
        return False
    if a.max_trades_day > 0 and SIM.day_opened[d] >= a.max_trades_day:
        SIM.errs["limit harian: max trade/hari (scan dilewati)"] += 1
        return False
    return True


def scan_random(pairs, modes, now_dt):
    """BASELINE ACAK (--random-entry): masuk di waktu & arah ACAK, tanpa sinyal SMC.

    Dipakai untuk mengukur apakah logika entry bot menambah edge dibanding entry acak.
    Yang SAMA dengan bot: exit (SL / TP1 partial / BE / TP2), fee, ukuran risiko per trade,
    max posisi, cooldown, ATR-based SL (SL = rand_sl_atr x ATR TF HTF, mirip rata-rata bot
    ~3.55 x ATR), TP1 = RR_GRADE_B R, TP2 = RR_GRADE_A+0.5 R.
    Yang BEDA: arah 50/50 acak, waktu acak (peluang rand_prob per pair per scan),
    entry = harga close terakhir (limit di harga pasar), tanpa filter apa pun.
    """
    a = SIM.a
    rng = SIM.rng
    mode = modes[0]
    tp1_r = float(a.rand_tp1_r) if getattr(a, "rand_tp1_r", None) else float(bot.RR_GRADE_B)
    tp2_r = float(a.rand_tp2_r) if getattr(a, "rand_tp2_r", None) else float(bot.RR_GRADE_A) + 0.5
    cands = []
    for pair in pairs:
        if pair in SIM.pending or pair in SIM.open:
            continue
        if rng.random() >= a.rand_prob:
            continue
        direction = "LONG" if rng.random() < 0.5 else "SHORT"
        if str(DIRECTION).lower() in ("long", "short") and direction.lower() != str(DIRECTION).lower():
            continue
        tag = "BULLISH" if direction == "LONG" else "BEARISH"
        if _bt_is_on_cooldown(pair, tag)[0]:
            continue
        df_e = bt_fetch(pair, mode["entry_tf"], 50)
        df_h = bt_fetch(pair, mode["htf_tf"], 250)
        if df_e is None or df_h is None:
            SIM.errs["acak: data candle belum cukup"] += 1
            continue
        try:
            atr = float(bot.calculate_atr(df_h))
        except Exception as e:  # noqa: BLE001
            SIM.errs[f"acak: ATR gagal ({type(e).__name__})"] += 1
            continue
        px = float(df_e["close"].iloc[-1])
        if not (atr > 0 and px > 0):
            SIM.errs["acak: ATR/harga tidak valid"] += 1
            continue
        dist = atr * a.rand_sl_atr
        sg = 1 if direction == "LONG" else -1
        entry = px
        cands.append({
            "pair": pair, "direction": tag, "signal": {"direction": direction},
            "mode": mode, "entry": entry, "sl": entry - sg * dist,
            "tp1": entry + sg * dist * tp1_r, "tp2": entry + sg * dist * tp2_r,
            "score": 0, "rr": tp2_r, "score_bd": "random_baseline",
            "atr_pct": round(atr / px * 100, 4), "sl_atr_mult": a.rand_sl_atr,
            "sl_source": "RANDOM_ATR",
        })
    rng.shuffle(cands)
    slots = max(0, a.max_open - len(SIM.open) - len(SIM.pending))
    n = 0
    for c in cands[:slots]:
        if place_order(c, now_dt):
            n += 1
            SIM.cooldown[(c["pair"], c["direction"])] = now_dt + timedelta(
                hours=bot.SIGNAL_COOLDOWN_HOURS)
        else:
            SIM.errs["acak: place_order ditolak"] += 1
    return n


def _sd_funnel(L, args):
    """Diagnostik jumlah sinyal: berapa evaluasi zona gugur di tiap aturan sd.py."""
    if getattr(args, "strategy", "") != "sd":
        return
    import sd as _sdm
    L.append("─" * 38)
    L.append("Funnel SD (evaluasi gugur per aturan):")
    for k, v in sorted(_sdm.STATS.items()):
        L.append(f"  {k}: {v}")


def scan_smc2(pairs, modes, now_dt):
    """STRATEGI BARU (--strategy smc2): sweep → MSS → entry FVG/OB → target liquidity (lihat smc2.py).
    --coinflip: BASELINE KONTROL — waktu, jarak SL & kelipatan R sama persis dengan sinyal smc2,
    tapi ARAH diundi (entry market di close terakhir). Mengisolasi apakah ARAH/entry smc2 punya edge."""
    import smc2
    import sd as _sd
    import ob as _ob
    import htf as _htf
    import fund as _fund
    a = SIM.a
    rng = SIM.rng
    mode = modes[0]
    cands = []
    sdmode = getattr(a, "strategy", "") == "sd"
    obmode = getattr(a, "strategy", "") == "ob"
    htfmode = getattr(a, "strategy", "") == "htf"
    fundmode = getattr(a, "strategy", "") == "fund"
    _mod = _sd if sdmode else (_ob if obmode else (_htf if htfmode else (_fund if fundmode else smc2)))
    ov = {}
    for kv in getattr(a, "smc2_p", []):
        k, v = kv.split("=")
        ov[k] = type(_mod.P[k])(float(v)) if isinstance(_mod.P[k], (int, float)) else v
    dfbtc = bt_fetch("BTCUSDT", "1h", 120) if ov.get("use_btc") else None
    for pair in pairs:
        if pair in SIM.pending or pair in SIM.open:
            continue
        df4h = bt_fetch(pair, "4h", 120) if (ov.get("use_h4") or htfmode) else None
        df1d = bt_fetch(pair, "1d", 250) if sdmode else None
        df15 = bt_fetch(pair, mode["entry_tf"], 250)
        df1h = bt_fetch(pair, mode["htf_tf"], 150 if htfmode else 120)
        if df15 is None or df1h is None or (htfmode and df4h is None):
            SIM.errs["smc2: data candle belum cukup"] += 1
            continue
        try:
            if sdmode:
                sigs = _sd.find_signal(df15, df1h, ov or None, df1d)
            elif obmode:
                sigs = _ob.find_signal(df15, df1h, ov or None)
            elif htfmode:
                sigs = _htf.find_signal(df15, df1h, ov or None, df4h)
            elif fundmode:
                _fd = bt_funding(pair)
                if _fd is None:
                    SIM.errs["fund: data funding tidak ada"] += 1
                    continue
                sigs = _fund.find_signal(df15, df1h, ov or None, _fd, _Clock.ms())
            else:
                sigs = smc2.find_signal(df15, df1h, ov or None, df4h, dfbtc)
        except Exception as e:  # noqa: BLE001
            SIM.errs[f"smc2: {type(e).__name__}: {str(e)[:60]}"] += 1
            continue
        for sg_ in sigs:
            direction = sg_["direction"]
            tag = "BULLISH" if direction == "LONG" else "BEARISH"
            if str(DIRECTION).lower() in ("long", "short") and direction.lower() != str(DIRECTION).lower():
                continue
            if _bt_is_on_cooldown(pair, tag)[0] or _bt_is_dup(pair, tag, sg_["entry"], sg_["sl"]):
                continue
            entry, sl, tp1, tp2 = sg_["entry"], sg_["sl"], sg_["tp1"], sg_["tp2"]
            c_ = {"pair": pair, "direction": tag, "signal": {"direction": direction},
                  "mode": mode, "entry": entry, "sl": sl, "tp1": tp1, "tp2": tp2,
                  "score": 0, "rr": round(sg_["tp2_r"], 2), "score_bd": f"smc2_{sg_['zone']}",
                  "atr_pct": round(sg_["atr15"] / float(df15["close"].iloc[-1]) * 100, 4),
                  "sl_atr_mult": round(abs(entry - sl) / sg_["atr15"], 2), "sl_source": "SWEEP",
                  "sweep_strength": f"{sg_['sweep_depth_atr']:.2f}ATR",
                  "expire_min": sg_.get("expire_min", 240), "invalid_px": sg_["invalid"],
                  "adx_htf": sg_.get("adx_htf", ""), "btc_bias": sg_.get("btc_bias", ""),
                  "skip_cap": sdmode or obmode or htfmode or fundmode}
            if getattr(a, "coinflip", False):
                px = float(df15["close"].iloc[-1])
                dist = abs(entry - sl)
                tp1_r = abs(tp1 - entry) / dist
                tp2_r = abs(tp2 - entry) / dist
                flip = "LONG" if rng.random() < 0.5 else "SHORT"
                f = 1 if flip == "LONG" else -1
                c_.update({"direction": "BULLISH" if flip == "LONG" else "BEARISH",
                           "signal": {"direction": flip}, "entry": px, "sl": px - f * dist,
                           "tp1": px + f * dist * tp1_r, "tp2": px + f * dist * tp2_r,
                           "score_bd": "coinflip", "sl_source": "COINFLIP",
                           "expire_min": 60, "invalid_px": None})
            cands.append(c_)
    if not cands:
        return 0
    # satu kandidat per pair (R:R terbesar), lalu isi slot
    best = {}
    for c_ in cands:
        k = c_["pair"]
        if k not in best or c_["rr"] > best[k]["rr"]:
            best[k] = c_
    cl = list(best.values())
    rng.shuffle(cl) if getattr(a, "coinflip", False) else cl.sort(key=lambda x: x["rr"], reverse=True)
    slots = max(0, a.max_open - len(SIM.open) - len(SIM.pending))
    n = 0
    for c_ in cl[:slots]:
        if place_order(c_, now_dt):
            n += 1
            SIM.seen.add((c_["pair"], c_["direction"], round(float(c_["entry"]), 6), round(float(c_["sl"]), 6)))
            SIM.cooldown[(c_["pair"], c_["direction"])] = now_dt + timedelta(hours=4)
        else:
            SIM.errs["smc2: place_order ditolak"] += 1
    return n


# ═════════════════════════════════════════════════════════════════════════════
#  RINGKASAN
# ═════════════════════════════════════════════════════════════════════════════
def summarize(rows, args, pairs, steps, n_open_end, errs):
    L = []
    ver = bot._bot_version_fields()
    L.append(f"📊 BACKTEST {args.run_id}")
    L.append(f"Versi bot : {ver['bot_version']} (upload {ver['build_date']})")
    L.append(f"Periode   : {args.start_str} → {args.end_str} ({args.days} hari)")
    L.append(f"Pair      : {len(pairs)} | Scan tiap {args.scan_every} menit | Max open {args.max_open}")
    L.append(f"Limit hari: loss {args.daily_loss_pct:g}% | win {args.daily_win_pct:g}% | max trade/hari {args.max_trades_day} (0 = off)")
    L.append(f"Limit periode: mingguan loss {args.weekly_loss_pct:g}% | bulanan loss {args.monthly_loss_pct:g}% | bulanan target {args.monthly_win_pct:g}% (0 = off)")
    _cfg = f"arah={DIRECTION} | BE_awal={EXP_BE_AT_R}"
    _cfg += f" | gate={'ON' if bot.TREND_GATE_ON else 'OFF'} adx>={bot.TREND_GATE_ADX_MIN:g} di={bot.TREND_GATE_DI_ALIGN}"
    _cfg += f" | floor={bot.MIN_SCORE_RELAXED_CUSTOM} shortpen={bot.SHORT_SCORE_PENALTY_RELAXED}"
    _cfg += f" | TP1={bot.RR_GRADE_B}R slow_trend={'ON' if bot.SLOW_TREND_GATE_ON else 'OFF'}"
    _cfg += f" | ema_pullback={getattr(bot, 'EMA_PULLBACK_MODE', 'OFF')}"
    if FLIP:
        _cfg += " | FLIP=ON (arah dibalik, SL<->TP)"
    if EXP_SINGLE_TP:
        _cfg += f" | SINGLE_TP={bot.RR_GRADE_B:g}R (tanpa partial)"
    _cfg += f" | adx_max={getattr(bot, 'TREND_GATE_ADX_MAX', 0):g} fvg_only_block={'ON' if getattr(bot, 'ENTRY_BLOCK_FVG_ONLY', False) else 'OFF'}"
    _cfg += f" | research={'ON' if getattr(bot, 'RESEARCH_MODE', False) else 'OFF'} retest_zone={getattr(bot, 'HTF_RETEST_ZONE_MODE', 0)}/src{getattr(bot, 'HTF_RETEST_ZONE_SRC', 0)}/pick{getattr(bot, 'RZ_OB_PICK', 0)} regime_aligned={'ON' if getattr(bot, 'REGIME_REQUIRE_ALIGNED', False) else 'OFF'} demand_long={getattr(bot, 'HTF_DEMAND_MODE', 0)} choch_demand={'ON' if getattr(bot, 'DZ_CHOCH_ENTRY', False) else 'OFF'} dz_disp={getattr(bot, 'DZ_MIN_DISP_ATR', 0):g} dz_buf={getattr(bot, 'DZ_SL_BUF_ATR', 0):g} struct_filter={'ON' if getattr(bot, 'STRUCT_FILTER_LTF', False) else 'OFF'} sl_buffer={getattr(bot, 'SL_BUFFER_ATR_MULT', 0):g}xATR dirs={'+'.join(sorted(getattr(bot, 'EDGE_ALLOWED_DIRS', [])))}"
    L.append(f"Config    : {_cfg}")
    if getattr(args, "random_entry", False):
        L.append(f"MODE      : *** BASELINE ACAK *** seed={args.seed} prob={args.rand_prob:g} "
                 f"SL={args.rand_sl_atr:g}xATR (BUKAN sinyal bot)")
    if getattr(args, "strategy", "default") == "ob":
        L.append("MODE      : *** OB *** sweep→BOS→limit di Order Block | TP 0.8R/3.5R" +
                 (" | KONTROL COINFLIP (arah diundi)" if getattr(args, "coinflip", False) else ""))
    if getattr(args, "strategy", "default") == "fund":
        L.append("MODE      : *** FUND *** kontrarian funding rate ekstrem + konfirmasi 1H | TP 1R/2R" +
                 (" | KONTROL COINFLIP (arah diundi)" if getattr(args, "coinflip", False) else ""))
    if getattr(args, "strategy", "default") == "htf":
        L.append("MODE      : *** HTF *** tren 4H → pullback 50% impuls 1H | TP 1R/3R" +
                 (" | KONTROL COINFLIP (arah diundi)" if getattr(args, "coinflip", False) else ""))
    if getattr(args, "strategy", "default") == "sd":
        L.append("MODE      : *** SD *** demand/supply zone 1D → konfirmasi 1H → target supply/demand" +
                 (" | KONTROL COINFLIP (arah diundi)" if getattr(args, "coinflip", False) else ""))
    if getattr(args, "strategy", "default") == "smc2":
        L.append("MODE      : *** SMC v2 *** sweep→MSS→FVG/OB→target liquidity" +
                 (" | KONTROL COINFLIP (arah diundi)" if getattr(args, "coinflip", False) else ""))
    L.append(f"Fix final : max_SL_entry={getattr(bot, 'MAX_SL_ENTRY_PCT', 0)*100:g}% (0=off) | BE_fee_buffer={getattr(bot, 'BE_FEE_BUFFER_PCT', 0)*100:g}%")
    L.append(f"Modal     : {args.balance:,.0f} USDT | Risk/trade {args.risk}% | Fee {args.fee}%/sisi")
    L.append("─" * 38)
    summary = {"run_id": args.run_id, "trades": len(rows)}
    if not rows:
        L.append("Tidak ada trade yang selesai pada periode ini.")
        if errs:
            L.append("Error analisa terbanyak: " + "; ".join(f"{k} (x{v})" for k, v in errs.most_common(3)))
        _sd_funnel(L, args)
        return "\n".join(L), summary
    df = pd.DataFrame(rows)
    df["pnl_usdt"] = df["pnl_usdt"].astype(float)
    df["is_win"] = df["pnl_usdt"] > 0
    n = len(df)
    wins = int(df["is_win"].sum())
    gp = df.loc[df["pnl_usdt"] > 0, "pnl_usdt"].sum()
    gl = -df.loc[df["pnl_usdt"] < 0, "pnl_usdt"].sum()
    pf = (gp / gl) if gl > 0 else float("inf")
    cum = df["pnl_usdt"].cumsum()
    dd = (cum.cummax() - cum).max()
    streak = cur = 0
    for w in df["is_win"]:
        cur = 0 if w else cur + 1
        streak = max(streak, cur)
    ct = df["close_type"].value_counts().to_dict()
    tp2, sl_n = ct.get("TP2", 0), ct.get("SL", 0)
    strict_wr = (tp2 / (tp2 + sl_n) * 100) if (tp2 + sl_n) else float("nan")
    exp_r = df["pnl_usdt"].mean() / SIM.risk_usdt
    L.append(f"Trade selesai : {n}   (masih terbuka di akhir data: {n_open_end})")
    L.append(f"Winrate       : {wins/n*100:.1f}%  ({wins}W / {n-wins}L)   [pnl>0, sama dengan log live]")
    L.append(f"Hasil per tipe: " + ", ".join(f"{k} {v}" for k, v in sorted(ct.items())))
    L.append(f"WR ketat      : {strict_wr:.1f}%  (TP2 vs SL saja, BE diabaikan)")
    L.append(f"Total PnL     : {df['pnl_usdt'].sum():+,.2f} USDT  ({df['pnl_usdt'].sum()/args.balance*100:+.2f}%)")
    L.append(f"Profit factor : {pf:.2f} | Expectancy: {exp_r:+.2f}R/trade")
    L.append(f"Max drawdown  : {dd:,.2f} USDT ({dd/args.balance*100:.2f}%) | Rugi beruntun maks: {streak}")
    L.append("─" * 38)
    df["bucket"] = df["score"].astype(float).apply(bot._score_bucket)
    L.append("Winrate per score:")
    for b, g in df.groupby("bucket"):
        L.append(f"  {b:>6}: {g['is_win'].mean()*100:5.1f}%  n={len(g):<3} pnl {g['pnl_usdt'].sum():+,.1f}")
    L.append("Per arah:")
    for d, g in df.groupby("direction"):
        L.append(f"  {d:>5}: {g['is_win'].mean()*100:5.1f}%  n={len(g):<3} pnl {g['pnl_usdt'].sum():+,.1f}")
    by_pair = df.groupby("symbol")["pnl_usdt"].agg(["sum", "count"]).sort_values("sum")
    L.append("Pair terburuk: " + ", ".join(f"{i} {r['sum']:+.0f}({int(r['count'])})" for i, r in by_pair.head(3).iterrows()))
    L.append("Pair terbaik : " + ", ".join(f"{i} {r['sum']:+.0f}({int(r['count'])})" for i, r in by_pair.tail(3)[::-1].iterrows()))
    if errs:
        L.append("─" * 38)
        L.append("⚠️ Error analisa: " + "; ".join(f"{k} (x{v})" for k, v in errs.most_common(3)))
    L.append("─" * 38)
    _sd_funnel(L, args)
    L.append("Catatan: hanya candle close dipakai; fee/slippage/minNotional disederhanakan.")
    summary.update({"wins": wins, "winrate": round(wins / n * 100, 2), "pnl": round(float(df['pnl_usdt'].sum()), 2),
                    "profit_factor": None if math.isinf(pf) else round(pf, 3),
                    "expectancy_r": round(float(exp_r), 3), "max_drawdown": round(float(dd), 2),
                    "close_types": ct})
    return "\n".join(L), summary


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════════════════════
def _norm_symbol(p):
    p = str(p).strip().upper()
    if not p:
        return ""
    return p if p.endswith(("USDT", "USDC", "BUSD")) else p + "USDT"


def parse_pairs(spec, base):
    if isinstance(spec, (list, tuple, set)):
        out = [_norm_symbol(p) for p in spec]
        return [p for p in dict.fromkeys(out) if p]
    if isinstance(spec, int):
        return list(base)[:spec]
    spec = str(spec).strip()
    if spec.lower() == "all":
        return list(base)
    if spec.isdigit():
        return list(base)[: int(spec)]
    out = [_norm_symbol(p) for p in spec.split(",")]
    return [p for p in dict.fromkeys(out) if p]


def main():
    global STORE, SIM, FIELDS
    ap = argparse.ArgumentParser(description="Backtest bot SMC (memakai analyze_pair asli)")
    ap.add_argument("--days", type=int, default=DAYS)
    ap.add_argument("--end", default=None, help="tanggal AKHIR periode (UTC), mis. 2026-10-12 atau \"2026-10-12 08:00\"; default: sekarang")
    ap.add_argument("--pairs", default=None, help="all | N (N pair pertama) | A,B,C  (default: PAIRS di atas file)")
    ap.add_argument("--balance", type=float, default=BALANCE)
    ap.add_argument("--risk", type=float, default=(RISK_PCT if RISK_PCT is not None else bot.MAX_SL_LOSS_PCT * 100), help="risk %% per trade")
    ap.add_argument("--max-open", type=int, default=MAX_OPEN)
    ap.add_argument("--single-tp", type=float, default=0.0,
                    help="EKSPERIMEN: satu TP di X R (mis. 1.5), tanpa partial/BE/trailing. 0 = off")
    ap.add_argument("--ema-pullback", choices=["off", "gate"], default="off",
                    help="gate = hanya entry saat pullback ke EMA20/50 searah tren (main.EMA_PULLBACK_MODE)")
    ap.add_argument("--daily-loss-pct", type=float, default=0.0,
                    help="stop scan sisa hari UTC bila PnL harian <= -X%% modal awal (0 = off)")
    ap.add_argument("--daily-win-pct", type=float, default=0.0,
                    help="stop scan sisa hari UTC bila PnL harian >= +X%% modal awal (0 = off)")
    ap.add_argument("--struct-filter", type=int, default=0,
                    help="1 = nyalakan main.STRUCT_FILTER_LTF (HH+HL / LH+LL, belum CHoCH di entry TF). 0 = ikut file")
    ap.add_argument("--sl-buffer", type=float, default=0.0,
                    help="override main.SL_BUFFER_ATR_MULT (buffer SL di luar zona, x ATR; mis. 0.3). 0 = ikut file")
    ap.add_argument("--retest-zone", type=int, default=0,
                    help="main.HTF_RETEST_ZONE_MODE: 0 = ikut file | 1 = hanya zona retest HTF | 2 = OB/FVG 30m atau zona retest HTF")
    ap.add_argument("--zone-src", type=int, default=-1,
                    help="main.HTF_RETEST_ZONE_SRC: 0 = band level swing 1h | 1 = OB gaya LuxAlgo 1h | 2 = OB gaya LuxAlgo 30m | -1 = ikut file")
    ap.add_argument("--choch-entry", type=int, default=-1,
                    help="main.DZ_CHOCH_ENTRY: 1 = CHoCH bullish 1h + demand di bawahnya, langsung entry di zona | 0 = off | -1 = ikut file")
    ap.add_argument("--dz-disp", type=float, default=-1.0,
                    help="main.DZ_MIN_DISP_ATR: demand kuat = reli sesudah OB >= N x ATR TF zona (0 = tanpa filter, -1 = ikut file)")
    ap.add_argument("--dz-buf", type=float, default=-1.0,
                    help="main.DZ_SL_BUF_ATR: ruang SL di bawah wick/dasar zona demand (x ATR TF zona, -1 = ikut file)")
    ap.add_argument("--demand-long", type=int, default=-1,
                    help="main.HTF_DEMAND_MODE: 1 = LONG di demand 1D + reaksi 30m | 2 = demand 1W | 0 = off | -1 = ikut file")
    ap.add_argument("--regime-aligned", type=int, default=-1,
                    help="main.REGIME_REQUIRE_ALIGNED: 1 = entry hanya kalau regime searah (LONG di BULL_REGIME, SHORT di BEAR_REGIME) | 0 = off | -1 = ikut file")
    ap.add_argument("--zone-pick", type=int, default=-1,
                    help="main.RZ_OB_PICK: 0 = OB terdekat | 1 = OB asal impuls | -1 = ikut file")
    ap.add_argument("--research", type=int, default=-1,
                    help="-1 = ikut file | 0 = gate lama aktif | 1 = MODE RISET (gate lama mati, hanya untuk backtest)")
    ap.add_argument("--weekly-loss-pct", type=float, default=0.0,
                    help="stop scan sisa minggu (ISO) bila PnL mingguan <= -X%% modal awal (0 = off)")
    ap.add_argument("--monthly-loss-pct", type=float, default=0.0,
                    help="stop scan sisa bulan bila PnL bulanan <= -X%% modal awal (0 = off)")
    ap.add_argument("--monthly-win-pct", type=float, default=0.0,
                    help="stop scan sisa bulan bila PnL bulanan >= +X%% modal awal (0 = off)")
    ap.add_argument("--max-trades-day", type=int, default=0,
                    help="maks order dibuka per hari UTC (0 = off)")
    ap.add_argument("--fee", type=float, default=FEE_PCT, help="fee %% per sisi")
    ap.add_argument("--max-sl-pct", type=float, default=None,
                    help="gate jarak SL maks %% dari entry (default: ikut bot MAX_SL_ENTRY_PCT; 0 = nonaktif)")
    ap.add_argument("--strategy", choices=["default", "smc2", "sd", "ob", "htf", "fund"], default="default",
                    help="default = analyze_pair bot | smc2 = sweep→MSS→FVG/OB→target liquidity (smc2.py) | sd = demand/supply zone 1D + konfirmasi 1H (sd.py) | ob = sweep→BOS→limit di Order Block, TP 0.8/3.5R (ob.py)")
    ap.add_argument("--smc2-p", action="append", default=[],
                    help="override param smc2, ulangi per param. mis. --smc2-p use_h4=1 --smc2-p use_btc=1 --smc2-p adx_min=20")
    ap.add_argument("--coinflip", action="store_true",
                    help="kontrol untuk --strategy smc2: waktu/SL/R sama, ARAH diundi")
    ap.add_argument("--rand-tp1-r", type=float, default=None, help="TP1 (R) untuk --random-entry (default ikut bot)")
    ap.add_argument("--rand-tp2-r", type=float, default=None, help="TP2 (R) untuk --random-entry (default ikut bot)")
    ap.add_argument("--random-entry", action="store_true",
                    help="BASELINE: entry acak (tanpa sinyal SMC), exit/fee/risk sama dengan bot")
    ap.add_argument("--seed", type=int, default=1, help="seed acak untuk --random-entry")
    ap.add_argument("--rand-prob", type=float, default=0.0015,
                    help="peluang entry acak per pair per scan (default 0.0015 ≈ laju trade bot ~2/hari/15 pair)")
    ap.add_argument("--rand-sl-atr", type=float, default=3.5,
                    help="SL = N x ATR TF HTF untuk --random-entry (default 3.5, mirip rata-rata bot)")
    ap.add_argument("--be-at-r", type=float, default=0.0,
                    help="EKSPERIMEN: geser SL ke breakeven (+buffer fee) saat profit menyentuh X x R. 0 = mati")
    ap.add_argument("--flip", action="store_true",
                    help="EKSPERIMEN: balik arah sinyal, TP asli jadi SL dan SL asli jadi TP")
    ap.add_argument("--scan-every", type=int, default=SCAN_EVERY, help="menit, kelipatan 15")
    ap.add_argument("--out", default=OUT_DIR)
    ap.add_argument("--tag", default="")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--selftest", action="store_true", help="data sintetis, tanpa internet")
    args = ap.parse_args()
    if args.ema_pullback == "gate":
        bot.EMA_PULLBACK_MODE = "GATE"
    global FLIP, EXP_BE_AT_R
    if args.struct_filter:
        bot.STRUCT_FILTER_LTF = True
    if args.retest_zone:
        bot.HTF_RETEST_ZONE_MODE = int(args.retest_zone)
    if args.research >= 0:
        bot.RESEARCH_MODE = bool(args.research)
    if args.zone_src >= 0:
        bot.HTF_RETEST_ZONE_SRC = int(args.zone_src)
    if args.zone_pick >= 0:
        bot.RZ_OB_PICK = int(args.zone_pick)
    if args.regime_aligned >= 0:
        bot.REGIME_REQUIRE_ALIGNED = bool(args.regime_aligned)
    if args.demand_long >= 0:
        bot.HTF_DEMAND_MODE = int(args.demand_long)
    if args.choch_entry >= 0:
        bot.DZ_CHOCH_ENTRY = bool(args.choch_entry)
    if args.dz_disp >= 0:
        bot.DZ_MIN_DISP_ATR = float(args.dz_disp)
    if args.dz_buf >= 0:
        bot.DZ_SL_BUF_ATR = float(args.dz_buf)
    if args.sl_buffer and args.sl_buffer > 0:
        bot.SL_BUFFER_ATR_MULT = float(args.sl_buffer)
    if args.be_at_r and args.be_at_r > 0:
        EXP_BE_AT_R = float(args.be_at_r)
    if args.flip:
        FLIP = True
    if args.single_tp > 0:
        global EXP_SINGLE_TP
        EXP_SINGLE_TP = True
        bot.TP1_R_MULT = args.single_tp
        bot.RR_GRADE_B = args.single_tp
        bot.SINGLE_TP_R_MULT = args.single_tp   # v75: TP tunggal dihitung dari konstanta ini

    if args.max_sl_pct is not None:
        bot.MAX_SL_ENTRY_PCT = max(0.0, args.max_sl_pct) / 100.0

    args.days = max(1, min(args.days, 90))
    args.scan_every = max(15, (args.scan_every // 15) * 15)
    out_dir = os.path.abspath(args.out or os.path.join(HERE, "backtests"))
    os.makedirs(out_dir, exist_ok=True)

    end_ms = int(_real_time.time() * 1000) // TF_MS["15m"] * TF_MS["15m"]
    if args.end:
        _fmt = "%Y-%m-%d %H:%M" if ":" in args.end else "%Y-%m-%d"
        try:
            _e = _RealDT.strptime(args.end.strip(), _fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            sys.exit(f"❌ Format --end salah: {args.end!r}. Pakai YYYY-MM-DD atau \"YYYY-MM-DD HH:MM\" (UTC)")
        _e_ms = int(_e.timestamp() * 1000) // TF_MS["15m"] * TF_MS["15m"]
        if _e_ms > end_ms:
            sys.exit("❌ --end tidak boleh di masa depan")
        end_ms = _e_ms
    start_ms = end_ms - args.days * 86_400_000
    args.start_str = _RealDT.fromtimestamp(start_ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M")
    args.end_str = _RealDT.fromtimestamp(end_ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    args.run_id = (_RealDT.fromtimestamp(end_ms / 1000, timezone.utc).strftime("%Y%m%d_%H%M")
                   + (f"_{args.tag}" if args.tag else "") + ("_selftest" if args.selftest else "")
                   + (f"_rand{args.seed}" if args.random_entry else "")
                   + (("_smc2flip" if args.coinflip else "_smc2") if args.strategy == "smc2" else "")
                   + (("_sdflip" if args.coinflip else "_sd") if args.strategy == "sd" else "")
                   + (("_obflip" if args.coinflip else "_ob") if args.strategy == "ob" else "")
                   + (("_htfflip" if args.coinflip else "_htf") if args.strategy == "htf" else "")
                   + (("_fundflip" if args.coinflip else "_fund") if args.strategy == "fund" else ""))

    pairs_spec = args.pairs if args.pairs is not None else PAIRS
    pairs = parse_pairs(pairs_spec, bot.BASE_PAIRS)
    if args.selftest:
        pairs = pairs[:6] if str(pairs_spec).lower() == "all" else pairs
    if args.max_open is None:
        args.max_open = bot.MAX_OPEN_TRADES
        for min_bal, mt in getattr(bot, "DYNAMIC_TRADE_TIERS", []):
            if args.balance >= min_bal:
                args.max_open = mt

    cache_dir = os.path.join(out_dir, "_cache")
    STORE = (SynthStore if args.selftest else Store)(start_ms, end_ms, cache_dir)
    SIM = Sim(args)
    FIELDS = _csv_fields()
    install_patches()
    modes = bot.get_active_modes()

    csv_path = os.path.join(out_dir, f"backtest_log_{args.run_id}.csv")
    SIM.fh = open(csv_path, "w", newline="", encoding="utf-8")
    SIM.writer = csv.DictWriter(SIM.fh, fieldnames=FIELDS, extrasaction="ignore")
    SIM.writer.writeheader()

    print(f"⚙️  Setting: pair={len(pairs)} ({', '.join(pairs[:6])}{'…' if len(pairs) > 6 else ''}) | "
          f"modal={args.balance:,.0f} USDT | max posisi={args.max_open} | risk={args.risk}% | "
          f"hari={args.days}", flush=True)
    print(f"📁 Hasil akan disimpan di: {out_dir}", flush=True)
    print(f"🧪 Backtest {args.run_id} | {args.days} hari | {len(pairs)} pair | "
          f"mode {[m['label'] for m in modes]} | max_open {args.max_open}", flush=True)

    # Pra-unduh data pair (supaya progres jelas & gagal-cepat)
    if not args.selftest:
        need = {m["htf_tf"] for m in modes} | {m["entry_tf"] for m in modes}
        keep = []
        for i, p in enumerate(pairs, 1):
            ok = all(STORE.get(p, tf) is not None for tf in need)
            if ok:
                keep.append(p)
            if i % 10 == 0 or i == len(pairs):
                print(f"PROGRESS_DL {i}/{len(pairs)}", flush=True)
        pairs = keep
        for sym in ("BTCUSDT", getattr(bot, "BTCDOM_TICKER", "BTCDOMUSDT")):
            for tf in ("15m", "1h"):
                STORE.get(sym, tf)
        if not pairs:
            print("❌ Tidak ada data yang berhasil diunduh (cek koneksi / IP diblokir Binance).", flush=True)
            sys.exit(2)

    step = TF_MS["15m"]
    total_steps = (end_ms - start_ms) // step
    scan_every_steps = args.scan_every // 15
    t0 = _real_time.time()
    last_pct = -1
    _orig_ap = bot.analyze_pair

    def _counting_ap(*a, **k):
        SKIPS.calls += 1
        _sc = k.get("signal_candidates")
        _n0 = len(_sc) if _sc is not None else 0
        r = _orig_ap(*a, **k)
        if _sc is not None and len(_sc) > _n0:
            SKIPS.signals += 1
        return r
    bot.analyze_pair = _counting_ap
    try:
        for i in range(1, total_steps + 1):
            t_ms = start_ms + i * step
            _Clock.set_ms(t_ms)
            manage_step(t_ms)
            if i % scan_every_steps == 0 and (len(SIM.open) + len(SIM.pending)) < args.max_open and _day_ok():
                _reset_caches()
                sink = contextlib.nullcontext() if args.verbose else contextlib.redirect_stdout(SKIPS)
                with sink:
                    if args.random_entry:
                        scan_random(pairs, modes, _Clock.dt())
                    elif args.strategy in ("smc2", "sd", "ob", "htf", "fund"):
                        scan_smc2(pairs, modes, _Clock.dt())
                    else:
                        scan(pairs, modes, _Clock.dt())
            pct = int(i / total_steps * 100)
            if pct != last_pct and pct % 2 == 0:
                last_pct = pct
                el = _real_time.time() - t0
                eta = el / i * (total_steps - i)
                print(f"PROGRESS {pct} trades={len(SIM.rows)} open={len(SIM.open)} pending={len(SIM.pending)} "
                      f"eta={int(eta//60)}m", flush=True)
    except KeyboardInterrupt:
        print("\n⏹️  Dihentikan manual — hasil sementara tetap disimpan.", flush=True)

    n_open_end = len(SIM.open)
    SIM.fh.close()
    text, summary = summarize(SIM.rows, args, pairs, total_steps, n_open_end, SIM.errs)
    text = text + "\n" + SKIPS.report()
    if getattr(bot, "HTF_DEMAND_MODE", 0) or getattr(bot, "DZ_CHOCH_ENTRY", False):
        _z = getattr(bot, "DZ_STATS", {})
        text += ("\n\nDIAGNOSA DEMAND-LONG (mode %s):\n  dicek (tiap analisa)       : %d\n  data zona gagal/kurang     : %d\n"
                 "  tidak ada OB bullish       : %d\n  demand lemah (reli < ambang): %d\n  zona kesentuh + di atas    : %d\n"
                 "  zona bertahan              : %d\n  ada reaksi buyer           : %d\n  candle 1h terakhir bullish : %d\n  jadi sinyal                : %d"
                 % (bot.HTF_DEMAND_MODE, _z.get("checks", 0), _z.get("fetch_fail", 0), _z.get("no_bull_ob", 0), _z.get("weak", 0), _z.get("touched", 0), _z.get("held", 0), _z.get("reacted", 0), _z.get("trigger", 0), _z.get("signals", 0)))
        text += "\n  [CHoCH+demand] ditemukan: %d | jadi sinyal: %d" % (_z.get("choch_found", 0), _z.get("signals_choch", 0))
    bot.analyze_pair = _orig_ap
    single = bool(SINGLE_FILE) and not args.tag     # run dari Telegram (--tag tg) tetap 3 file
    if single:
        sp = os.path.join(out_dir, f"backtest_{args.run_id}.txt")
        _write_single_file(sp, text, SIM.rows)
        try:
            os.remove(csv_path)                      # CSV sementara (dipakai saat run berjalan)
        except OSError:
            pass
        print("\n" + text, flush=True)
        print(f"RESULT_FILE {sp}", flush=True)
        print(f"\n✅ Selesai. Hasil (1 file saja):\n   {sp}", flush=True)
    else:
        sp = os.path.join(out_dir, f"backtest_summary_{args.run_id}.txt")
        with open(sp, "w", encoding="utf-8") as f:
            f.write(text + "\n")
        with open(os.path.join(out_dir, f"backtest_summary_{args.run_id}.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False, default=str)
        print("\n" + text, flush=True)
        print(f"CSV_FILE {csv_path}", flush=True)
        print(f"SUMMARY_FILE {sp}", flush=True)
        print(f"\n✅ Selesai. File hasil ada di folder:\n   {out_dir}", flush=True)
    if OPEN_FOLDER_AT_END:
        _open_folder(out_dir)


# Kolom yang dipertahankan di file tunggal (cukup untuk analisis; kolom meta/kosong dibuang)
SLIM_COLS = ["ts", "bt_signal_ts", "symbol", "direction", "entry", "close_price", "sl", "tp1", "tp2",
             "lot", "pnl_usdt", "close_type", "is_win", "score", "rr_actual", "atr_pct", "sl_atr_mult",
             "vol_rat", "htf_bias", "bos_strength", "engulf_break", "engulf_entry", "engulf_note",
             "fvg_retest", "fvg_retest_note", "stoch_div", "stoch_weak", "stoch_note", "sweep_strength", "sl_source", "rsi_entry",
             "mae_pct", "mfe_pct", "duration_sec", "btc_bias", "btcd_trend", "btc_ltf_dir", "mkt_regime", "alt_bull_pct", "alt_bear_pct",
             "adx_htf", "di_gap_htf", "ema_gap_htf", "htf_struct_ok", "htf_er"]
TRADES_MARKER = "### TRADES_CSV"


def _slim_val(v):
    if isinstance(v, float):
        return f"{v:.7g}"
    return v


# ── DIAGNOSA SINYAL: hitung alasan skip per gate (2026-11-17) ──────────────────────────────
class _SkipSink:
    """Pengganti stdout saat scan: menghitung baris '⛔ ... skip' per alasan (angka/pair dinormalisasi)."""
    KEYS = ("⛔", "skip", "Skip", "SKIP", "diblok", "ditolak", "tidak lolos", "BLOCK")

    def __init__(self):
        self.buf = ""
        self.c = Counter()
        self.calls = 0      # jumlah analyze_pair dipanggil
        self.signals = 0    # yang menghasilkan kandidat sinyal

    def write(self, s):
        self.buf += s
        while "\n" in self.buf:
            line, self.buf = self.buf.split("\n", 1)
            self._add(line)
        return len(s)

    def flush(self):
        pass

    def _add(self, line):
        l = line.strip()
        if not l or not any(k in l for k in self.KEYS):
            return
        import re as _re
        l = _re.sub(r"\[[^\]]*\]", "", l)
        l = _re.sub(r"\b[A-Z0-9]{2,15}USDT\b", "<P>", l)
        l = _re.sub(r"-?\d+(?:\.\d+)?", "#", l)
        l = _re.sub(r"\s+", " ", l).strip()[:110]
        self.c[l] += 1

    def report(self, top=25):
        printed = sum(self.c.values())
        silent = max(0, self.calls - self.signals - printed)
        L = ["", "=" * 60, "DIAGNOSA SINYAL (kenapa trade sedikit)", "=" * 60,
             f"analyze_pair dipanggil : {self.calls}",
             f"menghasilkan sinyal    : {self.signals}",
             f"skip berlabel (print)  : {printed}",
             f"return tanpa label     : ~{silent}  (gate yang tidak nge-print / data kurang)", "",
             "Top alasan skip:"]
        for k, v in self.c.most_common(top):
            L.append(f"  {v:>7}  {k}")
        return "\n".join(L)


SKIPS = _SkipSink()


def _write_single_file(path, text, rows):
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(SLIM_COLS)
    for r in rows:
        w.writerow([_slim_val(r.get(c, "")) for c in SLIM_COLS])
    with open(path, "w", encoding="utf-8") as f:
        f.write(text.rstrip() + "\n\n")
        f.write(TRADES_MARKER + "  (baris setelah ini = CSV: header lalu 1 baris per trade)\n")
        f.write(buf.getvalue())


def _open_folder(path):
    """Buka folder hasil di file manager (diabaikan kalau tidak ada GUI, mis. Railway/SSH)."""
    try:
        if sys.platform.startswith("win"):
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            import subprocess
            subprocess.Popen(["open", path])
        elif os.environ.get("DISPLAY"):
            import subprocess
            subprocess.Popen(["xdg-open", path])
    except Exception:  # noqa: BLE001
        pass


def _pause():
    # Hanya tahan window kalau dijalankan langsung di terminal interaktif.
    # Kalau dipanggil bot (/backtest di Telegram) atau background, JANGAN menunggu
    # input — kalau tidak, proses bisa menggantung selamanya.
    if not PAUSE_AT_END or not sys.stdin or not sys.stdin.isatty():
        return
    try:
        input("\nTekan ENTER untuk keluar...")
    except (EOFError, OSError, ValueError):
        pass  # non-interaktif (nohup/screen/Docker) — lewati


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass
    except Exception:  # noqa: BLE001
        import traceback
        print("\n❌ Backtest error:", flush=True)
        traceback.print_exc()
    finally:
        _pause()
