#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zone_study.py — STUDI REAKSI ZONA (Order Block / FVG)

Pertanyaan yang dijawab: "kalau harga MASUK ke zona OB / FVG, kebanyakan reaksinya apa,
dan RR berapa yang paling masuk akal?"  Ini BUKAN backtest strategi. Ini studi kejadian:
tiap zona dicatat SEKALI, pada sentuhan pertama setelah zona terbentuk (tanpa mengintip masa depan),
lalu dilihat apa yang terjadi sesudahnya.

Yang dicatat per kejadian
  • Reaksi: tembus langsung / pantul tipis lalu tembus / pantul >=1R lalu tembus / bertahan & naik / mandek
  • Seberapa dalam harga masuk zona, seberapa jauh pantulannya (MFE), kapan SL kena
  • Hasil trade kalau masuk dengan 2 cara:
      A = langsung masuk di close candle sentuh
      B = seperti bot sekarang: tunggu reaksi (ekor panjang / pantulan) + candle searah
  • RR 1 / 1.5 / 2 / 3 / 4: winrate dan expectancy (sudah dikurangi fee), SL di balik tepi zona + buffer ATR

Zona
  • OB  = Order Block internal gaya LuxAlgo (sama dengan yang dipakai bot: _lux_order_blocks), dihitung bar demi bar
  • FVG = Fair Value Gap gaya LuxAlgo (threshold otomatis), dihitung bar demi bar
  • OB+FVG = OB yang tumpang tindih dengan FVG searah yang masih hidup (konfluensi)

Cara pakai
    python zone_study.py --days 60 --pairs 15 --zone-tf 1h
    python zone_study.py --days 90 --pairs all --zone-tf 1h,4h --end 2026-11-20
    python zone_study.py --selftest            # tes alur pakai data acak (tanpa internet)
Lewat Telegram:  /zonestudy 60 15 1h
"""
import argparse
import ast
import csv
import math
import os
import sys
import time as _time
from datetime import datetime, timezone

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

# ═════════════════════════════════════════════════════════════════════════════
#  SETTING (bisa ditimpa argumen CLI)
# ═════════════════════════════════════════════════════════════════════════════
ZS_VERSION = "zs2-rezim-lambat"
ZS_LAST_EDIT = "2026-11-27 14:14 WIB"           # tanggal & jam edit terakhir (WIB = UTC+7)
ZS_EDITED_BY = "Claude - email akun: BELUM_DIISI"

PAIRS = "15"
DAYS = 60                # berapa hari yang dipelajari (maks 365)
ZONE_TFS = "1h"          # TF tempat zona dihitung (boleh beberapa: 1h,4h)
ENTRY_TF = "30m"         # TF candle yang menyentuh zona & dipakai simulasi
HORIZON = 48             # berapa candle ENTRY_TF setelah entry dipantau (48 x 30m = 24 jam)
SL_BUF_ATR = 0.3         # SL = tepi zona jauh + buffer ini x ATR (sama dengan RZ_SL_BUF_ATR bot)
RR_LIST = (1.0, 1.5, 2.0, 3.0, 4.0)
FEE_PCT = 0.04           # fee per sisi (%)
CONFIRM_BARS = 16        # cara B: batas candle setelah sentuh untuk menunggu reaksi (sama dengan DZ_TOUCH_BARS bot)
WICK_MIN = 0.30          # cara B: ekor >= 30% dari candle
BOUNCE_ATR = 0.30        # cara B: atau pantul >= 0.3 x ATR dari titik terjauh
MIN_RANGE_ATR = 0.50     # cara B: candle penilai ekor minimal 0.5 x ATR
OB_SIZE = 5              # internal structure size (RZ_OB_SIZE bot)
ENTRY_WARM_DAYS = 10     # hari sebelum periode untuk membiarkan zona terbentuk
ZONE_WARM_CANDLES = 420  # candle TF zona sebelum periode (pemanasan ATR200 / pivot)
SLOW_WARM_DAYS = 120     # hari data harian sebelum periode untuk tren lambat (EMA20/EMA50 harian, return 30 hari)
MIN_EVENTS = 30          # di bawah ini dianggap sampel kecil (tabel tetap ditampilkan)
MIN_OK_EVENTS = 60       # untuk lolos "konsisten": minimal kejadian (tiap paruh periode >= 30)
OOS_T_MIN = 2.5          # t-stat minimal di paruh 2 (RR dipilih di paruh 1) -> koreksi kasar untuk banyak kombinasi yang diuji
PAIRS_POS_MIN = 0.6      # minimal porsi pair yang positif (bila >= 5 pair punya >= 5 kejadian)
MAX_DAYS = 365

TF_MS = {"5m": 300_000, "15m": 900_000, "30m": 1_800_000, "1h": 3_600_000,
         "2h": 7_200_000, "4h": 14_400_000, "1d": 86_400_000}
DAY_MS = 86_400_000


# ═════════════════════════════════════════════════════════════════════════════
#  DATA
# ═════════════════════════════════════════════════════════════════════════════
def _load_bot():
    for name in ("main", "main_demand"):
        try:
            return __import__(name)
        except Exception:  # noqa: BLE001
            continue
    return None


def _base_pairs_fallback():
    for fn in ("main.py", "main_demand.py"):
        p = os.path.join(HERE, fn)
        if os.path.exists(p):
            try:
                tree = ast.parse(open(p, encoding="utf-8").read())
                for node in tree.body:
                    if isinstance(node, ast.Assign) and getattr(node.targets[0], "id", "") == "BASE_PAIRS":
                        return list(ast.literal_eval(node.value))
            except Exception:  # noqa: BLE001
                pass
    return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "BNBUSDT", "DOGEUSDT", "ADAUSDT"]


def parse_pairs(spec, base):
    spec = str(spec).strip()
    if spec.lower() == "all":
        return list(base)
    if spec.isdigit():
        return list(base)[: max(1, int(spec))]
    out = []
    for x in spec.replace(" ", ",").split(","):
        x = x.strip().upper()
        if not x:
            continue
        if not x.endswith("USDT"):
            x += "USDT"
            if x not in base and "1000" + x in base:
                x = "1000" + x
        out.append(x)
    return out


class Provider:
    """Unduh klines Binance Futures + cache CSV (folder _cache di sebelah file hasil)."""

    def __init__(self, cache_dir, bot=None):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.base_url = getattr(bot, "MARKET_DATA_BASE_URL", "https://fapi.binance.com") if bot else "https://fapi.binance.com"
        try:
            self.headers = bot.get_headers() if bot else {}
        except Exception:  # noqa: BLE001
            self.headers = {}
        self.mem = {}

    def _download(self, symbol, tf, from_ms, to_ms):
        import requests
        url = f"{self.base_url}/fapi/v1/klines"
        rows, cur, fails = [], from_ms, 0
        while cur < to_ms:
            try:
                r = requests.get(url, params={"symbol": symbol, "interval": tf, "startTime": cur,
                                              "endTime": to_ms, "limit": 1500}, headers=self.headers, timeout=20)
                if r.status_code in (418, 429):
                    _time.sleep(min(60, 10 * (fails + 1)))
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
            _time.sleep(0.12)
        if not rows:
            return None
        df = pd.DataFrame(rows, columns=["open_ms", "open", "high", "low", "close", "volume", "close_time",
                                         "qav", "num_trades", "tbb", "tbq", "ignore"])
        return df[["open_ms", "open", "high", "low", "close", "volume"]]

    def get(self, symbol, tf, from_ms, to_ms):
        key = (symbol, tf, from_ms, to_ms)
        if key in self.mem:
            return self.mem[key]
        d0 = datetime.fromtimestamp(from_ms / 1000, timezone.utc).strftime("%Y%m%d")
        d1 = datetime.fromtimestamp(to_ms / 1000, timezone.utc).strftime("%Y%m%d")
        path = os.path.join(self.cache_dir, f"zs_{symbol}_{tf}_{d0}_{d1}.csv")
        df = None
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
            except Exception:  # noqa: BLE001
                df = None
        if df is None or not len(df):
            df = self._download(symbol, tf, from_ms, to_ms)
            if df is not None:
                df.to_csv(path, index=False)
        if df is None:
            return None
        df = df.copy()
        for c in ("open", "high", "low", "close", "volume"):
            df[c] = df[c].astype(float)
        df["open_ms"] = df["open_ms"].astype("int64")
        df = df.drop_duplicates("open_ms").sort_values("open_ms").reset_index(drop=True)
        df = df[df["open_ms"] + TF_MS[tf] <= to_ms].reset_index(drop=True)   # hanya candle yang sudah close
        self.mem[key] = df
        return df


# ═════════════════════════════════════════════════════════════════════════════
#  INDIKATOR
# ═════════════════════════════════════════════════════════════════════════════
def atr_wilder(h, l, c, n=14):
    prev = np.r_[c[0], c[:-1]]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev), np.abs(l - prev)))
    atr = np.full(len(c), np.nan)
    if len(c) >= n:
        atr[n - 1] = tr[:n].mean()
        for i in range(n, len(c)):
            atr[i] = (atr[i - 1] * (n - 1) + tr[i]) / n
    return atr


def trend_up(closes):
    s = pd.Series(closes)
    return (s.ewm(span=50, adjust=False).mean() > s.ewm(span=200, adjust=False).mean()).values


def slow_flags(ddf):
    """Tren LAMBAT dari candle harian (hanya candle yang sudah close). Kembalikan (waktu_close_ms, {nama: (naik, valid)})."""
    c = ddf["close"].values.astype(float)
    s_ = pd.Series(c)
    up = (s_.ewm(span=20, adjust=False).mean() > s_.ewm(span=50, adjust=False).mean()).values
    ok_up = np.arange(len(c)) >= 50                      # EMA50 baru layak setelah 50 candle
    roc = np.full(len(c), np.nan)
    if len(c) > 30:
        roc[30:] = c[30:] / c[:-30] - 1.0
    roc_up = np.where(np.isnan(roc), False, roc > 0)
    ok_roc = ~np.isnan(roc)
    cms = ddf["open_ms"].values.astype("int64") + TF_MS["1d"]
    return cms, {"d": (up, ok_up), "roc": (roc_up, ok_roc)}


def _ctx_slow(cms, flags, ok, t_ms):
    j = int(np.searchsorted(cms, t_ms, side="right")) - 1
    if j < 0 or not bool(ok[j]):
        return None
    return bool(flags[j])


# ═════════════════════════════════════════════════════════════════════════════
#  ZONA (bar demi bar, tanpa mengintip masa depan)
# ═════════════════════════════════════════════════════════════════════════════
def detect_ob(df, size=OB_SIZE, swing_size=50, atr_len=200):
    """Port _lux_order_blocks bot, tapi mencatat kapan zona terbentuk (idx) dan kapan termitigasi (imit).
    side=+1 demand (OB bullish), side=-1 supply (OB bearish)."""
    n = len(df)
    if n < size + 5:
        return []
    H = df["high"].astype(float).values
    L = df["low"].astype(float).values
    C = df["close"].astype(float).values
    prev_c = np.r_[C[0], C[:-1]]
    tr = np.maximum(H - L, np.maximum(np.abs(H - prev_c), np.abs(L - prev_c)))
    atr = np.full(n, np.nan)
    if n >= atr_len:
        atr[atr_len - 1] = tr[:atr_len].mean()
        for k in range(atr_len, n):
            atr[k] = (atr[k - 1] * (atr_len - 1) + tr[k]) / atr_len
    hv = np.where(np.isnan(atr), False, (H - L) >= 2 * np.nan_to_num(atr, nan=1e18))
    PH = np.where(hv, L, H)
    PL = np.where(hv, H, L)

    def pivots(sz):
        ev = [(0, None, None)] * n
        leg = 0
        for t in range(sz, n):
            hi_ = H[t - sz] > H[t - sz + 1:t + 1].max() if sz > 0 else False
            lo_ = L[t - sz] < L[t - sz + 1:t + 1].min() if sz > 0 else False
            new = leg
            if hi_:
                new = 0
            elif lo_:
                new = 1
            if new != leg:
                ev[t] = (1, L[t - sz], t - sz) if new == 1 else (-1, H[t - sz], t - sz)
            leg = new
        return ev

    ev_i = pivots(size)
    ev_s = pivots(swing_size)
    ih = il = sh = sl_ = None
    ih_cr = il_cr = False
    ih_idx = il_idx = None
    prev_ih = prev_il = None
    zones, alive = [], []
    for t in range(n):
        e, lvl, _ = ev_s[t]
        if e == -1:
            sh = lvl
        elif e == 1:
            sl_ = lvl
        e, lvl, idx = ev_i[t]
        if e == -1:
            ih, ih_cr, ih_idx = lvl, False, idx
        elif e == 1:
            il, il_cr, il_idx = lvl, False, idx
        if t > 0:
            if ih is not None and prev_ih is not None and C[t] > ih and C[t - 1] <= prev_ih and not ih_cr and ih != sh:
                ih_cr = True
                seg = PL[ih_idx:t]
                if len(seg):
                    j = ih_idx + int(np.argmin(seg))
                    z = {"type": "OB", "side": 1, "lo": float(min(PH[j], PL[j])), "hi": float(max(PH[j], PL[j])), "idx": t, "imit": None}
                    zones.append(z)
                    alive.append(z)
            if il is not None and prev_il is not None and C[t] < il and C[t - 1] >= prev_il and not il_cr and il != sl_:
                il_cr = True
                seg = PH[il_idx:t]
                if len(seg):
                    j = il_idx + int(np.argmax(seg))
                    z = {"type": "OB", "side": -1, "lo": float(min(PH[j], PL[j])), "hi": float(max(PH[j], PL[j])), "idx": t, "imit": None}
                    zones.append(z)
                    alive.append(z)
        for z in list(alive):
            if (z["side"] == -1 and H[t] > z["hi"]) or (z["side"] == 1 and L[t] < z["lo"]):
                z["imit"] = t
                alive.remove(z)
        prev_ih, prev_il = ih, il
    return zones


def detect_fvg(df):
    """FVG gaya LuxAlgo (threshold otomatis = 2 x rata-rata |delta candle| sampai bar itu), bar demi bar."""
    n = len(df)
    if n < 5:
        return []
    O = df["open"].astype(float).values
    H = df["high"].astype(float).values
    L = df["low"].astype(float).values
    C = df["close"].astype(float).values
    delta = (C - O) / np.where(O == 0, 1.0, O)
    thr = 2 * np.cumsum(np.abs(delta)) / np.arange(1, n + 1)
    zones, alive = [], []
    for i in range(2, n):
        d = delta[i - 1]
        z = None
        if L[i] > H[i - 2] and C[i - 1] > H[i - 2] and d > thr[i]:
            z = {"type": "FVG", "side": 1, "lo": float(H[i - 2]), "hi": float(L[i]), "idx": i, "imit": None}
        elif H[i] < L[i - 2] and C[i - 1] < L[i - 2] and -d > thr[i]:
            z = {"type": "FVG", "side": -1, "lo": float(H[i]), "hi": float(L[i - 2]), "idx": i, "imit": None}
        if z is not None:
            zones.append(z)
            alive.append(z)
        for zz in list(alive):
            if (zz["side"] == 1 and L[i] < zz["lo"]) or (zz["side"] == -1 and H[i] > zz["hi"]):
                zz["imit"] = i
                alive.remove(zz)
    return zones


# ═════════════════════════════════════════════════════════════════════════════
#  SIMULASI HASIL
# ═════════════════════════════════════════════════════════════════════════════
def simulate(side, e, P, far, atr, el, eh, ec, n, horizon, buf, rr_list, fee_pct):
    """Entry di close candle e (harga P). SL = tepi zona jauh -/+ buf x ATR. Satu candle kena SL & TP -> SL duluan.
    Kembalikan None kalau tidak valid / data belum cukup."""
    if e + horizon > n - 1 or not (atr == atr) or atr <= 0:
        return None
    sl = far - side * buf * atr
    R = (P - sl) * side
    if R <= 0:
        return None
    last = e + horizon
    seg_l = el[e + 1:last + 1]
    seg_h = eh[e + 1:last + 1]
    if side == 1:
        adv = seg_l <= sl
        fav = (seg_h - P) / R
    else:
        adv = seg_h >= sl
        fav = (P - seg_l) / R
    jsl = int(np.argmax(adv)) if adv.any() else None
    upto = jsl if jsl is not None else len(fav)
    mfe = float(max(fav[:upto].max(), 0.0)) if upto > 0 else 0.0
    cost_r = 2 * fee_pct / 100.0 * P / R
    net, kind = {}, {}
    for k in rr_list:
        hit = fav >= k
        jtp = int(np.argmax(hit)) if hit.any() else None
        if jsl is not None and (jtp is None or jsl <= jtp):
            out, kd = -1.0, "SL"
        elif jtp is not None:
            out, kd = float(k), "TP"
        else:
            out, kd = float((ec[last] - P) * side / R), "EXP"
        net[k] = out - cost_r
        kind[k] = kd
    if jsl is not None:
        cat = "tembus_langsung" if mfe < 0.5 else ("pantul_tipis" if mfe < 1.0 else "pantul_lalu_tembus")
    else:
        cat = "bertahan_naik" if mfe >= 1.0 else "mandek"
    return {"R": R, "risk_pct": R / P * 100.0, "mfe": mfe, "cat": cat, "net": net, "kind": kind,
            "cost_r": cost_r, "sl_hit": jsl is not None}


def confirm_entry(side, k0, lo, hi, atr_e, eo, eh, el, ec, n, confirm_bars):
    """Cara B = aturan reaksi bot (_demand_buyer_reaction / _supply_seller_reaction): dalam confirm_bars candle sejak
    sentuhan pertama, masuk di close candle pertama yang searah + (ekor panjang di zona ATAU pantulan >= 0.3 ATR)."""
    best_wick = 0.0
    extreme = None
    for k in range(k0, min(k0 + confirm_bars, n - 1)):
        if side == 1 and el[k] < lo:
            return None
        if side == -1 and eh[k] > hi:
            return None
        a = atr_e[k]
        rng = eh[k] - el[k]
        if el[k] <= hi and eh[k] >= lo and rng > 0 and (not (a == a) or a <= 0 or rng >= MIN_RANGE_ATR * a):
            if side == 1 and ec[k] > lo:
                best_wick = max(best_wick, (min(eo[k], ec[k]) - el[k]) / rng)
            elif side == -1 and ec[k] < hi:
                best_wick = max(best_wick, (eh[k] - max(eo[k], ec[k])) / rng)
        extreme = el[k] if extreme is None and side == 1 else (eh[k] if extreme is None else extreme)
        extreme = min(extreme, el[k]) if side == 1 else max(extreme, eh[k])
        searah = (ec[k] > eo[k]) if side == 1 else (ec[k] < eo[k])
        di_dalam = (ec[k] > lo) if side == 1 else (ec[k] < hi)
        move = (ec[k] - extreme) if side == 1 else (extreme - ec[k])
        wick_ok = best_wick >= WICK_MIN
        bounce_ok = bool(a == a and a > 0 and move >= BOUNCE_ATR * a)
        if searah and di_dalam and (wick_ok or bounce_ok):
            return k
    return None


# ═════════════════════════════════════════════════════════════════════════════
#  KEJADIAN PER PAIR
# ═════════════════════════════════════════════════════════════════════════════
def _ctx_lookup(close_ms, up_flags, t_ms):
    j = int(np.searchsorted(close_ms, t_ms, side="right")) - 1
    if j < 0:
        return None
    return bool(up_flags[j])


def study_pair(symbol, ztf, zdf, edf, btc_zdf, start_ms, args, slow=None):
    """Kembalikan daftar kejadian (sentuhan pertama tiap zona) untuk satu pair & satu TF zona."""
    ztf_ms = TF_MS[ztf]
    eo = edf["open"].values.astype(float)
    eh = edf["high"].values.astype(float)
    el = edf["low"].values.astype(float)
    ec = edf["close"].values.astype(float)
    et = edf["open_ms"].values.astype("int64")
    n = len(edf)
    atr_e = atr_wilder(eh, el, ec, 14)
    ztrend = trend_up(zdf["close"].values.astype(float))
    zclose = zdf["open_ms"].values.astype("int64") + ztf_ms
    if btc_zdf is not None and len(btc_zdf):
        btrend = trend_up(btc_zdf["close"].values.astype(float))
        bclose = btc_zdf["open_ms"].values.astype("int64") + ztf_ms
    else:
        btrend = bclose = None
    types = set(args.types)
    zones = []
    if "ob" in types:
        zones += detect_ob(zdf)
    fvg_all = detect_fvg(zdf) if ("fvg" in types or "ob" in types) else []
    if "fvg" in types:
        zones += fvg_all
    # FVG untuk konfluensi: waktu terbentuk / termitigasi dalam ms
    fvg_t = [(z["side"], z["lo"], z["hi"], int(zdf["open_ms"].iloc[z["idx"]] + ztf_ms),
              (int(zdf["open_ms"].iloc[z["imit"]] + ztf_ms) if z["imit"] is not None else 1 << 62)) for z in fvg_all]
    events = []
    horizon, buf = args.horizon, args.sl_buf
    for z in zones:
        t_known = int(zdf["open_ms"].iloc[z["idx"]] + ztf_ms)
        if t_known < int(et[0]):           # zona terbentuk sebelum data entry dimulai: sentuhan pertama tidak bisa dipastikan
            continue
        s = int(np.searchsorted(et, t_known, side="left"))
        if s >= n - 1:
            continue
        side, lo, hi = z["side"], z["lo"], z["hi"]
        if hi <= lo:
            continue
        overlap = (el[s:] <= hi) & (eh[s:] >= lo)
        if not overlap.any():
            continue
        k0 = s + int(np.argmax(overlap))
        breach = (el[s:] < lo) if side == 1 else (eh[s:] > hi)
        kb = s + int(np.argmax(breach)) if breach.any() else None
        if kb is not None and kb < k0:
            continue                         # zona sudah jebol tanpa pernah "disentuh" (lompat/gap): bukan kejadian
        if int(et[k0]) < start_ms:
            continue                         # sentuhan pertama terjadi di masa pemanasan
        a0 = atr_e[k0]
        if not (a0 == a0) or a0 <= 0:
            continue
        far = lo if side == 1 else hi
        t_touch = int(et[k0])
        conf = False
        if z["type"] == "OB":
            for fs, flo, fhi, fk, fm in fvg_t:
                if fs == side and fk <= t_touch < fm and flo <= hi and fhi >= lo:
                    conf = True
                    break
        depth_win = slice(k0, min(k0 + 4, n))
        if side == 1:
            depth = (hi - float(el[depth_win].min())) / (hi - lo)
        else:
            depth = (float(eh[depth_win].max()) - lo) / (hi - lo)
        ev = {
            "pair": symbol, "ztype": z["type"], "side": side, "ztf": ztf, "t_touch": t_touch,
            "t_zone": t_known, "lo": lo, "hi": hi, "height_atr": (hi - lo) / a0, "depth": float(min(max(depth, 0.0), 3.0)),
            "ctx_trend": None, "ctx_btc": None, "ctx_btc_d": None, "ctx_btc_roc": None, "ctx_pair_d": None, "confl": conf, "jebol_sentuh": bool(kb is not None and kb == k0), "sim": {},
        }
        tr = _ctx_lookup(zclose, ztrend, t_touch)
        if tr is not None:
            ev["ctx_trend"] = "searah" if ((side == 1) == tr) else "lawan"
        if btrend is not None:
            bt = _ctx_lookup(bclose, btrend, t_touch)
            if bt is not None:
                ev["ctx_btc"] = "searah" if ((side == 1) == bt) else "lawan"
        if slow:
            for fld, key in (("ctx_btc_d", ("btc", "d")), ("ctx_btc_roc", ("btc", "roc")), ("ctx_pair_d", ("pair", "d"))):
                src = slow.get(key[0])
                if src is None:
                    continue
                cms_, fl = src
                v = _ctx_slow(cms_, fl[key[1]][0], fl[key[1]][1], t_touch)
                if v is not None:
                    ev[fld] = "searah" if ((side == 1) == v) else "lawan"
        if not ev["jebol_sentuh"]:
            # cara A: langsung masuk di close candle sentuh
            ev["sim"]["A"] = simulate(side, k0, float(ec[k0]), far, float(a0), el, eh, ec, n, horizon, buf, args.rr, args.fee)
            # cara B: tunggu reaksi seperti bot
            kb_e = confirm_entry(side, k0, lo, hi, atr_e, eo, eh, el, ec, n, args.confirm)
            if kb_e is not None and atr_e[kb_e] == atr_e[kb_e]:
                ev["sim"]["B"] = simulate(side, kb_e, float(ec[kb_e]), far, float(atr_e[kb_e]), el, eh, ec, n, horizon, buf, args.rr, args.fee)
                ev["b_delay"] = kb_e - k0
        events.append(ev)
    return events


# ═════════════════════════════════════════════════════════════════════════════
#  RINGKASAN
# ═════════════════════════════════════════════════════════════════════════════
SIDE_NAME = {1: "demand (LONG)", -1: "supply (SHORT)"}
CAT_NAME = [("jebol_sentuh", "Jebol di candle sentuh pertama"),
            ("tembus_langsung", "Tembus langsung (SL, tanpa pantul 0,5R)"),
            ("pantul_tipis", "Pantul tipis (0,5-1R) lalu tembus"),
            ("pantul_lalu_tembus", "Pantul 1R atau lebih, lalu tembus"),
            ("bertahan_naik", "Bertahan & naik 1R atau lebih"),
            ("mandek", "Bertahan tapi mandek")]


def groups_of(events):
    """Kelompok: (jenis, sisi, tf). OB+FVG = subset OB dengan konfluensi FVG."""
    out = {}
    for ev in events:
        keys = [(ev["ztype"], ev["side"], ev["ztf"])]
        if ev["ztype"] == "OB" and ev["confl"]:
            keys.append(("OB+FVG", ev["side"], ev["ztf"]))
        for k in keys:
            out.setdefault(k, []).append(ev)
    return out


def summarize_group(evs, mode, rr_list, t_split):
    rows = [e for e in evs if e["sim"].get(mode)]
    n = len(rows)
    res = {"n": n, "ks": {}, "best": None}
    if n == 0:
        return res
    for k in rr_list:
        nets = np.array([e["sim"][mode]["net"][k] for e in rows])
        wr = float(np.mean([e["sim"][mode]["kind"][k] == "TP" for e in rows]))
        sd = float(nets.std(ddof=1)) if n > 1 else 0.0
        t = float(nets.mean() / (sd / math.sqrt(n))) if sd > 0 else 0.0
        h1 = np.array([e["sim"][mode]["net"][k] for e in rows if e["t_touch"] < t_split])
        h2 = np.array([e["sim"][mode]["net"][k] for e in rows if e["t_touch"] >= t_split])
        res["ks"][k] = {"wr": wr, "exp": float(nets.mean()), "t": t,
                        "e1": float(h1.mean()) if len(h1) else None, "e2": float(h2.mean()) if len(h2) else None,
                        "n1": int(len(h1)), "n2": int(len(h2))}
    best = max(res["ks"], key=lambda k: res["ks"][k]["exp"])
    res["best"] = best
    # uji luar-sampel: pilih RR di paruh 1, nilai HANYA di paruh 2 (ini yang jadi dasar kelolosan)
    r1 = {k: res["ks"][k]["e1"] for k in rr_list if res["ks"][k]["e1"] is not None and res["ks"][k]["n1"] >= 15}
    res["oos_k"] = res["oos_exp"] = res["oos_t"] = None
    res["oos_n"] = 0
    if r1:
        k1 = max(r1, key=r1.get)
        h2 = np.array([e["sim"][mode]["net"][k1] for e in rows if e["t_touch"] >= t_split])
        res["oos_k"] = k1
        res["oos_n"] = int(len(h2))
        if len(h2) >= 15:
            sd2 = float(h2.std(ddof=1))
            res["oos_exp"] = float(h2.mean())
            res["oos_t"] = float(h2.mean() / (sd2 / math.sqrt(len(h2)))) if sd2 > 0 else 0.0
    # pair yang positif pada RR yang direkomendasikan (RR paruh 1 kalau ada, kalau tidak RR terbaik)
    kk = res["oos_k"] if res["oos_k"] is not None else best
    per = {}
    for e in rows:
        per.setdefault(e["pair"], []).append(e["sim"][mode]["net"][kk])
    pp = [(p, float(np.mean(v))) for p, v in per.items() if len(v) >= 5]
    res["pairs_pos"] = (sum(1 for _, m in pp if m > 0), len(pp))
    ks = res["ks"][best]
    pairs_ok = (len(pp) < 5) or (res["pairs_pos"][0] / max(len(pp), 1) >= PAIRS_POS_MIN)
    if n < MIN_OK_EVENTS:
        res["verdict"] = "kecil"
    elif (res["oos_exp"] is not None and r1.get(res["oos_k"], 0) > 0 and res["oos_exp"] > 0
          and res["oos_t"] >= OOS_T_MIN and pairs_ok):
        res["verdict"] = "ok"
    elif ks["exp"] > 0:
        res["verdict"] = "tipis"
    else:
        res["verdict"] = "negatif"
    return res


def taxonomy(evs):
    tot = len(evs)
    cnt = {k: 0 for k, _ in CAT_NAME}
    mfe, depth = [], []
    for e in evs:
        if e["jebol_sentuh"]:
            cnt["jebol_sentuh"] += 1
            depth.append(e["depth"])
            continue
        a = e["sim"].get("A")
        if not a:
            tot -= 1
            continue
        cnt[a["cat"]] += 1
        mfe.append(a["mfe"])
        depth.append(e["depth"])
    return tot, cnt, (float(np.median(mfe)) if mfe else None), (float(np.median(depth)) if depth else None)


def fmt_ks(res, rr_list):
    return "  ".join(f"{k:g}R {res['ks'][k]['wr'] * 100:.0f}%/{res['ks'][k]['exp']:+.2f}" for k in rr_list)


VERDICT_TXT = {"ok": "✅ lolos uji luar-sampel", "tipis": "🟡 positif tapi belum lolos uji luar-sampel (bisa kebetulan)", "negatif": "❌ negatif", "kecil": "⚪ sampel kecil (kurang dari %d kejadian), belum bisa disimpulkan" % MIN_OK_EVENTS}


def _mean_t(vals):
    v = np.array(vals, dtype=float)
    if len(v) < 2:
        return (float(v.mean()) if len(v) else 0.0), 0.0
    sd = float(v.std(ddof=1))
    return float(v.mean()), (float(v.mean() / (sd / math.sqrt(len(v)))) if sd > 0 else 0.0)


def slow_section(events, args):
    """Bagian 4: apakah zona hanya layak dipakai SEARAH tren harian (SHORT saat turun, LONG saat naik)?"""
    L = ["", "4) REZIM LAMBAT — apakah zona hanya layak dipakai SEARAH tren harian?",
         "   searah = SHORT saat tren harian turun / LONG saat tren harian naik. Tren harian = EMA20 vs EMA50 candle harian (BTC dan pair itu sendiri)."]
    evs_all = [e for e in events if e.get("ctx_btc_d") is not None and e.get("ctx_pair_d") is not None]
    if len(evs_all) < 30:
        L.append("   (data harian tidak tersedia / terlalu sedikit untuk bagian ini)")
        return L
    t0 = min(e["t_touch"] for e in evs_all)
    for ztype in ("OB", "FVG"):
        for mode in ("B",):
            base = [e for e in evs_all if e["ztype"] == ztype and e["sim"].get(mode)]
            if len(base) < 20:
                continue
            L.append("")
            L.append(f"▸ {ztype} cara {mode} (n={len(base)}), expectancy dalam R:")
            for k in (1.5, 3.0):
                if k not in args.rr:
                    continue
                full = [e for e in base if e["ctx_btc_d"] == "searah" and e["ctx_pair_d"] == "searah"]
                other = [e for e in base if not (e["ctx_btc_d"] == "searah" and e["ctx_pair_d"] == "searah")]
                parts = []
                for nm, sub in (("SEARAH penuh (BTC & pair)", full), ("lawan salah satu", other)):
                    m, t = _mean_t([x["sim"][mode]["net"][k] for x in sub]) if len(sub) >= 15 else (None, 0)
                    parts.append(f"{nm}: " + (f"{m:+.2f}R (t={t:+.1f}, n={len(sub)})" if m is not None else f"sampel kecil (n={len(sub)})"))
                L.append(f"   RR {k:g}: " + " | ".join(parts))
                for side in (-1, 1):
                    sub = [e for e in full if e["side"] == side]
                    sub2 = [e for e in other if e["side"] == side]
                    a = _mean_t([x["sim"][mode]["net"][k] for x in sub]) if len(sub) >= 15 else None
                    b = _mean_t([x["sim"][mode]["net"][k] for x in sub2]) if len(sub2) >= 15 else None
                    L.append(f"      {SIDE_NAME[side]:15s} searah: " + (f"{a[0]:+.2f}R (n={len(sub)})" if a else f"n={len(sub)} kecil")
                             + " | lawan: " + (f"{b[0]:+.2f}R (n={len(sub2)})" if b else f"n={len(sub2)} kecil"))
            # kestabilan per blok 30 hari untuk yang searah penuh
            full = [e for e in base if e["ctx_btc_d"] == "searah" and e["ctx_pair_d"] == "searah"]
            blocks = {}
            for e in full:
                blocks.setdefault(int((e["t_touch"] - t0) // (30 * DAY_MS)), []).append(e["sim"][mode]["net"][1.5 if 1.5 in args.rr else args.rr[0]])
            if len(blocks) >= 2:
                L.append("   searah penuh per blok 30 hari (RR 1.5): " + " | ".join(
                    f"blok {b + 1}: {np.mean(v):+.2f}R (n={len(v)})" for b, v in sorted(blocks.items()) if len(v) >= 5))
    L.append("")
    L.append("   Cara membaca: kalau 'searah penuh' positif di banyak bulan dan 'lawan' negatif, zona sebaiknya dipakai hanya searah tren harian.")
    L.append("   Kalau keduanya mirip, zona tidak punya keunggulan sendiri dan hasilnya hanya mengikuti arah pasar.")
    return L


def build_report(events, args, meta):
    ver = meta.get("version", "")
    t_all = sorted(e["t_touch"] for e in events)
    t_split = t_all[len(t_all) // 2] if t_all else 0
    G = groups_of(events)
    L = []
    L.append("📐 STUDI REAKSI ZONA")
    L.append("=" * 60)
    L.append(f"zone_study {ZS_VERSION} | Diedit: {ZS_LAST_EDIT} oleh {ZS_EDITED_BY}")
    if ver:
        L.append(ver)
    L.append(f"Periode  : {meta['start']} → {meta['end']} ({args.days} hari)")
    L.append(f"Pair     : {meta['n_pairs']} ({meta['n_pairs_ok']} berhasil) | Zona TF: {','.join(args.zone_tf)} | Masuk TF: {args.entry_tf}")
    L.append(f"Horizon  : {args.horizon} candle ({args.horizon * TF_MS[args.entry_tf] / 3_600_000:.0f} jam) | SL = tepi zona + {args.sl_buf:g} ATR | fee {args.fee:g}%/sisi")
    L.append(f"Kejadian : {len(events)} sentuhan pertama zona (OB {sum(1 for e in events if e['ztype'] == 'OB')}, FVG {sum(1 for e in events if e['ztype'] == 'FVG')})")
    L.append("")
    L.append("1) APA YANG TERJADI SAAT HARGA MASUK ZONA?")
    L.append("   (entry cara A = langsung di close candle sentuh; R = jarak ke SL)")
    order = sorted(G.keys(), key=lambda k: (k[2], {"OB": 0, "OB+FVG": 1, "FVG": 2}[k[0]], -k[1]))
    for key in order:
        evs = G[key]
        tot, cnt, mfe, depth = taxonomy(evs)
        if len(evs) < 10:
            continue
        nz = len(evs)
        L.append("")
        L.append(f"▸ {key[0]} {SIDE_NAME[key[1]]} {key[2]} — {nz} kejadian" + ("  (sampel kecil)" if nz < MIN_EVENTS else ""))
        for ck, cname in CAT_NAME:
            # persen terhadap semua kejadian yang bisa dinilai (termasuk yang jebol di candle sentuh)
            pct = cnt[ck] / max(tot, 1) * 100
            L.append(f"    {cname:<44s} {pct:5.1f}%")
        if mfe is not None:
            L.append(f"    Pantulan tertinggi (median) {mfe:.2f}R | kedalaman masuk zona (median) {((depth or 0) * 100):.0f}% tinggi zona")
    L.append("")
    L.append("2) RR YANG PALING MASUK AKAL  (winrate / expectancy dalam R, sudah dikurangi fee)")
    L.append("   cara A = langsung masuk | cara B = seperti bot sekarang (tunggu reaksi + candle searah)")
    best_rows = []
    for key in order:
        evs = G[key]
        for mode in ("A", "B"):
            r = summarize_group(evs, mode, args.rr, t_split)
            if r["n"] < 10:
                continue
            ks = r["ks"][r["best"]]
            L.append("")
            L.append(f"▸ {key[0]} {SIDE_NAME[key[1]]} {key[2]} [cara {mode}] n={r['n']}")
            L.append("    " + fmt_ks(r, args.rr))
            e1 = f"{ks['e1']:+.2f}" if ks["e1"] is not None else "-"
            e2 = f"{ks['e2']:+.2f}" if ks["e2"] is not None else "-"
            extra = ""
            if r.get("oos_exp") is not None:
                extra = f" | UJI JUJUR: RR dipilih paruh 1 = {r['oos_k']:g}R → paruh 2: exp {r['oos_exp']:+.2f}R (t={r['oos_t']:.1f}, n={r['oos_n']})"
            L.append(f"    RR terbaik (seluruh periode) {r['best']:g}R: exp {ks['exp']:+.2f}R (t={ks['t']:.1f}) | paruh 1 {e1} / paruh 2 {e2} | pair positif {r['pairs_pos'][0]}/{r['pairs_pos'][1]}")
            if extra:
                L.append("    " + extra.lstrip(" |"))
            L.append(f"    {VERDICT_TXT[r['verdict']]}")
            best_rows.append((key, mode, r))
    L.append("")
    L.append("3) KESIMPULAN OTOMATIS")
    good = sorted([x for x in best_rows if x[2]["verdict"] == "ok"], key=lambda x: -x[2]["oos_exp"])
    if good:
        L.append(f"Kombinasi yang lolos uji luar-sampel (RR dipilih di paruh 1, lalu terbukti di paruh 2 dengan t >= {OOS_T_MIN:g}, >= {int(PAIRS_POS_MIN * 100)}% pair positif):")
        for key, mode, r in good[:5]:
            k1 = r["oos_k"]
            ks = r["ks"][k1]
            L.append(f" • {key[0]} {SIDE_NAME[key[1]]} {key[2]} cara {mode}: RR {k1:g}R, winrate {ks['wr'] * 100:.0f}%, exp {ks['exp']:+.2f}R per trade (n={r['n']}); paruh 2 saja {r['oos_exp']:+.2f}R")
        L.append("Tetap hati-hati: ini hasil dari satu periode. Ulangi di periode lain (pakai tanggal akhir berbeda) sebelum dipercaya.")
    else:
        L.append("Tidak ada kombinasi yang lolos uji luar-sampel. Artinya di periode ini, masuk di zona saja belum terbukti")
        L.append("jadi keunggulan yang bisa dipercaya (tanpa filter tambahan seperti tren/BTC). Jangan dipaksakan.")
    # konteks untuk 3 kandidat terbaik (positif apa pun)
    pos = sorted([x for x in best_rows if x[2]["n"] >= MIN_OK_EVENTS and x[2]["ks"][x[2]["best"]]["exp"] > 0],
                 key=lambda x: -x[2]["ks"][x[2]["best"]]["exp"])[:3]
    if pos:
        L.append("")
        L.append("KONTEKS (apakah searah tren / BTC mengubah hasil?)  — RR sama dengan RR terbaik kelompoknya")
        for key, mode, r in pos:
            k = r["best"]
            evs = [e for e in G[key] if e["sim"].get(mode)]
            L.append(f"▸ {key[0]} {SIDE_NAME[key[1]]} {key[2]} cara {mode} ({k:g}R)")
            for cname, field in (("tren zona (EMA50/200 1H)", "ctx_trend"), ("BTC 1H (EMA50/200)", "ctx_btc"),
                                 ("BTC harian (EMA20/50)", "ctx_btc_d"), ("BTC 30 hari (naik/turun)", "ctx_btc_roc"),
                                 ("pair harian (EMA20/50)", "ctx_pair_d")):
                parts = []
                for val in ("searah", "lawan"):
                    sub = [e["sim"][mode]["net"][k] for e in evs if e[field] == val]
                    if len(sub) >= 10:
                        parts.append(f"{val}: exp {np.mean(sub):+.2f}R (n={len(sub)})")
                    else:
                        parts.append(f"{val}: sampel kecil")
                L.append(f"    {cname:<24s} " + " | ".join(parts))
    L.extend(slow_section(events, args))
    L.append("")
    L.append("CATATAN PENTING")
    L.append(" • Kejadian dihitung SEKALI per zona (sentuhan pertama). Zona OB+FVG adalah bagian dari OB, jadi kelompok saling tumpang tindih.")
    L.append(" • RR terbaik dipilih dari beberapa pilihan, jadi sedikit optimis; lihat kolom 'RR terbaik paruh 1 → paruh 2' sebagai uji jujur.")
    L.append(" • Satu candle kena SL dan TP sekaligus dihitung SL duluan (pesimis). Slippage & funding tidak dihitung.")
    L.append(" • Banyak kombinasi diuji sekaligus: beberapa akan tampak bagus karena kebetulan. Percayai yang konsisten di banyak pair dan periode.")
    return "\n".join(L)


# ═════════════════════════════════════════════════════════════════════════════
#  CSV
# ═════════════════════════════════════════════════════════════════════════════
def write_csv(path, events, rr_list):
    cols = ["pair", "ztype", "side", "ztf", "confl", "touch_time", "zone_time", "zone_lo", "zone_hi", "height_atr", "depth_pct",
            "ctx_trend", "ctx_btc", "ctx_btc_d", "ctx_btc_roc", "ctx_pair_d", "jebol_sentuh"]
    for m in ("A", "B"):
        cols += [f"{m}_ada", f"{m}_risk_pct", f"{m}_mfe_R", f"{m}_reaksi"] + [f"{m}_net_{k:g}R" for k in rr_list]
    cols += ["B_delay_candle"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for e in events:
            row = [e["pair"], e["ztype"], "LONG" if e["side"] == 1 else "SHORT", e["ztf"], int(e["confl"]),
                   datetime.fromtimestamp(e["t_touch"] / 1000, timezone.utc).isoformat(),
                   datetime.fromtimestamp(e["t_zone"] / 1000, timezone.utc).isoformat(),
                   round(e["lo"], 8), round(e["hi"], 8), round(e["height_atr"], 3), round(e["depth"] * 100, 1),
                   e["ctx_trend"] or "", e["ctx_btc"] or "", e.get("ctx_btc_d") or "", e.get("ctx_btc_roc") or "",
                   e.get("ctx_pair_d") or "", int(e["jebol_sentuh"])]
            for m in ("A", "B"):
                s = e["sim"].get(m)
                if s:
                    row += [1, round(s["risk_pct"], 4), round(s["mfe"], 3), s["cat"]] + [round(s["net"][k], 4) for k in rr_list]
                else:
                    row += [0, "", "", "jebol_sentuh" if (m == "A" and e["jebol_sentuh"]) else ""] + [""] * len(rr_list)
            row.append(e.get("b_delay", ""))
            w.writerow(row)


# ═════════════════════════════════════════════════════════════════════════════
#  DATA SINTETIS (untuk --selftest): acak murni -> tidak boleh ada keunggulan
# ═════════════════════════════════════════════════════════════════════════════
def synthetic_entry_df(n, tf_ms, start_ms, seed):
    rng = np.random.default_rng(seed)
    ret = rng.normal(0, 0.004, n)
    close = 100 * np.exp(np.cumsum(ret))
    open_ = np.r_[close[0], close[:-1]]
    spread = np.abs(rng.normal(0, 0.003, n))
    high = np.maximum(open_, close) * (1 + spread)
    low = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.003, n)))
    return pd.DataFrame({"open_ms": start_ms + np.arange(n, dtype="int64") * tf_ms, "open": open_, "high": high,
                         "low": low, "close": close, "volume": 1.0})


def resample(edf, factor, tf_ms_out):
    m = len(edf) // factor
    d = edf.iloc[: m * factor]
    g = d.groupby(np.arange(len(d)) // factor)
    return pd.DataFrame({"open_ms": g["open_ms"].first().values, "open": g["open"].first().values, "high": g["high"].max().values,
                         "low": g["low"].min().values, "close": g["close"].last().values, "volume": 1.0})


# ═════════════════════════════════════════════════════════════════════════════
#  UTAMA
# ═════════════════════════════════════════════════════════════════════════════
def run_study(pairs, args, get_df, start_ms, end_ms, log=print):
    ztfs = list(args.zone_tf)
    events, ok_pairs = [], 0
    btc = {}
    for zt in ztfs:
        try:
            btc[zt] = get_df("BTCUSDT", zt, start_ms - ZONE_WARM_CANDLES * TF_MS[zt], end_ms)
        except Exception:  # noqa: BLE001
            btc[zt] = None
    slow_btc = None
    try:
        bd = get_df("BTCUSDT", "1d", start_ms - SLOW_WARM_DAYS * DAY_MS, end_ms)
        if bd is not None and len(bd) >= 60:
            slow_btc = slow_flags(bd)
    except Exception:  # noqa: BLE001
        slow_btc = None
    for i, sym in enumerate(pairs, 1):
        try:
            slow = {"btc": slow_btc} if slow_btc else {}
            try:
                pdl = get_df(sym, "1d", start_ms - SLOW_WARM_DAYS * DAY_MS, end_ms)
                if pdl is not None and len(pdl) >= 60:
                    slow["pair"] = slow_flags(pdl)
            except Exception:  # noqa: BLE001
                pass
            edf = get_df(sym, args.entry_tf, start_ms - ENTRY_WARM_DAYS * DAY_MS, end_ms)
            if edf is None or len(edf) < 200:
                log(f"⚠️ {sym}: data entry kurang, dilewati")
                continue
            got = False
            for zt in ztfs:
                zdf = get_df(sym, zt, start_ms - ZONE_WARM_CANDLES * TF_MS[zt], end_ms)
                if zdf is None or len(zdf) < 260:
                    log(f"⚠️ {sym}@{zt}: data zona kurang, dilewati")
                    continue
                events += study_pair(sym, zt, zdf, edf, btc.get(zt), start_ms, args, slow)
                got = True
            ok_pairs += int(got)
        except Exception as e:  # noqa: BLE001
            log(f"⚠️ {sym}: {e}")
        print(f"PROGRESS {int(i / len(pairs) * 100)} pair={i}/{len(pairs)} kejadian={len(events)}", flush=True)
    return events, ok_pairs


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Studi reaksi zona OB/FVG")
    ap.add_argument("--days", type=int, default=DAYS)
    ap.add_argument("--end", default="", help="YYYY-MM-DD (UTC) akhir periode; default sekarang")
    ap.add_argument("--pairs", default=PAIRS)
    ap.add_argument("--zone-tf", default=ZONE_TFS)
    ap.add_argument("--entry-tf", default=ENTRY_TF)
    ap.add_argument("--types", default="ob,fvg")
    ap.add_argument("--horizon", type=int, default=HORIZON)
    ap.add_argument("--sl-buf", type=float, default=SL_BUF_ATR)
    ap.add_argument("--fee", type=float, default=FEE_PCT)
    ap.add_argument("--confirm", type=int, default=CONFIRM_BARS)
    ap.add_argument("--out", default=None)
    ap.add_argument("--tag", default="")
    ap.add_argument("--selftest", action="store_true")
    a = ap.parse_args(argv)
    a.zone_tf = [x.strip() for x in a.zone_tf.split(",") if x.strip()]
    a.types = [x.strip().lower() for x in a.types.split(",") if x.strip()]
    a.rr = RR_LIST
    a.days = max(7, min(a.days, MAX_DAYS))
    for tf in a.zone_tf + [a.entry_tf]:
        if tf not in TF_MS:
            ap.error(f"TF tidak dikenal: {tf} (pilihan: {', '.join(TF_MS)})")
    return a


def main(argv=None):
    args = parse_args(argv)
    bot = _load_bot()
    out_dir = args.out or os.path.join(HERE, "backtests")
    os.makedirs(out_dir, exist_ok=True)
    if args.end:
        end_dt = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_ms = int(end_dt.timestamp() * 1000)
    start_ms = end_ms - args.days * DAY_MS
    pairs = parse_pairs(args.pairs, list(getattr(bot, "BASE_PAIRS", None) or _base_pairs_fallback()))
    if args.selftest:
        return selftest(args)
    prov = Provider(os.path.join(out_dir, "_cache"), bot)
    print(f"🔬 Studi zona dimulai: {len(pairs)} pair, {args.days} hari, zona {','.join(args.zone_tf)}, masuk {args.entry_tf}", flush=True)
    events, ok_pairs = run_study(pairs, args, prov.get, start_ms, end_ms)
    if not events:
        print("❌ Tidak ada kejadian (data tidak berhasil diunduh atau terlalu sedikit zona).", flush=True)
        sys.exit(2)
    ver = ""
    try:
        ver = bot._bot_version_line_plain() if hasattr(bot, "_bot_version_line_plain") else ""
        if not ver and bot is not None:
            v = bot._bot_version_fields()
            ver = f"Versi bot: {v.get('bot_version')} (upload {v.get('build_date')}) | Diedit: {v.get('last_edit', '-')} oleh {v.get('edited_by', '-')}"
    except Exception:  # noqa: BLE001
        ver = ""
    meta = {"start": datetime.fromtimestamp(start_ms / 1000, timezone.utc).strftime("%Y-%m-%d"),
            "end": datetime.fromtimestamp(end_ms / 1000, timezone.utc).strftime("%Y-%m-%d"),
            "n_pairs": len(pairs), "n_pairs_ok": ok_pairs, "version": ver}
    text = build_report(events, args, meta)
    run_id = f"{end_dt.strftime('%Y%m%d_%H%M')}" + (f"_{args.tag}" if args.tag else "")
    txt_path = os.path.join(out_dir, f"zonestudy_{run_id}.txt")
    csv_path = os.path.join(out_dir, f"zonestudy_{run_id}.csv")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    write_csv(csv_path, events, args.rr)
    print("\n" + text, flush=True)
    print(f"CSV_FILE {csv_path}", flush=True)
    print(f"SUMMARY_FILE {txt_path}", flush=True)
    print(f"\n✅ Selesai. Hasil: {txt_path}", flush=True)


def selftest(args):
    """Data acak murni (random walk): tidak boleh ada keunggulan. Kalau expectancy positif signifikan -> ada kebocoran masa depan."""
    print("🧪 SELFTEST: data acak murni, 25 'pair' x 120 hari. Harapan: expectancy <= sekitar -biaya, tidak positif signifikan.")
    args.zone_tf = ["1h"]
    args.entry_tf = "30m"
    ent_ms, z_ms = TF_MS["30m"], TF_MS["1h"]
    start_ms = 1_700_000_000_000
    n_entry = (120 + ENTRY_WARM_DAYS + 20) * 48
    store = {}
    for s in range(25):
        e = synthetic_entry_df(n_entry, ent_ms, start_ms - (ENTRY_WARM_DAYS + 20) * DAY_MS, 1000 + s)
        store[(f"P{s}USDT", "30m")] = e
        store[(f"P{s}USDT", "1h")] = resample(e, 2, z_ms)
        store[(f"P{s}USDT", "1d")] = resample(e, 48, DAY_MS)
    first_ms = start_ms - (ENTRY_WARM_DAYS + 20) * DAY_MS
    end_ms = first_ms + n_entry * ent_ms

    def get_df(sym, tf, f, t):
        d = store.get((sym, tf))
        return d if d is not None else store.get(("P0USDT", tf))
    # periode studi = 120 hari terakhir
    s_ms = end_ms - 120 * DAY_MS
    pairs = [f"P{s}USDT" for s in range(25)]
    a = argparse.Namespace(**vars(args))
    a.days = 120
    events, ok = run_study(pairs, a, get_df, s_ms, end_ms)
    print(f"Kejadian: {len(events)} dari {ok} pair")
    t_all = sorted(e["t_touch"] for e in events)
    t_split = t_all[len(t_all) // 2]
    G = groups_of(events)
    worst_t, bad = -99, []
    for key, evs in sorted(G.items(), key=lambda x: (x[0][0], x[0][1])):
        for mode in ("A", "B"):
            r = summarize_group(evs, mode, a.rr, t_split)
            if r["n"] < 30:
                continue
            for k in a.rr:
                worst_t = max(worst_t, r["ks"][k]["t"])
                if r["ks"][k]["t"] > 3.0:
                    bad.append((key, mode, k, r["ks"][k]["exp"], r["ks"][k]["t"], r["n"]))
            ks = r["ks"][r["best"]]
            print(f" {key[0]:7s} {SIDE_NAME[key[1]]:15s} cara {mode} n={r['n']:4d} [{r['verdict']}] best {r['best']:g}R exp {ks['exp']:+.3f} (t={ks['t']:+.1f}) | exp@1R {r['ks'][1.0]['exp']:+.3f} exp@2R {r['ks'][2.0]['exp']:+.3f}")
    print(f"t-stat tertinggi di semua kelompok & RR: {worst_t:.2f} (acak murni: harus di bawah ~3)")
    if bad:
        print("❌ SELFTEST GAGAL: ada keunggulan palsu pada data acak ->", bad[:3])
        sys.exit(1)
    print("✅ SELFTEST LULUS: tidak ada keunggulan palsu pada data acak.")


if __name__ == "__main__":
    main()
