"""
trend_follow.py — Regime-Based Trend Following V1.0 (4H regime, 1H pullback continuation).

MODUL MURNI (numpy/pandas). Dipakai IDENTIK oleh backtest (tf_backtest.py) dan jalur live, sehingga
logika sinyal tidak bisa berbeda antara backtest dan live.

Input candle: DataFrame dengan kolom timestamp (waktu BUKA candle, datetime64), open, high, low, close.
HANYA candle yang SUDAH close. Tidak ada lookahead:
  • indikator memakai EMA/Wilder (hanya data sampai candle itu),
  • pivot swing dianggap ada baru setelah SWING_K candle konfirmasi (digeser K candle),
  • regime 4H dipetakan ke 1H memakai candle 4H yang close-nya ≤ close candle 1H (merge_asof backward).

Aturan (LONG; SHORT = cermin) pada candle konfirmasi 1H ke-i:
  1. Regime 4H BULLISH: EMA50 > EMA200 dan close 4H terakhir > EMA50.
  2. EMA50 1H > EMA200 1H.
  3. Pullback: dalam PULLBACK_LOOKBACK candle terakhir low ≤ sisi atas area EMA20–EMA50, dan tidak ada
     close yang menembus sisi bawah area lebih dari PULLBACK_MAX_BREAK_ATR × ATR.
  4. RSI14 > 50, 5. ADX14 > ADX_MIN.
  6. Candle konfirmasi bullish (close > open), 7. close > high candle sebelumnya.
  8. Tidak mengejar: (close − EMA20) ≤ MAX_DIST_FROM_EMA20_ATR × ATR.
Entry: open candle 1H berikutnya. SL: swing terakhir yang sudah terkonfirmasi ± SL_ATR_BUFFER × ATR,
jarak SL harus 1–2.5 ATR dari entry (di luar itu: skip). TP tunggal = entry ± RR × risiko.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

import tf_config as _C


def default_cfg() -> dict:
    return _C.as_dict()


# ═════════════════════════════ INDIKATOR ═════════════════════════════════════
def _rma(s: pd.Series, n: int) -> pd.Series:
    """Wilder smoothing."""
    return s.ewm(alpha=1.0 / n, adjust=False, min_periods=n).mean()


def calc_ema(close: pd.Series, n: int) -> pd.Series:
    return close.ewm(span=n, adjust=False, min_periods=n).mean()


def calc_rsi(close: pd.Series, n: int = 14) -> pd.Series:
    d = close.diff()
    up, dn = d.clip(lower=0.0), (-d).clip(lower=0.0)
    au, ad = _rma(up, n), _rma(dn, n)
    rs = au / ad.replace(0.0, np.nan)
    rsi = 100.0 - 100.0 / (1.0 + rs)
    rsi = rsi.where(~((ad == 0) & (au > 0)), 100.0)
    rsi = rsi.where(~((ad == 0) & (au == 0)), 50.0)
    return rsi


def calc_atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - pc).abs(), (df["low"] - pc).abs()], axis=1).max(axis=1)
    return _rma(tr, n)


def calc_adx(df: pd.DataFrame, n: int = 14) -> pd.Series:
    up = df["high"].diff()
    dn = -df["low"].diff()
    plus_dm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=df.index)
    pc = df["close"].shift(1)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - pc).abs(), (df["low"] - pc).abs()], axis=1).max(axis=1)
    trs = _rma(tr, n)
    pdi = 100.0 * _rma(plus_dm, n) / trs.replace(0.0, np.nan)
    mdi = 100.0 * _rma(minus_dm, n) / trs.replace(0.0, np.nan)
    dx = 100.0 * (pdi - mdi).abs() / (pdi + mdi).replace(0.0, np.nan)
    return _rma(dx.fillna(0.0), n)


def _confirmed_pivots(series: pd.Series, k: int, kind: str):
    """Nilai & indeks pivot terakhir yang SUDAH terkonfirmasi pada tiap candle (tanpa lookahead)."""
    win = 2 * k + 1
    ext = series.rolling(win, center=True).min() if kind == "low" else series.rolling(win, center=True).max()
    is_piv = (series == ext) & ext.notna()
    idx = pd.Series(np.arange(len(series), dtype=float), index=series.index)
    val = series.where(is_piv).shift(k).ffill()           # pivot di j baru tersedia di j+k
    pidx = idx.where(is_piv).shift(k).ffill()
    return val, pidx


def regime_4h(df4h: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """+1 bullish, -1 bearish, 0 netral — per candle 4H, tersedia saat candle 4H itu CLOSE."""
    c = df4h["close"].astype(float)
    e_mid = calc_ema(c, cfg["EMA_MID"])
    e_slow = calc_ema(c, cfg["EMA_SLOW"])
    bull = (e_mid > e_slow) & (c > e_mid)
    bear = (e_mid < e_slow) & (c < e_mid)
    reg = pd.Series(np.where(bull, 1, np.where(bear, -1, 0)), index=df4h.index, dtype=float)
    reg[e_slow.isna()] = 0.0
    out = pd.DataFrame({"close_time": pd.to_datetime(df4h["timestamp"]) + pd.Timedelta(hours=4), "regime": reg.to_numpy()})
    return out.sort_values("close_time").reset_index(drop=True)


def compute_indicators(df1h: pd.DataFrame, df4h: pd.DataFrame, cfg: dict | None = None) -> pd.DataFrame:
    """Kembalikan DataFrame (index sama dengan df1h) berisi indikator + sinyal long_sig/short_sig."""
    cfg = cfg or default_cfg()
    d = df1h.reset_index(drop=True).copy()
    for col in ("open", "high", "low", "close"):
        d[col] = d[col].astype(float)
    d["timestamp"] = pd.to_datetime(d["timestamp"])
    c = d["close"]
    d["ema20"] = calc_ema(c, cfg["EMA_FAST"])
    d["ema50"] = calc_ema(c, cfg["EMA_MID"])
    d["ema200"] = calc_ema(c, cfg["EMA_SLOW"])
    d["rsi"] = calc_rsi(c, cfg["RSI_PERIOD"])
    d["adx"] = calc_adx(d, cfg["ADX_PERIOD"])
    d["atr"] = calc_atr(d, cfg["ATR_PERIOD"])
    k = int(cfg["SWING_K"])
    d["swing_low"], d["swing_low_idx"] = _confirmed_pivots(d["low"], k, "low")
    d["swing_high"], d["swing_high_idx"] = _confirmed_pivots(d["high"], k, "high")

    # regime 4H → 1H (candle 4H yang sudah close saat candle 1H ini close)
    r4 = regime_4h(df4h, cfg) if df4h is not None and len(df4h) else pd.DataFrame({"close_time": [], "regime": []})
    h1 = pd.DataFrame({"close_time": d["timestamp"] + pd.Timedelta(hours=1)})
    if len(r4):
        m = pd.merge_asof(h1.sort_values("close_time"), r4, on="close_time", direction="backward")
        d["regime"] = m["regime"].fillna(0.0).to_numpy()
    else:
        d["regime"] = 0.0

    n = int(cfg["PULLBACK_LOOKBACK"])
    zone_hi = d[["ema20", "ema50"]].max(axis=1)
    zone_lo = d[["ema20", "ema50"]].min(axis=1)
    atr = d["atr"]
    prev_high, prev_low = d["high"].shift(1), d["low"].shift(1)
    brk = cfg["PULLBACK_MAX_BREAK_ATR"] * atr

    touched_l = (d["low"] <= zone_hi).astype(float).rolling(n, min_periods=n).max() == 1
    broke_l = (d["close"] < zone_lo - brk).astype(float).rolling(n, min_periods=n).max() == 1
    touched_s = (d["high"] >= zone_lo).astype(float).rolling(n, min_periods=n).max() == 1
    broke_s = (d["close"] > zone_hi + brk).astype(float).rolling(n, min_periods=n).max() == 1

    dist = cfg["MAX_DIST_FROM_EMA20_ATR"] * atr
    d["long_sig"] = (
        (d["regime"] == 1) & (d["ema50"] > d["ema200"]) & touched_l & ~broke_l
        & (d["rsi"] > cfg["RSI_LONG_MIN"]) & (d["adx"] > cfg["ADX_MIN"])
        & (d["close"] > d["open"]) & (d["close"] > prev_high)
        & ((d["close"] - d["ema20"]) <= dist) & atr.notna()
    ).fillna(False)
    d["short_sig"] = (
        (d["regime"] == -1) & (d["ema50"] < d["ema200"]) & touched_s & ~broke_s
        & (d["rsi"] < cfg["RSI_SHORT_MAX"]) & (d["adx"] > cfg["ADX_MIN"])
        & (d["close"] < d["open"]) & (d["close"] < prev_low)
        & ((d["ema20"] - d["close"]) <= dist) & atr.notna()
    ).fillna(False)
    return d


# ═════════════════════════════ ORDER (SL / TP) ═══════════════════════════════
def build_order(direction: str, entry: float, atr: float, swing_level: float, swing_age: float,
                cfg: dict | None = None) -> dict | None:
    """SL = swing terkonfirmasi ± buffer ATR; jarak SL harus 1–2.5 ATR; TP = entry ± RR × risiko.
    Dipanggil dengan entry AKTUAL (open candle berikutnya), bukan close candle konfirmasi."""
    cfg = cfg or default_cfg()
    if not all(map(math.isfinite, (entry, atr, swing_level))) or atr <= 0 or entry <= 0:
        return None
    if swing_age is None or not math.isfinite(swing_age) or swing_age > cfg["SWING_MAX_AGE"]:
        return None
    buf = cfg["SL_ATR_BUFFER"] * atr
    if direction == "LONG":
        sl = swing_level - buf
        risk = entry - sl
        if risk <= 0:
            return None
    elif direction == "SHORT":
        sl = swing_level + buf
        risk = sl - entry
        if risk <= 0:
            return None
    else:
        return None
    sl_atr = risk / atr
    if not (cfg["SL_MIN_ATR"] <= sl_atr <= cfg["SL_MAX_ATR"]):
        return None
    tp = entry + cfg["RISK_REWARD"] * risk if direction == "LONG" else entry - cfg["RISK_REWARD"] * risk
    return dict(direction=direction, entry=entry, sl=sl, tp=tp, risk_dist=risk, sl_atr=sl_atr, rr=cfg["RISK_REWARD"])


def signal_at(ind: pd.DataFrame, i: int) -> dict | None:
    """Sinyal pada candle konfirmasi ke-i (hanya baris ≤ i dipakai). None bila tidak ada."""
    r = ind.iloc[i]
    if bool(r["long_sig"]):
        d, lvl, age = "LONG", r["swing_low"], i - r["swing_low_idx"]
    elif bool(r["short_sig"]):
        d, lvl, age = "SHORT", r["swing_high"], i - r["swing_high_idx"]
    else:
        return None
    return dict(direction=d, index=int(i), close=float(r["close"]), atr=float(r["atr"]), swing=float(lvl),
                swing_age=float(age), rsi=float(r["rsi"]), adx=float(r["adx"]), regime=int(r["regime"]),
                timestamp=r["timestamp"])


def latest_signal(df1h: pd.DataFrame, df4h: pd.DataFrame, cfg: dict | None = None) -> dict | None:
    """Untuk jalur live: hitung pada data terkini, kembalikan sinyal di candle 1H TERTUTUP terakhir."""
    ind = compute_indicators(df1h, df4h, cfg)
    return signal_at(ind, len(ind) - 1) if len(ind) else None


# ═════════════════════════════ POSITION SIZING ═══════════════════════════════
def position_size(equity: float, entry: float, sl: float, cfg: dict | None = None,
                  risk_pct: float | None = None, qty_step: float | None = None,
                  min_qty: float = 0.0, min_notional: float = 0.0,
                  available_margin: float | None = None) -> dict:
    """qty = (equity × risk%) / |entry − sl|, dibatasi leverage, margin tersedia, step & minNotional."""
    cfg = cfg or default_cfg()
    risk_pct = cfg["RISK_PER_TRADE"] if risk_pct is None else risk_pct
    risk_pct = min(float(risk_pct), cfg["MAX_RISK_PER_TRADE"])
    dist = abs(entry - sl)
    if equity <= 0 or dist <= 0 or entry <= 0:
        return dict(qty=0.0, notional=0.0, risk_amount=0.0, reason="input tidak valid")
    qty = equity * risk_pct / dist
    max_notional = equity * cfg["LEVERAGE"]
    if available_margin is not None:
        max_notional = min(max_notional, available_margin * cfg["LEVERAGE"])
    if qty * entry > max_notional:
        qty = max_notional / entry
    if qty_step and qty_step > 0:
        qty = math.floor(qty / qty_step + 1e-12) * qty_step
    notional = qty * entry
    if qty <= 0 or qty < min_qty or notional < min_notional:
        return dict(qty=0.0, notional=0.0, risk_amount=0.0, reason="di bawah minimum order")
    return dict(qty=qty, notional=notional, risk_amount=qty * dist, reason="ok")


# ═════════════════════════════ RISK GATE ═════════════════════════════════════
class RiskGate:
    """Batas risiko portofolio. Tidak tahu apa pun soal exchange: hanya angka."""

    def __init__(self, cfg: dict | None = None):
        self.cfg = cfg or default_cfg()
        self.day_key = None
        self.week_key = None
        self.day_start_equity = None
        self.week_start_equity = None
        self.day_pnl = 0.0
        self.week_pnl = 0.0

    def roll(self, now: pd.Timestamp, equity: float):
        dk = (now.year, now.month, now.day)
        iso = now.isocalendar()
        wk = (iso[0], iso[1])
        if dk != self.day_key:
            self.day_key, self.day_start_equity, self.day_pnl = dk, equity, 0.0
        if wk != self.week_key:
            self.week_key, self.week_start_equity, self.week_pnl = wk, equity, 0.0

    def record_pnl(self, pnl: float):
        self.day_pnl += pnl
        self.week_pnl += pnl

    def can_open(self, now: pd.Timestamp, equity: float, direction: str, new_risk: float,
                 open_positions: list[dict]) -> tuple[bool, str]:
        c = self.cfg
        self.roll(now, equity)
        if self.day_start_equity and self.day_pnl <= -c["DAILY_LOSS_LIMIT"] * self.day_start_equity:
            return False, "daily loss limit"
        if self.week_start_equity and self.week_pnl <= -c["WEEKLY_LOSS_LIMIT"] * self.week_start_equity:
            return False, "weekly loss limit"
        if len(open_positions) >= c["MAX_OPEN_POSITIONS"]:
            return False, "max open positions"
        if sum(p["risk_amount"] for p in open_positions) + new_risk > c["MAX_TOTAL_OPEN_RISK"] * equity + 1e-9:
            return False, "max total open risk"
        if sum(1 for p in open_positions if p["direction"] == direction) >= c["MAX_SAME_DIRECTION"]:
            return False, "max same-direction exposure"
        if new_risk > c["MAX_RISK_PER_TRADE"] * equity + 1e-9:
            return False, "risk per trade > max"
        return True, "ok"
