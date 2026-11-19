"""
tf_backtest.py — backtest kronologis Trend Following V1.0 (4H regime + 1H pullback), multi-tahun.

Memakai trend_follow.py (modul sinyal YANG SAMA dengan jalur live) dan tf_config.py.
Jalankan:   python tf_backtest.py --years 5          (butuh internet ke fapi.binance.com)
Uji kode:   python tf_backtest.py --synthetic        (data acak; hanya memeriksa kode berjalan)

ASUMSI SIMULASI (konservatif):
  • Sinyal dari candle 1H yang sudah close; entry di OPEN candle 1H berikutnya (+ slippage melawan posisi).
  • Entry dibatalkan bila open menyimpang > MAX_ENTRY_GAP_ATR dari close konfirmasi, atau SL/TP baru melanggar batas.
  • Jika SL dan TP tersentuh pada candle yang sama → SL dianggap lebih dulu. Gap melewati SL → keluar di open.
  • Fee taker + slippage di setiap sisi; funding dibayar/diterima tiap 00/08/16 UTC bila data tersedia.
  • Risiko per trade dari equity saldo terkini (compounding), batas harian/mingguan memakai PnL terealisasi.
  • Tidak ada optimasi parameter. Split 60/20/20 kronologis berdasarkan waktu entry; walk-forward = blok 6 bulan.
"""
from __future__ import annotations

import argparse
import math
import sys
import time

import numpy as np
import pandas as pd

import tf_config as C
import trend_follow as TF

BASE = "https://fapi.binance.com"
HOUR = 3_600_000
DAY = 24 * HOUR


# ═════════════════════════════ DATA ══════════════════════════════════════════
def _get(url, params):
    import requests
    for k in range(5):
        r = requests.get(url, params=params, timeout=20)
        if r.status_code in (418, 429):
            time.sleep(10 * (k + 1))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("rate limit terus-menerus")


def dl_1h(sym, start_ms, end_ms):
    rows, cur = [], start_ms
    while cur < end_ms:
        d = _get(f"{BASE}/fapi/v1/klines", dict(symbol=sym, interval="1h", startTime=cur, endTime=end_ms, limit=1500))
        if not d:
            break
        rows += d
        cur = int(d[-1][0]) + HOUR
        if len(d) < 1500:
            break
    if not rows:
        return None
    df = pd.DataFrame(rows).iloc[:, [0, 1, 2, 3, 4, 5, 6]]
    df.columns = ["t", "open", "high", "low", "close", "volume", "close_t"]
    now_ms = int(time.time() * 1000)
    df = df[df["close_t"].astype("int64") < now_ms]            # buang candle yang belum close
    df["timestamp"] = pd.to_datetime(df["t"].astype("int64"), unit="ms")
    for c in ("open", "high", "low", "close", "volume"):
        df[c] = df[c].astype(float)
    return df[["timestamp", "open", "high", "low", "close", "volume"]].drop_duplicates("timestamp").reset_index(drop=True)


def dl_funding(sym, start_ms, end_ms):
    rows, cur = [], start_ms
    while cur < end_ms:
        d = _get(f"{BASE}/fapi/v1/fundingRate", dict(symbol=sym, startTime=cur, endTime=end_ms, limit=1000))
        if not d:
            break
        rows += d
        cur = int(d[-1]["fundingTime"]) + 1
        if len(d) < 1000:
            break
    if not rows:
        return None
    ts = pd.to_datetime([int(x["fundingTime"]) for x in rows], unit="ms").round("h")
    return pd.Series([float(x["fundingRate"]) for x in rows], index=ts).groupby(level=0).last()


def resample_4h(df1h: pd.DataFrame) -> pd.DataFrame:
    g = df1h.set_index("timestamp").resample("4h")
    out = g.agg(open=("open", "first"), high=("high", "max"), low=("low", "min"),
                close=("close", "last"), n=("close", "count"))
    out = out[out["n"] == 4].drop(columns="n").reset_index()      # hanya candle 4H lengkap (sudah close)
    return out


def synthetic(years=3, seed=7, null=False):
    rng = np.random.default_rng(seed)
    n = years * 365 * 24
    idx = pd.date_range("2022-01-01", periods=n, freq="h")
    out = {}
    for s in C.PAIRS:
        drift = np.repeat(rng.normal(0.0, 0.0004, n // 400 + 1), 400)[:n]
        if null:
            drift = np.zeros(n)          # random walk murni: TIDAK boleh ada edge
        r = drift + rng.normal(0, 0.007, n)
        c = 100 * np.exp(np.cumsum(r))
        o = np.r_[c[0], c[:-1]]
        h = np.maximum(o, c) * (1 + np.abs(rng.normal(0, 0.002, n)))
        l = np.minimum(o, c) * (1 - np.abs(rng.normal(0, 0.002, n)))
        df = pd.DataFrame(dict(timestamp=idx, open=o, high=h, low=l, close=c, volume=1.0))
        fi = pd.date_range(idx[0], idx[-1], freq="8h")
        out[s] = (df, pd.Series(rng.normal(0.0001, 0.0002, len(fi)), index=fi))
    return out


# ═════════════════════════════ SIMULASI ══════════════════════════════════════
def simulate(data: dict, cfg: dict, equity0: float = 10_000.0, use_funding: bool = True):
    syms = list(data.keys())
    ind = {s: TF.compute_indicators(data[s][0], resample_4h(data[s][0]), cfg) for s in syms}
    fund = {s: data[s][1] for s in syms}
    # timeline global
    all_ts = sorted(set().union(*[set(ind[s]["timestamp"]) for s in syms]))
    T = len(all_ts)
    tindex = pd.DatetimeIndex(all_ts)
    pos_in = {s: tindex.get_indexer(pd.DatetimeIndex(ind[s]["timestamp"])) for s in syms}   # idx global -> baris
    row_of = {}
    for s in syms:
        arr = np.full(T, -1, dtype=np.int64)
        g = pos_in[s]
        arr[g[g >= 0]] = np.arange(len(g))[g >= 0]
        row_of[s] = arr
    A = {s: {k: ind[s][k].to_numpy() for k in ("open", "high", "low", "close", "atr", "rsi", "ema50", "ema200",
                                                   "regime", "long_sig", "short_sig", "swing_low", "swing_low_idx",
                                                   "swing_high", "swing_high_idx", "adx")} for s in syms}
    eval_start = tindex[0] + pd.Timedelta(days=36)       # warmup EMA200 4H

    balance = equity0
    gate = TF.RiskGate(cfg)
    open_pos: list[dict] = []
    pending: dict[str, dict] = {}
    trades: list[dict] = []
    skips: dict[str, int] = {}
    eq_curve = []
    fee, slip = cfg["FEE_RATE"], cfg["SLIPPAGE"]

    def skip(reason):
        skips[reason] = skips.get(reason, 0) + 1

    def close_pos(p, price, t, reason):
        nonlocal balance
        sign = 1 if p["direction"] == "LONG" else -1
        gross = (price - p["entry"]) * p["qty"] * sign
        fees = p["fee_in"] + price * p["qty"] * fee
        net = gross - fees + p["funding"]
        balance += net
        gate.record_pnl(net)
        trades.append(dict(sym=p["sym"], direction=p["direction"], entry_time=p["entry_time"], exit_time=t,
                           entry=p["entry"], exit=price, qty=p["qty"], risk_amount=p["risk_amount"],
                           equity_at_entry=p["equity_at_entry"], gross=gross, fees=fees, funding=p["funding"],
                           net=net, R=net / p["risk_amount"] if p["risk_amount"] else 0.0, reason=reason,
                           hold_h=(t - p["entry_time"]) / pd.Timedelta(hours=1)))
        open_pos.remove(p)

    for i in range(T):
        t = all_ts[i]
        # A1: exit terjadwal di OPEN candle ini
        for p in list(open_pos):
            r = row_of[p["sym"]][i]
            if p.get("exit_reason") and r >= 0:
                o = A[p["sym"]]["open"][r]
                px = o * (1 - slip) if p["direction"] == "LONG" else o * (1 + slip)
                close_pos(p, px, t, p["exit_reason"])
        # A2: entry tertunda di OPEN candle ini
        held_before = list(open_pos)
        for s, sg in list(pending.items()):
            r = row_of[s][i]
            if r < 0:
                continue
            del pending[s]
            if t < eval_start:
                continue
            a = A[s]
            o = float(a["open"][r])
            if abs(o - sg["close"]) > cfg["MAX_ENTRY_GAP_ATR"] * sg["atr"]:
                skip("gap open vs close konfirmasi")
                continue
            entry = o * (1 + slip) if sg["direction"] == "LONG" else o * (1 - slip)
            od = TF.build_order(sg["direction"], entry, sg["atr"], sg["swing"], sg["swing_age"] + 1, cfg)
            if od is None:
                skip("SL di luar 1–2.5 ATR / swing tidak valid")
                continue
            used = sum(p["qty"] * p["entry"] for p in open_pos) / cfg["LEVERAGE"]
            sz = TF.position_size(balance, entry, od["sl"], cfg, available_margin=max(balance - used, 0.0))
            if sz["qty"] <= 0:
                skip("sizing: " + sz["reason"])
                continue
            ok, why = gate.can_open(t, balance, sg["direction"], sz["risk_amount"], open_pos)
            if not ok:
                skip(why)
                continue
            fee_in = sz["notional"] * fee
            open_pos.append(dict(sym=s, direction=sg["direction"], entry=entry, sl=od["sl"], tp=od["tp"],
                                 qty=sz["qty"], risk_amount=sz["risk_amount"], entry_time=t, fee_in=fee_in,
                                 funding=0.0, equity_at_entry=balance, exit_reason=None))
        # funding (settlement 00/08/16 UTC) untuk posisi yang sudah terbuka sebelum candle ini
        if use_funding and t.hour in (0, 8, 16) and t.minute == 0:
            for p in held_before:
                if p in open_pos and fund[p["sym"]] is not None:
                    rate = fund[p["sym"]].get(t, None)
                    if rate is not None and math.isfinite(rate):
                        r = row_of[p["sym"]][i]
                        if r >= 0:
                            notional = p["qty"] * A[p["sym"]]["open"][r]
                            p["funding"] -= (1 if p["direction"] == "LONG" else -1) * rate * notional
        # B: SL / TP pada candle ini (SL lebih dulu bila keduanya)
        for p in list(open_pos):
            r = row_of[p["sym"]][i]
            if r < 0:
                continue
            a = A[p["sym"]]
            o, h, l = float(a["open"][r]), float(a["high"][r]), float(a["low"][r])
            if p["direction"] == "LONG":
                if l <= p["sl"]:
                    close_pos(p, min(p["sl"], o) * (1 - slip), t, "SL")
                elif h >= p["tp"]:
                    close_pos(p, max(p["tp"], o), t, "TP")
            else:
                if h >= p["sl"]:
                    close_pos(p, max(p["sl"], o) * (1 + slip), t, "SL")
                elif l <= p["tp"]:
                    close_pos(p, min(p["tp"], o), t, "TP")
        # D: evaluasi exit di CLOSE candle → dieksekusi di open berikutnya
        for p in open_pos:
            r = row_of[p["sym"]][i]
            if r < 0 or p.get("exit_reason"):
                continue
            a = A[p["sym"]]
            c = float(a["close"][r])
            sign = 1 if p["direction"] == "LONG" else -1
            reg = a["regime"][r]
            if cfg["EXIT_ON_REGIME_REVERSAL"] and reg == -sign:
                p["exit_reason"] = "REVERSAL"
                continue
            age_h = (t + pd.Timedelta(hours=1) - p["entry_time"]) / pd.Timedelta(hours=1)
            if age_h >= cfg["TIME_EXIT_HOURS"]:
                risk_dist = abs(p["entry"] - p["sl"])
                unreal_r = (c - p["entry"]) * sign / risk_dist if risk_dist else 0.0
                rsi = a["rsi"][r]
                weak = (rsi < 50) if sign == 1 else (rsi > 50)
                struct_bad = (c < a["ema50"][r] or a["ema50"][r] <= a["ema200"][r]) if sign == 1 \
                    else (c > a["ema50"][r] or a["ema50"][r] >= a["ema200"][r])
                if unreal_r < cfg["TIME_EXIT_MIN_PROFIT_R"] and weak and struct_bad:
                    p["exit_reason"] = "TIME"
        # E: sinyal baru di close candle ini → entry di open berikutnya
        if t >= eval_start:
            held_syms = {p["sym"] for p in open_pos}
            for s in syms:
                r = row_of[s][i]
                if r < 0 or s in held_syms or s in pending:
                    continue
                a = A[s]
                if a["long_sig"][r] or a["short_sig"][r]:
                    d = "LONG" if a["long_sig"][r] else "SHORT"
                    lvl = a["swing_low"][r] if d == "LONG" else a["swing_high"][r]
                    lidx = a["swing_low_idx"][r] if d == "LONG" else a["swing_high_idx"][r]
                    pending[s] = dict(direction=d, close=float(a["close"][r]), atr=float(a["atr"][r]),
                                      swing=float(lvl), swing_age=float(r - lidx) if np.isfinite(lidx) else float("inf"))
        # kurva equity (mark-to-market di close)
        mtm = balance
        for p in open_pos:
            r = row_of[p["sym"]][i]
            if r >= 0:
                c = float(A[p["sym"]]["close"][r])
                mtm += (c - p["entry"]) * p["qty"] * (1 if p["direction"] == "LONG" else -1)
        eq_curve.append((t, mtm))

    # posisi yang masih terbuka di akhir data: tutup di close terakhir (dicatat)
    for p in list(open_pos):
        r = max(np.nonzero(row_of[p["sym"]] >= 0)[0][-1], 0)
        close_pos(p, float(A[p["sym"]]["close"][row_of[p["sym"]][r]]), all_ts[-1], "END")
    tr = pd.DataFrame(trades)
    eq = pd.Series([v for _, v in eq_curve], index=[t for t, _ in eq_curve])
    return tr, eq, skips, eval_start


# ═════════════════════════════ METRIK ════════════════════════════════════════
def _streak(flags):
    best = cur = 0
    for f in flags:
        cur = cur + 1 if f else 0
        best = max(best, cur)
    return best


def metrics(tr: pd.DataFrame, equity0: float):
    if tr is None or len(tr) == 0:
        return dict(n=0)
    tr = tr.sort_values("exit_time")
    net = tr["net"]
    gp, gl = net[net > 0].sum(), net[net < 0].sum()
    ret = (net / tr["equity_at_entry"]).to_numpy()
    curve = np.cumprod(1 + ret)
    peak = np.maximum.accumulate(np.r_[1.0, curve])[1:]
    mdd = float(((curve - peak) / peak).min())
    return dict(n=len(tr), wins=int((net > 0).sum()), losses=int((net <= 0).sum()),
                winrate=float((net > 0).mean()), gross_profit=float(gp), gross_loss=float(gl), net=float(net.sum()),
                pf=float(gp / abs(gl)) if gl < 0 else float("inf"), exp_usdt=float(net.mean()),
                avgR=float(tr["R"].mean()), mdd=mdd, hold_h=float(tr["hold_h"].mean()),
                fees=float(tr["fees"].sum()), funding=float(tr["funding"].sum()),
                ret_total=float(np.prod(1 + ret) - 1), cw=_streak((net > 0).to_numpy()),
                cl=_streak((net <= 0).to_numpy()))


def fmt(m, name):
    if m.get("n", 0) == 0:
        return f"{name:9s} tidak ada trade"
    return (f"{name:9s} n={m['n']:4d} WR={m['winrate']*100:4.1f}% PF={m['pf']:5.2f} E={m['avgR']:+.3f}R "
            f"ret={m['ret_total']*100:+7.1f}% MDD={m['mdd']*100:5.1f}%")


def report(tr, eq, skips, eval_start, equity0, cfg):
    L = []
    t0, t1 = eval_start, eq.index[-1]
    L.append(f"Periode evaluasi: {t0.date()} → {t1.date()} | pair: {', '.join(sorted(set(tr['sym']))) if len(tr) else '-'}")
    L.append(f"Modal awal ${equity0:,.0f} | risk {cfg['RISK_PER_TRADE']*100:g}%/trade | RR 1:{cfg['RISK_REWARD']:g} | "
             f"lev {cfg['LEVERAGE']}x | fee {cfg['FEE_RATE']*100:.2f}% + slip {cfg['SLIPPAGE']*100:.2f}%/sisi")
    m = metrics(tr, equity0)
    if m["n"] == 0:
        L.append("TIDAK ADA TRADE. Strategi tidak menghasilkan sinyal pada data ini.")
        return "\n".join(L), {}
    L.append("")
    L.append(f"Total trade {m['n']} | menang {m['wins']} | kalah {m['losses']} | win rate {m['winrate']*100:.1f}%")
    L.append(f"Gross profit ${m['gross_profit']:,.0f} | gross loss ${m['gross_loss']:,.0f} | NET ${m['net']:,.0f} ({m['ret_total']*100:+.1f}%)")
    L.append(f"Profit factor {m['pf']:.2f} | expectancy {m['exp_usdt']:+.2f} USD = {m['avgR']:+.3f}R per trade")
    L.append(f"Max drawdown {m['mdd']*100:.1f}% (kurva trade) / {((eq/eq.cummax()-1).min())*100:.1f}% (mark-to-market)")
    L.append(f"Rata-rata tahan {m['hold_h']:.1f} jam | fee ${m['fees']:,.0f} | funding ${m['funding']:+,.0f}")
    L.append(f"Menang beruntun maks {m['cw']} | kalah beruntun maks {m['cl']}")
    L.append("Exit: " + ", ".join(f"{k} {v}" for k, v in tr['reason'].value_counts().items()))
    L.append("")
    L.append("Per arah:   " + " | ".join(fmt(metrics(tr[tr.direction == d], equity0), d) for d in ("LONG", "SHORT")))
    L.append("")
    L.append("Per pair:")
    for s in sorted(set(tr["sym"])):
        mm = metrics(tr[tr.sym == s], equity0)
        L.append("  " + fmt(mm, s))
    # split 60/20/20 by waktu entry
    span = t1 - t0
    b1, b2 = t0 + span * 0.6, t0 + span * 0.8
    seg = {"TRAIN 60%": tr[tr.entry_time < b1], "VALID 20%": tr[(tr.entry_time >= b1) & (tr.entry_time < b2)],
           "OOS 20%": tr[tr.entry_time >= b2]}
    L.append("")
    L.append("Split kronologis (parameter tidak dioptimasi pada segmen mana pun):")
    segm = {}
    for k, v in seg.items():
        segm[k] = metrics(v, equity0)
        L.append("  " + fmt(segm[k], k))
    # walk-forward: blok 6 bulan
    L.append("")
    L.append("Walk-forward (blok 6 bulan, hasil per blok):")
    blk = tr.copy()
    blk["blok"] = blk["entry_time"].dt.year.astype(str) + "H" + ((blk["entry_time"].dt.month - 1) // 6 + 1).astype(str)
    pos = tot = 0
    for b, v in blk.groupby("blok"):
        mb = metrics(v, equity0)
        tot += 1
        pos += int(mb["net"] > 0)
        L.append(f"  {b}: n={mb['n']:3d} net={mb['ret_total']*100:+6.1f}% PF={mb['pf']:5.2f}")
    L.append(f"  Blok positif: {pos}/{tot}")
    # bulanan ringkas
    mon = tr.groupby(tr["entry_time"].dt.to_period("M"))["net"].sum()
    L.append("")
    L.append(f"Bulanan: {int((mon > 0).sum())} bulan positif dari {len(mon)}; terbaik ${mon.max():,.0f}, terburuk ${mon.min():,.0f}")
    if skips:
        L.append("Entry dilewati: " + ", ".join(f"{k}: {v}" for k, v in sorted(skips.items(), key=lambda x: -x[1])))
    # verdict
    oos = segm["OOS 20%"]
    checks = [
        (f"Profit factor > {cfg['TARGET_PROFIT_FACTOR']}", m["pf"] > cfg["TARGET_PROFIT_FACTOR"]),
        (f"Max drawdown < {cfg['TARGET_MAX_DRAWDOWN']*100:.0f}%", abs(m["mdd"]) < cfg["TARGET_MAX_DRAWDOWN"]),
        (f"Trade ≥ {cfg['TARGET_MIN_TRADES']}", m["n"] >= cfg["TARGET_MIN_TRADES"]),
        ("Expectancy positif", m["avgR"] > 0),
        ("OOS untung setelah biaya", oos.get("n", 0) > 0 and oos["net"] > 0),
    ]
    L.append("")
    L.append("KEPUTUSAN (kriteria tetap dari spesifikasi):")
    for name, ok in checks:
        L.append(f"  [{'LOLOS' if ok else 'GAGAL'}] {name}")
    allok = all(ok for _, ok in checks)
    L.append("  => " + ("SEMUA LOLOS — boleh lanjut ke paper trading." if allok
                        else "BELUM LOLOS — jangan dipakai live. Laporan ini apa adanya, tanpa penyesuaian."))
    return "\n".join(L), dict(overall=m, segs=segm, checks=checks)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=5)
    ap.add_argument("--synthetic", action="store_true")
    ap.add_argument("--null", action="store_true", help="dengan --synthetic: random walk murni tanpa tren (uji sanity: harus ≤ 0 setelah fee)")
    ap.add_argument("--equity", type=float, default=10_000.0)
    ap.add_argument("--rr", type=float, default=None)
    ap.add_argument("--risk", type=float, default=None)
    ap.add_argument("--no-funding", action="store_true")
    a = ap.parse_args()
    cfg = C.as_dict()
    if a.rr:
        cfg["RISK_REWARD"] = a.rr
    if a.risk:
        cfg["RISK_PER_TRADE"] = a.risk
    if a.synthetic:
        data = synthetic(null=a.null)
    else:
        end = int(time.time() * 1000)
        start = end - a.years * 365 * DAY - 40 * DAY
        data = {}
        for s in cfg["PAIRS"]:
            try:
                df = dl_1h(s, start, end)
                fd = None if a.no_funding else dl_funding(s, start, end)
                if df is not None and len(df) > 2000:
                    data[s] = (df, fd)
                    print(f"  {s}: {len(df)} candle 1H", flush=True)
            except Exception as e:  # noqa: BLE001
                print(f"  {s} gagal: {e}", flush=True)
    if len(data) < 2:
        print("Data kurang (butuh ≥2 pair).")
        sys.exit(1)
    tr, eq, skips, ev = simulate(data, cfg, a.equity, use_funding=not a.no_funding)
    text, _ = report(tr, eq, skips, ev, a.equity, cfg)
    print("\n" + text)


if __name__ == "__main__":
    main()
