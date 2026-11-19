"""
carry_research.py — uji FUNDING CARRY: beli spot + short perpetual dengan nominal sama (delta-netral),
menerima funding saat funding positif. Tidak butuh prediksi arah harga.

ATURAN & KRITERIA DITETAPKAN SEBELUM MELIHAT DATA (tidak ada optimasi):
  A) always : posisi selalu terbuka sejak awal data.
  B) filter : posisi ON bila rata-rata 9 settlement terakhir (≈3 hari) > 0.015% per 8 jam;
              OFF bila < 0.005% (histeresis supaya tidak sering bolak-balik). Keputusan memakai data
              yang SUDAH ada; berlaku untuk settlement berikutnya (tanpa lookahead).
  Modal per 1 unit nominal = 1 (spot) + 1/LEV (margin perp), LEV=3  → faktor 1.333.
  Biaya tiap kali buka/tutup: spot taker 0.10% + perp taker 0.04% + slippage 0.02% per kaki = 0.18% per sisi
  (round trip 0.36%). Modal yang menganggur (posisi OFF) dihitung return 0.
  Portofolio: bobot sama di 6 pair.

KEPUTUSAN LOLOS (tetap): return tahunan bersih (CAGR) > 8% DAN tidak ada tahun kalender negatif
  DAN max drawdown < 5%. Pembanding: bunga bebas risiko ~4.5%/tahun. Tanpa itu, buat apa repot.

YANG TIDAK DIMODELKAN (dan nyata di live): basis spot-perp bergerak, selisih harga eksekusi dua kaki,
  top-up margin saat harga naik tajam (short perp rugi, spot untung — modal harus dipindah), risiko
  exchange/likuidasi/penarikan. Skrip melaporkan kenaikan harga terbesar selama posisi terbuka sebagai
  indikator tekanan margin.

Jalankan:  python carry_research.py --years 4        |   uji kode:  python carry_research.py --synthetic
"""
import argparse
import sys
import time

import numpy as np
import pandas as pd

BASE = "https://fapi.binance.com"
SYMS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT"]
DAY = 86_400_000
LEV = 3.0
COST_SIDE = 0.0010 + 0.0004 + 0.0002 + 0.0002      # spot fee + perp fee + slippage 2 kaki
CAP = 1.0 + 1.0 / LEV
ENTER, EXIT_, WIN = 0.00015, 0.00005, 9


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


def dl_daily_high(sym, start_ms, end_ms):
    rows, cur = [], start_ms
    while cur < end_ms:
        d = _get(f"{BASE}/fapi/v1/klines", dict(symbol=sym, interval="1d", startTime=cur, endTime=end_ms, limit=1500))
        if not d:
            break
        rows += d
        cur = int(d[-1][0]) + DAY
        if len(d) < 1500:
            break
    if not rows:
        return None
    idx = pd.to_datetime([int(x[0]) for x in rows], unit="ms").normalize()
    return pd.DataFrame({"high": [float(x[2]) for x in rows], "close": [float(x[4]) for x in rows]}, index=idx)


def synth(years=4, seed=3):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2022-11-01", periods=years * 365 * 3, freq="8h")
    out = {}
    for s in SYMS:
        regime = np.repeat(rng.normal(0.00008, 0.00012, len(idx) // 270 + 1), 270)[:len(idx)]
        f = pd.Series(regime + rng.normal(0, 0.00004, len(idx)), index=idx)
        d = pd.date_range(idx[0].normalize(), idx[-1].normalize(), freq="D")
        c = 100 * np.exp(np.cumsum(rng.normal(0.0003, 0.03, len(d))))
        out[s] = (f, pd.DataFrame({"high": c * 1.02, "close": c}, index=d))
    return out


def run_pair(f: pd.Series, mode: str):
    """Return per settlement (index=f.index) dan posisi."""
    rate = f.to_numpy(float)
    n = len(rate)
    on = np.zeros(n, dtype=bool)           # posisi yang DIPEGANG selama settlement i
    if mode == "always":
        on[:] = True
    else:
        state = False
        for i in range(n):
            on[i] = state                  # status dari keputusan sebelumnya
            if i + 1 >= WIN:
                m = rate[i + 1 - WIN:i + 1].mean()     # hanya data sampai settlement i
                if not state and m > ENTER:
                    state = True
                elif state and m < EXIT_:
                    state = False
    prev = np.r_[False, on[:-1]]
    toggles = (on != prev).astype(float)
    if mode == "always":
        toggles[0] = 1.0                    # biaya buka sekali di awal
    ret = (np.where(on, rate, 0.0) - toggles * COST_SIDE) / CAP
    if mode == "always":
        ret[-1] -= COST_SIDE / CAP          # biaya tutup di akhir
    elif on[-1]:
        ret[-1] -= COST_SIDE / CAP
    return pd.Series(ret, index=f.index), pd.Series(on, index=f.index), int(toggles.sum())


def metrics(r: pd.Series):
    d = r.groupby(r.index.normalize()).sum()
    eq = (1 + d).cumprod()
    yrs = max((d.index[-1] - d.index[0]).days / 365.0, 1e-9)
    cagr = eq.iloc[-1] ** (1 / yrs) - 1
    mdd = float((eq / eq.cummax() - 1).min())
    sh = d.mean() / d.std() * np.sqrt(365) if d.std() > 0 else float("nan")
    return dict(cagr=float(cagr), mdd=mdd, sharpe=float(sh), total=float(eq.iloc[-1] - 1)), d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", type=int, default=4)
    ap.add_argument("--synthetic", action="store_true")
    a = ap.parse_args()
    if a.synthetic:
        data = synth()
    else:
        end = int(time.time() * 1000)
        start = end - a.years * 365 * DAY
        data = {}
        for s in SYMS:
            try:
                f = dl_funding(s, start, end)
                k = dl_daily_high(s, start, end)
                if f is not None and len(f) > 300:
                    data[s] = (f, k)
                    print(f"  {s}: {len(f)} settlement funding", flush=True)
            except Exception as e:  # noqa: BLE001
                print(f"  {s} gagal: {e}", flush=True)
    if len(data) < 3:
        print("Data kurang (butuh ≥3 pair).")
        sys.exit(1)

    out = {}
    for mode in ("always", "filter"):
        rets, ons, tg = {}, {}, {}
        for s, (f, _) in data.items():
            rets[s], ons[s], tg[s] = run_pair(f, mode)
        port = pd.concat(rets.values(), axis=1).fillna(0.0).mean(axis=1)
        out[mode] = dict(port=port, rets=rets, ons=ons, tg=tg)

    t0, t1 = min(f.index[0] for f, _ in data.values()), max(f.index[-1] for f, _ in data.values())
    print(f"\nPeriode: {t0.date()} → {t1.date()} | {len(data)} pair | LEV {LEV:g}x | biaya {COST_SIDE*100:.2f}%/sisi (round trip {2*COST_SIDE*100:.2f}%)")
    print("Funding bruto rata-rata (APR, %/tahun): " + ", ".join(
        f"{s[:-4]} {f.mean()*3*365*100:.1f}" for s, (f, _) in data.items()))

    print(f"\n{'mode':7s} {'CAGR':>7s} {'Sharpe':>7s} {'MaxDD':>7s} {'%waktu ON':>10s} {'ganti posisi':>13s}")
    res = {}
    for mode in ("always", "filter"):
        m, d = metrics(out[mode]["port"])
        res[mode] = (m, d)
        on_share = float(np.mean([o.mean() for o in out[mode]["ons"].values()]))
        print(f"{mode:7s} {m['cagr']*100:6.1f}% {m['sharpe']:7.2f} {m['mdd']*100:6.2f}% {on_share*100:9.1f}% {sum(out[mode]['tg'].values()):13d}")

    print("\nReturn per tahun kalender (%, modal total termasuk margin):")
    yr = pd.DataFrame({m: (1 + res[m][1]).groupby(res[m][1].index.year).prod() - 1 for m in res}) * 100
    print(yr.round(2).to_string())

    print("\nPer pair, mode filter (CAGR %, % waktu ON):")
    for s in data:
        m, _ = metrics(out["filter"]["rets"][s])
        print(f"  {s:9s} CAGR {m['cagr']*100:6.1f}%  ON {out['filter']['ons'][s].mean()*100:5.1f}%")

    print("\nTekanan margin (kenaikan harga maksimum selama posisi terbuka, dari entry ke high harian):")
    for s, (f, k) in data.items():
        if k is None or len(k) < 5:
            continue
        on_d = out["filter"]["ons"][s].groupby(out["filter"]["ons"][s].index.normalize()).max()
        k2 = k.reindex(on_d.index).dropna()
        worst, run_start, cur_on = 0.0, None, False
        for dt in on_d.index:
            if dt not in k2.index:
                continue
            if on_d[dt] and not cur_on:
                run_start, cur_on = k2.loc[dt, "close"], True
            elif not on_d[dt]:
                cur_on = False
            if cur_on and run_start:
                worst = max(worst, k2.loc[dt, "high"] / run_start - 1)
        flag = "  ⚠ > 1/LEV: akan butuh tambahan margin besar" if worst > 1 / LEV * 0.9 else ""
        print(f"  {s:9s} naik maks {worst*100:6.1f}%{flag}")

    print("\nKEPUTUSAN (kriteria tetap), mode filter:")
    m = res["filter"][0]
    yrs_neg = [y for y, v in yr["filter"].items() if v < 0 and y not in (yr.index[0], yr.index[-1])]
    checks = [("CAGR > 8%", m["cagr"] > 0.08), ("Tidak ada tahun kalender penuh yang negatif", not yrs_neg),
              ("Max drawdown < 5%", abs(m["mdd"]) < 0.05)]
    for name, ok in checks:
        print(f"  [{'LOLOS' if ok else 'GAGAL'}] {name}")
    print("  => " + ("LOLOS kriteria. Masih ada risiko eksekusi/basis/margin yang tidak dimodelkan."
                     if all(ok for _, ok in checks) else "BELUM LOLOS. Laporan apa adanya, tanpa penyesuaian."))
    print(f"  Pembanding: bunga bebas risiko ≈ 4.5%/tahun; mode always CAGR {res['always'][0]['cagr']*100:.1f}%.")


if __name__ == "__main__":
    main()
