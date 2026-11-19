"""
Unit test Trend Following V1.0.  Jalankan:  python -m unittest test_trend_follow -v
Cakupan: lookahead, regime, aturan entry (properti), SL/TP, position sizing, batas risiko,
urutan SL-vs-TP di candle yang sama, gap melewati SL, biaya.
"""
import unittest
from unittest import mock

import numpy as np
import pandas as pd

import tf_backtest as B
import tf_config as C
import trend_follow as TF


def make_df(n=3000, seed=0, drift=0.0004):
    rng = np.random.default_rng(seed)
    d = np.repeat(rng.normal(0, drift, n // 300 + 1), 300)[:n]
    r = d + rng.normal(0, 0.006, n)
    c = 100 * np.exp(np.cumsum(r))
    o = np.r_[c[0], c[:-1]]
    h = np.maximum(o, c) * (1 + np.abs(rng.normal(0, 0.002, n)))
    l = np.minimum(o, c) * (1 - np.abs(rng.normal(0, 0.002, n)))
    ts = pd.date_range("2023-01-01", periods=n, freq="h")
    return pd.DataFrame(dict(timestamp=ts, open=o, high=h, low=l, close=c, volume=1.0))


class TestNoLookahead(unittest.TestCase):
    def test_indicators_and_signals_do_not_change_when_future_is_appended(self):
        full = make_df(2500, seed=3)
        cut = 1800
        a = TF.compute_indicators(full.iloc[:cut], B.resample_4h(full.iloc[:cut]))
        b = TF.compute_indicators(full, B.resample_4h(full))
        # 4H candle terakhir di 'a' bisa lebih pendek; bandingkan sampai 8 jam sebelum cut
        m = cut - 8
        cols = ["ema20", "ema50", "ema200", "rsi", "adx", "atr", "swing_low", "swing_high", "regime",
                "long_sig", "short_sig"]
        for c in cols:
            x, y = a[c].iloc[:m].to_numpy(dtype=float), b[c].iloc[:m].to_numpy(dtype=float)
            np.testing.assert_allclose(x, y, rtol=1e-9, atol=1e-9, equal_nan=True, err_msg=c)

    def test_regime_uses_only_closed_4h_candle(self):
        ts = pd.date_range("2023-01-01", periods=400 * 4, freq="h")
        df = make_df(len(ts), seed=1)
        df["timestamp"] = ts
        d4 = B.resample_4h(df)
        r4 = TF.regime_4h(d4, C.as_dict())
        ind = TF.compute_indicators(df, d4)
        # tiap candle 1H hanya boleh memakai regime dari candle 4H yang close_time <= close 1H
        close1h = ind["timestamp"] + pd.Timedelta(hours=1)
        m = pd.merge_asof(pd.DataFrame({"close_time": close1h}), r4, on="close_time", direction="backward")
        np.testing.assert_array_equal(ind["regime"].to_numpy(), m["regime"].fillna(0).to_numpy())
        # candle 1H pada jam ke-0,1,2 dalam blok 4H memakai regime blok 4H SEBELUMNYA
        first = ind[(ind["timestamp"].dt.hour == 0)].iloc[10]
        self.assertTrue((r4["close_time"] <= first["timestamp"] + pd.Timedelta(hours=1)).any())


class TestRegime(unittest.TestCase):
    def _df4(self, drift):
        n = 600
        c = 100 * np.exp(np.cumsum(np.full(n, drift)))
        ts = pd.date_range("2023-01-01", periods=n, freq="4h")
        return pd.DataFrame(dict(timestamp=ts, open=c, high=c * 1.001, low=c * 0.999, close=c, volume=1.0))

    def test_uptrend_is_bullish_downtrend_bearish(self):
        cfg = C.as_dict()
        self.assertEqual(TF.regime_4h(self._df4(0.002), cfg)["regime"].iloc[-1], 1)
        self.assertEqual(TF.regime_4h(self._df4(-0.002), cfg)["regime"].iloc[-1], -1)

    def test_no_data_yet_is_neutral(self):
        r = TF.regime_4h(self._df4(0.002), C.as_dict())
        self.assertEqual(r["regime"].iloc[0], 0)       # EMA200 belum ada → tidak trade


class TestEntryRules(unittest.TestCase):
    def test_every_long_and_short_signal_satisfies_all_rules(self):
        cfg = C.as_dict()
        n_long = n_short = 0
        for seed in range(6):
            df = make_df(4000, seed=seed, drift=0.0008)
            ind = TF.compute_indicators(df, B.resample_4h(df), cfg)
            zhi = ind[["ema20", "ema50"]].max(axis=1)
            zlo = ind[["ema20", "ema50"]].min(axis=1)
            for i in np.nonzero(ind["long_sig"].to_numpy())[0]:
                r = ind.iloc[i]
                n_long += 1
                self.assertEqual(r["regime"], 1)
                self.assertGreater(r["ema50"], r["ema200"])
                self.assertGreater(r["rsi"], 50)
                self.assertGreater(r["adx"], cfg["ADX_MIN"])
                self.assertGreater(r["close"], r["open"])
                self.assertGreater(r["close"], ind["high"].iloc[i - 1])
                w = slice(i - cfg["PULLBACK_LOOKBACK"] + 1, i + 1)
                self.assertTrue((ind["low"].iloc[w] <= zhi.iloc[w]).any())
                self.assertFalse((ind["close"].iloc[w] < zlo.iloc[w] - cfg["PULLBACK_MAX_BREAK_ATR"] * ind["atr"].iloc[w]).any())
                self.assertLessEqual(r["close"] - r["ema20"], cfg["MAX_DIST_FROM_EMA20_ATR"] * r["atr"] + 1e-9)
            for i in np.nonzero(ind["short_sig"].to_numpy())[0]:
                r = ind.iloc[i]
                n_short += 1
                self.assertEqual(r["regime"], -1)
                self.assertLess(r["ema50"], r["ema200"])
                self.assertLess(r["rsi"], 50)
                self.assertGreater(r["adx"], cfg["ADX_MIN"])
                self.assertLess(r["close"], r["open"])
                self.assertLess(r["close"], ind["low"].iloc[i - 1])
        self.assertGreater(n_long, 0)
        self.assertGreater(n_short, 0)

    def test_no_signal_when_adx_threshold_impossible(self):
        cfg = C.as_dict()
        cfg["ADX_MIN"] = 1000
        df = make_df(3000, seed=2, drift=0.0008)
        ind = TF.compute_indicators(df, B.resample_4h(df), cfg)
        self.assertFalse(ind["long_sig"].any() or ind["short_sig"].any())

    def test_no_signal_without_regime(self):
        df = make_df(3000, seed=2, drift=0.0008)
        ind = TF.compute_indicators(df, B.resample_4h(df).iloc[0:0])
        self.assertFalse(ind["long_sig"].any() or ind["short_sig"].any())

    def test_one_signal_per_candle_by_construction(self):
        df = make_df(3000, seed=5, drift=0.0008)
        ind = TF.compute_indicators(df, B.resample_4h(df))
        self.assertFalse((ind["long_sig"] & ind["short_sig"]).any())


class TestBuildOrder(unittest.TestCase):
    def test_long_sl_tp(self):
        o = TF.build_order("LONG", 100.0, 2.0, 96.0, 5)
        self.assertAlmostEqual(o["sl"], 95.5)
        self.assertAlmostEqual(o["risk_dist"], 4.5)
        self.assertAlmostEqual(o["tp"], 109.0)       # entry + 2 × risiko
        self.assertAlmostEqual(o["sl_atr"], 2.25)

    def test_short_sl_tp(self):
        o = TF.build_order("SHORT", 100.0, 2.0, 104.0, 5)
        self.assertAlmostEqual(o["sl"], 104.5)
        self.assertAlmostEqual(o["tp"], 91.0)

    def test_sl_too_far_or_too_tight_is_skipped(self):
        self.assertIsNone(TF.build_order("LONG", 100.0, 2.0, 90.0, 5))     # 5.25 ATR
        self.assertIsNone(TF.build_order("LONG", 100.0, 2.0, 99.0, 5))     # 0.75 ATR
        self.assertIsNone(TF.build_order("SHORT", 100.0, 2.0, 110.0, 5))
        self.assertIsNone(TF.build_order("SHORT", 100.0, 2.0, 101.0, 5))

    def test_stale_or_invalid_swing_is_skipped(self):
        self.assertIsNone(TF.build_order("LONG", 100.0, 2.0, 96.0, 500))
        self.assertIsNone(TF.build_order("LONG", 100.0, 2.0, float("nan"), 5))
        self.assertIsNone(TF.build_order("LONG", 100.0, 2.0, 101.0, 5))    # swing di atas entry
        self.assertIsNone(TF.build_order("LONG", 100.0, 0.0, 96.0, 5))

    def test_rr_configurable(self):
        cfg = C.as_dict()
        cfg["RISK_REWARD"] = 3.0
        self.assertAlmostEqual(TF.build_order("LONG", 100.0, 2.0, 96.0, 5, cfg)["tp"], 100 + 3 * 4.5)


class TestPositionSizing(unittest.TestCase):
    def test_risk_based_qty(self):
        s = TF.position_size(10_000, 100.0, 98.0)
        self.assertAlmostEqual(s["qty"], 25.0)            # 50 USD risiko / 2
        self.assertAlmostEqual(s["risk_amount"], 50.0)

    def test_leverage_cap_reduces_qty_and_risk(self):
        s = TF.position_size(10_000, 100.0, 99.9)         # qty ideal 500 → notional 50k > 30k
        self.assertAlmostEqual(s["notional"], 30_000.0)
        self.assertLess(s["risk_amount"], 50.0)

    def test_risk_pct_never_exceeds_max(self):
        s = TF.position_size(10_000, 100.0, 98.0, risk_pct=0.5)
        self.assertLessEqual(s["risk_amount"], 10_000 * C.MAX_RISK_PER_TRADE + 1e-9)

    def test_step_and_min_notional(self):
        s = TF.position_size(10_000, 100.0, 98.0, qty_step=10)
        self.assertEqual(s["qty"], 20.0)                  # 25 dibulatkan ke bawah
        self.assertEqual(TF.position_size(100, 100.0, 98.0, min_notional=1000)["qty"], 0.0)

    def test_available_margin_limits_size(self):
        s = TF.position_size(10_000, 100.0, 99.9, available_margin=1_000)
        self.assertLessEqual(s["notional"], 3_000 + 1e-9)

    def test_bad_inputs(self):
        self.assertEqual(TF.position_size(0, 100, 98)["qty"], 0.0)
        self.assertEqual(TF.position_size(1000, 100, 100)["qty"], 0.0)


class TestRiskGate(unittest.TestCase):
    def setUp(self):
        self.g = TF.RiskGate()
        self.t = pd.Timestamp("2024-03-05 10:00")

    def test_max_open_positions(self):
        pos = [dict(risk_amount=10, direction="LONG"), dict(risk_amount=10, direction="SHORT"),
               dict(risk_amount=10, direction="SHORT")]
        self.assertEqual(self.g.can_open(self.t, 10_000, "LONG", 10, pos)[1], "max open positions")

    def test_total_open_risk_cap(self):
        pos = [dict(risk_amount=70, direction="LONG"), dict(risk_amount=70, direction="SHORT")]
        ok, why = self.g.can_open(self.t, 10_000, "LONG", 50, pos)     # 70+70+50 > 150
        self.assertFalse(ok)
        self.assertEqual(why, "max total open risk")

    def test_same_direction_cap(self):
        pos = [dict(risk_amount=10, direction="LONG"), dict(risk_amount=10, direction="LONG")]
        self.assertFalse(self.g.can_open(self.t, 10_000, "LONG", 10, pos)[0])
        self.assertTrue(self.g.can_open(self.t, 10_000, "SHORT", 10, pos)[0])

    def test_daily_loss_limit_blocks_then_resets_next_day(self):
        self.g.roll(self.t, 10_000)
        self.g.record_pnl(-250)                                         # -2.5%
        self.assertEqual(self.g.can_open(self.t, 9_750, "LONG", 10, [])[1], "daily loss limit")
        nxt = self.t + pd.Timedelta(days=1)
        self.assertTrue(self.g.can_open(nxt, 9_750, "LONG", 10, [])[0])

    def test_weekly_loss_limit(self):
        g = TF.RiskGate()
        mon = pd.Timestamp("2024-03-04 00:00")
        g.roll(mon, 10_000)
        for d in range(4):                                              # -1.5% per hari ×4 (tiap hari di bawah limit harian)
            day = mon + pd.Timedelta(days=d)
            g.roll(day, 10_000)
            g.record_pnl(-150)
        ok, why = g.can_open(mon + pd.Timedelta(days=4), 9_400, "LONG", 10, [])
        self.assertFalse(ok)
        self.assertEqual(why, "weekly loss limit")

    def test_risk_per_trade_above_max_rejected(self):
        self.assertFalse(self.g.can_open(self.t, 10_000, "LONG", 150, [])[0])


def _fake_ind(n, sig_i, direction="LONG", hi_after=None, lo_after=None, open_after=None):
    ts = pd.date_range("2023-01-01", periods=n, freq="h")
    d = pd.DataFrame(dict(timestamp=ts, open=100.0, high=100.5, low=99.5, close=100.0, atr=2.0, rsi=60.0,
                          ema50=99.0, ema200=95.0, regime=1.0, long_sig=False, short_sig=False,
                          swing_low=96.0, swing_low_idx=float(sig_i - 5), swing_high=104.0,
                          swing_high_idx=float(sig_i - 5), adx=30.0, ema20=100.0))
    if direction == "LONG":
        d.loc[sig_i, "long_sig"] = True
    else:
        d.loc[sig_i, "short_sig"] = True
    j = sig_i + 1
    if hi_after is not None:
        d.loc[j, "high"] = hi_after
    if lo_after is not None:
        d.loc[j, "low"] = lo_after
    if open_after is not None:
        d.loc[j, "open"] = open_after
    return d


class TestSimulationRules(unittest.TestCase):
    def _run(self, fake):
        n = len(fake)
        base = pd.DataFrame(dict(timestamp=fake["timestamp"], open=fake["open"], high=fake["high"],
                                 low=fake["low"], close=fake["close"], volume=1.0))
        cfg = C.as_dict()
        with mock.patch.object(TF, "compute_indicators", return_value=fake):
            tr, eq, sk, ev = B.simulate({"AAAUSDT": (base, None)}, cfg, 10_000.0, use_funding=False)
        return tr

    def test_sl_wins_when_sl_and_tp_touched_same_candle(self):
        fake = _fake_ind(1200, 1000, hi_after=112.0, lo_after=94.0)    # entry ≈100, SL 95.5, TP 109 → keduanya kena
        tr = self._run(fake)
        self.assertEqual(len(tr), 1)
        self.assertEqual(tr["reason"].iloc[0], "SL")
        self.assertLess(tr["net"].iloc[0], 0)

    def test_tp_exit_and_costs_deducted(self):
        fake = _fake_ind(1200, 1000, hi_after=112.0)
        tr = self._run(fake)
        self.assertEqual(tr["reason"].iloc[0], "TP")
        self.assertGreater(tr["fees"].iloc[0], 0)
        self.assertLess(tr["net"].iloc[0], tr["gross"].iloc[0])
        self.assertGreater(tr["net"].iloc[0], 0)
        self.assertGreater(tr["R"].iloc[0], 1.5)                       # ≈ 2R setelah biaya & slippage

    def test_gap_through_sl_exits_at_open_not_at_sl(self):
        fake = _fake_ind(1200, 1000)                                   # entry normal di candle 1001 (open 100)
        fake.loc[1002, ["open", "high", "low", "close"]] = [92.0, 92.5, 90.0, 91.0]   # candle berikutnya gap melewati SL 95.5
        tr = self._run(fake)
        self.assertEqual(tr["reason"].iloc[0], "SL")
        self.assertLess(tr["exit"].iloc[0], 95.5)                      # keluar di ≈92, lebih buruk dari SL

    def test_entry_skipped_when_open_gaps_away_from_confirmation_close(self):
        fake = _fake_ind(1200, 1000, open_after=104.0)                 # 2 ATR > MAX_ENTRY_GAP_ATR (0.5)
        self.assertEqual(len(self._run(fake)), 0)

    def test_no_entry_before_warmup(self):
        fake = _fake_ind(1200, 100)                                     # sinyal di jam ke-100 < 36 hari
        self.assertEqual(len(self._run(fake)), 0)

    def test_short_trade_tp(self):
        fake = _fake_ind(1200, 1000, direction="SHORT", lo_after=88.0)
        fake["regime"] = -1.0
        fake["rsi"] = 40.0
        tr = self._run(fake)
        self.assertEqual(len(tr), 1)
        self.assertEqual(tr["direction"].iloc[0], "SHORT")
        self.assertEqual(tr["reason"].iloc[0], "TP")

    def test_regime_reversal_closes_position_next_open(self):
        fake = _fake_ind(1200, 1000)
        fake.loc[1002:, "regime"] = -1.0                                # regime berbalik setelah entry
        tr = self._run(fake)
        self.assertEqual(tr["reason"].iloc[0], "REVERSAL")

    def test_time_exit_requires_all_three_conditions(self):
        fake = _fake_ind(1200, 1000)
        fake.loc[1001:, "rsi"] = 40.0                                   # momentum lemah
        fake.loc[1001:, "ema50"] = 101.0                                # struktur: close < EMA50 → tidak mendukung
        tr = self._run(fake)
        self.assertEqual(tr["reason"].iloc[0], "TIME")
        fake2 = _fake_ind(1200, 1000)
        fake2.loc[1001:, "rsi"] = 40.0                                  # struktur masih baik → TIDAK keluar karena waktu
        tr2 = self._run(fake2)
        self.assertEqual(tr2["reason"].iloc[0], "END")


class TestNullHypothesis(unittest.TestCase):
    def test_random_walk_has_no_edge_after_costs(self):
        data = B.synthetic(years=2, seed=11, null=True)
        tr, eq, sk, ev = B.simulate(data, C.as_dict(), 10_000.0)
        m = B.metrics(tr, 10_000.0)
        self.assertGreater(m["n"], 200)
        self.assertLess(m["avgR"], 0.05)                                # tanpa edge → tidak boleh jelas positif


if __name__ == "__main__":
    unittest.main()
